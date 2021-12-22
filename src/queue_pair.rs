use crate::{
    completion_queue::{WCError, WorkCompletion, WorkRequestId},
    event_listener::EventListener,
    gid::Gid,
    memory_region::{LocalMemoryRegion, RemoteMemoryRegion},
    protection_domain::ProtectionDomain,
    work_request::{RecvWr, SendWr},
};
use futures::{ready, Future};
use rdma_sys::{
    ibv_access_flags, ibv_cq, ibv_destroy_qp, ibv_modify_qp, ibv_post_recv, ibv_post_send, ibv_qp,
    ibv_qp_attr, ibv_qp_attr_mask, ibv_qp_init_attr, ibv_qp_state, ibv_recv_wr, ibv_send_wr,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    io,
    pin::Pin,
    ptr::{self, NonNull},
    sync::Arc,
    task::Poll,
};
use tokio::sync::mpsc;
use tracing::debug;

struct QueuePairInitAttr {
    qp_init_attr_inner: rdma_sys::ibv_qp_init_attr,
}

impl Default for QueuePairInitAttr {
    fn default() -> Self {
        let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        qp_init_attr.qp_context = ptr::null::<libc::c_void>() as _;
        qp_init_attr.send_cq = ptr::null::<ibv_cq>() as _;
        qp_init_attr.recv_cq = ptr::null::<ibv_cq>() as _;
        qp_init_attr.srq = ptr::null::<ibv_cq>() as _;
        qp_init_attr.cap.max_send_wr = 10;
        qp_init_attr.cap.max_recv_wr = 10;
        qp_init_attr.cap.max_send_sge = 10;
        qp_init_attr.cap.max_recv_sge = 10;
        qp_init_attr.cap.max_inline_data = 0;
        qp_init_attr.qp_type = rdma_sys::ibv_qp_type::IBV_QPT_RC;
        qp_init_attr.sq_sig_all = 0;
        Self {
            qp_init_attr_inner: qp_init_attr,
        }
    }
}

impl Debug for QueuePairInitAttr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuePairInitAttr")
            .field("qp_context", &self.qp_init_attr_inner.qp_context)
            .field("send_cq", &self.qp_init_attr_inner.send_cq)
            .field("recv_cq", &self.qp_init_attr_inner.recv_cq)
            .field("srq", &self.qp_init_attr_inner.srq)
            .field("max_send_wr", &self.qp_init_attr_inner.cap.max_send_wr)
            .field("max_recv_wr", &self.qp_init_attr_inner.cap.max_recv_wr)
            .field("max_send_sge", &self.qp_init_attr_inner.cap.max_send_sge)
            .field("max_recv_sge", &self.qp_init_attr_inner.cap.max_recv_sge)
            .field(
                "max_inline_data",
                &self.qp_init_attr_inner.cap.max_inline_data,
            )
            .field("qp_type", &self.qp_init_attr_inner.qp_type)
            .field("sq_sig_all", &self.qp_init_attr_inner.sq_sig_all)
            .finish()
    }
}

pub struct QueuePairBuilder {
    pub pd: Arc<ProtectionDomain>,
    event_listener: Option<EventListener>,
    qp_init_attr: QueuePairInitAttr,
}

impl QueuePairBuilder {
    pub fn new(pd: &Arc<ProtectionDomain>) -> Self {
        Self {
            pd: pd.clone(),
            qp_init_attr: QueuePairInitAttr::default(),
            event_listener: None,
        }
    }

    pub fn build(mut self) -> io::Result<QueuePair> {
        let inner_qp = NonNull::new(unsafe {
            rdma_sys::ibv_create_qp(
                self.pd.as_ptr(),
                &mut self.qp_init_attr.qp_init_attr_inner as *mut _,
            )
        })
        .ok_or(io::ErrorKind::Other)?;
        Ok(QueuePair {
            pd: self.pd.clone(),
            inner_qp,
            event_listener: self.event_listener.unwrap(),
        })
    }

    pub fn set_event_listener(mut self, el: EventListener) -> Self {
        self.qp_init_attr.qp_init_attr_inner.send_cq = el.cq.as_ptr();
        self.qp_init_attr.qp_init_attr_inner.recv_cq = el.cq.as_ptr();
        self.event_listener = Some(el);
        self
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct QueuePairEndpoint {
    qp_num: u32,
    lid: u16,
    gid: Gid,
}

pub struct QueuePair {
    pd: Arc<ProtectionDomain>,
    event_listener: EventListener,
    inner_qp: NonNull<ibv_qp>,
}

impl QueuePair {
    pub(crate) fn as_ptr(&self) -> *mut ibv_qp {
        self.inner_qp.as_ptr()
    }

    pub fn endpoint(&self) -> QueuePairEndpoint {
        QueuePairEndpoint {
            qp_num: unsafe { (*self.as_ptr()).qp_num },
            lid: self.pd.ctx.get_lid(),
            gid: self.pd.ctx.gid,
        }
    }

    pub fn modify_to_init(&self, flag: ibv_access_flags) -> io::Result<()> {
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.pkey_index = 0;
        attr.port_num = 1;
        attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
        attr.qp_access_flags = flag.0;
        let flags = ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0 as _) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    pub fn modify_to_rtr(
        &self,
        remote: QueuePairEndpoint,
        start_psn: u32,
        max_dest_rd_atomic: u8,
        min_rnr_timer: u8,
    ) -> io::Result<()> {
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
        attr.path_mtu = self.pd.ctx.get_active_mtu();
        attr.dest_qp_num = remote.qp_num;
        attr.rq_psn = start_psn;
        attr.max_dest_rd_atomic = max_dest_rd_atomic;
        attr.min_rnr_timer = min_rnr_timer;
        attr.ah_attr.dlid = remote.lid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        attr.ah_attr.grh.dgid = remote.gid.into();
        attr.ah_attr.grh.hop_limit = 0xff;
        //TODO: Configurable sgid_index
        attr.ah_attr.grh.sgid_index = 1;
        let flags = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_AV
            | ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN
            | ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
            | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0 as _) };
        debug!(
            "modify qp to rtr, err info : {:?}",
            io::Error::from_raw_os_error(errno)
        );
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    pub fn modify_to_rts(
        &self,
        timeout: u8,
        retry_cnt: u8,
        rnr_retry: u8,
        start_psn: u32,
        max_rd_atomic: u8,
    ) -> io::Result<()> {
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        attr.timeout = timeout;
        attr.retry_cnt = retry_cnt;
        attr.rnr_retry = rnr_retry;
        attr.sq_psn = start_psn;
        attr.max_rd_atomic = max_rd_atomic;
        let flags = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT
            | ibv_qp_attr_mask::IBV_QP_RNR_RETRY
            | ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0 as _) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    fn submit_send(&self, lms: Vec<&LocalMemoryRegion>, wr_id: WorkRequestId) -> io::Result<()> {
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        let mut sr = SendWr::new_send(lms, wr_id);
        self.event_listener.cq.req_notify(false).unwrap();
        let errno = unsafe { ibv_post_send(self.as_ptr(), sr.as_mut(), &mut bad_wr) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    fn submit_receive(&self, lms: Vec<&LocalMemoryRegion>, wr_id: WorkRequestId) -> io::Result<()> {
        let mut rr = RecvWr::new_recv(lms, wr_id);
        let mut bad_wr = std::ptr::null_mut::<ibv_recv_wr>();
        self.event_listener.cq.req_notify(false).unwrap();
        let errno = unsafe { ibv_post_recv(self.as_ptr(), rr.as_mut(), &mut bad_wr) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    fn submit_read(
        &self,
        lms: Vec<&LocalMemoryRegion>,
        rm: &RemoteMemoryRegion,
        wr_id: WorkRequestId,
    ) -> io::Result<()> {
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        let mut sr = SendWr::new_read(lms, wr_id, rm);
        self.event_listener.cq.req_notify(false).unwrap();
        let errno = unsafe { ibv_post_send(self.as_ptr(), sr.as_mut(), &mut bad_wr) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    fn submit_write(
        &self,
        lms: Vec<&LocalMemoryRegion>,
        rm: &RemoteMemoryRegion,
        wr_id: WorkRequestId,
    ) -> io::Result<()> {
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        let mut sr = SendWr::new_write(lms, wr_id, rm);
        self.event_listener.cq.req_notify(false).unwrap();
        let errno = unsafe { ibv_post_send(self.as_ptr(), sr.as_mut(), &mut bad_wr) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    pub fn send_sge(self: &Arc<Self>, lms: Vec<&LocalMemoryRegion>) -> QueuePairOps<QPSend> {
        let send = QPSend::new(lms);
        QueuePairOps::new(self.clone(), send)
    }

    pub fn receive_sge(self: &Arc<Self>, lms: Vec<&LocalMemoryRegion>) -> QueuePairOps<QPRecv> {
        let recv = QPRecv::new(lms);
        QueuePairOps::new(self.clone(), recv)
    }

    pub async fn read_sge(
        &self,
        lms: Vec<&LocalMemoryRegion>,
        rm: &RemoteMemoryRegion,
    ) -> Result<(), WCError> {
        let (wr_id, mut resp_rx) = self.event_listener.register();
        let len: usize = lms.iter().map(|lm| lm.length()).sum();
        self.submit_read(lms, rm, wr_id).unwrap();
        resp_rx
            .recv()
            .await
            .unwrap()
            .err()
            .map(|sz| assert_eq!(sz, len))
    }

    pub async fn write_sge(
        &self,
        lms: Vec<&LocalMemoryRegion>,
        rm: &RemoteMemoryRegion,
    ) -> Result<(), WCError> {
        let (wr_id, mut resp_rx) = self.event_listener.register();
        let len: usize = lms.iter().map(|lm| lm.length()).sum();
        self.submit_write(lms, rm, wr_id).unwrap();
        resp_rx
            .recv()
            .await
            .unwrap()
            .err()
            .map(|sz| assert_eq!(sz, len))
    }

    pub fn send(self: &Arc<Self>, lm: &LocalMemoryRegion) -> QueuePairOps<QPSend> {
        self.send_sge(vec![lm])
    }

    pub fn receive(self: &Arc<Self>, lm: &LocalMemoryRegion) -> QueuePairOps<QPRecv> {
        self.receive_sge(vec![lm])
    }

    pub async fn read(
        &self,
        lm: &mut LocalMemoryRegion,
        rm: &RemoteMemoryRegion,
    ) -> Result<(), WCError> {
        self.read_sge(vec![lm], rm).await
    }

    pub async fn write(
        &self,
        lm: &LocalMemoryRegion,
        rm: &RemoteMemoryRegion,
    ) -> Result<(), WCError> {
        self.write_sge(vec![lm], rm).await
    }
}

unsafe impl Sync for QueuePair {}

unsafe impl Send for QueuePair {}

impl Drop for QueuePair {
    fn drop(&mut self) {
        let errno = unsafe { ibv_destroy_qp(self.as_ptr()) };
        assert_eq!(errno, 0);
    }
}

pub trait QueuePairOp {
    type Output;

    fn submit(&self, qp: &QueuePair, wr_id: WorkRequestId) -> io::Result<()>;

    fn parse_wc(&self, wc: WorkCompletion) -> Self::Output;
}

pub struct QPSend<'lm> {
    lms: Vec<&'lm LocalMemoryRegion>,
    len: usize,
}

impl<'lm> QPSend<'lm> {
    fn new(lms: Vec<&'lm LocalMemoryRegion>) -> Self {
        Self {
            len: lms.iter().map(|lm| lm.length()).sum(),
            lms,
        }
    }
}

impl<'lm> QueuePairOp for QPSend<'lm> {
    type Output = Result<(), WCError>;

    fn submit(&self, qp: &QueuePair, wr_id: WorkRequestId) -> io::Result<()> {
        qp.submit_send(self.lms.to_owned(), wr_id)
    }

    fn parse_wc(&self, wc: WorkCompletion) -> Self::Output {
        wc.err().map(|sz| assert_eq!(sz, self.len))
    }
}

pub struct QPRecv<'lm> {
    lms: Vec<&'lm LocalMemoryRegion>,
}

impl<'lm> QPRecv<'lm> {
    fn new(lms: Vec<&'lm LocalMemoryRegion>) -> Self {
        Self { lms }
    }
}

impl<'lm> QueuePairOp for QPRecv<'lm> {
    type Output = Result<usize, WCError>;

    fn submit(&self, qp: &QueuePair, wr_id: WorkRequestId) -> io::Result<()> {
        qp.submit_receive(self.lms.to_owned(), wr_id)
    }

    fn parse_wc(&self, wc: WorkCompletion) -> Self::Output {
        wc.err()
    }
}

enum QueuePairOpsState {
    Init,
    Submitted(mpsc::Receiver<WorkCompletion>),
}

pub struct QueuePairOps<Op: QueuePairOp + Unpin> {
    qp: Arc<QueuePair>,
    state: QueuePairOpsState,
    op: Op,
}

impl<Op: QueuePairOp + Unpin> QueuePairOps<Op> {
    fn new(qp: Arc<QueuePair>, op: Op) -> Self {
        Self {
            qp,
            state: QueuePairOpsState::Init,
            op,
        }
    }
}

impl<Op: QueuePairOp + Unpin> Future for QueuePairOps<Op> {
    type Output = Op::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let s = self.get_mut();
        match &mut s.state {
            QueuePairOpsState::Init => {
                let (wr_id, recv) = s.qp.event_listener.register();
                s.op.submit(&s.qp, wr_id).unwrap();
                s.state = QueuePairOpsState::Submitted(recv);
                Pin::new(s).poll(cx)
            }
            QueuePairOpsState::Submitted(recv) => {
                let wc = ready!(recv.poll_recv(cx)).unwrap();
                Poll::Ready(s.op.parse_wc(wc))
            }
        }
    }
}
