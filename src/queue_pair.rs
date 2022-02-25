use crate::{
    completion_queue::{WCError, WorkCompletion, WorkRequestId},
    event_listener::EventListener,
    gid::Gid,
    memory_region::{
        local::{LocalMrReadAccess, LocalMrWriteAccess},
        remote::{RemoteMrReadAccess, RemoteMrWriteAccess},
    },
    protection_domain::ProtectionDomain,
    work_request::{RecvWr, SendWr},
};
use clippy_utilities::Cast;
use futures::{ready, Future, FutureExt};
use rdma_sys::{
    ibv_access_flags, ibv_cq, ibv_destroy_qp, ibv_modify_qp, ibv_post_recv, ibv_post_send, ibv_qp,
    ibv_qp_attr, ibv_qp_attr_mask, ibv_qp_init_attr, ibv_qp_state, ibv_recv_wr, ibv_send_wr,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    io,
    ops::Sub,
    pin::Pin,
    ptr::{self, NonNull},
    sync::Arc,
    task::Poll,
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{sleep, Sleep},
};
use tracing::debug;

/// Queue pair initialized attribute
struct QueuePairInitAttr {
    /// Internal `ibv_qp_init_attr` structure
    qp_init_attr_inner: ibv_qp_init_attr,
}

impl Default for QueuePairInitAttr {
    fn default() -> Self {
        let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        qp_init_attr.qp_context = ptr::null_mut::<libc::c_void>().cast();
        qp_init_attr.send_cq = ptr::null_mut::<ibv_cq>().cast();
        qp_init_attr.recv_cq = ptr::null_mut::<ibv_cq>().cast();
        qp_init_attr.srq = ptr::null_mut::<ibv_cq>().cast();
        qp_init_attr.cap.max_send_wr = 10;
        qp_init_attr.cap.max_recv_wr = 10;
        qp_init_attr.cap.max_send_sge = 10;
        qp_init_attr.cap.max_recv_sge = 10;
        qp_init_attr.cap.max_inline_data = 0;
        qp_init_attr.qp_type = rdma_sys::ibv_qp_type::IBV_QPT_RC;
        qp_init_attr.sq_sig_all = 0_i32;
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

/// Queue pair builder
#[derive(Debug)]
pub(crate) struct QueuePairBuilder {
    /// Protection domain it belongs to
    pd: Arc<ProtectionDomain>,
    /// Event linstener
    event_listener: Option<EventListener>,
    /// Queue pair init attribute
    qp_init_attr: QueuePairInitAttr,
    /// Port Number
    port_num: u8,
    /// Gid Index
    gid_index: usize,
}

impl QueuePairBuilder {
    /// Create a new queue pair builder
    pub(crate) fn new(pd: &Arc<ProtectionDomain>) -> Self {
        Self {
            pd: Arc::<ProtectionDomain>::clone(pd),
            qp_init_attr: QueuePairInitAttr::default(),
            event_listener: None,
            port_num: 1,
            gid_index: 0,
        }
    }

    /// Create a queue pair
    pub(crate) fn build(mut self) -> io::Result<QueuePair> {
        let inner_qp = NonNull::new(unsafe {
            rdma_sys::ibv_create_qp(self.pd.as_ptr(), &mut self.qp_init_attr.qp_init_attr_inner)
        })
        .ok_or(io::ErrorKind::Other)?;
        Ok(QueuePair {
            pd: Arc::<ProtectionDomain>::clone(&self.pd),
            inner_qp,
            event_listener: self.event_listener.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "event channel is not set for the queue pair builder",
                )
            })?,
            port_num: self.port_num,
            gid_index: self.gid_index,
        })
    }

    /// Set event listener
    pub(crate) fn set_event_listener(mut self, el: EventListener) -> Self {
        self.qp_init_attr.qp_init_attr_inner.send_cq = el.cq.as_ptr();
        self.qp_init_attr.qp_init_attr_inner.recv_cq = el.cq.as_ptr();
        self.event_listener = Some(el);
        self
    }

    /// Set port number
    pub(crate) fn set_port_num(mut self, port_num: u8) -> Self {
        self.port_num = port_num;
        self
    }

    /// Set gid index
    pub(crate) fn set_gid_index(mut self, gid_index: usize) -> Self {
        self.gid_index = gid_index;
        self
    }
}

/// Queue pair information used to hand shake
#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub(crate) struct QueuePairEndpoint {
    /// queue pair number
    qp_num: u32,
    /// lid
    lid: u16,
    /// device gid
    gid: Gid,
}

/// Queue pair wrapper
#[derive(Debug)]
pub(crate) struct QueuePair {
    /// protection domain it belongs to
    pd: Arc<ProtectionDomain>,
    /// event listener
    event_listener: EventListener,
    /// internal `ibv_qp` pointer
    inner_qp: NonNull<ibv_qp>,
    /// port number
    port_num: u8,
    /// gid index
    gid_index: usize,
}

impl QueuePair {
    /// get `ibv_qp` pointer
    pub(crate) fn as_ptr(&self) -> *mut ibv_qp {
        self.inner_qp.as_ptr()
    }

    /// get queue pair endpoint
    pub(crate) fn endpoint(&self) -> QueuePairEndpoint {
        QueuePairEndpoint {
            qp_num: unsafe { (*self.as_ptr()).qp_num },
            lid: self.pd.ctx.get_lid(),
            gid: self.pd.ctx.get_gid(),
        }
    }

    /// modify the queue pair state to init
    pub(crate) fn modify_to_init(&self, flag: ibv_access_flags, port_num: u8) -> io::Result<()> {
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.pkey_index = 0;
        attr.port_num = port_num;
        attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
        attr.qp_access_flags = flag.0;
        let flags = ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0.cast()) };
        if errno != 0_i32 {
            return Err(io::Error::from_raw_os_error(0_i32.sub(errno)));
        }
        Ok(())
    }

    /// modify the queue pair state to ready to receive
    pub(crate) fn modify_to_rtr(
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
        attr.ah_attr.port_num = self.port_num;
        attr.ah_attr.grh.dgid = remote.gid.into();
        attr.ah_attr.grh.hop_limit = 0xff;
        attr.ah_attr.grh.sgid_index = self.gid_index.cast();
        let flags = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_AV
            | ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN
            | ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
            | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0.cast()) };
        debug!(
            "modify qp to rtr, err info : {:?}",
            io::Error::from_raw_os_error(0_i32.sub(errno))
        );
        if errno != 0_i32 {
            return Err(io::Error::from_raw_os_error(0_i32.sub(errno)));
        }
        Ok(())
    }

    /// modify the queue pair state to ready to send
    pub(crate) fn modify_to_rts(
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
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0.cast()) };
        if errno != 0_i32 {
            return Err(io::Error::from_raw_os_error(0_i32.sub(errno)));
        }
        Ok(())
    }

    /// submit a send request
    fn submit_send(&self, lms: &[&dyn LocalMrReadAccess], wr_id: WorkRequestId) -> io::Result<()> {
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        let mut sr = SendWr::new_send(lms, wr_id);
        self.event_listener.cq.req_notify(false)?;
        let errno = unsafe { ibv_post_send(self.as_ptr(), sr.as_mut(), &mut bad_wr) };
        if errno != 0_i32 {
            return Err(io::Error::from_raw_os_error(0_i32.sub(errno)));
        }
        Ok(())
    }

    /// submit a receive request
    fn submit_receive(
        &self,
        lms: &[&mut dyn LocalMrWriteAccess],
        wr_id: WorkRequestId,
    ) -> io::Result<()> {
        let mut rr = RecvWr::new_recv(lms, wr_id);
        let mut bad_wr = std::ptr::null_mut::<ibv_recv_wr>();
        self.event_listener.cq.req_notify(false)?;
        let errno = unsafe { ibv_post_recv(self.as_ptr(), rr.as_mut(), &mut bad_wr) };
        if errno != 0_i32 {
            return Err(io::Error::from_raw_os_error(0_i32.sub(errno)));
        }
        Ok(())
    }

    /// submit a read request
    fn submit_read(
        &self,
        lms: &[&mut dyn LocalMrWriteAccess],
        rm: &dyn RemoteMrReadAccess,
        wr_id: WorkRequestId,
    ) -> io::Result<()> {
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        let mut sr = SendWr::new_read(lms, wr_id, rm);
        self.event_listener.cq.req_notify(false)?;
        let errno = unsafe { ibv_post_send(self.as_ptr(), sr.as_mut(), &mut bad_wr) };
        if errno != 0_i32 {
            return Err(io::Error::from_raw_os_error(0_i32.sub(errno)));
        }
        Ok(())
    }

    /// submit a write request
    fn submit_write(
        &self,
        lms: &[&dyn LocalMrReadAccess],
        rm: &mut dyn RemoteMrWriteAccess,
        wr_id: WorkRequestId,
    ) -> io::Result<()> {
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        let mut sr = SendWr::new_write(lms, wr_id, rm);
        self.event_listener.cq.req_notify(false)?;
        let errno = unsafe { ibv_post_send(self.as_ptr(), sr.as_mut(), &mut bad_wr) };
        if errno != 0_i32 {
            return Err(io::Error::from_raw_os_error(0_i32.sub(errno)));
        }
        Ok(())
    }

    /// send a slice of local memory regions
    pub(crate) fn send_sge<'a>(
        self: &Arc<Self>,
        lms: &'a [&'a dyn LocalMrReadAccess],
    ) -> QueuePairOps<QPSend<'a>> {
        let send = QPSend::new(lms);
        QueuePairOps::new(Arc::<Self>::clone(self), send)
    }

    /// receive data to a local memory region
    pub(crate) fn receive_sge<'a>(
        self: &Arc<Self>,
        lms: &'a [&'a mut dyn LocalMrWriteAccess],
    ) -> QueuePairOps<QPRecv<'a>> {
        let recv = QPRecv::new(lms);
        QueuePairOps::new(Arc::<Self>::clone(self), recv)
    }

    /// read data from `rm` to `lms`
    pub(crate) async fn read_sge(
        &self,
        lms: &[&mut dyn LocalMrWriteAccess],
        rm: &dyn RemoteMrReadAccess,
    ) -> io::Result<()> {
        let (wr_id, mut resp_rx) = self.event_listener.register();
        let len: usize = lms.iter().map(|lm| lm.length()).sum();
        self.submit_read(lms, rm, wr_id)?;
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "agent is dropped"))?
            .result()
            .map(|sz| assert_eq!(sz, len))
            .map_err(Into::into)
    }

    /// write data from `lms` to `rm`
    pub(crate) async fn write_sge(
        &self,
        lms: &[&dyn LocalMrReadAccess],
        rm: &mut dyn RemoteMrWriteAccess,
    ) -> io::Result<()> {
        let (wr_id, mut resp_rx) = self.event_listener.register();
        let len: usize = lms.iter().map(|lm| lm.length()).sum();
        self.submit_write(lms, rm, wr_id)?;
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "agent is dropped"))?
            .result()
            .map(|sz| assert_eq!(sz, len))
            .map_err(Into::into)
    }

    /// Send data in `lm`
    pub(crate) async fn send(self: &Arc<Self>, lm: &dyn LocalMrReadAccess) -> io::Result<()> {
        self.send_sge(&[lm]).await
    }

    /// Receive data and store it into `lm`
    pub(crate) async fn _receive(
        self: &Arc<Self>,
        lm: &'_ mut dyn LocalMrWriteAccess,
    ) -> io::Result<usize> {
        self.receive_sge(&[lm]).await
    }

    /// read data from `rm` to `lm`
    pub(crate) async fn read(
        &self,
        lm: &mut dyn LocalMrWriteAccess,
        rm: &dyn RemoteMrReadAccess,
    ) -> io::Result<()> {
        self.read_sge(&[lm], rm).await
    }

    /// write data from `lm` to `rm`
    pub(crate) async fn write(
        &self,
        lm: &dyn LocalMrReadAccess,
        rm: &mut dyn RemoteMrWriteAccess,
    ) -> io::Result<()> {
        self.write_sge(&[lm], rm).await
    }
}

unsafe impl Sync for QueuePair {}

unsafe impl Send for QueuePair {}

impl Drop for QueuePair {
    fn drop(&mut self) {
        let errno = unsafe { ibv_destroy_qp(self.as_ptr()) };
        assert_eq!(errno, 0_i32);
    }
}

/// Queue pair op resubmit delay, default is 1 sec
static RESUBMIT_DELAY: Duration = Duration::from_secs(1);

/// Queue pair operation trait
pub(crate) trait QueuePairOp {
    /// work completion output type
    type Output;

    /// submit the operation
    fn submit(&self, qp: &QueuePair, wr_id: WorkRequestId) -> io::Result<()>;

    /// should resubmit?
    fn should_resubmit(&self, e: &io::Error) -> bool;

    /// get the request result of the work completion
    fn result(&self, wc: WorkCompletion) -> Result<Self::Output, WCError>;
}

/// Queue pair send operation
#[derive(Debug)]
pub(crate) struct QPSend<'lm> {
    /// local memory regions
    lms: &'lm [&'lm dyn LocalMrReadAccess],
    /// length of data to send
    len: usize,
}

impl<'lm> QPSend<'lm> {
    /// Create a new send operation from `lms`
    fn new(lms: &'lm [&'lm dyn LocalMrReadAccess]) -> Self {
        Self {
            len: lms.iter().map(|lm| lm.length()).sum(),
            lms,
        }
    }
}

impl QueuePairOp for QPSend<'_> {
    type Output = ();

    fn submit(&self, qp: &QueuePair, wr_id: WorkRequestId) -> io::Result<()> {
        qp.submit_send(self.lms, wr_id)
    }

    fn should_resubmit(&self, e: &io::Error) -> bool {
        matches!(e.kind(), io::ErrorKind::OutOfMemory)
    }

    fn result(&self, wc: WorkCompletion) -> Result<Self::Output, WCError> {
        wc.result().map(|sz| assert_eq!(sz, self.len))
    }
}
/// Queue pair receive operation
#[derive(Debug)]
pub(crate) struct QPRecv<'lm> {
    /// the local memory regions
    lms: &'lm [&'lm mut dyn LocalMrWriteAccess],
}

impl<'lm> QPRecv<'lm> {
    /// create a new queue pair receive operation
    fn new(lms: &'lm [&'lm mut dyn LocalMrWriteAccess]) -> Self {
        Self { lms }
    }
}

impl QueuePairOp for QPRecv<'_> {
    type Output = usize;

    fn submit(&self, qp: &QueuePair, wr_id: WorkRequestId) -> io::Result<()> {
        qp.submit_receive(self.lms, wr_id)
    }

    fn should_resubmit(&self, e: &io::Error) -> bool {
        matches!(e.kind(), io::ErrorKind::OutOfMemory)
    }

    fn result(&self, wc: WorkCompletion) -> Result<Self::Output, WCError> {
        wc.result()
    }
}

/// Queue pair operation state
#[derive(Debug)]
enum QueuePairOpsState {
    /// It's in init state, not yet submitted
    Init,
    /// Submit
    Submit(WorkRequestId, Option<mpsc::Receiver<WorkCompletion>>),
    /// Sleep and prepare to resubmit
    PendingToResubmit(
        Pin<Box<Sleep>>,
        WorkRequestId,
        Option<mpsc::Receiver<WorkCompletion>>,
    ),
    /// have submitted
    Submitted(mpsc::Receiver<WorkCompletion>),
}

/// Queue pair operation wrapper
#[derive(Debug)]
pub(crate) struct QueuePairOps<Op: QueuePairOp + Unpin> {
    /// the internal queue pair
    qp: Arc<QueuePair>,
    /// operation state
    state: QueuePairOpsState,
    /// the operation
    op: Op,
}

impl<Op: QueuePairOp + Unpin> QueuePairOps<Op> {
    /// Create a new queue pair operation wrapper
    fn new(qp: Arc<QueuePair>, op: Op) -> Self {
        Self {
            qp,
            state: QueuePairOpsState::Init,
            op,
        }
    }
}

impl<Op: QueuePairOp + Unpin> Future for QueuePairOps<Op> {
    type Output = io::Result<Op::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let s = self.get_mut();
        match s.state {
            QueuePairOpsState::Init => {
                let (wr_id, recv) = s.qp.event_listener.register();
                s.state = QueuePairOpsState::Submit(wr_id, Some(recv));
                Pin::new(s).poll(cx)
            }
            QueuePairOpsState::Submit(wr_id, ref mut recv) => {
                if let Err(e) = s.op.submit(&s.qp, wr_id) {
                    if s.op.should_resubmit(&e) {
                        let sleep = Box::pin(sleep(RESUBMIT_DELAY));
                        s.state = QueuePairOpsState::PendingToResubmit(sleep, wr_id, recv.take());
                    } else {
                        tracing::error!("failed to submit the operation");
                        // TODO: deregister wrid
                        return Poll::Ready(Err(e));
                    }
                } else {
                    match recv.take().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::Other, "Bug in queue pair op poll")
                    }) {
                        Ok(recv) => s.state = QueuePairOpsState::Submitted(recv),
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                Pin::new(s).poll(cx)
            }
            QueuePairOpsState::PendingToResubmit(ref mut sleep, wr_id, ref mut recv) => {
                ready!(sleep.poll_unpin(cx));
                s.state = QueuePairOpsState::Submit(wr_id, recv.take());
                Pin::new(s).poll(cx)
            }
            QueuePairOpsState::Submitted(ref mut recv) => {
                Poll::Ready(match ready!(recv.poll_recv(cx)) {
                    Some(wc) => s.op.result(wc).map_err(Into::into),
                    None => Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Wc receiver unexpect closed",
                    )),
                })
            }
        }
    }
}
