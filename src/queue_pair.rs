use crate::{
    completion_queue::{WCError, WorkCompletion, WorkRequestId},
    error_utilities::{log_last_os_err, log_ret_last_os_err},
    event_listener::{EventListener, LmrInners},
    gid::Gid,
    memory_region::{
        local::{LocalMrReadAccess, LocalMrWriteAccess, RwLocalMrInner},
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

/// Maximum value of `send_wr`
pub(crate) static MAX_SEND_WR: u32 = 10;
/// Maximum value of `recv_wr`
pub(crate) static MAX_RECV_WR: u32 = 10;
/// Maximum value of `send_sge`
pub(crate) static MAX_SEND_SGE: u32 = 10;
/// Maximum value of `recv_sge`
pub(crate) static MAX_RECV_SGE: u32 = 10;

/// Queue pair initialized attribute
#[derive(Clone)]
pub(crate) struct QueuePairInitAttr {
    /// Internal `ibv_qp_init_attr` structure
    pub(crate) qp_init_attr_inner: ibv_qp_init_attr,
}

impl Default for QueuePairInitAttr {
    fn default() -> Self {
        // SAFETY: POD FFI type
        let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        qp_init_attr.qp_context = ptr::null_mut::<libc::c_void>().cast();
        qp_init_attr.send_cq = ptr::null_mut::<ibv_cq>().cast();
        qp_init_attr.recv_cq = ptr::null_mut::<ibv_cq>().cast();
        qp_init_attr.srq = ptr::null_mut::<ibv_cq>().cast();
        qp_init_attr.cap.max_send_wr = MAX_SEND_WR;
        qp_init_attr.cap.max_recv_wr = MAX_RECV_WR;
        qp_init_attr.cap.max_send_sge = MAX_SEND_SGE;
        qp_init_attr.cap.max_recv_sge = MAX_RECV_SGE;
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
    ///
    /// On failure of `ibv_create_qp`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid pd, `send_cq`, `recv_cq`, srq or invalid value provided in `max_send_wr`, `max_recv_wr`, `max_send_sge`, `max_recv_sge` or in `max_inline_data`
    ///
    /// `ENOMEM`    Not enough resources to complete this operation
    ///
    /// `ENOSYS`    QP with this Transport Service Type isn't supported by this RDMA device
    ///
    /// `EPERM`     Not enough permissions to create a QP with this Transport Service Type
    pub(crate) fn build(mut self) -> io::Result<QueuePair> {
        // SAFETY: ffi
        let inner_qp = NonNull::new(unsafe {
            rdma_sys::ibv_create_qp(self.pd.as_ptr(), &mut self.qp_init_attr.qp_init_attr_inner)
        })
        .ok_or_else(log_ret_last_os_err)?;
        Ok(QueuePair {
            pd: Arc::<ProtectionDomain>::clone(&self.pd),
            inner_qp,
            event_listener: Arc::new(self.event_listener.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "event channel is not set for the queue pair builder",
                )
            })?),
            port_num: self.port_num,
            gid_index: self.gid_index,
            qp_init_attr: self.qp_init_attr,
            access: None,
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

    /// Set maximum number of outstanding send requests in the send queue
    pub(crate) fn set_max_send_wr(mut self, max_send_wr: u32) -> Self {
        self.qp_init_attr.qp_init_attr_inner.cap.max_send_wr = max_send_wr;
        self
    }

    /// Set maximum number of outstanding receive requests in the receive queue
    pub(crate) fn set_max_recv_wr(mut self, max_recv_wr: u32) -> Self {
        self.qp_init_attr.qp_init_attr_inner.cap.max_recv_wr = max_recv_wr;
        self
    }

    /// Set maximum number of scatter/gather elements (SGE) in a WR on the send queue
    pub(crate) fn set_max_send_sge(mut self, max_send_sge: u32) -> Self {
        self.qp_init_attr.qp_init_attr_inner.cap.max_send_sge = max_send_sge;
        self
    }

    /// Set maximum number of scatter/gather elements (SGE) in a WR on the receive queue
    pub(crate) fn set_max_recv_sge(mut self, max_recv_sge: u32) -> Self {
        self.qp_init_attr.qp_init_attr_inner.cap.max_recv_sge = max_recv_sge;
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
    pub(crate) pd: Arc<ProtectionDomain>,
    /// event listener
    pub(crate) event_listener: Arc<EventListener>,
    /// internal `ibv_qp` pointer
    pub(crate) inner_qp: NonNull<ibv_qp>,
    /// port number
    pub(crate) port_num: u8,
    /// gid index
    pub(crate) gid_index: usize,
    /// backup for child qp
    pub(crate) qp_init_attr: QueuePairInitAttr,
    /// access of this qp
    pub(crate) access: Option<ibv_access_flags>,
}

impl QueuePair {
    /// get `ibv_qp` pointer
    pub(crate) fn as_ptr(&self) -> *mut ibv_qp {
        self.inner_qp.as_ptr()
    }

    /// get queue pair endpoint
    pub(crate) fn endpoint(&self) -> QueuePairEndpoint {
        QueuePairEndpoint {
            // SAFETY: ?
            // TODO: check safety
            qp_num: unsafe { (*self.as_ptr()).qp_num },
            lid: self.pd.ctx.get_lid(),
            gid: self.pd.ctx.get_gid(),
        }
    }

    /// modify the queue pair state to init
    ///
    /// On failure of `ibv_modify_qp`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid value provided in attr or in `attr_mask`
    ///
    /// `ENOMEM`    Not enough resources to complete this operation
    pub(crate) fn modify_to_init(
        &mut self,
        flag: ibv_access_flags,
        port_num: u8,
    ) -> io::Result<()> {
        // SAFETY: POD FFI type
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.pkey_index = 0;
        attr.port_num = port_num;
        attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
        attr.qp_access_flags = flag.0;
        let flags = ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0.cast()) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        self.access = Some(flag);
        Ok(())
    }

    /// modify the queue pair state to ready to receive
    ///
    /// On failure of `ibv_modify_qp`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid value provided in attr or in `attr_mask`
    ///
    /// `ENOMEM`    Not enough resources to complete this operation
    pub(crate) fn modify_to_rtr(
        &self,
        remote: QueuePairEndpoint,
        start_psn: u32,
        max_dest_rd_atomic: u8,
        min_rnr_timer: u8,
    ) -> io::Result<()> {
        // SAFETY: POD FFI type
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
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0.cast()) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        Ok(())
    }

    /// modify the queue pair state to ready to send
    ///
    /// `timeout`: Value representing the transport (ACK) timeout for use by the remote, expressed as (4.096 μS*2Local ACK Timeout).
    /// Calculated by REQ sender, based on (2 * `PacketLifeTime` + Local CA’s ACK delay).;
    ///
    /// `retry_cnt`: The Error retry counter is decremented each time the requester must retry a packet due to a Local Ack Timeout,
    /// NAK-Sequence Error, or Implied NAK.
    ///
    /// `rnr_retry`:The RNR NAK retry counter is decremented each time the responder returns an RNR NAK.
    /// If the requester’s RNR NAK retry counter is zero, and an RNR NAK packet is received, an RNR NAK retry error occurs.
    /// An exception to the following is if the RNR NAK retry counter is set to 7. This value indicates infinite retry and the counter is not decremented
    ///
    /// On failure of `ibv_modify_qp`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid value provided in attr or in `attr_mask`
    ///
    /// `ENOMEM`    Not enough resources to complete this operation
    pub(crate) fn modify_to_rts(
        &self,
        timeout: u8,
        retry_cnt: u8,
        rnr_retry: u8,
        start_psn: u32,
        max_rd_atomic: u8,
    ) -> io::Result<()> {
        // SAFETY: POD FFI type
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
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0.cast()) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        Ok(())
    }

    /// submit a send request
    ///
    /// On failure of `ibv_post_send`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid value provided in wr
    ///
    /// `ENOMEM`    Send Queue is full or not enough resources to complete this operation
    ///
    /// `EFAULT`    Invalid value provided in qp
    fn submit_send<LR>(&self, lms: &[&LR], wr_id: WorkRequestId, imm: Option<u32>) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
    {
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        let mut sr = SendWr::new_send(lms, wr_id, imm);
        self.event_listener.cq.req_notify(false)?;
        for lm in lms {
            debug!(
                "post_send addr {}, len {}, lkey {} wrid: {}",
                lm.addr(),
                lm.length(),
                // SAFETY: no date race here
                unsafe { lm.lkey_unchecked() },
                sr.as_ref().wr_id,
            );
        }
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_post_send(self.as_ptr(), sr.as_mut(), &mut bad_wr) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        Ok(())
    }

    /// submit a receive request
    ///
    /// On failure of `ibv_post_recv`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid value provided in wr
    ///
    /// `ENOMEM`    Send Queue is full or not enough resources to complete this operation
    ///
    /// `EFAULT`    Invalid value provided in qp
    fn submit_receive<LW>(&self, lms: &[&mut LW], wr_id: WorkRequestId) -> io::Result<()>
    where
        LW: LocalMrWriteAccess,
    {
        let mut rr = RecvWr::new_recv(lms, wr_id);
        let mut bad_wr = std::ptr::null_mut::<ibv_recv_wr>();
        self.event_listener.cq.req_notify(false)?;
        for lm in lms {
            debug!(
                "post_recv addr {}, len {}, lkey {} wrid: {}",
                lm.addr(),
                lm.length(),
                // SAFETY: no date race here
                unsafe { lm.lkey_unchecked() },
                rr.as_ref().wr_id,
            );
        }
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_post_recv(self.as_ptr(), rr.as_mut(), &mut bad_wr) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        Ok(())
    }

    /// submit a read request
    ///
    /// On failure of `ibv_post_send`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid value provided in wr
    ///
    /// `ENOMEM`    Send Queue is full or not enough resources to complete this operation
    ///
    /// `EFAULT`    Invalid value provided in qp
    fn submit_read<LW, RR>(&self, lms: &[&mut LW], rm: &RR, wr_id: WorkRequestId) -> io::Result<()>
    where
        LW: LocalMrWriteAccess,
        RR: RemoteMrReadAccess,
    {
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        let mut sr = SendWr::new_read(lms, wr_id, rm);
        self.event_listener.cq.req_notify(false)?;
        for lm in lms {
            debug!(
                "post_send addr {}, len {}, lkey {} wrid: {}",
                lm.addr(),
                lm.length(),
                // SAFETY: no date race here
                unsafe { lm.lkey_unchecked() },
                sr.as_ref().wr_id,
            );
        }
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_post_send(self.as_ptr(), sr.as_mut(), &mut bad_wr) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        Ok(())
    }

    /// submit a write request
    ///
    /// On failure of `ibv_post_send`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid value provided in wr
    ///
    /// `ENOMEM`    Send Queue is full or not enough resources to complete this operation
    ///
    /// `EFAULT`    Invalid value provided in qp
    fn submit_write<LR, RW>(
        &self,
        lms: &[&LR],
        rm: &mut RW,
        wr_id: WorkRequestId,
        imm: Option<u32>,
    ) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
        RW: RemoteMrWriteAccess,
    {
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        let mut sr = SendWr::new_write(lms, wr_id, rm, imm);
        self.event_listener.cq.req_notify(false)?;
        for lm in lms {
            debug!(
                "post_send addr {}, len {}, lkey_unchecked {} wrid: {}",
                lm.addr(),
                lm.length(),
                // SAFETY: no date race here
                unsafe { lm.lkey_unchecked() },
                sr.as_ref().wr_id,
            );
        }
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_post_send(self.as_ptr(), sr.as_mut(), &mut bad_wr) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        Ok(())
    }

    /// send a slice of local memory regions
    pub(crate) fn send_sge<'a, LR>(
        self: &Arc<Self>,
        lms: &'a [&'a LR],
        imm: Option<u32>,
    ) -> QueuePairOps<QPSend<'a, LR>>
    where
        LR: LocalMrReadAccess,
    {
        let send = QPSend::new(lms, imm);
        QueuePairOps::new(Arc::<Self>::clone(self), send, get_lmr_inners(lms))
    }

    /// Send raw data
    #[cfg(feature = "raw")]
    pub(crate) async fn send_sge_raw<'a, LR>(
        self: &Arc<Self>,
        lms: &'a [&'a LR],
        imm: Option<u32>,
    ) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
    {
        let (wr_id, mut resp_rx) = self
            .event_listener
            .register_for_read(&get_lmr_inners(lms))?;
        let len = lms.iter().map(|lm| lm.length()).sum();
        self.submit_send(lms, wr_id, imm)?;
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "agent is dropped"))?
            .result()
            .map(|sz| assert_eq!(sz, len))
            .map_err(Into::into)
    }

    /// receive data to a local memory region
    pub(crate) fn receive_sge<'a, LW>(
        self: &Arc<Self>,
        lms: &'a [&'a mut LW],
    ) -> QueuePairOps<QPRecv<'a, LW>>
    where
        LW: LocalMrWriteAccess,
    {
        let recv = QPRecv::new(lms);
        QueuePairOps::new(Arc::<Self>::clone(self), recv, get_mut_lmr_inners(lms))
    }

    /// Receive raw data
    #[cfg(feature = "raw")]
    pub(crate) async fn receive_sge_raw<'a, LW>(
        self: &Arc<Self>,
        lms: &'a [&'a mut LW],
    ) -> io::Result<Option<u32>>
    where
        LW: LocalMrWriteAccess,
    {
        let (wr_id, mut resp_rx) = self
            .event_listener
            .register_for_write(&get_mut_lmr_inners(lms))?;
        let len = lms.iter().map(|lm| lm.length()).sum();
        self.submit_receive(lms, wr_id)?;
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "event_listener is dropped"))?
            .result_with_imm()
            .map(|(sz, imm)| {
                assert_eq!(sz, len);
                imm
            })
            .map_err(Into::into)
    }

    /// read data from `rm` to `lms`
    pub(crate) async fn read_sge<LW, RR>(&self, lms: &[&mut LW], rm: &RR) -> io::Result<()>
    where
        LW: LocalMrWriteAccess,
        RR: RemoteMrReadAccess,
    {
        let (wr_id, mut resp_rx) = self
            .event_listener
            .register_for_write(&get_mut_lmr_inners(lms))?;
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
    pub(crate) async fn write_sge<LR, RW>(
        &self,
        lms: &[&LR],
        rm: &mut RW,
        imm: Option<u32>,
    ) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
        RW: RemoteMrWriteAccess,
    {
        let (wr_id, mut resp_rx) = self
            .event_listener
            .register_for_read(&get_lmr_inners(lms))?;
        let len: usize = lms.iter().map(|lm| lm.length()).sum();
        self.submit_write(lms, rm, wr_id, imm)?;
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "agent is dropped"))?
            .result()
            .map(|sz| assert_eq!(sz, len))
            .map_err(Into::into)
    }

    /// Send data in `lm`
    pub(crate) async fn send<LR>(self: &Arc<Self>, lm: &LR) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
    {
        self.send_sge(&[lm], None).await
    }

    /// read data from `rm` to `lm`
    pub(crate) async fn read<LW, RR>(&self, lm: &mut LW, rm: &RR) -> io::Result<()>
    where
        LW: LocalMrWriteAccess,
        RR: RemoteMrReadAccess,
    {
        self.read_sge(&[lm], rm).await
    }

    /// write data from `lm` to `rm`
    pub(crate) async fn write<LR, RW>(
        &self,
        lm: &LR,
        rm: &mut RW,
        imm: Option<u32>,
    ) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
        RW: RemoteMrWriteAccess,
    {
        self.write_sge(&[lm], rm, imm).await
    }
}

unsafe impl Sync for QueuePair {}

unsafe impl Send for QueuePair {}

impl Drop for QueuePair {
    fn drop(&mut self) {
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_destroy_qp(self.as_ptr()) };
        if errno != 0_i32 {
            log_last_os_err();
        }
    }
}

/// Get `LmrInners` corresponding to the lms that going to be
/// used by RDMA ops
fn get_lmr_inners<LR>(lms: &[&LR]) -> LmrInners
where
    LR: LocalMrReadAccess,
{
    lms.iter()
        .map(|lm| Arc::<RwLocalMrInner>::clone(lm.get_inner()))
        .collect()
}

/// Get `LmrInners` corresponding to the mutable lms that going to be
/// used by RDMA ops
fn get_mut_lmr_inners<LR>(lms: &[&mut LR]) -> LmrInners
where
    LR: LocalMrReadAccess,
{
    let imlms: Vec<&LR> = lms.iter().map(|lm| &**lm).collect();
    get_lmr_inners(&imlms)
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
pub(crate) struct QPSend<'lm, LR>
where
    LR: LocalMrReadAccess,
{
    /// local memory regions
    lms: &'lm [&'lm LR],
    /// length of data to send
    len: usize,
    /// Optionally, an immediate 4 byte value may be transmitted with the data buffer.
    imm: Option<u32>,
}

impl<'lm, LR> QPSend<'lm, LR>
where
    LR: LocalMrReadAccess,
{
    /// Create a new send operation from `lms`
    fn new(lms: &'lm [&'lm LR], imm: Option<u32>) -> Self
    where
        LR: LocalMrReadAccess,
    {
        Self {
            len: lms.iter().map(|lm| lm.length()).sum(),
            lms,
            imm,
        }
    }
}

impl<LR> QueuePairOp for QPSend<'_, LR>
where
    LR: LocalMrReadAccess,
{
    type Output = ();

    fn submit(&self, qp: &QueuePair, wr_id: WorkRequestId) -> io::Result<()> {
        qp.submit_send(self.lms, wr_id, self.imm)
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
pub(crate) struct QPRecv<'lm, LW>
where
    LW: LocalMrWriteAccess,
{
    /// the local memory regions
    lms: &'lm [&'lm mut LW],
}

impl<'lm, LW> QPRecv<'lm, LW>
where
    LW: LocalMrWriteAccess,
{
    /// create a new queue pair receive operation
    fn new(lms: &'lm [&'lm mut LW]) -> Self {
        Self { lms }
    }
}

impl<LW> QueuePairOp for QPRecv<'_, LW>
where
    LW: LocalMrWriteAccess,
{
    type Output = (usize, Option<u32>);

    fn submit(&self, qp: &QueuePair, wr_id: WorkRequestId) -> io::Result<()> {
        qp.submit_receive(self.lms, wr_id)
    }

    fn should_resubmit(&self, e: &io::Error) -> bool {
        matches!(e.kind(), io::ErrorKind::OutOfMemory)
    }

    fn result(&self, wc: WorkCompletion) -> Result<Self::Output, WCError> {
        wc.result_with_imm()
    }
}

/// Queue pair operation state
#[derive(Debug)]
enum QueuePairOpsState {
    /// It's in init state, not yet submitted
    Init(LmrInners),
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
    fn new(qp: Arc<QueuePair>, op: Op, inners: LmrInners) -> Self {
        Self {
            qp,
            state: QueuePairOpsState::Init(inners),
            op,
        }
    }
}

impl<Op: QueuePairOp + Unpin> Future for QueuePairOps<Op> {
    type Output = io::Result<Op::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let s = self.get_mut();
        match s.state {
            QueuePairOpsState::Init(ref inners) => {
                let (wr_id, recv) = s.qp.event_listener.register_for_write(inners)?;
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
