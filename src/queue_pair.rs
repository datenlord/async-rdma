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
use derive_builder::Builder;
use futures::{ready, Future, FutureExt};
use getset::{Getters, Setters};
use parking_lot::RwLock;
use rdma_sys::{
    ibv_access_flags, ibv_ah_attr, ibv_cq, ibv_destroy_qp, ibv_global_route, ibv_modify_qp,
    ibv_mtu, ibv_post_recv, ibv_post_send, ibv_qp, ibv_qp_attr, ibv_qp_attr_mask, ibv_qp_init_attr,
    ibv_qp_state,ibv_recv_wr, ibv_send_wr,
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
/// Default `port_num`
pub(crate) static DEFAULT_PORT_NUM: u8 = 1;
/// Default `gid_index`
pub(crate) static DEFAULT_GID_INDEX: usize = 1;
/// Default `pkey_index`
pub(crate) static DEFAULT_PKEY_INDEX: u16 = 0;

/// Default `flow_label`
pub(crate) static DEFAULT_FLOW_LABEL: u32 = 0;
/// Default `hop_limit`
pub(crate) static DEFAULT_HOP_LIMIT: u8 = 0xff;
/// Default `traffic_class`
pub(crate) static DEFAULT_TRAFFIC_CLASS: u8 = 0;

/// Default `service_level`
pub(crate) static DEFAULT_SERVICE_LEVEL: u8 = 0;
/// Default `src_path_bits`
pub(crate) static DEFAULT_SRC_PATH_BITS: u8 = 0;
/// Default `static_rate`
pub(crate) static DEFAULT_STATIC_RATE: u8 = 0;
/// Default `is_global`
pub(crate) static DEFAULT_IS_GLOBAL: u8 = 1;

/// Default `rq_psn`
pub(crate) static DEFAULT_RQ_PSN: u32 = 0;
/// Default `max_dest_rd_atomic`
pub(crate) static DEFAULT_MAX_DEST_RD_ATOMIC: u8 = 1;
/// Default `min_rnr_timer`
pub(crate) static DEFAULT_MIN_RNR_TIMER: u8 = 0x12;
/// Default `mtu`
pub(crate) static DEFAULT_MTU: MTU = MTU::MTU512;

/// Default `timeout`
pub(crate) static DEFAULT_TIMEOUT: u8 = 0x12;
/// Default `retry_cnt`
pub(crate) static DEFAULT_RETRY_CNT: u8 = 6;
/// Default `rnr_retry`
pub(crate) static DEFAULT_RNR_RETRY: u8 = 6;
/// Default `sq_psn`
pub(crate) static DEFAULT_SQ_PSN: u32 = 0;
/// Default `max_rd_atomic`
pub(crate) static DEFAULT_MAX_RD_ATOMIC: u8 = 1;
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
            port_num: DEFAULT_PORT_NUM,
            gid_index: DEFAULT_GID_INDEX,
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
            cur_state: RwLock::new(QueuePairState::Init),
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
#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize, Getters, Setters)]
#[getset(set, get = "with_prefix")]
pub(crate) struct QueuePairEndpoint {
    /// queue pair number
    qp_num: u32,
    /// lid
    lid: u16,
    /// device gid
    gid: Gid,
}

/// All of the necessary data to reach a remote destination. In connected transport modes (RC, UC)
/// the AH is associated with a queue pair (QP). In the datagram transport modes (UD), the AH is
/// associated with a work request (WR)
#[derive(Debug, Clone, Copy, Getters, Setters, Builder)]
#[builder(derive(Debug, Copy))]
#[getset(set, get = "with_prefix")]
pub(crate) struct AddressHandler {
    /// Attributes of the Global Routing Headers (GRH), as described in the table below.
    /// This is useful when sending packets to another subnet
    #[builder(
        field(type = "GlobalRouteHeaderBuilder", build = "self.grh.build()?"),
        setter(custom)
    )]
    grh: GlobalRouteHeader,
    /// If the destination is in same subnet, the LID of the port to which the subnet delivers the packets to.
    /// If the destination is in another subnet, the LID of the Router
    dest_lid: u16,
    /// 4 bits. The Service Level to be used
    #[builder(default = "DEFAULT_SERVICE_LEVEL")]
    service_level: u8,
    /// The used Source Path Bits. This is useful when LMC is used in the port, i.e. each port covers a
    /// range of LIDs. The packets are being sent with the port's base LID, bitwised ORed with the value
    /// of the source path bits. The value 0 indicates the port's base LID is used
    #[builder(default = "DEFAULT_SRC_PATH_BITS")]
    src_path_bits: u8,
    /// A value which limits the rate of packets that being sent to the subnet. This can be useful if the
    /// rate of the packet origin is higher than the rate of the destination
    #[builder(default = "DEFAULT_STATIC_RATE")]
    static_rate: u8,
    /// If this value contains any value other than zero, then GRH information exists in this AH, thus the
    /// field grh if valid
    #[builder(default = "DEFAULT_IS_GLOBAL")]
    is_global: u8,
    /// The local physical port that the packets will be sent from
    port_num: u8,
}

impl AddressHandlerBuilder {
    /// Get sub builder's mutable reference
    pub(crate) fn grh(&mut self) -> &mut GlobalRouteHeaderBuilder {
        &mut self.grh
    }
}

impl From<GlobalRouteHeaderBuilderError> for AddressHandlerBuilderError {
    #[inline]
    fn from(e: GlobalRouteHeaderBuilderError) -> Self {
        Self::ValidationError(e.to_string())
    }
}

impl From<AddressHandlerBuilderError> for RQAttrBuilderError {
    #[inline]
    fn from(e: AddressHandlerBuilderError) -> Self {
        Self::ValidationError(e.to_string())
    }
}

impl From<RQAttrBuilderError> for io::Error {
    #[inline]
    fn from(e: RQAttrBuilderError) -> Self {
        io::Error::new(io::ErrorKind::Other, e.to_string())
    }
}

impl From<SQAttrBuilderError> for io::Error {
    #[inline]
    fn from(e: SQAttrBuilderError) -> Self {
        io::Error::new(io::ErrorKind::Other, e.to_string())
    }
}

impl From<AddressHandler> for ibv_ah_attr {
    #[inline]
    fn from(ah: AddressHandler) -> Self {
        // SAFETY: POD FFI type
        let mut ah_attr = unsafe { std::mem::zeroed::<ibv_ah_attr>() };
        ah_attr.dlid = ah.dest_lid;
        ah_attr.sl = ah.service_level;
        ah_attr.src_path_bits = ah.src_path_bits;
        ah_attr.static_rate = ah.static_rate;
        ah_attr.is_global = ah.is_global;
        ah_attr.port_num = ah.port_num;
        ah_attr.grh = ah.grh.into();
        ah_attr
    }
}

/// Gloabel route information about remote end.
/// This is useful when sending packets to another subnet.
#[derive(Debug, Clone, Copy, Getters, Setters, Builder)]
#[builder(derive(Debug, Copy))]
#[getset(set, get = "with_prefix")]
pub(crate) struct GlobalRouteHeader {
    /// The GID that is used to identify the destination port of the packets
    dgid: Gid,
    /// 20 bits. If this value is set to a non-zero value, it gives a hint for switches and routers
    /// with multiple outbound paths that these sequence of packets must be delivered in order,
    /// those staying on the same path, so that they won't be reordered.
    #[builder(default = "DEFAULT_FLOW_LABEL")]
    flow_label: u32,
    /// An index in the port's GID table that will be used to identify the originator of the packet
    sgid_index: u8,
    /// The number of hops (i.e. the number of routers) that the packet is permitted to take before
    /// being discarded. This ensures that a packet will not loop indefinitely between routers if a
    /// routing loop occur. Each router decrement by one this value at the packet and when this value
    /// reaches 0, this packet is discarded. Setting the value to 0 or 1 will ensure that the packet
    /// won't leave the local subnet.
    #[builder(default = "DEFAULT_HOP_LIMIT")]
    hop_limit: u8,
    /// Using this value, the originator of the packets specifies the required delivery priority for
    /// handling them by the routers
    #[builder(default = "DEFAULT_TRAFFIC_CLASS")]
    traffic_class: u8,
}

impl From<GlobalRouteHeader> for ibv_global_route {
    #[inline]
    fn from(grh: GlobalRouteHeader) -> Self {
        // SAFETY: POD FFI type
        let mut ibv_grh = unsafe { std::mem::zeroed::<ibv_global_route>() };
        ibv_grh.dgid = grh.dgid.into();
        ibv_grh.flow_label = grh.flow_label;
        ibv_grh.hop_limit = grh.hop_limit;
        ibv_grh.sgid_index = grh.sgid_index;
        ibv_grh.traffic_class = grh.traffic_class;
        ibv_grh
    }
}

/// The path MTU (Maximum Transfer Unit) i.e. the maximum payload size of a packet that
/// can be transferred in the path. For UC and RC QPs, when needed, the RDMA device will
/// automatically fragment the messages to packet of this size.
#[derive(Debug, Clone, Copy)]
pub enum MTU {
    /// IBV_MTU_256 - 256 bytes
    MTU256,
    /// IBV_MTU_512 - 512 bytes
    MTU512,
    /// IBV_MTU_1024 - 1024 bytes
    MTU1024,
    /// IBV_MTU_2048 - 2048 bytes
    MTU2048,
    /// IBV_MTU_4096 - 4096 bytes
    MTU4096,
}

impl From<MTU> for u32 {
    #[inline]
    fn from(mtu: MTU) -> Self {
        match mtu {
            MTU::MTU256 => ibv_mtu::IBV_MTU_256,
            MTU::MTU512 => ibv_mtu::IBV_MTU_512,
            MTU::MTU1024 => ibv_mtu::IBV_MTU_1024,
            MTU::MTU2048 => ibv_mtu::IBV_MTU_2048,
            MTU::MTU4096 => ibv_mtu::IBV_MTU_4096,
        }
    }
}

/// Attributes for QP's receive queue to receive messages
#[derive(Debug, Clone, Copy, Getters, Setters, Builder)]
#[builder(derive(Debug, Copy))]
#[getset(set, get = "with_prefix")]
pub(crate) struct RQAttr {
    /// The path MTU (Maximum Transfer Unit) i.e. the maximum payload size of a packet that can be
    /// transferred in the path. For UC and RC QPs, when needed, the RDMA device will automatically
    /// fragment the messages to packet of this size.
    #[builder(default = "DEFAULT_MTU")]
    mtu: MTU,
    /// Retmoe destination qp number
    dest_qp_number: u32,
    /// Queue pair information used to establish connection
    #[builder(
        field(
            type = "AddressHandlerBuilder",
            build = "self.address_handler.build()?"
        ),
        setter(custom)
    )]
    address_handler: AddressHandler,
    /// A 24 bits value of the Packet Sequence Number of the received packets for RC and UC QPs
    #[builder(default = "DEFAULT_RQ_PSN")]
    rq_psn: u32,
    /// The number of RDMA Reads & atomic operations outstanding at any time that can be handled by
    /// this QP as a destination. Relevant only for RC QPs
    #[builder(default = "DEFAULT_MAX_DEST_RD_ATOMIC")]
    max_dest_rd_atomic: u8,
    /// Minimum RNR NAK Timer Field Value. When an incoming message to this QP should consume a Work
    /// Request from the Receive Queue, but not Work Request is outstanding on that Queue, the QP will
    /// send an RNR NAK packet to the initiator. It does not affect RNR NAKs sent for other reasons.
    /// The value can be one of the following numeric values since those values aren’t enumerated:
    ///     0 - 655.36 milliseconds delay
    ///     1 - 0.01 milliseconds delay
    ///     2 - 0.02 milliseconds delay
    ///     3 - 0.03 milliseconds delay
    ///     4 - 0.04 milliseconds delay
    ///     5 - 0.06 milliseconds delay
    ///     6 - 0.08 milliseconds delay
    ///     7 - 0.12 milliseconds delay
    ///     8 - 0.16 milliseconds delay
    ///     9 - 0.24 milliseconds delay
    ///     10 - 0.32 milliseconds delay
    ///     11 - 0.48 milliseconds delay
    ///     12 - 0.64 milliseconds delay
    ///     13 - 0.96 milliseconds delay
    ///     14 - 1.28 milliseconds delay
    ///     15 - 1.92 milliseconds delay
    ///     16 - 2.56 milliseconds delay
    ///     17 - 3.84 milliseconds delay
    ///     18 - 5.12 milliseconds delay
    ///     19 - 7.68 milliseconds delay
    ///     20 - 10.24 milliseconds delay
    ///     21 - 15.36 milliseconds delay
    ///     22 - 20.48 milliseconds delay
    ///     23 - 30.72 milliseconds delay
    ///     24 - 40.96 milliseconds delay
    ///     25 - 61.44 milliseconds delay
    ///     26 - 81.92 milliseconds delay
    ///     27 - 122.88 milliseconds delay
    ///     28 - 163.84 milliseconds delay
    ///     29 - 245.76 milliseconds delay
    ///     30 - 327.68 milliseconds delay
    ///     31 - 491.52 milliseconds delay
    /// Relevant only for RC QPs
    #[builder(default = "DEFAULT_MIN_RNR_TIMER")]
    min_rnr_timer: u8,
}

impl RQAttr {
    /// Reset the infomation of remote end
    pub(crate) fn reset(&mut self, remote: &QueuePairEndpoint, gid_index: u8, port_num: u8) {
        let _ = self
            .address_handler
            .grh
            .set_sgid_index(gid_index)
            .set_dgid(*remote.get_gid());
        let _ = self
            .address_handler
            .set_dest_lid(*remote.get_lid())
            .set_port_num(port_num);
        let _ = self.set_dest_qp_number(*remote.get_qp_num());
    }
}

impl RQAttrBuilder {
    /// Get sub builder's mutable reference
    pub(crate) fn address_handler(&mut self) -> &mut AddressHandlerBuilder {
        &mut self.address_handler
    }
}

/// Attributes for QP's send queue to receive messages
#[derive(Debug, Clone, Copy, Getters, Setters, Builder)]
#[builder(derive(Debug, Copy))]
pub(crate) struct SQAttr {
    /// The minimum timeout that a QP waits for ACK/NACK from remote QP before retransmitting the packet.
    /// The value zero is special value which means wait an infinite time for the ACK/NACK (useful for
    /// debugging). For any other value of timeout, the time calculation is: `4.09*2^timeout`.
    /// Relevant only to RC QPs.
    #[builder(default = "DEFAULT_TIMEOUT")]
    timeout: u8,
    /// A 3 bits value of the total number of times that the QP will try to resend the packets before
    /// reporting an error because the remote side doesn't answer in the primary path
    #[builder(default = "DEFAULT_RETRY_CNT")]
    retry_cnt: u8,
    /// A 3 bits value of the total number of times that the QP will try to resend the packets when an
    /// RNR NACK was sent by the remote QP before reporting an error. The value 7 is special and specify
    /// to retry infinite times in case of RNR.
    #[builder(default = "DEFAULT_RNR_RETRY")]
    rnr_retry: u8,
    /// A 24 bits value of the Packet Sequence Number of the sent packets for any QP.
    #[builder(default = "DEFAULT_SQ_PSN")]
    sq_psn: u32,
    /// The number of RDMA Reads & atomic operations outstanding at any time that can be handled by
    /// this QP as an initiator. Relevant only for RC QPs.
    #[builder(default = "DEFAULT_MAX_RD_ATOMIC")]
    max_rd_atomic: u8,
}

/// The state of qp
#[repr(u32)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum QueuePairState {
    /// IBV_QPS_RESET - Reset state
    Reset,
    /// IBV_QPS_INIT - Initialized state
    Init,
    /// IBV_QPS_RTR - Ready To Receive state
    ReadyToRecv,
    /// IBV_QPS_RTS - Ready To Send state
    ReadyToSend,
    /// IBV_QPS_SQD - Send Queue Drain state
    SQDrain,
    /// IBV_QPS_SQE - Send Queue Error state
    SQErr,
    /// IBV_QPS_ERR - Error state
    Err,
    /// IBV_QPS_UNKNOWN - Unknown state
    Unknown,
}

impl From<u32> for QueuePairState {
    fn from(num: u32) -> Self {
        if num == ibv_qp_state::IBV_QPS_RTS {
            Self::ReadyToSend
        } else if num == ibv_qp_state::IBV_QPS_RTR {
            Self::ReadyToRecv
        } else if num == ibv_qp_state::IBV_QPS_INIT {
            Self::Init
        } else if num == ibv_qp_state::IBV_QPS_ERR {
            Self::Err
        } else if num == ibv_qp_state::IBV_QPS_RESET {
            Self::Reset
        } else if num == ibv_qp_state::IBV_QPS_UNKNOWN {
            Self::Unknown
        } else if num == ibv_qp_state::IBV_QPS_SQE {
            Self::SQErr
        } else {
            Self::SQDrain
        }
    }
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
    /// Assume that this is the current QP state. This is useful if it is known
    /// to the application that the QP state is different from the assumed state
    /// by the low-level driver. It can be one of the enumerated values as qp_state.
    pub(crate) cur_state: RwLock<QueuePairState>,
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
        pkey_index: u16,
    ) -> io::Result<()> {
        // SAFETY: POD FFI type
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
        attr.pkey_index = pkey_index;
        attr.port_num = port_num;
        attr.qp_access_flags = flag.0;
        let flags = ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
            | ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_PORT
            | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
        // SAFETY: ffi, and qp will not modify by other threads
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0.cast()) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        self.access = Some(flag);
        *self.cur_state.write() = QueuePairState::Init;
        Ok(())
    }

    /// modify the queue pair state to ready to receive
    ///
    /// On failure of `ibv_modify_qp`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid value provided in attr or in `attr_mask`
    ///
    /// `ENOMEM`    Not enough resources to complete this operation
    pub(crate) fn modify_to_rtr(&self, rq_attr: RQAttr) -> io::Result<()> {
        // SAFETY: POD FFI type
        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
        qp_attr.path_mtu = rq_attr.mtu.into();
        qp_attr.dest_qp_num = rq_attr.dest_qp_number;
        qp_attr.rq_psn = rq_attr.rq_psn;
        qp_attr.max_dest_rd_atomic = rq_attr.max_dest_rd_atomic;
        qp_attr.min_rnr_timer = rq_attr.min_rnr_timer;
        qp_attr.ah_attr = rq_attr.address_handler.into();
        let flags = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_AV
            | ibv_qp_attr_mask::IBV_QP_PATH_MTU
            | ibv_qp_attr_mask::IBV_QP_DEST_QPN
            | ibv_qp_attr_mask::IBV_QP_RQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
            | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
        // SAFETY: ffi, and qp will not modify by other threads
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut qp_attr, flags.0.cast()) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        *self.cur_state.write() = QueuePairState::ReadyToRecv;
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
    pub(crate) fn modify_to_rts(&self, sq_attr: SQAttr) -> io::Result<()> {
        // SAFETY: POD FFI type
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        attr.timeout = sq_attr.timeout;
        attr.retry_cnt = sq_attr.retry_cnt;
        attr.rnr_retry = sq_attr.rnr_retry;
        attr.sq_psn = sq_attr.sq_psn;
        attr.max_rd_atomic = sq_attr.max_rd_atomic;
        let flags = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT
            | ibv_qp_attr_mask::IBV_QP_RNR_RETRY
            | ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        // SAFETY: ffi, and qp will not modify by other threads
        let errno = unsafe { ibv_modify_qp(self.as_ptr(), &mut attr, flags.0.cast()) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        *self.cur_state.write() = QueuePairState::ReadyToSend;
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

/// Builders to attributes helper
pub(crate) fn builders_into_attrs(
    mut recv_queue_attr: RQAttrBuilder,
    send_queue_attr: SQAttrBuilder,
    remote: &QueuePairEndpoint,
    port_num: u8,
    gid_index: u8,
) -> io::Result<(RQAttr, SQAttr)> {
    let _ = recv_queue_attr.dest_qp_number(remote.qp_num);
    let _ = recv_queue_attr
        .address_handler()
        .port_num(port_num)
        .dest_lid(*remote.get_lid());
    let _ = recv_queue_attr
        .address_handler()
        .grh()
        .sgid_index(gid_index)
        .dgid(*remote.get_gid());
    let sr = send_queue_attr.build()?;
    let rr = recv_queue_attr.build()?;
    Ok((rr, sr))
}
