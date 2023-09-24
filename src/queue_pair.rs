use crate::{
    completion_queue::{WCError, WorkCompletion, WorkRequestId},
    context::{check_dev_cap, Context},
    cq_event_listener::{CQEventListener, LmrInners},
    error_utilities::{log_last_os_err, log_ret_last_os_err, log_ret_last_os_err_with_note},
    gid::Gid,
    impl_from_buidler_error_for_another, impl_into_io_error,
    memory_region::{
        local::{LocalMrReadAccess, LocalMrWriteAccess, RwLocalMrInner},
        remote::{RemoteMrReadAccess, RemoteMrWriteAccess},
    },
    protection_domain::ProtectionDomain,
    work_request::{RecvWr, SendWr},
    DEFAULT_ACCESS,
};
use clippy_utilities::Cast;
use derive_builder::Builder;
use futures::{ready, Future, FutureExt};
use getset::{Getters, MutGetters, Setters};
use parking_lot::RwLock;
use rdma_sys::{
    ibv_access_flags, ibv_ah_attr, ibv_cq, ibv_destroy_qp, ibv_global_route, ibv_modify_qp,
    ibv_mtu, ibv_post_recv, ibv_post_send, ibv_qp, ibv_qp_attr, ibv_qp_attr_mask, ibv_qp_init_attr,
    ibv_qp_state, ibv_query_qp, ibv_recv_wr, ibv_send_wr, ibv_srq,
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
pub(crate) static MAX_SEND_SGE: u32 = 5;
/// Maximum value of `recv_sge`
pub(crate) static MAX_RECV_SGE: u32 = 5;
/// Maximum value of `inline_data`
pub(crate) static MAX_INLINE_DATA: u32 = 0;
/// Default value of `sq_sig_all`
pub(crate) static SQ_SIG_ALL: i32 = 0_i32;

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

/// The requested attributes of the newly created QP.
#[derive(Debug, Clone, Copy, Getters, Setters, Builder)]
#[builder(derive(Debug, Copy))]
#[getset(set, get = "pub")]
pub(crate) struct QueuePairCap {
    /// The maximum number of outstanding Work Requests that can be posted to the Send Queue
    /// in that Queue Pair. Value can be [1..dev_cap.max_qp_wr].
    #[builder(default = "MAX_SEND_WR")]
    max_send_wr: u32,
    /// The maximum number of outstanding Work Requests that can be posted to the Receive Queue
    /// in that Queue Pair. Value can be [1..dev_cap.max_qp_wr]. This value is ignored if the
    /// Queue Pair is associated with an SRQ.
    #[builder(default = "MAX_RECV_WR")]
    max_recv_wr: u32,
    /// The maximum number of scatter/gather elements in any Work Request that can be posted to
    /// the Send Queue in that Queue Pair. Value can be [1..dev_cap.max_sge].
    #[builder(default = "MAX_SEND_SGE")]
    max_send_sge: u32,
    /// The maximum number of scatter/gather elements in any Work Request that can be posted to
    /// the Receive Queue in that Queue Pair. Value can be [1..dev_cap.max_sge]. This value is
    /// ignored if the Queue Pair is associated with an SRQ.
    #[builder(default = "MAX_RECV_SGE")]
    max_recv_sge: u32,
    /// The maximum message size (in bytes) that can be posted inline to the Send Queue. 0, if
    /// no inline message is requested.
    #[builder(default = "MAX_INLINE_DATA")]
    max_inline_data: u32,
}

impl QueuePairCap {
    /// Check the attributes and return error if the preset values exceed the capabilities of
    /// rdma device.
    pub(crate) fn check_dev_qp_cap(&self, ctx: &Context) -> io::Result<()> {
        let dev_attr = ctx.dev_attr();
        // `dev_attr.max_sge` and `dev_attr.max_sge_rd` may be different, and you can check
        // this by running `ibv_devinfo -d <rdma_dev_name> -v` on your terminal.
        // check it in more detail if your dev has this feature.
        check_dev_cap(&self.max_recv_sge, &dev_attr.max_sge.cast(), "max_recv_sge")?;
        check_dev_cap(&self.max_send_sge, &dev_attr.max_sge.cast(), "max_send_sge")?;
        check_dev_cap(&self.max_recv_wr, &dev_attr.max_qp_wr.cast(), "max_recv_wr")?;
        check_dev_cap(&self.max_send_wr, &dev_attr.max_qp_wr.cast(), "max_send_wr")?;
        // TODO: check more attributes such as `max_srq_sge` ...
        Ok(())
    }
}

impl_from_buidler_error_for_another!(QueuePairCapBuilderError, QueuePairInitAttrBuilderError);

impl_into_io_error!(QueuePairCapBuilderError);

/// Describes the requested attributes of the newly created QP
#[derive(Debug, Clone, Copy, MutGetters, Getters, Setters, Builder)]
#[builder(derive(Debug, Copy))]
#[getset(set, get = "pub")]
pub(crate) struct QueuePairInitAttr {
    /// (optional) User defined value which will be available in qp->qp_context.
    /// Not support for now.
    #[builder(default = "None")]
    qp_context: Option<*mut libc::c_void>,
    /// A Completion Queue to be associated with the Send Queue
    send_cq: *mut ibv_cq,
    /// A Completion Queue to be associated with the Receive Queue
    recv_cq: *mut ibv_cq,
    /// (optional) A Shared Receive Queue, that was returned from ibv_create_srq(), that this
    /// Queue Pair will be associated with. Otherwise, NULL. Not support for now.
    #[builder(default = "None")]
    srq: Option<*mut ibv_srq>,
    /// Attributes of the Queue Pair size.
    #[builder(
        field(type = "QueuePairCapBuilder", build = "self.qp_cap.build()?"),
        setter(custom)
    )]
    #[getset(get_mut = "pub")]
    qp_cap: QueuePairCap,
    // TODO: add high level enum wrapper
    /// Requested Transport Service Type of this QP:
    /// IBV_QPT_RC  Reliable Connection
    /// IBV_QPT_UC  Unreliable Connection
    /// IBV_QPT_UD  Unreliable Datagram
    /// Only support RC for now.
    #[builder(default = "rdma_sys::ibv_qp_type::IBV_QPT_RC")]
    qp_type: u32,
    /// The Signaling level of Work Requests that will be posted to the Send Queue in this QP.
    /// If it is equal to 0, in every Work Request submitted to the Send Queue, the user must
    /// decide whether to generate a Work Completion for successful completions or not.
    /// Otherwise all Work Requests that will be submitted to the Send Queue will always generate
    /// a Work Completion.
    #[builder(default = "SQ_SIG_ALL")]
    sq_sig_all: i32,
    /// Access flag
    #[builder(default = "*DEFAULT_ACCESS")]
    access: ibv_access_flags,
    /// Primary P_Key index. The value of the entry in the P_Key table that outgoing
    /// packets from this QP will be sent with and incoming packets to this QP will
    /// be verified within the Primary path.
    #[builder(default = "DEFAULT_PKEY_INDEX")]
    pkey_index: u16,
}

impl_into_io_error!(QueuePairInitAttrBuilderError);

impl QueuePairInitAttrBuilder {
    /// Get sub builder's mutable reference
    pub(crate) fn qp_cap(&mut self) -> &mut QueuePairCapBuilder {
        &mut self.qp_cap
    }
}

impl From<QueuePairInitAttr> for ibv_qp_init_attr {
    #[inline]
    fn from(s: QueuePairInitAttr) -> Self {
        // SAFETY: POD FFI type
        let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        qp_init_attr.qp_context = s
            .qp_context
            .unwrap_or(ptr::null_mut::<libc::c_void>().cast());
        qp_init_attr.send_cq = s.send_cq;
        qp_init_attr.recv_cq = s.recv_cq;
        qp_init_attr.srq = s.srq.unwrap_or(ptr::null_mut::<ibv_srq>().cast());
        qp_init_attr.cap.max_send_wr = s.qp_cap.max_send_wr;
        qp_init_attr.cap.max_recv_wr = s.qp_cap.max_recv_wr;
        qp_init_attr.cap.max_send_sge = s.qp_cap.max_send_sge;
        qp_init_attr.cap.max_recv_sge = s.qp_cap.max_recv_sge;
        qp_init_attr.cap.max_inline_data = s.qp_cap.max_inline_data;
        qp_init_attr.qp_type = s.qp_type;
        qp_init_attr.sq_sig_all = s.sq_sig_all;
        qp_init_attr
    }
}

/// Queue pair information used to establish connection.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize, Getters, Setters, Builder)]
#[getset(set, get = "pub")]
pub struct QueuePairEndpoint {
    /// A 24 bits value of the QP number of RC and UC QPs; when sending data, packets will be
    /// sent to this QP number and when receiving data, packets will be accepted only from this
    /// QP number
    qp_num: u32,
    /// A 16 bit address assigned to end nodes by the subnet manager.Each LID is unique within
    /// its subnet.
    lid: u16,
    /// A 128-bit identifier used to identify a Port on a network adapter, a port on a Router,
    /// or a Multicast Group.
    ///
    /// A GID is a valid 128-bit IPv6 address (per RFC 2373) with additional properties / restrictions
    /// defined within IBA to facilitate efficient discovery, communication, and routing.
    gid: Gid,
}

impl_into_io_error!(QueuePairEndpointBuilderError);

/// All of the necessary data to reach a remote destination. In connected transport modes (RC, UC)
/// the AH is associated with a queue pair (QP). In the datagram transport modes (UD), the AH is
/// associated with a work request (WR)
#[derive(Debug, Clone, Copy, Getters, Setters, Builder)]
#[builder(derive(Debug, Copy))]
#[getset(set, get = "pub")]
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
    #[builder(default = "DEFAULT_PORT_NUM")]
    port_num: u8,
}

impl AddressHandlerBuilder {
    /// Get sub builder's mutable reference
    pub(crate) fn grh(&mut self) -> &mut GlobalRouteHeaderBuilder {
        &mut self.grh
    }

    /// Get `port_num` of this buidler
    pub(crate) fn get_port_num(&self) -> Option<u8> {
        self.port_num
    }
}

impl_into_io_error!(SQAttrBuilderError);

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

impl_from_buidler_error_for_another!(AddressHandlerBuilderError, RQAttrBuilderError);

/// Gloabel route information about remote end.
/// This is useful when sending packets to another subnet.
#[derive(Debug, Clone, Copy, Getters, Setters, Builder)]
#[builder(derive(Debug, Copy))]
#[getset(set, get = "pub")]
pub(crate) struct GlobalRouteHeader {
    /// The GID that is used to identify the destination port of the packets
    dgid: Gid,
    /// 20 bits. If this value is set to a non-zero value, it gives a hint for switches and routers
    /// with multiple outbound paths that these sequence of packets must be delivered in order,
    /// those staying on the same path, so that they won't be reordered.
    #[builder(default = "DEFAULT_FLOW_LABEL")]
    flow_label: u32,
    /// An index in the port's GID table that will be used to identify the originator of the packet
    #[builder(default = "DEFAULT_GID_INDEX.cast()")]
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

impl_from_buidler_error_for_another!(GlobalRouteHeaderBuilderError, AddressHandlerBuilderError);

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
#[getset(set, get = "pub")]
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
    /// Relevant only for RC QPs.
    #[builder(default = "DEFAULT_MIN_RNR_TIMER")]
    min_rnr_timer: u8,
}

impl_into_io_error!(RQAttrBuilderError);

impl RQAttrBuilder {
    /// Get sub builder's mutable reference
    pub(crate) fn address_handler(&mut self) -> &mut AddressHandlerBuilder {
        &mut self.address_handler
    }

    /// Reset the infomation of remote end
    pub(crate) fn reset_remote_info(&mut self, remote: &QueuePairEndpoint) {
        let _ = self
            .dest_qp_number(*remote.qp_num())
            .address_handler()
            .dest_lid(*remote.lid())
            .grh()
            .dgid(*remote.gid());
    }

    /// Get `port_num` of this buidler
    pub(crate) fn get_port_num(&self) -> u8 {
        self.address_handler
            .get_port_num()
            .unwrap_or(DEFAULT_PORT_NUM)
    }

    /// Get source gid index of this buidler
    pub(crate) fn get_sgid_index(&self) -> u8 {
        self.address_handler
            .grh
            .sgid_index
            .unwrap_or_else(|| DEFAULT_GID_INDEX.cast())
    }
}

/// Attributes for QP's send queue to receive messages
#[derive(Debug, Clone, Copy, Getters, Setters, Builder)]
#[builder(derive(Debug, Copy))]
#[getset(set, get = "pub")]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePairState {
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
    #[inline]
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
#[derive(Debug, Getters, Setters, Builder)]
#[getset(set, get = "pub")]
pub(crate) struct QueuePair {
    /// protection domain it belongs to
    pd: Arc<ProtectionDomain>,
    /// cq event listener
    cq_event_listener: Arc<CQEventListener>,
    /// internal `ibv_qp` pointer
    inner_qp: NonNull<ibv_qp>,
    /// Assume that this is the current QP state. This is useful if it is known
    /// to the application that the QP state is different from the assumed state
    /// by the low-level driver. It can be one of the enumerated values as qp_state.
    cur_state: Arc<RwLock<QueuePairState>>,
}

impl_into_io_error!(QueuePairBuilderError);

impl QueuePair {
    /// get `ibv_qp` pointer
    pub(crate) fn as_ptr(&self) -> *mut ibv_qp {
        self.inner_qp.as_ptr()
    }

    /// Get attributes of this qp.
    /// Make sure only get masked member from attrs.
    pub(crate) fn query_attrs(
        &self,
        mask: ibv_qp_attr_mask,
    ) -> io::Result<(ibv_qp_attr, ibv_qp_init_attr)> {
        // SAFETY: POD FFI type
        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        // SAFETY: POD FFI type
        let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        let errno = unsafe {
            ibv_query_qp(
                self.as_ptr(),
                &mut qp_attr,
                mask.0.cast(),
                &mut qp_init_attr,
            )
        };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err_with_note("ibv_query_qp failed"));
        }
        Ok((qp_attr, qp_init_attr))
    }

    /// Query the state of this qp
    pub(crate) fn query_state(&self) -> io::Result<QueuePairState> {
        let mask = ibv_qp_attr_mask::IBV_QP_STATE;
        let res = self.query_attrs(mask)?;
        Ok(res.0.qp_state.into())
    }

    /// get queue pair endpoint
    pub(crate) fn endpoint(&self) -> QueuePairEndpoint {
        QueuePairEndpoint {
            // SAFETY: ?
            // TODO: check safety
            qp_num: unsafe { (*self.as_ptr()).qp_num },
            lid: self.pd.ctx.get_lid(),
            gid: *self.pd.ctx.gid(),
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
        let mut send_attr = SendWr::new_send(lms, wr_id, imm);
        self.cq_event_listener.cq.req_notify(false)?;
        for lm in lms {
            debug!(
                "post_send addr {}, len {}, lkey {} wrid: {}",
                lm.addr(),
                lm.length(),
                // SAFETY: no date race here
                unsafe { lm.lkey_unchecked() },
                send_attr.as_ref().wr_id,
            );
        }
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_post_send(self.as_ptr(), send_attr.as_mut(), &mut bad_wr) };
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
        let mut recv_attr = RecvWr::new_recv(lms, wr_id);
        let mut bad_wr = std::ptr::null_mut::<ibv_recv_wr>();
        self.cq_event_listener.cq.req_notify(false)?;
        for lm in lms {
            debug!(
                "post_recv addr {}, len {}, lkey {} wrid: {}",
                lm.addr(),
                lm.length(),
                // SAFETY: no date race here
                unsafe { lm.lkey_unchecked() },
                recv_attr.as_ref().wr_id,
            );
        }
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_post_recv(self.as_ptr(), recv_attr.as_mut(), &mut bad_wr) };
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
        let mut send_attr = SendWr::new_read(lms, wr_id, rm);
        self.cq_event_listener.cq.req_notify(false)?;
        for lm in lms {
            debug!(
                "post_send addr {}, len {}, lkey {} wrid: {}",
                lm.addr(),
                lm.length(),
                // SAFETY: no date race here
                unsafe { lm.lkey_unchecked() },
                send_attr.as_ref().wr_id,
            );
        }
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_post_send(self.as_ptr(), send_attr.as_mut(), &mut bad_wr) };
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
        let mut send_attr = SendWr::new_write(lms, wr_id, rm, imm);
        self.cq_event_listener.cq.req_notify(false)?;
        for lm in lms {
            debug!(
                "post_send addr {}, len {}, lkey_unchecked {} wrid: {}",
                lm.addr(),
                lm.length(),
                // SAFETY: no date race here
                unsafe { lm.lkey_unchecked() },
                send_attr.as_ref().wr_id,
            );
        }
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_post_send(self.as_ptr(), send_attr.as_mut(), &mut bad_wr) };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err());
        }
        Ok(())
    }

    /// submit a atomic cas request
    ///
    /// On failure of `ibv_post_send`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid value provided in wr
    ///
    /// `ENOMEM`    Send Queue is full or not enough resources to complete this operation
    ///
    /// `EFAULT`    Invalid value provided in qp
    fn submit_cas<LR, RW>(
        &self,
        old_value: u64,
        new_value: u64,
        buf: &LR,
        rm: &mut RW,
        wr_id: WorkRequestId,
    ) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
        RW: RemoteMrWriteAccess,
    {
        let mut cas_wr = SendWr::new_cas(old_value, new_value, buf, rm, wr_id);
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        self.cq_event_listener.cq.req_notify(false)?;

        // SAFETY: ffi
        let errno = unsafe { ibv_post_send(self.as_ptr(), cas_wr.as_mut(), &mut bad_wr) };
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
            .cq_event_listener
            .register_for_read(&get_lmr_inners(lms))?;
        let len: usize = lms.iter().map(|lm| lm.length()).sum();
        self.submit_send(lms, wr_id, imm)?;
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "agent is dropped"))?
            .result()
            .map(|sz| debug!("post size: {sz}, mr len: {len}"))
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
            .cq_event_listener
            .register_for_write(&get_mut_lmr_inners(lms))?;
        let len: usize = lms.iter().map(|lm| lm.length()).sum();
        self.submit_receive(lms, wr_id)?;
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "cq_event_listener is dropped"))?
            .result_with_imm()
            .map(|(sz, imm)| {
                debug!("post size: {sz}, mr len: {len}");
                imm
            })
            .map_err(Into::into)
    }

    /// Receive raw data and excute an fn after `submit_receive` and before `poll`
    ///
    /// **This is an experimental API**
    #[cfg(feature = "exp")]
    pub(crate) async fn receive_sge_fn<'a, LW, F>(
        self: &Arc<Self>,
        lms: &'a [&'a mut LW],
        func: F,
    ) -> io::Result<Option<u32>>
    where
        LW: LocalMrWriteAccess,
        F: FnOnce(),
    {
        let (wr_id, mut resp_rx) = self
            .cq_event_listener
            .register_for_write(&get_mut_lmr_inners(lms))?;
        let len: usize = lms.iter().map(|lm| lm.length()).sum();
        self.submit_receive(lms, wr_id)?;
        func();
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "cq_event_listener is dropped"))?
            .result_with_imm()
            .map(|(sz, imm)| {
                debug!("receivede size {sz}, lms len {len}");
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
            .cq_event_listener
            .register_for_write(&get_mut_lmr_inners(lms))?;
        let len: usize = lms.iter().map(|lm| lm.length()).sum();
        self.submit_read(lms, rm, wr_id)?;
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "agent is dropped"))?
            .result()
            .map(|sz| debug!("post size: {sz}, mr len: {len}"))
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
            .cq_event_listener
            .register_for_read(&get_lmr_inners(lms))?;
        let len: usize = lms.iter().map(|lm| lm.length()).sum();
        self.submit_write(lms, rm, wr_id, imm)?;
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "agent is dropped"))?
            .result()
            .map(|sz| debug!("post size: {sz}, mr len: {len}"))
            .map_err(Into::into)
    }

    /// A 64 bits value in a remote mr being read, compared with `old_value` and if they are equal,
    /// the `new_value` is being written to the remote mr in an atomic way. The original data, before
    /// the compare operation, is being written to the local memory buffer `buf`.
    pub(crate) async fn atomic_cas<LR, RW>(
        &self,
        old_value: u64,
        new_value: u64,
        buf: &LR,
        rm: &mut RW,
    ) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
        RW: RemoteMrWriteAccess,
    {
        let (wr_id, mut resp_rx) = self
            .cq_event_listener
            .register_for_read(&get_lmr_inners(&[buf]))?;
        self.submit_cas(old_value, new_value, buf, rm, wr_id)?;
        resp_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "agent is dropped"))?
            .result()
            .map(|sz| assert_eq!(sz, 8))
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

unsafe impl Sync for QueuePairInitAttrBuilder {}

unsafe impl Send for QueuePairInitAttrBuilder {}

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

    /// is this op's memory region will use to be written with data? For example, send will return false, recv will return true.
    fn is_write() -> bool;
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
        wc.result()
            .map(|sz| debug!("post size: {sz}, mr len: {}", self.len))
    }

    fn is_write() -> bool {
        false
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

    fn is_write() -> bool {
        true
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
                let (wr_id, recv) = s.qp.cq_event_listener.register(inners, Op::is_write())?;
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
    mut recv_attr_builder: RQAttrBuilder,
    send_attr_builder: SQAttrBuilder,
    remote: &QueuePairEndpoint,
) -> io::Result<(RQAttr, SQAttr)> {
    let _ = recv_attr_builder
        .dest_qp_number(remote.qp_num)
        .address_handler()
        .dest_lid(*remote.lid())
        .grh()
        .dgid(*remote.gid());
    let send_attr = send_attr_builder.build()?;
    let recv_attr = recv_attr_builder.build()?;
    Ok((recv_attr, send_attr))
}

/// Test `LocalMr` APIs with multi-pd & multi-connection
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod dev_cap_check_tests {
    use super::*;

    #[test]
    #[should_panic]
    fn recv_sge_cap_overrun() {
        let ctx = Context::open(None, 1, 1).unwrap();
        let cap = QueuePairCapBuilder::default()
            .max_recv_sge(u32::MAX)
            .build()
            .unwrap();
        cap.check_dev_qp_cap(&ctx).unwrap();
    }

    #[test]
    #[should_panic]
    fn send_sge_cap_overrun() {
        let ctx = Context::open(None, 1, 1).unwrap();
        let cap = QueuePairCapBuilder::default()
            .max_send_sge(u32::MAX)
            .build()
            .unwrap();
        cap.check_dev_qp_cap(&ctx).unwrap();
    }

    #[test]
    #[should_panic]
    fn recv_wr_cap_overrun() {
        let ctx = Context::open(None, 1, 1).unwrap();
        let cap = QueuePairCapBuilder::default()
            .max_recv_wr(u32::MAX)
            .build()
            .unwrap();
        cap.check_dev_qp_cap(&ctx).unwrap();
    }

    #[test]
    #[should_panic]
    fn send_wr_cap_overrun() {
        let ctx = Context::open(None, 1, 1).unwrap();
        let cap = QueuePairCapBuilder::default()
            .max_send_wr(u32::MAX)
            .build()
            .unwrap();
        cap.check_dev_qp_cap(&ctx).unwrap();
    }
}
