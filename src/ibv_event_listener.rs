use getset::Getters;
use parking_lot::Mutex;
use rdma_sys::{ibv_ack_async_event, ibv_async_event, ibv_event_type, ibv_get_async_event};
use std::{fmt::Debug, io, mem::MaybeUninit, sync::Arc};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::context::Context;

/// Ibv async event listener
#[derive(Debug, Getters)]
pub(crate) struct IbvEventListener {
    /// Dependent ctx
    _ctx: Arc<Context>,
    /// Listener task handle
    task_handle: JoinHandle<io::Result<()>>,
    /// Type of the last ibv evnet
    #[get = "pub"]
    last_event_type: Arc<Mutex<Option<IbvEventType>>>,
}

impl Drop for IbvEventListener {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl IbvEventListener {
    /// Create a new ibv event listener
    pub(crate) fn new(ctx: Arc<Context>) -> Self {
        let last_event_type = Arc::new(Mutex::new(None));
        let task_handle = Self::start(Arc::clone(&ctx), Arc::clone(&last_event_type));
        Self {
            _ctx: ctx,
            task_handle,
            last_event_type,
        }
    }

    /// Start the ibv async event loop
    fn start(
        ctx: Arc<Context>,
        last_event_type: Arc<Mutex<Option<IbvEventType>>>,
    ) -> JoinHandle<io::Result<()>> {
        tokio::spawn(async move {
            let async_fd = ctx.get_async_fd()?;
            // async event loop
            loop {
                async_fd.readable().await?.clear_ready();
                let mut event = MaybeUninit::<ibv_async_event>::uninit();
                // SAFETY: ffi
                let ret = unsafe { ibv_get_async_event(ctx.as_ptr(), event.as_mut_ptr()) };
                // in non-blocking mode, `ret != 0` means that there isn't any async event to read
                if ret == 0_i32 {
                    // SAFETY: init by `ibv_get_async_event`
                    let mut event = IbvAsyncEvent::new(unsafe { event.assume_init() });
                    let event_type = event.event_type();
                    // Source of prompt message: https://www.rdmamojo.com/2012/08/11/ibv_get_async_event/
                    match event_type {
                        ibv_event_type::IBV_EVENT_CQ_ERR => error!("An error occurred when writing a completion to the CQ. This event may occur when there is a protection error (a rare condition) or when there is a CQ overrun (most likely)"),
                        ibv_event_type::IBV_EVENT_QP_FATAL => error!("A QP experienced an error that prevents the generation of completions while accessing or processing the Work Queue, either Send or Receive Queue."),
                        ibv_event_type::IBV_EVENT_QP_REQ_ERR => error!("The transport layer of the RDMA device detected a transport error violation in the responder side. This error may be one of the following:\n
                            1. Unsupported or reserved opcode;\n
                            2. Out of sequence opcode"),
                        ibv_event_type::IBV_EVENT_QP_ACCESS_ERR => error!("The transport layer of the RDMA device detected a request error violation in the responder side. This error may be one of the following:\n
                            1. Misaligned atomic request;\n
                            2. Too many RDMA Read or Atomic requests;\n
                            3. R_Key violation;\n
                            4. Length errors without immediate data;"),
                        ibv_event_type::IBV_EVENT_COMM_EST => info!("A QP which its state is IBV_QPS_RTR received the first packet in its Receive Queue and it was processed without any error."),
                        ibv_event_type::IBV_EVENT_SQ_DRAINED => info!("A QP, which its state was changed from IBV_QPS_RTS to IBV_QPS_SQD, completed sending all of the outstanding messages in progress in its Send Queue when the state change was requested. For RC QP, this means that all of those messages received acknowledgments, if applicable."),
                        ibv_event_type::IBV_EVENT_PATH_MIG => info!("Indicates the connection has migrated to the alternate path. This means that the alternate path attributes are now being used as the primary path attributes. If it is required that there will be another alternate path attribute loaded, the user can now set those attributes."),
                        ibv_event_type::IBV_EVENT_PATH_MIG_ERR => error!("A QP that has an alternate path attributes loaded tried to perform a path migration change, either by the RDMA device or explicitly by the user, and there was an error that prevented from moving to that alternate path. This error usually can happen if the alternate path attributes in both sides aren't consistent."),
                        ibv_event_type::IBV_EVENT_DEVICE_FATAL => error!("The RDMA device suffered from an error which isn't related to one of the above asynchronous events. When this event occurs, the behavior of the RDMA device isn't determined and it is highly recommended to close the process immediately since the attempt to destroy the RDMA resources may fail."),
                        ibv_event_type::IBV_EVENT_PORT_ACTIVE => info!("The link becomes active and it now available to send/receive packets."),
                        ibv_event_type::IBV_EVENT_PORT_ERR => error!("The link becomes inactive and it now unavailable to send/receive packets. This will not affect the QPs, which are associated with this port, states. Although if they are reliable and tries to send data, they may experience retry exceeded."),
                        ibv_event_type::IBV_EVENT_LID_CHANGE => info!("LID was changed on a port by the SM. If this is not the first time that the SM configures the port LID, this may indicate that there is a new SM in the subnet, or the SM reconfigures the subnet. QPs which send/receive data may experience connection failures (if the LIDs in the subnet were changed)."),
                        ibv_event_type::IBV_EVENT_PKEY_CHANGE => info!("P_Key table was changed on a port by the SM. Since QPs are using P_Key table indexes rather than absolute values, it is suggested for the client to check that the P_Key indexes which his QPs use weren't changed."),
                        ibv_event_type::IBV_EVENT_SM_CHANGE => info!("There is a new SM in the subnet which port belongs to and the client should reregister to all subscriptions previously requested from this port, for example (but not limited to) join a multicast group."),
                        ibv_event_type::IBV_EVENT_SRQ_ERR => error!("An error occurred that prevents from the RDMA device from dequeuing RRs from that SRQ and reporting of receive completions."),
                        ibv_event_type::IBV_EVENT_SRQ_LIMIT_REACHED => info!("A SRQ which was armed and the number of RR in that SRQ dropped below the limit value of that SRQ. When this event is being generated, the limit value of the SRQ will be set to zero."),
                        ibv_event_type::IBV_EVENT_QP_LAST_WQE_REACHED => info!("A QP, which is associated with an SRQ, was transitioned to the IBV_QPS_ERR state, either automatically by the RDMA device or explicitly by the user, and one of the following occurred:\n
                            1. A completion with error was generated for the last WQE;\n
                            2. The QP transitioned to the IBV_QPS_ERR state and there are no more WQEs on Receive Queue of that QP"),
                        ibv_event_type::IBV_EVENT_CLIENT_REREGISTER => info!("The SM requests that the client will reregister to all subscriptions previously requested from this port, for example (but not limited to) join a multicast group. This event may be generated when the SM suffered from a failure, which caused it to lose his records or when there is new SM in the subnet."),
                        ibv_event_type::IBV_EVENT_GID_CHANGE => info!("GID table was changed on a port by the SM. Since QPs are using GID table indexes rather than absolute values (as the source GID), it is suggested for the client to check that the GID indexes which his QPs use weren't changed."),
                        ibv_event_type::IBV_EVENT_WQ_FATAL => error!("Error occurred on a WQ and it transitioned to error state"),
                    }
                    // SAFETY: ffi
                    unsafe {
                        ibv_ack_async_event(&mut event.inner);
                    }
                    let _ = last_event_type.lock().replace(event_type);
                }
            }
        })
    }
}

/// Type of ibv async event
pub type IbvEventType = ibv_event_type;

/// Wrapper of `ibv_async_event`
pub(crate) struct IbvAsyncEvent {
    /// Inner binding type
    inner: ibv_async_event,
}

impl IbvAsyncEvent {
    /// Create a new ibv event
    fn new(event: ibv_async_event) -> Self {
        Self { inner: event }
    }

    /// Get the type of ibv event
    pub(crate) fn event_type(&self) -> IbvEventType {
        self.inner.event_type
    }
}

impl Debug for IbvAsyncEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IbvAsyncEvent")
            .field("type", &self.inner.event_type)
            .finish()
    }
}
