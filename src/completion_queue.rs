use crate::{
    context::{check_dev_cap, Context},
    cq_event_channel::EventChannel,
    error_utilities::log_ret_last_os_err,
    id,
};
use clippy_utilities::Cast;
use getset::Getters;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use rdma_sys::{
    ibv_cq, ibv_create_cq, ibv_destroy_cq, ibv_poll_cq, ibv_req_notify_cq, ibv_wc, ibv_wc_status,
};
use std::{fmt::Debug, io, mem, ptr::NonNull};
use thiserror::Error;
use tracing::error;

/// Indicator that `imm_data` is valid.
/// Relevant for Receive Work Completions.
pub(crate) static mut IBV_WC_WITH_IMM: u32 = 3;

/// Used to init `IBV_WC_WITH_IMM` only once.
pub(crate) static INIT_IMM_FLAG: parking_lot::Mutex<bool> = parking_lot::Mutex::new(false);

/// Minimum number of entries CQ will support
pub(crate) const DEFAULT_CQ_SIZE: u32 = 1024_u32;

/// Default maximum number of completion queue entries (CQE) to poll at a time.
pub(crate) static DEFAULT_MAX_CQE: i32 = 32_i32;

/// Complete Queue Structure
#[derive(Debug, Getters)]
pub(crate) struct CompletionQueue {
    /// Event Channel
    ec: EventChannel,
    /// Real Completion Queue
    inner_cq: NonNull<ibv_cq>,
    /// Maximum number of completion queue entries (CQE) to poll at a time.
    /// The higher the concurrency, the bigger this value should be and more memory allocated at a time.
    #[get = "pub"]
    max_poll_cqe: i32,
}

impl CompletionQueue {
    /// Get the internal cq ptr
    pub(crate) const fn as_ptr(&self) -> *mut ibv_cq {
        self.inner_cq.as_ptr()
    }

    /// Create a new completion queue and bind to the event channel `ec`, `cq_size` is the buffer
    /// size of the completion queue
    ///
    /// On failure of `ibv_create_cq`, errno indicates the failure reason:
    ///
    /// `EINVAL`    Invalid cqe, channel or `comp_vector`
    ///
    /// `ENOMEM`    Not enough resources to complete this operation
    pub(crate) fn create(
        ctx: &Context,
        cq_size: u32,
        ec: EventChannel,
        max_poll_cqe: i32,
    ) -> io::Result<Self> {
        if max_poll_cqe > cq_size.cast() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_poll_cqe should be smaller than cq_size",
            ));
        }

        // check device cap
        let dev_attr = ctx.dev_attr();
        check_dev_cap(&cq_size, &dev_attr.max_cqe.cast(), "max_cqe")?;
        // TODO: add cq number(max_cq) check

        // SAFETY: ffi
        let inner_cq = NonNull::new(unsafe {
            ibv_create_cq(
                ctx.as_ptr(),
                cq_size.cast(),
                std::ptr::null_mut(),
                ec.as_ptr(),
                0,
            )
        })
        .ok_or_else(log_ret_last_os_err)?;
        Ok(Self {
            ec,
            inner_cq,
            max_poll_cqe,
        })
    }

    /// Request notification on next complete event arrive
    pub(crate) fn req_notify(&self, solicited_only: bool) -> io::Result<()> {
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe {
            ibv_req_notify_cq(self.inner_cq.as_ptr(), if solicited_only { 1 } else { 0 })
        };
        if errno != 0_i32 {
            let err = log_ret_last_os_err();
            return Err(err);
        }
        Ok(())
    }

    /// Poll work completions from CQ up to `DEFAULT_CQ_SIZE` at a time
    pub(crate) fn poll_cq_multiple(&self, wc_buf: &mut Vec<WorkCompletion>) -> io::Result<()> {
        if wc_buf.capacity() < self.max_poll_cqe.cast() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "wc_buf is not big enough, {} required and currently {}",
                    self.max_poll_cqe,
                    wc_buf.capacity()
                ),
            ));
        }
        // SAFETY: ffi
        // TODO: check safety
        let cqe_num =
            unsafe { ibv_poll_cq(self.as_ptr(), self.max_poll_cqe, wc_buf.as_mut_ptr().cast()) };
        if cqe_num > 0_i32 {
            // this buffer will be used by RDMA NIC and it's len will not increase when NIC push data into it,
            // so we need set it's len manually.
            // Safety: NIC will return the length it has used and make sure the data is valid.
            unsafe {
                wc_buf.set_len(cqe_num.cast());
            }
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "nothing completed",
            ))
        }
    }

    /// Get the internal event channel
    pub(crate) fn event_channel(&self) -> &EventChannel {
        &self.ec
    }
}

unsafe impl Sync for CompletionQueue {}

unsafe impl Send for CompletionQueue {}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_destroy_cq(self.as_ptr()) };
        assert_eq!(errno, 0_i32);
    }
}

/// Work Completion
#[repr(C)]
pub(crate) struct WorkCompletion {
    /// The internal ibv work completion
    inner_wc: ibv_wc,
}

impl WorkCompletion {
    /// Get work request Id
    pub(crate) const fn wr_id(&self) -> WorkRequestId {
        WorkRequestId(self.inner_wc.wr_id)
    }

    /// Get work completion result.
    /// Returns the length on success otherwise returns an error.
    pub(crate) fn result(&self) -> Result<usize, WCError> {
        if self.inner_wc.status == ibv_wc_status::IBV_WC_SUCCESS {
            Ok(self.inner_wc.byte_len.cast())
        } else {
            error!("error wc wrid : {}", self.inner_wc.wr_id);
            Err(WCError::from_u32(self.inner_wc.status).unwrap_or(WCError::UnexpectedErr))
        }
    }

    /// Get work completion result and immediate data.
    /// Returns the length and immediate date on success otherwise returns an error.
    pub(crate) fn result_with_imm(&self) -> Result<(usize, Option<u32>), WCError> {
        if self.inner_wc.status == ibv_wc_status::IBV_WC_SUCCESS {
            let len: usize = self.inner_wc.byte_len.cast();
            // SAFETY: will only be init once.
            if self.inner_wc.wc_flags == unsafe { IBV_WC_WITH_IMM } {
                // use union to get imm_data after check opcode.
                Ok((
                    len,
                    // SAFETY: imm flag was checked
                    Some(unsafe { self.inner_wc.imm_data_invalidated_rkey_union.imm_data }),
                ))
            } else {
                Ok((self.inner_wc.byte_len.cast(), None))
            }
        } else {
            Err(WCError::from_u32(self.inner_wc.status).unwrap_or(WCError::UnexpectedErr))
        }
    }
}

impl Debug for WorkCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkCompletion")
            .field("wr_id", &self.wr_id())
            .finish()
    }
}

impl Default for WorkCompletion {
    fn default() -> Self {
        Self {
            // SAFETY: POD FFI type
            inner_wc: unsafe { mem::zeroed() },
        }
    }
}

/// Wrapper for work completion error
#[allow(clippy::missing_docs_in_private_items)]
#[derive(Error, Debug, FromPrimitive, Copy, Clone)]
pub(crate) enum WCError {
    #[error("Local Length Error: this happens if a Work Request that was posted in a local Send Queue contains a message that is greater than the maximum message size that is supported by the RDMA device port that should send the message or an Atomic operation which its size is different than 8 bytes was sent. This also may happen if a Work Request that was posted in a local Receive Queue isn't big enough for holding the incoming message or if the incoming message size if greater the maximum message size supported by the RDMA device port that received the message.")]
    LocLenErr = 1,
    #[error("Local QP Operation Error: an internal QP consistency error was detected while processing this Work Request: this happens if a Work Request that was posted in a local Send Queue of a UD QP contains an Address Handle that is associated with a Protection Domain to a QP which is associated with a different Protection Domain or an opcode which isn't supported by the transport type of the QP isn't supported (for example: RDMA Write over a UD QP).")]
    LocQpOpErr = 2,
    #[error("Local EE Context Operation Error: an internal EE Context consistency error was detected while processing this Work Request (unused, since its relevant only to RD QPs or EE Context, which aren't supported).")]
    LocEecOpErr = 3,
    #[error("Local Protection Error: the locally posted Work Request's buffers in the scatter/gather list does not reference a Memory Region that is valid for the requested operation.")]
    LocProtErr = 4,
    #[error("Work Request Flushed Error: A Work Request was in process or outstanding when the QP transitioned into the Error State.")]
    WrFlushErr = 5,
    #[error("Memory Window Binding Error: A failure happened when tried to bind a MW to a MR.")]
    MwBindErr = 6,
    #[error("Bad Response Error: an unexpected transport layer opcode was returned by the responder. Relevant for RC QPs.")]
    BadRespErr = 7,
    #[error("Local Access Error: a protection error occurred on a local data buffer during the processing of a RDMA Write with Immediate operation sent from the remote node. Relevant for RC QPs.")]
    LocAccessErr = 8,
    #[error("Remote Invalid Request Error: The responder detected an invalid message on the channel. Possible causes include the operation is not supported by this receive queue (qp_access_flags in remote QP wasn't configured to support this operation), insufficient buffering to receive a new RDMA or Atomic Operation request, or the length specified in a RDMA request is greater than 2^31 bytes. Relevant for RC QPs.")]
    RemInvReqErr = 9,
    #[error("Remote Access Error: a protection error occurred on a remote data buffer to be read by an RDMA Read, written by an RDMA Write or accessed by an atomic operation. This error is reported only on RDMA operations or atomic operations. Relevant for RC QPs.")]
    RemAccessErr = 10,
    #[error("Remote Operation Error: the operation could not be completed successfully by the responder. Possible causes include a responder QP related error that prevented the responder from completing the request or a malformed WQE on the Receive Queue. Relevant for RC QPs.")]
    RemOpErr = 11,
    #[error("Transport Retry Counter Exceeded: The local transport timeout retry counter was exceeded while trying to send this message. This means that the remote side didn't send any Ack or Nack. If this happens when sending the first message, usually this mean that the connection attributes are wrong or the remote side isn't in a state that it can respond to messages. If this happens after sending the first message, usually it means that the remote QP isn't available anymore. Relevant for RC QPs.")]
    RetryExc = 12,
    #[error("RNR Retry Counter Exceeded: The RNR NAK retry count was exceeded. This usually means that the remote side didn't post any WR to its Receive Queue. Relevant for RC QPs.")]
    RnrRetryExc = 13,
    #[error("Local RDD Violation Error: The RDD associated with the QP does not match the RDD associated with the EE Context (unused, since its relevant only to RD QPs or EE Context, which aren't supported).")]
    LocRddViolErr = 14,
    #[error("Remote Invalid RD Request: The responder detected an invalid incoming RD message. Causes include a Q_Key or RDD violation (unused, since its relevant only to RD QPs or EE Context, which aren't supported).")]
    RemInvRdReq = 15,
    #[error("Remote Aborted Error: For UD or UC QPs associated with a SRQ, the responder aborted the operation.")]
    RemAbortErr = 16,
    #[error("Invalid EE Context Number: An invalid EE Context number was detected (unused, since its relevant only to RD QPs or EE Context, which aren't supported).")]
    InvEecn = 17,
    #[error("Invalid EE Context State Error: Operation is not legal for the specified EE Context state (unused, since its relevant only to RD QPs or EE Context, which aren't supported).")]
    InvEecState = 18,
    #[error("Fatal Error.")]
    Fatal = 19,
    #[error("Response Timeout Error.")]
    RespTimeout = 20,
    #[error("General Error: other error which isn't one of the above errors.")]
    GeneralErr = 21,
    #[error("Unexpected Error.")]
    UnexpectedErr = 100,
    #[error("Failed to submit the request")]
    FailToSubmit = 101,
}

impl From<WCError> for io::Error {
    #[inline]
    fn from(e: WCError) -> Self {
        Self::new(io::ErrorKind::Other, e)
    }
}

/// Work request id
#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
pub(crate) struct WorkRequestId(u64);

impl WorkRequestId {
    /// Create a new id for `WorkRequest`
    pub(crate) fn new() -> Self {
        WorkRequestId(id::random_u64())
    }
}

impl Default for WorkRequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<WorkRequestId> for u64 {
    #[inline]
    fn from(wr_id: WorkRequestId) -> Self {
        wr_id.0
    }
}
