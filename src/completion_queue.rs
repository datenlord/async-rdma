use crate::{context::Context, event_channel::EventChannel};
use libc::c_void;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use rand::Rng;
use rdma_sys::{
    ibv_cq, ibv_create_cq, ibv_destroy_cq, ibv_poll_cq, ibv_req_notify_cq, ibv_wc, ibv_wc_status,
};
use std::{
    cmp::Ordering,
    fmt::Debug,
    io, mem,
    ptr::{self, NonNull},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

pub struct CompletionQueue {
    ec: Option<EventChannel>,
    inner_cq: NonNull<ibv_cq>,
}

impl CompletionQueue {
    pub fn as_ptr(&self) -> *mut ibv_cq {
        self.inner_cq.as_ptr()
    }

    pub fn create(ctx: &Context, cq_size: u32, ec: Option<EventChannel>) -> io::Result<Self> {
        let ec_inner = match &ec {
            Some(ec) => ec.as_ptr(),
            _ => ptr::null::<c_void>() as _,
        };
        let inner_cq = NonNull::new(unsafe {
            ibv_create_cq(
                ctx.as_ptr(),
                cq_size as _,
                std::ptr::null_mut(),
                ec_inner,
                0,
            )
        })
        .ok_or(io::ErrorKind::Other)?;
        Ok(CompletionQueue { ec, inner_cq })
    }

    pub fn req_notify(&self, solicited_only: bool) -> io::Result<()> {
        if self.ec.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "no event channel".to_string(),
            ));
        }
        let errno = unsafe { ibv_req_notify_cq(self.inner_cq.as_ptr(), solicited_only as _) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(())
    }

    pub fn poll(&self, num_entries: usize) -> io::Result<Vec<WorkCompletion>> {
        let mut ans = vec![WorkCompletion::default(); num_entries];
        let poll_res =
            unsafe { ibv_poll_cq(self.as_ptr(), num_entries as _, ans.as_mut_ptr() as _) };
        match poll_res.cmp(&0) {
            Ordering::Greater | Ordering::Equal => {
                let poll_res = poll_res as _;
                for _ in poll_res..num_entries as _ {
                    ans.remove(poll_res);
                }
                assert_eq!(ans.len(), poll_res);
                Ok(ans)
            }
            Ordering::Less => Err(io::Error::new(io::ErrorKind::WouldBlock, "")),
        }
    }

    pub fn poll_single(&self) -> io::Result<WorkCompletion> {
        let polled = self.poll(1)?;
        polled
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, ""))
    }

    pub fn event_channel(&self) -> &EventChannel {
        self.ec.as_ref().unwrap()
    }
}

unsafe impl Sync for CompletionQueue {}

unsafe impl Send for CompletionQueue {}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        let errno = unsafe { ibv_destroy_cq(self.as_ptr()) };
        assert_eq!(errno, 0);
    }
}

#[repr(C)]
pub struct WorkCompletion {
    inner_wc: ibv_wc,
}

impl WorkCompletion {
    pub fn as_ptr(&self) -> *mut ibv_wc {
        &self.inner_wc as *const _ as _
    }

    pub fn wr_id(&self) -> WorkRequestId {
        WorkRequestId(self.inner_wc.wr_id)
    }

    pub fn err(&self) -> Result<usize, WCError> {
        if self.inner_wc.status == ibv_wc_status::IBV_WC_SUCCESS {
            Ok(self.inner_wc.byte_len as usize)
        } else {
            Err(WCError::from_u32(self.inner_wc.status).unwrap())
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
            inner_wc: unsafe { mem::zeroed() },
        }
    }
}

impl Clone for WorkCompletion {
    fn clone(&self) -> Self {
        let ans = Self::default();
        unsafe { ptr::copy(self.as_ptr(), ans.as_ptr(), 1) };
        ans
    }
}

#[derive(Error, Debug, FromPrimitive)]
pub enum WCError {
    #[error("Local Length Error: this happens if a Work Request that was posted in a local Send Queue contains a message that is greater than the maximum message size that is supported by the RDMA device port that should send the message or an Atomic operation which its size is different than 8 bytes was sent. This also may happen if a Work Request that was posted in a local Receive Queue isn't big enough for holding the incoming message or if the incoming message size if greater the maximum message size supported by the RDMA device port that received the message.")]
    LocLenErr = 1,
    #[error("Local QP Operation Error: an internal QP consistency error was detected while processing this Work Request: this happens if a Work Request that was posted in a local Send Queue of a UD QP contains an Address Handle that is associated with a Protection Domain to a QP which is associated with a different Protection Domain or an opcode which isn't supported by the transport type of the QP isn't supported (for example: RDMA Write over a UD QP).")]
    LocQpOpErr = 2,
    #[error("Local EE Context Operation Error: an internal EE Context consistency error was detected while processing this Work Request (unused, since its relevant only to RD QPs or EE Context, which aren’t supported).")]
    LocEecOpErr = 3,
    #[error("Local Protection Error: the locally posted Work Request’s buffers in the scatter/gather list does not reference a Memory Region that is valid for the requested operation.")]
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
}

impl From<WCError> for io::Error {
    fn from(e: WCError) -> Self {
        io::Error::new(io::ErrorKind::Other, e)
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
pub struct WorkRequestId(u64);

impl WorkRequestId {
    pub fn new() -> Self {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let time = since_the_epoch.subsec_micros();
        let rand = rand::thread_rng().gen::<u32>();
        let left: u64 = time.into();
        let right: u64 = rand.into();
        Self((left << 32) | right)
    }
}

impl Default for WorkRequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<WorkRequestId> for u64 {
    fn from(wr_id: WorkRequestId) -> Self {
        wr_id.0
    }
}
