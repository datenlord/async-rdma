use crate::Context;
use rdma_sys::{ibv_comp_channel, ibv_create_comp_channel, ibv_destroy_comp_channel};
use std::os::unix::prelude::{AsRawFd, RawFd};
use std::{io, ptr::NonNull, sync::Arc};
use tokio::io::unix::AsyncFd;

/// Event channel wrapper for `ibv_comp_channel`
#[derive(Debug)]
pub struct EventChannel {
    /// The ibv device context
    pub ctx: Arc<Context>,
    /// The inner ibv_comp_channel pointer
    pub inner_ec: NonNull<ibv_comp_channel>,
}

impl EventChannel {
    /// Get the inner `ibv_comp_channel` pointer
    pub fn as_ptr(&self) -> *mut ibv_comp_channel {
        self.inner_ec.as_ptr()
    }

    /// Create a new `EventChannel`
    pub fn new(ctx: Arc<Context>) -> io::Result<Self> {
        let inner_ec = NonNull::new(unsafe { ibv_create_comp_channel(ctx.as_ptr()) })
            .ok_or(io::ErrorKind::Other)?;
        Ok(Self { ctx, inner_ec })
    }

    /// Get the event channel fd and wrap it into Tokio `AsyncFd`
    pub fn async_fd(&self) -> io::Result<AsyncFd<RawFd>> {
        AsyncFd::new(unsafe { *self.as_ptr() }.fd.as_raw_fd())
    }
}

unsafe impl Sync for EventChannel {}

unsafe impl Send for EventChannel {}

impl Drop for EventChannel {
    fn drop(&mut self) {
        let errno = unsafe { ibv_destroy_comp_channel(self.as_ptr()) };
        assert_eq!(errno, 0_i32);
    }
}
