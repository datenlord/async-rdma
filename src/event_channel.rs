use crate::Context;
use rdma_sys::{ibv_comp_channel, ibv_create_comp_channel, ibv_destroy_comp_channel};
use std::os::unix::prelude::{AsRawFd, RawFd};
use std::{io, ptr::NonNull, sync::Arc};

pub struct EventChannel {
    pub ctx: Arc<Context>,
    pub inner_ec: NonNull<ibv_comp_channel>,
}

impl EventChannel {
    pub fn as_ptr(&self) -> *mut ibv_comp_channel {
        self.inner_ec.as_ptr()
    }

    pub fn new(ctx: Arc<Context>) -> io::Result<Self> {
        let inner_ec = NonNull::new(unsafe { ibv_create_comp_channel(ctx.as_ptr()) })
            .ok_or(io::ErrorKind::Other)?;
        Ok(Self { ctx, inner_ec })
    }
}

unsafe impl Sync for EventChannel {}

unsafe impl Send for EventChannel {}

impl Drop for EventChannel {
    fn drop(&mut self) {
        let errno = unsafe { ibv_destroy_comp_channel(self.as_ptr()) };
        assert_eq!(errno, 0);
    }
}

impl AsRawFd for EventChannel {
    fn as_raw_fd(&self) -> RawFd {
        unsafe { *self.as_ptr() }.fd.as_raw_fd()
    }
}
