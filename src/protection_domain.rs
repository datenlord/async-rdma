use crate::{context::Context, memory_region::LocalMemoryRegion, queue_pair::QueuePairBuilder};
use rdma_sys::{ibv_access_flags, ibv_alloc_pd, ibv_dealloc_pd, ibv_pd};
use std::{alloc::Layout, io, ptr::NonNull, sync::Arc};

pub struct ProtectionDomain {
    pub(super) ctx: Arc<Context>,
    inner_pd: NonNull<ibv_pd>,
}

impl ProtectionDomain {
    pub(crate) fn as_ptr(&self) -> *mut ibv_pd {
        self.inner_pd.as_ptr()
    }

    pub fn create(ctx: &Arc<Context>) -> io::Result<Self> {
        let inner_pd =
            NonNull::new(unsafe { ibv_alloc_pd(ctx.as_ptr()) }).ok_or(io::ErrorKind::Other)?;
        Ok(Self {
            ctx: ctx.clone(),
            inner_pd,
        })
    }

    pub fn create_queue_pair_builder(self: &Arc<Self>) -> QueuePairBuilder {
        QueuePairBuilder::new(self)
    }

    pub fn alloc_memory_region(
        self: &Arc<Self>,
        layout: Layout,
        access: ibv_access_flags,
    ) -> io::Result<LocalMemoryRegion> {
        LocalMemoryRegion::new_from_pd(self, layout, access)
    }
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        let errno = unsafe { ibv_dealloc_pd(self.as_ptr()) };
        assert_eq!(errno, 0);
    }
}

unsafe impl Send for ProtectionDomain {}

unsafe impl Sync for ProtectionDomain {}
