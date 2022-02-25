use crate::{
    memory_region::{local::LocalMr, MrAccess, RawMemoryRegion},
    protection_domain::ProtectionDomain,
};
use rdma_sys::ibv_access_flags;
use std::{
    alloc::{alloc, Layout},
    io,
    sync::Arc,
};

/// Memory region allocator
#[derive(Debug)]
pub(crate) struct MrAllocator {
    /// Protection domain that holds the allocator
    pd: Arc<ProtectionDomain>,
}

impl MrAllocator {
    /// Create a new MR allocator
    pub(crate) fn new(pd: Arc<ProtectionDomain>) -> Self {
        Self { pd }
    }

    /// Allocate a MR according to the `layout`
    pub(crate) fn alloc(self: &Arc<Self>, layout: &Layout) -> io::Result<LocalMr> {
        let addr = unsafe { alloc(*layout) };
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        let raw_mr = Arc::new(RawMemoryRegion::register_from_pd(
            &self.pd,
            addr,
            layout.size(),
            access,
        )?);
        Ok(LocalMr::new(raw_mr.addr(), raw_mr.length(), raw_mr))
    }
}

#[cfg(test)]
mod tests {
    use crate::RdmaBuilder;
    use std::{alloc::Layout, io};

    #[tokio::test]
    async fn test1() -> io::Result<()> {
        let rdma = RdmaBuilder::default()
            .set_port_num(1)
            .set_gid_index(1)
            .build()?;
        let mut mrs = vec![];
        for _ in 0_i32..2_i32 {
            let mr = rdma.alloc_local_mr(Layout::new::<[u8; 4096]>())?;
            mrs.push(mr);
        }
        Ok(())
    }
}
