use crate::{memory_region::LocalMemoryRegion, protection_domain::ProtectionDomain};
use rdma_sys::ibv_access_flags;
use std::{alloc::Layout, io, sync::Arc};
use utilities::OverflowArithmetic;

/// Memory region allocator
#[derive(Debug)]
pub struct MRAllocator {
    /// Protection domain that holds the allocator
    _pd: Arc<ProtectionDomain>,
    /// The initial MR, and all allocated MRs comes from here
    mr: Arc<LocalMemoryRegion>,
}

impl MRAllocator {
    /// Create a new MR allocator
    #[allow(clippy::unwrap_in_result)]
    pub fn new(pd: Arc<ProtectionDomain>) -> io::Result<Self> {
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        let mr = Arc::new(
            // TODO: the default size should be configurable
            pd.alloc_memory_region(
                // the alignment is guaranteed
                #[allow(clippy::unwrap_used)]
                Layout::from_size_align(4096.overflow_mul(2), 4096).unwrap(),
                access,
            )?,
        );
        Ok(Self { _pd: pd, mr })
    }

    /// Allocate a MR according to the `layout`
    pub fn alloc(&self, layout: &Layout) -> io::Result<LocalMemoryRegion> {
        self.mr.alloc(layout)
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
