use crate::{memory_region::LocalMemoryRegion, protection_domain::ProtectionDomain};
use rdma_sys::ibv_access_flags;
use std::{alloc::Layout, io, sync::Arc};

pub struct MRAllocator {
    _pd: Arc<ProtectionDomain>,
    mr: Arc<LocalMemoryRegion>,
}

impl MRAllocator {
    pub fn new(pd: Arc<ProtectionDomain>) -> Self {
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        let mr = Arc::new(
            pd.alloc_memory_region(Layout::from_size_align(4096 * 1024, 4096).unwrap(), access)
                .unwrap(),
        );
        Self { _pd: pd, mr }
    }

    pub fn alloc(&self, layout: Layout) -> io::Result<LocalMemoryRegion> {
        self.mr.alloc(layout)
    }

    pub fn _release(&self, _mr: LocalMemoryRegion) {}
}

#[cfg(test)]
mod tests {
    use crate::RdmaBuilder;
    use std::alloc::Layout;

    #[tokio::test]
    async fn test1() {
        let rdma = RdmaBuilder::default().build().unwrap();
        let mut mrs = vec![];
        for _ in 0..1024 {
            let mr = rdma.alloc_local_mr(Layout::new::<[u8; 4096]>()).unwrap();
            mrs.push(mr);
        }
    }
}
