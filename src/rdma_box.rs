use rdma_sys::ibv_access_flags;
use serde::{Deserialize, Serialize};
use std::{alloc::Layout, ops::Deref, ptr::NonNull, sync::Arc};

pub struct RdmaLocalBox<T> {
    mr: MemoryRegion,
    data: NonNull<T>,
}

impl<T> RdmaLocalBox<T> {
    pub fn new(pd: &Arc<ProtectionDomain>, x: T) -> Self {
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        let mr = pd.alloc_memory_region(Layout::new::<T>(), access).unwrap();
        let ptr = unsafe { *mr.as_ptr() }.addr as *mut T;
        unsafe { ptr.write(x) };
        let data = NonNull::new(ptr).unwrap();
        RdmaLocalBox { mr, data }
    }

    pub fn new_with_zerod(pd: &Arc<ProtectionDomain>) -> Self {
        let x = unsafe { std::mem::zeroed::<T>() };
        Self::new(pd, x)
    }

    pub fn remote_box(&self) -> RdmaRemoteBox {
        RdmaRemoteBox {
            ptr: selfas_ptr() as _,
            len: self.length(),
            rkey: self.mr.rkey(),
        }
    }
}

impl<T> RdmaMemory for RdmaLocalBox<T> {
    fn addr(&self) -> *const u8 {
        self.data.as_ptr() as *const u8
    }

    fn length(&self) -> usize {
        self.mr.length()
    }
}

impl<T> RdmaLocalMemory for RdmaLocalBox<T> {
    fn new_from_pd(
        pd: &Arc<ProtectionDomain>,
        layout: Layout,
        access: ibv_access_flags,
    ) -> io::Result<Self>
    where
        Self: Sized,
    {
        let mr = pd.alloc_memory_region(layout, access)?;
        let ptr = unsafe { *mr.as_ptr() }.addr as *mut T;
        let data = NonNull::new(ptr).unwrap();
        Ok(RdmaLocalBox { mr, data })
    }

    fn lkey(&self) -> u32 {
        self.mr.lkey()
    }
}

impl<T> SizedLayout for RdmaLocalBox<T> {
    fn layout() -> Layout {
        Layout::new::<T>()
    }
}

impl<T> Deref for RdmaLocalBox<T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { self.data.as_ref() }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RdmaRemoteBox {
    pub ptr: usize,
    pub len: usize,
    pub rkey: u32,
}

impl RdmaRemoteBox {
    pub async fn get<T: Copy>(&self, rdma: &Rdma) -> T {
        let mut local: RdmaLocalBox<T> = RdmaLocalBox::new_with_zerod(&rdma.pd);
        rdma.read(&mut local, self).await.unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
        *local
    }
}

impl RdmaMemory for RdmaRemoteBox {
    fn addr(&self) -> *const u8 {
        self.ptr as _
    }

    fn length(&self) -> usize {
        self.len
    }
}

impl RdmaRemoteMemory for RdmaRemoteBox {
    fn rkey(&self) -> u32 {
        self.rkey
    }
}
