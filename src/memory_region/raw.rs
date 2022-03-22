use super::local::LocalMrReadAccess;
use super::MrAccess;
use crate::protection_domain::ProtectionDomain;
use clippy_utilities::Cast;
use rdma_sys::{ibv_access_flags, ibv_dereg_mr, ibv_mr, ibv_reg_mr};
use std::fmt::Debug;
use std::io;
use std::{ptr::NonNull, sync::Arc};
use tracing::error;

/// Raw Memory Region
pub(crate) struct RawMemoryRegion {
    /// the internal `ibv_mr` pointer
    inner_mr: NonNull<ibv_mr>,
    /// the addr of the raw memory region
    addr: *mut u8,
    /// the len of the raw memory region
    len: usize,
    /// the protection domain the raw memory region belongs to
    _pd: Arc<ProtectionDomain>,
}

impl MrAccess for RawMemoryRegion {
    #[allow(clippy::as_conversions)]
    fn addr(&self) -> usize {
        self.addr as usize
    }

    fn length(&self) -> usize {
        self.len
    }

    fn rkey(&self) -> u32 {
        unsafe { self.inner_mr.as_ref().rkey }
    }
}

impl LocalMrReadAccess for RawMemoryRegion {
    fn lkey(&self) -> u32 {
        unsafe { self.inner_mr.as_ref().lkey }
    }
}

impl RawMemoryRegion {
    /// Register a raw memory region from protection domain
    pub(crate) fn register_from_pd(
        pd: &Arc<ProtectionDomain>,
        addr: *mut u8,
        len: usize,
        access: ibv_access_flags,
    ) -> io::Result<Self> {
        let inner_mr =
            NonNull::new(unsafe { ibv_reg_mr(pd.as_ptr(), addr.cast(), len, access.0.cast()) })
                .ok_or_else(|| {
                    let err = io::Error::last_os_error();
                    error!(
                "ibv_reg_mr err, arguments:\n pd:{:?},\n addr:{:?},\n len:{:?},\n access:{:?}\n, err info:{:?}",
                pd, addr, len, access, err
            );
                           err
                })?;
        Ok(Self {
            inner_mr,
            addr,
            len,
            _pd: Arc::<ProtectionDomain>::clone(pd),
        })
    }
}

impl Debug for RawMemoryRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Local")
            .field("inner_mr", &self.inner_mr)
            .finish()
    }
}

unsafe impl Sync for RawMemoryRegion {}

unsafe impl Send for RawMemoryRegion {}

impl Drop for RawMemoryRegion {
    fn drop(&mut self) {
        let errno = unsafe { ibv_dereg_mr(self.inner_mr.as_ptr()) };
        assert_eq!(errno, 0_i32);
    }
}
