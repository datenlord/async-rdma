use super::{IbvAccess, MrAccess};
use crate::protection_domain::ProtectionDomain;
use clippy_utilities::Cast;
use rdma_sys::{ibv_access_flags, ibv_dereg_mr, ibv_mr, ibv_reg_mr};
use std::fmt::Debug;
use std::io;
use std::{ptr::NonNull, sync::Arc};
use tracing::{debug, error};

/// Raw Memory Region
pub(crate) struct RawMemoryRegion {
    /// the internal `ibv_mr` pointer
    inner_mr: NonNull<ibv_mr>,
    /// the addr of the raw memory region
    addr: *mut u8,
    /// the len of the raw memory region
    len: usize,
    /// the protection domain that raw memory region belongs to
    pd: Arc<ProtectionDomain>,
    /// the access of this raw memory region
    access: ibv_access_flags,
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
        // SAFETY: ?
        // TODO: check safety
        unsafe { self.inner_mr.as_ref().rkey }
    }
}

impl IbvAccess for RawMemoryRegion {
    #[inline]
    fn ibv_access(&self) -> ibv_access_flags {
        self.access
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
        // SAFETY: ffi
        // TODO: check safety
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
            pd: Arc::<ProtectionDomain>::clone(pd),
            access,
        })
    }
    /// Get local key of memory region
    pub(crate) fn lkey(&self) -> u32 {
        // SAFETY: ?
        // TODO: check safety
        unsafe { self.inner_mr.as_ref().lkey }
    }

    /// Get pd of this memory region
    #[cfg(test)]
    pub(crate) fn pd(&self) -> &Arc<ProtectionDomain> {
        &self.pd
    }
}

impl Debug for RawMemoryRegion {
    #[allow(clippy::as_conversions)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawMemoryRegion")
            .field("inner_mr", &self.inner_mr)
            .field("addr", &(self.addr as usize))
            .field("len", &self.len)
            .field("pd", &self.pd)
            .finish()
    }
}

unsafe impl Sync for RawMemoryRegion {}

unsafe impl Send for RawMemoryRegion {}

impl Drop for RawMemoryRegion {
    fn drop(&mut self) {
        debug!("dereg_mr {:?}", &self);
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_dereg_mr(self.inner_mr.as_ptr()) };
        assert_eq!(errno, 0_i32);
    }
}
