use crate::protection_domain::ProtectionDomain;
use rdma_sys::{ibv_alloc_mw, ibv_mw, ibv_mw_type};
use std::{io, sync::Arc};

#[allow(dead_code)]
struct MemoryWindow {
    inner_mw: *mut ibv_mw,
}

#[allow(dead_code)]
impl MemoryWindow {
    pub fn create(pd: &Arc<ProtectionDomain>) -> io::Result<Self> {
        let inner_mw = unsafe { ibv_alloc_mw(pd.as_ptr(), ibv_mw_type::IBV_MW_TYPE_1) }
            .ok_or_else(io::Error::last_os_error)?;
        if inner_mw.is_null() {
            dbg!(io::Error::last_os_error());
        }
        Ok(Self { inner_mw })
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryWindow;
    use crate::*;

    #[test]
    fn test_create() -> io::Result<()> {
        let ctx = Arc::new(Context::open(None)?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let _mw = MemoryWindow::create(&pd)?;
        dbg!(io::Error::last_os_error());
        Ok(())
    }
}
