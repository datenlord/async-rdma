use rdma_sys::ibv_gid;

/// Rdma device gid
#[derive(
    serde::Serialize, serde::Deserialize, Default, Copy, Clone, Debug, Eq, PartialEq, Hash,
)]
#[repr(transparent)]
pub struct Gid {
    /// Gid raw data
    raw: [u8; 16],
}

#[allow(dead_code)]
impl Gid {
    /// First 32 bits
    fn subnet_prefix(&self) -> u64 {
        // into always success
        #[allow(clippy::unwrap_used)]
        u64::from_be_bytes(self.raw[..8].try_into().unwrap())
    }

    /// Last 32 bits
    fn interface_id(&self) -> u64 {
        // into always success
        #[allow(clippy::unwrap_used)]
        u64::from_be_bytes(self.raw[8..].try_into().unwrap())
    }
}

impl From<ibv_gid> for Gid {
    fn from(gid: ibv_gid) -> Self {
        Self {
            raw: unsafe { gid.raw },
        }
    }
}

impl From<Gid> for ibv_gid {
    #[inline]
    fn from(mut gid: Gid) -> Self {
        *gid.as_mut()
    }
}

impl AsRef<ibv_gid> for Gid {
    fn as_ref(&self) -> &ibv_gid {
        // alignment is guaranteed
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &*self.raw.as_ptr().cast::<ibv_gid>()
        }
    }
}

impl AsMut<ibv_gid> for Gid {
    fn as_mut(&mut self) -> &mut ibv_gid {
        // alignment is guaranteed
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &mut *self.raw.as_mut_ptr().cast::<ibv_gid>()
        }
    }
}
