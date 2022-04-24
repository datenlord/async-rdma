use std::fmt;

use rdma_sys::ibv_gid;
use serde::{Deserialize, Serialize};

/// Rdma device gid
#[derive(Clone, Copy)]
#[repr(transparent)]
pub(crate) struct Gid(ibv_gid);

#[allow(dead_code)]
impl Gid {
    /// Build [`Gid`] from bytes
    fn from_raw(raw: [u8; 16]) -> Self {
        Self(ibv_gid { raw })
    }

    /// Re-interprets [`&Gid`](Gid) as `&[u8;16]`.
    fn as_raw(&self) -> &[u8; 16] {
        // SAFETY: POD type
        unsafe { &self.0.raw }
    }

    /// First 32 bits
    fn subnet_prefix(&self) -> u64 {
        // SAFETY: POD type
        unsafe { u64::from_be(self.0.global.subnet_prefix) }
    }

    /// Last 32 bits
    fn interface_id(&self) -> u64 {
        // SAFETY: POD type
        unsafe { u64::from_be(self.0.global.interface_id) }
    }
}

impl fmt::Debug for Gid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // FIXME: any better representation?
        f.debug_tuple("Gid").field(self.as_raw()).finish()
    }
}

impl PartialEq for Gid {
    fn eq(&self, other: &Self) -> bool {
        self.as_raw() == other.as_raw()
    }
}

impl Eq for Gid {}

impl From<ibv_gid> for Gid {
    fn from(gid: ibv_gid) -> Self {
        Self(gid)
    }
}

impl From<Gid> for ibv_gid {
    #[inline]
    fn from(gid: Gid) -> Self {
        gid.0
    }
}

impl AsRef<ibv_gid> for Gid {
    fn as_ref(&self) -> &ibv_gid {
        // SAFETY: repr(transparent)
        unsafe { &*<*const Self>::cast::<ibv_gid>(self) }
    }
}

impl AsMut<ibv_gid> for Gid {
    fn as_mut(&mut self) -> &mut ibv_gid {
        // SAFETY: repr(transparent)
        unsafe { &mut *<*mut Self>::cast::<ibv_gid>(self) }
    }
}

impl Serialize for Gid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // FIXME: bytes format or struct format?
        <[u8; 16] as Serialize>::serialize(self.as_raw(), serializer)
    }
}

impl<'de> Deserialize<'de> for Gid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <[u8; 16] as Deserialize<'de>>::deserialize(deserializer).map(Self::from_raw)
    }
}
