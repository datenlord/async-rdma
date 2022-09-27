use std::fmt;

use rdma_sys::ibv_gid;
use serde::{Deserialize, Serialize};

/// A 128-bit identifier used to identify a Port on a network adapter, a port on a Router,
/// or a Multicast Group.
///
/// A GID is a valid 128-bit IPv6 address (per RFC 2373) with additional properties / restrictions
/// defined within IBA to facilitate efficient discovery, communication, and routing.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Gid(ibv_gid);

impl Gid {
    /// Build [`Gid`] from bytes
    #[inline]
    #[must_use]
    pub fn from_raw(raw: [u8; 16]) -> Self {
        Self(ibv_gid { raw })
    }

    /// Re-interprets [`&Gid`](Gid) as `&[u8;16]`.
    #[inline]
    #[must_use]
    pub fn as_raw(&self) -> &[u8; 16] {
        // SAFETY: POD type
        unsafe { &self.0.raw }
    }

    /// First 32 bits
    #[inline]
    #[must_use]
    pub fn subnet_prefix(&self) -> u64 {
        // SAFETY: POD type
        unsafe { u64::from_be(self.0.global.subnet_prefix) }
    }

    /// Last 32 bits
    #[inline]
    #[must_use]
    pub fn interface_id(&self) -> u64 {
        // SAFETY: POD type
        unsafe { u64::from_be(self.0.global.interface_id) }
    }
}

impl fmt::Debug for Gid {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // FIXME: any better representation?
        f.debug_tuple("Gid").field(self.as_raw()).finish()
    }
}

impl PartialEq for Gid {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_raw() == other.as_raw()
    }
}

impl Eq for Gid {}

impl From<ibv_gid> for Gid {
    #[inline]
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
    #[inline]
    fn as_ref(&self) -> &ibv_gid {
        // SAFETY: repr(transparent)
        unsafe { &*<*const Self>::cast::<ibv_gid>(self) }
    }
}

impl AsMut<ibv_gid> for Gid {
    #[inline]
    fn as_mut(&mut self) -> &mut ibv_gid {
        // SAFETY: repr(transparent)
        unsafe { &mut *<*mut Self>::cast::<ibv_gid>(self) }
    }
}

impl Serialize for Gid {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // FIXME: bytes format or struct format?
        <[u8; 16] as Serialize>::serialize(self.as_raw(), serializer)
    }
}

impl<'de> Deserialize<'de> for Gid {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <[u8; 16] as Deserialize<'de>>::deserialize(deserializer).map(Self::from_raw)
    }
}

#[test]
fn repr_check() {
    use std::mem::{align_of, size_of};
    assert_eq!(size_of::<Gid>(), size_of::<ibv_gid>());
    assert_eq!(align_of::<Gid>(), align_of::<ibv_gid>());
}
