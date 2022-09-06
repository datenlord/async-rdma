/// Local Memory Region
pub(crate) mod local;
/// Raw Memory Region
mod raw;
/// Remote Memory Region
pub(crate) mod remote;
use enumflags2::BitFlags;
pub(crate) use raw::RawMemoryRegion;
use rdma_sys::ibv_access_flags;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::SystemTime};

use crate::{access::ibv_access_into_flags, AccessFlag};

/// Rdma Memory Region Access
pub trait MrAccess: Sync + Send + Debug + IbvAccess {
    /// Get the start addr
    fn addr(&self) -> usize;

    /// Get the length
    fn length(&self) -> usize;

    /// Get the remote key
    fn rkey(&self) -> u32;

    /// Get the `BitFlags<AccessFlag>` of mr
    #[inline]
    fn access(&self) -> BitFlags<AccessFlag> {
        ibv_access_into_flags(self.ibv_access())
    }
}

/// Get `ibv_access_flags` of mr
pub trait IbvAccess {
    /// Get `ibv_access_flags` of mr
    fn ibv_access(&self) -> ibv_access_flags;
}

/// Memory region token used for the remote access
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct MrToken {
    /// The start address
    pub addr: usize,
    /// The length
    pub len: usize,
    /// The rkey
    pub rkey: u32,
    /// Deadline for timeout
    pub ddl: SystemTime,
    /// Remote mr `ibv_access_flags` inner
    pub access: u32,
}
