/// Local Memory Region
pub(crate) mod local;
/// Raw Memory Region
mod raw;
/// Remote Memory Region
pub(crate) mod remote;
use derive_builder::Builder;
use enumflags2::BitFlags;
pub(crate) use raw::RawMemoryRegion;
use rdma_sys::ibv_access_flags;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, ops::Add, time::SystemTime};

use crate::{
    access::{flags_into_ibv_access, ibv_access_into_flags},
    rmr_manager::DEFAULT_RMR_TIMEOUT,
    AccessFlag,
};

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
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy, Debug, Builder)]
pub struct MrToken {
    /// The start address of this memory region
    pub addr: usize,
    /// The length of this memory region
    pub len: usize,
    /// The remote key of this memory region
    pub rkey: u32,
    /// The Deadline of this memory region.
    /// After ddl, this memory region maybe unavailable due to timeout.
    #[builder(default = "SystemTime::now().add(DEFAULT_RMR_TIMEOUT)")]
    pub ddl: SystemTime,
    /// Remote mr `ibv_access_flags` inner
    #[builder(field(
        type = "BitFlags<AccessFlag>",
        build = "flags_into_ibv_access(self.access).0"
    ))]
    pub access: u32,
}
