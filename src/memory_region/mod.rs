/// Local Memory Region
pub(crate) mod local;
/// Raw Memory Region
mod raw;
/// Remote Memory Region
pub(crate) mod remote;
pub(crate) use raw::RawMemoryRegion;
use rdma_sys::ibv_access_flags;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, time::SystemTime};

/// Rdma Memory Region Access
pub trait MrAccess: Sync + Send + Debug {
    /// Get the start addr
    fn addr(&self) -> usize;

    /// Get the length
    fn length(&self) -> usize;

    /// Get the remote key
    fn rkey(&self) -> u32;

    /// Get the access of mr
    fn access(&self) -> ibv_access_flags;
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
}
