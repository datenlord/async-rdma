/// Local Memory Region
pub(crate) mod local;
/// Raw Memory Region
mod raw;
/// Remote Memory Region
pub(crate) mod remote;
pub(crate) use raw::RawMemoryRegion;
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
