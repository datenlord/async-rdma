/// Local Memory Region
mod local;
/// Raw Memory Region
mod raw;
/// Remote Memory Region
mod remote;

pub(crate) use local::MrAffiliation;
#[allow(unreachable_pub)]
pub use local::{LocalMr, LocalMrAccess};
pub(crate) use raw::RawMemoryRegion;
#[allow(unreachable_pub)]
pub use remote::{RemoteMr, RemoteMrAccess};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Rdma Memory Region Access
pub trait MrAccess: Sync + Send + Debug {
    /// Get the start addr
    fn addr(&self) -> usize;

    /// Get the length
    fn length(&self) -> usize;

    /// Get the remote key
    fn rkey(&self) -> u32;

    /// Get the token
    #[inline]
    fn token(&self) -> MrToken {
        MrToken {
            addr: self.addr(),
            len: self.length(),
            rkey: self.rkey(),
        }
    }
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
}

/// Slice index trait, copy of unstable rust std lib
/// # Safety
/// Invalid index will result panic
pub(crate) unsafe trait SliceIndex<T: ?Sized> {
    /// The output type returned by methods.
    type Output: ?Sized;

    /// Returns a shared reference to the output at this location, if in
    /// bounds.
    fn get(self, slice: &T) -> Option<&Self::Output>;

    /// Returns a mutable reference to the output at this location, if in
    /// bounds.
    fn get_mut(self, slice: &mut T) -> Option<&mut Self::Output>;

    /// Returns a shared reference to the output at this location, without
    /// performing any bounds checking.
    /// Calling this method with an out-of-bounds index or a dangling `slice` pointer
    /// is *[undefined behavior]* even if the resulting reference is not used.
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    unsafe fn get_unchecked(self, slice: *const T) -> *const Self::Output;

    /// Returns a mutable reference to the output at this location, without
    /// performing any bounds checking.
    /// Calling this method with an out-of-bounds index or a dangling `slice` pointer
    /// is *[undefined behavior]* even if the resulting reference is not used.
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    unsafe fn get_unchecked_mut(self, slice: *mut T) -> *mut Self::Output;

    /// Returns a shared reference to the output at this location, panicking
    /// if out of bounds.
    fn index(self, slice: &T) -> &Self::Output;

    /// Returns a mutable reference to the output at this location, panicking
    /// if out of bounds.
    fn index_mut(self, slice: &mut T) -> &mut Self::Output;
}
