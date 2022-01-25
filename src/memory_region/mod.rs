/// Local Memory Region
mod local;
/// Raw Memory Region
mod raw;
/// Remote Memory Region
mod remote;

#[allow(unreachable_pub)]
pub use local::{LocalMr, LocalMrAccess, LocalMrSlice};
pub(crate) use raw::RawMemoryRegion;
#[allow(unreachable_pub)]
pub use remote::{RemoteMr, RemoteMrAccess, RemoteMrSlice};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utilities::cast_to_ptr;

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

/// Mr slice metadata, which size should equal to sizeof usize
#[repr(C)]
#[derive(Clone, Copy)]
struct SliceMetaData {
    /// start at the offset of Mr which this slice belong
    offset: u32,
    /// len of mr slice
    len: u32,
}

impl From<usize> for SliceMetaData {
    fn from(u: usize) -> Self {
        unsafe { *(cast_to_ptr(&u)) }
    }
}

impl From<SliceMetaData> for usize {
    #[inline]
    fn from(s: SliceMetaData) -> Self {
        unsafe { *(cast_to_ptr(&s)) }
    }
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

/// Mr slice Ptr Repr
#[repr(C)]
union MrSlicePtrRepr<T: ?Sized> {
    /// const ptr
    const_ptr: *const T,
    /// mut ptr
    mut_ptr: *mut T,
    /// ptr and metadata components
    components: MrSlicePtrComponents,
}

/// Mr slice components include Mr ptr and slice metadata
#[repr(C)]
#[derive(Clone, Copy)]
struct MrSlicePtrComponents {
    /// ptr to Mr
    data_address: *const (),
    /// metadata of mr slice
    metadata: SliceMetaData,
}

/// get mr slice ptr from addr and memtadata
#[inline]
const fn mr_slice_from_raw_parts<T: ?Sized>(
    data_address: *const (),
    metadata: SliceMetaData,
) -> *const T {
    // SAFETY: Accessing the value from the `PtrRepr` union is safe since *const T
    // and PtrComponents<T> have the same memory layouts. Only std can make this
    // guarantee.
    unsafe {
        MrSlicePtrRepr {
            components: MrSlicePtrComponents {
                data_address,
                metadata,
            },
        }
        .const_ptr
    }
}

/// get mut mr slice ptr from mut addr and memtadata
#[inline]
const fn mr_slice_from_raw_parts_mut<T: ?Sized>(
    data_address: *mut (),
    metadata: SliceMetaData,
) -> *mut T {
    // SAFETY: Accessing the value from the `PtrRepr` union is safe since *const T
    // and PtrComponents<T> have the same memory layouts. Only std can make this
    // guarantee.
    unsafe {
        MrSlicePtrRepr {
            components: MrSlicePtrComponents {
                data_address,
                metadata,
            },
        }
        .mut_ptr
    }
}

/// get the metadata of mr slice
#[inline]
const fn mr_slice_metadata<T: ?Sized>(ptr: *const T) -> SliceMetaData {
    // SAFETY: Accessing the value from the `PtrRepr` union is safe since *const T
    // and PtrComponents<T> have the same memory layouts. Only std can make this
    // guarantee.
    unsafe { MrSlicePtrRepr { const_ptr: ptr }.components.metadata }
}
