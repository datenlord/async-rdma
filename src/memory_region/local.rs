use super::{
    mr_slice_from_raw_parts, mr_slice_from_raw_parts_mut, mr_slice_metadata, raw::RawMemoryRegion,
    MrAccess, SliceIndex, SliceMetaData,
};
use crate::mr_allocator::MrAllocator;
use std::{
    fmt::Debug,
    ops::{Add, Index, IndexMut, Range, RangeFrom, RangeFull, RangeTo},
    slice,
    sync::Arc,
};
use utilities::{cast_to_mut_ptr, cast_to_ptr, Cast};

/// Local memory region trait
pub trait LocalMrAccess: MrAccess {
    /// Get the start pointer
    #[inline]
    #[allow(clippy::as_conversions)]
    fn as_ptr(&self) -> *const u8 {
        self.addr() as _
    }

    /// Get the memory region start mut addr
    #[inline]
    #[allow(clippy::as_conversions)]
    fn as_mut_ptr(&mut self) -> *mut u8 {
        // const pointer to mut pointer is safe
        self.as_ptr() as _
    }

    /// Get the memory region as slice
    #[inline]
    fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.length()) }
    }

    /// Get the memory region as mut slice
    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.length()) }
    }

    /// Get the local key
    fn lkey(&self) -> u32;
}

/// Local Memory Region
#[repr(C)]
#[derive(Debug)]
pub struct LocalMr {
    /// The start address of thie MR
    addr: usize,
    /// the length of this MR
    len: usize,
    /// raw memory region
    raw: Arc<RawMemoryRegion>,
    /// allocator the mr alloc from
    allocator: Arc<MrAllocator>,
}

impl MrAccess for LocalMr {
    #[inline]
    fn addr(&self) -> usize {
        self.addr
    }

    #[inline]
    fn length(&self) -> usize {
        self.len
    }

    #[inline]
    fn rkey(&self) -> u32 {
        self.raw.rkey()
    }
}

impl LocalMrAccess for LocalMr {
    #[inline]
    fn lkey(&self) -> u32 {
        self.raw.lkey()
    }
}

impl AsRef<LocalMrSlice> for LocalMr {
    #[inline]
    fn as_ref(&self) -> &LocalMrSlice {
        unsafe {
            &*mr_slice_from_raw_parts(
                cast_to_ptr(self),
                SliceMetaData {
                    offset: 0,
                    len: self.length().cast(),
                },
            )
        }
    }
}

impl AsMut<LocalMrSlice> for LocalMr {
    #[inline]
    fn as_mut(&mut self) -> &mut LocalMrSlice {
        unsafe {
            &mut *mr_slice_from_raw_parts_mut(
                cast_to_mut_ptr(self),
                SliceMetaData {
                    offset: 0,
                    len: self.length().cast(),
                },
            )
        }
    }
}

impl Index<Range<usize>> for LocalMr {
    type Output = LocalMrSlice;

    #[inline]
    fn index(&self, index: Range<usize>) -> &Self::Output {
        index.index(self.as_ref())
    }
}

impl Index<RangeFull> for LocalMr {
    type Output = LocalMrSlice;

    #[inline]
    fn index(&self, _index: RangeFull) -> &Self::Output {
        &self[0..self.length()]
    }
}

impl Index<RangeFrom<usize>> for LocalMr {
    type Output = LocalMrSlice;

    #[inline]
    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        &self[index.start..self.length()]
    }
}

impl Index<RangeTo<usize>> for LocalMr {
    type Output = LocalMrSlice;

    #[inline]
    fn index(&self, index: RangeTo<usize>) -> &Self::Output {
        &self[0..index.end]
    }
}

impl IndexMut<Range<usize>> for LocalMr {
    #[inline]
    fn index_mut(&mut self, index: Range<usize>) -> &mut Self::Output {
        index.index_mut(self.as_mut())
    }
}

impl IndexMut<RangeFull> for LocalMr {
    #[inline]
    fn index_mut(&mut self, _index: RangeFull) -> &mut Self::Output {
        let len = self.length();
        &mut self[0..len]
    }
}

impl IndexMut<RangeFrom<usize>> for LocalMr {
    #[inline]
    fn index_mut(&mut self, index: RangeFrom<usize>) -> &mut Self::Output {
        let len = self.length();
        &mut self[index.start..len]
    }
}

impl IndexMut<RangeTo<usize>> for LocalMr {
    #[inline]
    fn index_mut(&mut self, index: RangeTo<usize>) -> &mut Self::Output {
        &mut self[0..index.end]
    }
}

impl LocalMr {
    /// New Local Mr
    pub(crate) fn new(
        addr: usize,
        len: usize,
        raw: Arc<RawMemoryRegion>,
        allocator: Arc<MrAllocator>,
    ) -> Self {
        Self {
            addr,
            len,
            raw,
            allocator,
        }
    }

    /// take mr into sub mr
    #[inline]
    #[must_use]
    pub fn take(self, range: Range<usize>) -> Self {
        #[allow(clippy::restriction)]
        if range.start >= range.end || range.end > self.length() {
            panic!("out of local mr bound");
        }
        Self {
            addr: self.addr().add(range.start),
            len: range.len(),
            raw: Arc::<RawMemoryRegion>::clone(&self.raw),
            allocator: Arc::<MrAllocator>::clone(&self.allocator),
        }
    }
}

/// Local Mr Slice, A Dst
#[repr(C)]
#[derive(Debug)]
pub struct LocalMrSlice {
    /// local mr, the slice not own
    mr: LocalMr,
    /// unused for dst
    _unused: [u8],
}

impl MrAccess for &LocalMrSlice {
    #[inline]
    fn addr(&self) -> usize {
        let metadata = mr_slice_metadata(*self);
        let offset: usize = metadata.offset.cast();
        self.mr.addr().add(offset)
    }

    #[inline]
    fn length(&self) -> usize {
        let metadata = mr_slice_metadata(*self);
        metadata.len.cast()
    }

    #[inline]
    fn rkey(&self) -> u32 {
        self.mr.rkey()
    }
}

impl MrAccess for &mut LocalMrSlice {
    #[inline]
    fn addr(&self) -> usize {
        let metadata = mr_slice_metadata(*self);
        let offset: usize = metadata.offset.cast();
        self.mr.addr().add(offset)
    }

    #[inline]
    fn length(&self) -> usize {
        let metadata = mr_slice_metadata(*self);
        metadata.len.cast()
    }

    #[inline]
    fn rkey(&self) -> u32 {
        self.mr.rkey()
    }
}

impl LocalMrAccess for &LocalMrSlice {
    #[inline]
    fn lkey(&self) -> u32 {
        self.mr.lkey()
    }
}

impl LocalMrAccess for &mut LocalMrSlice {
    #[inline]
    fn lkey(&self) -> u32 {
        self.mr.lkey()
    }
}

unsafe impl SliceIndex<LocalMrSlice> for Range<usize> {
    type Output = LocalMrSlice;

    fn get(self, slice: &LocalMrSlice) -> Option<&Self::Output> {
        if self.start >= self.end || self.end > slice.length() {
            None
        } else {
            // SAFETY: `self` is checked to be valid and in bounds above.
            unsafe { Some(&*self.get_unchecked(slice)) }
        }
    }

    fn get_mut(self, slice: &mut LocalMrSlice) -> Option<&mut Self::Output> {
        if self.start >= self.end || self.end > slice.length() {
            None
        } else {
            // SAFETY: `self` is checked to be valid and in bounds above.
            unsafe { Some(&mut *self.get_unchecked_mut(slice)) }
        }
    }

    unsafe fn get_unchecked(self, slice: *const LocalMrSlice) -> *const Self::Output {
        mr_slice_from_raw_parts(
            slice.cast(),
            SliceMetaData {
                offset: self.start.cast(),
                len: self.len().cast(),
            },
        )
    }

    unsafe fn get_unchecked_mut(self, slice: *mut LocalMrSlice) -> *mut Self::Output {
        mr_slice_from_raw_parts_mut(
            slice.cast(),
            SliceMetaData {
                offset: self.start.cast(),
                len: self.len().cast(),
            },
        )
    }

    fn index(self, slice: &LocalMrSlice) -> &Self::Output {
        #[allow(clippy::restriction)]
        if self.start >= self.end {
            panic!("range start is greater than end");
        } else if self.end > slice.length() {
            panic!("range end is greater than slice end");
        } else {
            // SAFETY: `self` is checked to be valid and in bounds above.
            unsafe { &*self.get_unchecked(slice) }
        }
    }

    fn index_mut(self, slice: &mut LocalMrSlice) -> &mut Self::Output {
        #[allow(clippy::restriction)]
        if self.start >= self.end {
            panic!("range start is greater than end");
        } else if self.end > slice.length() {
            panic!("range end is greater than slice end");
        } else {
            // SAFETY: `self` is checked to be valid and in bounds above.
            unsafe { &mut *self.get_unchecked_mut(slice) }
        }
    }
}
