use super::{
    mr_slice_from_raw_parts, mr_slice_from_raw_parts_mut, mr_slice_metadata, MrAccess, MrToken,
    SliceIndex, SliceMetaData,
};
use crate::agent::AgentInner;
use std::{
    ops::{Add, Index, IndexMut, Range, RangeFrom, RangeFull, RangeTo},
    sync::Arc,
};
use utilities::{cast_to_mut_ptr, cast_to_ptr, Cast};

/// Remote Memory Region Accrss
pub trait RemoteMrAccess: MrAccess {}

/// Remote memory region
#[repr(C)]
#[derive(Debug)]
pub struct RemoteMr {
    /// The token
    token: MrToken,
    /// Local agent
    agent: Arc<AgentInner>,
}

impl MrAccess for RemoteMr {
    #[inline]
    fn addr(&self) -> usize {
        self.token.addr
    }

    #[inline]
    fn length(&self) -> usize {
        self.token.len
    }

    #[inline]
    fn rkey(&self) -> u32 {
        self.token.rkey
    }

    #[inline]
    fn token(&self) -> MrToken {
        self.token
    }
}

impl RemoteMrAccess for RemoteMr {}

impl AsRef<RemoteMrSlice> for RemoteMr {
    #[inline]
    fn as_ref(&self) -> &RemoteMrSlice {
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

impl AsMut<RemoteMrSlice> for RemoteMr {
    #[inline]
    fn as_mut(&mut self) -> &mut RemoteMrSlice {
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

impl Index<Range<usize>> for RemoteMr {
    type Output = RemoteMrSlice;

    #[inline]
    fn index(&self, index: Range<usize>) -> &Self::Output {
        index.index(self.as_ref())
    }
}

impl Index<RangeFull> for RemoteMr {
    type Output = RemoteMrSlice;

    #[inline]
    fn index(&self, _index: RangeFull) -> &Self::Output {
        &self[0..self.length()]
    }
}

impl Index<RangeFrom<usize>> for RemoteMr {
    type Output = RemoteMrSlice;

    #[inline]
    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        &self[index.start..self.length()]
    }
}

impl Index<RangeTo<usize>> for RemoteMr {
    type Output = RemoteMrSlice;

    #[inline]
    fn index(&self, index: RangeTo<usize>) -> &Self::Output {
        &self[0..index.end]
    }
}

impl IndexMut<Range<usize>> for RemoteMr {
    #[inline]
    fn index_mut(&mut self, index: Range<usize>) -> &mut Self::Output {
        index.index_mut(self.as_mut())
    }
}

impl IndexMut<RangeFull> for RemoteMr {
    #[inline]
    fn index_mut(&mut self, _index: RangeFull) -> &mut Self::Output {
        let len = self.length();
        &mut self[0..len]
    }
}

impl IndexMut<RangeFrom<usize>> for RemoteMr {
    #[inline]
    fn index_mut(&mut self, index: RangeFrom<usize>) -> &mut Self::Output {
        let len = self.length();
        &mut self[index.start..len]
    }
}

impl IndexMut<RangeTo<usize>> for RemoteMr {
    #[inline]
    fn index_mut(&mut self, index: RangeTo<usize>) -> &mut Self::Output {
        &mut self[0..index.end]
    }
}

impl RemoteMr {
    /// Create a remote memory region from the `token`
    pub(crate) fn new_from_token(token: MrToken, agent: Arc<AgentInner>) -> Self {
        let _addr = token.addr;
        let _len = token.len;
        Self { token, agent }
    }
}

impl Drop for RemoteMr {
    #[inline]
    fn drop(&mut self) {
        let agent = Arc::<AgentInner>::clone(&self.agent);
        let token = self.token;
        // detach the task
        let _task = tokio::spawn(async move { AgentInner::release_mr(&agent, token).await });
    }
}

/// Remote Mr Slice, A Dst
#[repr(C)]
#[derive(Debug)]
pub struct RemoteMrSlice {
    /// remote mr, the slice not own
    mr: RemoteMr,
    /// unused for dst
    _unused: [u8],
}

impl MrAccess for &RemoteMrSlice {
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

impl MrAccess for &mut RemoteMrSlice {
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

impl RemoteMrAccess for &RemoteMrSlice {}

impl RemoteMrAccess for &mut RemoteMrSlice {}

unsafe impl SliceIndex<RemoteMrSlice> for Range<usize> {
    type Output = RemoteMrSlice;

    fn get(self, slice: &RemoteMrSlice) -> Option<&Self::Output> {
        if self.start >= self.end || self.end > slice.length() {
            None
        } else {
            // SAFETY: `self` is checked to be valid and in bounds above.
            unsafe { Some(&*self.get_unchecked(slice)) }
        }
    }

    fn get_mut(self, slice: &mut RemoteMrSlice) -> Option<&mut Self::Output> {
        if self.start >= self.end || self.end > slice.length() {
            None
        } else {
            // SAFETY: `self` is checked to be valid and in bounds above.
            unsafe { Some(&mut *self.get_unchecked_mut(slice)) }
        }
    }

    unsafe fn get_unchecked(self, slice: *const RemoteMrSlice) -> *const Self::Output {
        mr_slice_from_raw_parts(
            slice.cast(),
            SliceMetaData {
                offset: self.start.cast(),
                len: self.len().cast(),
            },
        )
    }

    unsafe fn get_unchecked_mut(self, slice: *mut RemoteMrSlice) -> *mut Self::Output {
        mr_slice_from_raw_parts_mut(
            slice.cast(),
            SliceMetaData {
                offset: self.start.cast(),
                len: self.len().cast(),
            },
        )
    }

    fn index(self, slice: &RemoteMrSlice) -> &Self::Output {
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

    fn index_mut(self, slice: &mut RemoteMrSlice) -> &mut Self::Output {
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
