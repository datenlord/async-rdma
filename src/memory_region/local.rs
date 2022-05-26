use super::{raw::RawMemoryRegion, MrAccess, MrToken};
use crate::lock_utilities::{MappedRwLockReadGuard, MappedRwLockWriteGuard};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    fmt::Debug,
    ops::Range,
    slice,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::debug;

/// Local memory region trait
pub trait LocalMrReadAccess: MrAccess {
    /// Get the start pointer until it is readable
    ///
    /// If this mr is being used in RDMA ops, the thread may be blocked
    #[allow(clippy::as_conversions)]
    #[inline]
    fn as_ptr(&self) -> MappedRwLockReadGuard<*const u8> {
        MappedRwLockReadGuard::new(self.get_inner().read(), self.addr() as *const u8)
    }

    /// Try to get the start pointer
    ///
    /// Return `None` if this mr is being used in RDMA ops without blocking thread
    #[allow(clippy::as_conversions)]
    #[inline]
    fn try_as_ptr(&self) -> Option<MappedRwLockReadGuard<*const u8>> {
        self.get_inner().try_read().map_or_else(
            || None,
            |guard| return Some(MappedRwLockReadGuard::new(guard, self.addr() as *const u8)),
        )
    }

    /// Get the start pointer without lock
    ///
    /// # Safety:
    ///
    /// make sure the mr is readable without cancel safety issue
    ///
    /// TODO: move unchecked methords to unsafe trait
    #[inline]
    #[allow(clippy::as_conversions)]
    fn as_ptr_unchecked(&self) -> *const u8 {
        self.addr() as _
    }

    /// Get the memory region as slice until it is readable
    ///
    /// If this mr is being used in RDMA ops, the thread may be blocked
    #[inline]
    #[allow(clippy::as_conversions)]
    fn as_slice(&self) -> MappedRwLockReadGuard<&[u8]> {
        // SAFETY: unsoundness
        // BUG: unsound public api
        MappedRwLockReadGuard::map(self.as_ptr(), |ptr| unsafe {
            slice::from_raw_parts(ptr, self.length())
        })
    }

    /// Try to get the memory region as slice
    ///
    /// Return `None` if this mr is being used in RDMA ops without blocking thread
    #[allow(clippy::as_conversions)]
    #[inline]
    fn try_as_slice(&self) -> Option<MappedRwLockReadGuard<&[u8]>> {
        self.try_as_ptr().map_or_else(
            || None,
            |guard| {
                // SAFETY: unsoundness
                // BUG: unsound public api
                return Some(MappedRwLockReadGuard::map(guard, |ptr| unsafe {
                    slice::from_raw_parts(ptr, self.length())
                }));
            },
        )
    }

    /// Get the memory region as slice without lock
    ///
    /// # Safety:
    ///
    /// make sure the mr is readable without cancel safety issue
    ///
    /// TODO: move unchecked methords to unsafe trait
    #[inline]
    fn as_slice_unchecked(&self) -> &[u8] {
        // SAFETY: unsoundness
        // BUG: unsound public api
        unsafe { slice::from_raw_parts(self.as_ptr_unchecked(), self.length()) }
    }

    /// Get the local key
    fn lkey(&self) -> u32;

    /// Get the local key without lock
    ///
    /// # Safety:
    ///
    /// make sure the mr is readable without cancel safety issue
    ///
    /// TODO: move to unsafe trait
    #[inline]
    #[allow(clippy::unreachable)] // inner will not be null
    fn lkey_unchecked(&self) -> u32 {
        // SAFETY: unsoundness
        // BUG: unsound public api
        unsafe {
            <*const LocalMrInner>::as_ref(self.get_inner().data_ptr())
                .map_or_else(|| unreachable!("get null inner"), LocalMrInner::lkey)
        }
    }

    /// Get the remote key without lock
    ///
    /// # Safety:
    ///
    /// make sure the mr is readable without cancel safety issue
    ///
    /// TODO: move to unsafe trait
    #[inline]
    #[allow(clippy::unreachable)] // inner will not be null
    fn rkey_unchecked(&self) -> u32 {
        // SAFETY: unsoundness
        // BUG: unsound public api
        unsafe {
            <*const LocalMrInner>::as_ref(self.get_inner().data_ptr())
                .map_or_else(|| unreachable!("get null inner"), MrAccess::rkey)
        }
    }

    /// New a token with specified timeout
    #[inline]
    fn token_with_timeout(&self, timeout: Duration) -> Option<MrToken> {
        SystemTime::now().checked_add(timeout).map_or_else(
            || None,
            |ddl| {
                Some(MrToken {
                    addr: self.addr(),
                    len: self.length(),
                    rkey: self.rkey_unchecked(),
                    ddl,
                })
            },
        )
    }
    /// Get the corresponding `RwLocalMrInner`
    fn get_inner(&self) -> &Arc<RwLocalMrInner>;

    /// Is the corresponding `RwLocalMrInner` readable?
    #[inline]
    fn is_readable(&self) -> bool {
        !self.get_inner().is_locked_exclusive()
    }

    /// Get read lock of `LocalMrInenr`
    #[inline]
    fn read_inner(&self) -> RwLockReadGuard<LocalMrInner> {
        self.get_inner().read()
    }
}

/// Writable local mr trait
pub trait LocalMrWriteAccess: MrAccess + LocalMrReadAccess {
    /// Get the mutable start pointer until it is writeable
    ///
    /// If this mr is being used in RDMA ops, the thread may be blocked
    #[inline]
    #[allow(clippy::as_conversions)]
    fn as_mut_ptr(&mut self) -> MappedRwLockWriteGuard<*mut u8> {
        MappedRwLockWriteGuard::new(self.get_inner().write(), self.addr() as *mut u8)
    }

    /// Try to get the mutable start pointer
    ///
    /// Return `None` if this mr is being used in RDMA ops without blocking thread
    #[allow(clippy::as_conversions)]
    #[inline]
    fn try_as_mut_ptr(&self) -> Option<MappedRwLockWriteGuard<*mut u8>> {
        self.get_inner().try_write().map_or_else(
            || None,
            |guard| return Some(MappedRwLockWriteGuard::new(guard, self.addr() as *mut u8)),
        )
    }

    /// Get the memory region start mut addr without lock
    ///
    /// # Safety:
    ///
    /// make sure the mr is writeable without cancel safety issue
    ///
    /// TODO: move unchecked methords to unsafe trait
    #[inline]
    #[allow(clippy::as_conversions)]
    fn as_mut_ptr_unchecked(&mut self) -> *mut u8 {
        // const pointer to mut pointer is safe
        self.as_ptr_unchecked() as _
    }

    /// Get the memory region as mutable slice until it is writeable
    ///
    /// If this mr is being used in RDMA ops, the thread may be blocked
    #[inline]
    #[allow(clippy::as_conversions)]
    fn as_mut_slice(&mut self) -> MappedRwLockWriteGuard<&mut [u8]> {
        let len = self.length();
        // SAFETY: unsoundness
        // BUG: unsound public api
        MappedRwLockWriteGuard::map(self.as_mut_ptr(), |ptr| unsafe {
            slice::from_raw_parts_mut(ptr, len)
        })
    }

    /// Try to get the memory region as mutable slice
    ///
    /// Return `None` if this mr is being used in RDMA ops without blocking thread
    #[allow(clippy::as_conversions)]
    #[inline]
    fn try_as_mut_slice(&mut self) -> Option<MappedRwLockWriteGuard<&mut [u8]>> {
        self.try_as_mut_ptr().map_or_else(
            || None,
            |guard| {
                // SAFETY: unsoundness
                // BUG: unsound public api
                return Some(MappedRwLockWriteGuard::map(guard, |ptr| unsafe {
                    slice::from_raw_parts_mut(ptr, self.length())
                }));
            },
        )
    }

    /// Get the memory region as mut slice without lock
    ///
    /// # Safety:
    ///
    /// make sure the mr is writeable without cancel safety issue
    ///
    /// TODO: move unchecked methords to unsafe trait
    #[inline]
    fn as_mut_slice_unchecked(&mut self) -> &mut [u8] {
        // SAFETY: unsoundness
        // BUG: unsound public api
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr_unchecked(), self.length()) }
    }

    /// Is the corresponding `RwLocalMrInner` writeable?
    #[inline]
    fn is_writeable(&self) -> bool {
        !self.get_inner().is_locked()
    }

    /// Get write lock of `LocalMrInenr`
    #[inline]
    fn write_inner(&self) -> RwLockWriteGuard<LocalMrInner> {
        self.get_inner().write()
    }
}

/// Local Memory Region
#[derive(Debug)]
pub struct LocalMr {
    /// The corresponding `RwLocalMrInner`.
    inner: Arc<RwLocalMrInner>,
    /// The start address of this mr
    addr: usize,
    /// the length of this mr
    len: usize,
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
        self.read_inner().rkey()
    }
}

impl LocalMrReadAccess for LocalMr {
    #[inline]
    fn lkey(&self) -> u32 {
        self.read_inner().lkey()
    }

    #[inline]
    fn get_inner(&self) -> &Arc<RwLocalMrInner> {
        &self.inner
    }
}

impl LocalMrWriteAccess for LocalMr {}

impl LocalMr {
    /// New Local Mr
    pub(crate) fn new(inner: LocalMrInner) -> Self {
        let addr = inner.addr;
        let len = inner.len;
        let inner = Arc::new(RwLock::new(inner));
        Self { inner, addr, len }
    }

    /// Get a local mr slice
    ///
    /// Return `None` if the inputed range is wrong
    #[inline]
    #[must_use]
    pub fn get(&self, i: Range<usize>) -> Option<LocalMrSlice> {
        // SAFETY: `self` is checked to be valid and in bounds above.
        if i.start >= i.end || i.end > self.len {
            None
        } else {
            Some(LocalMrSlice::new(
                self,
                Arc::<RwLocalMrInner>::clone(&self.inner),
                self.addr().wrapping_add(i.start),
                i.len(),
            ))
        }
    }

    /// Get an unchecked local mr slice
    ///
    /// # Safety
    ///
    /// Callers of this function are responsible that these preconditions are
    /// satisfied:
    ///
    /// * The starting index must not exceed the ending index;
    /// * Indexes must be within bounds of the original `LocalMr`.
    #[inline]
    #[must_use]
    pub unsafe fn get_unchecked(&self, i: Range<usize>) -> LocalMrSlice {
        LocalMrSlice::new(
            self,
            Arc::<RwLocalMrInner>::clone(&self.inner),
            self.addr().wrapping_add(i.start),
            i.len(),
        )
    }

    /// Get a mutable local mr slice
    ///
    /// Return `None` if the inputed range is wrong
    #[inline]
    pub fn get_mut(&mut self, i: Range<usize>) -> Option<LocalMrSliceMut> {
        // SAFETY: `self` is checked to be valid and in bounds above.
        if i.start >= i.end || i.end > self.length() {
            None
        } else {
            Some(LocalMrSliceMut::new(
                self,
                Arc::<RwLocalMrInner>::clone(&self.inner),
                self.addr().wrapping_add(i.start),
                i.len(),
            ))
        }
    }

    /// Get an unchecked mutable local mr slice
    ///
    /// # Safety
    ///
    /// Callers of this function are responsible that these preconditions are
    /// satisfied:
    ///
    /// * The starting index must not exceed the ending index;
    /// * Indexes must be within bounds of the original `LocalMr`.
    #[inline]
    pub unsafe fn get_unchecked_mut(&mut self, i: Range<usize>) -> LocalMrSliceMut {
        LocalMrSliceMut::new(
            self,
            Arc::<RwLocalMrInner>::clone(&self.inner),
            self.addr().wrapping_add(i.start),
            i.len(),
        )
    }

    /// Take the ownership and return a sub local mr from self
    ///
    /// Return `None` if the inputed range is wrong
    #[inline]
    pub(crate) fn take(mut self, i: Range<usize>) -> Option<Self> {
        // SAFETY: `self` is checked to be valid and in bounds above.
        if i.start >= i.end || i.end > self.length() {
            None
        } else {
            self.addr = self.addr.wrapping_add(i.start);
            self.len = i.end.wrapping_sub(i.start);
            Some(self)
        }
    }

    /// Take the ownership and return an unchecked sub local mr from self
    ///
    /// # Safety
    ///
    /// Callers of this function are responsible that these preconditions are
    /// satisfied:
    ///
    /// * The starting index must not exceed the ending index;
    /// * Indexes must be within bounds of the original `LocalMr`.
    #[inline]
    #[allow(dead_code)]
    pub(crate) unsafe fn take_unchecked(mut self, i: Range<usize>) -> Self {
        self.addr = self.addr.wrapping_add(i.start);
        self.len = i.end.wrapping_sub(i.start);
        self
    }
}

/// `LocalMrInner` in `RwLock`
pub(crate) type RwLocalMrInner = RwLock<LocalMrInner>;
/// Local Memory Region inner
#[derive(Debug)]
pub struct LocalMrInner {
    /// The start address of this mr
    addr: usize,
    /// The length of this mr
    len: usize,
    /// The raw mr where this local mr comes from.
    raw: Arc<RawMemoryRegion>,
}

impl Drop for LocalMrInner {
    #[inline]
    #[allow(clippy::as_conversions)]
    fn drop(&mut self) {
        debug!("drop LocalMr {:?}", self);
        // SAFETY: ffi
        // TODO: check safety
        unsafe { tikv_jemalloc_sys::free(self.addr as _) }
    }
}

impl MrAccess for LocalMrInner {
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

impl LocalMrInner {
    /// Crate a new `LocalMrInner`
    pub(crate) fn new(addr: usize, len: usize, raw: Arc<RawMemoryRegion>) -> Self {
        Self { addr, len, raw }
    }

    /// Get local key of memory region
    fn lkey(&self) -> u32 {
        self.raw.lkey()
    }
}

impl MrAccess for &LocalMr {
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
        self.read_inner().rkey()
    }
}

impl LocalMrReadAccess for &LocalMr {
    #[inline]
    fn lkey(&self) -> u32 {
        self.read_inner().lkey()
    }

    #[inline]
    fn get_inner(&self) -> &Arc<RwLocalMrInner> {
        &self.inner
    }
}

/// A slice of `LocalMr`
#[derive(Debug)]
pub struct LocalMrSlice<'a> {
    /// The local mr where this local mr slice comes from.
    lmr: &'a LocalMr,
    /// The corresponding `RwLocalMrInner`.
    inner: Arc<RwLocalMrInner>,
    /// The start address of this mr
    addr: usize,
    /// the length of this mr
    len: usize,
}

impl MrAccess for LocalMrSlice<'_> {
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
        self.lmr.rkey()
    }
}

impl LocalMrReadAccess for LocalMrSlice<'_> {
    fn lkey(&self) -> u32 {
        self.lmr.lkey()
    }

    #[inline]
    fn get_inner(&self) -> &Arc<RwLocalMrInner> {
        &self.inner
    }
}

impl<'a> LocalMrSlice<'a> {
    /// New a local mr slice.
    pub(crate) fn new(
        lmr: &'a LocalMr,
        inner: Arc<RwLocalMrInner>,
        addr: usize,
        len: usize,
    ) -> Self {
        Self {
            lmr,
            inner,
            addr,
            len,
        }
    }
}

/// Mutable local mr slice
#[derive(Debug)]
pub struct LocalMrSliceMut<'a> {
    /// The local mr where this local mr slice comes from.
    lmr: &'a mut LocalMr,
    /// The corresponding `RwLocalMrInner`.
    inner: Arc<RwLocalMrInner>,
    /// The start address of this mr
    addr: usize,
    /// the length of this mr
    len: usize,
}

impl<'a> LocalMrSliceMut<'a> {
    /// New a mutable local mr slice.
    pub(crate) fn new(
        lmr: &'a mut LocalMr,
        inner: Arc<RwLocalMrInner>,
        addr: usize,
        len: usize,
    ) -> Self {
        Self {
            lmr,
            inner,
            addr,
            len,
        }
    }
}

impl MrAccess for LocalMrSliceMut<'_> {
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
        self.lmr.rkey()
    }
}

impl LocalMrReadAccess for LocalMrSliceMut<'_> {
    fn lkey(&self) -> u32 {
        self.lmr.lkey()
    }

    #[inline]
    fn get_inner(&self) -> &Arc<RwLocalMrInner> {
        &self.inner
    }
}

impl LocalMrWriteAccess for LocalMrSliceMut<'_> {}
