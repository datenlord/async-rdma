use super::{raw::RawMemoryRegion, MrAccess, MrToken};
use clippy_utilities::OverflowArithmetic;
use std::{
    fmt::Debug,
    io,
    ops::Range,
    slice,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::debug;

/// Local memory region trait
pub trait LocalMrReadAccess: MrAccess {
    /// Get the start pointer
    #[inline]
    #[allow(clippy::as_conversions)]
    fn as_ptr(&self) -> *const u8 {
        self.addr() as _
    }

    /// Get the memory region as slice
    #[inline]
    fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.length()) }
    }

    /// Get the local key
    fn lkey(&self) -> u32;

    /// New a token with specified timeout
    #[inline]
    fn token_with_timeout(&self, timeout: Duration) -> Option<MrToken> {
        SystemTime::now().checked_add(timeout).map_or_else(
            || None,
            |ddl| {
                Some(MrToken {
                    addr: self.addr(),
                    len: self.length(),
                    rkey: self.rkey(),
                    ddl,
                })
            },
        )
    }
}

/// Writable local mr trait
pub trait LocalMrWriteAccess: MrAccess + LocalMrReadAccess {
    /// Get the memory region start mut addr
    #[inline]
    #[allow(clippy::as_conversions)]
    fn as_mut_ptr(&mut self) -> *mut u8 {
        // const pointer to mut pointer is safe
        self.as_ptr() as _
    }

    /// Get the memory region as mut slice
    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.length()) }
    }
}

/// Local Memory Region
#[derive(Debug)]
pub struct LocalMr {
    /// `LocalMr take()` may change `addr`, use `origin_addr` when `free()`
    origin_addr: usize,
    /// The start address of this mr
    addr: usize,
    /// The length of this mr
    len: usize,
    /// The raw mr where this local mr comes from.
    raw: Arc<RawMemoryRegion>,
}

impl Drop for LocalMr {
    #[inline]
    #[allow(clippy::as_conversions)]
    fn drop(&mut self) {
        debug!("drop LocalMr {:?}", self);
        unsafe { tikv_jemalloc_sys::free(self.origin_addr as _) }
    }
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
        self.raw.lkey()
    }
}

impl LocalMrReadAccess for LocalMr {
    #[inline]
    fn lkey(&self) -> u32 {
        self.raw.lkey()
    }
}

impl LocalMrWriteAccess for LocalMr {}

impl LocalMr {
    /// New Local Mr
    pub(crate) fn new(addr: usize, len: usize, raw: Arc<RawMemoryRegion>) -> Self {
        Self {
            origin_addr: addr,
            addr,
            len,
            raw,
        }
    }

    /// Get a local mr slice
    #[inline]
    pub fn get(&self, i: Range<usize>) -> io::Result<LocalMrSlice> {
        // SAFETY: `self` is checked to be valid and in bounds above.
        if i.start >= i.end || i.end > self.length() {
            Err(io::Error::new(io::ErrorKind::Other, "wrong range of lmr"))
        } else {
            Ok(LocalMrSlice::new(
                self,
                self.addr().overflow_add(i.start),
                i.len(),
            ))
        }
    }

    /// Get a mutable local mr slice
    #[inline]
    pub fn get_mut(&mut self, i: Range<usize>) -> io::Result<LocalMrSliceMut> {
        // SAFETY: `self` is checked to be valid and in bounds above.
        if i.start >= i.end || i.end > self.length() {
            Err(io::Error::new(io::ErrorKind::Other, "wrong range of lmr"))
        } else {
            Ok(LocalMrSliceMut::new(
                self,
                self.addr().overflow_add(i.start),
                i.len(),
            ))
        }
    }

    /// take the ownership and return a sub local mr from self.
    #[inline]
    pub(crate) fn take(mut self, i: Range<usize>) -> io::Result<Self> {
        // SAFETY: `self` is checked to be valid and in bounds above.
        if i.start >= i.end || i.end > self.length() {
            Err(io::Error::new(io::ErrorKind::Other, "wrong range of lmr"))
        } else {
            self.addr = self.addr.overflow_add(i.start);
            self.len = i.end.overflow_sub(i.start);
            Ok(self)
        }
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
        self.raw.rkey()
    }
}

impl LocalMrReadAccess for &LocalMr {
    #[inline]
    fn lkey(&self) -> u32 {
        self.raw.lkey()
    }
}

/// A slice of `LocalMr`
#[derive(Debug)]
pub struct LocalMrSlice<'a> {
    /// The local mr where this local mr slice comes from.
    lmr: &'a LocalMr,
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
}

impl<'a> LocalMrSlice<'a> {
    /// New a local mr slice.
    pub(crate) fn new(lmr: &'a LocalMr, addr: usize, len: usize) -> Self {
        Self { lmr, addr, len }
    }
}

/// Mutable local mr slice
#[derive(Debug)]
pub struct LocalMrSliceMut<'a> {
    /// The local mr where this local mr slice comes from.
    lmr: &'a mut LocalMr,
    /// The start address of this mr
    addr: usize,
    /// the length of this mr
    len: usize,
}

impl<'a> LocalMrSliceMut<'a> {
    /// New a mutable local mr slice.
    pub(crate) fn new(lmr: &'a mut LocalMr, addr: usize, len: usize) -> Self {
        Self { lmr, addr, len }
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
}

impl LocalMrWriteAccess for LocalMrSliceMut<'_> {}
