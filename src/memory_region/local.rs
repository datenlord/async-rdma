use utilities::OverflowArithmetic;

use super::{raw::RawMemoryRegion, MrAccess};
use std::{fmt::Debug, io, ops::Range, slice, sync::Arc};

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

/// Affiliation of mr.
///
/// Get `Master mr` from `mr_allocator` by `alloc`, and get `Slave mr` from `Master mr`
/// by `get()`.
#[derive(Debug)]
pub(crate) enum MrAffiliation {
    /// master local mr comes from mr_allocator
    Master(Arc<RawMemoryRegion>),
    /// slave local mr comes from master mr
    Slave(Arc<LocalMr>),
}
/// Local Memory Region
#[derive(Debug)]
pub struct LocalMr {
    /// The affiliation of this mr
    affil: MrAffiliation,
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
        match self.affil {
            MrAffiliation::Master(ref rmr) => rmr.rkey(),
            // cause multi jump
            MrAffiliation::Slave(ref lmr) => lmr.rkey(),
        }
    }

    #[inline]
    fn token(&self) -> super::MrToken {
        super::MrToken {
            addr: self.addr(),
            len: self.length(),
            rkey: self.rkey(),
        }
    }
}

impl LocalMrAccess for LocalMr {
    #[inline]
    fn lkey(&self) -> u32 {
        match self.affil {
            MrAffiliation::Master(ref rmr) => rmr.lkey(),
            // cause multi jump
            MrAffiliation::Slave(ref lmr) => lmr.lkey(),
        }
    }
}

impl LocalMr {
    /// New Local Mr
    pub(crate) fn new(affil: MrAffiliation, addr: usize, len: usize) -> Self {
        Self { affil, addr, len }
    }

    /// Get a slave lmr
    #[inline]
    pub fn get(self: &Arc<Self>, i: Range<usize>) -> io::Result<Self> {
        // SAFETY: `self` is checked to be valid and in bounds above.
        if i.start >= i.end || i.end > self.length() {
            Err(io::Error::new(io::ErrorKind::Other, "wrong range of lmr"))
        } else {
            let affil = MrAffiliation::Slave(Arc::<Self>::clone(self));
            Ok(Self::new(affil, self.addr().overflow_add(i.start), i.len()))
        }
    }

    /// return a slave mr and take the ownership of the master mr
    pub(crate) fn take(self, i: Range<usize>) -> io::Result<Self> {
        // SAFETY: `self` is checked to be valid and in bounds above.
        if i.start >= i.end || i.end > self.length() {
            Err(io::Error::new(io::ErrorKind::Other, "wrong range of lmr"))
        } else {
            let addr = self.addr().overflow_add(i.start);
            let affil = MrAffiliation::Slave(Arc::new(self));
            Ok(Self::new(affil, addr, i.len()))
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
        match self.affil {
            MrAffiliation::Master(ref rmr) => rmr.rkey(),
            // cause multi jump
            MrAffiliation::Slave(ref lmr) => lmr.rkey(),
        }
    }
}

impl LocalMrAccess for &LocalMr {
    #[inline]
    fn lkey(&self) -> u32 {
        match self.affil {
            MrAffiliation::Master(ref rmr) => rmr.lkey(),
            // cause multi jump
            MrAffiliation::Slave(ref lmr) => lmr.lkey(),
        }
    }
}
