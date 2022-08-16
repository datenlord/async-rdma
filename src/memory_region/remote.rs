use rdma_sys::ibv_access_flags;

use super::{MrAccess, MrToken};
use crate::{agent::AgentInner, DEFAULT_ACCESS};
use std::{io, ops::Range, sync::Arc, time::SystemTime};

/// Remote Memory Region Accrss
pub trait RemoteMrReadAccess: MrAccess {
    /// Check if this `rmr` timeout.
    /// Return `true` if it times out, `false` otherwise.
    #[inline]
    fn timeout_check(&self) -> bool {
        SystemTime::now() >= self.token().ddl
    }

    /// Get the token of this `rmr`
    fn token(&self) -> MrToken;
}
/// Writable Remote mr trait
pub trait RemoteMrWriteAccess: MrAccess + RemoteMrReadAccess {}

/// Remote memory region
#[derive(Debug)]
pub struct RemoteMr {
    /// metadata of Remote mr
    token: MrToken,
    /// the agent which requested this remote mr.
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
    fn access(&self) -> ibv_access_flags {
        // TODO: add access control for rmr
        *DEFAULT_ACCESS
    }
}

impl RemoteMrReadAccess for RemoteMr {
    #[inline]
    fn token(&self) -> MrToken {
        self.token
    }
}
impl RemoteMrWriteAccess for RemoteMr {}

impl Drop for RemoteMr {
    #[inline]
    fn drop(&mut self) {
        let agent = Arc::<AgentInner>::clone(&self.agent);
        let token = self.token;
        // detach the task
        let _task = tokio::spawn(async move { AgentInner::release_mr(&agent, token).await });
    }
}

impl RemoteMr {
    /// Create a remote memory region from the `token`
    #[inline]
    pub(crate) fn new_from_token(token: MrToken, agent: Arc<AgentInner>) -> Self {
        Self { token, agent }
    }

    /// Get a remote mr slice
    #[inline]
    pub fn get(&self, i: Range<usize>) -> io::Result<RemoteMrSlice> {
        // SAFETY: `self` is checked to be valid and in bounds above.
        if i.start >= i.end || i.end > self.length() {
            Err(io::Error::new(io::ErrorKind::Other, "wrong range of rmr"))
        } else {
            let slice_token = MrToken {
                addr: self.addr().wrapping_add(i.start),
                len: i.end.wrapping_sub(i.start),
                rkey: self.rkey(),
                ddl: self.token.ddl,
            };
            Ok(RemoteMrSlice::new_from_token(self, slice_token))
        }
    }

    /// Get a mutable remote mr slice
    #[inline]
    pub fn get_mut(&mut self, i: Range<usize>) -> io::Result<RemoteMrSliceMut> {
        // SAFETY: `self` is checked to be valid and in bounds above.
        if i.start >= i.end || i.end > self.length() {
            Err(io::Error::new(io::ErrorKind::Other, "wrong range of rmr"))
        } else {
            let slice_token = MrToken {
                addr: self.addr().wrapping_add(i.start),
                len: i.end.wrapping_sub(i.start),
                rkey: self.rkey(),
                ddl: self.token.ddl,
            };
            Ok(RemoteMrSliceMut::new_from_token(self, slice_token))
        }
    }
}

impl MrAccess for &RemoteMr {
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
    fn access(&self) -> ibv_access_flags {
        // TODO: add access control for rmr
        *DEFAULT_ACCESS
    }
}

impl RemoteMrReadAccess for &RemoteMr {
    #[inline]
    fn token(&self) -> MrToken {
        self.token
    }
}

impl MrAccess for &mut RemoteMr {
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
    fn access(&self) -> ibv_access_flags {
        // TODO: add access control for rmr
        *DEFAULT_ACCESS
    }
}
impl RemoteMrReadAccess for &mut RemoteMr {
    #[inline]
    fn token(&self) -> MrToken {
        self.token
    }
}
impl RemoteMrWriteAccess for &mut RemoteMr {}

/// Remote memory region slice
#[derive(Debug)]
pub struct RemoteMrSlice<'a> {
    /// The remote mr where this slice comes from.
    /// Hold the reference here for the borrow checker and life time checking.
    #[allow(dead_code)]
    rmr: &'a RemoteMr,
    /// Metadata of Remote mr
    token: MrToken,
}

impl<'a> RemoteMrSlice<'a> {
    /// Create a remote memory region from the `token`
    #[inline]
    pub(crate) fn new_from_token(rmr: &'a RemoteMr, token: MrToken) -> Self {
        Self { rmr, token }
    }
}

impl MrAccess for RemoteMrSlice<'_> {
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
    fn access(&self) -> ibv_access_flags {
        // TODO: add access control for rmr
        *DEFAULT_ACCESS
    }
}

impl RemoteMrReadAccess for RemoteMrSlice<'_> {
    fn token(&self) -> MrToken {
        self.token
    }
}
/// Mutable remote memory region slice
#[derive(Debug)]
pub struct RemoteMrSliceMut<'a> {
    /// The remote mr where this slice comes from.
    /// Hold the reference here for the borrow checker and life time checking.
    #[allow(dead_code)]
    rmr: &'a mut RemoteMr,
    /// Metadata of Remote mr
    token: MrToken,
}

impl MrAccess for RemoteMrSliceMut<'_> {
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
    fn access(&self) -> ibv_access_flags {
        // TODO: add access control for rmr
        *DEFAULT_ACCESS
    }
}

impl<'a> RemoteMrSliceMut<'a> {
    /// Create a remote memory region from the `token`
    #[inline]
    pub(crate) fn new_from_token(rmr: &'a mut RemoteMr, token: MrToken) -> Self {
        Self { rmr, token }
    }
}

impl RemoteMrReadAccess for RemoteMrSliceMut<'_> {
    fn token(&self) -> MrToken {
        self.token
    }
}
impl RemoteMrWriteAccess for RemoteMrSliceMut<'_> {}
