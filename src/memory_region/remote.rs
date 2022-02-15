use super::{MrAccess, MrToken};
use crate::agent::AgentInner;
use std::{ops::Range, sync::Arc};

/// Remote Memory Region Accrss
pub trait RemoteMrAccess: MrAccess {}

/// Affiliation of mr.
///
/// Get `Master mr` from `mr_allocator` by `alloc`, and get `Slave mr` from `Master mr`
/// by `get()`.
#[derive(Debug)]
pub(crate) enum RemoteMrAffiliation {
    /// master remote mr comes from `request_remote_mr`
    Master(Arc<AgentInner>),
    /// slave remote mr comes from master mr
    Slave(Arc<RemoteMr>),
}

/// Remote memory region
#[derive(Debug)]
pub struct RemoteMr {
    /// The affiliation of this mr
    affil: RemoteMrAffiliation,
    /// The token
    token: MrToken,
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

impl Drop for RemoteMr {
    #[inline]
    fn drop(&mut self) {
        match self.affil {
            RemoteMrAffiliation::Master(ref inner_agent) => {
                let agent = Arc::<AgentInner>::clone(inner_agent);
                let token = self.token;
                // detach the task
                let _task =
                    tokio::spawn(async move { AgentInner::release_mr(&agent, token).await });
            }
            RemoteMrAffiliation::Slave(_) => {}
        }
    }
}

impl RemoteMr {
    /// Create a remote memory region from the `token`
    #[inline]
    pub(crate) fn new_from_token(token: MrToken, agent: Arc<AgentInner>) -> Self {
        let _addr = token.addr;
        let _len = token.len;
        let affil = RemoteMrAffiliation::Master(agent);
        Self { affil, token }
    }

    /// Get a slave lmr
    #[inline]
    #[must_use]
    pub fn get(self: &Arc<Self>, i: Range<usize>) -> Option<Self> {
        if i.start >= i.end || i.end > self.length() {
            None
        } else {
            let token = self.token();
            let affil = RemoteMrAffiliation::Slave(Arc::<Self>::clone(self));
            // SAFETY: `self` is checked to be valid and in bounds above.
            Some(Self { affil, token })
        }
    }
}
