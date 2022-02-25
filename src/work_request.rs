use crate::{
    completion_queue::WorkRequestId,
    memory_region::{
        local::{LocalMrReadAccess, LocalMrWriteAccess},
        remote::{RemoteMrReadAccess, RemoteMrWriteAccess},
    },
};
use clippy_utilities::Cast;
use rdma_sys::{ibv_recv_wr, ibv_send_flags, ibv_send_wr, ibv_sge, ibv_wr_opcode};
use std::borrow::Borrow;

/// Send work request
#[repr(C)]
pub(crate) struct SendWr {
    /// internal `ibv_send_wr`
    inner: ibv_send_wr,
    /// the `ibv_sge`s to be sent
    sges: Vec<ibv_sge>,
}

impl SendWr {
    /// create a new send work request
    #[allow(single_use_lifetimes)]
    fn new<'lm, LM: Borrow<dyn LocalMrReadAccess + 'lm>>(lms: &[LM], wr_id: WorkRequestId) -> Self {
        assert!(!lms.is_empty());
        let mut sges: Vec<ibv_sge> = lms
            .iter()
            .map(|lm| {
                let lm: &dyn LocalMrReadAccess = lm.borrow();
                Into::<ibv_sge>::into(lm)
            })
            .collect();
        let mut inner = unsafe { std::mem::zeroed::<ibv_send_wr>() };
        inner.next = std::ptr::null_mut();
        inner.wr_id = wr_id.into();
        inner.sg_list = sges.as_mut_ptr();
        inner.num_sge = sges.len().cast();
        Self { inner, sges }
    }

    /// create a new send work requet for "send"
    pub(crate) fn new_send(lms: &[&dyn LocalMrReadAccess], wr_id: WorkRequestId) -> Self {
        let mut sr = Self::new(lms, wr_id);
        sr.inner.opcode = ibv_wr_opcode::IBV_WR_SEND;
        sr.inner.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        sr
    }

    /// create a new send work requet for "read"
    #[allow(clippy::as_conversions)] // Convert pointer to usize is safe for later ibv lib use
    pub(crate) fn new_read(
        lms: &[&mut dyn LocalMrWriteAccess],
        wr_id: WorkRequestId,
        rm: &dyn RemoteMrReadAccess,
    ) -> Self {
        assert!(!lms.is_empty());
        let mut sges: Vec<ibv_sge> = lms
            .iter()
            .map(|lm| {
                let lm: &dyn LocalMrWriteAccess = lm.borrow();
                Into::<ibv_sge>::into(lm)
            })
            .collect();
        let mut inner = unsafe { std::mem::zeroed::<ibv_send_wr>() };
        inner.next = std::ptr::null_mut();
        inner.wr_id = wr_id.into();
        inner.sg_list = sges.as_mut_ptr();
        inner.num_sge = sges.len().cast();
        let mut sr = Self { inner, sges };
        sr.inner.opcode = ibv_wr_opcode::IBV_WR_RDMA_READ;
        sr.inner.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        sr.inner.wr.rdma.remote_addr = rm.addr().cast();
        sr.inner.wr.rdma.rkey = rm.rkey();
        sr
    }

    /// create a new send work requet for "write"
    #[allow(clippy::as_conversions)] // Convert pointer to usize is safe for later ibv lib use
    pub(crate) fn new_write(
        lms: &[&dyn LocalMrReadAccess],
        wr_id: WorkRequestId,
        rm: &mut dyn RemoteMrWriteAccess,
    ) -> Self {
        let mut sr = Self::new(lms, wr_id);
        sr.inner.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
        sr.inner.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        sr.inner.wr.rdma.remote_addr = rm.addr().cast();
        sr.inner.wr.rdma.rkey = rm.rkey();
        sr
    }
}

impl AsRef<ibv_send_wr> for SendWr {
    fn as_ref(&self) -> &ibv_send_wr {
        &self.inner
    }
}

impl<'lm> AsMut<ibv_send_wr> for SendWr {
    fn as_mut(&mut self) -> &mut ibv_send_wr {
        &mut self.inner
    }
}

/// Receive work request
#[repr(C)]
pub(crate) struct RecvWr {
    /// internal `ibv_recv_wr`
    inner: ibv_recv_wr,
    /// `ibv_sge`s to store the data
    sges: Vec<ibv_sge>,
}

impl RecvWr {
    /// create a new recv work request
    fn new(lms: &[&mut dyn LocalMrWriteAccess], wr_id: WorkRequestId) -> Self {
        assert!(!lms.is_empty());
        let mut sges: Vec<ibv_sge> = lms
            .iter()
            .map(|lm| {
                let lm: &dyn LocalMrWriteAccess = lm.borrow();
                Into::<ibv_sge>::into(lm)
            })
            .collect();
        let mut inner = unsafe { std::mem::zeroed::<ibv_recv_wr>() };
        inner.next = std::ptr::null_mut();
        inner.wr_id = wr_id.into();
        inner.sg_list = sges.as_mut_ptr();
        inner.num_sge = sges.len().cast();
        Self { inner, sges }
    }

    /// create a new recv work request for "recv"
    pub(crate) fn new_recv(lms: &[&mut dyn LocalMrWriteAccess], wr_id: WorkRequestId) -> Self {
        Self::new(lms, wr_id)
    }
}

impl AsRef<ibv_recv_wr> for RecvWr {
    fn as_ref(&self) -> &ibv_recv_wr {
        &self.inner
    }
}

impl AsMut<ibv_recv_wr> for RecvWr {
    fn as_mut(&mut self) -> &mut ibv_recv_wr {
        &mut self.inner
    }
}

impl From<&dyn LocalMrReadAccess> for ibv_sge {
    #[inline]
    fn from(lmr: &dyn LocalMrReadAccess) -> Self {
        Self {
            addr: lmr.addr().cast(),
            length: lmr.length().cast(),
            lkey: lmr.lkey(),
        }
    }
}

impl From<&dyn LocalMrWriteAccess> for ibv_sge {
    #[inline]
    fn from(lmr: &dyn LocalMrWriteAccess) -> Self {
        Self {
            addr: lmr.addr().cast(),
            length: lmr.length().cast(),
            lkey: lmr.lkey(),
        }
    }
}
