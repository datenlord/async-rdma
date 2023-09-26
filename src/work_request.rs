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
    fn new<LR>(lms: &[&LR], wr_id: WorkRequestId) -> Self
    where
        LR: LocalMrReadAccess,
    {
        assert!(!lms.is_empty());
        let mut sges: Vec<ibv_sge> = lms
            .iter()
            .map(|lm| {
                let lm: &LR = lm.borrow();
                sge_from_mr(lm)
            })
            .collect();
        // SAFETY: POD FFI type
        let mut inner = unsafe { std::mem::zeroed::<ibv_send_wr>() };
        inner.next = std::ptr::null_mut();
        inner.wr_id = wr_id.into();
        inner.sg_list = sges.as_mut_ptr();
        inner.num_sge = sges.len().cast();
        Self { inner, sges }
    }

    /// create a new send work requet for "send"
    pub(crate) fn new_send<LR>(lms: &[&LR], wr_id: WorkRequestId, imm: Option<u32>) -> Self
    where
        LR: LocalMrReadAccess,
    {
        let mut sr = Self::new(lms, wr_id);
        sr.inner.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        match imm {
            None => sr.inner.opcode = ibv_wr_opcode::IBV_WR_SEND,
            Some(imm_num) => {
                sr.inner.opcode = ibv_wr_opcode::IBV_WR_SEND_WITH_IMM;
                sr.inner.imm_data_invalidated_rkey_union.imm_data = imm_num;
            }
        }
        sr
    }

    /// create a new send work requet for "read"
    #[allow(clippy::as_conversions)] // Convert pointer to usize is safe for later ibv lib use
    pub(crate) fn new_read<LW, RR>(lms: &[&mut LW], wr_id: WorkRequestId, rm: &RR) -> Self
    where
        LW: LocalMrWriteAccess,
        RR: RemoteMrReadAccess,
    {
        assert!(!lms.is_empty());
        let mut sges: Vec<ibv_sge> = lms
            .iter()
            .map(|lm| {
                let lm: &LW = lm.borrow();
                sge_from_mr(lm)
            })
            .collect();
        // SAFETY: POD FFI type
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
    pub(crate) fn new_write<LR, RW>(
        lms: &[&LR],
        wr_id: WorkRequestId,
        rm: &mut RW,
        imm: Option<u32>,
    ) -> Self
    where
        LR: LocalMrReadAccess,
        RW: RemoteMrWriteAccess,
    {
        let mut sr = Self::new(lms, wr_id);
        sr.inner.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        sr.inner.wr.rdma.remote_addr = rm.addr().cast();
        sr.inner.wr.rdma.rkey = rm.rkey();
        match imm {
            Some(imm_num) => {
                sr.inner.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM;
                sr.inner.imm_data_invalidated_rkey_union.imm_data = imm_num;
                sr.inner.wr.rdma.remote_addr = rm.addr().cast();
                sr.inner.wr.rdma.rkey = rm.rkey().cast();
            }
            None => sr.inner.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE,
        }
        sr
    }

    /// create a new send work requet for "cas"
    pub(crate) fn new_cas<LR, RW>(
        old_value: u64,
        new_value: u64,
        lmr: &LR,
        rm: &mut RW,
        wr_id: WorkRequestId,
    ) -> Self
    where
        LR: LocalMrReadAccess,
        RW: RemoteMrWriteAccess,
    {
        let mut sr = Self::new(&[lmr], wr_id);
        sr.inner.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        sr.inner.opcode = ibv_wr_opcode::IBV_WR_ATOMIC_CMP_AND_SWP;

        sr.inner.wr.atomic.remote_addr = rm.addr().cast();
        sr.inner.wr.atomic.rkey = rm.rkey();
        sr.inner.wr.atomic.compare_add = old_value;
        sr.inner.wr.atomic.swap = new_value;
        sr
    }
}

impl AsRef<ibv_send_wr> for SendWr {
    fn as_ref(&self) -> &ibv_send_wr {
        &self.inner
    }
}

impl AsMut<ibv_send_wr> for SendWr {
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
    fn new<LW>(lms: &[&mut LW], wr_id: WorkRequestId) -> Self
    where
        LW: LocalMrWriteAccess,
    {
        assert!(!lms.is_empty());
        let mut sges: Vec<ibv_sge> = lms
            .iter()
            .map(|lm| {
                let lm: &LW = lm.borrow();
                sge_from_mr(lm)
            })
            .collect();
        // SAFETY: POD FFI type
        let mut inner = unsafe { std::mem::zeroed::<ibv_recv_wr>() };
        inner.next = std::ptr::null_mut();
        inner.wr_id = wr_id.into();
        inner.sg_list = sges.as_mut_ptr();
        inner.num_sge = sges.len().cast();
        Self { inner, sges }
    }

    /// create a new recv work request for "recv"
    pub(crate) fn new_recv<LW>(lms: &[&mut LW], wr_id: WorkRequestId) -> Self
    where
        LW: LocalMrWriteAccess,
    {
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

/// From lcoal mr to sge
///
/// Not impl From<&LR> for `ibv_sge` because implementing a foreign trait is only possible
/// if at least one of the types for which it is implemented is local only traits defined
/// in the current crate can be implemented for a type parameter rustc E0210.
///
/// If we impl<LR> From<&LR> for `ibv_sge` we will get an error: type parameter `LR` must
/// be used as the type parameter for some local type (e.g., `MyStruct<LR>`).
fn sge_from_mr<LR>(lmr: &LR) -> ibv_sge
where
    LR: LocalMrReadAccess,
{
    ibv_sge {
        addr: lmr.addr().cast(),
        length: lmr.length().cast(),
        // FIXME: turn to `unsafe` if use this fn as a pub API or `lkry` can be changed.
        // SAFETY: no date race if just use this methord during wr processing. But in fact, this fn
        // itself does not guarantee safety, it should depend on the context in which it is used.
        lkey: unsafe { lmr.lkey_unchecked() },
    }
}
