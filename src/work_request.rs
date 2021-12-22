use crate::{
    completion_queue::WorkRequestId,
    memory_region::{LocalMemoryRegion, RemoteMemoryRegion},
};
use rdma_sys::{ibv_recv_wr, ibv_send_flags, ibv_send_wr, ibv_sge, ibv_wr_opcode};

#[repr(C)]
pub struct SendWr {
    pub inner: ibv_send_wr,
    sges: Vec<ibv_sge>,
}

impl SendWr {
    fn new(lms: Vec<&LocalMemoryRegion>, wr_id: WorkRequestId) -> Self {
        assert!(!lms.is_empty());
        let mut sges: Vec<ibv_sge> = lms.iter().map(|lm| (*lm).into()).collect();
        let mut inner = unsafe { std::mem::zeroed::<ibv_send_wr>() };
        inner.next = std::ptr::null_mut();
        inner.wr_id = wr_id.into();
        inner.sg_list = sges.as_mut_ptr();
        inner.num_sge = sges.len() as _;
        Self { inner, sges }
    }

    pub fn new_send(lms: Vec<&LocalMemoryRegion>, wr_id: WorkRequestId) -> Self {
        let mut sr = Self::new(lms, wr_id);
        sr.inner.opcode = ibv_wr_opcode::IBV_WR_SEND;
        sr.inner.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        sr
    }

    pub fn new_read(
        lms: Vec<&LocalMemoryRegion>,
        wr_id: WorkRequestId,
        rm: &RemoteMemoryRegion,
    ) -> Self {
        let mut sr = Self::new(lms, wr_id);
        sr.inner.opcode = ibv_wr_opcode::IBV_WR_RDMA_READ;
        sr.inner.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        sr.inner.wr.rdma.remote_addr = rm.as_ptr() as u64;
        sr.inner.wr.rdma.rkey = rm.rkey();
        sr
    }

    pub fn new_write(
        lms: Vec<&LocalMemoryRegion>,
        wr_id: WorkRequestId,
        rm: &RemoteMemoryRegion,
    ) -> Self {
        let mut sr = Self::new(lms, wr_id);
        sr.inner.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
        sr.inner.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        sr.inner.wr.rdma.remote_addr = rm.as_ptr() as u64;
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

#[repr(C)]
pub struct RecvWr {
    inner: ibv_recv_wr,
    sges: Vec<ibv_sge>,
}

impl RecvWr {
    fn new(lms: Vec<&LocalMemoryRegion>, wr_id: WorkRequestId) -> Self {
        assert!(!lms.is_empty());
        let mut sges: Vec<ibv_sge> = lms.iter().map(|lm| (*lm).into()).collect();
        let mut inner = unsafe { std::mem::zeroed::<ibv_recv_wr>() };
        inner.next = std::ptr::null_mut();
        inner.wr_id = wr_id.into();
        inner.sg_list = sges.as_mut_ptr();
        inner.num_sge = sges.len() as _;
        Self { inner, sges }
    }

    pub fn new_recv(lms: Vec<&LocalMemoryRegion>, wr_id: WorkRequestId) -> Self {
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

impl From<&LocalMemoryRegion> for ibv_sge {
    fn from(lmr: &LocalMemoryRegion) -> Self {
        Self {
            addr: lmr.as_ptr() as u64,
            length: lmr.length() as u32,
            lkey: lmr.lkey(),
        }
    }
}
