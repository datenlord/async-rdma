use crate::{
    context::Context,
    cq_event_listener::CQEventListener,
    error_utilities::{log_last_os_err, log_ret_last_os_err},
    queue_pair::{QueuePair, QueuePairBuilder},
    QueuePairInitAttrBuilder, QueuePairState,
};
use parking_lot::RwLock;
use rdma_sys::{ibv_alloc_pd, ibv_dealloc_pd, ibv_pd};
use std::{io, ptr::NonNull, sync::Arc};

/// Protection Domain Wrapper
#[derive(Debug)]
pub(crate) struct ProtectionDomain {
    /// The device context
    pub(crate) ctx: Arc<Context>,
    /// Internal `ibv_pd` pointer
    pub(crate) inner_pd: NonNull<ibv_pd>,
}

impl ProtectionDomain {
    /// Get pointer to the internal `ibv_pd`
    pub(crate) fn as_ptr(&self) -> *mut ibv_pd {
        self.inner_pd.as_ptr()
    }

    /// Create a protection domain
    pub(crate) fn create(ctx: &Arc<Context>) -> io::Result<Self> {
        // SAFETY: ffi
        // TODO: check safety
        let inner_pd =
            NonNull::new(unsafe { ibv_alloc_pd(ctx.as_ptr()) }).ok_or_else(log_ret_last_os_err)?;
        Ok(Self {
            ctx: Arc::<Context>::clone(ctx),
            inner_pd,
        })
    }

    /// Create a queue pair builder
    pub(crate) fn create_queue_pair_builder(self: &Arc<Self>) -> QueuePairBuilder {
        QueuePairBuilder::default().pd(Arc::clone(self)).clone()
    }

    /// Create a queue pair
    pub(crate) fn create_qp(
        self: &Arc<Self>,
        cq_event_listener: Arc<CQEventListener>,
        qp_init_attr: QueuePairInitAttrBuilder,
        port_num: u8,
        init_qp: bool,
    ) -> io::Result<QueuePair> {
        let mut attr = qp_init_attr.build()?;
        attr.qp_cap_mut().check_dev_qp_cap(&self.ctx)?;

        // SAFETY: ffi
        let inner_qp =
            NonNull::new(unsafe { rdma_sys::ibv_create_qp(self.as_ptr(), &mut attr.into()) })
                .ok_or_else(log_ret_last_os_err)?;

        let mut qp = self
            .create_queue_pair_builder()
            .cq_event_listener(cq_event_listener)
            .inner_qp(inner_qp)
            .cur_state(Arc::new(RwLock::new(QueuePairState::Unknown)))
            .build()?;

        if init_qp {
            qp.modify_to_init(*attr.access(), port_num, *attr.pkey_index())?;
        }

        Ok(qp)
    }
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_dealloc_pd(self.as_ptr()) };
        if errno != 0_i32 {
            log_last_os_err();
        }
    }
}

unsafe impl Send for ProtectionDomain {}

unsafe impl Sync for ProtectionDomain {}
