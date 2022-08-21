use crate::device::DeviceList;
use crate::error_utilities::{log_last_os_err, log_ret, log_ret_last_os_err_with_note};
use crate::{
    completion_queue::CompletionQueue, event_channel::EventChannel, gid::Gid,
    protection_domain::ProtectionDomain,
};
use clippy_utilities::Cast;
use rdma_sys::{
    ibv_close_device, ibv_context, ibv_gid, ibv_open_device, ibv_port_attr, ibv_query_gid,
};
use std::mem::MaybeUninit;
use std::{fmt::Debug, io, ptr::NonNull, sync::Arc};

/// RDMA device context
pub(crate) struct Context {
    /// internal ibv context
    inner_ctx: NonNull<ibv_context>,
    /// ibv port attribute
    inner_port_attr: ibv_port_attr,
    /// Gid
    gid: Gid,
}

impl Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("inner_ctx", &self.inner_ctx)
            .field("gid", &self.gid)
            .finish()
    }
}

impl Context {
    /// Get the internal context pointer
    pub(crate) const fn as_ptr(&self) -> *mut ibv_context {
        self.inner_ctx.as_ptr()
    }

    /// Create a new context based on the provided device name, port number and gid index
    ///
    /// On failure of `ibv_get_device_list`, errno indicates the failure reason:
    ///
    /// `EPERM`     Permission denied.
    ///
    /// `ENOMEM`    Insufficient memory to complete the operation.
    ///
    /// `ENOSYS`    No kernel support for RDMA.
    pub(crate) fn open(dev_name: Option<&str>, port_num: u8, gid_index: usize) -> io::Result<Self> {
        let dev_list = log_ret(
            DeviceList::available(),
            "This is a basic verb that shouldn't fail, check if the module ib_uverbs is loaded.",
        )?;

        let dev = match dev_name {
            Some(name) => dev_list.iter().find(|&d| d.name() == name),
            None => dev_list.get(0),
        }
        .ok_or(io::ErrorKind::NotFound)?;

        // SAFETY: ffi
        // 1. `dev` is valid now.
        // 2. `*mut ibv_context` does not associate with the lifetime of `*mut ibv_device`.
        let inner_ctx = NonNull::new(unsafe { ibv_open_device(dev.ffi_ptr()) })
            .ok_or_else(|| log_ret_last_os_err_with_note("ibv_open_device failed"))?;

        drop(dev_list);

        let gid = {
            let mut gid = MaybeUninit::<ibv_gid>::uninit();
            let gid_index = gid_index.cast();
            // SAFETY: ffi
            let errno =
                unsafe { ibv_query_gid(inner_ctx.as_ptr(), port_num, gid_index, gid.as_mut_ptr()) };
            if errno != 0_i32 {
                return Err(log_ret_last_os_err_with_note("ibv_query_gid failed"));
            }
            // SAFETY: ffi init
            Gid::from(unsafe { gid.assume_init() })
        };

        // SAFETY: POD FFI type
        let mut inner_port_attr = unsafe { std::mem::zeroed() };
        let errno = unsafe {
            rdma_sys::___ibv_query_port(inner_ctx.as_ptr(), port_num, &mut inner_port_attr)
        };
        if errno != 0_i32 {
            return Err(log_ret_last_os_err_with_note("ibv_query_port failed"));
        }
        Ok(Context {
            inner_ctx,
            inner_port_attr,
            gid,
        })
    }

    /// Create an event channel
    pub(crate) fn create_event_channel(self: &Arc<Self>) -> io::Result<EventChannel> {
        EventChannel::new(Arc::<Self>::clone(self))
    }

    /// Create a completion queue
    pub(crate) fn create_completion_queue(
        &self,
        cq_size: u32,
        event_channel: EventChannel,
        max_cqe: i32,
    ) -> io::Result<CompletionQueue> {
        CompletionQueue::create(self, cq_size, event_channel, max_cqe)
    }

    /// Create a protection domain
    pub(crate) fn create_protection_domain(self: &Arc<Self>) -> io::Result<ProtectionDomain> {
        ProtectionDomain::create(self)
    }
    /// Get port Lid
    pub(crate) fn get_gid(&self) -> Gid {
        self.gid
    }

    /// Get port Lid
    pub(crate) fn get_lid(&self) -> u16 {
        self.inner_port_attr.lid
    }

    /// Get the port MTU
    pub(crate) fn get_active_mtu(&self) -> u32 {
        self.inner_port_attr.active_mtu
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        // SAFETY: ffi
        // TODO: check safety
        let errno = unsafe { ibv_close_device(self.as_ptr()) };
        if errno != 0_i32 {
            log_last_os_err();
        }
    }
}

unsafe impl Send for Context {}

unsafe impl Sync for Context {}

#[cfg(test)]
#[allow(unused, clippy::unwrap_used)]
mod tests {
    use crate::Context;
    use std::process::Command;

    fn rdma_dev() -> String {
        let out = Command::new("sh")
            .arg("-c")
            .arg("rdma link")
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .clone()
            .split(' ')
            .nth(1)
            .unwrap()
            .to_owned()
            .split('/')
            .next()
            .unwrap()
            .to_owned()
    }

    #[test]
    fn test1() {
        let ctx = Context::open(Some(&rdma_dev()), 1, 0).unwrap();
    }

    #[test]
    fn test2() {
        let ctx = Context::open(Some(""), 1, 0).err().unwrap();
    }

    #[test]
    fn test3() {
        let ctx = Context::open(None, 1, 0).unwrap();
    }
}
