use crate::device::DeviceList;
use crate::error_utilities::{log_last_os_err, log_ret, log_ret_last_os_err_with_note};
use crate::{
    completion_queue::CompletionQueue, cq_event_channel::EventChannel, gid::Gid,
    protection_domain::ProtectionDomain,
};
use clippy_utilities::Cast;
use getset::Getters;
use libc::{fcntl, F_GETFL, F_SETFL, O_NONBLOCK};
use rdma_sys::{
    ibv_close_device, ibv_context, ibv_device_attr, ibv_gid, ibv_open_device, ibv_port_attr,
    ibv_query_gid,
};
use std::mem::MaybeUninit;
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::RawFd;
use std::{fmt::Debug, io, ptr::NonNull, sync::Arc};
use tokio::io::unix::AsyncFd;
/// RDMA device context
#[derive(Getters)]
pub(crate) struct Context {
    /// internal ibv context
    inner_ctx: NonNull<ibv_context>,
    /// ibv port attribute
    inner_port_attr: ibv_port_attr,
    /// Gid
    #[get = "pub"]
    gid: Gid,
    /// Device attributes
    #[get = "pub"]
    dev_attr: ibv_device_attr,
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
            // choose the most recently added rdma device as default
            None => dev_list.get(dev_list.len().saturating_sub(1)),
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
            if unsafe { ibv_query_gid(inner_ctx.as_ptr(), port_num, gid_index, gid.as_mut_ptr()) }
                != 0_i32
            {
                return Err(log_ret_last_os_err_with_note("ibv_query_gid failed"));
            }
            // SAFETY: ffi init
            Gid::from(unsafe { gid.assume_init() })
        };

        // SAFETY: POD FFI type
        let mut inner_port_attr = unsafe { std::mem::zeroed() };
        if unsafe {
            rdma_sys::___ibv_query_port(inner_ctx.as_ptr(), port_num, &mut inner_port_attr)
        } != 0_i32
        {
            return Err(log_ret_last_os_err_with_note("ibv_query_port failed"));
        }

        let mut dev_attr = MaybeUninit::<ibv_device_attr>::uninit();
        // SAFETY: ffi
        if unsafe { rdma_sys::ibv_query_device(inner_ctx.as_ptr(), dev_attr.as_mut_ptr()) } != 0_i32
        {
            return Err(log_ret_last_os_err_with_note("ibv_query_device failed"));
        }

        Ok(Context {
            inner_ctx,
            inner_port_attr,
            gid,
            // SAFETY: ffi init
            dev_attr: unsafe { dev_attr.assume_init() },
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
    pub(crate) fn get_lid(&self) -> u16 {
        self.inner_port_attr.lid
    }

    /// Get the port MTU
    #[allow(dead_code)] // TODO: provide related pub API to choose mtu
    pub(crate) fn get_active_mtu(&self) -> u32 {
        self.inner_port_attr.active_mtu
    }

    /// Get the async event handle for this ctx.
    pub(crate) fn get_async_fd(&self) -> io::Result<AsyncFd<RawFd>> {
        // SAFETY: async_fd will be initialized by rdma driver
        let fd = unsafe { (*self.as_ptr()).async_fd };
        // SAFETY: ffi
        let flags = unsafe { fcntl(fd, F_GETFL) };
        // SAFETY: ffi
        let ret = unsafe { fcntl(fd, F_SETFL, flags | O_NONBLOCK) };
        if ret < 0_i32 {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Error, failed to change file descriptor of async event queue",
            ))
        } else {
            AsyncFd::new(fd.as_raw_fd())
        }
    }
}

/// Check if the device capability meets the requirement of `attr_val`.
pub(crate) fn check_dev_cap<T: PartialOrd + std::fmt::Display>(
    attr_val: &T,
    dev_cap: &T,
    attr_name: &str,
) -> io::Result<()> {
    if attr_val > dev_cap {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "The value of {} is: {}, which exceeds the hardware capability: {}",
                attr_name, attr_val, dev_cap
            ),
        ))
    } else {
        Ok(())
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
