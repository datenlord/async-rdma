use crate::{
    completion_queue::CompletionQueue, event_channel::EventChannel, gid::Gid,
    protection_domain::ProtectionDomain,
};
use clippy_utilities::Cast;
use rdma_sys::{
    ibv_close_device, ibv_context, ibv_free_device_list, ibv_get_device_list, ibv_get_device_name,
    ibv_open_device, ibv_port_attr, ibv_query_gid,
};
use std::{ffi::CStr, fmt::Debug, io, ops::Sub, ptr::NonNull, sync::Arc};
use tracing::{error, warn};

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
    pub(crate) fn open(dev_name: Option<&str>, port_num: u8, gid_index: usize) -> io::Result<Self> {
        let mut num_devs: i32 = 0;
        let dev_list_ptr = unsafe { ibv_get_device_list(&mut num_devs) };
        if dev_list_ptr.is_null() {
            return Err(io::Error::last_os_error());
        }
        let dev_list = unsafe { std::slice::from_raw_parts(dev_list_ptr, num_devs.cast()) };
        let dev = if let Some(dev_name_inner) = dev_name {
            dev_list
                .iter()
                .find(|iter_dev| -> bool {
                    let name = unsafe { ibv_get_device_name(**iter_dev) };
                    assert!(!name.is_null());
                    let name = unsafe { CStr::from_ptr(name) }.to_str();
                    if name.is_err() {
                        warn!("Device name {:?} is not valid", name);
                        return false;
                    }

                    // We've checked the name is not error
                    #[allow(clippy::unwrap_used)]
                    let name = name.unwrap();
                    dev_name_inner.eq(name)
                })
                .ok_or(io::ErrorKind::NotFound)?
        } else {
            dev_list.get(0).ok_or(io::ErrorKind::NotFound)?
        };
        let inner_ctx =
            NonNull::new(unsafe { ibv_open_device(*dev) }).ok_or_else(io::Error::last_os_error)?;
        unsafe { ibv_free_device_list(dev_list_ptr) };
        let mut gid = Gid::default();
        let mut errno =
            unsafe { ibv_query_gid(inner_ctx.as_ptr(), port_num, gid_index.cast(), gid.as_mut()) };
        if errno != 0_i32 {
            error!(
                "open_device, err info : {:?}",
                io::Error::from_raw_os_error(0_i32.sub(errno))
            );
            return Err(io::Error::from_raw_os_error(0_i32.sub(errno)));
        }
        let mut inner_port_attr = unsafe { std::mem::zeroed() };
        errno = unsafe { rdma_sys::___ibv_query_port(inner_ctx.as_ptr(), 1, &mut inner_port_attr) };
        if errno != 0_i32 {
            error!(
                "open_device, err info : {:?}",
                io::Error::from_raw_os_error(0_i32.sub(errno))
            );
            return Err(io::Error::from_raw_os_error(0_i32.sub(errno)));
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
    ) -> io::Result<CompletionQueue> {
        CompletionQueue::create(self, cq_size, event_channel)
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
        let errno = unsafe { ibv_close_device(self.as_ptr()) };
        assert_eq!(errno, 0_i32);
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
