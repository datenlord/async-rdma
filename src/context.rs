use crate::{
    completion_queue::CompletionQueue, event_channel::EventChannel, gid::Gid,
    protection_domain::ProtectionDomain,
};
use rdma_sys::{
    ibv_close_device, ibv_context, ibv_free_device_list, ibv_get_device_list, ibv_get_device_name,
    ibv_open_device, ibv_port_attr, ibv_query_gid,
};
use std::{ffi::CStr, io, ptr::NonNull, sync::Arc};

pub struct Context {
    pub inner_ctx: NonNull<ibv_context>,
    pub inner_port_attr: ibv_port_attr,
    pub gid: Gid,
}

impl Context {
    pub(crate) fn as_ptr(&self) -> *mut ibv_context {
        self.inner_ctx.as_ptr()
    }

    pub fn open(dev_name: Option<&str>) -> io::Result<Self> {
        let mut num_devs: i32 = 0;
        let dev_list_ptr = unsafe { ibv_get_device_list(&mut num_devs as *mut _) };
        if dev_list_ptr.is_null() {
            return Err(io::Error::last_os_error());
        }
        let dev_list = unsafe { std::slice::from_raw_parts(dev_list_ptr, num_devs as usize) };
        let dev = if let Some(dev_name) = dev_name {
            dev_list
                .iter()
                .find(|iter_dev| {
                    let name = unsafe { ibv_get_device_name(**iter_dev) };
                    assert!(!name.is_null());
                    let name = unsafe { CStr::from_ptr(name) }.to_str().unwrap();
                    dev_name.eq(name)
                })
                .ok_or(io::ErrorKind::NotFound)?
        } else {
            dev_list.get(0).ok_or(io::ErrorKind::NotFound)?
        };
        let inner_ctx =
            NonNull::new(unsafe { ibv_open_device(*dev) }).ok_or_else(io::Error::last_os_error)?;
        unsafe { ibv_free_device_list(dev_list_ptr) };
        let mut gid = Gid::default();
        let errno = unsafe { ibv_query_gid(inner_ctx.as_ptr(), 1, 1, gid.as_mut()) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        let mut inner_port_attr = unsafe { std::mem::zeroed() };
        let errno =
            unsafe { rdma_sys::___ibv_query_port(inner_ctx.as_ptr(), 1, &mut inner_port_attr) };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        Ok(Context {
            inner_ctx,
            inner_port_attr,
            gid,
        })
    }

    pub fn create_event_channel(self: &Arc<Self>) -> io::Result<EventChannel> {
        EventChannel::new(self.clone())
    }

    pub fn create_completion_queue(
        &self,
        cq_size: u32,
        event_channel: Option<EventChannel>,
    ) -> io::Result<CompletionQueue> {
        CompletionQueue::create(self, cq_size, event_channel)
    }

    pub fn create_protection_domain(self: &Arc<Self>) -> io::Result<ProtectionDomain> {
        ProtectionDomain::create(self)
    }

    pub fn get_lid(&self) -> u16 {
        self.inner_port_attr.lid
    }

    pub fn get_active_mtu(&self) -> u32 {
        self.inner_port_attr.active_mtu
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        let errno = unsafe { ibv_close_device(self.as_ptr()) };
        assert_eq!(errno, 0);
    }
}

unsafe impl Send for Context {}

unsafe impl Sync for Context {}

#[cfg(test)]
#[allow(unused)]
mod tests {
    use crate::*;
    use std::process::Command;

    fn rdma_dev() -> String {
        let out = Command::new("sh")
            .arg("-c")
            .arg("rdma link")
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .to_string()
            .split(' ')
            .nth(1)
            .unwrap()
            .to_string()
            .split('/')
            .next()
            .unwrap()
            .to_string()
    }

    #[test]
    fn test1() {
        let ctx = Context::open(Some(&rdma_dev())).unwrap();
    }

    #[test]
    fn test2() {
        let ctx = Context::open(Some("")).err().unwrap();
    }

    #[test]
    fn test3() {
        let ctx = Context::open(None).unwrap();
    }
}
