use crate::error_utilities::last_error;

use rdma_sys::__be64;
use rdma_sys::ibv_device;
use rdma_sys::{ibv_free_device_list, ibv_get_device_list};
use rdma_sys::{ibv_get_device_guid, ibv_get_device_name};

use std::ffi::CStr;
use std::io;
use std::ops::Deref;
use std::os::raw::c_int;
use std::ptr::NonNull;
use std::{fmt, mem, slice};

use numeric_cast::NumericCast;
use scopeguard::guard_on_unwind;

/// An array of RDMA devices.
pub struct DeviceList {
    /// base address
    arr: NonNull<Device>,
    /// array length
    len: usize,
}

/// SAFETY: owned array
unsafe impl Send for DeviceList {}
/// SAFETY: owned array
unsafe impl Sync for DeviceList {}

/// A RDMA device
#[allow(missing_copy_implementations)] // This type can not copy
#[repr(transparent)]
pub struct Device(NonNull<ibv_device>);

/// SAFETY: owned type
unsafe impl Send for Device {}
/// SAFETY: owned type
unsafe impl Sync for Device {}

/// A RDMA device guid
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct Guid(__be64);

impl DeviceList {
    /// Returns `*mut *mut ibv_device`
    fn ffi_ptr(&self) -> *mut *mut ibv_device {
        self.arr.as_ptr().cast()
    }

    /// Returns available rdma devices
    #[inline]
    pub fn available() -> io::Result<Self> {
        // SAFETY: ffi
        unsafe {
            let mut num_devices: c_int = 0;
            let arr = ibv_get_device_list(&mut num_devices);
            if arr.is_null() {
                return Err(last_error());
            }

            // SAFETY: repr(transparent)
            let arr: NonNull<Device> = NonNull::new_unchecked(arr.cast());

            let _guard = guard_on_unwind((), |()| ibv_free_device_list(arr.as_ptr().cast()));

            let len: usize = num_devices.numeric_cast();

            if mem::size_of::<c_int>() >= mem::size_of::<usize>() {
                let total_size = len.saturating_mul(mem::size_of::<*mut ibv_device>());
                assert!(total_size < usize::MAX.wrapping_div(2));
            }

            Ok(Self { arr, len })
        }
    }

    /// Returns the slice of devices
    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[Device] {
        // SAFETY: guaranteed by `DeviceList::available`
        unsafe { slice::from_raw_parts(self.arr.as_ptr(), self.len) }
    }
}

impl Drop for DeviceList {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: ffi
        unsafe { ibv_free_device_list(self.ffi_ptr()) }
    }
}

impl Deref for DeviceList {
    type Target = [Device];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl fmt::Debug for DeviceList {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <[Device] as fmt::Debug>::fmt(self, f)
    }
}

impl Device {
    /// Returns `*mut ibv_device`
    pub(crate) fn ffi_ptr(&self) -> *mut ibv_device {
        self.0.as_ptr()
    }

    /// Returns kernel device name
    #[inline]
    #[must_use]
    pub fn c_name(&self) -> &CStr {
        // SAFETY: ffi
        unsafe { CStr::from_ptr(ibv_get_device_name(self.ffi_ptr())) }
    }

    /// Returns kernel device name
    ///
    /// # Panics
    /// + if the device name is not a valid utf8 string
    #[inline]
    #[must_use]
    pub fn name(&self) -> &str {
        #[allow(clippy::expect_used)]
        self.c_name().to_str().expect("non-utf8 device name")
    }

    /// Returns device’s node GUID
    #[inline]
    #[must_use]
    pub fn guid(&self) -> Guid {
        // SAFETY: ffi
        unsafe { Guid(ibv_get_device_guid(self.ffi_ptr())) }
    }
}

impl fmt::Debug for Device {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = self.name();
        let guid = self.guid();
        f.debug_struct("Device")
            .field("name", &name)
            .field("guid", &guid)
            .finish()
    }
}

impl Guid {
    /// Constructs a Guid from network bytes.
    #[inline]
    #[must_use]
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_ne_bytes(bytes))
    }

    /// Returns the bytes of GUID in network byte order.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 8] {
        // SAFETY: transparent be64
        unsafe { &*<*const _>::cast(self) }
    }
}

impl fmt::Debug for Guid {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Guid({:x})", self)
    }
}

/// Encodes a guid to a hex string and process it
fn guid_to_hex<R>(guid: Guid, uppercase: bool, f: impl FnOnce(&str) -> R) -> R {
    let src: &[u8; 8] = guid.as_bytes();
    let mut buf: [u8; 16] = [0; 16];
    // SAFETY: The buf is two times of src, which is required by hex::encode_to_slice.
    // Therefore, the unwrap_unchecked on hex::encode_to_slice is safe.
    // After the hex encoding, the bytes in buf are valid UTF-8, because hex::encode_to_slice
    // only produces bytes in the ASCII range (0x00 - 0x7F), which are valid UTF-8.
    // Therefore, the unwrap_unchecked on std::str::from_utf8 is also safe.
    let ans = unsafe {
        hex::encode_to_slice(src, &mut buf).unwrap_unchecked();
        if uppercase {
            std::str::from_utf8(&buf).unwrap_unchecked().to_uppercase()
        } else {
            std::str::from_utf8(&buf).unwrap_unchecked().to_lowercase()
        }
    };
    f(&ans)
}

impl fmt::LowerHex for Guid {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        guid_to_hex(*self, false, |s| <str as fmt::Display>::fmt(s, f))
    }
}

impl fmt::UpperHex for Guid {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        guid_to_hex(*self, true, |s| <str as fmt::Display>::fmt(s, f))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use const_str::hex_bytes as hex;

    #[test]
    fn guid_fmt() {
        const GUID_HEX: &str = "26418cfffe021df9";
        let guid = Guid::from_bytes(hex!(GUID_HEX));

        let debug = format!("{:?}", guid);
        let lower_hex = format!("{:x}", guid);
        let upper_hex = format!("{:X}", guid);

        assert_eq!(debug, format!("Guid({GUID_HEX})"));
        assert_eq!(lower_hex, GUID_HEX);
        assert_eq!(upper_hex, GUID_HEX.to_ascii_uppercase());
    }

    #[test]
    fn marker() {
        fn require_send_sync<T: Send + Sync>() {}

        require_send_sync::<Device>();
        require_send_sync::<DeviceList>();
        require_send_sync::<Guid>();
    }
}
