//! Utility function to manipulate pointers and handle error numbers

/// Scoped closure
pub struct ScopedCB<F: FnOnce() + Copy> {
    ///
    closure: F,
}

impl<F> ScopedCB<F>
where
    F: FnOnce() + Copy,
{
    ///
    pub fn new(closure: F) -> Self {
        Self { closure }
    }
}

impl<F: FnOnce() + Copy> Drop for ScopedCB<F> {
    fn drop(&mut self) {
        (self.closure)()
    }
}

/// Copy slice with different size
#[allow(clippy::indexing_slicing)]
pub fn copy_slice<T: Copy>(dst: &mut [T], src: &[T]) {
    use std::cmp::Ordering;
    match dst.len().cmp(&src.len()) {
        Ordering::Equal => dst.copy_from_slice(src),
        Ordering::Greater => dst[0..src.len()].copy_from_slice(src),
        Ordering::Less => dst.copy_from_slice(&src[0..dst.len()]),
    }
}

// /// Cast any type to byte array
// #[inline]
// pub fn cast_to_bytes<T: Sized>(p: &T) -> &[u8] {
//     unsafe { std::slice::from_raw_parts(const_ptr_cast(p), std::mem::size_of::<T>()) }
// }

/// Cast a pointer to usize
#[allow(clippy::as_conversions)]
#[inline]
pub fn ptr_to_usize<T: ?Sized>(ptr: *const T) -> usize {
    ptr as *const u8 as usize
}

/// Cast a mut pointer to usize
#[allow(clippy::as_conversions)]
#[inline]
pub fn mut_ptr_to_usize<T: ?Sized>(ptr: *mut T) -> usize {
    let u = ptr as *mut u8 as usize;
    println!("mut_ptr_to_usize(): ptr={:?}, usize={:x}={}", ptr, u, u);
    u
}

/// Cast a const pointer to another type
#[allow(clippy::as_conversions)]
#[inline]
pub const fn const_ptr_cast<T: ?Sized, U>(ptr: *const T) -> *const U {
    ptr as *const u8 as *const U
}

/// Cast a mut pointer to another type
#[allow(clippy::as_conversions)]
#[inline]
pub const fn mut_ptr_cast<T: ?Sized, U>(ptr: *mut T) -> *mut U {
    ptr as *mut u8 as *mut U
}

/// Cast a const pointer to another mut type
#[allow(clippy::as_conversions)]
#[inline]
pub const fn const_ptr_cast_mut<T: ?Sized, U>(ptr: *const T) -> *mut U {
    ptr as *const u8 as *mut u8 as *mut U
}

// /// Cast a usize to a cosnt pointer
// #[allow(clippy::as_conversions)]
// #[inline]
// pub const unsafe fn usize_to_ptr<T: Sized>(addr: usize) -> *const T {
//     addr as *const T
// }

/// Cast a usize to a mut pointer
#[allow(clippy::as_conversions)]
#[inline]
pub const unsafe fn usize_to_mut_ptr<T: Sized>(addr: usize) -> *mut T {
    addr as *mut T
}

// ///
// pub fn vec_ptr_to_ref_vec<T: Sized>(vec_ptr: *mut *mut T, vec_size: usize) -> Vec<*mut T> {
//     (0..vec_size)
//         .map(|i| unsafe { (*vec_ptr).add(i) })
//         .collect::<Vec<_>>()
// }

///
// pub fn ref_vec_to_vec_ptr<T: ?Sized>(ref_vec: &[&T]) -> *mut *mut T {
//     if ref_vec.is_empty() {
//         new_mut_null_ptr::<*mut T>()
//     } else {
//         const_ptr_cast_mut(utilities::cast_to_ptr(
//             ref_vec
//                 .get(0)
//                 .unwrap_or_else(|| panic!("ref_vec_to_vec_ptr failed")),
//         ))
//     }
// }

// pub const fn new_mut_null_ptr<T: Sized>() -> *mut T {
//     std::ptr::null_mut::<T>()
// }

///
pub fn is_null_mut_ptr<T: Sized>(ptr: *mut T) -> bool {
    ptr == std::ptr::null_mut::<T>()
}

/// Get last error if any
pub fn get_last_error() -> nix::Error {
    let last_err = nix::Error::last();
    println!("last error is: {}", last_err);
    last_err
}

/// Convert C style error number to Rust style error result
pub fn check_errno(ret: std::os::raw::c_int) -> Result<(), nix::Error> {
    if ret == 0 {
        Ok(())
    } else {
        Err(get_last_error())
    }
}
