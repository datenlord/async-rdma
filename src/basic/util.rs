//! Utility function to manipulate pointers and handle error numbers

use utilities::OverflowArithmetic;

/// Cast a pointer to usize
#[allow(clippy::as_conversions)]
#[inline]
pub fn ptr_to_usize<T: ?Sized>(ptr: *const T) -> usize {
    ptr as *const u8 as usize
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

///
#[allow(clippy::missing_const_for_fn)]
pub fn is_null_mut_ptr<T: Sized>(ptr: *mut T) -> bool {
    // is not stable as const function
    ptr.is_null()
}

/// Get last error if any
pub fn get_last_error() -> nix::Error {
    let last_err = nix::Error::last();
    println!("last error is: {}", last_err);
    last_err
}

///
pub fn copy_to_buf_pad(dst: &mut [u8], src: &str) {
    let src_str = if dst.len() <= src.len() {
        format!(
            "{}\0",
            src.get(0..(dst.len().overflow_sub(1)))
                .unwrap_or_else(|| panic!("failed to slice src: {}", src))
        )
    } else {
        let padding = std::iter::repeat("\0")
            .take(dst.len().overflow_sub(src.len()))
            .collect::<String>();
        format!("{}{}", src, padding)
    };
    debug_assert_eq!(dst.len(), src_str.len(), "src str size not match dst");
    dst.copy_from_slice(src_str.as_bytes());
}
