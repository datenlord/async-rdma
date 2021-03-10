use std::env;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::os::unix::ffi::OsStringExt;

#[cfg(target_os = "linux")]
extern "C" {
    fn server_main(argc: c_int, argv: *const *const c_char) -> c_int;
    fn client_main(argc: c_int, argv: *const *const c_char) -> c_int;
}

fn main() {
    let v: Vec<CString> = env::args_os()
        .map(|a| CString::new(a.into_vec()).expect("failed to convert os-string to c-string"))
        .collect();
    let a: Vec<*const c_char> = v.iter().map(|c| c.as_ptr()).collect();
    unsafe {
        if a.len() > 2 {
            client_main(a.len() as c_int, a.as_ptr());
        } else {
            server_main(a.len() as c_int, a.as_ptr());
        }
    }
}