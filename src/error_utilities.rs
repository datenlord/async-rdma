use std::io;

use tracing::error;

/// Get the last os error, log with note and return the error
pub(crate) fn log_ret_last_os_err_with_note(note: &str) -> io::Error {
    let err = io::Error::last_os_error();
    if note.is_empty() {
        error!("OS error {:?}", err);
    } else {
        error!("OS error {:?}. Note: {}", err, note);
    }
    err
}

/// Get the last os error, log and return the error
pub(crate) fn log_ret_last_os_err() -> io::Error {
    log_ret_last_os_err_with_note("")
}

/// Get the last os error and just log it
pub(crate) fn log_last_os_err() {
    #[allow(clippy::pedantic)] // some errors occur in `drop` methord is not necessary to panic
    let _ = log_ret_last_os_err_with_note("");
}

/// Returns an error representing the last OS error which occurred.
pub(crate) fn last_error() -> io::Error {
    io::Error::last_os_error()
}

/// Logs a result and return it back
pub(crate) fn log_ret<T>(ret: io::Result<T>, note: &str) -> io::Result<T> {
    if let Err(ref err) = ret {
        if note.is_empty() {
            error!("OS error {:?}", err);
        } else {
            error!("OS error {:?}. Note: {}", err, note);
        }
    }
    ret
}
