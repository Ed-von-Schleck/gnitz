use std::panic;
use std::slice;

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

/// Initialize the Rust logging subsystem.
/// level: 0=QUIET, 1=NORMAL, 2=DEBUG. tag: process tag bytes (e.g. "M", "W0").
#[no_mangle]
pub extern "C" fn gnitz_log_init(level: u32, tag_ptr: *const u8, tag_len: u32) {
    let tag = if tag_ptr.is_null() || tag_len == 0 {
        &[] as &[u8]
    } else {
        unsafe { slice::from_raw_parts(tag_ptr, tag_len.min(3) as usize) }
    };
    crate::log::init(level, tag);
}

// ---------------------------------------------------------------------------
// Server bootstrap (bootstrap.rs)
// ---------------------------------------------------------------------------

/// Single entry point for the entire server bootstrap.
///
/// Encapsulates catalog open, IPC allocation, fork, SAL recovery, and executor
/// run. After this call, main.py is just arg parsing + one FFI call.
///
/// Returns 0 on clean exit, non-zero on error.
#[no_mangle]
pub extern "C" fn gnitz_server_main(
    data_dir_ptr: *const u8,
    data_dir_len: u32,
    socket_path_ptr: *const u8,
    socket_path_len: u32,
    num_workers: u32,
    log_level: u32,
) -> i32 {
    let result = panic::catch_unwind(|| {
        let data_dir = if data_dir_ptr.is_null() {
            return -1;
        } else {
            let bytes = unsafe { slice::from_raw_parts(data_dir_ptr, data_dir_len as usize) };
            match std::str::from_utf8(bytes) {
                Ok(s) => s,
                Err(_) => return -1,
            }
        };
        let socket_path = if socket_path_ptr.is_null() {
            return -1;
        } else {
            let bytes = unsafe {
                slice::from_raw_parts(socket_path_ptr, socket_path_len as usize)
            };
            match std::str::from_utf8(bytes) {
                Ok(s) => s,
                Err(_) => return -1,
            }
        };
        crate::bootstrap::server_main(data_dir, socket_path, num_workers, log_level)
    });
    match result {
        Ok(rc) => rc,
        Err(_) => {
            let msg = b"PANIC in server_main\n";
            unsafe { libc::write(2, msg.as_ptr() as *const libc::c_void, msg.len()); }
            -1
        }
    }
}
