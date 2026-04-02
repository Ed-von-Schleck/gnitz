use std::os::raw::{c_int, c_uint, c_void};
use std::panic;

use crate::transport::{OutputSlots, Transport};
use crate::uring::IoUringRing;

type TransportHandle = Transport<IoUringRing>;

/// Create a new transport with the given io_uring ring size.
/// Returns an opaque pointer, or null on failure.
#[no_mangle]
pub extern "C" fn transport_create(ring_size: c_int) -> *mut c_void {
    let result = panic::catch_unwind(|| {
        let ring = IoUringRing::new(ring_size as u32).ok()?;
        let transport = TransportHandle::new(ring, ring_size as usize);
        let boxed = Box::new(transport);
        Some(Box::into_raw(boxed) as *mut c_void)
    });
    match result {
        Ok(Some(ptr)) => ptr,
        _ => std::ptr::null_mut(),
    }
}

/// Destroy a transport, closing all connections.
#[no_mangle]
pub extern "C" fn transport_destroy(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    let _ = panic::catch_unwind(|| {
        let _ = unsafe { Box::from_raw(ptr as *mut TransportHandle) };
    });
}

/// Start accepting connections on `server_fd`.
#[no_mangle]
pub extern "C" fn transport_accept(ptr: *mut c_void, server_fd: c_int) {
    if ptr.is_null() {
        return;
    }
    let _ = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let transport = unsafe { &mut *(ptr as *mut TransportHandle) };
        transport.accept_conn(server_fd);
    }));
}

/// Poll for complete messages. Returns the number of messages written
/// to the output arrays, or -1 on error.
#[no_mangle]
pub extern "C" fn transport_poll(
    ptr: *mut c_void,
    timeout_ms: c_int,
    out_fds: *mut c_int,
    out_ptrs: *mut *mut c_void,
    out_lens: *mut c_uint,
    out_max: c_int,
) -> c_int {
    if ptr.is_null() || out_max <= 0 {
        return -1;
    }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let transport = unsafe { &mut *(ptr as *mut TransportHandle) };
        let max = out_max as usize;
        let fds = unsafe { std::slice::from_raw_parts_mut(out_fds, max) };
        let ptrs = unsafe {
            std::slice::from_raw_parts_mut(out_ptrs as *mut *mut u8, max)
        };
        let lens = unsafe { std::slice::from_raw_parts_mut(out_lens, max) };
        let mut out = OutputSlots { fds, ptrs, lens };
        transport.poll(timeout_ms, &mut out)
    }));
    match result {
        Ok(n) => n,
        Err(_) => -1,
    }
}

/// Queue a response for sending. Returns 0 on success, -1 if the
/// connection is closing or unknown.
#[no_mangle]
pub extern "C" fn transport_send(
    ptr: *mut c_void,
    fd: c_int,
    data: *const c_void,
    len: c_uint,
) -> c_int {
    if ptr.is_null() {
        return -1;
    }
    let result = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let transport = unsafe { &mut *(ptr as *mut TransportHandle) };
        transport.send(fd, data as *const u8, len as usize)
    }));
    match result {
        Ok(rc) => rc,
        Err(_) => -1,
    }
}

/// Free a recv buffer previously returned by `transport_poll`.
#[no_mangle]
pub extern "C" fn transport_free_recv(_ptr: *mut c_void, recv_ptr: *mut c_void) {
    // _ptr (Transport handle) is ignored — free is a leaf call.
    TransportHandle::free_recv(recv_ptr as *mut u8);
}

/// Request connection close. Actual close is deferred until all
/// outstanding SQEs have produced CQEs.
#[no_mangle]
pub extern "C" fn transport_close_fd(ptr: *mut c_void, fd: c_int) {
    if ptr.is_null() {
        return;
    }
    let _ = panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let transport = unsafe { &mut *(ptr as *mut TransportHandle) };
        transport.close_fd(fd);
    }));
}
