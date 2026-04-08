//! Thread-local buffer pool for `Vec<u8>` recycling.
//!
//! OwnedBatch uses only 2 heap allocations (data + blob). This pool
//! recycles those buffers so steady-state batch operations allocate nothing.
//!
//! No schema keying needed — any buffer works for any batch.
//! One worker = one OS thread = no contention.

use std::cell::RefCell;

use super::memtable::OwnedBatch;

const MAX_POOLED: usize = 64;

thread_local! {
    static BUF_POOL: RefCell<Vec<Vec<u8>>> = RefCell::new(Vec::new());
}

/// Take a buffer from the pool (retains previous capacity).
pub(crate) fn acquire_buf() -> Option<Vec<u8>> {
    BUF_POOL.try_with(|p| p.borrow_mut().pop()).ok().flatten()
}

/// Return a buffer to the pool (clears content, retains capacity).
/// Zero-capacity buffers (moved-from state) are ignored.
/// Uses `try_with` to handle thread-local teardown gracefully.
pub(crate) fn recycle_buf(mut buf: Vec<u8>) {
    if buf.capacity() == 0 { return; }
    buf.clear();
    let _ = BUF_POOL.try_with(|p| {
        let mut pool = p.borrow_mut();
        if pool.len() < MAX_POOLED {
            pool.push(buf);
        }
    });
}

/// Recycle an OwnedBatch's data and blob buffers back to the pool.
pub(crate) fn recycle(batch: OwnedBatch) {
    let (data, blob) = batch.into_buffers();
    recycle_buf(data);
    recycle_buf(blob);
}
