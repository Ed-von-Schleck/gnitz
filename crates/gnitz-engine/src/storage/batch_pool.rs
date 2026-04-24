//! Thread-local buffer pool for `Vec<u8>` recycling.
//!
//! Batch uses only 2 heap allocations (data + blob). This pool
//! recycles those buffers so steady-state batch operations allocate nothing.
//!
//! No schema keying needed — any buffer works for any batch.
//! One worker = one OS thread = no contention.

use std::cell::Cell;

use super::batch::Batch;

const MAX_POOLED: usize = 64;
/// Buffers larger than this are dropped on recycle rather than pooled.
/// Matches `HUGEPAGE_THRESHOLD` in batch.rs: data buffers already bypass the
/// pool on allocation above this size; blob buffers have no such bypass, so
/// this cap prevents a large-string outlier from trapping memory permanently.
const MAX_RECYCLE_CAPACITY: usize = 2 * 1024 * 1024;

thread_local! {
    static BUF_POOL: Cell<Vec<Vec<u8>>> = const { Cell::new(Vec::new()) };
}

/// Take a buffer from the pool (retains previous capacity).
/// Returns `Vec::new()` (0 capacity) when the pool is empty.
pub(crate) fn acquire_buf() -> Vec<u8> {
    BUF_POOL.try_with(|p| {
        let mut pool = p.take();
        let buf = pool.pop().unwrap_or_default();
        p.set(pool);
        buf
    }).unwrap_or_default()
}

/// Return a buffer to the pool (clears content, retains capacity).
/// Zero-capacity buffers (moved-from state) and buffers larger than
/// `MAX_RECYCLE_CAPACITY` are dropped instead of pooled.
/// Uses `try_with` to handle thread-local teardown gracefully.
pub(crate) fn recycle_buf(mut buf: Vec<u8>) {
    let cap = buf.capacity();
    if cap == 0 || cap > MAX_RECYCLE_CAPACITY { return; }
    buf.clear();
    let _ = BUF_POOL.try_with(|p| {
        let mut pool = p.take();
        if pool.len() < MAX_POOLED {
            pool.push(buf);
        }
        p.set(pool);
    });
}

/// Recycle an Batch's data and blob buffers back to the pool.
pub(crate) fn recycle(batch: Batch) {
    let (data, blob) = batch.into_buffers();
    recycle_buf(data);
    recycle_buf(blob);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain_pool() {
        while acquire_buf().capacity() > 0 {}
    }

    #[test]
    fn oversized_buf_not_pooled() {
        drain_pool();
        recycle_buf(vec![0u8; MAX_RECYCLE_CAPACITY + 1]);
        assert_eq!(acquire_buf().capacity(), 0, "oversized buffer must not enter pool");
    }

    #[test]
    fn buf_at_limit_is_pooled() {
        drain_pool();
        let v: Vec<u8> = Vec::with_capacity(MAX_RECYCLE_CAPACITY);
        recycle_buf(v);
        assert!(acquire_buf().capacity() >= MAX_RECYCLE_CAPACITY, "buffer at limit must be pooled");
    }

    #[test]
    fn zero_cap_buf_not_pooled() {
        drain_pool();
        recycle_buf(Vec::new());
        assert_eq!(acquire_buf().capacity(), 0, "zero-capacity buffer must not enter pool");
    }

    #[test]
    fn empty_pool_returns_zero_cap() {
        drain_pool();
        assert_eq!(acquire_buf().capacity(), 0, "empty pool must return zero-capacity Vec");
    }

    #[test]
    fn round_trip_clears_and_retains_capacity() {
        drain_pool();
        let cap = 4096usize;
        let mut v: Vec<u8> = Vec::with_capacity(cap);
        v.extend_from_slice(&[1u8; 64]);
        recycle_buf(v);
        let got = acquire_buf();
        assert!(got.capacity() >= cap);
        assert_eq!(got.len(), 0, "recycled buffer must be cleared");
    }

    #[test]
    fn pool_capped_at_max_pooled() {
        drain_pool();
        for _ in 0..MAX_POOLED + 4 {
            recycle_buf(vec![0u8; 128]);
        }
        let mut count = 0usize;
        while acquire_buf().capacity() > 0 { count += 1; }
        assert_eq!(count, MAX_POOLED);
    }
}
