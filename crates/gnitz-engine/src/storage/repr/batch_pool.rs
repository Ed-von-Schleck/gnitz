//! Thread-local buffer pool for `Vec<u8>` recycling, over the generic
//! [`tls_pool`] every `repr` object pool is built from.
//!
//! Batch uses only 2 heap allocations (data + blob). This pool
//! recycles those buffers so steady-state batch operations allocate nothing.
//!
//! No schema keying needed — any buffer works for any batch.
//! One worker = one OS thread = no contention.

use std::cell::Cell;

/// The take/pop/set half of every thread-local object pool in `repr` — the byte
/// arenas here and the blob-relocation caches in `merge`. Each pool keeps its own
/// admission rule (what is too big to retain); this owns only the plumbing, so
/// the pool-length cap and the thread-teardown handling are stated once.
pub(crate) mod tls_pool {
    use std::cell::Cell;
    use std::thread::LocalKey;

    /// Entries retained per pool. Bounds idle memory when a burst returns more
    /// items than steady state needs.
    pub(crate) const MAX_POOLED: usize = 64;

    /// Take an item, or `T::default()` when the pool is empty or the
    /// thread-local is being torn down.
    pub(crate) fn acquire<T: Default>(pool: &'static LocalKey<Cell<Vec<T>>>) -> T {
        pool.try_with(|p| {
            let mut items = p.take();
            let item = items.pop().unwrap_or_default();
            p.set(items);
            item
        })
        .unwrap_or_default()
    }

    /// Return an item, dropping it if the pool is already full or the
    /// thread-local is gone. Callers apply their own size admission first.
    pub(crate) fn recycle<T>(pool: &'static LocalKey<Cell<Vec<T>>>, item: T) {
        let _ = pool.try_with(|p| {
            let mut items = p.take();
            if items.len() < MAX_POOLED {
                items.push(item);
            }
            p.set(items);
        });
    }
}

/// Buffers larger than this are dropped on recycle rather than pooled. Data
/// buffers already bypass the pool on allocation at this size (they want
/// `MADV_HUGEPAGE`, which a pooled buffer would not carry); blob buffers have no
/// such bypass, so this cap prevents a large-string outlier from trapping memory
/// permanently.
const MAX_RECYCLE_CAPACITY: usize = super::batch::HUGEPAGE_THRESHOLD;

thread_local! {
    static BUF_POOL: Cell<Vec<Vec<u8>>> = const { Cell::new(Vec::new()) };
}

/// Take a buffer from the pool (retains previous capacity).
/// Returns `Vec::new()` (0 capacity) when the pool is empty.
pub(crate) fn acquire_buf() -> Vec<u8> {
    tls_pool::acquire(&BUF_POOL)
}

/// Return a buffer to the pool (clears content, retains capacity).
/// Zero-capacity buffers (moved-from state) and buffers larger than
/// `MAX_RECYCLE_CAPACITY` are dropped instead of pooled.
pub(crate) fn recycle_buf(mut buf: Vec<u8>) {
    let cap = buf.capacity();
    if cap == 0 || cap > MAX_RECYCLE_CAPACITY {
        return;
    }
    buf.clear();
    tls_pool::recycle(&BUF_POOL, buf);
}

/// A pooled send buffer that returns itself to the pool on drop.
pub(crate) struct PooledSendBuf(pub(crate) Vec<u8>);

impl Drop for PooledSendBuf {
    fn drop(&mut self) {
        recycle_buf(std::mem::take(&mut self.0));
    }
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
        assert!(
            acquire_buf().capacity() >= MAX_RECYCLE_CAPACITY,
            "buffer at limit must be pooled"
        );
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
        for _ in 0..tls_pool::MAX_POOLED + 4 {
            recycle_buf(vec![0u8; 128]);
        }
        let mut count = 0usize;
        while acquire_buf().capacity() > 0 {
            count += 1;
        }
        assert_eq!(count, tls_pool::MAX_POOLED);
    }
}
