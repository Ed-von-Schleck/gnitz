//! Arena provisioning for batch buffers, and the thread-local `Vec<u8>` pool
//! behind it — over the generic `tls_pool` every `repr` object pool is built
//! from.
//!
//! A batch has 2 heap allocations (data + blob); both are taken here
//! (`acquire_arena`) and returned here (`recycle_buf`), so one threshold decides
//! both halves and steady state allocates nothing. Any buffer works for any
//! batch, and one worker is one OS thread, so no keying and no contention.

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

/// The one boundary between "serve from the buffer pool" and "allocate fresh",
/// and equally the pool's only bound on bytes (`MAX_POOLED` bounds its count) —
/// what stops one outsized buffer from trapping memory for the process's
/// lifetime. Data buffers bypass the pool on allocation at this size, so on
/// recycle the cap only ever fires for them redundantly; blob buffers have no
/// such bypass, and for those it is what prevents a large-string outlier from
/// trapping memory permanently.
const POOL_BYPASS_BYTES: usize = 2 * 1024 * 1024;

thread_local! {
    static BUF_POOL: Cell<Vec<Vec<u8>>> = const { Cell::new(Vec::new()) };
}

/// Take a buffer from the pool (retains previous capacity).
/// Returns `Vec::new()` (0 capacity) when the pool is empty.
pub fn acquire_buf() -> Vec<u8> {
    tls_pool::acquire(&BUF_POOL)
}

/// Return a buffer to the pool (clears content, retains capacity).
/// Zero-capacity buffers (moved-from state) and buffers larger than
/// `POOL_BYPASS_BYTES` are dropped instead of pooled.
pub(crate) fn recycle_buf(mut buf: Vec<u8>) {
    let cap = buf.capacity();
    if cap == 0 || cap > POOL_BYPASS_BYTES {
        return;
    }
    buf.clear();
    tls_pool::recycle(&BUF_POOL, buf);
}

/// How [`acquire_arena`] initializes the returned buffer.
pub(super) enum Fill {
    /// `len == size`, contents uninitialized — the caller writes every live
    /// byte before any read. See [`debug_poison`] for the debug-build tripwire.
    Uninit,
    /// `len == 0`, `capacity >= size` — for growable arenas filled by append
    /// (blob heaps).
    Reserve,
}

/// Debug-build poison for bytes a caller has not written yet — a value that is
/// not a plausible zero, weight, or PK.
///
/// The batch invariant is *every counted row writes every region* ([`Fill::Uninit`],
/// `Batch::with_capacity`, `super::merge::DirectWriter`), so nothing may read
/// an unwritten byte. But a fresh OS allocation arrives demand-zero, so a caller
/// that wrongly relies on a zero passes its tests and only misbehaves in
/// production once the buffer pool starts recycling. Poisoning every byte the
/// batch exposes-but-has-not-written makes that mistake deterministic instead.
///
/// Applied wherever batch bytes become writable-but-unwritten: [`acquire_arena`]'s
/// `Uninit` arms, `Batch::reserve_rows`'s in-place grow (whose `set_len` would
/// otherwise leave the freshly exposed tail un-poisoned — the steady-state path
/// for a recycled batch being refilled), and `Batch::clear`'s dropped rows.
#[inline]
pub(super) fn debug_poison(bytes: &mut [u8]) {
    if cfg!(debug_assertions) {
        bytes.fill(0xA5);
    }
}

/// The one arena-provisioning path for batch data/blob buffers.
///
/// Sizes `>= POOL_BYPASS_BYTES` allocate fresh: the pool retains nothing above
/// that size, so probing it would pop the LIFO head, find it undersized, and
/// discard a hot buffer for nothing. (Exactly at the boundary a pooled buffer
/// could have served; the bypass gives up that one size to stay one compare.)
///
/// Below the threshold the pool is tried first; an undersized pooled buffer is
/// evicted rather than grown in place — `Vec::reserve` on a too-small buffer
/// copies the old bytes forward before the tail is written, slower than a fresh
/// allocation, and eviction converges the pool to larger sizes.
#[allow(clippy::uninit_vec)] // `Fill::Uninit` is the documented contract: callers write every live byte
pub(super) fn acquire_arena(size: usize, fill: Fill) -> Vec<u8> {
    #[inline]
    fn fresh(size: usize, fill: &Fill) -> Vec<u8> {
        match fill {
            Fill::Uninit => {
                let mut v = Vec::with_capacity(size);
                // SAFETY: u8 needs no init; callers write every `count`-bounded
                // live byte before it is read (the `Uninit` contract above).
                unsafe { v.set_len(size) };
                debug_poison(&mut v);
                v
            }
            Fill::Reserve => Vec::with_capacity(size),
        }
    }

    // Nothing asked for, nothing taken: a pooled buffer here would be retained
    // by whatever parks the batch, at whatever capacity the pool head carried.
    if size == 0 {
        return Vec::new();
    }
    if size >= POOL_BYPASS_BYTES {
        return fresh(size, &fill);
    }
    let mut buf = acquire_buf();
    if buf.capacity() < size {
        drop(buf); // evict the undersized buffer; pool converges to larger sizes
        return fresh(size, &fill);
    }
    // SAFETY: capacity checked above; `Uninit` is the documented
    // write-before-read contract.
    match fill {
        Fill::Uninit => {
            unsafe { buf.set_len(size) };
            debug_poison(&mut buf);
        }
        Fill::Reserve => {}
    }
    buf
}

/// A pooled send buffer that returns itself to the pool on drop.
pub struct PooledSendBuf(pub Vec<u8>);

impl Drop for PooledSendBuf {
    fn drop(&mut self) {
        recycle_buf(std::mem::take(&mut self.0));
    }
}

#[cfg(test)]
#[path = "tests/batch_pool.rs"]
mod tests;
