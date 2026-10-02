//! Thread-local object pools: `tls_pool`'s take and recycle, and the `Vec<u8>`
//! pool built on them. Each thread has its own pools; nothing is shared or
//! locked.

use std::cell::Cell;
use std::collections::VecDeque;

/// Take and recycle for a thread-local pool of `T`.
pub(crate) mod tls_pool {
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::thread::LocalKey;

    /// Items retained per pool.
    pub(crate) const MAX_POOLED: usize = 64;
    /// The largest item a pool retains, in bytes.
    pub(crate) const MAX_POOLED_BYTES: usize = 2 * 1024 * 1024;

    /// The most recently returned item `fits` accepts. `None` when there is none,
    /// or the thread-local is being torn down.
    pub(crate) fn take<T>(pool: &'static LocalKey<Cell<VecDeque<T>>>, fits: impl Fn(&T) -> bool) -> Option<T> {
        pool.try_with(|p| {
            let mut items = p.take();
            let item = items.iter().rposition(fits).and_then(|i| items.remove(i));
            p.set(items);
            item
        })
        .ok()
        .flatten()
    }

    /// Return `item`, which holds `bytes` of heap. Dropped when that is zero or
    /// above `MAX_POOLED_BYTES`; a full pool drops its oldest item instead.
    pub(crate) fn recycle<T>(pool: &'static LocalKey<Cell<VecDeque<T>>>, item: T, bytes: usize) {
        if bytes == 0 || bytes > MAX_POOLED_BYTES {
            return;
        }
        let _ = pool.try_with(|p| {
            let mut items = p.take();
            if items.len() == MAX_POOLED {
                items.pop_front();
            }
            items.push_back(item);
            p.set(items);
        });
    }
}

thread_local! {
    static BUF_POOL: Cell<VecDeque<Vec<u8>>> = const { Cell::new(VecDeque::new()) };
}

/// Whether `capacity` holds at most twice `need`.
pub(super) fn is_tight(capacity: usize, need: usize) -> bool {
    capacity <= need.saturating_mul(2)
}

/// A byte buffer from the thread's pool, returned to it on drop.
#[derive(Default)]
pub struct PooledBuf(pub Vec<u8>);

impl PooledBuf {
    /// Empty, with room for `size` bytes: a pooled buffer that [`is_tight`] for
    /// them, else a fresh one. A zero `size` holds no buffer.
    pub fn with_capacity(size: usize) -> Self {
        if size == 0 {
            return PooledBuf(Vec::new());
        }
        PooledBuf(
            tls_pool::take(&BUF_POOL, |b| b.capacity() >= size && is_tight(b.capacity(), size))
                .unwrap_or_else(|| Vec::with_capacity(size)),
        )
    }

    /// [`Self::with_capacity`] with `len == size`, the bytes uninitialized.
    ///
    /// # Safety
    /// The caller writes each byte before reading it.
    #[allow(clippy::uninit_vec)]
    pub(super) unsafe fn uninit(size: usize) -> Self {
        let mut buf = Self::with_capacity(size);
        // SAFETY: `capacity >= size`.
        unsafe { buf.0.set_len(size) };
        buf
    }
}

impl std::ops::Deref for PooledBuf {
    type Target = Vec<u8>;
    fn deref(&self) -> &Vec<u8> {
        &self.0
    }
}

impl std::ops::DerefMut for PooledBuf {
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        let mut buf = std::mem::take(&mut self.0);
        buf.clear();
        let cap = buf.capacity();
        tls_pool::recycle(&BUF_POOL, buf, cap);
    }
}

#[cfg(test)]
pub(super) fn drain_pool() -> VecDeque<Vec<u8>> {
    BUF_POOL.with(Cell::take)
}

#[cfg(test)]
#[path = "tests/batch_pool.rs"]
mod tests;
