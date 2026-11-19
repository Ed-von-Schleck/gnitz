//! Thread-local object pools: take and recycle for a pool of `T`, and the
//! `Vec<u8>` pool built on them. Each thread has its own pools; nothing is
//! shared or locked.

use std::cell::Cell;
use std::collections::VecDeque;
use std::thread::LocalKey;

/// Items retained per pool.
const MAX_POOLED: usize = 64;
/// The largest item a pool retains, in bytes.
pub const MAX_POOLED_BYTES: usize = 2 * 1024 * 1024;

/// The most recently returned item `fits` accepts. `None` when there is none,
/// or the thread-local is being torn down.
pub fn take<T>(pool: &'static LocalKey<Cell<VecDeque<T>>>, fits: impl Fn(&T) -> bool) -> Option<T> {
    pool.try_with(|p| {
        let mut items = p.take();
        let item = items.iter().rposition(fits).and_then(|i| items.remove(i));
        put_back(p, items);
        item
    })
    .ok()
    .flatten()
}

/// Return `item`, which holds `bytes` of heap. Dropped when that is zero or
/// above `MAX_POOLED_BYTES`; a full pool drops its oldest item instead.
pub fn recycle<T>(pool: &'static LocalKey<Cell<VecDeque<T>>>, item: T, bytes: usize) {
    if bytes == 0 || bytes > MAX_POOLED_BYTES {
        return;
    }
    let _ = pool.try_with(|p| {
        let mut items = p.take();
        if items.len() == MAX_POOLED {
            items.pop_front();
        }
        items.push_back(item);
        put_back(p, items);
    });
}

/// Store `items` in `p`, which holds the empty deque [`Cell::take`] left there.
/// That deque owns no allocation, so it is forgotten instead of dropped.
#[inline(always)]
fn put_back<T>(p: &Cell<VecDeque<T>>, items: VecDeque<T>) {
    let left = p.replace(items);
    debug_assert_eq!(left.capacity(), 0, "a pool refilled while its deque was out");
    std::mem::forget(left);
}

/// The bytes `v` holds allocated.
#[inline]
pub fn vec_bytes<T>(v: &Vec<T>) -> usize {
    v.capacity() * size_of::<T>()
}

/// Run `f` over the thread's scratch list in `slot`, emptied. A list grown past
/// `MAX_POOLED_BYTES` goes back to the allocator. The list is taken out of its
/// slot while `f` runs, so a nested call gets a fresh one.
pub fn with_scratch<T, R>(slot: &'static LocalKey<Cell<Vec<T>>>, f: impl FnOnce(&mut Vec<T>) -> R) -> R {
    let mut v = slot.try_with(Cell::take).unwrap_or_default();
    v.clear();
    let r = f(&mut v);
    if vec_bytes(&v) <= MAX_POOLED_BYTES {
        v.clear();
        let _ = slot.try_with(|c| c.set(v));
    }
    r
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
            take(&BUF_POOL, |b| b.capacity() >= size && is_tight(b.capacity(), size))
                .unwrap_or_else(|| Vec::with_capacity(size)),
        )
    }

    /// Empty, with capacity exactly `size`: a pooled buffer of that capacity,
    /// else a fresh one (`Vec::<u8>::with_capacity` allocates exactly `size`).
    /// A zero `size` holds no buffer, since the pool keeps none of capacity 0.
    pub(super) fn exact(size: usize) -> Self {
        PooledBuf(take(&BUF_POOL, |b| b.capacity() == size).unwrap_or_else(|| Vec::with_capacity(size)))
    }

    /// [`Self::exact`] with `len == size`, the bytes uninitialized.
    ///
    /// # Safety
    /// The caller writes each byte before reading it.
    #[allow(clippy::uninit_vec)]
    pub(super) unsafe fn uninit_exact(size: usize) -> Self {
        let mut buf = Self::exact(size);
        // SAFETY: `capacity == size`.
        unsafe { buf.0.set_len(size) };
        buf
    }

    /// [`Self::with_capacity`] with `len == size`, the bytes uninitialized.
    ///
    /// # Safety
    /// The caller writes each byte before reading it.
    #[allow(clippy::uninit_vec)]
    pub unsafe fn uninit(size: usize) -> Self {
        let mut buf = Self::with_capacity(size);
        // SAFETY: `capacity >= size`.
        unsafe { buf.0.set_len(size) };
        buf
    }

    /// Room for `additional` more bytes. A buffer that has to grow moves, bytes
    /// verbatim, into a pooled one of at least twice its capacity, so repeated
    /// reserves stay amortised, and the outgrown buffer returns to the pool. Past
    /// `MAX_POOLED_BYTES` it grows by [`Vec::reserve`].
    pub(super) fn reserve_pooled(&mut self, additional: usize) {
        if self.0.capacity() - self.0.len() >= additional {
            return;
        }
        let need = (self.0.len() + additional).max(2 * self.0.capacity());
        if need > MAX_POOLED_BYTES {
            self.0.reserve(additional);
            return;
        }
        let mut grown = PooledBuf::with_capacity(need);
        grown.extend_from_slice(&self.0);
        *self = grown;
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
        recycle(&BUF_POOL, buf, cap);
    }
}

#[cfg(test)]
pub(super) fn drain_pool() -> VecDeque<Vec<u8>> {
    BUF_POOL.with(Cell::take)
}

#[cfg(test)]
#[path = "tests/pool.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/pool.rs"]
mod bench;
