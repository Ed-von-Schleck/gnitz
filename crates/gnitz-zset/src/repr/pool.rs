//! Thread-local object pools: take and recycle for a pool of `T`, and the
//! scratch list lent to one kernel call. Each thread has its own pools; nothing
//! is shared or locked.

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

/// Whether `capacity` holds at most twice `need`.
pub(super) fn is_tight(capacity: usize, need: usize) -> bool {
    capacity <= need.saturating_mul(2)
}

#[cfg(test)]
#[path = "tests/pool.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/pool.rs"]
mod bench;
