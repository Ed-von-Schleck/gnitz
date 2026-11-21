//! The process's pool of large frame buffers: the payloads the [`Deframer`]
//! receives into and the outbound queues frames are encoded into.
//!
//! glibc serves an allocation past its mmap threshold (at most 32 MiB) with a
//! mapping of its own and unmaps it on free, and trims a freed heap top past its
//! trim threshold, so a frame buffer that malloc served would come back as fresh
//! pages on the next large request, each faulted in again. A buffer of at least
//! [`POOL_MIN_BYTES`] given back here keeps its pages mapped for the next frame
//! that fits it. A fresh buffer is allocated at its request's [`size_class`],
//! so it also serves a later request somewhat larger, faulting in only the pages
//! past those the earlier one touched; the slack is never touched until then.
//!
//! The pool is one per process, behind a lock: a reply's buffer is often freed
//! on a thread other than the one that received it. It holds buffers only while
//! a [`Deframer`] — one per open connection, on either end — is alive; the last
//! one dropped empties it, and a buffer given back while none is alive is freed.
//!
//! [`Deframer`]: crate::Deframer

use std::collections::TryReserveError;
use std::sync::{Mutex, PoisonError};

use crate::MAX_FRAME_PAYLOAD;

/// The smallest buffer the pool takes back or hands out.
pub const POOL_MIN_BYTES: usize = 1 << 20;

/// Capacity the pool holds while no one uses it: two of the largest frame
/// payload, the frames of a two-frame reply or a request's frame and its
/// reply's. Taking a buffer back that would pass it drops the oldest held first.
pub const POOL_BYTES: usize = 2 * MAX_FRAME_PAYLOAD;

struct Pool {
    /// Oldest first.
    bufs: Vec<Vec<u8>>,
    /// The capacities of `bufs`, summed.
    bytes: usize,
    /// The live [`Use`]s; the pool holds nothing while there are none.
    users: usize,
}

impl Pool {
    const fn new() -> Self {
        Pool { bufs: Vec::new(), bytes: 0, users: 0 }
    }

    /// The smallest held buffer that [`fits`] `need`.
    fn take(&mut self, need: usize) -> Option<Vec<u8>> {
        let (i, _) = (self.bufs.iter().enumerate())
            .filter(|(_, b)| fits(b.capacity(), need))
            .min_by_key(|(_, b)| b.capacity())?;
        let buf = self.bufs.remove(i);
        self.bytes -= buf.capacity();
        Some(buf)
    }

    /// Add `buf`, emptied, of a capacity in `POOL_MIN_BYTES..=POOL_BYTES`,
    /// returning what to free: the oldest held past [`POOL_BYTES`], or `buf`
    /// itself while the pool has no users.
    fn hold(&mut self, mut buf: Vec<u8>) -> Vec<Vec<u8>> {
        if self.users == 0 {
            return vec![buf];
        }
        let cap = buf.capacity();
        buf.clear();
        let mut evict = 0;
        while self.bytes + cap > POOL_BYTES {
            self.bytes -= self.bufs[evict].capacity();
            evict += 1;
        }
        let evicted = self.bufs.drain(..evict).collect();
        self.bufs.push(buf);
        self.bytes += cap;
        evicted
    }
}

static POOL: Mutex<Pool> = Mutex::new(Pool::new());

fn pool() -> std::sync::MutexGuard<'static, Pool> {
    POOL.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether `capacity` serves a `need`-byte request without holding more than
/// twice it.
fn fits(capacity: usize, need: usize) -> bool {
    capacity >= need && capacity / 2 <= need
}

/// The capacity a fresh buffer for `len` bytes gets: from [`POOL_MIN_BYTES`]
/// on, `len` rounded up past the next eighth of its power of two, so it has
/// room for at least a sixteenth more; never more than [`MAX_FRAME_PAYLOAD`]
/// unless `len` is. Below the pool's floor, `len` itself.
pub fn size_class(len: usize) -> usize {
    if len < POOL_MIN_BYTES {
        return len;
    }
    let step = 1 << (len.ilog2() - 3);
    ((len / step + 1) * step).min(MAX_FRAME_PAYLOAD).max(len)
}

/// An empty buffer with room for `len` bytes: a pooled one that fits, else a
/// fresh one of [`size_class`]`(len)`.
#[inline]
pub fn try_alloc(len: usize) -> Result<Vec<u8>, TryReserveError> {
    if len >= POOL_MIN_BYTES {
        return try_alloc_large(len);
    }
    let mut buf = Vec::new();
    buf.try_reserve_exact(len)?;
    Ok(buf)
}

#[inline(never)]
fn try_alloc_large(len: usize) -> Result<Vec<u8>, TryReserveError> {
    if let Some(buf) = pool().take(len) {
        return Ok(buf);
    }
    let mut buf = Vec::new();
    buf.try_reserve_exact(size_class(len))?;
    Ok(buf)
}

/// [`Vec::reserve`] for `additional` more bytes. Where `out` has to grow past
/// [`POOL_MIN_BYTES`] its bytes move into a pooled buffer that fits, else into
/// a fresh one of the [`size_class`] of at least twice its capacity, and the
/// outgrown buffer is given back.
#[inline]
pub fn reserve(out: &mut Vec<u8>, additional: usize) {
    if out.capacity() - out.len() < additional {
        grow(out, additional);
    }
}

#[inline(never)]
fn grow(out: &mut Vec<u8>, additional: usize) {
    let need = out.len() + additional;
    if need < POOL_MIN_BYTES {
        out.reserve(additional);
        return;
    }
    let mut grown = (pool().take(need)).unwrap_or_else(|| Vec::with_capacity(size_class(need.max(2 * out.capacity()))));
    grown.extend_from_slice(out);
    recycle(std::mem::replace(out, grown));
}

/// Give `buf` back. One under [`POOL_MIN_BYTES`] or over [`POOL_BYTES`] is
/// freed instead.
#[inline]
pub fn recycle(buf: Vec<u8>) {
    if (POOL_MIN_BYTES..=POOL_BYTES).contains(&buf.capacity()) {
        let freed = pool().hold(buf);
        // Unmapped outside the lock.
        drop(freed);
    }
}

/// One user of the pool, which holds buffers while any is alive.
pub(crate) struct Use(());

impl Use {
    pub(crate) fn new() -> Self {
        pool().users += 1;
        Use(())
    }
}

impl Drop for Use {
    fn drop(&mut self) {
        let mut pool = pool();
        pool.users -= 1;
        let freed = if pool.users == 0 {
            pool.bytes = 0;
            std::mem::take(&mut pool.bufs)
        } else {
            Vec::new()
        };
        drop(pool);
        drop(freed);
    }
}

/// A received frame's payload, its buffer given back to the pool on drop.
#[derive(Default, Clone)]
pub struct FrameBuf(Vec<u8>);

/// One whole frame payload as the [`Deframer`] hands it out: borrowed where
/// it lay whole in the bytes fed, else in a buffer of its own.
///
/// [`Deframer`]: crate::Deframer
#[derive(Debug)]
pub enum FrameBytes<'a> {
    Borrowed(&'a [u8]),
    Owned(FrameBuf),
}

impl FrameBytes<'_> {
    /// The payload in a buffer of its own, copied there if borrowed.
    pub fn into_owned(self) -> FrameBuf {
        match self {
            FrameBytes::Borrowed(bytes) => FrameBuf(bytes.to_vec()),
            FrameBytes::Owned(buf) => buf,
        }
    }
}

impl Default for FrameBytes<'_> {
    fn default() -> Self {
        FrameBytes::Borrowed(&[])
    }
}

impl std::ops::Deref for FrameBytes<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            FrameBytes::Borrowed(bytes) => bytes,
            FrameBytes::Owned(buf) => buf,
        }
    }
}

impl From<Vec<u8>> for FrameBuf {
    fn from(buf: Vec<u8>) -> Self {
        FrameBuf(buf)
    }
}

impl std::ops::Deref for FrameBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for FrameBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FrameBuf({} bytes)", self.0.len())
    }
}

impl Drop for FrameBuf {
    fn drop(&mut self) {
        recycle(std::mem::take(&mut self.0));
    }
}

#[cfg(test)]
#[path = "tests/frame_buf.rs"]
mod tests;
