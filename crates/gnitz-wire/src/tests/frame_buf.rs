use std::mem::MaybeUninit;

use super::*;
use crate::{Deframer, FrameLenError, framed};

/// A pool with one user, as one open connection makes it.
fn used() -> Pool {
    let mut pool = Pool::new();
    pool.users = 1;
    pool
}

/// Hold `cap`-byte buffers in `pool`, in order.
fn hold_all(pool: &mut Pool, caps: &[usize]) {
    for &cap in caps {
        pool.hold(Vec::with_capacity(cap));
    }
}

fn held(pool: &Pool) -> Vec<usize> {
    pool.bufs.iter().map(Vec::capacity).collect()
}

/// A held buffer serves a request of at most its capacity and at least half
/// of it, and no other; the smallest that fits is taken.
#[test]
fn a_held_buffer_serves_requests_it_fits() {
    let mut pool = used();
    let cap = 4 * POOL_MIN_BYTES;
    let b = Vec::<u8>::with_capacity(cap);
    let at = b.as_ptr();
    pool.hold(b);
    assert!(pool.take(cap + 1).is_none(), "too small for the request");
    assert!(pool.take(cap / 2 - 1).is_none(), "more than twice the request");
    let got = pool.take(cap / 2).expect("fits");
    assert_eq!((got.as_ptr(), got.capacity(), got.len()), (at, cap, 0));
    hold_all(&mut pool, &[4 * POOL_MIN_BYTES, 3 * POOL_MIN_BYTES]);
    assert_eq!(
        pool.take(2 * POOL_MIN_BYTES).map(|b| b.capacity()),
        Some(3 * POOL_MIN_BYTES)
    );
    assert_eq!(
        (held(&pool), pool.bytes),
        (vec![4 * POOL_MIN_BYTES], 4 * POOL_MIN_BYTES)
    );
}

/// Past its bound the pool frees the oldest it holds; with no users it holds
/// nothing.
#[test]
fn the_pool_holds_at_most_its_bound_and_only_while_used() {
    let mut pool = used();
    let big = POOL_BYTES / 2;
    hold_all(&mut pool, &[POOL_MIN_BYTES, big, big, 2 * POOL_MIN_BYTES]);
    assert_eq!(held(&pool), [big, 2 * POOL_MIN_BYTES]);
    assert_eq!(pool.bytes, big + 2 * POOL_MIN_BYTES);
    let mut idle = Pool::new();
    let freed = idle.hold(Vec::with_capacity(POOL_MIN_BYTES));
    assert_eq!((freed.len(), held(&idle)), (1, vec![]));
}

/// A fresh buffer's class has room for at least a sixteenth more than its
/// request and at most an eighth of the request's power of two, below the
/// largest frame; below the pool's floor it is exact.
#[test]
fn a_size_class_leaves_bounded_headroom() {
    assert_eq!(size_class(POOL_MIN_BYTES - 1), POOL_MIN_BYTES - 1);
    for len in [POOL_MIN_BYTES, 3 << 20, (5 << 20) + 7, 1 << 25, MAX_FRAME_PAYLOAD - 1] {
        let class = size_class(len);
        let pow = 1 << len.ilog2();
        assert!(class >= len + len / 16 || class == MAX_FRAME_PAYLOAD, "{len}: {class}");
        assert!(class <= len + pow / 8, "{len}: {class}");
        assert!(fits(class, len));
    }
    assert_eq!(size_class(MAX_FRAME_PAYLOAD), MAX_FRAME_PAYLOAD);
    assert_eq!(size_class(MAX_FRAME_PAYLOAD + 5), MAX_FRAME_PAYLOAD + 5);
}

/// A reserve growing past the floor with nothing pooled to fit moves the
/// bytes into a fresh buffer of a size class, with room for growth; one with
/// room does nothing.
#[test]
fn a_reserve_past_the_floor_grows_into_a_size_class() {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(b"head");
    let before = out.as_ptr();
    reserve(&mut out, 16);
    assert_eq!(out.as_ptr(), before, "room already");
    // A size no other test of the crate asks the pool for.
    let need = (5 << 20) + 3;
    reserve(&mut out, need - 4);
    assert_eq!(&out[..], b"head");
    assert!(out.capacity() >= size_class(need) && out.capacity() > need);
}

/// A dropped `FrameBuf` gives its buffer to the pool while a connection is
/// open.
#[test]
fn a_dropped_frame_buf_returns_its_buffer() {
    let _open = Deframer::<()>::default();
    // A size no other test of the crate asks the pool for.
    let cap = 11 << 20;
    let b = Vec::with_capacity(cap);
    let at = b.as_ptr();
    drop(FrameBuf::from(b));
    let again = try_alloc(cap).unwrap();
    assert_eq!((again.as_ptr(), again.capacity()), (at, cap), "the buffer given back");
}

/// A payload received into a buffer larger than it ends at its own length: no
/// read window reaches past it into the next frame, and the frame after it
/// parses whole.
#[test]
fn a_frame_in_a_larger_buffer_stops_at_its_length() {
    let len = 3 * POOL_MIN_BYTES / 2;
    let first: Vec<u8> = (0..len).map(|i| i as u8).collect();
    let wire = [framed(&first), framed(b"next")].concat();
    let mut d = Deframer::default();
    let mut carry: Box<[MaybeUninit<u8>]> = Box::new_uninit_slice(64 * 1024);
    let mut frames = Vec::new();
    let mut rest = &wire[..];
    while !rest.is_empty() {
        let window = d.window(&mut carry);
        let n = window.len().min(rest.len());
        window[..n].write_copy_of_slice(&rest[..n]);
        rest = &rest[n..];
        // SAFETY: `n` bytes were just written at the head of the window.
        let mut src = unsafe { d.landed(&carry, n) };
        while let Some((f, ())) = d.feed(&mut src, |_| Ok::<_, FrameLenError>(())).unwrap() {
            frames.push(f.into_owned());
        }
    }
    assert!(frames[0].0.capacity() > len, "received into a size class");
    assert_eq!(frames.iter().map(|f| &f[..]).collect::<Vec<_>>(), [&first[..], b"next"]);
}
