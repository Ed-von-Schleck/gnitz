use std::mem::MaybeUninit;

use super::*;

type Buf = Box<[MaybeUninit<u8>]>;

fn framed(payload: &[u8]) -> Vec<u8> {
    [&frame_len_prefix(payload.len())[..], payload].concat()
}

fn alloc(len: usize) -> Result<Buf, FrameLenError> {
    Ok(Box::new_uninit_slice(len))
}

fn bytes(b: Buf) -> Vec<u8> {
    // SAFETY: the deframer hands a payload out only once every byte is written.
    unsafe { b.assume_init() }.into_vec()
}

/// One `feed` over `src`, the payload as bytes.
fn feed_one(d: &mut Deframer<Buf>, src: &mut &[u8]) -> Option<Vec<u8>> {
    d.feed(src, alloc).expect("no refusal").map(bytes)
}

/// A pipelined run split at every byte offset — inside a prefix, inside a
/// payload, or on a frame boundary — yields each frame exactly once, in order,
/// and is mid-frame after the first half iff the split is not on a boundary.
#[test]
fn a_pipelined_run_split_anywhere_yields_each_frame_once() {
    let payloads: Vec<Vec<u8>> = (1..=3u8).map(|i| vec![i; i as usize * 7]).collect();
    let wire: Vec<u8> = payloads.iter().flat_map(|p| framed(p)).collect();
    let boundaries: Vec<usize> = payloads
        .iter()
        .scan(0, |end, p| {
            *end += FRAME_LEN_PREFIX_BYTES + p.len();
            Some(*end)
        })
        .collect();
    for split in 0..=wire.len() {
        let mut d = Deframer::<Buf>::default();
        let mut got = Vec::new();
        for (i, half) in [&wire[..split], &wire[split..]].into_iter().enumerate() {
            let mut src = half;
            while let Some(p) = feed_one(&mut d, &mut src) {
                got.push(p);
            }
            assert!(src.is_empty(), "split={split}: each half is consumed whole");
            if i == 0 {
                let on_boundary = split == 0 || boundaries.contains(&split);
                assert_eq!(d.is_mid_frame(), !on_boundary, "split={split}");
            }
        }
        assert_eq!(got, payloads, "split={split}");
        assert!(!d.is_mid_frame(), "split={split}: nothing left buffered");
    }
}

/// A zero prefix and a prefix over the ceiling are refused before any
/// allocation and leave nothing buffered; a prefix of exactly the ceiling
/// allocates once and opens a payload.
#[test]
fn a_prefix_is_bounded_before_allocating() {
    let open = |len: usize| {
        let mut d = Deframer::<Buf>::default();
        let mut allocs = 0;
        let prefix = (len as u32).to_le_bytes();
        let got = d
            .feed(&mut &prefix[..], |n| {
                allocs += 1;
                alloc(n)
            })
            .map(|p| p.is_some());
        (got, allocs, d.is_mid_frame())
    };
    assert_eq!(open(0), (Err(FrameLenError::Zero), 0, false));
    assert_eq!(
        open(MAX_FRAME_PAYLOAD + 1),
        (Err(FrameLenError::Oversize { len: MAX_FRAME_PAYLOAD + 1 }), 0, false)
    );
    assert_eq!(open(MAX_FRAME_PAYLOAD), (Ok(false), 1, true));
}

/// Bytes written straight into the payload tail complete the frame on the next
/// `feed`, even of nothing.
#[test]
fn a_directly_filled_payload_completes_on_the_next_feed() {
    let mut d = Deframer::<Buf>::default();
    let mut src = &framed(&[7u8; 10])[..6];
    assert_eq!(feed_one(&mut d, &mut src), None);
    let tail = d.payload_tail().expect("a payload in progress");
    assert_eq!(tail.len(), 8);
    tail.write_copy_of_slice(&[7u8; 8]);
    // SAFETY: the eight bytes of the tail were just written.
    unsafe { d.filled(8) };
    assert_eq!(feed_one(&mut d, &mut &[][..]), Some(vec![7u8; 10]));
    assert!(!d.is_mid_frame());
}
