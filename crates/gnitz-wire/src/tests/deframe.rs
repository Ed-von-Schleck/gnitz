use std::mem::MaybeUninit;

use super::*;

type Buf = Box<[MaybeUninit<u8>]>;

fn framed(payload: &[u8]) -> Vec<u8> {
    let mut v = (payload.len() as u32).to_le_bytes().to_vec();
    v.extend_from_slice(payload);
    v
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

/// A frame split at every byte offset — inside the prefix and inside the payload
/// alike — completes exactly once, on the second half.
#[test]
fn a_frame_split_at_every_offset_completes_once() {
    let payload: Vec<u8> = (0..40u8).collect();
    let wire = framed(&payload);
    for split in 1..wire.len() {
        let mut d = Deframer::<Buf>::default();
        let mut src = &wire[..split];
        assert_eq!(feed_one(&mut d, &mut src), None, "split={split}");
        assert!(src.is_empty(), "split={split}: the first half is consumed whole");
        assert!(d.is_mid_frame(), "split={split}");

        let mut src = &wire[split..];
        assert_eq!(feed_one(&mut d, &mut src), Some(payload.clone()), "split={split}");
        assert!(src.is_empty());
        assert!(!d.is_mid_frame(), "split={split}: nothing left buffered");
    }
}

/// A pipelined run yields one frame per call, in order, and then `None`.
#[test]
fn a_pipelined_run_yields_one_frame_per_call() {
    let payloads: Vec<Vec<u8>> = (1..=5u8).map(|i| vec![i; i as usize * 3]).collect();
    let wire: Vec<u8> = payloads.iter().flat_map(|p| framed(p)).collect();
    let mut d = Deframer::<Buf>::default();
    let mut src = &wire[..];
    for p in &payloads {
        assert_eq!(feed_one(&mut d, &mut src).as_ref(), Some(p));
    }
    assert_eq!(feed_one(&mut d, &mut src), None);
    assert!(src.is_empty());
}

/// A zero prefix and a prefix over the ceiling are refused before any allocation.
#[test]
fn a_bad_prefix_is_refused_before_allocating() {
    let refuse = |wire: &[u8]| {
        let mut d = Deframer::<Buf>::default();
        let mut src = wire;
        d.feed(&mut src, |_| -> Result<Buf, FrameLenError> {
            panic!("alloc on a refused prefix")
        })
        .expect_err("refused")
    };
    assert_eq!(refuse(&0u32.to_le_bytes()), FrameLenError::Zero);
    assert_eq!(
        refuse(&((MAX_FRAME_PAYLOAD + 1) as u32).to_le_bytes()),
        FrameLenError::Oversize { len: MAX_FRAME_PAYLOAD + 1 }
    );
}

/// A prefix of exactly the ceiling is accepted and opens a payload.
#[test]
fn a_prefix_at_the_ceiling_is_accepted() {
    let mut d = Deframer::<Buf>::default();
    let prefix = (MAX_FRAME_PAYLOAD as u32).to_le_bytes();
    let mut src = &prefix[..];
    assert!(matches!(d.feed(&mut src, alloc), Ok(None)));
    assert!(d.is_mid_frame());
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
