//! What reaches a server from a peer it has not admitted: the bytes of a
//! connection, deframed under the inbound budget — before the handshake under
//! its frame ceiling, after it under the full one — and the first frame read
//! as a HELLO. However the bytes split across reads,
//! they deframe to the same frames and end the same way — but for the budget,
//! which charges a frame until it is taken, so a run whose reads hold more
//! frames at once may stop short at its cap.

use super::*;
use gnitz_fuzz::seed::HELLO_TOKEN as TOKEN;

/// A peer's bytes, cut where the input's first byte says, under both ceilings.
#[test]
fn hello_fuzz() {
    bolero::check!().for_each(|bytes: &[u8]| {
        let Some((&cut, stream)) = bytes.split_first() else {
            return;
        };
        let step = cut as usize % 17 + 1;
        let [(frames, _), _] = [false, true].map(|admitted| {
            let whole = deframe(stream, stream.len().max(1), admitted);
            agree(&whole, &deframe(stream, step, admitted), step);
            whole
        });
        if let Some(f) = frames.first() {
            let _ = gnitz_wire::read_client_hello(f);
        }
    });
}

/// What the inbound budget holds, as a connection's whole share of it.
const BUDGET: usize = 1 << 20;

/// Frames deframed, and why the feed ended, if it did.
type Deframed = (Vec<Vec<u8>>, Option<RecvEnd>);

/// Two deframings of one stream agree: the same frames and the same end, or
/// one stopped short at the budget's cap with a prefix of the other's frames.
fn agree((a, a_end): &Deframed, (b, b_end): &Deframed, step: usize) {
    let capped = |end: &Option<RecvEnd>| matches!(end, Some(RecvEnd::CapBreach { .. }));
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    assert_eq!(short[..], long[..short.len()], "reads of {step} bytes");
    if !(capped(a_end) || capped(b_end)) {
        assert_eq!(
            (a.len(), format!("{a_end:?}")),
            (b.len(), format!("{b_end:?}")),
            "reads of {step} bytes"
        );
    }
}

/// The frames `stream` deframes to, fed `step` bytes a read, and why the feed
/// ended, if it did.
fn deframe(stream: &[u8], step: usize, admitted: bool) -> Deframed {
    let mut q = RecvQueue::new(Budget::new(BUDGET));
    if admitted {
        q.admit_full_frames();
    }
    let mut frames = Vec::new();
    for read in stream.chunks(step) {
        let fed = q.feed(read);
        while let Some(f) = q.try_recv() {
            frames.push(f.as_slice().to_vec());
        }
        if let Err(end) = fed {
            return (frames, Some(end));
        }
    }
    (frames, None)
}

/// A HELLO carrying [`TOKEN`] and then a frame past the handshake's ceiling is
/// read alike in one read and in many, and its HELLO is admitted.
#[test]
fn an_admitted_hello_deframes_alike_in_any_reads() {
    let hello = gnitz_wire::client_hello(TOKEN.as_bytes());
    let big = vec![7u8; gnitz_wire::HANDSHAKE_FRAME_MAX + 1];
    let stream = [gnitz_wire::framed(&hello), gnitz_wire::framed(&big)].concat();
    let (frames, end) = deframe(&stream, 3, true);
    assert!(frames.len() == 2 && end.is_none(), "{end:?}");
    let (frames, end) = deframe(&stream, stream.len(), false);
    assert_eq!(frames.len(), 1, "the handshake's ceiling refuses the second");
    assert!(end.is_some());
    let token = gnitz_wire::read_client_hello(&frames[0]).unwrap();
    assert_eq!(token, TOKEN.as_bytes());
}
