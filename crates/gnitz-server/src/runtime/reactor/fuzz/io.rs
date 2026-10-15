//! What reaches a server from a peer it has not admitted: the bytes of a
//! connection, deframed under the inbound budget — before the handshake under
//! its frame ceiling, after it under the full one — and the first frame read
//! as a HELLO whose token is checked. However the bytes split across reads,
//! they deframe to the same frames and end the same way — but for the budget,
//! which charges a frame until it is taken, so a run whose reads hold more
//! frames at once may stop short at its cap.

use super::*;
use crate::runtime::tls::Tokens;
use gnitz_fuzz::seed::HELLO_TOKEN as TOKEN;

/// A peer's bytes, cut where the input's first byte says, under both ceilings.
#[test]
fn hello_fuzz() {
    let tokens = tokens();
    bolero::check!().for_each(|bytes: &[u8]| {
        let Some((&cut, stream)) = bytes.split_first() else {
            return;
        };
        let step = cut as usize % 17 + 1;
        for admitted in [false, true] {
            let whole = deframe(stream, stream.len().max(1), admitted);
            let split = deframe(stream, step, admitted);
            agree(&whole, &split, step);
        }
        let (frames, _) = deframe(stream, stream.len().max(1), false);
        if let Some(Ok(token)) = frames.first().map(|f| gnitz_wire::read_client_hello(f)) {
            tokens.check(token);
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

/// The access tokens of a server holding the one token [`TOKEN`], the one
/// the HELLO starts carry.
fn tokens() -> Tokens {
    let digest = ring::digest::digest(&ring::digest::SHA256, TOKEN.as_bytes());
    let hex: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens");
    std::fs::write(&path, format!("fuzz {hex}\n")).unwrap();
    Tokens::load(path.to_str().unwrap()).unwrap()
}

/// A HELLO carrying [`TOKEN`] and then a frame past the handshake's ceiling is
/// read alike in one read and in many, and its HELLO is admitted.
#[test]
fn an_admitted_hello_deframes_alike_in_any_reads() {
    let hello = gnitz_wire::client_hello(TOKEN.as_bytes());
    let mut stream = gnitz_wire::frame_len_prefix(hello.len()).to_vec();
    stream.extend_from_slice(&hello);
    let big = vec![7u8; gnitz_wire::HANDSHAKE_FRAME_MAX + 1];
    stream.extend_from_slice(&gnitz_wire::frame_len_prefix(big.len()));
    stream.extend_from_slice(&big);
    let (frames, end) = deframe(&stream, 3, true);
    assert!(frames.len() == 2 && end.is_none(), "{end:?}");
    let (frames, end) = deframe(&stream, stream.len(), false);
    assert_eq!(frames.len(), 1, "the handshake's ceiling refuses the second");
    assert!(end.is_some());
    let token = gnitz_wire::read_client_hello(&frames[0]).unwrap();
    assert_eq!(tokens().check(token), Some("fuzz"));
}
