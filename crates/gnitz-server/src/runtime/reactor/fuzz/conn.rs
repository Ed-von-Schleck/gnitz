//! What reaches a server from a peer it has not admitted: the bytes of a
//! connection, deframed under the inbound budget — before the handshake under
//! its frame ceiling, after it under the full one — and the first frame read
//! as a HELLO. However the bytes split across reads,
//! they deframe to the same frames and end the same way — but for the budget,
//! which charges a frame until it is taken, so a run whose reads hold more
//! frames at once may stop short at its cap.

use super::tests::{Deframed, deframe};
use super::*;

/// A peer's bytes, cut where the input's first byte says, under both ceilings.
#[test]
fn hello_fuzz() {
    bolero::check!().for_each(|bytes: &[u8]| {
        let Some((&cut, stream)) = bytes.split_first() else {
            return;
        };
        let step = cut as usize % 17 + 1;
        let [(frames, _), _] = [false, true].map(|admitted| {
            let whole = deframe(stream, stream.len().max(1), admitted, BUDGET);
            agree(&whole, &deframe(stream, step, admitted, BUDGET), step);
            whole
        });
        if let Some(f) = frames.first() {
            let _ = gnitz_wire::read_client_hello(f);
        }
    });
}

/// What the inbound budget holds, as a connection's whole share of it.
const BUDGET: usize = 1 << 20;

/// Two deframings of one stream agree: the same frames and the same end, or
/// one stopped short at the budget's cap with a prefix of the other's frames.
fn agree((a, a_end): &Deframed, (b, b_end): &Deframed, step: usize) {
    let capped = |end: &Option<RecvEnd>| matches!(end, Some(RecvEnd::CapBreach { .. }));
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    assert_eq!(short[..], long[..short.len()], "reads of {step} bytes");
    if !(capped(a_end) || capped(b_end)) {
        assert_eq!((a.len(), a_end), (b.len(), b_end), "reads of {step} bytes");
    }
}
