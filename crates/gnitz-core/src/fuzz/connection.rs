//! A session on whatever a server sends back: a request of every kind a
//! session holds a slot for, then the input's bytes as the server's whole
//! reply stream, then the end of the connection. Every request is answered
//! exactly once — by its reply or by the session's end — and no stream of
//! bytes panics the client.

use super::*;
use crate::test_support::{session_pair, step_until_idle, wide_schema};
use gnitz_wire::{ReadBound, ReadSpec};

#[test]
fn reply_fuzz() {
    let schema = wide_schema(4, true);
    let spec = ReadSpec::all_rows(ReadBound::None);
    bolero::check!().for_each(|stream: &[u8]| {
        let (mut s, peer) = session_pair();
        let mut ack = s.submit(Request::AllocIds(1));
        let mut resolve = s.submit_resolve("s.t");
        let mut scan = s.submit_scan(Target::from(5), &spec, &schema);
        let mut multi = s.submit_scan_multi(&[(Target::from(5), &spec, &schema), (Target::from(6), &spec, &schema)]);
        let mut delta = s.submit_delta_read(Target::from(7), None, &schema, &spec.encode(), Some(1));

        peer.send_bytes(stream);
        drop(peer);
        s.step(Interest::WRITE);
        step_until_idle(&mut s);
        for (k, answered) in [
            ack.try_take().is_some(),
            resolve.try_take().is_some(),
            scan.try_take().is_some(),
            multi.try_take().is_some(),
            delta.try_take().is_some(),
        ]
        .into_iter()
        .enumerate()
        {
            assert!(answered, "request {k} was never answered");
        }
    });
}
