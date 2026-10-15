//! A session on whatever a server sends back: a request of every kind a
//! session holds a slot for, then the input's bytes as the server's whole
//! reply stream, then the end of the connection. Every request is answered
//! exactly once — by its reply or by the session's end — and no stream of
//! bytes panics the client.

use super::*;
use crate::protocol::transport::poll_fd;
use crate::test_support::{session_pair, wide_schema};
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
        let mut answered: [Box<dyn FnMut() -> bool>; 5] = [
            Box::new(move || ack.try_take().is_some()),
            Box::new(move || resolve.try_take().is_some()),
            Box::new(move || scan.try_take().is_some()),
            Box::new(move || multi.try_take().is_some()),
            Box::new(move || delta.try_take().is_some()),
        ];

        peer.send_bytes(stream);
        drop(peer);
        let mut ready = Interest::WRITE;
        while s.interest() != Interest::NONE {
            s.step(ready);
            if s.interest() == Interest::NONE {
                break;
            }
            let events = poll_fd(s.as_raw_fd(), s.interest().poll_events(), None).unwrap();
            ready = Interest::from_revents(events);
        }
        for (k, answered) in answered.iter_mut().enumerate() {
            assert!(answered(), "request {k} was never answered");
        }
    });
}
