//! A session on whatever a server sends back: a request of every kind a
//! session holds a slot for, then the input's bytes as the server's whole
//! reply stream, then the end of the connection. Every request is answered
//! exactly once — by its reply or by the session's end — and no stream of
//! bytes panics the client.

use super::*;
use crate::test_support::{session_pair, step_until_idle, sub_id, wide_schema};
use gnitz_wire::{ReadBound, ReadSpec};

#[test]
fn reply_fuzz() {
    let schema = wide_schema(4, true);
    let spec = ReadSpec::all_rows(ReadBound::None);
    let spec_bytes = spec.encode();
    bolero::check!().for_each(|stream: &[u8]| {
        let (mut s, peer) = session_pair();
        let mut ack = s.submit(Request::AllocIds(1));
        let mut resolve = s.submit_resolve("s.t");
        let mut scan = s.submit_scan(Target::from(5), &spec, &schema);
        let mut multi = s.submit_scan_multi(&[(Target::from(5), &spec, &schema), (Target::from(6), &spec, &schema)]);
        let mut delta = s.submit_delta_read(Target::from(7), None, &schema, &spec_bytes);
        let mut page = s.submit_snapshot_page(1, 8, &[], 16, &schema);
        s.submit_delta_poll(
            sub_id(100),
            &[multi_frame::DeltaPollItem {
                view: Target::from(9),
                from: None,
                reply_layout: schema.layout().layout_digest(),
                spec: &spec_bytes,
            }],
        );
        // A subscription the session holds, so a pushed train has a holder and the sync is sent.
        s.inbox.polls.server_holds = true;
        (s.inbox.polls.subs).insert(sub_id(2), Ok((Vec::new(), DeltaCursor::from_pair(1, 1).unwrap())));
        let mut sync = s.submit_sync(&[sub_id(2)], Duration::ZERO);

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
            page.try_take().is_some(),
            sync.try_take().is_some(),
        ]
        .into_iter()
        .enumerate()
        {
            assert!(answered, "request {k} was never answered");
        }
        assert!(
            s.inbox.polls.subs.contains_key(&sub_id(100)) && s.inbox.polls.reading.is_empty(),
            "the poll's one view ends"
        );
    });
}
