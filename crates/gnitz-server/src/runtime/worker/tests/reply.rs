use super::*;
use crate::runtime::w2m::fixtures::{id, test_rings};
use crate::runtime::w2m::W2mReceiver;
use crate::test_support::{
    make_batch_bytes_raw, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64, pk_only_schema,
    table_engine, u64_i64_cols, weighted_rows,
};
use gnitz_store::relation::IndexClaim;
use gnitz_wire::{peek_control_block, DecodedControl, PkColList, TypeCode, WireStatus};
use gnitz_zset::schema::SchemaDescriptor;
use std::rc::Rc;

// -- fixtures ---------------------------------------------------------------

/// A queue over a ring of `ring_bytes` whose budget is a whole frame, and the
/// receiver that reads the ring.
fn queue_over(ring_bytes: usize) -> (Replies, W2mReceiver) {
    let (mut writers, receiver) = test_rings(1, ring_bytes);
    (Replies::new(writers.pop().unwrap()), receiver)
}

impl Replies {
    /// Cut this queue's trains into frames of `budget` bytes.
    pub(in crate::runtime::worker) fn set_frame_budget(&mut self, budget: usize) {
        self.w2m.set_frame_budget(budget);
    }

    /// Owe `answer` at the back of the queue, sending none of it.
    fn reply_owed(&mut self, route: ReplyRoute, answer: Result<Rows, WireFault>) {
        self.owed.push_back(Owed::Train(Train::new(route, answer)));
    }
}

/// The route of request `request_id`, written in cut `cut`.
pub(in crate::runtime::worker) fn route(target_id: u64, request_id: u32, cut: u64) -> ReplyRoute {
    ReplyRoute {
        target_id,
        request_id: id(request_id),
        cut: Cut::nth(cut),
    }
}

/// One published ring message: its ring-prefix request id, control block and
/// bytes.
pub(in crate::runtime::worker) struct Frame {
    req: u32,
    /// Published as passing an owed reply.
    passed: bool,
    pub(in crate::runtime::worker) ctrl: DecodedControl,
    pub(in crate::runtime::worker) bytes: Vec<u8>,
}

impl Frame {
    /// The rows it carries; a frame without a data block carries none.
    pub(in crate::runtime::worker) fn rows(&self, schema: &SchemaDescriptor) -> Batch {
        match self.ctrl.data.clone() {
            Some(data) => {
                Batch::decode_from_wal_block(&self.bytes[data], schema).expect("the frame decodes against its schema")
            }
            None => Batch::empty_with_schema(schema),
        }
    }
}

/// Every message published since the last call, in publish order.
pub(in crate::runtime::worker) fn frames(rx: &W2mReceiver) -> Vec<Frame> {
    let mut out = Vec::new();
    while let Some(slot) = rx.try_read_slot(0) {
        let bytes = slot.bytes().to_vec();
        out.push(Frame {
            req: slot.request_id.get(),
            passed: slot.passed(),
            ctrl: peek_control_block(&bytes).expect("a control block"),
            bytes,
        });
    }
    out
}

/// The request ids of `frames`, in order.
pub(in crate::runtime::worker) fn reqs(frames: &[Frame]) -> Vec<u32> {
    frames.iter().map(|f| f.req).collect()
}

/// What the queue owes, front first: each entry's shape and request id.
pub(in crate::runtime::worker) fn owed(q: &Replies) -> Vec<(&'static str, u32)> {
    q.owed
        .iter()
        .map(|owed| match owed {
            Owed::Parked(route, _) => ("parked", route.request_id.get()),
            Owed::Train(Train { route, answer: Err(_), .. }) => ("fault", route.request_id.get()),
            Owed::Train(train) => ("train", train.route.request_id.get()),
        })
        .collect()
}

/// Emit owed frames until none remain, as successive drains of the SAL do.
pub(in crate::runtime::worker) fn drain(q: &mut Replies) {
    for _ in 0..1000 {
        if q.is_empty() {
            return;
        }
        q.emit_frame();
    }
    panic!("the replies must drain within a bounded pass count");
}

/// Wire size of `batch` sent whole as one frame.
pub(in crate::runtime::worker) fn frame_size(batch: &Batch) -> usize {
    WireMsg {
        data: batch.wire_whole(),
        ..Default::default()
    }
    .size()
}

/// A nonzero weight per row, of both signs, so a scrambled or dropped weight
/// region cannot reassemble to the source.
fn weight(i: u64) -> i64 {
    if i.is_multiple_of(2) {
        i as i64 + 1
    } else {
        -(i as i64)
    }
}

// -- the reply queue --------------------------------------------------------

/// Every reply shape through one budget. A reply that fits leaves at once
/// unless its own cut still owes something; every other reply queues, and the
/// queue drains front first, train by train. Each train's frames carry
/// `continuation`, no schema block, and `scan_last` on the terminal frame alone,
/// and reassemble to the source — PKs, payloads and **weights**, since in a
/// Z-set engine a row-set comparison would test nothing.
#[test]
fn reply_trains_drain_in_order_and_reassemble_exactly() {
    let fixed = make_schema_u64_i64();
    let strings = make_schema_pk_u64_payload_string();
    let budget = frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 4]));

    let a = make_batch_raw(&fixed, &(0..10).map(|i| (i, weight(i), i as i64)).collect::<Vec<_>>());
    let b_vals: Vec<String> = (0..25).map(|i| format!("value-{i}-{}", "p".repeat(40))).collect();
    let b = make_batch_bytes_raw(
        &strings,
        &(0..25u64)
            .map(|i| (i, weight(i), b_vals[i as usize].as_bytes()))
            .collect::<Vec<_>>(),
    );
    let wide = ["x".repeat(4096), "y".repeat(4096)];
    let c = make_batch_bytes_raw(&strings, &[(1, 1, wide[0].as_bytes()), (2, -2, wide[1].as_bytes())]);
    let one = make_batch_raw(&fixed, &[(9, -3, 90)]);
    let empty = Batch::empty_with_schema(&fixed);
    let trains = [
        (11, &a, fixed),
        (22, &b, strings),
        (33, &c, strings),
        (55, &one, fixed),
        (66, &empty, fixed),
    ];

    let (mut q, rx) = queue_over(1 << 22);
    q.set_frame_budget(budget);
    q.reply(route(1, 11, 1), Ok(Rows::from(a.clone())));
    q.reply(route(2, 22, 2), Ok(Rows::from(b.clone())));
    // Cut 3: a train, then two members that fit and stay behind it.
    q.reply(route(3, 33, 3), Ok(Rows::from(c.clone())));
    q.reply(route(5, 55, 3), Ok(Rows::from(one.clone())));
    q.reply(route(6, 66, 3), Ok(Rows::from(empty.clone())));
    // Cut 4 owes nothing: both members pass the queue.
    q.reply(route(4, 44, 4), Ok(Rows::from(one.clone())));
    q.reply(route(4, 45, 4), Ok(Rows::from(one.clone())));

    let inline = frames(&rx);
    assert_eq!(
        reqs(&inline),
        [44, 45],
        "only a fitting reply whose cut owes nothing leaves at once"
    );
    for f in &inline {
        assert!(f.ctrl.hdr.flags.scan_last);
        assert_eq!(weighted_rows(&f.rows(&fixed)), weighted_rows(&one));
    }

    drain(&mut q);
    let out = frames(&rx);
    let mut order = reqs(&out);
    order.dedup();
    assert_eq!(
        order,
        [11, 22, 33, 55, 66],
        "each train's frames fully precede the next's"
    );

    for (req, source, schema) in trains {
        let train: Vec<&Frame> = out.iter().filter(|f| f.req == req).collect();
        let mut got = Vec::new();
        for (i, f) in train.iter().enumerate() {
            let hdr = &f.ctrl.hdr;
            assert_eq!(hdr.status, WireStatus::Ok);
            assert!(hdr.flags.continuation);
            assert!(f.ctrl.schema.is_none(), "a reply frame carries no schema block");
            assert_eq!(
                hdr.flags.scan_last,
                i + 1 == train.len(),
                "train {req}: scan_last on frame {i}"
            );
            got.extend(weighted_rows(&f.rows(&schema)));
        }
        assert_eq!(got, weighted_rows(source), "train {req} reassembles to its source");
    }

    let sizes = |req| -> Vec<(usize, usize)> {
        out.iter()
            .filter(|f| f.req == req)
            .map(|f| (f.rows(if req == 11 { &fixed } else { &strings }).len(), f.bytes.len()))
            .collect()
    };
    // Fixed width: every non-terminal frame fills the budget exactly.
    assert_eq!(
        sizes(11),
        [
            (4, budget),
            (4, budget),
            (2, frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 2])))
        ]
    );
    // Heap strings: several rows a frame, each frame compacted to its own heap.
    let b_sizes = sizes(22);
    assert!(b_sizes.len() >= 2, "{b_sizes:?}");
    assert!(b_sizes.iter().all(|&(_, bytes)| bytes <= budget), "{b_sizes:?}");
    // A row wider than the budget ships alone, over budget, with no fault.
    let c_sizes = sizes(33);
    assert_eq!(c_sizes.len(), 2);
    assert!(
        c_sizes.iter().all(|&(rows, bytes)| rows == 1 && bytes > budget),
        "{c_sizes:?}"
    );
}

/// A reply that fits is ONE frame over the SOURCE batch, sent at once or from
/// the queue — no sub-batch, so no copy and no heap relocation on the path every
/// `scan_many` relation takes. The source carries dead heap bytes, which a
/// sub-batch would compact away: the frame's byte size is what tells the two
/// paths apart.
#[test]
fn a_fitting_reply_is_the_source_batch_verbatim() {
    let schema = make_schema_pk_u64_payload_string();
    let live = make_batch_bytes_raw(
        &schema,
        &[(1, 1, b"a long enough value"), (2, 1, b"another long value")],
    );
    // The same rows over a heap padded with 4096 unreferenced bytes, framed as
    // an engine block that states them.
    let padded = [live.blob(), &[0u8; 4096]].concat();
    let mut regions: Vec<&[u8]> = live.wire_regions().to_vec();
    *regions.last_mut().unwrap() = &padded;
    let mut block = Vec::new();
    gnitz_wire::wal::append_block(&regions, 4096, &mut block);
    let batch = Rc::new(Batch::decode_from_wal_block(&block, &schema).unwrap());
    assert!(
        batch.dead_heap() > 0,
        "the fixture must have dead heap bytes for the size test to discriminate"
    );

    let (mut q, rx) = queue_over(1 << 22);
    q.reply(route(1, 5, 1), Ok(Rows::from(ReadRows::Shared(Rc::clone(&batch)))));
    assert!(q.is_empty(), "the first member of its cut leaves at once");
    // Behind a fault its own cut owes, the same reply waits in the queue.
    q.reply_owed(route(1, 6, 2), Err("boom".into()));
    q.reply(route(1, 7, 2), Ok(Rows::from(ReadRows::Shared(Rc::clone(&batch)))));
    drain(&mut q);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [5, 6, 7]);
    for f in [&out[0], &out[2]] {
        assert!(f.ctrl.hdr.flags.scan_last);
        assert_eq!(
            f.bytes.len(),
            frame_size(&batch),
            "the frame must be the source batch verbatim, heap and all"
        );
    }
}

/// A row wider than [`gnitz_wire::MAX_FRAME_PAYLOAD`] answers its request with
/// the oversize fault, and the train behind it still ships.
#[test]
fn a_row_wider_than_the_frame_cap_faults_its_train_alone() {
    let schema = make_schema_pk_u64_payload_string();
    // One row whose string alone exceeds what a client can read in one frame.
    let batch = make_batch_bytes_raw(
        &schema,
        &[(1, 1, "z".repeat(gnitz_wire::MAX_FRAME_PAYLOAD + 4096).as_bytes())],
    );
    let next = make_batch_raw(&make_schema_u64_i64(), &[(1, 1, 10)]);

    let (mut q, rx) = queue_over(1 << 22);
    q.reply(route(1, 5, 1), Ok(Rows::from(batch)));
    q.reply(route(2, 6, 1), Ok(Rows::from(next)));
    assert_eq!(owed(&q), [("train", 5), ("train", 6)]);
    drain(&mut q);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [5, 6]);
    let fault = out[0].ctrl.fault(&out[0].bytes).expect("a fault frame");
    assert_eq!(fault.status, WireStatus::Error);
    assert!(
        fault.text.contains("exceeds the maximum frame payload"),
        "the fault names the cap: {fault}"
    );
    assert_eq!(out[1].ctrl.hdr.status, WireStatus::Ok);
    assert!(out[1].ctrl.hdr.flags.scan_last);
}

/// A row wider than the ring holds answers its request with a fault too: no
/// publish could ever fit it.
#[test]
fn a_row_wider_than_the_ring_faults_its_train() {
    let schema = make_schema_pk_u64_payload_string();
    let batch = make_batch_bytes_raw(&schema, &[(1, 1, "z".repeat(8192).as_bytes())]);
    let (mut q, rx) = queue_over(4096);
    q.reply(route(1, 5, 1), Ok(Rows::from(batch)));
    drain(&mut q);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [5]);
    let fault = out[0].ctrl.fault(&out[0].bytes).expect("a fault frame");
    assert!(fault.text.contains("exceeds what the reply ring holds"), "{fault}");
}

/// A fault is ordered as any reply is: behind the train its own cut still owes,
/// and at once past what another cut owes.
#[test]
fn an_ordered_fault_stays_behind_its_cuts_queued_train() {
    let fixed = make_schema_u64_i64();
    let rows = make_batch_raw(&fixed, &(0..10).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    let (mut q, rx) = queue_over(1 << 22);
    q.set_frame_budget(frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 4])));

    q.reply(route(1, 11, 1), Ok(Rows::from(rows)));
    q.reply(route(1, 12, 1), Err("of the train's cut".into()));
    q.reply(route(1, 13, 2), Err("of another cut".into()));
    let passing = frames(&rx);
    assert_eq!(reqs(&passing), [13], "another cut's fault leaves at once");
    assert!(passing[0].passed, "published as passing the owed train");
    assert_eq!(owed(&q), [("train", 11), ("fault", 12)]);

    drain(&mut q);
    let out = frames(&rx);
    assert_eq!(reqs(&out), [11, 11, 11, 12], "the fault follows the train's last frame");
    assert!(out.iter().all(|f| !f.passed), "each was the front of the queue");
    assert!(out[2].ctrl.hdr.flags.scan_last);
    let fault = out[3].ctrl.fault(&out[3].bytes).expect("a fault frame");
    assert_eq!(fault.text, "of the train's cut");
}

/// With no room on the ring a reply is owed, not waited for; the emitter sends
/// it once the master has released room.
#[test]
fn a_full_ring_owes_instead_of_blocking() {
    let fixed = make_schema_u64_i64();
    let one = make_batch_raw(&fixed, &[(9, -3, 90)]);
    let (mut q, rx) = queue_over(4096);
    while q.w2m.try_send_msg(id(1), false, &WireMsg::default()) {}

    q.reply(route(1, 5, 1), Ok(Rows::from(one.clone())));
    q.reply(route(1, 6, 2), Err("boom".into()));
    assert_eq!(owed(&q), [("train", 5), ("fault", 6)]);

    assert!(
        frames(&rx).iter().all(|f| f.req == 1),
        "nothing of either reply was sent"
    );
    drain(&mut q);
    let out = frames(&rx);
    assert_eq!(reqs(&out), [5, 6]);
    assert_eq!(weighted_rows(&out[0].rows(&fixed)), weighted_rows(&one));
}

/// A span train carries every span in order, cut into frames by `chunk_rows` or
/// the byte budget, whichever is hit first, and ends in one row-less terminal frame.
#[test]
fn a_span_train_carries_every_span_and_ends_in_one_empty_frame() {
    let span_schema = pk_only_schema(&[TypeCode::I64]);
    let cols = PkColList::from_slice(&[1]);
    let full = gnitz_wire::MAX_FRAME_PAYLOAD;
    // (rows held, frame budget, chunk rows, frames that carry spans)
    let cases: [(u64, usize, usize, std::ops::RangeInclusive<usize>); 4] = [
        (7, full, 3, 3..=3),
        (8, full, 4, 2..=2),
        (0, full, 4, 0..=0),
        (24, 400, 1024, 2..=24),
    ];
    for (n, budget, chunk_rows, carrying) in cases {
        let (mut engine, tid) = table_engine("worker", &format!("span_train_{n}"), 1, &u64_i64_cols());
        let claim = IndexClaim::Index { id: tid + 1, unique: false };
        engine.registry.add_indexes(tid, &[(claim, cols)]).unwrap();
        // Values descend as ids ascend, so span order is not row order.
        let held: Vec<_> = (0..n).map(|i| (i, 1, -(i as i64))).collect();
        engine
            .registry
            .ingest(tid, make_batch_raw(&make_schema_u64_i64(), &held))
            .unwrap();
        engine.registry.set_scan_chunk_rows(chunk_rows);
        let (mut q, rx) = queue_over(1 << 22);
        q.set_frame_budget(budget);

        let spans = engine.registry.key_spans(tid, cols.as_slice()).unwrap();
        q.reply(route(tid, 9, 1), Ok(Rows::Spans(Box::new(spans))));
        assert!(
            frames(&rx).is_empty(),
            "a span train is never sent from where it is answered"
        );
        drain(&mut q);

        let out = frames(&rx);
        let (last, body) = out.split_last().expect("a terminal frame");
        assert!(last.ctrl.hdr.flags.scan_last && last.ctrl.data.is_none());
        assert!(carrying.contains(&body.len()), "{n} spans: {} frames", body.len());
        let mut got = Vec::new();
        for f in &out {
            assert_eq!((f.req, f.ctrl.hdr.status), (9, WireStatus::Ok));
            assert!(f.ctrl.hdr.flags.continuation, "every span frame carries continuation");
            assert!(f.ctrl.schema.is_none(), "the master builds the frame schema itself");
            let rows = f.rows(&span_schema);
            got.extend((0..rows.len()).map(|i| rows.get_pk_bytes(i).to_vec()));
        }
        assert!(body
            .iter()
            .all(|f| !f.ctrl.hdr.flags.scan_last && f.ctrl.data.is_some()));
        let want: Vec<Vec<u8>> = (0..n)
            .rev()
            .map(|i| span_schema.opk_key_cols(&[-(i as i64) as u128]).pk_bytes().to_vec())
            .collect();
        assert_eq!(got, want, "{n} spans");
    }
}

/// A refusal asks the master for the ring back only when what holds the ring
/// is another cut's than a reply that waits: a train held up by its own frames
/// asks nothing, a reply of another cut behind those frames does.
#[test]
fn a_refusal_names_a_shortage_only_behind_another_cuts_bytes() {
    let fixed = make_schema_u64_i64();
    let rows = make_batch_raw(&fixed, &(0..1000).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    let (mut q, rx) = queue_over(4096);
    q.set_frame_budget(frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 16])));

    q.reply(route(1, 11, 1), Ok(Rows::from(rows)));
    while !q.w2m.short_of_room() {
        q.emit_frame();
    }
    assert!(!rx.answer_shortage(0), "a train held up by its own frames asks nothing");

    // As large as the train's frame the ring just refused.
    let behind = make_batch_raw(&fixed, &(0..16).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    q.reply(route(2, 22, 2), Ok(Rows::from(behind)));
    assert_eq!(owed(&q), [("train", 11), ("train", 22)]);
    assert!(
        rx.answer_shortage(0),
        "a reply of another cut behind them asks for the ring"
    );
    q.w2m.name_shortage();
    assert!(!rx.answer_shortage(0), "once per release");
}

/// A train the master holds sends nothing and holds up only its own cut: the
/// first sendable reply behind it goes, and with every reply held the worker
/// waits on the master, until the hold ends.
#[test]
fn a_held_train_steps_aside_until_its_hold_ends() {
    let fixed = make_schema_u64_i64();
    let rows = make_batch_raw(&fixed, &(0..100).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    let (mut q, rx) = queue_over(1 << 22);
    q.set_frame_budget(frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 16])));

    q.reply(route(1, 11, 1), Ok(Rows::from(rows.clone())));
    q.reply(route(1, 12, 1), Ok(Rows::from(rows)));
    q.reply_owed(route(2, 21, 2), Err("boom".into()));
    assert!(rx.hold(0, id(11)));
    q.emit_frame();
    assert_eq!(
        reqs(&frames(&rx)),
        [21],
        "the next cut's reply goes past the held train"
    );
    q.emit_frame();
    assert!(frames(&rx).is_empty(), "nor does the train behind it in its cut");
    assert!(q.waits_on_master());

    rx.unhold(0, id(11));
    assert!(!q.waits_on_master());
    drain(&mut q);
    let mut order = reqs(&frames(&rx));
    order.dedup();
    assert_eq!(order, [11, 12]);
}
