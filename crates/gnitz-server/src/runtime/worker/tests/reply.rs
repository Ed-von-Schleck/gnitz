use super::*;
use crate::runtime::w2m::W2mReceiver;
use crate::runtime::w2m::fixtures::{id, test_rings};
use crate::test_support::{
    chunked, make_batch, make_batch_bytes, make_schema_pk_u64_payload_string, make_schema_u64_i64, pk_only_schema,
    table_engine_with, u64_i64_cols, weighted_rows,
};
use gnitz_store::relation::IndexClaim;
use gnitz_wire::{DecodedControl, PkColList, TypeCode, WireStatus, peek_control_block};
use gnitz_zset::repr::MemBatch;
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
    /// Nothing is owed.
    pub(in crate::runtime::worker) fn is_empty(&self) -> bool {
        self.owed.is_empty()
    }

    /// Cut this queue's trains into frames of `budget` bytes.
    pub(in crate::runtime::worker) fn set_frame_budget(&mut self, budget: usize) {
        self.w2m.set_frame_budget(budget);
    }

    /// Owe `answer` at the back of the queue, sending none of it.
    fn reply_owed(&mut self, route: ReplyRoute, answer: Result<Rows, WireFault>) {
        self.owed.push_back(Owed::train(route, answer));
    }
}

/// The route of a read of `target_id` answered on lease `request_id`.
pub(in crate::runtime::worker) fn route(target_id: u64, request_id: u32) -> ReplyRoute {
    ReplyRoute { target_id, request_id: id(request_id) }
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
            Some(data) => MemBatch::of_wal_block(&self.bytes[data], schema)
                .expect("the frame decodes against its schema")
                .to_batch(),
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
    if i.is_multiple_of(2) { i as i64 + 1 } else { -(i as i64) }
}

// -- the reply queue --------------------------------------------------------

/// Every reply shape through one budget. A reply that fits leaves at once
/// unless its own lease still owes something; every other reply queues, and the
/// queue drains front first, train by train, one lease's trains split at their
/// terminals. Each train's frames carry `continuation`, and `scan_last` on the
/// terminal frame alone, and reassemble to the source — PKs, payloads and
/// **weights**, since in a Z-set engine a row-set comparison would test nothing.
#[test]
fn reply_trains_drain_in_order_and_reassemble_exactly() {
    let fixed = make_schema_u64_i64();
    let strings = make_schema_pk_u64_payload_string();
    let budget = frame_size(&make_batch(&fixed, &[(0, 1, 0); 4]));

    let a = make_batch(&fixed, &(0..10).map(|i| (i, weight(i), i as i64)).collect::<Vec<_>>());
    let b_vals: Vec<String> = (0..25).map(|i| format!("value-{i}-{}", "p".repeat(40))).collect();
    let b = make_batch_bytes(
        &strings,
        &(0..25u64)
            .map(|i| (i, weight(i), b_vals[i as usize].as_bytes()))
            .collect::<Vec<_>>(),
    );
    let wide = ["x".repeat(4096), "y".repeat(4096)];
    let c = make_batch_bytes(&strings, &[(1, 1, wide[0].as_bytes()), (2, -2, wide[1].as_bytes())]);
    let one = make_batch(&fixed, &[(9, -3, 90)]);
    let empty = Batch::empty_with_schema(&fixed);
    let sources = [
        (11, &a, fixed),
        (22, &b, strings),
        (33, &c, strings),
        (33, &one, fixed),
        (33, &empty, fixed),
    ];

    let (mut q, rx) = queue_over(1 << 22);
    q.set_frame_budget(budget);
    q.reply(route(1, 11), Ok(Rows::from(a.clone())));
    q.reply(route(2, 22), Ok(Rows::from(b.clone())));
    // Lease 33: a train, then two reads that fit and stay behind it.
    q.reply(route(3, 33), Ok(Rows::from(c.clone())));
    q.reply(route(5, 33), Ok(Rows::from(one.clone())));
    q.reply(route(6, 33), Ok(Rows::from(empty.clone())));
    // Lease 44 owes nothing: both its reads pass the queue.
    q.reply(route(4, 44), Ok(Rows::from(one.clone())));
    q.reply(route(4, 44), Ok(Rows::from(one.clone())));

    let inline = frames(&rx);
    assert_eq!(
        reqs(&inline),
        [44, 44],
        "only a fitting reply whose lease owes nothing leaves at once"
    );
    for f in &inline {
        assert!(f.ctrl.hdr.flags.scan_last);
        assert_eq!(weighted_rows(&f.rows(&fixed)), weighted_rows(&one));
    }

    drain(&mut q);
    let out = frames(&rx);
    // One train per terminal, in the order the reads were answered.
    let trains: Vec<&[Frame]> = out.split_inclusive(|f| f.ctrl.hdr.flags.scan_last).collect();
    assert_eq!(trains.len(), sources.len(), "one terminal per read");

    for (train, (req, source, schema)) in trains.iter().zip(sources) {
        assert!(
            train.iter().all(|f| f.req == req),
            "each train's frames fully precede the next's"
        );
        let mut got = Vec::new();
        for (i, f) in train.iter().enumerate() {
            let hdr = &f.ctrl.hdr;
            assert_eq!(hdr.status, WireStatus::Ok);
            assert!(hdr.flags.continuation);
            assert_eq!(
                hdr.flags.scan_last,
                i + 1 == train.len(),
                "train {req}: scan_last on frame {i}"
            );
            got.extend(weighted_rows(&f.rows(&schema)));
        }
        assert_eq!(got, weighted_rows(source), "train {req} reassembles to its source");
    }

    let sizes = |train: usize| -> Vec<(usize, usize)> {
        trains[train]
            .iter()
            .map(|f| (f.rows(&sources[train].2).len(), f.bytes.len()))
            .collect()
    };
    // Fixed width: every non-terminal frame fills the budget exactly.
    assert_eq!(
        sizes(0),
        [
            (4, budget),
            (4, budget),
            (2, frame_size(&make_batch(&fixed, &[(0, 1, 0); 2])))
        ]
    );
    // Heap strings: several rows a frame, each frame compacted to its own heap.
    let b_sizes = sizes(1);
    assert!(b_sizes.len() >= 2, "{b_sizes:?}");
    assert!(b_sizes.iter().all(|&(_, bytes)| bytes <= budget), "{b_sizes:?}");
    // A row wider than the budget ships alone, over budget, with no fault.
    let c_sizes = sizes(2);
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
    let live = make_batch_bytes(&schema, &[(1, 1, "a long enough value"), (2, 1, "another long value")]);
    // The same rows over a heap padded with 4096 unreferenced bytes, framed as
    // an engine block that states them.
    let padded = [live.blob(), &[0u8; 4096]].concat();
    let mut regions: Vec<&[u8]> = live.regions().to_vec();
    *regions.last_mut().unwrap() = &padded;
    let mut block = Vec::new();
    gnitz_wire::wal::append_block(&regions, 4096, &mut block);
    let batch = Rc::new(MemBatch::of_wal_block(&block, &schema).unwrap().to_batch());
    assert!(
        batch.dead_heap() > 0,
        "the fixture must have dead heap bytes for the size test to discriminate"
    );

    let (mut q, rx) = queue_over(1 << 22);
    q.reply(route(1, 5), Ok(Rows::from(Rc::clone(&batch))));
    assert!(q.is_empty(), "the first read of its lease leaves at once");
    // Behind a fault its own lease owes, the same reply waits in the queue.
    q.reply_owed(route(1, 6), Err("boom".into()));
    q.reply(route(1, 6), Ok(Rows::from(Rc::clone(&batch))));
    drain(&mut q);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [5, 6, 6]);
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
    let batch = make_batch_bytes(
        &schema,
        &[(1, 1, "z".repeat(gnitz_wire::MAX_FRAME_PAYLOAD + 4096).as_bytes())],
    );
    let next = make_batch(&make_schema_u64_i64(), &[(1, 1, 10)]);

    let (mut q, rx) = queue_over(1 << 22);
    q.reply(route(1, 5), Ok(Rows::from(batch)));
    q.reply(route(2, 5), Ok(Rows::from(next)));
    assert_eq!(owed(&q), [("train", 5), ("train", 5)]);
    drain(&mut q);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [5, 5]);
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
    let batch = make_batch_bytes(&schema, &[(1, 1, "z".repeat(8192).as_bytes())]);
    let (mut q, rx) = queue_over(4096);
    q.reply(route(1, 5), Ok(Rows::from(batch)));
    drain(&mut q);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [5]);
    let fault = out[0].ctrl.fault(&out[0].bytes).expect("a fault frame");
    assert!(fault.text.contains("exceeds what the reply ring holds"), "{fault}");
}

/// A fault is ordered as any reply is: behind the train its own lease still
/// owes, and at once past what another lease owes.
#[test]
fn an_ordered_fault_stays_behind_its_leases_queued_train() {
    let fixed = make_schema_u64_i64();
    let rows = make_batch(&fixed, &(0..10).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    let (mut q, rx) = queue_over(1 << 22);
    q.set_frame_budget(frame_size(&make_batch(&fixed, &[(0, 1, 0); 4])));

    q.reply(route(1, 11), Ok(Rows::from(rows)));
    q.reply(route(1, 11), Err("of the train's lease".into()));
    q.reply(route(1, 13), Err("of another lease".into()));
    let passing = frames(&rx);
    assert_eq!(reqs(&passing), [13], "another lease's fault leaves at once");
    assert!(passing[0].passed, "published as passing the owed train");
    assert_eq!(owed(&q), [("train", 11), ("fault", 11)]);

    drain(&mut q);
    let out = frames(&rx);
    assert_eq!(reqs(&out), [11; 4]);
    assert!(out.iter().all(|f| !f.passed), "each was the front of the queue");
    assert!(out[2].ctrl.hdr.flags.scan_last, "the train's last frame");
    let fault = out[3].ctrl.fault(&out[3].bytes).expect("the fault follows it");
    assert_eq!(fault.text, "of the train's lease");
}

/// With no room on the ring a reply is owed, not waited for; the emitter sends
/// it once the master has released room.
#[test]
fn a_full_ring_owes_instead_of_blocking() {
    let fixed = make_schema_u64_i64();
    let one = make_batch(&fixed, &[(9, -3, 90)]);
    let (mut q, rx) = queue_over(4096);
    while q.w2m.try_send_msg(id(1), false, &WireMsg::default()).is_ok() {}

    q.reply(route(1, 5), Ok(Rows::from(one.clone())));
    q.reply(route(1, 6), Err("boom".into()));
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

/// A read answered with no rows, owed for want of room, is one last frame
/// without a data block once the master has released room.
#[test]
fn a_reply_of_nothing_owed_is_one_empty_last_frame() {
    let (mut q, rx) = queue_over(4096);
    while q.w2m.try_send_msg(id(1), false, &WireMsg::default()).is_ok() {}

    q.reply(route(1, 5), Ok(Rows::Nothing));
    assert_eq!(owed(&q), [("train", 5)]);
    assert!(frames(&rx).iter().all(|f| f.req == 1), "nothing of the reply was sent");

    drain(&mut q);
    let out = frames(&rx);
    assert_eq!(reqs(&out), [5]);
    assert_eq!(out[0].ctrl.hdr.status, WireStatus::Ok);
    assert!(out[0].ctrl.hdr.flags.scan_last);
    assert!(out[0].ctrl.data.is_none(), "no data block");
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
        let (_dir, mut engine, tid) = table_engine_with(1, &u64_i64_cols(), chunked(chunk_rows));
        let claim = IndexClaim::Index { id: tid + 1, unique: false };
        engine.registry.add_indexes(tid, &[(claim, cols)]).unwrap();
        // Values descend as ids ascend, so span order is not row order.
        let held: Vec<_> = (0..n).map(|i| (i, 1, -(i as i64))).collect();
        engine
            .registry
            .ingest(tid, make_batch(&make_schema_u64_i64(), &held))
            .unwrap();
        let (mut q, rx) = queue_over(1 << 22);
        q.set_frame_budget(budget);

        let spans = engine.registry.key_spans(tid, cols.as_slice()).unwrap();
        q.reply(route(tid, 9), Ok(Rows::Spans(Box::new(spans))));
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
            let rows = f.rows(&span_schema);
            got.extend((0..rows.len()).map(|i| rows.get_pk_bytes(i).to_vec()));
        }
        assert!(
            body.iter()
                .all(|f| !f.ctrl.hdr.flags.scan_last && f.ctrl.data.is_some())
        );
        let want: Vec<Vec<u8>> = (0..n)
            .rev()
            .map(|i| span_schema.opk_key_cols(&[-(i as i64) as u128]).pk_bytes().to_vec())
            .collect();
        assert_eq!(got, want, "{n} spans");
    }
}

/// A refusal asks the master for the ring back only when what holds the ring
/// is another lease's than a reply that waits: a train held up by its own
/// lease's frames asks nothing, a reply of another lease behind those frames
/// does.
#[test]
fn a_refusal_names_a_shortage_only_behind_another_leases_bytes() {
    let fixed = make_schema_u64_i64();
    let rows = make_batch(&fixed, &(0..1000).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    let (mut q, rx) = queue_over(4096);
    q.set_frame_budget(frame_size(&make_batch(&fixed, &[(0, 1, 0); 16])));

    q.reply(route(1, 11), Ok(Rows::from(rows)));
    while !q.waits_on_master() {
        q.emit_frame();
    }
    assert!(!rx.answer_shortage(0), "a train held up by its own frames asks nothing");

    // As large as the train's frame the ring just refused.
    let behind = make_batch(&fixed, &(0..16).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    q.reply(route(2, 11), Ok(Rows::from(behind.clone())));
    assert_eq!(owed(&q), [("train", 11), ("train", 11)]);
    assert!(
        !rx.answer_shortage(0),
        "nor does the next read of its lease, queued behind it"
    );
    q.reply(route(2, 22), Ok(Rows::from(behind)));
    assert_eq!(owed(&q), [("train", 11), ("train", 11), ("train", 22)]);
    assert!(
        rx.answer_shortage(0),
        "a reply of another lease behind them asks for the ring"
    );
    q.w2m.name_shortage();
    assert!(!rx.answer_shortage(0), "once per release");
}

/// A train the ring refused keeps the worker waiting on the master while a
/// reply of another lease passes it into the room left, until a release.
#[test]
fn a_refused_train_waits_on_the_master_past_a_passing_reply() {
    let fixed = make_schema_u64_i64();
    let rows = make_batch(&fixed, &(0..1000).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    let (mut q, rx) = queue_over(4096);
    q.set_frame_budget(frame_size(&make_batch(&fixed, &[(0, 1, 0); 16])));

    q.reply(route(1, 11), Ok(Rows::from(rows)));
    while !q.waits_on_master() {
        q.emit_frame();
    }
    q.reply(route(2, 22), Err("boom".into()));
    assert_eq!(owed(&q), [("train", 11)], "the fault fit the room left");
    assert!(q.waits_on_master());
    let written = rx.write_cursor(0);
    q.emit_frame();
    assert_eq!(rx.write_cursor(0), written, "the refused frame is not tried again");

    drop(rx.try_read_slot(0).expect("a frame"));
    assert!(!q.waits_on_master());
}

/// A lease the master holds sends nothing, whichever of its trains is next:
/// the first sendable reply behind them goes, and with every reply held the
/// worker waits on the master, until the hold ends.
#[test]
fn a_held_train_steps_aside_until_its_hold_ends() {
    let fixed = make_schema_u64_i64();
    let rows = make_batch(&fixed, &(0..100).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    let (mut q, rx) = queue_over(1 << 22);
    q.set_frame_budget(frame_size(&make_batch(&fixed, &[(0, 1, 0); 16])));

    q.reply(route(1, 11), Ok(Rows::from(rows.clone())));
    q.reply(route(1, 11), Ok(Rows::from(rows)));
    q.reply_owed(route(2, 21), Err("boom".into()));
    assert!(rx.hold(0, id(11)));
    q.emit_frame();
    assert_eq!(
        reqs(&frames(&rx)),
        [21],
        "the next lease's reply goes past the held trains"
    );
    q.emit_frame();
    assert!(frames(&rx).is_empty(), "nothing of the held lease goes");
    assert!(q.waits_on_master());

    rx.unhold(0, id(11));
    assert!(!q.waits_on_master());
    drain(&mut q);
    let out = frames(&rx);
    assert!(out.iter().all(|f| f.req == 11));
    assert_eq!(
        out.iter().filter(|f| f.ctrl.hdr.flags.scan_last).count(),
        2,
        "both of its trains, each to its terminal"
    );
}
