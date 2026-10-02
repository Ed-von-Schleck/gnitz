use super::*;
use crate::catalog::{CatalogColumn, SysFamily};
use crate::runtime::sal::fixtures::TestLog;
use crate::runtime::sal::{DirectGroup, GroupData, GroupTargets, WorkerSet};
use crate::runtime::w2m::{self, W2mReceiver};
use crate::test_support::{
    col_def, make_batch_bytes_raw, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64,
    pk_only_schema, register_identity_view, scan_all, weighted_rows,
};
use gnitz_expr::SchemaFacts;
use gnitz_wire::control::{peek_control_block, DecodedControl};
use gnitz_wire::{PkColList, Probe, ReadBound, ReadSpec, TypeCode, WireStatus};
use gnitz_zset::algebra::ScatterPlan;
use gnitz_zset::schema::SchemaDescriptor;

// -- fixtures ---------------------------------------------------------------

/// A worker over a real SAL and a real W2M ring — the one ring its replies and
/// its SAL wakes share, as in production — through the production constructor
/// so a new field cannot be missed here, with the receiver that reads its ring. The budget is pinned rather than read from the environment,
/// so a shell that exports `GNITZ_REPLY_FRAME_BUDGET` does not reshape these
/// frames.
fn test_worker(catalog: &mut CatalogEngine) -> (WorkerProcess<'_>, TestLog, W2mReceiver) {
    worker_over(catalog, 1 << 22, 1 << 20)
}

/// [`test_worker`] over a ring of `ring_bytes` and a SAL of `sal_bytes`.
fn worker_over(
    catalog: &mut CatalogEngine,
    ring_bytes: usize,
    sal_bytes: usize,
) -> (WorkerProcess<'_>, TestLog, W2mReceiver) {
    let ring = w2m::fixtures::test_ring(ring_bytes);
    let sal = TestLog::with_rings(sal_bytes, vec![ring], 1);
    let mesh = crate::runtime::mesh::fixtures::meshes(1, crate::runtime::mesh::OUTBOX_BYTES)
        .pop()
        .unwrap();
    let mut wp = WorkerProcess::new(catalog, SalReader::new(sal.log(), 0, 1), W2mWriter::new(ring), mesh);
    wp.reply_frame_budget = gnitz_wire::MAX_FRAME_PAYLOAD;
    (wp, sal, W2mReceiver::new(vec![ring]))
}

/// The route of request `request_id`, written in cut `cut`.
fn route(target_id: u64, request_id: u32, cut: u64) -> ReplyRoute {
    ReplyRoute { target_id, request_id, cut }
}

/// The columns of [`make_schema_u64_i64`].
fn table_cols() -> [CatalogColumn; 2] {
    [col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)]
}

/// A fresh catalog holding `public.t` over [`table_cols`]; its id.
fn engine_with_table(name: &str) -> (CatalogEngine, u64) {
    let dir = crate::test_support::scratch_dir("worker", name);
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let tid = engine.create_table("public.t", &table_cols(), &[0]).unwrap();
    (engine, tid)
}

/// `request` answered on `request_id`; `later`: as a later member of its cut.
fn addressed<'a>(request: impl Into<SalRequest<'a>>, request_id: u32, later: bool) -> DirectGroup<'a> {
    DirectGroup {
        targets: GroupTargets {
            request_id,
            in_request_order: later,
            ..GroupTargets::UNADDRESSED
        },
        ..DirectGroup::new(request)
    }
}

/// An unbounded read of every row of `tid`, whose rows lay out as `schema`.
fn scan(tid: u64, schema: &SchemaDescriptor) -> Read<'static> {
    Read::ScanSpec {
        tid,
        reply_layout: schema.layout_digest(),
        spec: ReadSpec::all_rows(ReadBound::None).encode().into(),
    }
}

/// `ids` as the keys a PK probe of a `U64`-keyed relation carries.
fn pk_keys(ids: &[u64]) -> (ipc::WireSchema, Batch) {
    let schema = pk_only_schema(&[TypeCode::U64]);
    let mut keys = Batch::with_capacity(&schema, ids.len());
    for id in ids {
        keys.push_key_row(&id.to_be_bytes(), 1);
    }
    (ipc::WireSchema::encoded(0, &schema), keys)
}

/// A PK probe of `tid` at `keys`.
fn probe<'a>(tid: u64, keys: &'a (ipc::WireSchema, Batch), request_id: u32, later: bool) -> DirectGroup<'a> {
    DirectGroup {
        schema: Some(keys.0.block()),
        data: GroupData::Same(keys.1.wire_whole()),
        ..addressed(Read::HasPk { tid, probe: Probe::Pk }, request_id, later)
    }
}

/// A push of `rows` into `tid`, ACKed on `request_id`.
fn push<'a>(relation: &'a ipc::WireSchema, rows: &'a Batch, request_id: u32) -> DirectGroup<'a> {
    let targets = GroupTargets { request_id, ..GroupTargets::UNADDRESSED };
    DirectGroup::push(relation, GroupData::Same(rows.wire_whole()), targets)
}

/// One published ring message: its ring-prefix request id, control block and
/// bytes.
struct Frame {
    req: u32,
    ctrl: DecodedControl,
    bytes: Vec<u8>,
}

impl Frame {
    /// The rows it carries; a frame without a data block carries none.
    fn rows(&self, schema: &SchemaDescriptor) -> Batch {
        match self.ctrl.data.clone() {
            Some(data) => {
                Batch::decode_from_wal_block(&self.bytes[data], schema).expect("the frame decodes against its schema")
            }
            None => Batch::empty_with_schema(schema),
        }
    }
}

/// Every message published since the last call, in publish order.
fn frames(rx: &W2mReceiver) -> Vec<Frame> {
    let mut out = Vec::new();
    while let Some(slot) = rx.try_read_slot(0) {
        let bytes = slot.bytes().to_vec();
        out.push(Frame {
            req: slot.internal_req_id,
            ctrl: peek_control_block(&bytes).expect("a control block"),
            bytes,
        });
    }
    out
}

/// The request ids of `frames`, in order.
fn reqs(frames: &[Frame]) -> Vec<u32> {
    frames.iter().map(|f| f.req).collect()
}

/// What the worker owes, front first: each entry's shape and request id.
fn owed(wp: &WorkerProcess) -> Vec<(&'static str, u32)> {
    wp.replies
        .iter()
        .map(|owed| match owed {
            Owed::Parked(route, ..) => ("parked", route.request_id),
            Owed::Fault(route, _) => ("fault", route.request_id),
            Owed::Train(train) => ("train", train.route.request_id),
        })
        .collect()
}

/// Emit owed frames until none remain, as successive `drain_sal` passes do.
fn drain_replies(wp: &mut WorkerProcess) {
    for _ in 0..1000 {
        if wp.replies.is_empty() {
            return;
        }
        wp.emit_reply_frame();
    }
    panic!("the replies must drain within a bounded pass count");
}

/// Dispatch every group on the SAL as an exchange wait does.
fn drain_in_wait(wp: &mut WorkerProcess) {
    while let Some(req) = wp.next_request() {
        wp.dispatch_in_wait(req);
    }
}

/// Wire size of `batch` sent whole as one frame.
fn frame_size(batch: &Batch) -> usize {
    ipc::WireMsg {
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

// -- dispatch ---------------------------------------------------------------

/// The in-wait matrix, driven as the exchange wait's own drain loop drives it:
/// a read of a view parks in SAL order; a read of a base table, `Push` and
/// `Flush` answer from inside the wait; a DdlSync that addresses no worker is
/// stepped over; and a train stays unsent — a slow client can fill the ring, and
/// a worker blocked on it would stall the round every peer waits on.
#[test]
fn in_wait_dispositions() {
    let (mut engine, tid) = engine_with_table("in_wait_dispositions");
    let vid = register_identity_view(&mut engine, tid, "v", &table_cols());
    let schema = make_schema_u64_i64();
    let (mut wp, sal, rx) = test_worker(&mut engine);

    let delta = Read::Delta {
        view: vid,
        after_tick: 0,
        cut_round: 1,
        reply_layout: schema.layout_digest(),
    };
    let spans = Read::KeySpans { tid, cols: PkColList::from_slice(&[1]) };
    let ddl = DirectGroup {
        targets: GroupTargets {
            set: WorkerSet::EMPTY,
            ..GroupTargets::UNADDRESSED
        },
        ..DirectGroup::new(Apply::DdlSync { family: SysFamily::Sequence.id() })
    };
    // A control-only push carries no rows, so it ACKs without reaching the
    // store. `Flush` goes last: the reader leaves the epoch on stepping past it.
    let groups = [
        addressed(scan(vid, &schema), 1, false),
        addressed(delta, 2, false),
        addressed(scan(tid, &schema), 3, false),
        addressed(spans, 4, false),
        addressed(Read::HasPk { tid, probe: Probe::Pk }, 5, false),
        addressed(Apply::Push { tid }, 6, false),
        ddl,
        addressed(Apply::Flush, 7, false),
    ];
    for group in &groups {
        sal.excl().write(group).expect("group fits");
    }
    drain_in_wait(&mut wp);

    assert_eq!(owed(&wp), [("parked", 1), ("parked", 2), ("train", 4)]);
    let out = frames(&rx);
    assert_eq!(reqs(&out), [3, 5, 6, 7]);
    let statuses: Vec<_> = out.iter().map(|f| f.ctrl.hdr.status).collect();
    assert_eq!(
        statuses,
        [WireStatus::Ok, WireStatus::Error, WireStatus::Ok, WireStatus::Ok],
        "the table's read, HasPk (a keyless probe faults), Push and Flush answer"
    );
    assert!(out[0].ctrl.hdr.flags.scan_last);
    let fault = out[1].ctrl.fault(&out[1].bytes).expect("a fault frame");
    assert!(fault.text.contains("carries its keys"), "{fault}");
}

/// One Tick group carrying two tids ticks both — each drains its buffered
/// delta — and ACKs once.
#[test]
fn a_two_tid_tick_group_ticks_both_and_acks_once() {
    let dir = crate::test_support::scratch_dir("worker", "two_tid_tick");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let schema = make_schema_u64_i64();
    for tid in [500, 501] {
        engine
            .registry
            .register(gnitz_store::relation::RelationSpec {
                id: tid,
                kind: gnitz_store::relation::RelationKind::BaseTable,
                schema,
                placement: gnitz_zset::schema::Placement::full_pk(&schema),
            })
            .unwrap();
        engine
            .registry
            .ingest_pending(tid, make_batch_raw(&schema, &[(tid, 1, 10)]))
            .unwrap();
    }
    let (mut wp, sal, rx) = test_worker(&mut engine);

    let tick = Apply::Tick {
        first_round: 7,
        tids: [500u64, 501]
            .iter()
            .flat_map(|t| t.to_le_bytes())
            .collect::<Vec<u8>>()
            .into(),
    };
    sal.excl().write(&addressed(tick, 9, false)).expect("group fits");
    wp.drain_sal();

    for tid in [500, 501] {
        assert!(wp.catalog.registry.seal(tid).unwrap().is_none(), "tid {tid} was ticked");
    }
    let out = frames(&rx);
    assert_eq!(reqs(&out), [9], "the group ACKs once");
    assert_eq!(out[0].ctrl.hdr.status, WireStatus::Ok);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A base-table read drained inside an exchange wait answers at its place in the
/// SAL: the push behind it is applied, and is not in the reply.
#[test]
fn a_push_does_not_reach_a_base_table_read_ahead_of_it() {
    let (mut engine, tid) = engine_with_table("push_behind_read");
    let schema = make_schema_u64_i64();
    let relation = ipc::WireSchema::from_catalog(&engine, tid);
    let row = make_batch_raw(&schema, &[(1, 1, 10)]);
    let (mut wp, sal, rx) = test_worker(&mut engine);

    sal.excl()
        .write(&addressed(scan(tid, &schema), 1, false))
        .expect("group fits");
    sal.excl().write(&push(&relation, &row, 2)).expect("group fits");
    drain_in_wait(&mut wp);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [1, 2], "the read answers, then the push ACKs");
    assert!(out[0].ctrl.hdr.flags.scan_last);
    assert!(out[0].rows(&schema).is_empty(), "the read was written before the push");
    assert_eq!(weighted_rows(&scan_all(wp.catalog, tid)), weighted_rows(&row));
}

/// A view read drained inside an exchange wait parks, and the cut member behind
/// it — a base-table read, answered in the wait — stays behind it: the view's
/// reply reaches the ring first.
#[test]
fn a_parked_view_read_answers_ahead_of_the_cut_member_behind_it() {
    let (mut engine, tid) = engine_with_table("parked_view_read");
    let vid = register_identity_view(&mut engine, tid, "v", &table_cols());
    let schema = make_schema_u64_i64();
    let held = make_batch_raw(&schema, &[(1, 1, 10)]);
    engine.registry.ingest(tid, held.clone()).unwrap();
    let (mut wp, sal, rx) = test_worker(&mut engine);

    {
        let excl = sal.excl();
        excl.write(&addressed(scan(vid, &schema), 1, false))
            .expect("group fits");
        excl.write(&addressed(scan(tid, &schema), 2, true)).expect("group fits");
    }
    drain_in_wait(&mut wp);
    assert_eq!(owed(&wp), [("parked", 1), ("train", 2)]);
    assert!(frames(&rx).is_empty(), "nothing of the cut passes its parked member");

    wp.answer_parked();
    assert_eq!(owed(&wp), [("train", 2)]);
    drain_replies(&mut wp);
    let out = frames(&rx);
    assert_eq!(reqs(&out), [1, 2]);
    assert!(out[0].rows(&schema).is_empty(), "no tick has fed the view");
    assert_eq!(weighted_rows(&out[1].rows(&schema)), weighted_rows(&held));
}

/// Every probe of a validation burst answers from inside an exchange wait,
/// past what another cut owes.
#[test]
fn a_burst_passes_what_another_cut_owes() {
    let (mut engine, tid) = engine_with_table("burst");
    let vid = register_identity_view(&mut engine, tid, "v", &table_cols());
    let schema = make_schema_u64_i64();
    let (mut wp, sal, rx) = test_worker(&mut engine);

    let delta = Read::Delta {
        view: vid,
        after_tick: 0,
        cut_round: 1,
        reply_layout: schema.layout_digest(),
    };
    let keys = pk_keys(&[1]);
    sal.excl().write(&addressed(delta, 1, false)).expect("group fits");
    {
        let excl = sal.excl();
        excl.write(&probe(tid, &keys, 2, false)).expect("group fits");
        excl.write(&probe(tid, &keys, 3, true)).expect("group fits");
    }
    drain_in_wait(&mut wp);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [2, 3]);
    assert!(out
        .iter()
        .all(|f| f.ctrl.hdr.status == WireStatus::Ok && f.ctrl.hdr.flags.scan_last));
    assert_eq!(owed(&wp), [("parked", 1)]);
}

/// The wait looks at its round after every read: with the round complete, the
/// drive goes on after one and leaves the next on the SAL.
#[test]
fn a_complete_round_goes_ahead_of_the_reads_on_the_sal() {
    let (mut engine, tid) = engine_with_table("round_first");
    let schema = make_schema_u64_i64();
    let (mut wp, sal, rx) = test_worker(&mut engine);
    for request_id in [1, 2] {
        sal.excl()
            .write(&addressed(scan(tid, &schema), request_id, false))
            .expect("group fits");
    }

    // One worker: its own publish completes the round.
    let part = make_batch_raw(&schema, &[(1, 1, 10)]);
    let got = wp.exchange(9, std::borrow::Cow::Borrowed(&part), &ScatterPlan::broadcast(), false);
    assert_eq!(weighted_rows(&got), weighted_rows(&part));
    assert_eq!(reqs(&frames(&rx)), [1]);
    let next = wp.next_request().expect("the second read is still on the SAL");
    assert_eq!(next.route.request_id, 2);
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

    let dir = crate::test_support::scratch_dir("worker", "reply_trains");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (mut wp, _sal, rx) = test_worker(&mut engine);
    wp.reply_frame_budget = budget;
    wp.reply(route(1, 11, 1), Ok(Rows::Own(a.clone())));
    wp.reply(route(2, 22, 2), Ok(Rows::Own(b.clone())));
    // Cut 3: a train, then two members that fit and stay behind it.
    wp.reply(route(3, 33, 3), Ok(Rows::Own(c.clone())));
    wp.reply(route(5, 55, 3), Ok(Rows::Own(one.clone())));
    wp.reply(route(6, 66, 3), Ok(Rows::Own(empty.clone())));
    // Cut 4 owes nothing: both members pass the queue.
    wp.reply(route(4, 44, 4), Ok(Rows::Own(one.clone())));
    wp.reply(route(4, 45, 4), Ok(Rows::Own(one.clone())));

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

    drain_replies(&mut wp);
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

    let dir = crate::test_support::scratch_dir("worker", "verbatim_reply");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (mut wp, _sal, rx) = test_worker(&mut engine);
    wp.reply(route(1, 5, 1), Ok(Rows::Shared(Rc::clone(&batch))));
    assert!(wp.replies.is_empty(), "the first member of its cut leaves at once");
    // Behind a fault its own cut owes, the same reply waits in the queue.
    wp.replies.push_back(Owed::Fault(route(1, 6, 2), "boom".into()));
    wp.reply(route(1, 7, 2), Ok(Rows::Shared(Rc::clone(&batch))));
    drain_replies(&mut wp);

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

    let dir = crate::test_support::scratch_dir("worker", "oversized_row");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (mut wp, _sal, rx) = test_worker(&mut engine);
    wp.reply(route(1, 5, 1), Ok(Rows::Own(batch)));
    wp.reply(route(2, 6, 1), Ok(Rows::Own(next)));
    assert_eq!(owed(&wp), [("train", 5), ("train", 6)]);
    drain_replies(&mut wp);

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

/// A fault is ordered as any reply is: behind the train its own cut still owes,
/// and at once past what another cut owes.
#[test]
fn an_ordered_fault_stays_behind_its_cuts_queued_train() {
    let fixed = make_schema_u64_i64();
    let rows = make_batch_raw(&fixed, &(0..10).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    let dir = crate::test_support::scratch_dir("worker", "ordered_fault");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (mut wp, _sal, rx) = test_worker(&mut engine);
    wp.reply_frame_budget = frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 4]));

    wp.reply(route(1, 11, 1), Ok(Rows::Own(rows)));
    wp.reply(route(1, 12, 1), Err("of the train's cut".into()));
    wp.reply(route(1, 13, 2), Err("of another cut".into()));
    assert_eq!(reqs(&frames(&rx)), [13], "another cut's fault leaves at once");
    assert_eq!(owed(&wp), [("train", 11), ("fault", 12)]);

    drain_replies(&mut wp);
    let out = frames(&rx);
    assert_eq!(reqs(&out), [11, 11, 11, 12], "the fault follows the train's last frame");
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
    let dir = crate::test_support::scratch_dir("worker", "full_ring");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (mut wp, _sal, rx) = worker_over(&mut engine, 4096, 1 << 20);
    while wp.w2m_writer.try_send_msg(1, &ipc::WireMsg::default()) {}

    wp.reply(route(1, 5, 1), Ok(Rows::Own(one.clone())));
    wp.reply(route(1, 6, 2), Err("boom".into()));
    assert_eq!(owed(&wp), [("train", 5), ("fault", 6)]);

    assert!(
        frames(&rx).iter().all(|f| f.req == 1),
        "nothing of either reply was sent"
    );
    drain_replies(&mut wp);
    let out = frames(&rx);
    assert_eq!(reqs(&out), [5, 6]);
    assert_eq!(weighted_rows(&out[0].rows(&fixed)), weighted_rows(&one));
}

/// A queued train sends a frame at the top of a drain and one after every
/// request the drain serves, however many the SAL holds.
#[test]
fn a_train_advances_with_every_request() {
    const GROUPS: usize = 5;
    let fixed = make_schema_u64_i64();
    let rows = make_batch_raw(&fixed, &(0..40).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    let (mut engine, tid) = engine_with_table("train_advances");
    let (mut wp, sal, rx) = test_worker(&mut engine);
    wp.reply_frame_budget = frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 4]));
    wp.reply(route(1, 11, 1), Ok(Rows::Own(rows)));

    for _ in 0..GROUPS {
        // Control-only and unanswered: nothing of its own reaches the ring.
        sal.excl()
            .write(&DirectGroup::new(Apply::Push { tid }))
            .expect("group fits");
    }
    wp.drain_sal();
    assert_eq!(reqs(&frames(&rx)), [11; GROUPS + 1]);
    assert_eq!(owed(&wp), [("train", 11)], "ten frames, six sent");
}

/// A span train carries every span in order, off the index and off the sort
/// alike, cut into frames by `chunk_rows` or the byte budget, whichever is hit
/// first, and ends in one row-less terminal frame.
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
    for indexed in [true, false] {
        for (n, budget, chunk_rows, carrying) in cases.clone() {
            let (mut engine, tid) = engine_with_table(&format!("span_train_{indexed}_{n}"));
            if indexed {
                let claim = gnitz_store::relation::IndexClaim::Index { id: tid + 1, unique: false };
                engine.registry.add_index(tid, claim, cols).unwrap();
            }
            // Values descend as ids ascend, so span order is not row order.
            let held: Vec<_> = (0..n).map(|i| (i, 1, -(i as i64))).collect();
            engine
                .registry
                .ingest(tid, make_batch_raw(&make_schema_u64_i64(), &held))
                .unwrap();
            engine.registry.set_scan_chunk_rows(chunk_rows);
            let (mut wp, _sal, rx) = test_worker(&mut engine);
            wp.reply_frame_budget = budget;

            let spans = wp.catalog.registry.key_spans(tid, cols.as_slice()).unwrap();
            wp.reply(route(tid, 9, 1), Ok(Rows::Spans(Box::new(spans))));
            assert!(
                frames(&rx).is_empty(),
                "a span train is never sent from where it is answered"
            );
            drain_replies(&mut wp);

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
                .map(|i| {
                    span_schema
                        .opk_key(&(-(i as i64) as u128).to_le_bytes())
                        .pk_bytes()
                        .to_vec()
                })
                .collect();
            assert_eq!(got, want, "indexed={indexed}, {n} spans");
        }
    }
}

// -- benchmark --------------------------------------------------------------

/// Groups each arm of [`worker_request_bench`] writes.
const BENCH_GROUPS: u64 = 10_000;

/// Rows a chunk of the bench's span train holds.
const BENCH_CHUNK_ROWS: u64 = 1024;

/// [`test_worker`] over a SAL and a ring that hold a whole bench arm.
fn bench_worker(catalog: &mut CatalogEngine) -> (WorkerProcess<'_>, TestLog, W2mReceiver) {
    worker_over(catalog, 1 << 26, 32 << 20)
}

/// The worker's own request path, in instructions per request: a group read off
/// the SAL, decoded, applied or answered, and its reply on the ring.
///
/// `cd crates && cargo test -p gnitz-server --release worker_request_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn worker_request_bench() {
    use gnitz_store::relation::IndexClaim;
    use gnitz_wire::{PkKeys, ReadBound, ReadSpec};
    use std::hint::black_box;

    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let cols = table_cols();

    // One arm: a fresh table of `rows` rows, the groups `write` puts on the SAL,
    // and `drain_sal` until nothing is owed. `per` is what the count is divided by.
    let arm = |label: &str,
               rows: u64,
               index: bool,
               write: &dyn Fn(&TestLog, &ipc::WireSchema, &SchemaDescriptor, u64),
               per: &dyn Fn(usize) -> (u64, &'static str)| {
        if std::env::var("GNITZ_BENCH_ARM").is_ok_and(|only| only != label) {
            return;
        }
        let dir = crate::test_support::scratch_dir("worker", &format!("request_bench_{label}"));
        let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
        let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
        let schema = engine.registry.relation(tid).unwrap().schema();
        engine.registry.set_scan_chunk_rows(BENCH_CHUNK_ROWS as usize);
        if index {
            let claim = IndexClaim::Index { id: tid + 1, unique: false };
            engine
                .registry
                .add_index(tid, claim, PkColList::from_slice(&[1]))
                .unwrap();
        }
        if rows > 0 {
            let held: Vec<_> = (0..rows).map(|i| (i, 1, i as i64)).collect();
            engine.registry.ingest(tid, make_batch_raw(&schema, &held)).unwrap();
        }
        let relation = ipc::WireSchema::from_catalog(&engine, tid);
        let (mut wp, sal, rx) = bench_worker(&mut engine);
        write(&sal, &relation, &schema, tid);
        let ((), instructions) = counter.measure(|| loop {
            wp.drain_sal();
            if wp.replies.is_empty() {
                break;
            }
        });
        let sent = frames(&rx).len();
        black_box(&wp);
        let (n, unit) = per(sent);
        println!(
            "worker_request_bench {label:<14} {:>9.1} instr/{unit}  ({sent} frames)",
            instructions as f64 / n as f64
        );
        drop(wp);
        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    };
    let per_group = |_: usize| (BENCH_GROUPS, "request");

    arm(
        "push",
        0,
        false,
        &|sal, relation, schema, _| {
            let excl = sal.excl();
            for i in 0..BENCH_GROUPS {
                let row = make_batch_raw(schema, &[(i, 1, i as i64)]);
                let targets = GroupTargets {
                    request_id: i as u32 + 1,
                    ..GroupTargets::UNADDRESSED
                };
                excl.write(&DirectGroup::push(relation, GroupData::Same(row.wire_whole()), targets))
                    .expect("group fits");
            }
        },
        &per_group,
    );
    for (label, cut) in [("has_pk", 1), ("has_pk_cut2", 2)] {
        arm(
            label,
            1024,
            false,
            &|sal, _, _, tid| {
                let excl = sal.excl();
                for i in 0..BENCH_GROUPS {
                    excl.write(&probe(tid, &pk_keys(&[i % 1024]), i as u32 + 1, i % cut != 0))
                        .expect("group fits");
                }
            },
            &per_group,
        );
    }
    arm(
        "scan_spec",
        1024,
        false,
        &|sal, _, schema, tid| {
            let excl = sal.excl();
            for i in 0..BENCH_GROUPS {
                let key = (i % 1024).to_be_bytes();
                let spec = ReadSpec::all_rows(ReadBound::PkSet(PkKeys::from_keys(8, [&key[..]]))).encode();
                let read = Read::ScanSpec {
                    tid,
                    reply_layout: schema.layout_digest(),
                    spec: spec.into(),
                };
                excl.write(&addressed(read, i as u32 + 1, false)).expect("group fits");
            }
        },
        &per_group,
    );
    arm(
        "tick",
        0,
        false,
        &|sal, _, _, tid| {
            let excl = sal.excl();
            for i in 0..BENCH_GROUPS {
                let tick = Apply::Tick {
                    first_round: i + 2,
                    tids: tid.to_le_bytes().to_vec().into(),
                };
                excl.write(&addressed(tick, i as u32 + 1, false)).expect("group fits");
            }
        },
        &per_group,
    );
    // One train over an indexed table of 64 chunks, per frame.
    arm(
        "key_spans",
        64 * BENCH_CHUNK_ROWS,
        true,
        &|sal, _, _, tid| {
            let cols = PkColList::from_slice(&[1]);
            sal.excl()
                .write(&addressed(Read::KeySpans { tid, cols }, 1, false))
                .expect("group fits");
        },
        &|frames| (frames as u64, "frame"),
    );
}
