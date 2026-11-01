use super::*;
use crate::catalog::SysFamily;
use crate::runtime::frame::WireMsg;
use crate::runtime::park::WorkerSet;
use crate::runtime::sal::fixtures::test_log;
use crate::runtime::sal::GroupTargets;
use crate::runtime::sal::{Cut, GroupData, SalGroup, SalRequest, SalWriter};
use crate::runtime::w2m::fixtures::id;
use crate::runtime::w2m::{self, RequestId, W2mReceiver};
use crate::test_support::{
    make_batch_bytes_raw, make_batch_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64, pk_only_schema,
    register_full_pk, register_identity_view, scan_all, table_engine, u64_i64_cols, weighted_rows,
};
use gnitz_store::read::Probe;
use gnitz_store::read::ReadRows;
use gnitz_wire::{peek_control_block, DecodedControl, PkColList, PkKeys, ReadBound, ReadSpec, TypeCode, WireStatus};
use gnitz_zset::algebra::ScatterPlan;
use gnitz_zset::schema::SchemaDescriptor;
use std::borrow::Cow;
use std::rc::Rc;

// -- fixtures ---------------------------------------------------------------

/// A worker over a real SAL and a real W2M ring, through the production
/// constructor so a new field cannot be missed here, with the receiver that
/// reads its ring. The budget is pinned rather than read from the environment,
/// so a shell that exports `GNITZ_REPLY_FRAME_BUDGET` does not reshape these
/// frames.
fn test_worker(catalog: &mut CatalogEngine) -> (WorkerProcess<'_>, SalWriter, W2mReceiver) {
    worker_over(catalog, 1 << 22, 1 << 20)
}

/// [`test_worker`] over a ring of `ring_bytes` and a SAL of `sal_bytes`.
pub(super) fn worker_over(
    catalog: &mut CatalogEngine,
    ring_bytes: usize,
    sal_bytes: usize,
) -> (WorkerProcess<'_>, SalWriter, W2mReceiver) {
    let (mut writers, receiver) = w2m::fixtures::test_rings(1, ring_bytes);
    let sal = test_log(sal_bytes, 1, 1);
    // Over the SAL's parks, as at boot: the worker sleeps on one word for both.
    let mesh = crate::runtime::mesh::create(1 << 16, sal.parks())
        .unwrap()
        .pop()
        .unwrap();
    let mut wp = WorkerProcess::new(catalog, SalReader::new(sal.log(), 0, 1), writers.pop().unwrap(), mesh);
    wp.reply_frame_budget = gnitz_wire::MAX_FRAME_PAYLOAD;
    (wp, sal, receiver)
}

/// The route of request `request_id`, written in cut `cut`.
fn route(target_id: u64, request_id: u32, cut: u64) -> ReplyRoute {
    ReplyRoute {
        target_id,
        request_id: id(request_id),
        cut: Cut::nth(cut),
    }
}

/// `request` answered on `request_id`, 0 for none.
pub(super) fn addressed<'a>(request: impl Into<SalRequest<'a>>, request_id: u32) -> SalGroup<'a> {
    SalGroup {
        targets: GroupTargets {
            reply: RequestId::from_wire(request_id),
            ..GroupTargets::SILENT
        },
        ..SalGroup::new(request)
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

/// An unbounded delta read of every round of `view` after tick 0, whose rows lay out as `schema`.
fn delta(view: u64, schema: &SchemaDescriptor) -> Read<'static> {
    Read::Delta {
        view,
        after_tick: 0,
        reply_layout: schema.layout_digest(),
        spec: ReadSpec::all_rows(ReadBound::None).encode().into(),
    }
}

/// `ids` as the keys a PK probe of a `U64`-keyed relation carries, encoded by
/// [`PkKeys::encode_probe`].
pub(super) fn pk_keys(ids: &[u64]) -> Vec<u8> {
    let keys: Vec<[u8; 8]> = ids.iter().map(|id| id.to_be_bytes()).collect();
    PkKeys::from_keys(8, keys.iter().map(|k| &k[..])).encode_probe()
}

/// A PK probe of `tid` at `keys`.
pub(super) fn probe(tid: u64, keys: &[u8], request_id: u32) -> SalGroup<'_> {
    addressed(Read::HasPk { tid, probe: Probe::Pk, keys: keys.into() }, request_id)
}

/// A push of `rows` into `tid`, whose schema record is `record`, ACKed on
/// `request_id`.
pub(super) fn push<'a>(tid: u64, record: &'a [u8], rows: &'a Batch, request_id: u32) -> SalGroup<'a> {
    let targets = GroupTargets {
        reply: RequestId::from_wire(request_id),
        ..GroupTargets::SILENT
    };
    SalGroup::push(tid, record, GroupData::Same(rows.wire_whole()), targets)
}

/// One published ring message: its ring-prefix request id, control block and
/// bytes.
pub(super) struct Frame {
    req: u32,
    /// Published as passing an owed reply.
    passed: bool,
    pub(super) ctrl: DecodedControl,
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
pub(super) fn frames(rx: &W2mReceiver) -> Vec<Frame> {
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
fn reqs(frames: &[Frame]) -> Vec<u32> {
    frames.iter().map(|f| f.req).collect()
}

/// What the worker owes, front first: each entry's shape and request id.
fn owed(wp: &WorkerProcess) -> Vec<(&'static str, u32)> {
    wp.replies
        .iter()
        .map(|owed| match owed {
            Owed::Parked(route, ..) => ("parked", route.request_id.get()),
            Owed::Fault(route, _) => ("fault", route.request_id.get()),
            Owed::Train(train) => ("train", train.route.request_id.get()),
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

/// Dispatch every group on the SAL as an exchange wait does: `dispatch(req, true)`.
fn drain_in_wait(wp: &mut WorkerProcess) {
    while let Some(req) = wp.next_request() {
        wp.dispatch(req, true);
    }
}

/// Wire size of `batch` sent whole as one frame.
fn frame_size(batch: &Batch) -> usize {
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

// -- dispatch ---------------------------------------------------------------

/// `dispatch(req, true)`, driven as the exchange wait's own drain loop drives it:
/// a read of a view parks in SAL order; a read of a base table, `Push` and
/// `Flush` answer from inside the wait; a DdlSync that addresses no worker is
/// stepped over; and a train stays unsent — a slow client can fill the ring, and
/// a worker blocked on it would stall the round every peer waits on.
#[test]
fn in_wait_dispositions() {
    let (mut engine, tid) = table_engine("worker", "in_wait_dispositions", 1, &u64_i64_cols());
    let vid = register_identity_view(&mut engine, tid, "v", &u64_i64_cols());
    let cols = PkColList::from_slice(&[1]);
    let claim = IndexClaim::Index { id: vid + 1, unique: false };
    engine.registry.add_indexes(tid, &[(claim, cols)]).unwrap();
    let schema = make_schema_u64_i64();
    let (mut wp, sal, rx) = test_worker(&mut engine);

    let spans = Read::KeySpans { tid, cols, claim: false };
    let ddl = SalGroup {
        targets: GroupTargets {
            set: WorkerSet::EMPTY,
            ..GroupTargets::SILENT
        },
        ..SalGroup::new(Apply::DdlSync { family: SysFamily::Sequence })
    };
    // A control-only push carries no rows, so it ACKs without reaching the
    // store. `Flush` goes last: the reader leaves the epoch on stepping past it.
    let groups = [
        addressed(scan(vid, &schema), 1),
        addressed(delta(vid, &schema), 2),
        addressed(scan(tid, &schema), 3),
        addressed(spans, 4),
        probe(tid, &[], 5),
        addressed(Apply::Push { tid }, 6),
        ddl,
        addressed(Apply::Flush, 7),
    ];
    for group in &groups {
        sal.excl().write(group).expect("group fits");
    }
    drain_in_wait(&mut wp);

    assert_eq!(owed(&wp), [("parked", 1), ("parked", 2), ("train", 4)]);
    // A parked read is answered after the drive reads past a `Flush`, which
    // lets the master rewrite the SAL bytes it was decoded from.
    for owed in &wp.replies {
        if let Owed::Parked(_, read) = owed {
            assert!(
                matches!(
                    **read,
                    Read::ScanSpec { spec: Cow::Owned(_), .. } | Read::Delta { spec: Cow::Owned(_), .. }
                ),
                "a parked read owns its bytes: {read:?}"
            );
        }
    }
    let out = frames(&rx);
    assert_eq!(reqs(&out), [3, 5]);
    assert_eq!(rx.acks(0), [6, 7], "Push and Flush ACK");
    let statuses: Vec<_> = out.iter().map(|f| f.ctrl.hdr.status).collect();
    assert_eq!(
        statuses,
        [WireStatus::Ok, WireStatus::Error],
        "the table's read and HasPk (a keyless probe faults) answer"
    );
    assert!(out[0].ctrl.hdr.flags.scan_last);
    let fault = out[1].ctrl.fault(&out[1].bytes).expect("a fault frame");
    assert!(fault.text.contains("pk_probe"), "{fault}");
}

/// One Tick group carrying two tids ticks both — each drains its buffered
/// delta — and ACKs once.
#[test]
fn a_two_tid_tick_group_ticks_both_and_acks_once() {
    let dir = crate::test_support::scratch_dir("worker", "two_tid_tick");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let schema = make_schema_u64_i64();
    for tid in [500, 501] {
        register_full_pk(
            &mut engine.registry,
            tid,
            gnitz_store::relation::RelationKind::BaseTable,
            schema,
        );
        engine
            .registry
            .ingest_pending(tid, make_batch_raw(&schema, &[(tid, 1, 10)]))
            .unwrap();
    }
    let (mut wp, sal, rx) = test_worker(&mut engine);

    let tick = Apply::Tick {
        first_round: 7,
        tids: vec![500, 501].into(),
    };
    sal.excl().write(&addressed(tick, 9)).expect("group fits");
    wp.drain_sal();

    for tid in [500, 501] {
        assert!(wp.catalog.registry.seal(tid).unwrap().is_none(), "tid {tid} was ticked");
    }
    assert_eq!(rx.acks(0), [9], "the group ACKs once");
    assert!(frames(&rx).is_empty(), "an ACK takes no reply slot");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A base-table read drained inside an exchange wait answers at its place in the
/// SAL: the push behind it is applied, and is not in the reply.
#[test]
fn a_push_does_not_reach_a_base_table_read_ahead_of_it() {
    let (mut engine, tid) = table_engine("worker", "push_behind_read", 1, &u64_i64_cols());
    let schema = make_schema_u64_i64();
    let record = engine.wire_target(tid).unwrap().1.clone();
    let row = make_batch_raw(&schema, &[(1, 1, 10)]);
    let (mut wp, sal, rx) = test_worker(&mut engine);

    sal.excl().write(&addressed(scan(tid, &schema), 1)).expect("group fits");
    sal.excl().write(&push(tid, &record, &row, 2)).expect("group fits");
    drain_in_wait(&mut wp);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [1], "the read answers");
    assert_eq!(rx.acks(0), [2], "the push ACKs");
    assert!(out[0].ctrl.hdr.flags.scan_last);
    assert!(out[0].rows(&schema).is_empty(), "the read was written before the push");
    assert_eq!(weighted_rows(&scan_all(wp.catalog, tid)), weighted_rows(&row));
}

/// A view read drained inside an exchange wait parks, and the cut member behind
/// it — a base-table read, answered in the wait — stays behind it: the view's
/// reply reaches the ring first.
#[test]
fn a_parked_view_read_answers_ahead_of_the_cut_member_behind_it() {
    let (mut engine, tid) = table_engine("worker", "parked_view_read", 1, &u64_i64_cols());
    let vid = register_identity_view(&mut engine, tid, "v", &u64_i64_cols());
    let schema = make_schema_u64_i64();
    let held = make_batch_raw(&schema, &[(1, 1, 10)]);
    engine.registry.ingest(tid, held.clone()).unwrap();
    let (mut wp, sal, rx) = test_worker(&mut engine);

    {
        let mut excl = sal.excl();
        excl.write(&addressed(scan(vid, &schema), 1)).expect("group fits");
        excl.join(&addressed(scan(tid, &schema), 2)).expect("group fits");
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
    let (mut engine, tid) = table_engine("worker", "burst", 1, &u64_i64_cols());
    let vid = register_identity_view(&mut engine, tid, "v", &u64_i64_cols());
    let schema = make_schema_u64_i64();
    let (mut wp, sal, rx) = test_worker(&mut engine);

    let keys = pk_keys(&[1]);
    sal.excl()
        .write(&addressed(delta(vid, &schema), 1))
        .expect("group fits");
    {
        let mut excl = sal.excl();
        excl.write(&probe(tid, &keys, 2)).expect("group fits");
        excl.join(&probe(tid, &keys, 3)).expect("group fits");
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
    let (mut engine, tid) = table_engine("worker", "round_first", 1, &u64_i64_cols());
    let schema = make_schema_u64_i64();
    let (mut wp, sal, rx) = test_worker(&mut engine);
    for request_id in [1, 2] {
        sal.excl()
            .write(&addressed(scan(tid, &schema), request_id))
            .expect("group fits");
    }

    // One worker: its own publish completes the round.
    let part = make_batch_raw(&schema, &[(1, 1, 10)]);
    let (got, _) = wp.exchange(9, Cow::Borrowed(&part), &ScatterPlan::broadcast(), false, false);
    assert_eq!(weighted_rows(&got), weighted_rows(&part));
    assert_eq!(reqs(&frames(&rx)), [1]);
    let next = wp.next_request().expect("the second read is still on the SAL");
    assert!(matches!(next, Inbound::Read { route, .. } if route.request_id == id(2)));
}

/// A round is opened with the drive's own claim, and its close answers every
/// worker's: a peer whose source is not drained overturns it.
#[test]
fn exchange_answers_the_rounds_verdict() {
    let (mut engine, _) = table_engine("worker", "exchange_verdict", 1, &u64_i64_cols());
    let (mut wp, _sal, _rx) = test_worker(&mut engine);
    let part = make_batch_raw(&make_schema_u64_i64(), &[(1, 1, 10)]);
    let plan = ScatterPlan::broadcast();
    for (own, peers, all) in [(true, true, true), (true, false, false), (false, true, false)] {
        let parks = crate::runtime::park::WorkerParks::create(2).unwrap();
        let mut meshes = crate::runtime::mesh::create(1 << 16, parks).unwrap();
        let mut peer = meshes.pop().unwrap();
        wp.mesh = meshes.pop().unwrap();
        // The peer arrives first, so this worker's own open completes the round.
        let _peers_round = peer.open(9, Cow::Borrowed(&part), &plan, false, peers);
        let (_, drained) = wp.exchange(9, Cow::Borrowed(&part), &plan, false, own);
        assert_eq!(drained, all, "own claim {own}, the peer's {peers}");
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

    let dir = crate::test_support::scratch_dir("worker", "reply_trains");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (mut wp, _sal, rx) = test_worker(&mut engine);
    wp.reply_frame_budget = budget;
    wp.reply(route(1, 11, 1), Ok(Rows::from(a.clone())));
    wp.reply(route(2, 22, 2), Ok(Rows::from(b.clone())));
    // Cut 3: a train, then two members that fit and stay behind it.
    wp.reply(route(3, 33, 3), Ok(Rows::from(c.clone())));
    wp.reply(route(5, 55, 3), Ok(Rows::from(one.clone())));
    wp.reply(route(6, 66, 3), Ok(Rows::from(empty.clone())));
    // Cut 4 owes nothing: both members pass the queue.
    wp.reply(route(4, 44, 4), Ok(Rows::from(one.clone())));
    wp.reply(route(4, 45, 4), Ok(Rows::from(one.clone())));

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
    wp.reply(route(1, 5, 1), Ok(Rows::from(ReadRows::Shared(Rc::clone(&batch)))));
    assert!(wp.replies.is_empty(), "the first member of its cut leaves at once");
    // Behind a fault its own cut owes, the same reply waits in the queue.
    wp.replies.push_back(Owed::Fault(route(1, 6, 2), "boom".into()));
    wp.reply(route(1, 7, 2), Ok(Rows::from(ReadRows::Shared(Rc::clone(&batch)))));
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
    wp.reply(route(1, 5, 1), Ok(Rows::from(batch)));
    wp.reply(route(2, 6, 1), Ok(Rows::from(next)));
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

    wp.reply(route(1, 11, 1), Ok(Rows::from(rows)));
    wp.reply(route(1, 12, 1), Err("of the train's cut".into()));
    wp.reply(route(1, 13, 2), Err("of another cut".into()));
    let passing = frames(&rx);
    assert_eq!(reqs(&passing), [13], "another cut's fault leaves at once");
    assert!(passing[0].passed, "published as passing the owed train");
    assert_eq!(owed(&wp), [("train", 11), ("fault", 12)]);

    drain_replies(&mut wp);
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
    let dir = crate::test_support::scratch_dir("worker", "full_ring");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (mut wp, _sal, rx) = worker_over(&mut engine, 4096, 1 << 20);
    while wp.w2m_writer.try_send_msg(id(1), false, &WireMsg::default()) {}

    wp.reply(route(1, 5, 1), Ok(Rows::from(one.clone())));
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
    let (mut engine, tid) = table_engine("worker", "train_advances", 1, &u64_i64_cols());
    let (mut wp, sal, rx) = test_worker(&mut engine);
    wp.reply_frame_budget = frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 4]));
    wp.reply(route(1, 11, 1), Ok(Rows::from(rows)));

    for _ in 0..GROUPS {
        // Control-only and unanswered: nothing of its own reaches the ring.
        sal.excl()
            .write(&SalGroup::new(Apply::Push { tid }))
            .expect("group fits");
    }
    wp.drain_sal();
    assert_eq!(reqs(&frames(&rx)), [11; GROUPS + 1]);
    assert_eq!(owed(&wp), [("train", 11)], "ten frames, six sent");
}

/// The pre-flight's span read enters the index under its claim, filled from the
/// rows already held, and answers off it; its release drops the index no other
/// claim holds.
#[test]
fn a_claiming_span_read_holds_its_index_until_its_release() {
    let (mut engine, tid) = table_engine("worker", "preflight_claim", 1, &u64_i64_cols());
    let held: Vec<_> = (0..5).map(|i| (i, 1, -(i as i64))).collect();
    engine
        .registry
        .ingest(tid, make_batch_raw(&make_schema_u64_i64(), &held))
        .unwrap();
    let cols = PkColList::from_slice(&[1]);
    let (mut wp, sal, rx) = test_worker(&mut engine);
    let indexed = |wp: &WorkerProcess| {
        wp.catalog
            .registry
            .relation(tid)
            .unwrap()
            .index_on(cols.as_slice())
            .is_some()
    };

    let spans = Read::KeySpans { tid, cols, claim: true };
    sal.excl().write(&addressed(spans, 1)).expect("group fits");
    wp.drain_sal();
    drain_replies(&mut wp);
    assert!(indexed(&wp));
    let span_schema = pk_only_schema(&[TypeCode::I64]);
    let out = frames(&rx);
    assert!(reqs(&out).iter().all(|&r| r == 1));
    assert_eq!(
        out.iter().map(|f| f.rows(&span_schema).len()).sum::<usize>(),
        held.len()
    );

    sal.excl()
        .write(&addressed(Read::ReleasePreflight, 2))
        .expect("group fits");
    wp.drain_sal();
    assert!(!indexed(&wp));
    assert_eq!(reqs(&frames(&rx)), [2]);
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
    let dir = crate::test_support::scratch_dir("worker", "shortage");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (mut wp, _sal, rx) = worker_over(&mut engine, 4096, 1 << 20);
    wp.reply_frame_budget = frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 16]));

    wp.reply(route(1, 11, 1), Ok(Rows::from(rows)));
    while !wp.w2m_writer.short_of_room() {
        wp.emit_reply_frame();
    }
    assert!(!rx.answer_shortage(0), "a train held up by its own frames asks nothing");

    // As large as the train's frame the ring just refused.
    let behind = make_batch_raw(&fixed, &(0..16).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    wp.reply(route(2, 22, 2), Ok(Rows::from(behind)));
    assert_eq!(owed(&wp), [("train", 11), ("train", 22)]);
    assert!(
        rx.answer_shortage(0),
        "a reply of another cut behind them asks for the ring"
    );
    wp.w2m_writer.name_shortage();
    assert!(!rx.answer_shortage(0), "once per release");
}

/// A train the master holds sends nothing and holds up only its own cut: the
/// first sendable reply behind it goes, and with every reply held the worker
/// waits on the master, until the hold ends.
#[test]
fn a_held_train_steps_aside_until_its_hold_ends() {
    let fixed = make_schema_u64_i64();
    let rows = make_batch_raw(&fixed, &(0..100).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    let dir = crate::test_support::scratch_dir("worker", "held_train");
    let mut engine = CatalogEngine::open(&dir, 1).expect("open catalog");
    let (mut wp, _sal, rx) = test_worker(&mut engine);
    wp.reply_frame_budget = frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 16]));

    wp.reply(route(1, 11, 1), Ok(Rows::from(rows.clone())));
    wp.reply(route(1, 12, 1), Ok(Rows::from(rows)));
    wp.replies.push_back(Owed::Fault(route(2, 21, 2), "boom".into()));
    assert!(rx.hold(0, id(11)));
    wp.emit_reply_frame();
    assert_eq!(
        reqs(&frames(&rx)),
        [21],
        "the next cut's reply goes past the held train"
    );
    wp.emit_reply_frame();
    assert!(frames(&rx).is_empty(), "nor does the train behind it in its cut");
    assert!(wp.waits_on_master());

    rx.unhold(0, id(11));
    assert!(!wp.waits_on_master());
    drain_replies(&mut wp);
    let mut order = reqs(&frames(&rx));
    order.dedup();
    assert_eq!(order, [11, 12]);
}
