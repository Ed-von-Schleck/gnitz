use super::reply::tests::{drain, frame_size, frames, owed, reqs, route};
use super::*;
use crate::catalog::{SysFamily, SysFamilyExt};
use crate::query::{DriveHost, Lane};
use crate::runtime::park::WorkerSet;
use crate::runtime::sal::fixtures::test_log;
use crate::runtime::sal::{Routed, SalExcl, SalFull, SalWriter};
use crate::runtime::w2m::fixtures::id;
use crate::runtime::w2m::{self, W2mReceiver};
use crate::test_support::{
    make_batch_raw, make_schema_u64_i64, pk_only_schema, register_count_of, register_full_pk, register_identity_view,
    scan_all, table_engine, u64_i64_cols, weighted_rows,
};
use gnitz_store::read::Probe;
use gnitz_store::relation::PkRule;
use gnitz_wire::{PkColList, ReadBound, ReadSpec, TypeCode, WireStatus};
use gnitz_zset::algebra::ScatterPlan;
use gnitz_zset::schema::SchemaDescriptor;
use std::borrow::Cow;

// -- fixtures ---------------------------------------------------------------

/// A worker over a real SAL and a real W2M ring, through the production
/// constructor so a new field cannot be missed here, with the receiver that
/// reads its ring.
fn test_worker(catalog: &mut CatalogEngine) -> (WorkerProcess<'_>, SalWriter, W2mReceiver) {
    worker_over(catalog, 1 << 22, 1 << 20)
}

/// [`test_worker`] over a ring of `ring_bytes` and a SAL of `sal_bytes`.
pub(super) fn worker_over(
    catalog: &mut CatalogEngine,
    ring_bytes: usize,
    sal_bytes: usize,
) -> (WorkerProcess<'_>, SalWriter, W2mReceiver) {
    let sal = test_log(sal_bytes, 1, 1);
    // Over the SAL's parks, as at boot: the worker sleeps on one word for all three.
    let (mut writers, receiver) = w2m::fixtures::test_rings_over(sal.parks(), ring_bytes);
    let mesh = crate::runtime::mesh::create(1 << 16, sal.parks())
        .unwrap()
        .pop()
        .unwrap();
    let wp = WorkerProcess::new(catalog, sal.reader(0), writers.pop().unwrap(), mesh);
    (wp, sal, receiver)
}

/// Write `read` to every worker, answered on `request_id`.
pub(super) fn ask(excl: &mut SalExcl, read: Read<'_>, request_id: u32) -> Result<(), SalFull> {
    excl.read(read, WorkerSet::ALL, None, id(request_id))
}

/// The encoded spec of an unbounded read of every row.
fn all_rows() -> Vec<u8> {
    ReadSpec::all_rows(ReadBound::None).encode()
}

/// A read of `tid` under `spec`, whose rows lay out as `schema`.
fn scan<'a>(tid: u64, schema: &SchemaDescriptor, spec: &'a [u8]) -> Read<'a> {
    Read::ScanSpec {
        tid,
        reply_layout: schema.layout_digest(),
        spec,
    }
}

/// A delta read of every round of `view` after tick 0 under `spec`, whose rows lay out as `schema`.
fn delta<'a>(view: u64, schema: &SchemaDescriptor, spec: &'a [u8]) -> Read<'a> {
    Read::Delta {
        view,
        after_tick: 0,
        reply_layout: schema.layout_digest(),
        spec,
    }
}

/// `ids` as the keys a PK probe of a `U64`-keyed relation carries, encoded by
/// [`gnitz_wire::PkKeys::encode_probe`].
pub(super) fn pk_keys(ids: &[u64]) -> Vec<u8> {
    let keys: Vec<[u8; 8]> = ids.iter().map(|id| id.to_be_bytes()).collect();
    gnitz_zset::schema::pk_keys(8, keys.iter().map(|k| &k[..])).encode_probe()
}

/// A PK probe of `tid` at `keys`.
pub(super) fn probe(tid: u64, keys: &[u8]) -> Read<'_> {
    Read::HasPk { tid, probe: Probe::Pk, keys }
}

/// Serve every group on the SAL as an exchange wait whose round is still out
/// does.
fn drain_in_wait(wp: &mut WorkerProcess) {
    wp.drain_sal(true);
}

// -- dispatch ---------------------------------------------------------------

/// The drain of an exchange wait whose round is still out: a read of a view
/// parks in SAL order; a read of a base table answers and a push is applied
/// from inside the wait; a DdlSync that addresses no worker is stepped over;
/// and a train sends a frame after the group that queued it.
#[test]
fn in_wait_dispositions() {
    let (_dir, mut engine, tid) = table_engine(1, &u64_i64_cols());
    let vid = register_identity_view(&mut engine, tid, "v", &u64_i64_cols());
    let cols = PkColList::from_slice(&[1]);
    let claim = IndexClaim::Index { id: vid + 1, unique: false };
    engine.registry.add_indexes(tid, &[(claim, cols)]).unwrap();
    let schema = make_schema_u64_i64();
    let spec = all_rows();
    let record = engine.shape_target(tid).unwrap().1.to_vec();
    let row = make_batch_raw(&schema, &[(1, 1, 10)]);
    let (mut wp, sal, rx) = test_worker(&mut engine);

    let spans = Read::KeySpans { tid, cols, claim: false };
    // Each on an id of its own: no two reads share a lease.
    let reads = [
        scan(vid, &schema, &spec),
        delta(vid, &schema, &spec),
        scan(tid, &schema, &spec),
        spans,
        probe(tid, &[]),
    ];
    for (read, request_id) in reads.into_iter().zip(1..) {
        ask(&mut sal.excl(), read, request_id).expect("group fits");
    }
    sal.excl()
        .push(tid, &record, Routed::Same(row.wire_whole()))
        .expect("group fits");
    sal.ddl_zone(&[(
        SysFamily::Sequence,
        &Batch::empty_with_schema(SysFamily::Sequence.schema()),
    )]);
    drain_in_wait(&mut wp);

    assert_eq!(owed(&wp.replies), [("parked", 1), ("parked", 2)]);
    let out = frames(&rx);
    assert_eq!(reqs(&out), [3, 4, 5]);
    assert_eq!(rx.lowest_applied(), 0, "nothing served in a wait is ACKed");
    assert_eq!(
        weighted_rows(&scan_all(wp.catalog, tid)),
        weighted_rows(&row),
        "the push is applied"
    );
    let statuses: Vec<_> = out.iter().map(|f| f.ctrl.hdr.status).collect();
    assert_eq!(
        statuses,
        [WireStatus::Ok, WireStatus::Ok, WireStatus::Error],
        "the table's read, KeySpans and HasPk (a keyless probe faults) answer"
    );
    assert!(out[0].ctrl.hdr.flags.scan_last);
    assert!(out[1].ctrl.hdr.flags.scan_last, "KeySpans answers in one frame");
    let fault = out[2].ctrl.fault(&out[2].bytes).expect("a fault frame");
    assert!(fault.text.contains("pk_probe"), "{fault}");
}

/// One Tick group carrying two tids ticks both — each drains its buffered
/// delta — and is ACKed at the position its write answered.
#[test]
fn a_two_tid_tick_group_ticks_both_and_acks_its_end() {
    let (_dir, mut engine) = crate::test_support::empty_catalog(1);
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
            .ingest_with(tid, make_batch_raw(&schema, &[(tid, 1, 10)]), PkRule::Enforce, true)
            .unwrap();
    }
    let (mut wp, sal, rx) = test_worker(&mut engine);

    let tick = Apply::Tick {
        first_round: 7,
        tids: vec![500, 501].into(),
    };
    let at = sal.excl().apply(tick).expect("group fits");
    wp.drain_sal(false);

    for tid in [500, 501] {
        assert!(wp.catalog.registry.seal(tid).unwrap().is_none(), "tid {tid} was ticked");
    }
    assert_eq!(rx.lowest_applied(), at, "the group is ACKed at its end");
    assert!(frames(&rx).is_empty(), "an ACK takes no reply slot");
}

/// A base-table read drained inside an exchange wait answers at its place in the
/// SAL: the push behind it is applied, and is not in the reply.
#[test]
fn a_push_does_not_reach_a_base_table_read_ahead_of_it() {
    let (_dir, mut engine, tid) = table_engine(1, &u64_i64_cols());
    let schema = make_schema_u64_i64();
    let spec = all_rows();
    let record = engine.shape_target(tid).unwrap().1.to_vec();
    let row = make_batch_raw(&schema, &[(1, 1, 10)]);
    let (mut wp, sal, rx) = test_worker(&mut engine);

    ask(&mut sal.excl(), scan(tid, &schema, &spec), 1).expect("group fits");
    sal.excl()
        .push(tid, &record, Routed::Same(row.wire_whole()))
        .expect("group fits");
    drain_in_wait(&mut wp);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [1], "the read answers");
    assert!(out[0].ctrl.hdr.flags.scan_last);
    assert!(out[0].rows(&schema).is_empty(), "the read was written before the push");
    assert_eq!(weighted_rows(&scan_all(wp.catalog, tid)), weighted_rows(&row));
}

/// A view read drained inside an exchange wait parks, and the read behind it
/// on its lease — a base-table read, answered in the wait — stays behind it:
/// the view's reply reaches the ring first.
#[test]
fn a_parked_view_read_answers_ahead_of_the_read_behind_it_on_its_lease() {
    let (_dir, mut engine, tid) = table_engine(1, &u64_i64_cols());
    let vid = register_identity_view(&mut engine, tid, "v", &u64_i64_cols());
    let schema = make_schema_u64_i64();
    let spec = all_rows();
    let held = make_batch_raw(&schema, &[(1, 1, 10)]);
    engine.registry.ingest(tid, held.clone()).unwrap();
    let (mut wp, sal, rx) = test_worker(&mut engine);

    {
        let mut excl = sal.excl();
        ask(&mut excl, scan(vid, &schema, &spec), 1).expect("group fits");
        ask(&mut excl, scan(tid, &schema, &spec), 1).expect("group fits");
    }
    drain_in_wait(&mut wp);
    assert_eq!(owed(&wp.replies), [("parked", 1), ("train", 1)]);
    assert!(frames(&rx).is_empty(), "nothing of the lease passes its parked read");

    wp.replies
        .answer_parked(|read| answer(wp.catalog, &mut wp.snapshots, read));
    assert_eq!(owed(&wp.replies), [("train", 1)]);
    drain(&mut wp.replies);
    let out = frames(&rx);
    assert_eq!(reqs(&out), [1, 1]);
    assert!(out.iter().all(|f| f.ctrl.hdr.flags.scan_last), "one frame per read");
    assert!(out[0].rows(&schema).is_empty(), "no tick has fed the view");
    assert_eq!(weighted_rows(&out[1].rows(&schema)), weighted_rows(&held));
}

/// Every probe of a validation burst answers from inside an exchange wait,
/// past what another lease owes.
#[test]
fn a_burst_passes_what_another_lease_owes() {
    let (_dir, mut engine, tid) = table_engine(1, &u64_i64_cols());
    let vid = register_identity_view(&mut engine, tid, "v", &u64_i64_cols());
    let schema = make_schema_u64_i64();
    let spec = all_rows();
    let (mut wp, sal, rx) = test_worker(&mut engine);

    let keys = pk_keys(&[1]);
    ask(&mut sal.excl(), delta(vid, &schema, &spec), 1).expect("group fits");
    {
        let mut excl = sal.excl();
        ask(&mut excl, probe(tid, &keys), 2).expect("group fits");
        ask(&mut excl, probe(tid, &keys), 2).expect("group fits");
    }
    drain_in_wait(&mut wp);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [2, 2]);
    assert!(
        out.iter()
            .all(|f| f.ctrl.hdr.status == WireStatus::Ok && f.ctrl.hdr.flags.scan_last)
    );
    assert_eq!(owed(&wp.replies), [("parked", 1)]);
}

/// The wait looks at its round after every group: with the round complete, the
/// drive goes on after one, sends no frame of the train it queued, and leaves
/// the next group on the SAL.
#[test]
fn a_complete_round_goes_ahead_of_the_reads_on_the_sal() {
    let (_dir, mut engine, tid) = table_engine(1, &u64_i64_cols());
    let cols = PkColList::from_slice(&[1]);
    let claim = IndexClaim::Index { id: tid + 1, unique: false };
    engine.registry.add_indexes(tid, &[(claim, cols)]).unwrap();
    let schema = make_schema_u64_i64();
    let spec = all_rows();
    let (mut wp, sal, rx) = test_worker(&mut engine);
    ask(&mut sal.excl(), Read::KeySpans { tid, cols, claim: false }, 1).expect("group fits");
    ask(&mut sal.excl(), scan(tid, &schema, &spec), 2).expect("group fits");

    // One worker: its own publish completes the round.
    let part = make_batch_raw(&schema, &[(1, 1, 10)]);
    let plan = ScatterPlan::broadcast();
    let lane = Lane {
        view: 9,
        batch: Cow::Borrowed(&part),
        plan: &plan,
        fold: false,
    };
    let mut got = Vec::new();
    wp.exchange(std::iter::once(lane), false, &mut got);
    assert_eq!(weighted_rows(&got[0]), weighted_rows(&part));
    assert_eq!(owed(&wp.replies), [("train", 1)]);
    assert!(frames(&rx).is_empty(), "no frame leaves once the round is complete");
    let next = wp.sal_reader.next_request(|_, _| None);
    assert!(
        matches!(next, Some(Inbound::Read { route, .. }) if route.request_id == id(2)),
        "the second read is still on the SAL"
    );
}

/// A round is opened with the drive's own claim, and its close answers every
/// worker's: a peer whose source is not drained overturns it.
#[test]
fn exchange_answers_the_rounds_verdict() {
    let (_dir, mut engine, _) = table_engine(1, &u64_i64_cols());
    let (mut wp, _sal, _rx) = test_worker(&mut engine);
    let part = make_batch_raw(&make_schema_u64_i64(), &[(1, 1, 10)]);
    let plan = ScatterPlan::broadcast();
    for (own, peers, all) in [(true, true, true), (true, false, false), (false, true, false)] {
        let parks = crate::runtime::park::WorkerParks::create(2).unwrap();
        let mut meshes = crate::runtime::mesh::create(1 << 16, parks).unwrap();
        let mut peer = meshes.pop().unwrap();
        wp.mesh = meshes.pop().unwrap();
        // The peer arrives first, so this worker's own open completes the round.
        let _peers_round = peer.open_one(9, Cow::Borrowed(&part), &plan, false, peers);
        let lane = Lane {
            view: 9,
            batch: Cow::Borrowed(&part),
            plan: &plan,
            fold: false,
        };
        let drained = wp.exchange(std::iter::once(lane), own, &mut Vec::new());
        assert_eq!(drained, all, "own claim {own}, the peer's {peers}");
    }
}

/// A queued train sends a frame at the top of a drain and one after every
/// request the drain serves, however many the SAL holds.
#[test]
fn a_train_advances_with_every_request() {
    const GROUPS: usize = 5;
    let (_dir, mut engine, tid) = table_engine(1, &u64_i64_cols());
    let (mut wp, sal, rx) = test_worker(&mut engine);
    queue_train(&mut wp);

    for _ in 0..GROUPS {
        // Control-only and unanswered: nothing of its own reaches the ring.
        sal.excl().push(tid, &[], Routed::Same(None)).expect("group fits");
    }
    wp.drain_sal(false);
    assert_eq!(reqs(&frames(&rx)), [11; GROUPS + 1]);
    assert_eq!(owed(&wp.replies), [("train", 11)], "ten frames, six sent");
}

/// A train queued for a 40-row reply, cut into frames of four rows.
fn queue_train(wp: &mut WorkerProcess) {
    let fixed = make_schema_u64_i64();
    let rows = make_batch_raw(&fixed, &(0..40).map(|i| (i, 1, i as i64)).collect::<Vec<_>>());
    wp.replies
        .set_frame_budget(frame_size(&make_batch_raw(&fixed, &[(0, 1, 0); 4])));
    wp.replies.reply(route(1, 11), Ok(Rows::from(rows)));
}

/// An exchange wait whose round is still out sends a frame of a queued train
/// with nothing on the SAL.
#[test]
fn a_train_advances_in_a_wait_with_nothing_on_the_sal() {
    let (_dir, mut engine, _) = table_engine(1, &u64_i64_cols());
    let (mut wp, _sal, rx) = test_worker(&mut engine);
    queue_train(&mut wp);
    assert!(frames(&rx).is_empty(), "the reply is owed");

    drain_in_wait(&mut wp);
    assert_eq!(reqs(&frames(&rx)), [11]);
    assert_eq!(owed(&wp.replies), [("train", 11)]);
}

/// An exchange wait with a seal of its `Tick` group still ahead sends a frame
/// of a queued train and serves none of the SAL: the push stays on it.
#[test]
fn a_wait_behind_a_seal_sends_but_serves_nothing() {
    let (_dir, mut engine, tid) = table_engine(1, &u64_i64_cols());
    let schema = make_schema_u64_i64();
    let record = engine.shape_target(tid).unwrap().1.to_vec();
    let row = make_batch_raw(&schema, &[(1, 1, 10)]);
    let (mut wp, sal, rx) = test_worker(&mut engine);
    queue_train(&mut wp);
    sal.excl()
        .push(tid, &record, Routed::Same(row.wire_whole()))
        .expect("group fits");

    wp.seals_ahead = true;
    drain_in_wait(&mut wp);
    assert_eq!(reqs(&frames(&rx)), [11]);
    assert!(
        weighted_rows(&scan_all(wp.catalog, tid)).is_empty(),
        "the push is not applied"
    );
    assert!(!wp.sal_reader.is_empty(), "the push is still on the SAL");
}

/// The pre-flight's span read enters the index under its claim, filled from the
/// rows already held, and answers off it; its release drops the index no other
/// claim holds.
#[test]
fn a_claiming_span_read_holds_its_index_until_its_release() {
    let (_dir, mut engine, tid) = table_engine(1, &u64_i64_cols());
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
    ask(&mut sal.excl(), spans, 1).expect("group fits");
    wp.drain_sal(false);
    drain(&mut wp.replies);
    assert!(indexed(&wp));
    let span_schema = pk_only_schema(&[TypeCode::I64]);
    let out = frames(&rx);
    assert!(reqs(&out).iter().all(|&r| r == 1));
    assert_eq!(
        out.iter().map(|f| f.rows(&span_schema).len()).sum::<usize>(),
        held.len()
    );

    ask(&mut sal.excl(), Read::ReleasePreflight, 2).expect("group fits");
    wp.drain_sal(false);
    assert!(!indexed(&wp));
    assert_eq!(reqs(&frames(&rx)), [2]);
}

/// A push written behind a Tick group of two sources stays out of that group's
/// ticks: the first source's exchange wait serves none of the SAL, so the view
/// read written ahead of the push does not see it, and it is still pending.
#[test]
fn a_push_behind_a_two_source_tick_waits_for_the_next() {
    let cols = u64_i64_cols();
    let schema = make_schema_u64_i64();
    let spec = all_rows();
    let (_dir, mut engine, t1) = table_engine(4, &cols);
    let t2 = engine.create_table("public.t2", &cols, &[0]).unwrap();
    // A tick of `t1` waits on an exchange round, and on another a wave later.
    let (v1, counts) = register_count_of(&mut engine, t1, &schema, &[1], "v1");
    register_count_of(&mut engine, v1, counts.output_schema(), &[], "total");
    let v2 = register_identity_view(&mut engine, t2, "v2", &cols);
    let record = engine.shape_target(t2).unwrap().1.to_vec();
    let row = make_batch_raw(&schema, &[(1, 1, 10)]);
    engine
        .registry
        .ingest_with(t1, make_batch_raw(&schema, &[(7, 1, 70)]), PkRule::Enforce, true)
        .unwrap();
    let (mut wp, sal, rx) = test_worker(&mut engine);

    let tick = Apply::Tick {
        first_round: 2,
        tids: vec![t1, t2].into(),
    };
    sal.excl().apply(tick).expect("group fits");
    ask(&mut sal.excl(), scan(v2, &schema, &spec), 2).expect("group fits");
    sal.excl()
        .push(t2, &record, Routed::Same(row.wire_whole()))
        .expect("group fits");
    wp.drain_sal(false);
    drain(&mut wp.replies);

    let out = frames(&rx);
    assert_eq!(reqs(&out), [2]);
    assert!(
        out[0].rows(&schema).is_empty(),
        "the view read was written before the push"
    );
    let pending = wp
        .catalog
        .registry
        .seal(t2)
        .unwrap()
        .expect("the push is still pending");
    assert_eq!(weighted_rows(&pending), weighted_rows(&row));
}
