#![cfg(feature = "integration")]

//! `create_view_chain` — atomic multi-view bundle creation and the distributed
//! backfill tail's dependency-order sort. These drive the client API directly
//! with hand-built circuits (independent of the not-yet-built chain planner), so
//! the bundle mechanics and the row-order-independent backfill are exercised on
//! their own.
//!
//! Each segment is a minimal identity view, either `input_delta → shard → sink`
//! (an exchanging one, which runs a cross-worker barrier per backfill chunk) or
//! the shard-free `input_delta → sink`. Both take the same distributed backfill
//! tail — there is one driver for every view — and the dependency-order sort
//! governs it. A downstream segment reads an upstream segment's store, so a
//! bundle whose VIEW_TAB rows are deliberately misordered (consumer before
//! producer) still backfills correctly only if the tail re-derives the order;
//! that is pinned for both segment shapes, since the shard-free one has no
//! barrier and stops on local drain exhaustion instead.
//!
//! A bundle carries its internal segments first and the user-named view last;
//! `gnitz_core::segment_name` names the rest.

use gnitz_core::{CircuitBuilder, ColumnDef, GnitzClient, PlannedView};
use gnitz_sql::GnitzSqlError;
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// A minimal exchanging identity view over `source_id`, keyed on PK col 0:
/// `input_delta → shard([0]) → sink`. Its backfill runs a cross-worker exchange
/// round per chunk.
fn identity_exchange_circuit(source_id: u64) -> gnitz_core::Circuit {
    let mut cb = CircuitBuilder::new(source_id);
    let inp = cb.input_delta();
    let sh = cb.shard(inp, &[0]);
    cb.sink(sh);
    cb.build()
}

/// A minimal shard-free identity view over `source_id`: `input_delta → sink`,
/// the shape a plain projection/filter compiles to. Its backfill runs no
/// exchange, so each worker terminates on its own drain exhaustion.
fn identity_linear_circuit(source_id: u64) -> gnitz_core::Circuit {
    let mut cb = CircuitBuilder::new(source_id);
    let inp = cb.input_delta();
    cb.sink(inp);
    cb.build()
}

/// Base table `base(pk BIGINT PK, v BIGINT)` pre-populated with `(1,10),(2,20),
/// (3,30)`; returns `(tid, its column defs)`.
fn make_base(client: &mut GnitzClient, sn: &str) -> (u64, Vec<ColumnDef>) {
    exec(
        client,
        sn,
        "CREATE TABLE base (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(client, sn, "INSERT INTO base VALUES (1, 10), (2, 20), (3, 30)");
    let (tid, schema) = client.resolve_table_id(sn, "base").unwrap();
    (tid, schema.columns.clone())
}

/// One identity segment at chain-local slot `seg` reading `source_id`.
fn segment(
    circuit_of: fn(source_id: u64) -> gnitz_core::Circuit,
    seg: u32,
    source_id: u64,
    cols: &[ColumnDef],
) -> PlannedView {
    PlannedView {
        seg,
        circuit: circuit_of(source_id),
        output_columns: cols.to_vec(),
        pk_cols: vec![0],
        capacity_bytes: None,
        delta_bytes: None,
    }
}

/// Build two chained identity views `h → f` (f reads h reads base) as
/// `PlannedView`s: `h` is the internal segment, `f` the user-named view.
/// `cols` is the shared (base = view) column layout, pk at 0.
fn plan_chain(base_tid: u64, cols: &[ColumnDef]) -> [PlannedView; 2] {
    plan_chain_with(identity_exchange_circuit, base_tid, cols)
}

/// `plan_chain` with the segments' circuit shape chosen by the caller. The
/// upstream segment takes chain-local slot 1 and the final view slot 0, so the
/// final circuit `ScanDelta`s `segment_id(1)` and `create_view_chain`
/// substitutes the id it allocates for the upstream.
fn plan_chain_with(
    circuit_of: fn(source_id: u64) -> gnitz_core::Circuit,
    base_tid: u64,
    cols: &[ColumnDef],
) -> [PlannedView; 2] {
    [
        segment(circuit_of, 1, base_tid, cols),
        segment(circuit_of, 0, gnitz_core::segment_id(1), cols),
    ]
}

/// A three-element bundle `[g, h, f]` whose two *segments* are in reverse
/// dependency order: `g` (slot 1) reads `h` (slot 2) reads base, and `f` (slot
/// 0, the user-named view) reads `g`. The user view stays last, as the contract
/// requires, so what is misordered is exactly the segment order.
fn plan_misordered_chain(
    circuit_of: fn(source_id: u64) -> gnitz_core::Circuit,
    base_tid: u64,
    cols: &[ColumnDef],
) -> Vec<PlannedView> {
    vec![
        segment(circuit_of, 1, gnitz_core::segment_id(2), cols),
        segment(circuit_of, 2, base_tid, cols),
        segment(circuit_of, 0, gnitz_core::segment_id(1), cols),
    ]
}

/// The single segment `owner_vid` owns, by name.
fn sole_segment(client: &mut GnitzClient, owner_vid: u64) -> String {
    let segs = segment_vids_of(client, owner_vid);
    assert_eq!(segs.len(), 1, "expected exactly one segment, got {segs:?}");
    gnitz_core::segment_name(segs[0])
}

/// Dependency-ordered bundle `[h, f]` backfills both segments; the final view
/// mirrors the base table.
#[test]
fn chain_backfill_dependency_order() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    let views = plan_chain(base_tid, &cols);

    let vids = client
        .create_view_chain(&sn, "chain_f", Vec::from(views), None)
        .unwrap();
    assert_eq!(
        vids.last().copied(),
        Some(client.resolve_table_or_view_id(&sn, "chain_f").unwrap().0),
        "the user-named view is the bundle's last element"
    );
    let owner = *vids.last().unwrap();
    assert_eq!(
        segment_vids_of(&mut client, owner),
        vec![vids[0]],
        "the bundle's non-final element is a segment the user view owns"
    );

    let got = payload_rows(&mut client, &sn, "chain_f", &["pk", "v"]);
    assert_eq!(got, vec![vec![1, 10], vec![2, 20], vec![3, 30]]);
    // The upstream segment materialized too — read through the raw client, since
    // no SQL surface will spell a leading-`_` name.
    let (_, mid, _) = client.scan(vids[0]).unwrap();
    assert_eq!(
        mid.map_or(0, |b| b.len()),
        3,
        "the upstream segment holds the base's three rows"
    );
}

/// A deliberately row-misordered bundle `[g, h, f]` — the consuming segment's
/// VIEW_TAB row precedes its producer's — must still backfill correctly,
/// because the distributed tail sorts by intra-bundle dependency rather than by
/// row order.
#[test]
fn chain_backfill_row_misordered() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    let views = plan_misordered_chain(identity_exchange_circuit, base_tid, &cols);
    let vids = client.create_view_chain(&sn, "mis_f", views, None).unwrap();
    assert_eq!(
        vids.last().copied(),
        Some(client.resolve_table_or_view_id(&sn, "mis_f").unwrap().0),
        "vids returned in input order, the user view last"
    );

    let got = payload_rows(&mut client, &sn, "mis_f", &["pk", "v"]);
    assert_eq!(
        got,
        vec![vec![1, 10], vec![2, 20], vec![3, 30]],
        "final view backfilled through both segments despite reversed segment order"
    );
}

/// The same reversed bundle built from **shard-free** segments. Nothing about a
/// linear circuit orders it: it runs no exchange, so no cross-worker barrier can
/// hold it back and each worker stops on its own drain exhaustion. Correct output
/// therefore rests entirely on the one driver visiting the producer before its
/// consumer in the depth order it computes — the property this pins.
#[test]
fn chain_backfill_row_misordered_linear() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    let views = plan_misordered_chain(identity_linear_circuit, base_tid, &cols);
    client.create_view_chain(&sn, "lin_f", views, None).unwrap();

    let got = payload_rows(&mut client, &sn, "lin_f", &["pk", "v"]);
    assert_eq!(
        got,
        vec![vec![1, 10], vec![2, 20], vec![3, 30]],
        "a shard-free final backfilled from its shard-free source despite reversed row order"
    );
}

/// A mid-bundle failure (the second segment's name collides with a committed
/// view) rolls the whole chain back: neither segment is created.
#[test]
fn chain_atomic_rollback_on_name_collision() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    // Pre-existing committed view whose name the chain's user-named view reuses.
    exec(&mut client, &sn, "CREATE VIEW taken AS SELECT * FROM base");

    let views = plan_chain(base_tid, &cols);
    let res = client.create_view_chain(&sn, "taken", Vec::from(views), None);
    assert!(res.is_err(), "collision must fail the bundle");

    // Neither element committed — ids are drawn contiguously and never reused,
    // so no segment name in the whole allocated range may be live.
    for vid in 0..64u64 {
        let name = gnitz_core::segment_name(vid);
        assert!(
            client.resolve_table_or_view_id(&sn, &name).is_err(),
            "the segment must have rolled back, but {name} is live"
        );
    }
    // The pre-existing view is untouched and still queryable.
    let got = payload_rows(&mut client, &sn, "taken", &["pk", "v"]);
    assert_eq!(got, vec![vec![1, 10], vec![2, 20], vec![3, 30]]);
}

/// Dropping the user-named final view cascades to the internal segments it owns
/// — engine-side now, off `owner_view_id`: both retire in one atomic DDL zone.
#[test]
fn chain_drop_cascades_hidden_members() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    let views = plan_chain(base_tid, &cols);
    let vid = *client
        .create_view_chain(&sn, "userview", Vec::from(views), None)
        .unwrap()
        .last()
        .unwrap();
    let h_name = sole_segment(&mut client, vid);

    assert!(client.resolve_table_or_view_id(&sn, &h_name).is_ok(), "segment present");
    assert!(
        client.resolve_table_or_view_id(&sn, "userview").is_ok(),
        "user view present"
    );

    // Drop the user view — the cascade retires the hidden member too.
    client.drop_view(&sn, "userview").unwrap();
    assert!(
        client.resolve_table_or_view_id(&sn, "userview").is_err(),
        "user view dropped"
    );
    assert!(
        client.resolve_table_or_view_id(&sn, &h_name).is_err(),
        "segment cascaded away"
    );

    // Base table is now free of dependents and drops cleanly.
    client.drop_table(&sn, "base").unwrap();
}

/// A rename is a net-live `(-1, +1)` pair on the owner's VIEW_TAB row, and the
/// segment cascade must not fire for it: the drop hook classifies net-dead by
/// what storage holds after the batch is applied, so a rename leaves the owner
/// live and its segments untouched.
#[test]
fn chain_rename_does_not_cascade_its_segments() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    let views = plan_chain(base_tid, &cols);
    let vid = *client
        .create_view_chain(&sn, "userview", Vec::from(views), None)
        .unwrap()
        .last()
        .unwrap();
    let h_name = sole_segment(&mut client, vid);

    client.alter_rename_relation(&sn, "userview", "renamed").unwrap();
    assert!(
        client.resolve_table_or_view_id(&sn, &h_name).is_ok(),
        "a net-live rewrite pair must leave the owner's segments alone"
    );
    let got = payload_rows(&mut client, &sn, "renamed", &["pk", "v"]);
    assert_eq!(got, vec![vec![1, 10], vec![2, 20], vec![3, 30]]);

    // …and the drop after it still cascades.
    client.drop_view(&sn, "renamed").unwrap();
    assert!(
        client.resolve_table_or_view_id(&sn, &h_name).is_err(),
        "segment cascaded away with its owner"
    );
    client.drop_table(&sn, "base").unwrap();
}

/// A base table under a live chain cannot be dropped — the hidden and final
/// segments depend on it, so the engine RESTRICTs.
#[test]
fn chain_drop_table_restricts_under_chain() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    let views = plan_chain(base_tid, &cols);
    client
        .create_view_chain(&sn, "userview", Vec::from(views), None)
        .unwrap();

    assert!(
        client.drop_table(&sn, "base").is_err(),
        "base table under a chain must RESTRICT"
    );
    // The chain is intact after the rejected drop.
    let got = payload_rows(&mut client, &sn, "userview", &["pk", "v"]);
    assert_eq!(got, vec![vec![1, 10], vec![2, 20], vec![3, 30]]);
}

/// `drop_schema` drains a schema holding a chain — hidden members included — to
/// empty without orphan or error.
#[test]
fn chain_drop_schema_drains_hidden_members() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    let views = plan_chain(base_tid, &cols);
    client
        .create_view_chain(&sn, "userview", Vec::from(views), None)
        .unwrap();

    client.drop_schema(&sn).unwrap();
    // Re-creating the schema succeeds, proving it fully drained.
    client.create_schema(&sn).unwrap();
}

/// `create_view_chain` rejects a bundle exceeding `MAX_CHAIN_SEGMENTS` before any
/// allocation.
#[test]
fn chain_rejects_over_length() {
    let srv = match ServerHandle::start_n(1) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    // One more than the cap, all trivial (never assembled — the length guard
    // rejects first). Reuse one circuit id source; ids are irrelevant here.
    let over = gnitz_core::MAX_CHAIN_SEGMENTS + 1;
    let planned: Vec<PlannedView> = (0..over)
        .map(|_| segment(identity_exchange_circuit, 0, base_tid, &cols))
        .collect();
    let res = client.create_view_chain(&sn, "x", planned, None);
    assert!(res.is_err(), "over-length chain must be rejected");
    let msg = format!("{:?}", res.unwrap_err());
    assert!(msg.contains("segment"), "error names the segment limit: {msg}");
}

/// Create table `t(id, v)` and a WHERE'd-CTE view `name` over it, then return
/// `(owner view id, segment name)`. A WHERE'd CTE compiles into exactly one
/// internal segment, and the bundle's ids are one contiguous run with the
/// user-named view last — so the segment took the id below the owner's, and its
/// name is minted from that id.
fn make_hidden_chain(client: &mut GnitzClient, sn: &str, name: &str) -> (u64, String) {
    exec(
        client,
        sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(
        client,
        sn,
        &format!("CREATE VIEW {name} AS WITH c AS (SELECT id, v FROM t WHERE v > 10) SELECT id FROM c"),
    );
    let vid = client.resolve_table_or_view_id(sn, name).unwrap().0;
    (vid, gnitz_core::segment_name(vid - 1))
}

/// A SQL statement naming a hidden chain segment by its raw `__h…` name — a
/// bare `SELECT` or a `CREATE VIEW` body — is rejected at the read funnel
/// (`Binder::resolve`), while the same name still resolves through the raw
/// client API (the low-level surface is deliberately unguarded; the
/// chain-introspection tests above depend on it). Without the guard the view
/// body writes a dependency row that makes the owner view permanently
/// undroppable — the regression asserted here by the successful `DROP VIEW`.
#[test]
fn hidden_segment_reference_rejected() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (_vid, hidden) = make_hidden_chain(&mut client, &sn, "s");
    // The hidden segment is registered on the trusted low-level surface.
    assert!(
        client.resolve_table_or_view_id(&sn, &hidden).is_ok(),
        "hidden segment {hidden} should be registered"
    );
    // A bare SELECT naming it is rejected with the reserved-prefix error.
    let e_sel = try_exec(&mut client, &sn, &format!("SELECT * FROM {hidden}")).unwrap_err();
    assert!(
        matches!(e_sel, GnitzSqlError::Plan(_)),
        "reserved-prefix SELECT must be a Plan error, got {e_sel:?}"
    );
    // Referencing it from a view body is rejected the same way.
    let err = try_exec(&mut client, &sn, &format!("CREATE VIEW v2 AS SELECT * FROM {hidden}")).unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Plan(_)),
        "reserved-prefix reference must be a Plan error, got {err:?}"
    );
    // With no leaked dependent, the owner view drops — and cascades the segment.
    exec(&mut client, &sn, "DROP VIEW s");
    assert!(client.resolve_table_or_view_id(&sn, "s").is_err(), "owner view dropped");
    assert!(
        client.resolve_table_or_view_id(&sn, &hidden).is_err(),
        "hidden segment cascaded away with its owner"
    );
}

/// A write / index target naming a hidden segment is rejected with the
/// reserved-prefix error (not the "is a view" message), and a *non-existent*
/// hidden name yields the SAME error — the existence side-channel is closed.
#[test]
fn hidden_segment_write_target_rejected() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (_vid, hidden) = make_hidden_chain(&mut client, &sn, "s");
    let e_ins = try_exec(&mut client, &sn, &format!("INSERT INTO {hidden} VALUES (1, 1)")).unwrap_err();
    assert!(
        matches!(e_ins, GnitzSqlError::Plan(_)),
        "INSERT into a hidden segment must be a Plan error, got {e_ins:?}"
    );
    let e_idx = try_exec(&mut client, &sn, &format!("CREATE INDEX ix ON {hidden} (v)")).unwrap_err();
    assert!(
        matches!(e_idx, GnitzSqlError::Plan(_)),
        "CREATE INDEX on a hidden segment must be a Plan error, got {e_idx:?}"
    );
    // A non-existent hidden name must give the same reserved-prefix Plan error —
    // no "is a view" vs "not found" existence side-channel.
    let e_absent = try_exec(&mut client, &sn, "INSERT INTO _seg999999 VALUES (1, 1)").unwrap_err();
    assert!(
        matches!(e_absent, GnitzSqlError::Plan(_)),
        "a non-existent hidden name must give the same Plan error, got {e_absent:?}"
    );
}
