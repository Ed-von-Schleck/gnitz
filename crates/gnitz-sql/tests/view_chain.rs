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

use gnitz_core::{hidden_view_name, CircuitBuilder, ColumnDef, GnitzClient, PlannedView};
use gnitz_sql::GnitzSqlError;
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// A minimal exchanging identity view over `source_id`, keyed on PK col 0:
/// `input_delta → shard([0]) → sink`. Its backfill runs a cross-worker exchange
/// round per chunk.
fn identity_exchange_circuit(view_id: u64, source_id: u64) -> gnitz_core::Circuit {
    let mut cb = CircuitBuilder::new(view_id, source_id);
    let inp = cb.input_delta();
    let sh = cb.shard(inp, &[0]);
    cb.sink(sh);
    cb.build()
}

/// A minimal shard-free identity view over `source_id`: `input_delta → sink`,
/// the shape a plain projection/filter compiles to. Its backfill runs no
/// exchange, so each worker terminates on its own drain exhaustion.
fn identity_linear_circuit(view_id: u64, source_id: u64) -> gnitz_core::Circuit {
    let mut cb = CircuitBuilder::new(view_id, source_id);
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

/// Build two chained identity views `h → f` (f reads h reads base) as
/// `PlannedView`s. `cols` is the shared (base = view) column layout, pk at 0.
fn plan_chain(
    h_vid: u64,
    f_vid: u64,
    base_tid: u64,
    h_name: &str,
    f_name: &str,
    cols: &[ColumnDef],
) -> [PlannedView; 2] {
    plan_chain_with(identity_exchange_circuit, h_vid, f_vid, base_tid, h_name, f_name, cols)
}

/// `plan_chain` with the segments' circuit shape chosen by the caller.
/// `view_id` is pre-set so a downstream segment can `ScanDelta` it.
fn plan_chain_with(
    circuit_of: fn(view_id: u64, source_id: u64) -> gnitz_core::Circuit,
    h_vid: u64,
    f_vid: u64,
    base_tid: u64,
    h_name: &str,
    f_name: &str,
    cols: &[ColumnDef],
) -> [PlannedView; 2] {
    let h = PlannedView {
        name: h_name.to_string(),
        sql_text: "-- hidden segment".to_string(),
        circuit: circuit_of(h_vid, base_tid),
        output_columns: cols.to_vec(),
        pk_cols: vec![0],
    };
    let f = PlannedView {
        name: f_name.to_string(),
        sql_text: "-- final segment".to_string(),
        circuit: circuit_of(f_vid, h_vid),
        output_columns: cols.to_vec(),
        pk_cols: vec![0],
    };
    [h, f]
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

    let h_vid = client.alloc_table_id().unwrap();
    let f_vid = client.alloc_table_id().unwrap();
    let views = plan_chain(h_vid, f_vid, base_tid, "chain_h", "chain_f", &cols);

    let vids = client.create_view_chain(&sn, Vec::from(views), None).unwrap();
    assert_eq!(vids, vec![h_vid, f_vid], "vids returned in input order");

    let got = payload_rows(&mut client, &sn, "chain_f", &["pk", "v"]);
    assert_eq!(got, vec![vec![1, 10], vec![2, 20], vec![3, 30]]);
    // The hidden upstream segment materialized too.
    let mid = payload_rows(&mut client, &sn, "chain_h", &["pk", "v"]);
    assert_eq!(mid, vec![vec![1, 10], vec![2, 20], vec![3, 30]]);
}

/// A deliberately row-misordered bundle `[f, h]` — the consumer's VIEW_TAB row
/// precedes the producer's — must still backfill correctly, because the
/// distributed tail sorts by intra-bundle dependency rather than row order.
#[test]
fn chain_backfill_row_misordered() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    let h_vid = client.alloc_table_id().unwrap();
    let f_vid = client.alloc_table_id().unwrap();
    let [h, f] = plan_chain(h_vid, f_vid, base_tid, "mis_h", "mis_f", &cols);
    // Consumer (f) before producer (h) in the bundle.
    let vids = client.create_view_chain(&sn, vec![f, h], None).unwrap();
    assert_eq!(vids, vec![f_vid, h_vid], "vids returned in input order");

    let got = payload_rows(&mut client, &sn, "mis_f", &["pk", "v"]);
    assert_eq!(
        got,
        vec![vec![1, 10], vec![2, 20], vec![3, 30]],
        "final view backfilled from the hidden view despite reversed row order"
    );
}

/// The same reversed bundle built from **shard-free** segments. Nothing about a
/// linear circuit orders it: it runs no exchange, so no cross-worker barrier can
/// hold it back and each worker stops on its own drain exhaustion. Correct output
/// therefore rests entirely on the one driver visiting `h` before `f` in the
/// depth order it computes — the property this pins.
#[test]
fn chain_backfill_row_misordered_linear() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    let h_vid = client.alloc_table_id().unwrap();
    let f_vid = client.alloc_table_id().unwrap();
    let [h, f] = plan_chain_with(identity_linear_circuit, h_vid, f_vid, base_tid, "lin_h", "lin_f", &cols);
    // Consumer (f) before producer (h) in the bundle.
    client.create_view_chain(&sn, vec![f, h], None).unwrap();

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

    // Pre-existing committed view whose name the chain's 2nd segment reuses.
    exec(&mut client, &sn, "CREATE VIEW taken AS SELECT * FROM base");

    let h_vid = client.alloc_table_id().unwrap();
    let f_vid = client.alloc_table_id().unwrap();
    // h has a fresh name; f collides with `taken`.
    let views = plan_chain(h_vid, f_vid, base_tid, "roll_h", "taken", &cols);
    let res = client.create_view_chain(&sn, Vec::from(views), None);
    assert!(res.is_err(), "collision must fail the bundle");

    // Neither segment committed — the fresh hidden segment must be absent.
    assert!(
        client.resolve_table_or_view_id(&sn, "roll_h").is_err(),
        "first segment must have rolled back"
    );
    // The pre-existing view is untouched and still queryable.
    let got = payload_rows(&mut client, &sn, "taken", &["pk", "v"]);
    assert_eq!(got, vec![vec![1, 10], vec![2, 20], vec![3, 30]]);
}

/// Dropping the user-named final view cascades to its hidden segment views
/// (`__h{final_vid}_{i}`): both retire in one atomic bundle.
#[test]
fn chain_drop_cascades_hidden_members() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let (base_tid, cols) = make_base(&mut client, &sn);

    let h_vid = client.alloc_table_id().unwrap();
    let f_vid = client.alloc_table_id().unwrap();
    // Hidden segment named by the real `__h{final_vid}_{i}` convention so
    // `drop_view`'s prefix scan (`__h{f_vid}_`) matches it.
    let h_name = hidden_view_name(f_vid, 0);
    let views = plan_chain(h_vid, f_vid, base_tid, &h_name, "userview", &cols);
    client.create_view_chain(&sn, Vec::from(views), None).unwrap();

    assert!(
        client.resolve_table_or_view_id(&sn, &h_name).is_ok(),
        "hidden view present"
    );
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
        "hidden member cascaded away"
    );

    // Base table is now free of dependents and drops cleanly.
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

    let h_vid = client.alloc_table_id().unwrap();
    let f_vid = client.alloc_table_id().unwrap();
    let h_name = hidden_view_name(f_vid, 0);
    let views = plan_chain(h_vid, f_vid, base_tid, &h_name, "userview", &cols);
    client.create_view_chain(&sn, Vec::from(views), None).unwrap();

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

    let h_vid = client.alloc_table_id().unwrap();
    let f_vid = client.alloc_table_id().unwrap();
    let h_name = hidden_view_name(f_vid, 0);
    let views = plan_chain(h_vid, f_vid, base_tid, &h_name, "userview", &cols);
    client.create_view_chain(&sn, Vec::from(views), None).unwrap();

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
        .map(|_| PlannedView {
            name: "x".to_string(),
            sql_text: String::new(),
            circuit: identity_exchange_circuit(0, base_tid),
            output_columns: cols.clone(),
            pk_cols: vec![0],
        })
        .collect();
    let res = client.create_view_chain(&sn, planned, None);
    assert!(res.is_err(), "over-length chain must be rejected");
    let msg = format!("{:?}", res.unwrap_err());
    assert!(msg.contains("segment"), "error names the segment limit: {msg}");
}

/// Create table `t(id, v)` and a WHERE'd-CTE view `name` over it, then return
/// `(owner view id, hidden segment name)`. A WHERE'd CTE compiles into exactly one
/// hidden segment named `__h{owner_vid}_0`, where the owner vid is the id `name`
/// itself resolves to — so the hidden name is derived from the resolved owner.
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
    (vid, hidden_view_name(vid, 0))
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
    let e_absent = try_exec(&mut client, &sn, "INSERT INTO __h999999_0 VALUES (1, 1)").unwrap_err();
    assert!(
        matches!(e_absent, GnitzSqlError::Plan(_)),
        "a non-existent hidden name must give the same Plan error, got {e_absent:?}"
    );
}
