#![cfg(feature = "integration")]

//! `create_view_chain` — atomic multi-view bundle creation and the distributed
//! backfill tail's dependency-order sort. These drive the client API directly
//! with hand-built circuits (independent of the not-yet-built chain planner), so
//! the bundle mechanics and the row-order-independent backfill are exercised on
//! their own.
//!
//! Each segment is a minimal `input_delta → shard → gather → sink` identity view.
//! The `ExchangeShard` node makes `view_seeds_exchange_backfill` true, so every
//! segment takes the *distributed* backfill tail — the path the dependency-order
//! sort governs. A downstream segment reads an upstream segment's store, so a
//! bundle whose VIEW_TAB rows are deliberately misordered (consumer before
//! producer) still backfills correctly only if the tail re-derives the order.

use gnitz_core::{CircuitBuilder, ColumnDef, GnitzClient, PlannedView};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// A minimal exchange-seeding identity view over `source_id`, keyed on PK col 0:
/// `input_delta → shard([0]) → gather → sink`. `view_id` is pre-set so a
/// downstream segment can `ScanDelta` it.
fn identity_exchange_circuit(view_id: u64, source_id: u64) -> gnitz_core::Circuit {
    let mut cb = CircuitBuilder::new(view_id, source_id);
    let inp = cb.input_delta();
    let sh = cb.shard(inp, &[0]);
    let g = cb.gather(sh);
    cb.sink(g);
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
    let h = PlannedView {
        name: h_name.to_string(),
        sql_text: "-- hidden segment".to_string(),
        circuit: identity_exchange_circuit(h_vid, base_tid),
        output_columns: cols.to_vec(),
        pk_cols: vec![0],
    };
    let f = PlannedView {
        name: f_name.to_string(),
        sql_text: "-- final segment".to_string(),
        circuit: identity_exchange_circuit(f_vid, h_vid),
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

    let vids = client.create_view_chain(&sn, Vec::from(views)).unwrap();
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
    let vids = client.create_view_chain(&sn, vec![f, h]).unwrap();
    assert_eq!(vids, vec![f_vid, h_vid], "vids returned in input order");

    let got = payload_rows(&mut client, &sn, "mis_f", &["pk", "v"]);
    assert_eq!(
        got,
        vec![vec![1, 10], vec![2, 20], vec![3, 30]],
        "final view backfilled from the hidden view despite reversed row order"
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
    let res = client.create_view_chain(&sn, Vec::from(views));
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
    let h_name = format!("__h{f_vid}_0");
    let views = plan_chain(h_vid, f_vid, base_tid, &h_name, "userview", &cols);
    client.create_view_chain(&sn, Vec::from(views)).unwrap();

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
    let h_name = format!("__h{f_vid}_0");
    let views = plan_chain(h_vid, f_vid, base_tid, &h_name, "userview", &cols);
    client.create_view_chain(&sn, Vec::from(views)).unwrap();

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
    let h_name = format!("__h{f_vid}_0");
    let views = plan_chain(h_vid, f_vid, base_tid, &h_name, "userview", &cols);
    client.create_view_chain(&sn, Vec::from(views)).unwrap();

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
    let res = client.create_view_chain(&sn, planned);
    assert!(res.is_err(), "over-length chain must be rejected");
    let msg = format!("{:?}", res.unwrap_err());
    assert!(msg.contains("segment"), "error names the segment limit: {msg}");
}
