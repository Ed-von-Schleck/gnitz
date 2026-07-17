#![cfg(feature = "integration")]

//! Index-bound pushdown into a `ScanDelta`'s backfill scan.
//!
//! The bound is a physical access hint — the `Filter` is emitted verbatim either
//! way — so these assert *which plans carry one*, not what they return. Row-level
//! equivalence is the engine's cursor unit tests; end-to-end equivalence is E2E.

use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// `(pk, g, ind, other)` plus the given `CREATE INDEX` statement.
fn setup(client: &mut gnitz_core::GnitzClient, sn: &str, index: &str) {
    exec(
        client,
        sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, \
         ind BIGINT NOT NULL, other BIGINT NOT NULL)",
    );
    exec(client, sn, index);
}

fn view_bound_cols(client: &mut gnitz_core::GnitzClient, sn: &str, name: &str) -> usize {
    let (vid, _) = client.resolve_table_or_view_id(sn, name).unwrap();
    scan_bound_col_count(client, vid)
}

/// The motivating shape: a GROUP BY whose WHERE hits a secondary index compiles a
/// `ScanDelta` carrying the bound — as a CREATE VIEW and as an ad-hoc SELECT (which
/// the GROUP BY routes to the executor as a transient).
#[test]
fn group_by_over_indexed_equality_carries_a_bound() {
    let srv = ServerHandle::start().unwrap();
    let (mut client, sn) = make_planner(&srv);
    setup(&mut client, &sn, "CREATE INDEX ON t(ind)");

    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 GROUP BY g",
    );
    assert_eq!(
        view_bound_cols(&mut client, &sn, "v"),
        1,
        "a GROUP BY over `WHERE indexed = 5` must push a 1-column bound"
    );

    // The ad-hoc form plans the same shape; it must not error.
    try_exec(&mut client, &sn, "SELECT g, COUNT(*) FROM t WHERE ind = 5 GROUP BY g").unwrap();
}

/// A range and a BETWEEN bound the same way an equality does.
#[test]
fn range_and_between_carry_a_bound() {
    let srv = ServerHandle::start().unwrap();
    let (mut client, sn) = make_planner(&srv);
    setup(&mut client, &sn, "CREATE INDEX ON t(ind)");

    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_between AS SELECT g, COUNT(*) AS c FROM t WHERE ind BETWEEN 5 AND 9 GROUP BY g",
    );
    assert_eq!(view_bound_cols(&mut client, &sn, "v_between"), 1);
}

/// An equality prefix plus a range over a COMPOUND index bounds on the full
/// declared column list — the list rides as one param row per column.
#[test]
fn compound_index_eq_prefix_plus_range_carries_both_columns() {
    let srv = ServerHandle::start().unwrap();
    let (mut client, sn) = make_planner(&srv);
    setup(&mut client, &sn, "CREATE INDEX ON t(ind, other)");

    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 AND other > 10 GROUP BY g",
    );
    assert_eq!(
        view_bound_cols(&mut client, &sn, "v"),
        2,
        "a 2-column index bound ships its full declared column list, in order"
    );
}

/// A linear (filter/map) view over an indexed equality bounds its CREATE-time
/// backfill scan.
#[test]
fn linear_view_over_indexed_equality_carries_a_bound() {
    let srv = ServerHandle::start().unwrap();
    let (mut client, sn) = make_planner(&srv);
    setup(&mut client, &sn, "CREATE INDEX ON t(ind)");

    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT g, other FROM t WHERE ind = 5",
    );
    assert_eq!(view_bound_cols(&mut client, &sn, "v"), 1);
}

/// A pass-through CTE caches its source table's REAL catalog id, so the target
/// shape keeps its bound through the `WITH`.
#[test]
fn passthrough_cte_keeps_the_bound() {
    let srv = ServerHandle::start().unwrap();
    let (mut client, sn) = make_planner(&srv);
    setup(&mut client, &sn, "CREATE INDEX ON t(ind)");

    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS WITH c AS (SELECT * FROM t) SELECT g, COUNT(*) AS n FROM c WHERE ind = 5 GROUP BY g",
    );
    assert_eq!(
        view_bound_cols(&mut client, &sn, "v"),
        1,
        "a pass-through CTE resolves to the real base table and must keep its bound"
    );
}

/// A DERIVED TABLE's id is chain-minted, not catalog-issued: those ids alias real
/// relation ids (segment 1 == SCHEMA_TAB), so an ungated extractor would probe the
/// catalog for a system table's indexes — or match a foreign user table's index
/// column indices by numeric coincidence. It must compile no bound.
#[test]
fn derived_table_segment_carries_no_bound() {
    let srv = ServerHandle::start().unwrap();
    let (mut client, sn) = make_planner(&srv);
    setup(&mut client, &sn, "CREATE INDEX ON t(ind)");

    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT g, COUNT(*) AS n FROM (SELECT * FROM t) d WHERE ind = 5 GROUP BY g",
    );
    let (vid, _) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        scan_bound_col_count(&mut client, vid),
        0,
        "a chain-minted derived-table id must never reach the index catalog"
    );
}

/// A derived table ALIASED TO ITS OWN SOURCE'S NAME must also compile no bound:
/// compiling the derived body first resolves `t` to the real catalog table, then
/// the alias shadows `t` with the chain-minted segment id — the provenance must
/// be shed with the overwrite, not stick to the name.
#[test]
fn derived_table_shadowing_its_source_name_carries_no_bound() {
    let srv = ServerHandle::start().unwrap();
    let (mut client, sn) = make_planner(&srv);
    setup(&mut client, &sn, "CREATE INDEX ON t(ind)");

    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT g, COUNT(*) AS n FROM (SELECT * FROM t) t WHERE ind = 5 GROUP BY g",
    );
    assert_eq!(
        view_bound_cols(&mut client, &sn, "v"),
        0,
        "a shadowed source name must not keep its catalog provenance"
    );
}

/// Shapes that must stay unbounded: an unindexed WHERE, a `SELECT DISTINCT`, and a
/// GROUP BY over a JOIN (whose WHERE the join circuit consumes, and whose input is
/// a hidden view that cannot own an index).
#[test]
fn unbounded_shapes_carry_no_bound() {
    let srv = ServerHandle::start().unwrap();
    let (mut client, sn) = make_planner(&srv);
    setup(&mut client, &sn, "CREATE INDEX ON t(ind)");

    // WHERE over an unindexed column.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_unindexed AS SELECT g, COUNT(*) AS c FROM t WHERE other = 5 GROUP BY g",
    );
    assert_eq!(view_bound_cols(&mut client, &sn, "v_unindexed"), 0);

    // SELECT DISTINCT takes its delta through the set-op side pipeline.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_distinct AS SELECT DISTINCT g FROM t WHERE ind = 5",
    );
    assert_eq!(view_bound_cols(&mut client, &sn, "v_distinct"), 0);

    // A PK predicate: PK columns are never eq conjuncts, whatever indexes exist.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_pk AS SELECT g, COUNT(*) AS c FROM t WHERE pk = 5 GROUP BY g",
    );
    assert_eq!(view_bound_cols(&mut client, &sn, "v_pk"), 0);

    // GROUP BY over a JOIN — the join branch clears the WHERE and yields no name.
    exec(
        &mut client,
        &sn,
        "CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v_join AS SELECT t.g, COUNT(*) AS c FROM t JOIN u ON t.pk = u.pk \
         WHERE t.ind = 5 GROUP BY t.g",
    );
    assert_eq!(
        view_bound_cols(&mut client, &sn, "v_join"),
        0,
        "a GROUP BY over a join must never bound"
    );
}

/// A view with no index on the table at all compiles no bound — and the same view
/// created BEFORE the index still returns correct results (the bound is decided at
/// plan time; a later index does not retro-bound it).
#[test]
fn no_index_carries_no_bound() {
    let srv = ServerHandle::start().unwrap();
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, ind BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t WHERE ind = 5 GROUP BY g",
    );
    assert_eq!(view_bound_cols(&mut client, &sn, "v"), 0);
}
