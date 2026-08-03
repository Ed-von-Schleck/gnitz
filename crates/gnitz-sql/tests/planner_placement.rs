#![cfg(feature = "integration")]

//! Where a relation's rows live must be transitive down a view chain.
//!
//! A view over any source that is not itself key-placed produces its rows on the
//! worker that produced them, not on the one the key names — and so does every
//! view over *that* view. When the property was re-derived from a view's direct
//! sources instead of read off the source's own stamped answer, it died at the
//! first hop: a linear view over `fact ⋈ replicated_dim` was built hash-addressed
//! over an unkeyed source, and the rows the two disagreed about left through the
//! store's non-local ingest skip with no error and no log line.
//!
//! The fixture is a plain star schema — no compound PK, no `CLUSTER BY`, no
//! opt-in — so every assertion runs at `GNITZ_WORKERS=4`, where a placement /
//! addressing split is observable, and compares whole multisets with weights.

mod common;
use common::*;
use gnitz_core::GnitzClient;
use gnitz_test_harness::ServerHandle;

/// `d` replicated (10 rows), `f` and `p` partitioned (100 rows each).
fn create_tables(client: &mut GnitzClient, sn: &str) {
    exec(
        client,
        sn,
        "CREATE TABLE d (id BIGINT UNSIGNED PRIMARY KEY, nm BIGINT NOT NULL) WITH (replicated = true)",
    );
    exec(
        client,
        sn,
        "CREATE TABLE f (id BIGINT UNSIGNED PRIMARY KEY, r BIGINT UNSIGNED NOT NULL)",
    );
    exec(
        client,
        sn,
        "CREATE TABLE p (id BIGINT UNSIGNED PRIMARY KEY, q BIGINT NOT NULL)",
    );
}

fn insert_all(client: &mut GnitzClient, sn: &str) {
    let d: Vec<Vec<i64>> = (1..=10i64).map(|id| vec![id, id * 10]).collect();
    insert_rows(client, sn, "d", &["id", "nm"], &d);
    let f: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id, dim_of(id)]).collect();
    insert_rows(client, sn, "f", &["id", "r"], &f);
    let p: Vec<Vec<i64>> = (1..=100i64).map(|id| vec![id, id * 3]).collect();
    insert_rows(client, sn, "p", &["id", "q"], &p);
}

/// The `d.id` fact `id` references — 10 facts per dim row.
fn dim_of(id: i64) -> i64 {
    (id - 1) % 10 + 1
}

fn sorted(mut rows: Vec<Vec<i64>>) -> Vec<Vec<i64>> {
    rows.sort();
    rows
}

// ── A view chain over a replicated-sourced join keeps every row ──────────────

#[test]
fn view_chain_over_replicated_source_join_keeps_every_row_multiworker() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    create_tables(&mut client, &sn);
    // Every view is created before the data, so each materializes incrementally
    // tick by tick rather than through a backfill.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW jm AS SELECT f.id AS fid, d.nm AS nm FROM f JOIN d ON f.r = d.id",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW dv AS SELECT fid, nm FROM jm WHERE nm > 0",
    );
    exec(&mut client, &sn, "CREATE VIEW dv2 AS SELECT fid FROM dv");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW dvd AS SELECT fid FROM (SELECT fid, nm FROM jm WHERE nm > 0) x",
    );
    // The control: a GROUP BY over the same join exchanges, so it was whole even
    // while the linear hops were not.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW gj AS SELECT nm AS knm, COUNT(*) AS n FROM jm GROUP BY nm",
    );
    insert_all(&mut client, &sn);

    let joined = sorted((1..=100i64).map(|id| vec![id, dim_of(id) * 10, 1]).collect());
    assert_eq!(
        query_rows_weighted(&mut client, &sn, "SELECT * FROM jm", &["fid", "nm"]),
        joined,
        "every fact joins its replicated dim row exactly once",
    );
    assert_eq!(
        query_rows_weighted(&mut client, &sn, "SELECT * FROM dv", &["fid", "nm"]),
        joined,
        "the linear hop over the join keeps every row",
    );
    let fids = sorted((1..=100i64).map(|id| vec![id, 1]).collect());
    assert_eq!(
        query_rows_weighted(&mut client, &sn, "SELECT * FROM dv2", &["fid"]),
        fids,
        "a third hop keeps every row too",
    );
    assert_eq!(
        query_rows_weighted(&mut client, &sn, "SELECT * FROM dvd", &["fid"]),
        fids,
        "the derived-table form of the same",
    );
    assert_eq!(
        query_rows_weighted(&mut client, &sn, "SELECT * FROM gj", &["knm", "n"]),
        at_weight_one(&sorted((1..=10i64).map(|k| vec![k * 10, 10]).collect())),
        "the exchanging GROUP BY control is unchanged",
    );

    // A keyed read of the downstream view must not seek one worker: its rows are
    // not placed by their key, so nothing names an owner and the read gathers.
    for id in 1..=100i64 {
        assert_eq!(
            query_rows_weighted(
                &mut client,
                &sn,
                &format!("SELECT * FROM dv WHERE fid = {id}"),
                &["fid", "nm"],
            ),
            vec![vec![id, dim_of(id) * 10, 1]],
            "point read of fid {id}",
        );
    }
}

// ── The shapes around it that must not move ─────────────────────────────────

#[test]
fn placement_controls_multiworker() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    create_tables(&mut client, &sn);
    // Every source replicated ⇒ the view is replicated end to end: each worker
    // computes the whole result and the read single-sources worker 0, so a
    // gather would return one copy per worker.
    exec(&mut client, &sn, "CREATE VIEW rv AS SELECT id, nm FROM d WHERE nm > 0");
    // A bag union of the replicated and the partitioned side, then a linear hop
    // over it.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW ua AS SELECT id FROM d UNION ALL SELECT id FROM f",
    );
    exec(&mut client, &sn, "CREATE VIEW uav AS SELECT id FROM ua WHERE id > 0");
    // Two partitioned sides: the join stays key-placed, and so does the linear
    // hop over it — the direction this change must NOT widen.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW pj AS SELECT f.id AS fid, p.q AS q FROM f JOIN p ON f.id = p.id",
    );
    exec(&mut client, &sn, "CREATE VIEW pjv AS SELECT fid, q FROM pj WHERE q > 0");
    insert_all(&mut client, &sn);

    assert_eq!(
        query_rows_weighted(&mut client, &sn, "SELECT * FROM rv", &["id", "nm"]),
        sorted((1..=10i64).map(|id| vec![id, id * 10, 1]).collect()),
        "a fully replicated view reads back one copy",
    );
    // `UNION ALL` is a bag, and both branches survive: 100 rows from `f` plus 10
    // from `d`, kept apart by the set-op's per-branch key.
    let bag = query_rows_weighted(&mut client, &sn, "SELECT * FROM ua", &["id"]);
    assert_eq!(bag.len(), 110, "the mixed UNION ALL keeps both branches: {bag:?}");
    assert_eq!(
        sorted(bag.iter().map(|r| vec![r[0]]).collect()),
        sorted(
            (1..=100i64)
                .map(|id| vec![id])
                .chain((1..=10i64).map(|id| vec![id]))
                .collect()
        ),
        "every id from both sides is present",
    );
    // The hop is compared against its own source rather than a computed
    // expectation: what this control governs is that a linear view over a
    // mixed-source set-op loses nothing, not what the set-op's own weights are.
    assert_eq!(
        query_rows_weighted(&mut client, &sn, "SELECT * FROM uav", &["id"]),
        bag,
        "the linear hop over it reproduces its source row for row and weight for weight",
    );
    let pj_rows = sorted((1..=100i64).map(|id| vec![id, id * 3, 1]).collect());
    assert_eq!(
        query_rows_weighted(&mut client, &sn, "SELECT * FROM pj", &["fid", "q"]),
        pj_rows,
        "a join of two partitioned sides is complete",
    );
    assert_eq!(
        query_rows_weighted(&mut client, &sn, "SELECT * FROM pjv", &["fid", "q"]),
        pj_rows,
        "and its linear hop stays key-placed and complete",
    );
    for id in [1i64, 37, 100] {
        assert_eq!(
            query_rows_weighted(
                &mut client,
                &sn,
                &format!("SELECT * FROM pjv WHERE fid = {id}"),
                &["fid", "q"],
            ),
            vec![vec![id, id * 3, 1]],
            "a keyed read of the key-placed hop still seeks its one owner",
        );
    }
}
