#![cfg(feature = "integration")]

//! Weight pins — the **primary correctness net** of the pinning suite. "In a
//! Z-set engine correctness is weights, not row
//! presence" (CLAUDE.md): these insert-then-delete pins are the net that
//! **survives** a structural golden legitimately becoming an exception (a
//! post-exception structural sentinel stops being a behavior-preservation oracle;
//! the weight pin does not).
//!
//! They also carry the **predicate-content** coverage the circuit dumps
//! deliberately omit (expr is not in the dump), so an operator flip /
//! constant / column swap that the structural sentinel is blind to is caught here
//! behaviorally.
//!
//! All run at `GNITZ_WORKERS=4` (the replicated reduce's N-fold-multiply hazard
//! only manifests at W>1; the two-phase combine and the self-join collision
//! wrapper likewise exercise the exchange/fanout paths only above W=1).

use gnitz_test_harness::ServerHandle;
use std::collections::BTreeMap;

mod common;
use common::*;

/// Net weight per row of `view` over the named integer columns: `row-tuple →
/// Σweight`, dropping any net-zero row. The view store is consolidated, so this is
/// the current materialized multiplicity — a bag weight of 2, or a 0 after a
/// balanced retraction, is visible here where row-presence checks are not.
fn row_weights(client: &mut gnitz_core::GnitzClient, sn: &str, view: &str, cols: &[&str]) -> BTreeMap<Vec<i64>, i64> {
    let (schema, batch) = read_view(client, sn, view);
    let idxs: Vec<usize> = cols.iter().map(|c| col_idx(&schema, c)).collect();
    let mut m: BTreeMap<Vec<i64>, i64> = BTreeMap::new();
    for r in 0..batch.len() {
        let key: Vec<i64> = idxs.iter().map(|&ci| cell_i64(&schema, &batch, ci, r)).collect();
        *m.entry(key).or_insert(0) += batch.weights[r];
    }
    m.retain(|_, w| *w != 0);
    m
}

fn w(pairs: &[(&[i64], i64)]) -> BTreeMap<Vec<i64>, i64> {
    pairs.iter().map(|(k, v)| (k.to_vec(), *v)).collect()
}

// ── Predicate-bearing: linear + WHERE ────────────────────────────────
// Expr is excluded from the structural dump, so this is the predicate net for a
// filter. Data is chosen so a flipped operator / wrong constant would admit a
// row (`ind = 6`) the correct predicate excludes.
#[test]
fn linear_where_weights() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, ind BIGINT NOT NULL, other BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT other FROM t WHERE ind = 5");
    exec(
        &mut client,
        &sn,
        "INSERT INTO t VALUES (1, 5, 100), (2, 6, 200), (3, 5, 300)",
    );
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["other"]),
        w(&[(&[100], 1), (&[300], 1)]),
        "only ind=5 rows pass; ind=6 (other=200) must be absent"
    );
    // Retract one matching row — its weight must go to 0 (row gone).
    exec(&mut client, &sn, "DELETE FROM t WHERE id = 3");
    assert_eq!(row_weights(&mut client, &sn, "v", &["other"]), w(&[(&[100], 1)]));
}

// ── Predicate-bearing: equi join with a residual ─────────────────────
// The residual `a.v > b.w` is expr, not in the dump. Data is chosen so a flipped
// residual (`>` → `<`) would pick the other b-row.
#[test]
fn join_residual_weights() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.k = b.k AND a.v > b.w",
    );
    exec(&mut client, &sn, "INSERT INTO a VALUES (1, 10, 100)");
    exec(&mut client, &sn, "INSERT INTO b VALUES (1, 10, 50), (2, 10, 200)");
    // a.v=100 > b.w=50 (b1) true; > 200 (b2) false.
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["aid", "bid"]),
        w(&[(&[1, 1], 1)]),
        "only the pair passing the residual survives, at weight 1"
    );
    exec(&mut client, &sn, "DELETE FROM b WHERE id = 1");
    assert_eq!(row_weights(&mut client, &sn, "v", &["aid", "bid"]), BTreeMap::new());
}

// ── Self-join (collision wrapper) ────────────────────────────────────
// `test_self_join.py` is presence-only (`if r.weight > 0`). The collision wrapper
// is a named silent-weight-corruption risk with no engine backstop, so pin the
// exact weights: each report/manager pair is weight 1, never doubled.
#[test]
fn self_join_weights() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, nm BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT e.nm AS emp, m.nm AS boss FROM emp e JOIN emp m ON e.mgr = m.id",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 1, 300)",
    );
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["emp", "boss"]),
        w(&[(&[200, 100], 1), (&[300, 100], 1)]),
        "two reports of manager 1, each weight 1 — the wrapper must not double"
    );
    exec(&mut client, &sn, "INSERT INTO emp VALUES (4, 2, 400)");
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["emp", "boss"]),
        w(&[(&[200, 100], 1), (&[300, 100], 1), (&[400, 200], 1)]),
    );
    // Retract manager 1 — both of its reports lose their boss row (balanced).
    exec(&mut client, &sn, "DELETE FROM emp WHERE id = 1");
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["emp", "boss"]),
        w(&[(&[400, 200], 1)]),
        "manager-1 pairs retract cleanly; only 4→2 remains"
    );
}

// ── Two-phase all-linear global aggregate ────────────────────────────
// No incremental test today. The two-phase combine (local partial → exchange →
// combine) must sum to the true aggregate, not W× it — assert the values, at W>1.
#[test]
fn two_phase_global_weights() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t2p (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT SUM(x) AS s, COUNT(*) AS c FROM t2p",
    );
    exec(&mut client, &sn, "INSERT INTO t2p VALUES (1, 10), (2, 20), (3, 30)");
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["s", "c"]),
        w(&[(&[60, 3], 1)]),
        "SUM=60, COUNT=3 — the combine must not W-multiply the partials"
    );
    // Retract one row: the single aggregate row transitions (old -1, new +1) to net.
    exec(&mut client, &sn, "DELETE FROM t2p WHERE id = 2");
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["s", "c"]),
        w(&[(&[40, 2], 1)]),
        "SUM=40, COUNT=2 after the retraction"
    );
}

// ── Replicated-table reduce (MANDATORY W>1) ──────────────────────────
// The `reduce_multi_local` arm over a replicated source: every worker holds a
// full copy, so a wrong arm (or a broadcast double-count) N-fold-multiplies the
// aggregate. Only manifests at W>1 — assert the un-multiplied values.
#[test]
fn replicated_reduce_weights() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE rt (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL) \
         WITH (replicated = true)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT g, SUM(x) AS s FROM rt GROUP BY g",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO rt VALUES (1, 7, 10), (2, 7, 20), (3, 8, 5)",
    );
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["g", "s"]),
        w(&[(&[7, 30], 1), (&[8, 5], 1)]),
        "g=7 sums to 30 (not 120=30×4), g=8 to 5 — no N-fold multiply at W=4"
    );
    exec(&mut client, &sn, "DELETE FROM rt WHERE id = 2");
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["g", "s"]),
        w(&[(&[7, 10], 1), (&[8, 5], 1)]),
        "g=7 re-sums to 10 after the retraction"
    );
}

// ── Replicated-view chain (MANDATORY W>1) ────────────────────────────
// `replicated_reduce_weights` above is the one-hop, base-table form. Insert a
// single view hop and the whole classification must still hold: a view all of
// whose sources are replicated is itself replicated, so every worker holds its
// output in full and computes locally. Get that wrong and each exchange round
// relays W byte-identical payloads that consolidate into one row at weight W —
// `g=7` reads 120 instead of 30. Every downstream shape that carries an exchange
// is pinned, plus a mixed-source control (`vmixagg`) that must NOT be
// re-classified.

/// Two replicated tables and one partitioned table — the source set both chain
/// tests read.
fn replicated_chain_fixture(client: &mut gnitz_core::GnitzClient, sn: &str) {
    for t in ["rt", "ru"] {
        exec(
            client,
            sn,
            &format!(
                "CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL) \
                 WITH (replicated = true)"
            ),
        );
    }
    exec(
        client,
        sn,
        "CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
    );
}

fn insert_replicated_chain_rows(client: &mut gnitz_core::GnitzClient, sn: &str) {
    for t in ["rt", "ru"] {
        exec(
            client,
            sn,
            &format!("INSERT INTO {t} VALUES (1, 7, 10), (2, 7, 20), (3, 8, 5)"),
        );
    }
    exec(client, sn, "INSERT INTO fact VALUES (1, 7), (2, 8)");
}

#[test]
fn replicated_view_chain_weights() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    replicated_chain_fixture(&mut client, &sn);

    exec(&mut client, &sn, "CREATE VIEW rv AS SELECT id, g, x FROM rt");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vagg AS SELECT g, SUM(x) AS s FROM rv GROUP BY g",
    );
    exec(&mut client, &sn, "CREATE VIEW vsum AS SELECT SUM(x) AS s FROM rv");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vunion AS SELECT id, x FROM rv UNION ALL SELECT id, x FROM ru",
    );
    // The join key must be a NON-PK column of `rv`: on `fact.k = rv.id` the key
    // matches the distribution prefix, the join is marked co-partitioned, and the
    // shape passes even unfixed.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vjoin AS SELECT fact.id AS fid, rv.x AS x FROM fact JOIN rv ON fact.k = rv.g",
    );
    exec(&mut client, &sn, "CREATE VIEW v3 AS SELECT g, s FROM vagg");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vmixagg AS SELECT fid, SUM(x) AS s FROM vjoin GROUP BY fid",
    );
    exec(&mut client, &sn, "CREATE VIEW vdist AS SELECT DISTINCT g FROM rv");
    exec(&mut client, &sn, "CREATE VIEW vlin AS SELECT id, x FROM rv WHERE x > 0");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vcv AS SELECT COUNT(*) AS c FROM rv WHERE x > 1000",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vcb AS SELECT COUNT(*) AS c FROM rt WHERE x > 1000",
    );

    insert_replicated_chain_rows(&mut client, &sn);

    assert_eq!(
        row_weights(&mut client, &sn, "vagg", &["g", "s"]),
        w(&[(&[7, 30], 1), (&[8, 5], 1)]),
        "GROUP BY over a replicated view: g=7 sums to 30, not 120=30×4"
    );
    assert_eq!(
        row_weights(&mut client, &sn, "vsum", &["s"]),
        w(&[(&[35], 1)]),
        "global aggregate over a replicated view sums to 35, not 140=35×4"
    );
    assert_eq!(
        row_weights(&mut client, &sn, "vunion", &["id", "x"]),
        w(&[(&[1, 10], 2), (&[2, 20], 2), (&[3, 5], 2)]),
        "UNION ALL of a replicated view and a replicated table: weight 2 (one per side), not 8"
    );
    assert_eq!(
        row_weights(&mut client, &sn, "vjoin", &["fid", "x"]),
        w(&[(&[1, 10], 1), (&[1, 20], 1), (&[2, 5], 1)]),
        "partitioned ⋈ replicated-view on a non-PK key: weight 1, not 4"
    );
    assert_eq!(
        row_weights(&mut client, &sn, "v3", &["g", "s"]),
        w(&[(&[7, 30], 1), (&[8, 5], 1)]),
        "the third level — the induction, not just one hop"
    );
    assert_eq!(
        row_weights(&mut client, &sn, "vmixagg", &["fid", "s"]),
        w(&[(&[1, 30], 1), (&[2, 5], 1)]),
        "aggregate over a MIXED-source view: the fix must not over-claim replication"
    );
    assert_eq!(
        row_weights(&mut client, &sn, "vdist", &["g"]),
        w(&[(&[7], 1), (&[8], 1)]),
        "DISTINCT over a replicated view — now a replicated store, no weight clamp to hide behind"
    );
    assert_eq!(
        row_weights(&mut client, &sn, "vlin", &["id", "x"]),
        w(&[(&[1, 10], 1), (&[2, 20], 1), (&[3, 5], 1)]),
        "linear view over a replicated view — now a replicated store, no partition trim to hide behind"
    );
    // The empty global aggregate: exactly one worker mints the ground row, and it
    // must be the one the single-sourced read goes to.
    assert_eq!(
        row_weights(&mut client, &sn, "vcv", &["c"]),
        w(&[(&[0], 1)]),
        "an empty COUNT(*) over a replicated view must still return its ground row"
    );
    assert_eq!(
        row_weights(&mut client, &sn, "vcb", &["c"]),
        w(&[(&[0], 1)]),
        "the same query over the replicated base table — the reference the view shape must match"
    );

    exec(&mut client, &sn, "DELETE FROM rt WHERE id = 2");
    assert_eq!(
        row_weights(&mut client, &sn, "vagg", &["g", "s"]),
        w(&[(&[7, 10], 1), (&[8, 5], 1)]),
        "g=7 re-sums to 10 after the retraction — the incremental path is un-multiplied too"
    );
}

// The same chain built AFTER the insert, so the distributed-backfill driver runs
// instead of the tick. The path is selected by fixture ordering, so this cannot
// fold into the test above.
#[test]
fn replicated_view_chain_backfill_weights() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    replicated_chain_fixture(&mut client, &sn);
    insert_replicated_chain_rows(&mut client, &sn);

    exec(&mut client, &sn, "CREATE VIEW rv AS SELECT id, g, x FROM rt");
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vagg AS SELECT g, SUM(x) AS s FROM rv GROUP BY g",
    );
    assert_eq!(
        row_weights(&mut client, &sn, "vagg", &["g", "s"]),
        w(&[(&[7, 30], 1), (&[8, 5], 1)]),
        "backfilled GROUP BY over a replicated view: g=7 sums to 30, not 120=30×4"
    );
}

// ── 3-way chain (cut rule) ───────────────────────────────────────────
// A 3-way equi chain over distinct tables. The cut segment `h0 = a3 ⋈ b3` carries
// only the live columns (a3.av for the final projection, a3.id for the 2nd ON,
// b3.bv for the final); a3.k / b3.* are pruned. Weight-pins the per-step cut
// correctness (the off-by-`k` guard) — NOT a structural sentinel, so a later
// box-8 pruning pass never churns it.
#[test]
fn three_way_chain_weights() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a3 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b3 (id BIGINT NOT NULL PRIMARY KEY, bk BIGINT NOT NULL, bv BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE c3 (id BIGINT NOT NULL PRIMARY KEY, cv BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a3.av AS av, b3.bv AS bv, c3.cv AS cv \
         FROM a3 JOIN b3 ON a3.k = b3.bk JOIN c3 ON a3.id = c3.id",
    );
    exec(&mut client, &sn, "INSERT INTO a3 VALUES (1, 10, 100), (2, 10, 101)");
    exec(&mut client, &sn, "INSERT INTO b3 VALUES (1, 10, 200)");
    exec(&mut client, &sn, "INSERT INTO c3 VALUES (1, 300), (2, 301)");
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["av", "bv", "cv"]),
        w(&[(&[100, 200, 300], 1), (&[101, 200, 301], 1)]),
        "each a-row joins b (k=10) then its own c (a.id=c.id), weight 1 — the cut must not drop or double"
    );
    exec(&mut client, &sn, "DELETE FROM a3 WHERE id = 2");
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["av", "bv", "cv"]),
        w(&[(&[100, 200, 300], 1)]),
        "retracting a3(2) cleanly drops its chain row"
    );
}

// ── 3-way self-join (cut × collision) ────────────────────────────────
// `emp e JOIN emp m JOIN emp g` — the cut×collision intersection: the inner
// `e ⋈ m` self-join wraps its right side (collision), and the outer step joins
// the cut segment against a third `emp` read. `test_self_join.py` is presence-only
// for the chain; pin the exact weights here.
#[test]
fn three_way_self_join_weights() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, nm BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT e.nm AS emp, m.nm AS boss, g.nm AS grandboss \
         FROM emp e JOIN emp m ON e.mgr = m.id JOIN emp g ON m.mgr = g.id",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 2, 300)",
    );
    // e=3→m=2→g=1: (300,200,100). e=2→m=1→g=0 (none). e=1→m=0 (none).
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["emp", "boss", "grandboss"]),
        w(&[(&[300, 200, 100], 1)]),
        "the one full 3-level chain, weight 1 — the cut wrapper must not double"
    );
    exec(&mut client, &sn, "INSERT INTO emp VALUES (4, 3, 400)"); // e=4→m=3→g=2
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["emp", "boss", "grandboss"]),
        w(&[(&[300, 200, 100], 1), (&[400, 300, 200], 1)])
    );
    exec(&mut client, &sn, "DELETE FROM emp WHERE id = 4");
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["emp", "boss", "grandboss"]),
        w(&[(&[300, 200, 100], 1)]),
        "retracting emp 4 drops its chain row; the reused base id stays weight-correct"
    );
}

// ── 3-way chain with an OUTER step (null-widen × cut segment) ─────────
// `(a3 JOIN b3) LEFT JOIN c3`: the cut INNER segment `h0 = a3 ⋈ b3` feeds a LEFT
// join, so an h0 row with no matching c3 null-fills. Pin on the non-null a/b
// columns (the null-filled `cv` is NULL). Catches a null-fill weight error over a
// cut segment.
#[test]
fn three_way_outer_chain_weights() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a3 (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE b3 (id BIGINT NOT NULL PRIMARY KEY, bk BIGINT NOT NULL, bv BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE c3 (id BIGINT NOT NULL PRIMARY KEY, cv BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a3.av AS av, b3.bv AS bv \
         FROM a3 JOIN b3 ON a3.k = b3.bk LEFT JOIN c3 ON a3.id = c3.id",
    );
    exec(&mut client, &sn, "INSERT INTO a3 VALUES (1, 10, 100), (2, 10, 101)");
    exec(&mut client, &sn, "INSERT INTO b3 VALUES (1, 10, 200)");
    exec(&mut client, &sn, "INSERT INTO c3 VALUES (1, 300)"); // matches a3(1) only
                                                              // h0={(a1,b1),(a2,b1)}; LEFT JOIN c3: a1 matches c1, a2 null-fills — both survive.
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["av", "bv"]),
        w(&[(&[100, 200], 1), (&[101, 200], 1)]),
        "the unmatched a-row null-fills at weight 1 (not 0, not doubled)"
    );
    exec(&mut client, &sn, "DELETE FROM b3 WHERE id = 1"); // empties h0
    assert_eq!(
        row_weights(&mut client, &sn, "v", &["av", "bv"]),
        BTreeMap::new(),
        "retracting the sole b-row empties h0; the matched AND the null-filled row retract with it"
    );
}
