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
