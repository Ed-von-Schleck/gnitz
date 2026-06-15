#![cfg(feature = "integration")]

//! `CLUSTER BY` (per-table hash distribution key) — DDL validation plus
//! end-to-end correctness of co-partitioned joins, local GROUP BY, and
//! prefix-distributed uniqueness/retraction.
//!
//! The co-partition exchange-skip is a *runtime* optimization (the `ExchangeShard`
//! node still exists; it is simply not executed), so these tests assert
//! **correctness under `GNITZ_WORKERS=4`**: a co-partition skip that fires when
//! the two sides are NOT actually co-located silently drops matches, and a
//! retraction/uniqueness probe that does not slice to the distribution prefix
//! misses the row — both surface as wrong query results here. Mirrors the
//! correctness-under-W>1 style of `test_group_by_compound_pk_multiworker`.

mod common;
use common::*;
use gnitz_test_harness::ServerHandle;

// ── DDL validation: CLUSTER BY must be a leading PK prefix ───────────────────

#[test]
fn cluster_by_validation() {
    let srv = match ServerHandle::start() { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);

    // The leading prefix (one column, or the full PK) is accepted.
    assert!(try_exec(&mut client, &sn,
        "CREATE TABLE ok1 (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, \
         PRIMARY KEY (a, b)) CLUSTER BY a").is_ok());
    assert!(try_exec(&mut client, &sn,
        "CREATE TABLE ok2 (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, \
         PRIMARY KEY (a, b)) CLUSTER BY a, b").is_ok());
    // A single-column PK clustered by that column is the default, spelled out.
    assert!(try_exec(&mut client, &sn,
        "CREATE TABLE ok3 (a BIGINT UNSIGNED PRIMARY KEY, v BIGINT NOT NULL) CLUSTER BY a").is_ok());

    // A non-leading PK column, a non-contiguous prefix, the wrong order, and a
    // non-PK column are all rejected at DDL.
    assert!(try_exec(&mut client, &sn,
        "CREATE TABLE bad1 (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, \
         PRIMARY KEY (a, b)) CLUSTER BY b").is_err(), "non-leading PK column");
    assert!(try_exec(&mut client, &sn,
        "CREATE TABLE bad2 (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, v BIGINT NOT NULL, \
         PRIMARY KEY (a, b, c)) CLUSTER BY a, c").is_err(), "non-contiguous prefix (skips b)");
    assert!(try_exec(&mut client, &sn,
        "CREATE TABLE bad3 (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, \
         PRIMARY KEY (a, b)) CLUSTER BY b, a").is_err(), "wrong order");
    assert!(try_exec(&mut client, &sn,
        "CREATE TABLE bad4 (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, \
         PRIMARY KEY (a, b)) CLUSTER BY v").is_err(), "non-PK column");
}

// ── Co-partitioned local join (proper-prefix on the orders side) ─────────────

// `orders` is `CLUSTER BY customer_id` (a *proper* prefix of its compound PK),
// `customers` is default (PK = id). Joining on `customer_id = id` co-partitions
// both sides (both key by their distribution key at a common U64 width), so the
// exchange is skipped — but the per-side `map_reindex` still re-keys orders'
// (customer_id, order_id) PK down to customer_id before cogroup. Results must be
// correct under W=4.
#[test]
fn cluster_by_local_join_multiworker() {
    let srv = match ServerHandle::start_n(4) { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY, region BIGINT NOT NULL)");
    exec(&mut client, &sn,
        "CREATE TABLE orders (customer_id BIGINT UNSIGNED, order_id BIGINT UNSIGNED, \
         amount BIGINT NOT NULL, PRIMARY KEY (customer_id, order_id)) CLUSTER BY customer_id");
    exec(&mut client, &sn,
        "CREATE VIEW co AS SELECT o.customer_id AS cid, o.order_id AS oid, \
         o.amount AS amt, c.region AS reg \
         FROM orders o JOIN customers c ON o.customer_id = c.id");
    // Proper-prefix correctness: a local GROUP BY on the distribution prefix of
    // `orders` aggregates each customer's orders across the cluster. Created
    // before the data so it materializes incrementally — live backfill of an
    // exchange-requiring view (GROUP BY/reduce/join) on a multi-worker server in
    // one session is a separate, unsupported path (the dedicated GROUP BY test is
    // likewise view-before-data); the co-partition exchange-skip is exercised on
    // every incremental tick regardless.
    exec(&mut client, &sn,
        "CREATE VIEW og AS SELECT customer_id AS cid, COUNT(*) AS n, SUM(amount) AS s \
         FROM orders GROUP BY customer_id");

    exec(&mut client, &sn,
        "INSERT INTO customers (id, region) VALUES (1, 100), (2, 200), (3, 300)");
    // Customer 1 has two orders (same customer_id, distinct order_id ⇒ prefix
    // twins). Customer 4 has an order but no customer row (no inner match).
    exec(&mut client, &sn,
        "INSERT INTO orders (customer_id, order_id, amount) VALUES \
         (1, 10, 1000), (1, 11, 1100), (2, 20, 2000), (3, 30, 3000), (4, 40, 4000)");

    assert_eq!(
        payload_rows(&mut client, &sn, "co", &["cid", "oid", "amt", "reg"]),
        vec![
            vec![1, 10, 1000, 100],
            vec![1, 11, 1100, 100],
            vec![2, 20, 2000, 200],
            vec![3, 30, 3000, 300],
        ],
        "co-partitioned join returns every matched order with its customer; the \
         unmatched customer_id=4 order is dropped by the inner join",
    );

    assert_eq!(
        payload_rows(&mut client, &sn, "og", &["cid", "n", "s"]),
        vec![vec![1, 2, 2100], vec![2, 1, 2000], vec![3, 1, 3000], vec![4, 1, 4000]],
        "GROUP BY the distribution prefix aggregates each group on one worker",
    );
}

// ── Super-prefix join safety: the exact-match detector is load-bearing ───────

// Side A `CLUSTER BY a1` (k=1) vs side B default (k=2), joined on the FULL key
// `a1=b1 AND a2=b2`. Exact-match co-partition detection means A (join-key length
// 2 ≠ its k=1) does NOT skip — it exchanges and repartitions to the full (a1,a2),
// converging with B (which keeps its native full-PK distribution). A super-prefix
// detector (`>= k`) would wrongly let A skip while still hashed on a1 alone, so
// rows whose partition(a1) ≠ partition(a1,a2) would land on a different worker
// than B and the elided exchange would drop the match. MUST run multi-worker.
#[test]
fn cluster_by_super_prefix_join_safety_multiworker() {
    let srv = match ServerHandle::start_n(4) { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE sa (a1 BIGINT UNSIGNED, a2 BIGINT UNSIGNED, va BIGINT NOT NULL, \
         PRIMARY KEY (a1, a2)) CLUSTER BY a1");
    exec(&mut client, &sn,
        "CREATE TABLE sb (b1 BIGINT UNSIGNED, b2 BIGINT UNSIGNED, vb BIGINT NOT NULL, \
         PRIMARY KEY (b1, b2))");
    exec(&mut client, &sn,
        "CREATE VIEW j AS SELECT a.a1 AS k1, a.a2 AS k2, a.va AS va, b.vb AS vb \
         FROM sa a JOIN sb b ON a.a1 = b.b1 AND a.a2 = b.b2");

    // Several distinct (a1, a2) pairs sharing a1 values, so a1-only routing and
    // (a1,a2) routing diverge for some rows under W=4.
    exec(&mut client, &sn,
        "INSERT INTO sa (a1, a2, va) VALUES \
         (1, 1, 11), (1, 2, 12), (1, 3, 13), (2, 1, 21), (2, 7, 27), (9, 4, 94)");
    exec(&mut client, &sn,
        "INSERT INTO sb (b1, b2, vb) VALUES \
         (1, 1, 110), (1, 2, 120), (1, 3, 130), (2, 1, 210), (2, 7, 270), (5, 5, 550)");

    assert_eq!(
        payload_rows(&mut client, &sn, "j", &["k1", "k2", "va", "vb"]),
        vec![
            vec![1, 1, 11, 110],
            vec![1, 2, 12, 120],
            vec![1, 3, 13, 130],
            vec![2, 1, 21, 210],
            vec![2, 7, 27, 270],
        ],
        "exact-match co-partition keeps the full key-equal join complete; a \
         super-prefix skip would drop the rows whose a1 and (a1,a2) hash apart",
    );
}

// ── GROUP BY the distribution prefix runs locally and stays correct ──────────

// On a `CLUSTER BY a` table, `GROUP BY a` co-partitions (every row for a group
// value lives on one worker) and skips the shuffle; `GROUP BY` a non-prefix
// column must still exchange. Both must return the same multiset as a recompute.
#[test]
fn cluster_by_group_by_prefix_multiworker() {
    let srv = match ServerHandle::start_n(4) { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, \
         PRIMARY KEY (a, b)) CLUSTER BY a");
    // GROUP BY a (= distribution prefix): the co-partition-skip path.
    exec(&mut client, &sn,
        "CREATE VIEW g_pre AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY a");
    // GROUP BY b (a non-leading PK column, not the distribution key): exchanges.
    exec(&mut client, &sn,
        "CREATE VIEW g_other AS SELECT b AS kb, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY b");
    // GROUP BY a, b (the full PK): also co-partitions (singleton groups).
    exec(&mut client, &sn,
        "CREATE VIEW g_full AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM t GROUP BY a, b");

    exec(&mut client, &sn,
        "INSERT INTO t (a, b, v) VALUES \
         (1, 1, 10), (1, 2, 20), (1, 3, 30), (2, 1, 40), (2, 2, 50), (3, 1, 60)");

    assert_eq!(
        payload_rows(&mut client, &sn, "g_pre", &["ka", "n", "s"]),
        vec![vec![1, 3, 60], vec![2, 2, 90], vec![3, 1, 60]],
        "GROUP BY the distribution prefix aggregates every group correctly under W=4",
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "g_other", &["kb", "n", "s"]),
        vec![vec![1, 3, 110], vec![2, 2, 70], vec![3, 1, 30]],
        "GROUP BY a non-prefix column still exchanges and stays correct",
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "g_full", &["ka", "kb", "n"]),
        vec![vec![1,1,1], vec![1,2,1], vec![1,3,1], vec![2,1,1], vec![2,2,1], vec![3,1,1]],
        "GROUP BY the full PK is co-partitioned and correct",
    );
}

// ── Filtered GROUP BY the distribution prefix still runs locally ─────────────

// Filtered GROUP BY on the distribution prefix: a `Filter` between the scan and
// the `ExchangeShard` (`SELECT a … WHERE v >= 30 GROUP BY a`) no longer breaks the
// exchange-skip detector. A Filter is row-selective and never moves a row off its
// worker, so `GROUP BY a` (the distribution prefix) co-partitions exactly as the
// unfiltered case does. The skip is invisible at this layer (exchanging is also
// correct), so this asserts the *engaged* skip stays correct under W=4: a skip that
// wrongly fired would drop the surviving filtered rows on some worker. `GROUP BY b`
// (a non-prefix column) + the same WHERE must still exchange and stay correct.
#[test]
fn cluster_by_filtered_group_by_prefix_multiworker() {
    let srv = match ServerHandle::start_n(4) { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, \
         PRIMARY KEY (a, b)) CLUSTER BY a");
    // GROUP BY a (= distribution prefix) with a WHERE: the planner emits a
    // ScanDelta → Filter → ExchangeShard chain that the relaxed detector now walks
    // through, skipping the shuffle. Created before the data (live backfill of an
    // exchange-requiring view on a multi-worker server in one session is a separate,
    // unsupported path), so it materializes incrementally tick by tick.
    exec(&mut client, &sn,
        "CREATE VIEW gf_pre AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s \
         FROM t WHERE v >= 30 GROUP BY a");
    // GROUP BY b (a non-leading PK column, not the distribution key) with the same
    // WHERE: still exchanges, since b is not the distribution prefix.
    exec(&mut client, &sn,
        "CREATE VIEW gf_other AS SELECT b AS kb, COUNT(*) AS n, SUM(v) AS s \
         FROM t WHERE v >= 30 GROUP BY b");

    exec(&mut client, &sn,
        "INSERT INTO t (a, b, v) VALUES \
         (1, 1, 10), (1, 2, 20), (1, 3, 30), (2, 1, 40), (2, 2, 50), (3, 1, 60)");

    // WHERE v >= 30 keeps (1,3,30), (2,1,40), (2,2,50), (3,1,60).
    assert_eq!(
        payload_rows(&mut client, &sn, "gf_pre", &["ka", "n", "s"]),
        vec![vec![1, 1, 30], vec![2, 2, 90], vec![3, 1, 60]],
        "filtered GROUP BY the distribution prefix co-partitions and aggregates \
         only the surviving rows correctly under W=4",
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "gf_other", &["kb", "n", "s"]),
        vec![vec![1, 2, 100], vec![2, 1, 50], vec![3, 1, 30]],
        "filtered GROUP BY a non-prefix column still exchanges and stays correct",
    );
}

// ── Uniqueness / retraction under a prefix key (the §4.4 regression) ─────────

// On a `CLUSTER BY k1` compound-PK table, two rows that share the distribution
// prefix but differ in the PK suffix are prefix twins. An UPSERT of one and a
// DELETE of the other both route by the prefix, and their enforce_unique_pk /
// seek probes must slice to the same prefix to find the existing row. Without the
// `local_index_bytes` slice the UPSERT would duplicate the PK and the DELETE
// would be dropped — observable here as extra/leftover rows. MUST run multi-worker.
#[test]
fn cluster_by_uniqueness_retraction_multiworker() {
    let srv = match ServerHandle::start_n(4) { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE u (k1 BIGINT UNSIGNED, k2 BIGINT UNSIGNED, v BIGINT NOT NULL, \
         PRIMARY KEY (k1, k2)) CLUSTER BY k1");
    // Three prefix twins under k1=5, plus an unrelated row.
    exec(&mut client, &sn,
        "INSERT INTO u (k1, k2, v) VALUES (5, 1, 100), (5, 2, 200), (5, 3, 300), (8, 1, 1)");

    // UPSERT (5,1): enforce_unique_pk must find and retract the old (5,1,100) in
    // its prefix-partition, leaving exactly one (5,1) at the new value. The
    // conflict target defaults to the PK — a partial-tuple `ON CONFLICT (k1, k2)`
    // target is not supported on compound-PK tables (the upsert is keyed on the
    // full PK regardless), so the no-target form is used.
    exec(&mut client, &sn,
        "INSERT INTO u (k1, k2, v) VALUES (5, 1, 999) ON CONFLICT DO UPDATE SET v = EXCLUDED.v");
    // DELETE (5,2): the seek/retract must reach the same prefix-partition.
    let n = affected(&mut client, &sn, "DELETE FROM u WHERE k1 = 5 AND k2 = 2");
    assert_eq!(n, 1, "exactly the (5,2) twin is deleted");

    assert_eq!(
        payload_rows(&mut client, &sn, "u", &["k1", "k2", "v"]),
        vec![vec![5, 1, 999], vec![5, 3, 300], vec![8, 1, 1]],
        "UPSERT retracts the old twin (no duplicate PK) and DELETE removes the \
         other twin — both probes reached the prefix-partition",
    );
}

// ── Default parity: a table with no clause behaves as before ─────────────────

// A compound-PK table with no `CLUSTER BY` distributes by the full PK. Joining it
// on its full PK still co-partitions (the k = |PK| case), and GROUP BY the full
// PK stays correct — byte-for-byte the pre-feature behavior. MUST run multi-worker.
#[test]
fn cluster_by_default_parity_multiworker() {
    let srv = match ServerHandle::start_n(4) { Some(s) => s, None => return };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn,
        "CREATE TABLE da (a1 BIGINT UNSIGNED, a2 BIGINT UNSIGNED, va BIGINT NOT NULL, \
         PRIMARY KEY (a1, a2))");
    exec(&mut client, &sn,
        "CREATE TABLE db (b1 BIGINT UNSIGNED, b2 BIGINT UNSIGNED, vb BIGINT NOT NULL, \
         PRIMARY KEY (b1, b2))");
    exec(&mut client, &sn,
        "CREATE VIEW dj AS SELECT a.a1 AS k1, a.a2 AS k2, a.va AS va, b.vb AS vb \
         FROM da a JOIN db b ON a.a1 = b.b1 AND a.a2 = b.b2");

    exec(&mut client, &sn,
        "INSERT INTO da (a1, a2, va) VALUES (1, 1, 11), (1, 2, 12), (2, 3, 23)");
    exec(&mut client, &sn,
        "INSERT INTO db (b1, b2, vb) VALUES (1, 1, 110), (1, 2, 120), (7, 7, 770)");

    assert_eq!(
        payload_rows(&mut client, &sn, "dj", &["k1", "k2", "va", "vb"]),
        vec![vec![1, 1, 11, 110], vec![1, 2, 12, 120]],
        "default full-PK distribution still co-partitions a full-PK join correctly",
    );
}
