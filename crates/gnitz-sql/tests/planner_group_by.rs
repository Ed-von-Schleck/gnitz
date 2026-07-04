#![cfg(feature = "integration")]

use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// The declared nullability of `schema`'s column `name` (case-insensitive).
fn col_nullable(schema: &gnitz_core::Schema, name: &str) -> bool {
    schema
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
        .unwrap()
        .is_nullable
}

// ── item 42: AVG ignores NULLs and never emits NaN/Infinity ───────
//
// The plan's literal scenario (a *zero-count* group reaching `float_div` and
// producing NaN) is unreachable: AVG's divisor is COUNT(non-null), and
// `EXPR_FLOAT_DIV` null-marks any zero divisor, so an all-NULL group's AVG reads
// as NULL — never NaN/Infinity. The group itself is NOT suppressed: group
// existence is gated on net cardinality (`COUNT(*) > 0`, the appended companion),
// so a group that has a row — even an all-NULL one — is emitted with AVG = NULL,
// exactly as SQL requires and as the transition-to-all-NULL sibling below. This
// guard pins the no-NaN invariant.

#[test]
fn test_avg_ignores_nulls_no_nan() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // signed g → synthetic-PK path so g is a readable payload column.
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, x DOUBLE)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT g, AVG(x) AS ax FROM t GROUP BY g")
            .unwrap();
        // g=5: only NULL → the group exists (one row) → AVG = NULL (not NaN).
        // g=6: NULL + 3.0 → mean ignores NULL → 3.0.
        // g=7: 4.0 → 4.0.
        p.execute("INSERT INTO t (id, g, x) VALUES (1, 5, NULL), (2, 6, NULL), (3, 6, 3.0), (4, 7, 4.0)")
            .unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    let g_col = col_idx(&schema, "g");
    let ax_col = col_idx(&schema, "ax");
    let ax_payload = ax_col - 1; // single synthetic U128 PK at index 0

    // Every group with a row is emitted; the only NULL average is the all-NULL
    // group g=5, and no emitted average is ever NaN/Infinity.
    let mut saw_g5_null = false;
    for r in 0..batch.len() {
        let g = i64_at(&batch, g_col, r);
        if is_null_at(&batch, ax_payload, r) {
            assert_eq!(g, 5, "only the all-NULL group g=5 has a NULL average");
            saw_g5_null = true;
        } else {
            let ax = f64_at(&batch, ax_col, r);
            assert!(ax.is_finite(), "AVG must never be NaN/Infinity, got {} for g={}", ax, g);
            match g {
                6 => assert!((ax - 3.0).abs() < 1e-9, "g=6 mean ignores NULL → 3.0, got {}", ax),
                7 => assert!((ax - 4.0).abs() < 1e-9, "g=7 → 4.0, got {}", ax),
                other => panic!("unexpected group {}", other),
            }
        }
    }
    assert!(
        saw_g5_null,
        "the all-NULL group g=5 is emitted with AVG = NULL (cardinality gate, not suppressed)"
    );
    assert_eq!(batch.len(), 3, "groups g=5 (AVG NULL), g=6, and g=7 are all emitted");
}

// A group that transitions to all-NULL via DELETE persists — like a from-inception
// all-NULL group (above): group existence is gated on net cardinality
// (`COUNT(*) > 0`, the appended companion), and the group still holds the NULL row,
// so it is re-emitted with COUNT_NON_NULL=0 → AVG = float_div(_, 0) → NULL. The AVG
// output column must therefore be nullable to represent the emitted NULL; declaring
// it NOT NULL mislabels that value.

#[test]
fn test_avg_emits_null_on_delete_to_all_null() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // signed g → synthetic-PK path so g is a readable payload column.
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, x DOUBLE)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT g, AVG(x) AS ax FROM t GROUP BY g")
            .unwrap();
        // g=8 has one non-NULL contributor (5.0) and one NULL row.
        p.execute("INSERT INTO t (id, g, x) VALUES (10, 8, 5.0), (11, 8, NULL)")
            .unwrap();
    }

    // The AVG column must be declared nullable.
    {
        let (schema, batch) = read_view(&mut client, &sn, "v");
        assert!(
            schema.columns[col_idx(&schema, "ax")].is_nullable,
            "AVG output column must be nullable"
        );
        assert_eq!(batch.len(), 1, "one group before delete");
        let ax_payload = col_idx(&schema, "ax") - 1;
        assert!(!is_null_at(&batch, ax_payload, 0), "AVG=5.0 is not NULL before delete");
        assert!((f64_at(&batch, col_idx(&schema, "ax"), 0) - 5.0).abs() < 1e-9);
    }

    // Delete the only non-NULL contributor; the group still has the NULL row.
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("DELETE FROM t WHERE id = 10").unwrap();
    }

    // The group persists (it still has rows) and its AVG reads as NULL —
    // not absent, not 0.0, not NaN.
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 1, "the group must persist — it still has the NULL row");
    assert_eq!(i64_at(&batch, col_idx(&schema, "g"), 0), 8);
    let ax_payload = col_idx(&schema, "ax") - 1;
    assert!(
        is_null_at(&batch, ax_payload, 0),
        "AVG of a group that lost all non-NULL values must read as NULL"
    );
}

// ── item 13: numeric aggregate type rejection ────────────────────────

#[test]
fn test_sum_string_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, s TEXT)")
        .unwrap();
    let err = p
        .execute("CREATE VIEW v AS SELECT g, SUM(s) AS x FROM t GROUP BY g")
        .unwrap_err();
    assert!(matches!(err, GnitzSqlError::Bind(_)), "expected Bind, got {:?}", err);
}

#[test]
fn test_sum_avg_u128_rejected() {
    // SUM/AVG accumulate into a 64-bit slot; a U128 (DECIMAL(38,0)) source
    // overflows it and the engine's decode marks it unreachable, so the planner
    // must reject SUM/AVG over U128 at bind time (mirroring the MIN/MAX reject).
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, big DECIMAL(38,0) NOT NULL)")
        .unwrap();
    for func in ["SUM", "AVG"] {
        let err = p
            .execute(&format!(
                "CREATE VIEW v AS SELECT g, {func}(big) AS x FROM t GROUP BY g"
            ))
            .unwrap_err();
        assert!(
            matches!(err, GnitzSqlError::Bind(_)),
            "expected Bind for {func}(u128), got {:?}",
            err
        );
    }
}

// ── C2: HAVING-only aggregates are type-validated (not just SELECT-list) ──
//
// A HAVING aggregate binds through the same leaf binder as a SELECT-list one
// (unorderable MIN/MAX rejected there), and its SUM/AVG argument type is
// validated by `push_agg_specs` when it is materialised.

#[test]
fn test_having_only_sum_over_u128_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, big DECIMAL(38,0) NOT NULL)")
        .unwrap();
    let err = p
        .execute("CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING SUM(big) > 0")
        .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Bind(_)),
        "HAVING-only SUM(u128) must be Bind, got {err:?}"
    );
}

#[test]
fn test_having_only_avg_over_uuid_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, u UUID)")
        .unwrap();
    let err = p
        .execute("CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING AVG(u) > 0")
        .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Bind(_)),
        "HAVING-only AVG(uuid) must be Bind, got {err:?}"
    );
}

#[test]
fn test_having_only_min_over_string_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, s TEXT)")
        .unwrap();
    let err = p
        .execute("CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING MIN(s) > 'a'")
        .unwrap_err();
    // The same leaf-binder rejection a SELECT-list MIN(string) gets.
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "HAVING-only MIN(string) must be Unsupported, got {err:?}"
    );
}

#[test]
fn test_having_only_sum_over_bigint_succeeds() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, c BIGINT NOT NULL)")
        .unwrap();
    // HAVING-only SUM over an i64 column is valid and must still compile.
    p.execute("CREATE VIEW v AS SELECT g, COUNT(*) AS cnt FROM t GROUP BY g HAVING SUM(c) > 0")
        .unwrap();
}

#[test]
fn test_min_uuid_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, u UUID)")
        .unwrap();
    let err = p
        .execute("CREATE VIEW v AS SELECT g, MIN(u) AS x FROM t GROUP BY g")
        .unwrap_err();
    // A clean rejection (not a server panic) — the binder may reject MIN(uuid)
    // as Unsupported before the planner's numeric/orderable guard is reached.
    assert!(
        matches!(err, GnitzSqlError::Bind(_) | GnitzSqlError::Unsupported(_)),
        "expected clean rejection, got {:?}",
        err
    );
}

/// A GROUP BY over a cross-sign `U64 = I64` join view aggregates its `_join_pk`,
/// which is the signed-128 type `I128`. A 16-byte value cannot be order-encoded
/// into the AVI slot or accumulated in the 64-bit aggregate slot, so MIN/MAX/SUM/
/// AVG over it must be rejected cleanly at bind time — this guards the engine's
/// `decode_signed(I128)` / `decode_float(I128)` `unreachable!` panics.
#[test]
fn test_agg_i128_join_pk_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT UNSIGNED NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL)")
        .unwrap();
    // The join's `_join_pk` is I128 (the cross-sign common type).
    p.execute("CREATE VIEW jv AS SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON a.fk = b.fk")
        .unwrap();
    for func in ["MIN", "MAX", "SUM", "AVG"] {
        let err = p
            .execute(&format!(
                "CREATE VIEW v_{func} AS SELECT aid, {func}(_join_pk) AS x FROM jv GROUP BY aid"
            ))
            .unwrap_err();
        assert!(
            matches!(err, GnitzSqlError::Bind(_) | GnitzSqlError::Unsupported(_)),
            "expected clean rejection for {func}(_join_pk:I128), got {:?}",
            err
        );
    }
}

// ── item 12 + 34: float SUM and AVG ──────────────────────────────────

#[test]
fn test_float_sum_and_avg() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE m (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, x DOUBLE NOT NULL)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT g, SUM(x) AS sx, AVG(x) AS ax FROM m GROUP BY g")
            .unwrap();
        p.execute("INSERT INTO m (id, g, x) VALUES (1, 7, 1.5), (2, 7, 2.5)")
            .unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 1, "one group expected");
    let sx = f64_at(&batch, col_idx(&schema, "sx"), 0);
    let ax = f64_at(&batch, col_idx(&schema, "ax"), 0);
    assert!((sx - 4.0).abs() < 1e-9, "SUM(x) should be 4.0, got {}", sx);
    assert!((ax - 2.0).abs() < 1e-9, "AVG(x) should be 2.0, got {}", ax);
    // Output column types must be F64.
    assert_eq!(
        schema.columns[col_idx(&schema, "sx")].type_code,
        gnitz_core::TypeCode::F64
    );
    assert_eq!(
        schema.columns[col_idx(&schema, "ax")].type_code,
        gnitz_core::TypeCode::F64
    );
}

// ── item 11: HAVING binds to the correct aggregate ───────────────────

#[test]
fn test_having_binds_correct_aggregate() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // signed g forces the synthetic-PK path so g is a readable payload column.
        p.execute("CREATE TABLE h (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL)")
            .unwrap();
        // HAVING filters on MAX(c2), which appears AFTER SUM(c1) in the SELECT list.
        p.execute("CREATE VIEW v AS SELECT g, SUM(c1) AS s1, MAX(c2) AS m2 FROM h GROUP BY g HAVING MAX(c2) > 5")
            .unwrap();
        // g=1: SUM(c1)=10, MAX(c2)=3 → MAX(c2) not > 5 → excluded.
        // g=2: SUM(c1)=1,  MAX(c2)=9 → MAX(c2) > 5 → included.
        // (If HAVING wrongly bound to SUM(c1), g=1 would survive and g=2 would not.)
        p.execute("INSERT INTO h (id, g, c1, c2) VALUES (1, 1, 10, 3), (2, 2, 1, 9)")
            .unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "v");
    assert_eq!(batch.len(), 1, "exactly one group should satisfy HAVING MAX(c2) > 5");
    let m2 = i64_at(&batch, col_idx(&schema, "m2"), 0);
    let g = i64_at(&batch, col_idx(&schema, "g"), 0);
    assert_eq!(m2, 9, "surviving group must be the one with MAX(c2)=9");
    assert_eq!(g, 2, "surviving group must be g=2");
}

// ── GROUP BY over a compound-PK source ───────────────────────────────
//
// The reduce output carries the source PK in its 0..k region (source-PK order),
// so a GROUP BY view over a compound-PK table persists a real multi-column PK.
// Grouping by the full PK is `group_set_eq_pk` (each group a singleton); grouping
// by one PK component is the single-natural path that actually aggregates.
//
// Group columns that coincide with the source PK become the view's natural PK
// region: the post-reduce MAP inherits them verbatim and the alias renames the
// PK slot in place (no duplicate payload copy). The SELECT * result batch holds
// those values in `batch.pks`, which the client decodes back to native LE on
// receive — so `payload_rows` reads them from the PK region by name.

#[test]
fn test_group_by_compound_pk_full_and_partial() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    // Full-PK grouping → compound natural PK [0, 1]; each (a,b) group a singleton.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW g_full AS SELECT a AS ka, b AS kb, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY a, b",
    );
    // One-component grouping → single natural PK [0]; aggregates across b.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW g_part AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY a",
    );

    let (_, sf) = client.resolve_table_or_view_id(&sn, "g_full").unwrap();
    assert_eq!(sf.pk_indices(), &[0, 1], "full-PK GROUP BY carries the compound PK");
    let (_, sp) = client.resolve_table_or_view_id(&sn, "g_part").unwrap();
    assert_eq!(sp.pk_indices(), &[0], "one-component GROUP BY → single natural PK");

    exec(
        &mut client,
        &sn,
        "INSERT INTO t (a, b, v) VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)",
    );

    assert_eq!(
        payload_rows(&mut client, &sn, "g_full", &["ka", "kb", "n", "s"]),
        vec![vec![1, 1, 1, 10], vec![1, 2, 1, 20], vec![2, 1, 1, 30]],
        "full-PK groups are singletons with the source PK passed through"
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "g_part", &["ka", "n", "s"]),
        vec![vec![1, 2, 30], vec![2, 1, 30]],
        "a=1 aggregates the two b-rows (n=2, s=30); a=2 has one (n=1, s=30)"
    );
}

#[test]
fn test_group_by_compound_pk_permuted() {
    // GROUP BY b, a permutes the PK in the grouping list, but the reduce output PK
    // region is always source-PK order [a, b]. If the helper mapped by GROUP BY
    // order instead, the a/b output values would transpose — values are chosen so
    // that transposition is observable.
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW gp AS SELECT a AS ka, b AS kb, SUM(v) AS s FROM t GROUP BY b, a",
    );

    let (_, s) = client.resolve_table_or_view_id(&sn, "gp").unwrap();
    assert_eq!(
        s.pk_indices(),
        &[0, 1],
        "permuted full-PK GROUP BY still carries source-order PK"
    );

    // a ∈ {1,2}, b ∈ {7,8}; the (a,b) pairing must survive the permutation.
    exec(
        &mut client,
        &sn,
        "INSERT INTO t (a, b, v) VALUES (1, 7, 100), (2, 8, 200)",
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "gp", &["ka", "kb", "s"]),
        vec![vec![1, 7, 100], vec![2, 8, 200]],
        "output 'ka' carries a's values and 'kb' carries b's — not transposed"
    );
}

#[test]
fn test_having_over_compound_natural_pk() {
    // HAVING references a compound-PK group column by name; it must bind to the
    // correct reduce column via the shared helper. Values make an a-filter and a
    // b-filter select different groups, so a mis-binding would be observable.
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW gh AS SELECT a AS ka, b AS kb, SUM(v) AS s FROM t GROUP BY a, b HAVING a > 1",
    );
    exec(
        &mut client,
        &sn,
        "INSERT INTO t (a, b, v) VALUES (1, 1, 10), (2, 2, 20), (3, 1, 30)",
    );
    // HAVING a > 1 keeps (2,2) and (3,1). (A b>1 mis-binding would keep only (2,2).)
    assert_eq!(
        payload_rows(&mut client, &sn, "gh", &["ka", "kb", "s"]),
        vec![vec![2, 2, 20], vec![3, 1, 30]],
        "HAVING binds to group column 'a', not 'b'"
    );
}

// Change C regression: a reduce that shards by one component of a compound PK
// must run the in-circuit exchange (the source is partitioned by the FULL PK, so
// same-`a` rows live on different workers). With the old analyzers the exchange
// was skipped and each worker reduced a partial group — wrong only under W>1.
// MUST run multi-worker.
#[test]
fn test_group_by_compound_pk_multiworker() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    // GROUP BY a (one PK component): the path the old analyzers wrongly skipped.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW g_part AS SELECT a AS ka, COUNT(*) AS n, SUM(v) AS s FROM t GROUP BY a",
    );
    // GROUP BY a, b (full PK, the co-partition-skip path): each group a singleton.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW g_full AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM t GROUP BY a, b",
    );
    // GROUP BY b, a (PERMUTED full PK): group_set_eq_pk shards in PK order, so the
    // view stays partitioned by its real PK (a, b). Without the PK-order shard
    // normalization the shuffle hash-routes by (b, a) and the gather drops the
    // rows that hashed to a different worker than their PK home — wrong only W>1.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW g_perm AS SELECT a AS ka, b AS kb, COUNT(*) AS n FROM t GROUP BY b, a",
    );

    // Repeated `a` across distinct `b` so same-`a` rows scatter by full-PK hash.
    exec(
        &mut client,
        &sn,
        "INSERT INTO t (a, b, v) VALUES \
         (1, 1, 10), (1, 2, 20), (1, 3, 30), (2, 1, 40), (2, 2, 50), (3, 1, 60)",
    );

    // GROUP BY a must aggregate ACROSS workers: one row per a with the full count.
    let part = payload_rows(&mut client, &sn, "g_part", &["ka", "n", "s"]);
    assert_eq!(
        part,
        vec![vec![1, 3, 60], vec![2, 2, 90], vec![3, 1, 60]],
        "compound-PK GROUP BY a aggregates every b across all workers"
    );
    assert_eq!(
        part.len(),
        3,
        "exactly one row per distinct a — no per-worker partial groups"
    );

    // GROUP BY full PK and its permutation: 6 singleton groups, count 1 each. The
    // permuted view must return the SAME complete set (no rows dropped on gather).
    let expected_singletons = vec![
        vec![1, 1, 1],
        vec![1, 2, 1],
        vec![1, 3, 1],
        vec![2, 1, 1],
        vec![2, 2, 1],
        vec![3, 1, 1],
    ];
    let full = payload_rows(&mut client, &sn, "g_full", &["ka", "kb", "n"]);
    assert_eq!(
        full, expected_singletons,
        "full-PK GROUP BY is co-partitioned and stays correct"
    );
    let perm = payload_rows(&mut client, &sn, "g_perm", &["ka", "kb", "n"]);
    assert_eq!(
        perm, expected_singletons,
        "permuted full-PK GROUP BY shards in PK order — no rows dropped under W>1"
    );
}

// ── GROUP BY view schema: no duplicate columns when group cols == source PK ───
//
// When the group set coincides with the source PK (natural-PK reduce path), the
// post-reduce MAP inherits the PK region verbatim. The view schema must rename
// those inherited PK slots in place, not re-emit each group column as a second
// payload copy.

#[test]
fn test_group_by_compound_pk_source_no_dup_cols() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (k1, k2))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vg AS SELECT k1, k2, SUM(v) AS total FROM t GROUP BY k1, k2",
    );

    let (_, s) = client.resolve_table_or_view_id(&sn, "vg").unwrap();
    // Schema must be exactly [k1, k2, total] — no duplicates.
    assert_eq!(s.columns.len(), 3, "no duplicate columns: k1, k2, total only");
    assert_eq!(s.columns[0].name, "k1");
    assert_eq!(s.columns[1].name, "k2");
    assert_eq!(s.columns[2].name, "total");
    assert_eq!(s.pk_indices(), &[0, 1]);
}

#[test]
fn test_group_by_natural_pk_source_no_dup_col() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vg AS SELECT id, SUM(v) AS total FROM t GROUP BY id",
    );

    let (_, s) = client.resolve_table_or_view_id(&sn, "vg").unwrap();
    assert_eq!(s.columns.len(), 2, "no duplicate columns: id, total only");
    assert_eq!(s.columns[0].name, "id");
    assert_eq!(s.columns[1].name, "total");
}

// ── float GROUP BY key rejection (Fix A1) ────────────────────────────
//
// A float grouping key splits -0.0/+0.0 and distinct-NaN bit patterns into
// separate groups (and routes them to distinct workers), violating SQL grouping
// where -0.0 = +0.0. Mirrors the PK / join-key float exclusions. STRING and
// integer keys are unaffected.

#[test]
fn test_group_by_float_key_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    // FLOAT → F32, DOUBLE → F64; both must be rejected as grouping keys.
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, f FLOAT NOT NULL, d DOUBLE NOT NULL, v BIGINT NOT NULL)")
        .unwrap();
    for col in ["f", "d"] {
        let err = p
            .execute(&format!(
                "CREATE VIEW vbad AS SELECT {col}, COUNT(*) AS n FROM t GROUP BY {col}"
            ))
            .unwrap_err();
        assert!(
            matches!(&err, GnitzSqlError::Unsupported(s) if s.contains("float")),
            "expected float-key Unsupported for GROUP BY {col}, got {:?}",
            err,
        );
    }

    // A non-float (integer / string) group key still succeeds.
    p.execute("CREATE TABLE u (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, s TEXT NOT NULL, v BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE VIEW vi AS SELECT g, COUNT(*) AS n FROM u GROUP BY g")
        .unwrap();
    p.execute("CREATE VIEW vs AS SELECT s, COUNT(*) AS n FROM u GROUP BY s")
        .unwrap();
}

// ── Aggregate output-column nullability (SUM/MIN/MAX vs COUNT) ────────
//
// emit.rs sets the null bit on a direct aggregate output when the accumulator is
// is_untouched() and not empty_renders_zero() (an all-NULL group), so SUM/MIN/MAX
// emit NULL while COUNT / COUNT_NON_NULL render a concrete 0 (null bit clear). The
// view schema's nullability must match, or a schema-driven decoder reads raw zero
// bytes as a live value (or, for COUNT, surfaces a forbidden NULL).

#[test]
fn test_aggregate_output_nullability_schema() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE agg_t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, x BIGINT)")
            .unwrap();
        p.execute(
            "CREATE VIEW agg_v AS SELECT g, SUM(x) AS sx, MIN(x) AS mnx, MAX(x) AS mxx, \
             COUNT(x) AS cx, COUNT(*) AS ca FROM agg_t GROUP BY g",
        )
        .unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "agg_v").unwrap();
    let nullable = |name: &str| col_nullable(&s, name);
    assert!(nullable("sx"), "SUM output must be nullable");
    assert!(nullable("mnx"), "MIN output must be nullable");
    assert!(nullable("mxx"), "MAX output must be nullable");
    assert!(!nullable("cx"), "COUNT(x) output must be non-nullable");
    assert!(!nullable("ca"), "COUNT(*) output must be non-nullable");
}

// A grouped SUM/MIN/MAX over a NON-nullable source can never render NULL — an
// emptied group is retracted, not null-filled — so the output column is declared
// NOT NULL (the exact structural fact, tighter than the blanket nullable mark). A
// global (no GROUP BY) aggregate keeps its nullability: an empty source seeds a
// NULL ground row. Complements the nullable-source case above.
#[test]
fn test_direct_aggregate_nonnull_source_nullability_schema() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE nn_t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL)")
            .unwrap();
        p.execute(
            "CREATE VIEW nn_grouped AS SELECT g, SUM(x) AS sx, MIN(x) AS mnx, MAX(x) AS mxx, \
             COUNT(x) AS cx FROM nn_t GROUP BY g",
        )
        .unwrap();
        p.execute("CREATE VIEW nn_global AS SELECT SUM(x) AS sx, MIN(x) AS mnx, MAX(x) AS mxx FROM nn_t")
            .unwrap();
    }
    let (_, sg) = client.resolve_table_or_view_id(&sn, "nn_grouped").unwrap();
    assert!(
        !col_nullable(&sg, "sx"),
        "grouped SUM over a NOT NULL source is NOT NULL"
    );
    assert!(
        !col_nullable(&sg, "mnx"),
        "grouped MIN over a NOT NULL source is NOT NULL"
    );
    assert!(
        !col_nullable(&sg, "mxx"),
        "grouped MAX over a NOT NULL source is NOT NULL"
    );
    assert!(!col_nullable(&sg, "cx"), "COUNT(x) output is NOT NULL");

    let (_, sgl) = client.resolve_table_or_view_id(&sn, "nn_global").unwrap();
    assert!(
        col_nullable(&sgl, "sx"),
        "global SUM is nullable (empty-source ground row)"
    );
    assert!(
        col_nullable(&sgl, "mnx"),
        "global MIN is nullable (empty-source ground row)"
    );
    assert!(
        col_nullable(&sgl, "mxx"),
        "global MAX is nullable (empty-source ground row)"
    );
}

#[test]
fn test_aggregate_all_null_group_emits_null() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // g UNSIGNED NOT NULL → natural-PK path (g is the lone PK at slot 0).
        p.execute("CREATE TABLE an_t (id BIGINT PRIMARY KEY, g BIGINT UNSIGNED NOT NULL, x BIGINT)")
            .unwrap();
        // COUNT(*) keeps the group alive even though every x is NULL — without a
        // count-of-rows aggregate a from-inception all-NULL group is suppressed.
        p.execute(
            "CREATE VIEW an_v AS SELECT g, SUM(x) AS sx, MIN(x) AS mnx, MAX(x) AS mxx, \
             COUNT(x) AS cx, COUNT(*) AS ca FROM an_t GROUP BY g",
        )
        .unwrap();
        // One group (g=5) whose only rows have x = NULL.
        p.execute("INSERT INTO an_t (id, g, x) VALUES (1, 5, NULL)").unwrap();
        p.execute("INSERT INTO an_t (id, g, x) VALUES (2, 5, NULL)").unwrap();
    }
    let (schema, batch) = read_view(&mut client, &sn, "an_v");
    assert_eq!(batch.len(), 1, "the group is kept alive by COUNT(*)");
    let pk = schema.pk_cols.len();
    let cx = col_idx(&schema, "cx");
    let ca = col_idx(&schema, "ca");
    // SUM/MIN/MAX over an all-NULL group read as NULL — the schema marks these
    // columns nullable (§8), so a decoder surfaces NULL rather than zero bytes.
    assert!(
        is_null_at(&batch, col_idx(&schema, "sx") - pk, 0),
        "SUM of all-NULL group must be NULL"
    );
    assert!(
        is_null_at(&batch, col_idx(&schema, "mnx") - pk, 0),
        "MIN of all-NULL group must be NULL"
    );
    assert!(
        is_null_at(&batch, col_idx(&schema, "mxx") - pk, 0),
        "MAX of all-NULL group must be NULL"
    );
    // COUNT columns decode to integers: COUNT(x)=0, COUNT(*)=2. emit.rs renders an
    // untouched COUNT_NON_NULL as a concrete 0 with the null bit CLEAR (its
    // empty_renders_zero family), so COUNT(x) of an all-NULL group is 0, never the
    // forbidden NULL on this non-nullable column.
    assert!(
        !is_null_at(&batch, cx - pk, 0),
        "COUNT(x) of all-NULL group is 0, not NULL"
    );
    assert_eq!(i64_at(&batch, cx, 0), 0, "COUNT(x) of all-NULL group must decode to 0");
    assert_eq!(i64_at(&batch, ca, 0), 2, "COUNT(*) must count both rows");
}

// ── HAVING wildcard aggregate: SUM(*)/MIN(*)/MAX(*)/AVG(*) must error, not panic ──
//
// Only COUNT(*) accepts a wildcard. Every other aggregate rejects it at the
// leaf binder ("requires exactly one column argument"), and `push_agg_specs`
// keeps its own wildcard guard so no caller can reach its `arg_col.unwrap()`.

#[test]
fn having_wildcard_agg_returns_error_not_panic() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");

    let sqls = [
        "CREATE VIEW vs AS SELECT id, SUM(v) FROM t GROUP BY id HAVING SUM(*) > 0",
        "CREATE VIEW vmn AS SELECT id, MIN(v) FROM t GROUP BY id HAVING MIN(*) > 0",
        "CREATE VIEW vmx AS SELECT id, MAX(v) FROM t GROUP BY id HAVING MAX(*) > 0",
        "CREATE VIEW va AS SELECT id, AVG(v) FROM t GROUP BY id HAVING AVG(*) > 0",
    ];
    for sql in &sqls {
        assert!(
            try_exec(&mut client, &sn, sql).is_err(),
            "expected plan error for wildcard aggregate in HAVING, got Ok for: {}",
            sql
        );
    }

    // COUNT(*) in HAVING remains valid.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW vc AS SELECT id, COUNT(*) FROM t GROUP BY id HAVING COUNT(*) > 0",
    );
}
