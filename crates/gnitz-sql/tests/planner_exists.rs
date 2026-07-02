#![cfg(feature = "integration")]

//! Circuit-shape and rejection tests for `[NOT] EXISTS` / `[NOT] IN (SELECT …)`
//! view compilation (the semi/anti-join builder).

use gnitz_core::{
    OPCODE_DISTINCT, OPCODE_EXCHANGE_SHARD, OPCODE_FILTER, OPCODE_JOIN_DELTA_TRACE, OPCODE_JOIN_DELTA_TRACE_RANGE,
    OPCODE_MAP_EXPR, OPCODE_NEGATE, OPCODE_NULL_EXTEND, OPCODE_PARTITION_FILTER, OPCODE_POSITIVE_PART, OPCODE_REDUCE,
    OPCODE_UNION,
};
use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

/// Assert `sql` fails with an `Unsupported`/`Bind` error containing `want`.
fn assert_rejects(client: &mut gnitz_core::GnitzClient, sn: &str, sql: &str, want: &str) {
    let err = try_exec(client, sn, sql).unwrap_err();
    let msg = match &err {
        GnitzSqlError::Unsupported(m) | GnitzSqlError::Bind(m) | GnitzSqlError::Plan(m) => m.clone(),
        other => format!("{other:?}"),
    };
    assert!(msg.contains(want), "expected error containing {want:?}, got: {msg}");
}

// ── Equi circuit shapes ──────────────────────────────────────────────────────

/// EXISTS and NOT EXISTS over a non-nullable equi key: node counts pin the
/// derivation — 2 join terms, 1 positive_part (ν), no exchange, no distinct, no
/// null_extend, and `a_all` aliasing `reindex_a` (2 reindex maps only).
#[test]
fn test_equi_exists_circuit_shape() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE eq_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE eq_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)")
            .unwrap();
        p.execute(
            "CREATE VIEW eq_semi AS SELECT eq_a.v FROM eq_a \
             WHERE EXISTS (SELECT 1 FROM eq_b WHERE eq_b.k = eq_a.k)",
        )
        .unwrap();
        p.execute(
            "CREATE VIEW eq_anti AS SELECT eq_a.v FROM eq_a \
             WHERE NOT EXISTS (SELECT 1 FROM eq_b WHERE eq_b.k = eq_a.k)",
        )
        .unwrap();
    }
    let semi = client.resolve_table_or_view_id(&sn, "eq_semi").unwrap().0;
    let anti = client.resolve_table_or_view_id(&sn, "eq_anti").unwrap().0;

    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();
    let n = |vid: u64, op: u64| opcode_node_count(nodes, vid, op);

    for vid in [semi, anti] {
        assert_eq!(n(vid, OPCODE_JOIN_DELTA_TRACE), 2, "the 2 symmetric join terms");
        assert_eq!(n(vid, OPCODE_POSITIVE_PART), 1, "exactly the ν clamp");
        assert_eq!(n(vid, OPCODE_EXCHANGE_SHARD), 0, "equi semi-join has no exchange");
        assert_eq!(n(vid, OPCODE_DISTINCT), 0, "weight-exact — no distinct");
        assert_eq!(n(vid, OPCODE_NULL_EXTEND), 0, "no columns are null-filled");
        assert_eq!(
            n(vid, OPCODE_MAP_EXPR),
            2,
            "reindex_a + reindex_b only (a_all aliases reindex_a on a NOT NULL key)"
        );
        assert_eq!(n(vid, OPCODE_FILTER), 0, "no local conjuncts, no NULL gates");
    }
    // Anti = ν alone (1 negate inside positive_diff, 2 unions: π_A + diff).
    assert_eq!(n(anti, OPCODE_NEGATE), 1);
    assert_eq!(n(anti, OPCODE_UNION), 2);
    // Semi = A − ν (one more negate + union).
    assert_eq!(n(semi, OPCODE_NEGATE), 2);
    assert_eq!(n(semi, OPCODE_UNION), 3);
}

/// A nullable correlation key splits `a_all` off `reindex_a` (3 reindex maps)
/// and adds the two NULL gates.
#[test]
fn test_equi_exists_nullable_key_shape() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE nk_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT)")
            .unwrap();
        p.execute("CREATE TABLE nk_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT)")
            .unwrap();
        p.execute(
            "CREATE VIEW nk_v AS SELECT id FROM nk_a \
             WHERE EXISTS (SELECT 1 FROM nk_b WHERE nk_b.k = nk_a.k)",
        )
        .unwrap();
    }
    let vid = client.resolve_table_or_view_id(&sn, "nk_v").unwrap().0;
    let nodes = scan_circuit_nodes(&mut client);
    let n = |op: u64| opcode_node_count(nodes.as_ref(), vid, op);
    assert_eq!(n(OPCODE_MAP_EXPR), 3, "reindex_a + reindex_b + separate a_all");
    assert_eq!(n(OPCODE_FILTER), 2, "one NULL gate per side");
}

// ── Band circuit shape ───────────────────────────────────────────────────────

#[test]
fn test_band_exists_circuit_shape() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE bd_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE bd_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)")
            .unwrap();
        p.execute(
            "CREATE VIEW bd_v AS SELECT id FROM bd_a \
             WHERE EXISTS (SELECT 1 FROM bd_b WHERE bd_b.k = bd_a.k AND bd_b.t < bd_a.t)",
        )
        .unwrap();
    }
    let vid = client.resolve_table_or_view_id(&sn, "bd_v").unwrap().0;
    let nodes = scan_circuit_nodes(&mut client);
    let n = |op: u64| opcode_node_count(nodes.as_ref(), vid, op);
    assert_eq!(n(OPCODE_JOIN_DELTA_TRACE_RANGE), 2, "the 2 band join terms");
    assert_eq!(n(OPCODE_EXCHANGE_SHARD), 1, "one output exchange over the source PK");
    assert_eq!(n(OPCODE_POSITIVE_PART), 1, "the ν clamp");
    assert_eq!(n(OPCODE_PARTITION_FILTER), 0, "band scatters by eq prefix — no trim");
    assert_eq!(n(OPCODE_REDUCE), 0, "band uses π_A(inner), not a threshold");
}

// ── Pure-range circuit shapes ────────────────────────────────────────────────

#[test]
fn test_pure_range_exists_circuit_shape() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE pr_a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)")
            .unwrap();
        p.execute("CREATE TABLE pr_b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)")
            .unwrap();
        p.execute(
            "CREATE VIEW pr_semi AS SELECT id FROM pr_a \
             WHERE EXISTS (SELECT 1 FROM pr_b WHERE pr_b.y < pr_a.x)",
        )
        .unwrap();
        p.execute(
            "CREATE VIEW pr_anti AS SELECT id FROM pr_a \
             WHERE NOT EXISTS (SELECT 1 FROM pr_b WHERE pr_b.y < pr_a.x)",
        )
        .unwrap();
    }
    let semi = client.resolve_table_or_view_id(&sn, "pr_semi").unwrap().0;
    let anti = client.resolve_table_or_view_id(&sn, "pr_anti").unwrap().0;
    let nodes = scan_circuit_nodes(&mut client);
    let nodes = nodes.as_ref();
    let n = |vid: u64, op: u64| opcode_node_count(nodes, vid, op);

    for vid in [semi, anti] {
        assert_eq!(
            n(vid, OPCODE_JOIN_DELTA_TRACE_RANGE),
            2,
            "the two threshold match terms (ΔA⋈{{m}}, Δm⋈I(A))"
        );
        assert_eq!(n(vid, OPCODE_REDUCE), 1, "the inline m = MIN/MAX reduce");
        assert_eq!(n(vid, OPCODE_EXCHANGE_SHARD), 1, "one output exchange");
        assert_eq!(n(vid, OPCODE_POSITIVE_PART), 0, "threshold form needs no clamp");
        assert_eq!(n(vid, OPCODE_PARTITION_FILTER), 1, "int_a trims the broadcast");
    }
    // Semi = matched alone; anti = A − matched (one negate, one more union).
    assert_eq!(n(semi, OPCODE_NEGATE), 0);
    assert_eq!(n(anti, OPCODE_NEGATE), 1);
    assert_eq!(n(anti, OPCODE_UNION) - n(semi, OPCODE_UNION), 1);
    // Anti materializes the extra a_pass re-key.
    assert_eq!(n(anti, OPCODE_MAP_EXPR) - n(semi, OPCODE_MAP_EXPR), 1);
}

// ── Rejections ───────────────────────────────────────────────────────────────

#[test]
fn test_exists_rejections() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE rj_a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL, nk BIGINT, \
         big DECIMAL(38,0) NOT NULL)",
    );
    exec(
        &mut client,
        &sn,
        "CREATE TABLE rj_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL, nk BIGINT, \
         big DECIMAL(38,0) NOT NULL)",
    );

    // Two subquery conjuncts.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r1 AS SELECT * FROM rj_a \
         WHERE EXISTS (SELECT 1 FROM rj_b WHERE rj_b.k = rj_a.k) \
         AND EXISTS (SELECT 1 FROM rj_b WHERE rj_b.w = rj_a.v)",
        "at most one EXISTS/IN subquery conjunct",
    );
    // Same relation on both sides.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r2 AS SELECT * FROM rj_a \
         WHERE EXISTS (SELECT 1 FROM rj_a AS x WHERE x.k = rj_a.k)",
        "own FROM relation",
    );
    // Outer-only conjunct inside the subquery WHERE.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r3 AS SELECT * FROM rj_a \
         WHERE EXISTS (SELECT 1 FROM rj_b WHERE rj_b.k = rj_a.k AND rj_a.v > 5)",
        "hoist it into the view's own WHERE",
    );
    // Residual correlation (a both-sides conjunct the join cannot consume).
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r4 AS SELECT * FROM rj_a \
         WHERE EXISTS (SELECT 1 FROM rj_b WHERE rj_b.k = rj_a.k AND rj_b.w <> rj_a.v)",
        "match-existence",
    );
    // NOT IN over a nullable column.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r5 AS SELECT * FROM rj_a WHERE nk NOT IN (SELECT k FROM rj_b)",
        "NOT NULL",
    );
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r5b AS SELECT * FROM rj_a WHERE k NOT IN (SELECT nk FROM rj_b)",
        "NOT NULL",
    );
    // Tuple IN.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r6 AS SELECT * FROM rj_a WHERE (k, v) IN (SELECT k, w FROM rj_b)",
        "tuple",
    );
    // Uncorrelated EXISTS.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r7 AS SELECT * FROM rj_a WHERE EXISTS (SELECT 1 FROM rj_b WHERE rj_b.w > 5)",
        "uncorrelated EXISTS",
    );
    // 16-byte pure-range column (DECIMAL(38,0) = U128 has no MIN/MAX accumulator).
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r8 AS SELECT * FROM rj_a \
         WHERE EXISTS (SELECT 1 FROM rj_b WHERE rj_b.big < rj_a.big)",
        "8-byte integer range column",
    );
    // GROUP BY together with the subquery conjunct.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r9 AS SELECT k, COUNT(*) FROM rj_a \
         WHERE EXISTS (SELECT 1 FROM rj_b WHERE rj_b.k = rj_a.k) GROUP BY k",
        "GROUP BY/aggregates",
    );
    // Under OR the conjunct partitioner never intercepts — the binder's targeted
    // placement message fires.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r10 AS SELECT * FROM rj_a \
         WHERE v = 1 OR EXISTS (SELECT 1 FROM rj_b WHERE rj_b.k = rj_a.k)",
        "top-level AND conjunct",
    );
    // Subquery with a join inside.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r11 AS SELECT * FROM rj_a \
         WHERE EXISTS (SELECT 1 FROM rj_b JOIN rj_a AS z ON rj_b.k = z.k WHERE rj_b.k = rj_a.k)",
        "single FROM table without JOINs",
    );
    // Subquery with GROUP BY inside.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r12 AS SELECT * FROM rj_a \
         WHERE EXISTS (SELECT k FROM rj_b WHERE rj_b.k = rj_a.k GROUP BY k)",
        "GROUP BY",
    );
    // Nested subquery inside the subquery WHERE.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r13 AS SELECT * FROM rj_a \
         WHERE EXISTS (SELECT 1 FROM rj_b WHERE rj_b.k = rj_a.k AND \
         EXISTS (SELECT 1 FROM rj_b AS z WHERE z.k = rj_b.w))",
        "nested subqueries",
    );
    // IN subquery selecting more than one column.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r14 AS SELECT * FROM rj_a WHERE k IN (SELECT k, w FROM rj_b)",
        "exactly one plain column",
    );
    // Duplicate alias between the outer FROM and the subquery.
    assert_rejects(
        &mut client,
        &sn,
        "CREATE VIEW r15 AS SELECT * FROM rj_a AS t \
         WHERE EXISTS (SELECT 1 FROM rj_b AS t WHERE t.k = t.k)",
        "rename one",
    );
}

/// Cross-width promotion (INT outer vs BIGINT inner) and a compound (2-column)
/// correlation key both register cleanly.
#[test]
fn test_exists_accepts_promotion_and_compound_keys() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE cw_a (id BIGINT NOT NULL PRIMARY KEY, k INT NOT NULL, k2 BIGINT NOT NULL)")
        .unwrap();
    p.execute("CREATE TABLE cw_b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, k2 BIGINT NOT NULL)")
        .unwrap();
    assert!(
        p.execute(
            "CREATE VIEW cw_v AS SELECT id FROM cw_a \
             WHERE k IN (SELECT k FROM cw_b)"
        )
        .is_ok(),
        "cross-width IN should register"
    );
    assert!(
        p.execute(
            "CREATE VIEW cw_v2 AS SELECT id FROM cw_a \
             WHERE EXISTS (SELECT 1 FROM cw_b WHERE cw_b.k = cw_a.k2 AND cw_b.k2 = cw_a.k2)"
        )
        .is_ok(),
        "compound-key EXISTS should register"
    );
}
