#![cfg(feature = "integration")]

use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

// ── item 45: CTE column aliases applied ──────────────────────────────

#[test]
fn test_cte_column_aliases_applied() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)")
            .unwrap();
        p.execute(
            "CREATE VIEW v AS WITH cte(alias_a, alias_b) AS (SELECT a, b FROM t) \
             SELECT alias_a FROM cte",
        )
        .unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert!(
        s.columns[0].name.eq_ignore_ascii_case("alias_a"),
        "outer reference to CTE alias must bind, got {:?}",
        s.columns[0].name
    );
}

// ── item 45: CTE alias count mismatch ────────────────────────────────

#[test]
fn test_cte_alias_count_mismatch_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)")
        .unwrap();
    let err = p
        .execute("CREATE VIEW v AS WITH cte(x, y, z) AS (SELECT a, b FROM t) SELECT x FROM cte")
        .unwrap_err();
    match err {
        GnitzSqlError::Plan(s) => assert!(s.contains("alias"), "got: {}", s),
        e => panic!("expected Plan, got {:?}", e),
    }
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "mismatched-alias CTE view must not be registered"
    );
}

// ── item 15: a CTE body with a WHERE compiles into a hidden view segment ──
// chained to the final view — never silently discarded (which would return
// the unfiltered base table).

#[test]
fn test_cte_with_where_compiles_to_hidden_chain() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)")
        .unwrap();
    p.execute("INSERT INTO t VALUES (1, 10), (2, 3)").unwrap();
    p.execute("CREATE VIEW v AS WITH cte AS (SELECT * FROM t WHERE b > 5) SELECT a FROM cte")
        .unwrap();
    // The WHERE was compiled (a hidden filter segment feeds `v`), not dropped:
    // only the b > 5 row survives.
    let rows = payload_rows(&mut client, &sn, "v", &["a"]);
    assert_eq!(rows, vec![vec![1]], "CTE WHERE must filter, not be discarded");
}

#[test]
fn test_cte_with_fetch_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)")
        .unwrap();
    // FETCH was the one envelope clause inline_ctes silently dropped.
    let err = p
        .execute("CREATE VIEW v AS WITH cte AS (SELECT * FROM t FETCH FIRST 5 ROWS ONLY) SELECT a FROM cte")
        .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "CTE FETCH must be rejected, got {err:?}"
    );
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err());
}

#[test]
fn test_cte_nested_with_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)")
        .unwrap();
    // A nested CTE body (`WITH d …`) parses as body=Select, so only the loop-top
    // Query guard catches it — verifying the placement is load-bearing.
    let err = p
        .execute("CREATE VIEW v AS WITH cte AS (WITH d AS (SELECT * FROM t) SELECT * FROM d) SELECT a FROM cte")
        .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "nested CTE must be rejected, got {err:?}"
    );
    assert!(client.resolve_table_or_view_id(&sn, "v").is_err());
}

// A CTE body carrying an exotic Select clause (PREWHERE here) must be rejected by
// the shared guard, not silently inlined as a pass-through with the clause dropped.
#[test]
fn test_cte_with_prewhere_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)")
        .unwrap();
    let err = p
        .execute("CREATE VIEW v AS WITH cte AS (SELECT * FROM t PREWHERE b > 5) SELECT a FROM cte")
        .unwrap_err();
    assert!(
        matches!(err, GnitzSqlError::Unsupported(_)),
        "CTE PREWHERE must be rejected, got {err:?}"
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "rejected-CTE view must not be registered"
    );
}

#[test]
fn test_cte_subset_projection_compiles_to_hidden_chain() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)")
        .unwrap();
    p.execute("INSERT INTO t VALUES (1, 10), (2, 3)").unwrap();
    // Projection drops column b — not an identity pass-through, so the CTE body
    // compiles into a hidden projection segment (same machinery as a derived
    // table) rather than aliasing the base table.
    p.execute("CREATE VIEW v AS WITH cte AS (SELECT a FROM t) SELECT a FROM cte")
        .unwrap();
    let (_, schema) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["a"], "the subset projection must drop column b");
    let mut rows = payload_rows(&mut client, &sn, "v", &["a"]);
    rows.sort();
    assert_eq!(rows, vec![vec![1], vec![2]]);
}

#[test]
fn test_cte_plain_passthrough_accepted() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)")
        .unwrap();
    // Identity pass-through (all columns in order) stays supported.
    p.execute("CREATE VIEW v AS WITH cte AS (SELECT * FROM t) SELECT a FROM cte")
        .unwrap();
    assert!(client.resolve_table_or_view_id(&sn, "v").is_ok());
}

#[test]
fn test_cte_qualified_identity_passthrough_accepted() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT NOT NULL)")
        .unwrap();
    // A qualified projection (`t.a, t.b`) is a positional identity pass-through,
    // equivalent to `SELECT *`. The identity check accepts the
    // `CompoundIdentifier` form by comparing `parts[1]` against each source
    // column in order (§5.3) — it stays a positional test, never a name lookup.
    p.execute("CREATE VIEW v AS WITH cte AS (SELECT t.a, t.b FROM t) SELECT * FROM cte")
        .unwrap();
    let (_, schema) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        ["a", "b"],
        "qualified CTE pass-through must preserve column order"
    );
}

// ── Case-insensitive CTE references ──────────────────────────────────

#[test]
fn cte_reference_case_insensitive() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    let mut p = SqlPlanner::new(&mut client, &sn);
    p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)").unwrap();
    // A base table `cc` exists too — the CTE `Cc` must shadow it (standard SQL),
    // and the case-varying `cc` reference must bind to the CTE, not the base table.
    p.execute("CREATE TABLE cc (other BIGINT PRIMARY KEY)").unwrap();
    p.execute("CREATE VIEW v AS WITH Cc AS (SELECT id FROM t WHERE id > 0) SELECT id FROM cc")
        .unwrap();
    // The view's output column is `id` (from the CTE over `t`); had `cc` bound to
    // the base table (column `other`), `SELECT id FROM cc` would have failed.
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert!(
        s.columns.iter().any(|c| c.name.eq_ignore_ascii_case("id")),
        "CTE `Cc` must shadow base table `cc`; got {:?}",
        s.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
}

// ── A CTE / derived-table alias obeys the reserved-prefix rule ───────
// A user-chosen alias is held to the same rule as a created relation, rejected
// when the alias is registered (`Binder::cache_alias` — the one gate every alias
// passes to become resolvable). The bodies are valid, isolating the alias.

#[test]
fn reserved_prefix_cte_and_derived_alias_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    // A reserved-prefix CTE name is rejected when its alias registers.
    let e_cte = try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v1 AS WITH _cte AS (SELECT id FROM t WHERE id > 0) SELECT id FROM _cte",
    )
    .unwrap_err();
    assert!(
        matches!(e_cte, GnitzSqlError::Plan(_)),
        "reserved-prefix CTE alias must be a Plan error, got {e_cte:?}"
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v1").is_err(),
        "rejected-CTE view must not be registered"
    );
    // A reserved-prefix derived-table alias is likewise rejected.
    let e_derived = try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v2 AS SELECT x FROM (SELECT id AS x FROM t) AS __h0_0",
    )
    .unwrap_err();
    assert!(
        matches!(e_derived, GnitzSqlError::Plan(_)),
        "reserved-prefix derived-table alias must be a Plan error, got {e_derived:?}"
    );
    assert!(
        client.resolve_table_or_view_id(&sn, "v2").is_err(),
        "rejected derived-table view must not be registered"
    );
}

// ── A derived table must not see its FROM siblings (non-LATERAL) ──────
// A subquery in FROM is not correlated unless LATERAL (which is rejected), so its
// body may reference CTEs and catalog relations but never another item in the same
// FROM. Sibling aliases are registered only after the whole FROM is compiled.

#[test]
fn sibling_derived_table_out_of_scope() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    // No relation named `a` exists. `b`'s body `FROM a` names a sibling derived
    // table — out of scope for a non-LATERAL derived table — so it falls through to
    // the catalog and errors, rather than binding the sibling.
    let res = try_exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT b.id FROM (SELECT id FROM t) a \
         JOIN (SELECT id FROM a) b ON a.id = b.id",
    );
    assert!(res.is_err(), "sibling `a` must be out of scope");
    assert!(
        client.resolve_table_or_view_id(&sn, "v").is_err(),
        "the rejected view must not be registered"
    );
}

#[test]
fn sibling_alias_binds_catalog_relation() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(&mut client, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    exec(
        &mut client,
        &sn,
        "CREATE TABLE a (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    );
    // `b`'s `FROM a` binds the catalog table `a` (has `v`), not the sibling derived
    // `a` (only `id`). Compiling `SELECT b.v` proves the reference resolved through
    // the catalog: the sibling has no `v`.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT b.v FROM (SELECT id FROM t) a \
         JOIN (SELECT id, v FROM a) b ON a.id = b.id",
    );
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert!(
        s.columns.iter().any(|c| c.name.eq_ignore_ascii_case("v")),
        "b.v must resolve through catalog `a`; got {:?}",
        s.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
}

#[test]
fn sibling_reference_binds_cte_not_sibling() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
    );
    // CTE `a` (has `v`) is in scope for both derived tables. `b`'s `FROM a` must bind
    // the CTE — not the sibling derived `a`, which has only `id` — so `SELECT id, v`
    // resolves. The sibling `a` shadows the CTE only in the final body's join.
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS WITH a AS (SELECT id, v FROM t WHERE v > 0) \
         SELECT b.v FROM (SELECT id FROM t) a JOIN (SELECT id, v FROM a) b ON a.id = b.id",
    );
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert!(
        s.columns.iter().any(|c| c.name.eq_ignore_ascii_case("v")),
        "b.v must resolve through CTE `a`; got {:?}",
        s.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
}
