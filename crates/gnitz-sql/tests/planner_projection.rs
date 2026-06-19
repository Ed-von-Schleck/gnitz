#![cfg(feature = "integration")]

use gnitz_core::GnitzClient;
use gnitz_sql::{GnitzSqlError, SqlPlanner};
use gnitz_test_harness::ServerHandle;

mod common;
use common::*;

fn names(s: &gnitz_core::Schema) -> Vec<String> {
    s.columns.iter().map(|c| c.name.to_lowercase()).collect()
}

// ── item 40: PK column move must preserve remaining order ─────────────

#[test]
fn test_projection_pk_move_preserves_order() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        // PK `id` is at source column index 2.
        p.execute("CREATE TABLE t (name TEXT, age BIGINT, id BIGINT PRIMARY KEY)")
            .unwrap();
        // SELECT name, age, id → PK moves to front; remaining order must be name, age.
        p.execute("CREATE VIEW v AS SELECT name, age, id FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    let names: Vec<String> = s.columns.iter().map(|c| c.name.to_lowercase()).collect();
    assert_eq!(
        names,
        vec!["id", "name", "age"],
        "PK move must shift remaining columns, not swap"
    );
}

// ── item 43: PK column with alias is PassThrough, not Computed ─────────

#[test]
fn test_projection_pk_alias_accepted() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE employees (id BIGINT PRIMARY KEY, name TEXT)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT id AS employee_id, name FROM employees")
            .unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert!(
        s.columns[0].name.eq_ignore_ascii_case("employee_id"),
        "aliased PK must be the first output column, got {:?}",
        s.columns[0].name
    );
    assert!(!s.columns[0].is_nullable, "PK column must stay non-nullable");
}

// ── Qualified / parenthesized refs are pass-throughs, not Computed ────

#[test]
fn test_projection_qualified_and_parenthesized_refs() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)").unwrap();
        // Qualified PK ref + qualified non-PK ref.
        p.execute("CREATE VIEW vq AS SELECT t.id, t.name FROM t AS t").unwrap();
        // Parenthesized refs (also bind to a bare ColRef).
        p.execute("CREATE VIEW vp AS SELECT (id), name FROM t").unwrap();
    }
    for v in ["vq", "vp"] {
        let (_, s) = client.resolve_table_or_view_id(&sn, v).unwrap();
        assert_eq!(
            names(&s),
            vec!["id", "name"],
            "{v}: qualified/parenthesized refs must pass through with source names (no _expr*)"
        );
        assert_eq!(s.pk_indices(), &[0], "{v}: PK at slot 0");
        assert!(!s.columns[0].is_nullable, "{v}: PK column non-nullable");
    }
}

// ── Computed-over-PK is accepted (no "PK cannot be computed" rejection) ──

#[test]
fn test_projection_computed_over_pk_accepted() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)").unwrap();
        // `id + 1` is a payload column; the physical PK `id` is auto-prepended.
        p.execute("CREATE VIEW v AS SELECT id + 1, name FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(names(&s), vec!["id", "_expr0", "name"]);
    assert_eq!(s.pk_indices(), &[0]);
}

// ── Duplicate PK column (SELECT id, id AS id2) is a payload copy ───────

#[test]
fn test_projection_duplicate_pk_column() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)").unwrap();
        p.execute("CREATE VIEW v AS SELECT id, id AS id2, name FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        names(&s),
        vec!["id", "id2", "name"],
        "first id is the physical PK; the second is a payload copy"
    );
    assert_eq!(s.pk_indices(), &[0]);
}

// ── Wildcard over a non-leading PK pins the PK to slot 0 ───────────────

#[test]
fn test_projection_wildcard_non_leading_pk() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (name TEXT, age BIGINT, id BIGINT PRIMARY KEY)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        names(&s),
        vec!["id", "name", "age"],
        "wildcard must reorder the source PK to slot 0"
    );
    assert_eq!(s.pk_indices(), &[0]);
}

// ── Duplicate output *name* is rejected; duplicate PK *value* is not ───

#[test]
fn test_projection_duplicate_output_name_rejected() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)").unwrap();
        // Two columns both named `name` → ambiguous downstream.
        let err = p.execute("CREATE VIEW vbad AS SELECT name, name FROM t").unwrap_err();
        assert!(
            matches!(err, GnitzSqlError::Plan(_)),
            "duplicate output name must be a Plan error, got {err:?}"
        );
        // Distinct names over the same PK value stay accepted.
        p.execute("CREATE VIEW vok AS SELECT id, id AS id2 FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "vok").unwrap();
    assert_eq!(names(&s), vec!["id", "id2"]);
}

// ── Compound-PK pass-through (explicit projection, reordered) ──────────

#[test]
fn test_projection_compound_pk_passthrough() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (a BIGINT, b BIGINT, c TEXT, PRIMARY KEY (a, b))")
            .unwrap();
        // PK columns appear out of order and after a non-PK column.
        p.execute("CREATE VIEW v AS SELECT b, c, a FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        names(&s),
        vec!["a", "b", "c"],
        "compound PK pinned to slots 0..2 in pk_indices() order, then c"
    );
    assert_eq!(s.pk_indices(), &[0, 1]);
    assert!(
        !s.columns[0].is_nullable && !s.columns[1].is_nullable,
        "both PK columns non-nullable"
    );
}

// ── Compound-PK with a computed payload (drives the expr-map) ──────────

#[test]
fn test_projection_compound_pk_computed_payload() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT, PRIMARY KEY (a, b))")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT a, b, c + 1 FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(names(&s), vec!["a", "b", "_expr2"]);
    assert_eq!(s.pk_indices(), &[0, 1]);
}

// ── Compound-PK auto-prepend (projection omits the PK) ─────────────────

#[test]
fn test_projection_compound_pk_auto_prepend() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (a BIGINT, b BIGINT, c TEXT, PRIMARY KEY (a, b))")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT c FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        names(&s),
        vec!["a", "b", "c"],
        "the full source PK is carried even when the projection omits it"
    );
    assert_eq!(s.pk_indices(), &[0, 1]);
}

// ── Parenthesized single PK ref over a compound PK keeps both columns ──

#[test]
fn test_projection_partial_pk_ref_keeps_full_pk() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (a BIGINT, b BIGINT, c TEXT, PRIMARY KEY (a, b))")
            .unwrap();
        // `(a)` references only the first PK column; `b` must still be prepended,
        // and `a` must not be duplicated.
        p.execute("CREATE VIEW v AS SELECT (a), c FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(names(&s), vec!["a", "b", "c"]);
    assert_eq!(s.pk_indices(), &[0, 1]);
}

// ── Chained projection over a compound-PK view inherits the PK ─────────

#[test]
fn test_projection_chained_compound_view() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE base (a BIGINT, b BIGINT, c TEXT, PRIMARY KEY (a, b))")
            .unwrap();
        p.execute("CREATE VIEW v1 AS SELECT b, c, a FROM base").unwrap();
        // v2 reads v1, whose resolved schema must report the compound PK.
        p.execute("CREATE VIEW v2 AS SELECT a, b FROM v1").unwrap();
    }
    let (_, s1) = client.resolve_table_or_view_id(&sn, "v1").unwrap();
    assert_eq!(s1.pk_indices(), &[0, 1]);
    let (_, s2) = client.resolve_table_or_view_id(&sn, "v2").unwrap();
    assert_eq!(names(&s2), vec!["a", "b"]);
    assert_eq!(s2.pk_indices(), &[0, 1]);
}

// ── k == 1 collapse: single PK not at column 0 ────────────────────────

#[test]
fn test_projection_k1_collapse_pk_middle() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    {
        let mut p = SqlPlanner::new(&mut client, &sn);
        p.execute("CREATE TABLE t (name TEXT, id BIGINT PRIMARY KEY, age BIGINT)")
            .unwrap();
        p.execute("CREATE VIEW v AS SELECT name, age, id FROM t").unwrap();
    }
    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(
        names(&s),
        vec!["id", "name", "age"],
        "single-PK move-to-front is the k == 1 case of the same loop"
    );
    assert_eq!(s.pk_indices(), &[0]);
}

// ── Compound-PK projection view: incremental INSERT / UPDATE / DELETE ──
//
// Drives data through the full server: the compound PK rides the PK region
// while duplicate-PK copies (a2, b2) and a computed column (vp) are payload, so
// reading the view back exposes the pass-through, the COPY_COL PK-decode, and
// the expr-map together. Each DML statement is its own epoch, so the assertions
// after each one check the *incremental* result, not a one-shot recompute.

fn compound_pk_view_data(client: &mut GnitzClient, sn: &str) {
    exec(
        client,
        sn,
        "CREATE TABLE t (a BIGINT, b BIGINT, val BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(
        client,
        sn,
        "CREATE VIEW v AS SELECT a, b, val, a AS a2, b AS b2, val + 1 AS vp FROM t",
    );
    exec(
        client,
        sn,
        "INSERT INTO t (a, b, val) VALUES (1, 1, 100), (1, 2, 200), (2, 1, 300)",
    );

    let cols = ["a2", "b2", "val", "vp"];
    assert_eq!(
        payload_rows(client, sn, "v", &cols),
        vec![vec![1, 1, 100, 101], vec![1, 2, 200, 201], vec![2, 1, 300, 301]],
        "initial contents: PK passed through, duplicate-PK copies + computed payload correct"
    );

    // UPDATE a payload value on a row that shares its first PK column with another.
    exec(client, sn, "UPDATE t SET val = 250 WHERE a = 1 AND b = 2");
    assert_eq!(
        payload_rows(client, sn, "v", &cols),
        vec![vec![1, 1, 100, 101], vec![1, 2, 250, 251], vec![2, 1, 300, 301]],
        "incremental UPDATE recomputes only the touched row"
    );

    // DELETE a row that differs from another only in the trailing PK column.
    exec(client, sn, "DELETE FROM t WHERE a = 1 AND b = 1");
    assert_eq!(
        payload_rows(client, sn, "v", &cols),
        vec![vec![1, 2, 250, 251], vec![2, 1, 300, 301]],
        "incremental DELETE drops exactly the retracted compound-PK row"
    );
}

#[test]
fn test_projection_compound_pk_view_data() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    compound_pk_view_data(&mut client, &sn);
}

// Same workload across 4 workers: the compound view PK must survive sharding on
// ingest and gather on read-back (exchange routing of a multi-column key).
#[test]
fn test_projection_compound_pk_view_data_multiworker() {
    let srv = match ServerHandle::start_n(4) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    compound_pk_view_data(&mut client, &sn);
}

// ── Wide compound PK (stride > 16): the byte-path tie-break ────────────
//
// A 3-column BIGINT PK packs to 24 bytes, so two rows sharing (a, b) collide on
// the first 16 PK bytes and are distinguished only past byte 16 by the trailing
// column c. The view must keep them as distinct rows.
#[test]
fn test_projection_wide_compound_pk_tie_break() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT, val BIGINT NOT NULL, PRIMARY KEY (a, b, c))",
    );
    exec(
        &mut client,
        &sn,
        "CREATE VIEW v AS SELECT a, b, c, val, a AS a2, b AS b2, c AS c2 FROM t",
    );

    // Confirm the source PK really is wide (stride 24 > 16).
    let (_, sv) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(sv.pk_indices(), &[0, 1, 2]);
    assert_eq!(sv.pk_stride(), 24, "three BIGINT PK columns pack to 24 bytes (wide)");

    // (1,1,1) and (1,1,2) share the first 16 PK bytes, differ only at byte 16+.
    exec(
        &mut client,
        &sn,
        "INSERT INTO t (a, b, c, val) VALUES (1, 1, 1, 100), (1, 1, 2, 200), (1, 2, 1, 300)",
    );
    let cols = ["a2", "b2", "c2", "val"];
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &cols),
        vec![vec![1, 1, 1, 100], vec![1, 1, 2, 200], vec![1, 2, 1, 300]],
        "rows sharing the first 16 PK bytes stay distinct on the byte tie-break path"
    );

    // Retract just one of the colliding pair.
    exec(&mut client, &sn, "DELETE FROM t WHERE a = 1 AND b = 1 AND c = 1");
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &cols),
        vec![vec![1, 1, 2, 200], vec![1, 2, 1, 300]],
        "deleting one tie-break sibling leaves the other intact"
    );
}

// ── Pure pass-through subset that omits a non-PK column ────────────────
//
// A projection that drops a non-PK column (and adds no computed column) takes
// the pure-projection branch, which builds a `cb.map` column list. That list
// must contain only the non-PK payload indices: the PK region is inherited by
// the bulk PK copy / build_map_output_schema's PK prepend. Passing PK indices
// there shifts every payload destination out of range and crashes the worker.

#[test]
fn test_projection_single_pk_subset_omits_col() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL, c BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT a, b FROM t");

    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(s.columns.len(), 2);
    assert_eq!(s.pk_indices(), &[0]);

    exec(
        &mut client,
        &sn,
        "INSERT INTO t (a, b, c) VALUES (1, 10, 100), (2, 20, 200)",
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["b"]),
        vec![vec![10], vec![20]],
        "single-PK subset: b correct, c omitted, PK index not aliased into payload"
    );
}

#[test]
fn test_projection_compound_pk_subset_omits_col() {
    let srv = match ServerHandle::start() {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, \
         d BIGINT NOT NULL, PRIMARY KEY (a, b))",
    );
    exec(&mut client, &sn, "CREATE VIEW v AS SELECT a, b, c FROM t");

    let (_, s) = client.resolve_table_or_view_id(&sn, "v").unwrap();
    assert_eq!(s.columns.len(), 3);
    assert_eq!(s.pk_indices(), &[0, 1]);

    exec(
        &mut client,
        &sn,
        "INSERT INTO t (a, b, c, d) VALUES (1, 2, 10, 99), (3, 4, 20, 88)",
    );
    assert_eq!(
        payload_rows(&mut client, &sn, "v", &["c"]),
        vec![vec![10], vec![20]],
        "c values correct; d omitted; no panic from PK indices in the cols slice"
    );
}
