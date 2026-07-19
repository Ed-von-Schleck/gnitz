#![cfg(feature = "integration")]

//! `ALTER TABLE` / `ALTER VIEW` reject/accept/round-trip matrix, end to end
//! through the SQL planner, client, and a single-worker engine. The multi-worker
//! distributed behaviors (exchange-join view rename, FK indices, ALTER VIEW
//! backfill, qname race) live in the Python e2e suite (`test_alter.py`).

mod common;
use common::*;
use gnitz_sql::SqlResult;
use gnitz_test_harness::ServerHandle;

/// Assert `sql` is rejected and its error message contains `needle`.
fn reject_contains(client: &mut gnitz_core::GnitzClient, sn: &str, sql: &str, needle: &str) {
    match try_exec(client, sn, sql) {
        Ok(_) => panic!("expected rejection of `{sql}`"),
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                msg.to_lowercase().contains(&needle.to_lowercase()),
                "`{sql}` rejected with `{msg}`, expected to contain `{needle}`"
            );
        }
    }
}

fn last(client: &mut gnitz_core::GnitzClient, sn: &str, sql: &str) -> SqlResult {
    try_exec(client, sn, sql).unwrap().pop().unwrap()
}

// ── Rename table ────────────────────────────────────────────────────────────

#[test]
fn rename_table_old_gone_new_resolves_data_intact() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut c, &sn, "INSERT INTO t VALUES (1, 100), (2, 200)");

    match last(&mut c, &sn, "ALTER TABLE t RENAME TO t2") {
        SqlResult::Altered { object, name } => {
            assert_eq!(object, "table");
            assert_eq!(name, "t2");
        }
        other => panic!("expected Altered, got {other:?}"),
    }

    // Old name is gone; new name resolves and the data is intact.
    reject_contains(&mut c, &sn, "SELECT * FROM t", "not found");
    let (schema, batch) = read_view(&mut c, &sn, "t2");
    let ci = col_idx(&schema, "v");
    let sum: i64 = (0..batch.len()).map(|r| i64_at(&batch, ci, r)).sum();
    assert_eq!(sum, 300, "renamed table must keep its rows");
}

#[test]
fn rename_missing_table_errors_unless_if_exists() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    reject_contains(&mut c, &sn, "ALTER TABLE nope RENAME TO x", "does not exist");
    // IF EXISTS makes it a no-op success.
    assert!(try_exec(&mut c, &sn, "ALTER TABLE IF EXISTS nope RENAME TO x").is_ok());
}

#[test]
fn cross_schema_rename_rejected() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY)");
    reject_contains(&mut c, &sn, "ALTER TABLE t RENAME TO otherschema.t2", "cross-schema");
    // Same-schema qualifier is accepted.
    exec(&mut c, &sn, &format!("ALTER TABLE t RENAME TO {sn}.t3"));
}

// ── Rename column ───────────────────────────────────────────────────────────

#[test]
fn rename_column_new_name_resolves() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut c, &sn, "INSERT INTO t VALUES (1, 42)");
    match last(&mut c, &sn, "ALTER TABLE t RENAME COLUMN v TO w") {
        SqlResult::Altered { object, name } => {
            assert_eq!(object, "column");
            assert_eq!(name, "w");
        }
        other => panic!("expected Altered, got {other:?}"),
    }
    // New name resolves; old name no longer does.
    let (schema, _) = read_view(&mut c, &sn, "t");
    assert!(schema.columns.iter().any(|col| col.name.eq_ignore_ascii_case("w")));
    assert!(!schema.columns.iter().any(|col| col.name.eq_ignore_ascii_case("v")));
    reject_contains(&mut c, &sn, "SELECT v FROM t", "");
}

#[test]
fn rename_column_collision_and_missing_rejected() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(
        &mut c,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)",
    );
    reject_contains(&mut c, &sn, "ALTER TABLE t RENAME COLUMN a TO b", "already exists");
    reject_contains(&mut c, &sn, "ALTER TABLE t RENAME COLUMN nope TO z", "not found");
}

#[test]
fn rename_column_on_view_rejected() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut c, &sn, "CREATE VIEW vw AS SELECT * FROM t");
    reject_contains(&mut c, &sn, "ALTER TABLE vw RENAME COLUMN v TO w", "base table");
}

// ── Rename a view via ALTER TABLE (Postgres form) ───────────────────────────

#[test]
fn rename_view_via_alter_table() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut c, &sn, "CREATE VIEW vw AS SELECT * FROM t");
    match last(&mut c, &sn, "ALTER TABLE vw RENAME TO vw2") {
        SqlResult::Altered { object, name } => {
            assert_eq!(object, "view");
            assert_eq!(name, "vw2");
        }
        other => panic!("expected Altered(view), got {other:?}"),
    }
    reject_contains(&mut c, &sn, "SELECT * FROM vw", "not found");
    let _ = read_view(&mut c, &sn, "vw2");
}

// ── ADD / DROP CONSTRAINT UNIQUE ────────────────────────────────────────────

#[test]
fn add_named_unique_constraint_round_trips() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    match last(&mut c, &sn, "ALTER TABLE t ADD CONSTRAINT uq UNIQUE (v)") {
        SqlResult::IndexCreated { .. } => {}
        other => panic!("ADD CONSTRAINT UNIQUE must return IndexCreated, got {other:?}"),
    }
    // The unique constraint is enforced.
    exec(&mut c, &sn, "INSERT INTO t VALUES (1, 5)");
    reject_contains(&mut c, &sn, "INSERT INTO t VALUES (2, 5)", "");
    // Drop it by name; the duplicate insert is then allowed.
    match last(&mut c, &sn, "ALTER TABLE t DROP CONSTRAINT uq") {
        SqlResult::Altered { object, name } => {
            assert_eq!(object, "constraint");
            assert_eq!(name, "uq");
        }
        other => panic!("expected Altered(constraint), got {other:?}"),
    }
    exec(&mut c, &sn, "INSERT INTO t VALUES (2, 5)");
}

#[test]
fn add_unnamed_unique_constraint_creates() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    match last(&mut c, &sn, "ALTER TABLE t ADD CONSTRAINT UNIQUE (v)") {
        SqlResult::IndexCreated { .. } => {}
        other => panic!("unnamed ADD CONSTRAINT UNIQUE must return IndexCreated, got {other:?}"),
    }
}

#[test]
fn drop_constraint_if_exists_missing_no_ops() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    // No such constraint; IF EXISTS swallows it.
    assert!(try_exec(&mut c, &sn, "ALTER TABLE t DROP CONSTRAINT IF EXISTS nope").is_ok());
    // Without IF EXISTS it errors.
    reject_contains(&mut c, &sn, "ALTER TABLE t DROP CONSTRAINT nope", "not found");
}

#[test]
fn add_constraint_rejections() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    // NOT VALID is rejected.
    reject_contains(
        &mut c,
        &sn,
        "ALTER TABLE t ADD CONSTRAINT c UNIQUE (v) NOT VALID",
        "NOT VALID",
    );
    // A non-UNIQUE constraint (CHECK) is rejected.
    reject_contains(&mut c, &sn, "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 0)", "UNIQUE");
    // A __fk_-infixed DROP CONSTRAINT name is rejected client-side.
    reject_contains(&mut c, &sn, "ALTER TABLE t DROP CONSTRAINT some__fk_thing", "__fk_");
}

// ── ALTER VIEW ... AS ───────────────────────────────────────────────────────

#[test]
fn alter_view_replaces_definition() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut c, &sn, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    exec(&mut c, &sn, "CREATE VIEW vw AS SELECT id, v FROM t WHERE v >= 20");
    // Redefine with a stricter filter.
    match last(&mut c, &sn, "ALTER VIEW vw AS SELECT id, v FROM t WHERE v >= 30") {
        SqlResult::Altered { object, name } => {
            assert_eq!(object, "view");
            assert_eq!(name, "vw");
        }
        other => panic!("expected Altered(view), got {other:?}"),
    }
    // The view now reflects the new definition (backfilled over the base).
    let (schema, batch) = read_view(&mut c, &sn, "vw");
    let ci = col_idx(&schema, "v");
    let vals: Vec<i64> = (0..batch.len()).map(|r| i64_at(&batch, ci, r)).collect();
    assert_eq!(vals, vec![30], "ALTER VIEW must replace the definition and backfill");
}

#[test]
fn alter_view_self_reference_rejected() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut c, &sn, "CREATE VIEW vw AS SELECT id, v FROM t");
    reject_contains(&mut c, &sn, "ALTER VIEW vw AS SELECT id, v FROM vw", "itself");
}

#[test]
fn alter_view_on_table_rejected() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    reject_contains(&mut c, &sn, "ALTER VIEW t AS SELECT id FROM t", "is a table");
}

#[test]
fn alter_view_with_dependent_view_restricted() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut c, &sn, "CREATE VIEW base AS SELECT id, v FROM t");
    exec(&mut c, &sn, "CREATE VIEW dependent AS SELECT id, v FROM base");
    // Redefining `base` must be rejected while `dependent` reads it (RESTRICT).
    reject_contains(
        &mut c,
        &sn,
        "ALTER VIEW base AS SELECT id, v FROM t WHERE v > 0",
        "dependency",
    );
    // `base` still exists and is readable.
    let _ = read_view(&mut c, &sn, "base");
}

// ── Rejected operations (feature not yet supported / unsupported clause) ─────

#[test]
fn unsupported_alter_operations_rejected() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(
        &mut c,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)",
    );

    reject_contains(&mut c, &sn, "ALTER TABLE t ADD COLUMN x BIGINT", "ADD COLUMN");
    reject_contains(&mut c, &sn, "ALTER TABLE t ALTER COLUMN a SET NOT NULL", "SET NOT NULL");
    reject_contains(
        &mut c,
        &sn,
        "ALTER TABLE t ALTER COLUMN a SET DATA TYPE INT",
        "SET DATA TYPE",
    );
    reject_contains(
        &mut c,
        &sn,
        "ALTER TABLE t ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY",
        "GENERATED",
    );
    // Multi-operation (comma-separated) ALTER.
    reject_contains(
        &mut c,
        &sn,
        "ALTER TABLE t RENAME COLUMN a TO a2, RENAME COLUMN b TO b2",
        "multiple",
    );
    // ONLY.
    reject_contains(&mut c, &sn, "ALTER TABLE ONLY t RENAME TO t2", "ONLY");
}

// ── DROP COLUMN / DROP NOT NULL (plan 4) ────────────────────────────────────

#[test]
fn drop_column_hides_middle_column_and_remaps_inserts() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(
        &mut c,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)",
    );
    exec(&mut c, &sn, "INSERT INTO t VALUES (1, 10, 100)");

    match last(&mut c, &sn, "ALTER TABLE t DROP COLUMN a") {
        SqlResult::Altered { object, name } => {
            assert_eq!(object, "column");
            assert_eq!(name, "a");
        }
        other => panic!("expected Altered, got {other:?}"),
    }

    // `SELECT *` excludes the dropped column (wildcard-leak regression), and the
    // pre-DROP row reads back unchanged on the visible columns.
    let (schema, batch) = read_view(&mut c, &sn, "t");
    assert_eq!(visible_names(&schema), vec!["id".to_string(), "b".to_string()]);
    let bi = col_idx(&schema, "b");
    let idi = col_idx(&schema, "id");
    assert_eq!(batch.len(), 1);
    assert_eq!(cell_i64(&schema, &batch, bi, 0), 100);
    assert_eq!(pk_i64_at(&schema, &batch, idi, 0), 1);

    // A new INSERT supplies only the visible columns; the value lands in `b`,
    // not the dropped slot (positional-remap regression).
    exec(&mut c, &sn, "INSERT INTO t VALUES (2, 200)");
    let (s2, b2) = read_view(&mut c, &sn, "t");
    let (bi2, idi2) = (col_idx(&s2, "b"), col_idx(&s2, "id"));
    let row2 = (0..b2.len())
        .find(|&r| pk_i64_at(&s2, &b2, idi2, r) == 2)
        .expect("row id=2 present");
    assert_eq!(cell_i64(&s2, &b2, bi2, row2), 200);

    // The dropped column is unnameable: a projection and an explicit-list INSERT
    // that reference it both error.
    reject_contains(&mut c, &sn, "SELECT a FROM t", "not found");
    reject_contains(&mut c, &sn, "INSERT INTO t (id, a, b) VALUES (3, 1, 2)", "column list");
}

#[test]
fn drop_column_rejects_pk_multi_cascade_and_missing() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(
        &mut c,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)",
    );

    reject_contains(&mut c, &sn, "ALTER TABLE t DROP COLUMN id", "primary key");
    // gnitz's dialect rejects the comma form `DROP COLUMN a, b` at parse time, so
    // the multi-column guard is defense-in-depth for a directly-built AST; via SQL
    // text the parser is what rejects it.
    reject_contains(&mut c, &sn, "ALTER TABLE t DROP COLUMN a, b", "parser");
    reject_contains(&mut c, &sn, "ALTER TABLE t DROP COLUMN a CASCADE", "cascade");
    reject_contains(&mut c, &sn, "ALTER TABLE t DROP COLUMN nope", "does not exist");
    reject_contains(&mut c, &sn, "ALTER TABLE nope DROP COLUMN a", "does not exist");
    // IF EXISTS on a missing table is a no-op success.
    assert!(try_exec(&mut c, &sn, "ALTER TABLE IF EXISTS nope DROP COLUMN a").is_ok());
}

#[test]
fn drop_column_rejects_index_covered() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(
        &mut c,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)",
    );
    exec(&mut c, &sn, "CREATE INDEX ix ON t (a)");
    // The index covers `a`; DROP COLUMN must reject until the index is dropped.
    reject_contains(&mut c, &sn, "ALTER TABLE t DROP COLUMN a", "index");
    // A column NOT covered by the index still drops cleanly.
    match last(&mut c, &sn, "ALTER TABLE t DROP COLUMN b") {
        SqlResult::Altered { .. } => {}
        other => panic!("expected Altered, got {other:?}"),
    }
}

#[test]
fn drop_not_null_permits_null_after() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    // All-fixed-int NOT NULL, so the table is on the FixedIntNonnull fast
    // comparator and DROP NOT NULL forces the real Generic swap.
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)");
    exec(&mut c, &sn, "INSERT INTO t VALUES (1, 100)");

    // A NULL is rejected before the ALTER (NOT NULL column).
    reject_contains(&mut c, &sn, "INSERT INTO t VALUES (2, NULL)", "null");

    match last(&mut c, &sn, "ALTER TABLE t ALTER COLUMN v DROP NOT NULL") {
        SqlResult::Altered { object, name } => {
            assert_eq!(object, "column");
            assert_eq!(name, "v");
        }
        other => panic!("expected Altered, got {other:?}"),
    }

    // A NULL is now accepted, and the pre-ALTER non-null row still reads back.
    exec(&mut c, &sn, "INSERT INTO t VALUES (2, NULL)");
    let (schema, batch) = read_view(&mut c, &sn, "t");
    assert_eq!(batch.len(), 2, "both rows present after DROP NOT NULL");
    let (vi, idi) = (col_idx(&schema, "v"), col_idx(&schema, "id"));
    let row1 = (0..batch.len())
        .find(|&r| pk_i64_at(&schema, &batch, idi, r) == 1)
        .expect("row id=1 present");
    assert_eq!(cell_i64(&schema, &batch, vi, row1), 100);
}

#[test]
fn drop_not_null_rejects_pk_column() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    exec(&mut c, &sn, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    reject_contains(&mut c, &sn, "ALTER TABLE t ALTER COLUMN id DROP NOT NULL", "primary");
}

#[test]
fn drop_restrict_dependent_view_but_rename_ok() {
    let Some(srv) = ServerHandle::start() else { return };
    let (mut c, sn) = make_planner(&srv);
    // `a` is NOT NULL so DROP NOT NULL on it is a real transition that reaches the
    // dependent-view guard (a no-op on an already-nullable column would not).
    exec(
        &mut c,
        &sn,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT NOT NULL, b BIGINT)",
    );
    exec(&mut c, &sn, "CREATE VIEW v AS SELECT id, b FROM t");

    // A dependent view blocks DROP COLUMN and DROP NOT NULL of ANY column (the
    // operator traces hold re-keyed base rows under the pre-ALTER comparator).
    reject_contains(&mut c, &sn, "ALTER TABLE t DROP COLUMN a", "dependent view");
    reject_contains(
        &mut c,
        &sn,
        "ALTER TABLE t ALTER COLUMN a DROP NOT NULL",
        "dependent view",
    );
    // But RENAME COLUMN is exempt (guard-scoping regression): it stays supported
    // with a dependent view, since views bind columns by ordinal.
    match last(&mut c, &sn, "ALTER TABLE t RENAME COLUMN a TO a2") {
        SqlResult::Altered { .. } => {}
        other => panic!("expected Altered, got {other:?}"),
    }
}
