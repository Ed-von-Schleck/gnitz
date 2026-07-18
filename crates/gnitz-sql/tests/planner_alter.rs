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
    reject_contains(&mut c, &sn, "ALTER TABLE t DROP COLUMN a", "DROP COLUMN");
    reject_contains(&mut c, &sn, "ALTER TABLE t ALTER COLUMN a SET NOT NULL", "SET NOT NULL");
    reject_contains(
        &mut c,
        &sn,
        "ALTER TABLE t ALTER COLUMN a DROP NOT NULL",
        "DROP NOT NULL",
    );
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
