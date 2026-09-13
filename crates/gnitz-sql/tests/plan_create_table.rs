//! `CREATE TABLE` planned with no server against a hand-built catalog: the
//! precedence between the passes, the constraint spellings that are rejected,
//! and the bundle a plan carries.

use gnitz_core::{FkTarget, RelClass, TypeCode};

mod pure;
use pure::*;

use gnitz_sql::{plan_create_table, GnitzSqlError, TablePlan};

/// Plan `sql` against `cat`. The statement must parse as a `CREATE TABLE`.
fn plan_table(cat: &gnitz_core::CatalogSnapshot, sql: &str) -> Result<TablePlan, GnitzSqlError> {
    match parse(sql) {
        gnitz_sql::sqlparser::ast::Statement::CreateTable(c) => plan_create_table(&c, cat, SN),
        other => panic!("`{sql}` is not a CREATE TABLE: {other}"),
    }
}

/// The `Create` payload of a plan expected to succeed.
struct Created {
    cols: Vec<gnitz_core::ColumnDef>,
    pk_indices: Vec<u32>,
    props: gnitz_core::TableProps,
    unique_indexes: Vec<(Vec<u32>, String)>,
}

fn created(cat: &gnitz_core::CatalogSnapshot, sql: &str) -> Created {
    match plan_table(cat, sql).unwrap_or_else(|e| panic!("`{sql}`: {e:?}")) {
        TablePlan::Create {
            cols, pk_indices, props, unique_indexes, ..
        } => Created { cols, pk_indices, props, unique_indexes },
        TablePlan::Skip { .. } => panic!("`{sql}` planned a skip"),
    }
}

/// `p (id BIGINT PK, u BIGINT)` with a **unique** index on `u`, and `q` with the
/// same shape and a non-unique one — so only `p.u` is a legal non-PK FK target.
fn fk_catalog() -> gnitz_core::CatalogSnapshot {
    let i = TypeCode::I64;
    let cols = || vec![col("id", i), col("u", i)];
    let mut cat = catalog(vec![("q", rel(40, RelClass::Table, false, cols(), vec![0], &[&[1]]))]);
    cat.insert(
        SN,
        "p",
        Some(rel_with(41, RelClass::Table, false, cols(), vec![0], &[(&[1], true)])),
    );
    cat
}

// ── the PRIMARY KEY precedence table ─────────────────────────────────────────

/// Every ordering the two PK spellings can be written in. Rows 1-3 and 5 are
/// also pinned end-to-end in `engine_ddl.rs`; row 4 is pinned only here.
#[test]
fn primary_key_precedence() {
    let cat = catalog(vec![]);
    for (sql, variant, needle) in [
        (
            "CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT, PRIMARY KEY (a, b))",
            "Plan",
            "Multiple PRIMARY KEYs",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT PRIMARY KEY)",
            "Plan",
            "Multiple PRIMARY KEYs",
        ),
        // The list resolves before the conflict is reported, so the typo is named.
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, PRIMARY KEY (typo))",
            "Bind",
            "typo",
        ),
        // Two table-level clauses: the second is the conflict.
        (
            "CREATE TABLE t (a BIGINT, b BIGINT, PRIMARY KEY (a), PRIMARY KEY (b))",
            "Plan",
            "Multiple PRIMARY KEYs",
        ),
    ] {
        assert_rejects(sql, plan_table(&cat, sql), variant, needle);
    }

    // A self-referencing FK declared before the inline PK it targets: the whole
    // PK is known before any FK resolves.
    let c = created(
        &cat,
        "CREATE TABLE sref (refc BIGINT REFERENCES sref(id), id BIGINT PRIMARY KEY)",
    );
    assert_eq!(c.pk_indices, vec![1]);
    assert_eq!(c.cols[0].fk, Some(FkTarget::SelfTable { col: 1 }));
}

// ── the constraint spellings that are rejected ───────────────────────────────

/// A repeated UNIQUE is one constraint written twice, whichever spelling it is
/// written in — the column-option form is not silently deduped.
#[test]
fn a_repeated_unique_is_rejected_in_both_spellings() {
    let cat = catalog(vec![]);
    for sql in [
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT UNIQUE UNIQUE)",
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, UNIQUE(a), UNIQUE(a))",
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT UNIQUE, UNIQUE(a))",
    ] {
        assert_rejects(sql, plan_table(&cat, sql), "Plan", "duplicate UNIQUE constraint");
    }
}

/// Two FOREIGN KEYs on one column: the loop would resolve both and keep one.
#[test]
fn a_second_foreign_key_on_one_column_is_rejected() {
    let cat = fk_catalog();
    let sql = "CREATE TABLE c (id BIGINT PRIMARY KEY, a BIGINT REFERENCES p(id), FOREIGN KEY (a) REFERENCES q(id))";
    assert_rejects(sql, plan_table(&cat, sql), "Plan", "more than one FOREIGN KEY");
}

/// A UNIQUE equal to the lone PK creates no index, so a name written on it would
/// name nothing and `DROP CONSTRAINT` would fail. The unnamed spelling keeps the
/// silent drop — nothing the user wrote is lost.
#[test]
fn a_named_unique_equal_to_the_lone_pk_is_rejected() {
    let cat = catalog(vec![]);
    for sql in [
        "CREATE TABLE t (id BIGINT PRIMARY KEY, CONSTRAINT uq UNIQUE(id))",
        "CREATE TABLE t (id BIGINT PRIMARY KEY CONSTRAINT uq UNIQUE)",
    ] {
        assert_rejects(sql, plan_table(&cat, sql), "Plan", "uq");
    }
    // Unnamed: accepted, and no index is planned.
    let c = created(&cat, "CREATE TABLE t (id BIGINT PRIMARY KEY UNIQUE)");
    assert!(c.unique_indexes.is_empty(), "the lone PK already provides it");
    // A compound-PK member declared UNIQUE is not individually unique, so its
    // index survives — named or not.
    let c = created(
        &cat,
        "CREATE TABLE c (a BIGINT UNSIGNED, b BIGINT UNSIGNED, PRIMARY KEY (a, b), CONSTRAINT uq UNIQUE(a))",
    );
    assert_eq!(c.unique_indexes, vec![(vec![0], "uq".to_string())]);
}

/// The engine's whole schema-admissibility rule is the column cap plus the PK
/// rules; the cap is the planner's too, so a too-wide table is `Unsupported`
/// here rather than an `Exec` failure inside the client gateway — the same
/// variant the view surface produces.
#[test]
fn a_table_past_the_column_cap_is_unsupported() {
    let cat = catalog(vec![]);
    let cols: Vec<String> = (0..66).map(|i| format!("c{i} BIGINT")).collect();
    let sql = format!("CREATE TABLE t (id BIGINT PRIMARY KEY, {})", cols.join(", "));
    assert_rejects(&sql, plan_table(&cat, &sql), "Unsupported", "65");
}

/// `OR REPLACE` would discard the table's rows; `DROP TABLE` says that out loud.
#[test]
fn or_replace_is_rejected() {
    let cat = catalog(vec![]);
    let sql = "CREATE OR REPLACE TABLE t (id BIGINT PRIMARY KEY)";
    assert_rejects(sql, plan_table(&cat, sql), "Unsupported", "OR REPLACE");
}

/// A stream holds no rows, so it backs no index, enforces no referential action
/// and seeds no SERIAL generator. The engine refuses all three by relation id;
/// the planner names the column.
#[test]
fn a_stream_refuses_serial_a_foreign_key_and_a_unique() {
    let cat = fk_catalog();
    for (sql, needle) in [
        (
            "CREATE TABLE s (id BIGINT PRIMARY KEY, a BIGINT UNIQUE) WITH (stream = true)",
            "UNIQUE",
        ),
        ("CREATE TABLE s (id SERIAL PRIMARY KEY) WITH (stream = true)", "SERIAL"),
        (
            "CREATE TABLE s (id BIGINT PRIMARY KEY, r BIGINT REFERENCES p(id)) WITH (stream = true)",
            "FOREIGN KEY",
        ),
        // Written table-level, the UNIQUE arrives through the constraints pass.
        (
            "CREATE TABLE s (id BIGINT PRIMARY KEY, a BIGINT, UNIQUE(a)) WITH (stream = true)",
            "UNIQUE",
        ),
    ] {
        assert_rejects(sql, plan_table(&cat, sql), "Unsupported", needle);
    }
    // The lone-PK-equal spelling plans no index, so it reaches no owner check.
    let c = created(
        &cat,
        "CREATE TABLE s (id BIGINT PRIMARY KEY UNIQUE) WITH (stream = true)",
    );
    assert!(c.props.stream && c.unique_indexes.is_empty());
}

/// A clause `CREATE TABLE` does not honour is named rather than dropped — a
/// dropped CHECK or DEFAULT is a constraint never enforced — and a FOREIGN KEY
/// target must be one stored, individually unique column.
#[test]
fn an_unhonoured_clause_or_fk_target_is_named() {
    let mut known = fk_catalog();
    let u = TypeCode::U64;
    let cp = table(
        42,
        vec![col("a", u), col("b", u), ncol("payload", TypeCode::I64)],
        vec![0, 1],
    );
    known.insert(SN, "cp", Some(cp));
    let st = rel(
        43,
        RelClass::Stream,
        false,
        vec![col("id", TypeCode::I64)],
        vec![0],
        &[],
    );
    known.insert(SN, "st", Some(st));
    for (sql, variant, needle) in [
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY) AS SELECT id FROM p",
            "Unsupported",
            "CTAS",
        ),
        (
            "CREATE TEMPORARY TABLE t (id BIGINT PRIMARY KEY)",
            "Unsupported",
            "TEMPORARY",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT CHECK (x > 0))",
            "Unsupported",
            "CHECK",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT, CHECK (x > 0))",
            "Unsupported",
            "CHECK",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT DEFAULT 5)",
            "Unsupported",
            "DEFAULT",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES p(id) ON DELETE CASCADE)",
            "Unsupported",
            "ON DELETE",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES phantom(id))",
            "Bind",
            "does not exist",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES st(id))",
            "Unsupported",
            "is a stream",
        ),
        // A compound PK has no lone column, so a member qualifies only through a
        // UNIQUE index of its own.
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT UNSIGNED REFERENCES cp(a))",
            "Unsupported",
            "UNIQUE index",
        ),
        (
            "CREATE TABLE t (x BIGINT UNSIGNED, y BIGINT UNSIGNED, id BIGINT PRIMARY KEY, \
             FOREIGN KEY (x, y) REFERENCES cp (a, b))",
            "Unsupported",
            "multi-column",
        ),
    ] {
        let (plan, _) = resolving(&known, |c| plan_table(c, sql).map(|_| ()));
        assert_rejects(sql, plan, variant, needle);
    }
}

/// The generated id has no compound form, so a SERIAL column is the table's
/// whole primary key: outside the PK, inside a compound one, or beside a second
/// SERIAL it is refused.
#[test]
fn a_serial_column_must_be_the_single_column_pk() {
    let cat = catalog(vec![]);
    for (sql, needle) in [
        (
            "CREATE TABLE a (id SERIAL, k BIGINT PRIMARY KEY)",
            "single-column PRIMARY KEY",
        ),
        (
            "CREATE TABLE b (id SERIAL, x BIGINT, PRIMARY KEY (id, x))",
            "single-column PRIMARY KEY",
        ),
        (
            "CREATE TABLE c (id SERIAL PRIMARY KEY, id2 SERIAL)",
            "at most one SERIAL column",
        ),
    ] {
        assert_rejects(sql, plan_table(&cat, sql), "Unsupported", needle);
    }
}

// ── the bundle a plan carries ────────────────────────────────────────────────

/// The FK loop rewrites its child column to the parent's type, and the UNIQUE
/// index over that column is checked against the rewritten one.
#[test]
fn an_fk_child_adopts_the_parent_type_before_the_index_is_checked() {
    let cat = fk_catalog();
    let c = created(
        &cat,
        "CREATE TABLE c (id BIGINT PRIMARY KEY, r INT UNIQUE REFERENCES p(id))",
    );
    assert_eq!(c.cols[1].type_code, TypeCode::I64, "widened to the parent's type");
    assert_eq!(c.cols[1].fk, Some(FkTarget::Table { id: 41, col: 0 }));
    assert_eq!(c.unique_indexes, vec![(vec![1], format!("{SN}__c__idx_r"))]);

    // A non-PK parent column is a legal target only with a single-column UNIQUE
    // index on it.
    created(
        &cat,
        "CREATE TABLE c2 (id BIGINT PRIMARY KEY, r BIGINT REFERENCES p(u))",
    );
    let sql = "CREATE TABLE c3 (id BIGINT PRIMARY KEY, r BIGINT REFERENCES q(u))";
    assert_rejects(sql, plan_table(&cat, sql), "Unsupported", "UNIQUE index");
}

/// Distinct column sets can render the same auto-name base when a column name
/// contains `_`; the second takes the `_2` suffix, within the bundle.
#[test]
fn colliding_auto_names_are_disambiguated_within_the_bundle() {
    let cat = catalog(vec![]);
    let c = created(
        &cat,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, d BIGINT, e_f BIGINT, d_e BIGINT, f BIGINT, \
         UNIQUE(d, e_f), UNIQUE(d_e, f))",
    );
    assert_eq!(
        c.unique_indexes,
        vec![
            (vec![1, 2], format!("{SN}__t__idx_d_e_f")),
            (vec![3, 4], format!("{SN}__t__idx_d_e_f_2")),
        ]
    );
    // Two constraint names folding to one canonical name would write two IDX_TAB
    // rows the catalog cannot both reach.
    let sql = "CREATE TABLE u (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, \
               CONSTRAINT MyIdx UNIQUE(a), CONSTRAINT myidx UNIQUE(b))";
    assert_rejects(sql, plan_table(&cat, sql), "Plan", "'myidx'");
}

/// `IF NOT EXISTS` tests the name, whatever kind stands under it — and the
/// AST-only clause rejections outrank it.
#[test]
fn if_not_exists_plans_a_skip_but_a_bad_with_key_still_errors() {
    let cat = base();
    match plan_table(&cat, "CREATE TABLE IF NOT EXISTS tv (id BIGINT PRIMARY KEY)") {
        Ok(TablePlan::Skip { existing_id }) => assert_eq!(existing_id, 24, "the standing view's id"),
        other => panic!("expected a skip, got {:?}", other.map(|_| "a create")),
    }
    let sql = "CREATE TABLE IF NOT EXISTS tv (id BIGINT PRIMARY KEY) WITH (bogus = true)";
    assert_rejects(sql, plan_table(&cat, sql), "Plan", "bogus");
}

/// A name the snapshot has not probed is the control signal `plan_resolving`
/// answers, and the planner asks for exactly the names it needs.
#[test]
fn the_loop_resolves_only_the_fk_targets_and_the_if_not_exists_name() {
    let known = fk_catalog();
    let (plan, asked) = resolving(&known, |c| {
        plan_table(c, "CREATE TABLE c (id BIGINT PRIMARY KEY, r BIGINT REFERENCES p(id))").map(|_| ())
    });
    plan.unwrap();
    assert_eq!(asked, vec!["p".to_string()]);

    // No clause, no probe of the table's own name; a self-FK resolves in-flight.
    let (plan, asked) = resolving(&known, |c| {
        plan_table(c, "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES t(id))").map(|_| ())
    });
    plan.unwrap();
    assert!(asked.is_empty(), "asked for {asked:?}");
}

// ── the schema qualifier ─────────────────────────────────────────────────────

/// The active schema is a session parameter, so a qualifier is accepted only
/// when it names that schema. Dropping it silently planned `REFERENCES other.t`
/// as a *self*-reference.
#[test]
fn a_cross_schema_qualifier_is_rejected_rather_than_dropped() {
    let cat = fk_catalog();
    for sql in [
        "CREATE TABLE other.t (id BIGINT PRIMARY KEY)",
        "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES other.t(id))",
    ] {
        assert_rejects(sql, plan_table(&cat, sql), "Unsupported", "cross-schema");
    }
    // The same-schema spelling is the one a Postgres user writes.
    let c = created(&cat, &format!("CREATE TABLE {SN}.t (id BIGINT PRIMARY KEY)"));
    assert_eq!(c.pk_indices, vec![0]);
}
