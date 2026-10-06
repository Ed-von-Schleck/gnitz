//! `CREATE TABLE` planned with no server against a hand-built catalog: the
//! precedence between the passes, the constraint spellings that are rejected,
//! and the bundle a plan carries.

use gnitz_core::{FkTarget, InlineUniqueIndex};
use gnitz_wire::sys_rows::FkRef;
use gnitz_wire::{PkColList, RelClass, TableDistribution, TypeCode};

use super::*;

use crate::ddl::{plan_create_table, TablePlan};

/// Plan `sql` against `cat`. The statement must parse as a `CREATE TABLE`.
fn plan_table(cat: &Catalog<'_>, sql: &str) -> Result<Option<TablePlan>, GnitzSqlError> {
    match parse_stmt(sql) {
        sqlparser::ast::Statement::CreateTable(c) => plan_create_table(&c, cat),
        other => panic!("`{sql}` is not a CREATE TABLE: {other}"),
    }
}

/// The bundle of a plan expected to create.
fn created(cat: &Catalog<'_>, sql: &str) -> TablePlan {
    plan_table(cat, sql)
        .unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
        .unwrap_or_else(|| panic!("`{sql}` planned nothing"))
}

/// An inline UNIQUE index on `cols`, named `name`.
fn unique(cols: &[u32], name: &str) -> InlineUniqueIndex {
    InlineUniqueIndex {
        cols: PkColList::from_slice(cols),
        name: name.to_string(),
    }
}

/// `p (id BIGINT PK, u BIGINT)` with a **unique** index on `u`, and `q` with the
/// same shape and a non-unique one — so only `p.u` is a legal non-PK FK target.
fn fk_catalog() -> Catalog<'static> {
    let i = TypeCode::I64;
    let cols = || vec![col("id", i), col("u", i)];
    let cat = catalog(vec![("q", rel(40, RelClass::Table, cols(), vec![0], vec![ix(&[1])]))]);
    cat.insert(
        &in_sn("p"),
        Some(rel(41, RelClass::Table, cols(), vec![0], vec![uq(&[1])])),
    );
    cat
}

// ── the PRIMARY KEY precedence table ─────────────────────────────────────────

/// The PK list, not column order, decides the key order; every PK column is an
/// integer made NOT NULL, one to four columns of any width.
#[test]
fn primary_key_admission() {
    use TypeCode::*;
    let cat = catalog(vec![]);
    for (sql, want) in [
        ("CREATE TABLE t (id INT PRIMARY KEY)", &[(0, I32)][..]),
        ("CREATE TABLE t (id SMALLINT PRIMARY KEY)", &[(0, I16)]),
        ("CREATE TABLE t (id BIGINT, name TEXT, PRIMARY KEY (ID))", &[(0, I64)]),
        (
            "CREATE TABLE t (a BIGINT UNSIGNED, b INT UNSIGNED, PRIMARY KEY (a, b))",
            &[(0, U64), (1, U32)],
        ),
        (
            "CREATE TABLE t (v BIGINT NOT NULL, a BIGINT UNSIGNED, b BIGINT UNSIGNED, PRIMARY KEY (b, a))",
            &[(2, U64), (1, U64)],
        ),
        (
            "CREATE TABLE t (a UUID, b UUID, c UUID, d UUID, v BIGINT, PRIMARY KEY (a, b, c, d))",
            &[(0, UUID), (1, UUID), (2, UUID), (3, UUID)],
        ),
    ] {
        let s = created(&cat, sql).schema;
        let got: Vec<(u32, TypeCode)> = s.pk_cols.iter().map(|&c| (c, s.columns[c as usize].ty.tc)).collect();
        assert_eq!(got, want, "{sql}");
        assert!(s.pk_cols.iter().all(|&c| !s.columns[c as usize].is_nullable), "{sql}");
    }
    for (sql, needle) in [
        ("CREATE TABLE t (id INT)", "primary key must name at least one column"),
        (
            "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, PRIMARY KEY (a, a))",
            "duplicate column",
        ),
        (
            "CREATE TABLE t (a TINYINT UNSIGNED, b TINYINT UNSIGNED, c TINYINT UNSIGNED, d TINYINT UNSIGNED, \
             e TINYINT UNSIGNED, PRIMARY KEY (a, b, c, d, e))",
            "out of range 1..=4",
        ),
        (
            "CREATE TABLE t (a TEXT, b INT UNSIGNED, PRIMARY KEY (a, b))",
            "primary key column 'a' has type_code STRING",
        ),
        (
            "CREATE TABLE t (id REAL PRIMARY KEY)",
            "primary key column 'id' has type_code F64",
        ),
        (
            "CREATE TABLE t (id DOUBLE PRIMARY KEY)",
            "primary key column 'id' has type_code F64",
        ),
    ] {
        assert_rejects(sql, plan_table(&cat, sql), needle);
    }
}

/// `CLUSTER BY` names a leading prefix of the PK list, which becomes the
/// table's distribution prefix.
#[test]
fn cluster_by_is_a_leading_pk_prefix() {
    let cat = catalog(vec![]);
    let two = "a BIGINT UNSIGNED, b BIGINT UNSIGNED, v BIGINT NOT NULL, PRIMARY KEY (a, b)";
    for (clause, prefix_len) in [("CLUSTER BY a", 1), ("CLUSTER BY a, b", 2)] {
        let c = created(&cat, &format!("CREATE TABLE t ({two}) {clause}"));
        assert_eq!(
            c.props.distribution,
            TableDistribution::Keyed { prefix_len },
            "{clause}"
        );
    }
    for (sql, needle) in [
        (format!("CREATE TABLE t ({two}) CLUSTER BY b"), "leading prefix"),
        (format!("CREATE TABLE t ({two}) CLUSTER BY b, a"), "leading prefix"),
        (
            format!("CREATE TABLE t ({two}) CLUSTER BY v"),
            "is not a PRIMARY KEY column",
        ),
        (format!("CREATE TABLE t ({two}) CLUSTER BY nope"), "not found"),
        (
            format!("CREATE TABLE t ({two}) WITH (replicated = true) CLUSTER BY a"),
            "mutually exclusive",
        ),
        (
            "CREATE TABLE t (a BIGINT UNSIGNED, b BIGINT UNSIGNED, c BIGINT UNSIGNED, PRIMARY KEY (a, b, c)) \
             CLUSTER BY a, c"
                .to_string(),
            "leading prefix",
        ),
    ] {
        assert_rejects(&sql, plan_table(&cat, &sql), needle);
    }
}

/// Every ordering the two PK spellings can be written in.
#[test]
fn primary_key_precedence() {
    let cat = catalog(vec![]);
    for (sql, needle) in [
        (
            "CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT, PRIMARY KEY (a, b))",
            "Multiple PRIMARY KEYs",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT PRIMARY KEY)",
            "Multiple PRIMARY KEYs",
        ),
        // The list resolves before the conflict is reported, so the typo is named.
        ("CREATE TABLE t (id BIGINT PRIMARY KEY, PRIMARY KEY (typo))", "typo"),
        // Two table-level clauses: the second is the conflict.
        (
            "CREATE TABLE t (a BIGINT, b BIGINT, PRIMARY KEY (a), PRIMARY KEY (b))",
            "Multiple PRIMARY KEYs",
        ),
    ] {
        assert_rejects(sql, plan_table(&cat, sql), needle);
    }

    // A self-referencing FK declared before the inline PK it targets: the whole
    // PK is known before any FK resolves.
    let c = created(
        &cat,
        "CREATE TABLE sref (refc BIGINT REFERENCES sref(id), id BIGINT PRIMARY KEY)",
    );
    assert_eq!(c.schema.pk_cols, vec![1]);
    assert_eq!(c.fks, vec![Some(FkTarget::SelfTable { col: 1 }), None]);
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
        assert_rejects(sql, plan_table(&cat, sql), "duplicate UNIQUE constraint");
    }
}

/// An inline UNIQUE builds an index, so it takes only what an index takes: a
/// key column, a user name, and an index record (the columns plus the source
/// PK) within the key arity limit.
#[test]
fn an_inline_unique_must_be_an_admissible_index() {
    let cat = catalog(vec![]);
    for (sql, needle) in [
        ("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT UNIQUE)", "'name'"),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT, CONSTRAINT _my_idx UNIQUE(a))",
            "cannot start with '_'",
        ),
        (
            "CREATE TABLE t (a BIGINT, b BIGINT, c BIGINT, d BIGINT, e BIGINT, f BIGINT, \
             UNIQUE(e, f), PRIMARY KEY (a, b, c, d))",
            "UNIQUE: column list column count 2 out of range 1..=1",
        ),
    ] {
        assert_rejects(sql, plan_table(&cat, sql), needle);
    }
}

/// Two FOREIGN KEYs on one column: the loop would resolve both and keep one.
#[test]
fn a_second_foreign_key_on_one_column_is_rejected() {
    let cat = fk_catalog();
    let sql = "CREATE TABLE c (id BIGINT PRIMARY KEY, a BIGINT REFERENCES p(id), FOREIGN KEY (a) REFERENCES q(id))";
    assert_rejects(sql, plan_table(&cat, sql), "more than one FOREIGN KEY");
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
        assert_rejects(sql, plan_table(&cat, sql), "uq");
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
    assert_eq!(c.unique_indexes, vec![unique(&[0], "uq")]);
}

#[test]
fn a_table_past_the_column_cap_is_unsupported() {
    let cat = catalog(vec![]);
    let cols: Vec<String> = (0..66).map(|i| format!("c{i} BIGINT")).collect();
    let sql = format!("CREATE TABLE t (id BIGINT PRIMARY KEY, {})", cols.join(", "));
    assert_rejects(&sql, plan_table(&cat, &sql), "column count 67 exceeds MAX_COLUMNS (65)");
}

/// `OR REPLACE` would discard the table's rows; `DROP TABLE` says that out loud.
#[test]
fn or_replace_is_rejected() {
    let cat = catalog(vec![]);
    let sql = "CREATE OR REPLACE TABLE t (id BIGINT PRIMARY KEY)";
    assert_rejects(sql, plan_table(&cat, sql), "OR REPLACE");
}

/// A stream holds no rows, so it backs no index, enforces no referential action
/// and seeds no SERIAL generator.
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
        assert_rejects(sql, plan_table(&cat, sql), needle);
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
    let known = fk_catalog();
    let u = TypeCode::U64;
    let cp = table(
        42,
        vec![col("a", u), col("b", u), ncol("payload", TypeCode::I64)],
        vec![0, 1],
    );
    known.insert(&in_sn("cp"), Some(cp));
    let st = rel(43, RelClass::Stream, vec![col("id", TypeCode::I64)], vec![0], vec![]);
    known.insert(&in_sn("st"), Some(st));
    for (sql, needle) in [
        ("CREATE TABLE t (id BIGINT PRIMARY KEY) AS SELECT id FROM p", "CTAS"),
        ("CREATE TEMPORARY TABLE t (id BIGINT PRIMARY KEY)", "TEMPORARY"),
        // Inheriting would silently drop the parent's columns.
        ("CREATE TABLE t (id BIGINT PRIMARY KEY) INHERITS (p)", "INHERITS"),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT CHECK (x > 0))",
            "CHECK",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT, CHECK (x > 0))",
            "CHECK",
        ),
        ("CREATE TABLE t (id BIGINT PRIMARY KEY, x BIGINT DEFAULT 5)", "DEFAULT"),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES p(id) ON DELETE CASCADE)",
            "ON DELETE",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT UNSIGNED REFERENCES p(id))",
            "FK type mismatch",
        ),
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES st(id))",
            "is a stream",
        ),
        // A compound PK has no lone column, so a member qualifies only through a
        // UNIQUE index of its own.
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT UNSIGNED REFERENCES cp(a))",
            "UNIQUE index",
        ),
        (
            "CREATE TABLE t (x BIGINT UNSIGNED, y BIGINT UNSIGNED, id BIGINT PRIMARY KEY, \
             FOREIGN KEY (x, y) REFERENCES cp (a, b))",
            "multi-column",
        ),
    ] {
        let (plan, _) = resolving(&known, |c| plan_table(c, sql).map(|_| ()));
        assert_rejects(sql, plan, needle);
    }
    // A target the catalog does not hold is the server's refusal, not the planner's.
    let sql = "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES phantom(id))";
    let (plan, _) = resolving(&known, |c| plan_table(c, sql).map(|_| ()));
    assert!(
        matches!(&plan, Err(GnitzSqlError::Client(e)) if e.to_string().contains("not found")),
        "{plan:?}"
    );
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
            "single-column PRIMARY KEY",
        ),
    ] {
        assert_rejects(sql, plan_table(&cat, sql), needle);
    }
}

/// SERIAL is the table's property, not a column's: the plan carries it on its
/// props and the column is a plain NOT NULL signed integer.
#[test]
fn a_serial_table_carries_serial_on_its_props() {
    let cat = catalog(vec![]);
    let c = created(&cat, "CREATE TABLE s (id BIGSERIAL PRIMARY KEY, v BIGINT)");
    assert!(c.props.serial);
    assert_eq!(c.schema.columns[0].ty.tc, TypeCode::I64);
    assert!(!c.schema.columns[0].is_nullable);
    assert!(!created(&cat, "CREATE TABLE t (id BIGINT PRIMARY KEY)").props.serial);
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
    assert_eq!(c.schema.columns[1].ty.tc, TypeCode::I64, "widened to the parent's type");
    assert_eq!(c.fks, vec![None, Some(FkTarget::Table(FkRef { table_id: 41, col: 0 }))]);
    assert_eq!(c.unique_indexes, vec![unique(&[1], &format!("{SN}__c__idx_r"))]);

    // A non-PK parent column is a legal target only with a single-column UNIQUE
    // index on it.
    created(
        &cat,
        "CREATE TABLE c2 (id BIGINT PRIMARY KEY, r BIGINT REFERENCES p(u))",
    );
    let sql = "CREATE TABLE c3 (id BIGINT PRIMARY KEY, r BIGINT REFERENCES q(u))";
    assert_rejects(sql, plan_table(&cat, sql), "UNIQUE index");
}

/// A self-referencing FK resolves against the in-flight columns: it targets the
/// table's lone PK, and its child adopts the PK's type like any other FK child.
#[test]
fn a_self_fk_targets_the_lone_pk_and_adopts_its_type() {
    let cat = catalog(vec![]);
    for (sql, col_idx) in [
        // An omitted column list defaults to the lone PK.
        (
            "CREATE TABLE tree (id BIGINT PRIMARY KEY, parent_id INT REFERENCES tree)",
            1,
        ),
        // A tautology every row satisfies.
        ("CREATE TABLE tree (id BIGINT PRIMARY KEY REFERENCES tree(id))", 0),
    ] {
        let c = created(&cat, sql);
        let mut fks = vec![None; c.schema.columns.len()];
        fks[col_idx] = Some(FkTarget::SelfTable { col: 0 });
        assert_eq!(c.fks, fks, "{sql}");
        assert_eq!(c.schema.columns[col_idx].ty.tc, TypeCode::I64, "{sql}");
    }
    for (sql, needle) in [
        (
            "CREATE TABLE tree (id BIGINT PRIMARY KEY, tag BIGINT, p BIGINT REFERENCES tree(tag))",
            "column 'tag' is not the lone PK",
        ),
        // A compound PK has no lone column: named, a member is not it; omitted,
        // there is no default.
        (
            "CREATE TABLE tree (a BIGINT, b BIGINT, p BIGINT REFERENCES tree(a), PRIMARY KEY (a, b))",
            "column 'a' is not the lone PK",
        ),
        (
            "CREATE TABLE tree (a BIGINT, b BIGINT, p BIGINT REFERENCES tree, PRIMARY KEY (a, b))",
            "must name the referenced column",
        ),
        (
            "CREATE TABLE tree (id BIGINT PRIMARY KEY, p BIGINT UNSIGNED REFERENCES tree(id))",
            "FK type mismatch",
        ),
    ] {
        assert_rejects(sql, plan_table(&cat, sql), needle);
    }
}

/// A self-reference adopts the PK column's type as the cross-table FK on that
/// column leaves it, whichever column is declared first.
#[test]
fn a_self_fk_adopts_the_type_its_pk_column_adopted() {
    let cat = fk_catalog();
    for sql in [
        "CREATE TABLE t (parent INT REFERENCES t(id), id INT PRIMARY KEY REFERENCES p(id))",
        "CREATE TABLE t (id INT PRIMARY KEY REFERENCES p(id), parent INT REFERENCES t(id))",
    ] {
        let c = created(&cat, sql);
        let types: Vec<TypeCode> = c.schema.columns.iter().map(|c| c.ty.tc).collect();
        assert_eq!(types, [TypeCode::I64, TypeCode::I64], "{sql}");
    }
}

/// Every FK column carries an index, which keys on any PK-eligible type: a
/// HUGEINT column references a HUGEINT key.
#[test]
fn an_fk_column_may_be_any_key_type() {
    let parent = rel(42, RelClass::Table, vec![col("id", TypeCode::I128)], vec![0], vec![]);
    let cat = catalog(vec![("parent", parent)]);
    let c = created(
        &cat,
        "CREATE TABLE c (id BIGINT PRIMARY KEY, p HUGEINT REFERENCES parent(id))",
    );
    assert_eq!(c.schema.columns[1].ty.tc, TypeCode::I128);
}

/// A repeated column is the definition's error, reported before a column list
/// resolves the name as ambiguous, and named as the repeat was written.
#[test]
fn a_repeated_column_name_is_rejected_before_it_is_resolved() {
    let sql = "CREATE TABLE t (a BIGINT, b BIGINT, A BIGINT, PRIMARY KEY (a))";
    assert_rejects(
        sql,
        plan_table(&catalog(vec![]), sql),
        "duplicate column name 'A' in table definition",
    );
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
            unique(&[1, 2], &format!("{SN}__t__idx_d_e_f")),
            unique(&[3, 4], &format!("{SN}__t__idx_d_e_f_2")),
        ]
    );
    // Two constraint names folding to one canonical name would write two IDX_TAB
    // rows the catalog cannot both reach.
    let sql = "CREATE TABLE u (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, \
               CONSTRAINT MyIdx UNIQUE(a), CONSTRAINT myidx UNIQUE(b))";
    assert_rejects(sql, plan_table(&cat, sql), "'myidx'");
}

/// A quoted column name is any string, and the index name built from it is an
/// identifier: each byte outside the identifier charset becomes `_`.
#[test]
fn an_auto_name_over_a_quoted_column_is_an_identifier() {
    let cat = catalog(vec![]);
    let c = created(
        &cat,
        r#"CREATE TABLE t (id BIGINT PRIMARY KEY, "a b" INT UNIQUE, "c-d" INT, e INT, UNIQUE("c-d", e))"#,
    );
    assert_eq!(
        c.unique_indexes,
        vec![
            unique(&[1], &format!("{SN}__t__idx_a_b")),
            unique(&[2, 3], &format!("{SN}__t__idx_c_d_e")),
        ]
    );
}

/// `IF NOT EXISTS` tests the name, whatever kind stands under it — and the
/// AST-only clause rejections outrank it.
#[test]
fn if_not_exists_plans_a_skip_but_a_bad_with_key_still_errors() {
    let cat = base();
    match plan_table(&cat, "CREATE TABLE IF NOT EXISTS tv (id BIGINT PRIMARY KEY)") {
        Ok(None) => {}
        other => panic!("expected nothing planned, got {:?}", other.map(|_| "a create")),
    }
    let sql = "CREATE TABLE IF NOT EXISTS tv (id BIGINT PRIMARY KEY) WITH (bogus = true)";
    assert_rejects(sql, plan_table(&cat, sql), "bogus");
}

/// The planner asks for exactly the names it needs.
#[test]
fn a_plan_resolves_only_the_fk_targets_and_the_if_not_exists_name() {
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

/// A qualifier places the table, and decides which relation a `REFERENCES`
/// means.
#[test]
fn a_schema_qualifier_names_the_schema_rather_than_being_dropped() {
    let cat = fk_catalog();
    let i = TypeCode::I64;
    let other = |name: &str| gnitz_core::RelName::new("other", name).unwrap();
    cat.insert(&other("t"), Some(table(42, vec![col("id", i)], vec![0])));

    let c = created(&cat, "CREATE TABLE Other.t2 (id BIGINT PRIMARY KEY, u BIGINT UNIQUE)");
    assert_eq!(c.name, other("t2"));
    assert_eq!(c.unique_indexes, [unique(&[1], "other__t2__idx_u")]);
    // The session schema, spelled out, is the session schema.
    let c = created(&cat, &format!("CREATE TABLE {SN}.t (id BIGINT PRIMARY KEY)"));
    assert_eq!(c.name, in_sn("t"));

    for (sql, want) in [
        // Two tables named `t`: the session's own and `other`'s.
        (
            "CREATE TABLE t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES other.t(id))",
            FkTarget::Table(FkRef { table_id: 42, col: 0 }),
        ),
        (
            "CREATE TABLE other.t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES other.t(id))",
            FkTarget::SelfTable { col: 0 },
        ),
        (
            "CREATE TABLE other.x (id BIGINT PRIMARY KEY, r BIGINT REFERENCES p(id))",
            FkTarget::Table(FkRef { table_id: 41, col: 0 }),
        ),
    ] {
        assert_eq!(created(&cat, sql).fks[1], Some(want), "`{sql}`");
    }
    for (sql, needle) in [
        // An unqualified parent is the session's, whatever schema the child is in.
        (
            "CREATE TABLE other.t (id BIGINT PRIMARY KEY, r BIGINT REFERENCES t(id))",
            "not found",
        ),
        ("CREATE TABLE db.other.t (id BIGINT PRIMARY KEY)", "too many name parts"),
        (
            "CREATE TABLE _system.t (id BIGINT PRIMARY KEY)",
            "cannot start with '_'",
        ),
    ] {
        let err = plan_table(&cat, sql).err().unwrap_or_else(|| panic!("`{sql}` planned"));
        assert!(format!("{err:?}").contains(needle), "`{sql}`: {err:?}");
    }
}
