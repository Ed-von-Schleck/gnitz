use super::*;
use gnitz_core::ColType;

/// The `table_options` of a parsed `CREATE TABLE`, so the option tests below
/// exercise the same AST shape `execute_create_table` receives.
fn table_options_of(sql: &str) -> CreateTableOptions {
    match crate::test_support::parse_stmt(sql) {
        sqlparser::ast::Statement::CreateTable(c) => c.table_options,
        other => panic!("not a CREATE TABLE: {other}"),
    }
}

#[test]
fn with_options_carry_replicated_and_stream_independently() {
    let t = "CREATE TABLE t (id BIGINT PRIMARY KEY)";
    let props = |replicated, stream| TableProps { replicated, stream, dist_prefix_len: 0 };
    for (tail, want) in [
        ("", props(false, false)),
        (" WITH (replicated = true)", props(true, false)),
        (" WITH (stream = true)", props(false, true)),
        (" WITH (stream = true, replicated = true)", props(true, true)),
        (" WITH (STREAM = false)", props(false, false)),
    ] {
        let sql = format!("{t}{tail}");
        assert_eq!(parse_table_options(&table_options_of(&sql)).unwrap(), want, "{sql}");
    }
}

#[test]
fn unknown_with_key_and_non_boolean_value_are_rejected() {
    let t = "CREATE TABLE t (id BIGINT PRIMARY KEY)";
    let e = parse_table_options(&table_options_of(&format!("{t} WITH (streem = true)"))).unwrap_err();
    assert!(format!("{e:?}").contains("streem"), "must name the typo'd key: {e:?}");
    let e = parse_table_options(&table_options_of(&format!("{t} WITH (stream = 1)"))).unwrap_err();
    assert!(format!("{e:?}").contains("stream"), "must name the key: {e:?}");
}

/// A non-`WITH` option form carries keys nothing reads, so accepting it would
/// turn `OPTIONS(stream = true)` into an ordinary durable table with the opposite
/// `INSERT` semantics and no error anywhere.
#[test]
fn a_non_with_option_form_is_rejected_by_form() {
    let sql = "CREATE TABLE t (id BIGINT PRIMARY KEY) OPTIONS(stream = true)";
    let e = parse_table_options(&table_options_of(sql)).unwrap_err();
    assert!(format!("{e:?}").contains("OPTIONS"), "must name the form: {e:?}");
}

fn fk_ok(child: ColType, parent: ColType) -> bool {
    check_fk_type_compat(
        &ColumnDef::typed("c", child, false),
        &ColumnDef::typed("p", parent, false),
    )
    .is_ok()
}

#[test]
fn fk_widening_and_identity_accepted() {
    let tc = ColType::of;
    assert!(fk_ok(tc(TypeCode::I32), tc(TypeCode::I64))); // safe widen
    assert!(fk_ok(tc(TypeCode::U32), tc(TypeCode::I64))); // unsigned → wider signed
    assert!(fk_ok(tc(TypeCode::I32), tc(TypeCode::I32))); // identity
    assert!(fk_ok(tc(TypeCode::U64), tc(TypeCode::U64))); // identity
    assert!(fk_ok(tc(TypeCode::UUID), tc(TypeCode::UUID))); // non-integer exact match
    assert!(fk_ok(tc(TypeCode::String), tc(TypeCode::String))); // ditto
    assert!(fk_ok(tc(TypeCode::F64), tc(TypeCode::F64))); // ditto

    // A DECIMAL is its own domain: the same scale only, never an integer.
    assert!(fk_ok(ColType::decimal(2), ColType::decimal(2)));
    assert!(!fk_ok(ColType::decimal(2), ColType::decimal(3)));
    assert!(!fk_ok(ColType::decimal(0), tc(TypeCode::I64)));
    assert!(!fk_ok(tc(TypeCode::I64), ColType::decimal(0)));
}

#[test]
fn fk_narrowing_resigning_or_cross_type_rejected() {
    for (child, parent) in [
        (TypeCode::U64, TypeCode::I32),   // narrow + re-sign
        (TypeCode::U64, TypeCode::I64),   // same width, unsigned → signed
        (TypeCode::I64, TypeCode::U64),   // signed → unsigned
        (TypeCode::U128, TypeCode::I128), // 16→16, unsigned → signed
        (TypeCode::U64, TypeCode::UUID),  // integer child → non-integer parent
        (TypeCode::UUID, TypeCode::U128), // UUID is in no integer domain, U128's width notwithstanding
        (TypeCode::F64, TypeCode::I64),   // float child → integer parent
    ] {
        let (c, p) = (ColumnDef::new("c", child, false), ColumnDef::new("p", parent, false));
        assert!(
            matches!(check_fk_type_compat(&c, &p), Err(GnitzSqlError::Bind(_))),
            "{child:?} → {parent:?} must be rejected"
        );
    }
}

/// `tree (id BIGINT PK, parent_id BIGINT, tag BIGINT)` — the in-flight
/// column list a self-referencing FK resolves against.
fn tree_cols() -> Vec<ColumnDef> {
    vec![
        ColumnDef::new("id", TypeCode::I64, false),
        ColumnDef::new("parent_id", TypeCode::I64, true),
        ColumnDef::new("tag", TypeCode::I64, true),
    ]
}

fn ident(name: &str) -> sqlparser::ast::Ident {
    sqlparser::ast::Ident::new(name)
}

/// An `ObjectName` for the target of a `REFERENCES` clause. The self-FK resolver
/// never reads it — it takes the already-extracted name — but the site carries it.
fn obj(name: &str) -> sqlparser::ast::ObjectName {
    sqlparser::ast::ObjectName::from(vec![ident(name)])
}

/// The `REFERENCES` site `col_idx` declares against `tree`'s `referred`.
fn site<'a>(
    foreign_table: &'a sqlparser::ast::ObjectName,
    referred: &'a [sqlparser::ast::Ident],
    col_idx: usize,
) -> FkSite<'a> {
    FkSite {
        col_idx,
        foreign_table,
        referred_columns: referred,
    }
}

#[test]
fn self_fk_resolves_to_the_self_target_not_a_table_id() {
    let cols = tree_cols();
    // `parent_id BIGINT REFERENCES tree(id)`. The table has no id yet, so the
    // planner names the target as the table being created; `col_tab_row`
    // substitutes the owner id.
    let (fk, parent_type) =
        resolve_fk_target_inline(&cols, &[0], "tree", &site(&obj("tree"), &[ident("id")], 1)).unwrap();
    assert_eq!(fk, FkTarget::SelfTable { col: 0 });
    assert_eq!(parent_type, TypeCode::I64.into());
}

#[test]
fn self_fk_omitted_column_list_defaults_to_the_lone_pk() {
    let cols = tree_cols();
    let (fk, _) = resolve_fk_target_inline(&cols, &[0], "tree", &site(&obj("tree"), &[], 1)).unwrap();
    assert_eq!(fk, FkTarget::SelfTable { col: 0 });
}

#[test]
fn self_fk_column_referencing_itself_rejected() {
    let cols = tree_cols();
    // `id BIGINT PRIMARY KEY REFERENCES tree(id)` — a tautology, and its
    // child column would be a PK column, which the FK auto-index skips.
    let err = resolve_fk_target_inline(&cols, &[0], "tree", &site(&obj("tree"), &[ident("id")], 0)).unwrap_err();
    match err {
        GnitzSqlError::Bind(m) => assert!(m.contains("must not be the referenced column itself"), "got: {m}"),
        e => panic!("expected Bind, got {e:?}"),
    }
}

#[test]
fn self_fk_against_non_pk_column_rejected() {
    let cols = tree_cols();
    let err = resolve_fk_target_inline(&cols, &[0], "tree", &site(&obj("tree"), &[ident("tag")], 1)).unwrap_err();
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "got: {err:?}");
}

#[test]
fn self_fk_against_compound_pk_rejected() {
    let cols = tree_cols();
    // Named column: it is a PK member, but not the *lone* PK.
    let err = resolve_fk_target_inline(&cols, &[0, 1], "tree", &site(&obj("tree"), &[ident("id")], 2)).unwrap_err();
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "got: {err:?}");
    // Omitted column list: there is no single default target.
    let err = resolve_fk_target_inline(&cols, &[0, 1], "tree", &site(&obj("tree"), &[], 2)).unwrap_err();
    assert!(matches!(err, GnitzSqlError::Bind(_)), "got: {err:?}");
}

#[test]
fn duplicate_column_names_are_rejected_case_insensitively() {
    reject_duplicate_names(["a", "b"].into_iter(), "table definition").unwrap();
    let e = reject_duplicate_names(["a", "B", "A"].into_iter(), "table definition").unwrap_err();
    assert!(
        matches!(&e, GnitzSqlError::Plan(m) if m.contains("duplicate column name 'A'")),
        "{e:?}"
    );
}
