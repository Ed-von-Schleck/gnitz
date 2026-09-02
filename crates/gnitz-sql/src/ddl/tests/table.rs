use super::*;

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
    let props = |replicated, stream| TableProps {
        replicated,
        stream,
        dist_prefix_len: 0,
    };
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

#[test]
fn fk_widening_and_identity_accepted() {
    assert!(check_fk_type_compat(TypeCode::I32, TypeCode::I64).is_ok()); // safe widen
    assert!(check_fk_type_compat(TypeCode::U32, TypeCode::I64).is_ok()); // unsigned → wider signed
    assert!(check_fk_type_compat(TypeCode::I32, TypeCode::I32).is_ok()); // identity
    assert!(check_fk_type_compat(TypeCode::U64, TypeCode::U64).is_ok()); // identity
    assert!(check_fk_type_compat(TypeCode::UUID, TypeCode::UUID).is_ok()); // non-integer exact match
}

#[test]
fn fk_narrowing_resigning_or_cross_type_rejected() {
    for (child, parent) in [
        (TypeCode::U64, TypeCode::I32),   // narrow + re-sign
        (TypeCode::U64, TypeCode::I64),   // same width, unsigned → signed
        (TypeCode::I64, TypeCode::U64),   // signed → unsigned
        (TypeCode::U128, TypeCode::I128), // 16→16, unsigned → signed
        (TypeCode::U64, TypeCode::UUID),  // integer child → non-integer parent
    ] {
        assert!(
            matches!(check_fk_type_compat(child, parent), Err(GnitzSqlError::Bind(_))),
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

#[test]
fn self_fk_resolves_to_the_marker_not_a_table_id() {
    let cols = tree_cols();
    // `parent_id BIGINT REFERENCES tree(id)`. The table has no id yet, so
    // the planner marks the column; `ColumnDef::col_tab_row` substitutes the owner
    // id. `0` would collide with the engine's "no FK" encoding.
    let (tid, ref_col, parent_type) = resolve_fk_target_inline(&cols, &[0], "tree", &[ident("id")], 1).unwrap();
    assert_eq!(tid, ColumnDef::SELF_FK_TABLE_ID);
    assert_ne!(tid, 0);
    assert_eq!(ref_col, 0);
    assert_eq!(parent_type, TypeCode::I64);
}

#[test]
fn self_fk_omitted_column_list_defaults_to_the_lone_pk() {
    let cols = tree_cols();
    let (tid, ref_col, _) = resolve_fk_target_inline(&cols, &[0], "tree", &[], 1).unwrap();
    assert_eq!(tid, ColumnDef::SELF_FK_TABLE_ID);
    assert_eq!(ref_col, 0);
}

#[test]
fn self_fk_column_referencing_itself_rejected() {
    let cols = tree_cols();
    // `id BIGINT PRIMARY KEY REFERENCES tree(id)` — a tautology, and its
    // child column would be a PK column, which the FK auto-index skips.
    let err = resolve_fk_target_inline(&cols, &[0], "tree", &[ident("id")], 0).unwrap_err();
    match err {
        GnitzSqlError::Bind(m) => assert!(m.contains("must not be the referenced column itself"), "got: {m}"),
        e => panic!("expected Bind, got {e:?}"),
    }
}

#[test]
fn self_fk_against_non_pk_column_rejected() {
    let cols = tree_cols();
    let err = resolve_fk_target_inline(&cols, &[0], "tree", &[ident("tag")], 1).unwrap_err();
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "got: {err:?}");
}

#[test]
fn self_fk_against_compound_pk_rejected() {
    let cols = tree_cols();
    // Named column: it is a PK member, but not the *lone* PK.
    let err = resolve_fk_target_inline(&cols, &[0, 1], "tree", &[ident("id")], 2).unwrap_err();
    assert!(matches!(err, GnitzSqlError::Unsupported(_)), "got: {err:?}");
    // Omitted column list: there is no single default target.
    let err = resolve_fk_target_inline(&cols, &[0, 1], "tree", &[], 2).unwrap_err();
    assert!(matches!(err, GnitzSqlError::Bind(_)), "got: {err:?}");
}
