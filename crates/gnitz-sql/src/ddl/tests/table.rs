use super::*;

fn table_options_of(with: &str) -> CreateTableOptions {
    match crate::test_support::parse_stmt(&format!("CREATE TABLE t (id BIGINT PRIMARY KEY) {with}")) {
        sqlparser::ast::Statement::CreateTable(c) => c.table_options,
        other => panic!("not a CREATE TABLE: {other}"),
    }
}

#[test]
fn with_options_carry_replicated_and_stream_independently() {
    let (keyed, repl) = (TableDistribution::default(), TableDistribution::Replicated);
    for (with, stream, distribution) in [
        ("", false, keyed),
        ("WITH (replicated = true)", false, repl),
        ("WITH (stream = true)", true, keyed),
        ("WITH (stream = true, replicated = false)", true, keyed),
        ("WITH (stream = true, replicated = true)", true, repl),
    ] {
        let want = TableProps { stream, distribution, serial: false };
        assert_eq!(parse_table_options(&table_options_of(with)).unwrap(), want, "{with}");
    }
    let e = parse_table_options(&table_options_of("WITH (stream = 1)")).unwrap_err();
    assert!(
        matches!(&e, GnitzSqlError::Rejected(m) if m.contains("WITH (stream = …) expects a boolean")),
        "{e:?}"
    );
}

/// A child adopts the referenced type: identity, an integer domain that fits
/// inside it, or the same DECIMAL scale. The integer ladder itself is
/// `int_domain_fits`'s own test.
#[test]
fn an_fk_child_type_must_fit_the_parent_type() {
    use TypeCode as T;
    let (t, d) = (ColType::of, ColType::decimal);
    for (child, parent, ok) in [
        (t(T::I32), t(T::I64), true),
        (t(T::U32), t(T::I64), true),
        (t(T::U64), t(T::I64), false),
        (t(T::I64), t(T::U64), false),
        // A non-integer type only by identity.
        (t(T::UUID), t(T::UUID), true),
        (t(T::String), t(T::String), true),
        (t(T::F64), t(T::F64), true),
        (t(T::UUID), t(T::U128), false),
        (t(T::U64), t(T::UUID), false),
        (t(T::F64), t(T::I64), false),
        // A DECIMAL only at its own scale, never against an integer.
        (d(2), d(2), true),
        (d(2), d(3), false),
        (d(0), t(T::I64), false),
        (t(T::I64), d(0), false),
    ] {
        let r = check_fk_type_compat(child, parent);
        assert!(
            if ok {
                r.is_ok()
            } else {
                matches!(&r, Err(GnitzSqlError::Rejected(m)) if m.contains("FK type mismatch"))
            },
            "{child} → {parent}: {r:?}"
        );
    }
}
