use super::*;
use gnitz_wire::TypeCode;

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

/// Every name disambiguation hands out is recognized as an auto-name of its
/// base, and no other base's or a written name is.
#[test]
fn an_auto_name_is_recognized_from_its_base() {
    let base = default_index_name(&RelName::new("s", "t").unwrap(), &["a", "B c"]);
    assert_eq!(base, "s__t__idx_a_b_c");
    let mut taken = HashSet::new();
    for want in ["s__t__idx_a_b_c", "s__t__idx_a_b_c_2", "s__t__idx_a_b_c_3"] {
        let name = disambiguate_index_name(base.clone(), &taken);
        assert_eq!(name, want);
        assert!(is_auto_name(&name, &base));
        taken.insert(name);
    }
    for other in ["s__t__idx_a_b_c_", "s__t__idx_a_b_c_d", "s__t__idx_a_b", "my_idx"] {
        assert!(!is_auto_name(other, &base), "{other}");
    }
}

#[test]
fn a_dist_prefix_is_a_leading_pk_prefix_in_pk_order() {
    let cols: Vec<ColumnDef> = ["a", "b", "c", "d"]
        .iter()
        .map(|&n| ColumnDef::new(n, TypeCode::U64, false))
        .collect();
    let check = |pk: &[u32], cluster: &[u32]| validate_dist_prefix(&cols, pk, cluster);
    assert!(check(&[0, 1], &[0]).is_ok());
    assert!(check(&[0, 1], &[0, 1]).is_ok());
    // A single-column PK: only the whole PK is a valid prefix.
    assert!(check(&[3], &[3]).is_ok());
    // A reordered PK whose distribution column leads.
    assert!(check(&[2, 1], &[2]).is_ok());

    assert!(check(&[0, 1], &[1]).is_err(), "non-leading PK column");
    assert!(check(&[0, 1, 2], &[0, 2]).is_err(), "skips col 1");
    assert!(check(&[0, 1], &[1, 0]).is_err(), "wrong order");
    assert!(check(&[0, 1], &[]).is_err(), "empty");
    assert!(check(&[0, 1], &[0, 1, 2]).is_err(), "longer than PK");
    let e = check(&[0, 1], &[3]).unwrap_err();
    assert!(
        matches!(&e, GnitzSqlError::Rejected(m) if m.contains("column 'd' is not a PRIMARY KEY column")),
        "{e:?}"
    );
}
