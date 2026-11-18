use super::*;
use crate::test_support::assert_rejects;
use gnitz_wire::TypeCode;

/// A child adopts the referenced type: identity, or an integer domain that fits
/// inside it. The integer ladder itself is `int_domain_fits`'s own test.
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
        // A calendar type only by identity: adopting another would restate its
        // values in another unit.
        (t(T::Date), t(T::Date), true),
        (t(T::Date), t(T::Timestamp), false),
        (t(T::Date), t(T::I64), false),
        (t(T::I32), t(T::Date), false),
        // A DECIMAL only at its own scale, never against an integer.
        (d(2), d(2), true),
        (d(2), d(3), false),
        (d(0), t(T::I64), false),
        (t(T::I64), d(0), false),
    ] {
        let r = check_fk_type_compat(child, parent);
        match ok {
            true => assert!(r.is_ok(), "{child} → {parent}: {r:?}"),
            false => assert_rejects(&format!("{child} → {parent}"), r, "FK type mismatch"),
        }
    }
}

#[test]
fn a_dist_prefix_is_a_leading_pk_prefix_in_pk_order() {
    let cols: Vec<ColumnDef> = ["a", "b", "c", "d"]
        .iter()
        .map(|&n| ColumnDef::new(n, TypeCode::U64, false))
        .collect();
    let check = |pk: &[u32], cluster: &[u32]| dist_prefix_len(&cols, pk, cluster);
    assert_eq!(check(&[0, 1], &[0]).unwrap(), 1);
    assert_eq!(check(&[0, 1], &[0, 1]).unwrap(), 2);
    // A single-column PK: only the whole PK is a valid prefix.
    assert_eq!(check(&[3], &[3]).unwrap(), 1);
    // A reordered PK whose distribution column leads.
    assert_eq!(check(&[2, 1], &[2]).unwrap(), 1);

    assert!(check(&[0, 1], &[1]).is_err(), "non-leading PK column");
    assert!(check(&[0, 1, 2], &[0, 2]).is_err(), "skips col 1");
    assert!(check(&[0, 1], &[1, 0]).is_err(), "wrong order");
    assert!(check(&[0, 1], &[]).is_err(), "empty");
    assert!(check(&[0, 1], &[0, 1, 2]).is_err(), "longer than PK");
    assert_rejects(
        "cluster by d",
        check(&[0, 1], &[3]),
        "column 'd' is not a PRIMARY KEY column",
    );
}
