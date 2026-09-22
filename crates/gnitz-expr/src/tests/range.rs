//! Range membership over key images, driven through [`TestView`].

use crate::test_support::{TestSchema, TestView};
use crate::{RangeMembership, SchemaFacts};
use gnitz_wire::TypeCode;
use gnitz_wire::{Cut, FixedInt, RangeDescriptor};

/// The rows of `v` the walk `desc` over `cols` admits.
fn admitted(cols: &[u32], desc: RangeDescriptor, schema: &TestSchema, v: &TestView) -> Vec<usize> {
    let (mut words, mut ranges) = (Vec::new(), Vec::new());
    RangeMembership::new(cols, desc, schema)
        .unwrap()
        .filter_ranges(v, &mut words, &mut ranges);
    ranges.into_iter().flat_map(|(s, e)| s..e).collect()
}

/// A signed payload column: `[-3, 3)` straddles zero, and the cut kinds decide the edges.
#[test]
fn signed_cuts_admit_across_zero() {
    let schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::I64, true)], &[0]);
    let vals = [-4i64, -3, -1, 0, 2, 3, i64::MIN, i64::MAX];
    let mut v = TestView::new(vals.len(), 8);
    v.push_col(8);
    for (r, x) in vals.iter().enumerate() {
        v.set_payload(r, 0, &x.to_le_bytes());
    }
    let p = |x: i128| FixedInt::I64.pack(x);
    let desc = RangeDescriptor::new(&[], Cut::Before(p(-3)), Cut::Before(p(3)));
    assert_eq!(admitted(&[1], desc, &schema, &v), vec![1, 2, 3, 4]);
    let desc = RangeDescriptor::new(&[], Cut::After(p(-3)), Cut::After(p(3)));
    assert_eq!(admitted(&[1], desc, &schema, &v), vec![2, 3, 4, 5]);
    let (lo, hi) = Cut::type_edges(gnitz_wire::TypeCode::I64).unwrap();
    let desc = RangeDescriptor::new(&[], lo, hi);
    assert_eq!(admitted(&[1], desc, &schema, &v), (0..vals.len()).collect::<Vec<_>>());
    // An inverted interval, and one past the type's top edge, admit nothing.
    let desc = RangeDescriptor::new(&[], Cut::After(p(3)), Cut::Before(p(-3)));
    assert!(admitted(&[1], desc, &schema, &v).is_empty());
    let desc = RangeDescriptor::new(&[], Cut::After(p(i64::MAX as i128)), hi);
    assert!(admitted(&[1], desc, &schema, &v).is_empty());
}

/// A compound PK in PK-list order `[1, 0]`: the equality pins column 1, the range bounds
/// column 0.
#[test]
fn a_compound_prefix_pins_then_bounds() {
    let schema = TestSchema::new(&[(TypeCode::U32, false), (TypeCode::I16, false)], &[1, 0]);
    let rows: [(i16, u32); 5] = [(-1, 5), (-1, 9), (2, 5), (-1, 10), (-1, 4)];
    let mut v = TestView::new(rows.len(), schema.pk_stride());
    for (r, &(a, b)) in rows.iter().enumerate() {
        v.set_pk_col(r, 0, &a.to_le_bytes(), TypeCode::I16);
        v.set_pk_col(r, 2, &b.to_le_bytes(), TypeCode::U32);
    }
    let desc = RangeDescriptor::new(&[FixedInt::I16.pack(-1)], Cut::Before(5), Cut::After(9));
    assert_eq!(admitted(&[1, 0], desc, &schema, &v), vec![0, 1]);
}

/// An index holds no row with a NULL in any of its columns, the trailing ones included.
#[test]
fn a_null_in_any_indexed_column_is_outside_the_walk() {
    let schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::U64, true), (TypeCode::U64, true)],
        &[0],
    );
    let mut v = TestView::new(3, 8);
    v.push_col(8);
    v.push_col(8);
    for r in 0..3 {
        v.set_payload(r, 0, &7u64.to_le_bytes());
    }
    v.set_null(1, 0);
    v.set_null(2, 1);
    assert_eq!(admitted(&[1, 2], RangeDescriptor::point(&[], 7), &schema, &v), vec![0]);
}

/// A descriptor value wider than its column is masked to the column's width, as the store's
/// key encoder masks it.
#[test]
fn an_over_wide_value_is_masked_to_the_column() {
    let schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::U8, true)], &[0]);
    let mut v = TestView::new(2, 8);
    v.push_col(1);
    v.set_payload(0, 0, &[7]);
    v.set_payload(1, 0, &[8]);
    assert_eq!(
        admitted(&[1], RangeDescriptor::point(&[], 0x1_07), &schema, &v),
        vec![0]
    );
}

/// A U128 column orders unsigned over its whole width, across more than one filter word.
#[test]
fn a_u128_column_orders_over_its_full_width() {
    let schema = TestSchema::new(&[(TypeCode::U128, false)], &[0]);
    let vals: Vec<u128> = (0..130)
        .map(|i| if i % 2 == 0 { 5 } else { (i as u128) << 100 })
        .collect();
    let mut v = TestView::new(vals.len(), 16);
    for (r, x) in vals.iter().enumerate() {
        v.set_pk_col(r, 0, &x.to_le_bytes(), TypeCode::U128);
    }
    let desc = RangeDescriptor::new(&[], Cut::After(5), Cut::After(u128::MAX));
    assert_eq!(
        admitted(&[0], desc, &schema, &v),
        (1..130).step_by(2).collect::<Vec<_>>()
    );
}

/// A walk exists only over key-ordered columns and with a range column left.
#[test]
fn a_malformed_walk_is_refused() {
    let schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::F64, true), (TypeCode::U64, true)],
        &[0],
    );
    assert!(RangeMembership::new(&[1], RangeDescriptor::point(&[], 0), &schema).is_err());
    assert!(RangeMembership::new(&[2], RangeDescriptor::new(&[3], Cut::Before(0), Cut::After(9)), &schema).is_err());
}
