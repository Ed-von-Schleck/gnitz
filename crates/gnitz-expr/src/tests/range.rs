//! Range membership over key images, driven through [`TestView`].

use crate::test_support::{TestSchema, TestView};
use crate::{RangeMembership, RowSource, SchemaFacts};
use gnitz_wire::TypeCode;
use gnitz_wire::{image_mask, key_image, Cut, FixedInt, KeyRange, PkColList};

fn range(cols: &[u32], eq: &[u128], start: Cut, end: Cut) -> KeyRange {
    KeyRange::new(PkColList::from_slice(cols), eq, start, end)
}

fn point(cols: &[u32], v: u128) -> KeyRange {
    KeyRange::point(PkColList::from_slice(cols), &[], v)
}

/// The rows of `v` the walk `range` admits, ANDed into all-ones words.
fn admitted(range: KeyRange, schema: &TestSchema, v: &TestView) -> Vec<usize> {
    let n = v.row_count();
    let mut words = vec![u64::MAX; n.div_ceil(64)];
    if !n.is_multiple_of(64) {
        words[n / 64] = gnitz_wire::low_bits_mask(n % 64);
    }
    RangeMembership::new(&range, schema).unwrap().and_into(v, &mut words);
    (0..n).filter(|&r| (words[r / 64] >> (r % 64)) & 1 != 0).collect()
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
    let p = |x: i128| key_image(TypeCode::I64, FixedInt::I64.pack(x));
    let r = range(&[1], &[], Cut::before(p(-3)), Cut::before(p(3)));
    assert_eq!(admitted(r, &schema, &v), vec![1, 2, 3, 4]);
    let r = range(&[1], &[], Cut::after(p(-3)), Cut::after(p(3)));
    assert_eq!(admitted(r, &schema, &v), vec![2, 3, 4, 5]);
    let (lo, hi) = (Cut::before(0), Cut::after(image_mask(8)));
    assert_eq!(
        admitted(range(&[1], &[], lo, hi), &schema, &v),
        (0..vals.len()).collect::<Vec<_>>()
    );
    // An inverted interval, and one past the type's top edge, admit nothing.
    let r = range(&[1], &[], Cut::after(p(3)), Cut::before(p(-3)));
    assert!(admitted(r, &schema, &v).is_empty());
    let r = range(&[1], &[], Cut::after(p(i64::MAX as i128)), hi);
    assert!(admitted(r, &schema, &v).is_empty());
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
    let eq = key_image(TypeCode::I16, FixedInt::I16.pack(-1));
    let r = range(&[1, 0], &[eq], Cut::before(5), Cut::after(9));
    assert_eq!(admitted(r, &schema, &v), vec![0, 1]);
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
    assert_eq!(admitted(point(&[1, 2], 7), &schema, &v), vec![0]);
}

/// An image wider than its column is masked to the column's width, as the store's key
/// encoder masks it.
#[test]
fn an_over_wide_value_is_masked_to_the_column() {
    let schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::U8, true)], &[0]);
    let mut v = TestView::new(2, 8);
    v.push_col(1);
    v.set_payload(0, 0, &[7]);
    v.set_payload(1, 0, &[8]);
    assert_eq!(admitted(point(&[1], 0x1_07), &schema, &v), vec![0]);
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
    let r = range(&[0], &[], Cut::after(5), Cut::after(u128::MAX));
    assert_eq!(admitted(r, &schema, &v), (1..130).step_by(2).collect::<Vec<_>>());
}

/// An I128 column orders signed across the sign bias: its images are its values plus
/// `2^127`, so a negative bound admits the non-negative values above it.
#[test]
fn an_i128_column_orders_signed_across_the_sign_bias() {
    let schema = TestSchema::new(&[(TypeCode::I128, false)], &[0]);
    let vals: [i128; 6] = [i128::MIN, -7, -1, 0, 5, i128::MAX];
    let mut v = TestView::new(vals.len(), 16);
    for (r, x) in vals.iter().enumerate() {
        v.set_pk_col(r, 0, &x.to_le_bytes(), TypeCode::I128);
    }
    let img = |x: i128| key_image(TypeCode::I128, x as u128);
    let r = range(&[0], &[], Cut::after(img(-7)), Cut::after(img(5)));
    assert_eq!(admitted(r, &schema, &v), vec![2, 3, 4]);
    let r = range(&[0], &[], Cut::before(img(-1)), Cut::after(u128::MAX));
    assert_eq!(admitted(r, &schema, &v), vec![2, 3, 4, 5]);
}

/// A walk exists only over in-range, key-ordered columns.
#[test]
fn a_malformed_walk_is_refused() {
    let schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::F64, true), (TypeCode::U64, true)],
        &[0],
    );
    assert!(RangeMembership::new(&point(&[1], 0), &schema).is_err());
    assert!(matches!(
        RangeMembership::new(&point(&[3], 0), &schema),
        Err(crate::ExprValidateErr::BadWalk(_))
    ));
}
