//! Range membership over key images, driven through [`TestView`].

use crate::test_support::{make_n_col_view, passing_rows, TestSchema, TestView};
use crate::{ExprValidateErr, RowFilter, SchemaFacts};
use gnitz_wire::{image_mask, key_image, Cut, FixedInt, KeyRange, PkColList, ReadBound, TypeCode};

fn range(cols: &[u32], eq: &[u128], start: Cut, end: Cut) -> KeyRange {
    KeyRange::new(PkColList::from_slice(cols), eq, start, end)
}

fn point(cols: &[u32], v: u128) -> KeyRange {
    KeyRange::point(PkColList::from_slice(cols), &[], v)
}

/// The rows of `v` the walk `range` admits, through the read filter that runs it.
fn admitted(range: KeyRange, schema: &TestSchema, v: &TestView) -> Vec<usize> {
    let mut f = RowFilter::for_read(&[], &ReadBound::Range(range), schema).unwrap();
    passing_rows(&mut f, v)
        .iter()
        .enumerate()
        .filter(|(_, &p)| p)
        .map(|(r, _)| r)
        .collect()
}

/// A signed payload column: `[-3, 3)` straddles zero, and the cut kinds decide the edges.
#[test]
fn signed_cuts_admit_across_zero() {
    let schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::I64, true)], &[0]);
    let vals = [-4i64, -3, -1, 0, 2, 3, i64::MIN, i64::MAX];
    let v = make_n_col_view(&schema, vals.len(), |r, _| vals[r], |_, _| false);
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
    let v = make_n_col_view(&schema, 3, |_, _| 7, |r, c| r == c + 1);
    assert_eq!(admitted(point(&[1, 2], 7), &schema, &v), vec![0]);
}

/// An image wider than its column is masked to the column's width, as the store's key
/// encoder masks it.
#[test]
fn an_over_wide_value_is_masked_to_the_column() {
    let schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::U8, true)], &[0]);
    let v = make_n_col_view(&schema, 2, |r, _| 7 + r as i64, |_, _| false);
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
    let walk = |r| RowFilter::for_read(&[], &ReadBound::Range(r), &schema).err();
    assert!(matches!(walk(point(&[1], 0)), Some(ExprValidateErr::BadWalk(_))));
    assert!(matches!(walk(point(&[3], 0)), Some(ExprValidateErr::BadWalk(_))));
}
