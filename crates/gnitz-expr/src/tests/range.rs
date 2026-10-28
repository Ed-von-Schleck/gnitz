//! Range membership over key images, driven through [`TestView`].

use crate::test_support::{filter_verdicts, schema_pk, TestView, PATTERNS};
use crate::SchemaColumn;
use crate::SchemaDescriptor;
use crate::{ExprValidateErr, RowFilter};
use gnitz_wire::{image_mask, key_image, KeyRange, PkColList, TypeCode};

/// A walk pinning one column and bounding the next admits exactly the rows
/// within its bounds, every image masked to its column's width — over every key
/// type, walked through the key and through payload columns.
#[test]
fn every_walk_admits_exactly_the_rows_within_its_bounds() {
    const N: usize = 130;
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let mask = image_mask(tc.wire_stride());
        let img = |row: usize| key_image(tc, PATTERNS[row % PATTERNS.len()]);
        let pinned = |row: usize| 7 + u128::from(row.is_multiple_of(3));
        let cands: Vec<u128> = PATTERNS
            .iter()
            .map(|&p| key_image(tc, p))
            .chain([u128::MAX, (1 << 64) | 0x7F])
            .flat_map(|x| [x, x.wrapping_add(1)])
            .collect();
        let walked_key = SchemaDescriptor::new(
            &[SchemaColumn::new(TypeCode::U16, false), SchemaColumn::new(tc, false)],
            &[0, 1],
        );
        let walked_payload = schema_pk(&[TypeCode::I64, tc], true);
        for (schema, [pin_col, range_col]) in [(walked_key, [0, 1]), (walked_payload, [1, 2])] {
            let pin_tc = schema.columns()[pin_col].type_code;
            let null = |row: usize, ci: usize| schema.payload_slot(ci).is_some() && row.is_multiple_of(10 + ci);
            let mut v = TestView::for_schema(&schema, N);
            for row in 0..N {
                v.set_native(&schema, row, pin_col, pinned(row));
                v.set_native(&schema, row, range_col, img(row) ^ gnitz_wire::opk_bias(tc));
                for ci in [pin_col, range_col].into_iter().filter(|&ci| null(row, ci)) {
                    v.set_null(row, schema.payload_slot(ci).unwrap());
                }
            }
            let over_wide_pin = key_image(pin_tc, 7) | 1 << 100;
            let cols = PkColList::from_slice(&[pin_col as u32, range_col as u32]);
            for &first in &cands {
                for &last in &cands {
                    let range = KeyRange::new(cols, &[over_wide_pin], first, last);
                    let mut f = RowFilter::for_read(&[], Some(&range), &schema).unwrap();
                    let want: Vec<bool> = (0..N)
                        .map(|row| {
                            !null(row, pin_col)
                                && !null(row, range_col)
                                && pinned(row) == 7
                                && first & mask <= img(row)
                                && img(row) <= last & mask
                        })
                        .collect();
                    assert_eq!(
                        filter_verdicts(&mut f, &v),
                        want,
                        "{tc} {cols:?}: {first:#x} ..= {last:#x}"
                    );
                }
            }
        }
    }
}

/// A walk exists only over in-range, key-ordered columns.
#[test]
fn a_malformed_walk_is_refused() {
    let schema = schema_pk(&[TypeCode::F64, TypeCode::U64], true);
    let walk = |col| {
        let range = KeyRange::point(PkColList::from_slice(&[col]), &[], 0);
        RowFilter::for_read(&[], Some(&range), &schema).err()
    };
    assert!(matches!(walk(1), Some(ExprValidateErr::BadWalk(_))));
    assert!(matches!(walk(3), Some(ExprValidateErr::BadWalk(_))));
}
