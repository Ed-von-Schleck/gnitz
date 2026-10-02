//! Range membership over key images, driven through [`TestView`].

use crate::test_support::{passing_ranges, runs, TestSchema, TestView};
use crate::{ColumnTable, ExprValidateErr, RowFilter, SchemaFacts};
use gnitz_wire::{image_mask, key_image, Cut, KeyRange, PkColList, ReadBound, TypeCode};

/// A walk pinning one column and bounding the next admits exactly the rows
/// between its cuts, every image masked to its column's width — over every key
/// type, walked through the key and through payload columns.
#[test]
fn every_walk_admits_exactly_the_rows_between_its_cuts() {
    const PATTERNS: [u128; 9] = [0, 1, 0x7F, 0x80, 0xFF, 1 << 31, 1 << 63, 1 << 127, u128::MAX];
    const N: usize = 130;
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let mask = image_mask(tc.wire_stride());
        let img = |row: usize| key_image(tc, PATTERNS[row % PATTERNS.len()]);
        let pinned = |row: usize| 7 + u128::from(row.is_multiple_of(3));
        let cands: Vec<Cut> = PATTERNS
            .iter()
            .map(|&p| key_image(tc, p))
            .chain([u128::MAX, (1 << 64) | 0x7F])
            .flat_map(|x| [Cut::before(x), Cut::after(x)])
            .collect();
        let walked_key = TestSchema::new(&[(TypeCode::U16, false), (tc, false)], &[0, 1]);
        let walked_payload = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::I64, true), (tc, true)], &[0]);
        for (schema, [pin_col, range_col]) in [(walked_key, [0, 1]), (walked_payload, [1, 2])] {
            let pin_tc = schema.col_type_code(pin_col);
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
            for &start in &cands {
                for &end in &cands {
                    let range = KeyRange::new(cols, &[over_wide_pin], start, end);
                    let mut f = RowFilter::for_read(&[], &ReadBound::Range(range), &schema).unwrap();
                    let masked = |c: Cut| Cut { image: c.image & mask, ..c };
                    let want: Vec<bool> = (0..N)
                        .map(|row| {
                            !null(row, pin_col)
                                && !null(row, range_col)
                                && pinned(row) == 7
                                && masked(start) <= Cut::before(img(row))
                                && Cut::after(img(row)) <= masked(end)
                        })
                        .collect();
                    assert_eq!(
                        passing_ranges(&mut f, &v),
                        runs(&want),
                        "{tc} {cols:?}: {start:?} .. {end:?}"
                    );
                }
            }
        }
    }
}

/// A walk exists only over in-range, key-ordered columns.
#[test]
fn a_malformed_walk_is_refused() {
    let schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::F64, true), (TypeCode::U64, true)],
        &[0],
    );
    let walk = |col| {
        let range = KeyRange::point(PkColList::from_slice(&[col]), &[], 0);
        RowFilter::for_read(&[], &ReadBound::Range(range), &schema).err()
    };
    assert!(matches!(walk(1), Some(ExprValidateErr::BadWalk(_))));
    assert!(matches!(walk(3), Some(ExprValidateErr::BadWalk(_))));
}
