//! Range membership over key images, driven through [`TestView`].

use crate::test_support::{passing_ranges, TestSchema, TestView};
use crate::{ExprValidateErr, RowFilter};
use gnitz_wire::{image_mask, key_image, Cut, KeyRange, PkColList, ReadBound, TypeCode};

/// A walk `[start, end)` pinned by one equality column, against its definition:
/// a row is admitted iff no walked payload column is NULL, the pinned column's
/// image equals the pinned one, and the bounded column's image `x` sits between
/// the cuts — `start <= before(x)` and `after(x) <= end`. Every image, the cuts'
/// and the pin's included, is first masked to its column's width, so an
/// over-wide one names the value it masks to.
///
/// Over every key type, bounded both as a PK column after a pinned PK column —
/// at a non-zero offset of the key — and as a payload column after a pinned
/// payload column; each batch crosses a filter word. The cuts sit either side of
/// every width's sign boundary and extremes, so every signed bias, every
/// wrapping span and every empty or inverted interval is reached.
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

        // (arm, schema, walked columns, the pin's type, which walked cell is NULL)
        type Arm = (&'static str, TestSchema, [u32; 2], TypeCode, fn(usize, usize) -> bool);
        let arms: [Arm; 2] = [
            (
                "pk",
                TestSchema::new(&[(TypeCode::U16, false), (tc, false), (TypeCode::I64, true)], &[0, 1]),
                [0, 1],
                TypeCode::U16,
                |_, _| false,
            ),
            (
                "payload",
                TestSchema::new(&[(TypeCode::U64, false), (TypeCode::I64, true), (tc, true)], &[0]),
                [1, 2],
                TypeCode::I64,
                |row, pi| row % [13, 11][pi] == 0,
            ),
        ];
        for (arm, schema, cols, pin_tc, null) in arms {
            let mut v = TestView::for_schema(&schema, N);
            for row in 0..N {
                let native = |image: u128, tc: TypeCode| image ^ gnitz_wire::opk_bias(tc, tc.wire_stride());
                match arm {
                    "pk" => v.set_key(&schema, row, &[pinned(row), native(img(row), tc)]),
                    _ => {
                        v.set_int(row, 0, pinned(row) as i64);
                        v.set_payload(row, 1, &native(img(row), tc).to_le_bytes()[..tc.wire_stride()]);
                        v.set_null_word(row, u64::from(null(row, 0)) | u64::from(null(row, 1)) << 1);
                    }
                }
            }
            // The pin, over-wide past its own column.
            let pin = key_image(pin_tc, 7) | 1 << 100;
            for &start in &cands {
                for &end in &cands {
                    let range = KeyRange::new(PkColList::from_slice(&cols), &[pin], start, end);
                    let mut f = RowFilter::for_read(&[], &ReadBound::Range(range), &schema).unwrap();
                    let mut got = vec![false; N];
                    for (s, e) in passing_ranges(&mut f, &v) {
                        got[s..e].fill(true);
                    }
                    let masked = |c: Cut| Cut { image: c.image & mask, ..c };
                    let want: Vec<bool> = (0..N)
                        .map(|row| {
                            let x = img(row);
                            !null(row, 0)
                                && !null(row, 1)
                                && pinned(row) == 7
                                && masked(start) <= Cut::before(x)
                                && Cut::after(x) <= masked(end)
                        })
                        .collect();
                    assert_eq!(got, want, "{tc} {arm}: {start:?} .. {end:?}");
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
