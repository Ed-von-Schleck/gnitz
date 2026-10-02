//! Resolved-addressing reads, driven through a non-engine [`crate::BatchView`].

use std::cmp::Ordering;

use crate::test_support::{TestSchema, TestView};
use crate::{cmp_order_keys, order_locators, ColumnLocator, OrderLocator, SchemaFacts};
use gnitz_wire::{FixedInt, OrderKey, TypeCode};

/// Every locator read of every key type, from a key column and from a payload
/// slot holding the same values, against the value's own encoding.
#[test]
fn every_locator_read_agrees_with_the_values_encoding() {
    const PATTERNS: &[u128] = &[
        0,
        1,
        2,
        0x7F,
        0x80,
        0xFF,
        0x100,
        i64::MAX as u128,
        1 << 63,
        u64::MAX as u128,
        1 << 64,
        i128::MAX as u128,
        1 << 127,
        u128::MAX,
    ];
    let opk = |native: u128, tc: TypeCode| {
        let mut out = vec![0u8; tc.wire_stride()];
        gnitz_wire::store_opk(&mut out, native, tc.is_signed_int());
        out
    };
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let w = tc.wire_stride();
        let schema = TestSchema::new(
            &[
                (TypeCode::U16, false), // key column 0
                (tc, false),            // key column 1, at byte 2
                (TypeCode::I64, true),  // payload slot 0
                (tc, true),             // payload slot 1
            ],
            &[0, 1],
        );
        let natives: Vec<Vec<u8>> = PATTERNS.iter().map(|p| p.to_le_bytes()[..w].to_vec()).collect();
        let mut v = TestView::for_schema(&schema, PATTERNS.len());
        for (row, &p) in PATTERNS.iter().enumerate() {
            v.set_native(&schema, row, 1, p);
            v.set_native(&schema, row, 3, p);
            v.set_null_word(row, u64::from(row % 2 == 0) | u64::from(row % 3 == 0) << 1);
        }
        let (from_pk, from_payload) = (schema.locate(1), schema.locate(3));
        assert!(matches!(from_pk, ColumnLocator::Pk { byte_off: 2, .. }), "{from_pk:?}");
        for loc in [from_pk, from_payload] {
            let arm = if loc == from_pk { "pk" } else { "payload" };
            for (row, native) in natives.iter().enumerate() {
                let label = format!("{tc} {arm} row {row}");
                assert_eq!(
                    loc.opk_image(&v, row),
                    gnitz_wire::widen_pk_be(&opk(PATTERNS[row], tc)),
                    "{label}"
                );
                assert_eq!(loc.native_le_bytes(&v, row, &mut [0u8; 16]), &native[..], "{label}");
                assert_eq!(loc.is_null(&v, row), loc == from_payload && row % 3 == 0, "{label}");
                let Some(fi) = FixedInt::from_type_code(tc) else {
                    continue;
                };
                assert_eq!(loc.decode_i64(&v, row, fi), fi.decode_le_i64(native), "{label}");
            }
            for i in 0..natives.len() {
                for j in 0..natives.len() {
                    assert_eq!(
                        loc.cmp_non_null(&v, i, &v, j),
                        loc.opk_image(&v, i).cmp(&loc.opk_image(&v, j)),
                        "{tc} {arm} rows {i}, {j}"
                    );
                }
            }
        }
    }
    // The at-rest image itself: big-endian with the sign bit flipped, so `-1`
    // lands just below `i64::MAX` and `i64::MIN` at all-zero.
    assert_eq!(
        opk(-1i64 as u128, TypeCode::I64),
        [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    );
    assert_eq!(opk(i64::MIN as u128, TypeCode::I64), [0; 8]);
}

/// The identity tiebreak follows the written keys: PK columns in PK-list order
/// (here the reverse of column order), then payload columns in slot order, all
/// ASC NULLS FIRST. No written key, no tiebreak.
#[test]
fn order_locators_append_the_identity_tiebreak_in_pk_list_order() {
    // `PRIMARY KEY (c3, c0)`.
    let schema = TestSchema::new(
        &[
            (TypeCode::U32, false),
            (TypeCode::String, true),
            (TypeCode::F64, false),
            (TypeCode::I64, false),
        ],
        &[3, 0],
    );
    let written = OrderKey { col: 2, desc: true, nulls_first: false };
    let c0 = ColumnLocator::Pk {
        byte_off: 8,
        size: 4,
        type_code: TypeCode::U32,
    };
    let c1 = ColumnLocator::Payload {
        slot: 0,
        size: 16,
        type_code: TypeCode::String,
    };
    let c2 = ColumnLocator::Payload {
        slot: 1,
        size: 8,
        type_code: TypeCode::F64,
    };
    let c3 = ColumnLocator::Pk {
        byte_off: 0,
        size: 8,
        type_code: TypeCode::I64,
    };
    let asc = |loc| OrderLocator { loc, desc: false, nulls_first: true };
    assert_eq!(
        order_locators(&[written], &schema),
        vec![
            OrderLocator { loc: c2, desc: true, nulls_first: false },
            asc(c3),
            asc(c0),
            asc(c1),
            asc(c2)
        ],
    );
    assert!(order_locators(&[], &schema).is_empty());
}

/// NULL placement is absolute: `nulls_first` alone decides it, whatever the
/// direction, and two NULLs tie.
#[test]
fn null_placement_ignores_the_direction() {
    let schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::I64, true)], &[0]);
    let mut v = TestView::for_schema(&schema, 3);
    v.set_int(1, 0, 5);
    v.set_null(0, 0);
    v.set_null(2, 0);
    let loc = schema.locate(1);
    for nulls_first in [false, true] {
        let want = if nulls_first { Ordering::Less } else { Ordering::Greater };
        for desc in [false, true] {
            let keys = [OrderLocator { loc, desc, nulls_first }];
            assert_eq!(
                cmp_order_keys(&keys, &v, 0, &v, 1),
                want,
                "nulls_first={nulls_first} desc={desc}"
            );
            assert_eq!(
                cmp_order_keys(&keys, &v, 1, &v, 0),
                want.reverse(),
                "nulls_first={nulls_first} desc={desc}"
            );
            assert_eq!(cmp_order_keys(&keys, &v, 0, &v, 2), Ordering::Equal, "two NULLs tie");
        }
    }
}

/// A PK key never reads the null word: with every bit set, it still orders by
/// value, where a NULL arm would call the rows tied.
#[test]
fn a_pk_key_never_takes_the_null_arm() {
    let schema = TestSchema::new(&[(TypeCode::U64, false)], &[0]);
    // Keys 1 and 2.
    let mut v = TestView::for_schema(&schema, 2);
    for row in 0..2 {
        v.set_null_word(row, u64::MAX);
    }
    let loc = schema.locate(0);
    for desc in [false, true] {
        let keys = [OrderLocator { loc, desc, nulls_first: true }];
        let want = if desc { Ordering::Greater } else { Ordering::Less };
        assert_eq!(cmp_order_keys(&keys, &v, 0, &v, 1), want, "desc={desc}");
    }
}
