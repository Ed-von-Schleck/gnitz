//! The order of rows under ORDER BY keys, driven through a non-engine [`crate::BatchView`].

use std::cmp::Ordering;

use super::{cmp_order_keys, order_bits, order_locators, OrderLocator, RowRanking};
use crate::test_support::{schema_pk_ints, TestView, PATTERNS};
use crate::{ColumnLocator, SchemaColumn, SchemaDescriptor};
use gnitz_wire::{OrderKey, RowSource, ScalarKind, TypeCode};

/// `order_bits` of a float payload slot is the order image of the slot's bits.
#[test]
fn order_bits_reads_a_float_slot_as_its_image() {
    const FLOATS: [f64; 3] = [-1.5, 0.0, f64::NAN];
    for (kind, tc) in [(ScalarKind::F32, TypeCode::F32), (ScalarKind::F64, TypeCode::F64)] {
        let schema = SchemaDescriptor::new(
            &[SchemaColumn::new(TypeCode::U64, false), SchemaColumn::new(tc, false)],
            &[0],
        );
        let mut v = TestView::for_schema(&schema, FLOATS.len());
        let bits = |f: f64| match kind {
            ScalarKind::F32 => (f as f32).to_bits() as u64,
            _ => f.to_bits(),
        };
        for (row, &f) in FLOATS.iter().enumerate() {
            v.set_native(&schema, row, 1, bits(f) as u128);
        }
        let loc = schema.locate(1);
        for (row, &f) in FLOATS.iter().enumerate() {
            assert_eq!(order_bits(&loc, &v, row, kind), kind.order_image(bits(f)), "{tc} {f}");
        }
    }
}

/// Under `total` the identity tiebreak follows the written keys: PK columns in
/// PK-list order (here the reverse of column order), then payload columns in slot
/// order, all ASC NULLS FIRST.
#[test]
fn order_locators_append_the_identity_tiebreak_in_pk_list_order() {
    // `PRIMARY KEY (c3, c0)`.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U32, false),
            SchemaColumn::new(TypeCode::String, true),
            SchemaColumn::new(TypeCode::F64, false),
            SchemaColumn::new(TypeCode::I64, false),
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
    let own = OrderLocator { loc: c2, desc: true, nulls_first: false };
    assert_eq!(order_locators(&[written], &schema, false).unwrap(), vec![own]);
    assert_eq!(
        order_locators(&[written], &schema, true).unwrap(),
        vec![own, asc(c3), asc(c0), asc(c1), asc(c2)],
    );
}

/// NULL placement is absolute: `nulls_first` alone decides it, whatever the
/// direction, and two NULLs tie.
#[test]
fn null_placement_ignores_the_direction() {
    let schema = schema_pk_ints(1, true);
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
                cmp_order_keys(&keys, &v, 0, 1),
                want,
                "nulls_first={nulls_first} desc={desc}"
            );
            assert_eq!(
                cmp_order_keys(&keys, &v, 1, 0),
                want.reverse(),
                "nulls_first={nulls_first} desc={desc}"
            );
            assert_eq!(cmp_order_keys(&keys, &v, 0, 2), Ordering::Equal, "two NULLs tie");
        }
    }
}

/// A PK key never reads the null word: with every bit set, it still orders by
/// value, where a NULL arm would call the rows tied.
#[test]
fn a_pk_key_never_takes_the_null_arm() {
    let schema = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false)], &[0]);
    // Keys 1 and 2.
    let mut v = TestView::for_schema(&schema, 2);
    for row in 0..2 {
        v.set_null_word(row, u64::MAX);
    }
    let loc = schema.locate(0);
    for desc in [false, true] {
        let keys = [OrderLocator { loc, desc, nulls_first: true }];
        let want = if desc { Ordering::Greater } else { Ordering::Less };
        assert_eq!(cmp_order_keys(&keys, &v, 0, 1), want, "desc={desc}");
    }
}

/// `RowRanking` against the comparator it stands in for, over `keys` on `v`: `sorted` is the
/// stable comparator sort, and under a total order `keep_smallest` keeps that sort's prefix.
fn assert_ranks_as_the_comparator(keys: &[OrderLocator], total: bool, v: &TestView, label: &str) {
    let n = v.row_count();
    let mut want: Vec<u32> = (0..n as u32).collect();
    want.sort_by(|&a, &b| cmp_order_keys(keys, v, a as usize, b as usize));
    assert_eq!(RowRanking::new(keys, v).sorted(), want, "{label}");
    if !total {
        return;
    }
    for k in [0, 1, n / 3, n - 1, n, n + 1] {
        let mut ranking = RowRanking::new(keys, v);
        ranking.keep_smallest(k);
        let mut kept: Vec<u32> = ranking.rows().collect();
        kept.sort_unstable();
        let mut prefix = want[..k.min(n)].to_vec();
        prefix.sort_unstable();
        assert_eq!(kept, prefix, "{label} k={k}");
        assert_eq!(ranking.sorted(), want[..k.min(n)], "{label} k={k}");
    }
}

/// A leading key of every type, as a payload column with NULLs on both sides of a tie and, where
/// the type allows, as a key column: in both directions and NULL placements, alone (its ties
/// left in input order) and under the identity tiebreak.
#[test]
fn a_ranking_orders_rows_as_the_comparator_does() {
    const ROWS: usize = 48;
    const STRINGS: &[&[u8]] = &[
        b"",
        b"a",
        b"a\0",
        b"abcdefgh",
        b"abcdefgh\0",
        b"abcdefghi",
        b"abcdefghijklmnop",
        b"abcdefghijklmnoq",
        b"b",
    ];
    let mut st = 0x9E3779B97F4A7C15u64;
    let mut rng = move || crate::test_support::xorshift(&mut st) as usize;
    for &tc in TypeCode::ALL {
        // `(key column 0, the leading key, a payload column)`, the leading key a payload column.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(tc, true),
                SchemaColumn::new(TypeCode::I64, false),
            ],
            &[0],
        );
        let mut v = TestView::for_schema(&schema, ROWS);
        for row in 0..ROWS {
            match tc.is_german_string() {
                true => v.set_string(row, 0, STRINGS[rng() % STRINGS.len()]),
                false => v.set_native(&schema, row, 1, PATTERNS[rng() % PATTERNS.len()]),
            }
            if rng() % 5 == 0 {
                v.set_null(row, 0);
            }
            v.set_int(row, 1, (rng() % 3) as i64);
        }
        let mut cases = vec![(schema, v, 1u16)];
        if tc.is_pk_eligible() {
            // The leading key the first of two key columns, the second telling its ties apart.
            let schema = SchemaDescriptor::new(
                &[
                    SchemaColumn::new(tc, false),
                    SchemaColumn::new(TypeCode::U16, false),
                    SchemaColumn::new(TypeCode::I64, true),
                ],
                &[0, 1],
            );
            let mut v = TestView::for_schema(&schema, ROWS);
            for row in 0..ROWS {
                v.set_native(&schema, row, 0, PATTERNS[rng() % PATTERNS.len()]);
            }
            cases.push((schema, v, 0));
        }
        for (schema, v, col) in &cases {
            for (desc, nulls_first) in [(false, false), (false, true), (true, false), (true, true)] {
                let label = format!("{tc} column {col} desc={desc} nulls_first={nulls_first}");
                let first = OrderKey { col: *col, desc, nulls_first };
                assert_ranks_as_the_comparator(&order_locators(&[first], schema, false).unwrap(), false, v, &label);
                assert_ranks_as_the_comparator(&order_locators(&[first], schema, true).unwrap(), true, v, &label);
                // A second written key, ranked by its own image inside the first's ties.
                let second = OrderKey { col: 2, desc: !desc, nulls_first };
                assert_ranks_as_the_comparator(
                    &order_locators(&[first, second], schema, false).unwrap(),
                    false,
                    v,
                    &label,
                );
            }
        }
    }
}
