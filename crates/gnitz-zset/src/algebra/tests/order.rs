//! The order of rows under ORDER BY keys, over batches built row by row.

use std::cmp::Ordering;

use super::super::order_image::order_bits;
use super::{ImageCol, OrderCol, RowRanking, cmp_order_keys, order_cols};
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{make_int_batch, pk_only_schema, u64_pk_schema};
use gnitz_expr::ColumnLocator;
use gnitz_wire::{FixedInt, Heap, OrderKey, RowSource, ScalarKind, TypeCode, image_mask};

/// The key over `loc`, never a 16-byte integer's low half.
fn key(loc: ColumnLocator, desc: bool, nulls_first: bool, nullable: bool) -> OrderCol {
    OrderCol {
        image: ImageCol::new(loc, desc),
        nulls_first,
        nullable,
        low_half: false,
    }
}

/// Key values that tie, that differ only in the low half of 16 bytes, that cross
/// each width's sign bit, and one whose every byte differs.
const PATTERNS: &[u128] = &[
    0,
    1,
    0x7F,
    0x80,
    0xFF,
    0x7FFF,
    0x8000,
    1 << 31,
    i64::MAX as u128,
    1 << 63,
    u64::MAX as u128,
    0x0123_4567_89AB_CDEF,
    1 << 64,
    (1 << 64) + 1,
    i128::MAX as u128,
    1 << 127,
    u128::MAX,
];

/// `v` cut to the low bytes a `tc` cell holds.
fn native(tc: TypeCode, v: u128) -> u128 {
    v & image_mask(tc.wire_stride())
}

/// `order_bits` of a float payload slot is the order image of the slot's bits.
#[test]
fn order_bits_reads_a_float_slot_as_its_image() {
    const FLOATS: [f64; 3] = [-1.5, 0.0, f64::NAN];
    for (kind, tc) in [(ScalarKind::F32, TypeCode::F32), (ScalarKind::F64, TypeCode::F64)] {
        let schema = SchemaDescriptor::new(
            &[SchemaColumn::new(TypeCode::U64, false), SchemaColumn::new(tc, false)],
            &[0],
        );
        let bits = |f: f64| match kind {
            ScalarKind::F32 => (f as f32).to_bits() as u64,
            _ => f.to_bits(),
        };
        let mut b = BatchBuilder::new(&schema);
        for (row, &f) in FLOATS.iter().enumerate() {
            b.begin_row(row as u128 + 1, 1).put_float(f);
        }
        let b = b.finish();
        let loc = schema.locate(1);
        for (row, &f) in FLOATS.iter().enumerate() {
            assert_eq!(order_bits(&loc, &b, row, kind), kind.order_image(bits(f)), "{tc} {f}");
        }
    }
}

/// `order_bits` of an integer column, from a key column and from a payload slot
/// holding the same values, is the order image of the value it decodes to.
#[test]
fn order_bits_reads_an_integer_as_its_image() {
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let Some(fi) = FixedInt::from_type_code(tc) else {
            continue;
        };
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U16, false), // key column 0
                SchemaColumn::new(tc, false),            // key column 1, at byte 2
                SchemaColumn::new(tc, true),             // payload slot 0
            ],
            &[0, 1],
        );
        let mut b = BatchBuilder::new(&schema);
        for (row, &p) in PATTERNS.iter().enumerate() {
            let v = native(tc, p);
            b.begin_row_natives(&[row as u128 + 1, v], 1).put_int(v);
        }
        let b = b.finish();
        let kind = ScalarKind::Int(fi);
        for loc in [schema.locate(1), schema.locate(2)] {
            for (row, &p) in PATTERNS.iter().enumerate() {
                let le = native(tc, p).to_le_bytes();
                assert_eq!(
                    order_bits(&loc, &b, row, kind),
                    kind.order_image(fi.decode_le_i64(&le[..tc.wire_stride()]) as u64),
                    "{tc} {loc:?} row {row}"
                );
            }
        }
    }
}

/// Under `total` the identity tiebreak follows the written keys: PK columns in
/// PK-list order (here the reverse of column order), then payload columns in slot
/// order, all ASC NULLS FIRST.
#[test]
fn order_cols_append_the_identity_tiebreak_in_pk_list_order() {
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
    let asc = |loc, nullable| key(loc, false, true, nullable);
    let own = key(c2, true, false, false);
    assert_eq!(order_cols(&[written], &schema, false).unwrap(), vec![own]);
    assert_eq!(
        order_cols(&[written], &schema, true).unwrap(),
        vec![own, asc(c3, false), asc(c0, false), asc(c1, true), asc(c2, false)],
    );
}

/// NULL placement is absolute: `nulls_first` alone decides it, whatever the
/// direction, and two NULLs tie.
#[test]
fn null_placement_ignores_the_direction() {
    let schema = u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, true)]);
    let v = make_int_batch(&schema, &[(1, 1, &[None]), (2, 1, &[Some(5)]), (3, 1, &[None])]);
    let loc = schema.locate(1);
    for nulls_first in [false, true] {
        let want = if nulls_first { Ordering::Less } else { Ordering::Greater };
        for desc in [false, true] {
            let keys = [key(loc, desc, nulls_first, true)];
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

/// A batch's rows with every null bit set.
struct AllNull<'a>(&'a Batch);

impl RowSource for AllNull<'_> {
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        self.0.get_pk_bytes(row)
    }
    fn get_null_word(&self, _row: usize) -> u64 {
        u64::MAX
    }
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        self.0.get_col_ptr(row, payload_col, col_size)
    }
    fn heap(&self) -> Heap<'_> {
        RowSource::heap(self.0)
    }
    fn row_count(&self) -> usize {
        RowSource::row_count(self.0)
    }
}

/// A PK key never reads the null word: with every bit set, it still orders by
/// value, where a NULL arm would call the rows tied.
#[test]
fn a_pk_key_never_takes_the_null_arm() {
    let schema = pk_only_schema(&[TypeCode::U64]);
    let b = make_int_batch(&schema, &[(1, 1, &[]), (2, 1, &[])]);
    let v = AllNull(&b);
    let loc = schema.locate(0);
    for desc in [false, true] {
        let keys = [key(loc, desc, true, false)];
        let want = if desc { Ordering::Greater } else { Ordering::Less };
        assert_eq!(cmp_order_keys(&keys, &v, 0, 1), want, "desc={desc}");
    }
}

/// `RowRanking` against the comparator it stands in for, over `keys` on `v`: `sorted` is the
/// stable comparator sort, and under a total order `keep_smallest` keeps that sort's prefix.
fn assert_ranks_as_the_comparator(keys: &[OrderCol], total: bool, v: &Batch, label: &str) {
    let n = v.len();
    let mut want: Vec<u32> = (0..n as u32).collect();
    want.sort_by(|&a, &b| cmp_order_keys(keys, v, a as usize, b as usize));
    let mb = v.as_mem_batch();
    assert_eq!(RowRanking::new(keys, &mb).sorted(), want, "{label}");
    if !total {
        return;
    }
    for k in [0, 1, n / 3, n - 1, n, n + 1] {
        let mut ranking = RowRanking::new(keys, &mb);
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
    let mut rng = gnitz_foundation::rng::Rng::new(0x9E3779B97F4A7C15);
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
        let mut b = BatchBuilder::new(&schema);
        for row in 0..ROWS {
            b.begin_row(row as u128 + 1, 1);
            if rng.gen_range(5) == 0 {
                b.put_null();
            } else if tc.is_german_string() {
                b.put_blob(rng.pick(STRINGS));
            } else {
                b.put_int(native(tc, rng.pick(PATTERNS)));
            }
            b.put_int(rng.gen_range(3) as u128);
        }
        let mut cases = vec![(schema, b.finish(), 1u32)];
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
            let mut b = BatchBuilder::new(&schema);
            for row in 0..ROWS {
                let lead = native(tc, rng.pick(PATTERNS));
                b.begin_row_natives(&[lead, row as u128 + 1], 1).put_int(0);
            }
            cases.push((schema, b.finish(), 0));
        }
        for (schema, v, col) in &cases {
            for (desc, nulls_first) in [(false, false), (false, true), (true, false), (true, true)] {
                let label = format!("{tc} column {col} desc={desc} nulls_first={nulls_first}");
                let first = OrderKey { col: *col, desc, nulls_first };
                assert_ranks_as_the_comparator(&order_cols(&[first], schema, false).unwrap(), false, v, &label);
                assert_ranks_as_the_comparator(&order_cols(&[first], schema, true).unwrap(), true, v, &label);
                // A second written key, ranked by its own image inside the first's ties.
                let second = OrderKey { col: 2, desc: !desc, nulls_first };
                assert_ranks_as_the_comparator(&order_cols(&[first, second], schema, false).unwrap(), false, v, &label);
            }
        }
    }
}

/// A sort ranks each tie of the leading key by the next, yet the largest lead stays the
/// leading key's.
#[test]
fn max_lead_after_a_sort_is_the_leading_keys() {
    // `(id, v, w)`: v ties on rows 0 and 1, which w tells apart far above any v.
    let schema = u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, false); 2]);
    let v = make_int_batch(
        &schema,
        &[
            (1, 1, &[Some(1), Some(100)]),
            (2, 1, &[Some(1), Some(50)]),
            (3, 1, &[Some(0), Some(7)]),
        ],
    );
    let order = [
        OrderKey { col: 1, desc: false, nulls_first: false },
        OrderKey { col: 2, desc: false, nulls_first: false },
    ];
    let keys = order_cols(&order, &schema, false).unwrap();
    let mb = v.as_mem_batch();
    let want = RowRanking::new(&keys, &mb).max_lead();
    let mut ranking = RowRanking::new(&keys, &mb);
    ranking.sort();
    assert_eq!(ranking.rows().collect::<Vec<_>>(), [2, 1, 0]);
    assert!(want == ranking.max_lead());
}
