//! The order of rows under ORDER BY keys, over batches built row by row.

use std::cmp::Ordering;

use super::{OrderCol, RowRanking, cmp_order_keys, order_cols};
use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{keyed_schema, make_int_batch, pk_only_schema, pk_u64_two_i64_schema, u64_pk_schema};
use gnitz_foundation::rng::PATTERNS;
use gnitz_wire::{Heap, OrderKey, RowSource, TypeCode, image_mask};

/// The key `ORDER BY col` in direction `desc`, NULLs placed by `nulls_first`.
fn key(schema: &SchemaDescriptor, col: u32, desc: bool, nulls_first: bool) -> OrderCol {
    OrderCol::resolve(schema, &OrderKey { col, desc, nulls_first }).unwrap()
}

/// `v` cut to the low bytes a `tc` cell holds.
fn native(tc: TypeCode, v: u128) -> u128 {
    v & image_mask(tc.wire_stride())
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
    let asc = |col| key(&schema, col, false, true);
    let own = key(&schema, 2, true, false);
    assert_eq!(order_cols(&[written], &schema, false).unwrap(), vec![own]);
    assert_eq!(
        order_cols(&[written], &schema, true).unwrap(),
        vec![own, asc(3), asc(0), asc(1), asc(2)],
    );
}

/// NULL placement is absolute: `nulls_first` alone decides it, whatever the
/// direction, and two NULLs tie.
#[test]
fn null_placement_ignores_the_direction() {
    let schema = u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, true)]);
    let v = make_int_batch(&schema, &[(1, 1, &[None]), (2, 1, &[Some(5)]), (3, 1, &[None])]);
    for nulls_first in [false, true] {
        let want = if nulls_first { Ordering::Less } else { Ordering::Greater };
        for desc in [false, true] {
            let keys = [key(&schema, 1, desc, nulls_first)];
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
    for desc in [false, true] {
        let keys = [key(&schema, 0, desc, true)];
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
        let schema = u64_pk_schema(&[SchemaColumn::new(tc, true), SchemaColumn::new(TypeCode::I64, false)]);
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
            let schema = keyed_schema(&[tc, TypeCode::U16], &[SchemaColumn::new(TypeCode::I64, true)]);
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
    let schema = pk_u64_two_i64_schema();
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
