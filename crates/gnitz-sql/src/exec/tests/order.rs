use super::*;
use crate::test_support::Cell::{self, Int, Null, Str};
use crate::test_support::{col, ncol, schema};
use gnitz_core::BatchAppender;
use gnitz_wire::{OrderKey, TypeCode, WeightSlots};

/// A wire key over column `col`.
const fn key(col: u32, desc: bool, nulls_first: bool) -> OrderKey {
    OrderKey { col, desc, nulls_first }
}

/// `ORDER BY v`.
const BY_V: [OrderKey; 1] = [key(1, false, false)];

/// `(id U64 pk, v I64 nullable, s STRING nullable)`.
fn kv_schema() -> Schema {
    schema(
        vec![
            col("id", TypeCode::U64),
            ncol("v", TypeCode::I64),
            ncol("s", TypeCode::String),
        ],
        &[0],
    )
}

/// A `kv_schema` batch of `(id, v, s, weight)` rows.
fn kv(rows: &[(u128, Cell, Cell, i64)]) -> ZSetBatch {
    let mut b = ZSetBatch::new(&kv_schema());
    let mut app = BatchAppender::new(&mut b);
    for &(id, v, s, w) in rows {
        app.add_row(id, w);
        v.push(&mut app);
        s.push(&mut app);
    }
    b
}

/// `order` and `slice` over `kv_schema`.
fn cut_of(order: &[OrderKey], slice: WeightSlots) -> ResultCut {
    ResultCut::new(std::sync::Arc::new(kv_schema()), order, slice).unwrap()
}

/// The sort and cut of `b`, checked to be a well-formed batch.
fn run(b: &ZSetBatch, order: &[OrderKey], slice: WeightSlots) -> ZSetBatch {
    let out = cut_of(order, slice).apply(b.clone());
    out.validate(&kv_schema()).unwrap();
    out
}

/// The id of each entry of `out`, in order.
fn ids(out: &ZSetBatch) -> Vec<u64> {
    (0..out.len()).map(|i| out.pks.get(i) as u64).collect()
}

/// Ids 4 and 2 tie on `v`, arriving against identity order: uncut ties stay in input order,
/// a cut breaks them by identity.
#[test]
fn orders_by_the_written_keys() {
    let b = kv(&[
        (1, Int(30), Str("a"), 1),
        (4, Int(10), Str("a"), 1),
        (3, Null, Str("a"), 1),
        (2, Int(10), Str("b"), 1),
    ]);
    let cut = |skip, keep| WeightSlots { skip, keep };
    for (keys, slice, want) in [
        (&[][..], WeightSlots::ALL, &[1, 4, 3, 2][..]),
        (&[], cut(1, 2), &[4, 3]),
        (&BY_V, WeightSlots::ALL, &[4, 2, 1, 3]),
        (&BY_V, cut(0, 4), &[2, 4, 1, 3]),
        (
            &[key(1, true, true), key(2, false, false)],
            WeightSlots::ALL,
            &[3, 1, 4, 2],
        ),
        (&[key(0, true, true)], WeightSlots::ALL, &[4, 3, 2, 1]),
        (&[key(1, true, true)], cut(0, 4), &[3, 1, 2, 4]),
    ] {
        assert_eq!(ids(&run(&b, keys, slice)), want, "{keys:?} LIMIT {}", slice.keep);
    }
}

/// Each id's summed weight, in output order: the bag a cut is a function of,
/// whichever of several same-identity entries carries it.
fn bag(out: &ZSetBatch) -> Vec<(u64, i64)> {
    let mut bag: Vec<(u64, i64)> = Vec::new();
    for (id, &w) in ids(out).into_iter().zip(&out.weights) {
        match bag.last_mut() {
            Some((last, n)) if *last == id => *n += w,
            _ => bag.push((id, w)),
        }
    }
    bag
}

/// A slice clips the first and the last entry it reaches and no entry between them. Id 1
/// arrives as two entries, which the partial selection may split.
#[test]
fn offset_and_limit_count_logical_rows() {
    let b = kv(&[
        (3, Int(30), Null, 3),
        (1, Int(10), Null, 1),
        (2, Int(20), Null, 3),
        (1, Int(10), Null, 2),
    ]);
    // Sorted, logical rows [0, 3) are id 1, [3, 6) id 2, [6, 9) id 3.
    for (order, offset, limit, want) in [
        (&BY_V[..], 0, 1, vec![(1, 1)]),
        (&BY_V, 0, 2, vec![(1, 2)]),
        (&BY_V, 4, 1, vec![(2, 1)]),
        (&BY_V, 0, 4, vec![(1, 3), (2, 1)]),
        (&BY_V, 2, 5, vec![(1, 1), (2, 3), (3, 1)]),
        (&BY_V, 4, 3, vec![(2, 2), (3, 1)]),
        (&BY_V, 3, 3, vec![(2, 3)]),
        (&BY_V, 7, u64::MAX, vec![(3, 2)]),
        (&BY_V, 9, 1, vec![]),
        (&BY_V, 0, 100, vec![(1, 3), (2, 3), (3, 3)]),
        // Unsorted, the cut walks input order.
        (&[], 2, 3, vec![(3, 1), (1, 1), (2, 1)]),
    ] {
        let out = run(&b, order, WeightSlots { skip: offset, keep: limit });
        assert_eq!(bag(&out), want, "{order:?} OFFSET {offset} LIMIT {limit}");
    }
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "non-positive weight violates the bag invariant")]
fn cut_names_a_ghost_it_would_discard_unseen() {
    // n = 3, k = 2 — the ghost sorts last, so the truncation drops it and the
    // multiplicity walk never reaches it. Only the whole-batch check sees it.
    let b = kv(&[(1, Int(10), Null, 1), (2, Int(20), Null, 1), (3, Int(30), Null, 0)]);
    let _ = cut_of(&BY_V, WeightSlots { skip: 0, keep: 2 }).apply(b);
}
