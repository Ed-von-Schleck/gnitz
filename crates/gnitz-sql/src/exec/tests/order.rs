use super::*;
use crate::test_support::{col, ncol};
use gnitz_core::BatchAppender;
use gnitz_wire::{OrderKey, TypeCode};

/// A wire key over column `col`.
fn key(col: u16, desc: bool, nulls_first: bool) -> OrderKey {
    OrderKey { col, desc, nulls_first }
}

/// `ORDER BY v`.
const BY_V: [OrderKey; 1] = [OrderKey { col: 1, desc: false, nulls_first: false }];

const UNCUT: Window = Window { offset: 0, limit: None };

/// `(id U64 pk, v I64 nullable, s STRING nullable)`.
fn kv_schema() -> Schema {
    Schema {
        columns: vec![
            col("id", TypeCode::U64),
            ncol("v", TypeCode::I64),
            ncol("s", TypeCode::String),
        ],
        pk_cols: vec![0],
    }
}

/// A `kv_schema` batch of `(id, v, s, weight)` rows.
fn kv(rows: &[(u64, Option<i64>, Option<&str>, i64)]) -> ZSetBatch {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut b, &schema);
    for &(id, v, s, w) in rows {
        app.add_row(id as u128, w);
        match v {
            Some(v) => app.i64_val(v),
            None => app.null(),
        };
        match s {
            Some(s) => app.str_val(s),
            None => app.null(),
        };
    }
    b
}

/// The id of each entry of `out`, in order.
fn ids(out: &ZSetBatch) -> Vec<u64> {
    (0..out.len()).map(|i| out.pks.get(&kv_schema(), i) as u64).collect()
}

#[test]
fn orders_by_the_written_keys() {
    let b = kv(&[
        (1, Some(30), Some("a"), 1),
        (2, Some(10), Some("b"), 1),
        (3, None, Some("a"), 1),
        (4, Some(10), Some("a"), 1),
    ]);
    // Every key list is total over the data, so no tie order is pinned.
    for (keys, want) in [
        (vec![key(1, false, false), key(2, false, false)], [4, 2, 1, 3]),
        (vec![key(1, false, true), key(2, true, true)], [3, 2, 4, 1]),
        // `desc` reverses the values, never the NULL placement.
        (vec![key(1, true, true), key(2, false, false)], [3, 1, 4, 2]),
        (vec![key(1, true, false), key(2, false, false)], [1, 4, 2, 3]),
        (vec![key(2, true, true), key(1, false, false)], [2, 4, 1, 3]),
        (vec![key(0, true, true)], [4, 3, 2, 1]),
    ] {
        let out = order_and_window(&kv_schema(), b.clone(), &keys, UNCUT);
        assert_eq!(ids(&out), want, "{keys:?}");
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

/// Id 1 arrives as two entries, which the partial selection may split.
#[test]
fn offset_and_limit_count_logical_rows() {
    let b = kv(&[
        (3, Some(30), None, 3),
        (1, Some(10), None, 1),
        (2, Some(20), None, 3),
        (1, Some(10), None, 2),
    ]);
    // Logical rows [0, 3) are id 1, [3, 6) id 2, [6, 9) id 3.
    for (offset, limit, want) in [
        (0, Some(1), vec![(1, 1)]),
        (0, Some(2), vec![(1, 2)]),
        (4, Some(1), vec![(2, 1)]),
        (0, Some(4), vec![(1, 3), (2, 1)]),
        (4, Some(3), vec![(2, 2), (3, 1)]),
        (3, Some(3), vec![(2, 3)]),
        (7, None, vec![(3, 2)]),
        (9, Some(1), vec![]),
        (0, Some(100), vec![(1, 3), (2, 3), (3, 3)]),
    ] {
        let out = order_and_window(&kv_schema(), b.clone(), &BY_V, Window { offset, limit });
        assert_eq!(bag(&out), want, "OFFSET {offset} LIMIT {limit:?}");
    }
}

#[test]
fn a_cut_breaks_key_ties_by_identity() {
    let b = kv(&[(3, Some(20), None, 1), (2, Some(20), None, 1)]);
    let out = order_and_window(&kv_schema(), b, &BY_V, Window { offset: 0, limit: Some(2) });
    assert_eq!(ids(&out), [2, 3]);
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "non-positive weight violates the bag invariant")]
fn cut_names_a_ghost_it_would_discard_unseen() {
    // n = 3, k = 2 — the ghost sorts last, so the truncation drops it and the
    // multiplicity walk never reaches it. Only the whole-batch check sees it.
    let b = kv(&[(1, Some(10), None, 1), (2, Some(20), None, 1), (3, Some(30), None, 0)]);
    let _ = order_and_window(&kv_schema(), b, &BY_V, Window { offset: 0, limit: Some(2) });
}
