use super::*;
use crate::test_support::{col, ncol, schema, xorshift};
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
fn kv(rows: &[(u64, Option<i64>, Option<&str>, i64)]) -> ZSetBatch {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut b);
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
    (0..out.len()).map(|i| out.pks.get(i) as u64).collect()
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

/// The sort and cut as one comparator sort of a row permutation, each kept row rebuilt.
fn comparator_sort(schema: &Schema, batch: &ZSetBatch, order: &[OrderKey], window: Window) -> ZSetBatch {
    let cut = window.cuts();
    let end = window.end().unwrap_or(usize::MAX);
    let tiebroken = order_locators(order, schema);
    let keys = if cut { &tiebroken[..] } else { &tiebroken[..order.len()] };
    let mut perm: Vec<usize> = (0..batch.len()).collect();
    perm.sort_by(|&a, &b| gnitz_expr::cmp_order_keys(keys, batch, a, batch, b));
    let mut out = ZSetBatch::new(schema);
    let mut at = 0usize;
    for r in perm {
        let next = at.saturating_add(batch.weights[r] as usize);
        let kept = next.min(end).saturating_sub(at.max(window.offset));
        if kept > 0 {
            out.copy_row_at(batch, r, kept as i64);
        }
        at = next;
        if at >= end {
            break;
        }
    }
    out
}

/// A row's key bytes, weight, null word and payload cells, a string by its content.
type RowCells = (Vec<u8>, i64, u64, Vec<Vec<u8>>);

fn cells(schema: &Schema, b: &ZSetBatch) -> Vec<RowCells> {
    (0..b.len())
        .map(|r| {
            let payload = schema
                .payload_columns()
                .map(|(pi, _, c)| {
                    let w = c.ty.tc.wire_stride();
                    let cell = &b.payload[pi].bytes[r * w..(r + 1) * w];
                    match c.ty.tc.is_german_string() {
                        true => gnitz_wire::german_string_content(cell, &b.blob).to_vec(),
                        false => cell.to_vec(),
                    }
                })
                .collect();
            (b.pks.get_bytes(r).to_vec(), b.weights[r], b.nulls[r], payload)
        })
        .collect()
}

/// Random batches under three key shapes, zero to three keys over any column in either
/// direction and NULL placement, weights above 1 and random windows.
#[test]
fn a_ranked_sort_and_cut_is_the_comparator_sort_and_cut() {
    let mut st = 0x2545F4914F6CDD1Du64;
    let mut rng = move || xorshift(&mut st);
    let floats = [
        0.0,
        -0.0,
        1.5,
        -1.5,
        f64::NAN,
        -f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        1e300,
        5e-324,
    ];
    let strings = [
        "",
        "a",
        "ab",
        "user_0001",
        "user_0002",
        "user_00020",
        "user_0002\0",
        "zzzzzzzzzzzz",
        "zzzzzzzzzzzzzzzzzzzzzzzz_1",
        "zzzzzzzzzzzzzzzzzzzzzzzz_2",
    ];
    const COLS: usize = 9;
    for pk in [vec![0u32], vec![5, 0], vec![4]] {
        let s = schema(
            vec![
                col("id", TypeCode::U64),
                ncol("v", TypeCode::I64),
                ncol("f", TypeCode::F64),
                ncol("s", TypeCode::String),
                col("u", TypeCode::U128),
                col("n", TypeCode::I16),
                ncol("g", TypeCode::I32),
                ncol("b", TypeCode::Bool),
                ncol("h", TypeCode::F32),
            ],
            &pk,
        );
        for round in 0..400 {
            let n = (rng() % 60) as usize;
            let mut b = ZSetBatch::new(&s);
            let mut app = BatchAppender::new(&mut b);
            for i in 0..n as u64 {
                // `id` and `u` carry the row number, so every key shape is unique. `u` takes
                // three values in each half, the row number in the low one.
                let id = i + 256 * (rng() % 2);
                let u = ((rng() % 3) as u128) << 64 | (i << 16) as u128 | (rng() % 3) as u128;
                let small = (rng() % 5) as i16 - 2;
                let natives: Vec<u128> = pk
                    .iter()
                    .map(|&c| match c {
                        0 => id as u128,
                        4 => u,
                        _ => small as u16 as u128,
                    })
                    .collect();
                app.add_row_natives(&natives, (rng() % 3) as i64 + 1);
                for c in (0..COLS as u32).filter(|c| !pk.contains(c)) {
                    let null = s.columns[c as usize].is_nullable && rng() % 4 == 0;
                    match c {
                        _ if null => app.null(),
                        0 => app.u64_val(id),
                        1 => app.i64_val((rng() % 5) as i64 - 2),
                        2 => app.f64_val(floats[(rng() % 10) as usize]),
                        3 => app.str_val(strings[(rng() % 10) as usize]),
                        4 => app.u128_val(u),
                        5 => app.int_val(small as i128),
                        6 => app.int_val((rng() % 7) as i128 - 3),
                        7 => app.int_val((rng() % 2) as i128),
                        _ => app.f32_val(floats[(rng() % 10) as usize] as f32),
                    };
                }
            }
            let order: Vec<OrderKey> = (0..rng() % 4)
                .map(|_| key((rng() % COLS as u64) as u16, rng() % 2 == 0, rng() % 2 == 0))
                .collect();
            let window = Window {
                offset: (rng() % 2) as usize * (rng() % 20) as usize,
                limit: (rng() % 3 > 0).then(|| (rng() % 40) as usize),
            };
            let want = comparator_sort(&s, &b, &order, window);
            let got = order_and_window(&s, b, &order, window);
            let at = format!(
                "pk {pk:?} round {round} {order:?} offset {} limit {:?}",
                window.offset, window.limit
            );
            assert_eq!(cells(&s, &got), cells(&s, &want), "{at}");
            got.validate(&s).unwrap();
        }
    }
}

/// A window clips the first and the last entry it reaches and no entry between them.
#[test]
fn a_window_clips_only_its_two_ends() {
    let b = kv(&[(1, Some(10), None, 3), (2, Some(20), None, 1), (3, Some(30), None, 4)]);
    for (offset, limit, want) in [
        (2, 3, vec![(1, 1), (2, 1), (3, 1)]),
        (1, 6, vec![(1, 2), (2, 1), (3, 3)]),
        // Inside one entry.
        (5, 2, vec![(3, 2)]),
        (1, 1, vec![(1, 1)]),
    ] {
        let out = order_and_window(&kv_schema(), b.clone(), &BY_V, Window { offset, limit: Some(limit) });
        let got: Vec<(u64, i64)> = ids(&out).into_iter().zip(out.weights.iter().copied()).collect();
        assert_eq!(got, want, "OFFSET {offset} LIMIT {limit}");
    }
}
