use super::*;
use crate::test_support::col_def;
use gnitz_core::TypeCode;
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_wire::OrderKey;

// The per-key comparison itself (`cmp_order_keys`) is pinned by gnitz-expr's own
// tests; the tests here cover the finish built on it.

// ---- paginate (multiplicity walk) ----

#[test]
fn paginate_all_weight_one_is_entry_counting() {
    let w = [1i64; 5];
    // LIMIT 3.
    assert_eq!(
        paginate(w.iter().copied().enumerate(), 0, 3),
        vec![(0, 1), (1, 1), (2, 1)]
    );
    // OFFSET 2 LIMIT 2.
    assert_eq!(paginate(w.iter().copied().enumerate(), 2, 4), vec![(2, 1), (3, 1)]);
}

#[test]
fn paginate_boundary_entry_keeps_reduced_weight() {
    // Entries [2,3,2]; LIMIT 3 lands inside entry 1 (weight 3 → 1 survives).
    let w = [2i64, 3, 2];
    assert_eq!(paginate(w.iter().copied().enumerate(), 0, 3), vec![(0, 2), (1, 1)]);
    // The surviving logical-row count is exactly 3 (bag semantics).
    assert_eq!(
        paginate(w.iter().copied().enumerate(), 0, 3)
            .iter()
            .map(|(_, x)| *x)
            .sum::<i64>(),
        3
    );
}

#[test]
fn paginate_offset_and_limit_inside_one_entry() {
    // Weight-5 entry, OFFSET 1 LIMIT 1 → the overlap formula yields weight 1
    // (a two-independent-passes impl gets this wrong).
    let w = [5i64];
    assert_eq!(paginate(w.iter().copied().enumerate(), 1, 2), vec![(0, 1)]);
}

#[test]
fn paginate_weight3_straddles_cut() {
    // A weight-3 entry straddling the window keeps only the overlapping part.
    let w = [3i64, 3, 3];
    // window [0,4): entry0 whole (3), entry1 clipped to 1.
    assert_eq!(paginate(w.iter().copied().enumerate(), 0, 4), vec![(0, 3), (1, 1)]);
    // window [4,7): entry1 clipped low (2), entry2 clipped high (1).
    assert_eq!(paginate(w.iter().copied().enumerate(), 4, 7), vec![(1, 2), (2, 1)]);
}

// ---- the finish ----

/// A wire key over column `col`.
fn key(col: u16, desc: bool, nulls_first: bool) -> OrderKey {
    OrderKey { col, desc, nulls_first }
}

/// `ASC NULLS LAST`, the SQL default for ASC.
fn asc(col: u16) -> OrderKey {
    key(col, false, false)
}

/// `DESC NULLS FIRST`, the SQL default for DESC.
fn desc(col: u16) -> OrderKey {
    key(col, true, true)
}

fn window(offset: usize, limit: Option<usize>) -> Window {
    Window { offset, limit }
}

const UNCUT: Window = Window { offset: 0, limit: None };

/// `(id U64 pk, v I64 nullable, s STRING nullable)`.
fn kv_schema() -> Schema {
    Schema {
        columns: vec![
            col_def("id", TypeCode::U64, false),
            col_def("v", TypeCode::I64, true),
            col_def("s", TypeCode::String, true),
        ],
        pk_cols: vec![0],
    }
}

/// Push one row (`pk`, optional `v`, optional `s`, weight) into a `kv_schema` batch.
fn push_kv(b: &mut ZSetBatch, pk: u64, v: Option<i64>, s: Option<&str>, weight: i64) {
    b.pks.push_u128(&kv_schema(), pk as u128);
    b.weights.push(weight);
    let mut null_word = 0u64;
    if v.is_none() {
        null_word |= 1 << 0; // payload idx 0 = v
    }
    if s.is_none() {
        null_word |= 1 << 1; // payload idx 1 = s
    }
    b.nulls.push(null_word);
    b.payload[0].bytes.extend_from_slice(&v.unwrap_or(0).to_le_bytes());
    let cell = gnitz_wire::encode_german_string(s.unwrap_or("").as_bytes(), &mut b.blob);
    b.payload[1].bytes.extend_from_slice(&cell);
}

/// The PK of each entry of `out`, in order.
fn ids(schema: &Schema, out: &ZSetBatch) -> Vec<u64> {
    (0..out.len()).map(|i| out.pks.get(schema, i) as u64).collect()
}

#[test]
fn orders_a_payload_column_asc_and_desc() {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 1, Some(30), Some("a"), 1);
    push_kv(&mut b, 2, Some(10), Some("b"), 1);
    push_kv(&mut b, 3, Some(20), Some("c"), 1);

    let out = order_and_window(&schema, b.clone(), &[asc(1)], UNCUT);
    assert_eq!(ids(&schema, &out), vec![2, 3, 1]);
    let out = order_and_window(&schema, b, &[desc(1)], UNCUT);
    assert_eq!(ids(&schema, &out), vec![1, 3, 2]);
}

#[test]
fn null_placement_is_absolute_under_both_directions() {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 1, Some(10), None, 1);
    push_kv(&mut b, 2, None, None, 1);
    push_kv(&mut b, 3, Some(20), None, 1);

    for (k, want) in [
        (key(1, false, false), [1, 3, 2]),
        (key(1, false, true), [2, 1, 3]),
        // `desc` reverses the values, never the NULL placement.
        (key(1, true, true), [2, 3, 1]),
        (key(1, true, false), [3, 1, 2]),
    ] {
        let out = order_and_window(&schema, b.clone(), &[k], UNCUT);
        assert_eq!(ids(&schema, &out), want, "{k:?}");
    }
}

#[test]
fn orders_by_string_then_multiple_keys() {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 1, Some(1), Some("banana"), 1);
    push_kv(&mut b, 2, Some(1), Some("apple"), 1);
    push_kv(&mut b, 3, Some(2), Some("apple"), 1);

    // ORDER BY v, s: (1,apple)=2, (1,banana)=1, (2,apple)=3.
    let out = order_and_window(&schema, b.clone(), &[asc(1), asc(2)], UNCUT);
    assert_eq!(ids(&schema, &out), vec![2, 1, 3]);
    // ORDER BY s DESC, v: banana=1, then (apple,1)=2, (apple,2)=3.
    let out = order_and_window(&schema, b, &[desc(2), asc(1)], UNCUT);
    assert_eq!(ids(&schema, &out), vec![1, 2, 3]);
}

#[test]
fn limit_counts_multiplicity() {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    // A weight-3 entry sorts first, then a weight-1 entry.
    push_kv(&mut b, 1, Some(10), Some("x"), 3);
    push_kv(&mut b, 2, Some(20), Some("y"), 1);

    // LIMIT 2 must clip the weight-3 entry to weight 2 (2 logical rows), not
    // return 2 entries (= 4 logical rows).
    let out = order_and_window(&schema, b, &[asc(1)], window(0, Some(2)));
    assert_eq!(ids(&schema, &out), vec![1], "one entry survives");
    assert_eq!(out.weights, [2], "clipped to weight 2");
}

#[test]
fn offset_and_limit_inside_one_entry() {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 2, Some(20), None, 1);
    push_kv(&mut b, 1, Some(10), None, 5);

    let out = order_and_window(&schema, b, &[asc(1)], window(1, Some(1)));
    assert_eq!(ids(&schema, &out), vec![1]);
    assert_eq!(out.weights, [1]);
}

/// Expand a windowed `kv_schema` batch into its logical rows — each entry
/// repeated by its weight. The bag is the only thing a cut is a function of;
/// *which* of several entries sharing an identity survives is not.
fn expand_kv(schema: &Schema, out: &ZSetBatch) -> Vec<(u64, Option<i64>)> {
    let v_ci = schema.columns.iter().position(|c| c.name == "v").unwrap();
    let mut rows = Vec::new();
    for i in 0..out.len() {
        let v = if gnitz_wire::null_word_get(out.nulls[i], schema.payload_idx(v_ci)) {
            None
        } else {
            let buf = &out.payload[schema.payload_idx(v_ci)].bytes;
            Some(i64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap()))
        };
        for _ in 0..out.weights[i] {
            rows.push((out.pks.get(schema, i) as u64, v));
        }
    }
    rows
}

#[test]
fn cut_splitting_a_tie_group_matches_the_consolidated_input() {
    // Three entries share one identity (weights 1, 1, 3) plus one larger row:
    // n = 4, k = 2, so the partial selection keeps two of the three tied
    // entries and splits the tie group. The result must be the same bag the
    // consolidated batch (one weight-5 entry) yields — duplicate
    // (PK, payload) is not a precondition the cut may assume.
    let schema = kv_schema();

    let mut dup = ZSetBatch::new(&schema);
    for w in [1i64, 1, 3] {
        push_kv(&mut dup, 1, Some(10), None, w);
    }
    push_kv(&mut dup, 2, Some(20), None, 1);

    let mut folded = ZSetBatch::new(&schema);
    push_kv(&mut folded, 1, Some(10), None, 5);
    push_kv(&mut folded, 2, Some(20), None, 1);

    let out_dup = order_and_window(&schema, dup, &[asc(1)], window(0, Some(2)));
    let out_folded = order_and_window(&schema, folded, &[asc(1)], window(0, Some(2)));

    assert_eq!(expand_kv(&schema, &out_dup), vec![(1, Some(10)); 2]);
    assert_eq!(expand_kv(&schema, &out_dup), expand_kv(&schema, &out_folded));
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "non-positive weight violates the bag invariant")]
fn cut_names_a_ghost_it_would_discard_unseen() {
    // n = 3, k = 2 — the ghost sorts last, so the truncation drops it and the
    // multiplicity walk never reaches it. Only the whole-batch check sees it.
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 1, Some(10), None, 1);
    push_kv(&mut b, 2, Some(20), None, 1);
    push_kv(&mut b, 3, Some(30), None, 0);
    let _ = order_and_window(&schema, b, &[asc(1)], window(0, Some(2)));
}

#[test]
fn a_null_u128_is_read_from_the_bitmap() {
    // A NULL U128 payload is zero-filled filler; NULL must be read from the
    // bitmap, not off the value.
    let schema = Schema {
        columns: vec![col_def("id", TypeCode::U64, false), col_def("u", TypeCode::U128, true)],
        pk_cols: vec![0],
    };
    let mut b = ZSetBatch::new(&schema);
    for (pk, u) in [(1u128, Some(5u128)), (2, None), (3, Some(1))] {
        b.pks.push_u128(&schema, pk);
        b.weights.push(1);
        b.nulls.push(u.is_none() as u64);
        b.payload[0].bytes.extend_from_slice(&u.unwrap_or(0).to_le_bytes());
    }

    // ASC NULLS LAST: 1, 5, NULL → ids 3, 1, 2.
    let out = order_and_window(&schema, b, &[asc(1)], UNCUT);
    assert_eq!(ids(&schema, &out), vec![3, 1, 2]);
}

#[test]
fn a_second_signed_pk_column_orders_by_value() {
    // Compound PK (a U16, b I16); sort by the SECOND PK column `b` which holds
    // a negative — pins that a multi-byte value (256 vs 1) and a non-leading
    // signed column order by value.
    let schema = Schema {
        columns: vec![
            col_def("a", TypeCode::U16, false),
            col_def("b", TypeCode::I16, false),
            col_def("w", TypeCode::I64, true),
        ],
        pk_cols: vec![0, 1],
    };
    let mut b = ZSetBatch::new(&schema);
    // b ∈ {1, -5, 256}; ascending signed order is -5, 1, 256.
    for (a, bb) in [(1u16, 1i16), (2, -5), (3, 256)] {
        let mut pk = [0u8; 4];
        pk[..2].copy_from_slice(&a.to_le_bytes());
        pk[2..].copy_from_slice(&bb.to_le_bytes());
        b.pks.push_bytes(&schema, &pk);
        b.weights.push(1);
        b.nulls.push(1); // w NULL (payload idx 0)
        b.payload[0].bytes.extend_from_slice(&0i64.to_le_bytes());
    }

    let out = order_and_window(&schema, b, &[asc(1)], UNCUT);
    // Read `b` from the compound PK region, addressed the way the comparator
    // addresses it rather than by re-deriving the offset.
    let ColumnLocator::Pk { byte_off, size, type_code } = SchemaFacts::locate(&schema, 1) else {
        panic!("column 1 is a PK column");
    };
    let bs: Vec<i16> = (0..out.len())
        .map(|i| {
            let w = &out.pks.get_bytes(i)[byte_off as usize..(byte_off + size) as usize];
            let mut native = [0u8; 16];
            gnitz_wire::decode_pk_column(w, type_code, &mut native[..w.len()]);
            i16::from_le_bytes(native[..2].try_into().unwrap())
        })
        .collect();
    assert_eq!(bs, vec![-5, 1, 256]);
}

#[test]
fn nothing_to_order_or_cut_hands_the_batch_back() {
    let schema = kv_schema();
    let mut b = ZSetBatch::new(&schema);
    push_kv(&mut b, 3, Some(30), Some("c"), 1);
    push_kv(&mut b, 1, Some(10), Some("a"), 1);
    let weights = b.weights.as_ptr();
    let out = order_and_window(&schema, b, &[], UNCUT);
    assert_eq!(out.weights.as_ptr(), weights);
    assert_eq!(ids(&schema, &out), vec![3, 1]);
}
