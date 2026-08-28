use super::*;
use crate::test_support::make_schema_u64_i64;
use std::rc::Rc;

/// Build a sorted+consolidated batch of `(pk, weight, payload)` rows.
fn make_batch(rows: &[(u64, i64, i64)]) -> Rc<Batch> {
    Rc::new(crate::test_support::make_batch(&make_schema_u64_i64(), rows))
}

/// A recorded co-group triple: the key, the delta range, and the PKs of the
/// match-group rows the callback walked (each with its weight).
type Triple = (u64, Range<usize>, Vec<(u64, i64)>);

/// A co-group test case: `(delta_rows, match_rows)`, each `(pk, weight, payload)`.
type CoGroupCase = (&'static [(u64, i64, i64)], &'static [(u64, i64, i64)]);

/// Recover a u64 PK from its OPK byte window (unsigned ⇒ OPK == BE).
fn pk_of(key: &[u8]) -> u64 {
    gnitz_wire::widen_pk_be(key, key.len()) as u64
}

/// Naive intersection reference: for every delta PK group, if the match
/// batch has the same PK, emit (pk, delta-range, match-PKs).
fn naive_intersection(delta: &Batch, m: &Batch) -> Vec<Triple> {
    naive(delta, m, true)
}
fn naive_left(delta: &Batch, m: &Batch) -> Vec<Triple> {
    naive(delta, m, false)
}
fn naive(delta: &Batch, m: &Batch, intersection: bool) -> Vec<Triple> {
    let mut out = Vec::new();
    let n = delta.count;
    let mut i = 0;
    while i < n {
        let dk = delta.get_pk_bytes(i).to_vec();
        let mut j = i + 1;
        while j < n && delta.get_pk_bytes(j) == &dk[..] {
            j += 1;
        }
        // Match group from the reference batch.
        let mut match_rows = Vec::new();
        for r in 0..m.count {
            if m.get_pk_bytes(r) == &dk[..] {
                match_rows.push((pk_of(m.get_pk_bytes(r)), m.get_weight(r)));
            }
        }
        if !intersection || !match_rows.is_empty() {
            out.push((pk_of(&dk), i..j, match_rows));
        }
        i = j;
    }
    out
}

/// Run the intersection skeleton, recording each triple's key, delta range,
/// and walked match PKs.
fn run_intersection(delta: &Batch, m: &mut ReadCursor) -> Vec<Triple> {
    let mut out = Vec::new();
    cogroup_intersection(delta, m, |key, range, m| {
        let mut rows = Vec::new();
        while m.valid && m.current_pk_eq(key) {
            rows.push((pk_of(m.current_pk_bytes()), 0));
            m.advance();
        }
        out.push((pk_of(key), range, rows));
    });
    out
}
fn run_left(delta: &Batch, m: &mut ReadCursor) -> Vec<Triple> {
    let mut out = Vec::new();
    cogroup_left(delta, m, |key, range, m| {
        let mut rows = Vec::new();
        while m.valid && m.current_pk_eq(key) {
            rows.push((pk_of(m.current_pk_bytes()), 0));
            m.advance();
        }
        out.push((pk_of(key), range, rows));
    });
    out
}

/// Strip weights (the trait can't read them) for a structural comparison.
fn strip(t: &[Triple]) -> Vec<(u64, Range<usize>, Vec<u64>)> {
    t.iter()
        .map(|(k, r, rows)| (*k, r.clone(), rows.iter().map(|(pk, _)| *pk).collect()))
        .collect()
}

/// The skeleton's triples must match the naive reference (keys, delta ranges,
/// and walked match PKs) over a range of shapes.
#[test]
fn intersection_matches_reference_all_shapes() {
    let s = make_schema_u64_i64();
    let cases: &[CoGroupCase] = &[
        // (delta, match)
        (&[], &[(1, 1, 10)]),                                   // empty delta
        (&[(1, 1, 10)], &[]),                                   // empty match
        (&[(1, 1, 10), (3, 1, 30)], &[(2, 1, 20), (4, 1, 40)]), // no shared
        (&[(1, 1, 10), (2, 1, 20)], &[(1, 1, 11), (2, 1, 22)]), // all shared
        // duplicate keys on each side (multiset delta + multi-payload match)
        (
            &[(1, 1, 10), (1, 1, 11), (5, 1, 50)],
            &[(1, 1, 90), (1, 1, 91), (1, 1, 92), (5, 1, 55)],
        ),
        // huge delta, tiny match (old swapped regime)
        (
            &[(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5), (6, 1, 6)],
            &[(4, 1, 44)],
        ),
        // tiny delta, huge match (old merge-walk regime)
        (
            &[(4, 1, 4)],
            &[(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 44), (5, 1, 5), (6, 1, 6)],
        ),
    ];

    for (di, mi) in cases {
        let delta = make_batch(di);
        let mb = make_batch(mi);
        let want = strip(&naive_intersection(&delta, &mb));

        let mut ch = ReadCursor::over_batches(std::slice::from_ref(&mb), s);
        let got_rc = strip(&run_intersection(&delta, &mut ch));
        assert_eq!(got_rc, want, "ReadCursor delta={di:?} match={mi:?}");
    }
}

#[test]
fn left_matches_reference_all_shapes() {
    let s = make_schema_u64_i64();
    let cases: &[CoGroupCase] = &[
        (&[], &[(1, 1, 10)]),
        (&[(1, 1, 10)], &[]),                                   // every delta key, empty match
        (&[(1, 1, 10), (3, 1, 30)], &[(2, 1, 20)]),             // match absent for delta keys
        (&[(1, 1, 10), (2, 1, 20)], &[(1, 1, 11), (2, 1, 22)]), // all present
        (
            &[(1, 1, 10), (1, 1, 11), (5, 1, 50)],
            &[(1, 1, 90), (1, 1, 91), (5, 1, 55)],
        ),
    ];

    for (di, mi) in cases {
        let delta = make_batch(di);
        let mb = make_batch(mi);
        let want = strip(&naive_left(&delta, &mb));

        let mut ch = ReadCursor::over_batches(std::slice::from_ref(&mb), s);
        let got_rc = strip(&run_left(&delta, &mut ch));
        assert_eq!(got_rc, want, "ReadCursor delta={di:?} match={mi:?}");
    }
}

/// A multi-source trace whose consolidation produces a ghost group (PK=3
/// nets to weight 0 across two sources, so the merge must skip it). The
/// skeleton must behave as if that key is absent — `advance_to`/`drive`
/// land past the ghost, identically to a from-scratch seek.
#[test]
fn intersection_skips_ghost_group_multi_source() {
    let s = make_schema_u64_i64();
    // Source A: pk 1,3,5 ; Source B: pk 3 (negated) → pk=3 nets to 0.
    let src_a = make_batch(&[(1, 1, 10), (3, 1, 30), (5, 1, 50)]);
    let src_b = make_batch(&[(3, -1, 30)]);
    let delta = make_batch(&[(1, 1, 1), (3, 1, 3), (5, 1, 5)]);

    // Reference: a consolidated trace holding only pk 1 and 5 (3 is a ghost).
    let live = make_batch(&[(1, 1, 10), (5, 1, 50)]);
    let want = strip(&naive_intersection(&delta, &live));

    let mut ch = ReadCursor::over_batches(&[Rc::clone(&src_a), Rc::clone(&src_b)], s);
    let got = strip(&run_intersection(&delta, &mut ch));
    assert_eq!(got, want, "ghost group pk=3 must be skipped");
}

/// A pre-advanced (stale) shared match cursor must still produce the full
/// intersection — the skeleton self-positions via `advance_to(delta[0])`,
/// which is backward-capable. This is the shared-trace-register case (two
/// inner joins on one trace in a LEFT JOIN epoch) without the old `rewind`.
#[test]
fn intersection_self_positions_stale_cursor() {
    let s = make_schema_u64_i64();
    let mb = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50)]);
    let delta = make_batch(&[(1, 1, 1), (3, 1, 3), (5, 1, 5)]);
    let want = strip(&naive_intersection(&delta, &mb));

    let mut ch = ReadCursor::over_batches(std::slice::from_ref(&mb), s);
    // Stale-advance the cursor past several keys before co-grouping.
    ch.advance_to(&(4u128).to_be_bytes()[8..]);
    assert!(ch.valid && ch.current_key_narrow() == 4, "precondition: stale at pk=4");

    let got = strip(&run_intersection(&delta, &mut ch));
    assert_eq!(got, want, "stale cursor must be reset by self-positioning");
}
