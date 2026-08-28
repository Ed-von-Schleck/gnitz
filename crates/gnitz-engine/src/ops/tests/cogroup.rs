use super::*;
use crate::test_support::make_schema_u64_i64;
use std::rc::Rc;

/// Build a sorted+consolidated batch of `(pk, weight, payload)` rows.
fn make_batch(rows: &[(u64, i64, i64)]) -> Rc<Batch> {
    Rc::new(crate::test_support::make_batch(&make_schema_u64_i64(), rows))
}

/// A recorded co-group triple: the key, the delta range, and the PKs of the
/// match-group rows the callback walked.
type Triple = (u64, Range<usize>, Vec<u64>);

/// A co-group test case: `(delta_rows, match_rows)`, each `(pk, weight, payload)`.
type CoGroupCase = (&'static [(u64, i64, i64)], &'static [(u64, i64, i64)]);

/// Naive reference: for every delta PK group emit `(pk, delta-range, match PKs)`.
/// `intersection` drops the groups the match batch has no row for; `left` keeps
/// them with an empty match list.
fn naive(delta: &Batch, m: &Batch, intersection: bool) -> Vec<Triple> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < delta.count {
        let dk = delta.get_pk_bytes(i).to_vec();
        let mut j = i + 1;
        while j < delta.count && delta.get_pk_bytes(j) == &dk[..] {
            j += 1;
        }
        let match_rows: Vec<u64> = (0..m.count)
            .filter(|&r| m.get_pk_bytes(r) == &dk[..])
            .map(|r| m.get_pk(r) as u64)
            .collect();
        if !intersection || !match_rows.is_empty() {
            out.push((delta.get_pk(i) as u64, i..j, match_rows));
        }
        i = j;
    }
    out
}

/// Run the skeleton `intersection` selects, recording each triple's key, delta
/// range, and walked match PKs.
fn run(delta: &Batch, m: &mut ReadCursor, intersection: bool) -> Vec<Triple> {
    let mut out = Vec::new();
    {
        let record = |key: &[u8], range: Range<usize>, m: &mut ReadCursor| {
            let mut rows = Vec::new();
            while m.valid && m.current_pk_eq(key) {
                rows.push(m.current_key_narrow() as u64);
                m.advance();
            }
            out.push((delta.get_pk(range.start) as u64, range, rows));
        };
        if intersection {
            cogroup_intersection(delta, m, record);
        } else {
            cogroup_left(delta, m, record);
        }
    }
    out
}

/// Both skeletons' triples must match the naive reference (keys, delta ranges,
/// and walked match PKs) over a range of shapes: empty on each side, disjoint
/// keys, fully shared keys, duplicate keys on each side, and a large size skew
/// in each direction.
#[test]
fn both_skeletons_match_the_reference_over_every_shape() {
    let s = make_schema_u64_i64();
    let cases: &[CoGroupCase] = &[
        (&[], &[(1, 1, 10)]),
        (&[(1, 1, 10)], &[]),
        (&[(1, 1, 10), (3, 1, 30)], &[(2, 1, 20), (4, 1, 40)]),
        (&[(1, 1, 10), (2, 1, 20)], &[(1, 1, 11), (2, 1, 22)]),
        // multiset delta against a multi-payload match group
        (
            &[(1, 1, 10), (1, 1, 11), (5, 1, 50)],
            &[(1, 1, 90), (1, 1, 91), (1, 1, 92), (5, 1, 55)],
        ),
        // huge delta, tiny match
        (
            &[(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5), (6, 1, 6)],
            &[(4, 1, 44)],
        ),
        // tiny delta, huge match
        (
            &[(4, 1, 4)],
            &[(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 44), (5, 1, 5), (6, 1, 6)],
        ),
    ];

    for intersection in [true, false] {
        for (di, mi) in cases {
            let delta = make_batch(di);
            let mb = make_batch(mi);
            let mut ch = ReadCursor::over_batches(std::slice::from_ref(&mb), s);
            assert_eq!(
                run(&delta, &mut ch, intersection),
                naive(&delta, &mb, intersection),
                "intersection={intersection} delta={di:?} match={mi:?}",
            );
        }
    }
}

/// A multi-source trace whose consolidation produces a ghost group (PK=3 nets
/// to weight 0 across two sources, so the merge skips it). The skeleton must
/// behave as if that key is absent — `advance_to` lands past the ghost,
/// identically to a from-scratch seek.
#[test]
fn intersection_skips_ghost_group_multi_source() {
    let s = make_schema_u64_i64();
    let src_a = make_batch(&[(1, 1, 10), (3, 1, 30), (5, 1, 50)]);
    let src_b = make_batch(&[(3, -1, 30)]);
    let delta = make_batch(&[(1, 1, 1), (3, 1, 3), (5, 1, 5)]);

    // Reference: a trace holding only pk 1 and 5.
    let live = make_batch(&[(1, 1, 10), (5, 1, 50)]);

    let mut ch = ReadCursor::over_batches(&[Rc::clone(&src_a), Rc::clone(&src_b)], s);
    assert_eq!(
        run(&delta, &mut ch, true),
        naive(&delta, &live, true),
        "ghost group pk=3 must be skipped",
    );
}

/// A pre-advanced match cursor must still produce the full intersection: the
/// skeleton self-positions via `advance_to(delta[0])`, which is
/// backward-capable. This is the shape a trace register shared by two ops in
/// one epoch takes.
#[test]
fn intersection_self_positions_stale_cursor() {
    let s = make_schema_u64_i64();
    let mb = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50)]);
    let delta = make_batch(&[(1, 1, 1), (3, 1, 3), (5, 1, 5)]);

    let mut ch = ReadCursor::over_batches(std::slice::from_ref(&mb), s);
    ch.advance_to(&(4u128).to_be_bytes()[8..]);
    assert!(ch.valid && ch.current_key_narrow() == 4, "precondition: stale at pk=4");

    assert_eq!(
        run(&delta, &mut ch, true),
        naive(&delta, &mb, true),
        "stale cursor must be reset by self-positioning",
    );
}
