//! Sorted-stream co-group merge skeletons.
//!
//! Every place the engine merges two sorted OPK-byte streams — the delta-trace
//! joins, distinct, the natural-PK reduce reads, the delta-delta joins, and the
//! symmetric set-union merge — is one operation: **co-group two sorted byte-key
//! streams** by equal PK, hand each `(key, left-group, right-group)` to the
//! operator. These skeletons own that control flow; the operator callback owns
//! the row copy. The skip step galloping-seeks from the live position
//! (`SortedKeyStream::advance_to`), so K ascending probes over an N-row source
//! cost `O(K · log gap)`, not `O(K · log N)` or a linear `O(K + N)` scan.

use std::cmp::Ordering;
use std::ops::Range;

use crate::storage::{pk_bytes_eq, Batch, ReadCursor};

/// A sorted OPK-byte stream with a galloping forward skip. `ReadCursor`
/// implements it via `advance_to` (seeding each source at its live position); a
/// [`BatchCursor`] implements it via `Batch::advance_to` over the batch rows —
/// so the delta-delta joins co-group through the same skeleton as the
/// delta-trace joins. The skeletons own the control flow with just these two
/// operations; the per-group walk that reads weights/payloads is the operator
/// callback's concern, done through the concrete cursor's own accessors.
pub(crate) trait SortedKeyStream {
    /// Current row's OPK bytes; `None` when exhausted.
    fn key(&self) -> Option<&[u8]>;
    /// Galloping forward lower-bound skip to the first row with `key() >= key`.
    fn advance_to(&mut self, key: &[u8]);
}

impl SortedKeyStream for ReadCursor {
    #[inline]
    fn key(&self) -> Option<&[u8]> {
        if self.valid {
            Some(self.current_pk_bytes())
        } else {
            None
        }
    }
    #[inline]
    fn advance_to(&mut self, key: &[u8]) {
        ReadCursor::advance_to(self, key);
    }
}

/// A random-access cursor over a sorted `&Batch`, advanced by row index. Beyond
/// the [`SortedKeyStream`] skip it exposes its `pos` and equal-PK group extent
/// (`group_end`/`seek`), which the DD-join callbacks and [`cogroup_union`] use to
/// bracket and bulk-advance both sides.
pub(crate) struct BatchCursor<'a> {
    batch: &'a Batch,
    pub(crate) pos: usize,
}

impl<'a> BatchCursor<'a> {
    #[inline]
    pub(crate) fn new(batch: &'a Batch) -> Self {
        Self { batch, pos: 0 }
    }

    /// Jump to an absolute row index (used to skip past a bracketed group).
    #[inline]
    pub(crate) fn seek(&mut self, idx: usize) {
        self.pos = idx;
    }

    /// First row index past the equal-PK group beginning at `pos`. Requires
    /// `pos < batch.count`.
    #[inline]
    pub(crate) fn group_end(&self) -> usize {
        let k = self.batch.get_pk_bytes(self.pos);
        let mut j = self.pos + 1;
        while j < self.batch.count && pk_bytes_eq(self.batch.get_pk_bytes(j), k) {
            j += 1;
        }
        j
    }
}

impl SortedKeyStream for BatchCursor<'_> {
    #[inline]
    fn key(&self) -> Option<&[u8]> {
        if self.pos < self.batch.count {
            Some(self.batch.get_pk_bytes(self.pos))
        } else {
            None
        }
    }
    #[inline]
    fn advance_to(&mut self, key: &[u8]) {
        self.pos = self.batch.advance_to(key, self.pos);
    }
}

/// Intersection co-group: emit only at keys present on **both** sides. Both
/// pointers galloping-skip to catch up, so the cost is bounded by the smaller
/// side's matches plus galloping skips — optimal whichever side is larger,
/// replacing any size selector. Inner join, inner DD join.
///
/// **Self-positioning.** The first act is `m.advance_to(delta[0])`, so a shared
/// match cursor (a trace register reused by several ops in one epoch) is reset
/// to the co-group start from any prior position — `advance_to` is
/// backward-capable. This subsumes the old `rewind` + `seek_bytes(delta[0])`
/// the merge-walk paid; `cogroup_left` self-positions the same way on its first
/// group's `advance_to`.
///
/// **Callback contract.** `on_match` reads its match group by walking the
/// forward-only `m` with the concrete cursor's own step (`while m.key() ==
/// Some(key) { … step }`) and must walk the *whole* group to emit correctly. Loop
/// *progress* is robust either way: the skeleton re-establishes the match
/// position with `advance_to` at the next delta key (an under-walk forfeits rows
/// the callback should have read; an over-walk merely sends the following
/// `advance_to` down its bounded-backward branch, still correct).
#[inline]
pub(crate) fn cogroup_intersection<M: SortedKeyStream>(
    delta: &Batch,
    m: &mut M,
    mut on_match: impl FnMut(&[u8], Range<usize>, &mut M),
) {
    let n = delta.count;
    if n == 0 {
        return;
    }
    m.advance_to(delta.get_pk_bytes(0));
    let mut i = 0;
    while i < n {
        let Some(mk) = m.key() else { break };
        let dk = delta.get_pk_bytes(i);
        match dk.cmp(mk) {
            Ordering::Less => i = delta.advance_to(mk, i), // skip delta
            Ordering::Greater => m.advance_to(dk),         // skip match side
            Ordering::Equal => {
                let mut j = i + 1;
                while j < n && pk_bytes_eq(delta.get_pk_bytes(j), dk) {
                    j += 1;
                } // delta group
                on_match(dk, i..j, m); // walks match group
                i = j;
            }
        }
    }
}

/// Left co-group: visit **every** delta group (the match side galloping-skips to
/// it), because the operator emits per delta key whether or not the match group
/// exists. Outer join (null-fill on an empty group), anti join, distinct,
/// reduce, anti DD.
///
/// **Callback contract.** Same as [`cogroup_intersection`]: `on_group` walks the
/// forward-only `m` over the match group (possibly empty) and must walk the
/// whole group; loop progress is robust to under/over-walk.
#[inline]
pub(crate) fn cogroup_left<M: SortedKeyStream>(
    delta: &Batch,
    m: &mut M,
    mut on_group: impl FnMut(&[u8], Range<usize>, &mut M),
) {
    let n = delta.count;
    let mut i = 0;
    while i < n {
        let dk = delta.get_pk_bytes(i);
        let mut j = i + 1;
        while j < n && pk_bytes_eq(delta.get_pk_bytes(j), dk) {
            j += 1;
        }
        m.advance_to(dk); // galloping skip; group may be empty
        on_group(dk, i..j, m);
        i = j;
    }
}

/// Full-outer co-group: emit every key from **both** sides (the symmetric
/// set-union merge). Galloping-skips the lagging side to the other's key, so a
/// long single-source run is one `advance_to` (`O(log gap)`) + one bulk append
/// instead of a per-row crawl. At a shared key the skeleton brackets both
/// equal-PK groups and hands them to `on_shared` to interleave by payload. Both
/// sides are random-access batches, so the skeleton owns every advance: it
/// bulk-appends each single-source run into `out` itself (the trivial part), and
/// the callback only emits the payload-merged shared groups.
#[inline]
pub(crate) fn cogroup_union(
    a: &mut BatchCursor,
    b: &mut BatchCursor,
    out: &mut Batch,
    mut on_shared: impl FnMut(&mut Batch, Range<usize>, Range<usize>),
) {
    loop {
        match (a.key().is_some(), b.key().is_some()) {
            (true, true) => {
                // Both keys present: temporaries from key() drop at the end of
                // this `let`, freeing a/b for the per-arm mutation below.
                let ord = a.key().unwrap().cmp(b.key().unwrap());
                match ord {
                    Ordering::Less => {
                        let s = a.pos;
                        let k = b.key().unwrap();
                        a.advance_to(k);
                        out.append_batch(a.batch, s, a.pos);
                    }
                    Ordering::Greater => {
                        let s = b.pos;
                        let k = a.key().unwrap();
                        b.advance_to(k);
                        out.append_batch(b.batch, s, b.pos);
                    }
                    Ordering::Equal => {
                        let ga = a.group_end();
                        let gb = b.group_end();
                        on_shared(out, a.pos..ga, b.pos..gb);
                        a.seek(ga);
                        b.seek(gb);
                    }
                }
            }
            (true, false) => {
                out.append_batch(a.batch, a.pos, a.batch.count);
                return;
            }
            (false, true) => {
                out.append_batch(b.batch, b.pos, b.batch.count);
                return;
            }
            (false, false) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::{Batch, CursorHandle, Layout};
    use std::rc::Rc;

    fn schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    /// Build a sorted+consolidated batch of `(pk, weight, payload)` rows.
    fn make_batch(rows: &[(u64, i64, i64)]) -> Rc<Batch> {
        let s = schema();
        let mut b = Batch::with_schema(s, rows.len().max(1));
        for &(pk, w, v) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &v.to_le_bytes());
            b.count += 1;
        }
        b.certify_layout(Layout::Consolidated, &s);
        Rc::new(b)
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

    /// Concrete single-step for each match-stream impl, passed to the run
    /// helpers as a `fn` pointer (the trait owns only `key`/`advance_to`; the
    /// in-group walk is the consumer's concern).
    fn step_rc(m: &mut ReadCursor) {
        m.advance();
    }
    fn step_bc(m: &mut BatchCursor) {
        m.pos += 1;
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

    /// Run the intersection skeleton over `M`, recording each triple's key, delta
    /// range, and walked match PKs. `step` is the concrete single-step for `M`.
    fn run_intersection<M: SortedKeyStream>(delta: &Batch, m: &mut M, step: fn(&mut M)) -> Vec<Triple> {
        let mut out = Vec::new();
        cogroup_intersection(delta, m, |key, range, m| {
            let mut rows = Vec::new();
            while m.key() == Some(key) {
                rows.push((pk_of(m.key().unwrap()), 0));
                step(m);
            }
            out.push((pk_of(key), range, rows));
        });
        out
    }
    fn run_left<M: SortedKeyStream>(delta: &Batch, m: &mut M, step: fn(&mut M)) -> Vec<Triple> {
        let mut out = Vec::new();
        cogroup_left(delta, m, |key, range, m| {
            let mut rows = Vec::new();
            while m.key() == Some(key) {
                rows.push((pk_of(m.key().unwrap()), 0));
                step(m);
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
    /// and walked match PKs) for both M = BatchCursor and M = ReadCursor, over a
    /// range of shapes.
    #[test]
    fn intersection_matches_reference_all_shapes() {
        let s = schema();
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

            // M = BatchCursor
            let mut bc = BatchCursor::new(&mb);
            let got_bc = strip(&run_intersection(&delta, &mut bc, step_bc));
            assert_eq!(got_bc, want, "BatchCursor delta={di:?} match={mi:?}");

            // M = ReadCursor (single source)
            let mut ch = CursorHandle::from_owned(std::slice::from_ref(&mb), s);
            let got_rc = strip(&run_intersection(&delta, ch.cursor_mut(), step_rc));
            assert_eq!(got_rc, want, "ReadCursor delta={di:?} match={mi:?}");
        }
    }

    #[test]
    fn left_matches_reference_all_shapes() {
        let s = schema();
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

            let mut bc = BatchCursor::new(&mb);
            let got_bc = strip(&run_left(&delta, &mut bc, step_bc));
            assert_eq!(got_bc, want, "BatchCursor delta={di:?} match={mi:?}");

            let mut ch = CursorHandle::from_owned(std::slice::from_ref(&mb), s);
            let got_rc = strip(&run_left(&delta, ch.cursor_mut(), step_rc));
            assert_eq!(got_rc, want, "ReadCursor delta={di:?} match={mi:?}");
        }
    }

    /// A multi-source trace whose consolidation produces a ghost group (PK=3
    /// nets to weight 0 across two sources, so the merge must skip it). The
    /// skeleton must behave as if that key is absent — `advance_to`/`drive`
    /// land past the ghost, identically to a from-scratch seek.
    #[test]
    fn intersection_skips_ghost_group_multi_source() {
        let s = schema();
        // Source A: pk 1,3,5 ; Source B: pk 3 (negated) → pk=3 nets to 0.
        let src_a = make_batch(&[(1, 1, 10), (3, 1, 30), (5, 1, 50)]);
        let src_b = make_batch(&[(3, -1, 30)]);
        let delta = make_batch(&[(1, 1, 1), (3, 1, 3), (5, 1, 5)]);

        // Reference: a consolidated trace holding only pk 1 and 5 (3 is a ghost).
        let live = make_batch(&[(1, 1, 10), (5, 1, 50)]);
        let want = strip(&naive_intersection(&delta, &live));

        let mut ch = CursorHandle::from_owned(&[Rc::clone(&src_a), Rc::clone(&src_b)], s);
        let got = strip(&run_intersection(&delta, ch.cursor_mut(), step_rc));
        assert_eq!(got, want, "ghost group pk=3 must be skipped");
    }

    /// A pre-advanced (stale) shared match cursor must still produce the full
    /// intersection — the skeleton self-positions via `advance_to(delta[0])`,
    /// which is backward-capable. This is the shared-trace-register case (two
    /// inner joins on one trace in a LEFT JOIN epoch) without the old `rewind`.
    #[test]
    fn intersection_self_positions_stale_cursor() {
        let s = schema();
        let mb = make_batch(&[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40), (5, 1, 50)]);
        let delta = make_batch(&[(1, 1, 1), (3, 1, 3), (5, 1, 5)]);
        let want = strip(&naive_intersection(&delta, &mb));

        let mut ch = CursorHandle::from_owned(std::slice::from_ref(&mb), s);
        // Stale-advance the cursor past several keys before co-grouping.
        ch.cursor_mut().advance_to(&(4u128).to_be_bytes()[8..]);
        assert!(
            ch.cursor_mut().valid && ch.cursor_mut().current_key_narrow() == 4,
            "precondition: stale at pk=4"
        );

        let got = strip(&run_intersection(&delta, ch.cursor_mut(), step_rc));
        assert_eq!(got, want, "stale cursor must be reset by self-positioning");
    }

    /// `cogroup_union` emits **every** row from both sides exactly once: the
    /// reconstructed output multiset equals the concatenation of a's and b's
    /// rows. Covers disjoint, shared-key, skewed (tiny ∪ huge, exercising the
    /// gallop skip over a long run), all-equal, and one-empty-side shapes.
    #[test]
    fn union_emits_every_row_both_sides() {
        let cases: &[CoGroupCase] = &[
            (
                &[(1, 1, 10), (3, 1, 30), (5, 1, 50)],
                &[(2, 1, 20), (3, 1, 33), (4, 1, 40)],
            ),
            (&[(1, 1, 1)], &[(2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5), (6, 1, 6)]), // tiny ∪ huge
            (&[(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5)], &[(3, 1, 9)]), // huge ∪ tiny
            (&[(7, 1, 70), (7, 1, 71)], &[(7, 1, 72), (7, 1, 73)]),                   // all equal, multi-payload
            (&[(1, 1, 1), (2, 1, 2)], &[]),                                           // empty b
            (&[], &[(1, 1, 1), (2, 1, 2)]),                                           // empty a
        ];

        for (ai, bi) in cases {
            let a = make_batch(ai);
            let b = make_batch(bi);
            let mut ac = BatchCursor::new(&a);
            let mut bc = BatchCursor::new(&b);

            // Emit into a real output batch; the skeleton bulk-appends the
            // single-source runs, the callback appends both shared groups.
            let mut out = Batch::with_schema(schema(), (a.count + b.count).max(1));
            cogroup_union(&mut ac, &mut bc, &mut out, |o, ra, rb| {
                o.append_batch(&a, ra.start, ra.end);
                o.append_batch(&b, rb.start, rb.end);
            });

            let mut emitted: Vec<u64> = (0..out.count).map(|i| pk_of(out.get_pk_bytes(i))).collect();
            emitted.sort_unstable();

            let mut expected: Vec<u64> = (0..a.count)
                .map(|i| pk_of(a.get_pk_bytes(i)))
                .chain((0..b.count).map(|i| pk_of(b.get_pk_bytes(i))))
                .collect();
            expected.sort_unstable();

            assert_eq!(emitted, expected, "union a={ai:?} b={bi:?}");
        }
    }
}
