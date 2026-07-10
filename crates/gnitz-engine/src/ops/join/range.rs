//! Non-equi (range / band) join delta-trace.

use crate::schema::SchemaDescriptor;
use crate::storage::{pk_bytes_eq, range_cut_points, Batch, MemBatch, ReadCursor};
use gnitz_wire::RangeRel;

use super::rowwrite::write_join_row;

// ---------------------------------------------------------------------------
// Non-equi (range) join delta-trace
// ---------------------------------------------------------------------------

/// Range join delta-trace: like `op_join_delta_trace`, but each consolidated
/// delta row matches the trace by an ordered half-open `[start, end)` range
/// (the §3 cut-point table) instead of an equal-key seek. `n_eq` leading PK
/// slots are equality-pinned (the band-join prefix), `rel` is the relation the
/// trace slot must satisfy versus the delta slot.
///
/// Both sides reindex to `[eq slots…, range slot]` at the pair's common promoted
/// type, so the trace PK region is an ordered arrangement by the range key and
/// the probe bound is the delta row's own packed PK bytes — no decode/re-encode.
///
/// Size-adaptive. Unlike the equi-join (now a single symmetric co-group), the
/// two range strategies are structurally distinct algorithms — not two spellings
/// of one merge — so the selector is load-bearing, not redundant: rewind so
/// `estimated_length` sees the full trace, then drive from whichever side is
/// smaller. When `|delta| ≤ |trace|` keep the delta-driven per-row probe
/// (`range_per_row_seek`); when the delta outnumbers the trace, sweep the trace
/// forward exactly once with a monotone delta pointer (`range_merge_walk`),
/// eliminating the per-row re-seek and the shared-region re-advance. Both emit
/// the identical match multiset, unsorted, re-ordered downstream.
pub fn op_join_delta_trace_range(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
    n_eq: usize,
    rel: RangeRel,
) -> Batch {
    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_ref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty_joined(left_schema, right_schema);
    }

    // Trace PK = [eq slots…, range slot]. Both sides pack at the same common type,
    // so the delta PK shape equals the trace PK shape.
    debug_assert_eq!(
        left_schema.pk_stride(),
        right_schema.pk_stride(),
        "range join: delta and trace PK strides must match (common-T reindex)"
    );
    debug_assert_eq!(
        right_schema.pk_indices().len(),
        n_eq + 1,
        "range join: trace PK arity must be n_eq + 1"
    );
    // (`range_cut_points` asserts the range slot is non-empty per row.)

    // The same trace register may be reused across a tick; reset to position 0 so
    // `estimated_length` sees the full trace and both strategies' first
    // `advance_to` galloping-seeks forward from the head.
    cursor.rewind();
    let trace_len = cursor.estimated_length();
    if n > trace_len {
        range_merge_walk(consolidated, cursor, left_schema, right_schema, n_eq, rel)
    } else {
        range_per_row_seek(consolidated, cursor, left_schema, right_schema, n_eq, rel)
    }
}

/// Strategy 1 — delta-driven per-row probe (`|delta| ≤ |trace|`). For each
/// consolidated delta row, derive its `[start, end)` cut interval and walk the
/// matching trace run. The start cut is globally monotone non-decreasing across
/// rows (except `Lt`/`Le` at `n_eq == 0`, where every row seeks the minimum), so
/// the hint-seeded `advance_to` galloping-skips forward across the monotone runs
/// and falls back to a bounded backward search on the rare reset — same matches.
fn range_per_row_seek(
    consolidated: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
    n_eq: usize,
    rel: RangeRel,
) -> Batch {
    let eq_size = right_schema.leading_key_size(n_eq);
    let n = consolidated.count;

    let delta_mb = consolidated.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    for i in 0..n {
        let pk_i = consolidated.get_pk_bytes(i);
        let w_delta = consolidated.get_weight(i);
        let (eq, d) = pk_i.split_at(eq_size);

        // Provably-empty interval (e.g. `Gt` of the maximal slot): no match.
        let Some((start, end)) = range_cut_points(eq, d, rel) else {
            continue;
        };

        // Galloping forward skip seeded at the live position; falls back to a
        // bounded backward search on the `Lt`/`Le` `n_eq == 0` reset (all rows
        // seek 0x00…), so it stays correct for that non-monotone case too.
        cursor.advance_to(start.as_slice());
        while cursor.valid
            && end
                .as_ref()
                .is_none_or(|e| cursor.current_pk_cmp_bytes(e.as_slice()).is_lt())
        {
            let w_out = w_delta.wrapping_mul(cursor.current_weight);
            if w_out != 0 {
                write_join_row(&mut output, &delta_mb, i, cursor, w_out, left_schema, right_schema);
            }
            cursor.advance();
        }
    }

    // Probe order is delta-major with arbitrary trace payload runs per row — not
    // (PK, payload)-sorted, and the cartesian emission is unfolded. `empty_joined`
    // leaves both flags clear so no consumer trusts it as either; the re-key +
    // output exchange + consolidation downstream restore order.
    output
}

/// Strategy 2 — trace-driven eq-group merge walk (`|delta| > |trace|`). Per delta
/// eq-group, a single `range_cut_points` cut on the group's extreme slot gives the
/// one `[start, end)` span covering the whole group; seek to `start` and sweep only
/// that span with a monotone delta pointer, emitting the matching contiguous delta
/// sub-range per trace row.
/// The seek skips untouched trace groups *and* the intra-group dead head/tail, so
/// every walked trace row matches some delta row — none is walked redundantly.
/// Structurally a sort-merge band join: `O(g·log r + covered + m + output)`,
/// `g` = #delta groups, `covered` = matched trace rows.
fn range_merge_walk(
    consolidated: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
    n_eq: usize,
    rel: RangeRel,
) -> Batch {
    let eq_size = right_schema.leading_key_size(n_eq);
    // `MemBatch::get_pk_bytes` returns `&'a` tied to the batch data (not a `&self`
    // borrow), so `e` is held freely across grouping while `delta_mb` is read again.
    let delta_mb = consolidated.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);
    let m = consolidated.count;

    let mut lo = 0; // start of the current delta eq group
    while lo < m {
        // Delta eq group [lo, hi): contiguous rows sharing the eq prefix E, range
        // slots ascending in [d_lo, d_hi].
        let e = &delta_mb.get_pk_bytes(lo)[..eq_size];
        let mut hi = lo + 1;
        while hi < m && pk_bytes_eq(&delta_mb.get_pk_bytes(hi)[..eq_size], e) {
            hi += 1;
        }

        // One cut spanning the whole eq-group. The union of the group's per-row
        // intervals collapses to a single range_cut_points cut on the group's extreme
        // value: d_hi for Lt/Le (the open `end` grows with d), d_lo for Gt/Ge (the
        // open `start` shrinks with d). `None` ⇒ provably-empty group (e.g. Gt of an
        // all-maximal slot) — skip it.
        let d_lo = &delta_mb.get_pk_bytes(lo)[eq_size..];
        let d_hi = &delta_mb.get_pk_bytes(hi - 1)[eq_size..];
        let d = match rel {
            RangeRel::Lt | RangeRel::Le => d_hi,
            RangeRel::Gt | RangeRel::Ge => d_lo,
        };
        let Some((start, end)) = range_cut_points(e, d, rel) else {
            lo = hi;
            continue;
        };

        // Galloping forward skip to the covered start: the targets ascend across
        // groups, so `advance_to` seeded at the live position skips untouched
        // trace groups and the intra-group dead head in one hop.
        cursor.advance_to(start.as_slice());
        let mut ptr = lo; // monotone delta pointer
        while cursor.valid {
            // Bind the PK once: both the span-end bound and the range-slot read
            // use it, so the row is fetched from the cursor a single time. Every
            // key in [start, end) carries eq prefix E, so `end` alone delimits
            // the group — no per-row eq check needed.
            let pk = cursor.current_pk_bytes();
            if end.as_ref().is_some_and(|e2| pk >= e2.as_slice()) {
                break; // reached the covered-span end (the dead tail follows)
            }
            let s = &pk[eq_size..]; // trace range slot (PK is exactly stride bytes)
            let w_t = cursor.current_weight;
            // Advance `ptr` and select the matching contiguous delta sub-range.
            // The `pk`/`s` cursor borrow dies in the pointer advance — before the
            // emission reborrows the cursor and the trailing `advance()`.
            let (rs, re) = match rel {
                // prefix [lo, ub): grows as s↑
                RangeRel::Gt => {
                    advance_delta_ptr(&delta_mb, eq_size, &mut ptr, hi, |d| d < s);
                    (lo, ptr)
                }
                RangeRel::Ge => {
                    advance_delta_ptr(&delta_mb, eq_size, &mut ptr, hi, |d| d <= s);
                    (lo, ptr)
                }
                // suffix [lb, hi): its lower bound rises (shrinks) as s↑
                RangeRel::Lt => {
                    advance_delta_ptr(&delta_mb, eq_size, &mut ptr, hi, |d| d <= s);
                    (ptr, hi)
                }
                RangeRel::Le => {
                    advance_delta_ptr(&delta_mb, eq_size, &mut ptr, hi, |d| d < s);
                    (ptr, hi)
                }
            };
            for k in rs..re {
                let w_out = delta_mb.get_weight(k).wrapping_mul(w_t);
                if w_out != 0 {
                    write_join_row(&mut output, &delta_mb, k, cursor, w_out, left_schema, right_schema);
                }
            }
            cursor.advance();
        }
        lo = hi;
    }

    // Trace-major with delta runs per trace row — not (PK, payload)-sorted and
    // unfolded; `empty_joined` leaves both flags clear, downstream re-sorts and
    // consolidates.
    output
}

/// Advance the monotone delta pointer `*ptr` forward over the eq group `[*, hi)`
/// while the range slot at `*ptr` satisfies `pred` against the current trace
/// slot. Serves as both the prefix upper bound (`Gt`/`Ge`) and the suffix lower
/// bound (`Lt`/`Le`): as the trace slot ascends the prefix end and the suffix
/// start each move forward only, so starting at the group head and advancing is
/// exact and amortizes to `O(m_group)` across the whole trace group. Compares the
/// delta row's range slot by `memcmp` (OPK order = typed order).
#[inline]
fn advance_delta_ptr(delta_mb: &MemBatch, eq_size: usize, ptr: &mut usize, hi: usize, pred: impl Fn(&[u8]) -> bool) {
    while *ptr < hi && pred(&delta_mb.get_pk_bytes(*ptr)[eq_size..]) {
        *ptr += 1;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::test_common::*;
    use super::*;
    use crate::foundation::codec::read_i64_le;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::{Batch, Layout};

    /// All four rels over an unsigned key, n_eq = 0. The boundary case x == y
    /// must match `Le`/`Ge` and not `Lt`/`Gt`. The trace payload tags the rows
    /// {y=10→110, y=20→120, y=30→130}; the delta probes x = 20.
    #[test]
    fn test_range_dt_four_rels_unsigned() {
        let schema = make_schema_u64_i64();
        let cases = [
            (RangeRel::Lt, vec![110]),      // y < 20
            (RangeRel::Le, vec![110, 120]), // y <= 20
            (RangeRel::Gt, vec![130]),      // y > 20
            (RangeRel::Ge, vec![120, 130]), // y >= 20
        ];
        for (rel, want) in cases {
            let mut ch = trace_cursor(make_batch(&schema, &[(10, 1, 110), (20, 1, 120), (30, 1, 130)]), schema);
            let delta = make_batch(&schema, &[(20, 1, 200)]);
            let out = op_join_delta_trace_range(&delta, &mut ch, &schema, &schema, 0, rel);
            let got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
            assert_eq!(got, want, "rel {rel:?}");
            // The left PK (delta range key x) and left payload survive on every row.
            for r in 0..out.count {
                assert_eq!(out.get_pk(r) as u64, 20);
                assert_eq!(read_i64_le(out.col_data(0), r * 8), 200);
            }
        }
    }

    /// Band join (n_eq = 1): the probe stays inside the delta row's equality
    /// group. A trace row in the NEXT eq group with an in-range slot must NOT
    /// match.
    #[test]
    fn test_range_dt_eq_prefix_stops_at_group_edge() {
        let schema = make_schema_compound(); // (U64 k, U64 range) PK, I64 payload
                                             // Trace sorted by (k, range): k=1 group then k=2 group. k=2,y=5 has an
                                             // in-range slot for x=15 but is in a different group → must not match.
        let trace = make_band_batch(&schema, &[(1, 10, 1, 110), (1, 20, 1, 120), (2, 5, 1, 205)]);
        let mut ch = trace_cursor(trace, schema);
        let delta = make_band_batch(&schema, &[(1, 15, 1, 200)]);
        // rel Lt: {y < 15 within k=1} = {10}. k=2,y=5 excluded by the group edge.
        let out = op_join_delta_trace_range(&delta, &mut ch, &schema, &schema, 1, RangeRel::Lt);
        let got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
        assert_eq!(got, vec![110]);
    }

    /// Band join, `Le` with a maximal range slot (`d = u64::MAX`): the end cut's
    /// carry ripples into the eq prefix (next group's first key). The whole k=1
    /// group matches and the k=2 group does not — no panic, no spill.
    #[test]
    fn test_range_dt_eq_prefix_ripple_le_max() {
        let schema = make_schema_compound();
        let trace = make_band_batch(&schema, &[(1, 0, 1, 100), (1, u64::MAX, 1, 199), (2, 0, 1, 200)]);
        let mut ch = trace_cursor(trace, schema);
        let delta = make_band_batch(&schema, &[(1, u64::MAX, 1, 9)]);
        // y <= u64::MAX within k=1 → both k=1 rows; k=2 excluded.
        let out = op_join_delta_trace_range(&delta, &mut ch, &schema, &schema, 1, RangeRel::Le);
        let mut got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
        got.sort_unstable();
        assert_eq!(got, vec![100, 199]);
    }

    /// `n_eq = 0`, `rel = Lt`, multiple delta rows: every row's start cut is the
    /// all-`0x00` lower bound, so each probe re-seeks the cursor *backward* from
    /// where the previous row's walk ended. All matches must still be found.
    #[test]
    fn test_range_dt_lt_backward_reseek() {
        let schema = make_schema_u64_i64();
        let mut ch = trace_cursor(make_batch(&schema, &[(5, 1, 105), (15, 1, 115), (25, 1, 125)]), schema);
        // Two delta rows in ascending (PK) order (10, 30). Processing 30 after 10
        // forces a backward seek to 0x00 for the second probe.
        let delta = make_batch(&schema, &[(10, 1, 100), (30, 1, 300)]);
        let out = op_join_delta_trace_range(&delta, &mut ch, &schema, &schema, 0, RangeRel::Lt);
        // x=10 → {y<10}={105}; x=30 → {y<30}={105,115,125}. 4 rows total.
        let mut pairs: Vec<(u64, i64)> = (0..out.count)
            .map(|r| (out.get_pk(r) as u64, read_i64_le(out.col_data(1), r * 8)))
            .collect();
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(10, 105), (30, 105), (30, 115), (30, 125)]);
    }

    /// Signed range pair via cross-sign promotion (both sides reindex to I64).
    /// Negative trace keys must order below positives — guards an OPK sign-flip
    /// regression at the probe layer (raw bytes would put -100 above +50).
    #[test]
    fn test_range_dt_signed_promoted_key() {
        let schema = make_schema_signed(); // I64 PK, I64 payload
        let trace = make_signed_batch(&schema, &[(-100, 1, 1), (0, 1, 2), (50, 1, 3)]);
        let mut ch = trace_cursor(trace, schema);
        let delta = make_signed_batch(&schema, &[(0, 1, 9)]);
        // Gt: {y > 0} = {50→3}. Lt: {y < 0} = {-100→1}.
        let mut ch2 = trace_cursor(
            make_signed_batch(&schema, &[(-100, 1, 1), (0, 1, 2), (50, 1, 3)]),
            schema,
        );
        let out_gt = op_join_delta_trace_range(&delta, &mut ch, &schema, &schema, 0, RangeRel::Gt);
        let out_lt = op_join_delta_trace_range(&delta, &mut ch2, &schema, &schema, 0, RangeRel::Lt);
        assert_eq!(
            range_out_pairs(&out_gt).into_iter().map(|(p, _)| p).collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(
            range_out_pairs(&out_lt).into_iter().map(|(p, _)| p).collect::<Vec<_>>(),
            vec![1]
        );
    }

    /// Output weight = `w_delta × w_trace` (wrapping); a retraction delta negates,
    /// a doubled trace doubles, and a zero-weight trace row is skipped.
    #[test]
    fn test_range_dt_weights() {
        let schema = make_schema_u64_i64();
        // Trace: y=10 (w=2), y=20 (w=0, must be skipped), y=30 (w=1).
        let mut trace = Batch::with_schema(schema, 3);
        for &(pk, w, val) in &[(10u64, 2i64, 110i64), (20, 0, 120), (30, 1, 130)] {
            trace.extend_pk(pk as u128);
            trace.extend_weight(&w.to_le_bytes());
            trace.extend_null_bmp(&0u64.to_le_bytes());
            trace.extend_col(0, &val.to_le_bytes());
            trace.count += 1;
        }
        trace.set_layout_unchecked(Layout::Consolidated);
        let mut ch = trace_cursor(trace, schema);
        // Delta retraction: x=40, w=-1, so all matches negate.
        let delta = make_batch(&schema, &[(40, -1, 400)]);
        // rel Lt: {y < 40} = all three, but y=20's product is -1*0 = 0 (skipped).
        let out = op_join_delta_trace_range(&delta, &mut ch, &schema, &schema, 0, RangeRel::Lt);
        let mut got = range_out_pairs(&out);
        got.sort_unstable();
        // y=10: -1*2 = -2; y=30: -1*1 = -1; y=20 skipped.
        assert_eq!(got, vec![(110, -2), (130, -1)]);
    }

    /// `Gt` of the maximal slot (n_eq = 0, x = u64::MAX) is provably empty — the
    /// cut helper returns `None` and the probe emits nothing without seeking.
    #[test]
    fn test_range_dt_gt_max_is_empty() {
        let schema = make_schema_u64_i64();
        let mut ch = trace_cursor(make_batch(&schema, &[(10, 1, 110)]), schema);
        let delta = make_batch(&schema, &[(u64::MAX, 1, 1)]);
        let out = op_join_delta_trace_range(&delta, &mut ch, &schema, &schema, 0, RangeRel::Gt);
        assert_eq!(out.count, 0);
    }

    /// Empty delta → empty output, no panic.
    #[test]
    fn test_range_dt_empty_delta() {
        let schema = make_schema_u64_i64();
        let mut ch = trace_cursor(make_batch(&schema, &[(10, 1, 110)]), schema);
        let delta = make_batch(&schema, &[]);
        let out = op_join_delta_trace_range(&delta, &mut ch, &schema, &schema, 0, RangeRel::Le);
        assert_eq!(out.count, 0);
    }

    // -----------------------------------------------------------------------
    // Range join DT: Strategy 2 (merge walk) vs Strategy 1 (per-row) oracle
    // -----------------------------------------------------------------------

    use crate::test_rng::Rng;

    /// Differential: the merge walk matches the per-row oracle over random
    /// delta + trace for all four rels and `n_eq ∈ {0, 1, 2}`. The small key
    /// space exercises dense groups, boundary equality, tombstones (trace weight
    /// 0/≤0), retractions (delta weight < 0), multiset duplicates, empty trace,
    /// and non-matching groups in one sweep.
    #[test]
    fn test_range_dt_merge_vs_per_row_differential() {
        let rels = [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge];
        let mut rng = Rng::new(0xC0FF_EE12_3456_789A);
        let mut total = 0usize;
        for n_eq in 0..=2usize {
            let schema = make_range_schema(n_eq);
            for &rel in &rels {
                for _ in 0..40 {
                    let n_delta = 1 + rng.gen_range(7) as usize;
                    let n_trace = rng.gen_range(8) as usize;
                    let delta_rows = gen_range_rows(&mut rng, n_eq, n_delta, 4);
                    let trace_rows = gen_range_rows(&mut rng, n_eq, n_trace, 4);
                    let delta = make_range_batch(&schema, &delta_rows);
                    let trace = make_range_batch(&schema, &trace_rows);
                    total += assert_merge_eq_per_row(schema, n_eq, rel, &delta, trace);
                }
            }
        }
        assert!(total > 0, "differential never emitted a row — inputs too sparse");
    }

    /// `d == s` boundary: `Le`/`Ge` include the equal trace slot, `Lt`/`Gt`
    /// exclude it. Merge must split it exactly where the per-row cut does.
    #[test]
    fn test_range_dt_merge_boundary_equality() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(20, 1, 200)]);
        for rel in [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge] {
            let trace = make_batch(&schema, &[(10, 1, 110), (20, 1, 120), (30, 1, 130)]);
            assert_merge_eq_per_row(schema, 0, rel, &delta, trace);
        }
    }

    /// Maximal range slot, `n_eq = 0`: `Gt` of `u64::MAX` is provably empty in
    /// the per-row path (`range_cut_points → None`); the merge must agree (the
    /// maximal delta row is never `< s`, so the prefix excludes it). All rels.
    #[test]
    fn test_range_dt_merge_maximal_slot_n_eq0() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(u64::MAX, 1, 9)]);
        for rel in [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge] {
            let trace = make_batch(&schema, &[(0, 1, 100), (50, 1, 150), (u64::MAX, 1, 199)]);
            assert_merge_eq_per_row(schema, 0, rel, &delta, trace);
        }
    }

    /// Maximal slot in a non-maximal eq group (`n_eq = 1`): `Gt`/`Ge` of
    /// `u64::MAX` in group k=1 yield a zero-width `(next_E‖0, next_E‖0)` cut — no
    /// match, and crucially no spill into the k=2 group. Merge must agree.
    #[test]
    fn test_range_dt_merge_maximal_slot_n_eq1() {
        let schema = make_schema_compound(); // (U64 k, U64 range), I64 payload
        let delta = make_band_batch(&schema, &[(1, u64::MAX, 1, 9)]);
        for rel in [RangeRel::Gt, RangeRel::Ge, RangeRel::Lt, RangeRel::Le] {
            let trace = make_band_batch(&schema, &[(1, 0, 1, 100), (1, 50, 1, 150), (2, 0, 1, 200)]);
            assert_merge_eq_per_row(schema, 1, rel, &delta, trace);
        }
    }

    /// Mid-group tombstone: a trace row with weight ≤ 0 must still advance the
    /// delta pointer (it participates in the monotone walk) yet emit nothing
    /// (its product is net-zero / suppressed). Covers a zero and a negative
    /// trace weight against the per-row oracle.
    #[test]
    fn test_range_dt_merge_tombstone_advances_pointer() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(40, -1, 400)]); // retraction
        for rel in [RangeRel::Lt, RangeRel::Le] {
            // y=20 is a tombstone (w=0), y=25 carries a negative weight. A trace
            // carrying a weight-0 row is sorted but not consolidated, so it is
            // flagged `Sorted` (consolidation would have dropped the ghost).
            let trace = make_sorted_u64_batch(&schema, &[(10, 1, 110), (20, 0, 120), (25, -1, 125), (30, 1, 130)]);
            assert_merge_eq_per_row(schema, 0, rel, &delta, trace);
        }
    }

    /// High fan-out (`output ≫ r`), both pointer directions: every delta row
    /// matches most of its trace group. The suffix rel (`Lt`) over large `d` and
    /// the prefix rel (`Gt`) over small `d` each drive the monotone pointer the
    /// full width of the group; output must equal the oracle.
    #[test]
    fn test_range_dt_merge_high_fanout_both_directions() {
        let schema = make_schema_u64_i64();
        let trace_rows: Vec<(u64, i64, i64)> = (0..8u64).map(|y| (y, 1, 100 + y as i64)).collect();
        // suffix rel: large delta keys → each matches the whole low prefix.
        let delta_lt = make_batch(&schema, &[(5, 1, 1), (6, 1, 2), (7, 1, 3)]);
        let n1 = assert_merge_eq_per_row(schema, 0, RangeRel::Lt, &delta_lt, make_batch(&schema, &trace_rows));
        // prefix rel: small delta keys → each matches the whole high suffix.
        let delta_gt = make_batch(&schema, &[(0, 1, 1), (1, 1, 2), (2, 1, 3)]);
        let n2 = assert_merge_eq_per_row(schema, 0, RangeRel::Gt, &delta_gt, make_batch(&schema, &trace_rows));
        assert!(n1 > 8 && n2 > 8, "high fan-out should emit ≫ |trace| rows: {n1}, {n2}");
    }

    /// Multiset delta + multi-payload trace: duplicate `[eq‖d]` on both sides
    /// (distinct payloads). The merge must emit the full cross-product per trace
    /// row, weights multiplied — same as the per-row oracle.
    #[test]
    fn test_range_dt_merge_multiset_multipayload() {
        let schema = make_schema_u64_i64();
        // Trace PK=10 twice (distinct payloads); delta PK=15 twice.
        let trace = make_batch(&schema, &[(10, 1, 101), (10, 2, 102), (20, 1, 200)]);
        let delta = make_batch(&schema, &[(15, 1, 1), (15, 3, 2)]);
        // Lt: y < 15 → both PK=10 rows; each delta row matches both → 4 rows.
        assert_merge_eq_per_row(schema, 0, RangeRel::Lt, &delta, trace);
    }

    /// Seek-anchoring elides dead trace regions: the delta's covered span is a
    /// narrow slice of a large trace. `Gt`/`Ge` seek past the dead low head (to
    /// `succ(min_d)`); `Lt`/`Le` stop before the dead high tail (at `max_d`). Both
    /// must match the per-row oracle over the same large trace.
    #[test]
    fn test_range_dt_merge_seek_anchored_elides_dead_region() {
        let schema = make_schema_u64_i64();
        let trace_rows: Vec<(u64, i64, i64)> = (0..30u64).map(|y| (y, 1, 100 + y as i64)).collect();
        // Gt over a large min: covered = (25, 30); the 0..=25 head is seek-skipped.
        let delta_gt = make_batch(&schema, &[(25, 1, 1), (26, 1, 2)]);
        assert_merge_eq_per_row(schema, 0, RangeRel::Gt, &delta_gt, make_batch(&schema, &trace_rows));
        // Lt over a small max: covered = [0, 4); the 4..30 tail is early-stopped.
        let delta_lt = make_batch(&schema, &[(3, 1, 1), (4, 1, 2)]);
        assert_merge_eq_per_row(schema, 0, RangeRel::Lt, &delta_lt, make_batch(&schema, &trace_rows));
    }

    /// `n_eq = 0`, `Lt`/`Le`: one eq group spanning the whole trace, walked once.
    /// The degenerate single-group case the pure-range merge targets.
    #[test]
    fn test_range_dt_merge_n_eq0_single_walk() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(15, 1, 1), (25, 1, 2), (35, 1, 3)]);
        for rel in [RangeRel::Lt, RangeRel::Le] {
            let trace = make_batch(&schema, &[(10, 1, 110), (20, 1, 120), (30, 1, 130)]);
            assert_merge_eq_per_row(schema, 0, rel, &delta, trace);
        }
    }

    /// Empty trace with a non-empty delta: the merge (which the `n > trace_len`
    /// selector picks when `trace_len == 0`) walks zero trace rows and emits
    /// nothing — same as the per-row path landing every seek on an invalid cursor.
    #[test]
    fn test_range_dt_merge_empty_trace() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(10, 1, 1), (20, 1, 2)]);
        // Through the public selector: n=2 > trace_len=0 → merge path, empty out.
        let mut ch = trace_cursor(make_batch(&schema, &[]), schema);
        let out = op_join_delta_trace_range(&delta, &mut ch, &schema, &schema, 0, RangeRel::Le);
        assert_eq!(out.count, 0);
    }

    /// Band join with non-matching groups (`n_eq = 1`): a trace eq group with no
    /// delta group (merge forward-skips it) and a delta eq group with no trace
    /// group (emits nothing). Only the shared group k=3 produces output.
    #[test]
    fn test_range_dt_merge_non_matching_groups() {
        let schema = make_schema_compound();
        // Trace groups k=1, k=3; delta groups k=2 (no trace), k=3 (matches).
        let delta = make_band_batch(&schema, &[(2, 7, 1, 2), (3, 7, 1, 3)]);
        for rel in [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge] {
            let trace = make_band_batch(&schema, &[(1, 5, 1, 15), (3, 5, 1, 35), (3, 9, 1, 39)]);
            assert_merge_eq_per_row(schema, 1, rel, &delta, trace);
        }
    }

    /// The public selector routes by size: an oversized delta (`n > trace_len`)
    /// takes the merge, a small delta the per-row path — both equal the per-row
    /// oracle. Pins the `n > trace_len` boundary end to end.
    #[test]
    fn test_range_dt_selector_routes_by_size() {
        let schema = make_schema_u64_i64();
        let trace_rows: Vec<(u64, i64, i64)> = (0..4u64).map(|y| (y * 10, 1, 100 + y as i64)).collect();
        // Oversized delta (8 > 4) → merge; small delta (2 ≤ 4) → per-row.
        for delta_rows in [
            vec![
                (5u64, 1i64, 0i64),
                (6, 1, 0),
                (7, 1, 0),
                (8, 1, 0),
                (9, 1, 0),
                (11, 1, 0),
                (12, 1, 0),
                (13, 1, 0),
            ],
            vec![(15, 1, 0), (25, 1, 0)],
        ] {
            let delta = make_batch(&schema, &delta_rows);
            // Oracle: force the per-row path directly.
            let mut ch_oracle = trace_cursor(make_batch(&schema, &trace_rows), schema);
            let oracle = range_per_row_seek(&delta, &mut ch_oracle, &schema, &schema, 0, RangeRel::Lt);
            // Subject: the public selector.
            let mut ch = trace_cursor(make_batch(&schema, &trace_rows), schema);
            let out = op_join_delta_trace_range(&delta, &mut ch, &schema, &schema, 0, RangeRel::Lt);
            assert_eq!(range_multiset(&oracle), range_multiset(&out));
        }
    }

    // -----------------------------------------------------------------------
    // Band/range-join test fixtures (OPK-ordered batches)
    // -----------------------------------------------------------------------

    /// Build a band-join trace/delta over the `(U64 k, U64 range)` compound PK
    /// using OPK (big-endian) encoding, so memcmp on the PK region is numeric
    /// order — the contract the range probe relies on (an order-blind native-LE
    /// encoding would seek wrong). Rows must be passed in ascending `(k, range)`
    /// order. Range-join inputs need only be (PK, payload)-sorted: the walk handles
    /// tombstones (weight 0) and multiset duplicates explicitly, so the rows are
    /// flagged `Sorted`, not `Consolidated` — the contract the data actually meets.
    fn make_band_batch(schema: &SchemaDescriptor, rows: &[(u64, u64, i64, i64)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(k, range, w, val) in rows {
            b.extend_pk_opk(schema, &[k as u128, range as u128]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.certify_layout(Layout::Sorted, schema);
        b
    }

    /// `make_schema_u64_i64` batch flagged `Sorted` (not `Consolidated`): for a
    /// trace that deliberately carries a weight-0 tombstone, which a consolidated
    /// batch may not contain. Rows must be (PK, payload)-sorted.
    fn make_sorted_u64_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.certify_layout(Layout::Sorted, schema);
        b
    }

    /// Right-payload (trace I64 col) + weight of each emitted range-join row, in
    /// emission order. The trace payload identifies which trace rows matched.
    fn range_out_pairs(out: &Batch) -> Vec<(i64, i64)> {
        (0..out.count)
            .map(|r| (read_i64_le(out.col_data(1), r * 8), out.get_weight(r)))
            .collect()
    }

    fn trace_cursor(batch: Batch, schema: SchemaDescriptor) -> crate::storage::ReadCursor {
        use std::rc::Rc;
        crate::storage::ReadCursor::from_owned(&[Rc::new(batch)], schema)
    }

    /// Schema with `n_eq` U64 equality columns + 1 U64 range column (all PK) and
    /// a single trailing I64 payload — the canonical band-join reindex shape.
    fn make_range_schema(n_eq: usize) -> SchemaDescriptor {
        let mut cols: Vec<SchemaColumn> = (0..n_eq + 1).map(|_| SchemaColumn::new(type_code::U64, 0)).collect();
        cols.push(SchemaColumn::new(type_code::I64, 0)); // payload
        let pk: Vec<u32> = (0..n_eq as u32 + 1).collect();
        SchemaDescriptor::new(&cols, &pk)
    }

    /// Build a batch over `make_range_schema(n_eq)`. Each row is
    /// `(eq_cols, range, weight, payload)`; the `n_eq + 1` PK columns are
    /// OPK-encoded (big-endian U64) so memcmp equals numeric order. Rows must be
    /// pre-sorted ascending by `(eq.., range, payload)` — the merge walk groups by
    /// a forward scan and the cursor expects (PK, payload)-sorted rows.
    ///
    /// Flagged `Sorted`, not `Consolidated`: the differential inputs carry
    /// tombstones (weight 0) and multiset duplicates the walk handles directly, so
    /// the data is sorted but not ghost-free/duplicate-folded.
    fn make_range_batch(schema: &SchemaDescriptor, rows: &[(Vec<u64>, u64, i64, i64)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for (eq, range, w, val) in rows {
            let mut vals: Vec<u128> = eq.iter().map(|&x| x as u128).collect();
            vals.push(*range as u128);
            b.extend_pk_opk(schema, &vals);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.certify_layout(Layout::Sorted, schema);
        b
    }

    /// Random small `(eq.., range, weight, payload)` rows, sorted by `(eq.., range)`.
    /// Weights span `{-2,-1,0,1,2}` so the trace carries tombstones (weight ≤ 0)
    /// and the delta carries retractions; the small key space forces dense eq
    /// groups, boundary equality (`d == s`), and non-matching groups.
    fn gen_range_rows(rng: &mut Rng, n_eq: usize, n_rows: usize, key_space: u64) -> Vec<(Vec<u64>, u64, i64, i64)> {
        const WEIGHTS: [i64; 5] = [-2, -1, 0, 1, 2];
        let mut rows: Vec<(Vec<u64>, u64, i64, i64)> = (0..n_rows)
            .map(|_| {
                let eq: Vec<u64> = (0..n_eq).map(|_| rng.gen_range(key_space)).collect();
                let range = rng.gen_range(key_space);
                let w = WEIGHTS[rng.gen_range(5) as usize];
                let val = rng.gen_range(4) as i64; // few payloads → multiset duplicates
                (eq, range, w, val)
            })
            .collect();
        // Sort by (eq.., range, payload) — the full (PK, payload) order the
        // `Sorted` flag the builder sets asserts (payload breaks PK ties).
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.3.cmp(&b.3)));
        rows
    }

    /// Output as a `(pk_bytes, left_payload, right_payload) → Σweight` multiset,
    /// dropping net-zero entries — the consolidated form the two strategies must
    /// agree on regardless of (delta-major vs trace-major) emission order.
    fn range_multiset(out: &Batch) -> std::collections::BTreeMap<(Vec<u8>, i64, i64), i64> {
        let mut m = std::collections::BTreeMap::new();
        for r in 0..out.count {
            let pk = out.get_pk_bytes(r).to_vec();
            let lp = read_i64_le(out.col_data(0), r * 8);
            let rp = read_i64_le(out.col_data(1), r * 8);
            *m.entry((pk, lp, rp)).or_insert(0i64) += out.get_weight(r);
        }
        m.retain(|_, w| *w != 0);
        m
    }

    /// Assert the trace-driven merge (Strategy 2) emits the same consolidated
    /// multiset as the per-row probe (Strategy 1, the shipped oracle). One trace
    /// batch drives both: per-row seeks absolutely, then a rewind feeds the merge
    /// from the head — exactly what the selector does. Returns the merged row
    /// count so callers can assert non-vacuous coverage.
    fn assert_merge_eq_per_row(
        schema: SchemaDescriptor,
        n_eq: usize,
        rel: RangeRel,
        delta: &Batch,
        trace: Batch,
    ) -> usize {
        let mut ch = trace_cursor(trace, schema);
        let oracle = range_per_row_seek(delta, &mut ch, &schema, &schema, n_eq, rel);
        ch.rewind();
        let merged = range_merge_walk(delta, &mut ch, &schema, &schema, n_eq, rel);
        // The merge emits trace-major with delta runs — unsorted and unfolded — so
        // it leaves the layout tag clear for the downstream re-sort. Assert on the
        // tag, not `is_sorted()`: an empty merge has no rows and so is structurally
        // sorted regardless of the tag.
        assert_eq!(
            merged.layout(),
            Layout::Raw,
            "merge output must leave the layout tag clear (Raw) for downstream re-sort",
        );
        assert_eq!(
            range_multiset(&oracle),
            range_multiset(&merged),
            "merge vs per-row mismatch: n_eq={n_eq} rel={rel:?}",
        );
        merged.count
    }
}
