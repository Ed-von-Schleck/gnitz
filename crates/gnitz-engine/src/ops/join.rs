//! The delta-trace inner join, equi and non-equi.
//!
//! One opcode over a compiler-baked [`JoinProbe`]: `Equi` co-groups the delta
//! against the trace on equal key, `Range` walks an ordered half-open span per
//! delta equality group. Both drive the same emission — the product of a
//! contiguous delta run with the cursor's current trace row — and both produce
//! `[left_PK, left_payload…, right_payload…]`.

use std::cmp::Ordering;

use crate::schema::key::{compare_pk_ordering, key_range_between_cuts, pk_bytes_eq, KeyCut, PkBuf};
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, BlobCacheGuard, MemBatch, ReadCursor};

use super::cogroup::cogroup_intersection;

use gnitz_wire::{merge_null_words, RangeRel};

// ---------------------------------------------------------------------------
// The probe
// ---------------------------------------------------------------------------

/// How one join instruction probes its trace. Baked by the compiler, which holds
/// both sides' schemas and so resolves the probe once, off the epoch path.
#[derive(Clone, Copy)]
pub(crate) enum JoinProbe {
    /// Equal key: the trace group at each delta group's own PK.
    Equi,
    /// An ordered span within an equality group.
    Range(RangeProbe),
}

/// A range relation as its two independent dimensions, plus the width they
/// operate at. [`RangeProbe::new`] is the only public constructor, so the pair
/// always spells a real relation over a key width the sweep can slice.
#[derive(Clone, Copy)]
pub(crate) struct RangeProbe {
    /// Width in OPK bytes of the equality-pinned leading slots; the rest of a PK
    /// region is the range slot.
    eq_size: usize,
    /// Matches lie above the delta row's slot (`Gt`/`Ge`), not below (`Lt`/`Le`).
    prefix: bool,
    /// A trace slot equal to the delta row's own matches (`Ge`/`Le`).
    nonstrict: bool,
}

impl RangeProbe {
    /// Resolve a wire range relation against the two input schemas, or name the
    /// precondition the circuit violated. `Result`, not a debug assert: a circuit
    /// is client-supplied catalog data, and the sweep's `pk[..eq_size]` /
    /// `pk[eq_size..]` slices have nothing else establishing them.
    pub(crate) fn new(
        delta_schema: &SchemaDescriptor,
        trace_schema: &SchemaDescriptor,
        n_eq: u8,
        rel: RangeRel,
    ) -> Result<RangeProbe, &'static str> {
        // The trace's reindexed key is `[eq slots…, range slot]`.
        if n_eq as usize + 1 != trace_schema.pk_indices().len() {
            return Err("range join: n_eq does not match trace key arity");
        }
        // Both sides reindex at the pair's common promoted type, so their PK
        // regions have one width — which the sweep slices both of at.
        if delta_schema.pk_stride() != trace_schema.pk_stride() {
            return Err(
                "range join: delta and trace PK strides differ (both sides must reindex at the pair's common type)",
            );
        }
        // Not implied by the arity check above: `leading_key_size` sums *schema*-
        // order columns, so a crafted circuit can spend the whole key on the eq
        // prefix while still naming `n_eq + 1` PK columns.
        let eq_size = trace_schema.leading_key_size(n_eq as usize);
        if eq_size >= trace_schema.pk_stride() as usize {
            return Err("range join: eq prefix covers the whole key");
        }
        Ok(RangeProbe::of(eq_size, rel))
    }

    /// The relation's two dimensions at a validated `eq_size`. Private: only
    /// [`RangeProbe::new`] establishes that width against a real key region.
    fn of(eq_size: usize, rel: RangeRel) -> RangeProbe {
        let (prefix, nonstrict) = match rel {
            RangeRel::Gt => (true, false),
            RangeRel::Ge => (true, true),
            RangeRel::Lt => (false, false),
            RangeRel::Le => (false, true),
        };
        RangeProbe {
            eq_size,
            prefix,
            nonstrict,
        }
    }

    /// The trace key range this probe covers for a delta row whose PK region is
    /// `pk` (`equality prefix ‖ range slot`). `None` when it is provably empty.
    fn cut_points(&self, pk: &[u8]) -> Option<(PkBuf, Option<PkBuf>)> {
        let group = &pk[..self.eq_size];
        let (start, end) = match (self.prefix, self.nonstrict) {
            // Gt: (slot, group end)
            (true, false) => (KeyCut::above(pk), KeyCut::above(group)),
            // Ge: [slot, group end)
            (true, true) => (KeyCut::min_of(pk), KeyCut::above(group)),
            // Lt: [group start, slot)
            (false, false) => (KeyCut::min_of(group), KeyCut::min_of(pk)),
            // Le: [group start, slot]
            (false, true) => (KeyCut::min_of(group), KeyCut::above(pk)),
        };
        key_range_between_cuts(start, end, pk.len())
    }

    /// The row of the delta group `[lo, hi)` whose slot the group's single cut is
    /// taken on. The union of the rows' spans opens at the group's smallest slot
    /// and closes at its largest, and only one of those two bounds is open.
    #[inline]
    fn cut_row(&self, lo: usize, hi: usize) -> usize {
        if self.prefix {
            lo
        } else {
            hi - 1
        }
    }

    /// The greatest ordering a delta slot may have against a trace slot while
    /// still falling before the split that slot induces in the group: `Gt` and
    /// `Le` put an equal slot after the split, `Ge` and `Lt` before it.
    #[inline]
    fn last_before_split(&self) -> Ordering {
        if self.prefix != self.nonstrict {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}

// ---------------------------------------------------------------------------
// The operator
// ---------------------------------------------------------------------------

/// Join delta rows against the trace. Output schema:
/// `[left_PK, left_payload..., right_payload...]`, built by the compiler's
/// `merge_schemas_for_join` and handed down as `out_schema`.
///
/// Emission is trace-major under both probes: each trace row is walked once and
/// producted against a contiguous, random-access delta run. So the output is
/// PK-sorted but not (PK, payload)-sorted, and unfolded; it carries no layout
/// claim, and downstream re-sorts and consolidates.
pub(crate) fn op_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    probe: JoinProbe,
) -> Batch {
    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_ref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty_with_schema(out_schema);
    }
    let delta_mb = consolidated.as_mem_batch();
    // Seeded at the delta size: exact for a 1:1 key match, a floor otherwise.
    let mut output = Batch::with_capacity(*out_schema, n);
    // One dedup cache for the whole join: a `D×T` key group re-appends each left
    // payload `T` times and each right payload `D` times.
    let mut cache = BlobCacheGuard::acquire(out_schema, n);

    let mut emit = |rs: usize, re: usize, c: &ReadCursor| {
        let w_trace = c.current_weight;
        for i in rs..re {
            let w_out = delta_mb.get_weight(i).wrapping_mul(w_trace);
            if w_out != 0 {
                write_join_row(
                    &mut output,
                    &delta_mb,
                    i,
                    c,
                    w_out,
                    left_schema,
                    right_schema,
                    &mut cache,
                );
            }
        }
    };

    match probe {
        JoinProbe::Equi => cogroup_intersection(consolidated, cursor, |key, r, m| {
            m.for_each_pk_group_row(key, |c| emit(r.start, r.end, c));
        }),
        JoinProbe::Range(probe) => range_merge_walk(&delta_mb, cursor, probe, emit),
    }

    output
}

// ---------------------------------------------------------------------------
// The range walk
// ---------------------------------------------------------------------------

/// Trace-driven equality-group merge walk: sweep each delta equality group in
/// turn, each sweep saying where the next one starts.
fn range_merge_walk(
    delta: &MemBatch,
    cursor: &mut ReadCursor,
    probe: RangeProbe,
    mut emit: impl FnMut(usize, usize, &ReadCursor),
) {
    let mut lo = 0;
    while lo < delta.count {
        let hi = eq_group_end(delta, lo, probe.eq_size);
        // Group spans ascend across the walk, so once the trace runs out inside
        // one span no later group can match either.
        let Some(next) = sweep_eq_group(delta, cursor, probe, lo, hi, &mut emit) else {
            return;
        };
        lo = next;
    }
}

/// Sweep the delta equality group `[lo, hi)` against the trace: one cut spans the
/// whole group, so seek to its start and walk only the covered span with a
/// monotone delta pointer, handing `emit` the matching delta run per trace row.
/// The seek passes over untouched trace groups and the dead intra-group head.
///
/// Returns where the next delta group starts: `hi`, or further when the trace
/// itself proves the groups in between hold nothing. `None` when the trace is
/// exhausted.
fn sweep_eq_group(
    delta: &MemBatch,
    cursor: &mut ReadCursor,
    probe: RangeProbe,
    lo: usize,
    hi: usize,
    emit: &mut impl FnMut(usize, usize, &ReadCursor),
) -> Option<usize> {
    let Some((start, end)) = probe.cut_points(delta.get_pk_bytes(probe.cut_row(lo, hi))) else {
        // Provably empty, so the cursor is never positioned here — and so it says
        // nothing about what the trace holds after this group either.
        return Some(hi);
    };
    // Hoisted out of the sweep: the walk below reads these once per covered trace
    // row, where loading them back off `probe` costs more than the branch each
    // one decides — worth ~1% of the range kernel on `join_range_dt_bench`.
    let RangeProbe { eq_size, prefix, .. } = probe;
    let last_before_split = probe.last_before_split();

    // Galloping skip to the covered start: seeded at the live position it passes
    // over untouched trace groups and the dead head in one hop, and being
    // backward-capable it also repositions a cursor another op on the same trace
    // register left elsewhere.
    cursor.advance_to(start.pk_bytes());
    let end = end.as_ref().map(PkBuf::pk_bytes);
    let mut ptr = lo; // monotone across the whole sweep
    cursor.for_each_row_while(
        // Every key in [start, end) carries the group's equality prefix, so the
        // end bound alone delimits the group.
        |pk| end.is_none_or(|e| compare_pk_ordering(pk, e).is_lt()),
        |c| {
            let s = &c.current_pk_bytes()[eq_size..];
            // Matching rows form a prefix of the group under `Gt`/`Ge` and a
            // suffix under `Lt`/`Le`, so either way the pointer walks the rows
            // that come first.
            while ptr < hi && compare_pk_ordering(&delta.get_pk_bytes(ptr)[eq_size..], s) <= last_before_split {
                ptr += 1;
            }
            let (rs, re) = if prefix { (lo, ptr) } else { (ptr, hi) };
            emit(rs, re, c);
        },
    );
    // The cursor sits on the first trace row past the span, so its own equality
    // prefix is the next group the trace holds.
    cursor
        .valid
        .then(|| skip_to_group(delta, eq_size, hi, cursor.current_pk_bytes()))
}

/// First delta row past the equality group starting at `lo`.
fn eq_group_end(delta: &MemBatch, lo: usize, eq_size: usize) -> usize {
    let e = &delta.get_pk_bytes(lo)[..eq_size];
    let mut hi = lo + 1;
    while hi < delta.count && pk_bytes_eq(&delta.get_pk_bytes(hi)[..eq_size], e) {
        hi += 1;
    }
    hi
}

/// First delta row at index `from` or later whose equality prefix is not below
/// `group`'s. Every row passed has a strictly smaller prefix, so the landing row
/// heads its group.
fn skip_to_group(delta: &MemBatch, eq_size: usize, from: usize, group: &[u8]) -> usize {
    let group = &group[..eq_size];
    let mut hi = from;
    while hi < delta.count && compare_pk_ordering(&delta.get_pk_bytes(hi)[..eq_size], group).is_lt() {
        hi += 1;
    }
    hi
}

// ---------------------------------------------------------------------------
// The row writer
// ---------------------------------------------------------------------------

/// Write one composite join output row: `[left_PK, left_payload..., right_payload...]`.
///
/// Left columns come from the delta MemBatch, right columns from the cursor's
/// current row — both halves through the shared, monomorphic
/// `Batch::append_payload_cols` body (German-string blob relocation included).
#[inline]
#[allow(clippy::too_many_arguments)]
fn write_join_row(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    right_cursor: &ReadCursor,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
    cache: &mut BlobCacheGuard,
) {
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_cursor.current_null_word;

    let left_npc = left_schema.num_payload_cols();
    let null_word = merge_null_words(left_null, right_null, left_npc);

    output.extend_pk_bytes(left_batch.get_pk_bytes(left_row));
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    output.append_payload_cols(0, left_schema, left_batch, left_row, left_null, cache.get_mut());
    let (right_src, right_row) = right_cursor.current_row_source();
    output.append_payload_cols(
        left_npc,
        right_schema,
        right_src,
        right_row,
        right_null,
        cache.get_mut(),
    );

    output.count += 1;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use proptest::prelude::*;

    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::{Batch, Layout};
    use crate::test_support::{
        make_batch, make_batch_i64pk as make_signed_batch, make_batch_raw, make_schema_i64pk_i64 as make_schema_signed,
        make_schema_u64_i64, make_wide_batch, opk_pk, wide_pk_3xu64_schema,
    };
    use gnitz_wire::read_i64_le;

    const RELS: [RangeRel; 4] = [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge];

    /// The join output schema `reg_meta` carries and `exec.rs` hands the op —
    /// the compiler's own builder, not a look-alike, so a change to the layout
    /// reaches these tests instead of silently passing against a stale copy.
    fn join_out_schema(left: &SchemaDescriptor, right: &SchemaDescriptor) -> SchemaDescriptor {
        crate::schema::merge_schemas_for_join(left, right).expect("test join schema exceeds MAX_COLUMNS")
    }

    fn trace_cursor(batch: Batch, schema: SchemaDescriptor) -> ReadCursor {
        ReadCursor::over_batches(&[Rc::new(batch)], schema)
    }

    /// The equi join over one schema on both sides.
    fn equi_join(schema: &SchemaDescriptor, delta: &Batch, cursor: &mut ReadCursor) -> Batch {
        op_join_delta_trace(
            delta,
            cursor,
            schema,
            schema,
            &join_out_schema(schema, schema),
            JoinProbe::Equi,
        )
    }

    /// The range join over one schema on both sides, with the probe the compiler
    /// would bake for `(n_eq, rel)`.
    fn range_join(
        schema: &SchemaDescriptor,
        n_eq: usize,
        rel: RangeRel,
        delta: &Batch,
        cursor: &mut ReadCursor,
    ) -> Batch {
        op_join_delta_trace(
            delta,
            cursor,
            schema,
            schema,
            &join_out_schema(schema, schema),
            JoinProbe::Range(RangeProbe::new(schema, schema, n_eq as u8, rel).expect("fixture probe is well-formed")),
        )
    }

    // -----------------------------------------------------------------------
    // Equi delta-trace
    // -----------------------------------------------------------------------

    /// Inner join delta×trace on a narrow (I32, 4-byte) signed PK. Every delta
    /// key has a trace match; all must be found, including the smallest and a
    /// negative one (regression guard for the byte-seek path at sub-8-byte
    /// stride, and for the OPK sign flip a narrow signed PK carries).
    #[test]
    fn test_join_dt_i32_key_all_match() {
        let schema = make_schema_i32();
        // Trace: keys -7, 1 and 2, all present.
        let trace = make_i32_batch(&schema, &[(-7, 1, 50), (1, 1, 100), (2, 1, 200)]);
        let mut ch = trace_cursor(trace, schema);
        let delta = make_i32_batch(&schema, &[(-7, 1, 5), (1, 1, 10), (2, 1, 20)]);

        let out = equi_join(&schema, &delta, &mut ch);
        assert_eq!(out.count, 3, "every I32 key must join (smallest key not dropped)");
        let keys: Vec<i32> = (0..out.count)
            .map(|i| {
                let mut le = [0u8; 4];
                gnitz_wire::decode_pk_column(out.get_pk_bytes(i), type_code::I32, &mut le);
                i32::from_le_bytes(le)
            })
            .collect();
        assert_eq!(keys, vec![-7, 1, 2]);
    }

    /// Many-to-many inner join: the co-group walks each trace group once and
    /// products it against the whole delta group, emitting the cartesian product
    /// in trace-major order — left payloads interleave out of (PK, payload)
    /// order, so the output must not be marked sorted.
    #[test]
    fn test_join_dt_many_to_many_output_unsorted() {
        let schema = make_schema_u64_i64();
        // Trace: PK=1 with two right payloads (100, 200).
        let mut ch = trace_cursor(make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]), schema);
        // Delta: PK=1 with three left payloads.
        let delta = make_batch(&schema, &[(1, 1, 10), (1, 1, 20), (1, 1, 30)]);

        let out = equi_join(&schema, &delta, &mut ch);
        assert_eq!(out.count, 6, "3 left × 2 right = 6 join outputs");

        // col 0 = left payload, col 1 = right payload (all PKs equal). The sorted
        // flag may be true ONLY if the rows are actually in (left, right) order.
        let pairs: Vec<(i64, i64)> = (0..out.count)
            .map(|r| (read_i64_le(out.col_data(0), r * 8), read_i64_le(out.col_data(1), r * 8)))
            .collect();
        let actually_sorted = pairs.windows(2).all(|w| w[0] <= w[1]);
        assert!(
            !out.is_sorted() || actually_sorted,
            "many-to-many join marked output sorted but payload order is {pairs:?}",
        );
    }

    /// Two delta rows sharing one wide PK but differing in payload (a multiset
    /// delta) against one trace row for that PK: the co-group hands the whole
    /// same-PK delta group to the callback at once, producted against the
    /// once-walked trace group with no re-seek.
    #[test]
    fn test_join_dt_wide_pk_multiset_delta() {
        let schema = wide_pk_3xu64_schema();
        let mut ch = trace_cursor(make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]), schema);

        let mut delta = Batch::with_capacity(schema, 2);
        for payload in [10i64, 20] {
            delta.extend_pk_opk(&schema, &[1, 0, 0]);
            delta.extend_weight(&1i64.to_le_bytes());
            delta.extend_null_bmp(&0u64.to_le_bytes());
            delta.extend_col(0, &payload.to_le_bytes());
            delta.count += 1;
        }
        delta.certify_layout(Layout::Sorted, &schema);

        let out = equi_join(&schema, &delta, &mut ch);
        assert_eq!(out.count, 2, "multiset delta: expected 2 join outputs");
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(out.get_weight(1), 1);
    }

    /// Two wide-PK rows sharing their first 16 OPK bytes but differing in column
    /// 2. The co-group must treat them as distinct keys, not conflate via a u128
    /// prefix: only the row the trace holds may join.
    #[test]
    fn test_join_dt_wide_pk_prefix_collision() {
        let schema = wide_pk_3xu64_schema();
        let mut ch = trace_cursor(make_wide_batch(&schema, &[(1, 1, 2, 1, 100)]), schema);
        let delta = make_wide_batch(
            &schema,
            &[
                (1, 1, 2, 1, 200), // matches trace pk
                (1, 1, 9, 1, 300), // different pk — must NOT match
            ],
        );

        let out = equi_join(&schema, &delta, &mut ch);
        assert_eq!(out.count, 1, "prefix-collision: only matching PK should produce output");
        assert_eq!(out.get_pk_bytes(0), opk_pk(&schema, &[1, 1, 2]).as_slice());
        assert_eq!(out.get_weight(0), 1);
    }

    /// `write_join_row` with `left_npc == 64` and `right_npc == 0`: the
    /// `right_null << 64` shift would panic in debug without the width guard.
    #[test]
    fn test_write_join_row_shift_guard_64() {
        let left_schema = make_wide_left_schema_64();
        let right_schema = crate::test_support::pk_only_schema(&[type_code::U64]);
        let left = build_wide_left_row(&left_schema, 1);

        let mut trace = Batch::with_capacity(right_schema, 1);
        trace.extend_pk(1u128);
        trace.extend_weight(&1i64.to_le_bytes());
        trace.extend_null_bmp(&0u64.to_le_bytes());
        trace.count += 1;
        trace.certify_layout(Layout::Consolidated, &right_schema);
        let mut cursor = trace_cursor(trace, right_schema);
        cursor.seek_bytes(&1u64.to_be_bytes());

        let mut output = Batch::with_capacity(left_schema, 1);
        write_join_row(
            &mut output,
            &left.as_mem_batch(),
            0,
            &cursor,
            1,
            &left_schema,
            &right_schema,
            &mut BlobCacheGuard::empty(),
        );
        assert_eq!(output.count, 1);
    }

    fn make_schema_i32() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_i32_batch(schema: &SchemaDescriptor, rows: &[(i32, i64, i64)]) -> Batch {
        let mut b = Batch::with_capacity(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk_bytes(&opk_pk(schema, &[(pk as i64) as u128]));
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.certify_layout(Layout::Consolidated, schema);
        b
    }

    fn make_wide_left_schema_64() -> SchemaDescriptor {
        let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..64 {
            cols.push(SchemaColumn::new(type_code::I64, 0));
        }
        SchemaDescriptor::new(&cols, &[0])
    }

    fn build_wide_left_row(left_schema: &SchemaDescriptor, pk: u64) -> Batch {
        let mut b = Batch::with_capacity(*left_schema, 1);
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        for pi in 0..left_schema.num_payload_cols() {
            b.extend_col(pi, &0i64.to_le_bytes());
        }
        b.count += 1;
        b
    }

    // -----------------------------------------------------------------------
    // Range delta-trace: literal output
    // -----------------------------------------------------------------------

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
            let out = range_join(&schema, 0, rel, &delta, &mut ch);
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
        let schema = make_range_schema(1, false); // (U64 k, U64 range) PK, I64 payload
                                                  // Trace sorted by (k, range): k=1 group then k=2 group. k=2,y=5 has an
                                                  // in-range slot for x=15 but is in a different group → must not match.
        let trace = make_range_batch(
            &schema,
            &[(vec![1], 10, 1, 110), (vec![1], 20, 1, 120), (vec![2], 5, 1, 205)],
        );
        let mut ch = trace_cursor(trace, schema);
        let delta = make_range_batch(&schema, &[(vec![1], 15, 1, 200)]);
        // rel Lt: {y < 15 within k=1} = {10}. k=2,y=5 excluded by the group edge.
        let out = range_join(&schema, 1, RangeRel::Lt, &delta, &mut ch);
        let got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
        assert_eq!(got, vec![110]);
    }

    /// Band join, `Le` with a maximal range slot (`d = u64::MAX`): the end cut's
    /// carry ripples into the eq prefix (next group's first key). The whole k=1
    /// group matches and the k=2 group does not — no panic, no spill.
    #[test]
    fn test_range_dt_eq_prefix_ripple_le_max() {
        let schema = make_range_schema(1, false);
        let trace = make_range_batch(
            &schema,
            &[(vec![1], 0, 1, 100), (vec![1], u64::MAX, 1, 199), (vec![2], 0, 1, 200)],
        );
        let mut ch = trace_cursor(trace, schema);
        let delta = make_range_batch(&schema, &[(vec![1], u64::MAX, 1, 9)]);
        // y <= u64::MAX within k=1 → both k=1 rows; k=2 excluded.
        let out = range_join(&schema, 1, RangeRel::Le, &delta, &mut ch);
        let mut got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
        got.sort_unstable();
        assert_eq!(got, vec![100, 199]);
    }

    /// `n_eq = 0`, `rel = Lt`: two delta rows form one equality group spanning
    /// the whole trace, and the monotone suffix pointer must widen the emitted
    /// run as the trace slot ascends. Pinned as literal output.
    #[test]
    fn test_range_dt_lt_suffix_pointer_widens() {
        let schema = make_schema_u64_i64();
        let mut ch = trace_cursor(make_batch(&schema, &[(5, 1, 105), (15, 1, 115), (25, 1, 125)]), schema);
        let delta = make_batch(&schema, &[(10, 1, 100), (30, 1, 300)]);
        let out = range_join(&schema, 0, RangeRel::Lt, &delta, &mut ch);
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
        let trace_rows = [(-100i64, 1i64, 1i64), (0, 1, 2), (50, 1, 3)];
        let delta = make_signed_batch(&schema, &[(0, 1, 9)]);
        // Gt: {y > 0} = {50→3}. Lt: {y < 0} = {-100→1}.
        for (rel, want) in [(RangeRel::Gt, vec![3]), (RangeRel::Lt, vec![1])] {
            let mut ch = trace_cursor(make_signed_batch(&schema, &trace_rows), schema);
            let out = range_join(&schema, 0, rel, &delta, &mut ch);
            let got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
            assert_eq!(got, want, "rel {rel:?}");
        }
    }

    /// Output weight = `w_delta × w_trace` (wrapping); a retraction delta negates,
    /// a doubled trace doubles, and a zero-weight trace row is skipped.
    #[test]
    fn test_range_dt_weights() {
        let schema = make_schema_u64_i64();
        // Trace: y=10 (w=2), y=20 (w=0, must be skipped), y=30 (w=1). A weight-0
        // row is sorted but not consolidated, so it is flagged `Sorted`.
        let mut trace = make_batch_raw(&schema, &[(10, 2, 110), (20, 0, 120), (30, 1, 130)]);
        trace.certify_layout(Layout::Sorted, &schema);
        let mut ch = trace_cursor(trace, schema);
        // Delta retraction: x=40, w=-1, so all matches negate.
        let delta = make_batch(&schema, &[(40, -1, 400)]);
        // rel Lt: {y < 40} = all three, but y=20's product is -1*0 = 0 (skipped).
        let out = range_join(&schema, 0, RangeRel::Lt, &delta, &mut ch);
        let mut got = range_out_pairs(&out);
        got.sort_unstable();
        // y=10: -1*2 = -2; y=30: -1*1 = -1; y=20 skipped.
        assert_eq!(got, vec![(110, -2), (130, -1)]);
    }

    /// Empty delta → empty output, no panic.
    #[test]
    fn test_range_dt_empty_delta() {
        let schema = make_schema_u64_i64();
        let mut ch = trace_cursor(make_batch(&schema, &[(10, 1, 110)]), schema);
        let delta = make_batch(&schema, &[]);
        let out = range_join(&schema, 0, RangeRel::Le, &delta, &mut ch);
        assert_eq!(out.count, 0);
    }

    /// Empty trace with a non-empty delta: the walk visits zero trace rows and
    /// emits nothing.
    #[test]
    fn test_range_dt_empty_trace() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(10, 1, 1), (20, 1, 2)]);
        let mut ch = trace_cursor(make_batch(&schema, &[]), schema);
        let out = range_join(&schema, 0, RangeRel::Le, &delta, &mut ch);
        assert_eq!(out.count, 0);
    }

    /// The walk leaves the layout tag clear (`Raw`) for the downstream re-sort:
    /// its emission is trace-major with delta runs per trace row, so it is
    /// neither (PK, payload)-sorted nor folded. Asserted on the tag rather than
    /// `is_sorted()`, which an empty output satisfies structurally.
    #[test]
    fn test_range_dt_output_layout_is_raw() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(15, 1, 1), (25, 1, 2)]);
        let mut ch = trace_cursor(make_batch(&schema, &[(10, 1, 110), (20, 1, 120)]), schema);
        let out = range_join(&schema, 0, RangeRel::Lt, &delta, &mut ch);
        assert!(out.count > 0, "fixture must emit rows for the tag to mean anything");
        assert_eq!(out.layout(), Layout::Raw);
    }

    // -----------------------------------------------------------------------
    // Range delta-trace versus a brute-force reference
    // -----------------------------------------------------------------------

    /// Maximal range slot, `n_eq = 0`: `Gt` of `u64::MAX` is a provably-empty
    /// cut, and the trace *contains* `u64::MAX`, so a walk that failed to skip
    /// would emit. All four rels against the reference.
    #[test]
    fn test_range_dt_maximal_slot_n_eq0() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(u64::MAX, 1, 9)]);
        for rel in RELS {
            let trace = make_batch(&schema, &[(0, 1, 100), (50, 1, 150), (u64::MAX, 1, 199)]);
            assert_matches_reference(schema, 0, rel, &delta, trace);
        }
    }

    /// Maximal slot in a non-maximal eq group (`n_eq = 1`): `Gt` of `u64::MAX`
    /// in group k=1 is a provably-empty cut — no match, and crucially no spill
    /// into the k=2 group.
    #[test]
    fn test_range_dt_maximal_slot_n_eq1() {
        let schema = make_range_schema(1, false);
        let delta = make_range_batch(&schema, &[(vec![1], u64::MAX, 1, 9)]);
        for rel in RELS {
            let trace = make_range_batch(
                &schema,
                &[(vec![1], 0, 1, 100), (vec![1], 50, 1, 150), (vec![2], 0, 1, 200)],
            );
            assert_matches_reference(schema, 1, rel, &delta, trace);
        }
    }

    /// Mid-group tombstone: a trace row with weight ≤ 0 must still advance the
    /// delta pointer (it participates in the monotone walk) yet emit nothing
    /// (its product is net-zero / suppressed).
    #[test]
    fn test_range_dt_tombstone_advances_pointer() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(40, -1, 400)]); // retraction
        for rel in [RangeRel::Lt, RangeRel::Le] {
            // y=20 is a tombstone (w=0), y=25 carries a negative weight. A trace
            // carrying either is sorted but not consolidated.
            let mut trace = make_batch_raw(&schema, &[(10, 1, 110), (20, 0, 120), (25, -1, 125), (30, 1, 130)]);
            trace.certify_layout(Layout::Sorted, &schema);
            assert_matches_reference(schema, 0, rel, &delta, trace);
        }
    }

    /// High fan-out (`output ≫ |trace|`), both pointer directions: every delta
    /// row matches most of its trace group. The suffix rel (`Lt`) over large `d`
    /// and the prefix rel (`Gt`) over small `d` each drive the monotone pointer
    /// the full width of the group.
    #[test]
    fn test_range_dt_high_fanout_both_directions() {
        let schema = make_schema_u64_i64();
        let trace_rows: Vec<(u64, i64, i64)> = (0..8u64).map(|y| (y, 1, 100 + y as i64)).collect();
        // suffix rel: large delta keys → each matches the whole low prefix.
        let delta_lt = make_batch(&schema, &[(5, 1, 1), (6, 1, 2), (7, 1, 3)]);
        let n1 = assert_matches_reference(schema, 0, RangeRel::Lt, &delta_lt, make_batch(&schema, &trace_rows));
        // prefix rel: small delta keys → each matches the whole high suffix.
        let delta_gt = make_batch(&schema, &[(0, 1, 1), (1, 1, 2), (2, 1, 3)]);
        let n2 = assert_matches_reference(schema, 0, RangeRel::Gt, &delta_gt, make_batch(&schema, &trace_rows));
        assert!(n1 > 8 && n2 > 8, "high fan-out should emit ≫ |trace| rows: {n1}, {n2}");
    }

    /// Multiset delta + multi-payload trace: duplicate `[eq‖d]` on both sides
    /// with distinct payloads. The walk must emit the full cross-product per
    /// trace row, weights multiplied.
    #[test]
    fn test_range_dt_multiset_multipayload() {
        let schema = make_schema_u64_i64();
        // Trace PK=10 twice (distinct payloads); delta PK=15 twice.
        let trace = make_batch(&schema, &[(10, 1, 101), (10, 2, 102), (20, 1, 200)]);
        let delta = make_batch(&schema, &[(15, 1, 1), (15, 3, 2)]);
        assert_matches_reference(schema, 0, RangeRel::Lt, &delta, trace);
    }

    /// A narrow covered span inside a large trace: `Gt`/`Ge` seek past the dead
    /// low head, `Lt`/`Le` stop before the dead high tail. Reference equality is
    /// all this observes — how many trace rows were walked is not visible from
    /// outside the op.
    #[test]
    fn test_range_dt_narrow_span_over_large_trace() {
        let schema = make_schema_u64_i64();
        let trace_rows: Vec<(u64, i64, i64)> = (0..30u64).map(|y| (y, 1, 100 + y as i64)).collect();
        // Gt over a large min: covered = (25, 30).
        let delta_gt = make_batch(&schema, &[(25, 1, 1), (26, 1, 2)]);
        assert_matches_reference(schema, 0, RangeRel::Gt, &delta_gt, make_batch(&schema, &trace_rows));
        // Lt over a small max: covered = [0, 4).
        let delta_lt = make_batch(&schema, &[(3, 1, 1), (4, 1, 2)]);
        assert_matches_reference(schema, 0, RangeRel::Lt, &delta_lt, make_batch(&schema, &trace_rows));
    }

    /// Band join with non-matching groups (`n_eq = 1`): a trace eq group with no
    /// delta group, and a delta eq group with no trace group — the shape the
    /// walk's trailing group skip elides. Only the shared group k=3 emits.
    #[test]
    fn test_range_dt_non_matching_groups() {
        let schema = make_range_schema(1, false);
        // Trace groups k=1, k=3; delta groups k=2 (no trace), k=3 (matches).
        let delta = make_range_batch(&schema, &[(vec![2], 7, 1, 2), (vec![3], 7, 1, 3)]);
        for rel in RELS {
            let trace = make_range_batch(
                &schema,
                &[(vec![1], 5, 1, 15), (vec![3], 5, 1, 35), (vec![3], 9, 1, 39)],
            );
            assert_matches_reference(schema, 1, rel, &delta, trace);
        }
    }

    /// The range op against a *used* trace cursor: `bind_trace_cursors` binds one
    /// per trace register, so two ops on one trace share it for the whole epoch.
    /// A parked and an exhausted cursor must both produce the fresh-cursor
    /// output — the group skip reads the cursor position.
    #[test]
    fn test_range_dt_stale_trace_cursor() {
        let schema = make_range_schema(1, false);
        let out_schema = join_out_schema(&schema, &schema);
        let delta = make_range_batch(&schema, &[(vec![1], 5, 1, 1), (vec![3], 5, 1, 3)]);
        let trace_rows = [
            (vec![1u64], 0u64, 1i64, 10i64),
            (vec![1], 9, 1, 19),
            (vec![3], 0, 1, 30),
            (vec![3], 9, 1, 39),
        ];
        for rel in RELS {
            let mut fresh_ch = trace_cursor(make_range_batch(&schema, &trace_rows), schema);
            let want = range_join(&schema, 1, rel, &delta, &mut fresh_ch);

            for park_past_end in [false, true] {
                let mut ch = trace_cursor(make_range_batch(&schema, &trace_rows), schema);
                ch.advance_to(&opk_pk(&schema, &[3, 9]));
                if park_past_end {
                    ch.advance();
                    assert!(!ch.valid);
                }
                let got = range_join(&schema, 1, rel, &delta, &mut ch);
                assert_eq!(got.count, want.count, "rel {rel:?} past_end={park_past_end}");
                assert_eq!(
                    row_multiset(&got, &out_schema),
                    row_multiset(&want, &out_schema),
                    "rel {rel:?} past_end={park_past_end}",
                );
            }
        }
    }

    /// Random `(eq.., range, weight, payload)` rows over a tiny key space,
    /// returned in `(eq.., range, payload)` order — the full (PK, payload) order
    /// the `Sorted` flag `make_range_batch` sets claims. Weights span `{-2..=2}`
    /// so a trace carries tombstones and a delta retractions, and the four-value
    /// key space forces dense eq groups, boundary equality (`d == s`), and
    /// non-matching groups.
    fn arb_range_rows(n_eq: usize) -> impl Strategy<Value = Vec<(Vec<u64>, u64, i64, i64)>> {
        let row = (prop::collection::vec(0u64..4, n_eq), 0u64..4, -2i64..=2i64, -2i64..2i64);
        prop::collection::vec(row, 0..8).prop_map(|mut rows| {
            rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.3.cmp(&b.3)));
            rows
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// The range join matches the brute-force reference over random delta and
        /// trace batches, for all four rels, `n_eq ∈ {0, 1, 2}`, and both payload
        /// shapes — `wide` adds the NULL and STRING columns a fixed-int fixture
        /// cannot exercise. Sparse eq groups at `n_eq ∈ {1, 2}` are what drive
        /// the walk's trailing group skip.
        #[test]
        fn range_dt_matches_reference(
            (n_eq, wide, rel_i, delta_rows, trace_rows) in (0usize..3, any::<bool>(), 0usize..4).prop_flat_map(
                |(n_eq, wide, rel_i)| {
                    (Just(n_eq), Just(wide), Just(rel_i), arb_range_rows(n_eq), arb_range_rows(n_eq))
                },
            ),
        ) {
            let schema = make_range_schema(n_eq, wide);
            let delta = make_range_batch(&schema, &delta_rows);
            let trace = make_range_batch(&schema, &trace_rows);
            assert_matches_reference(schema, n_eq, RELS[rel_i], &delta, trace);
        }
    }

    // -----------------------------------------------------------------------
    // Range fixtures and the reference
    // -----------------------------------------------------------------------

    /// Payload strings for the `wide` fixtures: an empty one, one short enough
    /// to stay inline, and two past `SHORT_STRING_THRESHOLD` that live in the
    /// blob heap — so the emit's German-string relocation runs on some rows and
    /// not others.
    const FIXTURE_STRINGS: [&[u8]; 4] = [
        b"",
        b"abc",
        b"a-long-string-past-the-inline-limit",
        b"another-long-payload-string",
    ];

    /// `n_eq` U64 equality columns + 1 U64 range column (all PK) and a trailing
    /// I64 payload — the canonical band-join reindex shape. `wide` adds a
    /// nullable I64 and a STRING, the columns a null-merge or blob-relocation bug
    /// in the row writer would show up in.
    fn make_range_schema(n_eq: usize, wide: bool) -> SchemaDescriptor {
        let mut cols: Vec<SchemaColumn> = (0..n_eq + 1).map(|_| SchemaColumn::new(type_code::U64, 0)).collect();
        cols.push(SchemaColumn::new(type_code::I64, 0)); // payload
        if wide {
            cols.push(SchemaColumn::new(type_code::I64, 1));
            cols.push(SchemaColumn::new(type_code::STRING, 0));
        }
        let pk: Vec<u32> = (0..n_eq as u32 + 1).collect();
        SchemaDescriptor::new(&cols, &pk)
    }

    /// Build a batch over [`make_range_schema`] from `(eq_cols, range, weight,
    /// payload)` rows, which must arrive sorted by `(eq.., range, payload)`. On a
    /// wide schema the two extra cells are *derived* from `payload`, so equal
    /// payloads stay equal cell-for-cell and that sort order is the full
    /// (PK, payload) order.
    ///
    /// Flagged `Sorted`, not `Consolidated`: these fixtures carry tombstones and
    /// multiset duplicates.
    fn make_range_batch(schema: &SchemaDescriptor, rows: &[(Vec<u64>, u64, i64, i64)]) -> Batch {
        let wide = schema.num_payload_cols() > 1;
        let mut b = Batch::with_capacity(*schema, rows.len().max(1));
        for (eq, range, w, val) in rows {
            let mut vals: Vec<u128> = eq.iter().map(|&x| x as u128).collect();
            vals.push(*range as u128);
            b.extend_pk_opk(schema, &vals);
            b.extend_weight(&w.to_le_bytes());
            let null_word = u64::from(wide && *val < 0) << 1;
            b.extend_null_bmp(&null_word.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            if wide {
                b.extend_col(1, &val.wrapping_mul(3).to_le_bytes());
                let s = FIXTURE_STRINGS[val.rem_euclid(FIXTURE_STRINGS.len() as i64) as usize];
                let cell = gnitz_wire::encode_german_string(s, &mut b.blob);
                b.extend_col(2, &cell);
            }
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

    /// A join output row as a comparison key: the PK bytes, the null word, and
    /// every payload cell's *content*. A German string is keyed by its decoded
    /// bytes — its 16-byte cell carries a blob offset that depends on append
    /// order, so the raw cell is not comparable across two emissions.
    type RowKey = (Vec<u8>, u64, Vec<Vec<u8>>);

    /// Every payload cell of `row`; a NULL cell reads as an empty vector, which
    /// the null word in the key already distinguishes from an empty string.
    fn payload_cells(b: &Batch, schema: &SchemaDescriptor, row: usize) -> Vec<Vec<u8>> {
        let null = b.get_null_word(row);
        schema
            .payload_columns()
            .map(|(pi, col)| {
                if (null >> pi) & 1 == 1 {
                    return Vec::new();
                }
                if col.type_code == type_code::STRING {
                    return crate::test_support::read_german_string(b, pi, row);
                }
                let cs = col.size() as usize;
                b.col_data(pi)[row * cs..(row + 1) * cs].to_vec()
            })
            .collect()
    }

    /// `Σ weight` per output row identity, net-zero entries dropped — the
    /// consolidated form the walk and the reference must agree on regardless of
    /// emission order.
    fn row_multiset(out: &Batch, schema: &SchemaDescriptor) -> std::collections::BTreeMap<RowKey, i64> {
        let mut m = std::collections::BTreeMap::new();
        for r in 0..out.count {
            let key = (
                out.get_pk_bytes(r).to_vec(),
                out.get_null_word(r),
                payload_cells(out, schema, r),
            );
            *m.entry(key).or_insert(0i64) += out.get_weight(r);
        }
        m.retain(|_, w| *w != 0);
        m
    }

    /// Brute-force reference: every `(delta row, trace row)` pair whose eq
    /// prefixes agree and whose range slots satisfy `rel`, composed into the
    /// output row the join must build, at weight `w_d · w_t`. Returns the folded
    /// multiset and the number of pairs with a non-zero product. The composition
    /// spells the output layout independently of the row writer under test.
    fn reference(
        schema: &SchemaDescriptor,
        n_eq: usize,
        rel: RangeRel,
        delta: &Batch,
        trace: &Batch,
    ) -> (std::collections::BTreeMap<RowKey, i64>, usize) {
        let eq_size = schema.leading_key_size(n_eq);
        let left_npc = schema.num_payload_cols();
        let mut m = std::collections::BTreeMap::new();
        let mut rows = 0usize;
        for i in 0..delta.count {
            let dpk = delta.get_pk_bytes(i);
            for j in 0..trace.count {
                let tpk = trace.get_pk_bytes(j);
                if dpk[..eq_size] != tpk[..eq_size] {
                    continue;
                }
                // Equal-width OPK slot slices, so a raw byte compare IS the
                // typed comparison the relation names.
                let (d, s) = (&dpk[eq_size..], &tpk[eq_size..]);
                let hit = match rel {
                    RangeRel::Lt => s < d,
                    RangeRel::Le => s <= d,
                    RangeRel::Gt => s > d,
                    RangeRel::Ge => s >= d,
                };
                if !hit {
                    continue;
                }
                let w = delta.get_weight(i).wrapping_mul(trace.get_weight(j));
                if w == 0 {
                    continue;
                }
                rows += 1;
                let mut cells = payload_cells(delta, schema, i);
                cells.extend(payload_cells(trace, schema, j));
                let null = merge_null_words(delta.get_null_word(i), trace.get_null_word(j), left_npc);
                *m.entry((dpk.to_vec(), null, cells)).or_insert(0i64) += w;
            }
        }
        m.retain(|_, w| *w != 0);
        (m, rows)
    }

    /// Assert the range join's output matches [`reference`] on both the folded
    /// multiset and the raw row count — the count is what catches an
    /// equal-and-opposite miss/spurious pair that the multiset alone hides.
    /// Returns it, so callers can assert non-vacuous coverage.
    ///
    /// The reference reads the *consolidated* delta, which is what the op joins;
    /// the trace needs no such step, since a single-source cursor emits every row
    /// verbatim.
    fn assert_matches_reference(
        schema: SchemaDescriptor,
        n_eq: usize,
        rel: RangeRel,
        delta: &Batch,
        trace: Batch,
    ) -> usize {
        let out_schema = join_out_schema(&schema, &schema);
        let cs = Batch::consolidate_if_needed(delta, &schema);
        let (want, want_rows) = reference(&schema, n_eq, rel, cs.as_ref().unwrap_or(delta), &trace);

        let mut ch = trace_cursor(trace, schema);
        let out = range_join(&schema, n_eq, rel, delta, &mut ch);

        assert_eq!(out.count, want_rows, "row count: n_eq={n_eq} rel={rel:?}");
        assert_eq!(
            row_multiset(&out, &out_schema),
            want,
            "multiset: n_eq={n_eq} rel={rel:?}"
        );
        out.count
    }

    // -----------------------------------------------------------------------
    // RangeProbe::cut_points, over 1-byte slots so the keys are exact-comparable
    // -----------------------------------------------------------------------

    /// The cut points for `pk = eq ‖ d` under `rel`, over the all-`U8` PK schema
    /// whose one-byte slots make `pk` its own OPK image.
    fn cuts(eq: &[u8], d: &[u8], rel: RangeRel) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        let mut pk = eq.to_vec();
        pk.extend_from_slice(d);
        let cols: Vec<SchemaColumn> = (0..pk.len()).map(|_| SchemaColumn::new(type_code::U8, 0)).collect();
        let pk_idx: Vec<u32> = (0..pk.len() as u32).collect();
        let schema = SchemaDescriptor::new(&cols, &pk_idx);
        RangeProbe::new(&schema, &schema, eq.len() as u8, rel)
            .expect("u8-slot fixture is a well-formed range key")
            .cut_points(&pk)
            .map(|(s, e)| (s.pk_bytes().to_vec(), e.map(|e| e.pk_bytes().to_vec())))
    }

    #[test]
    fn no_eq_interior_value() {
        // d = 0x05, slot only (stride = 1).
        assert_eq!(cuts(&[], &[0x05], RangeRel::Gt), Some((vec![0x06], None)));
        assert_eq!(cuts(&[], &[0x05], RangeRel::Ge), Some((vec![0x05], None)));
        assert_eq!(cuts(&[], &[0x05], RangeRel::Lt), Some((vec![0x00], Some(vec![0x05]))));
        assert_eq!(cuts(&[], &[0x05], RangeRel::Le), Some((vec![0x00], Some(vec![0x06]))));
    }

    #[test]
    fn no_eq_all_zero_value() {
        // d = 0x00: Gt of 0 starts at 1; Lt of 0 spans [0, 0) — provably empty,
        // so the group is skipped without a seek.
        assert_eq!(cuts(&[], &[0x00], RangeRel::Gt), Some((vec![0x01], None)));
        assert_eq!(cuts(&[], &[0x00], RangeRel::Ge), Some((vec![0x00], None)));
        assert_eq!(cuts(&[], &[0x00], RangeRel::Lt), None);
        assert_eq!(cuts(&[], &[0x00], RangeRel::Le), Some((vec![0x00], Some(vec![0x01]))));
    }

    #[test]
    fn no_eq_all_ff_value() {
        // d = 0xFF (maximal slot, n_eq=0): Gt matches nothing (provably empty);
        // Ge starts at 0xFF; Le's succ carries out → scan to table end.
        assert_eq!(cuts(&[], &[0xFF], RangeRel::Gt), None);
        assert_eq!(cuts(&[], &[0xFF], RangeRel::Ge), Some((vec![0xFF], None)));
        assert_eq!(cuts(&[], &[0xFF], RangeRel::Lt), Some((vec![0x00], Some(vec![0xFF]))));
        assert_eq!(cuts(&[], &[0xFF], RangeRel::Le), Some((vec![0x00], None)));
    }

    #[test]
    fn eq_prefix_interior_value() {
        // eq = 0x07, d = 0x05 (stride = 2). Cut keys stay within / cap at the group.
        let eq = [0x07];
        assert_eq!(
            cuts(&eq, &[0x05], RangeRel::Gt),
            Some((vec![0x07, 0x06], Some(vec![0x08, 0x00])))
        );
        assert_eq!(
            cuts(&eq, &[0x05], RangeRel::Ge),
            Some((vec![0x07, 0x05], Some(vec![0x08, 0x00])))
        );
        assert_eq!(
            cuts(&eq, &[0x05], RangeRel::Lt),
            Some((vec![0x07, 0x00], Some(vec![0x07, 0x05])))
        );
        assert_eq!(
            cuts(&eq, &[0x05], RangeRel::Le),
            Some((vec![0x07, 0x00], Some(vec![0x07, 0x06])))
        );
    }

    #[test]
    fn eq_prefix_ripple_into_eq() {
        // eq = 0x07, d = 0xFF (maximal slot). The Le end and the Ge end both
        // ripple to the next eq group's first key 0x08‖0x00 — byte-identical.
        let eq = [0x07];
        assert_eq!(
            cuts(&eq, &[0xFF], RangeRel::Le),
            Some((vec![0x07, 0x00], Some(vec![0x08, 0x00])))
        );
        assert_eq!(
            cuts(&eq, &[0xFF], RangeRel::Ge),
            Some((vec![0x07, 0xFF], Some(vec![0x08, 0x00])))
        );
        // Gt of the maximal slot in a non-maximal group: start == end == the next
        // group's key, a zero-width and so provably-empty interval.
        assert_eq!(cuts(&eq, &[0xFF], RangeRel::Gt), None);
    }

    #[test]
    fn eq_prefix_maximal_group_scans_to_end() {
        // eq = 0xFF (last eq group): the Gt/Ge end carries out of the eq prefix →
        // scan to the table end (None), not a wrapped lower key.
        let eq = [0xFF];
        assert_eq!(cuts(&eq, &[0x05], RangeRel::Ge), Some((vec![0xFF, 0x05], None)));
        assert_eq!(cuts(&eq, &[0x05], RangeRel::Gt), Some((vec![0xFF, 0x06], None)));
    }
}
