//! Incremental REDUCE operator: δ_out = Agg(history + δ_in) − Agg(history).

use crate::schema::key::pk_bytes_eq;
use crate::schema::key::NarrowPkOpk;
use crate::storage::{Batch, MemBatch, ReadCursor};

use super::super::reindex::ReindexPacker;
use super::agg::{apply_agg_from_value_index, fold_old_aggs, read_old_minmax_encoded, Accumulator};
use super::emit::{emit_global_ground, emit_reduce_row};
use super::plan::ReducePlan;
use super::sort::{argsort_delta, argsort_pk_canonical, compare_by_group_cols};
use gnitz_wire::AggFunc;

/// The history a non-linear (MIN/MAX) reduce consults: the combined
/// aggregate-value index's cursor, together with the packer that spells a
/// group's key into the prefix that cursor seeks.
///
/// The two arrive as one value because they are one fact. A cursor without its
/// packer — or a `ReducePlan` flag claiming an index the caller did not open —
/// is a state nothing produces, and the operator used to re-prove that at six
/// sites. `None` *is* "every aggregate is linear": the two are the same bit
/// (`AggFunc::is_linear` and `uses_value_index` are disjoint and exhaustive, and
/// every group set is indexable), which [`ReducePlan::new`] asserts.
pub(crate) struct AviHistory<'a> {
    pub cursor: &'a mut ReadCursor,
    pub packer: &'a ReindexPacker,
}

/// Upper bound on the per-group delta rows the AVI probe-skip path pre-steps into
/// a MIN/MAX accumulator. Pre-stepping is O(positive delta rows); the probe it
/// saves is a fixed O(log N) seek, so beyond this many delta rows in a group the
/// pre-step costs more than the probe. A group longer than the cap force-probes
/// (its partial pre-stepped accumulator is overwritten by the probe, so the cap
/// is correctness-neutral). Keeps the optimization net-beneficial across the
/// whole grouping-cardinality range — a low-cardinality GROUP BY, a boot backfill
/// (whole table in one epoch), or a large single-key batch never pays an
/// unbounded pre-step to save one cheap probe.
const SKIP_TRACK_CAP: usize = 128;

/// Walk one group's delta rows — visit positions `start..` in group-visit
/// order, mapped to batch rows by `row_of` — stepping the linear (and, on the
/// AVI pre-step path, MIN/MAX) accumulators. Returns the first position past
/// the group and whether any row's weight was ≤ 0. Generic over the row mapper
/// so the pre-sorted natural-PK path instantiates a direct positional walk
/// (no per-row indirection or `Option` branch) while the argsort paths keep
/// their indexed walk — byte-identical bodies, dispatched once per group.
/// `#[inline(always)]` is what keeps that per-group dispatch monomorphic.
#[inline(always)]
fn walk_group_rows(
    row_of: impl Fn(usize) -> usize,
    mb: &MemBatch,
    plan: &ReducePlan,
    start: usize,
    group_start_idx: usize,
    group_pk_bytes: &[u8],
    accs: &mut [Accumulator],
) -> (usize, bool) {
    let n = mb.count;
    // Per group: has any consolidated delta row weight ≤ 0? A retraction is
    // the only thing that can make a MIN/MAX extreme recede, so an all-insert
    // group can skip the AVI probe and fold `combine(old, pos)` instead.
    let mut saw_negative = false;
    let mut idx = start;
    while idx < n {
        let curr_idx = row_of(idx);
        if plan.pk_is_group_key {
            // Full PK byte-window compare: exact at every width, unlike the
            // lossy u128 get_pk for pk_stride > 16.
            if !pk_bytes_eq(mb.get_pk_bytes(curr_idx), group_pk_bytes) {
                break;
            }
        } else if compare_by_group_cols(mb, curr_idx, mb, group_start_idx, &plan.sort_descs)
            != std::cmp::Ordering::Equal
        {
            break;
        }

        let w = mb.get_weight(curr_idx);
        if w <= 0 {
            saw_negative = true;
        }
        // Pre-step the delta's inserts into the MIN/MAX accumulators so each
        // holds `pos` (the extreme over the group's positive-weight, non-NULL
        // delta rows) for the skip path; the probe path overwrites it. Gated
        // once per row (independent of which accumulator): only on the AVI
        // path, only for positive rows, only while the group stays all-insert
        // and under the pre-step cap — beyond it the group force-probes and the
        // partial `pos` is discarded. A mixed-in float MIN/MAX pre-stepped here
        // is likewise discarded by its unconditional probe below.
        let prestep_nonlinear = plan.track_nonlinear && w > 0 && !saw_negative && (idx - start) < SKIP_TRACK_CAP;
        for acc in accs.iter_mut() {
            if acc.is_linear() || prestep_nonlinear {
                acc.step_from_batch(mb, curr_idx, w);
            }
        }
        idx += 1;
    }
    (idx, saw_negative)
}

/// Incremental DBSP REDUCE: δ_out = Agg(history + δ_in) - Agg(history).
///
/// Everything that is a pure function of compile-time facts — schemas, group
/// columns, aggregate descriptors, linearity, key kind, the group-exemplar
/// locators, the global-ground flags — arrives baked in `plan` (one construction
/// site, `ReducePlan::new`).
pub(crate) fn op_reduce(
    delta: &Batch,
    trace_out_cursor: &mut ReadCursor,
    // The MIN/MAX history: the combined value index's cursor and the packer that
    // keys it. `None` iff every aggregate is linear.
    mut history: Option<AviHistory<'_>>,
    plan: &ReducePlan,
) -> Batch {
    let input_schema = &plan.input_schema;
    let output_schema = &plan.output_schema;
    let agg_descs = &plan.agg_descs[..];
    let all_linear = plan.all_linear;
    let global_ground = plan.global_ground;
    // `AggFunc::is_linear` and `uses_value_index` are disjoint and exhaustive,
    // and every group set has a packed key the value index can hold — so "some
    // aggregate is non-linear" and "this reduce was handed a history" are the
    // same bit. Everything below reads one or the other and never re-derives.
    debug_assert_eq!(
        all_linear,
        history.is_none(),
        "a non-linear reduce is served by its value index; a linear one is handed none",
    );

    // Consolidate only for non-linear aggregates; linear aggregates work on raw delta.
    // Fast path (linear or already consolidated): borrow delta directly — no allocation.
    let cs = if all_linear {
        None
    } else {
        Batch::consolidate_if_needed(delta, input_schema)
    };
    let working: &Batch = cs.as_ref().unwrap_or(delta);

    let n = working.count;
    if n == 0 {
        // Seed site for the global-aggregate ground row. An empty source delivers
        // an empty delta every pad round, so any post-loop code is unreachable;
        // this is the only place to mint the one row SQL requires over a
        // never-populated / fully-retracted source. Idempotent and owner-guarded:
        //   * Only the V₀ owner seeds — every worker runs this pad, but exactly one
        //     is `worker_for_key(V₀)`. `i_am_owner` is always true
        //     for a replicated reduce (single-source-read from worker 0).
        //   * Only if `trace_out` has no row at V₀ yet — the reduce integrates its
        //     own output within the epoch and `bind_trace_cursors` rebuilds the
        //     cursor each epoch, so a prior pad's seed is visible here and never
        //     re-seeded → no weight-2 ground is constructible.
        if global_ground && plan.i_am_owner {
            // Batch-free: there is no row to read the key off.
            let v0 = NarrowPkOpk::new(gnitz_wire::global_group_key(), output_schema.pk_stride() as usize);
            let out_pk_bytes = v0.bytes();

            trace_out_cursor.seek_bytes(out_pk_bytes);
            let has_v0 = trace_out_cursor.valid && trace_out_cursor.current_pk_eq(out_pk_bytes);
            if !has_v0 {
                let mut raw_output = Batch::with_capacity(*output_schema, 1);
                emit_global_ground(&mut raw_output, out_pk_bytes, plan);
                return raw_output;
            }
        }
        return Batch::empty_with_schema(output_schema);
    }

    // `pk_is_group_key`: the input PK region IS the group key, so group
    // membership is one full-PK-window `pk_bytes_eq` (exact at every width) and
    // canonical PK order is group order. The group-detection loop walks rows in
    // iteration order and breaks on mismatch — sound iff iteration order is that
    // order. A `sorted_verified` input already is (consolidation, integrated
    // trace, sorted union); otherwise `argsort_pk_canonical` puts it there.
    let group_descs = &plan.sort_descs[..];

    let mb = working.as_mem_batch();

    // A pre-sorted input under a PK group key needs no order at all — `None`
    // visits positions directly and skips the identity-Vec allocation.
    let sorted_indices: Option<Vec<u32>> = if plan.pk_is_group_key {
        (!working.sorted_verified(input_schema)).then(|| argsort_pk_canonical(&mb))
    } else {
        Some(argsort_delta(working, group_descs))
    };

    // Seed at the delta row count. The epoch emits a retract + a new row per
    // changed group and a group needs at least one delta row, so `n` is within
    // one doubling of the worst case and exact when every group is a singleton —
    // where seeding at a constant took ~13 growths for a backfill chunk, each one
    // scatter-copying every live byte into a fresh arena.
    let mut raw_output = Batch::with_capacity(*output_schema, n);

    let mut accs: Vec<Accumulator> = agg_descs
        .iter()
        .zip(&plan.agg_locs)
        .map(|(d, &loc)| Accumulator::new(d, loc))
        .collect();
    // Output width of each trailing agg column — the trace read-back offset
    // stride and slice length before `readback_agg_bits` rebuilds the 8-byte
    // accumulator (width-gated, so a float MIN/MAX widened to F64 is read
    // verbatim).
    let agg_col_widths = &plan.agg_col_widths[..];
    // First aggregate column's logical index (the aggregates are the trailing
    // output columns, so this holds at any PK arity). Each aggregate's null bit
    // is resolved from this logical index via `ReadCursor::col_is_null`.
    let cbase = plan.cbase;

    // A group exists iff its net cardinality (row weight) is positive; the unique
    // AggFunc::Count accumulator carries that signal (baked by `ReducePlan::new`,
    // where the planner's companion-COUNT promise is local; `None` degrades a
    // genuinely count-less reduce to the touched-ness test below).
    let cardinality_idx: Option<usize> = plan.cardinality_idx.map(|i| i as usize);

    let mut idx = 0usize;
    let mut num_groups = 0usize;
    // Debug tripwire: on the monotone paths assert out_pk_bytes strictly
    // ascends in group-visit order, so a comparator/key-encoding divergence
    // surfaces as a test failure rather than a silent `advance_to`
    // degrade-to-rescan perf regression.
    #[cfg(debug_assertions)]
    let mut prev_out_pk: Vec<u8> = Vec::new();
    while idx < n {
        let group_start_pos = idx;
        let group_start_idx = match &sorted_indices {
            Some(order) => order[group_start_pos] as usize,
            None => group_start_pos,
        };

        // The input row's PK bytes — the group-membership compare, and (where the
        // two coincide) the emitted row's key.
        let group_pk_bytes = mb.get_pk_bytes(group_start_idx);

        // The group's *output* PK bytes, materialised once for both the trace_out
        // retraction seek and the emitted row (so the two can never drift).
        // `out_pk_is_in_pk` — the output PK region *is* the input's, laid out by
        // `push_pk_of` — copies it verbatim, exact at every width and arity. The
        // other kinds key the output by a value the input PK does not carry (a
        // single natural group column, or the synthetic fold), whose ≤ 16-byte
        // OPK image `NarrowPkOpk` writes at the output stride.
        let narrow_out_pk;
        let out_pk_bytes: &[u8] = if plan.out_pk_is_in_pk {
            group_pk_bytes
        } else {
            narrow_out_pk = NarrowPkOpk::new(
                plan.group_key.key_row(&mb, group_start_idx),
                output_schema.pk_stride() as usize,
            );
            narrow_out_pk.bytes()
        };

        // Strict `<`: consecutive groups are comparator-distinct and the
        // monotone key forms are injective on the compared bytes, so an equal
        // consecutive key is itself an encoding/comparator divergence — exactly
        // what this catches. All slices share the loop-invariant output stride,
        // so slice-lex order is OPK order.
        #[cfg(debug_assertions)]
        if plan.monotone_out_pk {
            assert!(
                prev_out_pk.is_empty() || prev_out_pk.as_slice() < out_pk_bytes,
                "monotone group key must strictly ascend in group-visit order",
            );
            out_pk_bytes.clone_into(&mut prev_out_pk);
        }

        // Step accumulators over the group's delta rows (the Some/None dispatch
        // is per group; each arm is a monomorphic walk with no per-row branch).
        for acc in accs.iter_mut() {
            acc.reset();
        }
        let (group_end, saw_negative) = match &sorted_indices {
            Some(order) => walk_group_rows(
                |i| order[i] as usize,
                &mb,
                plan,
                group_start_pos,
                group_start_idx,
                group_pk_bytes,
                &mut accs,
            ),
            None => walk_group_rows(
                |i| i,
                &mb,
                plan,
                group_start_pos,
                group_start_idx,
                group_pk_bytes,
                &mut accs,
            ),
        };
        idx = group_end;
        // A group longer than the cap force-probes below. The pre-step gate and
        // this share the per-group row span (`idx - group_start_pos`): pre-step
        // covers positions `0..SKIP_TRACK_CAP`, and a longer group is `capped`, so
        // the two never disagree — an un-capped group has every positive row in
        // its accumulator.
        let capped = (idx - group_start_pos) > SKIP_TRACK_CAP;

        // Retraction: read old value from trace_out, keyed by the group's output
        // PK (`out_pk_bytes`). `advance_to` seeds each source's search at its live
        // position, so a monotone visit order sweeps forward; a hashed group key
        // visits out of order, which costs the skip but not correctness.
        trace_out_cursor.advance_to(out_pk_bytes);
        let has_old = trace_out_cursor.valid && trace_out_cursor.current_pk_eq(out_pk_bytes);

        if has_old {
            // δ_out's −Agg(history) term IS the stored output row: copy trace_out's
            // current row at weight -1. Byte-identical (PK, payload, null bits,
            // blobs) by construction, so it consolidates against the row it cancels
            // with zero per-column reconstruction and no null plumbing.
            trace_out_cursor.copy_current_row_into(&mut raw_output, -1);
        }

        // New value calculation. `new = old + delta` for the linear
        // accumulators: fold the old aggregates straight off the still-positioned
        // trace cursor (a no-op for a new group). `fold_old_aggs` skips the
        // non-linear ones — the value index owns each MIN/MAX and overwrites it
        // below.
        if has_old {
            fold_old_aggs(&mut accs, trace_out_cursor, agg_col_widths, cbase);
        }
        if let Some(h) = history.as_mut() {
            // Gather the group key once into a buffer with one spare trailing
            // byte for the per-aggregate ordinal; then for each non-linear
            // aggregate write its ordinal and prefix-seek `group ‖ ordinal`,
            // seeding the post-delta extreme (or resetting on an empty seek).
            // `for_max`/type come from `agg_descs[k]`; the ordinal `j` is the
            // aggregate's position in non-linear-descriptor order, matching the
            // index's write side.
            let mut gk = [0u8; crate::schema::MAX_PK_BYTES];
            let gstride = h.packer.out_stride;
            h.packer.pack_into(&mut gk[..gstride], &mb, group_start_idx);
            // Ordinal `j` is the position among the value-indexed aggregates
            // in descriptor order — selected by the same `uses_value_index`
            // predicate, in the same order, the index write side used, so the
            // two agree by construction.
            for (j, (k, d)) in agg_descs
                .iter()
                .enumerate()
                .filter(|(_, d)| d.agg_op.uses_value_index())
                .enumerate()
            {
                // The source type lives on the accumulator, resolved from the
                // same locator the index write side read the value through.
                let src_tc = accs[k].type_code();
                // Skip the AVI probe for an all-insert integer group that
                // already has a stored extreme: a MIN/MAX extreme can only
                // recede on a retraction, so an existing group's new extreme is
                // `combine(old, pos)`. `old` (= extreme(I_pre), the previously
                // emitted value) is under the still-positioned trace_out cursor,
                // and `pos` (the delta's positive-row extreme) is already in
                // `accs[k]`, pre-stepped in the group walk. Probe otherwise —
                // a retraction, a float source, a capped group, or a new group
                // (`!has_old`, no `old` to combine; the index is its own source
                // of truth).
                if saw_negative || src_tc.is_float() || capped || !has_old {
                    gk[gstride] = j as u8;
                    apply_agg_from_value_index(h.cursor, &gk[..gstride + 1], d.agg_op == AggFunc::Max, &mut accs[k]);
                } else if let Some(enc) = read_old_minmax_encoded(
                    // `accs[k]` holds `pos` (or is untouched → NULL); fold in
                    // `old`, read off the trace_out cursor already positioned by
                    // the `has_old` seek and left in place by `fold_old_aggs`
                    // above — a column read, not a seek. A NULL `old`
                    // (previously all-NULL group) folds nothing, leaving `pos`.
                    trace_out_cursor,
                    cbase + k,
                    agg_col_widths[k],
                    src_tc,
                ) {
                    accs[k].merge_encoded_extreme(enc);
                }
            }
        }

        // Emission: a group exists iff its net cardinality is positive. With a
        // COUNT signal (every grouped or global scalar planner reduce): read it —
        // correct for emptied groups (the folded companion nets to 0), new all-NULL
        // groups (the count is positive, the value accumulators untouched →
        // SUM/MIN/MAX render NULL while COUNT(col) renders `0`), and surviving
        // groups. Without one (the range-join
        // threshold reduce, or a companion-less low-level circuit): fall back to
        // the any_nonzero touched-ness test.
        let should_emit = match cardinality_idx {
            Some(ci) => {
                // Bag-positive reduce inputs ⇒ cardinality ≥ 0; fail loudly if a
                // future operator ever violates that, since the gate is not
                // sign-robust the way the old any_nonzero touched-ness test was.
                debug_assert!(
                    accs[ci].count_value() >= 0,
                    "reduce input must be bag-positive: negative group cardinality",
                );
                accs[ci].count_value() > 0
            }
            None => accs.iter().any(|a| !a.is_untouched()),
        };
        if should_emit {
            emit_reduce_row(&mut raw_output, (&mb, group_start_idx), out_pk_bytes, &accs, plan);
        } else if global_ground {
            // The emission gate shed the computed row (this group emptied, or a
            // lone all-NULL MIN/MAX). For the user's ungrouped scalar aggregate
            // this is THE one logical group, and SQL requires exactly one row, so
            // emit the ground (COUNT=0, SUM/MIN/MAX/AVG=NULL) in its place. The
            // pre-existing `has_old` retraction above already copied whatever was
            // at trace_out@V₀ at weight −1, so computed→ground, ground→computed,
            // and value-change ticks all net to one row. (Grouped reduces have
            // `global_ground = false` and correctly emit nothing here.)
            emit_global_ground(&mut raw_output, out_pk_bytes, plan);
        }

        num_groups += 1;
    }

    gnitz_debug!("op_reduce: in={} groups={} out={}", n, num_groups, raw_output.count);

    // A reduce tick emits, per changed group, the old aggregate row @ -1 then the
    // new aggregate row @ +1, in emit order — neither (PK, payload)-sorted (a
    // decreasing aggregate, e.g. MAX after deleting the current max, yields a
    // descending old/new pair) nor ghost-free (an unchanged re-emit yields a
    // net-zero pair). The output is thus an unconsolidated delta, carried by the
    // `Raw` constructor default (the `extend_*` emit path never raises the layout),
    // so a downstream consumer (e.g. the reduce→exchange relay) re-folds it rather
    // than trusting a false claim.
    raw_output
}
