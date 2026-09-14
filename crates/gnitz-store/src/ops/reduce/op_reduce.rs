//! Incremental REDUCE operator: δ_out = Agg(history + δ_in) − Agg(history).

use crate::schema::key::pk_bytes_eq;
use crate::schema::key::NarrowPkOpk;
use crate::schema::{ColumnLocator, ReduceOutKey};
use crate::storage::{Batch, MemBatch, ReadCursor};

use super::agg::Accumulator;
use super::emit::{emit_global_ground, emit_reduce_row};
use super::plan::ReducePlan;
use super::sort::{argsort_delta, argsort_pk_canonical};
use gnitz_expr::cmp_group_cols;

/// Per-group delta rows the AVI probe-skip path will pre-step into a MIN/MAX
/// accumulator before giving up and probing. Pre-stepping is O(positive rows),
/// the probe it saves is a fixed O(log N) seek, so past this many rows the
/// pre-step costs more than the probe. Correctness-neutral: a capped group's
/// partial accumulator is overwritten by the probe that follows.
const SKIP_TRACK_CAP: usize = 128;

/// Whether a row still belongs to the group being walked. Built by
/// [`Self::for_group`] from the plan and the group's own first row, so the
/// membership rule and the exemplar it compares against cannot be handed in
/// separately and disagree.
enum GroupBoundary<'a> {
    /// The input PK region **is** the group key (`ReduceOutKey::PkPermutation`):
    /// the full PK byte window, exact at every width where a lossy `get_pk`
    /// u128 would not be.
    Pk(&'a [u8]),
    /// Group columns, compared by value against the group's first row.
    Cols {
        descs: &'a [ColumnLocator],
        exemplar: usize,
    },
    /// No group columns: one group spanning the whole delta.
    Single,
}

impl<'a> GroupBoundary<'a> {
    #[inline(always)]
    fn for_group(plan: &'a ReducePlan, mb: &'a MemBatch, first_row: usize) -> Self {
        if plan.out_key == ReduceOutKey::PkPermutation {
            GroupBoundary::Pk(mb.get_pk_bytes(first_row))
        } else if plan.group_key.cols.is_empty() {
            GroupBoundary::Single
        } else {
            GroupBoundary::Cols {
                descs: plan.group_key.cols.locs(),
                exemplar: first_row,
            }
        }
    }

    #[inline(always)]
    fn holds(&self, mb: &MemBatch, row: usize) -> bool {
        match *self {
            GroupBoundary::Pk(pk) => pk_bytes_eq(mb.get_pk_bytes(row), pk),
            GroupBoundary::Cols { descs, exemplar } => {
                cmp_group_cols(mb, row, mb, exemplar, descs) == std::cmp::Ordering::Equal
            }
            GroupBoundary::Single => true,
        }
    }
}

/// Walk one group's delta rows — positions `start..`, mapped to batch rows by
/// `row_of` — stepping the accumulators. Returns the first position past the
/// group and whether any row's weight was ≤ 0. Generic over `row_of` so the
/// pre-sorted path walks positions directly.
///
/// `prestep_extremes` also steps the MIN/MAX accumulators, leaving each holding
/// the extreme over the group's positive rows for the AVI probe-skip path. The
/// probe overwrites that whenever it runs, so this is a cost gate, never a
/// correctness one.
#[inline(always)]
fn walk_group_rows(
    row_of: impl Fn(usize) -> usize,
    mb: &MemBatch,
    boundary: &GroupBoundary<'_>,
    prestep_extremes: bool,
    start: usize,
    accs: &mut [Accumulator],
) -> (usize, bool) {
    let n = mb.count;
    // A retraction is the only thing that can make a MIN/MAX extreme recede, so
    // an all-insert group can skip the AVI probe and fold `combine(old, pos)`.
    let mut saw_negative = false;
    let mut idx = start;
    while idx < n {
        let curr_idx = row_of(idx);
        if !boundary.holds(mb, curr_idx) {
            break;
        }

        let w = mb.get_weight(curr_idx);
        if w <= 0 {
            saw_negative = true;
        }
        // Past the cap the group force-probes, discarding whatever `pos` it has;
        // so does a group that has seen a retraction, or a float extreme.
        let prestep = prestep_extremes && w > 0 && !saw_negative && (idx - start) < SKIP_TRACK_CAP;
        for acc in accs.iter_mut() {
            if acc.is_linear() || prestep {
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
/// site, `ReducePlan::build`).
pub fn op_reduce(
    delta: &Batch,
    trace_out_cursor: &mut ReadCursor,
    // A cursor over the combined value index `plan.avi` describes, opened by the
    // caller after this epoch's entries were populated into it. `Some` exactly
    // when `plan.avi` is.
    mut history: Option<&mut ReadCursor>,
    plan: &ReducePlan,
) -> Batch {
    let input_schema = &plan.input_schema;
    let output_schema = &plan.output_schema;
    let global_ground = plan.global_ground;
    // The plan's value index *is* the "some aggregate is non-linear" bit;
    // everything below reads it and never re-derives linearity.
    let avi = plan.avi.as_ref();

    // Consolidate only for non-linear aggregates; linear aggregates work on raw delta.
    // Fast path (linear or already consolidated): borrow delta directly — no allocation.
    let cs = if avi.is_none() {
        None
    } else {
        Batch::consolidate_if_needed(delta, input_schema)
    };
    let working: &Batch = cs.as_ref().unwrap_or(delta);

    let n = working.count;
    if n == 0 {
        // The only place to mint the one row SQL requires over a never-populated
        // or fully-retracted source: an empty source delivers an empty delta every
        // pad round, so nothing past the group loop ever runs. Two guards make it
        // idempotent — one worker owns V₀ (`seeds_ground`), and the seed is skipped
        // if `trace_out` already carries a row there, which a prior pad's seed
        // does because the reduce integrates its own output within the epoch.
        if plan.seeds_ground {
            // Batch-free: there is no row to read the key off.
            let v0 = NarrowPkOpk::new(gnitz_wire::global_group_key(), output_schema.pk_stride());
            let out_pk_bytes = v0.bytes();

            trace_out_cursor.seek_bytes(out_pk_bytes);
            let has_v0 = trace_out_cursor.valid && trace_out_cursor.current_pk_eq(out_pk_bytes);
            if !has_v0 {
                let mut raw_output = Batch::with_capacity(output_schema, 1);
                emit_global_ground(&mut raw_output, out_pk_bytes, &plan.acc_template);
                return raw_output;
            }
        }
        return Batch::empty_with_schema(output_schema);
    }

    let mb = working.as_mem_batch();

    // The visit order, following `GroupBoundary::for_group`'s three cases.
    // `PkPermutation`: canonical PK order *is* group order, so a consolidated
    // input (consolidation, integrated trace, merged union) needs no order at all
    // and `None` visits positions directly. One group: likewise no order.
    // Otherwise one keyed argsort.
    let ungrouped = plan.out_key != ReduceOutKey::PkPermutation && plan.group_key.cols.is_empty();
    let sorted_indices: Option<Vec<u32>> = if plan.out_key == ReduceOutKey::PkPermutation {
        (!working.consolidated_verified(input_schema)).then(|| argsort_pk_canonical(&mb))
    } else if ungrouped {
        None
    } else {
        Some(argsort_delta(&mb, &plan.group_key))
    };

    // Seed at the delta row count. The epoch emits a retract + a new row per
    // changed group and a group needs at least one delta row, so `n` is within
    // one doubling of the worst case and exact when every group is a singleton.
    // An ungrouped reduce is exact at 2 instead: it has one group, which emits at
    // most a retraction and a row.
    let mut raw_output = Batch::with_capacity(output_schema, if ungrouped { 2 } else { n });

    let mut accs: Vec<Accumulator> = plan.acc_template.clone();

    let cardinality = plan.cardinality_idx.expect("from_wire refuses a count-less reduce") as usize;

    // The AVI seek prefix `group ‖ ordinal`, hoisted out of the group loop: each
    // group's pack fully overwrites the key's group span and each ordinal
    // overwrites the byte behind it, so reuse needs no inter-group clear — where
    // a per-group declaration is an 80-byte zero store per group at
    // `opt-level = 0`.
    let mut gk = [0u8; crate::schema::MAX_PK_BYTES];
    let mut idx = 0usize;
    let mut num_groups = 0usize;
    // Every path visits in ascending output-PK order: the PK paths in canonical
    // PK order, the keyed argsort in group-key order (the output PK being that
    // key's big-endian image). `seek_pk_group_ascending` is what turns a
    // key-encoding divergence into a test failure rather than a silent misprobe.
    while idx < n {
        let group_start_pos = idx;
        let group_start_idx = match &sorted_indices {
            Some(order) => order[group_start_pos] as usize,
            None => group_start_pos,
        };

        // Materialised once for both the retraction seek and the emitted row, so
        // the two cannot drift.
        let out_pk = plan.out_pk(&mb, group_start_idx);
        let out_pk_bytes: &[u8] = out_pk.bytes();

        // Step accumulators over the group's delta rows (the Some/None dispatch
        // is per group; each arm is a monomorphic walk).
        for acc in accs.iter_mut() {
            acc.reset();
        }
        let boundary = GroupBoundary::for_group(plan, &mb, group_start_idx);
        let (group_end, saw_negative) = match &sorted_indices {
            Some(order) => walk_group_rows(
                |i| order[i] as usize,
                &mb,
                &boundary,
                plan.track_nonlinear,
                group_start_pos,
                &mut accs,
            ),
            None => walk_group_rows(|i| i, &mb, &boundary, plan.track_nonlinear, group_start_pos, &mut accs),
        };
        idx = group_end;
        // Shares the row span with the walk's pre-step gate, so the two cannot
        // disagree: an un-capped group has every positive row in its accumulator.
        let capped = (idx - group_start_pos) > SKIP_TRACK_CAP;

        // Retraction: the old value from trace_out, keyed by the group's output
        // PK. The visit order ascends, so a group this reduce has never emitted —
        // every group of a first epoch — costs one comparison.
        let has_old = trace_out_cursor.seek_pk_group_ascending(out_pk_bytes);

        if has_old {
            // δ_out's −Agg(history) term IS the stored output row: copy trace_out's
            // current row at weight -1. Byte-identical (PK, payload, null bits,
            // blobs) by construction, so it consolidates against the row it cancels
            // with zero per-column reconstruction and no null plumbing.
            trace_out_cursor.copy_current_row_into(&mut raw_output, -1);
        }

        // New value calculation. The group's stored row — this reduce's own last
        // output for it — stays under the cursor the retraction seek positioned,
        // so both folds below are column reads rather than seeks.
        let stored = if has_old {
            Some(trace_out_cursor.current_row_source())
        } else {
            None
        };
        // `new = old + delta` for the linear accumulators. The non-linear ones
        // are skipped: the value index owns each MIN/MAX and the block below
        // overwrites it, so folding the old extreme here would be discarded.
        if let Some((stored_row, stored_idx)) = stored {
            for acc in accs.iter_mut().filter(|a| a.is_linear()) {
                acc.fold_stored(stored_row, stored_idx);
            }
        }
        if let Some(bake) = avi {
            let cursor = history
                .as_deref_mut()
                .expect("a value-indexed reduce is handed a cursor over the index its plan describes");
            // Pack the group key once; each ordinal then rewrites only the tail.
            bake.pack_group(&mut gk, &mb, group_start_idx);
            // An extreme only recedes on a retraction, so an all-insert group
            // that stayed under the pre-step cap holds `pos` — its own positive
            // rows' extreme — and needs no probe.
            let group_holds_pos = !saw_negative && !capped;
            for (j, (k, trace_foldable)) in bake.acc_indices().enumerate() {
                let acc = &mut accs[k];
                match stored {
                    // `combine(old, pos)`, `old` folded off the stored row. A
                    // NULL `old` folds nothing, leaving `pos`.
                    Some((stored_row, stored_idx)) if trace_foldable && group_holds_pos => {
                        acc.fold_stored(stored_row, stored_idx)
                    }
                    // Everything else probes, the index being its own source of truth.
                    _ => bake.seed_extreme(cursor, &mut gk, j, acc),
                }
            }
        }

        debug_assert!(
            accs[cardinality].count_value() >= 0,
            "reduce input must be bag-positive: negative group cardinality",
        );
        if accs[cardinality].count_value() > 0 {
            emit_reduce_row(&mut raw_output, (&mb, group_start_idx), out_pk_bytes, &accs, plan);
        } else if global_ground {
            // The gate shed the computed row, but an ungrouped scalar aggregate
            // must still publish exactly one row, so the ground replaces it. The
            // `has_old` retraction above already cancelled whatever stood at V₀,
            // so computed→ground, ground→computed and value changes all net to one.
            emit_global_ground(&mut raw_output, out_pk_bytes, &plan.acc_template);
        }

        num_groups += 1;
    }

    gnitz_debug!("op_reduce: in={} groups={} out={}", n, num_groups, raw_output.count);

    // Per changed group: the old row @ −1 then the new row @ +1, in emit order.
    // Neither (PK, payload)-sorted (a falling MAX yields a descending pair) nor
    // ghost-free (an unchanged re-emit nets to zero), so the batch keeps the `Raw`
    // layout its constructor gave it and a downstream consumer re-folds it.
    raw_output
}
