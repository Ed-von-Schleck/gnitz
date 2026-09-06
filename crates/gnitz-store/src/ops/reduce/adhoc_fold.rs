//! Ad-hoc aggregation hash-fold — the stateless per-worker sink behind a
//! fold-sink `ReadSpec` (single-relation GROUP BY / global aggregate / DISTINCT
//! over the committed base). No DBSP circuit, no operator-trace tables, no
//! exchange: each surviving scan chunk folds into per-group accumulators in
//! bounded RAM, then one partial reduce-output batch is emitted after full
//! accumulation.
//!
//! Semantics parity with a CREATE VIEW of the same statement is **structural**,
//! not reimplemented: the accumulation kernel (`Accumulator::step_from_batch`),
//! the group comparator (`compare_by_group_cols`), the group-key hash
//! (`GroupKeyCols`), and the emission path (`emit_reduce_row` driven by a
//! `ReducePlan`) are the exact shared code a view's reduce runs, so partial agg
//! columns are byte-identical to a view's reduce output.
//!
//! Exactness over the committed base: the scan cursor delivers consolidated,
//! positive-net-weight rows (ghosts excluded, base tables DML-forced
//! non-negative), so with no history there is no retraction arithmetic — the
//! accumulator over such rows *is* the aggregate. The fold has no `should_emit`
//! gate: it emits one partial per present group (over positive weights a present
//! group always has net cardinality > 0, matching the view). It carries no
//! COUNT(*) cardinality companion — that is a `should_emit` signal the stateless
//! fold does not need.

use std::cmp::Ordering;

use rustc_hash::FxHashMap;

use gnitz_wire::AggReadSpec;

use super::agg::Accumulator;

use super::emit::emit_reduce_row;
use super::plan::ReducePlan;
use super::sort::compare_by_group_cols;
use crate::schema::{ReduceOutKey, SchemaDescriptor};
use crate::storage::{Batch, StoreError};

/// The request-scoped fold state. `pub(crate)` so `read::scan_spec` can drive
/// it; every reduce building block it composes is reached at `pub(super)` from
/// this descendant of `ops::reduce`.
pub(crate) struct AdhocFold {
    /// The baked reduce plan — the single home of the schemas, group columns,
    /// the group keyer and comparator locators, the accumulator template, and
    /// the emission roles the fold reads (`ReducePlan::new` derives them once).
    plan: ReducePlan,
    /// One representative source row per group, in group-discovery order; the
    /// row index IS the group ordinal (and `rep_rows.count` the group count).
    /// `emit_reduce_row` reads the group columns from it, and the group key is
    /// re-derived from it at `finish` (a pure function of the group columns).
    rep_rows: Batch,
    /// Flat accumulator matrix: group `ord` owns
    /// `accs[ord * n_aggs .. (ord + 1) * n_aggs]`.
    accs: Vec<Accumulator>,
    /// Group key → group ordinals sharing it (FxHash — the key is already a
    /// uniform 128-bit XXH3 digest, so no second strong hash is needed). A
    /// hashed key is confirmed by value with `compare_by_group_cols` before a
    /// row joins a bucket, so two groups colliding on the digest stay distinct
    /// partials; a canonical key is injective and skips that confirmation.
    by_hash: FxHashMap<u128, Vec<u32>>,
    /// Same-group memo: the previous row's `(key, ordinal)`. Consecutive rows
    /// of one group — per cluster, when the scan is ordered by the group
    /// column — resolve without a map probe.
    last: Option<(u128, u32)>,
    group_cap: usize,
}

impl AdhocFold {
    /// Build the fold state from a decoded fold spec. The spec and the client's
    /// reply schema are a trust boundary: the column indices are range-checked
    /// here, the aggregate types and output width by `ReducePlan::new`, and the
    /// reply schema against the plan's own layout. (Aggregate-op validity is
    /// already decode-enforced — `AggDescriptor.agg_op` is typed.)
    pub(crate) fn new(
        src_schema: &SchemaDescriptor,
        reply_schema: &SchemaDescriptor,
        agg: &AggReadSpec,
        group_cap: usize,
    ) -> Result<Self, StoreError> {
        let n_cols = src_schema.num_columns();

        // `ReadSpec`'s decoder carries no schema, so this is the only place a
        // client-supplied column index meets a column count — and downstream
        // `build_reduce_output_schema` indexes a fixed array, reading a zeroed
        // slot rather than panicking.
        for &c in &agg.group_cols {
            if c as usize >= n_cols {
                return Err(StoreError::rejected(format!(
                    "scan_spec fold: group column {c} out of range ({n_cols} cols)"
                )));
            }
        }
        for d in &agg.aggs {
            if d.col_idx as usize >= n_cols {
                return Err(StoreError::rejected(format!(
                    "scan_spec fold: agg column {} out of range ({n_cols} cols)",
                    d.col_idx
                )));
            }
        }

        // The ad-hoc partial layout is ALWAYS synthetic-fold: `_agg_pk` U128 PK,
        // group cols as payload, then the agg partial columns — a pure function
        // of `(src_schema, agg)`, laid out by the same authority the compiler
        // lays every reduce output with. The client's reply schema must match it
        // physically (types + PK region; nullability is presentation) or the
        // frame is malformed.
        let plan = ReducePlan::new(
            src_schema,
            &agg.group_cols,
            &agg.aggs,
            ReduceOutKey::SyntheticFold,
            false, // global_ground — the client synthesizes the empty-input ground row
            false, // i_am_owner
        )
        .map_err(StoreError::rejected)?;
        if !reply_schema.same_physical_layout(&plan.output_schema) {
            return Err(StoreError::rejected(
                "scan_spec fold: reply schema does not match the derived fold layout",
            ));
        }

        Ok(AdhocFold {
            plan,
            rep_rows: Batch::empty_with_schema(src_schema),
            accs: Vec::new(),
            by_hash: FxHashMap::default(),
            last: None,
            group_cap,
        })
    }

    /// Fold every `[start, end)` row range of one source chunk into the group
    /// state — the caller passes the filter's surviving row ranges directly, so
    /// no survivor batch is materialized. `Err` on exceeding the per-worker group
    /// cap.
    ///
    /// The whole list rather than one range: a fragmented survivor list would
    /// otherwise repay the per-call setup — the state destructure, the keyer and
    /// the two batch views — once per range instead of once per chunk.
    pub(crate) fn fold_ranges(&mut self, chunk: &Batch, ranges: &[(usize, usize)]) -> Result<(), StoreError> {
        let Self {
            plan,
            rep_rows,
            accs,
            by_hash,
            last,
            group_cap,
        } = self;
        let n_aggs = plan.acc_template.len();
        let mb = chunk.as_mem_batch();
        // Append onto the flat matrix, never a fresh `Vec` per group: the tail
        // grows amortized, where a per-group allocation would be one malloc per
        // group, up to the cap.
        let new_accs = |accs: &mut Vec<Accumulator>| accs.extend_from_slice(&plan.acc_template);
        if plan.group_key.cols.is_empty() {
            // Global aggregate: one group at ordinal 0, created on the first
            // surviving row — no per-row key hash, memo, or comparator probe.
            for row in ranges.iter().flat_map(|&(s, e)| s..e) {
                let w = mb.get_weight(row);
                debug_assert!(w > 0, "adhoc fold: scan cursor must deliver positive weights");
                if w <= 0 {
                    continue;
                }
                if rep_rows.count == 0 {
                    rep_rows.append_batch(chunk, row, row + 1);
                    new_accs(accs);
                }
                for acc in &mut accs[..n_aggs] {
                    acc.step_from_batch(&mb, row, w);
                }
            }
            return Ok(());
        }
        // A canonical key is `route_key` over one non-nullable `is_pk_eligible`
        // column, so key equality *is* group equality and the value confirmation
        // below cannot change the answer.
        let injective_key = plan.group_key.canonical_col().is_some();
        // Rebuilt only when a group insert mutates `rep_rows` — never per row.
        let mut rep_mb = rep_rows.as_mem_batch();
        for row in ranges.iter().flat_map(|&(s, e)| s..e) {
            let w = mb.get_weight(row);
            // The scan cursor delivers consolidated, positive-net-weight rows
            // (ghosts excluded); the kernel's extreme arm asserts the same, and
            // `op_reduce`'s group walk guards with the same `w > 0`.
            debug_assert!(w > 0, "adhoc fold: scan cursor must deliver positive weights");
            if w <= 0 {
                continue;
            }
            let key = plan.group_key.key_row(&mb, row);
            // A hashed key is confirmed by value, on the memo hit and the bucket
            // probe alike, so a digest collision can never merge two groups.
            let same = |ord: u32| {
                injective_key
                    || compare_by_group_cols(&mb, row, &rep_mb, ord as usize, plan.group_key.cols.locs())
                        == Ordering::Equal
            };
            let ord = match *last {
                Some((k, ord)) if k == key && same(ord) => ord,
                _ => {
                    let found = by_hash.get(&key).and_then(|b| b.iter().copied().find(|&o| same(o)));
                    let ord = match found {
                        Some(ord) => ord,
                        None => {
                            // A new group. The cap is per-worker (a worker sees
                            // a subset of the global groups), so it never fires
                            // when the global group count ≤ cap; firing is a
                            // stated resource-exhaustion abort, not silent
                            // degradation.
                            let ord = rep_rows.count;
                            if ord >= *group_cap {
                                return Err(StoreError::rejected(format!(
                                    "GROUP BY exceeds {group_cap} distinct groups for ad-hoc execution; \
                                     CREATE VIEW to maintain this aggregation incrementally"
                                )));
                            }
                            rep_rows.append_batch(chunk, row, row + 1);
                            new_accs(accs);
                            by_hash.entry(key).or_default().push(ord as u32);
                            rep_mb = rep_rows.as_mem_batch();
                            ord as u32
                        }
                    };
                    *last = Some((key, ord));
                    ord
                }
            };
            let ord = ord as usize;
            for acc in &mut accs[ord * n_aggs..(ord + 1) * n_aggs] {
                acc.step_from_batch(&mb, row, w);
            }
        }
        Ok(())
    }

    /// Emit one partial reduce-output row per present group (weight +1), in the
    /// synthetic-fold reply layout and group-discovery order. A worker that saw
    /// no rows emits an empty batch (the client synthesizes the global ground
    /// row when needed).
    pub(crate) fn finish(self) -> Batch {
        let n_aggs = self.plan.acc_template.len();
        // The exact output row count is the group count — reserve once.
        let mut output = Batch::with_capacity(&self.plan.output_schema, self.rep_rows.count.max(1));
        let rep_mb = self.rep_rows.as_mem_batch();
        for ord in 0..self.rep_rows.count {
            // Synthetic `_agg_pk`, off the retained representative row — a pure
            // function of its group columns. An empty group set folds to
            // `global_group_key()`, the same V₀ a view's ground row lands on.
            let pk = self.plan.out_pk(&rep_mb, ord);
            emit_reduce_row(
                &mut output,
                (&rep_mb, ord),
                pk.bytes(),
                &self.accs[ord * n_aggs..(ord + 1) * n_aggs],
                &self.plan,
            );
        }
        output
    }
}

#[cfg(test)]
#[path = "tests/adhoc_fold.rs"]
mod tests;
