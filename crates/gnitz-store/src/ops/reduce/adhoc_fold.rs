//! Ad-hoc aggregation hash-fold — the stateless per-worker sink behind a
//! fold-sink `ReadSpec` (single-relation GROUP BY / global aggregate / DISTINCT
//! over the committed base). No DBSP circuit, no operator-trace tables, no
//! exchange: each surviving scan chunk folds into per-group accumulators in
//! bounded RAM, then one partial reduce-output batch is emitted after full
//! accumulation.
//!
//! Semantics parity with a CREATE VIEW of the same statement is **structural**,
//! not reimplemented: the accumulation kernel (`Accumulator::step_from_batch`),
//! the group comparator (`cmp_group_cols`), the group-key hash
//! (`GroupKeyCols`), and the emission path (`emit_reduce_row` driven by a
//! `ReducePlan`) are the exact shared code a view's reduce runs, so partial agg
//! columns are byte-identical to a view's reduce output.
//!
//! Exactness over the committed base: the scan cursor delivers consolidated,
//! positive-net-weight rows (ghosts excluded, base tables DML-forced
//! non-negative), so with no history there is no retraction arithmetic — the
//! accumulator over such rows *is* the aggregate. The fold has no `should_emit`
//! gate: a grouped fold emits one partial per present group (over positive
//! weights a present group always has net cardinality > 0, matching the view),
//! and a global fold its one row, over no input included. It carries no COUNT(*)
//! cardinality companion — that is a `should_emit` signal the stateless fold does
//! not need.

use std::cmp::Ordering;

use gnitz_expr::cmp_group_cols;
use rustc_hash::FxHashMap;

use gnitz_wire::AggReadSpec;

use super::agg::Accumulator;

use super::emit::{emit_global_ground, emit_reduce_row};
use super::plan::ReducePlan;
use crate::schema::key::NarrowPkOpk;
use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, StoreError};

/// The end of a [`AdhocFold::same_key`] chain.
const NO_GROUP: u32 = u32::MAX;

/// The request-scoped fold state.
pub(crate) struct AdhocFold {
    /// The baked reduce plan — the single home of the schemas, group columns,
    /// the group keyer and comparator locators, the accumulator template, and
    /// the emission roles the fold reads (`ReducePlan::for_adhoc_fold` derives them
    /// once).
    plan: ReducePlan,
    /// Grouped folds only: one representative source row per group, in
    /// group-discovery order; the row index IS the group ordinal (and
    /// `rep_rows.count` the group count).
    /// `emit_reduce_row` reads the group columns from it, and the group key is
    /// re-derived from it at `finish` (a pure function of the group columns).
    rep_rows: Batch,
    /// Flat accumulator matrix: group `ord` owns
    /// `accs[ord * n_aggs .. (ord + 1) * n_aggs]`.
    accs: Vec<Accumulator>,
    /// Group key → the newest group ordinal carrying it. A digest can collide, so a
    /// match is confirmed by value unless the key is canonical.
    by_hash: FxHashMap<u128, u32>,
    /// Per ordinal, the next-older ordinal sharing its key ([`NO_GROUP`] ends
    /// it) — the 128-bit digest-collision chain a value confirmation walks.
    same_key: Vec<u32>,
    /// Same-group memo: the previous row's `(key, ordinal)`. Consecutive rows
    /// of one group — per cluster, when the scan is ordered by the group
    /// column — resolve without a map probe.
    last: Option<(u128, u32)>,
    group_cap: usize,
}

impl AdhocFold {
    /// The fold of `agg` over `src_schema`; [`ReducePlan::for_adhoc_fold`] rejects
    /// a spec the schema cannot serve.
    pub(crate) fn new(src_schema: &SchemaDescriptor, agg: &AggReadSpec, group_cap: usize) -> Result<Self, StoreError> {
        let plan = ReducePlan::for_adhoc_fold(src_schema, agg)
            .map_err(|e| StoreError::rejected(format!("scan_spec fold: {e}")))?;

        Ok(AdhocFold {
            accs: if plan.global_ground {
                plan.acc_template.clone()
            } else {
                Vec::new()
            },
            plan,
            rep_rows: Batch::empty_with_schema(src_schema),
            by_hash: FxHashMap::default(),
            same_key: Vec::new(),
            last: None,
            group_cap,
        })
    }

    /// The partial reduce-output layout [`Self::finish`] emits.
    pub(crate) fn output_schema(&self) -> &SchemaDescriptor {
        &self.plan.output_schema
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
            same_key,
            last,
            group_cap,
        } = self;
        let n_aggs = plan.acc_template.len();
        let mb = chunk.as_mem_batch();
        // Append onto the flat matrix, never a fresh `Vec` per group: the tail
        // grows amortized, where a per-group allocation would be one malloc per
        // group, up to the cap.
        let new_accs = |accs: &mut Vec<Accumulator>| accs.extend_from_slice(&plan.acc_template);
        if plan.global_ground {
            // Global aggregate: its one group's accumulators exist from `new` — no
            // per-row key hash, memo, or comparator probe.
            for row in ranges.iter().flat_map(|&(s, e)| s..e) {
                let w = mb.get_weight(row);
                debug_assert!(w > 0, "adhoc fold: scan cursor must deliver positive weights");
                if w <= 0 {
                    continue;
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
                    || cmp_group_cols(&mb, row, &rep_mb, ord as usize, plan.group_key.cols.locs()) == Ordering::Equal
            };
            let ord = match *last {
                Some((k, ord)) if k == key && same(ord) => ord,
                _ => {
                    let found = std::iter::successors(by_hash.get(&key).copied(), |&o| {
                        Some(same_key[o as usize]).filter(|&n| n != NO_GROUP)
                    })
                    .find(|&o| same(o));
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
                            same_key.push(by_hash.insert(key, ord as u32).unwrap_or(NO_GROUP));
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
    /// synthetic-fold reply layout and group-discovery order — or a global fold's
    /// one row, which a worker that saw no rows emits as the ground row.
    pub(crate) fn finish(self) -> Batch {
        let n_aggs = self.plan.acc_template.len();
        // The exact output row count is the group count — reserve once.
        let mut output = Batch::with_capacity(&self.plan.output_schema, self.rep_rows.count.max(1));
        if self.plan.global_ground {
            let v0 = NarrowPkOpk::new(gnitz_wire::global_group_key(), self.plan.output_schema.pk_stride());
            emit_global_ground(&mut output, v0.bytes(), &self.accs);
            return output;
        }
        let rep_mb = self.rep_rows.as_mem_batch();
        for ord in 0..self.rep_rows.count {
            // Synthetic `_group_pk`, off the retained representative row — a pure
            // function of its group columns.
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
