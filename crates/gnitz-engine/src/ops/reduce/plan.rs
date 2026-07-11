//! `ReducePlan` — everything `op_reduce` needs that is a pure function of
//! compile-time facts, baked once at emit time (the compiler's `emit_reduce`)
//! into `Program::reduce_plans`. The single construction site enforces the
//! coherence the old 13-parameter `op_reduce` signature spread across the
//! instruction operands; the per-epoch call re-derives nothing.

use crate::schema::{ColumnLocator, ReduceOutKey, SchemaDescriptor};

use super::agg::{AggDescriptor, AggOp};
use super::sort::{build_sort_descs, SortDesc};

/// Role of one reduce-output payload column, resolved at plan build so the
/// per-emitted-group loop does no `locate()` walk or bounds re-derivation.
#[derive(Clone, Copy)]
pub(super) enum OutColRole {
    /// Trailing aggregate column: `accs[k]`, emitted at output width `cs`.
    Agg { k: u8, cs: u8 },
    /// Group-exemplar column, read from the input exemplar row through a
    /// pre-resolved locator. `tc` is the column's type code (German-string
    /// dispatch), `cs` its width.
    Exemplar { loc: ColumnLocator, tc: u8, cs: u8 },
}

/// The baked per-instruction reduce plan. Input facts (schemas, group columns,
/// aggregate descriptors) plus every derived gate `op_reduce` previously
/// recomputed per epoch. Built by [`ReducePlan::new`] only.
pub struct ReducePlan {
    pub(crate) input_schema: SchemaDescriptor,
    pub(crate) output_schema: SchemaDescriptor,
    pub(crate) group_by_cols: Vec<u32>,
    pub(crate) agg_descs: Vec<AggDescriptor>,
    /// The planner's SQL-intent discriminator for the global-aggregate ground
    /// row, and the per-worker ownership of its seed (see `op_reduce`).
    pub(crate) global_ground: bool,
    pub(crate) i_am_owner: bool,
    // ── Derived (single home: `new`) ────────────────────────────────────────
    /// Every aggregate is linear (COUNT/SUM family): no history replay.
    pub(crate) all_linear: bool,
    /// GROUP BY is a permutation of the source PK columns.
    pub(crate) group_by_pk: bool,
    /// Groups are visited in ascending output-PK order, so the `trace_out`
    /// retraction probe can gallop from the live position.
    pub(crate) monotone_out_pk: bool,
    /// Pre-step MIN/MAX accumulators during the group walk for the AVI
    /// probe-skip path (only meaningful with an AVI; a float MIN/MAX always
    /// probes, so an all-float extreme set never benefits).
    pub(crate) track_nonlinear: bool,
    /// Position of the NULL-blind COUNT that carries a group's net cardinality
    /// for the emission gate. `Some` only for the planner shapes that promise a
    /// companion COUNT (all-linear, grouped, or global-ground); a genuinely
    /// count-less reduce (the range-join threshold reduce, low-level
    /// CircuitBuilder reduces) degrades to the touched-ness test.
    pub(crate) cardinality_idx: Option<u8>,
    /// Group-column comparator descriptors; empty on the natural-PK path
    /// (membership is the full PK byte window there).
    pub(crate) sort_descs: Vec<SortDesc>,
    /// Output width of each trailing agg column — the trace read-back stride.
    pub(crate) agg_col_widths: Vec<usize>,
    /// First aggregate column's logical index (aggregates are the trailing
    /// output columns, so this holds at any PK arity).
    pub(crate) cbase: usize,
    /// Per-output-payload-column emit role, in payload order.
    pub(super) out_roles: Vec<OutColRole>,
}

impl ReducePlan {
    /// Bake a reduce plan. `has_avi` states whether the instruction carries a
    /// combined value-index table (the exec dispatch then always opens its
    /// cursor, so the compile-time flag and the runtime cursor agree).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_schema: &SchemaDescriptor,
        output_schema: &SchemaDescriptor,
        group_by_cols: &[u32],
        agg_descs: &[AggDescriptor],
        out_key: ReduceOutKey,
        has_avi: bool,
        global_ground: bool,
        i_am_owner: bool,
    ) -> Self {
        let num_aggs = agg_descs.len();
        let num_out_cols = output_schema.num_columns();
        let cbase = num_out_cols - num_aggs;

        // `AggOp::Null` is neither linear nor value-indexed (verbatim from the
        // per-epoch computation this bakes).
        let all_linear = agg_descs.iter().all(|d| d.agg_op.is_linear());
        let group_by_pk = out_key == ReduceOutKey::PkPermutation;
        let monotone_out_pk =
            group_by_pk || super::super::util::single_col_canonical_group_key(input_schema, group_by_cols).is_some();
        // Either natural kind keys the emitted row by the group value itself, so
        // the output schema carries no group-exemplar columns.
        let use_natural_pk = out_key != ReduceOutKey::SyntheticFold;
        let track_nonlinear = has_avi
            && agg_descs
                .iter()
                .any(|d| d.agg_op.uses_value_index() && !d.col_type_code.is_float());

        // A group exists iff its net cardinality (row weight) is positive; the
        // unique NULL-blind COUNT carries that signal. All three disjuncts are
        // load-bearing — see the emission gate in `op_reduce`.
        let cardinality_idx: Option<u8> = (all_linear || !group_by_cols.is_empty() || global_ground)
            .then(|| agg_descs.iter().position(|d| d.agg_op == AggOp::Count))
            .flatten()
            .map(|i| i as u8);

        let sort_descs: Vec<SortDesc> = if group_by_pk {
            Vec::new()
        } else {
            let (arr, len) = build_sort_descs(input_schema, group_by_cols);
            arr[..len].to_vec()
        };

        let agg_col_widths: Vec<usize> = (0..num_aggs)
            .map(|k| output_schema.columns[cbase + k].size() as usize)
            .collect();

        // Output-column roles. The output schema is compiler-built
        // (`build_reduce_output_schema`), so a natural-PK output has no
        // exemplar columns and a synthetic-fold output has exactly one exemplar
        // per group column — anything else is unconstructible.
        let out_roles: Vec<OutColRole> = output_schema
            .payload_columns()
            .map(|(_pi, ci, col)| {
                let cs = col.size();
                if ci >= cbase {
                    OutColRole::Agg {
                        k: (ci - cbase) as u8,
                        cs,
                    }
                } else if use_natural_pk {
                    unreachable!("natural-PK reduce output has no group-exemplar columns");
                } else {
                    // Synthetic fold: exemplar ci = 1..N maps to group_by_cols[ci-1]
                    // (the leading `_group_pk` occupies index 0).
                    let grp_idx = ci - 1;
                    assert!(
                        grp_idx < group_by_cols.len(),
                        "reduce output schema exemplar column without a group column",
                    );
                    let src_ci = group_by_cols[grp_idx] as usize;
                    OutColRole::Exemplar {
                        loc: input_schema.locate(src_ci),
                        tc: col.type_code,
                        cs,
                    }
                }
            })
            .collect();

        ReducePlan {
            input_schema: *input_schema,
            output_schema: *output_schema,
            group_by_cols: group_by_cols.to_vec(),
            agg_descs: agg_descs.to_vec(),
            global_ground,
            i_am_owner,
            all_linear,
            group_by_pk,
            monotone_out_pk,
            track_nonlinear,
            cardinality_idx,
            sort_descs,
            agg_col_widths,
            cbase,
            out_roles,
        }
    }
}
