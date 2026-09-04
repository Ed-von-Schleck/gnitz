//! `ReducePlan` — everything `op_reduce` needs that is a pure function of
//! compile-time facts, baked once at emit time (the compiler's `emit_reduce`)
//! into `Program::reduce_plans`. The single construction site enforces the
//! coherence the old 13-parameter `op_reduce` signature spread across the
//! instruction operands; the per-epoch call re-derives nothing.

use crate::schema::{type_code, ColumnLocator, DerivedSchema, ReduceOutKey, SchemaColumn, SchemaDescriptor};

use super::super::group_key::GroupKeyCols;
use super::agg::Accumulator;
use super::avi::AviBake;
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;

/// Build the reduce output schema by **obeying** the planner's shipped
/// `out_key`, which `emit_reduce` has already validated against the input
/// schema — so the three arms are byte-identical to what the planner laid out.
/// `None` when the columns would overflow the fixed `[_; 65]` schema array.
pub(super) fn build_reduce_output_schema(
    input: &SchemaDescriptor,
    group_cols: &[u32],
    agg_descs: &[AggDescriptor],
    out_key: ReduceOutKey,
) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    for slot in out_key.output_layout(input.pk_indices(), group_cols) {
        match slot {
            gnitz_wire::ReduceOutSlot::SyntheticKey => b.push_pk(SchemaColumn::new(type_code::U128, 0))?,
            gnitz_wire::ReduceOutSlot::Key(c) => b.push_pk(input.columns[c as usize])?,
            gnitz_wire::ReduceOutSlot::Carried(c) => b.push(input.columns[c as usize])?,
        }
    }
    // Aggregate results (same for all arms). Nullability must cover what
    // `emit_agg_col` writes — see `AggFunc::raw_output_nullable`. `col_idx` is
    // range-checked by both callers before `agg_descs` is built.
    let ungrouped = group_cols.is_empty();
    for ad in agg_descs {
        let src = input.columns[ad.col_idx as usize];
        let nullable = ad.agg_op.raw_output_nullable(src.nullable != 0, ungrouped);
        b.push(SchemaColumn::new(
            gnitz_wire::agg_output_type(ad.agg_op, src.type_code),
            nullable as u8,
        ))?;
    }
    Some(b.finish())
}

/// The baked per-instruction reduce plan. Input facts (schemas, group columns,
/// aggregate descriptors) plus every derived gate `op_reduce` previously
/// recomputed per epoch. Built by [`ReducePlan::new`] only.
pub struct ReducePlan {
    pub(super) input_schema: SchemaDescriptor,
    /// The output layout, derived here so no caller derives it a second time to
    /// hand back in — the compiler reads it for the trace table and the delta
    /// register, the ad-hoc fold checks the client's reply schema against it.
    pub output_schema: SchemaDescriptor,
    /// The planner's SQL-intent discriminator for the global-aggregate ground
    /// row: the reduce must publish exactly one row even over an empty source
    /// (see `op_reduce`). Read on its own by the cardinality-zero arm.
    pub(super) global_ground: bool,
    // ── Derived (single home: `new`) ────────────────────────────────────────
    /// This worker mints the global-aggregate ground row's seed — the
    /// conjunction of `global_ground` and per-worker V₀ ownership, so
    /// "owns V₀ but is not a ground reduce" is unrepresentable. Public because
    /// the VM reads it off the finished plan pool to decide whether an empty
    /// epoch can produce anything at all.
    pub seeds_ground: bool,
    /// What the emitted row is keyed by, and — for `PkPermutation`, the one kind
    /// selected by "the group set *is* the PK" — that the input PK region is
    /// itself the group key.
    pub(super) out_key: ReduceOutKey,
    /// Pre-step MIN/MAX accumulators during the group walk for the AVI
    /// probe-skip path (only meaningful with an AVI; a float MIN/MAX always
    /// probes, so an all-float extreme set never benefits).
    pub(super) track_nonlinear: bool,
    /// Position of the NULL-blind COUNT carrying a group's net cardinality, when
    /// the aggregate set has one; else `op_reduce` falls back to touched-ness.
    /// A companion COUNT cannot be made mandatory: `AdhocFold` shares this plan,
    /// never reads this field, and would have to ship a per-group I64 nobody reads.
    pub(super) cardinality_idx: Option<u8>,
    /// The epoch's accumulator set in its empty-group state, cloned per use —
    /// so the per-aggregate `locate()` walk and the `(agg_op, type)` step
    /// classification are paid once per plan, not once per epoch or per group.
    pub acc_template: Vec<Accumulator>,
    /// Baked group-key hasher and the group columns' locators — the emitted
    /// row's synthetic PK, the group sort key, the group-membership comparator,
    /// and (for `SyntheticFold`) the exemplar copies all read it.
    pub(super) group_key: GroupKeyCols,
    /// The combined aggregate-value index this reduce is maintained through:
    /// its schema, its key writers, and the value-indexed aggregates in ordinal
    /// order. `Some` iff some aggregate is non-linear, so this *is* the
    /// "needs history" bit — nothing re-derives it from the descriptor list.
    pub avi: Option<AviBake>,
}

impl ReducePlan {
    /// The one authority over everything derived from `(input_schema,
    /// group_by_cols, agg_descs, out_key)` — the output layout included, so no
    /// caller derives it a second time to hand back in. `Err` for the two shapes
    /// an untrusted producer can name and the operator cannot execute: an output
    /// wider than the schema array, and an unaggregatable column type.
    pub fn new(
        input_schema: &SchemaDescriptor,
        group_by_cols: &[u32],
        agg_descs: &[AggDescriptor],
        out_key: ReduceOutKey,
        global_ground: bool,
        i_am_owner: bool,
    ) -> Result<Self, &'static str> {
        let output_schema = build_reduce_output_schema(input_schema, group_by_cols, agg_descs, out_key)
            .ok_or("reduce: output exceeds MAX_COLUMNS")?;

        // The aggregates are the trailing output columns, so aggregate `k` owns
        // logical column `cbase + k` at any PK arity — pairing each accumulator
        // with its own output column here is what leaves no index for a later
        // caller to get wrong.
        let cbase = output_schema.num_columns() - agg_descs.len();
        // Building the accumulators *is* the eligibility test, so no caller can
        // skip it: every value-reading aggregate needs a scalar register image.
        let acc_template: Vec<Accumulator> = agg_descs
            .iter()
            .enumerate()
            .map(|(k, d)| {
                Accumulator::new(
                    d.agg_op,
                    input_schema.locate(d.col_idx as usize),
                    output_schema.locate(cbase + k),
                )
            })
            .collect::<Option<Vec<_>>>()
            .ok_or("reduce: aggregate column type has no scalar register image")?;

        // Every group set has a packed key and the accumulator build above
        // already rejected any aggregate the index could not encode, so a
        // non-linear reduce always gets one — there is no eligibility gate and
        // no trace-replay fallback.
        let avi = acc_template
            .iter()
            .any(|a| !a.is_linear())
            .then(|| AviBake::new(input_schema, group_by_cols, &acc_template));
        // Read off the bake's own aggregate list, so the value-indexed set has one
        // walk rather than a second one that has to keep agreeing with it.
        let track_nonlinear = avi.as_ref().is_some_and(AviBake::any_trace_foldable);

        let cardinality_idx: Option<u8> = agg_descs
            .iter()
            .position(|d| d.agg_op == AggFunc::Count)
            .map(|i| i as u8);

        Ok(ReducePlan {
            input_schema: *input_schema,
            output_schema,
            global_ground,
            seeds_ground: global_ground && i_am_owner,
            out_key,
            track_nonlinear,
            cardinality_idx,
            acc_template,
            group_key: GroupKeyCols::new(input_schema, group_by_cols),
            avi,
        })
    }

    /// Group-exemplar output columns, in payload order — the *leading* payload
    /// columns, with the aggregates trailing, so exemplar `j` sits at payload
    /// index `j` and aggregate `k` at `exemplar_locs().len() + k`. Empty unless
    /// the output key is `SyntheticFold`: either natural key spells the group
    /// value into the PK region itself.
    #[inline(always)]
    pub(super) fn exemplar_locs(&self) -> &[ColumnLocator] {
        if self.out_key == ReduceOutKey::SyntheticFold {
            &self.group_key.cols
        } else {
            &[]
        }
    }
}
