//! `ReducePlan` — everything `op_reduce` needs that is a pure function of
//! compile-time facts, baked once at emit time (the compiler's `emit_reduce`)
//! into `Program::reduce_plans`. The single construction site enforces the
//! coherence the old 13-parameter `op_reduce` signature spread across the
//! instruction operands; the per-epoch call re-derives nothing.

use crate::schema::{type_code, ColumnLocator, DerivedSchema, ReduceOutKey, SchemaColumn, SchemaDescriptor, TypeCode};

use super::super::reindex::ReindexPacker;
use super::super::util::GroupKeyCols;
use super::agg::AggDescriptor;
use super::sort::packed_sort_spec;
use gnitz_wire::AggFunc;

/// The shared typing rule (`gnitz_wire::agg_output_type`), taking the typed
/// `TypeCode` its engine callers hold rather than a raw code.
pub(crate) const fn agg_output_type(agg_op: AggFunc, col_type_code: TypeCode) -> u8 {
    gnitz_wire::agg_output_type(agg_op, col_type_code as u8)
}

/// Build the reduce output schema by **obeying** the planner's shipped
/// `out_key`. The compiler (`emit_reduce`) validates `out_key` against the
/// input schema (`SchemaDescriptor::reduce_out_key`) before calling this, so
/// the three arms are byte-identical to what the planner laid out; the ad-hoc
/// fold (`AdhocFold::new`) derives its SyntheticFold layout through the same
/// single authority. `None` when the group + aggregate columns would overflow
/// the fixed `[_; 65]` schema array — self-protecting like the compiler's
/// sibling builders, so no caller owns the bound.
pub(crate) fn build_reduce_output_schema(
    input: &SchemaDescriptor,
    group_cols: &[u32],
    agg_descs: &[AggDescriptor],
    out_key: ReduceOutKey,
) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    match out_key {
        ReduceOutKey::PkPermutation => {
            // Output PK region mirrors the source's PK byte layout: walk
            // `pk_columns()` in pk-list order rather than `group_cols` order.
            b.push_pk_of(input)?;
        }
        ReduceOutKey::SingleNaturalCol => {
            // A single non-PK-or-PK natural group column keyed directly (e.g.
            // GROUP BY a U64 payload column, where `pk_columns()` would name the
            // wrong column).
            b.push_pk(input.columns[group_cols[0] as usize])?;
        }
        ReduceOutKey::SyntheticFold => {
            // Synthetic U128 PK, group columns as payload.
            b.push_pk(SchemaColumn::new(type_code::U128, 0))?;
            for &gc in group_cols {
                b.push(input.columns[gc as usize])?;
            }
        }
    }
    // Aggregate results (same for all arms). Nullability must cover what
    // `emit_agg_col` writes — see `AggFunc::raw_output_nullable`. `col_idx` is
    // range-checked by both callers before `agg_descs` is built.
    let ungrouped = group_cols.is_empty();
    for ad in agg_descs {
        let nullable = ad
            .agg_op
            .raw_output_nullable(input.columns[ad.col_idx as usize].nullable != 0, ungrouped);
        b.push(SchemaColumn::new(
            agg_output_type(ad.agg_op, ad.col_type_code),
            nullable as u8,
        ))?;
    }
    Some(b.finish())
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
    /// Groups are visited in ascending output-PK order. Read only by the
    /// debug-only strict-ascent assertion in the group walk — the retraction
    /// probe galloping from its live position needs no flag, because
    /// `ReadCursor::advance_to` is backward-capable and never slower than an
    /// absolute seek.
    #[cfg_attr(not(debug_assertions), allow(dead_code))]
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
    /// Group-column comparator locators; empty on the natural-PK path
    /// (membership is the full PK byte window there).
    pub(crate) sort_descs: Vec<ColumnLocator>,
    /// `argsort_delta`'s packed-sort fast-path spec (see `packed_sort_spec`).
    pub(super) packed_sort: Option<(u8, TypeCode)>,
    /// Per-aggregate source-column locator, parallel to `agg_descs` — the
    /// accumulators are rebuilt per epoch, but the `locate()` walk is not.
    pub(super) agg_locs: Vec<ColumnLocator>,
    /// AVI group-key gatherer for the combined-index read path; `Some` iff the
    /// instruction carries a value-index table.
    pub(super) avi_key_packer: Option<ReindexPacker>,
    /// Baked group-key hasher for the non-linear no-index fallback's
    /// per-trace-row routing; `Some` exactly on that path.
    pub(super) fallback_keys: Option<GroupKeyCols>,
    /// Output width of each trailing agg column — the trace read-back stride.
    pub(crate) agg_col_widths: Vec<usize>,
    /// First aggregate column's logical index (aggregates are the trailing
    /// output columns, so this holds at any PK arity).
    pub(crate) cbase: usize,
    /// Whether the output key is `ReduceOutKey::SyntheticFold` — the only shape
    /// that carries group-exemplar payload columns (see [`Self::exemplar_locs`]).
    pub(super) synthetic_key: bool,
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

        let all_linear = agg_descs.iter().all(|d| d.agg_op.is_linear());
        let group_by_pk = out_key == ReduceOutKey::PkPermutation;
        let monotone_out_pk =
            group_by_pk || super::super::util::single_col_canonical_group_key(input_schema, group_by_cols);
        // Either natural kind keys the emitted row by the group value itself, so
        // the output schema carries no group-exemplar columns.
        let use_natural_pk = out_key != ReduceOutKey::SyntheticFold;
        let track_nonlinear = has_avi
            && agg_descs
                .iter()
                .any(|d| d.agg_op.uses_value_index() && !d.col_type_code.is_float());

        // A group exists iff its net cardinality (row weight) is positive; the
        // unique NULL-blind COUNT carries that signal. The disjunction spells the
        // planner shapes that promise a companion COUNT (see the field doc); any
        // other shape falls back to the touched-ness test in `op_reduce`.
        let cardinality_idx: Option<u8> = (all_linear || !group_by_cols.is_empty() || global_ground)
            .then(|| agg_descs.iter().position(|d| d.agg_op == AggFunc::Count))
            .flatten()
            .map(|i| i as u8);

        let sort_descs: Vec<ColumnLocator> = if group_by_pk {
            Vec::new()
        } else {
            group_by_cols.iter().map(|&c| input_schema.locate(c as usize)).collect()
        };
        let packed_sort = packed_sort_spec(input_schema, group_by_cols);
        let agg_locs: Vec<ColumnLocator> = agg_descs
            .iter()
            .map(|d| input_schema.locate(d.col_idx as usize))
            .collect();
        let avi_key_packer = has_avi.then(|| super::super::index::avi_key_packer(input_schema, group_by_cols));
        let fallback_keys =
            (!all_linear && !has_avi && !group_by_pk).then(|| GroupKeyCols::new(input_schema, group_by_cols));

        let agg_col_widths: Vec<usize> = (0..num_aggs)
            .map(|k| output_schema.columns[cbase + k].size() as usize)
            .collect();

        // Group-exemplar columns. The output schema is compiler-built
        // (`build_reduce_output_schema`), so a natural-PK output has none and a
        // synthetic-fold output has exactly one per group column, in order, ahead
        // of the trailing aggregates. The assertion pins that layout — it is what
        // makes the emitters positional (exemplar `j` at payload index `j`,
        // aggregate `k` at `exemplar_locs().len() + k`) with no per-column role tag.
        let num_exemplars = if use_natural_pk { 0 } else { group_by_cols.len() };
        assert_eq!(
            num_exemplars + num_aggs,
            output_schema.num_payload_cols(),
            "reduce output schema must be [key columns…, group exemplars…, aggregates…]",
        );

        // The declaration must **cover** the emission: `emit_agg_col` sets a null
        // bit exactly when an untouched accumulator does not render a concrete `0`,
        // and `raw_output_nullable` is that same rule over compile-time facts. A
        // NOT NULL declaration on a column that can render NULL puts the row on the
        // null-blind fixed-int comparator, which ranks that cell as a real `0` — so
        // a `MIN: NULL → 0` transition compares equal to its own retraction and
        // nets away, stranding a stale row in the trace. Checked once per plan
        // (every reduce, data-independent) rather than per emitted row, and it
        // covers the ad-hoc fold's schema too since that lands here as well. The
        // converse slack is deliberate: declaring a column nullable that never
        // renders NULL only forfeits a comparator class, and `op_reduce`'s callers
        // may legitimately widen. `build_reduce_output_schema` produces the exact
        // rule, which its own test pins.
        let ungrouped = group_by_cols.is_empty();
        for (k, d) in agg_descs.iter().enumerate() {
            let renders_null = d
                .agg_op
                .raw_output_nullable(input_schema.columns[d.col_idx as usize].nullable != 0, ungrouped);
            assert!(
                !renders_null || output_schema.columns[cbase + k].nullable != 0,
                "reduce output schema: aggregate column {k} can render NULL but is declared NOT NULL",
            );
        }

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
            packed_sort,
            agg_locs,
            avi_key_packer,
            fallback_keys,
            agg_col_widths,
            cbase,
            synthetic_key: !use_natural_pk,
        }
    }

    /// Group-exemplar output columns, in payload order: a verbatim copy of the
    /// input group column read through its pre-resolved locator (whose type code
    /// and width are the emit dispatch and copy width). These are the *leading*
    /// payload columns and the aggregates the trailing ones, so exemplar `j` sits
    /// at payload index `j` and aggregate `k` at `exemplar_locs().len() + k`.
    /// Empty unless the output key is `SyntheticFold` — either natural key spells
    /// the group value into the PK region itself. Otherwise these are exactly the
    /// group-column locators `sort_descs` already holds: `SyntheticFold` is not
    /// `PkPermutation`, so that field is the same non-empty `locate(group_cols)`.
    #[inline]
    pub(super) fn exemplar_locs(&self) -> &[ColumnLocator] {
        if self.synthetic_key {
            &self.sort_descs
        } else {
            &[]
        }
    }
}
