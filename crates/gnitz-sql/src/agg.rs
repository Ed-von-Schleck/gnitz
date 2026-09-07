//! The aggregate layout model — the shared contract between the two reduce
//! lowerings (`hir::lower::reduce` for a view's circuit, `hir::lower::fold` for
//! an ad-hoc read) and the client finisher (`exec::agg_finish`). Owns the
//! physical spec layout (`push_agg_specs` is the single authority for how many
//! specs an aggregate materialises and their output types), the finalize
//! composite that renders one (`finalize_agg_bexpr`), and the SyntheticFold
//! reduce-output column layout (`reduce_out_key_region`). A sibling of `ir` so
//! `exec` keeps its documented shape (it sinks only into shared lower layers,
//! never up into `dml` or the `hir` view compiler).

use crate::ast_util::agg_func_name;
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BExpr, BinOp};
use crate::types::has_scalar_register;
use gnitz_core::{ColumnDef, ReduceOutKey, Schema, TypeCode};
use gnitz_wire::{AggFunc as WireAggFunc, ReduceOutSlot};

/// How an aggregate's value column is finalized — which also fixes how many
/// physical specs `push_agg_specs` emits and what composite
/// [`finalize_agg_bexpr`] builds over them. `Avg` and `NullfillSum` each carry a hidden
/// COUNT_NON_NULL companion at `specs_start + 1`: their null-ness derives from
/// `companion == 0`, never the value column's saturating `has_value` bit.
/// `Direct` is a single spec copied straight through.
#[derive(Clone, Copy)]
pub(crate) enum AggShape {
    Direct,
    Avg,
    NullfillSum,
}

impl AggShape {
    /// True iff a hidden COUNT_NON_NULL companion sits at `specs_start + 1` and
    /// carries this aggregate's null-ness (AVG and nullable-source SUM).
    pub(crate) fn has_count_companion(self) -> bool {
        matches!(self, AggShape::Avg | AggShape::NullfillSum)
    }
}

/// One physical reduce spec: the typed wire aggregate op, its source column,
/// and the output column type it produces. `out_type` is computed once at spec
/// creation by `push_agg_specs` (the spec-layout authority), so the reduce
/// schema builder reads it directly instead of reconstructing it from `op`. The
/// circuit builder consumes only `(op.as_wire(), col)`.
pub(crate) struct AggSpec {
    pub(crate) op: WireAggFunc,
    pub(crate) col: usize,
    pub(crate) out_type: TypeCode,
}

/// A reduce's output layout: the columns ahead of the aggregates, the PK
/// positions among them, and each group column's slot in GROUP BY order. All
/// three come off one [`ReduceOutKey::output_layout`] walk, so no consumer
/// re-derives a position from the out-key kind.
struct KeyRegion {
    cols: Vec<ColumnDef>,
    pk_cols: Vec<u32>,
    group_slots: Vec<usize>,
}

/// Materialize the shared reduce-output layout ([`ReduceOutKey::output_layout`])
/// into this side's column defs. The aggregate columns are the caller's to
/// append: their type and nullability come from the physical specs, which are
/// per-side.
fn reduce_out_key_region(out_key: ReduceOutKey, source_schema: &Schema, group_col_indices: &[usize]) -> KeyRegion {
    let group: Vec<u32> = group_col_indices.iter().map(|&i| i as u32).collect();
    let mut r = KeyRegion {
        cols: Vec::new(),
        pk_cols: Vec::new(),
        group_slots: Vec::new(),
    };
    // Where each source column landed, in slot order.
    let mut slot_of_src: Vec<(u32, usize)> = Vec::new();
    for slot in out_key.output_layout(&source_schema.pk_cols, &group) {
        let at = r.cols.len();
        let src = match slot {
            // Hidden: the synthetic group key is a physical PK column but not a
            // presentation column. The group columns follow it as visible payload,
            // so `SELECT *` shows the grouping values and aggregates, not the hash.
            ReduceOutSlot::SyntheticKey => {
                r.pk_cols.push(at as u32);
                r.cols.push(ColumnDef::new("_group_pk", TypeCode::U128, false).hidden());
                continue;
            }
            ReduceOutSlot::Key(c) => {
                r.pk_cols.push(at as u32);
                c
            }
            ReduceOutSlot::Carried(c) => c,
        };
        slot_of_src.push((src, at));
        r.cols.push(source_schema.columns[src as usize].clone());
    }
    // A group column may repeat (`SELECT DISTINCT *, *`), and the fold arm then
    // carries it once per occurrence; every repeat reads the first slot holding it.
    r.group_slots = group
        .iter()
        .map(|&g| {
            slot_of_src
                .iter()
                .find_map(|&(c, at)| (c == g).then_some(at))
                .expect("every group column has an output slot")
        })
        .collect();
    r
}

/// The SyntheticFold key region ([`reduce_out_key_region`]) plus one column per
/// physical agg spec, at the spec's `out_type` — the ad-hoc partial reply layout.
///
/// Every aggregate column is declared nullable, where the view path's
/// [`reduce_output_schema`] states each one's exact nullability. Conservative
/// rather than cosmetic, and only safe in this direction: this schema decodes
/// the worker partials and is what the HAVING and finalize expressions resolve
/// against, so over-declaring forces the evaluator's null-carrying arm, while
/// under-declaring would let it read a NULL cell's zero bytes as a real value.
fn synthetic_fold_cols(
    source_schema: &Schema,
    group_col_indices: &[usize],
    agg_specs: &[AggSpec],
) -> (Vec<ColumnDef>, Vec<usize>) {
    let mut r = reduce_out_key_region(ReduceOutKey::SyntheticFold, source_schema, group_col_indices);
    r.cols
        .extend(agg_specs.iter().map(|s| ColumnDef::new("_agg", s.out_type, true)));
    (r.cols, r.group_slots)
}

/// The ad-hoc fold's partial reply schema — the one home for "what the workers
/// emit and the client decodes", built for a grouped reduce and for the
/// degenerate `SELECT DISTINCT` fold alike.
pub(crate) fn fold_partial_schema(
    reduce_in: &Schema,
    group_positions: &[usize],
    agg_specs: &[AggSpec],
) -> Result<ReduceLayout, GnitzSqlError> {
    let (cols, group_slots) = synthetic_fold_cols(reduce_in, group_positions, agg_specs);
    let agg_col_offset = cols.len() - agg_specs.len();
    let schema = Schema::from_parts(cols, vec![0])
        .map_err(|e| GnitzSqlError::Unsupported(format!("aggregate SELECT partial-reply layout: {e}")))?;
    Ok(ReduceLayout { schema, group_slots, agg_col_offset })
}

/// The raw reduce column's nullability for one physical spec, via the shared
/// `AggFunc::raw_output_nullable` the engine's `build_reduce_output_schema`
/// obeys — so the planner's virtual reduce schema and the physical one agree.
fn agg_raw_nullable(source_schema: &Schema, spec: &AggSpec, is_global: bool) -> bool {
    let src_nullable = source_schema
        .columns
        .get(spec.col)
        .map(|c| c.is_nullable)
        .unwrap_or(false);
    spec.op.raw_output_nullable(src_nullable, is_global)
}

/// What a reduce's output looks like to everything downstream: the schema, each
/// group column's slot in it (GROUP BY order), and the offset of the first
/// aggregate column. Produced by [`reduce_output_schema`] and
/// [`fold_partial_schema`] off one layout walk.
pub(crate) struct ReduceLayout {
    pub(crate) schema: Schema,
    pub(crate) group_slots: Vec<usize>,
    pub(crate) agg_col_offset: usize,
}

/// A reduce's physical shape: what it groups by, what it aggregates, and the two
/// derived facts every downstream decision reads — the output-key kind and
/// whether the group set is empty (a global aggregate). Constructed once per
/// reduce and shared by the layout (`reduce_output_schema`) and the emission
/// (`emit_reduce`), so the two cannot disagree about the shape they describe.
pub(crate) struct ReduceShape<'a> {
    pub(crate) source_schema: &'a Schema,
    pub(crate) group_cols: &'a [usize],
    pub(crate) specs: &'a [AggSpec],
    /// `source_schema.reduce_out_key(group_cols)`, a field rather than a method
    /// because `reduce_out_key` allocates two `Vec<u32>` per call and every read
    /// path reads it repeatedly.
    pub(crate) out_key: ReduceOutKey,
    pub(crate) source_replicated: bool,
}

impl<'a> ReduceShape<'a> {
    pub(crate) fn new(
        source_schema: &'a Schema,
        group_cols: &'a [usize],
        specs: &'a [AggSpec],
        source_replicated: bool,
    ) -> Self {
        ReduceShape {
            out_key: source_schema.reduce_out_key(group_cols),
            source_schema,
            group_cols,
            specs,
            source_replicated,
        }
    }

    /// An empty group set: the ungrouped global aggregate that grounds to one row.
    pub(crate) fn global_ground(&self) -> bool {
        self.group_cols.is_empty()
    }
}

/// The reduce output schema for a group set, plus the offset of the first
/// aggregate column. The key region comes from the shared
/// [`ReduceOutKey::output_layout`] the engine's `build_reduce_output_schema` also
/// materializes, so the planner's virtual reduce schema matches the physical one
/// by construction rather than by mirroring. The aggregate columns trail it at
/// the output type `push_agg_specs` computed per spec (float SUM/MIN/MAX → F64,
/// MIN/MAX preserve the source type, SUM/COUNT* → I64). One home for the view
/// path and the HIR reduce shell. The width is gated here, at the planner, so an
/// over-wide reduce output (many aggregates the finalize projection narrows again)
/// is a feature-limit error rather than the engine's trust-boundary rejection.
pub(crate) fn reduce_output_schema(sh: &ReduceShape<'_>) -> Result<ReduceLayout, GnitzSqlError> {
    let (source_schema, agg_specs) = (sh.source_schema, sh.specs);
    let is_global = sh.global_ground();
    let mut r = reduce_out_key_region(sh.out_key, source_schema, sh.group_cols);
    r.cols.extend(
        agg_specs
            .iter()
            .map(|s| ColumnDef::new("_agg", s.out_type, agg_raw_nullable(source_schema, s, is_global))),
    );
    let agg_col_offset = r.cols.len() - agg_specs.len();
    let schema = Schema::from_parts(r.cols, r.pk_cols)
        .map_err(|e| GnitzSqlError::Unsupported(format!("GROUP BY output: {e}")))?;
    Ok(ReduceLayout {
        schema,
        group_slots: r.group_slots,
        agg_col_offset,
    })
}

/// Emit the reduce operator(s) for a group set — the two-phase / replicated /
/// sharded strategy selection. One home for the view path and the HIR reduce
/// shell; `filtered` is the (already WHERE-filtered) input node.
///
/// For `PkPermutation`, shard/reindex the reduce by the group columns in
/// source-PK (schema) order, not the user's GROUP BY order. The groups are
/// identical under any permutation of the PK (each group is a PK singleton),
/// and `build_reduce_output_schema` emits the output PK in source-PK order
/// regardless — so this only normalizes the shard key. Without it a permuted
/// grouping (e.g. `GROUP BY pk1, pk0`) shards by a non-PK-order key: the
/// co-partition analyzer (correctly) declines to skip the exchange, the shuffle
/// hash-routes by `[pk1, pk0]`, and the reduce output lands partitioned by
/// `hash(pk1, pk0)` rather than by the view's declared PK `(pk0, pk1)` — so the
/// multi-worker gather drops the rows that hashed to a different worker.
/// Sharding in PK order keeps the reduce co-partitioned with the source (the
/// exchange is skipped, or routes by `worker_for_pk_bytes`), so the view
/// stays partitioned by its real PK. The other kinds keep the user order: their
/// synthetic/single-natural PK and reduce layout depend on it (the fold arm
/// carries the group columns in GROUP BY order).
pub(crate) fn emit_reduce(
    cb: &mut gnitz_core::CircuitBuilder,
    filtered: gnitz_core::NodeId,
    sh: &ReduceShape<'_>,
) -> gnitz_core::NodeId {
    let agg_specs = sh.specs;
    let reduce_group_cols: Vec<usize> = if sh.out_key == ReduceOutKey::PkPermutation {
        sh.source_schema.pk_cols.iter().map(|&c| c as usize).collect()
    } else {
        sh.group_cols.to_vec()
    };
    // The circuit builder needs only (op, col) per spec; out_type is the
    // planner's concern and already shaped the reduce schema above.
    let circuit_specs: Vec<(WireAggFunc, usize)> = agg_specs.iter().map(|s| (s.op, s.col)).collect();
    let all_linear = agg_specs.iter().all(|s| s.op.is_linear());
    // Two-phase (distributable) path for an all-linear, integer, partitioned GLOBAL
    // aggregate: fold a per-worker partial locally (no exchange), then exchange only
    // the ≤ N partials to V₀'s owner and combine them. A linear aggregate satisfies
    // Agg(A+B)=Agg(A)+Agg(B), so this replaces the single-worker full-delta funnel.
    // Float SUM (and AVG over a float, whose SUM component is float) is excluded:
    // IEEE-754 addition is non-associative, so summing per-worker partials would make
    // the result depend on the worker count — those keep the deterministic funnel.
    // `two_phase ⊆ global_ground`: it is the distributable refinement of the
    // ungrouped (empty group set) case, so it reuses that predicate.
    let two_phase = sh.global_ground()
        && !sh.source_replicated
        && all_linear
        && !agg_specs
            .iter()
            .any(|s| s.op == WireAggFunc::Sum && s.out_type.is_float());
    if two_phase {
        // Phase 1 — per-worker local partial. No ExchangeShard, global_ground = false
        // (a worker with no local rows contributes no partial, never a ground row).
        // Output: [_group_pk:U128 (col 0, PK), agg0 (col 1), agg1 (col 2), ...].
        let local = cb.reduce_multi_local(filtered, &[], &circuit_specs, false);
        // Phase 2 (exchange) + Phase 3 (combine). reduce_multi inserts the single
        // ExchangeShard(∅) routing every partial (all at PK V₀) to V₀'s owner, then
        // the combine reduce sums each partial column: a COUNT/COUNT_NON_NULL partial
        // sums with SumZero (Sum fold, 0 ground — a COUNT's empty value is 0, not
        // NULL), a SUM partial with plain Sum (NULL ground). The user aggregate
        // columns land at the same positions as the funnel reduce's, so the post-map
        // is unchanged; the trailing COUNT-of-partials is the existence gate (the
        // reduce's cardinality gate finds it via the lone AggFunc::Count). global_ground
        // = true: an empty global source emits exactly one ground row here.
        // Merge each partial with the shared per-op combine rule
        // (`AggFunc::merge_func` — the same rule the ad-hoc client combiner
        // applies). Local output column `1 + i` holds local agg `i` (col 0 is
        // _group_pk).
        let mut combine_specs: Vec<(WireAggFunc, usize)> = agg_specs
            .iter()
            .enumerate()
            .map(|(i, s)| (s.op.merge_func(), 1 + i))
            .collect();
        combine_specs.push((WireAggFunc::Count, 0)); // COUNT-of-partials existence gate
        cb.reduce_multi(local, &[], &combine_specs, true)
    } else if sh.source_replicated {
        // Shard-free: every worker reduces its full local copy to the same global
        // aggregate (no ExchangeShard ⇒ no gather barrier, no N-fold sum).
        cb.reduce_multi_local(filtered, &reduce_group_cols, &circuit_specs, sh.global_ground())
    } else {
        cb.reduce_multi(filtered, &reduce_group_cols, &circuit_specs, sh.global_ground())
    }
}

/// Push the engine `agg_specs` for one aggregate and return its shape (an AVG
/// materialises two specs — SUM then COUNT_NON_NULL). The single source of
/// truth for the spec layout — and for each spec's output column type, recorded
/// here (the one place the AVG split lives) so the reduce schema builder never
/// reconstructs it from the op code. Both reduce lowerings call it, so the
/// AVG-emits-two-specs invariant — on which every reduce-output column position
/// depends — holds identically for a view's circuit and an ad-hoc fold.
pub(crate) fn push_agg_specs(
    agg_func: AggFunc,
    arg_col: Option<usize>,
    cols: &[ColumnDef],
    agg_specs: &mut Vec<AggSpec>,
) -> Result<AggTyping, GnitzSqlError> {
    let typing = agg_typing(agg_func, arg_col.map(|c| &cols[c]))?;
    // Every op of one aggregate reads the same source column. COUNT(*) has no
    // argument and the engine reads none, so slot 0 is the conventional
    // placeholder (its output type is I64 regardless of what sits there).
    let col = arg_col.unwrap_or(0);
    agg_specs.extend(typing.ops.iter().map(|&(op, out_type)| AggSpec { op, col, out_type }));
    Ok(typing)
}

/// The default output column name for an unaliased aggregate at SELECT position
/// `idx` — `_` + the aggregate's canonical SQL name + the position. User-visible
/// — it names a column in a view's schema and in an ad-hoc result alike —
/// and derived from the one name↔aggregate table, so it cannot drift from the
/// spelling the parser accepts.
pub(crate) fn default_agg_name(func: AggFunc, idx: usize) -> String {
    format!("_{}{idx}", agg_func_name(func))
}

/// The finalize composite that renders one aggregate's SELECT/HAVING value from
/// its raw reduce output column(s) — the single definition of the rule, and the
/// reason neither reduce lowering nor the client finisher carries a
/// per-aggregate shape switch of its own: the AVG divide and the nullable-SUM
/// null gate are *in* the expression, wherever it is evaluated.
///
/// Generic over the leaf `R` because [`BExpr`] is: the binder builds it over
/// `HirRef` column identities, and the lowering resolves those to positions like
/// any other expression.
///
/// * **AVG** (`companion`, `func == Avg`) — `(sum * 1.0) / cnt`. The `* 1.0`
///   forces float division, which an int-source SUM/COUNT would otherwise
///   truncate; a zero count divides by zero, which renders NULL — exactly AVG's
///   empty/all-NULL-group result.
/// * **Nullable SUM** (`companion`, any other func) — `sum / (cnt != 0)`. The raw
///   SUM column saturates to a concrete 0 once its last non-null contributor is
///   retracted, so null-ness comes from the COUNT_NON_NULL companion instead: the
///   divisor is an exact identity (1) while the count is positive and 0 when it
///   hits zero (div-by-zero → NULL). Type-preserving — unlike AVG, SUM keeps its
///   own output type, since the divide dispatches on the SUM column's type.
/// * **Direct** (no companion) — the value column itself, raw null bit included.
///
/// So a null test over the composite is exact in every shape, which is what
/// lets the binder treat an aggregate like any other computed operand.
pub(crate) fn finalize_agg_bexpr<R>(value: R, companion: Option<R>, func: AggFunc) -> BExpr<R> {
    let value = BExpr::ColRef(value);
    let Some(cnt) = companion else {
        return value;
    };
    let cnt = BExpr::ColRef(cnt);
    if func == AggFunc::Avg {
        BExpr::BinOp(
            Box::new(BExpr::BinOp(
                Box::new(value),
                BinOp::Mul,
                Box::new(BExpr::LitFloat(1.0)),
            )),
            BinOp::Div,
            Box::new(cnt),
        )
    } else {
        BExpr::BinOp(
            Box::new(value),
            BinOp::Div,
            Box::new(BExpr::BinOp(Box::new(cnt), BinOp::Ne, Box::new(BExpr::LitInt(0)))),
        )
    }
}

/// An aggregate's typing: everything decided by the function and its argument's
/// definition alone, with no physical column positions involved.
pub(crate) struct AggTyping {
    pub(crate) shape: AggShape,
    /// The physical ops and their output types, in spec order. `ops[0].1` is the
    /// aggregate's **raw** reduce value type (for AVG, the SUM component's — not
    /// the F64 the finalize renders; `HirAgg::view_type` is what renders it).
    pub(crate) ops: Vec<(WireAggFunc, TypeCode)>,
}

/// Decide an aggregate's shape, physical op sequence, and output types from its
/// function and its argument column's definition. The typing half of
/// [`push_agg_specs`], split out so a caller that needs the typing facts (the HIR
/// bind, deciding whether to mint a companion and how to type the finalize
/// projection) does not have to materialize a physical spec list at fabricated
/// column positions to read them back.
pub(crate) fn agg_typing(agg_func: AggFunc, arg: Option<&ColumnDef>) -> Result<AggTyping, GnitzSqlError> {
    // `classify_agg_call` admits a wildcard only for COUNT(*).
    if !matches!(agg_func, AggFunc::Count) && arg.is_none() {
        return Err(GnitzSqlError::Internal(format!(
            "{agg_func:?} reached agg_typing without an argument column"
        )));
    }
    // Every value-reading aggregate needs its argument in a scalar register,
    // which excludes the wide integer-ish types and the german-string pair
    // alike: MIN/MAX have no accumulator for them and SUM/AVG cannot add them.
    // The one gate for every surface that binds an aggregate call.
    if let Some(c) = arg {
        let needs_value = match agg_func {
            AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => true,
            AggFunc::Count => false,
        };
        if needs_value && !has_scalar_register(c.type_code) {
            return Err(GnitzSqlError::Unsupported(format!(
                "{}: not supported on {:?} column '{}'",
                agg_func_name(agg_func).to_ascii_uppercase(),
                c.type_code,
                c.name,
            )));
        }
    }
    // An op's output type comes straight from the shared wire typing rule over
    // its own (op, source type) — the typed `op` IS the wire selector, so no
    // parallel planner-enum representation rides along. A source-less COUNT
    // passes I64, which the rule maps to its own default arm.
    let src_tc = arg.map(|c| c.type_code as u8).unwrap_or(TypeCode::I64 as u8);
    let op = |o: WireAggFunc| (o, TypeCode::from_validated_u8(gnitz_core::agg_output_type(o, src_tc)));
    let (shape, ops) = match agg_func {
        // `COUNT(x)` counts the rows where `x` is non-NULL; `COUNT(*)` counts
        // every row of the group.
        AggFunc::Count if arg.is_some() => (AggShape::Direct, vec![op(WireAggFunc::CountNonNull)]),
        AggFunc::Count => (AggShape::Direct, vec![op(WireAggFunc::Count)]),
        AggFunc::Min => (AggShape::Direct, vec![op(WireAggFunc::Min)]),
        AggFunc::Max => (AggShape::Direct, vec![op(WireAggFunc::Max)]),
        // A nullable source means the group's non-null count can fall back to
        // zero — its last contributor retracted — while the group still survives
        // (via COUNT(*) or another aggregate), which SQL renders as NULL. The raw
        // SUM column cannot express that on the linear fold: its `has_value`
        // boolean saturates true and never returns to false, so a netted-to-zero
        // SUM emits a concrete 0 where NULL is correct. Attach a hidden
        // COUNT_NON_NULL companion and let the finalize null-gate the SUM on it —
        // distinguishing SUM({5,-5})=0 from SUM({NULL})=NULL.
        AggFunc::Sum if arg.expect("SUM has an argument").is_nullable => (
            AggShape::NullfillSum,
            vec![op(WireAggFunc::Sum), op(WireAggFunc::CountNonNull)],
        ),
        AggFunc::Sum => (AggShape::Direct, vec![op(WireAggFunc::Sum)]),
        AggFunc::Avg => (AggShape::Avg, vec![op(WireAggFunc::Sum), op(WireAggFunc::CountNonNull)]),
    };
    Ok(AggTyping { shape, ops })
}

/// Every planner-built reduce gates group existence on a NULL-blind COUNT(*)
/// cardinality (a group exists iff its net row weight > 0). Both the combined
/// value-index path and the single-scan fallback read it — a mixed reduce folds
/// its linear companions to a numeric value whose saturating `has_value` cannot
/// signal an emptied group, so without this companion an emptied group would emit
/// a phantom `(g, NULL, 0)` row.
///
/// Reuse a user COUNT(*) when present; else append exactly one hidden trailing
/// companion. Appended last — after every SELECT and HAVING-only aggregate — so it
/// shifts no existing aggregate's `specs_start`; it gets no output column, so the
/// post-reduce MAP strips it (like a HAVING-only aggregate). Every planner-built
/// reduce is grouped or a global scalar aggregate, so the guard is simply "no
/// COUNT(*) present yet" — linear or not. (The companion is itself a COUNT, so it
/// never changes the linearity `emit_reduce`'s two-phase decision reads off the
/// full spec list.) The circuit lowering's alone: the cardinality COUNT is a
/// `should_emit` signal, and the stateless ad-hoc fold emits one partial per
/// present group without one.
pub(crate) fn ensure_cardinality_count(cols: &[ColumnDef], agg_specs: &mut Vec<AggSpec>) -> Result<(), GnitzSqlError> {
    if !agg_specs.iter().any(|s| s.op == WireAggFunc::Count) {
        push_agg_specs(AggFunc::Count, None, cols, agg_specs)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/agg.rs"]
mod tests;
