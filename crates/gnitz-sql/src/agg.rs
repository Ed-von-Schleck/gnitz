//! The aggregate layout model — the shared contract between the view circuit
//! emitter (`hir::lower::reduce`), the ad-hoc fold router (`dml::select` /
//! `dml::group_by`), and the client finisher (`exec::agg_finish`). Owns the
//! physical spec layout
//! (`push_agg_specs` is the single authority for how many specs an aggregate
//! materialises and their output types), the per-aggregate finishing metadata
//! (`AggMapping`/`AggShape`), and the SyntheticFold reduce-output column layout
//! (`synthetic_fold_cols`). A sibling of `ir` so `exec` keeps its documented
//! shape (it sinks only into shared lower layers, never up into `plan`).

use crate::ast_util::agg_func_name;
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BExpr, BinOp, BoundExpr};
use crate::types::{has_register_image, is_integer_type};
use gnitz_core::{ColumnDef, ReduceOutKey, Schema, TypeCode};
use gnitz_wire::AggFunc as WireAggFunc;

/// Tracks how a user-level aggregate maps to reduce agg_specs.
pub(crate) struct AggMapping {
    pub(crate) specs_start: usize, // index into agg_specs
    pub(crate) shape: AggShape,
    pub(crate) output_name: String,
    pub(crate) output_type: TypeCode,
    /// Whether the output column can be NULL at runtime, computed once at
    /// construction (`append_agg_mapping`) and read by both the SELECT
    /// projection's output schema and the HAVING `IS [NOT] NULL` const-fold, so
    /// the two cannot drift. Companion shapes are blanket-nullable; `Direct` is
    /// the exact structural fact (`AggFunc::raw_output_nullable`).
    pub(crate) output_nullable: bool,
    pub(crate) agg_func: AggFunc,
    pub(crate) arg_col: Option<usize>,
}

/// How an aggregate's value column is finalized — which also fixes how many
/// physical specs `push_agg_specs` emits and how the SELECT projection / HAVING
/// binding read the result. `Avg` and `NullfillSum` each carry a hidden
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
/// circuit builder consumes only `(op.as_u64(), col)`.
pub(crate) struct AggSpec {
    pub(crate) op: WireAggFunc,
    pub(crate) col: usize,
    pub(crate) out_type: TypeCode,
}

/// The aggregate output type, via the single shared planner/engine rule
/// (`gnitz_wire::agg_output_type`), over the argument column's definition. AVG is
/// planner-lowered (SUM/COUNT + a finalize divide) before the wire and always
/// produces F64. A source-less aggregate (COUNT) passes I64, which the rule maps
/// to its own default arms.
pub(crate) fn agg_result_type_of(func: AggFunc, arg: Option<&ColumnDef>) -> TypeCode {
    let wire_func = match func {
        AggFunc::Avg => return TypeCode::F64,
        AggFunc::Count => gnitz_core::AggFunc::Count,
        AggFunc::CountNonNull => gnitz_core::AggFunc::CountNonNull,
        AggFunc::Sum => gnitz_core::AggFunc::Sum,
        AggFunc::Min => gnitz_core::AggFunc::Min,
        AggFunc::Max => gnitz_core::AggFunc::Max,
    };
    let src_tc = arg.map(|c| c.type_code as u8).unwrap_or(TypeCode::I64 as u8);
    TypeCode::from_validated_u8(gnitz_core::agg_output_type(wire_func, src_tc))
}

/// What each SELECT item represents in a GROUP BY query (in SELECT order).
pub(crate) enum GroupBySelectItem {
    GroupCol { src_col: usize, name: String },
    Aggregate { agg_idx: usize },
}

/// The validated GROUP BY / aggregate layout for a single-relation aggregate
/// SELECT — the analysis the ad-hoc fold path (`dml::select` /
/// `exec::agg_finish`) consumes, kept in parity with the HIR reduce lowering by
/// construction. `agg_specs` is the **pre-companion** physical reduce
/// layout: the view path appends its emission-only COUNT(*) cardinality
/// companion afterwards (a `should_emit` signal), and the stateless fold does
/// not need one.
pub(crate) struct GroupByLayout {
    /// Group columns as source-schema indices (empty = a global aggregate).
    pub(crate) group_col_indices: Vec<usize>,
    /// Physical reduce spec layout, pre-companion (AVG → `[Sum, CountNonNull]`,
    /// nullable SUM → `NullfillSum` companion, HAVING-only aggregates appended).
    pub(crate) agg_specs: Vec<AggSpec>,
    /// Per-SELECT/HAVING finishing info (AVG split, NullfillSum, output type /
    /// nullability / name), indexed by `GroupBySelectItem::Aggregate::agg_idx`.
    pub(crate) agg_mappings: Vec<AggMapping>,
    /// SELECT items in projection order (group columns interleaved with
    /// aggregates) — the arrangement the output row follows.
    pub(crate) select_items: Vec<GroupBySelectItem>,
}

impl GroupByLayout {
    /// The degenerate layout of `SELECT DISTINCT c1, …` — a grouped fold with
    /// zero aggregates: every projected column is a group column, in SELECT
    /// order. `out_cols` carries the (aliased) output names, parallel to
    /// `group_col_indices`.
    pub(crate) fn distinct(group_col_indices: Vec<usize>, out_cols: &[ColumnDef]) -> Self {
        let select_items = group_col_indices
            .iter()
            .zip(out_cols)
            .map(|(&src_col, c)| GroupBySelectItem::GroupCol {
                src_col,
                name: c.name.clone(),
            })
            .collect();
        GroupByLayout {
            group_col_indices,
            agg_specs: Vec::new(),
            agg_mappings: Vec::new(),
            select_items,
        }
    }

    /// `true` iff the group set is empty (an ungrouped / global scalar
    /// aggregate): its SUM/MIN/MAX/AVG outputs are nullable and it grounds to
    /// a single row over an empty source. Derived, so no constructor can state
    /// the impossible `{empty groups, not global}` combination.
    pub(crate) fn global_ground(&self) -> bool {
        self.group_col_indices.is_empty()
    }

    /// First aggregate column in the SyntheticFold reduce-output layout
    /// (`[_group_pk | group cols | agg partials]`).
    pub(crate) fn synthetic_agg_col_offset(&self) -> usize {
        1 + self.group_col_indices.len()
    }
}

/// Reduce-output column index for a group column `src_col`, mirroring the
/// reduce output schema layout keyed by `out_key`:
///
/// * `PkPermutation` — the PK region holds the source PK columns in source-PK
///   order; locate `src_col` there.
/// * `SingleNaturalCol` — the lone group col is the PK at index 0.
/// * `SyntheticFold` — group cols follow the U128 `_group_pk`, in GROUP BY order.
pub(crate) fn group_col_reduce_pos(
    src_col: usize,
    out_key: ReduceOutKey,
    source_schema: &Schema,
    group_col_indices: &[usize],
) -> usize {
    match out_key {
        ReduceOutKey::PkPermutation => source_schema
            .pk_cols
            .iter()
            .position(|&pi| pi == src_col)
            .expect("PkPermutation: every group col is a source PK col"),
        ReduceOutKey::SingleNaturalCol => 0,
        ReduceOutKey::SyntheticFold => 1 + group_col_indices.iter().position(|&gi| gi == src_col).unwrap(),
    }
}

/// The SyntheticFold reduce-output column layout: the hidden U128 group-key PK,
/// the group columns (source definitions), then one column per physical agg
/// spec (at the spec's `out_type`). The single home of the layout the view
/// path's virtual reduce schema, the ad-hoc partial reply schema, and the
/// HAVING binder's `agg_col_offset = 1 + n_group` all assume. `aggs_nullable`
/// is the one divergence: the view path passes the exact per-spec rule
/// (`agg_raw_nullable`, matching the engine's physical reduce schema), while the
/// ad-hoc partial reply schema passes a blanket `true`. That is conservative
/// rather than cosmetic — the ad-hoc schema is also what the HAVING predicate
/// resolves against, so over-declaring nullable only forces the evaluator's
/// null-carrying arm, never a wrong answer.
pub(crate) fn synthetic_fold_cols(
    source_schema: &Schema,
    group_col_indices: &[usize],
    agg_specs: &[AggSpec],
    aggs_nullable: &dyn Fn(&AggSpec) -> bool,
) -> Vec<ColumnDef> {
    let mut cols = Vec::with_capacity(1 + group_col_indices.len() + agg_specs.len());
    // Hidden: the synthetic group key is a physical PK column but not a
    // presentation column. The group columns follow it as visible payload, so
    // `SELECT *` shows the grouping values and aggregates, not the hash.
    cols.push(ColumnDef::new("_group_pk", TypeCode::U128, false).hidden());
    for &gi in group_col_indices {
        cols.push(source_schema.columns[gi].clone());
    }
    for spec in agg_specs {
        cols.push(ColumnDef::new("_agg", spec.out_type, aggs_nullable(spec)));
    }
    cols
}

/// The raw reduce column's nullability for one physical spec, via the shared
/// `AggFunc::raw_output_nullable` the engine's `build_reduce_output_schema`
/// obeys — so the planner's virtual reduce schema and the physical one agree.
pub(crate) fn agg_raw_nullable(source_schema: &Schema, spec: &AggSpec, is_global: bool) -> bool {
    let src_nullable = source_schema
        .columns
        .get(spec.col)
        .map(|c| c.is_nullable)
        .unwrap_or(false);
    spec.op.raw_output_nullable(src_nullable, is_global)
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
    /// `source_schema.reduce_out_key(group_cols)` — derived, never passed in.
    pub(crate) out_key: ReduceOutKey,
    /// An empty group set: the ungrouped global aggregate that grounds to one row.
    pub(crate) global_ground: bool,
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
            global_ground: group_cols.is_empty(),
            source_schema,
            group_cols,
            specs,
            source_replicated,
        }
    }
}

/// The reduce output schema for a group set, mirroring the engine's
/// `build_reduce_output_schema` (which lays out from the same `out_key`), plus
/// the offset of the first aggregate column. The aggregate columns trail the PK
/// region — plus, on the synthetic path only, the group cols carried as payload
/// — at the output type `push_agg_specs` computed per spec (float SUM/MIN/MAX →
/// F64, MIN/MAX preserve the source type, SUM/COUNT* → I64), so the planner's
/// virtual reduce schema matches the compiler's physical reduce output with no
/// per-op reconstruction. One home for the view path and the HIR reduce shell.
pub(crate) fn reduce_output_schema(sh: &ReduceShape<'_>) -> (Schema, usize) {
    let (source_schema, agg_specs) = (sh.source_schema, sh.specs);
    let is_global = sh.global_ground;
    let (columns, pk_cols) = match sh.out_key {
        ReduceOutKey::PkPermutation => {
            let mut cols: Vec<ColumnDef> = source_schema
                .pk_cols
                .iter()
                .map(|&pi| source_schema.columns[pi].clone())
                .collect();
            let pk: Vec<usize> = (0..cols.len()).collect();
            cols.extend(
                agg_specs
                    .iter()
                    .map(|s| ColumnDef::new("_agg", s.out_type, agg_raw_nullable(source_schema, s, is_global))),
            );
            (cols, pk)
        }
        ReduceOutKey::SingleNaturalCol => {
            let mut cols = vec![source_schema.columns[sh.group_cols[0]].clone()];
            cols.extend(
                agg_specs
                    .iter()
                    .map(|s| ColumnDef::new("_agg", s.out_type, agg_raw_nullable(source_schema, s, is_global))),
            );
            (cols, vec![0])
        }
        // The shared SyntheticFold layout (also the ad-hoc partial schema).
        ReduceOutKey::SyntheticFold => (
            synthetic_fold_cols(source_schema, sh.group_cols, agg_specs, &|s| {
                agg_raw_nullable(source_schema, s, is_global)
            }),
            vec![0],
        ),
    };
    let agg_col_offset = columns.len() - agg_specs.len();
    (Schema { columns, pk_cols }, agg_col_offset)
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
/// exchange is skipped, or routes by `partition_for_pk_bytes`), so the view
/// stays partitioned by its real PK. The other kinds keep the user order: their
/// synthetic/single-natural PK and reduce layout depend on it
/// (`group_col_reduce_pos`'s synthetic arm indexes by GROUP BY order).
pub(crate) fn emit_reduce(
    cb: &mut gnitz_core::CircuitBuilder,
    filtered: gnitz_core::NodeId,
    sh: &ReduceShape<'_>,
) -> gnitz_core::NodeId {
    let agg_specs = sh.specs;
    let reduce_group_cols: Vec<usize> = if sh.out_key == ReduceOutKey::PkPermutation {
        sh.source_schema.pk_cols.clone()
    } else {
        sh.group_cols.to_vec()
    };
    // The circuit builder needs only (op, col) per spec; out_type is the
    // planner's concern and already shaped the reduce schema above.
    let circuit_specs: Vec<(u64, usize)> = agg_specs.iter().map(|s| (s.op.as_u64(), s.col)).collect();
    let all_linear = agg_specs
        .iter()
        .all(|s| matches!(s.op, WireAggFunc::Count | WireAggFunc::Sum | WireAggFunc::CountNonNull));
    // Two-phase (distributable) path for an all-linear, integer, partitioned GLOBAL
    // aggregate: fold a per-worker partial locally (no exchange), then exchange only
    // the ≤ N partials to V₀'s owner and combine them. A linear aggregate satisfies
    // Agg(A+B)=Agg(A)+Agg(B), so this replaces the single-worker full-delta funnel.
    // Float SUM (and AVG over a float, whose SUM component is float) is excluded:
    // IEEE-754 addition is non-associative, so summing per-worker partials would make
    // the result depend on the worker count — those keep the deterministic funnel.
    // `two_phase ⊆ global_ground`: it is the distributable refinement of the
    // ungrouped (empty group set) case, so it reuses that predicate.
    let two_phase = sh.global_ground
        && !sh.source_replicated
        && all_linear
        && !agg_specs
            .iter()
            .any(|s| s.op == WireAggFunc::Sum && s.out_type.is_float());
    if two_phase {
        // Phase 1 — per-worker local partial. No ExchangeShard, global_ground = false
        // (a worker with no local rows contributes no partial, never a ground row).
        // Output: [_group_pk:U128 (col 0, PK), agg0 (col 1), agg1 (col 2), ...].
        let local = cb.reduce_multi_local(filtered, &[], &circuit_specs, false, ReduceOutKey::SyntheticFold);
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
        let mut combine_specs: Vec<(u64, usize)> = agg_specs
            .iter()
            .enumerate()
            .map(|(i, s)| (s.op.merge_func().as_u64(), 1 + i))
            .collect();
        combine_specs.push((WireAggFunc::Count.as_u64(), 0)); // COUNT-of-partials existence gate
        cb.reduce_multi(local, &[], &combine_specs, true, ReduceOutKey::SyntheticFold)
    } else if sh.source_replicated {
        // Shard-free: every worker reduces its full local copy to the same global
        // aggregate (no ExchangeShard ⇒ no gather barrier, no N-fold sum).
        cb.reduce_multi_local(
            filtered,
            &reduce_group_cols,
            &circuit_specs,
            sh.global_ground,
            sh.out_key,
        )
    } else {
        cb.reduce_multi(
            filtered,
            &reduce_group_cols,
            &circuit_specs,
            sh.global_ground,
            sh.out_key,
        )
    }
}

/// The physical source column of a bound aggregate argument: `None` for
/// COUNT(*), the column index for a plain (possibly qualified) reference. A
/// computed argument (`SUM(a + b)`) is rejected — the engine aggregates a
/// physical column.
pub(crate) fn agg_arg_col(arg: Option<&BoundExpr>) -> Result<Option<usize>, GnitzSqlError> {
    match arg {
        None => Ok(None),
        Some(BoundExpr::ColRef(c)) => Ok(Some(*c)),
        Some(_) => Err(GnitzSqlError::Unsupported(
            "aggregate on computed expression not supported".to_string(),
        )),
    }
}

/// Push the engine `agg_specs` for one aggregate and return its shape (an AVG
/// materialises two specs — SUM then COUNT_NON_NULL). The single source of
/// truth for the spec layout — and for each spec's output column type, recorded
/// here (the one place the AVG split lives) so the reduce schema builder never
/// reconstructs it from the op code. Shared by the SELECT projection and the
/// HAVING-only materialisation so the two stay in lockstep — notably the
/// AVG-emits-two-specs invariant, on which the reduce-output column positions
/// and `AggMapping::specs_start` both depend.
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
/// in the view schema, so both binders must agree; derived from the one
/// name↔aggregate table so it cannot drift from the spelling the parser accepts.
pub(crate) fn default_agg_name(func: AggFunc, idx: usize) -> String {
    format!("_{}{idx}", agg_func_name(func))
}

/// Reject a MIN/MAX over an argument type the operator cannot order. MIN/MAX
/// have no correct accumulator path for wide (U128/UUID/I128) types — the i64
/// slot cannot hold them — Blob has no ordering, and the engine's integer
/// widening reads a String's prefix as LE signed i64, which orders by neither
/// bytes nor signedness. A non-MIN/MAX aggregate passes through, so a caller can
/// hand its function over unconditionally. One home for the ad-hoc (DML) and HIR
/// binds, which both reject ahead of `agg_typing`'s `Bind` backstop so the message
/// and the error variant are the same on either path.
pub(crate) fn reject_min_max_unorderable(func: AggFunc, ty: TypeCode) -> Result<(), GnitzSqlError> {
    if matches!(func, AggFunc::Min | AggFunc::Max) && !has_register_image(ty) {
        return Err(GnitzSqlError::Unsupported(format!(
            "{}: not supported on {ty:?} columns",
            agg_func_name(func).to_ascii_uppercase()
        )));
    }
    Ok(())
}

/// The finalize composite that renders one aggregate's SELECT/HAVING value from
/// its raw reduce output column(s) — the single definition of the rule, shared by
/// the ad-hoc DML binder (`R = usize`, a reduce-output column position) and the
/// HIR binder (`R = HirRef`, a column identity).
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

/// Whether an aggregate's **finalize** output is nullable. AVG's and nullable-SUM's
/// null-ness lives in the COUNT_NON_NULL companion (the finalize renders NULL via
/// div-by-zero), so a companion-carrying shape is unconditionally nullable; a direct
/// shape is exactly the raw reduce column's nullability, which is the shared
/// `AggFunc::raw_output_nullable` the engine's physical reduce schema also obeys
/// (`ops[0]` is the value spec — for AVG the SUM component, whose shape
/// short-circuits above anyway). One home for the ad-hoc (DML) and HIR binds.
pub(crate) fn agg_output_nullable(typing: &AggTyping, arg_nullable: bool, is_global: bool) -> bool {
    typing.shape.has_count_companion() || typing.ops[0].0.raw_output_nullable(arg_nullable, is_global)
}

/// An aggregate's typing: everything decided by the function and its argument's
/// definition alone, with no physical column positions involved.
pub(crate) struct AggTyping {
    pub(crate) shape: AggShape,
    /// The physical ops and their output types, in spec order. `ops[0].1` is the
    /// aggregate's **raw** reduce value type (for AVG, the SUM component's — not
    /// the F64 the finalize renders).
    pub(crate) ops: Vec<(WireAggFunc, TypeCode)>,
    /// The finalize (SELECT-visible) output type — F64 for AVG, else the raw type.
    pub(crate) view_type: TypeCode,
}

/// Decide an aggregate's shape, physical op sequence, and output types from its
/// function and its argument column's definition. The typing half of
/// [`push_agg_specs`], split out so a caller that needs the typing facts (the HIR
/// bind, deciding whether to mint a companion and how to type the finalize
/// projection) does not have to materialize a physical spec list at fabricated
/// column positions to read them back.
pub(crate) fn agg_typing(agg_func: AggFunc, arg: Option<&ColumnDef>) -> Result<AggTyping, GnitzSqlError> {
    // Every aggregate except COUNT(*) needs a column argument, which the arms
    // below unwrap. Validating here — the single source of truth for spec
    // layout — covers both the SELECT-list and HAVING callers, so neither needs
    // its own wildcard guard and a future caller cannot reintroduce the panic.
    if !matches!(agg_func, AggFunc::Count) && arg.is_none() {
        return Err(GnitzSqlError::Plan(format!(
            "{agg_func:?} requires an argument column; only COUNT(*) accepts a wildcard"
        )));
    }
    // Reject argument column types the engine cannot evaluate. Single validated
    // gate for both the SELECT-list and HAVING callers. Both bind their aggregate
    // call through the leaf binder, which already rejects unorderable MIN/MAX —
    // that arm here is the backstop; the SUM/AVG arm is the sole gate.
    if let Some(c) = arg {
        let tc = c.type_code;
        match agg_func {
            AggFunc::Sum | AggFunc::Avg => {
                if !(is_integer_type(tc) || tc.is_float()) || tc.is_wide_int() {
                    return Err(GnitzSqlError::Bind(format!(
                        "{agg_func:?} is not supported on column type {tc:?} ('{}')",
                        c.name,
                    )));
                }
            }
            AggFunc::Min | AggFunc::Max => {
                if !has_register_image(tc) {
                    return Err(GnitzSqlError::Bind(format!(
                        "{agg_func:?} is not supported on column type {tc:?} ('{}')",
                        c.name,
                    )));
                }
            }
            AggFunc::Count | AggFunc::CountNonNull => {}
        }
    }
    // An op's output type comes straight from the shared wire typing rule over
    // its own (op, source type) — the typed `op` IS the wire selector, so no
    // parallel planner-enum representation rides along. A source-less COUNT
    // passes I64, which the rule maps to its own default arm.
    let src_tc = arg.map(|c| c.type_code as u8).unwrap_or(TypeCode::I64 as u8);
    let op = |o: WireAggFunc| (o, TypeCode::from_validated_u8(gnitz_core::agg_output_type(o, src_tc)));
    let (shape, ops) = match agg_func {
        AggFunc::Count => (AggShape::Direct, vec![op(WireAggFunc::Count)]),
        AggFunc::CountNonNull => (AggShape::Direct, vec![op(WireAggFunc::CountNonNull)]),
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
    Ok(AggTyping {
        shape,
        view_type: agg_result_type_of(agg_func, arg),
        ops,
    })
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
/// full spec list.) One home for the ad-hoc (DML) and HIR reduce emitters.
pub(crate) fn ensure_cardinality_count(cols: &[ColumnDef], agg_specs: &mut Vec<AggSpec>) -> Result<(), GnitzSqlError> {
    if !agg_specs.iter().any(|s| s.op == WireAggFunc::Count) {
        push_agg_specs(AggFunc::Count, None, cols, agg_specs)?;
    }
    Ok(())
}

/// Push the agg_specs + `AggMapping` for one aggregate — the single
/// construction site, shared by the SELECT projection and the HAVING-only
/// materialisation (`collect_having_aggs`) so the reduce-output column
/// positions and the output nullability cannot drift between the two. Reuses
/// `push_agg_specs` (the spec-layout authority); `is_global` is whether the
/// group set is empty (a global aggregate's ground row renders NULL).
pub(crate) fn append_agg_mapping(
    agg_func: AggFunc,
    arg_col: Option<usize>,
    output_name: String,
    is_global: bool,
    source_schema: &Schema,
    agg_specs: &mut Vec<AggSpec>,
    agg_mappings: &mut Vec<AggMapping>,
) -> Result<(), GnitzSqlError> {
    let start = agg_specs.len();
    let typing = push_agg_specs(agg_func, arg_col, &source_schema.columns, agg_specs)?;
    let arg_nullable = arg_col.map(|c| source_schema.columns[c].is_nullable).unwrap_or(false);
    let output_nullable = agg_output_nullable(&typing, arg_nullable, is_global);
    agg_mappings.push(AggMapping {
        specs_start: start,
        shape: typing.shape,
        output_name,
        output_type: typing.view_type,
        output_nullable,
        agg_func,
        arg_col,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::col_def;

    // Columns: 0=pk(U64), 1=n(I64), 2=b(Blob), 3=u(UUID), 4=s(String).
    fn schema() -> Schema {
        Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("n", TypeCode::I64, true),
                col_def("b", TypeCode::Blob, true),
                col_def("u", TypeCode::UUID, true),
                col_def("s", TypeCode::String, true),
            ],
            pk_cols: vec![0],
        }
    }

    fn try_push(func: AggFunc, arg_col: Option<usize>) -> Result<AggShape, GnitzSqlError> {
        let mut specs = Vec::new();
        push_agg_specs(func, arg_col, &schema().columns, &mut specs).map(|t| t.shape)
    }

    #[test]
    fn push_agg_specs_rejects_unevaluatable_arg_types() {
        assert!(matches!(try_push(AggFunc::Sum, Some(2)), Err(GnitzSqlError::Bind(_)))); // SUM(blob)
        assert!(matches!(try_push(AggFunc::Avg, Some(3)), Err(GnitzSqlError::Bind(_)))); // AVG(uuid)
        assert!(matches!(try_push(AggFunc::Min, Some(4)), Err(GnitzSqlError::Bind(_)))); // MIN(str)
        assert!(matches!(try_push(AggFunc::Max, Some(2)), Err(GnitzSqlError::Bind(_))));
        // MAX(blob)
    }

    #[test]
    fn push_agg_specs_accepts_valid_arg_types() {
        assert!(try_push(AggFunc::Sum, Some(1)).is_ok()); // SUM(i64)
        assert!(try_push(AggFunc::Avg, Some(1)).is_ok()); // AVG(i64)
        assert!(try_push(AggFunc::Min, Some(1)).is_ok()); // MIN(i64)
        assert!(try_push(AggFunc::Count, None).is_ok()); // COUNT(*)
        assert!(try_push(AggFunc::CountNonNull, Some(2)).is_ok()); // COUNT(blob) — presence only
    }

    /// SUM over a U64 source is typed U64 (bit pattern is the correct unsigned
    /// sum), so a downstream unsigned compare re-seeds; a narrow unsigned / signed
    /// source widens to I64. MIN/MAX preserve the U64 source type as before.
    #[test]
    fn agg_result_type_sum_preserves_u64() {
        let s = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("u", TypeCode::U64, true),
                col_def("w", TypeCode::U32, true),
                col_def("i", TypeCode::I64, true),
                col_def("f", TypeCode::F64, true),
            ],
            pk_cols: vec![0],
        };
        let rt = |f, i: usize| agg_result_type_of(f, Some(&s.columns[i]));
        assert_eq!(rt(AggFunc::Sum, 1), TypeCode::U64); // SUM(u64) → U64
        assert_eq!(rt(AggFunc::Sum, 2), TypeCode::I64); // SUM(u32) → I64
        assert_eq!(rt(AggFunc::Sum, 3), TypeCode::I64); // SUM(i64) → I64
        assert_eq!(rt(AggFunc::Sum, 4), TypeCode::F64); // SUM(f64) → F64
        assert_eq!(rt(AggFunc::Min, 1), TypeCode::U64);
        // MIN(u64) preserved
    }

    /// The unaliased aggregate column names are user-visible in the view schema
    /// (e2e-pinned), so the derivation from the canonical name table must render
    /// exactly these — in particular both COUNT shapes share `_count`.
    #[test]
    fn default_agg_name_renders_pinned_view_column_names() {
        assert_eq!(default_agg_name(AggFunc::Count, 0), "_count0");
        assert_eq!(default_agg_name(AggFunc::CountNonNull, 0), "_count0");
        assert_eq!(default_agg_name(AggFunc::Sum, 1), "_sum1");
        assert_eq!(default_agg_name(AggFunc::Min, 2), "_min2");
        assert_eq!(default_agg_name(AggFunc::Max, 3), "_max3");
        assert_eq!(default_agg_name(AggFunc::Avg, 4), "_avg4");
    }

    // The Direct-aggregate nullability decision shared by the SELECT projection
    // (output-schema nullability) and the HAVING `IS [NOT] NULL` const-fold — and,
    // through the same shared rule, by the engine's physical reduce output schema.
    #[test]
    fn raw_output_nullable_matches_emit_semantics() {
        use WireAggFunc as W;
        // COUNT / COUNT_NON_NULL / SumZero: always a concrete integer.
        for f in [W::Count, W::CountNonNull, W::SumZero] {
            for src_nullable in [false, true] {
                for ungrouped in [false, true] {
                    assert!(!f.raw_output_nullable(src_nullable, ungrouped), "{f:?}");
                }
            }
        }
        // SUM / MIN / MAX: NULL over an empty group set (the ground row stands in
        // for a never-populated source), or grouped over a nullable source (an
        // all-NULL group). Grouped over a non-nullable source never renders NULL —
        // that is what keeps such a reduce on the null-blind fixed-int comparator.
        for f in [W::Sum, W::Min, W::Max] {
            assert!(!f.raw_output_nullable(false, false), "{f:?} grouped, non-nullable");
            assert!(f.raw_output_nullable(true, false), "{f:?} grouped, nullable");
            assert!(f.raw_output_nullable(false, true), "{f:?} global, non-nullable");
            assert!(f.raw_output_nullable(true, true), "{f:?} global, nullable");
        }
    }
}
