//! The aggregate layout model — the shared contract between the view circuit
//! emitter (`plan::view::group_by`), the ad-hoc fold router (`dml::select`),
//! and the client finisher (`exec::agg_finish`). Owns the physical spec layout
//! (`push_agg_specs` is the single authority for how many specs an aggregate
//! materialises and their output types), the per-aggregate finishing metadata
//! (`AggMapping`/`AggShape`), and the SyntheticFold reduce-output column layout
//! (`synthetic_fold_cols`). A sibling of `ir` so `exec` keeps its documented
//! shape (it sinks only into shared lower layers, never up into `plan`).

use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BoundExpr};
use crate::types::{is_integer_type, is_min_max_orderable};
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
    /// the exact structural fact (`direct_agg_nullable`).
    pub(crate) output_nullable: bool,
    pub(crate) agg_func: AggFunc,
    pub(crate) arg_col: Option<usize>,
}

impl AggMapping {
    /// Whether the aggregate's argument column holds floats — selects the float
    /// vs integer load/divide path when finalizing AVG / nullable SUM.
    pub(crate) fn arg_is_float(&self, schema: &Schema) -> bool {
        self.arg_col
            .map(|c| schema.columns[c].type_code.is_float())
            .unwrap_or(false)
    }
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
/// (`gnitz_wire::agg_output_type`). AVG is planner-lowered (SUM/COUNT + a
/// finalize divide) before the wire and always produces F64. A source-less
/// aggregate (COUNT) passes I64, which the rule maps to its own default arms.
pub(crate) fn agg_result_type(func: AggFunc, src_col: Option<usize>, schema: &Schema) -> TypeCode {
    let wire_func = match func {
        AggFunc::Avg => return TypeCode::F64,
        AggFunc::Count => gnitz_core::AggFunc::Count,
        AggFunc::CountNonNull => gnitz_core::AggFunc::CountNonNull,
        AggFunc::Sum => gnitz_core::AggFunc::Sum,
        AggFunc::Min => gnitz_core::AggFunc::Min,
        AggFunc::Max => gnitz_core::AggFunc::Max,
    };
    let src_tc = match src_col {
        Some(c) => schema.columns[c].type_code as u8,
        None => TypeCode::I64 as u8,
    };
    TypeCode::from_validated_u8(gnitz_core::agg_output_type(wire_func, src_tc))
}

/// Whether an `AggShape::Direct` aggregate's output column can be NULL at
/// runtime, given the query shape. Computed once per aggregate at mapping
/// construction (`AggMapping::output_nullable`); everything downstream reads
/// the stored field.
///
/// A **global** (empty group set) aggregate seeds a ground row that renders
/// SUM/MIN/MAX/AVG as NULL over an empty source, so it is always nullable. A
/// **grouped** aggregate never renders NULL from emptiness (an emptied group is
/// retracted, not null-filled), so on a surviving group:
/// * COUNT / COUNT_NON_NULL are always a concrete integer (`empty_renders_zero`);
/// * a Direct SUM is only reached for a non-nullable source (a nullable source
///   routes to `NullfillSum`), so it always has a value;
/// * MIN / MAX render NULL only for an all-NULL group, i.e. a nullable source.
///
/// Mirrors `emit.rs`'s null-bit rule (`is_untouched() && !empty_renders_zero()`).
/// AVG is never `Direct` (it always carries a COUNT_NON_NULL companion).
fn direct_agg_nullable(agg_func: AggFunc, arg_col: Option<usize>, is_global: bool, schema: &Schema) -> bool {
    match agg_func {
        AggFunc::Count | AggFunc::CountNonNull => false,
        AggFunc::Sum => is_global,
        AggFunc::Min | AggFunc::Max => is_global || schema.columns[arg_col.unwrap()].is_nullable,
        AggFunc::Avg => unreachable!("AVG is never AggShape::Direct"),
    }
}

/// What each SELECT item represents in a GROUP BY query (in SELECT order).
pub(crate) enum GroupBySelectItem {
    GroupCol { src_col: usize, name: String },
    Aggregate { agg_idx: usize },
}

/// The validated GROUP BY / aggregate layout for a single-relation aggregate
/// SELECT — the shared analysis both the CREATE VIEW planner
/// (`emit_group_by_pieces`) and the ad-hoc fold path (`dml::select` /
/// `exec::agg_finish`) consume, so their column layout and validation are
/// parity by construction. `agg_specs` is the **pre-companion** physical reduce
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
/// is the one divergence: the view's virtual schema marks agg columns
/// non-nullable (it is a binding target, never wire-decoded), while the ad-hoc
/// partial schema must mark them nullable (an all-NULL SUM/MIN/MAX group ships
/// a NULL partial).
pub(crate) fn synthetic_fold_cols(
    source_schema: &Schema,
    group_col_indices: &[usize],
    agg_specs: &[AggSpec],
    aggs_nullable: bool,
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
        cols.push(ColumnDef::new("_agg", spec.out_type, aggs_nullable));
    }
    cols
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
    schema: &Schema,
    agg_specs: &mut Vec<AggSpec>,
) -> Result<AggShape, GnitzSqlError> {
    // Every aggregate except COUNT(*) needs a column argument, which the specs
    // below unwrap. Validating here — the single source of truth for spec
    // layout — covers both the SELECT-list and HAVING callers, so neither needs
    // its own wildcard guard and a future caller cannot reintroduce the panic.
    if !matches!(agg_func, AggFunc::Count) && arg_col.is_none() {
        return Err(GnitzSqlError::Plan(format!(
            "{agg_func:?} requires an argument column; only COUNT(*) accepts a wildcard"
        )));
    }
    // Reject argument column types the engine cannot evaluate. Single validated
    // gate for both the SELECT-list and HAVING callers. Both bind their aggregate
    // call through the leaf binder, which already rejects unorderable MIN/MAX —
    // that arm here is the backstop; the SUM/AVG arm is the sole gate.
    if let Some(c) = arg_col {
        let tc = schema.columns[c].type_code;
        match agg_func {
            AggFunc::Sum | AggFunc::Avg => {
                if !(is_integer_type(tc) || tc.is_float()) || tc.is_wide_int() {
                    return Err(GnitzSqlError::Bind(format!(
                        "{agg_func:?} is not supported on column type {tc:?} ('{}')",
                        schema.columns[c].name,
                    )));
                }
            }
            AggFunc::Min | AggFunc::Max => {
                if !is_min_max_orderable(tc) {
                    return Err(GnitzSqlError::Bind(format!(
                        "{agg_func:?} is not supported on column type {tc:?} ('{}')",
                        schema.columns[c].name,
                    )));
                }
            }
            AggFunc::Count | AggFunc::CountNonNull => {}
        }
    }
    // The spec's output type comes straight from the shared wire typing rule
    // over its own (op, source column) — the typed `op` IS the wire selector,
    // so no parallel planner-enum representation rides along.
    let mut push = |op: WireAggFunc, col: usize| {
        let src_tc = schema.columns[col].type_code as u8;
        agg_specs.push(AggSpec {
            op,
            col,
            out_type: TypeCode::from_validated_u8(gnitz_core::agg_output_type(op, src_tc)),
        });
    };
    Ok(match agg_func {
        AggFunc::Count => {
            push(WireAggFunc::Count, 0);
            AggShape::Direct
        }
        AggFunc::CountNonNull => {
            push(WireAggFunc::CountNonNull, arg_col.unwrap());
            AggShape::Direct
        }
        AggFunc::Sum => {
            let c = arg_col.unwrap();
            push(WireAggFunc::Sum, c);
            // A nullable source means the group's non-null count can fall back to
            // zero — its last contributor retracted — while the group still
            // survives (via COUNT(*) or another aggregate), which SQL renders as
            // NULL. The raw SUM column cannot express that on the linear fold: its
            // `has_value` boolean saturates true and never returns to false, so a
            // netted-to-zero SUM emits a concrete 0 where NULL is correct. Attach a
            // hidden COUNT_NON_NULL companion and let the finalize null-gate the
            // SUM on it — distinguishing SUM({5,-5})=0 from SUM({NULL})=NULL. A
            // non-nullable source can never be NULL on a surviving group, so it
            // keeps its plain single-spec copy.
            if schema.columns[c].is_nullable {
                push(WireAggFunc::CountNonNull, c);
                AggShape::NullfillSum
            } else {
                AggShape::Direct
            }
        }
        AggFunc::Min => {
            push(WireAggFunc::Min, arg_col.unwrap());
            AggShape::Direct
        }
        AggFunc::Max => {
            push(WireAggFunc::Max, arg_col.unwrap());
            AggShape::Direct
        }
        AggFunc::Avg => {
            let c = arg_col.unwrap();
            push(WireAggFunc::Sum, c);
            push(WireAggFunc::CountNonNull, c);
            AggShape::Avg
        }
    })
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
    let out_type = agg_result_type(agg_func, arg_col, source_schema);
    let start = agg_specs.len();
    let shape = push_agg_specs(agg_func, arg_col, source_schema, agg_specs)?;
    let output_nullable = match shape {
        // AVG's and nullable-SUM's null-ness lives in the COUNT_NON_NULL
        // companion (the finalize renders NULL via div-by-zero), so their
        // outputs keep the blanket nullable mark.
        AggShape::Avg | AggShape::NullfillSum => true,
        AggShape::Direct => direct_agg_nullable(agg_func, arg_col, is_global, source_schema),
    };
    agg_mappings.push(AggMapping {
        specs_start: start,
        shape,
        output_name,
        output_type: out_type,
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
        push_agg_specs(func, arg_col, &schema(), &mut specs)
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
        assert_eq!(agg_result_type(AggFunc::Sum, Some(1), &s), TypeCode::U64); // SUM(u64) → U64
        assert_eq!(agg_result_type(AggFunc::Sum, Some(2), &s), TypeCode::I64); // SUM(u32) → I64
        assert_eq!(agg_result_type(AggFunc::Sum, Some(3), &s), TypeCode::I64); // SUM(i64) → I64
        assert_eq!(agg_result_type(AggFunc::Sum, Some(4), &s), TypeCode::F64); // SUM(f64) → F64
        assert_eq!(agg_result_type(AggFunc::Min, Some(1), &s), TypeCode::U64); // MIN(u64) preserved
    }

    // The Direct-aggregate nullability decision shared by the SELECT projection
    // (output-schema nullability) and the HAVING `IS [NOT] NULL` const-fold. Col 0
    // (`pk`, U64) is non-nullable; col 1 (`n`, I64) is nullable.
    #[test]
    fn direct_agg_nullable_matches_emit_semantics() {
        let s = schema();
        // COUNT / COUNT_NON_NULL: always a concrete integer, grouped or global.
        assert!(!direct_agg_nullable(AggFunc::Count, None, false, &s));
        assert!(!direct_agg_nullable(AggFunc::Count, None, true, &s));
        assert!(!direct_agg_nullable(AggFunc::CountNonNull, Some(1), false, &s));
        assert!(!direct_agg_nullable(AggFunc::CountNonNull, Some(1), true, &s));
        // Direct SUM (only reached for a non-nullable source): NULL only globally,
        // where an empty source seeds a NULL ground row. Grouped never renders NULL.
        assert!(!direct_agg_nullable(AggFunc::Sum, Some(0), false, &s));
        assert!(direct_agg_nullable(AggFunc::Sum, Some(0), true, &s));
        // MIN / MAX: NULL globally (ground row), or grouped over a nullable source
        // (all-NULL group). Grouped over a non-nullable source never renders NULL.
        assert!(!direct_agg_nullable(AggFunc::Min, Some(0), false, &s)); // grouped, non-nullable pk
        assert!(direct_agg_nullable(AggFunc::Min, Some(1), false, &s)); // grouped, nullable n
        assert!(direct_agg_nullable(AggFunc::Max, Some(0), true, &s)); // global, non-nullable pk
        assert!(direct_agg_nullable(AggFunc::Max, Some(1), false, &s)); // grouped, nullable n
    }
}
