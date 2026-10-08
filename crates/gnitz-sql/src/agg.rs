//! The per-aggregate rules the binder, both reduce lowerings and the window
//! desugar share, and the one SQL-name ↔ aggregate map. It reads no SQL text.

use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp};
use gnitz_core::Schema;
use gnitz_wire::AggFunc as WireAggFunc;
use gnitz_wire::{ColType, ColumnDef, TypeCode};

#[derive(Clone, Debug, Copy, PartialEq)]
pub(crate) enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// The one SQL-name ↔ aggregate map, read in both directions by
/// [`agg_func_from_name`] and [`agg_func_name`] — a bijection, so a name can
/// never drift between the two directions.
const AGG_NAMES: [(&str, AggFunc); 5] = [
    ("count", AggFunc::Count),
    ("sum", AggFunc::Sum),
    ("min", AggFunc::Min),
    ("max", AggFunc::Max),
    ("avg", AggFunc::Avg),
];

/// The `AggFunc` a function name denotes (`count`, `sum`, `min`, `max`, `avg`),
/// matched case-insensitively without allocating; `None` for any other name.
pub(crate) fn agg_func_from_name(name: &str) -> Option<AggFunc> {
    AGG_NAMES
        .into_iter()
        .find_map(|(n, f)| name.eq_ignore_ascii_case(n).then_some(f))
}

/// The canonical lowercase SQL name of an aggregate — [`agg_func_from_name`]
/// inverted over the same table.
pub(crate) fn agg_func_name(f: AggFunc) -> &'static str {
    AGG_NAMES
        .iter()
        .find_map(|&(n, g)| (g == f).then_some(n))
        .expect("every AggFunc spelling is in AGG_NAMES")
}

/// An aggregate's physical value op, and the count op its null-ness is read off
/// when it has one. The one type gate every aggregate call goes through.
pub(crate) fn agg_ops(
    func: AggFunc,
    arg: Option<&ColumnDef>,
    ungrouped: bool,
) -> Result<(WireAggFunc, Option<WireAggFunc>), GnitzSqlError> {
    // `classify_agg_call` admits a wildcard only for COUNT(*).
    if func != AggFunc::Count && arg.is_none() {
        return Err(GnitzSqlError::Internal(format!(
            "{func:?} reached agg_ops without an argument column"
        )));
    }
    let nullable = arg.is_some_and(|c| c.is_nullable);
    // Over a NOT NULL argument, a count is COUNT(*).
    let count = if nullable {
        WireAggFunc::CountNonNull
    } else {
        WireAggFunc::Count
    };
    let ops = match func {
        AggFunc::Count => (count, None),
        AggFunc::Min => (WireAggFunc::Min, None),
        AggFunc::Max => (WireAggFunc::Max, None),
        // A raw SUM is 0 over no non-null row: its count says whether there was one.
        AggFunc::Sum => (WireAggFunc::Sum, (nullable || ungrouped).then_some(count)),
        AggFunc::Avg => (WireAggFunc::Sum, Some(count)),
    };
    if let Some(c) = arg {
        if gnitz_wire::agg_output_type(ops.0, c.ty.tc).is_none() {
            return Err(GnitzSqlError::Rejected(format!(
                "{}: not supported on {} column '{}'",
                agg_func_name(func).to_ascii_uppercase(),
                c.ty,
                c.name,
            )));
        }
    }
    Ok(ops)
}

/// The raw reduce column `op` over `src` produces, as the engine declares it.
pub(crate) fn agg_col_def(op: WireAggFunc, src: Option<&ColumnDef>, ungrouped: bool) -> ColumnDef {
    let src_ty = src.map_or(ColType::of(TypeCode::I64), |c| c.ty);
    let tc = gnitz_wire::agg_output_type(op, src_ty.tc).expect("agg_ops admitted this aggregate");
    let ty = ColType {
        tc,
        scale: if tc == TypeCode::Decimal { src_ty.scale } else { 0 },
    };
    let nullable = op.raw_output_nullable(src.is_some_and(|c| c.is_nullable), ungrouped);
    ColumnDef::typed("_agg", ty, nullable).hidden()
}

/// Hidden: the synthetic group key is a physical PK column but not a presentation
/// column, so `SELECT *` shows the grouping values and aggregates, not the hash.
pub(crate) fn group_pk_def() -> ColumnDef {
    ColumnDef::new("_group_pk", TypeCode::U128, false).hidden()
}

/// The fold's partial layout with no group columns and no aggregates: the one
/// hidden `_group_pk`. A FROM-less SELECT finalizes its constant row over it,
/// through the same `FoldFinish` a global aggregate takes.
pub(crate) fn ground_partial_schema() -> Schema {
    Schema::from_parts(vec![group_pk_def()], &[0]).expect("one hidden U128 key is a valid schema")
}

/// An aggregate's SELECT/HAVING value from its raw value and count ([`agg_ops`]).
/// A zero count renders NULL in both shapes.
pub(crate) fn finalize_agg_bexpr<R>(value: BExpr<R>, count: Option<BExpr<R>>, func: AggFunc) -> BExpr<R> {
    let Some(cnt) = count else {
        return value;
    };
    if func == AggFunc::Avg {
        // The cast makes the division a float one; dividing by a zero count is NULL.
        let value = BExpr::Cast {
            expr: Box::new(value),
            to: ColType::of(TypeCode::F64),
        };
        BExpr::bin(value, BinOp::Div, cnt)
    } else {
        let present = BExpr::bin(cnt, BinOp::Ne, BExpr::LitInt(0));
        BExpr::Case {
            branches: vec![(present, value)],
            else_: Box::new(BExpr::LitNull),
        }
    }
}

/// AVG divides to F64; every other aggregate renders its raw value type.
pub(crate) fn agg_view_type(func: AggFunc, raw: ColType) -> ColType {
    if func == AggFunc::Avg {
        ColType::of(TypeCode::F64)
    } else {
        raw
    }
}

#[cfg(test)]
#[path = "tests/agg.rs"]
mod tests;
