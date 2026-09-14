//! The per-aggregate rules the binder, both reduce lowerings and the client
//! finisher share. A sibling of `ir`, so `exec` reaches it without `hir`.

use crate::ast_util::agg_func_name;
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp};
use crate::types::has_scalar_register;
use gnitz_core::{ColType, ColumnDef, Schema, TypeCode};
use gnitz_wire::AggFunc as WireAggFunc;

#[derive(Clone, Debug, Copy, PartialEq)]
pub(crate) enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// An aggregate's physical value op, and the count op its null-ness is read off
/// when it has one. The one type gate every aggregate call goes through.
pub(crate) fn agg_ops(
    func: AggFunc,
    arg: Option<&ColumnDef>,
) -> Result<(WireAggFunc, Option<WireAggFunc>), GnitzSqlError> {
    // `classify_agg_call` admits a wildcard only for COUNT(*).
    if func != AggFunc::Count && arg.is_none() {
        return Err(GnitzSqlError::Internal(format!(
            "{func:?} reached agg_ops without an argument column"
        )));
    }
    // A summing aggregate adds its argument in a scalar register, which excludes
    // the wide integers, the german-string pair, and a calendar value (adding two
    // dates is meaningless). MIN/MAX select a row and take any type.
    if let Some(c) = arg {
        let summing = matches!(func, AggFunc::Sum | AggFunc::Avg);
        if summing && (!has_scalar_register(c.type_code) || c.type_code.is_temporal()) {
            return Err(GnitzSqlError::Unsupported(format!(
                "{}: not supported on {:?} column '{}'",
                agg_func_name(func).to_ascii_uppercase(),
                c.type_code,
                c.name,
            )));
        }
    }
    let nullable = arg.is_some_and(|c| c.is_nullable);
    // Over a NOT NULL argument, a count is COUNT(*).
    let count = if nullable {
        WireAggFunc::CountNonNull
    } else {
        WireAggFunc::Count
    };
    Ok(match func {
        AggFunc::Count => (count, None),
        AggFunc::Min => (WireAggFunc::Min, None),
        AggFunc::Max => (WireAggFunc::Max, None),
        // A raw SUM reads 0, not NULL, once its last non-null row retracts.
        AggFunc::Sum => (WireAggFunc::Sum, nullable.then_some(WireAggFunc::CountNonNull)),
        AggFunc::Avg => (WireAggFunc::Sum, Some(count)),
    })
}

/// The raw reduce column `op` over `src` produces, as the engine declares it.
pub(crate) fn agg_col_def(op: WireAggFunc, src: Option<&ColumnDef>, ungrouped: bool) -> ColumnDef {
    let src_ty = src.map_or(ColType::of(TypeCode::I64), ColumnDef::ty);
    let tc = TypeCode::from_validated_u8(gnitz_core::agg_output_type(op, src_ty.tc as u8));
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
    Schema::from_parts(vec![group_pk_def()], vec![0]).expect("one hidden U128 key is a valid schema")
}

/// The default output column name for an unaliased aggregate at SELECT position
/// `idx` — `_` + the aggregate's canonical SQL name + the position. User-visible
/// — it names a column in a view's schema and in an ad-hoc result alike —
/// and derived from the one name↔aggregate table, so it cannot drift from the
/// spelling the parser accepts.
pub(crate) fn default_agg_name(func: AggFunc, idx: usize) -> String {
    format!("_{}{idx}", agg_func_name(func))
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
            else_: None,
        }
    }
}

/// AVG divides to F64; every other aggregate renders its raw value type. The one
/// home for the rule, read by [`crate::hir::HirAgg::view_type`] and by the window
/// desugar, which types a call before it has a `HirAgg` to ask.
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
