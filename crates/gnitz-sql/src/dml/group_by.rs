//! The ad-hoc read path's aggregate / DISTINCT fold shape (consumed by
//! `dml::select`).
//!
//! A grouped or `SELECT DISTINCT` body is **not bound here.** It goes through
//! the one front end — `hir::bind_and_lower_fold`, the same binder and the same
//! lowering the equivalent `CREATE VIEW` body uses — and this module only turns
//! the layout that comes back into the fold sink it ships and the
//! [`FoldFinish`] the client runs over the reply, and applies the one limit that
//! is the ad-hoc *sink's* own rather than the query's: a duplicate output name.

use crate::dml::select::SinkTail;
use crate::error::GnitzSqlError;
use crate::exec::agg_finish::FoldFinish;
use crate::exec::order::wire_order;
use crate::hir::bind_and_lower_fold;
use crate::tail::{order_exprs, OrderKey};
use crate::validate::reject_duplicate_projection_names;
use gnitz_core::Schema;
use gnitz_wire::{AggDescriptor, AggReadSpec, ReadSink, SinkKind};
use sqlparser::ast::Select;
use std::sync::Arc;

/// The fold sink a GROUP BY / global aggregate / HAVING / DISTINCT read ships, the
/// client tail that finishes its reply, and its ORDER BY resolved over the output —
/// a pure function of the AST and the source schema. A shape the fold cannot
/// express is a feature-named `Unsupported`; a binder's own propagates.
pub(super) fn build_fold_shape(
    select: &Select,
    schema: &Arc<Schema>,
    alias: &str,
    keys: &[OrderKey<'_>],
) -> Result<(ReadSink, SinkTail, Vec<gnitz_wire::OrderKey>), GnitzSqlError> {
    let (pieces, order_cols) = bind_and_lower_fold(select, schema, alias, &order_exprs(keys))?;

    // Over what THIS sink outputs. A view compile gates `lower_reduce`'s output,
    // which also carries the group columns — a different column set, so the two
    // gates legitimately disagree: `SELECT COUNT(*) AS kind FROM t GROUP BY kind`
    // is `[kind, kind]` as a view and refused, `[kind]` here and accepted.
    let is_distinct = select.distinct.is_some();
    let ctx = if is_distinct {
        "SELECT DISTINCT"
    } else {
        "aggregate SELECT"
    };
    reject_duplicate_projection_names(&select.projection, pieces.finalize.iter().map(|(_, d)| d), ctx)?;

    // `group_cols` / `col_idx` index the reduce input — the pre-map's output when
    // the fold carries one.
    let sink = ReadSink {
        map: pieces.pre,
        kind: SinkKind::Fold(AggReadSpec {
            group_cols: pieces.group_positions.iter().map(|&c| c as u32).collect(),
            aggs: pieces
                .agg_specs
                .iter()
                .map(|s| AggDescriptor { agg_op: s.op, col_idx: s.col as u32 })
                .collect(),
        }),
    };
    // Compiled here rather than at finish, so every rejection is pre-dispatch — and
    // the same one a view gives, the finalize being compiled as a view's map is.
    let finish = FoldFinish::new(
        pieces.partial_schema,
        pieces.agg_specs.iter().map(|s| s.op),
        &pieces.having,
        pieces.finalize,
    )?;
    let order = wire_order(keys, &finish.out_schema, &order_cols)?;
    let tail = SinkTail::Fold {
        finish: Box::new(finish),
        reduce_schema: pieces.reduce_schema,
        is_distinct,
    };
    Ok((sink, tail, order))
}
