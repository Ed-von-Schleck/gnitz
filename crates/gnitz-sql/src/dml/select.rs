//! Direct `SELECT`: an ad-hoc SELECT reads exactly one relation, served through
//! the **parameterized bounded read** (a `ReadSpec` rows sink executed
//! server-side: bound pushdown, predicate, projection, ORDER BY / LIMIT top-k)
//! or, for aggregate / DISTINCT shapes, through the **fold sink** (a per-worker
//! hash-fold + client finishing) — except a DISTINCT over rows already a set,
//! which reads as rows. A FROM-less SELECT reads nothing: its one
//! constant row is a projection over the global ground row, computed at plan time.
//!
//! A query that *derives* a new relation — a JOIN, a set operation, an EXISTS/IN
//! or scalar subquery, a derived table — has no single-relation sink and is
//! rejected ([`crate::error::derivation`]), pointing at CREATE VIEW: the binder
//! refuses a set operation, a deriving FROM and a subquery where it meets one,
//! and the read lowering a CTE that derives. A read the direct path merely cannot
//! express is a feature-named rejection, never that template. The body and its
//! CTEs bind through the view binder (`hir::bind_adhoc_read`), so a CTE reads
//! through the sink of the flat query it composes to.
//!
//! Planning is separate from dispatch, and the seam is [`ReadPlan`]:
//! [`plan_read`] validates, resolves, and decides the sink's access and reply
//! shape without reaching a server; [`execute_select`] runs the resulting plan and
//! `dml::explain` formats the same plan instead of dispatching it.

use crate::SqlResult;
use crate::access::bound_and_predicate;
use crate::agg::group_pk_def;
use crate::catalog::Catalog;
use crate::error::GnitzSqlError;
use crate::exec::agg_finish::FoldFinish;
use crate::exec::client_map::ClientMap;
use crate::exec::order::{Slice, order_and_slice};
use crate::expr_lower::compile_scalar_evaluator;
use crate::hir::{AdhocRead, AdhocShape, FoldPieces, RowsReply, bind_adhoc_read};
use crate::ir::BoundExpr;
use crate::project::compute_map;
use crate::tail::{extract_limit_offset, key_slots, order_exprs, parse_order_by, wire_keys};
use crate::validate::{QueryEnvelope, reject_unhonored_query_clauses};
use gnitz_core::{BatchAppender, GnitzClient, RelDescriptor, ScanReply, Schema, ZSetBatch};
use gnitz_wire::{ReadBound, ReadSink, ReadSpec, RowsCut, SinkKind};
use sqlparser::ast::Query;
use std::num::NonZeroU64;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// The planning seam
// ---------------------------------------------------------------------------

/// A finished ad-hoc read: what it reads and the client finish over the result.
pub(crate) struct ReadPlan {
    pub(super) case: ReadCase,
    /// ORDER BY over the finished result's columns; empty for a constant row, which one row
    /// sorts to itself.
    pub(super) order: Vec<gnitz_wire::OrderKey>,
    pub(super) slice: Slice,
}

pub(super) enum ReadCase {
    Rows {
        read: SpecRead,
        reply_schema: Arc<Schema>,
        /// [`RowsReply::copied`].
        copied: Vec<(u32, u32)>,
    },
    Fold {
        read: SpecRead,
        finish: Box<FoldFinish>,
        /// The reduce input the fold spec's columns index; what EXPLAIN names them against.
        reduce_schema: Arc<Schema>,
        /// Nothing in the layout tells a DISTINCT fold from a zero-aggregate GROUP BY.
        is_distinct: bool,
    },
    /// A FROM-less SELECT: its one row, finished at plan time.
    Constant { schema: Arc<Schema>, row: ZSetBatch },
}

/// The relation a `ReadSpec` names, and what ships.
pub(super) struct SpecRead {
    /// As the query names it.
    pub(super) name: String,
    pub(super) desc: Arc<RelDescriptor>,
    pub(super) spec: ReadSpec,
}

impl ReadPlan {
    /// The schema of the batch the read produces before client finishing: the rows reply,
    /// the fold's partial reply, or the constant row.
    #[cfg(test)]
    pub(crate) fn reply_schema(&self) -> &Schema {
        match &self.case {
            ReadCase::Rows { reply_schema, .. } => reply_schema,
            ReadCase::Fold { finish, .. } => finish.partial_schema(),
            ReadCase::Constant { schema, .. } => schema,
        }
    }

    /// The `ReadSpec` this read ships; `None` for a constant row.
    #[cfg(test)]
    pub(crate) fn spec(&self) -> Option<&ReadSpec> {
        self.spec_read().map(|read| &read.spec)
    }

    /// This read as the subscription a delta read carries: a rows read with
    /// nothing ordered, cut or folded, since a delta of any of those is not
    /// that read of a delta.
    pub(crate) fn into_subscription(self) -> Result<gnitz_core::Subscription, GnitzSqlError> {
        // The sink ships a cut for a LIMIT only; the slice counts an OFFSET too.
        let plain = self.order.is_empty() && !self.slice.cuts();
        match self.case {
            ReadCase::Rows { read, reply_schema, copied } if plain => {
                // An index survives where every column it lists does, under the
                // reply's numbering.
                let at = |c: &u32| copied.iter().find(|(src, _)| src == c).map(|&(_, to)| to);
                let indexes = read.desc.indexes.iter().filter_map(|ix| {
                    let cols: Option<Vec<u32>> = ix.cols.as_slice().iter().map(at).collect();
                    Some(gnitz_wire::RelIndex {
                        cols: gnitz_wire::PkColList::from_slice(&cols?),
                        is_unique: ix.is_unique,
                    })
                });
                Ok(gnitz_core::Subscription {
                    indexes: indexes.collect(),
                    spec: read.spec.encode(),
                    schema: reply_schema,
                    upstream: read.desc,
                })
            }
            // A FROM-less SELECT reads no relation, so it has no feed to subscribe to.
            ReadCase::Constant { .. } => Err(crate::error::unsupported_clause(
                "a subscription",
                "a SELECT without FROM",
            )),
            _ => Err(crate::error::unsupported_clause(
                "a subscription",
                "an aggregate, DISTINCT, ORDER BY, LIMIT or OFFSET",
            )),
        }
    }

    /// The relation read and what ships; `None` for a constant row.
    pub(super) fn spec_read(&self) -> Option<&SpecRead> {
        match &self.case {
            ReadCase::Rows { read, .. } | ReadCase::Fold { read, .. } => Some(read),
            ReadCase::Constant { .. } => None,
        }
    }

    /// Whether the result is `LIMIT 0`'s: answered from the schema alone, with
    /// no request issued.
    pub(crate) fn answers_from_schema(&self) -> bool {
        self.slice.limit == Some(0)
    }

    /// The schema the finished result is built under.
    fn out_schema(&self) -> &Arc<Schema> {
        match &self.case {
            ReadCase::Rows { reply_schema, .. } => reply_schema,
            ReadCase::Fold { finish, .. } => finish.out_schema(),
            ReadCase::Constant { schema, .. } => schema,
        }
    }
}

/// Validate an ad-hoc SELECT's shape, bind it to the one relation it reads, and
/// decide its access and sink. An EXPLAIN plans the query it wraps through this
/// same function.
pub(crate) fn plan_read(query: &Query, cat: &dyn Catalog) -> Result<ReadPlan, GnitzSqlError> {
    // ORDER BY / LIMIT / OFFSET are the client finish's; any other query clause is refused.
    reject_unhonored_query_clauses(query, QueryEnvelope::WithAndTail, "direct SELECT")?;
    let (limit, offset) = extract_limit_offset(query)?;
    let slice = Slice { limit, offset: offset.unwrap_or(0) };
    let keys = parse_order_by(query.order_by.as_ref())?;
    let (desc, name, conjuncts, shape) = match bind_adhoc_read(cat, query, &order_exprs(&keys))? {
        AdhocRead::Relation { desc, name, conjuncts, shape } => (desc, name, conjuncts, shape),
        AdhocRead::Constant(items) => {
            // One row sorts to itself: a positional key is range-checked and no key
            // is placed, so an expression key's slot is never read.
            key_slots(&keys, items.iter().map(|(_, d)| d), std::iter::repeat(0))?;
            let (schema, row) = plan_constant(items)?;
            return Ok(ReadPlan {
                case: ReadCase::Constant { schema, row },
                order: Vec::new(),
                slice,
            });
        }
    };
    let (bound, predicate) = bound_and_predicate(&desc.schema, &conjuncts, &desc.indexes)?;
    let in_pk_order = walks_in_pk_order(&bound, desc.schema.pk_cols());
    let read = |sink| SpecRead {
        name,
        desc: Arc::clone(&desc),
        spec: ReadSpec { bound, predicate, sink },
    };
    let (case, order) = match shape {
        AdhocShape::Fold { pieces, order_cols, is_distinct } => {
            let FoldPieces {
                reduce_schema,
                agg,
                partial_schema,
                pre,
                having,
                finalize,
            } = *pieces;
            // Compiled here rather than at finish, so every rejection is pre-dispatch.
            let finish = FoldFinish::new(partial_schema, &agg.aggs, &having, finalize)?;
            // The finalize items follow the output's key.
            let out = finish.out_schema();
            let order = wire_keys(
                &keys,
                out.columns(),
                order_cols.iter().map(|&at| out.pk_cols().len() + at),
            )?;
            let sink = ReadSink { map: pre, kind: SinkKind::Fold(agg) };
            let case = ReadCase::Fold {
                read: read(sink),
                finish: Box::new(finish),
                reduce_schema,
                is_distinct,
            };
            (case, order)
        }
        AdhocShape::Rows(rows) => {
            let RowsReply {
                schema: reply_schema,
                program,
                order,
                sink_order,
                copied,
            } = rows.reply(&keys, in_pk_order)?;
            // OFFSET+LIMIT logical rows; an OFFSET with no LIMIT cuts nothing.
            let cut = slice
                .end()
                .and_then(|end| NonZeroU64::new(end as u64))
                // A worker walking in the order asked for meets its smallest rows
                // first, so it stops at the slice's end instead of ranking every row.
                .map(|k| RowsCut { k, order: sink_order });
            let sink = ReadSink {
                map: program.map(|p| compute_map(p, &reply_schema)),
                kind: SinkKind::Rows { cut },
            };
            (ReadCase::Rows { read: read(sink), reply_schema, copied }, order)
        }
    };
    Ok(ReadPlan { case, order, slice })
}

/// Whether a worker reading under `bound` yields its rows in ascending PK order:
/// every walk but a secondary index's.
fn walks_in_pk_order(bound: &ReadBound, pk_cols: &[u32]) -> bool {
    match bound {
        ReadBound::None | ReadBound::PkSet(_) => true,
        ReadBound::Range(r) => r.walks_pk(pk_cols),
    }
}

/// A FROM-less SELECT's one row, finished at plan time: each item is a constant
/// expression, projected over the ground row. A hidden item is an ORDER BY key,
/// which is compiled and dropped.
fn plan_constant(items: Vec<(BoundExpr, gnitz_wire::ColumnDef)>) -> Result<(Arc<Schema>, ZSetBatch), GnitzSqlError> {
    // The global ground row: one hidden `_group_pk`, keyed by the global group key.
    let ground = Schema::from_parts(vec![group_pk_def()], &[0]).expect("one hidden U128 key is a valid schema");
    let (keys, items): (Vec<_>, Vec<_>) = items.into_iter().partition(|(_, def)| def.is_hidden);
    for (key, _) in &keys {
        compile_scalar_evaluator(key, &ground)?;
    }
    let mut map = ClientMap::projecting(items, &ground)?;
    let mut ground_row = ZSetBatch::with_capacity(&ground, 1);
    BatchAppender::new(&mut ground_row).add_row(gnitz_wire::global_group_key(), 1);
    Ok((Arc::clone(map.out_schema()), map.apply(ground_row)))
}

// ---------------------------------------------------------------------------
// The dispatch tail
// ---------------------------------------------------------------------------

/// Run one planned `SELECT`, reading local-first: a relation the client's copy
/// holds is answered off it, and every other one over the wire.
pub(crate) async fn execute_select(client: &mut GnitzClient, plan: ReadPlan) -> Result<SqlResult, GnitzSqlError> {
    if plan.answers_from_schema() {
        let schema = Arc::clone(plan.out_schema());
        return Ok(SqlResult::Rows(ScanReply {
            batch: ZSetBatch::new(&schema),
            schema,
            lsn: None,
        }));
    }
    let ReadPlan { case, order, slice } = plan;
    let (schema, batch, lsn) = match case {
        ReadCase::Constant { schema, row } => (schema, row, None),
        ReadCase::Rows { read, reply_schema, .. } => {
            let reply = client
                .scan_spec_local_first(&*read.desc, read.spec, &reply_schema)
                .await?;
            (reply_schema, reply.batch, reply.lsn)
        }
        ReadCase::Fold { read, mut finish, .. } => {
            // A wire error (the per-worker group cap included) is hard: the fold is
            // mid-flight on the workers and cannot fall back.
            let partial = client
                .scan_spec_local_first(&*read.desc, read.spec, finish.partial_schema())
                .await?;
            (
                Arc::clone(finish.out_schema()),
                finish.finish(partial.batch),
                partial.lsn,
            )
        }
    };
    let batch = order_and_slice(&schema, batch, &order, slice);
    Ok(SqlResult::Rows(ScanReply { schema, batch, lsn }))
}
