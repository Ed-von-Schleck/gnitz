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
use crate::catalog::Catalog;
use crate::error::GnitzSqlError;
use crate::exec::agg_finish::FoldFinish;
use crate::exec::client_map::ClientMap;
use crate::exec::order::ResultCut;
use crate::hir::{AdhocRead, AdhocShape, FoldPieces, RowsReply, bind_adhoc_read};
use crate::project::compute_map;
use crate::tail::extract_limit_offset;
use crate::validate::{QueryEnvelope, reject_unhonored_query_clauses};
use gnitz_core::{GnitzClient, RelDescriptor, ScanReply, Schema, ZSetBatch};
use gnitz_wire::{ReadBound, ReadSink, ReadSpec, RowsCut, SinkKind, WeightSlots};
use sqlparser::ast::Query;
use std::num::NonZeroU64;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// The planning seam
// ---------------------------------------------------------------------------

/// A finished ad-hoc read: what it reads and the client finish over the result.
pub(crate) struct ReadPlan {
    pub(super) case: ReadCase,
    /// ORDER BY / OFFSET / LIMIT over the finished result, under its schema; unordered for
    /// a constant row, which one row sorts to itself.
    pub(super) cut: ResultCut,
}

pub(super) enum ReadCase {
    /// A read whose reply is the result, under [`ResultCut::schema`].
    Rows {
        read: SpecRead,
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
    Constant { row: ZSetBatch },
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
            ReadCase::Fold { finish, .. } => finish.partial_schema(),
            ReadCase::Rows { .. } | ReadCase::Constant { .. } => self.cut.schema(),
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
        let ReadPlan { case, cut } = self;
        match case {
            ReadCase::Rows { read, copied } if cut.is_identity() => {
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
                    schema: Arc::clone(cut.schema()),
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
        self.cut.keeps_nothing()
    }
}

/// Validate an ad-hoc SELECT's shape, bind it to the one relation it reads, and
/// decide its access and sink. An EXPLAIN plans the query it wraps through this
/// same function.
pub(crate) fn plan_read(query: &Query, cat: &dyn Catalog) -> Result<ReadPlan, GnitzSqlError> {
    // ORDER BY / LIMIT / OFFSET are the client finish's; any other query clause is refused.
    reject_unhonored_query_clauses(query, QueryEnvelope::WithAndTail, "direct SELECT")?;
    let (limit, offset) = extract_limit_offset(query)?;
    let slice = WeightSlots {
        skip: offset.unwrap_or(0) as u64,
        keep: limit.map_or(u64::MAX, |l| l as u64),
    };
    let (desc, name, conjuncts, shape) = match bind_adhoc_read(cat, query)? {
        AdhocRead::Relation { desc, name, conjuncts, shape } => (desc, name, conjuncts, shape),
        AdhocRead::Constant(rows) => {
            let (schema, row) = plan_constant(rows)?;
            return Ok(ReadPlan {
                case: ReadCase::Constant { row },
                cut: ResultCut::new(schema, &[], slice)?,
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
    let (case, cut) = match shape {
        AdhocShape::Fold { pieces, is_distinct } => {
            let FoldPieces {
                reduce_schema,
                agg,
                pre,
                having,
                finalize,
            } = *pieces;
            let (finalize, order) = ClientMap::over(finalize)?;
            let cut = ResultCut::new(Arc::clone(finalize.out_schema()), &order, slice)?;
            // Compiled here rather than at finish, so every rejection is pre-dispatch.
            let finish = FoldFinish::new(&agg.aggs, &having, finalize)?;
            let sink = ReadSink { map: pre, kind: SinkKind::Fold(agg) };
            let case = ReadCase::Fold {
                read: read(sink),
                finish: Box::new(finish),
                reduce_schema,
                is_distinct,
            };
            (case, cut)
        }
        AdhocShape::Rows(rows) => {
            let RowsReply {
                schema,
                program,
                order,
                sink_order,
                pk_ordered,
                copied,
                ..
            } = rows;
            // OFFSET+LIMIT logical rows; an OFFSET with no LIMIT cuts nothing.
            let worker_cut = slice
                .end()
                .and_then(NonZeroU64::new)
                // A worker walking in the order asked for meets its smallest rows
                // first, so it stops at the slice's end instead of ranking every row.
                .map(|k| RowsCut {
                    k,
                    order: match in_pk_order && pk_ordered {
                        true => Vec::new(),
                        false => sink_order,
                    },
                });
            let sink = ReadSink {
                map: program.map(|p| compute_map(p, &schema)),
                kind: SinkKind::Rows { cut: worker_cut },
            };
            (
                ReadCase::Rows { read: read(sink), copied },
                ResultCut::new(schema, &order, slice)?,
            )
        }
    };
    Ok(ReadPlan { case, cut })
}

/// Whether a worker reading under `bound` yields its rows in ascending PK order:
/// every walk but a secondary index's.
fn walks_in_pk_order(bound: &ReadBound, pk_cols: &[u32]) -> bool {
    match bound {
        ReadBound::None | ReadBound::PkSet(_) => true,
        ReadBound::Range(r) => r.walks_pk(pk_cols),
    }
}

/// A FROM-less SELECT's one row, finished at plan time: `rows` over the global ground
/// row. One row sorts to itself, so its order is dropped.
fn plan_constant(rows: RowsReply) -> Result<(Arc<Schema>, ZSetBatch), GnitzSqlError> {
    let (mut map, _) = ClientMap::over(rows)?;
    let mut ground_row = ZSetBatch::with_capacity(map.src(), 1);
    ground_row.builder().begin_row(gnitz_wire::global_group_key(), 1);
    Ok((Arc::clone(map.out_schema()), map.apply(ground_row)))
}

// ---------------------------------------------------------------------------
// The dispatch tail
// ---------------------------------------------------------------------------

/// Run one planned `SELECT`, reading local-first: a relation the client's copy
/// holds is answered off it, and every other one over the wire.
pub(crate) async fn execute_select(client: &mut GnitzClient, plan: ReadPlan) -> Result<SqlResult, GnitzSqlError> {
    let ReadPlan { case, cut } = plan;
    let schema = Arc::clone(cut.schema());
    if cut.keeps_nothing() {
        return Ok(SqlResult::Rows(ScanReply {
            batch: ZSetBatch::new(&schema),
            schema,
            lsn: None,
        }));
    }
    let (batch, lsn) = match case {
        ReadCase::Constant { row } => (row, None),
        ReadCase::Rows { read, .. } => {
            let reply = client.scan_spec(&*read.desc, read.spec, &schema).await?;
            (reply.batch, reply.lsn)
        }
        ReadCase::Fold { read, mut finish, .. } => {
            // A wire error (the per-worker group cap included) is hard: the fold is
            // mid-flight on the workers and cannot fall back.
            let partial = client
                .scan_spec(&*read.desc, read.spec, finish.partial_schema())
                .await?;
            (finish.finish(partial.batch), partial.lsn)
        }
    };
    Ok(SqlResult::Rows(ScanReply { batch: cut.apply(batch), schema, lsn }))
}
