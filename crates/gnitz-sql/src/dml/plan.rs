//! WHERE → access-path **planning and fetching** for every direct-read verb: the
//! one recognizer ladder ([`bound_and_predicate`], over **bound conjuncts**
//! recognized by [`crate::access`]) turning a WHERE into an [`AccessPlan`], the
//! rows sink a read ships under it ([`rows_sink`]), and the `ReadSpec` dispatcher
//! that walks it ([`fetch_bound`]). Read-only analysis and
//! fetching — no row mutation lives here,
//! and recognition itself lives in the `access` leaf. `select` and `mutate` sink
//! into this module; it never references either.

use std::sync::Arc;

use crate::access::{candidates, residual, Candidate};
use crate::codec::project_schema::{read_reply_shape, ProjItem};
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_wire_conjuncts;
use crate::ir::BoundExpr;
use crate::tail::{order_exprs, parse_order_by, wire_keys};
use gnitz_core::{ColumnDef, GnitzClient, IndexMeta, Schema, ZSetBatch};
use gnitz_wire::{IndexWalk, PkKeys, ReadBound, ReadSink, ReadSpec, SinkKind};
use sqlparser::ast::{OrderBy, SelectItem};

// ---------------------------------------------------------------------------
// The access-path ladder
// ---------------------------------------------------------------------------

/// How many `ReadSpec` requests one statement may issue — the one thing the
/// verbs genuinely disagree about.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadBudget {
    /// One request, so the whole read is one server-side cut. A `pk IN (…)` list
    /// past the wire's per-gather key cap declines to the rest of the ladder,
    /// which serves it as an ordinary predicate scan.
    OneRequest,
    /// Any number: [`fetch_bound`] chunks a long gather. Safe for the DML verbs
    /// because their RMW driver preconditions the whole statement on the table
    /// being unwritten since the basis.
    MayChunk,
}

/// The pushed-down [`ReadBound`] (an access superset) and the compiled
/// server-side predicate re-imposing whatever the bound does not apply. Together
/// they are the whole WHERE, so the reply rows a [`fetch_bound`] read returns are
/// final.
///
/// Fully owned, so a finished read plan carries it across the seam between
/// planning and dispatch.
pub(crate) struct Access {
    bound: ReadBound,
    predicate: Vec<u8>,
}

impl Access {
    /// The walk this access pushes down.
    pub(crate) fn bound(&self) -> &ReadBound {
        &self.bound
    }

    /// Whether a compiled predicate ships with the bound. False means the bound
    /// is exact: the walk alone is the whole WHERE.
    pub(crate) fn has_predicate(&self) -> bool {
        !self.predicate.is_empty()
    }

    /// The `ReadSpec` this access ships under `sink`, walking `bound` rather than
    /// [`Self::bound`], so a chunked `PkSet` gather sends a sub-list per request
    /// under the same predicate and sink.
    pub(crate) fn spec(&self, bound: ReadBound, sink: &ReadSink) -> ReadSpec {
        ReadSpec {
            bound,
            predicate: self.predicate.clone(),
            sink: sink.clone(),
        }
    }
}

/// A WHERE turned into an access path: the owned [`Access`] plus the borrowed
/// conjuncts a DML verb needs to complete it over rows the server never saw.
///
/// [`fetch_bound`] is the only way to run an access, and
/// [`AccessPlan::buffered_scope`] the only way to complete it over the
/// transaction's own buffer. A read takes the `Access` and drops the rest.
pub(crate) struct AccessPlan<'e> {
    pub(super) access: Access,
    /// Every conjunct of the WHERE this plan serves; empty when there was none.
    all: Vec<&'e BoundExpr>,
    /// The conjuncts the walk does not apply exactly.
    residual: Vec<&'e BoundExpr>,
}

impl<'e> AccessPlan<'e> {
    /// The plan for `bound`, its predicate compiled from `residual`. With
    /// [`Self::whole`], the only two producers of an [`Access`] — so a plan's
    /// predicate is always its own residual's, which every rung below relies on.
    pub(super) fn new(
        bound: ReadBound,
        all: &[&'e BoundExpr],
        residual: Vec<&'e BoundExpr>,
        schema: &Schema,
    ) -> Result<Self, GnitzSqlError> {
        Ok(AccessPlan {
            access: Access {
                predicate: compile_wire_conjuncts(residual.iter().copied(), &schema.columns)?,
                bound,
            },
            all: all.to_vec(),
            residual,
        })
    }

    /// The plan whose predicate is the whole WHERE, already compiled: the
    /// residual *is* `all`, so the walk re-imposes nothing. Takes the blob the
    /// ladder already holds, where [`Self::new`] would compile it a second time.
    pub(super) fn whole(bound: ReadBound, all: &[&'e BoundExpr], predicate: Vec<u8>) -> Self {
        AccessPlan {
            access: Access { bound, predicate },
            all: all.to_vec(),
            residual: all.to_vec(),
        }
    }

    /// The keys a DML verb restricts the transaction's buffered rows to, and the
    /// conjuncts it re-imposes on them: a `PkSet`'s keys stand in for the conjuncts
    /// it consumed, and any other bound restricts nothing and re-imposes the WHERE.
    pub(crate) fn buffered_scope(&self) -> (BufferedKeys<'_>, &[&'e BoundExpr]) {
        match &self.access.bound {
            ReadBound::PkSet(keys) => (BufferedKeys::Keys(keys), &self.residual),
            _ => (BufferedKeys::All, &self.all),
        }
    }
}

/// The PKs a plan restricts the transaction's buffered rows to.
pub(crate) enum BufferedKeys<'a> {
    /// A `PkSet`'s keys.
    Keys(&'a PkKeys),
    /// Every PK the transaction touched.
    All,
}

/// Bind a single-table `WHERE` (or its absence) for [`bound_and_predicate`], as
/// its conjunct list. `alias` is the relation's effective alias, which a written
/// qualifier must name.
pub(crate) fn bind_where(
    schema: &Schema,
    alias: &str,
    where_expr: Option<&sqlparser::ast::Expr>,
) -> Result<Vec<BoundExpr>, GnitzSqlError> {
    match where_expr {
        Some(we) => crate::bind::bind_conjuncts(we, &crate::bind::SingleTable { schema, alias }),
        None => Ok(Vec::new()),
    }
}

/// Bind-once WHERE → the access plan that serves it: the first of [`candidates`]
/// whose predicate compiles, else an unbounded scan under the whole WHERE.
/// `indexes` is the relation's declared secondary-index list.
pub(crate) fn bound_and_predicate<'e>(
    schema: &Schema,
    conjuncts: &'e [BoundExpr],
    budget: ReadBudget,
    indexes: &[IndexMeta],
) -> Result<AccessPlan<'e>, GnitzSqlError> {
    let all: Vec<&'e BoundExpr> = conjuncts.iter().collect();
    // Compiled on first need: `Ok` is the predicate an index walk keeps (walk
    // Optional); `Err` makes the walk strip its own conjuncts (walk Required).
    let compile_all = || compile_wire_conjuncts(all.iter().copied(), &schema.columns);
    let mut whole: Option<Result<Vec<u8>, GnitzSqlError>> = None;
    for Candidate { bound, consumed } in candidates(conjuncts, schema, indexes) {
        let plan = match bound {
            ReadBound::PkSet(keys) if budget == ReadBudget::OneRequest && !keys.fits_one_request() => continue,
            ReadBound::IndexRange { bound, .. } => match whole.get_or_insert_with(compile_all) {
                Ok(p) => Ok(AccessPlan::whole(
                    ReadBound::IndexRange { bound, walk: IndexWalk::Optional },
                    &all,
                    p.clone(),
                )),
                Err(_) => {
                    let walk = ReadBound::IndexRange { bound, walk: IndexWalk::Required };
                    AccessPlan::new(walk, &all, residual(conjuncts, &consumed), schema)
                }
            },
            bound => AccessPlan::new(bound, &all, residual(conjuncts, &consumed), schema),
        };
        if let Some(p) = if_supported(plan)? {
            return Ok(p);
        }
    }
    Ok(AccessPlan::whole(
        ReadBound::None,
        &all,
        whole.unwrap_or_else(compile_all)?,
    ))
}

/// `Ok(None)` for the one error a candidate may be abandoned on — `Unsupported`
/// means "this candidate cannot express the WHERE", so the next one gets its turn.
/// Every other error is fatal.
fn if_supported<T>(r: Result<T, GnitzSqlError>) -> Result<Option<T>, GnitzSqlError> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(GnitzSqlError::Unsupported(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// The rows sink
// ---------------------------------------------------------------------------

/// The rows sink a read ships, the schema its reply decodes under, and its ORDER BY keys
/// over that reply. A projection reproducing the relation ships no map and replies in the
/// relation's own layout.
pub(crate) fn rows_sink(
    projection: &[SelectItem],
    order_by: Option<&OrderBy>,
    schema: &Arc<Schema>,
    alias: &str,
    limit_k: u64,
) -> Result<(Arc<Schema>, ReadSink, Vec<gnitz_wire::OrderKey>), GnitzSqlError> {
    let keys = parse_order_by(order_by)?;
    let crate::hir::AdhocRows { items, cols: out_cols, placed } =
        crate::hir::bind_adhoc_rows(projection, schema, alias, &order_exprs(&keys))?;
    let mut order = wire_keys(&keys, &out_cols, placed)?;
    let (reply_schema, map) = if reproduces(schema, &items, &out_cols) {
        for key in &mut order {
            key.col = items[key.col as usize]
                .passthrough_src()
                .expect("a reproducing projection only copies") as u16;
        }
        (Arc::clone(schema), None)
    } else {
        let (reply, map) = read_reply_shape(&items, out_cols, schema, "read-spec reply schema is invalid")?;
        (Arc::new(reply), Some(map))
    };
    let sink = ReadSink {
        map,
        kind: SinkKind::Rows { order: order.clone(), limit_k },
    };
    Ok((reply_schema, sink, order))
}

/// Whether the reply past its hidden PK prefix is the relation's visible columns, each
/// copied in place, visible, under its own name.
fn reproduces(schema: &Schema, items: &[ProjItem], out_cols: &[ColumnDef]) -> bool {
    let k = schema.pk_cols.len();
    let reply = items[k..]
        .iter()
        .zip(&out_cols[k..])
        .map(|(item, col)| (item.passthrough_src(), col.is_hidden, col.name.as_str()));
    let relation = schema
        .visible_columns()
        .map(|(i, col)| (Some(i), false, col.name.as_str()));
    !schema.has_hidden_payload() && reply.eq(relation)
}

// ---------------------------------------------------------------------------
// Fetching
// ---------------------------------------------------------------------------

/// Run `access` under `sink`, concatenating the replies: one request, or one per
/// [`gnitz_wire::PkKeys::per_request`] sub-list of a long `PkSet`. Local-first, which a DML
/// read may take too: only a view is mirrored, and a view is never a DML target.
pub(crate) fn fetch_bound(
    client: &mut GnitzClient,
    table_id: u64,
    access: &Access,
    sink: &ReadSink,
    reply_schema: &Arc<Schema>,
) -> Result<ZSetBatch, GnitzSqlError> {
    let mut out: Option<ZSetBatch> = None;
    // The client stays an argument rather than a capture, so the borrow of
    // `client` and the borrow of `out` never overlap.
    let mut send = |client: &mut GnitzClient, spec: ReadSpec| -> Result<(), GnitzSqlError> {
        let batch = client.scan_spec_local_first(table_id, spec, reply_schema)?;
        match out.as_mut() {
            Some(acc) => acc.extend_from_owned(batch),
            None => out = Some(batch),
        }
        Ok(())
    };
    match &access.bound {
        ReadBound::PkSet(keys) if !keys.fits_one_request() => {
            for part in keys.per_request() {
                send(client, access.spec(ReadBound::PkSet(part), sink))?;
            }
        }
        bound => send(client, access.spec(bound.clone(), sink))?,
    }
    Ok(out.unwrap_or_else(|| ZSetBatch::new(reply_schema)))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/plan.rs"]
mod tests;
