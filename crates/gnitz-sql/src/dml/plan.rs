//! WHERE → access-path **planning and fetching** for every direct-read verb: the
//! one recognizer ladder ([`bound_and_predicate`], over **bound conjuncts**
//! recognized by [`crate::access`]) turning a WHERE into an [`AccessPlan`], the
//! `ReadSpec` dispatcher that walks it ([`fetch_bound`]), and the LIMIT/OFFSET
//! literal readers. Read-only analysis and fetching — no row mutation lives here,
//! and recognition itself lives in the `access` leaf. `select` and `mutate` sink
//! into this module; it never references either.

use std::sync::Arc;

use crate::access::{
    pk_point_tuple, ranked_index_bounds, try_extract_pk_in, try_extract_pk_range, IndexRangeCandidate,
};
use crate::ast_util::expr_usize_literal;
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_wire_conjuncts;
use crate::ir::BoundExpr;
use gnitz_core::{GnitzClient, IndexMeta, PkTuple, Schema, ZSetBatch};
use gnitz_wire::{ReadBound, ReadSink, ReadSpec};
use sqlparser::ast::LimitClause;

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

    /// The `ReadSpec` blob this access ships under `sink`, walking `bound` rather
    /// than [`Self::bound`], so a chunked `PkSet` gather can send a sub-range per
    /// request under the same predicate and sink. The one encode site.
    pub(crate) fn encode(&self, bound: &ReadBound, sink: &ReadSink) -> Vec<u8> {
        ReadSpec::encode_parts(bound, &self.predicate, sink)
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
    /// The WHERE this plan serves, `None` when there was none.
    where_expr: Option<&'e BoundExpr>,
    /// The bound conjuncts the walk does not apply exactly.
    residual: Vec<&'e BoundExpr>,
}

impl<'e> AccessPlan<'e> {
    /// The plan for `bound`, its predicate compiled from `residual`. The only
    /// constructor, and the only producer of an [`Access`], so a plan cannot carry
    /// a predicate that is not its own residual's. Every rung of the ladder below
    /// relies on that.
    pub(super) fn new(
        bound: ReadBound,
        where_expr: Option<&'e BoundExpr>,
        residual: Vec<&'e BoundExpr>,
        schema: &Schema,
    ) -> Result<Self, GnitzSqlError> {
        Ok(AccessPlan {
            access: Access {
                predicate: compile_wire_conjuncts(&residual, &schema.columns)?,
                bound,
            },
            where_expr,
            residual,
        })
    }

    /// What a DML verb must do about the transaction's own buffered rows, which
    /// no server-side walk ever saw: the key set to restrict them to (`None` =
    /// every PK the transaction touched), and the conjuncts to re-impose on them.
    ///
    /// The two travel together because they are one decision. A bound naming an
    /// **exact key set** — a `PkSet` gather, or a `PkRange` pinning *every* PK
    /// column — restricts the candidates itself, which supplies the PK conjunct
    /// its walk consumed, so the residual completes the WHERE and nothing the
    /// access recognizers consume (a wide PK literal, a U128 equality) reaches the
    /// expression VM. Any looser bound — a PK *prefix* point names a key group,
    /// not a key — restricts nothing, so the FULL WHERE applies.
    ///
    /// The keys are derived here rather than at plan time: only a DML statement
    /// inside an open transaction asks, and a max-size gather is megabytes of
    /// `PkTuple`.
    pub(crate) fn buffered_scope(&self, schema: &Schema) -> (Option<Vec<PkTuple>>, &[&'e BoundExpr]) {
        let stride = schema.pk_stride() as u8;
        let keys = match &self.access.bound {
            ReadBound::PkSet(keys) => Some(keys.iter().map(|&k| PkTuple::from_u128(stride, k)).collect()),
            ReadBound::PkRange(desc) => pk_point_tuple(desc, schema).map(|k| vec![k]),
            _ => None,
        };
        match keys {
            Some(keys) => (Some(keys), &self.residual),
            None => (None, self.where_expr.as_slice()),
        }
    }
}

/// Bind a single-table `WHERE` (or its absence) for [`bound_and_predicate`].
pub(crate) fn bind_where(
    schema: &Schema,
    where_expr: Option<&sqlparser::ast::Expr>,
) -> Result<Option<BoundExpr>, GnitzSqlError> {
    where_expr
        .map(|we| crate::bind::bind_single_table(we, schema))
        .transpose()
}

/// Bind-once WHERE → the access plan that serves it. The one way any statement
/// turns a bound WHERE into an access path.
///
/// The predicate is: empty for `PkSet` (the gather is exact); the extractor's
/// residual for `PkRange` (a byte-exact walk at any PK width — consumed
/// conjuncts are applied exactly and stripped); the whole bound WHERE for
/// `None`; and for an `IndexRange` whichever [`index_plan`] can compile. A PK
/// bound that pins no PK column yields to a point covering every column of a
/// UNIQUE index, which admits at most one row. A WHERE the expression VM cannot
/// compile is an `Unsupported`, propagated.
///
/// `indexes` is the relation's declared secondary-index list, which the caller
/// already holds: the pure path reads it off the statement's resolved descriptor,
/// the write verbs off the one `resolve_base_table` returned.
///
/// The caller owns the bound WHERE, because the plan borrows its conjuncts.
pub(crate) fn bound_and_predicate<'e>(
    schema: &Schema,
    where_expr: Option<&'e BoundExpr>,
    budget: ReadBudget,
    indexes: &[IndexMeta],
) -> Result<AccessPlan<'e>, GnitzSqlError> {
    let Some(bound_where) = where_expr else {
        return AccessPlan::new(ReadBound::None, None, Vec::new(), schema);
    };

    // `pk IN (…)` → an exact gather of those keys, with the remaining conjuncts as
    // the predicate. Keys ship deduplicated (`try_extract_pk_in`); the worker
    // OPK-sorts before its forward sweep.
    let gather = try_extract_pk_in(bound_where, schema)
        .filter(|(keys, _)| budget == ReadBudget::MayChunk || keys.len() <= gnitz_wire::MAX_PK_SET_KEYS);
    if let Some((keys, residual)) = gather {
        return AccessPlan::new(ReadBound::PkSet(keys), where_expr, residual, schema);
    }

    // A PK equality / range → a byte-exact bounded PK walk; the residual (the WHERE
    // minus every conjunct the walk applies exactly) is the predicate. Exactness at
    // any PK width is what serves a wide (U128) PK range without the predicate VM.
    // It yields only when the descriptor pins no PK column and a point covering
    // every column of a UNIQUE index is available: that admits one row where an
    // unpinned PK range admits the table.
    if let Some((desc, residual)) = try_extract_pk_range(bound_where, schema) {
        // A descriptor pinning nothing is the only one worth giving up: the ladder
        // bets that a pinned leading PK column shares the distribution prefix and
        // unicasts, which holds only under a `CLUSTER BY` shorter than the PK.
        if desc.pins_none() {
            // The index arm re-imposes more of the WHERE than the PK arm's
            // residual, so a conjunct the VM refuses (a wide literal, a U128
            // column) can sink it; keep the PK walk rather than fail the query.
            if let Some(c) = ranked_index_bounds(bound_where, schema, indexes)
                .into_iter()
                .next()
                .filter(|c| c.is_unique_point())
            {
                if let Some(p) = if_supported(index_plan(c, bound_where, schema))? {
                    return Ok(p);
                }
            }
        }
        return AccessPlan::new(ReadBound::PkRange(desc), where_expr, residual, schema);
    }

    // Most-constrained first, and the first whose plan compiles wins: the tightest
    // bound is not servable if its leftover conjunct has no compiled form, where a
    // looser candidate consumes that same conjunct byte-exactly.
    for c in ranked_index_bounds(bound_where, schema, indexes) {
        if let Some(p) = if_supported(index_plan(c, bound_where, schema))? {
            return Ok(p);
        }
    }

    AccessPlan::new(ReadBound::None, where_expr, vec![bound_where], schema)
}

/// `Ok(None)` for the one error a ladder rung may be abandoned on — `Unsupported`
/// means "this rung cannot express the WHERE", so the next rung gets its turn.
/// Every other error is fatal.
fn if_supported<T>(r: Result<T, GnitzSqlError>) -> Result<Option<T>, GnitzSqlError> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(GnitzSqlError::Unsupported(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// An index candidate → its plan.
///
/// **Keeping the whole WHERE in the predicate is preferred**, because it leaves
/// the worker free to trade the index walk for a full cursor when the range
/// covers too much of the table. The bounded conjuncts are stripped — and the
/// walk marked `exact`, which forbids that trade — exactly when the predicate
/// cannot carry them, which the compiler alone decides (a wide-int index column
/// has no VM register; a literal past the VM's `i64` constant has no encoding).
/// Stripping and exactness are set together, so a conjunct the predicate drops is
/// always one the walk applies.
fn index_plan<'e>(
    c: IndexRangeCandidate<'e>,
    bound_where: &'e BoundExpr,
    schema: &Schema,
) -> Result<AccessPlan<'e>, GnitzSqlError> {
    let idx_cols = gnitz_wire::pack_pk_cols(c.idx_cols.as_slice());
    let inexact = ReadBound::IndexRange {
        idx_cols,
        exact: false,
        desc: c.desc,
    };
    if let Some(p) = if_supported(AccessPlan::new(inexact, Some(bound_where), vec![bound_where], schema))? {
        return Ok(p);
    }
    // Something in the WHERE has no compiled form. The exact walk applies the
    // bounded conjuncts byte-exactly; if what is left over still cannot compile,
    // that error is the real one.
    let exact = ReadBound::IndexRange {
        idx_cols,
        exact: true,
        desc: c.desc,
    };
    AccessPlan::new(exact, Some(bound_where), c.residual, schema)
}

// ---------------------------------------------------------------------------
// Fetching
// ---------------------------------------------------------------------------

/// Run `plan`'s bound as a `ReadSpec` under `sink` and `reply_schema`,
/// concatenating the replies.
///
/// The read is local-first, and that is safe for the read-before-write callers
/// too: only a view can be mirrored, and every writable target the binder admits
/// is a table or a stream, so a DML `table_id` can never name a copy.
///
/// A `PkSet` is chunked at `MAX_PK_SET_KEYS` — the decoder's per-gather cap —
/// which is what lets a [`ReadBudget::MayChunk`] caller plan a gather of any
/// length; every other bound is one request. Absent keys contribute no rows, so
/// a count taken off the reply reports rows actually touched. An empty result is
/// an empty batch, not an absent one — no caller distinguishes the two.
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
    let mut send = |client: &mut GnitzClient, bound: &ReadBound| -> Result<(), GnitzSqlError> {
        let blob = access.encode(bound, sink);
        if let Some(batch) = client.scan_spec_local_first(table_id, &blob, reply_schema)? {
            match out.as_mut() {
                Some(acc) => acc.extend_from_owned(batch),
                None => out = Some(batch),
            }
        }
        Ok(())
    };
    match &access.bound {
        ReadBound::PkSet(keys) if keys.len() > gnitz_wire::MAX_PK_SET_KEYS => {
            for chunk in keys.chunks(gnitz_wire::MAX_PK_SET_KEYS) {
                send(client, &ReadBound::PkSet(chunk.to_vec()))?;
            }
        }
        bound => send(client, bound)?,
    }
    Ok(out.unwrap_or_else(|| ZSetBatch::new(reply_schema)))
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET
// ---------------------------------------------------------------------------

/// The `LIMIT n` value, or `None` when absent. A non-integer-literal LIMIT
/// (`LIMIT 1+1`, `LIMIT 'x'`) is a clean error — silently degrading it would
/// return every row, violating the unhonored-clause contract.
pub(crate) fn extract_limit(query: &sqlparser::ast::Query) -> Result<Option<usize>, GnitzSqlError> {
    let limit = match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit: Some(e), .. }) => e,
        Some(LimitClause::OffsetCommaLimit { limit: e, .. }) => e,
        _ => return Ok(None),
    };
    expr_usize_literal(limit, "LIMIT").map(Some)
}

/// The `OFFSET n` value (both `LIMIT … OFFSET n` and the MySQL `LIMIT off, lim`
/// form), or `0` when absent. Mirrors [`extract_limit`]: a non-integer-literal
/// value errors rather than silently skipping nothing.
pub(crate) fn extract_offset(query: &sqlparser::ast::Query) -> Result<usize, GnitzSqlError> {
    let offset = match &query.limit_clause {
        Some(LimitClause::LimitOffset { offset: Some(o), .. }) => &o.value,
        Some(LimitClause::OffsetCommaLimit { offset, .. }) => offset,
        _ => return Ok(0),
    };
    expr_usize_literal(offset, "OFFSET")
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/plan.rs"]
mod tests;
