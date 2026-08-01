//! WHERE access-path **classification + fetching** for the DML verbs: the
//! `AccessPath` the UPDATE/DELETE driver consumes (over **bound conjuncts**,
//! recognized by [`crate::access`]), the multi-key seek fetcher serving
//! [`AccessPath::PkMultiSeek`], the first-existing-index fetch loop, and the
//! LIMIT/OFFSET literal readers. Read-only analysis and fetching — no row mutation
//! lives here, and recognition itself lives in the `access` leaf. `select` and
//! `mutate` sink into this module; it never references either.

use crate::access::{try_extract_pk_in, try_extract_pk_seek_residual};
use crate::ast_util::expr_usize_literal;
use crate::error::GnitzSqlError;
use crate::ir::BoundExpr;
use gnitz_core::{ClientError, GnitzClient, PkTuple, Schema, ZSetBatch};
use sqlparser::ast::LimitClause;

// ---------------------------------------------------------------------------
// Access-path classification
// ---------------------------------------------------------------------------

/// How a single-table UPDATE/DELETE `WHERE` (or its absence) selects rows.
/// `mutate::resolve_where_rows` matches on this to fetch the candidate batch. The
/// index-vs-scan choice inside [`AccessPath::Filtered`] is deferred to execution,
/// which must probe the live index set (and fall back to a predicate scan when no
/// index serves the predicate).
pub(crate) enum AccessPath<'e> {
    /// No `WHERE` — a full scan selects every row.
    ScanAll,
    /// `WHERE` fully binds the PK: a single point seek; `residual` post-filters the
    /// seeked row's non-PK conjuncts (borrowed from the bound `WHERE`).
    PkSeek { pk: PkTuple, residual: Vec<&'e BoundExpr> },
    /// `WHERE` holds a `pk IN (literal, …)` conjunct on a single-column PK: one point
    /// seek per key, `residual` post-filtering the gathered rows' other conjuncts.
    /// Mutually exclusive with [`AccessPath::PkSeek`].
    PkMultiSeek {
        pks: Vec<u128>,
        residual: Vec<&'e BoundExpr>,
    },
    /// `WHERE` needs a secondary-index probe, falling back to a predicate full scan
    /// when no index serves it.
    Filtered { where_expr: &'e BoundExpr },
}

/// Classify a single-table UPDATE/DELETE bound `WHERE` (or its absence) into the
/// access path that serves it. Pure: detecting a full PK binding (single- or
/// multi-key) is the only decision made here; the index-vs-scan split is left to
/// execution.
pub(crate) fn classify_access<'e>(selection: Option<&'e BoundExpr>, schema: &Schema) -> AccessPath<'e> {
    match selection {
        None => AccessPath::ScanAll,
        Some(where_expr) => {
            if let Some((pks, residual)) = try_extract_pk_in(where_expr, schema) {
                AccessPath::PkMultiSeek { pks, residual }
            } else if let Some((pk, residual)) = try_extract_pk_seek_residual(where_expr, schema) {
                AccessPath::PkSeek { pk, residual }
            } else {
                AccessPath::Filtered { where_expr }
            }
        }
    }
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
// Fetchers
// ---------------------------------------------------------------------------

/// Fetch every key of a [`AccessPath::PkMultiSeek`] list — the UPDATE/DELETE
/// committed-row fetch — as identity ScanSpec `PkSet` gathers (no predicate, no
/// projection): one round trip per `MAX_PK_SET_KEYS` chunk instead of one per key.
/// Absent keys contribute no rows. `pks` must be deduplicated (`try_extract_pk_in`
/// guarantees it; the wire decoder rejects duplicates).
pub(crate) fn seek_pk_multi(
    client: &mut GnitzClient,
    table_id: u64,
    schema: &Schema,
    pks: &[u128],
) -> Result<Option<ZSetBatch>, GnitzSqlError> {
    let mut out: Option<ZSetBatch> = None;
    for chunk in pks.chunks(gnitz_wire::MAX_PK_SET_KEYS) {
        let spec = gnitz_wire::ReadSpec {
            bound: gnitz_wire::ReadBound::PkSet(chunk.to_vec()),
            predicate: Vec::new(),
            sink: gnitz_wire::ReadSink::all_rows(),
        };
        if let Some(batch) = client
            .scan_spec(table_id, &spec.encode(), schema)
            .map_err(GnitzSqlError::Exec)?
        {
            match &mut out {
                None => out = Some(batch),
                Some(acc) => acc.extend_from_owned(batch),
            }
        }
    }
    Ok(out)
}

/// Try each index candidate's seek in order, treating `ClientError::NoIndex` as
/// "try the next candidate". Returns the first hit's `(candidate, reply)` — a hit
/// with no matching rows is still terminal — or `None` when no candidate's index
/// exists. The loop skeleton of the UPDATE/DELETE filtered fetch.
pub(crate) fn first_index_hit<C, T>(
    candidates: Vec<C>,
    mut seek: impl FnMut(&C) -> Result<T, ClientError>,
) -> Result<Option<(C, T)>, GnitzSqlError> {
    for cand in candidates {
        match seek(&cand) {
            Ok(res) => return Ok(Some((cand, res))),
            Err(ClientError::NoIndex) => continue,
            Err(e) => return Err(GnitzSqlError::Exec(e)),
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_query_sql(src: &str) -> sqlparser::ast::Query {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        match Parser::parse_sql(&GenericDialect {}, src)
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
        {
            sqlparser::ast::Statement::Query(q) => *q,
            _ => panic!("not a query"),
        }
    }

    /// The IN-list arity picks the path: one key folds to `Eq` at bind and takes the
    /// unicast point seek; two or more take the multi-key gather; `NOT IN` binds to
    /// `Not(…)`, which no recognizer matches, so it falls to a filtered scan.
    #[test]
    fn in_list_arity_picks_the_access_path() {
        use crate::test_support::{bind_where, pk_schema};
        use gnitz_core::TypeCode;

        let schema = pk_schema(TypeCode::U64);
        for (sql, want) in [
            ("id IN (7)", "PkSeek"),
            ("id IN (7, 9)", "PkMultiSeek"),
            ("id NOT IN (7)", "Filtered"),
            ("id NOT IN (7, 9)", "Filtered"),
        ] {
            let got = match classify_access(Some(&bind_where(sql, &schema)), &schema) {
                AccessPath::PkSeek { residual, .. } => {
                    assert!(residual.is_empty(), "{sql}");
                    "PkSeek"
                }
                AccessPath::PkMultiSeek { pks, residual } => {
                    assert_eq!(pks, vec![7, 9], "{sql}");
                    assert!(residual.is_empty(), "{sql}");
                    "PkMultiSeek"
                }
                AccessPath::Filtered { .. } => "Filtered",
                AccessPath::ScanAll => "ScanAll",
            };
            assert_eq!(got, want, "{sql}");
        }
    }

    /// A `pk IN (…)` conjunct still routes to the gather when the WHERE has more to
    /// it; the rest rides along as the residual the fetched rows are filtered by.
    #[test]
    fn pk_in_with_a_companion_conjunct_still_gathers() {
        use crate::test_support::{bind_where, pk_schema};
        use gnitz_core::TypeCode;

        let schema = pk_schema(TypeCode::U64);
        let where_expr = bind_where("id IN (7, 9) AND v > 5", &schema);
        match classify_access(Some(&where_expr), &schema) {
            AccessPath::PkMultiSeek { pks, residual } => {
                assert_eq!(pks, vec![7, 9]);
                assert_eq!(residual.len(), 1, "`v > 5` post-filters the gathered rows");
            }
            _ => panic!("expected PkMultiSeek"),
        }
    }

    #[test]
    fn extract_limit_offset_literals_and_errors() {
        let q = parse_query_sql("SELECT * FROM t LIMIT 3 OFFSET 2");
        assert_eq!(extract_limit(&q).unwrap(), Some(3));
        assert_eq!(extract_offset(&q).unwrap(), 2);
        // MySQL `LIMIT off, lim`.
        let q = parse_query_sql("SELECT * FROM t LIMIT 2, 3");
        assert_eq!(extract_limit(&q).unwrap(), Some(3));
        assert_eq!(extract_offset(&q).unwrap(), 2);
        // Absent → None / 0.
        let q = parse_query_sql("SELECT * FROM t");
        assert_eq!(extract_limit(&q).unwrap(), None);
        assert_eq!(extract_offset(&q).unwrap(), 0);
        // A non-integer-literal errors instead of silently degrading.
        for sql in [
            "SELECT * FROM t LIMIT 1+1",
            "SELECT * FROM t LIMIT 'x'",
            "SELECT * FROM t LIMIT -1",
        ] {
            assert!(
                matches!(extract_limit(&parse_query_sql(sql)), Err(GnitzSqlError::Unsupported(_))),
                "{sql} must error"
            );
        }
        for sql in [
            "SELECT * FROM t LIMIT 1 OFFSET 1+1",
            "SELECT * FROM t LIMIT 1 OFFSET 'x'",
        ] {
            assert!(
                matches!(
                    extract_offset(&parse_query_sql(sql)),
                    Err(GnitzSqlError::Unsupported(_))
                ),
                "{sql} must error"
            );
        }
    }
}
