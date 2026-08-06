//! WHERE → access-path **planning and fetching** for every direct-read verb: the
//! one recognizer ladder ([`bound_and_predicate`], over **bound conjuncts**
//! recognized by [`crate::access`]) turning a WHERE into an [`AccessPlan`], the
//! `ReadSpec` dispatcher that walks it ([`fetch_bound`]), and the LIMIT/OFFSET
//! literal readers. Read-only analysis and fetching — no row mutation lives here,
//! and recognition itself lives in the `access` leaf. `select` and `mutate` sink
//! into this module; it never references either.

use crate::access::{
    best_index_bound, pk_bound_is_preemptible, pk_point_tuple, try_extract_pk_in, try_extract_pk_range,
    IndexRangeCandidate,
};
use crate::ast_util::expr_usize_literal;
use crate::error::GnitzSqlError;
use crate::expr_lower::compile_wire_predicate;
use crate::ir::BoundExpr;
use gnitz_core::{ClientError, GnitzClient, IndexMeta, PkTuple, Schema, ZSetBatch};
use gnitz_wire::{ReadBound, ReadSink, ReadSpec};
use sqlparser::ast::LimitClause;
use std::sync::Arc;

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

/// A WHERE turned into an access path: the pushed-down [`ReadBound`] (an access
/// superset) and the compiled server-side predicate re-imposing whatever the
/// bound does not apply. Together they are the whole WHERE, so the reply rows a
/// [`fetch_bound`] read returns are final.
///
/// Opaque: [`fetch_bound`] is the only way to run it, and
/// [`AccessPlan::buffered_scope`] the only way to complete it over rows the
/// server never saw. `SELECT` needs the former alone.
pub(crate) struct AccessPlan<'e> {
    bound: ReadBound,
    predicate: Vec<u8>,
    /// The WHERE this plan serves, `None` when there was none.
    where_expr: Option<&'e BoundExpr>,
    /// The bound conjuncts the walk does not apply exactly.
    residual: Vec<&'e BoundExpr>,
}

impl<'e> AccessPlan<'e> {
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
    pub fn buffered_scope(&self, schema: &Schema) -> (Option<Vec<PkTuple>>, &[&'e BoundExpr]) {
        let stride = schema.pk_stride() as u8;
        let keys = match &self.bound {
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

/// Bind-once WHERE → the access plan that serves it. The one way any statement
/// turns a bound WHERE into an access path.
///
/// The predicate is: empty for `PkSet` (the gather is exact); the extractor's
/// residual for `PkRange` (a byte-exact walk at any PK width — consumed
/// conjuncts are applied exactly and stripped); the whole bound WHERE for
/// `None`; and for an `IndexRange` whichever [`index_plan`] can compile. A PK
/// bound that pins no PK column yields to a point covering every column of a
/// UNIQUE index, which admits at most one row. A LIKE / string-arithmetic WHERE
/// the expression VM cannot compile is an `Unsupported`, propagated.
///
/// `fetch_indexes` is the GET_INDICES probe, injected so the ladder stays
/// client-free; it is called at most once per statement (`best_index_bound`
/// memoizes across its two collectors, and the ladder reaches it once).
pub(crate) fn bound_and_predicate<'e, F>(
    schema: &Schema,
    where_expr: Option<&'e BoundExpr>,
    budget: ReadBudget,
    mut fetch_indexes: F,
) -> Result<AccessPlan<'e>, GnitzSqlError>
where
    F: FnMut() -> Result<Arc<Vec<IndexMeta>>, ClientError>,
{
    let Some(bound_where) = where_expr else {
        return Ok(AccessPlan {
            bound: ReadBound::None,
            predicate: Vec::new(),
            where_expr: None,
            residual: Vec::new(),
        });
    };

    // `pk IN (…)` → an exact gather of those keys, with the remaining conjuncts as
    // the predicate. Keys ship deduplicated (`try_extract_pk_in`); the worker
    // OPK-sorts before its forward sweep.
    let gather = try_extract_pk_in(bound_where, schema)
        .filter(|(keys, _)| budget == ReadBudget::MayChunk || keys.len() <= gnitz_wire::MAX_PK_SET_KEYS);
    if let Some((keys, residual)) = gather {
        let predicate = compile_read_spec_predicate(&residual, schema)?;
        return Ok(AccessPlan {
            bound: ReadBound::PkSet(keys),
            predicate,
            where_expr,
            residual,
        });
    }

    // A PK equality / range → a byte-exact bounded PK walk; the residual (the WHERE
    // minus every conjunct the walk applies exactly) is the predicate. Exactness at
    // any PK width is what serves a wide (U128) PK range without the predicate VM.
    // It yields only when the descriptor pins no PK column and a point covering
    // every column of a UNIQUE index is available: that admits one row where an
    // unpinned PK range admits the table.
    if let Some((desc, residual)) = try_extract_pk_range(bound_where, schema) {
        if pk_bound_is_preemptible(&desc, bound_where, schema) {
            // The index arm re-imposes more of the WHERE than the PK arm's
            // residual, so it can need a conjunct the VM refuses (a wide literal,
            // a U128 column) that the PK walk consumes byte-exactly. Keep the PK
            // walk instead of failing the query; an uncompilable residual still
            // raises below.
            if let Some(c) = best_index_bound(bound_where, schema, &mut fetch_indexes)?.filter(|c| c.is_unique_point())
            {
                if let Some(p) = if_supported(index_plan(c, bound_where, schema))? {
                    return Ok(p);
                }
            }
        }
        let predicate = compile_read_spec_predicate(&residual, schema)?;
        return Ok(AccessPlan {
            bound: ReadBound::PkRange(desc),
            predicate,
            where_expr,
            residual,
        });
    }

    // The best secondary-index bound, keeping its residual.
    if let Some(c) = best_index_bound(bound_where, schema, &mut fetch_indexes)? {
        return index_plan(c, bound_where, schema);
    }

    let predicate = compile_read_spec_predicate(&[bound_where], schema)?;
    Ok(AccessPlan {
        bound: ReadBound::None,
        predicate,
        where_expr,
        residual: vec![bound_where],
    })
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
    if let Some(predicate) = if_supported(compile_read_spec_predicate(&[bound_where], schema))? {
        return Ok(AccessPlan {
            bound: ReadBound::IndexRange {
                idx_cols,
                exact: false,
                desc: c.desc,
            },
            predicate,
            where_expr: Some(bound_where),
            residual: vec![bound_where],
        });
    }
    // Something in the WHERE has no compiled form. The exact walk applies the
    // bounded conjuncts byte-exactly; if what is left over still cannot compile,
    // that error is the real one.
    let predicate = compile_read_spec_predicate(&c.residual, schema)?;
    Ok(AccessPlan {
        bound: ReadBound::IndexRange {
            idx_cols,
            exact: true,
            desc: c.desc,
        },
        predicate,
        where_expr: Some(bound_where),
        residual: c.residual,
    })
}

/// AND-combine the bound residual conjuncts and compile to the wire predicate
/// blob. Empty input or a statically-true predicate → an empty blob (the bound is
/// exact).
fn compile_read_spec_predicate(exprs: &[&BoundExpr], schema: &Schema) -> Result<Vec<u8>, GnitzSqlError> {
    let Some(pred) = crate::ir::and_fold(exprs.iter().map(|e| (*e).clone())) else {
        return Ok(Vec::new());
    };
    compile_wire_predicate(&pred, &schema.columns)
}

// ---------------------------------------------------------------------------
// Fetching
// ---------------------------------------------------------------------------

/// Run `plan`'s bound as a `ReadSpec` under `sink` and `reply_schema`,
/// concatenating the replies.
///
/// A `PkSet` is chunked at `MAX_PK_SET_KEYS` — the decoder's per-gather cap —
/// which is what lets a [`ReadBudget::MayChunk`] caller plan a gather of any
/// length; every other bound is one request. Absent keys contribute no rows, so
/// a count taken off the reply reports rows actually touched.
pub(crate) fn fetch_bound(
    client: &mut GnitzClient,
    table_id: u64,
    plan: &AccessPlan<'_>,
    sink: &ReadSink,
    reply_schema: &Schema,
) -> Result<Option<ZSetBatch>, GnitzSqlError> {
    let mut out: Option<ZSetBatch> = None;
    let mut send = |client: &mut GnitzClient, bound: &ReadBound| -> Result<(), GnitzSqlError> {
        let blob = ReadSpec::encode_parts(bound, &plan.predicate, sink);
        if let Some(batch) = client
            .scan_spec(table_id, &blob, reply_schema)
            .map_err(GnitzSqlError::Exec)?
        {
            match &mut out {
                None => out = Some(batch),
                Some(acc) => acc.extend_from_owned(batch),
            }
        }
        Ok(())
    };
    match &plan.bound {
        ReadBound::PkSet(keys) if keys.len() > gnitz_wire::MAX_PK_SET_KEYS => {
            for chunk in keys.chunks(gnitz_wire::MAX_PK_SET_KEYS) {
                send(client, &ReadBound::PkSet(chunk.to_vec()))?;
            }
        }
        bound => send(client, bound)?,
    }
    Ok(out)
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
mod tests {
    use super::*;
    use crate::test_support::{bind_where, col_def, idx_metas_flagged, pk_schema, two_col};
    use gnitz_core::TypeCode;

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

    /// The plan for `where_expr` against `lists` (the table's indexes), with the
    /// index-probe count — a probe is wire traffic, so "zero" is contract.
    fn plan_of<'e>(
        where_expr: Option<&'e BoundExpr>,
        schema: &Schema,
        lists: &[(&[u32], bool)],
        budget: ReadBudget,
    ) -> (AccessPlan<'e>, u32) {
        let calls = std::cell::Cell::new(0);
        let plan = bound_and_predicate(schema, where_expr, budget, || {
            calls.set(calls.get() + 1);
            Ok(idx_metas_flagged(lists))
        })
        .expect("the WHERE must plan");
        (plan, calls.get())
    }

    /// The bound's discriminant name, for a shape assertion that does not care
    /// about the descriptor's contents.
    fn shape(bound: &ReadBound) -> &'static str {
        match bound {
            ReadBound::None => "None",
            ReadBound::PkRange(_) => "PkRange",
            ReadBound::IndexRange { .. } => "IndexRange",
            ReadBound::PkSet(_) => "PkSet",
        }
    }

    /// Every WHERE shape, one ladder: which bound it takes, how much of it stays
    /// residual, how many keys the bound pins, and how many index probes it cost
    /// (a probe is wire traffic, so "zero" is contract). The schema is `(id U64
    /// pk, v I64)` with an index on `v`, so every rung is reachable from one
    /// table.
    #[test]
    fn the_ladder_maps_each_where_shape_to_its_bound() {
        let schema = pk_schema(TypeCode::U64);
        let idx: &[(&[u32], bool)] = &[(&[1], false)];
        for (sql, want_shape, want_residual, want_keys, want_calls) in [
            // No WHERE: nothing to walk, nothing to re-impose, no probe.
            (None, "None", 0, 0, 0),
            // A `pk IN (…)` gather, and the point a one-key list folds to at bind.
            (Some("id IN (7, 9)"), "PkSet", 0, 2, 0),
            (Some("id IN (7)"), "PkRange", 0, 1, 0),
            (Some("id = 7"), "PkRange", 0, 1, 0),
            // `NOT IN` binds to `Not(…)`, which no PK recognizer matches.
            (Some("id NOT IN (7, 9)"), "None", 1, 0, 0),
            // A companion conjunct rides the residual of a key-pinning bound: the
            // key restriction supplies the consumed PK conjunct, the residual the rest.
            (Some("id IN (7, 9) AND v > 5"), "PkSet", 1, 2, 0),
            (Some("id = 7 AND v > 5"), "PkRange", 1, 1, 0),
            // No PK conjunct: the index rung, then the unbounded scan. An
            // arithmetic WHERE has no `col OP literal` conjunct at all, so it
            // reaches the scan without probing.
            (Some("v = 7"), "IndexRange", 1, 0, 1),
            (Some("id + v = 7"), "None", 1, 0, 0),
        ] {
            let bound_where = sql.map(|s| bind_where(s, &schema));
            let (plan, calls) = plan_of(bound_where.as_ref(), &schema, idx, ReadBudget::OneRequest);
            let label = sql.unwrap_or("<no WHERE>");
            assert_eq!(shape(&plan.bound), want_shape, "{label}");
            assert_eq!(plan.residual.len(), want_residual, "{label}: residual");
            assert_eq!(
                plan.buffered_scope(&schema).0.map_or(0, |k| k.len()),
                want_keys,
                "{label}: pinned keys"
            );
            assert_eq!(calls, want_calls, "{label}: index probes");
            assert_eq!(
                plan.predicate.is_empty(),
                want_residual == 0,
                "{label}: the residual is what ships as a predicate"
            );
        }
    }

    /// The budget is the whole difference between the verbs: a gather past the
    /// wire's per-request key cap declines to the rest of the ladder (an ordinary
    /// predicate scan) unless the caller may chunk.
    #[test]
    fn an_over_cap_pk_in_list_needs_a_chunking_budget() {
        let schema = pk_schema(TypeCode::U64);
        let n = gnitz_wire::MAX_PK_SET_KEYS + 1;
        // Built directly: the same list as SQL text is megabytes for the parser.
        let where_expr = BoundExpr::InList {
            inner: Box::new(BoundExpr::ColRef(0)),
            items: (0..n as i64).map(BoundExpr::LitInt).collect(),
        };
        let (declined, _) = plan_of(Some(&where_expr), &schema, &[], ReadBudget::OneRequest);
        assert_eq!(shape(&declined.bound), "None", "{n} keys past the one-request cap");
        assert!(declined.buffered_scope(&schema).0.is_none());
        assert!(!declined.predicate.is_empty(), "the list ships as a predicate instead");

        let (gathered, _) = plan_of(Some(&where_expr), &schema, &[], ReadBudget::MayChunk);
        assert_eq!(shape(&gathered.bound), "PkSet");
        assert_eq!(gathered.buffered_scope(&schema).0.map_or(0, |k| k.len()), n);
    }

    /// The index walk is marked exact — and its conjunct stripped from the
    /// predicate — exactly when the predicate could not carry that conjunct: a
    /// literal past the VM's `i64` constant on a narrow column, or a wide-int
    /// column outright. An ordinary narrow bound keeps the whole WHERE, leaving
    /// the worker free to trade the walk for a full cursor.
    #[test]
    fn an_index_walk_is_exact_only_when_the_predicate_cannot_carry_its_conjunct() {
        // `(id U64 pk, v U64, w U64)` — `w` keeps a companion conjunct off the PK,
        // which would otherwise take the PK-range rung ahead of any index.
        let narrow = Schema {
            columns: vec![
                col_def("id", TypeCode::U64, false),
                col_def("v", TypeCode::U64, false),
                col_def("w", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        let wide = two_col(TypeCode::U128); // `val` is U128
        let idx: &[(&[u32], bool)] = &[(&[1], false)];
        for (schema, sql, want_exact, want_residual) in [
            (&narrow, "v = 5", false, 1),
            (&narrow, "v = 5 AND w = 9", false, 1),
            (&narrow, "v = 18446744073709551615", true, 0),
            (&wide, "val = 7", true, 0),
        ] {
            let where_expr = bind_where(sql, schema);
            let (plan, _) = plan_of(Some(&where_expr), schema, idx, ReadBudget::OneRequest);
            let ReadBound::IndexRange { exact, .. } = plan.bound else {
                panic!("{sql}: expected an index bound, got {}", shape(&plan.bound));
            };
            assert_eq!(exact, want_exact, "{sql}: exactness");
            assert_eq!(plan.residual.len(), want_residual, "{sql}: residual");
        }
    }

    /// A PK bound pinning no PK column yields to a point covering every column of
    /// a UNIQUE index — one row, where the unpinned range admits the table.
    #[test]
    fn an_unpinned_pk_range_yields_to_a_full_unique_point() {
        let schema = two_col(TypeCode::U64);
        let where_expr = bind_where("pk > 0 AND val = 42", &schema);
        let (plan, calls) = plan_of(Some(&where_expr), &schema, &[(&[1], true)], ReadBudget::OneRequest);
        assert_eq!(shape(&plan.bound), "IndexRange");
        assert!(
            plan.buffered_scope(&schema).0.is_none(),
            "an index bound never pins a PK key"
        );
        assert_eq!(calls, 1, "one probe serves the arbitration");
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
