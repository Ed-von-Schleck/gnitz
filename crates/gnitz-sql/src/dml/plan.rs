//! WHERE access-path planning for the DML verbs: the conjunct flattener, the
//! PK-seek / secondary-index equality / ordered-range recognizers, the
//! `AccessPath` classification the UPDATE/DELETE driver consumes, and the
//! multi-key seek fetcher serving [`AccessPath::PkMultiSeek`]. Read-only
//! analysis and fetching — no row mutation lives here. `select` and `mutate`
//! sink into this module; it never references either.

use crate::ast_util::{flatten_conjuncts, single_relation_col_name};
use crate::bind::find_unique_column;
use crate::codec::pk_codec::{
    extract_sql_literal, parse_literal_i128, parse_pk_literal_packed, parse_uuid_str, SqlLiteral,
};
use crate::error::GnitzSqlError;
use gnitz_core::{ClientError, Cut, FixedInt, GnitzClient, PkTuple, RangeDescriptor, Schema, TypeCode, ZSetBatch};
use sqlparser::ast::{BinaryOperator, Expr, LimitClause, Value};
use std::collections::HashSet;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Access-path classification
// ---------------------------------------------------------------------------

/// How a single-table UPDATE/DELETE `WHERE` (or its absence) selects rows.
/// `mutate::resolve_where_rows` matches on this to fetch the candidate batch.
/// The index-vs-scan choice inside [`AccessPath::Filtered`] is deferred to
/// execution, which must probe the live index set (and fall back to a predicate
/// scan when no index serves the predicate).
pub(crate) enum AccessPath<'e> {
    /// No `WHERE` — a full scan selects every row.
    ScanAll,
    /// `WHERE` fully binds the PK: a single point seek; `residual` post-filters
    /// the seeked row's non-PK conjuncts.
    PkSeek { pk: PkTuple, residual: Vec<&'e Expr> },
    /// `WHERE` is exactly `pk IN (literal, …)` on a single-column PK: one point
    /// seek per key, no residual. Mutually exclusive with [`AccessPath::PkSeek`]
    /// — a bare top-level `IN` list never also binds the PK by equality.
    PkMultiSeek { pks: Vec<u128> },
    /// `WHERE` needs a secondary-index probe, falling back to a predicate full
    /// scan when no index serves it.
    Filtered { where_expr: &'e Expr },
}

/// Classify a single-table UPDATE/DELETE `WHERE` (or its absence) into the
/// access path that serves it. Pure: detecting a full PK binding (single- or
/// multi-key) is the only decision made here; the index-vs-scan split is left
/// to execution.
pub(crate) fn classify_access<'e>(selection: Option<&'e Expr>, schema: &Schema) -> AccessPath<'e> {
    match selection {
        None => AccessPath::ScanAll,
        Some(where_expr) => {
            if let Some(pks) = try_extract_pk_in(where_expr, schema) {
                AccessPath::PkMultiSeek { pks }
            } else if let Some((pk, residual)) = try_extract_pk_seek_residual(where_expr, schema) {
                AccessPath::PkSeek { pk, residual }
            } else {
                AccessPath::Filtered { where_expr }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// LIMIT
// ---------------------------------------------------------------------------

pub(crate) fn extract_limit(query: &sqlparser::ast::Query) -> Option<usize> {
    let limit = match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit: Some(e), .. }) => e,
        Some(LimitClause::OffsetCommaLimit { limit: e, .. }) => e,
        _ => return None,
    };
    match limit {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => n.parse::<usize>().ok(),
            _ => None,
        },
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Conjunct flattening + PK-seek extraction
// ---------------------------------------------------------------------------

/// `Some((pk_tuple, residual))` when the conjuncts of `expr` bind every PK
/// column to a literal; `residual` holds the leftover conjuncts (non-PK
/// equalities, ranges, an `OR`-group, a repeated PK column) to filter against
/// the seeked row, and is empty when the `WHERE` is exactly the PK equality.
/// `None` when the PK is not fully bound — the caller then tries a secondary
/// index. This is what lets `WHERE pk = 1 AND name = 'x'` take the PK point
/// lookup (residual `name = 'x'`) instead of degrading to a full scan.
pub(crate) fn try_extract_pk_seek_residual<'e>(expr: &'e Expr, schema: &Schema) -> Option<(PkTuple, Vec<&'e Expr>)> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expr, &mut conjuncts);

    let stride = schema.pk_stride() as u8;
    let mut tuple = PkTuple::new(stride);
    let mut slot_set = [false; gnitz_core::MAX_PK_COLUMNS];
    let mut bound = 0usize;
    let mut residual = Vec::new();

    for &cand in &conjuncts {
        // Consume `pk_col = literal` for an as-yet-unbound PK slot into the
        // tuple; everything else (non-PK columns, non-equalities, a repeated PK
        // column) routes to the residual.
        let mut consumed = false;
        if let Some((col_idx, val)) = try_col_eq_literal(cand, schema) {
            if let Some(pk_pos) = schema.pk_indices().iter().position(|&pi| pi == col_idx) {
                if !slot_set[pk_pos] {
                    slot_set[pk_pos] = true;
                    bound += 1;
                    let off = schema.pk_byte_offset(col_idx);
                    let w = schema.columns[col_idx].type_code.wire_stride();
                    tuple.buf[off..off + w].copy_from_slice(&val.to_le_bytes()[..w]);
                    consumed = true;
                }
            }
        }
        if !consumed {
            residual.push(cand);
        }
    }

    (bound == schema.pk_count()).then_some((tuple, residual))
}

/// Classify a binary op's two operands as (column, literal): the column — bare
/// or qualified (`t.pk`), the crate-wide single-relation convention — may sit on
/// either side, and the literal is whatever `extract_sql_literal` accepts
/// (numbers, optionally negated, or a single-quoted string — never a
/// double-quoted identifier). Returns `(col_name, literal, flipped)`, where
/// `flipped` is `true` when the literal was on the left (`lit OP col`), so a
/// range recognizer can mirror the operator. Shared by the `=` and range
/// (`<,<=,>,>=`) recognizers.
fn split_col_vs_literal<'e>(a: &'e Expr, b: &'e Expr) -> Option<(&'e str, SqlLiteral<'e>, bool)> {
    if let (Some(n), Some(l)) = (single_relation_col_name(a), extract_sql_literal(b)) {
        Some((n, l, false))
    } else if let (Some(n), Some(l)) = (single_relation_col_name(b), extract_sql_literal(a)) {
        Some((n, l, true))
    } else {
        None
    }
}

/// Extracts (col_idx, key) from `col = literal`. Does NOT check index existence.
pub(crate) fn try_col_eq_literal(expr: &Expr, schema: &Schema) -> Option<(usize, u128)> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return None;
    };
    // `=` is symmetric, so the flipped flag is irrelevant here.
    let (col_name, lit, _) = split_col_vs_literal(left, right)?;
    // Ambiguous (a dup-named `SELECT *` view) OR absent → None → fast path
    // declined; the WHERE then falls through to a bind that raises the precise
    // error rather than seeking the wrong column.
    let col_idx = find_unique_column(&schema.columns, col_name).ok().flatten()?;
    let col_tc = schema.columns[col_idx].type_code;

    // UUID: accept a single-quoted UUID string or a non-negated numeric u128
    // (equivalent to the old `n.parse::<u128>()` via parse_pk_literal_packed).
    if col_tc == TypeCode::UUID {
        return match lit {
            SqlLiteral::Str(s) => parse_uuid_str(s).ok().map(|v| (col_idx, v)),
            SqlLiteral::Number(n, false) => parse_pk_literal_packed(TypeCode::UUID, n, false).map(|v| (col_idx, v)),
            _ => None,
        };
    }

    // Non-UUID: numeric literal only. Secondary indexes store column values as
    // zero-extended LE bytes cast to u64; for signed types we produce the same
    // bit pattern by casting through the type's unsigned equivalent. Delegated
    // to parse_pk_literal_packed so the three SEEK/INSERT parse sites cannot drift.
    let SqlLiteral::Number(n_str, negated) = lit else {
        return None;
    };
    let key = parse_pk_literal_packed(col_tc, n_str, negated)?;
    Some((col_idx, key))
}

/// `Some(keys)` when `expr` is exactly `pk IN (literal, …)` on a single-column
/// PK; the keys are deduped (first occurrence wins), so every consumer counts a
/// repeated key once. `None` routes the WHERE back to the seek/index/scan ladder.
pub(crate) fn try_extract_pk_in(expr: &Expr, schema: &Schema) -> Option<Vec<u128>> {
    // Compound PK has no IN-list fast path; fall back to a full delta scan.
    if schema.pk_count() != 1 {
        return None;
    }
    if let Expr::InList {
        expr: col_expr,
        list,
        negated,
        ..
    } = expr
    {
        if *negated {
            return None;
        }
        // Bare or qualified (`t.pk`) — the single-relation convention.
        let col_name = single_relation_col_name(col_expr)?;
        let pk_idx = schema.pk_indices()[0];
        // Resolve through the visibility-aware chokepoint: a hidden PK (a view
        // keyed by a synthetic `_join_pk`/`_group_pk`) is not name-resolvable, so
        // the WHERE falls through to the binder, which raises a clean "column
        // not found" rather than silently seeking it.
        if find_unique_column(&schema.columns, col_name).ok().flatten() != Some(pk_idx) {
            return None;
        }
        let pk_col = &schema.columns[pk_idx];
        let mut seen = HashSet::with_capacity(list.len());
        let mut pks = Vec::with_capacity(list.len());
        for item in list {
            // Optionally-negated numerics, plus single-quoted UUID strings for a
            // UUID PK — exactly the literals `try_col_eq_literal` accepts for the
            // `=` seek, so `IN (…)` and `= …` route identically. A NULL, a
            // non-literal, or an unparseable UUID aborts to the slow scan.
            let v = match extract_sql_literal(item)? {
                SqlLiteral::Number(n, negated) => parse_pk_literal_packed(pk_col.type_code, n, negated)?,
                SqlLiteral::Str(s) if pk_col.type_code == TypeCode::UUID => parse_uuid_str(s).ok()?,
                _ => return None,
            };
            if seen.insert(v) {
                pks.push(v);
            }
        }
        Some(pks)
    } else {
        None
    }
}

/// Seek every key of a [`AccessPath::PkMultiSeek`] list and concatenate the
/// replies into one batch: absent keys contribute no rows, so the batch holds
/// exactly the rows the IN list matches. `row_cap` stops seeking once that many
/// rows have accumulated (SELECT's LIMIT — every fetched row is an output row
/// on this path, so further round trips are pure waste).
pub(crate) fn seek_pk_multi(
    client: &mut GnitzClient,
    table_id: u64,
    schema: &Schema,
    pks: &[u128],
    row_cap: Option<usize>,
) -> Result<(Option<Arc<Schema>>, Option<ZSetBatch>), GnitzSqlError> {
    let stride = schema.pk_stride() as u8;
    let mut reply_schema: Option<Arc<Schema>> = None;
    let mut out: Option<ZSetBatch> = None;
    for &v in pks {
        let (schema_opt, batch_opt, _) = client.seek(table_id, &PkTuple::from_u128(stride, v))?;
        if reply_schema.is_none() {
            reply_schema = schema_opt;
        }
        if let Some(batch) = batch_opt {
            match &mut out {
                None => out = Some(batch),
                Some(acc) => acc.extend_from_owned(batch),
            }
        }
        if row_cap.is_some_and(|cap| out.as_ref().is_some_and(|b| b.pks.len() >= cap)) {
            break;
        }
    }
    Ok((reply_schema, out))
}

// ---------------------------------------------------------------------------
// Secondary-index equality seek candidates
// ---------------------------------------------------------------------------

/// One index-servable seek candidate: the index's FULL declared column list,
/// the covered leading key values, and the residual conjuncts to filter after
/// the seek.
pub(crate) type IndexSeekCandidate<'e> = (gnitz_core::PkColList, Vec<u128>, Vec<&'e Expr>);

/// Every index-servable seek candidate among the conjuncts of `expr` —
/// `(index column list, covered leading key values, residual conjuncts)` —
/// best candidate first. The caller attempts each candidate's `seek_by_index`
/// in turn and treats `ClientError::NoIndex` as "try the next candidate".
/// Matching every index against every `col = literal` conjunct lets
/// `WHERE unindexed = 1 AND indexed = 2` find the index whichever conjunct
/// carries it; flattening the whole `AND`-tree extends that to
/// three-or-more-way conjunctions. PK columns are skipped —
/// `try_extract_pk_seek_residual` handles them first — and so are column types
/// that can never carry a secondary index.
///
/// `fetch_indexes` (one epoch-validated GET_INDICES round-trip) is called only
/// when at least one eligible equality exists, so a WHERE that can never seek
/// (`x > 5`, an OR-tree, …) costs no wire traffic here.
pub(crate) fn collect_index_seek_candidates<'e>(
    expr: &'e Expr,
    schema: &Schema,
    fetch_indexes: impl FnOnce() -> Result<Arc<Vec<gnitz_core::IndexMeta>>, ClientError>,
) -> Result<Vec<IndexSeekCandidate<'e>>, ClientError> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expr, &mut conjuncts);

    let eqs = collect_eq_conjuncts(&conjuncts, schema);
    if eqs.is_empty() {
        return Ok(Vec::new());
    }

    let indexes = fetch_indexes()?;
    let mut out: Vec<IndexSeekCandidate<'e>> = Vec::new();
    for meta in indexes.iter() {
        // The index's FULL declared column list (sent verbatim for the exact
        // circuit match); `vals` covers a leading prefix of it (`<=` arity).
        let idx_cols = meta.cols.as_slice();
        let (vals, consumed) = consume_leading_eq_prefix(idx_cols, &eqs);
        if vals.is_empty() {
            continue;
        } // index does not apply

        // Leading-prefix safety: a row is omitted from the index if ANY indexed
        // column is NULL (`batch_project_index`). The covered columns are
        // non-null (they equal a non-null literal), but an uncovered *trailing*
        // column that is nullable would drop rows whose trailing value is NULL —
        // rows the prefix predicate still matches — so the seek would silently
        // lose them. Reject the prefix; a different index or (for UPDATE/DELETE)
        // the full-scan fallback serves the query correctly, and direct SELECT
        // raises its clean "non-indexed" error rather than returning wrong rows.
        if uncovered_trailing_nullable(idx_cols, vals.len(), schema) {
            continue;
        }

        out.push((meta.cols, vals, residual_conjuncts(&conjuncts, &consumed)));
    }

    // Best first: longer covered prefix wins; on a tie the tighter index (fewer
    // columns — an exact lookup over a leading-prefix scan) wins.
    out.sort_by(|a, b| {
        b.1.len()
            .cmp(&a.1.len())
            .then_with(|| a.0.as_slice().len().cmp(&b.0.as_slice().len()))
    });
    Ok(out)
}

/// Every `col = literal` equality among `conjuncts`, tagged with its conjunct
/// index so the residual can exclude exactly the consumed conjuncts. PK columns
/// and index-ineligible types can never carry a secondary index, so they are
/// never collected. Shared by the equality and range candidate collectors —
/// the eligibility rule must stay identical between them.
fn collect_eq_conjuncts(
    conjuncts: &[&Expr],
    schema: &Schema,
) -> Vec<(usize /*conjunct*/, usize /*col*/, u128 /*key*/)> {
    let mut eqs = Vec::new();
    for (ci, &cand) in conjuncts.iter().enumerate() {
        if let Some((col, key)) = try_col_eq_literal(cand, schema) {
            let tc = schema.columns[col].type_code;
            if !schema.is_pk_col(col) && tc.is_pk_eligible() {
                eqs.push((ci, col, key));
            }
        }
    }
    eqs
}

/// Consume equality conjuncts as an index's leading columns (leading-prefix
/// rule: stop at the first column with no covering equality). Returns the
/// covered key values and the consumed conjunct indices.
fn consume_leading_eq_prefix(idx_cols: &[u32], eqs: &[(usize, usize, u128)]) -> (Vec<u128>, Vec<usize>) {
    let mut vals = Vec::new();
    let mut consumed = Vec::new();
    for &col in idx_cols {
        match eqs.iter().find(|&&(_, c, _)| c as u32 == col) {
            Some(&(conj, _, key)) => {
                vals.push(key);
                consumed.push(conj);
            }
            None => break,
        }
    }
    (vals, consumed)
}

/// True when an index column past the first `covered` is nullable — the
/// leading-prefix safety rejection both collectors share (a NULL in any
/// indexed column omits the whole row from the index, so an uncovered
/// nullable trailing column would silently drop rows the predicate matches).
fn uncovered_trailing_nullable(idx_cols: &[u32], covered: usize, schema: &Schema) -> bool {
    covered < idx_cols.len()
        && idx_cols[covered..]
            .iter()
            .any(|&c| schema.columns[c as usize].is_nullable)
}

/// The conjuncts a seek/range plan did not consume, kept as post-scan filters.
fn residual_conjuncts<'e>(conjuncts: &[&'e Expr], consumed: &[usize]) -> Vec<&'e Expr> {
    conjuncts
        .iter()
        .enumerate()
        .filter(|(i, _)| !consumed.contains(i))
        .map(|(_, &e)| e)
        .collect()
}

// ---------------------------------------------------------------------------
// Ordered range scans over a secondary index
// ---------------------------------------------------------------------------

/// Which end of the candidate interval a conjunct bounds. This is the only
/// start/end distinction left in range planning — every bound becomes a
/// `Cut`, and the side only says whether that cut starts or ends the interval.
#[derive(Clone, Copy, PartialEq)]
enum RangeSide {
    Start,
    End,
}

/// One end of a range predicate on a column: which interval end it bounds and
/// the cut it induces there.
struct RangeEnd {
    side: RangeSide,
    cut: Cut,
}

/// Parse a range-end literal for column type `tc` into its cut; `mk` is the
/// constructor for an in-range literal — `Cut::After` when the cut falls
/// above the literal's whole duplicate group (`col > v` for a start cut,
/// `col <= v` for an end cut), `Cut::Before` when below (`>=`, `<`). Parses
/// as `i128` so a literal past the type's min/max SATURATES to the matching
/// `Cut::type_edges` edge instead of wrapping into a different in-range value
/// (the cff7c58-class trap one layer up) — and saturation is
/// constructor-independent: below the type minimum cuts at `Before(min)`,
/// above the maximum at `After(max)`. A bound saturating *towards* its
/// interval thereby yields a zero-width interval the engine rejects
/// byte-wise, so there is no empty-range special case anywhere. Returns
/// `None` for a type that cannot carry an ordered range bound here (UUID,
/// float, string) so the predicate stays a residual.
fn parse_range_cut(tc: TypeCode, n_str: &str, negated: bool, mk: fn(u128) -> Cut) -> Option<Cut> {
    // U128: full unsigned range — saturation is impossible (an i128 cannot
    // represent its upper half), and a literal past u128::MAX fails the parse,
    // keeping the conjunct a residual.
    if tc == TypeCode::U128 {
        if negated {
            return None;
        }
        return n_str.parse::<u128>().ok().map(mk);
    }
    let fi = FixedInt::from_type_code(tc)?;
    let (min, max) = fi.range();
    let (below, above) = Cut::type_edges(tc)?;
    let v = parse_literal_i128(n_str, negated)?;
    Some(if v < min {
        below
    } else if v > max {
        above
    } else {
        mk(fi.pack(v))
    })
}

/// Parse a `BETWEEN` endpoint expression to its cut (numeric literal only).
/// Both BETWEEN ends are inclusive: the start cuts before its boundary group
/// (`Cut::Before`), the end after its group (`Cut::After`).
fn between_cut(tc: TypeCode, expr: &Expr, mk: fn(u128) -> Cut) -> Option<Cut> {
    match extract_sql_literal(expr)? {
        SqlLiteral::Number(n, negated) => parse_range_cut(tc, n, negated, mk),
        _ => None,
    }
}

/// Recognize `col OP lit` (and the flipped `lit OP col`) for OP in
/// `>`,`>=`,`<`,`<=`, mapping it to the column index and a `RangeEnd`. Returns
/// `None` for anything else (equality, non-numeric literal, non-range-servable
/// type), leaving it for the equality extractor or the residual.
fn try_col_range_literal(expr: &Expr, schema: &Schema) -> Option<(usize, RangeEnd)> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    // Which side is the column, which is the literal, and is the operator flipped
    // (`lit OP col` ≡ `col FLIP(OP) lit`)?
    let (col_name, lit, flipped) = split_col_vs_literal(left, right)?;
    use BinaryOperator as B;
    let (side, mk): (RangeSide, fn(u128) -> Cut) = match (op, flipped) {
        (B::Gt, false) | (B::Lt, true) => (RangeSide::Start, Cut::After), // col > lit / lit < col
        (B::GtEq, false) | (B::LtEq, true) => (RangeSide::Start, Cut::Before), // col >= lit / lit <= col
        (B::Lt, false) | (B::Gt, true) => (RangeSide::End, Cut::Before),  // col < lit / lit > col
        (B::LtEq, false) | (B::GtEq, true) => (RangeSide::End, Cut::After), // col <= lit / lit >= col
        _ => return None,
    };
    let col_idx = find_unique_column(&schema.columns, col_name).ok().flatten()?;
    let tc = schema.columns[col_idx].type_code;
    let SqlLiteral::Number(n_str, negated) = lit else {
        return None;
    };
    let cut = parse_range_cut(tc, n_str, negated, mk)?;
    Some((col_idx, RangeEnd { side, cut }))
}

/// One index-servable range candidate: the index's FULL declared column list,
/// the wire descriptor (the equality-pinned leading values plus the half-open
/// cut interval on the next index column), and the residual conjuncts to
/// filter after the scan.
pub(crate) struct IndexRangeCandidate<'e> {
    pub(crate) idx_cols: gnitz_core::PkColList,
    pub(crate) desc: RangeDescriptor,
    pub(crate) residual: Vec<&'e Expr>,
}

/// One collected range end tagged with the conjunct it came from. A simple
/// `col OP lit` contributes one entry; a non-negated `BETWEEN` contributes two
/// (a lower and an upper) sharing the same conjunct index.
struct RangeEndEntry {
    conjunct: usize,
    col: usize,
    end: RangeEnd,
}

/// Recognize a NON-negated `col BETWEEN lo AND hi` (numeric literals only) as
/// the two range ends it desugars to — `col >= lo` and `col <= hi`. BETWEEN is
/// a single AST node (`flatten_conjuncts` keeps it whole). A negated BETWEEN
/// is `col < lo OR col > hi` — not a contiguous interval — and stays a
/// residual, as does any endpoint that fails to parse for the column type.
fn try_col_between(expr: &Expr, schema: &Schema) -> Option<(usize, [RangeEnd; 2])> {
    let Expr::Between {
        expr: be,
        negated: false,
        low,
        high,
    } = expr
    else {
        return None;
    };
    // Bare or qualified (`t.x`) — the single-relation convention.
    let name = single_relation_col_name(be)?;
    let col = find_unique_column(&schema.columns, name).ok().flatten()?;
    let tc = schema.columns[col].type_code;
    Some((
        col,
        [
            RangeEnd {
                side: RangeSide::Start,
                cut: between_cut(tc, low, Cut::Before)?,
            },
            RangeEnd {
                side: RangeSide::End,
                cut: between_cut(tc, high, Cut::After)?,
            },
        ],
    ))
}

/// Every index-servable range candidate among the conjuncts of `expr`. Mirrors
/// `collect_index_seek_candidates` but for an ordered range scan: collect
/// equality conjuncts (the leading prefix) and range ends (incl. desugared
/// `BETWEEN`); for each index consume the equality conjuncts as the leading `E`
/// columns, then require column `E` to be covered by ≥1 range end. The FIRST
/// start-side and FIRST end-side cut on column `E` become the interval — never
/// a compare of two packed natives to pick the tighter one (the cff7c58 trap one
/// layer up; `apply_residual_filter` trims any redundant same-side end exactly).
/// An unconstrained side widens to the column type's edge cut, and an
/// out-of-type-range literal saturates to the same edges (`parse_range_cut`) —
/// a contradictory bound yields a zero-width interval the engine rejects
/// byte-wise, so there is no empty-range special case.
pub(crate) fn collect_index_range_candidates<'e>(
    expr: &'e Expr,
    schema: &Schema,
    fetch_indexes: impl FnOnce() -> Result<Arc<Vec<gnitz_core::IndexMeta>>, ClientError>,
) -> Result<Vec<IndexRangeCandidate<'e>>, ClientError> {
    let mut conjuncts = Vec::new();
    flatten_conjuncts(expr, &mut conjuncts);

    // Equality conjuncts (for the leading prefix) and range ends, both tagged
    // with their conjunct index so the residual excludes exactly the consumed
    // conjuncts.
    let eqs = collect_eq_conjuncts(&conjuncts, schema);
    let mut ends: Vec<RangeEndEntry> = Vec::new();
    for (ci, &cand) in conjuncts.iter().enumerate() {
        if let Some((col, end)) = try_col_range_literal(cand, schema) {
            ends.push(RangeEndEntry { conjunct: ci, col, end });
        } else if let Some((col, both)) = try_col_between(cand, schema) {
            ends.extend(both.map(|end| RangeEndEntry { conjunct: ci, col, end }));
        }
    }
    // A range candidate needs at least one range end; a pure-equality WHERE is
    // handled by collect_index_seek_candidates, so this costs no wire traffic then.
    if ends.is_empty() {
        return Ok(Vec::new());
    }

    let indexes = fetch_indexes()?;
    let mut out: Vec<IndexRangeCandidate<'e>> = Vec::new();
    for meta in indexes.iter() {
        let idx_cols = meta.cols.as_slice();
        let (eq_vals, eq_consumed) = consume_leading_eq_prefix(idx_cols, &eqs);
        let n_eq = eq_vals.len();
        // The range column is the next index column after the equality prefix.
        if n_eq >= idx_cols.len() {
            continue;
        } // no column left to range over
        let range_col = idx_cols[n_eq];

        // First start-side and first end-side cut on the range column become
        // the interval. Never compare multiple same-side cuts (the cff7c58 trap
        // one layer up); `apply_residual_filter` trims any redundant same-side end.
        let start_idx = ends
            .iter()
            .position(|e| e.col as u32 == range_col && e.end.side == RangeSide::Start);
        let end_idx = ends
            .iter()
            .position(|e| e.col as u32 == range_col && e.end.side == RangeSide::End);
        if start_idx.is_none() && end_idx.is_none() {
            continue;
        } // range column not covered

        // Covered columns are the equality prefix + the range column (n_eq + 1);
        // the leading-prefix safety rejection is shared with the equality path.
        if uncovered_trailing_nullable(idx_cols, n_eq + 1, schema) {
            continue;
        }

        // An unconstrained side widens to the type-edge cut — `Before(min)` /
        // `After(max)` ARE "unbounded", since no index entry lies outside the
        // range column's type. (At least one end on this column parsed into
        // `ends`, so the type is range-servable and the edges exist.)
        let tc = schema.columns[range_col as usize].type_code;
        let Some((edge_start, edge_end)) = Cut::type_edges(tc) else {
            continue;
        };
        let start = start_idx.map_or(edge_start, |i| ends[i].end.cut);
        let end = end_idx.map_or(edge_end, |i| ends[i].end.cut);

        // Consume the range conjuncts whose ends were ALL chosen as bounds. A
        // simple `col OP lit` has one end; a BETWEEN has two sharing one
        // conjunct, and with only one chosen its un-applied half must stay a
        // residual filter (else `b > 5 AND b BETWEEN 10 AND 50` would lose
        // `b >= 10` while seeding lo from `b > 5`).
        let chosen = [start_idx, end_idx];
        let mut consumed = eq_consumed;
        for cj in chosen.iter().flatten().map(|&i| ends[i].conjunct) {
            let all_chosen = ends
                .iter()
                .enumerate()
                .filter(|(_, e)| e.conjunct == cj)
                .all(|(i, _)| chosen.contains(&Some(i)));
            if all_chosen && !consumed.contains(&cj) {
                consumed.push(cj);
            }
        }

        out.push(IndexRangeCandidate {
            idx_cols: meta.cols,
            desc: RangeDescriptor::new(&eq_vals, start, end),
            residual: residual_conjuncts(&conjuncts, &consumed),
        });
    }

    // Best first: more equality-pinned columns first, then the tighter index.
    out.sort_by(|a, b| {
        b.desc
            .eq_vals()
            .len()
            .cmp(&a.desc.eq_vals().len())
            .then_with(|| a.idx_cols.as_slice().len().cmp(&b.idx_cols.as_slice().len()))
    });
    Ok(out)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::pk_codec::extract_pk_value;
    use crate::test_support::{
        col_def, compound_schema_u64_u64, dquote_expr, neg_num_expr, num_expr, pk_schema, uuid_schema_payload,
        uuid_schema_pk, uuid_str_expr,
    };

    fn make_schema_col(tc: TypeCode) -> Schema {
        Schema {
            columns: vec![col_def("pk", TypeCode::U64, false), col_def("val", tc, true)],
            pk_cols: vec![0],
        }
    }

    /// schema: (id U64 pk, a U64, b U64) → indexable cols a=1, b=2.
    fn abc_schema() -> Schema {
        Schema {
            columns: vec![
                col_def("id", TypeCode::U64, false),
                col_def("a", TypeCode::U64, false),
                col_def("b", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        }
    }

    fn eq_expr(col: &str, rhs: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Identifier(sqlparser::ast::Ident::new(col))),
            op: BinaryOperator::Eq,
            right: Box::new(rhs),
        }
    }

    fn in_list_expr(col: &str, items: Vec<Expr>) -> Expr {
        Expr::InList {
            expr: Box::new(Expr::Identifier(sqlparser::ast::Ident::new(col))),
            list: items,
            negated: false,
        }
    }

    fn idx_metas(col_lists: &[&[u32]]) -> Arc<Vec<gnitz_core::IndexMeta>> {
        Arc::new(
            col_lists
                .iter()
                .map(|cols| gnitz_core::IndexMeta {
                    cols: gnitz_core::PkColList::from_slice(cols),
                    is_unique: false,
                })
                .collect(),
        )
    }

    fn idx_list(metas: &[(&[u32], bool)]) -> Arc<Vec<gnitz_core::IndexMeta>> {
        Arc::new(
            metas
                .iter()
                .map(|(cols, uniq)| gnitz_core::IndexMeta {
                    cols: gnitz_core::PkColList::from_slice(cols),
                    is_unique: *uniq,
                })
                .collect(),
        )
    }

    fn parse_expr_sql(src: &str) -> Expr {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        Parser::new(&GenericDialect {})
            .try_with_sql(src)
            .unwrap()
            .parse_expr()
            .unwrap()
    }

    // ------------------------------------------------------------------
    // try_col_eq_literal — UUID + negative signed integer index seek keys
    // ------------------------------------------------------------------

    #[test]
    fn test_uuid_index_seek_string_literal() {
        let schema = uuid_schema_payload();
        let expr = eq_expr("uid", uuid_str_expr("550e8400-e29b-41d4-a716-446655440000"));
        let result = try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, 0x550e8400_e29b_41d4_a716_446655440000_u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i64() {
        let schema = make_schema_col(TypeCode::I64);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = try_col_eq_literal(&expr, &schema);
        // -1i64 as u64 = u64::MAX
        assert_eq!(result, Some((1, ((-1i64) as u64) as u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i32() {
        let schema = make_schema_col(TypeCode::I32);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = try_col_eq_literal(&expr, &schema);
        // I32 -1 stored as 4 bytes zero-padded → (-1i32 as u32) as u64
        assert_eq!(result, Some((1, ((-1i32 as u32) as u64) as u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i16() {
        let schema = make_schema_col(TypeCode::I16);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, ((-1i16 as u16) as u64) as u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i8() {
        let schema = make_schema_col(TypeCode::I8);
        let expr = eq_expr("val", neg_num_expr("5"));
        let result = try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, ((-5i8 as u8) as u64) as u128)));
    }

    #[test]
    fn test_try_col_eq_literal_positive_i64_still_works() {
        let schema = make_schema_col(TypeCode::I64);
        let expr = eq_expr("val", num_expr("42"));
        let result = try_col_eq_literal(&expr, &schema);
        assert_eq!(result, Some((1, 42u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_u64_returns_none() {
        // Cannot have a negative value for an unsigned column.
        let schema = make_schema_col(TypeCode::U64);
        let expr = eq_expr("val", neg_num_expr("1"));
        let result = try_col_eq_literal(&expr, &schema);
        assert_eq!(result, None);
    }

    #[test]
    fn test_double_quoted_not_a_uuid_seek_literal() {
        // A single-quoted UUID is a valid seek key (see
        // test_uuid_index_seek_string_literal); a double-quoted token is an
        // identifier in GenericDialect, so it must NOT be treated as a literal.
        let schema = uuid_schema_payload();
        let sq = eq_expr("uid", uuid_str_expr("550e8400-e29b-41d4-a716-446655440000"));
        assert!(
            try_col_eq_literal(&sq, &schema).is_some(),
            "single-quoted UUID is a seek key"
        );

        let dq = eq_expr("uid", dquote_expr("550e8400-e29b-41d4-a716-446655440000"));
        assert_eq!(
            try_col_eq_literal(&dq, &schema),
            None,
            "double-quoted token must not be parsed as a UUID seek literal"
        );
    }

    // ------------------------------------------------------------------
    // try_extract_pk_seek_residual — compound PK + residual carry
    // ------------------------------------------------------------------

    #[test]
    fn compound_pk_try_extract_pk_seek_full_binding() {
        let schema = compound_schema_u64_u64();
        // (a = 1) AND (b = 2) → full bind
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("a", num_expr("1"))),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("b", num_expr("2"))),
        };
        let (pk, residual) = try_extract_pk_seek_residual(&expr, &schema).expect("must bind");
        assert!(residual.is_empty());
        assert_eq!(pk.stride, 16);
        let mut expect = [0u8; 16];
        expect[..8].copy_from_slice(&1u64.to_le_bytes());
        expect[8..16].copy_from_slice(&2u64.to_le_bytes());
        assert_eq!(pk.as_bytes(), &expect[..]);
    }

    #[test]
    fn compound_pk_try_extract_pk_seek_reordered_and_tree() {
        let schema = compound_schema_u64_u64();
        // (b = 2) AND (a = 1) — order swapped; tuple must still pack in
        // pk-list order, not source order.
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("b", num_expr("2"))),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("a", num_expr("1"))),
        };
        let (pk, residual) = try_extract_pk_seek_residual(&expr, &schema).expect("must bind");
        assert!(residual.is_empty());
        let mut expect = [0u8; 16];
        expect[..8].copy_from_slice(&1u64.to_le_bytes());
        expect[8..16].copy_from_slice(&2u64.to_le_bytes());
        assert_eq!(pk.as_bytes(), &expect[..]);
    }

    #[test]
    fn compound_pk_try_extract_pk_seek_partial_returns_none() {
        let schema = compound_schema_u64_u64();
        let expr = eq_expr("a", num_expr("1")); // only one of two PK cols
        assert!(try_extract_pk_seek_residual(&expr, &schema).is_none());
    }

    #[test]
    fn compound_pk_try_extract_pk_seek_incomplete_pk_with_payload_returns_none() {
        let schema = compound_schema_u64_u64();
        // (a = 1) AND (v = 9) — `a` binds, `v` is a payload conjunct that goes
        // to the residual; the PK stays incomplete (`b` unbound) → None.
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("a", num_expr("1"))),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("v", num_expr("9"))),
        };
        assert!(try_extract_pk_seek_residual(&expr, &schema).is_none());
    }

    #[test]
    fn compound_pk_try_extract_pk_seek_duplicate_binding_returns_none() {
        let schema = compound_schema_u64_u64();
        // (a = 1) AND (a = 2) — duplicate binding of column 0; the second
        // routes to the residual, so `b` stays unbound → None.
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("a", num_expr("1"))),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("a", num_expr("2"))),
        };
        assert!(try_extract_pk_seek_residual(&expr, &schema).is_none());
    }

    #[test]
    fn pk_seek_residual_keeps_non_pk_conjunct() {
        let schema = pk_schema(TypeCode::U64);
        // id = 1 AND v = 9 → PK binds fully; `v = 9` becomes the residual.
        let expr = Expr::BinaryOp {
            left: Box::new(eq_expr("id", num_expr("1"))),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("v", num_expr("9"))),
        };
        let (pk, residual) = try_extract_pk_seek_residual(&expr, &schema).expect("PK binds");
        assert_eq!(pk.as_bytes(), &1u64.to_le_bytes()[..]);
        assert_eq!(residual.len(), 1);
    }

    // ------------------------------------------------------------------
    // collect_index_seek_candidates
    // ------------------------------------------------------------------

    #[test]
    fn collect_index_seek_candidates_skips_float_col() {
        // `WHERE val = 1` on a float column emits no candidate even with an index
        // present: a float column is never index-key-eligible, so it is filtered
        // out of the equality set before any index is consulted — fetch_indexes
        // must not even be called (no GET_INDICES round-trip).
        let schema = make_schema_col(TypeCode::F64);
        let expr = eq_expr("val", num_expr("1"));
        let cands = collect_index_seek_candidates(&expr, &schema, || {
            panic!("no eligible equality — the index list must not be fetched")
        })
        .unwrap();
        assert!(cands.is_empty());
    }

    #[test]
    fn collect_index_seek_candidates_flattens_and_tree() {
        // pk(U64) + a,b,c (all U64, pk-eligible non-PK), each with its own
        // single-column index.
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("a", TypeCode::U64, true),
                col_def("b", TypeCode::U64, true),
                col_def("c", TypeCode::U64, true),
            ],
            pk_cols: vec![0],
        };
        // (a = 1 AND b = 2) AND c = 3 — left-assoc nesting.
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(eq_expr("a", num_expr("1"))),
                op: BinaryOperator::And,
                right: Box::new(eq_expr("b", num_expr("2"))),
            }),
            op: BinaryOperator::And,
            right: Box::new(eq_expr("c", num_expr("3"))),
        };
        let indexes = idx_metas(&[&[1], &[2], &[3]]);
        let cands = collect_index_seek_candidates(&expr, &schema, || Ok(indexes)).unwrap();
        // One candidate per applicable index; flattening the whole AND-tree finds
        // all three (would regress to just `c` under a two-way-only extractor).
        let mut cols: Vec<Vec<u32>> = cands.iter().map(|(c, _, _)| c.as_slice().to_vec()).collect();
        cols.sort();
        assert_eq!(cols, vec![vec![1], vec![2], vec![3]]);
        // Each candidate binds exactly its one covered value, residual is the
        // other two conjuncts.
        for (_, vals, residual) in &cands {
            assert_eq!(vals.len(), 1);
            assert_eq!(residual.len(), 2);
        }
    }

    // ------------------------------------------------------------------
    // try_extract_pk_in
    // ------------------------------------------------------------------

    #[test]
    fn compound_pk_try_extract_pk_in_returns_none() {
        let schema = compound_schema_u64_u64();
        let expr = in_list_expr("a", vec![num_expr("1"), num_expr("2")]);
        assert!(try_extract_pk_in(&expr, &schema).is_none());
    }

    #[test]
    fn try_extract_pk_in_uuid_string_list_fast_path() {
        let schema = uuid_schema_pk();
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let expected = 0x550e8400_e29b_41d4_a716_446655440000_u128;
        let expr = in_list_expr("id", vec![uuid_str_expr(uuid_str)]);
        let got = try_extract_pk_in(&expr, &schema).expect("UUID string IN-list should take fast path");
        assert_eq!(got, vec![expected]);
    }

    #[test]
    fn try_extract_pk_in_uuid_invalid_string_falls_back() {
        let schema = uuid_schema_pk();
        let expr = in_list_expr(
            "id",
            vec![
                uuid_str_expr("550e8400-e29b-41d4-a716-446655440000"),
                uuid_str_expr("not-a-uuid"),
            ],
        );
        assert!(
            try_extract_pk_in(&expr, &schema).is_none(),
            "invalid UUID in list should fall back to slow scan"
        );
    }

    #[test]
    fn try_extract_pk_in_negative_i32_list() {
        // Today's u64-only body returned None for negative items, falling
        // through to the slow full-scan path. Native-PK fast path must hit.
        let schema = pk_schema(TypeCode::I32);
        let expr = in_list_expr("id", vec![neg_num_expr("1"), neg_num_expr("2")]);
        let got = try_extract_pk_in(&expr, &schema).expect("should match fast path");
        assert_eq!(got, vec![(-1i32 as u32) as u128, (-2i32 as u32) as u128]);
    }

    // ------------------------------------------------------------------
    // Routing parity: extract_pk_value / try_col_eq_literal / try_extract_pk_in
    // must agree byte-for-byte on the packed u128 for a given (PK type, literal).
    // ------------------------------------------------------------------

    /// All three parser entry points must agree byte-for-byte on the u128
    /// produced by a given (PK type, literal) pair. Master routes SEEK via
    /// `partition_for_key(pk)`; drift between any pair sends INSERT and
    /// DELETE to different workers.
    fn check_pk_parity(pk_tc: TypeCode, literal: Expr, expected: u128) {
        let schema = pk_schema(pk_tc);

        // 1. extract_pk_value (INSERT row).
        let row = vec![literal.clone(), num_expr("0")];
        let got_insert = extract_pk_value(&row, &schema).unwrap_or_else(|e| panic!("extract_pk_value({pk_tc:?}): {e}"));
        assert_eq!(got_insert.split_wire().0, expected, "extract_pk_value");

        // 2. try_col_eq_literal (WHERE pk = literal).
        let where_expr = eq_expr("id", literal.clone());
        let got_eq = try_col_eq_literal(&where_expr, &schema).expect("try_col_eq_literal returned None");
        assert_eq!(got_eq, (0, expected), "try_col_eq_literal");

        // 3. try_extract_pk_in (WHERE pk IN (literal)).
        let in_expr = in_list_expr("id", vec![literal]);
        let got_in = try_extract_pk_in(&in_expr, &schema).expect("try_extract_pk_in returned None");
        assert_eq!(got_in, vec![expected], "try_extract_pk_in");
    }

    #[test]
    fn pk_parity_i8_neg1() {
        check_pk_parity(TypeCode::I8, neg_num_expr("1"), (-1i8 as u8) as u128);
    }

    #[test]
    fn pk_parity_i16_neg1() {
        check_pk_parity(TypeCode::I16, neg_num_expr("1"), (-1i16 as u16) as u128);
    }

    #[test]
    fn pk_parity_i32_neg1() {
        check_pk_parity(TypeCode::I32, neg_num_expr("1"), (-1i32 as u32) as u128);
    }

    #[test]
    fn pk_parity_i64_neg1() {
        check_pk_parity(TypeCode::I64, neg_num_expr("1"), ((-1i64) as u64) as u128);
    }

    #[test]
    fn pk_parity_i64_min() {
        // Regression for the prepend-`-` parse rule: `(i64::MIN as u64) as u128`.
        check_pk_parity(
            TypeCode::I64,
            neg_num_expr("9223372036854775808"),
            (i64::MIN as u64) as u128,
        );
    }

    #[test]
    fn pk_parity_u16_max() {
        check_pk_parity(TypeCode::U16, num_expr("65535"), 65535u128);
    }

    #[test]
    fn pk_parity_u32_max() {
        check_pk_parity(TypeCode::U32, num_expr("4294967295"), 4294967295u128);
    }

    // ------------------------------------------------------------------
    // Qualified single-relation references take the same fast paths
    // ------------------------------------------------------------------

    /// Every WHERE fast-path recognizer accepts a qualified (`t.col`) reference
    /// like the bare form — the crate-wide single-relation convention. The
    /// qualifier value itself is ignored (`bogus.col` binds `col`), matching the
    /// binder's established leniency (`single_relation_col_name`); this test
    /// pins that chosen policy.
    #[test]
    fn qualified_refs_take_fast_paths() {
        let schema = pk_schema(TypeCode::U64); // (id U64 pk, v)

        // `t.v = 5` and the flipped `5 = t.v` → equality fast path.
        assert_eq!(try_col_eq_literal(&parse_expr_sql("t.v = 5"), &schema), Some((1, 5)));
        assert_eq!(try_col_eq_literal(&parse_expr_sql("5 = t.v"), &schema), Some((1, 5)));
        // Wrong qualifier is ignored — binds the bare name (chosen leniency).
        assert_eq!(
            try_col_eq_literal(&parse_expr_sql("bogus.v = 5"), &schema),
            Some((1, 5))
        );

        // `t.id = 1` binds the full PK → point seek.
        let seek_expr = parse_expr_sql("t.id = 1");
        let (pk, residual) = try_extract_pk_seek_residual(&seek_expr, &schema).expect("PK binds");
        assert_eq!(pk.as_bytes(), &1u64.to_le_bytes()[..]);
        assert!(residual.is_empty());

        // `t.id IN (1, 2)` → multi-seek fast path.
        assert_eq!(
            try_extract_pk_in(&parse_expr_sql("t.id IN (1, 2)"), &schema),
            Some(vec![1, 2])
        );

        // `t.v > 5` / flipped `5 < t.v` → range end.
        let (c, e) = try_col_range_literal(&parse_expr_sql("t.v > 5"), &schema).unwrap();
        assert_eq!(c, 1);
        assert!(e.side == RangeSide::Start && e.cut == Cut::After(5));
        let (_, e) = try_col_range_literal(&parse_expr_sql("5 < t.v"), &schema).unwrap();
        assert!(e.side == RangeSide::Start && e.cut == Cut::After(5));

        // `t.v BETWEEN 1 AND 5` → both inclusive ends.
        let (c, [lo, hi]) = try_col_between(&parse_expr_sql("t.v BETWEEN 1 AND 5"), &schema).unwrap();
        assert_eq!(c, 1);
        assert!(lo.side == RangeSide::Start && lo.cut == Cut::Before(1));
        assert!(hi.side == RangeSide::End && hi.cut == Cut::After(5));
    }

    // ------------------------------------------------------------------
    // Ordered range-scan extraction
    // ------------------------------------------------------------------

    #[test]
    fn parse_range_cut_saturates() {
        use Cut::{After, Before};
        let ck = |tc, s, neg, mk: fn(u128) -> Cut| parse_range_cut(tc, s, neg, mk);
        // In-range literals: `mk` picks the cut side, the value packs to the
        // column's native LE u128 (negatives two's-complement at native width).
        assert_eq!(ck(TypeCode::I32, "5", false, Before), Some(Before(5)));
        assert_eq!(ck(TypeCode::I32, "5", false, After), Some(After(5)));
        assert_eq!(
            ck(TypeCode::I32, "5", true, Before),
            Some(Before((-5i32 as u32) as u128))
        );
        // Saturation is constructor-independent: past the type range, the cut
        // lands on the type edge whichever interval end asked for it — a bound
        // saturating towards its interval becomes a zero-width range.
        let (min, max) = ((i32::MIN as u32) as u128, i32::MAX as u128);
        assert_eq!(ck(TypeCode::I32, "3000000000", false, Before), Some(After(max)));
        assert_eq!(ck(TypeCode::I32, "3000000000", false, After), Some(After(max)));
        assert_eq!(ck(TypeCode::I32, "3000000000", true, Before), Some(Before(min)));
        assert_eq!(ck(TypeCode::I32, "3000000000", true, After), Some(Before(min)));
        assert_eq!(ck(TypeCode::U8, "300", false, Before), Some(After(255)));
        // Non-range-servable scalar types decline.
        assert_eq!(ck(TypeCode::String, "5", false, Before), None);
    }

    #[test]
    fn try_col_range_literal_orientations() {
        use Cut::{After, Before};
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("x", TypeCode::I64, false)],
            pk_cols: vec![0],
        };
        let ck = |sql: &str| try_col_range_literal(&parse_expr_sql(sql), &schema);
        // col > lit and the flipped lit < col are the same start cut, falling
        // above the literal's whole duplicate group.
        let (c, e) = ck("x > 5").unwrap();
        assert_eq!(c, 1);
        assert!(e.side == RangeSide::Start && e.cut == After(5));
        let (_, e) = ck("5 < x").unwrap();
        assert!(e.side == RangeSide::Start && e.cut == After(5));
        // col <= lit / lit >= col → the end side, cutting above the same group
        // (same cut as `x > 5`; only the side differs).
        let (_, e) = ck("x <= 5").unwrap();
        assert!(e.side == RangeSide::End && e.cut == After(5));
        let (_, e) = ck("5 >= x").unwrap();
        assert!(e.side == RangeSide::End && e.cut == After(5));
        // col >= lit / col < lit cut below the group.
        let (_, e) = ck("x >= 5").unwrap();
        assert!(e.side == RangeSide::Start && e.cut == Before(5));
        let (_, e) = ck("x < 5").unwrap();
        assert!(e.side == RangeSide::End && e.cut == Before(5));
        // Equality is not a range end.
        assert!(ck("x = 5").is_none());
    }

    #[test]
    fn range_candidate_composite_eq_prefix() {
        use Cut::Before;
        let schema = abc_schema();
        let expr = parse_expr_sql("a = 7 AND b < 50");
        let cands = collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1, 2], false)]))).unwrap();
        assert_eq!(cands.len(), 1);
        let c = &cands[0];
        assert_eq!(c.desc.eq_vals(), &[7u128]);
        // No start conjunct → the U64 edge cut; `b < 50` cuts below 50's group.
        assert_eq!(c.desc.start, Before(0));
        assert_eq!(c.desc.end, Before(50));
        assert!(c.residual.is_empty(), "both conjuncts consumed");
    }

    #[test]
    fn range_candidate_between_desugars() {
        use Cut::{After, Before};
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("x", TypeCode::I64, false)],
            pk_cols: vec![0],
        };
        // BETWEEN → inclusive lower + inclusive upper, both consumed:
        // [Before(10), After(20)) keeps both boundary groups whole.
        let expr = parse_expr_sql("x BETWEEN 10 AND 20");
        let cands = collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1], false)]))).unwrap();
        assert_eq!(cands.len(), 1);
        let c = &cands[0];
        assert_eq!((c.desc.start, c.desc.end), (Before(10), After(20)));
        assert!(c.residual.is_empty());

        // NOT BETWEEN is non-contiguous → contributes no range end → no candidate.
        let expr = parse_expr_sql("x NOT BETWEEN 10 AND 20");
        let cands = collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1], false)]))).unwrap();
        assert!(cands.is_empty());
    }

    #[test]
    fn range_candidate_redundant_same_side_keeps_first() {
        use Cut::After;
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("x", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        // First lower end becomes the start cut; the second stays a residual
        // (the planner never compares two packed natives to pick the tighter
        // one).
        for sql in ["x > 5 AND x > 10", "x > 10 AND x > 5"] {
            let expr = parse_expr_sql(sql);
            let cands = collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1], false)]))).unwrap();
            assert_eq!(cands.len(), 1, "{sql}");
            let c = &cands[0];
            // The FIRST conjunct's cut is taken; the other is residual.
            let first_val: u128 = if sql.starts_with("x > 5") { 5 } else { 10 };
            assert_eq!(c.desc.start, After(first_val), "{sql}");
            assert_eq!(c.residual.len(), 1, "{sql}: other same-side end stays residual");
        }
    }

    #[test]
    fn range_candidate_saturates_out_of_range() {
        use Cut::{After, Before};
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("x", TypeCode::I32, false)],
            pk_cols: vec![0],
        };
        let (min, max) = ((i32::MIN as u32) as u128, i32::MAX as u128);

        // Lower above the type max ⇒ a zero-width interval (start == end) the
        // engine rejects byte-wise — no planner-side empty special case.
        let expr = parse_expr_sql("x > 3000000000");
        let c = collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1], false)]))).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!((c[0].desc.start, c[0].desc.end), (After(max), After(max)));

        // Upper above the type max ⇒ saturates to the edge (and no lower
        // conjunct widens to the other edge) → full scan.
        let expr = parse_expr_sql("x < 3000000000");
        let c = collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[1], false)]))).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!((c[0].desc.start, c[0].desc.end), (Before(min), After(max)));
    }

    #[test]
    fn range_candidate_residual_non_range_conjunct() {
        use Cut::After;
        let schema = abc_schema();
        // `b` indexed, `a` not part of the index → `a = 7` stays residual.
        let expr = parse_expr_sql("b > 10 AND a = 7");
        let cands = collect_index_range_candidates(&expr, &schema, || Ok(idx_list(&[(&[2], false)]))).unwrap();
        assert_eq!(cands.len(), 1);
        let c = &cands[0];
        assert!(c.desc.eq_vals().is_empty());
        assert_eq!(c.desc.start, After(10));
        assert_eq!(c.residual.len(), 1, "`a = 7` is a residual conjunct");
    }
}
