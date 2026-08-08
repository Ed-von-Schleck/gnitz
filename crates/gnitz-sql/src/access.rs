//! The PK / secondary-index **access-path recognizers**, over **bound conjuncts**
//! ([`BoundExpr`]). One home, shared by the DML `ReadSpec`/mutate planners and the
//! view compiler's scan-bound bridge: recognition is done once on the resolved
//! bound IR, never re-matched on the AST.
//!
//! This is a genuine **AST-free leaf**: it imports strictly `ir`, `codec::pk_codec`,
//! `error`, and `gnitz-core` — no `ast_util`, `bind`, `plan`, or `dml`. Column
//! identity keys off the resolved `ColRef(idx)`; literals come back through the one
//! seam [`bound_num_literal`] and pack **byte-exactly** via `pk_codec`
//! (`pack_pk_value` / `parse_pk_literal_packed` / `parse_uuid_str`), so a bound is
//! bit-identical to the retired AST recognizer's — the packing depends only on the
//! parsed value and sign, never the literal's spelling.
//!
//! Residual conjuncts are returned as **borrows** of the caller's bound WHERE:
//! candidates are collected per index but at most one is ever used, so the caller
//! clones (or compiles) only the winner's residual.

use crate::codec::pk_codec::{pack_pk_value, parse_literal_i128, parse_pk_literal_packed, parse_uuid_str};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp, BoundExpr, UnaryOp};
use gnitz_core::{ClientError, Cut, FixedInt, IndexMeta, PkColList, PkTuple, RangeDescriptor, Schema, TypeCode};
use std::cmp::Reverse;
use std::collections::HashSet;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// The bound-literal seam
// ---------------------------------------------------------------------------

/// A bound numeric literal, sign applied. `LitInt` and `LitWide` — the two numeric
/// literal shapes binding produces — optionally under an outer `Neg`, are the only
/// cases: a negative literal rides as `UnaryOp(Neg, Lit…)`.
#[derive(Clone, Copy)]
enum NumLit<'e> {
    /// A native literal (any i64, sign applied — `-(i64::MIN)` fits i128).
    Small(i128),
    /// A wide magnitude digit string + sign. Kept as the raw string because the
    /// `i128`-vs-`u128` parse is the recognizer's call: it holds the column
    /// `TypeCode`, and a `LitWide` in the `(i128::MAX, u128::MAX]` band
    /// (`U128`/`UUID`) needs the u128 parse a signed value could not represent.
    Wide(&'e str, bool),
}

/// The one seam from a bound numeric literal to a [`NumLit`].
fn bound_num_literal(e: &BoundExpr) -> Option<NumLit<'_>> {
    match e {
        BExpr::LitInt(v) => Some(NumLit::Small(*v as i128)),
        BExpr::LitWide(s) => Some(NumLit::Wide(s, false)),
        BExpr::UnaryOp(UnaryOp::Neg, inner) => match inner.as_ref() {
            BExpr::LitInt(v) => Some(NumLit::Small(-(*v as i128))),
            BExpr::LitWide(s) => Some(NumLit::Wide(s, true)),
            _ => None,
        },
        _ => None,
    }
}

/// Pack a numeric literal as a seek/range key for column type `tc`, byte-exactly
/// via `pk_codec` (an out-of-type-range literal declines, never wraps).
fn pack_num(tc: TypeCode, lit: NumLit<'_>) -> Option<u128> {
    match lit {
        NumLit::Small(v) => pack_pk_value(tc, v),
        NumLit::Wide(s, negated) => parse_pk_literal_packed(tc, s, negated),
    }
}

/// A bound literal accepted for a seek/range key: a numeric value + sign, or a
/// string (a single-quoted UUID). The AST-free analogue of `pk_codec::SqlLiteral`.
enum BoundLit<'e> {
    Num(NumLit<'e>),
    Str(&'e str),
}

fn bound_literal(e: &BoundExpr) -> Option<BoundLit<'_>> {
    if let Some(n) = bound_num_literal(e) {
        return Some(BoundLit::Num(n));
    }
    if let BExpr::LitStr(s) = e {
        return Some(BoundLit::Str(s));
    }
    None
}

/// Classify a bound binary op's operands as `(col_idx, literal, flipped)`: the
/// resolved `ColRef` may sit on either side, `flipped` is true when the literal was
/// on the left (`lit OP col`), so a range recognizer can mirror the operator. The
/// bound analogue of the AST `split_col_vs_literal`.
fn bound_col_vs_literal<'e>(a: &'e BoundExpr, b: &'e BoundExpr) -> Option<(usize, BoundLit<'e>, bool)> {
    if let BExpr::ColRef(idx) = a {
        if let Some(l) = bound_literal(b) {
            return Some((*idx, l, false));
        }
    }
    if let BExpr::ColRef(idx) = b {
        if let Some(l) = bound_literal(a) {
            return Some((*idx, l, true));
        }
    }
    None
}

/// Flatten a bound `AND`-tree into its leaf conjuncts, left to right. Binding
/// already unwrapped `Nested`, so there is nothing else to descend. The bound
/// analogue of `ast_util::flatten_conjuncts`, kept local so this leaf imports no
/// `ast_util`.
fn flatten_bound_conjuncts<'e>(expr: &'e BoundExpr, out: &mut Vec<&'e BoundExpr>) {
    if let BExpr::BinOp(l, BinOp::And, r) = expr {
        flatten_bound_conjuncts(l, out);
        flatten_bound_conjuncts(r, out);
    } else {
        out.push(expr);
    }
}

// ---------------------------------------------------------------------------
// Front matchers
// ---------------------------------------------------------------------------

/// Extract `(col_idx, packed_key)` from a bound `col = literal`. Does NOT check
/// index existence.
fn try_col_eq_literal(expr: &BoundExpr, schema: &Schema) -> Option<(usize, u128)> {
    let BExpr::BinOp(left, BinOp::Eq, right) = expr else {
        return None;
    };
    // `=` is symmetric, so the flipped flag is irrelevant here.
    let (col_idx, lit, _) = bound_col_vs_literal(left, right)?;
    let col_tc = schema.columns[col_idx].type_code;

    // A single-quoted string is a seek key only for a UUID column; numerics pack
    // byte-exactly through pk_codec so the SEEK/INSERT parse sites cannot drift.
    let key = match lit {
        BoundLit::Str(s) if col_tc == TypeCode::UUID => parse_uuid_str(s).ok()?,
        BoundLit::Str(_) => return None,
        BoundLit::Num(n) => pack_num(col_tc, n)?,
    };
    Some((col_idx, key))
}

/// The keys of a bound `pk IN (literal, …)` conjunct on a single-column PK, deduped
/// (first occurrence wins). `None` for any other conjunct. `NOT IN` binds to
/// `UnaryOp(Not, InList)` and so never matches here; a one-key `IN` folds to `Eq` at
/// bind and is served by [`try_extract_pk_range`].
fn pk_in_keys(conjunct: &BoundExpr, schema: &Schema) -> Option<Vec<u128>> {
    // Compound PK has no IN-list fast path; fall back to a full delta scan.
    if schema.pk_count() != 1 {
        return None;
    }
    let BExpr::InList { inner, items } = conjunct else {
        return None;
    };
    let BExpr::ColRef(col_idx) = inner.as_ref() else {
        return None;
    };
    let pk_idx = schema.pk_indices()[0];
    if *col_idx != pk_idx {
        return None;
    }
    let pk_col = &schema.columns[pk_idx];
    let mut seen = HashSet::with_capacity(items.len());
    let mut pks = Vec::with_capacity(items.len());
    for item in items {
        // Optionally-negated numerics, plus single-quoted UUID strings for a UUID
        // PK — exactly the literals `try_col_eq_literal` accepts, so `IN (…)` and
        // `= …` route identically. A NULL/float/non-literal or an unparseable UUID
        // aborts to the slow scan.
        let v = match bound_literal(item)? {
            BoundLit::Num(n) => pack_num(pk_col.type_code, n)?,
            BoundLit::Str(s) if pk_col.type_code == TypeCode::UUID => parse_uuid_str(s).ok()?,
            _ => return None,
        };
        if seen.insert(v) {
            pks.push(v);
        }
    }
    Some(pks)
}

/// `Some((keys, residual))` when one conjunct of `expr` is `pk IN (literal, …)` on a
/// single-column PK: the gather keys plus the bound conjuncts to filter against the
/// gathered rows. Conjunct-level like every recognizer beside it, so
/// `pk IN (1, 2) AND v > 5` is a two-key gather rather than a full scan. `None`
/// routes the WHERE back to the seek/index/scan ladder.
pub(crate) fn try_extract_pk_in<'e>(expr: &'e BoundExpr, schema: &Schema) -> Option<(Vec<u128>, Vec<&'e BoundExpr>)> {
    let mut conjuncts = Vec::new();
    flatten_bound_conjuncts(expr, &mut conjuncts);
    let (ci, keys) = conjuncts
        .iter()
        .enumerate()
        .find_map(|(i, &c)| pk_in_keys(c, schema).map(|k| (i, k)))?;
    Some((keys, residual_conjuncts(&conjuncts, &[ci])))
}

/// The single key a **fully-pinned** point PK descriptor names: `desc`'s equality
/// values pin the leading PK columns and its cut value pins the next one, which
/// must be the last. `None` for any looser descriptor — a point covering only a
/// PK *prefix* names a key group, not a key, and admits more than one row.
///
/// Packs each value natively little-endian at the column's `pk_byte_offset` in
/// `wire_stride` width, so a mixed-width compound PK lands correctly (the offset
/// is the running sum in PK-list order). Equality keys and range cuts both pack
/// through `FixedInt::pack`, so the tuple is byte-identical whichever conjunct
/// spelling produced the point.
pub(crate) fn pk_point_tuple(desc: &RangeDescriptor, schema: &Schema) -> Option<PkTuple> {
    if !desc.is_point() || desc.eq_vals().len() + 1 != schema.pk_count() {
        return None;
    }
    let mut tuple = PkTuple::new(schema.pk_stride() as u8);
    let vals = desc
        .eq_vals()
        .iter()
        .copied()
        .chain(std::iter::once(desc.start.value()));
    for (&col_idx, val) in schema.pk_indices().iter().zip(vals) {
        let off = schema.pk_byte_offset(col_idx);
        let w = schema.columns[col_idx].type_code.wire_stride();
        tuple.buf[off..off + w].copy_from_slice(&val.to_le_bytes()[..w]);
    }
    Some(tuple)
}

/// An **exact-or-superset** PK range for `where_expr` plus the residual bound
/// conjuncts. `None` when no PK conjunct bounds the PK. The PK is treated as its
/// own index: the leading equality prefix in pk-list order, then the next PK
/// column's first start/end cuts. The PK walk is byte-exact at any width, so every
/// consumed conjunct is applied exactly and stripped from the residual — that is
/// what makes a wide (U128) PK range servable without the predicate VM.
pub(crate) fn try_extract_pk_range<'e>(
    where_expr: &'e BoundExpr,
    schema: &Schema,
) -> Option<(RangeDescriptor, Vec<&'e BoundExpr>)> {
    let pk = schema.pk_indices();
    let mut conjuncts = Vec::new();
    flatten_bound_conjuncts(where_expr, &mut conjuncts);

    let eqs = collect_eq_conjuncts(&conjuncts, schema, |col| schema.is_pk_col(col));
    let pk_cols: Vec<u32> = pk.iter().map(|&c| c as u32).collect();
    let (mut eq_vals, mut consumed) = consume_leading_eq_prefix(&pk_cols, &eqs);

    if eq_vals.len() < pk.len() {
        let ends = collect_range_ends(&conjuncts, schema);
        if let Some((start, end)) = bound_next_column(pk_cols[eq_vals.len()], &ends, schema, &mut consumed) {
            let desc = RangeDescriptor::new(&eq_vals, start, end);
            return Some((desc, residual_conjuncts(&conjuncts, &consumed)));
        }
    }
    // No range on the next column (or every PK column pinned): a full/partial
    // equality. Lower the last pinned column to a degenerate point, else nothing
    // bounds the PK.
    let last = eq_vals.pop()?;
    Some((
        RangeDescriptor::point(&eq_vals, last),
        residual_conjuncts(&conjuncts, &consumed),
    ))
}

/// True iff `desc` is a PK bound loose enough to give up for a one-row index
/// point. Requires both:
///
/// * **No PK column pinned at all.** A descriptor that pins a leading PK column
///   — an equality prefix, or the point `try_extract_pk_range` lowers a bare
///   prefix to — can share the distribution prefix and unicast to one worker,
///   which an `IndexRange` bound never does; and when the point covers the whole
///   PK it already admits one row. [`Schema`] carries no `dist_prefix_len`, so
///   "nothing pinned" is the only test available here.
/// * **An index-eligible equality exists.** Nothing a unique point could be built
///   from otherwise, and `table_indexes` always hits the wire — this keeps the
///   GET_INDICES probe off the common `WHERE pk > x` read.
pub(crate) fn pk_bound_is_preemptible(desc: &RangeDescriptor, where_expr: &BoundExpr, schema: &Schema) -> bool {
    if !desc.eq_vals().is_empty() || desc.is_point() {
        return false;
    }
    let mut conjuncts = Vec::new();
    flatten_bound_conjuncts(where_expr, &mut conjuncts);
    conjuncts
        .iter()
        .any(|c| try_col_eq_literal(c, schema).is_some_and(|(col, _)| index_eligible_col(schema, col)))
}

// ---------------------------------------------------------------------------
// Shared collectors
// ---------------------------------------------------------------------------

/// Every `col = literal` equality among `conjuncts` whose column `eligible`
/// accepts, tagged with its conjunct index so the residual can exclude exactly the
/// consumed conjuncts. The index collectors pass [`index_eligible_col`]; the
/// PK-range extractor passes `is_pk_col`.
fn collect_eq_conjuncts(
    conjuncts: &[&BoundExpr],
    schema: &Schema,
    eligible: impl Fn(usize) -> bool,
) -> Vec<(usize /*conjunct*/, usize /*col*/, u128 /*key*/)> {
    let mut eqs = Vec::new();
    for (ci, &cand) in conjuncts.iter().enumerate() {
        if let Some((col, key)) = try_col_eq_literal(cand, schema) {
            if eligible(col) {
                eqs.push((ci, col, key));
            }
        }
    }
    eqs
}

/// The secondary-index eligibility both index collectors share: PK columns and
/// index-ineligible types can never carry a secondary index.
fn index_eligible_col(schema: &Schema, col: usize) -> bool {
    !schema.is_pk_col(col) && schema.columns[col].type_code.is_pk_eligible()
}

/// Consume equality conjuncts as an index's leading columns (leading-prefix rule:
/// stop at the first column with no covering equality). Returns the covered key
/// values and the consumed conjunct indices.
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
/// leading-prefix safety rejection both collectors share (a NULL in any indexed
/// column omits the whole row from the index, so an uncovered nullable trailing
/// column would silently drop rows the predicate matches).
fn uncovered_trailing_nullable(idx_cols: &[u32], covered: usize, schema: &Schema) -> bool {
    covered < idx_cols.len()
        && idx_cols[covered..]
            .iter()
            .any(|&c| schema.columns[c as usize].is_nullable)
}

/// The conjuncts a seek/range plan did not consume, kept as post-scan filters.
/// Borrows: at most one candidate wins, so only the winner's residual is ever
/// cloned or compiled by the caller.
fn residual_conjuncts<'e>(conjuncts: &[&'e BoundExpr], consumed: &[usize]) -> Vec<&'e BoundExpr> {
    conjuncts
        .iter()
        .enumerate()
        .filter(|(i, _)| !consumed.contains(i))
        .map(|(_, &e)| e)
        .collect()
}

// ---------------------------------------------------------------------------
// Ordered range scans
// ---------------------------------------------------------------------------

/// Which end of the candidate interval a conjunct bounds.
#[derive(Clone, Copy, PartialEq)]
enum RangeSide {
    Start,
    End,
}

/// One end of a range predicate on a column: which interval end it bounds and the
/// cut it induces there.
struct RangeEnd {
    side: RangeSide,
    cut: Cut,
}

/// One collected range end tagged with the conjunct it came from.
struct RangeEndEntry {
    conjunct: usize,
    col: usize,
    end: RangeEnd,
}

/// Turn a range-end literal for column type `tc` into its cut; `mk` is the
/// constructor for an in-range literal — `Cut::After` when the cut falls above the
/// literal's whole duplicate group, `Cut::Before` when below. Values run as `i128`
/// so a literal past the type's min/max SATURATES to the matching
/// `Cut::type_edges` edge instead of wrapping. Returns `None` for a type that
/// cannot carry an ordered range bound here (UUID, float, string), leaving the
/// predicate a residual.
fn parse_range_cut(tc: TypeCode, lit: NumLit<'_>, mk: fn(u128) -> Cut) -> Option<Cut> {
    // U128: full unsigned range — saturation is impossible (an i128 cannot
    // represent its upper half), and a literal past u128::MAX fails the parse,
    // keeping the conjunct a residual.
    if tc == TypeCode::U128 {
        return match lit {
            NumLit::Small(v) => (v >= 0).then(|| mk(v as u128)),
            NumLit::Wide(_, true) => None,
            NumLit::Wide(s, false) => s.parse::<u128>().ok().map(mk),
        };
    }
    let fi = FixedInt::from_type_code(tc)?;
    let (min, max) = fi.range();
    let (below, above) = Cut::type_edges(tc)?;
    let v = match lit {
        NumLit::Small(v) => v,
        NumLit::Wide(s, negated) => parse_literal_i128(s, negated)?,
    };
    Some(if v < min {
        below
    } else if v > max {
        above
    } else {
        mk(fi.pack(v))
    })
}

/// Recognize a bound `col OP lit` (and the flipped `lit OP col`) for OP in
/// `>`,`>=`,`<`,`<=`, mapping it to the column index and a `RangeEnd`. Returns
/// `None` for anything else (equality, non-numeric literal, non-range-servable
/// type). A `BETWEEN` never reaches here: binding desugars it into `>= AND <=`,
/// which flatten to two ordinary range-end conjuncts.
fn try_col_range_literal(expr: &BoundExpr, schema: &Schema) -> Option<(usize, RangeEnd)> {
    let BExpr::BinOp(left, op, right) = expr else {
        return None;
    };
    let (col_idx, lit, flipped) = bound_col_vs_literal(left, right)?;
    // Orient to `col OP lit` first, so the four arms below read as the operator
    // they name rather than as eight (operator, side) pairs.
    let op = if flipped { op.converse() } else { *op };
    let (side, mk): (RangeSide, fn(u128) -> Cut) = match op {
        BinOp::Gt => (RangeSide::Start, Cut::After),
        BinOp::Ge => (RangeSide::Start, Cut::Before),
        BinOp::Lt => (RangeSide::End, Cut::Before),
        BinOp::Le => (RangeSide::End, Cut::After),
        _ => return None,
    };
    let tc = schema.columns[col_idx].type_code;
    let BoundLit::Num(n) = lit else {
        return None;
    };
    let cut = parse_range_cut(tc, n, mk)?;
    Some((col_idx, RangeEnd { side, cut }))
}

/// Every range end among `conjuncts` (`col OP lit` and flipped forms), tagged with
/// its conjunct index. Shared by the index range collector and the PK-range
/// extractor. BETWEEN desugars to two `>=`/`<=` conjuncts at bind, each a separate
/// entry here.
fn collect_range_ends(conjuncts: &[&BoundExpr], schema: &Schema) -> Vec<RangeEndEntry> {
    let mut ends: Vec<RangeEndEntry> = Vec::new();
    for (ci, &cand) in conjuncts.iter().enumerate() {
        if let Some((col, end)) = try_col_range_literal(cand, schema) {
            ends.push(RangeEndEntry { conjunct: ci, col, end });
        }
    }
    ends
}

/// Bound `range_col` by the FIRST start-side and FIRST end-side cut among `ends`
/// (never a compare of two packed natives to pick the tighter one — the residual
/// re-imposes any redundant same-side end exactly), widening an unconstrained side
/// to the column type's edge cut. Pushes each chosen end's conjunct onto `consumed`
/// — `collect_range_ends` emits at most one end per conjunct, so choosing an end
/// consumes its conjunct outright. `None` when no end covers `range_col`, or its
/// type carries no ordered range.
fn bound_next_column(
    range_col: u32,
    ends: &[RangeEndEntry],
    schema: &Schema,
    consumed: &mut Vec<usize>,
) -> Option<(Cut, Cut)> {
    let start_idx = ends
        .iter()
        .position(|e| e.col as u32 == range_col && e.end.side == RangeSide::Start);
    let end_idx = ends
        .iter()
        .position(|e| e.col as u32 == range_col && e.end.side == RangeSide::End);
    if start_idx.is_none() && end_idx.is_none() {
        return None;
    }
    let tc = schema.columns[range_col as usize].type_code;
    let (edge_start, edge_end) = Cut::type_edges(tc)?;
    let start = start_idx.map_or(edge_start, |i| ends[i].end.cut);
    let end = end_idx.map_or(edge_end, |i| ends[i].end.cut);

    for cj in [start_idx, end_idx].iter().flatten().map(|&i| ends[i].conjunct) {
        if !consumed.contains(&cj) {
            consumed.push(cj);
        }
    }
    Some((start, end))
}

// ---------------------------------------------------------------------------
// Secondary-index seek + range candidates
// ---------------------------------------------------------------------------

/// One index-servable seek candidate: the index's FULL declared column list, the
/// covered leading key values, and the residual bound conjuncts to filter after
/// the seek.
struct IndexSeekCandidate<'e> {
    cols: PkColList,
    vals: Vec<u128>,
    residual: Vec<&'e BoundExpr>,
    /// Whether the index this candidate seeks is UNIQUE.
    is_unique: bool,
}

impl IndexSeekCandidate<'_> {
    /// A seek covering **every** column of a UNIQUE index: at most one row.
    /// A point on a *prefix* of a unique index is not itself unique, and `cols`
    /// is the index's FULL declared list, so this is exactly "fully covered".
    fn is_unique_point(&self) -> bool {
        self.is_unique && self.vals.len() == self.cols.as_slice().len()
    }
}

/// Every index-servable seek candidate among the conjuncts of `expr`, best first.
/// `fetch_indexes` (one epoch-validated GET_INDICES round-trip) is called only when
/// at least one eligible equality exists.
fn collect_index_seek_candidates<'e>(
    expr: &'e BoundExpr,
    schema: &Schema,
    fetch_indexes: impl FnOnce() -> Result<Arc<Vec<IndexMeta>>, ClientError>,
) -> Result<Vec<IndexSeekCandidate<'e>>, ClientError> {
    let mut conjuncts = Vec::new();
    flatten_bound_conjuncts(expr, &mut conjuncts);

    let eqs = collect_eq_conjuncts(&conjuncts, schema, |col| index_eligible_col(schema, col));
    if eqs.is_empty() {
        return Ok(Vec::new());
    }

    let indexes = fetch_indexes()?;
    let mut out: Vec<IndexSeekCandidate<'e>> = Vec::new();
    for meta in indexes.iter() {
        let idx_cols = meta.cols.as_slice();
        let (vals, consumed) = consume_leading_eq_prefix(idx_cols, &eqs);
        if vals.is_empty() {
            continue;
        }
        if uncovered_trailing_nullable(idx_cols, vals.len(), schema) {
            continue;
        }
        out.push(IndexSeekCandidate {
            cols: meta.cols,
            vals,
            residual: residual_conjuncts(&conjuncts, &consumed),
            is_unique: meta.is_unique,
        });
    }

    // Best first: a seek covering every column of a UNIQUE index selects at most
    // one row and outranks any longer prefix of a non-unique index; then longer
    // covered prefix; then the tighter index.
    out.sort_by(|a, b| {
        b.is_unique_point()
            .cmp(&a.is_unique_point())
            .then_with(|| b.vals.len().cmp(&a.vals.len()))
            .then_with(|| a.cols.as_slice().len().cmp(&b.cols.as_slice().len()))
    });
    Ok(out)
}

/// One index-servable range candidate: the index's FULL declared column list, the
/// wire descriptor, and the residual bound conjuncts to filter after the scan.
pub(crate) struct IndexRangeCandidate<'e> {
    pub(crate) idx_cols: PkColList,
    pub(crate) desc: RangeDescriptor,
    pub(crate) residual: Vec<&'e BoundExpr>,
    /// Whether the index this candidate walks is UNIQUE.
    is_unique: bool,
}

impl IndexRangeCandidate<'_> {
    /// The range column: the index column immediately after the equality prefix —
    /// the one layout invariant of the descriptor, kept here so consumers never
    /// re-derive it from the candidate's internals.
    pub(crate) fn range_col(&self) -> usize {
        self.idx_cols.as_slice()[self.desc.eq_vals().len()] as usize
    }

    /// A point covering **every** column of a UNIQUE index — the one fact
    /// comparable against a PK candidate, since such a point admits at most one
    /// row whatever the walk costs. A point on a *prefix* of a unique index is not
    /// itself unique; `idx_cols` is the index's FULL declared list, so
    /// `n_eq + 1 == len` is exactly "the point covers every column".
    pub(crate) fn is_unique_point(&self) -> bool {
        self.is_unique && self.desc.eq_vals().len() + 1 == self.idx_cols.as_slice().len() && self.desc.is_point()
    }
}

/// Every index-servable range candidate among the conjuncts of `expr`. Collect
/// equality conjuncts (the leading prefix) and range ends; for each index consume
/// the equalities as the leading `E` columns, then require column `E` to be covered
/// by ≥1 range end. The FIRST start/end cut on column `E` become the interval.
fn collect_index_range_candidates<'e>(
    expr: &'e BoundExpr,
    schema: &Schema,
    fetch_indexes: impl FnOnce() -> Result<Arc<Vec<IndexMeta>>, ClientError>,
) -> Result<Vec<IndexRangeCandidate<'e>>, ClientError> {
    let mut conjuncts = Vec::new();
    flatten_bound_conjuncts(expr, &mut conjuncts);

    let eqs = collect_eq_conjuncts(&conjuncts, schema, |col| index_eligible_col(schema, col));
    let ends = collect_range_ends(&conjuncts, schema);
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
        }
        let range_col = idx_cols[n_eq];

        // Covered columns are the equality prefix + the range column (n_eq + 1).
        if uncovered_trailing_nullable(idx_cols, n_eq + 1, schema) {
            continue;
        }

        let mut consumed = eq_consumed;
        let Some((start, end)) = bound_next_column(range_col, &ends, schema, &mut consumed) else {
            continue; // range column not covered
        };

        out.push(IndexRangeCandidate {
            idx_cols: meta.cols,
            desc: RangeDescriptor::new(&eq_vals, start, end),
            residual: residual_conjuncts(&conjuncts, &consumed),
            is_unique: meta.is_unique,
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

/// Memoizes one `table_indexes` list across the range → equality collector
/// fall-through, so one bound extraction fetches at most once regardless of how
/// the caller's `fetch` is backed. It does not assume `table_indexes` hits the
/// wire — inside a statement snapshot that call is already served from the
/// client's epoch-validated cache — the point is that this function's own
/// "called at most once" contract holds locally, without depending on a caching
/// detail of a lower layer.
#[derive(Default)]
struct IndexListMemo(Option<Arc<Vec<IndexMeta>>>);

impl IndexListMemo {
    fn get(
        &mut self,
        fetch: impl FnOnce() -> Result<Arc<Vec<IndexMeta>>, ClientError>,
    ) -> Result<Arc<Vec<IndexMeta>>, ClientError> {
        match &self.0 {
            Some(list) => Ok(Arc::clone(list)),
            None => {
                let list = fetch()?;
                self.0 = Some(Arc::clone(&list));
                Ok(list)
            }
        }
    }
}

/// The best index range/equality bound for `where_expr` as a full
/// [`IndexRangeCandidate`] — descriptor plus the chosen candidate's residual (the
/// WHERE conjuncts the bound does not apply). `fetch` (the GET_INDICES probe) is
/// injected and called **at most once** (both collectors are lazy and share one
/// [`IndexListMemo`]).
///
/// Every candidate unifies as a range: a pure n-column equality lowers to a
/// degenerate point range over its LAST column, since `RangeDescriptor` cannot
/// express "no range column". The merged candidates are ranked by [`pinned_score`]
/// — most-constrained first — and the head taken outright.
pub(crate) fn best_index_bound<'e, F>(
    where_expr: &'e BoundExpr,
    schema: &Schema,
    mut fetch: F,
) -> Result<Option<IndexRangeCandidate<'e>>, GnitzSqlError>
where
    F: FnMut() -> Result<Arc<Vec<IndexMeta>>, ClientError>,
{
    let mut memo = IndexListMemo::default();
    let ranges =
        collect_index_range_candidates(where_expr, schema, || memo.get(&mut fetch)).map_err(GnitzSqlError::Exec)?;
    let seeks =
        collect_index_seek_candidates(where_expr, schema, || memo.get(&mut fetch)).map_err(GnitzSqlError::Exec)?;

    let cands = ranges.into_iter().chain(seeks.into_iter().map(|c| {
        let n = c.vals.len();
        IndexRangeCandidate {
            idx_cols: c.cols,
            desc: RangeDescriptor::point(&c.vals[..n - 1], c.vals[n - 1]),
            residual: c.residual,
            is_unique: c.is_unique,
        }
    }));
    // `min_by_key` keeps the FIRST of equal keys, so on a full tie the range
    // collector's candidate (listed first) wins, matching each collector's own
    // internal preference order. It also scores each candidate exactly once,
    // where a sort would re-derive the key per comparison.
    Ok(cands.min_by_key(|c| Reverse(pinned_score(c, schema))))
}

/// How constrained a candidate leaves the index walk, as a totally ordered key:
/// a full unique point first, then 2 per equality-pinned column plus 1 per real
/// range side, then (ascending arity) the tighter index on a tie. One rule ranks
/// equality and range candidates together — "most pinned columns wins".
///
/// A point covering every column of a UNIQUE index selects at most one row, which
/// no magnitude score can express: on a signed column a two-sided interval ties a
/// point at 2, and on an unsigned column `WHERE a = 0` scores 1 — *below* an
/// interval — because `type_edges(U64).0` is `Before(0)`. Leading with the flag
/// settles both.
fn pinned_score(c: &IndexRangeCandidate<'_>, schema: &Schema) -> (bool, u32, Reverse<usize>) {
    let sides = match Cut::type_edges(schema.columns[c.range_col()].type_code) {
        Some((lo, hi)) => (c.desc.start != lo) as u32 + (c.desc.end != hi) as u32,
        // Unreachable: both collectors only emit range-servable column types.
        None => 2,
    };
    (
        c.is_unique_point(),
        2 * c.desc.eq_vals().len() as u32 + sides,
        Reverse(c.idx_cols.as_slice().len()),
    )
}

// ---------------------------------------------------------------------------
// Unit tests — over bound conjuncts (parse + bind), asserted values unchanged.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bind::bind_single_table;
    use crate::codec::pk_codec::extract_pk_value;
    use crate::test_support::{
        bind_where, col_def, compound_schema_u64_u64, eq_expr, idx_metas, idx_metas_flagged, in_list_expr,
        neg_num_expr, num_expr, parse_expr_sql, pk_schema, two_col, uuid_schema_payload, uuid_schema_pk,
    };
    use sqlparser::ast::Expr;

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

    // ------------------------------------------------------------------
    // try_col_eq_literal — UUID + negative signed integer index seek keys
    // ------------------------------------------------------------------

    #[test]
    fn test_uuid_index_seek_string_literal() {
        let schema = uuid_schema_payload();
        let expr = bind_where("uid = '550e8400-e29b-41d4-a716-446655440000'", &schema);
        assert_eq!(
            try_col_eq_literal(&expr, &schema),
            Some((1, 0x550e8400_e29b_41d4_a716_446655440000_u128))
        );
    }

    #[test]
    fn test_try_col_eq_literal_negative_i64() {
        let schema = two_col(TypeCode::I64);
        let expr = bind_where("val = -1", &schema);
        // -1i64 as u64 = u64::MAX
        assert_eq!(try_col_eq_literal(&expr, &schema), Some((1, ((-1i64) as u64) as u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_i32() {
        let schema = two_col(TypeCode::I32);
        let expr = bind_where("val = -1", &schema);
        assert_eq!(
            try_col_eq_literal(&expr, &schema),
            Some((1, ((-1i32 as u32) as u64) as u128))
        );
    }

    #[test]
    fn test_try_col_eq_literal_negative_i16() {
        let schema = two_col(TypeCode::I16);
        let expr = bind_where("val = -1", &schema);
        assert_eq!(
            try_col_eq_literal(&expr, &schema),
            Some((1, ((-1i16 as u16) as u64) as u128))
        );
    }

    #[test]
    fn test_try_col_eq_literal_negative_i8() {
        let schema = two_col(TypeCode::I8);
        let expr = bind_where("val = -5", &schema);
        assert_eq!(
            try_col_eq_literal(&expr, &schema),
            Some((1, ((-5i8 as u8) as u64) as u128))
        );
    }

    #[test]
    fn test_try_col_eq_literal_positive_i64_still_works() {
        let schema = two_col(TypeCode::I64);
        let expr = bind_where("val = 42", &schema);
        assert_eq!(try_col_eq_literal(&expr, &schema), Some((1, 42u128)));
    }

    #[test]
    fn test_try_col_eq_literal_negative_u64_returns_none() {
        // Cannot have a negative value for an unsigned column.
        let schema = two_col(TypeCode::U64);
        let expr = bind_where("val = -1", &schema);
        assert_eq!(try_col_eq_literal(&expr, &schema), None);
    }

    #[test]
    fn test_try_col_eq_literal_wide_u64_max() {
        // u64::MAX overflows i64 → binds to `LitWide`; the recognizer parses it
        // byte-exactly for a U64 column (the servable wide PK/index seek).
        let schema = two_col(TypeCode::U64);
        let expr = bind_where("val = 18446744073709551615", &schema);
        assert_eq!(try_col_eq_literal(&expr, &schema), Some((1, u64::MAX as u128)));
    }

    /// A single-quoted UUID binds as a servable seek literal; a double-quoted token
    /// is an `Identifier` in `GenericDialect`, so binding it as a column reference
    /// fails (no such column) — it is never a seek literal. The double-quote
    /// guarantee moved from the AST recognizer to the parser + binder.
    #[test]
    fn double_quoted_uuid_binds_as_column_ref_not_seek() {
        let schema = uuid_schema_payload();
        let sq = bind_single_table(&parse_expr_sql("uid = '550e8400-e29b-41d4-a716-446655440000'"), &schema).unwrap();
        assert!(
            try_col_eq_literal(&sq, &schema).is_some(),
            "single-quoted UUID is a seek key"
        );

        let err = bind_single_table(
            &parse_expr_sql("uid = \"550e8400-e29b-41d4-a716-446655440000\""),
            &schema,
        )
        .expect_err("double-quoted token must not bind as a literal");
        assert!(matches!(err, GnitzSqlError::Bind(_)), "got {err:?}");
    }

    // ------------------------------------------------------------------
    // pk_point_tuple — the exact key a fully-pinned PK point names
    // ------------------------------------------------------------------

    /// The key a `WHERE` resolves to through the PK-range recognizer, plus that
    /// recognizer's residual. `None` when the descriptor pins fewer than every PK
    /// column (it then names a key group, not a key).
    fn pk_point_of<'e>(expr: &'e BoundExpr, schema: &Schema) -> Option<(PkTuple, Vec<&'e BoundExpr>)> {
        let (desc, residual) = try_extract_pk_range(expr, schema)?;
        pk_point_tuple(&desc, schema).map(|t| (t, residual))
    }

    /// Which WHEREs over a compound PK name a single key, and which name only a
    /// key *group* — a group must never restrict a caller's buffered rows to one
    /// PK. The named key packs in pk-list order however the conjuncts were
    /// spelled, and whatever does not bind the PK rides the residual.
    #[test]
    fn a_compound_pk_point_names_a_key_only_when_every_column_binds() {
        let schema = compound_schema_u64_u64();
        let mut key_1_2 = [0u8; 16];
        key_1_2[..8].copy_from_slice(&1u64.to_le_bytes());
        key_1_2[8..16].copy_from_slice(&2u64.to_le_bytes());

        for (sql, want_residual) in [
            ("a = 1 AND b = 2", 0),
            // Order swapped; the tuple must still pack in pk-list order.
            ("b = 2 AND a = 1", 0),
            ("a = 1 AND b = 2 AND v = 9", 1),
        ] {
            let expr = bind_where(sql, &schema);
            let (pk, residual) = pk_point_of(&expr, &schema).unwrap_or_else(|| panic!("{sql}: must bind"));
            assert_eq!(pk.stride, 16, "{sql}");
            assert_eq!(pk.as_bytes(), &key_1_2[..], "{sql}");
            assert_eq!(residual.len(), want_residual, "{sql}: residual");
        }

        for sql in [
            // A bare prefix: the worker still walks the `a = 1` group.
            "a = 1",
            // `a` binds, `v` is a payload conjunct → residual; the PK stays incomplete.
            "a = 1 AND v = 9",
            // The duplicate routes to the residual, so `b` stays unbound.
            "a = 1 AND a = 2",
        ] {
            let expr = bind_where(sql, &schema);
            assert!(try_extract_pk_range(&expr, &schema).is_some(), "{sql}: still bounds");
            assert!(pk_point_of(&expr, &schema).is_none(), "{sql}: names no single key");
        }
    }

    /// A single-column PK: the equality and the degenerate two-sided range reach
    /// the same `(Before(v), After(v))` cuts through `bound_next_column`, so they
    /// name the same key byte for byte and both consume every conjunct. An open
    /// range names no key.
    #[test]
    fn a_single_pk_point_is_the_same_key_however_it_is_spelled() {
        let schema = pk_schema(TypeCode::U64);
        for sql in ["id = 5", "id >= 5 AND id <= 5"] {
            let expr = bind_where(sql, &schema);
            let (pk, residual) = pk_point_of(&expr, &schema).unwrap_or_else(|| panic!("{sql}: must bind"));
            assert_eq!(pk.as_bytes(), &5u64.to_le_bytes()[..], "{sql}");
            assert!(residual.is_empty(), "{sql}: every conjunct is consumed by the walk");
        }
        let with_payload = bind_where("id = 1 AND v = 9", &schema);
        let (pk, residual) = pk_point_of(&with_payload, &schema).expect("PK binds");
        assert_eq!(pk.as_bytes(), &1u64.to_le_bytes()[..]);
        assert_eq!(residual.len(), 1, "the non-PK conjunct rides the residual");
        assert!(pk_point_of(&bind_where("id > 5", &schema), &schema).is_none());
    }

    // ------------------------------------------------------------------
    // collect_index_seek_candidates
    // ------------------------------------------------------------------

    #[test]
    fn collect_index_seek_candidates_skips_float_col() {
        // `WHERE val = 1` on a float column emits no candidate: a float column is
        // never index-key-eligible, so fetch_indexes must not even be called.
        let schema = two_col(TypeCode::F64);
        let expr = bind_where("val = 1", &schema);
        let cands = collect_index_seek_candidates(&expr, &schema, || {
            panic!("no eligible equality — the index list must not be fetched")
        })
        .unwrap();
        assert!(cands.is_empty());
    }

    #[test]
    fn collect_index_seek_candidates_flattens_and_tree() {
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("a", TypeCode::U64, true),
                col_def("b", TypeCode::U64, true),
                col_def("c", TypeCode::U64, true),
            ],
            pk_cols: vec![0],
        };
        // (a = 1 AND b = 2) AND c = 3 — flattening the whole AND-tree finds all three.
        let expr = bind_where("a = 1 AND b = 2 AND c = 3", &schema);
        let indexes = idx_metas(&[&[1], &[2], &[3]]);
        let cands = collect_index_seek_candidates(&expr, &schema, || Ok(indexes)).unwrap();
        let mut cols: Vec<Vec<u32>> = cands.iter().map(|c| c.cols.as_slice().to_vec()).collect();
        cols.sort();
        assert_eq!(cols, vec![vec![1], vec![2], vec![3]]);
        for c in &cands {
            assert_eq!(c.vals.len(), 1);
            assert_eq!(c.residual.len(), 2);
        }
    }

    /// `(id U64 pk, x U64, a U64, b U64, c U64)` — all payload cols non-nullable so
    /// no candidate is dropped by `uncovered_trailing_nullable`.
    fn seek_rank_schema() -> Schema {
        Schema {
            columns: vec![
                col_def("id", TypeCode::U64, false),
                col_def("x", TypeCode::U64, false),
                col_def("a", TypeCode::U64, false),
                col_def("b", TypeCode::U64, false),
                col_def("c", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        }
    }

    /// The columns of the head (best) seek candidate for `where_sql`.
    fn head_seek_cols(where_sql: &str, sch: &Schema, lists: &[(&[u32], bool)]) -> Vec<u32> {
        let expr = bind_where(where_sql, sch);
        let cands = collect_index_seek_candidates(&expr, sch, || Ok(idx_metas_flagged(lists))).unwrap();
        cands
            .first()
            .expect("some index must be seekable")
            .cols
            .as_slice()
            .to_vec()
    }

    /// A point covering every column of UNIQUE(x) selects at most one row, so it
    /// outranks the longer — but non-unique, and disjoint — prefix of INDEX(a, b).
    #[test]
    fn a_covered_unique_index_outranks_a_longer_disjoint_prefix() {
        let cols = head_seek_cols(
            "x = 1 AND a = 2 AND b = 3",
            &seek_rank_schema(),
            &[(&[1], true), (&[2, 3], false)],
        );
        assert_eq!(cols, vec![1], "UNIQUE(x) admits one row; INDEX(a, b) admits many");
    }

    /// With no candidate fully covered the new leading term is false for both, so
    /// the pre-existing rule decides: equal prefix lengths, then the tighter index.
    #[test]
    fn a_partial_cover_of_a_unique_index_does_not_jump_the_queue() {
        let cols = head_seek_cols("a = 1", &seek_rank_schema(), &[(&[2, 3], true), (&[2, 3, 4], false)]);
        assert_eq!(cols, vec![2, 3], "a prefix point on UNIQUE(a, b) is not itself unique");
    }

    // ------------------------------------------------------------------
    // try_extract_pk_in
    // ------------------------------------------------------------------

    /// The gather keys of `sql`, dropping the residual (which borrows the bound
    /// expression and so cannot outlive this call).
    fn pk_in_keys_of(sql: &str, schema: &Schema) -> Option<Vec<u128>> {
        try_extract_pk_in(&bind_where(sql, schema), schema).map(|(keys, _)| keys)
    }

    /// The gather needs a genuine multi-key list on a single-column PK: a compound
    /// PK declines, and a one-key list is an `Eq` by the time it gets here.
    #[test]
    fn try_extract_pk_in_declines_compound_pk_and_a_folded_one_key_list() {
        assert!(pk_in_keys_of("a IN (1, 2)", &compound_schema_u64_u64()).is_none());
        assert!(pk_in_keys_of("id IN (7)", &pk_schema(TypeCode::U64)).is_none());
    }

    #[test]
    fn try_extract_pk_in_uuid_string_list() {
        assert_eq!(
            pk_in_keys_of(
                "id IN ('550e8400-e29b-41d4-a716-446655440000', '6ba7b810-9dad-11d1-80b4-00c04fd430c8')",
                &uuid_schema_pk()
            ),
            Some(vec![
                0x550e8400_e29b_41d4_a716_446655440000,
                0x6ba7b810_9dad_11d1_80b4_00c04fd430c8
            ])
        );
    }

    #[test]
    fn try_extract_pk_in_uuid_invalid_string_falls_back() {
        assert!(
            pk_in_keys_of(
                "id IN ('550e8400-e29b-41d4-a716-446655440000', 'not-a-uuid')",
                &uuid_schema_pk()
            )
            .is_none(),
            "invalid UUID in list should fall back to slow scan"
        );
    }

    #[test]
    fn try_extract_pk_in_negative_i32_list() {
        assert_eq!(
            pk_in_keys_of("id IN (-1, -2)", &pk_schema(TypeCode::I32)),
            Some(vec![(-1i32 as u32) as u128, (-2i32 as u32) as u128])
        );
    }

    /// Conjunct-level like every recognizer beside it: the list still bounds the
    /// gather when it sits in an AND-tree, and the companion conjunct is returned as
    /// the residual rather than sinking the whole WHERE to a scan.
    #[test]
    fn pk_in_inside_an_and_tree_gathers_with_a_residual() {
        let schema = pk_schema(TypeCode::U64);
        let expr = bind_where("v > 5 AND id IN (7, 9)", &schema);
        let (keys, residual) = try_extract_pk_in(&expr, &schema).expect("the IN conjunct bounds the gather");
        assert_eq!(keys, vec![7, 9]);
        assert_eq!(residual.len(), 1, "`v > 5` stays residual");
    }

    // ------------------------------------------------------------------
    // A one-element IN list is the equality it spells, so the `=` recognizers
    // serve it. `bind::structural` pins the fold itself.
    // ------------------------------------------------------------------

    /// On `PRIMARY KEY (a, b)`, `a IN (7) AND b = 3` bounds the whole key. A genuine
    /// multi-key list leaves the leading column unbound, which pins that the fold —
    /// not some other path — is doing the work.
    #[test]
    fn one_key_in_list_bounds_the_pk_range() {
        let schema = compound_schema_u64_u64();
        let expr = bind_where("a IN (7) AND b = 3", &schema);
        let (desc, residual) = try_extract_pk_range(&expr, &schema).expect("one-key IN bounds");
        assert_eq!(desc, RangeDescriptor::point(&[7], 3));
        assert!(residual.is_empty());
        assert!(
            try_extract_pk_range(&bind_where("a IN (7, 8) AND b = 3", &schema), &schema).is_none(),
            "a real multi-key list leaves the leading PK column unbound"
        );
    }

    /// On a U128 column the index bound must consume the conjunct outright — nothing
    /// may reach the expression VM, which has no 16-byte slot for the OR-chain a
    /// list lowers to.
    #[test]
    fn one_key_in_list_takes_an_index_bound() {
        let schema = two_col(TypeCode::U128);
        let expr = bind_where("val IN (7)", &schema);
        let c = best_index_bound(&expr, &schema, || Ok(idx_metas(&[&[1]])))
            .unwrap()
            .expect("one-key IN must take an index bound");
        assert_eq!(c.desc, RangeDescriptor::point(&[], 7));
        assert!(c.residual.is_empty(), "the bound consumes the conjunct");
    }

    // ------------------------------------------------------------------
    // Routing parity: extract_pk_value (INSERT, AST) / try_col_eq_literal /
    // try_extract_pk_in (bound) must agree byte-for-byte on the packed u128.
    // ------------------------------------------------------------------

    fn check_pk_parity(pk_tc: TypeCode, literal: Expr, expected: u128) {
        let schema = pk_schema(pk_tc);

        // 1. extract_pk_value (INSERT row) — pk_codec, AST-based.
        let row = vec![literal.clone(), num_expr("0")];
        let got_insert = extract_pk_value(&row, &schema).unwrap_or_else(|e| panic!("extract_pk_value({pk_tc:?}): {e}"));
        assert_eq!(got_insert.split_wire().0, expected, "extract_pk_value");

        // 2. try_col_eq_literal (bound WHERE pk = literal).
        let eq = bind_single_table(&eq_expr("id", literal.clone()), &schema).expect("bind eq");
        assert_eq!(
            try_col_eq_literal(&eq, &schema),
            Some((0, expected)),
            "try_col_eq_literal"
        );

        // 3. try_extract_pk_in — the repeat keeps it an `InList` (a one-item list
        // folds to the `Eq` leg 2 already covers); the dedup collapses it to one key.
        let in_e = bind_single_table(&in_list_expr("id", vec![literal.clone(), literal]), &schema).expect("bind in");
        assert_eq!(
            try_extract_pk_in(&in_e, &schema).map(|(keys, _)| keys),
            Some(vec![expected]),
            "try_extract_pk_in"
        );
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
        // Regression for the prepend-`-` parse rule: the `i64::MIN` magnitude
        // overflows i64 → `LitWide`, and the recognizer parses it byte-exactly.
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

    #[test]
    fn pk_parity_u64_max_wide() {
        // u64::MAX binds to `LitWide` for the WHERE seeks; INSERT parses it directly.
        check_pk_parity(TypeCode::U64, num_expr("18446744073709551615"), u64::MAX as u128);
    }

    // ------------------------------------------------------------------
    // Qualified single-relation references take the same fast paths
    // ------------------------------------------------------------------

    #[test]
    fn qualified_refs_take_fast_paths() {
        let schema = pk_schema(TypeCode::U64); // (id U64 pk, v)

        // `t.v = 5` and the flipped `5 = t.v` → equality fast path (qualifier is
        // stripped at bind — the single-relation leniency).
        assert_eq!(
            try_col_eq_literal(&bind_where("t.v = 5", &schema), &schema),
            Some((1, 5))
        );
        assert_eq!(
            try_col_eq_literal(&bind_where("5 = t.v", &schema), &schema),
            Some((1, 5))
        );

        // `t.id = 1` binds the full PK → point seek.
        let where_expr = bind_where("t.id = 1", &schema);
        let (pk, residual) = pk_point_of(&where_expr, &schema).expect("PK binds");
        assert_eq!(pk.as_bytes(), &1u64.to_le_bytes()[..]);
        assert!(residual.is_empty());

        // `t.id IN (1, 2)` → multi-seek fast path.
        assert_eq!(pk_in_keys_of("t.id IN (1, 2)", &schema), Some(vec![1, 2]));

        // `t.v > 5` / flipped `5 < t.v` → range end.
        let (c, e) = try_col_range_literal(&bind_where("t.v > 5", &schema), &schema).unwrap();
        assert_eq!(c, 1);
        assert!(e.side == RangeSide::Start && e.cut == Cut::After(5));
        let (_, e) = try_col_range_literal(&bind_where("5 < t.v", &schema), &schema).unwrap();
        assert!(e.side == RangeSide::Start && e.cut == Cut::After(5));
    }

    // ------------------------------------------------------------------
    // Ordered range-scan extraction
    // ------------------------------------------------------------------

    #[test]
    fn parse_range_cut_saturates() {
        use Cut::{After, Before};
        // The wide (digit-string) arm exercises the same value classification the
        // native arm takes, so one literal shape covers both.
        let ck = |tc, s, neg, mk: fn(u128) -> Cut| parse_range_cut(tc, NumLit::Wide(s, neg), mk);
        assert_eq!(ck(TypeCode::I32, "5", false, Before), Some(Before(5)));
        assert_eq!(ck(TypeCode::I32, "5", false, After), Some(After(5)));
        assert_eq!(
            ck(TypeCode::I32, "5", true, Before),
            Some(Before((-5i32 as u32) as u128))
        );
        let (min, max) = ((i32::MIN as u32) as u128, i32::MAX as u128);
        assert_eq!(ck(TypeCode::I32, "3000000000", false, Before), Some(After(max)));
        assert_eq!(ck(TypeCode::I32, "3000000000", false, After), Some(After(max)));
        assert_eq!(ck(TypeCode::I32, "3000000000", true, Before), Some(Before(min)));
        assert_eq!(ck(TypeCode::I32, "3000000000", true, After), Some(Before(min)));
        assert_eq!(ck(TypeCode::U8, "300", false, Before), Some(After(255)));
        assert_eq!(ck(TypeCode::String, "5", false, Before), None);
    }

    #[test]
    fn try_col_range_literal_orientations() {
        use Cut::{After, Before};
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("x", TypeCode::I64, false)],
            pk_cols: vec![0],
        };
        let ck = |sql: &str| try_col_range_literal(&bind_where(sql, &schema), &schema);
        let (c, e) = ck("x > 5").unwrap();
        assert_eq!(c, 1);
        assert!(e.side == RangeSide::Start && e.cut == After(5));
        let (_, e) = ck("5 < x").unwrap();
        assert!(e.side == RangeSide::Start && e.cut == After(5));
        let (_, e) = ck("x <= 5").unwrap();
        assert!(e.side == RangeSide::End && e.cut == After(5));
        let (_, e) = ck("5 >= x").unwrap();
        assert!(e.side == RangeSide::End && e.cut == After(5));
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
        let expr = bind_where("a = 7 AND b < 50", &schema);
        let cands = collect_index_range_candidates(&expr, &schema, || Ok(idx_metas(&[&[1, 2]]))).unwrap();
        assert_eq!(cands.len(), 1);
        let c = &cands[0];
        assert_eq!(c.desc.eq_vals(), &[7u128]);
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
        // BETWEEN desugars at bind to `x >= 10 AND x <= 20` → two consumed range ends.
        let expr = bind_where("x BETWEEN 10 AND 20", &schema);
        let cands = collect_index_range_candidates(&expr, &schema, || Ok(idx_metas(&[&[1]]))).unwrap();
        assert_eq!(cands.len(), 1);
        let c = &cands[0];
        assert_eq!((c.desc.start, c.desc.end), (Before(10), After(20)));
        assert!(c.residual.is_empty());

        // NOT BETWEEN binds to `Not(x >= 10 AND x <= 20)` — one leaf conjunct, no
        // range end → no candidate.
        let expr = bind_where("x NOT BETWEEN 10 AND 20", &schema);
        let cands = collect_index_range_candidates(&expr, &schema, || Ok(idx_metas(&[&[1]]))).unwrap();
        assert!(cands.is_empty());
    }

    #[test]
    fn range_candidate_redundant_same_side_keeps_first() {
        use Cut::After;
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("x", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        for sql in ["x > 5 AND x > 10", "x > 10 AND x > 5"] {
            let expr = bind_where(sql, &schema);
            let cands = collect_index_range_candidates(&expr, &schema, || Ok(idx_metas(&[&[1]]))).unwrap();
            assert_eq!(cands.len(), 1, "{sql}");
            let c = &cands[0];
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

        let expr = bind_where("x > 3000000000", &schema);
        let c = collect_index_range_candidates(&expr, &schema, || Ok(idx_metas(&[&[1]]))).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!((c[0].desc.start, c[0].desc.end), (After(max), After(max)));

        let expr = bind_where("x < 3000000000", &schema);
        let c = collect_index_range_candidates(&expr, &schema, || Ok(idx_metas(&[&[1]]))).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!((c[0].desc.start, c[0].desc.end), (Before(min), After(max)));
    }

    #[test]
    fn range_candidate_residual_non_range_conjunct() {
        use Cut::After;
        let schema = abc_schema();
        // `b` indexed, `a` not part of the index → `a = 7` stays residual.
        let expr = bind_where("b > 10 AND a = 7", &schema);
        let cands = collect_index_range_candidates(&expr, &schema, || Ok(idx_metas(&[&[2]]))).unwrap();
        assert_eq!(cands.len(), 1);
        let c = &cands[0];
        assert!(c.desc.eq_vals().is_empty());
        assert_eq!(c.desc.start, After(10));
        assert_eq!(c.residual.len(), 1, "`a = 7` is a residual conjunct");
    }

    // ── best_index_bound: the whole-WHERE arbitration ────────────────────────────
    //
    // These pin the *chosen* bound, not just the per-shape collectors above: which
    // candidate wins across shapes and across separate indexes, and how many index
    // round-trips the choice costs (a probe is wire traffic, so "zero" is contract).

    /// `(id U64 pk, a U64, b U64 [nullable per arg])` — indexable cols a=1, b=2.
    fn bound_schema(b_nullable: bool) -> Schema {
        Schema {
            columns: vec![
                col_def("id", TypeCode::U64, false),
                col_def("a", TypeCode::U64, false),
                col_def("b", TypeCode::U64, b_nullable),
            ],
            pk_cols: vec![0],
        }
    }

    /// The bound `best_index_bound` picks for `where_sql`, plus the index-list fetch
    /// count.
    #[allow(clippy::type_complexity)]
    fn bound_of(
        where_sql: &str,
        sch: &Schema,
        lists: &[&[u32]],
    ) -> (Option<(gnitz_wire::PkColList, gnitz_wire::RangeDescriptor)>, u32) {
        let calls = std::cell::Cell::new(0);
        let bound = bind_single_table(&parse_expr_sql(where_sql), sch).unwrap();
        let c = best_index_bound(&bound, sch, || {
            calls.set(calls.get() + 1);
            Ok(idx_metas(lists))
        })
        .unwrap();
        (c.map(|c| (c.idx_cols, c.desc)), calls.get())
    }

    /// A pure equality on a 1-column index lowers to a degenerate point range.
    #[test]
    fn equality_lowers_to_a_degenerate_point_range() {
        let (b, calls) = bound_of("a = 5", &bound_schema(false), &[&[1]]);
        let (idx_cols, desc) = b.expect("a = 5 on an index over `a` must bound");
        assert_eq!(idx_cols.as_slice(), &[1]);
        assert_eq!(desc.eq_vals(), &[] as &[u128]);
        assert_eq!((desc.start, desc.end), (Cut::Before(5), Cut::After(5)));
        assert_eq!(calls, 1, "one round-trip serves both collectors");
    }

    /// A two-column equality pins the leading column and points at the last.
    #[test]
    fn compound_equality_pins_the_leading_column() {
        let (b, _) = bound_of("a = 5 AND b = 7", &bound_schema(false), &[&[1, 2]]);
        let (idx_cols, desc) = b.expect("a compound equality must bound the compound index");
        assert_eq!(idx_cols.as_slice(), &[1, 2]);
        assert_eq!(desc.eq_vals(), &[5u128]);
        assert_eq!((desc.start, desc.end), (Cut::Before(7), Cut::After(7)));
    }

    /// An equality prefix plus a range on ONE index takes the range candidate.
    #[test]
    fn equality_prefix_plus_range_takes_the_range_candidate() {
        let (b, _) = bound_of("a = 5 AND b > 10", &bound_schema(false), &[&[1, 2]]);
        let (_, desc) = b.expect("an eq-prefix + range must bound");
        assert_eq!(desc.eq_vals(), &[5u128], "`a` is the pinned prefix");
        assert_eq!(desc.start, Cut::After(10), "`b > 10` is an exclusive lower cut");
        assert_ne!(desc.end, Cut::After(10), "the upper side stays open, not a point");
    }

    /// Across SEPARATE indexes, most-pinned wins.
    #[test]
    fn point_on_one_index_beats_half_open_range_on_another() {
        let (b, _) = bound_of("a = 5 AND b > 10", &bound_schema(false), &[&[1], &[2]]);
        let (idx_cols, desc) = b.expect("the point candidate must bound");
        assert_eq!(idx_cols.as_slice(), &[1], "INDEX(a)'s point beats INDEX(b)'s range");
        assert_eq!((desc.start, desc.end), (Cut::Before(5), Cut::After(5)));
    }

    /// A PK predicate never bounds: the collectors skip PK columns unconditionally.
    #[test]
    fn pk_equality_never_bounds() {
        assert!(bound_of("id = 5", &bound_schema(false), &[&[1]]).0.is_none());
    }

    /// An uncovered NULLABLE trailing index column must NOT bound.
    #[test]
    fn uncovered_nullable_trailing_column_never_bounds() {
        assert!(bound_of("a = 5", &bound_schema(true), &[&[1, 2]]).0.is_none());
        assert!(bound_of("a = 5", &bound_schema(false), &[&[1, 2]]).0.is_some());
    }

    /// A WHERE no index covers costs ZERO round-trips on the range path and at most
    /// one overall — the collectors are lazy by contract.
    #[test]
    fn unindexed_column_bounds_nothing() {
        let (b, calls) = bound_of("a = 5", &bound_schema(false), &[&[2]]);
        assert!(b.is_none());
        assert_eq!(calls, 1, "the eq collector probes once, then finds no match");
        let (b, calls) = bound_of("a + b > 3", &bound_schema(false), &[&[1]]);
        assert!(b.is_none());
        assert_eq!(calls, 0, "a non-servable WHERE must cost no wire traffic");
    }

    /// A BETWEEN is a two-sided range over one column (desugared at bind).
    #[test]
    fn between_bounds_both_sides() {
        let (b, _) = bound_of("a BETWEEN 5 AND 9", &bound_schema(false), &[&[1]]);
        let (_, desc) = b.expect("BETWEEN must bound");
        assert_eq!(desc.eq_vals(), &[] as &[u128]);
        assert_eq!((desc.start, desc.end), (Cut::Before(5), Cut::After(9)));
    }

    // ── best_index_bound: a full unique point outranks every interval ────────────
    //
    // Intervals here sit strictly inside the column's type range: an interval whose
    // own cut lands on a type edge scores 1 and the comparison proves nothing.

    /// `(id U64 pk, a T, b T)` — indexable cols a=1, b=2, both non-nullable.
    fn typed_schema(tc: TypeCode) -> Schema {
        Schema {
            columns: vec![
                col_def("id", TypeCode::U64, false),
                col_def("a", tc, false),
                col_def("b", tc, false),
            ],
            pk_cols: vec![0],
        }
    }

    /// The index the arbitration picks for `where_sql`, given per-index uniqueness.
    fn picked(where_sql: &str, sch: &Schema, lists: &[(&[u32], bool)]) -> (Vec<u32>, Cut, Cut) {
        let bound = bind_single_table(&parse_expr_sql(where_sql), sch).unwrap();
        let c = best_index_bound(&bound, sch, || Ok(idx_metas_flagged(lists)))
            .unwrap()
            .expect("the WHERE must bound some index");
        (c.idx_cols.as_slice().to_vec(), c.desc.start, c.desc.end)
    }

    /// A full point on a UNIQUE index beats a two-sided interval on another index,
    /// on a signed column (where both score 2 and the stable sort favoured the
    /// range) and on an unsigned one (where the point's `Before(0)` start scores 1).
    #[test]
    fn full_unique_point_beats_an_interval() {
        for (tc, pt) in [(TypeCode::U64, "5"), (TypeCode::I64, "5")] {
            let sch = typed_schema(tc);
            let where_sql = format!("a = {pt} AND b BETWEEN 1 AND 9");
            let (cols, start, end) = picked(&where_sql, &sch, &[(&[1], true), (&[2], false)]);
            assert_eq!(cols, vec![1], "{tc:?}: the unique point on `a` must win");
            assert_eq!((start, end), (Cut::Before(5), Cut::After(5)));
        }
    }

    /// The same at both type edges of both signednesses — the point's cuts coincide
    /// with `type_edges` there, so its magnitude score is at its worst.
    #[test]
    fn full_unique_point_wins_at_the_type_edges() {
        for (tc, lit) in [
            (TypeCode::U64, "0"),
            (TypeCode::U64, "18446744073709551615"),
            (TypeCode::I64, "-9223372036854775808"),
            (TypeCode::I64, "9223372036854775807"),
        ] {
            let sch = typed_schema(tc);
            let where_sql = format!("a = {lit} AND b BETWEEN 1 AND 9");
            let (cols, ..) = picked(&where_sql, &sch, &[(&[1], true), (&[2], false)]);
            assert_eq!(cols, vec![1], "{tc:?} `a = {lit}` must win");
        }
    }

    /// Without the UNIQUE flag the magnitude score decides as before: the interval
    /// on `b` takes the signed tie.
    #[test]
    fn a_non_unique_point_still_loses_the_tie() {
        let sch = typed_schema(TypeCode::I64);
        let (cols, ..) = picked("a = 5 AND b BETWEEN 1 AND 9", &sch, &[(&[1], false), (&[2], false)]);
        assert_eq!(cols, vec![2], "two candidates at score 2: the range collector's wins");
    }

    /// A point on a PREFIX of a UNIQUE index is not a unique point: it leaves the
    /// index's trailing column free, so the interval keeps the tie.
    #[test]
    fn a_partial_prefix_of_a_unique_index_is_not_a_unique_point() {
        // `(id U64 pk, a I64, b I64, c I64)` — `b` non-nullable, or the partial
        // candidate is rejected outright by `uncovered_trailing_nullable`.
        let sch = Schema {
            columns: vec![
                col_def("id", TypeCode::U64, false),
                col_def("a", TypeCode::I64, false),
                col_def("b", TypeCode::I64, false),
                col_def("c", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        };
        let (cols, ..) = picked("a = 5 AND c BETWEEN 1 AND 9", &sch, &[(&[1, 2], true), (&[3], false)]);
        assert_eq!(
            cols,
            vec![3],
            "a prefix point on UNIQUE(a, b) selects more than one row"
        );
    }

    /// The uniqueness lookup is not seek-only: a degenerate point the RANGE
    /// collector produced (`a >= 5 AND a <= 5`) is scored unique too.
    #[test]
    fn a_range_collectors_point_is_scored_unique() {
        let sch = typed_schema(TypeCode::I64);
        let (cols, start, end) = picked(
            "a >= 5 AND a <= 5 AND b BETWEEN 1 AND 9",
            &sch,
            &[(&[1], true), (&[2], false)],
        );
        assert_eq!(cols, vec![1]);
        assert_eq!((start, end), (Cut::Before(5), Cut::After(5)));
    }

    // ── when a PK bound yields to a one-row index point ─────────────────────────

    /// The winner of the arbitration is a point covering a whole UNIQUE index.
    #[test]
    fn a_full_unique_point_is_recognized() {
        let sch = bound_schema(false);
        let bound = bind_where("a = 42", &sch);
        let c = best_index_bound(&bound, &sch, || Ok(idx_metas_flagged(&[(&[1], true)])))
            .unwrap()
            .expect("the WHERE must bound some index");
        assert!(c.is_unique_point());
    }

    /// Whether the PK bound `where_sql` extracts would yield to a one-row index
    /// point. Panics when the WHERE bounds no PK range at all.
    fn preemptible(where_sql: &str, sch: &Schema) -> bool {
        let bound = bind_where(where_sql, sch);
        let (desc, _) = try_extract_pk_range(&bound, sch).expect("the WHERE must bound the PK");
        pk_bound_is_preemptible(&desc, &bound, sch)
    }

    #[test]
    fn an_unpinned_pk_range_is_preemptible() {
        assert!(preemptible("pk > 0 AND val = 42", &two_col(TypeCode::U64)));
    }

    /// A pinned leading PK column can share the distribution prefix and unicast to
    /// one worker (`PRIMARY KEY (tenant, id) CLUSTER BY (tenant)`), which an
    /// `IndexRange` bound never does. False regardless of `dist_prefix_len`, which
    /// the client cannot see.
    #[test]
    fn a_pinned_prefix_is_not_preemptible() {
        let sch = Schema {
            columns: vec![
                col_def("tenant", TypeCode::U64, false),
                col_def("id", TypeCode::U64, false),
                col_def("email", TypeCode::U64, false),
            ],
            pk_cols: vec![0, 1],
        };
        assert!(!preemptible("tenant = 7 AND id > 0 AND email = 42", &sch));
        // The bare prefix lowers to a point over `tenant` with nothing pinned
        // before it — still one worker's rows, not one row.
        assert!(!preemptible("tenant = 7 AND email = 42", &sch));
    }

    #[test]
    fn a_pk_point_is_not_preemptible() {
        assert!(!preemptible("pk = 0 AND val = 42", &two_col(TypeCode::U64)));
    }

    /// No index-eligible equality: nothing a unique point could be built from, so
    /// the GET_INDICES probe stays off the common bounded-read path.
    #[test]
    fn no_index_eligible_equality_is_not_preemptible() {
        assert!(!preemptible("pk > 5 AND val > 1", &two_col(TypeCode::U64)));
    }
}
