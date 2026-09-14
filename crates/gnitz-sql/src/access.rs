//! The PK / secondary-index **access-path recognizers**, over **bound conjuncts**
//! ([`BoundExpr`]). One home, shared by the DML `ReadSpec`/mutate planners and the
//! view compiler's scan-bound bridge: recognition is done once on the resolved
//! bound IR, never re-matched on the AST.
//!
//! An **AST-free leaf** — `ir`, `codec::pk_codec` and `gnitz-core`, nothing else.
//! Columns key off the resolved `ColRef(idx)`; literals pack byte-exactly through
//! `pk_codec`'s bound-literal seam, so a key depends on the parsed value and sign
//! and never on the literal's spelling.
//!
//! [`bound_column_list`] recognizes every access path: the PK and each secondary
//! index are one ordered key list under one leading-equality-prefix rule.
//!
//! Residuals are **borrows** of the caller's bound WHERE — at most one candidate
//! is ever used, so only the winner's residual is cloned or compiled.

use crate::codec::pk_codec::{bound_key_literal, col_key_literal, pack_num, BoundLit};
use crate::ir::NumLit;
use crate::ir::{BExpr, BinOp, BoundExpr};
use gnitz_core::{
    opk_key_cols, opk_key_packed, Cut, FixedInt, IndexMeta, PkBuf, PkColList, RangeDescriptor, Schema, TypeCode,
};
use gnitz_wire::PkKeys;
use std::cmp::Reverse;

/// Classify a bound binary op as `col OP literal`, the `ColRef` on either side.
/// The returned operator always reads `col OP lit` — a literal on the left is
/// mirrored through [`BinOp::converse`] here — so no caller handles sides. The
/// literal is read as a key of that column (`col_key_literal`), so a DECIMAL
/// column's literal arrives already at the column's scale.
fn bound_col_vs_literal<'e>(expr: &'e BoundExpr, schema: &Schema) -> Option<(usize, BoundLit<'e>, BinOp)> {
    let BExpr::BinOp(left, op, right) = expr else {
        return None;
    };
    if let BExpr::ColRef(idx) = left.as_ref() {
        if let Some(l) = col_key_literal(right, &schema.columns[*idx]) {
            return Some((*idx, l, *op));
        }
    }
    if let BExpr::ColRef(idx) = right.as_ref() {
        if let Some(l) = col_key_literal(left, &schema.columns[*idx]) {
            return Some((*idx, l, op.converse()));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Front matchers
// ---------------------------------------------------------------------------

/// Extract `(col_idx, packed_key)` from a bound `col = literal`. Does NOT check
/// index existence.
fn try_col_eq_literal(expr: &BoundExpr, schema: &Schema) -> Option<(usize, u128)> {
    let (col_idx, lit, op) = bound_col_vs_literal(expr, schema)?;
    if !matches!(op, BinOp::Eq) {
        return None;
    }
    let key = bound_key_literal(lit, schema.columns[col_idx].type_code).ok()?;
    Some((col_idx, key))
}

/// The native keys of a bound `pk IN (literal, …)` conjunct on a single-column PK.
fn pk_in_keys(conjunct: &BoundExpr, schema: &Schema) -> Option<Vec<u128>> {
    // Compound PK has no IN-list fast path; the WHERE routes back to the
    // PK-range rung, then the index rung, then an unbounded scan.
    if schema.pk_count() != 1 {
        return None;
    }
    let BExpr::InList { inner, items } = conjunct else {
        return None;
    };
    let BExpr::ColRef(col_idx) = inner.as_ref() else {
        return None;
    };
    let pk_idx = schema.pk_cols[0] as usize;
    if *col_idx != pk_idx {
        return None;
    }
    let pk_col = &schema.columns[pk_idx];
    items
        .iter()
        .map(|item| bound_key_literal(col_key_literal(item, pk_col)?, pk_col.type_code).ok())
        .collect()
}

/// A `pk IN (literal, …)` conjunct's keys, plus the other conjuncts as residual:
/// `pk IN (1, 2) AND v > 5` is a two-key gather.
pub(crate) fn try_extract_pk_in<'e>(
    conjuncts: &'e [BoundExpr],
    schema: &Schema,
) -> Option<(PkKeys, Vec<&'e BoundExpr>)> {
    let (ci, natives) = conjuncts
        .iter()
        .enumerate()
        .find_map(|(i, c)| pk_in_keys(c, schema).map(|k| (i, k)))?;
    let stride = schema.pk_stride();
    let mut flat = Vec::with_capacity(natives.len() * stride);
    for &v in &natives {
        flat.extend_from_slice(opk_key_packed(schema, v).pk_bytes());
    }
    let keys = PkKeys::from_keys(stride, flat.chunks_exact(stride));
    Some((keys, residual_conjuncts(conjuncts, &[ci])))
}

/// The single key a **fully-pinned** point PK descriptor names: `desc`'s equality
/// values pin the leading PK columns and its cut value pins the next one, which
/// must be the last. `None` for any looser descriptor — a point covering only a
/// PK *prefix* names a key group, not a key, and admits more than one row.
///
/// Equality keys and range cuts both pack through `FixedInt::pack`, so the tuple
/// is byte-identical whichever conjunct spelling produced the point.
pub(crate) fn pk_point_tuple(desc: &RangeDescriptor, schema: &Schema) -> Option<PkBuf> {
    if !desc.pins_all(schema.pk_count()) {
        return None;
    }
    let vals = desc
        .eq_vals()
        .iter()
        .copied()
        .chain(std::iter::once(desc.start.value()));
    Some(opk_key_cols(schema, vals))
}

/// An **exact-or-superset** PK range for `conjuncts` plus the residual ones —
/// the PK as one more key list through [`bound_column_list`]. The walk is
/// byte-exact at any width, so a consumed conjunct is applied exactly and
/// stripped, which is what serves a wide (U128) PK range without the predicate VM.
pub(crate) fn try_extract_pk_range<'e>(
    conjuncts: &'e [BoundExpr],
    schema: &Schema,
) -> Option<(RangeDescriptor, Vec<&'e BoundExpr>)> {
    let eqs = collect_eq_conjuncts(conjuncts, schema);
    let ends = collect_range_ends(conjuncts, schema);

    let (desc, consumed) = bound_column_list(&schema.pk_cols, &eqs, &ends, schema)?;
    Some((desc, residual_conjuncts(conjuncts, &consumed)))
}

// ---------------------------------------------------------------------------
// Shared collectors
// ---------------------------------------------------------------------------

/// One collected equality tagged with the conjunct it came from — the equality
/// twin of [`RangeEndEntry`].
struct EqConjunct {
    conjunct: usize,
    col: usize,
    key: u128,
}

/// Every `col = literal` equality among `conjuncts`, tagged with its conjunct
/// index so the residual can exclude exactly the consumed ones. Unscreened by
/// column: [`consume_leading_eq_prefix`] matches against the key list itself, and
/// a PK column carrying a secondary index is matched there like any other.
fn collect_eq_conjuncts(conjuncts: &[BoundExpr], schema: &Schema) -> Vec<EqConjunct> {
    conjuncts
        .iter()
        .enumerate()
        .filter_map(|(ci, cand)| {
            try_col_eq_literal(cand, schema).map(|(col, key)| EqConjunct { conjunct: ci, col, key })
        })
        .collect()
}

/// Consume equality conjuncts as a key list's leading columns (leading-prefix
/// rule: stop at the first column with no covering equality). Returns the covered
/// key values and the consumed conjunct indices.
fn consume_leading_eq_prefix(cols: &[u32], eqs: &[EqConjunct]) -> (Vec<u128>, Vec<usize>) {
    let mut vals = Vec::new();
    let mut consumed = Vec::new();
    for &col in cols {
        match eqs.iter().find(|e| e.col as u32 == col) {
            Some(e) => {
                vals.push(e.key);
                consumed.push(e.conjunct);
            }
            None => break,
        }
    }
    (vals, consumed)
}

/// True when a key-list column past the first `covered` is nullable — the
/// leading-prefix safety rejection (a NULL in any indexed column omits the whole
/// row from the index, so an uncovered nullable trailing column would silently
/// drop rows the predicate matches).
fn uncovered_trailing_nullable(cols: &[u32], covered: usize, schema: &Schema) -> bool {
    covered < cols.len() && cols[covered..].iter().any(|&c| schema.columns[c as usize].is_nullable)
}

/// The conjuncts a seek/range plan did not consume, kept as post-scan filters.
/// Borrows: at most one candidate wins, so only the winner's residual is ever
/// cloned or compiled by the caller.
fn residual_conjuncts<'e>(conjuncts: &'e [BoundExpr], consumed: &[usize]) -> Vec<&'e BoundExpr> {
    conjuncts
        .iter()
        .enumerate()
        .filter(|(i, _)| !consumed.contains(i))
        .map(|(_, e)| e)
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

/// A range-end literal → its cut for a column of type `tc`; `mk` builds the cut of
/// an in-range value (`Cut::After` above the literal's duplicate group,
/// `Cut::Before` below). A literal past the type's min/max SATURATES to that edge
/// rather than wrapping; a type carrying no ordered range gives `None`.
fn parse_range_cut(tc: TypeCode, lit: NumLit, mk: fn(u128) -> Cut) -> Option<Cut> {
    // U128: full unsigned range — saturation is impossible (an i128 cannot
    // represent its upper half), and a literal past u128::MAX binds as no
    // integer literal at all, keeping the conjunct a residual. `pack_num` is exactly that classification,
    // and declines the negative literal a U128 column cannot hold.
    if tc == TypeCode::U128 {
        return pack_num(tc, lit).map(mk);
    }
    let fi = FixedInt::from_type_code(tc)?;
    let (min, max) = fi.range();
    let (below, above) = Cut::type_edges(tc)?;
    let v = lit.to_i128()?;
    Some(if v < min {
        below
    } else if v > max {
        above
    } else {
        mk(fi.pack(v))
    })
}

/// A bound `col OP lit` for OP in `>`,`>=`,`<`,`<=` → its column and `RangeEnd`;
/// `None` for anything else. A `BETWEEN` never reaches here — binding desugars it
/// into `>= AND <=`, two ordinary range-end conjuncts.
fn try_col_range_literal(expr: &BoundExpr, schema: &Schema) -> Option<(usize, RangeEnd)> {
    let (col_idx, lit, op) = bound_col_vs_literal(expr, schema)?;
    let (side, mk): (RangeSide, fn(u128) -> Cut) = match op {
        BinOp::Gt => (RangeSide::Start, Cut::After),
        BinOp::Ge => (RangeSide::Start, Cut::Before),
        BinOp::Lt => (RangeSide::End, Cut::Before),
        BinOp::Le => (RangeSide::End, Cut::After),
        _ => return None,
    };
    let BoundLit::Num(n) = lit else {
        return None;
    };
    let cut = parse_range_cut(schema.columns[col_idx].type_code, n, mk)?;
    Some((col_idx, RangeEnd { side, cut }))
}

/// Every range end among `conjuncts` (`col OP lit` and flipped forms), tagged with
/// its conjunct index. BETWEEN desugars to two `>=`/`<=` conjuncts at bind, each a
/// separate entry here.
fn collect_range_ends(conjuncts: &[BoundExpr], schema: &Schema) -> Vec<RangeEndEntry> {
    let mut ends: Vec<RangeEndEntry> = Vec::new();
    for (ci, cand) in conjuncts.iter().enumerate() {
        if let Some((col, end)) = try_col_range_literal(cand, schema) {
            ends.push(RangeEndEntry { conjunct: ci, col, end });
        }
    }
    ends
}

/// One column's interval, with the conjuncts that produced it — at most two,
/// since `collect_range_ends` emits one end per conjunct.
struct ColumnBound {
    start: Cut,
    end: Cut,
    consumed: [Option<usize>; 2],
}

/// The interval `ends` induce on `col`: its FIRST start-side and FIRST end-side
/// cut, each unconstrained side widened to the column type's edge. Same-side ends
/// are never compared to pick the tighter one — the residual re-imposes the one
/// not taken, exactly. `None` when no end covers `col`, or its type has no edges.
fn bound_next_column(col: u32, ends: &[RangeEndEntry], schema: &Schema) -> Option<ColumnBound> {
    let side_idx = |want| ends.iter().position(|e| e.col as u32 == col && e.end.side == want);
    let (start_idx, end_idx) = (side_idx(RangeSide::Start), side_idx(RangeSide::End));
    if start_idx.is_none() && end_idx.is_none() {
        return None;
    }
    let (edge_start, edge_end) = Cut::type_edges(schema.columns[col as usize].type_code)?;
    Some(ColumnBound {
        start: start_idx.map_or(edge_start, |i| ends[i].end.cut),
        end: end_idx.map_or(edge_end, |i| ends[i].end.cut),
        consumed: [start_idx, end_idx].map(|found| found.map(|i| ends[i].conjunct)),
    })
}

// ---------------------------------------------------------------------------
// The one recognizer, and the index candidates it produces
// ---------------------------------------------------------------------------

/// Bound `cols` — a PK or a secondary index, the same ordered key list either way
/// — by its leading equality prefix plus, where a conjunct covers the next column,
/// a range on it. `None` when nothing pins or bounds the list, or when an
/// uncovered trailing column is nullable.
fn bound_column_list(
    cols: &[u32],
    eqs: &[EqConjunct],
    ends: &[RangeEndEntry],
    schema: &Schema,
) -> Option<(RangeDescriptor, Vec<usize>)> {
    let (eq_vals, mut consumed) = consume_leading_eq_prefix(cols, eqs);
    let n_eq = eq_vals.len();
    let next = (n_eq < cols.len())
        .then(|| bound_next_column(cols[n_eq], ends, schema))
        .flatten();
    // Both arms pass an equality prefix strictly shorter than `cols` — the arity
    // `RangeDescriptor::new` asserts, `PK_LIST_MAX_COLS`-capped for an index list
    // and a validated PK alike.
    let (desc, covered) = match next {
        Some(b) => {
            consumed.extend(b.consumed.into_iter().flatten());
            (RangeDescriptor::new(&eq_vals, b.start, b.end), n_eq + 1)
        }
        // No range on the next column, or every column pinned: the last equality
        // lowers to a degenerate point on its own column. No equality either, and
        // nothing bounds.
        None => {
            let (&last, pinned) = eq_vals.split_last()?;
            (RangeDescriptor::point(pinned, last), n_eq)
        }
    };
    (!uncovered_trailing_nullable(cols, covered, schema)).then_some((desc, consumed))
}

/// One index-servable bound: the index's FULL declared column list, the wire
/// descriptor, and the residual bound conjuncts to filter after the walk.
pub(crate) struct IndexRangeCandidate<'e> {
    pub(crate) idx_cols: PkColList,
    pub(crate) desc: RangeDescriptor,
    pub(crate) residual: Vec<&'e BoundExpr>,
    /// Whether the index this candidate walks is UNIQUE.
    is_unique: bool,
}

impl IndexRangeCandidate<'_> {
    /// The walk's wire bound.
    pub(crate) fn bound(&self) -> gnitz_wire::IndexBound {
        gnitz_wire::IndexBound { idx_cols: self.idx_cols, desc: self.desc }
    }

    /// A point pinning every column of a UNIQUE index: at most one row, whatever
    /// the walk costs — the one fact comparable against a PK candidate.
    pub(crate) fn is_unique_point(&self) -> bool {
        self.is_unique && self.desc.pins_all(self.idx_cols.as_slice().len())
    }
}

/// Every index-servable bound for `conjuncts`, most-constrained first, each with
/// the residual its own walk leaves behind. A ranked list rather than one winner
/// because only the caller can tell whether a residual has a compiled form — the
/// tightest bound is not servable if its leftover conjunct is not.
pub(crate) fn ranked_index_bounds<'e>(
    conjuncts: &'e [BoundExpr],
    schema: &Schema,
    indexes: &[IndexMeta],
) -> Vec<IndexRangeCandidate<'e>> {
    let eqs = collect_eq_conjuncts(conjuncts, schema);
    let ends = collect_range_ends(conjuncts, schema);

    let mut out: Vec<IndexRangeCandidate<'e>> = indexes
        .iter()
        .filter_map(|meta| {
            let (desc, consumed) = bound_column_list(meta.cols.as_slice(), &eqs, &ends, schema)?;
            Some(IndexRangeCandidate {
                idx_cols: meta.cols,
                desc,
                residual: residual_conjuncts(conjuncts, &consumed),
                is_unique: meta.is_unique,
            })
        })
        .collect();
    // Stable, so a full rank tie leaves the first declared index first. A
    // single-winner `max_by_key` would return the LAST of equal maxima instead;
    // `min_by_key(Reverse(..))` is the form that keeps this order.
    out.sort_by_key(|c| Reverse(index_rank(&c.desc, c.is_unique, c.idx_cols.as_slice(), schema)));
    out
}

/// How constrained a candidate leaves the walk, most constrained first: a point
/// covering every column of a UNIQUE index (it admits one row, which no magnitude
/// can express), then columns pinned outright, then how tightly the first column
/// left unpinned is bounded, then the tighter index.
fn index_rank(
    desc: &RangeDescriptor,
    is_unique: bool,
    cols: &[u32],
    schema: &Schema,
) -> (bool, usize, u32, Reverse<usize>) {
    // A cut on the range column type's own edge excludes no index entry. A point
    // has no unpinned column — it pins the one it names — whichever conjuncts
    // spelled it, so it scores 0 without ever reaching an edge comparison.
    let unpinned_sides = if desc.is_point() {
        0
    } else {
        let tc = schema.columns[cols[desc.eq_vals().len()] as usize].type_code;
        // `bound_next_column` declines a type with no edges, so this default is inert.
        Cut::type_edges(tc).map_or(0, |(lo, hi)| (desc.start != lo) as u32 + (desc.end != hi) as u32)
    };
    (
        is_unique && desc.pins_all(cols.len()),
        desc.eq_vals().len() + desc.is_point() as usize,
        unpinned_sides,
        Reverse(cols.len()),
    )
}

// ---------------------------------------------------------------------------
// Unit tests — over bound conjuncts (parse + bind), asserted values unchanged.
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/access.rs"]
mod tests;
