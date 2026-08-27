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

use crate::codec::pk_codec::{bound_key_literal, bound_literal, pack_num, BoundLit, NumLit};
use crate::ir::{BExpr, BinOp, BoundExpr};
use gnitz_core::{Cut, FixedInt, IndexMeta, PkColList, PkTuple, RangeDescriptor, Schema, TypeCode};
use std::cmp::Reverse;
use std::collections::HashSet;

/// Classify a bound binary op as `col OP literal`, the `ColRef` on either side.
/// The returned operator always reads `col OP lit` — a literal on the left is
/// mirrored through [`BinOp::converse`] here — so no caller handles sides.
fn bound_col_vs_literal(expr: &BoundExpr) -> Option<(usize, BoundLit<'_>, BinOp)> {
    let BExpr::BinOp(left, op, right) = expr else {
        return None;
    };
    if let BExpr::ColRef(idx) = left.as_ref() {
        if let Some(l) = bound_literal(right) {
            return Some((*idx, l, *op));
        }
    }
    if let BExpr::ColRef(idx) = right.as_ref() {
        if let Some(l) = bound_literal(left) {
            return Some((*idx, l, op.converse()));
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
    let (col_idx, lit, op) = bound_col_vs_literal(expr)?;
    if !matches!(op, BinOp::Eq) {
        return None;
    }
    let key = bound_key_literal(lit, schema.columns[col_idx].type_code)?;
    Some((col_idx, key))
}

/// The keys of a bound `pk IN (literal, …)` conjunct on a single-column PK, deduped
/// (first occurrence wins). `None` for any other conjunct. `NOT IN` binds to
/// `UnaryOp(Not, InList)` and so never matches here; a one-key `IN` folds to `Eq` at
/// bind and is served by [`try_extract_pk_range`].
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
    let pk_idx = schema.pk_indices()[0];
    if *col_idx != pk_idx {
        return None;
    }
    let pk_tc = schema.columns[pk_idx].type_code;
    let mut seen = HashSet::with_capacity(items.len());
    let mut pks = Vec::with_capacity(items.len());
    for item in items {
        // `bound_key_literal` is the same rule `try_col_eq_literal` applies, so
        // `IN (…)` and `= …` route identically. A NULL/float/non-literal or an
        // unparseable UUID aborts to the slow scan.
        let v = bound_key_literal(bound_literal(item)?, pk_tc)?;
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
    if !desc.pins_all(schema.pk_count()) {
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
/// conjuncts — the PK as one more key list through [`bound_column_list`]. The walk
/// is byte-exact at any width, so a consumed conjunct is applied exactly and
/// stripped, which is what serves a wide (U128) PK range without the predicate VM.
pub(crate) fn try_extract_pk_range<'e>(
    where_expr: &'e BoundExpr,
    schema: &Schema,
) -> Option<(RangeDescriptor, Vec<&'e BoundExpr>)> {
    let mut conjuncts = Vec::new();
    flatten_bound_conjuncts(where_expr, &mut conjuncts);
    let eqs = collect_eq_conjuncts(&conjuncts, schema);
    let ends = collect_range_ends(&conjuncts, schema);

    let pk_cols: Vec<u32> = schema.pk_indices().iter().map(|&c| c as u32).collect();
    let (desc, consumed) = bound_column_list(&pk_cols, &eqs, &ends, schema)?;
    Some((desc, residual_conjuncts(&conjuncts, &consumed)))
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
fn collect_eq_conjuncts(conjuncts: &[&BoundExpr], schema: &Schema) -> Vec<EqConjunct> {
    conjuncts
        .iter()
        .enumerate()
        .filter_map(|(ci, &cand)| {
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

/// A range-end literal → its cut for a column of type `tc`; `mk` builds the cut of
/// an in-range value (`Cut::After` above the literal's duplicate group,
/// `Cut::Before` below). A literal past the type's min/max SATURATES to that edge
/// rather than wrapping; a type carrying no ordered range gives `None`.
fn parse_range_cut(tc: TypeCode, lit: NumLit<'_>, mk: fn(u128) -> Cut) -> Option<Cut> {
    // U128: full unsigned range — saturation is impossible (an i128 cannot
    // represent its upper half), and a literal past u128::MAX fails the parse,
    // keeping the conjunct a residual. `pack_num` is exactly that classification,
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
    let (col_idx, lit, op) = bound_col_vs_literal(expr)?;
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
fn collect_range_ends(conjuncts: &[&BoundExpr], schema: &Schema) -> Vec<RangeEndEntry> {
    let mut ends: Vec<RangeEndEntry> = Vec::new();
    for (ci, &cand) in conjuncts.iter().enumerate() {
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
    /// A point pinning every column of a UNIQUE index: at most one row, whatever
    /// the walk costs — the one fact comparable against a PK candidate.
    pub(crate) fn is_unique_point(&self) -> bool {
        self.is_unique && self.desc.pins_all(self.idx_cols.as_slice().len())
    }
}

/// Every index-servable bound for `where_expr`, most-constrained first, each with
/// the residual its own walk leaves behind. A ranked list rather than one winner
/// because only the caller can tell whether a residual has a compiled form — the
/// tightest bound is not servable if its leftover conjunct is not.
pub(crate) fn ranked_index_bounds<'e>(
    where_expr: &'e BoundExpr,
    schema: &Schema,
    indexes: &[IndexMeta],
) -> Vec<IndexRangeCandidate<'e>> {
    let mut conjuncts = Vec::new();
    flatten_bound_conjuncts(where_expr, &mut conjuncts);
    let eqs = collect_eq_conjuncts(&conjuncts, schema);
    let ends = collect_range_ends(&conjuncts, schema);

    let mut out: Vec<IndexRangeCandidate<'e>> = indexes
        .iter()
        .filter_map(|meta| {
            let (desc, consumed) = bound_column_list(meta.cols.as_slice(), &eqs, &ends, schema)?;
            Some(IndexRangeCandidate {
                idx_cols: meta.cols,
                desc,
                residual: residual_conjuncts(&conjuncts, &consumed),
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
mod tests {
    use super::*;
    use crate::bind::bind_single_table;
    use crate::codec::pk_codec::extract_pk_value;
    use crate::test_support::{
        bind_where, col_def, compound_schema_u64_u64, eq_expr, idx_metas, idx_metas_flagged, in_list_expr,
        neg_num_expr, num_expr, parse_expr_sql, pk_schema, two_col, uuid_schema_payload, uuid_schema_pk,
    };
    use sqlparser::ast::Expr;

    /// `(id U64 pk, a tc, b tc)` — indexable cols a=1, b=2, `b` nullable per arg.
    fn schema3(tc: TypeCode, b_nullable: bool) -> Schema {
        Schema {
            columns: vec![
                col_def("id", TypeCode::U64, false),
                col_def("a", tc, false),
                col_def("b", tc, b_nullable),
            ],
            pk_cols: vec![0],
        }
    }

    /// The head (best) candidate the arbitration picks for `where_sql`: the
    /// winning index's declared column list and its descriptor. `lists` carries
    /// each index's `is_unique` flag.
    fn picked_flagged(where_sql: &str, sch: &Schema, lists: &[(&[u32], bool)]) -> Option<(Vec<u32>, RangeDescriptor)> {
        let bound = bind_where(where_sql, sch);
        ranked_index_bounds(&bound, sch, &idx_metas_flagged(lists))
            .into_iter()
            .next()
            .map(|c| (c.idx_cols.as_slice().to_vec(), c.desc))
    }

    /// [`picked_flagged`] over non-unique indexes.
    fn picked(where_sql: &str, sch: &Schema, lists: &[&[u32]]) -> Option<(Vec<u32>, RangeDescriptor)> {
        let flagged: Vec<(&[u32], bool)> = lists.iter().map(|&cols| (cols, false)).collect();
        picked_flagged(where_sql, sch, &flagged)
    }

    /// The bound [`bound_column_list`] derives for the key list `cols`, plus its
    /// residual — the one recognizer alone, without the PK-list construction or
    /// the cross-index ranking wrapped around it.
    fn bound_list_of<'e>(
        expr: &'e BoundExpr,
        cols: &[u32],
        sch: &Schema,
    ) -> Option<(RangeDescriptor, Vec<&'e BoundExpr>)> {
        let mut conjuncts = Vec::new();
        flatten_bound_conjuncts(expr, &mut conjuncts);
        let eqs = collect_eq_conjuncts(&conjuncts, sch);
        let ends = collect_range_ends(&conjuncts, sch);
        let (desc, consumed) = bound_column_list(cols, &eqs, &ends, sch)?;
        Some((desc, residual_conjuncts(&conjuncts, &consumed)))
    }

    // ------------------------------------------------------------------
    // try_col_eq_literal — UUID + signed/wide index seek keys
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

    /// The seek key `col = literal` packs, per column type: a negative signed
    /// literal packs at the column's own width, an unsigned column takes none, and
    /// a magnitude past `i64` (which binds as `LitWide`) still packs exactly.
    #[test]
    fn an_equality_packs_its_key_at_the_columns_width() {
        for (tc, sql, want) in [
            (TypeCode::I64, "val = -1", Some(((-1i64) as u64) as u128)),
            (TypeCode::I32, "val = -1", Some(((-1i32) as u32) as u128)),
            (TypeCode::I16, "val = -1", Some(((-1i16) as u16) as u128)),
            (TypeCode::I8, "val = -5", Some(((-5i8) as u8) as u128)),
            (TypeCode::I64, "val = 42", Some(42u128)),
            // An unsigned column holds no negative value.
            (TypeCode::U64, "val = -1", None),
            // u64::MAX overflows i64 → binds to `LitWide`; the recognizer parses it
            // byte-exactly for a U64 column (the servable wide index seek).
            (TypeCode::U64, "val = 18446744073709551615", Some(u64::MAX as u128)),
        ] {
            let schema = two_col(tc);
            let expr = bind_where(sql, &schema);
            assert_eq!(
                try_col_eq_literal(&expr, &schema),
                want.map(|key| (1, key)),
                "{tc:?}: {sql}"
            );
        }
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
        assert!(matches!(err, crate::GnitzSqlError::Bind(_)), "got {err:?}");
    }

    /// A qualified reference resolves to the same column (the single-relation
    /// leniency at bind), and the literal may sit on EITHER side of the `=` — the
    /// mirrored arm of `bound_col_vs_literal` no other equality test reaches.
    #[test]
    fn a_flipped_qualified_equality_is_the_same_seek_key() {
        let schema = pk_schema(TypeCode::U64); // (id U64 pk, v I64)
        assert_eq!(
            try_col_eq_literal(&bind_where("5 = t.v", &schema), &schema),
            Some((1, 5))
        );
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
    /// range names no key. A qualified reference takes the same path.
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

        let qualified = bind_where("t.id = 1", &schema);
        let (pk, residual) = pk_point_of(&qualified, &schema).expect("a qualified PK binds");
        assert_eq!(pk.as_bytes(), &1u64.to_le_bytes()[..]);
        assert!(residual.is_empty());
    }

    // ------------------------------------------------------------------
    // What a column list matches at all
    // ------------------------------------------------------------------

    /// `WHERE val = 1` on a float column bounds nothing: `try_col_eq_literal`
    /// declines a type that has no packed key, so no index can serve it.
    #[test]
    fn a_float_column_is_never_an_index_key() {
        let schema = two_col(TypeCode::F64);
        let expr = bind_where("val = 1", &schema);
        assert!(ranked_index_bounds(&expr, &schema, &idx_metas(&[&[1]])).is_empty());
    }

    /// The whole AND-tree is walked. `a = 1 AND b = 2 AND c = 3` binds as
    /// `((a = 1 AND b = 2) AND c = 3)`, so finding the leftmost-deepest conjunct
    /// and the rightmost one — each against an index that names only its column —
    /// proves every leaf is reached.
    #[test]
    fn every_leaf_of_the_and_tree_is_reached() {
        let schema = Schema {
            columns: vec![
                col_def("pk", TypeCode::U64, false),
                col_def("a", TypeCode::U64, true),
                col_def("b", TypeCode::U64, true),
                col_def("c", TypeCode::U64, true),
            ],
            pk_cols: vec![0],
        };
        let expr = bind_where("a = 1 AND b = 2 AND c = 3", &schema);
        for (col, key) in [(1u32, 1u128), (3, 3)] {
            let cands = ranked_index_bounds(&expr, &schema, &idx_metas(&[&[col]]));
            assert_eq!(cands.len(), 1, "INDEX({col})");
            assert_eq!(cands[0].desc, RangeDescriptor::point(&[], key), "INDEX({col})");
            assert_eq!(cands[0].residual.len(), 2, "INDEX({col})");
        }
    }

    /// `(id U64 pk, x U64, a U64, b U64, c U64)` — every payload col non-nullable,
    /// so no candidate is dropped by `uncovered_trailing_nullable`.
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

    /// A point covering every column of UNIQUE(x) selects at most one row, so it
    /// outranks the longer — but non-unique, and disjoint — prefix of INDEX(a, b).
    #[test]
    fn a_covered_unique_index_outranks_a_longer_disjoint_prefix() {
        let (cols, _) = picked_flagged(
            "x = 1 AND a = 2 AND b = 3",
            &seek_rank_schema(),
            &[(&[1], true), (&[2, 3], false)],
        )
        .expect("some index must bound");
        assert_eq!(cols, vec![1], "UNIQUE(x) admits one row; INDEX(a, b) admits many");
    }

    /// With no candidate fully covered the leading term is false for both, so the
    /// rest of the rank decides: equal pinned depth, then the tighter index.
    /// (`seek_rank_schema`'s payload columns are all non-nullable, so the
    /// uncovered trailing column of each index does not reject it outright.)
    #[test]
    fn a_partial_cover_of_a_unique_index_does_not_jump_the_queue() {
        let (cols, _) = picked_flagged("a = 1", &seek_rank_schema(), &[(&[2, 3], true), (&[2, 3, 4], false)])
            .expect("some index must bound");
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
    /// the residual rather than sinking the whole WHERE to a scan. A qualified
    /// reference takes the same path.
    #[test]
    fn pk_in_inside_an_and_tree_gathers_with_a_residual() {
        let schema = pk_schema(TypeCode::U64);
        let expr = bind_where("v > 5 AND id IN (7, 9)", &schema);
        let (keys, residual) = try_extract_pk_in(&expr, &schema).expect("the IN conjunct bounds the gather");
        assert_eq!(keys, vec![7, 9]);
        assert_eq!(residual.len(), 1, "`v > 5` stays residual");
        assert_eq!(pk_in_keys_of("t.id IN (1, 2)", &schema), Some(vec![1, 2]));
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
        let c = ranked_index_bounds(&expr, &schema, &idx_metas(&[&[1]]))
            .into_iter()
            .next()
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
    fn every_pk_type_routes_to_one_packed_key() {
        for (tc, literal, expected) in [
            (TypeCode::I8, neg_num_expr("1"), (-1i8 as u8) as u128),
            (TypeCode::I16, neg_num_expr("1"), (-1i16 as u16) as u128),
            (TypeCode::I32, neg_num_expr("1"), (-1i32 as u32) as u128),
            (TypeCode::I64, neg_num_expr("1"), ((-1i64) as u64) as u128),
            // The prepend-`-` parse rule: the `i64::MIN` magnitude overflows i64 →
            // `LitWide`, and every path parses it byte-exactly.
            (
                TypeCode::I64,
                neg_num_expr("9223372036854775808"),
                (i64::MIN as u64) as u128,
            ),
            (TypeCode::U16, num_expr("65535"), 65535u128),
            (TypeCode::U32, num_expr("4294967295"), 4294967295u128),
            // u64::MAX binds to `LitWide` for the WHERE seeks; INSERT parses it directly.
            (TypeCode::U64, num_expr("18446744073709551615"), u64::MAX as u128),
        ] {
            check_pk_parity(tc, literal, expected);
        }
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
        // U128 takes the full unsigned range: no saturation, and no negative.
        assert_eq!(
            ck(TypeCode::U128, "340282366920938463463374607431768211455", false, Before),
            Some(Before(u128::MAX))
        );
        assert_eq!(ck(TypeCode::U128, "5", true, Before), None);
    }

    #[test]
    fn try_col_range_literal_orientations() {
        use Cut::{After, Before};
        let schema = two_col(TypeCode::I64);
        let ck = |sql: &str| try_col_range_literal(&bind_where(sql, &schema), &schema);
        let (c, e) = ck("val > 5").unwrap();
        assert_eq!(c, 1);
        assert!(e.side == RangeSide::Start && e.cut == After(5));
        let (_, e) = ck("5 < val").unwrap();
        assert!(e.side == RangeSide::Start && e.cut == After(5));
        let (_, e) = ck("val <= 5").unwrap();
        assert!(e.side == RangeSide::End && e.cut == After(5));
        let (_, e) = ck("5 >= val").unwrap();
        assert!(e.side == RangeSide::End && e.cut == After(5));
        let (_, e) = ck("val >= 5").unwrap();
        assert!(e.side == RangeSide::Start && e.cut == Before(5));
        let (_, e) = ck("val < 5").unwrap();
        assert!(e.side == RangeSide::End && e.cut == Before(5));
        // Equality is not a range end.
        assert!(ck("val = 5").is_none());
    }

    // ── bound_column_list: the one recognizer, over one key list ────────────────

    #[test]
    fn a_composite_eq_prefix_pins_and_then_bounds() {
        use Cut::Before;
        let schema = schema3(TypeCode::U64, false);
        let expr = bind_where("a = 7 AND b < 50", &schema);
        let (desc, residual) = bound_list_of(&expr, &[1, 2], &schema).expect("the eq prefix + range bounds");
        assert_eq!(desc.eq_vals(), &[7u128]);
        assert_eq!(desc.start, Before(0));
        assert_eq!(desc.end, Before(50));
        assert!(residual.is_empty(), "both conjuncts consumed");
    }

    #[test]
    fn between_desugars_to_two_ends_and_not_between_to_none() {
        use Cut::{After, Before};
        let schema = two_col(TypeCode::I64);
        // BETWEEN desugars at bind to `val >= 10 AND val <= 20` → two consumed ends.
        let expr = bind_where("val BETWEEN 10 AND 20", &schema);
        let (desc, residual) = bound_list_of(&expr, &[1], &schema).expect("BETWEEN bounds");
        assert_eq!((desc.start, desc.end), (Before(10), After(20)));
        assert!(residual.is_empty());

        // NOT BETWEEN binds to `Not(val >= 10 AND val <= 20)` — one leaf conjunct,
        // no range end → nothing bounds.
        let expr = bind_where("val NOT BETWEEN 10 AND 20", &schema);
        assert!(bound_list_of(&expr, &[1], &schema).is_none());
    }

    #[test]
    fn a_redundant_same_side_end_keeps_the_first_and_stays_residual() {
        use Cut::After;
        let schema = two_col(TypeCode::U64);
        for sql in ["val > 5 AND val > 10", "val > 10 AND val > 5"] {
            let expr = bind_where(sql, &schema);
            let (desc, residual) = bound_list_of(&expr, &[1], &schema).unwrap_or_else(|| panic!("{sql}: bounds"));
            let first_val: u128 = if sql.starts_with("val > 5") { 5 } else { 10 };
            assert_eq!(desc.start, After(first_val), "{sql}");
            assert_eq!(residual.len(), 1, "{sql}: other same-side end stays residual");
        }
    }

    #[test]
    fn an_out_of_range_end_saturates_to_the_type_edge() {
        use Cut::{After, Before};
        let schema = two_col(TypeCode::I32);
        let (min, max) = ((i32::MIN as u32) as u128, i32::MAX as u128);

        let expr = bind_where("val > 3000000000", &schema);
        let (desc, _) = bound_list_of(&expr, &[1], &schema).expect("a saturated end still bounds");
        assert_eq!((desc.start, desc.end), (After(max), After(max)));

        let expr = bind_where("val < 3000000000", &schema);
        let (desc, _) = bound_list_of(&expr, &[1], &schema).expect("a saturated end still bounds");
        assert_eq!((desc.start, desc.end), (Before(min), After(max)));
    }

    #[test]
    fn a_conjunct_the_key_list_does_not_name_stays_residual() {
        use Cut::After;
        let schema = schema3(TypeCode::U64, false);
        // The list names `b` only, so `a = 7` is not consumed.
        let expr = bind_where("b > 10 AND a = 7", &schema);
        let (desc, residual) = bound_list_of(&expr, &[2], &schema).expect("`b > 10` bounds INDEX(b)");
        assert!(desc.eq_vals().is_empty());
        assert_eq!(desc.start, After(10));
        assert_eq!(residual.len(), 1, "`a = 7` is a residual conjunct");
    }

    // ── ranked_index_bounds: the whole-WHERE arbitration ─────────────────────────
    //
    // These pin the *chosen* bound, not just the recognizer above: which candidate
    // wins across shapes and across separate indexes.

    /// A pure equality on a 1-column index lowers to a degenerate point range.
    #[test]
    fn equality_lowers_to_a_degenerate_point_range() {
        let (idx_cols, desc) =
            picked("a = 5", &schema3(TypeCode::U64, false), &[&[1]]).expect("a = 5 on an index over `a` must bound");
        assert_eq!(idx_cols, vec![1]);
        assert_eq!(desc, RangeDescriptor::point(&[], 5));
    }

    /// A two-column equality pins the leading column and points at the last.
    #[test]
    fn compound_equality_pins_the_leading_column() {
        let b = picked("a = 5 AND b = 7", &schema3(TypeCode::U64, false), &[&[1, 2]]);
        let (idx_cols, desc) = b.expect("a compound equality must bound the compound index");
        assert_eq!(idx_cols, vec![1, 2]);
        assert_eq!(desc, RangeDescriptor::point(&[5], 7));
    }

    /// An equality prefix plus a range on ONE index takes the range.
    #[test]
    fn equality_prefix_plus_range_takes_the_range_candidate() {
        let b = picked("a = 5 AND b > 10", &schema3(TypeCode::U64, false), &[&[1, 2]]);
        let (_, desc) = b.expect("an eq-prefix + range must bound");
        assert_eq!(desc.eq_vals(), &[5u128], "`a` is the pinned prefix");
        assert_eq!(desc.start, Cut::After(10), "`b > 10` is an exclusive lower cut");
        assert_ne!(desc.end, Cut::After(10), "the upper side stays open, not a point");
    }

    /// Across SEPARATE indexes, most-pinned wins.
    #[test]
    fn point_on_one_index_beats_half_open_range_on_another() {
        let b = picked("a = 5 AND b > 10", &schema3(TypeCode::U64, false), &[&[1], &[2]]);
        let (idx_cols, desc) = b.expect("the point candidate must bound");
        assert_eq!(idx_cols, vec![1], "INDEX(a)'s point beats INDEX(b)'s range");
        assert_eq!(desc, RangeDescriptor::point(&[], 5));
    }

    /// A PK column is an ordinary index column: an equality on it bounds nothing
    /// against an index that does not name it, and points one that does.
    #[test]
    fn a_pk_equality_bounds_the_index_that_names_it() {
        let sch = schema3(TypeCode::U64, false);
        assert!(picked("id = 5", &sch, &[&[1]]).is_none());
        let (idx_cols, desc) = picked("id = 5", &sch, &[&[0]]).expect("INDEX(id) is bounded by `id = 5`");
        assert_eq!(idx_cols, vec![0]);
        assert_eq!(desc, RangeDescriptor::point(&[], 5));
    }

    /// An index over a PK column of a COMPOUND PK is the shape the PK rung cannot
    /// serve: nothing pins the leading PK column, so only the index bounds. Its
    /// range form already worked, since range ends were never screened by column.
    #[test]
    fn an_index_over_a_compound_pk_column_bounds_like_any_other() {
        let sch = Schema {
            columns: vec![
                col_def("a", TypeCode::U64, false),
                col_def("b", TypeCode::U64, false),
                col_def("v", TypeCode::U64, false),
            ],
            pk_cols: vec![0, 1],
        };
        for sql in ["b = 1 AND v = 2", "b > 5"] {
            assert!(
                try_extract_pk_range(&bind_where(sql, &sch), &sch).is_none(),
                "{sql}: nothing pins the leading PK column"
            );
        }
        let (idx_cols, desc) = picked("b = 1 AND v = 2", &sch, &[&[1, 2]]).expect("INDEX(b, v) is fully pinned");
        assert_eq!(idx_cols, vec![1, 2]);
        assert_eq!(desc, RangeDescriptor::point(&[1], 2));

        let (_, desc) = picked("b > 5", &sch, &[&[1, 2]]).expect("a range on a PK column bounds the index");
        assert_eq!(desc.start, Cut::After(5));
    }

    /// An uncovered NULLABLE trailing index column must NOT bound.
    #[test]
    fn uncovered_nullable_trailing_column_never_bounds() {
        assert!(picked("a = 5", &schema3(TypeCode::U64, true), &[&[1, 2]]).is_none());
        assert!(picked("a = 5", &schema3(TypeCode::U64, false), &[&[1, 2]]).is_some());
    }

    /// A column no index covers bounds nothing, and neither does a WHERE with no
    /// `col OP literal` conjunct at all.
    #[test]
    fn unindexed_column_bounds_nothing() {
        assert!(picked("a = 5", &schema3(TypeCode::U64, false), &[&[2]]).is_none());
        assert!(picked("a + b > 3", &schema3(TypeCode::U64, false), &[&[1]]).is_none());
    }

    /// A BETWEEN is a two-sided range over one column (desugared at bind).
    #[test]
    fn between_bounds_both_sides() {
        let b = picked("a BETWEEN 5 AND 9", &schema3(TypeCode::U64, false), &[&[1]]);
        let (_, desc) = b.expect("BETWEEN must bound");
        assert_eq!(desc.eq_vals(), &[] as &[u128]);
        assert_eq!((desc.start, desc.end), (Cut::Before(5), Cut::After(9)));
    }

    /// A UUID index column: `Cut::type_edges(UUID)` is `None`, so an equality on
    /// one must be ranked without ever consulting the column's edges. Such a
    /// predicate has no index-free plan — the VM has no 128-bit register — so the
    /// bound must consume the conjunct outright.
    #[test]
    fn a_uuid_index_equality_bounds_a_point() {
        let schema = uuid_schema_payload();
        let expr = bind_where("uid = '550e8400-e29b-41d4-a716-446655440000'", &schema);
        let cands = ranked_index_bounds(&expr, &schema, &idx_metas(&[&[1]]));
        assert_eq!(cands.len(), 1);
        assert_eq!(
            cands[0].desc,
            RangeDescriptor::point(&[], 0x550e8400_e29b_41d4_a716_446655440000_u128)
        );
        assert!(cands[0].residual.is_empty());
    }

    // ── the rank: a point outranks an interval at equal depth ───────────────────

    /// A full point on a UNIQUE index beats a two-sided interval on another index,
    /// on a signed column and on an unsigned one (`a = 0`, whose cuts coincide with
    /// `type_edges(U64)`).
    #[test]
    fn full_unique_point_beats_an_interval() {
        for (tc, pt) in [(TypeCode::U64, "5"), (TypeCode::I64, "5")] {
            let sch = schema3(tc, false);
            let where_sql = format!("a = {pt} AND b BETWEEN 1 AND 9");
            let (cols, desc) = picked_flagged(&where_sql, &sch, &[(&[1], true), (&[2], false)]).expect("must bound");
            assert_eq!(cols, vec![1], "{tc:?}: the unique point on `a` must win");
            assert_eq!((desc.start, desc.end), (Cut::Before(5), Cut::After(5)));
        }
    }

    /// The same at both type edges of both signednesses.
    #[test]
    fn full_unique_point_wins_at_the_type_edges() {
        for (tc, lit) in [
            (TypeCode::U64, "0"),
            (TypeCode::U64, "18446744073709551615"),
            (TypeCode::I64, "-9223372036854775808"),
            (TypeCode::I64, "9223372036854775807"),
        ] {
            let sch = schema3(tc, false);
            let where_sql = format!("a = {lit} AND b BETWEEN 1 AND 9");
            let (cols, _) = picked_flagged(&where_sql, &sch, &[(&[1], true), (&[2], false)]).expect("must bound");
            assert_eq!(cols, vec![1], "{tc:?} `a = {lit}` must win");
        }
    }

    /// Uniqueness is not what decides it: at equal pinned depth a point outranks a
    /// two-sided interval outright, because it pins its column where the interval
    /// only narrows one.
    #[test]
    fn a_point_outranks_an_interval_at_equal_depth() {
        let sch = schema3(TypeCode::I64, false);
        let (cols, _) = picked("a = 5 AND b BETWEEN 1 AND 9", &sch, &[&[1], &[2]]).expect("some index must bound");
        assert_eq!(cols, vec![1], "the point on `a` admits one group, the interval nine");
    }

    /// A point on a PREFIX of a UNIQUE index is not a unique point — it leaves the
    /// index's trailing column free — but it still pins one column, which outranks
    /// the interval's none.
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
        let (cols, desc) =
            picked_flagged("a = 5 AND c BETWEEN 1 AND 9", &sch, &[(&[1, 2], true), (&[3], false)]).expect("bounds");
        assert_eq!(cols, vec![1, 2]);
        assert!(
            !desc.pins_all(2),
            "a prefix point on UNIQUE(a, b) selects more than one row"
        );
    }

    /// The uniqueness lookup is not equality-only: a degenerate point the range
    /// path produced (`a >= 5 AND a <= 5`) is scored unique too.
    #[test]
    fn a_range_collectors_point_is_scored_unique() {
        let sch = schema3(TypeCode::I64, false);
        let (cols, desc) = picked_flagged(
            "a >= 5 AND a <= 5 AND b BETWEEN 1 AND 9",
            &sch,
            &[(&[1], true), (&[2], false)],
        )
        .expect("must bound");
        assert_eq!(cols, vec![1]);
        assert_eq!((desc.start, desc.end), (Cut::Before(5), Cut::After(5)));
    }

    /// The two spellings of one point rank identically. `0` is
    /// `type_edges(U64).0`, so scoring a point by comparing its cuts against the
    /// type edges would have ranked `a = 0` below the interval it must beat, and
    /// split the two spellings apart.
    #[test]
    fn a_point_ranks_the_same_however_it_is_spelled() {
        let sch = schema3(TypeCode::U64, false);
        for sql in ["a = 0 AND b BETWEEN 1 AND 9", "a >= 0 AND a <= 0 AND b BETWEEN 1 AND 9"] {
            let (cols, desc) = picked(sql, &sch, &[&[1], &[2]]).unwrap_or_else(|| panic!("{sql}: must bound"));
            assert_eq!(cols, vec![1], "{sql}");
            assert_eq!(desc, RangeDescriptor::point(&[], 0), "{sql}");
        }
    }

    // ── when a PK bound yields to a one-row index point ─────────────────────────

    /// The winner of the arbitration is a point covering a whole UNIQUE index.
    #[test]
    fn a_full_unique_point_is_recognized() {
        let sch = schema3(TypeCode::U64, false);
        let bound = bind_where("a = 42", &sch);
        let c = ranked_index_bounds(&bound, &sch, &idx_metas_flagged(&[(&[1], true)]))
            .into_iter()
            .next()
            .expect("the WHERE must bound some index");
        assert!(c.is_unique_point());
    }
}
