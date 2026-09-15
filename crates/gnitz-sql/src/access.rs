//! Access-path recognition over bound conjuncts, shared by the DML planners and the
//! view compiler: [`candidates`] lists every bound a WHERE admits, best first.
//!
//! An **AST-free leaf** — `ir`, `codec::pk_codec` and `gnitz-core`, nothing else.

use crate::codec::pk_codec::{bound_key_literal, col_key_literal, BoundLit, KeyLitError};
use crate::ir::{BExpr, BinOp, BoundExpr};
use gnitz_core::{Cut, IndexMeta, PkColumn, RangeDescriptor, Schema, TypeCode, PK_LIST_MAX_COLS};
use gnitz_wire::{IndexBound, IndexWalk, PkKeys, ReadBound};
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
// One term per conjunct
// ---------------------------------------------------------------------------

/// What one conjunct pins on one column.
enum Pin {
    Eq(u128),
    In(Vec<u128>),
    Start(Cut),
    End(Cut),
}

/// A classified conjunct: its index in the WHERE, the column it names, its pin.
struct Term {
    conjunct: usize,
    col: usize,
    pin: Pin,
}

/// A conjunct as the pin it puts on one column; `None` when no bound consumes it.
fn term(conjunct: &BoundExpr, schema: &Schema) -> Option<(usize, Pin)> {
    if let BExpr::InList { inner, items } = conjunct {
        let BExpr::ColRef(col) = inner.as_ref() else {
            return None;
        };
        // Only a PK key set consumes a list; an index bound is one interval.
        if !schema.pk_cols.contains(&(*col as u32)) {
            return None;
        }
        let c = &schema.columns[*col];
        let mut keys: Vec<u128> = items
            .iter()
            .map(|i| bound_key_literal(col_key_literal(i, c)?, c.type_code).ok())
            .collect::<Option<_>>()?;
        // The binder keeps a list as written; a repeat is no second key.
        keys.sort_unstable();
        keys.dedup();
        return Some((*col, Pin::In(keys)));
    }
    let (col, lit, op) = bound_col_vs_literal(conjunct, schema)?;
    let tc = schema.columns[col].type_code;
    let pin = match op {
        BinOp::Eq => Pin::Eq(bound_key_literal(lit, tc).ok()?),
        BinOp::Gt => Pin::Start(range_cut(lit, tc, Cut::After)?),
        BinOp::Ge => Pin::Start(range_cut(lit, tc, Cut::Before)?),
        BinOp::Lt => Pin::End(range_cut(lit, tc, Cut::Before)?),
        BinOp::Le => Pin::End(range_cut(lit, tc, Cut::After)?),
        _ => return None,
    };
    Some((col, pin))
}

/// A range-end literal's cut: the key rule's value when the column holds it, else
/// the type edge on the literal's side. `None` for a type with no ordered range.
fn range_cut(lit: BoundLit, tc: TypeCode, mk: fn(u128) -> Cut) -> Option<Cut> {
    let (below, above) = Cut::type_edges(tc)?;
    let negative = matches!(lit, BoundLit::Num(n) if n.is_negative());
    match bound_key_literal(lit, tc) {
        Ok(v) => Some(mk(v)),
        Err(KeyLitError::NegativeIntoUnsigned | KeyLitError::OutOfRange) => Some(if negative { below } else { above }),
        Err(KeyLitError::NotOfType | KeyLitError::NotNumeric) => None,
    }
}

// ---------------------------------------------------------------------------
// A column's pins, folded into its interval
// ---------------------------------------------------------------------------

/// A column's pins intersected, and the conjuncts that fed them. The intersection
/// is exact, so a consumed conjunct needs no re-imposing.
struct Folded {
    start: Option<Cut>,
    end: Option<Cut>,
    consumed: Vec<usize>,
}

impl Folded {
    /// The one value the interval admits, when it is a point.
    fn point(&self) -> Option<u128> {
        match (self.start?, self.end?) {
            (Cut::Before(a), Cut::After(b)) if a == b => Some(a),
            _ => None,
        }
    }
}

/// Cut order on a column of type `tc`: value in key order (a signed type's sign bit
/// flipped at its stride, as the OPK encoding does), then `Before` below `After`.
fn cut_key(c: Cut, tc: TypeCode) -> (u128, bool) {
    let flip = if tc.is_signed_int() {
        1u128 << (8 * tc.wire_stride() - 1)
    } else {
        0
    };
    (c.value() ^ flip, matches!(c, Cut::After(_)))
}

/// Every `Eq`/`Start`/`End` pin on `col` intersected; `Eq(v)` is the interval
/// `[Before(v), After(v))`. `None` when no such pin names `col`.
fn fold(col: u32, terms: &[Term], tc: TypeCode) -> Option<Folded> {
    let mut f = Folded {
        start: None,
        end: None,
        consumed: Vec::new(),
    };
    for t in terms.iter().filter(|t| t.col as u32 == col) {
        let (start, end) = match t.pin {
            Pin::Eq(v) => (Some(Cut::Before(v)), Some(Cut::After(v))),
            Pin::Start(c) => (Some(c), None),
            Pin::End(c) => (None, Some(c)),
            Pin::In(_) => continue,
        };
        if let Some(s) = start {
            f.start = Some(f.start.map_or(s, |a| std::cmp::max_by_key(a, s, |c| cut_key(*c, tc))));
        }
        if let Some(e) = end {
            f.end = Some(f.end.map_or(e, |a| std::cmp::min_by_key(a, e, |c| cut_key(*c, tc))));
        }
        f.consumed.push(t.conjunct);
    }
    (!f.consumed.is_empty()).then_some(f)
}

/// Bound the key list `cols` — a PK or an index — by its leading point columns and
/// the next column's interval. `None` when nothing bounds it, or when an uncovered
/// trailing column is nullable.
fn bound_column_list(cols: &[u32], terms: &[Term], schema: &Schema) -> Option<(RangeDescriptor, Vec<usize>)> {
    let mut eq_vals = Vec::new();
    let mut consumed = Vec::new();
    let mut next = None;
    for &col in cols {
        let tc = schema.columns[col as usize].type_code;
        let Some(f) = fold(col, terms, tc) else {
            break;
        };
        if let Some(v) = f.point() {
            eq_vals.push(v);
            consumed.extend(f.consumed);
            continue;
        }
        let edges = Cut::type_edges(tc);
        let start = f.start.or(edges.map(|e| e.0));
        let end = f.end.or(edges.map(|e| e.1));
        next = start.zip(end).map(|(s, e)| (s, e, f.consumed));
        break;
    }
    let (desc, covered) = match next {
        Some((start, end, c)) => {
            consumed.extend(c);
            (RangeDescriptor::new(&eq_vals, start, end), eq_vals.len() + 1)
        }
        None => {
            let (&last, pinned) = eq_vals.split_last()?;
            (RangeDescriptor::point(pinned, last), eq_vals.len())
        }
    };
    // An index holds no row with a NULL in any of its columns.
    let trailing_nullable = cols[covered..].iter().any(|&c| schema.columns[c as usize].is_nullable);
    (!trailing_nullable).then_some((desc, consumed))
}

// ---------------------------------------------------------------------------
// The PK key set
// ---------------------------------------------------------------------------

/// The PK keys the WHERE names: each PK column's point, else its first IN list,
/// crossed into OPK keys.
fn pk_key_set(terms: &[Term], schema: &Schema) -> Option<(PkKeys, Vec<usize>)> {
    let pk_count = schema.pk_count();
    let mut points = [0u128; PK_LIST_MAX_COLS];
    let mut lists: [Option<&[u128]>; PK_LIST_MAX_COLS] = [None; PK_LIST_MAX_COLS];
    let mut consumed = Vec::new();
    for (k, &col) in schema.pk_cols.iter().enumerate() {
        let tc = schema.columns[col as usize].type_code;
        if let Some(f) = fold(col, terms, tc) {
            if let Some(v) = f.point() {
                points[k] = v;
                consumed.extend(f.consumed);
                continue;
            }
        }
        // A non-point fold on a list column stays residual.
        let (conjunct, keys) = terms.iter().find_map(|t| match &t.pin {
            Pin::In(keys) if t.col as u32 == col => Some((t.conjunct, keys.as_slice())),
            _ => None,
        })?;
        lists[k] = Some(keys);
        consumed.push(conjunct);
    }
    let lists = &lists[..pk_count];
    let slices: Vec<&[u128]> = lists
        .iter()
        .zip(&points)
        .map(|(l, p)| l.unwrap_or(std::slice::from_ref(p)))
        .collect();
    let n = slices.iter().try_fold(1usize, |n, s| n.checked_mul(s.len()))?;
    let longest = slices.iter().map(|s| s.len()).max().unwrap_or(1);
    let stride = schema.pk_stride();
    // Only a cross product is capped: a lone list is keys the statement spelled out.
    if n > longest.max(PkKeys::max_per_request(stride)) {
        return None;
    }
    let mut col = PkColumn::empty_for_schema(schema);
    col.reserve(n);
    let mut at = [0usize; PK_LIST_MAX_COLS];
    let mut tuple = [0u128; PK_LIST_MAX_COLS];
    for _ in 0..n {
        for (k, s) in slices.iter().enumerate() {
            tuple[k] = s[at[k]];
        }
        col.push_natives(schema, &tuple[..pk_count]);
        for k in (0..pk_count).rev() {
            at[k] += 1;
            if at[k] < slices[k].len() {
                break;
            }
            at[k] = 0;
        }
    }
    let keys = PkKeys::from_keys(stride, (0..n).map(|i| col.get_bytes(i)));
    Some((keys, consumed))
}

// ---------------------------------------------------------------------------
// The candidate list
// ---------------------------------------------------------------------------

/// One servable bound for a WHERE, and the conjuncts its walk applies exactly.
pub(crate) struct Candidate {
    pub(crate) bound: ReadBound,
    pub(crate) consumed: Vec<usize>,
}

/// The conjuncts a candidate's walk does not apply, kept as a post-walk filter.
pub(crate) fn residual<'e>(conjuncts: &'e [BoundExpr], consumed: &[usize]) -> Vec<&'e BoundExpr> {
    conjuncts
        .iter()
        .enumerate()
        .filter(|(i, _)| !consumed.contains(i))
        .map(|(_, e)| e)
        .collect()
}

/// A candidate's place in [`candidates`], first to last.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Tier {
    PkKeys,
    /// Bet to unicast under a `CLUSTER BY` shorter than the PK.
    PinnedPkRange,
    UniqueIndexPoint,
    PkRange,
    Index,
    /// Bounds nothing, but consumes conjuncts a residual may not carry (`u128pk > -1`).
    UnboundedPkRange,
}

/// Every bound `conjuncts` admit, best first. A list, because a candidate whose
/// residual does not compile gives way to the next.
pub(crate) fn candidates(conjuncts: &[BoundExpr], schema: &Schema, indexes: &[IndexMeta]) -> Vec<Candidate> {
    let terms: Vec<Term> = conjuncts
        .iter()
        .enumerate()
        .filter_map(|(i, c)| term(c, schema).map(|(col, pin)| Term { conjunct: i, col, pin }))
        .collect();
    let mut out: Vec<(Tier, IndexRank, Candidate)> = Vec::new();

    if let Some((keys, consumed)) = pk_key_set(&terms, schema) {
        out.push((
            Tier::PkKeys,
            IndexRank::default(),
            Candidate { bound: ReadBound::PkSet(keys), consumed },
        ));
    }
    // A range pinning every PK column is the key set's one key.
    let pk_range = bound_column_list(&schema.pk_cols, &terms, schema).filter(|(d, _)| !d.pins_all(schema.pk_count()));
    if let Some((desc, consumed)) = pk_range {
        let tc = schema.columns[schema.pk_cols[0] as usize].type_code;
        let tier = if !desc.pins_none() {
            Tier::PinnedPkRange
        } else if bounded_sides(&desc, tc) > 0 {
            Tier::PkRange
        } else {
            Tier::UnboundedPkRange
        };
        out.push((
            tier,
            IndexRank::default(),
            Candidate {
                bound: ReadBound::PkRange(desc),
                consumed,
            },
        ));
    }
    for meta in indexes {
        let Some((desc, consumed)) = bound_column_list(meta.cols.as_slice(), &terms, schema) else {
            continue;
        };
        let bound = IndexBound { idx_cols: meta.cols, desc };
        let cols = meta.cols.as_slice();
        let tier = if meta.is_unique && desc.pins_all(cols.len()) {
            Tier::UniqueIndexPoint
        } else {
            Tier::Index
        };
        let tc = schema.columns[cols[desc.eq_vals().len()] as usize].type_code;
        let rank = IndexRank {
            pinned: desc.eq_vals().len() + desc.is_point() as usize,
            bounded_sides: bounded_sides(&desc, tc),
            narrower: Reverse(cols.len()),
        };
        let walk = ReadBound::IndexRange { bound, walk: IndexWalk::Optional };
        out.push((tier, rank, Candidate { bound: walk, consumed }));
    }
    // Stable, so a full tie keeps declared order.
    out.sort_by_key(|(tier, rank, _)| (*tier, Reverse(*rank)));
    out.into_iter().map(|(_, _, c)| c).collect()
}

/// How constrained an index walk is within its tier; fields compare in order.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
struct IndexRank {
    pinned: usize,
    bounded_sides: u32,
    narrower: Reverse<usize>,
}

/// How many of a range's cuts sit off its column type's edges. A point scores its
/// pin in [`IndexRank::pinned`] instead.
fn bounded_sides(desc: &RangeDescriptor, tc: TypeCode) -> u32 {
    if desc.is_point() {
        return 0;
    }
    Cut::type_edges(tc).map_or(0, |(lo, hi)| (desc.start != lo) as u32 + (desc.end != hi) as u32)
}

#[cfg(test)]
#[path = "tests/access.rs"]
mod tests;
