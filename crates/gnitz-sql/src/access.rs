//! Access-path recognition over bound conjuncts, shared by the DML planners and the
//! view compiler: [`candidates`] lists every bound a WHERE admits, best first.
//!
//! An **AST-free leaf** — `ir`, `codec::literal` and `gnitz-core`, nothing else.

use crate::codec::literal::{place, Placed};
use crate::ir::{BExpr, BinOp, BoundExpr};
use gnitz_core::{Cut, IndexMeta, RangeDescriptor, Schema, TypeCode, PK_LIST_MAX_COLS};
use gnitz_expr::SchemaFacts;
use gnitz_wire::{key_image, IndexBound, PkKeys, ReadBound};
use std::cmp::Reverse;

/// Classify a bound binary op as `col OP literal`, the `ColRef` on either side,
/// with the literal placed among the column's values. The returned operator
/// always reads `col OP lit`.
fn bound_col_vs_literal(expr: &BoundExpr, schema: &Schema) -> Option<(usize, Placed, BinOp)> {
    let BExpr::BinOp(left, op, right) = expr else {
        return None;
    };
    let placed = |idx: usize, lit: &BoundExpr| place(lit, schema.columns[idx].ty());
    match (left.as_ref(), right.as_ref()) {
        (BExpr::ColRef(idx), lit) => Some((*idx, placed(*idx, lit)?, *op)),
        (lit, BExpr::ColRef(idx)) => Some((*idx, placed(*idx, lit)?, op.converse())),
        _ => None,
    }
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
        if !schema.is_pk_col(*col) {
            return None;
        }
        let c = &schema.columns[*col];
        // A NULL member never makes the conjunct true, and a placement other
        // than `At` names no row.
        let mut keys = Vec::with_capacity(items.len());
        for item in items.iter().filter(|i| !matches!(i, BExpr::LitNull)) {
            if let Placed::At(v) = place(item, c.ty())? {
                keys.push(v);
            }
        }
        if keys.is_empty() {
            return Some((*col, empty_pin(c.type_code)?));
        }
        // In key order, so the key set crosses the lists already sorted. The binder
        // keeps a list as written; a repeat is no second key.
        keys.sort_unstable_by_key(|&v| key_image(c.type_code as u8, v));
        keys.dedup();
        return Some((*col, Pin::In(keys)));
    }
    let (col, p, op) = bound_col_vs_literal(conjunct, schema)?;
    let tc = schema.columns[col].type_code;
    let pin = match (op, p) {
        (BinOp::Eq, Placed::At(v)) => Pin::Eq(v),
        (BinOp::Eq, _) => empty_pin(tc)?,
        (BinOp::Gt, p) => Pin::Start(range_cut(p, tc, Cut::After)?),
        (BinOp::Ge, p) => Pin::Start(range_cut(p, tc, Cut::Before)?),
        (BinOp::Lt, p) => Pin::End(range_cut(p, tc, Cut::Before)?),
        (BinOp::Le, p) => Pin::End(range_cut(p, tc, Cut::After)?),
        _ => return None,
    };
    Some((col, pin))
}

/// The pin that admits no value: an interval ending before the type's first.
fn empty_pin(tc: TypeCode) -> Option<Pin> {
    Some(Pin::End(Cut::type_edges(tc)?.0))
}

/// A range-end literal's cut. A literal between two values cuts after the lower
/// one whichever the operator: `x > lit` is `x ≥ lo + 1` and `x < lit` is
/// `x ≤ lo`. `None` for a type with no ordered range.
fn range_cut(p: Placed, tc: TypeCode, mk: fn(u128) -> Cut) -> Option<Cut> {
    let (below, above) = Cut::type_edges(tc)?;
    Some(match p {
        Placed::At(v) => mk(v),
        Placed::Between { lo, .. } => Cut::After(lo),
        Placed::Below { .. } => below,
        Placed::Above { .. } => above,
    })
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

/// Cut order on a column of type `tc`: value in key order, then `Before` below `After`.
fn cut_key(c: Cut, tc: TypeCode) -> (u128, bool) {
    (key_image(tc as u8, c.value()), matches!(c, Cut::After(_)))
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

/// The size past which a crossed key set is served as a predicate scan instead: a
/// product of lists grows multiplicatively past anything the statement spelled out.
const MAX_CROSS_PRODUCT_KEYS: usize = 65_536;

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
    let slices: Vec<&[u128]> = lists[..pk_count]
        .iter()
        .zip(&points)
        .map(|(l, p)| l.unwrap_or(std::slice::from_ref(p)))
        .collect();
    let n = slices.iter().try_fold(1usize, |n, s| n.checked_mul(s.len()))?;
    let longest = slices.iter().map(|s| s.len()).max().unwrap_or(1);
    // Only a cross product is capped: a lone list is keys the statement spelled out.
    if n > longest.max(MAX_CROSS_PRODUCT_KEYS) {
        return None;
    }
    // Every list is in key order, so varying the last position fastest emits
    // strictly ascending tuples.
    let stride = schema.pk_stride();
    let mut bytes = Vec::with_capacity(n * stride);
    let mut at = [0usize; PK_LIST_MAX_COLS];
    let mut tuple = [0u128; PK_LIST_MAX_COLS];
    for _ in 0..n {
        for (k, s) in slices.iter().enumerate() {
            tuple[k] = s[at[k]];
        }
        bytes.extend_from_slice(schema.opk_key_cols(&tuple[..pk_count]).pk_bytes());
        for k in (0..pk_count).rev() {
            at[k] += 1;
            if at[k] < slices[k].len() {
                break;
            }
            at[k] = 0;
        }
    }
    Some((PkKeys::from_sorted(stride, bytes), consumed))
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
        out.push((
            tier,
            rank,
            Candidate {
                bound: ReadBound::IndexRange(bound),
                consumed,
            },
        ));
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
