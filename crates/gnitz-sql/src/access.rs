//! Access-path recognition over bound conjuncts, shared by the DML planners and the
//! view compiler: [`candidates`] lists every bound a WHERE admits, best first.
//!
//! An **AST-free leaf** — `ir`, `codec::literal` and the client crates, nothing else.

use crate::codec::literal::{place, Compared, Placed};
use crate::ir::{BExpr, BinOp, BoundExpr};
use gnitz_core::Schema;
use gnitz_expr::{CmpOp, SchemaFacts};
use gnitz_wire::{image_mask, key_image, Cut, KeyRange, PkColList, PkKeys, ReadBound};
use gnitz_wire::{RelIndex, TypeCode, PK_LIST_MAX_COLS};
use std::cmp::Reverse;
use std::ops::RangeInclusive;

/// Classify a bound binary op as `col OP literal`, the `ColRef` on either side,
/// with the literal placed among the column's values. The returned operator
/// always reads `col OP lit`.
fn bound_col_vs_literal(expr: &BoundExpr, schema: &Schema) -> Option<(usize, Placed, BinOp)> {
    let BExpr::BinOp(left, op, right) = expr else {
        return None;
    };
    let placed = |idx: usize, lit: &BoundExpr| place(lit, schema.columns[idx].ty);
    match (left.as_ref(), right.as_ref()) {
        (BExpr::ColRef(idx), lit) => Some((*idx, placed(*idx, lit)?, *op)),
        (lit, BExpr::ColRef(idx)) => Some((*idx, placed(*idx, lit)?, op.converse())),
        _ => None,
    }
}

/// A key column's greatest image; `None` for a type that is no key column.
fn image_max(tc: TypeCode) -> Option<u128> {
    tc.is_pk_eligible().then(|| image_mask(tc.wire_stride()))
}

/// The admitted-image interval that admits nothing.
const NOTHING: RangeInclusive<u128> = RangeInclusive::new(1, 0);

// ---------------------------------------------------------------------------
// One term per conjunct
// ---------------------------------------------------------------------------

/// What one conjunct pins on one column: the key images it admits, or a list of them
/// in key order.
enum Pin {
    Range(RangeInclusive<u128>),
    In(Vec<u128>),
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
        image_max(c.ty.tc)?;
        // A NULL member never makes the conjunct true, and a placement other
        // than `At` names no row.
        let mut keys = Vec::with_capacity(items.len());
        for item in items.iter().filter(|i| !matches!(i, BExpr::LitNull)) {
            if let Placed::At(v) = place(item, c.ty)? {
                keys.push(key_image(c.ty.tc, v));
            }
        }
        if keys.is_empty() {
            return Some((*col, Pin::Range(NOTHING)));
        }
        // The key set crosses the lists in key order. The binder keeps a list as
        // written; a repeat is no second key.
        keys.sort_unstable();
        keys.dedup();
        return Some((*col, Pin::In(keys)));
    }
    let (col, p, op) = bound_col_vs_literal(conjunct, schema)?;
    let cmp = op.as_cmp().filter(|c| *c != CmpOp::Ne)?;
    let tc = schema.columns[col].ty.tc;
    let max = image_max(tc)?;
    let admits = match p.compare(cmp) {
        Compared::Always(true) => 0..=max,
        Compared::Always(false) => NOTHING,
        Compared::Cmp(cmp, v) => {
            let i = key_image(tc, v);
            match cmp {
                CmpOp::Eq => i..=i,
                CmpOp::Ge => i..=max,
                CmpOp::Gt if i < max => i + 1..=max,
                CmpOp::Le => 0..=i,
                CmpOp::Lt if i > 0 => 0..=i - 1,
                _ => NOTHING,
            }
        }
    };
    Some((col, Pin::Range(admits)))
}

// ---------------------------------------------------------------------------
// A column's pins, folded into its interval
// ---------------------------------------------------------------------------

/// A column's `Range` pins intersected, and the conjuncts that fed them. The
/// intersection is exact, so a consumed conjunct needs no re-imposing.
struct Folded {
    admits: RangeInclusive<u128>,
    consumed: Vec<usize>,
}

impl Folded {
    fn point(&self) -> Option<u128> {
        (self.admits.start() == self.admits.end()).then_some(*self.admits.start())
    }
}

/// Every `Range` pin on `col` intersected. `None` when no such pin names `col`.
fn fold(col: u32, terms: &[Term], schema: &Schema) -> Option<Folded> {
    let mut f = Folded {
        admits: 0..=image_max(schema.columns[col as usize].ty.tc)?,
        consumed: Vec::new(),
    };
    for t in terms.iter().filter(|t| t.col as u32 == col) {
        if let Pin::Range(r) = &t.pin {
            f.admits = *f.admits.start().max(r.start())..=*f.admits.end().min(r.end());
            f.consumed.push(t.conjunct);
        }
    }
    (!f.consumed.is_empty()).then_some(f)
}

/// A key list bounded by the WHERE.
struct ListBound {
    range: KeyRange,
    consumed: Vec<usize>,
    /// Leading columns pinned to one value.
    pinned: usize,
    /// How many ends of the range column's interval the WHERE bounds; 0 for a point.
    bounded_sides: u32,
}

/// Bound the key list `cols` — a PK or an index — by its leading point columns and
/// the next column's interval. `None` when nothing bounds it, or when an uncovered
/// trailing column is nullable.
fn bound_column_list(cols: PkColList, terms: &[Term], schema: &Schema) -> Option<ListBound> {
    let mut eq = Vec::new();
    let mut consumed = Vec::new();
    let mut next = None;
    for &col in cols.as_slice() {
        let Some(f) = fold(col, terms, schema) else {
            break;
        };
        match f.point() {
            Some(v) => {
                eq.push(v);
                consumed.extend(f.consumed);
            }
            None => {
                next = Some((col, f));
                break;
            }
        }
    }
    let pinned = eq.len();
    let (range, bounded_sides) = match next {
        Some((col, f)) => {
            let max = image_max(schema.columns[col as usize].ty.tc)?;
            consumed.extend(f.consumed);
            let (&first, &last) = (f.admits.start(), f.admits.end());
            let sides = (first != 0) as u32 + (last != max) as u32;
            (KeyRange::new(cols, &eq, Cut::before(first), Cut::after(last)), sides)
        }
        None => {
            let (&last, init) = eq.split_last()?;
            (KeyRange::point(cols, init, last), 0)
        }
    };
    let covered = range.eq_vals().len() + 1;
    // An index holds no row with a NULL in any of its columns.
    let trailing_nullable = cols.as_slice()[covered..]
        .iter()
        .any(|&c| schema.columns[c as usize].is_nullable);
    (!trailing_nullable).then_some(ListBound { range, consumed, pinned, bounded_sides })
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
    let pk_count = schema.pk_cols.len();
    let mut points = [0u128; PK_LIST_MAX_COLS];
    let mut lists: [Option<&[u128]>; PK_LIST_MAX_COLS] = [None; PK_LIST_MAX_COLS];
    let mut consumed = Vec::new();
    for (k, &col) in schema.pk_cols.iter().enumerate() {
        if let Some(f) = fold(col, terms, schema) {
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
        let key = gnitz_wire::encode_pk_images(schema.pk_cols.iter().zip(&tuple[..pk_count]).map(|(&c, &v)| {
            let tc = schema.columns[c as usize].ty.tc;
            (tc, tc, v)
        }));
        bytes.extend_from_slice(key.pk_bytes());
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
pub(crate) fn candidates(conjuncts: &[BoundExpr], schema: &Schema, indexes: &[RelIndex]) -> Vec<Candidate> {
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
    let pk_range = bound_column_list(PkColList::from_slice(&schema.pk_cols), &terms, schema)
        .filter(|b| b.pinned < schema.pk_cols.len());
    if let Some(b) = pk_range {
        let tier = if b.pinned > 0 {
            Tier::PinnedPkRange
        } else if b.bounded_sides > 0 {
            Tier::PkRange
        } else {
            Tier::UnboundedPkRange
        };
        out.push((
            tier,
            IndexRank::default(),
            Candidate {
                bound: ReadBound::Range(b.range),
                consumed: b.consumed,
            },
        ));
    }
    for meta in indexes {
        let Some(b) = bound_column_list(meta.cols, &terms, schema) else {
            continue;
        };
        let n_cols = meta.cols.as_slice().len();
        let tier = if meta.is_unique && b.pinned == n_cols {
            Tier::UniqueIndexPoint
        } else {
            Tier::Index
        };
        let rank = IndexRank {
            pinned: b.pinned,
            bounded_sides: b.bounded_sides,
            narrower: Reverse(n_cols),
        };
        out.push((
            tier,
            rank,
            Candidate {
                bound: ReadBound::Range(b.range),
                consumed: b.consumed,
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

#[cfg(test)]
#[path = "tests/access.rs"]
mod tests;
