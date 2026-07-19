//! The view-body HIR: one IR between the sqlparser AST and the `CircuitBuilder`
//! call sequence, compiled by the `bind → rewrite → physicalize → lower`
//! pipeline. The linear (`Simple`) and join (`Join`) view shapes route through
//! it; the remaining shapes follow with their own nodes.
//!
//! The discipline: a column is an **opaque `ColId`** minted once at bind and
//! never renumbered, so a resolved reference survives every structural rewrite
//! (predicate classification, the segment-cut and source-collision rules). The
//! logical IR carries no layout — physical positions are assigned by one
//! positional pass (`physical.rs`) at lowering, the single home of the
//! PK-front convention and the `HirRef → ColRef(position)` substitution.

pub(crate) mod bind;
pub(crate) mod lower;
pub(crate) mod physical;
pub(crate) mod rewrite;

use crate::plan::view::join::JoinType;
use gnitz_core::{ColumnDef, RangeRel, Schema, TypeCode};
use std::rc::Rc;

/// Opaque column identity, unique within one `bind_query` invocation, never
/// renumbered. Its `u32` payload is an allocation order, not a layout position —
/// consumers compare ids, never arithmetic on them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ColId(u32);

/// Monotonic `ColId` minter, threaded through bind as a `&mut` field on the bind
/// context. `Get` mints one per schema column at its reference site; `Project`
/// mints one per output `ProjEntry`.
pub(crate) struct ColIdGen(u32);

impl ColIdGen {
    pub(crate) fn new() -> Self {
        ColIdGen(0)
    }
    pub(crate) fn next(&mut self) -> ColId {
        let id = ColId(self.0);
        self.0 += 1;
        id
    }
    /// `n` fresh ids for a physical layout's hidden key slots (`_join_pk`,
    /// `_pair_pk`, …) — placeholders referenced by nothing, minted so a layout
    /// position exists for every physical column.
    pub(crate) fn placeholders(&mut self, n: usize) -> Vec<ColId> {
        (0..n).map(|_| self.next()).collect()
    }
}

/// A column's logical identity + type. No layout.
#[derive(Clone)]
pub(crate) struct HirCol {
    pub id: ColId,
    pub def: ColumnDef,
}

/// The `HirCol` a `ColId` names within a column list, `None` when absent.
pub(crate) fn col_by_id(cols: &[HirCol], id: ColId) -> Option<&HirCol> {
    cols.iter().find(|c| c.id == id)
}

/// The physical position of a `ColId` in a layout.
pub(crate) fn slot_of(layout: &[ColId], id: ColId) -> Result<usize, crate::error::GnitzSqlError> {
    layout
        .iter()
        .position(|c| *c == id)
        .ok_or_else(|| crate::error::GnitzSqlError::Plan("internal: HIR column reference has no layout slot".into()))
}

/// Leaf reference for HIR expressions. Only `Col` for now; `Subquery` arrives
/// with subquery decorrelation (additive — one variant). `Clone` is required for
/// `bind_structural`'s `R: Clone` bound (the CASE/COALESCE desugars clone
/// sub-exprs).
#[derive(Clone)]
pub(crate) enum HirRef {
    Col(ColId),
}

/// The bound-expression IR hosted on the HIR leaf. Structurally identical to the
/// runtime `BoundExpr`; only the leaf reference differs (`ColId` vs `usize`).
pub(crate) type HirExpr = crate::ir::BExpr<HirRef>;

/// One output column of a projection: its computed expression and its minted
/// output identity + type.
#[derive(Clone)]
pub(crate) struct ProjEntry {
    pub expr: HirExpr,
    pub out: HirCol,
}

/// The logical relational IR. Linear variants (`Get`/`Filter`/`Project`) plus
/// `Join`; `Reduce`/`Distinct`/`SetOp` arrive with their own lowering. Only
/// `Get` stores columns (its ids are minted once and must stay stable); every
/// other node's output columns are derived on demand by [`RelExpr::cols`].
pub(crate) enum RelExpr {
    Get {
        tid: u64,
        schema: Rc<Schema>,
        cols: Vec<HirCol>,
        from_catalog: bool,
    },
    Filter {
        input: Rc<RelExpr>,
        preds: Vec<HirExpr>,
    },
    Project {
        input: Rc<RelExpr>,
        items: Vec<ProjEntry>,
    },
    Join {
        left: Rc<RelExpr>,
        right: Rc<RelExpr>,
        kind: JoinType,
        /// Raw ON conjuncts over the left ∪ right `ColId` space; partitioned into
        /// `classified` by the predicate-classification rewrite (`None` before it).
        on: Vec<HirExpr>,
        classified: Option<JoinClass>,
    },
}

/// An equality join-key pair: the two `ColId`s and their promoted common type.
#[derive(Clone, Copy)]
pub(crate) struct EqPair {
    pub left: ColId,
    pub right: ColId,
    pub tc: TypeCode,
}

/// The one range/band conjunct of a join: the two `ColId`s, the canonicalized
/// relation (left-to-right), and the promoted common type.
#[derive(Clone, Copy)]
pub(crate) struct HirRange {
    pub left: ColId,
    pub right: ColId,
    pub op: RangeRel,
    pub tc: TypeCode,
}

/// A join's ON predicate classified into equality pairs, an optional range
/// conjunct, and the residual (non-key) conjuncts — filled by the rewrite.
pub(crate) struct JoinClass {
    pub eq: Vec<EqPair>,
    pub range: Option<HirRange>,
    pub residual: Vec<HirExpr>,
}

impl RelExpr {
    /// A base table or committed/hidden-view source: one fresh `ColId` per
    /// registered schema column, in schema order (so a `ColId`'s env position is
    /// its schema position).
    pub(crate) fn get(ids: &mut ColIdGen, tid: u64, schema: Rc<Schema>, from_catalog: bool) -> Rc<RelExpr> {
        let cols = schema
            .columns
            .iter()
            .map(|c| HirCol {
                id: ids.next(),
                def: c.clone(),
            })
            .collect();
        Rc::new(RelExpr::Get {
            tid,
            schema,
            cols,
            from_catalog,
        })
    }

    /// A linear filter: pass-through columns (same ids, same order as `input`).
    pub(crate) fn filter(input: Rc<RelExpr>, preds: Vec<HirExpr>) -> Rc<RelExpr> {
        Rc::new(RelExpr::Filter { input, preds })
    }

    /// A projection: its output columns are the `ProjEntry.out`s (SELECT order —
    /// `place_pk_front` is physical, applied at lowering, not here).
    pub(crate) fn project(input: Rc<RelExpr>, items: Vec<ProjEntry>) -> Rc<RelExpr> {
        Rc::new(RelExpr::Project { input, items })
    }

    /// A join. `classified` is `None` until the predicate rewrite fills it.
    pub(crate) fn join(left: Rc<RelExpr>, right: Rc<RelExpr>, kind: JoinType, on: Vec<HirExpr>) -> Rc<RelExpr> {
        Rc::new(RelExpr::Join {
            left,
            right,
            kind,
            on,
            classified: None,
        })
    }

    /// The node's output columns. `Get` returns its minted cols; `Filter` passes
    /// through; `Project` is its `ProjEntry.out`s; `Join` is left ++ right with
    /// the null-providing side widened per `kind` (right nullable for Left, left
    /// for Right, both for Full) — exactly `combined_payload_coldefs`, applied to
    /// the `HirCol` defs (same `ColId`s — widening changes only nullability).
    pub(crate) fn cols(&self) -> Vec<HirCol> {
        match self {
            RelExpr::Get { cols, .. } => cols.clone(),
            RelExpr::Filter { input, .. } => input.cols(),
            RelExpr::Project { items, .. } => items.iter().map(|e| e.out.clone()).collect(),
            RelExpr::Join { left, right, kind, .. } => {
                let mut cols = widen_cols(left.cols(), kind.preserves_right());
                cols.extend(widen_cols(right.cols(), kind.preserves_left()));
                cols
            }
        }
    }
}

/// Widen every def to nullable when `make_nullable` — the per-side outer-join
/// nullability adjustment (`combined_payload_coldefs`), keeping each `ColId` so a
/// resolved reference survives the widening.
fn widen_cols(mut cols: Vec<HirCol>, make_nullable: bool) -> Vec<HirCol> {
    if make_nullable {
        for c in &mut cols {
            c.def.is_nullable = true;
        }
    }
    cols
}
