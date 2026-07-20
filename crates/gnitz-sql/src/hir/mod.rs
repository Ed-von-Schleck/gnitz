//! The view-body HIR: one IR between the sqlparser AST and the `CircuitBuilder`
//! call sequence, compiled by the `bind → rewrite → physicalize → lower`
//! pipeline. Every relational view body routes through it — linear, join,
//! GROUP BY / aggregate, DISTINCT, and set operations; the subquery shapes
//! follow with `HirRef::Subquery` and the decorrelation rewrites.
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

use crate::error::GnitzSqlError;
use crate::ir::AggFunc;
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

/// The logical relational IR: the linear variants (`Get`/`Filter`/`Project`) and
/// the combine-class ones (`Join`/`Reduce`/`Distinct`/`SetOp`). Only `Get` stores
/// columns (its ids are minted once and must stay stable); every other node's
/// output columns are derived on demand by [`RelExpr::cols`].
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
    /// A GROUP BY / aggregate reduce. `group_cols` are `ColId`s of `input`; its
    /// output carries those same `ColId`s (source names) followed by each
    /// aggregate's raw value column (and, for AVG / nullable SUM, its
    /// `COUNT_NON_NULL` companion) and the optional cardinality `ground`. This is
    /// the **raw** reduce output — the finalized SELECT shape is the finalize
    /// `Project` above.
    Reduce {
        input: Rc<RelExpr>,
        group_cols: Vec<ColId>,
        aggs: Vec<HirAgg>,
        ground: Option<HirCol>,
    },
    /// SELECT DISTINCT: dedup over the input's (visible) columns via a synthetic
    /// content-hash key.
    Distinct {
        input: Rc<RelExpr>,
    },
    /// A set operation (UNION / INTERSECT / EXCEPT, ALL or distinct). `out` pairs
    /// each left/right column positionally with the promoted common type and the
    /// per-operator output nullability.
    SetOp {
        op: SetOpKind,
        all: bool,
        left: Rc<RelExpr>,
        right: Rc<RelExpr>,
        out: Vec<SetOpCol>,
    },
}

/// One raw reduce output. `func`/`arg` are the **logical** aggregate (AVG is
/// never decomposed here — physicalization expands it to `Sum`+`CountNonNull`).
/// `out` is the raw value column; `companion` is the hidden `COUNT_NON_NULL`
/// column present iff the aggregate's null-ness derives from it (AVG, nullable
/// SUM).
#[derive(Clone)]
pub(crate) struct HirAgg {
    pub func: AggFunc,
    pub arg: Option<ColId>,
    pub out: HirCol,
    pub companion: Option<HirCol>,
}

/// Which set operation. Copy so the generalized `classify` rebuild can carry it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum SetOpKind {
    Union,
    Intersect,
    Except,
}

/// One output column of a set operation: the paired left/right source `ColId`s
/// and the promoted output column (common type + per-operator nullability).
#[derive(Clone)]
pub(crate) struct SetOpCol {
    pub left: ColId,
    pub right: ColId,
    pub out: HirCol,
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
#[derive(Clone)]
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

    /// A reduce. `aggs` are already validated + nullability-stamped by bind (which
    /// holds the input env), so this is a plain node build — parity by construction.
    pub(crate) fn reduce(
        input: Rc<RelExpr>,
        group_cols: Vec<ColId>,
        aggs: Vec<HirAgg>,
        ground: Option<HirCol>,
    ) -> Rc<RelExpr> {
        Rc::new(RelExpr::Reduce {
            input,
            group_cols,
            aggs,
            ground,
        })
    }

    /// A DISTINCT over its input's visible columns.
    pub(crate) fn distinct(input: Rc<RelExpr>) -> Rc<RelExpr> {
        Rc::new(RelExpr::Distinct { input })
    }

    /// A set operation. Pairs the two sides' output columns positionally,
    /// promoting each pair to its common type (`set_op_common_type`) and stamping
    /// the per-operator output nullability (Union `l||r`, Intersect `l&&r`, Except
    /// `l`). Rejects an arity or type mismatch — the one home for those guards.
    pub(crate) fn set_op(
        ids: &mut ColIdGen,
        op: SetOpKind,
        all: bool,
        left: Rc<RelExpr>,
        right: Rc<RelExpr>,
    ) -> Result<Rc<RelExpr>, GnitzSqlError> {
        let lcols = left.cols();
        let rcols = right.cols();
        if lcols.len() != rcols.len() {
            return Err(GnitzSqlError::Plan(format!(
                "set operation: column count mismatch ({} vs {})",
                lcols.len(),
                rcols.len()
            )));
        }
        let mut out = Vec::with_capacity(lcols.len());
        for (i, (l, r)) in lcols.iter().zip(&rcols).enumerate() {
            let tc =
                crate::plan::view::set_op::set_op_common_type(l.def.type_code, r.def.type_code).ok_or_else(|| {
                    GnitzSqlError::Plan(format!(
                        "set operation: column {} type mismatch ({:?} vs {:?})",
                        i, l.def.type_code, r.def.type_code
                    ))
                })?;
            // Output name comes from the left side (SQL takes output names from
            // the first query); nullability is operator-specific.
            let is_nullable = match op {
                SetOpKind::Intersect => l.def.is_nullable && r.def.is_nullable,
                SetOpKind::Except => l.def.is_nullable,
                SetOpKind::Union => l.def.is_nullable || r.def.is_nullable,
            };
            let mut def = l.def.clone();
            def.type_code = tc;
            def.is_nullable = is_nullable;
            out.push(SetOpCol {
                left: l.id,
                right: r.id,
                out: HirCol { id: ids.next(), def },
            });
        }
        Ok(Rc::new(RelExpr::SetOp {
            op,
            all,
            left,
            right,
            out,
        }))
    }

    /// Rebuild this node with each child replaced by `f(child)`, preserving `Rc`
    /// identity when every child is unchanged. The one generic spine walk every
    /// HIR→HIR pass drives, so a pass states only what it does to a node, never
    /// how to reassemble each of the seven kinds.
    pub(crate) fn map_children(
        rel: &Rc<RelExpr>,
        f: &mut impl FnMut(&Rc<RelExpr>) -> Result<Rc<RelExpr>, GnitzSqlError>,
    ) -> Result<Rc<RelExpr>, GnitzSqlError> {
        let same = |a: &Rc<RelExpr>, b: &Rc<RelExpr>| Rc::ptr_eq(a, b);
        Ok(match rel.as_ref() {
            RelExpr::Get { .. } => Rc::clone(rel),
            RelExpr::Filter { input, preds } => {
                let n = f(input)?;
                if same(&n, input) {
                    Rc::clone(rel)
                } else {
                    RelExpr::filter(n, preds.clone())
                }
            }
            RelExpr::Project { input, items } => {
                let n = f(input)?;
                if same(&n, input) {
                    Rc::clone(rel)
                } else {
                    RelExpr::project(n, items.clone())
                }
            }
            RelExpr::Distinct { input } => {
                let n = f(input)?;
                if same(&n, input) {
                    Rc::clone(rel)
                } else {
                    RelExpr::distinct(n)
                }
            }
            RelExpr::Reduce {
                input,
                group_cols,
                aggs,
                ground,
            } => {
                let n = f(input)?;
                if same(&n, input) {
                    Rc::clone(rel)
                } else {
                    RelExpr::reduce(n, group_cols.clone(), aggs.clone(), ground.clone())
                }
            }
            RelExpr::Join {
                left,
                right,
                kind,
                on,
                classified,
            } => {
                let (nl, nr) = (f(left)?, f(right)?);
                if same(&nl, left) && same(&nr, right) {
                    Rc::clone(rel)
                } else {
                    Rc::new(RelExpr::Join {
                        left: nl,
                        right: nr,
                        kind: *kind,
                        on: on.clone(),
                        classified: classified.clone(),
                    })
                }
            }
            RelExpr::SetOp {
                op,
                all,
                left,
                right,
                out,
            } => {
                let (nl, nr) = (f(left)?, f(right)?);
                if same(&nl, left) && same(&nr, right) {
                    Rc::clone(rel)
                } else {
                    Rc::new(RelExpr::SetOp {
                        op: *op,
                        all: *all,
                        left: nl,
                        right: nr,
                        out: out.clone(),
                    })
                }
            }
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
            RelExpr::Reduce {
                input,
                group_cols,
                aggs,
                ground,
            } => {
                // Group cols keep their source `HirCol`s (from the input); then
                // the raw agg value + companion columns, then the ground.
                let in_cols = input.cols();
                let mut cols: Vec<HirCol> = group_cols
                    .iter()
                    .map(|id| col_by_id(&in_cols, *id).expect("reduce group col in input").clone())
                    .collect();
                for a in aggs {
                    cols.push(a.out.clone());
                    if let Some(c) = &a.companion {
                        cols.push(c.clone());
                    }
                }
                if let Some(g) = ground {
                    cols.push(g.clone());
                }
                cols
            }
            RelExpr::Distinct { input } => input.cols(),
            RelExpr::SetOp { out, .. } => out.iter().map(|c| c.out.clone()).collect(),
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
