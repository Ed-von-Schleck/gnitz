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

mod bind;
mod chain;
mod create;
mod guards;
mod lower;
mod physical;
mod rewrite;

pub(crate) use create::{execute_alter_view, execute_create_view};

use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BExpr};
use chain::{EmitPieces, ViewChain};
use gnitz_core::{ColumnDef, GnitzClient, RangeRel, Schema, TypeCode};
use sqlparser::ast::SetExpr;
use std::rc::Rc;
use std::sync::Arc;

/// The one shared compiler core: bind a query body to `RelExpr`, decorrelate its
/// subqueries, classify join predicates, and lower to circuit pieces for the
/// pre-allocated `view_id`. Every body reaches it — the top-level view body
/// and each CTE body (inside its own `chain.add_segment`) — so the CTE phase and
/// the top path share one pipeline; only the CTE phase threads `chain` ahead of
/// it (to compile CTE segments before the body binds against their aliases).
pub(crate) fn bind_and_lower(
    client: &mut GnitzClient,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    body: &SetExpr,
    view_id: u64,
    bounded: bool,
) -> Result<EmitPieces, GnitzSqlError> {
    let ids = ColIdGen::new();
    let rel = bind::bind_body(client, binder, &ids, body)?;
    let rel = rewrite::decorrelate(rel, &ids)?;
    let rel = rewrite::classify(rel)?;
    lower::lower(client, chain, rel, view_id, bounded)
}

/// Opaque column identity, unique within one `bind_and_lower` invocation, never
/// renumbered. Its `u32` payload is an allocation order, not a layout position —
/// consumers compare ids, never arithmetic on them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ColId(u32);

impl ColId {
    /// The identity-free slot: a physical layout position with no logical column
    /// behind it — a hidden synthetic key (`_join_pk`, `_set_pk`, …), a reduce's
    /// cardinality COUNT, an auto-prepended pass-through PK. Nothing can reference
    /// such a slot (bind mints an id only where a name resolves), so it needs no
    /// identity, only a position. Distinct from every minted id by construction:
    /// `ColIdGen` counts up from 0.
    pub(crate) const NONE: ColId = ColId(u32::MAX);
}

/// Monotonic `ColId` minter, threaded through bind by shared reference. `Get`
/// mints one per schema column at its reference site; `Project` mints one per
/// output `ProjEntry`. Bind-only: lowering assigns positions, never identities, so
/// it pads its layouts with [`ColId::NONE`] instead.
///
/// Interior-mutable because minting is order-dependent but not exclusive: the
/// subquery leaf mints ids from behind the `&self` of `LeafBinder`, while the
/// projection binder around it mints its own. A `&mut` counter would force those
/// two into separate passes over the AST for no reason — the counter is the one
/// thing they genuinely share.
pub(crate) struct ColIdGen(std::cell::Cell<u32>);

impl ColIdGen {
    pub(crate) fn new() -> Self {
        ColIdGen(std::cell::Cell::new(0))
    }
    pub(crate) fn next(&self) -> ColId {
        let id = ColId(self.0.get());
        self.0.set(id.0 + 1);
        id
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

/// The `ColId` of a bare `ColRef` leaf, else `None` (a literal, a computed
/// expression, or an undecorrelated subquery operand).
pub(crate) fn as_col(e: &HirExpr) -> Option<ColId> {
    match e {
        BExpr::ColRef(HirRef::Col(id)) => Some(*id),
        _ => None,
    }
}

/// The physical position of a `ColId` in a layout.
pub(crate) fn slot_of(layout: &[ColId], id: ColId) -> Result<usize, GnitzSqlError> {
    layout
        .iter()
        .position(|c| *c == id)
        .ok_or_else(|| GnitzSqlError::Plan("internal: HIR column reference has no layout slot".into()))
}

/// The physical position of a bare `ColRef` leaf in a layout. Every projection
/// item that reaches a combine emit is a bare column reference — a computed one is
/// cut to a linear segment before it gets there — so anything else is an internal
/// compile error, not a user-facing limit.
pub(crate) fn slot_of_expr(e: &HirExpr, layout: &[ColId]) -> Result<usize, GnitzSqlError> {
    match as_col(e) {
        Some(id) => slot_of(layout, id),
        None => Err(GnitzSqlError::Plan(
            "internal: a combine projection item is not a column reference".into(),
        )),
    }
}

/// Leaf reference for HIR expressions: a resolved column, or a bound subquery
/// awaiting decorrelation. A `Subquery` leaf is minted by bind (an EXISTS/IN/
/// scalar node bound in place) and consumed entirely by the decorrelation rewrite
/// (`hir::rewrite::decorrelate`), which rebuilds it into `Join`/`Reduce` structure
/// and substitutes the leaf with a `Col`/computed expression — none survive to
/// physicalization. `Clone` is required for `bind_structural`'s `R: Clone` bound
/// (the CASE/COALESCE desugars clone sub-exprs); the `Box` keeps the leaf small
/// and breaks the `HirRef → SubqueryRef → HirExpr` type cycle.
#[derive(Clone)]
pub(crate) enum HirRef {
    Col(ColId),
    Subquery(Box<SubqueryRef>),
}

/// A bound subquery leaf, produced by bind and consumed by decorrelation. `rel`
/// is the inner relation already bound to logical structure: a `Filter?(Get)` for
/// EXISTS/IN, or a grouped/global `Reduce` for a scalar aggregate. `correlation`
/// are the mixed-scope WHERE conjuncts (over the outer ∪ inner `ColId` space) that
/// become the decorrelated `Join`'s ON; `in_pair` (present only for the IN shape)
/// carries the `(outer, inner)` equality folded into the ON at decorrelation,
/// together with its combined operand nullability driving the NOT-IN /
/// mark-position 3VL guards.
#[derive(Clone)]
pub(crate) struct SubqueryRef {
    pub kind: SubqueryKind,
    pub rel: Rc<RelExpr>,
    pub correlation: Vec<HirExpr>,
    pub in_pair: Option<InPair>,
}

/// An IN subquery's `outer IN (SELECT inner …)` comparison: the column pair whose
/// equality becomes the decorrelated join key, and whether either operand is
/// nullable (nullability is meaningless without a pair, so it rides here rather
/// than as a separate always-co-set flag).
#[derive(Clone, Copy)]
pub(crate) struct InPair {
    pub outer: ColId,
    pub inner: ColId,
    pub nullable: bool,
}

/// The two decorrelation shapes a bound subquery takes. `Exists` (covering
/// `[NOT] EXISTS` and `[NOT] IN`) decorrelates to a Semi/Anti/Mark join;
/// `negated` folds the `NOT`. `Scalar` (covering a scalar aggregate subquery and
/// the MIN/MAX-normalized range ANY/ALL) decorrelates to a `Reduce` joined to the
/// outer, its value substituted for the leaf.
#[derive(Clone, Copy)]
pub(crate) enum SubqueryKind {
    Exists { negated: bool },
    Scalar,
}

impl SubqueryRef {
    /// The type of the value this subquery contributes where its leaf sits — an
    /// EXISTS/IN test is the `0/1` truth constant (`I64`); a scalar aggregate is
    /// its finalize type (AVG divides to `F64`, every other aggregate keeps its
    /// raw output type). Used to type a computed projection column that embeds the
    /// subquery leaf, before decorrelation substitutes the real expression.
    pub(crate) fn value_type(&self) -> TypeCode {
        match self.kind {
            SubqueryKind::Exists { .. } => TypeCode::I64,
            SubqueryKind::Scalar => match self.scalar_agg() {
                Ok(agg) if agg.func == AggFunc::Avg => TypeCode::F64,
                // COALESCE(COUNT, 0) is I64 — as is a raw COUNT's own out type.
                Ok(agg) => agg.out.def.type_code,
                Err(_) => TypeCode::I64,
            },
        }
    }

    /// Whether this subquery's value is provably never NULL: an EXISTS/IN test is
    /// the `0/1` truth constant, and a COUNT over an empty group is `0`. Both
    /// consumers of the fact derive it here rather than caching it — bind folds
    /// `IS [NOT] NULL` / COALESCE over such a leaf to a constant, and the LEFT-join
    /// decorrelation re-floors a COUNT to `0` to restore it after the null-fill.
    pub(crate) fn never_null(&self) -> bool {
        match self.kind {
            SubqueryKind::Exists { .. } => true,
            SubqueryKind::Scalar => matches!(
                self.scalar_agg().map(|a| a.func),
                Ok(AggFunc::Count | AggFunc::CountNonNull)
            ),
        }
    }

    /// The single aggregate of a scalar subquery — its `rel` is invariantly the
    /// one-aggregate `Reduce` built by `build_scalar_reduce`. The one home for
    /// reading that invariant: every decorrelation site that needs the aggregate
    /// (finalize value, null test, uncorrelated join key) resolves it here.
    pub(crate) fn scalar_agg(&self) -> Result<&HirAgg, GnitzSqlError> {
        match self.rel.as_ref() {
            RelExpr::Reduce { aggs, .. } if !aggs.is_empty() => Ok(&aggs[0]),
            _ => Err(GnitzSqlError::Plan(
                "internal: a scalar subquery's rel is not a one-aggregate Reduce".into(),
            )),
        }
    }
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
        schema: Arc<Schema>,
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
        /// The ON predicate, in whichever of its two forms the pipeline has reached.
        on: JoinOn,
        /// The synthetic `0/1` mark column of a `JoinType::Mark` decorrelation
        /// (`Some` iff `kind == Mark`, minted at decorrelation). Its `ColId` is the
        /// `HirRef::Col(mark_id)` leaf the substituted subquery expression reads,
        /// and it appears in `cols()` after the left side.
        mark: Option<HirCol>,
    },
    /// A GROUP BY / aggregate reduce. `group_cols` are `ColId`s of `input`; its
    /// output carries those same `ColId`s (source names) followed by each
    /// aggregate's raw value column and, for AVG / nullable SUM, its
    /// `COUNT_NON_NULL` companion. This is the **raw** reduce output — the
    /// finalized SELECT shape is the finalize `Project` above.
    ///
    /// The hidden cardinality COUNT the engine gates group existence on is *not*
    /// modelled here: it is a physical emission artifact with no logical identity
    /// (nothing can reference it), appended by `agg::ensure_cardinality_count` at
    /// the layer that owns spec layout.
    Reduce {
        input: Rc<RelExpr>,
        group_cols: Vec<ColId>,
        aggs: Vec<HirAgg>,
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
/// SUM). Both are synthetic staging columns of the reduce's physical output —
/// referenced only by the finalize/HAVING expressions bind builds over them — so
/// [`HirAgg::new`] mints their defs from the aggregate's typing rather than any
/// caller authoring them.
#[derive(Clone)]
pub(crate) struct HirAgg {
    pub func: AggFunc,
    pub arg: Option<ColId>,
    pub out: HirCol,
    pub companion: Option<HirCol>,
}

impl HirAgg {
    /// Mint an aggregate's raw output column (and its `COUNT_NON_NULL` companion
    /// when the shape carries one) from the typing `agg::agg_typing` decided.
    ///
    /// The raw value column's nullability is the shared
    /// `AggFunc::raw_output_nullable` — the same rule the engine's physical reduce
    /// output schema obeys, so the two layers agree on what the reduce can emit.
    /// It is **not** cosmetic here: `null_gate` emits no NULL filter for a NOT NULL
    /// key, so a companion-free MIN/MAX keyed directly by an uncorrelated scalar
    /// subquery would carry its ground row's NULL (zero bytes) into `map_reindex`
    /// ungated, where it OPK-encodes as the real key `0` and spuriously matches
    /// `outer = 0`.
    pub(crate) fn new(
        ids: &ColIdGen,
        func: AggFunc,
        arg: Option<ColId>,
        typing: &crate::agg::AggTyping,
        arg_nullable: bool,
        is_global: bool,
    ) -> Self {
        HirAgg {
            func,
            arg,
            out: HirCol {
                id: ids.next(),
                def: ColumnDef::new(
                    "_agg",
                    typing.ops[0].1,
                    typing.ops[0].0.raw_output_nullable(arg_nullable, is_global),
                ),
            },
            // The companion is COUNT_NON_NULL, whose empty render is a concrete `0`.
            companion: typing.shape.has_count_companion().then(|| HirCol {
                id: ids.next(),
                def: ColumnDef::new("_cnt", TypeCode::I64, false),
            }),
        }
    }
}

/// Which set operation. Copy so the generalized `classify` rebuild can carry it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum SetOpKind {
    Union,
    Intersect,
    Except,
}

/// One output column of a set operation: the paired left/right source `ColId`s,
/// the promoted output column (common type + per-operator nullability), and each
/// side's content-hash widening target.
///
/// A target is `0` when that side already carries the promoted type (hash it as
/// it lies), else the promoted type code — so both sides hash one physical
/// representation. Stamped here, where the pair's source types and the promoted
/// type are all in hand, rather than re-derived at lowering by walking both
/// sides' whole column lists back out of the tree.
#[derive(Clone)]
pub(crate) struct SetOpCol {
    pub left: ColId,
    pub right: ColId,
    pub out: HirCol,
    pub left_target: u8,
    pub right_target: u8,
}

/// Which side(s) of a join survive unmatched — the join kind carried in
/// `RelExpr::Join.kind`, and the driver of both null-fill emission and
/// output-column nullability. The two predicates:
///   - `preserves_left()`  (`Left | Full`): a left row survives unmatched ⇒ the
///     right columns can be NULL, and the null-fill `ν_A = positive_part(A − π_A(inner))`
///     is emitted.
///   - `preserves_right()` (`Right | Full`): a right row survives unmatched ⇒ the
///     left columns can be NULL, and the mirror `ν_B = positive_part(B − π_B(inner))`
///     is emitted.
///
/// `Full` satisfies both. `Inner` neither.
///
/// `Semi`, `Anti`, and `Mark` are the decorrelation-only kinds an EXISTS/IN
/// subquery lowers to (never produced by a FROM-clause JOIN): a semi/anti join
/// keeps/drops each left row by match existence, and a mark join tags each left
/// row with a `0/1` match column. All three preserve neither side under the
/// `preserves_left`/`preserves_right` predicates (they are `matches!`-based, so
/// the new variants get `false` automatically), and carry only the left columns
/// (plus, for `Mark`, the mark column) — see `RelExpr::cols`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Mark,
}

impl JoinType {
    pub(crate) fn preserves_left(self) -> bool {
        matches!(self, JoinType::Left | JoinType::Full)
    }

    pub(crate) fn preserves_right(self) -> bool {
        matches!(self, JoinType::Right | JoinType::Full)
    }

    /// [`Self::preserves_left`] or [`Self::preserves_right`], selected by side —
    /// for the emit loops that walk both sides of a null-fill uniformly and would
    /// otherwise spell the same two-arm dispatch out per loop.
    pub(crate) fn preserves(self, is_left: bool) -> bool {
        if is_left {
            self.preserves_left()
        } else {
            self.preserves_right()
        }
    }
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

/// A join's ON predicate in one of its two forms: the raw conjuncts bind produced
/// (over the left ∪ right `ColId` space), or the [`JoinClass`] the classification
/// rewrite partitioned them into. One field rather than a `Vec` plus an `Option`
/// that are each dead in the other phase, so "classified before lowering" is a
/// `match` the compiler checks instead of an `expect`.
#[derive(Clone)]
pub(crate) enum JoinOn {
    Raw(Vec<HirExpr>),
    Class(JoinClass),
}

impl JoinOn {
    /// The classified form. Lowering runs strictly after the rewrite, so a `Raw`
    /// here is an internal invariant break.
    pub(crate) fn class(&self) -> Result<&JoinClass, GnitzSqlError> {
        match self {
            JoinOn::Class(c) => Ok(c),
            JoinOn::Raw(_) => Err(GnitzSqlError::Plan(
                "internal: join reached lowering unclassified".into(),
            )),
        }
    }
}

impl RelExpr {
    /// A base table or committed/hidden-view source: one fresh `ColId` per
    /// registered schema column, in schema order (so a `ColId`'s env position is
    /// its schema position).
    pub(crate) fn get(ids: &ColIdGen, tid: u64, schema: Arc<Schema>, from_catalog: bool) -> Rc<RelExpr> {
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

    /// A join with its raw (unclassified) ON conjuncts; `mark` is `Some` only for a
    /// `JoinType::Mark` decorrelation.
    pub(crate) fn join(
        left: Rc<RelExpr>,
        right: Rc<RelExpr>,
        kind: JoinType,
        on: Vec<HirExpr>,
        mark: Option<HirCol>,
    ) -> Rc<RelExpr> {
        Rc::new(RelExpr::Join {
            left,
            right,
            kind,
            on: JoinOn::Raw(on),
            mark,
        })
    }

    /// A reduce. `aggs` are already validated + nullability-stamped by bind (which
    /// holds the input env), so this is a plain node build — parity by construction.
    pub(crate) fn reduce(input: Rc<RelExpr>, group_cols: Vec<ColId>, aggs: Vec<HirAgg>) -> Rc<RelExpr> {
        Rc::new(RelExpr::Reduce {
            input,
            group_cols,
            aggs,
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
        ids: &ColIdGen,
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
            let tc = guards::set_op_common_type(l.def.type_code, r.def.type_code).ok_or_else(|| {
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
            let target = |src: TypeCode| if src == tc { 0 } else { tc as u8 };
            out.push(SetOpCol {
                left: l.id,
                right: r.id,
                left_target: target(l.def.type_code),
                right_target: target(r.def.type_code),
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
        /// One child: rebuild via `$ctor` only if `f` returned a different node.
        macro_rules! one {
            ($input:expr, $ctor:expr) => {{
                let n = f($input)?;
                if Rc::ptr_eq(&n, $input) {
                    Rc::clone(rel)
                } else {
                    ($ctor)(n)
                }
            }};
        }
        /// Two children: same, but both must be unchanged to preserve identity.
        macro_rules! two {
            ($l:expr, $r:expr, $ctor:expr) => {{
                let (nl, nr) = (f($l)?, f($r)?);
                if Rc::ptr_eq(&nl, $l) && Rc::ptr_eq(&nr, $r) {
                    Rc::clone(rel)
                } else {
                    ($ctor)(nl, nr)
                }
            }};
        }
        Ok(match rel.as_ref() {
            RelExpr::Get { .. } => Rc::clone(rel),
            RelExpr::Filter { input, preds } => one!(input, |n| RelExpr::filter(n, preds.clone())),
            RelExpr::Project { input, items } => one!(input, |n| RelExpr::project(n, items.clone())),
            RelExpr::Distinct { input } => one!(input, RelExpr::distinct),
            RelExpr::Reduce {
                input,
                group_cols,
                aggs,
            } => one!(input, |n| RelExpr::reduce(n, group_cols.clone(), aggs.clone())),
            RelExpr::Join {
                left,
                right,
                kind,
                on,
                mark,
            } => two!(left, right, |l, r| Rc::new(RelExpr::Join {
                left: l,
                right: r,
                kind: *kind,
                on: on.clone(),
                mark: mark.clone(),
            })),
            RelExpr::SetOp {
                op,
                all,
                left,
                right,
                out,
            } => two!(left, right, |l, r| Rc::new(RelExpr::SetOp {
                op: *op,
                all: *all,
                left: l,
                right: r,
                out: out.clone(),
            })),
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
            RelExpr::Join {
                left,
                right,
                kind,
                mark,
                ..
            } => match kind {
                // A semi/anti join carries only the left (outer) columns; a mark
                // join appends its synthetic `0/1` column after them. The equi/
                // outer joins are left ++ right with the null-providing side widened
                // per `kind` (`combined_payload_coldefs`, applied to the `HirCol`
                // defs — same `ColId`s, widening changes only nullability).
                JoinType::Semi | JoinType::Anti => left.cols(),
                JoinType::Mark => {
                    let mut cols = left.cols();
                    cols.push(mark.clone().expect("Mark join carries a mark column"));
                    cols
                }
                _ => {
                    let mut cols = left.cols();
                    widen_if(cols.iter_mut().map(|c| &mut c.def), kind.preserves_right());
                    let mut rcols = right.cols();
                    widen_if(rcols.iter_mut().map(|c| &mut c.def), kind.preserves_left());
                    cols.extend(rcols);
                    cols
                }
            },
            RelExpr::Reduce {
                input,
                group_cols,
                aggs,
            } => {
                // Group cols keep their source `HirCol`s (from the input); then
                // each aggregate's raw value + companion columns. The physical
                // cardinality COUNT has no logical column and is absent here.
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
                cols
            }
            RelExpr::Distinct { input } => input.cols(),
            RelExpr::SetOp { out, .. } => out.iter().map(|c| c.out.clone()).collect(),
        }
    }
}

/// Widen every def to nullable when `make_nullable` — the per-side outer-join
/// nullability adjustment, and its **one** home. A preserved side forces the
/// *other* side's columns nullable (`kind.preserves_left()` widens the right,
/// `preserves_right()` the left), so the logical output columns ([`RelExpr::cols`])
/// and the physical ones (`lower::join::combined_payload_coldefs`) derive
/// nullability from the same primitive rather than restating the rule. Widening
/// touches only `is_nullable`, so a `ColId` survives it and a resolved reference
/// stays valid.
pub(crate) fn widen_if<'a>(defs: impl Iterator<Item = &'a mut ColumnDef>, make_nullable: bool) {
    if !make_nullable {
        return;
    }
    for d in defs {
        d.is_nullable = true;
    }
}
