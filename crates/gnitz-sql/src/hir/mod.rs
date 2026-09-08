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
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod bind;
mod chain;
mod create;
mod guards;
mod lower;
mod physical;
mod rewrite;
mod window;

pub(crate) use create::{execute_alter_view, execute_create_view};
pub use create::{plan_view, PlannedChain, ViewPlan};

use crate::bind::Binder;
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BExpr};
use chain::{EmitPieces, ViewChain};
use gnitz_core::{CatalogSnapshot, ColType, ColumnDef, RangeRel, RelDescriptor, Schema, TypeCode};
use std::rc::Rc;
use std::sync::Arc;

/// The one compiler core: bind a query — its CTEs, then its body — to one
/// `RelExpr` tree, decorrelate its subqueries, classify join predicates, and
/// lower to circuit pieces. A CTE is a shared subtree of that one tree, read
/// through an `Alias` wherever it is named, so nothing is compiled before the
/// tree is whole; the lowering decides what each shared subtree becomes.
pub(crate) fn bind_and_lower(
    cat: &CatalogSnapshot,
    binder: &mut Binder<'_>,
    chain: &mut ViewChain,
    query: &sqlparser::ast::Query,
    bounded: bool,
    surface: bind::ViewSurface,
) -> Result<EmitPieces, GnitzSqlError> {
    let ids = ColIdGen::new();
    let mut cx = bind::BindCx::new(cat, binder, &ids, surface);
    bind::bind_ctes(&mut cx, query)?;
    let rel = bind::bind_body(&mut cx, query.body.as_ref())?;
    let rel = rewrite::decorrelate(rel, &ids)?;
    let rel = rewrite::classify(rel)?;
    lower::lower(chain, rel, bounded)
}

/// The ad-hoc read path's entry to the same core: bind a single-relation grouped
/// or `SELECT DISTINCT` body and lower it to fold pieces instead of to a
/// circuit. One binder, two sinks — which is what makes `SELECT … GROUP BY …`
/// and `CREATE VIEW AS SELECT … GROUP BY …` accept the same statements and
/// compute them the same way, DISTINCT included.
///
/// Returns the fold's pieces and the finalize item each key of `order_exprs`
/// sorts on.
///
/// There is no decorrelate/classify step: the ad-hoc router rejects every
/// subquery and join shape before a body reaches here, so the bound tree is
/// already one of the two shapes `lower_fold` expects.
pub(crate) fn bind_and_lower_fold(
    select: &sqlparser::ast::Select,
    schema: &Arc<Schema>,
    alias: &str,
    order_exprs: &[&sqlparser::ast::Expr],
) -> Result<(lower::fold::FoldPieces, Vec<usize>), GnitzSqlError> {
    let ids = ColIdGen::new();
    let (rel, order_cols) = if select.distinct.is_some() {
        bind::bind_adhoc_distinct(&ids, select, Arc::clone(schema), alias, order_exprs)?
    } else {
        bind::bind_adhoc_grouped(&ids, select, Arc::clone(schema), alias, order_exprs)?
    };
    Ok((lower::fold::lower_fold(&rel)?, order_cols))
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

impl HirCol {
    pub(crate) fn new(id: ColId, def: ColumnDef) -> HirCol {
        HirCol { id, def }
    }
}

/// The `HirCol` a `ColId` names within a column list, `None` when absent.
pub(crate) fn col_by_id(cols: &[HirCol], id: ColId) -> Option<&HirCol> {
    cols.iter().find(|c| c.id == id)
}

/// The `HirCol` a `ColId` names, panicking when absent. A HIR `ColRef` always
/// references a column of the list it is resolved against (bind mints them
/// there), so absence is an internal compile error, not a user-facing one.
pub(crate) fn hircol_of(cols: &[HirCol], id: ColId) -> &HirCol {
    col_by_id(cols, id).expect("HIR ColRef references a column of its list")
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
        .ok_or_else(|| GnitzSqlError::Internal("HIR column reference has no layout slot".into()))
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

/// Two leaves are the same reference iff they name the same column. A subquery
/// leaf never matches: a grouped body — the only place expressions are compared —
/// rejects subqueries, and never-equal is the safe direction regardless.
impl PartialEq for HirRef {
    fn eq(&self, other: &Self) -> bool {
        matches!((self, other), (HirRef::Col(a), HirRef::Col(b)) if a == b)
    }
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
    pub(crate) fn value_type(&self) -> ColType {
        match self.kind {
            SubqueryKind::Exists { .. } => ColType::of(TypeCode::I64),
            SubqueryKind::Scalar => match self.scalar_agg() {
                Ok(agg) => agg.view_type(),
                Err(_) => ColType::of(TypeCode::I64),
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
            SubqueryKind::Scalar => matches!(self.scalar_agg().map(|a| a.func), Ok(AggFunc::Count)),
        }
    }

    /// The single aggregate of a scalar subquery — its `rel` is invariantly the
    /// one-aggregate `Reduce` `hir::bind`'s `scalar_leaf` builds. The one home for
    /// reading that invariant: every decorrelation site that needs the aggregate
    /// (finalize value, null test, uncorrelated join key) resolves it here.
    pub(crate) fn scalar_agg(&self) -> Result<&HirAgg, GnitzSqlError> {
        match self.rel.as_ref() {
            RelExpr::Reduce { aggs, .. } if !aggs.is_empty() => Ok(&aggs[0]),
            _ => Err(GnitzSqlError::Internal(
                "a scalar subquery's rel is not a one-aggregate Reduce".into(),
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
        source: GetSource,
        schema: Arc<Schema>,
        cols: Vec<HirCol>,
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
    },
    /// A GROUP BY / aggregate reduce over `pre` applied to `input`. Its output is
    /// the group columns, then each aggregate's raw value and (AVG / nullable SUM)
    /// its `COUNT_NON_NULL` companion — the **raw** shape the finalize `Project`
    /// above renders.
    ///
    /// The hidden cardinality COUNT the engine gates group existence on is *not*
    /// modelled here: it is a physical emission artifact with no logical identity
    /// (nothing can reference it), appended by `agg::ensure_cardinality_count` at
    /// the layer that owns spec layout.
    Reduce {
        input: Rc<RelExpr>,
        /// The pre-map bind inserted so the reduce can group by, or aggregate, an
        /// expression (`GROUP BY a + b`): `input`'s columns passed through, then
        /// one item per materialized expression. Empty when there is none.
        pre: Vec<ProjEntry>,
        /// `ColId`s of the reduce's input — `pre`'s outputs, or `input`'s own.
        group_cols: Vec<ColId>,
        aggs: Vec<HirAgg>,
    },
    /// SELECT DISTINCT — and the set a DISTINCT aggregate reduces over: dedup over
    /// every one of the input's columns, hidden ones included, via a synthetic
    /// content-hash key.
    Distinct {
        input: Rc<RelExpr>,
    },
    /// The same relation read again under fresh column identities — the HIR of
    /// a second reference to one *subtree*, as a second mention of a CTE or the
    /// window desugar's repeated reads of its own input are. (A second `FROM`
    /// mention of a base table mints a fresh `Get` instead; there is no subtree
    /// to share.) `cols` is parallel to `input.cols()` and carries the same defs
    /// under new ids, so two aliases of one shared `input` are distinguishable
    /// by id wherever they meet (a self-join's two sides).
    /// The lowering resolves it to its input's delta source — a table in place,
    /// anything else cut once and shared through the cut memo — under the
    /// alias's ids; it never materializes anything of its own.
    Alias {
        input: Rc<RelExpr>,
        cols: Vec<HirCol>,
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

/// Where a `Get`'s rows come from: a catalog relation, or the ad-hoc read's
/// source, which names its relation in the `ReadSpec` instead — so it has no
/// `tid` and no descriptor here, and never reaches the circuit lowering.
pub(crate) enum GetSource {
    Catalog { tid: u64, desc: Arc<RelDescriptor> },
    AdHoc,
}

impl GetSource {
    /// A stream holds no rows, so nothing read from one has a row identity.
    fn is_stream(&self) -> bool {
        matches!(self, GetSource::Catalog { desc, .. } if desc.class == gnitz_core::RelClass::Stream)
    }
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
    /// The raw column's nullability is the shared `AggFunc::raw_output_nullable`,
    /// so the planner and the engine's reduce output schema agree on what the
    /// reduce can emit: declared NOT NULL, `null_gate` would leave it ungated.
    pub(crate) fn new(
        ids: &ColIdGen,
        func: AggFunc,
        arg: Option<ColId>,
        env: &[HirCol],
        is_global: bool,
    ) -> Result<Self, GnitzSqlError> {
        let arg_def = arg.map(|id| &hircol_of(env, id).def);
        let typing = crate::agg::agg_typing(func, arg_def)?;
        let arg_nullable = arg_def.map(|d| d.is_nullable).unwrap_or(false);
        Ok(HirAgg {
            func,
            arg,
            // Hidden: a raw reduce-output column is addressed by `ColId`, never
            // by name — the finalize composite is built for it, not looked up.
            out: HirCol::new(
                ids.next(),
                ColumnDef::typed(
                    "_agg",
                    typing.ops[0].1,
                    typing.ops[0].0.raw_output_nullable(arg_nullable, is_global),
                )
                .hidden(),
            ),
            // The companion is COUNT_NON_NULL, whose empty render is a concrete `0`.
            companion: typing
                .shape
                .has_count_companion()
                .then(|| HirCol::new(ids.next(), ColumnDef::new("_cnt", TypeCode::I64, false).hidden())),
        })
    }

    /// The finalize composite over this aggregate's raw reduce column(s) — the one
    /// home, shared with `hir::rewrite`'s scalar-subquery substitution and the
    /// grouped binder's SELECT / HAVING leaf.
    pub(crate) fn finalize(&self) -> HirExpr {
        crate::agg::finalize_agg_bexpr(
            HirRef::Col(self.out.id),
            self.companion.as_ref().map(|c| HirRef::Col(c.id)),
            self.func,
        )
    }

    /// The type this aggregate renders where a view reads it.
    pub(crate) fn view_type(&self) -> ColType {
        crate::agg::agg_view_type(self.func, self.out.def.ty())
    }

    /// A companion carries the null-ness (the finalize renders NULL by
    /// div-by-zero); otherwise the raw column's own.
    pub(crate) fn view_nullable(&self) -> bool {
        self.companion.is_some() || self.out.def.is_nullable
    }

    /// This aggregate as a view-facing value column: what computes it, and the
    /// type and nullability that value is declared with.
    pub(crate) fn as_value(&self) -> (HirExpr, ColType, bool) {
        (self.finalize(), self.view_type(), self.view_nullable())
    }
}

/// Split an optional `Filter` off a node, returning its conjuncts (empty when
/// absent) and the source below it. Hands back the source as the `Rc` every caller
/// holds anyway — a cut/exists path needs to clone it, and a `&RelExpr` deref-coerces
/// for the rest — so this is the one home for the peel.
pub(crate) fn split_filter(input: &Rc<RelExpr>) -> (&[HirExpr], &Rc<RelExpr>) {
    match input.as_ref() {
        RelExpr::Filter { input, preds } => (preds, input),
        _ => (&[], input),
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
/// A target is `None` when that side already carries the promoted type (hash it
/// as it lies), else the promoted type — so both sides hash one physical
/// representation. Stamped here rather than at lowering, where `lower_sides`
/// would have to hand two more values back.
#[derive(Clone)]
pub(crate) struct SetOpCol {
    pub left: ColId,
    pub right: ColId,
    pub out: HirCol,
    pub left_target: Option<TypeCode>,
    pub right_target: Option<TypeCode>,
}

/// Which side(s) of a join survive unmatched: the driver of null-fill emission
/// and of output-column nullability, read through the predicates below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    /// EXISTS/IN: keep each left row by match existence.
    Semi,
    /// NOT EXISTS / NOT IN: drop each left row by match existence.
    Anti,
    /// EXISTS/IN in a mark position, carrying the `ColId` of the synthetic `0/1`
    /// column tagging each left row — the leaf the substituted subquery reads.
    Mark(ColId),
}

impl JoinType {
    /// The mark column a [`JoinType::Mark`] appends after its left columns.
    /// Hidden: nothing but the substituted subquery expression reaches it, and it
    /// is addressed by id, never by name.
    pub(crate) fn mark_col(id: ColId) -> HirCol {
        HirCol::new(id, ColumnDef::new("_mark", TypeCode::I64, false).hidden())
    }

    /// A left row survives unmatched ⇒ the right columns can be NULL, and
    /// `ν_A = positive_part(A − π_A(inner))` is emitted.
    pub(crate) fn preserves_left(self) -> bool {
        matches!(self, JoinType::Left | JoinType::Full)
    }

    /// The mirror: a right row survives unmatched, and `ν_B` is emitted.
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

    /// Widen the two sides of one join step for its null semantics — a preserved
    /// side forces the *other* side's columns nullable. The one home for that
    /// cross, and it moves only `is_nullable`, so a resolved reference survives.
    pub(crate) fn widen_sides<'a>(
        self,
        left: impl Iterator<Item = &'a mut ColumnDef>,
        right: impl Iterator<Item = &'a mut ColumnDef>,
    ) {
        if self.preserves_right() {
            left.for_each(|d| d.is_nullable = true);
        }
        if self.preserves_left() {
            right.for_each(|d| d.is_nullable = true);
        }
    }

    /// The three decorrelation-only kinds an EXISTS/IN subquery lowers to, never
    /// produced by a FROM-clause JOIN: they preserve neither side and carry only
    /// the left (outer) columns.
    pub(crate) fn is_decorrelated(self) -> bool {
        matches!(self, JoinType::Semi | JoinType::Anti | JoinType::Mark(_))
    }

    /// Whether this side gets a **ν** — the unmatched set
    /// `positive_part(P_all − π_P(inner))`. Wider than [`Self::preserves`], which
    /// answers "emit an outer null-fill branch": `Semi`/`Anti`/`Mark` preserve
    /// neither side yet all three build a ν over their left side to decide match
    /// existence. The reindex keep set protects exactly the ν operands, so it asks
    /// this rather than `preserves`.
    pub(crate) fn has_nu(self, is_left: bool) -> bool {
        self.preserves(is_left) || (is_left && self.is_decorrelated())
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

/// A join's predicate classified into equality pairs, an optional range conjunct,
/// and the residual (non-key) conjuncts — filled by the rewrite. Its source is
/// the join's own ON / USING / NATURAL keys plus, for an INNER join, the WHERE
/// conjuncts above it that span its two sides.
#[derive(Clone)]
pub(crate) struct JoinClass {
    pub eq: Vec<EqPair>,
    pub range: Option<HirRange>,
    pub residual: Vec<HirExpr>,
}

/// Which physical join a classified ON calls for. An enum rather than field probes
/// at each site: a pure range join has an empty `eq` too, so probing `eq` first
/// would read it as keyless and drop the range predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinShape {
    Equi,
    Range,
    Cross,
}

impl JoinClass {
    /// The key columns this ON names on one side — what that side's reindex reads,
    /// and so what a cut input must keep however narrow the demand above it is.
    pub(crate) fn key_cols(&self, is_left: bool) -> impl Iterator<Item = ColId> + '_ {
        let pick = move |(l, r)| if is_left { l } else { r };
        self.eq
            .iter()
            .map(move |p| pick((p.left, p.right)))
            .chain(self.range.iter().map(move |r| pick((r.left, r.right))))
    }

    pub(crate) fn shape(&self) -> JoinShape {
        match (self.range.is_some(), self.eq.is_empty()) {
            (true, _) => JoinShape::Range,
            (false, true) => JoinShape::Cross,
            (false, false) => JoinShape::Equi,
        }
    }
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
            JoinOn::Raw(_) => Err(GnitzSqlError::Internal("join reached lowering unclassified".into())),
        }
    }
}

impl RelExpr {
    /// A base table or committed/hidden-view source: one fresh `ColId` per
    /// registered schema column, in schema order (so a `ColId`'s env position is
    /// its schema position).
    pub(crate) fn get(ids: &ColIdGen, tid: u64, schema: Arc<Schema>, desc: Arc<RelDescriptor>) -> Rc<RelExpr> {
        Self::get_of(ids, GetSource::Catalog { tid, desc }, schema)
    }

    /// The ad-hoc read's source, under the same column minting.
    pub(crate) fn get_adhoc(ids: &ColIdGen, schema: Arc<Schema>) -> Rc<RelExpr> {
        Self::get_of(ids, GetSource::AdHoc, schema)
    }

    fn get_of(ids: &ColIdGen, source: GetSource, schema: Arc<Schema>) -> Rc<RelExpr> {
        let cols = schema
            .columns
            .iter()
            .map(|c| HirCol::new(ids.next(), c.clone()))
            .collect();
        Rc::new(RelExpr::Get { source, schema, cols })
    }

    /// A linear filter: pass-through columns (same ids, same order as `input`).
    pub(crate) fn filter(input: Rc<RelExpr>, preds: Vec<HirExpr>) -> Rc<RelExpr> {
        Rc::new(RelExpr::Filter { input, preds })
    }

    /// A projection item passing one column through to itself, keeping its
    /// identity.
    pub(crate) fn passthrough_item(c: HirCol) -> ProjEntry {
        ProjEntry {
            expr: BExpr::ColRef(HirRef::Col(c.id)),
            out: c,
        }
    }

    /// Projection items passing each column through to itself, keeping its
    /// identity — the item half of `SELECT <cols>`.
    pub(crate) fn passthrough_items(cols: impl IntoIterator<Item = HirCol>) -> Vec<ProjEntry> {
        cols.into_iter().map(RelExpr::passthrough_item).collect()
    }

    /// A projection: its output columns are the `ProjEntry.out`s (SELECT order —
    /// `place_pk_front` is physical, applied at lowering, not here).
    pub(crate) fn project(input: Rc<RelExpr>, items: Vec<ProjEntry>) -> Rc<RelExpr> {
        Rc::new(RelExpr::Project { input, items })
    }

    /// A join with its raw (unclassified) ON conjuncts.
    pub(crate) fn join(left: Rc<RelExpr>, right: Rc<RelExpr>, kind: JoinType, on: Vec<HirExpr>) -> Rc<RelExpr> {
        Rc::new(RelExpr::Join { left, right, kind, on: JoinOn::Raw(on) })
    }

    /// A reduce. `aggs` are already validated + nullability-stamped by bind (which
    /// holds the input env), so this is a plain node build — parity by construction.
    pub(crate) fn reduce(
        input: Rc<RelExpr>,
        pre: Vec<ProjEntry>,
        group_cols: Vec<ColId>,
        aggs: Vec<HirAgg>,
    ) -> Rc<RelExpr> {
        Rc::new(RelExpr::Reduce { input, pre, group_cols, aggs })
    }

    /// A DISTINCT over its input's visible columns.
    pub(crate) fn distinct(input: Rc<RelExpr>) -> Rc<RelExpr> {
        Rc::new(RelExpr::Distinct { input })
    }

    /// A fresh read of `input`: every column under a newly minted id.
    pub(crate) fn alias(ids: &ColIdGen, input: Rc<RelExpr>) -> Rc<RelExpr> {
        let cols = input
            .cols()
            .into_iter()
            .map(|c| HirCol::new(ids.next(), c.def))
            .collect();
        Rc::new(RelExpr::Alias { input, cols })
    }

    /// A fresh read of `input` under `defs`, one per input column in order — a
    /// CTE reference, whose columns carry the CTE's names.
    pub(crate) fn alias_as(ids: &ColIdGen, input: Rc<RelExpr>, defs: &[ColumnDef]) -> Rc<RelExpr> {
        let cols = defs.iter().map(|d| HirCol::new(ids.next(), d.clone())).collect();
        Rc::new(RelExpr::Alias { input, cols })
    }

    /// The relation's row key — the columns that identify a row uniquely — as
    /// `ColId`s of its output, or `None` where rows have no unique key: a join
    /// (its key is the join key), a stream (an append-only bag), or a relation
    /// keyed by a join key. A table's is its primary key, a reduce's its group
    /// key, a DISTINCT's or set operation's every column; a projection keeps
    /// the key iff it passes every key column through.
    pub(crate) fn row_key(&self) -> Option<Vec<ColId>> {
        match self {
            RelExpr::Get { source, schema, cols } => {
                if source.is_stream() {
                    return None;
                }
                let pk = &schema.pk_cols;
                if pk
                    .iter()
                    .any(|&i| guards::is_join_key_name(&schema.columns[i as usize].name))
                {
                    return None;
                }
                Some(pk.iter().map(|&i| cols[i as usize].id).collect())
            }
            RelExpr::Filter { input, .. } => input.row_key(),
            RelExpr::Project { input, items } => input
                .row_key()?
                .iter()
                .map(|k| items.iter().find(|it| as_col(&it.expr) == Some(*k)).map(|it| it.out.id))
                .collect(),
            RelExpr::Alias { input, cols } => {
                let in_cols = input.cols();
                input
                    .row_key()?
                    .iter()
                    .map(|k| in_cols.iter().position(|c| c.id == *k).map(|p| cols[p].id))
                    .collect()
            }
            RelExpr::Reduce { group_cols, .. } => Some(group_cols.clone()),
            RelExpr::Distinct { input } => Some(input.cols().iter().map(|c| c.id).collect()),
            RelExpr::SetOp { out, .. } => Some(out.iter().map(|c| c.out.id).collect()),
            RelExpr::Join { .. } => None,
        }
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
            let ty = guards::set_op_common_type(l.def.ty(), r.def.ty()).ok_or_else(|| {
                GnitzSqlError::Plan(format!(
                    "set operation: column {} type mismatch ({} vs {})",
                    i,
                    l.def.ty(),
                    r.def.ty()
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
            def.set_ty(ty);
            def.is_nullable = is_nullable;
            let target = |src: TypeCode| (src != ty.tc).then_some(ty.tc);
            out.push(SetOpCol {
                left: l.id,
                right: r.id,
                left_target: target(l.def.type_code),
                right_target: target(r.def.type_code),
                out: HirCol::new(ids.next(), def),
            });
        }
        Ok(Rc::new(RelExpr::SetOp { op, all, left, right, out }))
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
            RelExpr::Alias { input, cols } => one!(input, |n| Rc::new(RelExpr::Alias { input: n, cols: cols.clone() })),
            RelExpr::Reduce { input, pre, group_cols, aggs } => {
                one!(input, |n| RelExpr::reduce(
                    n,
                    pre.clone(),
                    group_cols.clone(),
                    aggs.clone()
                ))
            }
            RelExpr::Join { left, right, kind, on } => two!(left, right, |l, r| Rc::new(RelExpr::Join {
                left: l,
                right: r,
                kind: *kind,
                on: on.clone(),
            })),
            RelExpr::SetOp { op, all, left, right, out } => two!(left, right, |l, r| Rc::new(RelExpr::SetOp {
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
            RelExpr::Join { left, right, kind, .. } => match kind {
                // A semi/anti join carries only the left (outer) columns; a mark
                // join appends its synthetic `0/1` column after them. The equi/
                // outer joins are left ++ right with the null-providing side widened
                // per `kind` (`combined_payload_coldefs`, applied to the `HirCol`
                // defs — same `ColId`s, widening changes only nullability).
                JoinType::Semi | JoinType::Anti => left.cols(),
                JoinType::Mark(id) => {
                    let mut cols = left.cols();
                    cols.push(JoinType::mark_col(*id));
                    cols
                }
                _ => {
                    let mut cols = left.cols();
                    let mut rcols = right.cols();
                    kind.widen_sides(
                        cols.iter_mut().map(|c| &mut c.def),
                        rcols.iter_mut().map(|c| &mut c.def),
                    );
                    cols.extend(rcols);
                    cols
                }
            },
            RelExpr::Reduce { input, pre, group_cols, aggs } => {
                // Group cols keep their source `HirCol`s (from the reduce's input);
                // then each aggregate's raw value + companion columns. The physical
                // cardinality COUNT has no logical column and is absent here.
                let in_cols = if pre.is_empty() {
                    input.cols()
                } else {
                    pre.iter().map(|e| e.out.clone()).collect()
                };
                let mut cols: Vec<HirCol> = group_cols.iter().map(|id| hircol_of(&in_cols, *id).clone()).collect();
                for a in aggs {
                    cols.push(a.out.clone());
                    if let Some(c) = &a.companion {
                        cols.push(c.clone());
                    }
                }
                cols
            }
            RelExpr::Distinct { input } => input.cols(),
            RelExpr::Alias { cols, .. } => cols.clone(),
            RelExpr::SetOp { out, .. } => out.iter().map(|c| c.out.clone()).collect(),
        }
    }
}
