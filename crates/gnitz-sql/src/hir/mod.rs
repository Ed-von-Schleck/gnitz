//! The relational HIR: one IR between the sqlparser AST and what a query
//! lowers to, compiled by `bind → lower`; bind places each predicate
//! (`place.rs`) and joins in each subquery (`decorrelate.rs`) as it builds. A
//! view body lowers to its `Circuit`s ([`bind_and_lower`]); an ad-hoc SELECT
//! binds through the same binder and lowers to one read of one relation
//! ([`bind_adhoc_read`]).
//!
//! The discipline: a column is an **opaque `ColId`** minted once at bind and
//! never renumbered, so a resolved reference survives every tree built over it
//! (predicate placement, the segment-cut and source-collision rules). The
//! logical IR carries no layout — physical positions are assigned by one
//! positional pass (`physical.rs`) at lowering, the single home of the
//! PK-front convention and the `ColId → ColRef(position)` substitution.

mod bind;
mod decorrelate;
mod lower;
mod physical;
mod place;
mod window;

pub(crate) use bind::{bind_single_table, ViewBody};

use crate::agg::AggFunc;
use crate::ast_util::AggArg;
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp, LeafTyping};
use gnitz_core::{RelDescriptor, ViewBundle};
use gnitz_wire::AggFunc as WireAggFunc;
use gnitz_wire::{ColType, ColumnDef, RangeRel, TypeCode};
use std::rc::Rc;
use std::sync::Arc;

/// The one compiler core: bind a query — its CTEs, then its body — to one
/// `RelExpr` tree, and lower it to its view bundle. A CTE is a subtree of that
/// tree shared by every `Alias` naming it, so nothing compiles before the tree is
/// whole.
pub(crate) fn bind_and_lower<'a>(
    cat: &dyn crate::bind::Catalog,
    query: &sqlparser::ast::Query,
    view: bind::ViewBody,
    bounded: bool,
    aliases: impl ExactSizeIterator<Item = &'a sqlparser::ast::Ident>,
) -> Result<ViewBundle, GnitzSqlError> {
    let ids = ColIdGen::new();
    let stmt = view.stmt;
    let mut cx = bind::BindCx::new(cat, &ids, view);
    bind::bind_ctes(&mut cx, query)?;
    let rel = bind::bind_query(&mut cx, query)?;
    let mut names = rel.cols();
    crate::bind::apply_positional_aliases(aliases, names.iter_mut().map(|c| &mut c.def), stmt)?;
    lower::lower(rel, bounded, &names)
}

pub(crate) use lower::fold::FoldPieces;
pub(crate) use lower::read::{AdhocRead, AdhocRows, AdhocShape};

/// The ad-hoc read path's entry to the same core: bind a query that reads one
/// relation — its CTEs, then its body — and lower it to one read of that
/// relation. One binder, two sinks, so a statement means the same thing as an
/// ad-hoc read and as a view body.
pub(crate) fn bind_adhoc_read(
    cat: &dyn crate::bind::Catalog,
    query: &sqlparser::ast::Query,
    select: &sqlparser::ast::Select,
    op: &'static str,
    order_exprs: &[&sqlparser::ast::Expr],
) -> Result<AdhocRead, GnitzSqlError> {
    let ids = ColIdGen::new();
    let mut cx = bind::BindCx::adhoc(cat, &ids, op);
    bind::bind_ctes(&mut cx, query)?;
    let (rel, placed) = bind::bind_select(&mut cx, select, order_exprs)?;
    lower::read::lower_read(&rel, placed, &cx.names)
}

/// An `INSERT … RETURNING` list as the reply items of a read of `desc`.
pub(crate) fn bind_returning(
    projection: &[sqlparser::ast::SelectItem],
    desc: &Arc<RelDescriptor>,
    alias: &str,
) -> Result<AdhocRows, GnitzSqlError> {
    let ids = ColIdGen::new();
    let (source, items) = bind::bind_returning(&ids, projection, Arc::clone(desc), alias)?;
    lower::read::reply_rows(desc, &source, items, Vec::new())
}

/// Opaque column identity, unique within one `bind_and_lower` invocation, never
/// renumbered. Its `u32` payload is an allocation order, not a layout position —
/// consumers compare ids, never arithmetic on them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ColId(u32);

/// Monotonic `ColId` minter, threaded through bind by shared reference. `Get`
/// mints one per schema column at its reference site; `Project` mints one per
/// output `ProjEntry`. Bind-only: lowering assigns positions and never mints an
/// identity; a slot it adds carries none.
/// Interior-mutable: a leaf binder mints behind `&self`.
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

impl LeafTyping<ColId> for [HirCol] {
    fn decl(&self, id: &ColId) -> (ColType, bool) {
        let def = &hircol_of(self, *id).def;
        (def.ty, def.is_nullable)
    }
}

/// The `ColId` of a bare `ColRef` leaf, else `None` (a literal or a computed
/// expression).
pub(crate) fn as_col(e: &HirExpr) -> Option<ColId> {
    match e {
        BExpr::ColRef(id) => Some(*id),
        _ => None,
    }
}

/// Which of two inputs' columns a conjunct names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Left,
    Right,
    Both,
    Neither,
}

/// Which input's columns `conj` names; a column in neither input (a Mark join's
/// own column) counts as both.
pub(crate) fn side(conj: &HirExpr, left: &[HirCol], right: &[HirCol]) -> Side {
    let (mut l, mut r, mut other) = (false, false, false);
    conj.for_each_ref(&mut |id| match () {
        _ if col_by_id(left, *id).is_some() => l = true,
        _ if col_by_id(right, *id).is_some() => r = true,
        _ => other = true,
    });
    match (l, r, other) {
        (true, true, _) | (_, _, true) => Side::Both,
        (true, false, false) => Side::Left,
        (false, true, false) => Side::Right,
        (false, false, false) => Side::Neither,
    }
}

/// `conj` as a comparison between a column of `left` and one of `right`, turned
/// to read left-to-right.
pub(crate) fn cross_comparison<'a>(
    conj: &HirExpr,
    left: &'a [HirCol],
    right: &'a [HirCol],
) -> Option<(&'a HirCol, &'a HirCol, BinOp)> {
    let BExpr::BinOp(a, op, b) = conj else { return None };
    let (a, b) = (as_col(a)?, as_col(b)?);
    match (col_by_id(left, a), col_by_id(right, b)) {
        (Some(l), Some(r)) => Some((l, r, *op)),
        _ => Some((col_by_id(left, b)?, col_by_id(right, a)?, op.converse())),
    }
}

/// A subquery bound where its expression was written: the column it is read
/// through, and what joins that column in when the body's projection is built.
#[derive(Clone)]
pub(crate) struct SubqueryRef {
    /// The column this subquery is read through: its Mark join's BOOLEAN column,
    /// or its finalized aggregate.
    pub id: ColId,
    pub kind: SubqueryKind,
    /// EXISTS/IN: `Filter?(Get)`. Scalar: `Project(Reduce)` passing the group
    /// columns through and computing the finalized aggregate as `id`.
    pub rel: Rc<RelExpr>,
    /// The mixed-scope conjuncts that become the join's ON, an IN's
    /// `outer = inner` equality among them.
    pub correlation: Vec<HirExpr>,
}

/// How a subquery joins in: `Exists` (EXISTS / IN) as a Semi/Anti/Mark join,
/// `Scalar` (an aggregate, or a range ANY/ALL's MIN/MAX) as its reduce.
#[derive(Clone, Copy)]
pub(crate) enum SubqueryKind {
    /// `nullable`: an IN whose operands can be NULL, so its truth can be.
    Exists { nullable: bool },
    /// `count`: a COUNT, which a group of no rows renders as 0, never NULL.
    Scalar { ty: ColType, count: bool },
}

/// The bound-expression IR hosted on the HIR leaf. Structurally identical to the
/// runtime `BoundExpr`; only the leaf reference differs (`ColId` vs `usize`).
pub(crate) type HirExpr = crate::ir::BExpr<ColId>;

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
    /// The one row of no columns a FROM-less SELECT reads. Only an ad-hoc read
    /// binds one.
    Unit,
    Get {
        desc: Arc<RelDescriptor>,
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
        /// The join's keys. Every other predicate was placed into an input or
        /// left in a `Filter` above the join as it was built.
        on: JoinClass,
    },
    /// A GROUP BY / aggregate reduce: the group columns, then the aggregate
    /// columns the finalize `Project` above renders.
    Reduce {
        input: Rc<RelExpr>,
        /// `ColId`s of `input`'s columns; an expression key is a `Project` column below.
        group_cols: Vec<ColId>,
        aggs: AggCols,
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
    /// The lowering reads it through its input's source and materializes
    /// nothing of its own.
    Alias {
        input: Rc<RelExpr>,
        cols: Vec<HirCol>,
    },
    /// A set operation (UNION / INTERSECT / EXCEPT, ALL or distinct). `out[i]` is
    /// the promoted common type of each side's `i`-th column, under the
    /// per-operator output nullability.
    SetOp {
        op: SetOpKind,
        all: bool,
        left: Rc<RelExpr>,
        right: Rc<RelExpr>,
        out: Vec<HirCol>,
    },
    /// Per-partition top-N: the rows of `input` filling weight slots
    /// `offset .. offset + limit` of each `partition` value in `order`, then the
    /// whole row, which leads with the input's key — a total order, so the result
    /// is a function of the Z-set. Its output is `input`'s columns. `ORDER BY …
    /// LIMIT` on a view body is the empty partition; `QUALIFY ROW_NUMBER() OVER
    /// (…) <= n` names one.
    TopN {
        input: Rc<RelExpr>,
        partition: Vec<ColId>,
        order: Vec<TopNKey>,
        limit: u64,
        offset: u64,
    },
}

/// One ORDER BY key of a [`RelExpr::TopN`]: a column of its input, its
/// direction, and where NULLs go.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct TopNKey {
    pub col: ColId,
    pub desc: bool,
    pub nulls_first: bool,
}

/// One physical reduce column: the op, the column it reads (`None` for COUNT(*)),
/// and its output identity and def.
#[derive(Clone)]
pub(crate) struct AggCol {
    pub op: WireAggFunc,
    pub arg: Option<ColId>,
    pub col: HirCol,
}

/// A reduce's physical aggregate columns: each once, in first-use order.
#[derive(Clone)]
pub(crate) struct AggCols(Vec<AggCol>);

impl std::ops::Deref for AggCols {
    type Target = [AggCol];
    fn deref(&self) -> &[AggCol] {
        &self.0
    }
}

impl IntoIterator for AggCols {
    type Item = AggCol;
    type IntoIter = std::vec::IntoIter<AggCol>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// A value column: what computes it, and the type and nullability it is declared
/// with.
pub(crate) type Value = (HirExpr, ColType, bool);

/// One aggregate over a reduce: the logical `func(arg)` — a call's identity
/// within its reduce — and the physical value and count columns
/// ([`crate::agg::agg_ops`]) it may share with other aggregates.
#[derive(Clone)]
pub(crate) struct HirAgg {
    pub func: AggFunc,
    pub arg: AggArg<ColId>,
    pub out: AggCol,
    pub companion: Option<AggCol>,
}

impl HirAgg {
    /// Mint an aggregate's columns, reusing any identical column of `prior`, the
    /// aggregates of the same reduce.
    pub(crate) fn new(
        ids: &ColIdGen,
        func: AggFunc,
        arg: AggArg<ColId>,
        env: &[HirCol],
        is_global: bool,
        prior: &[HirAgg],
    ) -> Result<Self, GnitzSqlError> {
        let read = arg.ignoring_distinct();
        let arg_def = read.map(|id| &hircol_of(env, id).def);
        let (value, count) = crate::agg::agg_ops(func, arg_def, is_global)?;
        let col = |op: WireAggFunc| {
            // COUNT(*) reads no column, whatever argument the aggregate names.
            let arg = read.filter(|_| op != WireAggFunc::Count);
            let def = crate::agg::agg_col_def(op, arg.and(arg_def), is_global);
            match prior
                .iter()
                .flat_map(HirAgg::cols)
                .find(|c| c.op == op && c.arg == arg && c.col.def == def)
            {
                Some(shared) => shared.clone(),
                None => AggCol {
                    op,
                    arg,
                    col: HirCol::new(ids.next(), def),
                },
            }
        };
        Ok(HirAgg {
            func,
            arg,
            out: col(value),
            companion: count.map(col),
        })
    }

    /// Its physical columns: the value, then the count.
    pub(crate) fn cols(&self) -> impl Iterator<Item = &AggCol> {
        std::iter::once(&self.out).chain(&self.companion)
    }

    /// The physical columns of `aggs`.
    pub(crate) fn physical(aggs: &[HirAgg]) -> AggCols {
        let mut out: Vec<AggCol> = Vec::new();
        for c in aggs.iter().flat_map(HirAgg::cols) {
            if !out.iter().any(|p| p.col.id == c.col.id) {
                out.push(c.clone());
            }
        }
        AggCols(out)
    }

    /// The finalize composite over this aggregate's raw reduce column(s).
    pub(crate) fn finalize(&self) -> HirExpr {
        let col = |c: &AggCol| BExpr::ColRef(c.col.id);
        crate::agg::finalize_agg_bexpr(col(&self.out), self.companion.as_ref().map(col), self.func)
    }

    /// The type this aggregate renders where a view reads it.
    pub(crate) fn view_type(&self) -> ColType {
        crate::agg::agg_view_type(self.func, self.out.col.def.ty)
    }

    /// A companion carries the null-ness; otherwise the raw column's own.
    pub(crate) fn view_nullable(&self) -> bool {
        self.companion.is_some() || self.out.col.def.is_nullable
    }

    /// This aggregate as a view-facing value column.
    pub(crate) fn as_value(&self) -> Value {
        (self.finalize(), self.view_type(), self.view_nullable())
    }
}

/// Split an optional `Filter` off a node, returning its conjuncts (empty when
/// absent) and the source below it, as the `Rc` decorrelation builds on.
pub(crate) fn split_filter(input: &Rc<RelExpr>) -> (&[HirExpr], &Rc<RelExpr>) {
    match input.as_ref() {
        RelExpr::Filter { input, preds } => (preds, input),
        _ => (&[], input),
    }
}

/// Which set operation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum SetOpKind {
    Union,
    Intersect,
    Except,
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
    /// EXISTS/IN in a mark position, carrying the `ColId` of the synthetic BOOLEAN
    /// column tagging each left row — the column the subquery is read through.
    Mark(ColId),
}

impl JoinType {
    /// The mark column a [`JoinType::Mark`] appends after its left columns.
    /// Hidden: nothing but the expressions reading the subquery reach it, and it
    /// is addressed by id, never by name.
    pub(crate) fn mark_col(id: ColId) -> HirCol {
        HirCol::new(id, ColumnDef::new("_mark", TypeCode::Bool, false).hidden())
    }

    /// A left row survives unmatched ⇒ the right columns can be NULL, and
    /// `ν_A`, A's rows no right row matches, is emitted.
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

    /// Whether this side gets a **ν** — the unmatched set `w_P · [S = 0]`, its
    /// rows no row of the other side matches. Wider than [`Self::preserves`], which
    /// answers "emit an outer null-fill branch": `Semi`/`Anti`/`Mark` preserve
    /// neither side yet all three decide per left row by match existence. The
    /// reindex keep set protects the ν operands, so it asks this rather than
    /// `preserves`.
    pub(crate) fn has_nu(self, is_left: bool) -> bool {
        self.preserves(is_left) || (is_left && self.is_decorrelated())
    }

    /// Whether this side's unmatched rows are an output: a null-fill, an anti row,
    /// or a mark-0 row. A Semi reads no complement.
    pub(crate) fn emits_unmatched(self, is_left: bool) -> bool {
        self.preserves(is_left) || (is_left && matches!(self, JoinType::Anti | JoinType::Mark(_)))
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

/// A join's keys: its equality pairs and at most one range conjunct.
#[derive(Clone, Default)]
pub(crate) struct JoinClass {
    pub eq: Vec<EqPair>,
    pub range: Option<HirRange>,
}

/// Which physical join a join's keys call for. An enum rather than field probes
/// at each site: a pure range join has an empty `eq` too, so probing `eq` first
/// would read it as keyless and drop the range predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinShape {
    Equi,
    /// A range conjunct behind an equality prefix.
    Band,
    /// A range conjunct with no equality prefix.
    PureRange,
    Cross,
}

/// The key a join step's output rows carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutKey {
    /// The equi join key: each match is made once, on the worker its operands meet on.
    JoinKey,
    /// Both sides' source PKs, computed after the join, away from the worker owning it.
    PairPk,
    /// The outer side's source PK; `owned` when every row was kept on that PK's
    /// owner before the join.
    OuterPk { owned: bool },
}

impl OutKey {
    /// Which sides pin their `pk_cols` to the front of the keep list.
    pub(crate) fn pins(self) -> [bool; 2] {
        match self {
            OutKey::JoinKey => [false, false],
            OutKey::PairPk => [true, true],
            OutKey::OuterPk { .. } => [true, false],
        }
    }

    /// Whether the output needs the `shard(0..npk)` exchange.
    pub(crate) fn exchanged(self) -> bool {
        matches!(self, OutKey::PairPk | OutKey::OuterPk { owned: false })
    }
}

impl JoinClass {
    /// The key a step of `kind` over these keys emits its rows under.
    pub(crate) fn out_key(&self, kind: JoinType) -> OutKey {
        match (self.shape(), kind.is_decorrelated()) {
            (JoinShape::Equi, _) => OutKey::JoinKey,
            (JoinShape::Band, true) => OutKey::OuterPk { owned: false },
            (JoinShape::PureRange, true) => OutKey::OuterPk { owned: true },
            (JoinShape::Band | JoinShape::PureRange | JoinShape::Cross, _) => OutKey::PairPk,
        }
    }

    /// Whether `side`'s rows are unique on this join's equality columns, so a row of
    /// the other side matches at most one of them.
    pub(crate) fn side_unique(&self, side: &RelExpr, is_left: bool) -> bool {
        side.unique_key().is_some_and(|key| {
            key.iter()
                .all(|c| self.eq.iter().any(|p| (if is_left { p.left } else { p.right }) == *c))
        })
    }
    /// The key columns this ON names on one side — what that side's reindex reads,
    /// and so what a cut input must keep however narrow the demand above it is.
    pub(crate) fn key_cols(&self, is_left: bool) -> impl Iterator<Item = ColId> + '_ {
        let pick = move |(l, r)| if is_left { l } else { r };
        self.eq
            .iter()
            .map(move |p| pick((p.left, p.right)))
            .chain(self.range.iter().map(move |r| pick((r.left, r.right))))
    }

    /// Each key slot's common type, in [`Self::key_cols`] order: the eq pairs',
    /// then the range's.
    pub(crate) fn key_tcs(&self) -> Vec<TypeCode> {
        self.eq
            .iter()
            .map(|p| p.tc)
            .chain(self.range.iter().map(|r| r.tc))
            .collect()
    }

    pub(crate) fn shape(&self) -> JoinShape {
        match (self.range.is_some(), self.eq.is_empty()) {
            (true, true) => JoinShape::PureRange,
            (true, false) => JoinShape::Band,
            (false, true) => JoinShape::Cross,
            (false, false) => JoinShape::Equi,
        }
    }
}

/// The type a set-op column pair shares: its own where the two agree, else one a
/// column copy widens both into. `None` where there is neither.
fn set_op_common_type(l: ColType, r: ColType) -> Option<ColType> {
    if l == r {
        return Some(l);
    }
    if !l.decimal_domains_match(r) || l.tc.is_temporal() || r.tc.is_temporal() {
        return None;
    }
    let t = l.tc.join_key_common_type(r.tc).ok()?;
    let widens = |src: TypeCode| src.is_widening_promotion(t);
    (widens(l.tc) && widens(r.tc)).then_some(ColType::of(t))
}

impl RelExpr {
    /// A base table or committed/hidden-view source: one fresh `ColId` per
    /// registered schema column, in schema order (so a `ColId`'s env position is
    /// its schema position).
    pub(crate) fn get(ids: &ColIdGen, desc: Arc<RelDescriptor>) -> Rc<RelExpr> {
        let cols = desc
            .schema
            .columns
            .iter()
            .map(|c| HirCol::new(ids.next(), c.clone()))
            .collect();
        Rc::new(RelExpr::Get { desc, cols })
    }

    /// A projection item passing one column through to itself, keeping its
    /// identity.
    pub(crate) fn passthrough_item(c: HirCol) -> ProjEntry {
        ProjEntry { expr: BExpr::ColRef(c.id), out: c }
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

    pub(crate) fn reduce(input: Rc<RelExpr>, group_cols: Vec<ColId>, aggs: AggCols) -> Rc<RelExpr> {
        Rc::new(RelExpr::Reduce { input, group_cols, aggs })
    }

    /// A DISTINCT over every column of `input`, or `input` itself when it is
    /// already a set. `role` names the clause in the refusal of a float column.
    pub(crate) fn distinct(input: Rc<RelExpr>, role: &str) -> Result<Rc<RelExpr>, GnitzSqlError> {
        crate::rules::reject_float_keys(input.cols().iter().map(|c| &c.def), role)?;
        // Already a set: nothing to deduplicate.
        if input.unique_key().is_some() {
            return Ok(input);
        }
        Ok(Rc::new(RelExpr::Distinct { input }))
    }

    /// A top-N over `input`. `limit ≥ 1`, and every key names an input column.
    pub(crate) fn top_n(
        input: Rc<RelExpr>,
        partition: Vec<ColId>,
        order: Vec<TopNKey>,
        limit: u64,
        offset: u64,
    ) -> Rc<RelExpr> {
        debug_assert!(limit >= 1, "a top-N selects at least one slot");
        Rc::new(RelExpr::TopN { input, partition, order, limit, offset })
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

    /// Columns no two live rows share a value of, each row at weight 1: a relation
    /// whose PK does not repeat, a reduce's group columns, every column of a DISTINCT
    /// or a deduplicating set operation — through filters, pass-through projections,
    /// aliases, a top-N, and a join matching each row of one side at most once.
    pub(crate) fn unique_key(&self) -> Option<Vec<ColId>> {
        if let Some(mapped) = self.key_through(RelExpr::unique_key) {
            return mapped;
        }
        match self {
            RelExpr::Unit => Some(Vec::new()),
            RelExpr::Get { desc, cols } if !desc.pk_repeats => {
                Some(desc.schema.pk_cols.iter().map(|&i| cols[i as usize].id).collect())
            }
            RelExpr::Reduce { group_cols, .. } => Some(group_cols.clone()),
            RelExpr::Distinct { input } => Some(input.cols().iter().map(|c| c.id).collect()),
            RelExpr::SetOp { all: false, out, .. } => Some(out.iter().map(|c| c.id).collect()),
            // A subset of its input's rows under its input's identities.
            RelExpr::TopN { input, .. } => input.unique_key(),
            RelExpr::Join { left, right, kind, on } => match kind {
                // One output row per left row.
                k if k.is_decorrelated() => left.unique_key(),
                JoinType::Inner | JoinType::Left if on.side_unique(right, false) => left.unique_key(),
                JoinType::Inner | JoinType::Right if on.side_unique(left, true) => right.unique_key(),
                _ => None,
            },
            _ => None,
        }
    }

    /// `key` of a pass-through node's input, renamed to this node's output ids —
    /// `None` when `self` is not a filter, projection or alias.
    fn key_through(&self, key: fn(&RelExpr) -> Option<Vec<ColId>>) -> Option<Option<Vec<ColId>>> {
        Some(match self {
            RelExpr::Filter { input, .. } => key(input),
            RelExpr::Project { input, items } => key(input).and_then(|k| {
                k.iter()
                    .map(|k| items.iter().find(|it| as_col(&it.expr) == Some(*k)).map(|it| it.out.id))
                    .collect()
            }),
            RelExpr::Alias { input, cols } => {
                let in_cols = input.cols();
                key(input).and_then(|k| {
                    k.iter()
                        .map(|k| in_cols.iter().position(|c| c.id == *k).map(|p| cols[p].id))
                        .collect()
                })
            }
            _ => return None,
        })
    }

    /// A set operation. Pairs the two sides' output columns positionally,
    /// promoting each pair to its common type ([`set_op_common_type`]) and stamping
    /// the per-operator output nullability (Union `l||r`, Intersect `l&&r`, Except
    /// `l`). Rejects an arity or type mismatch, and a float output column — the
    /// content hash identifying a set-op row cannot carry one.
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
            return Err(GnitzSqlError::Rejected(format!(
                "set operation: column count mismatch ({} vs {})",
                lcols.len(),
                rcols.len()
            )));
        }
        let mut out = Vec::with_capacity(lcols.len());
        for (i, (l, r)) in lcols.iter().zip(&rcols).enumerate() {
            let ty = set_op_common_type(l.def.ty, r.def.ty).ok_or_else(|| {
                GnitzSqlError::Rejected(format!(
                    "set operation: column {} type mismatch ({} vs {})",
                    i, l.def.ty, r.def.ty
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
            def.ty = ty;
            def.is_nullable = is_nullable;
            out.push(HirCol::new(ids.next(), def));
        }
        crate::rules::reject_float_keys(out.iter().map(|c| &c.def), "set operation")?;
        Ok(Rc::new(RelExpr::SetOp { op, all, left, right, out }))
    }

    /// The node's output columns.
    pub(crate) fn cols(&self) -> Vec<HirCol> {
        match self {
            RelExpr::Unit => Vec::new(),
            RelExpr::Get { cols, .. } => cols.clone(),
            RelExpr::Filter { input, .. } => input.cols(),
            RelExpr::Project { items, .. } => items.iter().map(|e| e.out.clone()).collect(),
            RelExpr::Join { left, right, kind, .. } => match kind {
                // `join_frame`'s widening, over the logical columns.
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
            RelExpr::Reduce { input, group_cols, aggs } => {
                let in_cols = input.cols();
                let mut cols: Vec<HirCol> = group_cols.iter().map(|id| hircol_of(&in_cols, *id).clone()).collect();
                cols.extend(aggs.iter().map(|c| c.col.clone()));
                cols
            }
            RelExpr::Distinct { input } | RelExpr::TopN { input, .. } => input.cols(),
            RelExpr::Alias { cols, .. } => cols.clone(),
            RelExpr::SetOp { out, .. } => out.clone(),
        }
    }
}

#[cfg(test)]
#[path = "tests/hir.rs"]
mod tests;
