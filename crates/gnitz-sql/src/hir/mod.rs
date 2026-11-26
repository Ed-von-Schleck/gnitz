//! The relational HIR: one IR between the sqlparser AST and what a query
//! lowers to, compiled by `bind → lower`; bind places each predicate
//! (`place.rs`) and joins in each subquery as it builds. A view body lowers to
//! its `Circuit`s ([`bind_and_lower`]); an ad-hoc SELECT binds through the same
//! binder and lowers to one read of one relation ([`bind_adhoc_read`]).
//!
//! The discipline: a column is an **opaque `ColId`** minted once at bind and
//! never renumbered, so a resolved reference survives every tree built over it
//! (predicate placement, the segment-cut and source-collision rules). The
//! logical IR carries no layout — physical positions are assigned by one
//! positional pass (`physical.rs`) at lowering, the single home of the
//! PK-front convention and the `ColId → ColRef(position)` substitution.

mod bind;
mod lower;
mod physical;
mod place;
mod read;

pub(crate) use bind::{BoundRef, ViewBody};

use crate::agg::{AggArg, AggFunc};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp, F64Bits, LeafTyping};
use gnitz_core::{PlannedView, RelDescriptor, Schema};
use gnitz_wire::AggFunc as WireAggFunc;
use gnitz_wire::{ColType, ColumnDef, RangeRel, ReduceKind, TypeCode, WeightSlots};
use std::borrow::Cow;
use std::rc::Rc;
use std::sync::Arc;

/// A view body compiled: its segments and the user-named view, every place its text names a catalog
/// relation, and the names it gives its visible output columns before the
/// statement's column list renames them.
pub(crate) struct LoweredView {
    pub(crate) segments: Vec<PlannedView>,
    pub(crate) view: PlannedView,
    pub(crate) refs: Vec<BoundRef>,
    pub(crate) body_columns: Vec<String>,
}

/// The one compiler core: bind a query — its envelope, its CTEs, then its body — to one
/// `RelExpr` tree, and lower it to its segments and view. A CTE is a subtree of that
/// tree shared by every reference naming it, so nothing compiles before the tree is
/// whole.
pub(crate) fn bind_and_lower<'a>(
    cat: &dyn crate::catalog::Catalog,
    query: &sqlparser::ast::Query,
    view: bind::ViewBody,
    aliases: impl ExactSizeIterator<Item = &'a sqlparser::ast::Ident>,
) -> Result<LoweredView, GnitzSqlError> {
    let ids = ColIdGen::new();
    let stmt = view.stmt;
    let mut cx = bind::BindCx::new(cat, &ids, bind::Surface::View(view));
    let rel = bind::bind_query(&mut cx, query)?;
    let mut names = rel.cols().into_owned();
    let body_columns = names
        .iter()
        .filter(|c| !c.def.is_hidden)
        .map(|c| String::from(&*c.def.name))
        .collect();
    bind::apply_positional_aliases(aliases, names.iter_mut().map(|c| &mut c.def), stmt)?;
    let (segments, view) = lower::lower(rel, &names)?;
    Ok(LoweredView {
        segments,
        view,
        refs: cx.refs.into_inner(),
        body_columns,
    })
}

pub(crate) use bind::ranking_names;
pub(crate) use read::{AdhocRead, AdhocShape, FoldPieces, RowsReply};

/// The ad-hoc read path's entry to the same core: bind a query that reads one
/// relation — its CTEs, then its body — and lower it to one read of that
/// relation. One binder, two sinks, so a statement means the same thing as an
/// ad-hoc read and as a view body.
pub(crate) fn bind_adhoc_read(
    cat: &dyn crate::catalog::Catalog,
    query: &sqlparser::ast::Query,
) -> Result<AdhocRead, GnitzSqlError> {
    use crate::error::derivation;
    let keys = crate::tail::parse_order_by(query.order_by.as_ref())?;
    // The top-level body reads through one relation's sink, so a set operation or a
    // join is refused before anything is resolved; a CTE or a derived table may
    // derive, and the read lowering refuses what it cannot compose to one read.
    let select = match query.body.as_ref() {
        sqlparser::ast::SetExpr::SetOperation { .. } => return Err(derivation("set operation")),
        // Not the derivation template: CREATE VIEW refuses these bodies too.
        body => crate::validate::as_plain_select(body, "direct SELECT")?,
    };
    match select.from.as_slice() {
        [_, _, ..] => return Err(derivation("comma-join FROM")),
        [single] if !single.joins.is_empty() => return Err(derivation("JOIN")),
        _ => {}
    }
    let ids = ColIdGen::new();
    let mut cx = bind::BindCx::new(cat, &ids, bind::Surface::Adhoc);
    bind::bind_ctes(&mut cx, query)?;
    let (rel, order) = bind::bind_select(&cx, select, &keys)?;
    read::lower_read(&rel, order, cx.refs.into_inner())
}

/// An `INSERT … RETURNING` list as the reply items of a read of `desc`.
pub(crate) fn bind_returning(
    projection: &[sqlparser::ast::SelectItem],
    desc: &Arc<RelDescriptor>,
    alias: &str,
) -> Result<RowsReply, GnitzSqlError> {
    let ids = ColIdGen::new();
    let (source, items) = bind::bind_returning(&ids, projection, &desc.schema, alias)?;
    read::reply_rows(physical::Frame::scan(desc, &source), items, Vec::new(), false)
}

/// Opaque column identity, unique within one bind (one [`ColIdGen`]), never
/// renumbered. Its `u32` payload is an allocation order, not a layout position —
/// consumers compare ids, never arithmetic on them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ColId(u32);

/// Monotonic `ColId` minter: mints every column identity. Bind-only: lowering
/// assigns positions and never mints; a slot it adds carries none.
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

/// A set of one bind's [`ColId`]s, a bit per id: [`ColIdGen`] mints them dense
/// from 0.
#[derive(Clone, Default)]
pub(crate) struct ColSet(Vec<u64>);

impl ColSet {
    pub(crate) fn insert(&mut self, id: ColId) {
        let word = id.0 as usize / 64;
        if word >= self.0.len() {
            self.0.resize(word + 1, 0);
        }
        self.0[word] |= 1 << (id.0 % 64);
    }

    pub(crate) fn contains(&self, id: ColId) -> bool {
        self.0
            .get(id.0 as usize / 64)
            .is_some_and(|w| w >> (id.0 % 64) & 1 == 1)
    }

    /// The ids, ascending.
    pub(crate) fn iter(&self) -> impl Iterator<Item = ColId> + '_ {
        (self.0.iter().enumerate())
            .flat_map(|(w, &bits)| gnitz_wire::BitIter(bits).map(move |b| ColId((w * 64 + b) as u32)))
    }
}

impl Extend<ColId> for ColSet {
    fn extend<I: IntoIterator<Item = ColId>>(&mut self, ids: I) {
        ids.into_iter().for_each(|id| self.insert(id));
    }
}

impl FromIterator<ColId> for ColSet {
    fn from_iter<I: IntoIterator<Item = ColId>>(ids: I) -> Self {
        let mut set = ColSet::default();
        set.extend(ids);
        set
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

/// Which of two inputs' columns a conjunct names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    /// Only input `0` (the left) or `1` (the right).
    One(usize),
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
        (true, false, false) => Side::One(0),
        (false, true, false) => Side::One(1),
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
    let (a, b) = (a.as_col()?, b.as_col()?);
    match (col_by_id(left, a), col_by_id(right, b)) {
        (Some(l), Some(r)) => Some((l, r, *op)),
        _ => Some((col_by_id(left, b)?, col_by_id(right, a)?, op.converse())),
    }
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

impl ProjEntry {
    /// A projection item passing one column through to itself, keeping its
    /// identity.
    pub(crate) fn passthrough(c: HirCol) -> ProjEntry {
        ProjEntry { expr: BExpr::ColRef(c.id), out: c }
    }

    /// Projection items passing each column through to itself, keeping its
    /// identity — the item half of `SELECT <cols>`.
    pub(crate) fn passthroughs(cols: impl IntoIterator<Item = HirCol>) -> Vec<ProjEntry> {
        cols.into_iter().map(ProjEntry::passthrough).collect()
    }
}

/// The logical relational IR. `Get` and `SetOp` store their whole output column
/// list; `Reduce`, a window `Ordered` and a Mark join store only the columns they
/// add (aggregate, rank and mark columns). Every other column is derived on demand
/// by [`RelExpr::cols`] from the node's inputs. Ids are minted once at bind and
/// stay stable.
///
/// A body is a DAG: a node more than one parent holds by `Rc` — a CTE named twice,
/// a window input read again — is one subtree, which the lowering computes once.
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
        /// The join's keys, admitted for `kind`. Every other predicate was placed
        /// into an input or left in a `Filter` above the join as it was built.
        on: JoinClass,
    },
    /// A GROUP BY / aggregate reduce: the group columns, then the aggregate
    /// columns the finalize `Project` above renders.
    Reduce {
        input: Rc<RelExpr>,
        /// `ColId`s of `input`'s columns; an expression key is a `Project` column below.
        group_cols: Vec<ColId>,
        aggs: Vec<AggCol>,
        /// [`ReduceKind::Aggregate`] for SQL's aggregate, [`ReduceKind::Groups`]
        /// for one that owes no row over an empty input.
        kind: ReduceKind,
    },
    /// Every weight of `input` clamped to `[0, 1]`, its element identified by all
    /// its columns, hidden ones included.
    Distinct {
        input: Rc<RelExpr>,
    },
    /// A set operation over weights: UNION sums them, EXCEPT is
    /// `positive_part(A − B)`, INTERSECT is `min(A, B)`. `out[i]` is the promoted
    /// common type of each side's `i`-th column, under the per-operator output
    /// nullability.
    SetOp {
        op: SetOpKind,
        left: Rc<RelExpr>,
        right: Rc<RelExpr>,
        out: Vec<HirCol>,
    },
    /// An operator over each `partition` value of `input` kept in `order`; `op`
    /// says what it computes there.
    Ordered {
        input: Rc<RelExpr>,
        partition: Vec<ColId>,
        order: Vec<OrderCol>,
        op: OrderedOp,
    },
}

/// What a [`RelExpr::Ordered`] computes over each partition.
pub(crate) enum OrderedOp {
    /// Top-N: the rows filling weight slots `skip .. skip + keep` in
    /// `order`, then the whole row, which leads with the input's key — a total
    /// order, so the result is a function of the Z-set. Its output is the
    /// input's columns. `ORDER BY … LIMIT` on a view body is the empty
    /// partition; `QUALIFY ROW_NUMBER() OVER (…) <= n` names one.
    TopN(WeightSlots),
    /// Running values: every row, widened by its values over the rows of its
    /// partition at or before its peers — the rows equal to it on every key of
    /// `order`. Its output is the input's columns, then `rank` — one more than
    /// the number of rows before the row's peers — then `dense_rank` — the
    /// number of peer groups up to and including the row's — then each of
    /// `aggs` over those rows, the peers included.
    Window {
        aggs: Vec<AggCol>,
        rank: Option<HirCol>,
        dense_rank: Option<HirCol>,
    },
}

impl OrderedOp {
    /// The columns it adds behind its input's: `rank`, `dense_rank`, then each
    /// aggregate's.
    pub(crate) fn added_cols(&self) -> impl Iterator<Item = &HirCol> {
        let (aggs, rank, dense_rank): (&[AggCol], _, _) = match self {
            OrderedOp::Window { aggs, rank, dense_rank } => (aggs, rank.as_ref(), dense_rank.as_ref()),
            OrderedOp::TopN(_) => (&[], None, None),
        };
        rank.into_iter().chain(dense_rank).chain(aggs.iter().map(|c| &c.col))
    }
}

/// One ORDER BY key of a [`RelExpr::Ordered`]: a column of its input, its
/// direction, and where NULLs go.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrderCol {
    pub col: ColId,
    pub desc: bool,
    pub nulls_first: bool,
}

impl OrderCol {
    /// The written `key`'s direction and NULL placement on the column `col`.
    pub(crate) fn of(key: &crate::tail::OrderKey<'_>, col: ColId) -> OrderCol {
        OrderCol {
            col,
            desc: key.desc,
            nulls_first: key.nulls_first,
        }
    }
}

/// One physical reduce column: the op, the column it reads (`None` for COUNT(*)),
/// and its output identity and def.
#[derive(Clone)]
pub(crate) struct AggCol {
    pub op: WireAggFunc,
    pub arg: Option<ColId>,
    pub col: HirCol,
}

/// The columns a reduce over `group_cols` computing `aggs` reads: the group
/// columns, then each aggregate argument not already listed.
pub(crate) fn reduce_reads(group_cols: &[ColId], aggs: &[AggCol]) -> Vec<ColId> {
    let mut reads = group_cols.to_vec();
    for id in aggs.iter().filter_map(|c| c.arg) {
        if !reads.contains(&id) {
            reads.push(id);
        }
    }
    reads
}

/// One aggregate over a reduce: the logical `func(arg)` — a call's identity
/// within its reduce — and the physical value and count columns
/// ([`crate::agg::agg_ops`]) it may share with other aggregates.
#[derive(Clone)]
pub(crate) struct HirAgg {
    pub func: AggFunc,
    pub arg: AggArg<ColId>,
    pub out: AggCol,
    pub companion: Option<AggCol>,
    /// A [`WireAggFunc::FiniteSum`]'s counts of +inf and of -inf rows, NaN
    /// under both.
    pub infs: Option<[AggCol; 2]>,
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
        let arg_distinct = arg.is_distinct();
        let read = arg.ignoring_distinct();
        let arg_def = read.map(|id| &hircol_of(env, id).def);
        let (value, count) = crate::agg::agg_ops(func, arg_def, is_global)?;
        let col = |op: WireAggFunc| {
            // COUNT(*) reads no column, whatever argument the aggregate names.
            let arg = read.filter(|_| op != WireAggFunc::Count);
            let def = crate::agg::agg_col_def(op, arg.and(arg_def), is_global);
            // A DISTINCT call's columns are reduced over another set than a plain
            // call's, so the two share none.
            match prior
                .iter()
                .filter(|p| p.arg.is_distinct() == arg_distinct)
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
            infs: (value == WireAggFunc::FiniteSum)
                .then(|| [col(WireAggFunc::CountPosInf), col(WireAggFunc::CountNegInf)]),
        })
    }

    /// Its physical columns: the value, the count, then the infinity counts.
    pub(crate) fn cols(&self) -> impl Iterator<Item = &AggCol> {
        std::iter::once(&self.out)
            .chain(&self.companion)
            .chain(self.infs.iter().flatten())
    }

    /// The physical columns of `aggs`: each once, in first-use order.
    pub(crate) fn physical(aggs: &[HirAgg]) -> Vec<AggCol> {
        let mut out: Vec<AggCol> = Vec::new();
        for c in aggs.iter().flat_map(HirAgg::cols) {
            if !out.iter().any(|p| p.col.id == c.col.id) {
                out.push(c.clone());
            }
        }
        out
    }

    /// The SELECT/HAVING value over this aggregate's raw reduce column(s): the
    /// raw value when it has no count companion ([`crate::agg::agg_ops`]), else
    /// NULL on a zero count — AVG as a float division by the count, every other
    /// aggregate as a CASE on it. A float SUM or AVG over a row of +inf is +inf,
    /// over one of -inf is -inf, and over both, or over a NaN, is NaN.
    pub(crate) fn finalize(&self) -> HirExpr {
        let col = |c: &AggCol| BExpr::ColRef(c.col.id);
        let value = match &self.companion {
            None => col(&self.out),
            // The cast makes the division a float one; dividing by a zero count is NULL.
            Some(count) if self.func == AggFunc::Avg => {
                let value = BExpr::Cast {
                    expr: Box::new(col(&self.out)),
                    to: ColType::of(TypeCode::F64),
                };
                BExpr::bin(value, BinOp::Div, col(count))
            }
            Some(count) => BExpr::Case {
                branches: vec![(BExpr::bin(col(count), BinOp::Ne, BExpr::LitInt(0)), col(&self.out))],
                else_: Box::new(BExpr::LitNull),
            },
        };
        let Some([pos, neg]) = &self.infs else {
            return value;
        };
        let some = |c: &AggCol| BExpr::bin(col(c), BinOp::Ne, BExpr::LitInt(0));
        let float = |v: f64| BExpr::LitFloat { v: F64Bits(v), dec: None };
        BExpr::Case {
            branches: vec![
                (BExpr::bin(some(pos), BinOp::And, some(neg)), float(f64::NAN)),
                (some(pos), float(f64::INFINITY)),
                (some(neg), float(f64::NEG_INFINITY)),
            ],
            else_: Box::new(value),
        }
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
    /// The decorrelation-only kind an EXISTS/IN subquery lowers to, never produced
    /// by a FROM-clause JOIN: it preserves neither side and carries only the left
    /// (outer) columns.
    Exists(ExistsKind),
}

impl JoinType {
    /// A row of `side` (0 left, 1 right) survives unmatched ⇒ the other side's
    /// columns can be NULL, and `side`'s unmatched rows (`ν_A` for the left,
    /// `ν_B` for the right) are emitted.
    pub(crate) fn preserves(self, side: usize) -> bool {
        matches!(
            (self, side),
            (JoinType::Left | JoinType::Full, 0) | (JoinType::Right | JoinType::Full, 1)
        )
    }

    /// Widen the two sides of one join step for its null semantics — a preserved
    /// side forces the *other* side's columns nullable. It moves only
    /// `is_nullable`, so a resolved reference survives.
    pub(crate) fn widen_sides<'a>(
        self,
        left: impl Iterator<Item = &'a mut ColumnDef>,
        right: impl Iterator<Item = &'a mut ColumnDef>,
    ) {
        if self.preserves(1) {
            left.for_each(|d| d.is_nullable = true);
        }
        if self.preserves(0) {
            right.for_each(|d| d.is_nullable = true);
        }
    }

    /// Whether this side gets a **ν** — the unmatched set `w_P · [S = 0]`, its
    /// rows no row of the other side matches. Wider than [`Self::preserves`], which
    /// answers "emit an outer null-fill branch": an `Exists` kind preserves
    /// neither side yet decides per left row by match existence.
    pub(crate) fn has_nu(self, side: usize) -> bool {
        self.preserves(side) || (side == 0 && matches!(self, JoinType::Exists(_)))
    }

    /// Whether this side's unmatched rows are an output: a null-fill, an anti row,
    /// or a mark-0 row. A Semi reads no complement.
    pub(crate) fn emits_unmatched(self, side: usize) -> bool {
        self.preserves(side) || (side == 0 && matches!(self, JoinType::Exists(ExistsKind::Anti | ExistsKind::Mark(_))))
    }
}

/// A [`JoinType::Exists`] join's kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExistsKind {
    /// EXISTS/IN: keep each left row by match existence.
    Semi,
    /// NOT EXISTS / NOT IN: drop each left row by match existence.
    Anti,
    /// EXISTS/IN in a mark position, carrying the `ColId` of the synthetic BOOLEAN
    /// column tagging each left row — the column the subquery is read through.
    Mark(ColId),
}

/// A join-key column pair — `cols[0]` the left side's, `cols[1]` the right's —
/// and its promoted common type.
#[derive(Clone, Copy)]
pub(crate) struct KeyPair {
    pub cols: [ColId; 2],
    pub ty: ColType,
}

/// A join's keys: its equality pairs and at most one range conjunct.
#[derive(Clone, Default)]
pub(crate) struct JoinClass {
    pub eq: Vec<KeyPair>,
    /// The range pair and its relation, canonicalized left-to-right.
    pub range: Option<(KeyPair, RangeRel)>,
}

/// Which physical join a step's keys call for. A pure range join has an empty
/// `eq`, as a keyless one does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinPlan {
    Equi,
    /// A range conjunct behind an equality prefix.
    Band(RangeRel),
    /// A range conjunct with no equality prefix.
    PureRange(RangeRel),
    /// No key: INNER only.
    Cross,
}

impl JoinClass {
    /// Whether `rel`, this join's input `side`, has rows unique on its equality
    /// columns, so a row of the other side matches at most one of them.
    pub(crate) fn side_unique(&self, rel: &RelExpr, side: usize) -> bool {
        rel.unique_key()
            .is_some_and(|key| key.iter().all(|c| self.eq.iter().any(|p| p.cols[side] == *c)))
    }

    /// The key pairs, one per key slot: the eq pairs, then the range's.
    pub(crate) fn keys(&self) -> impl Iterator<Item = &KeyPair> {
        self.eq.iter().chain(self.range.as_ref().map(|(p, _)| p))
    }

    /// The number of key slots.
    pub(crate) fn arity(&self) -> usize {
        self.eq.len() + usize::from(self.range.is_some())
    }

    /// The key columns this ON names on one side — what that side's reindex reads,
    /// and so what a cut input must keep however narrow the demand above it is.
    pub(crate) fn key_cols(&self, side: usize) -> impl Iterator<Item = ColId> {
        self.keys().map(move |p| p.cols[side])
    }

    /// The physical join these keys call for.
    pub(crate) fn plan(&self) -> JoinPlan {
        match (self.range.map(|(_, rel)| rel), self.eq.is_empty()) {
            (None, false) => JoinPlan::Equi,
            (Some(rel), false) => JoinPlan::Band(rel),
            (None, true) => JoinPlan::Cross,
            (Some(rel), true) => JoinPlan::PureRange(rel),
        }
    }

    /// Refuse a step of `kind` over these keys that no emitter builds. Both
    /// refusals are ordinary SQL with a buildable lowering; neither emitter was
    /// written.
    pub(crate) fn admit(&self, kind: JoinType) -> Result<(), GnitzSqlError> {
        match (self.plan(), kind) {
            (JoinPlan::Cross, k) if k != JoinType::Inner => Err(GnitzSqlError::rejected(
                "a LEFT/RIGHT/FULL JOIN or an EXISTS/IN correlation needs at least one equijoin \
                 or range predicate between its two sides; only an INNER step (CROSS JOIN, a \
                 comma-separated FROM, or JOIN … ON with no cross-table comparison) may be keyless.",
            )),
            (JoinPlan::PureRange(_), JoinType::Right | JoinType::Full) => Err(GnitzSqlError::rejected(
                "pure-range RIGHT/FULL JOIN (a sole inequality range conjunct with no \
                 equality prefix) is not supported; its mirror null-fill has no inner-join \
                 witness on the preserved side. Use INNER/LEFT JOIN, or add an equality \
                 conjunct to make it a band join.",
            )),
            _ => Ok(()),
        }
    }
}

/// The type a set-op column pair shares: its own where the two agree, else one a
/// column copy widens both into. `None` where there is neither.
fn set_op_common_type(l: ColType, r: ColType) -> Option<ColType> {
    if l == r {
        return Some(l);
    }
    let t = l.tc.join_key_common_type(r.tc).ok()?;
    let widens = |src: TypeCode| src.is_widening_promotion(t);
    (widens(l.tc) && widens(r.tc)).then_some(ColType::of(t))
}

/// One fresh `ColId` per column of `schema`, in schema order (so a `ColId`'s env
/// position is its schema position).
fn schema_cols(ids: &ColIdGen, schema: &Schema) -> Vec<HirCol> {
    schema
        .columns()
        .iter()
        .map(|c| HirCol::new(ids.next(), c.clone()))
        .collect()
}

impl RelExpr {
    /// A base table or committed/hidden-view source: its columns from [`schema_cols`].
    pub(crate) fn get(ids: &ColIdGen, desc: Arc<RelDescriptor>) -> Rc<RelExpr> {
        let cols = schema_cols(ids, &desc.schema);
        Rc::new(RelExpr::Get { desc, cols })
    }

    /// A projection: its output columns are the `ProjEntry.out`s (SELECT order —
    /// the PK-front placement is physical, applied at lowering, not here).
    pub(crate) fn project(input: Rc<RelExpr>, items: Vec<ProjEntry>) -> Rc<RelExpr> {
        Rc::new(RelExpr::Project { input, items })
    }

    /// A reduce of `input` grouped by `group_cols`. `role` names the clause the
    /// keys were written in, in the refusal of a float key.
    pub(crate) fn reduce(
        input: Rc<RelExpr>,
        group_cols: Vec<ColId>,
        aggs: Vec<AggCol>,
        kind: ReduceKind,
        role: &str,
    ) -> Result<Rc<RelExpr>, GnitzSqlError> {
        let cols = input.cols();
        crate::rules::reject_float_keys(group_cols.iter().map(|&k| &hircol_of(&cols, k).def), role)?;
        Ok(Rc::new(RelExpr::Reduce { input, group_cols, aggs, kind }))
    }

    /// A DISTINCT over every column of `input`, or `input` itself when it is
    /// already a set. `role` names the clause in the refusal of a float column.
    pub(crate) fn distinct(input: Rc<RelExpr>, role: &str) -> Result<Rc<RelExpr>, GnitzSqlError> {
        // Already a set: nothing to deduplicate.
        if input.unique_key().is_some() {
            return Ok(input);
        }
        crate::rules::reject_float_keys(input.cols().iter().map(|c| &c.def), role)?;
        Ok(Rc::new(RelExpr::Distinct { input }))
    }

    /// `op` over each `partition` value of `input` kept in `order`. Every key names
    /// an input column; a top-N keeps at least one slot, and a window has an order
    /// and a value.
    pub(crate) fn ordered(
        input: Rc<RelExpr>,
        partition: Vec<ColId>,
        order: Vec<OrderCol>,
        op: OrderedOp,
    ) -> Result<Rc<RelExpr>, GnitzSqlError> {
        debug_assert!(match &op {
            OrderedOp::TopN(slots) => slots.keep >= 1,
            OrderedOp::Window { aggs, rank, dense_rank } =>
                !order.is_empty() && (!aggs.is_empty() || rank.is_some() || dense_rank.is_some()),
        });
        let cols = input.cols();
        crate::rules::reject_float_keys(
            partition.iter().map(|&k| &hircol_of(&cols, k).def),
            "window PARTITION BY",
        )?;
        Ok(Rc::new(RelExpr::Ordered { input, partition, order, op }))
    }

    /// A fresh read of `input` under `defs`, one per input column in order, each
    /// under a newly minted id: a projection renaming every column, so two reads of
    /// one shared `input` are distinguishable by id wherever they meet (a
    /// self-join's two sides).
    pub(crate) fn alias(ids: &ColIdGen, input: Rc<RelExpr>, defs: impl IntoIterator<Item = ColumnDef>) -> Rc<RelExpr> {
        let items = (input.cols().iter())
            .zip(defs)
            .map(|(c, def)| ProjEntry {
                expr: BExpr::ColRef(c.id),
                out: HirCol::new(ids.next(), def),
            })
            .collect();
        RelExpr::project(input, items)
    }

    /// Columns no two live rows share a value of, each row at weight 1; `None` where
    /// the relation may be a bag.
    pub(crate) fn unique_key(&self) -> Option<Vec<ColId>> {
        match self {
            RelExpr::Unit => Some(Vec::new()),
            RelExpr::Get { desc, cols } if !desc.pk_repeats => {
                Some(desc.schema.pk_cols().iter().map(|&i| cols[i as usize].id).collect())
            }
            RelExpr::Get { .. } => None,
            // Rows of its input — a subset of them, or each once — under its
            // input's identities.
            RelExpr::Filter { input, .. } | RelExpr::Ordered { input, .. } => input.unique_key(),
            RelExpr::Project { input, items } => input
                .unique_key()?
                .iter()
                .map(|k| items.iter().find(|it| it.expr.as_col() == Some(*k)).map(|it| it.out.id))
                .collect(),
            RelExpr::Reduce { group_cols, .. } => Some(group_cols.clone()),
            RelExpr::Distinct { input } => Some(input.cols().iter().map(|c| c.id).collect()),
            // positive_part(A − B) ≤ A and min(A, B) ≤ A, B; a widened key stays a key.
            RelExpr::SetOp { op, left, right, out } => {
                // A side's key as the columns at the same positions of `out`.
                let kept = |side: &RelExpr| {
                    let cols = side.cols();
                    let at = |k: &ColId| cols.iter().position(|c| c.id == *k).map(|p| out[p].id);
                    side.unique_key()?.iter().map(at).collect()
                };
                match op {
                    SetOpKind::Union => None,
                    SetOpKind::Except => kept(left),
                    SetOpKind::Intersect => kept(left).or_else(|| kept(right)),
                }
            }
            RelExpr::Join { left, right, kind, on, .. } => match kind {
                // One output row per left row.
                JoinType::Exists(_) => left.unique_key(),
                JoinType::Inner | JoinType::Left if on.side_unique(right, 1) => left.unique_key(),
                JoinType::Inner | JoinType::Right if on.side_unique(left, 0) => right.unique_key(),
                _ => None,
            },
        }
    }

    /// A set operation, its sides' columns paired positionally at their common type
    /// ([`set_op_common_type`]) under the per-operator output nullability (Union
    /// `l||r`, Intersect `l&&r`, Except `l`). A float column is refused: SQL
    /// equality, which identifies an element, is not a float's byte equality.
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
            return Err(GnitzSqlError::rejected(format!(
                "set operation: column count mismatch ({} vs {})",
                lcols.len(),
                rcols.len()
            )));
        }
        let mut out = Vec::with_capacity(lcols.len());
        for (i, (l, r)) in lcols.iter().zip(rcols.iter()).enumerate() {
            let ty = set_op_common_type(l.def.ty, r.def.ty).ok_or_else(|| {
                GnitzSqlError::rejected(format!(
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
        let set_op = |left, right| Rc::new(RelExpr::SetOp { op, left, right, out });
        Ok(match op {
            _ if all => set_op(left, right),
            SetOpKind::Union => Rc::new(RelExpr::Distinct { input: set_op(left, right) }),
            SetOpKind::Except => set_op(RelExpr::distinct(left, "set operation")?, right),
            SetOpKind::Intersect => {
                let left = match right.unique_key() {
                    Some(_) => left,
                    None => RelExpr::distinct(left, "set operation")?,
                };
                set_op(left, right)
            }
        })
    }

    /// The node's output columns.
    pub(crate) fn cols(&self) -> Cow<'_, [HirCol]> {
        match self {
            RelExpr::Unit => Cow::Borrowed(&[]),
            RelExpr::Get { cols, .. } => Cow::Borrowed(cols),
            RelExpr::Filter { input, .. } | RelExpr::Distinct { input } => input.cols(),
            RelExpr::Project { items, .. } => Cow::Owned(items.iter().map(|e| e.out.clone()).collect()),
            RelExpr::Join { left, right, kind, .. } => match kind {
                JoinType::Exists(ExistsKind::Mark(id)) => {
                    let mut cols = left.cols().into_owned();
                    // Hidden: nothing but the expressions reading the subquery
                    // reach it, and it is addressed by id, never by name.
                    cols.push(HirCol::new(
                        *id,
                        ColumnDef::new("_mark", TypeCode::Bool, false).hidden(),
                    ));
                    Cow::Owned(cols)
                }
                JoinType::Exists(_) => left.cols(),
                // Null-widened per `JoinType::widen_sides`, as the join shell's frame is.
                _ => {
                    let mut cols = left.cols().into_owned();
                    let mut rcols = right.cols().into_owned();
                    kind.widen_sides(
                        cols.iter_mut().map(|c| &mut c.def),
                        rcols.iter_mut().map(|c| &mut c.def),
                    );
                    cols.extend(rcols);
                    Cow::Owned(cols)
                }
            },
            RelExpr::Reduce { input, group_cols, aggs, .. } => {
                let in_cols = input.cols();
                let mut cols: Vec<HirCol> = group_cols.iter().map(|id| hircol_of(&in_cols, *id).clone()).collect();
                cols.extend(aggs.iter().map(|c| c.col.clone()));
                Cow::Owned(cols)
            }
            RelExpr::Ordered { input, op, .. } => {
                let mut cols = input.cols().into_owned();
                cols.extend(op.added_cols().cloned());
                Cow::Owned(cols)
            }
            RelExpr::SetOp { out, .. } => Cow::Borrowed(out),
        }
    }
}

#[cfg(test)]
#[path = "tests/hir.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/hir.rs"]
mod benches;
