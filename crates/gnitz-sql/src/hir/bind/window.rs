//! Window functions (`f(…) OVER (PARTITION BY … ORDER BY …)`), desugared at
//! bind into the window / join / reduce / top-N HIR the rest of the pipeline
//! lowers.
//!
//! Let `W` be the relation the SELECT list projects (the FROM/WHERE tree, or
//! the grouped relation) narrowed to one column per distinct expression a window
//! operand, the SELECT list or QUALIFY reads. Every call belongs to a
//! specification — its partition columns `P` and its order `O` — and the body is
//! one pipeline over `W`:
//!
//! 1. **The running values.** A specification with an order (the default `RANGE
//!    … CURRENT ROW` frame) is one window [`RelExpr::Ordered`] over the rows so
//!    far: every row, widened by each aggregate folded up to and including the
//!    row's peers, and by its rank among them. `ROW_NUMBER` is the rank under an
//!    order the input's unique key makes total. Its partition is a reduce group
//!    key, so it takes any key but a float; its order takes any type and honours
//!    the written NULL placement.
//! 2. **The cut.** A `ROW_NUMBER` bounded by a QUALIFY conjunct (`<= n`, `< n`,
//!    `= n`) and read by nothing else is a per-partition top-N of those rows,
//!    under the rules of a window's keys.
//! 3. **The partition values.** A specification with no order (no ORDER BY, or an
//!    aggregate under an `UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame) is
//!    `γ_P(W)`, each aggregate per partition, inner-joined onto the rows on `P`.
//!    It is computed over the whole of `W` and matches each row exactly once, so
//!    the cut commutes with the join. `P` is a join key here, and carries a
//!    join key's rules: no float, and provably NOT NULL (an inner join drops a
//!    NULL key).
//! 4. QUALIFY's remaining conjuncts filter the result, and the SELECT list projects it.
//!
//! A frame is read for an aggregate only: a ranking function ignores it.
//!
//! `ROW_NUMBER` numbers rows only where each peer group is one row at weight 1,
//! so outside the cut its input must be a set with a unique key. The cut needs
//! neither: over a bag each copy of a row fills its own weight slot.
//!
//! The desugar is purely logical: `W` is a shared subtree read through aliases,
//! and the lowering's cut memo and collision rule decide what is read in place
//! and what is cut once to a hidden segment.
//!
//! Cost under incremental maintenance: a changed row re-emits the rows of its
//! partition at or after it (the whole partition for an unordered
//! specification). An ordered specification also reads the rows of the
//! partition before the changed one, and keeps one index entry per row of `W`.

use super::super::{ColId, ColIdGen, HirAgg, HirCol, HirExpr, JoinType, OrderCol, ProjEntry, RelExpr};
use super::{bind_projection, place_order_keys, ItemLeaf};
use crate::agg::{agg_func_from_name, AggArg, AggFunc};
use crate::ast_util::{classify_agg_shape, single_part_ident, unknown_function, CallSurface, PlainCall};
use crate::bind::{bind_conjuncts, bind_structural, LeafBinder};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp, LeafTyping};
use crate::rules::{find_unique_column, reject_float_key};
use crate::tail::parse_order_key;
use gnitz_wire::{ColType, ColumnDef, TypeCode};
use sqlparser::ast::{
    Expr, Function, Ident, NamedWindowDefinition, NamedWindowExpr, OrderByExpr, Select, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowSpec, WindowType,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// A view body's SELECT list over the relation `rel` builds, with the item each
/// ORDER BY key sorts on. Window calls in the list, in the ORDER BY keys and in
/// QUALIFY are found by binding them; a body holding one is desugared. `leaf` is the body's own item leaf; `ctx` names the surface.
pub(super) fn bind_view_select_list<L: ItemLeaf>(
    ids: &ColIdGen,
    select: &Select,
    rel: impl FnOnce() -> Result<Rc<RelExpr>, GnitzSqlError>,
    leaf: &L,
    ctx: &str,
    order_exprs: &[&Expr],
) -> Result<(Rc<RelExpr>, Vec<usize>), GnitzSqlError> {
    if let Some(name) = gnitz_wire::first_duplicate(select.named_window.iter().map(|d| d.0.value.as_str())) {
        return Err(GnitzSqlError::Rejected(format!("WINDOW clause defines '{name}' twice")));
    }
    let state = RefCell::new(Windows::default());
    let over = WindowLeaf {
        inner: leaf,
        ids,
        named: &select.named_window,
        state: &state,
        aliases: &[],
    };
    let mut items = bind_projection(&select.projection, &over, ids, ctx)?;
    let placed = place_order_keys(order_exprs, &mut items, ids, &over)?;
    let qualify = match &select.qualify {
        Some(q) => bind_conjuncts(q, &WindowLeaf { aliases: &items, ..over }).map_err(|e| e.in_clause("QUALIFY"))?,
        None => Vec::new(),
    };
    let win = state.into_inner();
    let rel = rel()?;
    if win.calls.is_empty() {
        if select.qualify.is_some() {
            return Err(GnitzSqlError::Rejected(
                "QUALIFY needs a window function in the SELECT list or in the QUALIFY predicate".into(),
            ));
        }
        return Ok((leaf.project(rel, items)?, placed));
    }
    Ok((desugar(ids, rel, leaf, items, qualify, win)?, placed))
}

// ── Binding ─────────────────────────────────────────────────────────────────────

/// A window function. `RowNumber` is `RANK` over an order its input's unique key
/// makes total, or — bounded by QUALIFY and read nowhere else — a top-N.
#[derive(Clone, Copy, PartialEq, Debug)]
enum WinFunc {
    Agg(AggFunc),
    Rank,
    DenseRank,
    RowNumber,
}

/// One window call, its operands columns of `W`, and the column its value is
/// read through. `order` is as written until the desugar makes it the call's
/// effective order; it is empty for a whole-partition window.
struct Call {
    func: WinFunc,
    arg: Option<ColId>,
    partition: Vec<ColId>,
    order: Vec<OrderCol>,
    out: HirCol,
}

/// What binding a windowed SELECT list collects: `W`'s items — one per distinct
/// expression a window operand, the SELECT list or QUALIFY reads — and the calls.
#[derive(Default)]
struct Windows {
    w: Vec<ProjEntry>,
    calls: Vec<Call>,
}

impl Windows {
    /// The `W` column holding `e` — a pass-through when `e` is a bare source
    /// column, else a computed column evaluating it over the body's scope, typed
    /// through the body's `leaf`. One per distinct expression.
    fn hoist<L: ItemLeaf>(&mut self, ids: &ColIdGen, leaf: &L, e: &HirExpr) -> ColId {
        if let Some(it) = self.w.iter().find(|it| it.expr == *e) {
            return it.out.id;
        }
        let (ty, nullable) = e.declared(leaf);
        let name = e
            .as_col()
            .and_then(|id| leaf.source_name(id))
            .unwrap_or_else(|| format!("_w{}", self.w.len()));
        let out = HirCol::new(ids.next(), ColumnDef::new(name, ty, nullable));
        let id = out.id;
        self.w.push(ProjEntry { expr: e.clone(), out });
        id
    }

    /// `e` with every source reference replaced by its `W` column; a reference to
    /// one of `placeholders` stays.
    fn over_w<L: ItemLeaf>(&mut self, ids: &ColIdGen, leaf: &L, placeholders: &[ColId], e: &HirExpr) -> HirExpr {
        e.rebuild(&mut |id| {
            BExpr::ColRef(if placeholders.contains(id) {
                *id
            } else {
                self.hoist(ids, leaf, &BExpr::ColRef(*id))
            })
        })
    }
}

/// The def of `W`'s column `id`.
fn def_of(w: &[ProjEntry], id: ColId) -> &ColumnDef {
    &w.iter()
        .find(|it| it.out.id == id)
        .expect("a window operand is a column of W")
        .out
        .def
}

/// A window specification's partition, order and frame.
type SpecParts<'f> = (&'f [Expr], &'f [OrderByExpr], Option<&'f WindowFrame>);

/// The leaf a view body's SELECT list, ORDER BY keys and QUALIFY bind through:
/// every window call becomes a placeholder column, everything else goes to the
/// body's own leaf.
struct WindowLeaf<'a, L> {
    inner: &'a L,
    ids: &'a ColIdGen,
    named: &'a [NamedWindowDefinition],
    state: &'a RefCell<Windows>,
    /// The SELECT list QUALIFY may name by alias; empty for the list itself and ORDER BY.
    aliases: &'a [ProjEntry],
}

impl<L: ItemLeaf> WindowLeaf<'_, L> {
    /// The `(type, nullable)` of the placeholder `id` names, when it names one —
    /// a window value's declaration, which the body's own leaf does not know.
    fn placeholder(&self, id: &ColId) -> Option<(ColType, bool)> {
        self.state
            .borrow()
            .calls
            .iter()
            .find(|c| c.out.id == *id)
            .map(|c| (c.out.def.ty, c.out.def.is_nullable))
    }

    /// The window specification a call names — written inline, or defined in
    /// the WINDOW clause (which may itself name another definition) — as its
    /// partition, order and frame.
    fn resolve_spec<'f>(&'f self, over: &'f WindowType) -> Result<SpecParts<'f>, GnitzSqlError> {
        let inline = |s: &'f WindowSpec| {
            // Exhaustive (no `..`): a future frame or exclusion field stops the
            // build rather than being dropped into a maintained view.
            let WindowSpec {
                window_name,
                partition_by,
                order_by,
                window_frame,
            } = s;
            if window_name.is_some() {
                return Err(GnitzSqlError::Rejected(
                    "a window specification extending a named window (OVER (w …)) is not supported".into(),
                ));
            }
            Ok((partition_by.as_slice(), order_by.as_slice(), window_frame.as_ref()))
        };
        let mut name: &Ident = match over {
            WindowType::WindowSpec(s) => return inline(s),
            WindowType::NamedWindow(name) => name,
        };
        // Every hop resolves a distinct definition, so a chain longer than the
        // clause is a cycle.
        for _ in 0..=self.named.len() {
            let def = self
                .named
                .iter()
                .find(|d| d.0.value.eq_ignore_ascii_case(&name.value))
                .ok_or_else(|| {
                    GnitzSqlError::Rejected(format!("window '{}' is not defined in the WINDOW clause", name.value))
                })?;
            match &def.1 {
                NamedWindowExpr::WindowSpec(s) => return inline(s),
                NamedWindowExpr::NamedWindow(next) => name = next,
            }
        }
        Err(GnitzSqlError::Rejected(format!(
            "window '{}' is defined in terms of itself",
            name.value
        )))
    }

    /// Bind one `f(…) OVER (…)` call and hand back the placeholder column its
    /// value stands in for until the desugar computes it. Two calls equal in
    /// function, argument and specification share one.
    fn bind_window_call(&self, f: &Function, over: &WindowType) -> Result<HirCol, GnitzSqlError> {
        let (func, arg_expr) = classify_window_call(f)?;
        let (partition_by, order_by, window_frame) = self.resolve_spec(over)?;
        // Operands bind through the body's leaf, so an aggregate argument in a
        // grouped body resolves to its finalize and a nested OVER is refused
        // there.
        let bind = |e: &Expr| bind_structural(e, self.inner);
        let mut arg = arg_expr.map(bind).transpose()?;
        let partition = partition_by.iter().map(bind).collect::<Result<Vec<_>, _>>()?;
        let mut order = Vec::with_capacity(order_by.len());
        for o in order_by {
            let (expr, desc, nulls_first) = parse_order_key(o, "window ORDER BY")?;
            order.push((bind(expr)?, desc, nulls_first));
        }
        // A ranking function ignores its frame; an aggregate over the whole
        // partition reads no order.
        if matches!(func, WinFunc::Agg(_)) && frame_is_whole(window_frame)? {
            order.clear();
        }
        if matches!(func, WinFunc::Rank | WinFunc::DenseRank) && order.is_empty() {
            return Err(GnitzSqlError::Rejected(
                "RANK / DENSE_RANK need an ORDER BY in their window specification".into(),
            ));
        }
        // Over an argument that is never NULL, COUNT(e) is COUNT(*).
        let never_null = |e: &HirExpr| e.never_null(self.inner);
        if func == WinFunc::Agg(AggFunc::Count) && arg.as_ref().is_some_and(never_null) {
            arg = None;
        }

        // Hoisted only now, so `W` holds no column whose one reader was dropped.
        let mut st = self.state.borrow_mut();
        let arg = arg.map(|e| st.hoist(self.ids, self.inner, &e));
        let mut partition_cols: Vec<ColId> = Vec::with_capacity(partition.len());
        for e in &partition {
            let col = st.hoist(self.ids, self.inner, e);
            if !partition_cols.contains(&col) {
                partition_cols.push(col);
            }
        }
        let order: Vec<OrderCol> = order
            .iter()
            .map(|(e, desc, nulls_first)| OrderCol {
                col: st.hoist(self.ids, self.inner, e),
                desc: *desc,
                nulls_first: *nulls_first,
            })
            .collect();
        let (ty, is_nullable) = call_typing(func, arg.map(|a| def_of(&st.w, a)))?;

        let same = |c: &&Call| c.func == func && c.arg == arg && c.partition == partition_cols && c.order == order;
        if let Some(call) = st.calls.iter().find(same) {
            return Ok(call.out.clone());
        }
        // Addressed by id only: its value column is minted under this id.
        let out = HirCol::new(
            self.ids.next(),
            ColumnDef::new(format!("_win{}", st.calls.len()), ty, is_nullable).hidden(),
        );
        st.calls.push(Call {
            func,
            arg,
            partition: partition_cols,
            order,
            out: out.clone(),
        });
        Ok(out)
    }
}

/// A call's output type and nullability over its argument column. The type is
/// the aggregate's own; the nullability is not, because a window frame always
/// contains the current row, so an aggregate over a NOT NULL argument is never
/// NULL.
fn call_typing(func: WinFunc, arg: Option<&ColumnDef>) -> Result<(ColType, bool), GnitzSqlError> {
    let WinFunc::Agg(agg) = func else {
        return Ok((ColType::of(TypeCode::I64), false));
    };
    let (op, _) = crate::agg::agg_ops(agg, arg, false)?;
    let raw = crate::agg::agg_col_def(op, arg, false).ty;
    let nullable = match agg {
        AggFunc::Count => false,
        _ => arg.is_some_and(|d| d.is_nullable),
    };
    Ok((crate::agg::agg_view_type(agg, raw), nullable))
}

impl<L: ItemLeaf> LeafTyping<ColId> for WindowLeaf<'_, L> {
    /// This leaf's own placeholders; everything else is the body's.
    fn decl(&self, r: &ColId) -> (ColType, bool) {
        self.placeholder(r).unwrap_or_else(|| self.inner.decl(r))
    }
}

impl<L: ItemLeaf> LeafBinder<ColId> for WindowLeaf<'_, L> {
    fn bind_node(&self, e: &Expr) -> Option<HirExpr> {
        self.inner.bind_node(e)
    }
    /// A source column outranks a SELECT alias of the same name; the alias
    /// table is consulted only where the body's own scope has nothing, and where
    /// it has no single answer the body's own error stands.
    fn bind_column(&self, qual: Option<&str>, name: &str) -> Result<HirExpr, GnitzSqlError> {
        let err = match self.inner.bind_column(qual, name) {
            Ok(bound) => return Ok(bound),
            Err(err) => err,
        };
        // An output column belongs to no relation, so only a bare name is one.
        let aliased = match qual {
            None => find_unique_column(self.aliases.iter().map(|it| &it.out.def), name),
            Some(_) => Ok(None),
        };
        match aliased {
            Ok(Some(at)) => Ok(self.aliases[at].expr.clone()),
            _ => Err(err),
        }
    }
    fn bind_aggregate(&self, func: AggFunc, arg: AggArg<&Expr>) -> Result<HirExpr, GnitzSqlError> {
        self.inner.bind_aggregate(func, arg)
    }
    /// The one context that admits a window call: it binds to the placeholder
    /// column its value stands in for until the desugar computes it.
    fn bind_window(&self, f: &Function, over: &WindowType) -> Result<HirExpr, GnitzSqlError> {
        Ok(BExpr::ColRef(self.bind_window_call(f, over)?.id))
    }
    fn bind_subquery(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        self.inner.bind_subquery(e)
    }
}

impl<L: ItemLeaf> ItemLeaf for WindowLeaf<'_, L> {
    fn source_name(&self, id: ColId) -> Option<String> {
        self.inner.source_name(id)
    }
    fn project(&self, source: Rc<RelExpr>, items: Vec<ProjEntry>) -> Result<Rc<RelExpr>, GnitzSqlError> {
        self.inner.project(source, items)
    }
    fn wildcard_cols(&self, qualifier: Option<&str>) -> Result<Option<Vec<&HirCol>>, GnitzSqlError> {
        self.inner.wildcard_cols(qualifier)
    }
}

/// The window-only functions, by their SQL spelling; the aggregates are
/// windowed under their own names.
const RANKING_NAMES: [(&str, WinFunc); 3] = [
    ("rank", WinFunc::Rank),
    ("dense_rank", WinFunc::DenseRank),
    ("row_number", WinFunc::RowNumber),
];

/// The names of [`RANKING_NAMES`].
pub(crate) fn ranking_names() -> impl Iterator<Item = &'static str> {
    RANKING_NAMES.into_iter().map(|(n, _)| n)
}

/// Classify a windowed call into its function and its unbound argument.
fn classify_window_call(f: &Function) -> Result<(WinFunc, Option<&Expr>), GnitzSqlError> {
    let call = PlainCall::check(f, CallSurface::Window)?;
    let name = single_part_ident(&f.name).ok_or_else(|| unknown_function(f))?;
    match RANKING_NAMES.into_iter().find(|(n, _)| name.eq_ignore_ascii_case(n)) {
        Some((_, func)) => {
            call.args(&name.to_ascii_uppercase(), (0, Some(0)))?;
            Ok((func, None))
        }
        None if agg_func_from_name(name).is_some() => {
            let (agg, arg) = classify_agg_shape(call)?;
            Ok((WinFunc::Agg(agg), arg))
        }
        None => Err(GnitzSqlError::Rejected(format!(
            "{}: not supported as a window function",
            name.to_ascii_uppercase()
        ))),
    }
}

/// Whether a frame covers the partition whole (`true`) or folds it up to the
/// current row's peers (`false`, the default frame) — the two frames accepted.
fn frame_is_whole(frame: Option<&WindowFrame>) -> Result<bool, GnitzSqlError> {
    let Some(frame) = frame else {
        return Ok(false);
    };
    // Exhaustive (no `..`): a frame field sqlparser adds (EXCLUDE) stops the build.
    let WindowFrame { units, start_bound, end_bound } = frame;
    let end = end_bound.as_ref().unwrap_or(&WindowFrameBound::CurrentRow);
    match (start_bound, end) {
        (WindowFrameBound::Preceding(None), WindowFrameBound::Following(None)) => Ok(true),
        (WindowFrameBound::Preceding(None), WindowFrameBound::CurrentRow) => match units {
            WindowFrameUnits::Range => Ok(false),
            WindowFrameUnits::Rows | WindowFrameUnits::Groups => Err(GnitzSqlError::Rejected(
                "window frames: ROWS / GROUPS … CURRENT ROW excludes the current row's peers, which \
                 needs a total row order; use RANGE (the default frame)"
                    .into(),
            )),
        },
        _ => Err(GnitzSqlError::Rejected(
            "window frames: only RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (the default) and \
             BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING are supported"
                .into(),
        )),
    }
}

// ── Desugar ─────────────────────────────────────────────────────────────────────

/// A call's order as its plan keys on it: the written keys — a ROW_NUMBER's
/// extended by `key`, the input's unique key, which makes the order total over a
/// set — less any key the partition or an earlier key already fixes.
fn effective_order(call: &Call, key: Option<&[ColId]>) -> Vec<OrderCol> {
    let tiebreak = key
        .filter(|_| call.func == WinFunc::RowNumber)
        .into_iter()
        .flatten()
        .map(|&col| OrderCol { col, desc: false, nulls_first: false });
    let mut order: Vec<OrderCol> = Vec::new();
    for k in call.order.iter().copied().chain(tiebreak) {
        if !call.partition.contains(&k.col) && !order.iter().any(|o| o.col == k.col) {
            order.push(k);
        }
    }
    order
}

/// The `(limit, offset)` of the slots `offset .. offset + limit` a QUALIFY
/// conjunct `rn <= n`, `rn < n` or `rn = n` over placeholder `rn` keeps of each
/// partition, the literal on either side; `None` for any other predicate, or a
/// bound keeping no slot.
fn row_number_bound(conjunct: &HirExpr, rn: ColId) -> Option<(u64, u64)> {
    let BExpr::BinOp(l, op, r) = conjunct else {
        return None;
    };
    let is_rn = |e: &HirExpr| matches!(e, BExpr::ColRef(id) if *id == rn);
    // Normalize to `rn OP n`; `x OP y` ⟺ `y OP.converse() x`.
    let (op, n) = match (l.as_ref(), r.as_ref()) {
        (lhs, BExpr::LitInt(n)) if is_rn(lhs) => (*op, *n),
        (BExpr::LitInt(n), rhs) if is_rn(rhs) => (op.converse(), *n),
        _ => return None,
    };
    // The row numbered `n` fills slot `n - 1`.
    let (limit, offset) = match op {
        BinOp::Le => (n, 0),
        BinOp::Lt => (n.saturating_sub(1), 0),
        BinOp::Eq => (1, n.saturating_sub(1)),
        _ => return None,
    };
    (n >= 1 && limit >= 1).then_some((limit as u64, offset as u64))
}

/// The body's top-N cut: a ROW_NUMBER with an order, bounded by one QUALIFY
/// conjunct and read by nothing else — its index in `calls`, that conjunct's in
/// `qualify`, and the `(limit, offset)` the bound keeps.
fn top_n_cut(calls: &[Call], items: &[ProjEntry], qualify: &[HirExpr]) -> Option<(usize, usize, (u64, u64))> {
    calls.iter().enumerate().find_map(|(ci, call)| {
        if call.func != WinFunc::RowNumber || call.order.is_empty() {
            return None;
        }
        let rn = call.out.id;
        let reads = |e: &HirExpr| {
            let mut found = false;
            e.for_each_ref(&mut |id| found |= *id == rn);
            found
        };
        let (qi, slots) = qualify
            .iter()
            .enumerate()
            .find_map(|(i, c)| Some((i, row_number_bound(c, rn)?)))?;
        let others = qualify.iter().enumerate().filter(|(i, _)| *i != qi).map(|(_, c)| c);
        let mut elsewhere = items.iter().map(|it| &it.expr).chain(others);
        (!elsewhere.any(reads)).then_some((ci, qi, slots))
    })
}

/// One window specification of the desugar: its keys as `W` columns, and the
/// calls it serves. `order` is empty for a whole-partition specification.
struct Spec {
    partition: Vec<ColId>,
    order: Vec<OrderCol>,
    calls: Vec<Call>,
}

/// A fresh read of a relation: the alias, its columns, and its column for each
/// of the relation's.
struct Read {
    rel: Rc<RelExpr>,
    cols: Vec<HirCol>,
    at: HashMap<ColId, ColId>,
}

impl Read {
    fn of(ids: &ColIdGen, input: &Rc<RelExpr>) -> Read {
        let in_cols = input.cols();
        let rel = RelExpr::alias(ids, Rc::clone(input), in_cols.iter().map(|c| c.def.clone()));
        let cols = rel.cols();
        let at = in_cols.iter().map(|c| c.id).zip(cols.iter().map(|c| c.id)).collect();
        Read { rel, cols, at }
    }
    fn col(&self, src: ColId) -> HirExpr {
        BExpr::ColRef(self.at[&src])
    }
}

/// Rewrite a windowed body: widen `W` by every ordered specification's running
/// values, cut it by the QUALIFY-bounded ROW_NUMBER if there is one, join every
/// unordered specification's value relation onto an outer read of that, filter
/// by what is left of QUALIFY and project the SELECT list.
fn desugar<L: ItemLeaf>(
    ids: &ColIdGen,
    input: Rc<RelExpr>,
    leaf: &L,
    items: Vec<ProjEntry>,
    mut qualify: Vec<HirExpr>,
    mut win: Windows,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let mut calls = std::mem::take(&mut win.calls);
    let placeholders: Vec<ColId> = calls.iter().map(|c| c.out.id).collect();
    let key: Option<Vec<ColId>> = if calls.iter().any(|c| c.func == WinFunc::RowNumber) {
        input
            .unique_key()
            .map(|key| key.iter().map(|&id| win.hoist(ids, leaf, &BExpr::ColRef(id))).collect())
    } else {
        None
    };
    for call in &mut calls {
        call.order = effective_order(call, key.as_deref());
        for &p in &call.partition {
            let def = def_of(&win.w, p);
            // A partition is a reduce group key: GROUP BY's rule.
            reject_float_key(def.ty.tc, None, "window PARTITION BY")?;
            if call.order.is_empty() && def.is_nullable {
                return Err(GnitzSqlError::Rejected(
                    "window PARTITION BY: the key must be provably NOT NULL (a NOT NULL column, or an \
                     expression over NOT NULL columns) — a window over its whole partition is joined on \
                     it, and a NULL key neither groups nor matches"
                        .into(),
                ));
            }
        }
    }

    let cut = top_n_cut(&calls, &items, &qualify).map(|(ci, qi, slots)| {
        qualify.remove(qi);
        (calls.remove(ci), slots)
    });

    let mut specs: Vec<Spec> = Vec::new();
    for call in calls {
        if call.func == WinFunc::RowNumber && key.is_none() {
            return Err(GnitzSqlError::Rejected(
                "ROW_NUMBER needs an input with a unique row key (a table, or a grouped or DISTINCT \
                 body); over a join body, a stream, a UNION ALL or a view keyed by a join key its rows \
                 have no total order — use RANK or DENSE_RANK, give it an ORDER BY and bound it in \
                 QUALIFY without selecting it, or window over a view of the join"
                    .into(),
            ));
        }
        match specs
            .iter_mut()
            .find(|s| s.partition == call.partition && s.order == call.order)
        {
            Some(spec) => spec.calls.push(call),
            None => specs.push(Spec {
                partition: call.partition.clone(),
                order: call.order.clone(),
                calls: vec![call],
            }),
        }
    }

    let items: Vec<ProjEntry> = items
        .into_iter()
        .map(|it| ProjEntry {
            expr: win.over_w(ids, leaf, &placeholders, &it.expr),
            out: it.out,
        })
        .collect();
    let qualify: Vec<HirExpr> = qualify
        .iter()
        .map(|q| win.over_w(ids, leaf, &placeholders, q))
        .collect();

    // W: the narrowing projection every read of the input aliases.
    let w = leaf.project(input, win.w)?;
    // A call's value is an expression over columns its specification adds.
    let mut values: HashMap<ColId, HirExpr> = HashMap::new();
    // The rows: W under every ordered specification's values, then the cut. Every
    // later node carries the columns a window adds.
    let (ordered, unordered): (Vec<Spec>, Vec<Spec>) = specs.into_iter().partition(|s| !s.order.is_empty());
    let mut rows = Rc::clone(&w);
    for spec in ordered {
        rows = running_values(ids, rows, spec, &mut values)?;
    }
    if let Some((call, (limit, offset))) = cut {
        rows = RelExpr::top_n(rows, call.partition, call.order, limit, offset);
    }
    // Every unordered specification's `γ_P(W)`, joined onto an outer read of the
    // rows. One with no partition key is a keyless (cross) join against the one
    // global row.
    let outer = (!unordered.is_empty()).then(|| Read::of(ids, &rows));
    if let Some(outer) = &outer {
        rows = Rc::clone(&outer.rel);
        for spec in &unordered {
            let (rel, keys) = partition_values(ids, &w, spec, &mut values)?;
            let on = spec
                .partition
                .iter()
                .zip(keys)
                .map(|(&k, r)| BExpr::bin(outer.col(k), BinOp::Eq, BExpr::ColRef(r)))
                .collect();
            rows = RelExpr::join(rows, rel, JoinType::Inner, on)?;
        }
    }

    // A placeholder reads its value; a column of the rows reads the outer side,
    // where there is one.
    let rewrite = |e: &HirExpr| {
        let valued = e.rebuild(&mut |id| values.get(id).cloned().unwrap_or(BExpr::ColRef(*id)));
        match &outer {
            Some(outer) => valued.rebuild(&mut |id| BExpr::ColRef(outer.at.get(id).copied().unwrap_or(*id))),
            None => valued,
        }
    };
    let qualify = qualify.iter().map(rewrite).collect();
    let items = items
        .into_iter()
        .map(|it| ProjEntry { expr: rewrite(&it.expr), out: it.out })
        .collect();
    Ok(RelExpr::project(RelExpr::filter(rows, qualify)?, items))
}

/// The value of aggregate `(func, arg)` over a relation of columns `env`,
/// listed in `aggs` with the aggregates of the same reduce or window.
fn agg_value(
    ids: &ColIdGen,
    env: &[HirCol],
    is_global: bool,
    aggs: &mut Vec<HirAgg>,
    func: AggFunc,
    arg: Option<ColId>,
) -> Result<HirExpr, GnitzSqlError> {
    let agg = HirAgg::new(ids, func, arg.map_or(AggArg::Star, AggArg::All), env, is_global, aggs)?;
    let value = agg.finalize();
    aggs.push(agg);
    Ok(value)
}

/// `rows` under an ordered specification's running values, and each of its
/// calls' values into `values` under the call's placeholder id.
fn running_values(
    ids: &ColIdGen,
    rows: Rc<RelExpr>,
    spec: Spec,
    values: &mut HashMap<ColId, HirExpr>,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let Spec { partition, order, calls } = spec;
    let cols = rows.cols();
    let mut aggs = Vec::new();
    let (mut rank, mut dense_rank) = (None, None);
    let number = |col: &mut Option<HirCol>| {
        let def = || ColumnDef::new("_win", TypeCode::I64, false);
        BExpr::ColRef(col.get_or_insert_with(|| HirCol::new(ids.next(), def())).id)
    };
    for c in &calls {
        let value = match c.func {
            // A row's own frame holds the row, so no aggregate is one over no rows.
            WinFunc::Agg(func) => agg_value(ids, &cols, false, &mut aggs, func, c.arg)?,
            // ROW_NUMBER's order is total over a set, so each row is its own peer
            // group and its number is its RANK.
            WinFunc::Rank | WinFunc::RowNumber => number(&mut rank),
            WinFunc::DenseRank => number(&mut dense_rank),
        };
        values.insert(c.out.id, value);
    }
    Ok(RelExpr::window(
        rows,
        partition,
        order,
        HirAgg::physical(&aggs),
        rank,
        dense_rank,
    ))
}

/// An unordered specification's `γ_P(W)` and its key columns in partition order,
/// and each of its calls' values into `values` under the call's placeholder id.
fn partition_values(
    ids: &ColIdGen,
    w: &Rc<RelExpr>,
    spec: &Spec,
    values: &mut HashMap<ColId, HirExpr>,
) -> Result<(Rc<RelExpr>, Vec<ColId>), GnitzSqlError> {
    let wf = Read::of(ids, w);
    let keys: Vec<ColId> = spec.partition.iter().map(|k| wf.at[k]).collect();
    let mut aggs = Vec::new();
    for c in &spec.calls {
        let value = match c.func {
            WinFunc::Agg(func) => {
                let arg = c.arg.map(|a| wf.at[&a]);
                agg_value(ids, &wf.cols, keys.is_empty(), &mut aggs, func, arg)?
            }
            // Nothing orders the partition, so every row of it is first.
            _ => BExpr::LitInt(1),
        };
        values.insert(c.out.id, value);
    }
    let reduce = RelExpr::reduce(Rc::clone(&wf.rel), keys.clone(), HirAgg::physical(&aggs));
    Ok((reduce, keys))
}

#[cfg(test)]
#[path = "tests/window.rs"]
mod tests;
