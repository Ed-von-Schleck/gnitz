//! Window functions (`f(…) OVER (PARTITION BY … ORDER BY …)`), desugared at
//! bind into the join / reduce / top-N HIR the rest of the pipeline already
//! lowers. No operator is added.
//!
//! Let `W` be the relation the SELECT list projects (the FROM/WHERE tree, or
//! the grouped relation) narrowed to one column per distinct expression a window
//! operand, the SELECT list or QUALIFY reads. The body is one pipeline over it:
//!
//! 1. **The cut.** A `ROW_NUMBER` bounded by a QUALIFY conjunct (`<= n`, `< n`,
//!    `= n`) and read by nothing else is a per-partition top-N of `W`, which
//!    replaces `W` as the outer side below. Its partition is a reduce group key,
//!    so it takes any key but a float; its order takes any type and honours the
//!    written NULL placement.
//! 2. **The value relations.** Every other call belongs to a specification — its
//!    partition columns `P` and its order `O` — and each specification is one
//!    relation keyed on `(P, O)`, inner-joined onto the outer side:
//!    * no order (no ORDER BY, or an aggregate under an `UNBOUNDED PRECEDING AND
//!      UNBOUNDED FOLLOWING` frame): `γ_P(W)`, each aggregate per partition;
//!    * an order (the default `RANGE … CURRENT ROW` frame): `G = γ_{P,O}(W)`
//!      carries each aggregate per peer group, and `R = γ_{P,O}(G g1 ⋈ G g2 ON
//!      g1.P = g2.P ∧ g2.O ≤ g1.O)` folds every peer group up to and including
//!      the current one. `RANK` is `1 + Σ_{≤} cnt − own cnt`, `DENSE_RANK` the
//!      number of peer groups folded, and every aggregate the fold of its
//!      per-group value (AVG as `Σ sum / Σ count`). The `≤` band join matches
//!      every group with itself, so the fold is never empty and needs no
//!      null-fill; a multi-column order keys the band on its first column and
//!      refines it lexicographically in the residual.
//! 3. QUALIFY's remaining conjuncts filter the join, and the SELECT list projects it.
//!
//! Every value relation is computed over the whole of `W` and matches each outer
//! row exactly once, so cutting the outer side commutes with the joins.
//!
//! A frame is read for an aggregate only: a ranking function ignores it.
//!
//! `ROW_NUMBER` orders by its ORDER BY extended by the input's unique key. In a
//! value relation it is the count of peer groups at or before the current one,
//! which numbers rows only where each peer group is one row at weight 1 — so the
//! input must be a set with a unique key. The cut needs neither: over a bag each
//! copy of a row fills its own weight slot.
//!
//! A specification's keys are join keys, and carry rules of their own: no float,
//! provably NOT NULL (an inner join drops a NULL key), no string in the band
//! slot, and no 128-bit key after the first order key.
//!
//! The desugar is purely logical: `W` and `G` are shared subtrees read through
//! aliases, and the lowering's cut memo and collision rule decide what is read
//! in place and what is cut once to a hidden segment.
//!
//! Cost under incremental maintenance: a changed row re-emits the rows of its
//! partition at or after it (the whole partition for an unordered
//! specification), and an ordered specification stores the band join's output
//! under its fold — one row per ordered pair of peer groups of a partition.

use super::bind::{bind_projection, place_order_keys, ItemLeaf};
use super::{as_col, col_by_id, hircol_of, ColId, ColIdGen, HirAgg, HirCol, HirExpr, JoinType, ProjEntry, RelExpr};
use super::{TopNKey, Value};
use crate::agg::{agg_func_from_name, AggFunc};
use crate::ast_util::{classify_agg_shape, single_fn_name, unknown_function, CallSurface, PlainCall};
use crate::bind::{bind_conjuncts, bind_structural, output_column, LeafBinder};
use crate::error::GnitzSqlError;
use crate::ir::{BExpr, BinOp};
use crate::rules::{first_duplicate, reject_float_key};
use crate::tail::parse_order_key;
use gnitz_wire::{ColType, ColumnDef, FixedInt, TypeCode};
use sqlparser::ast::{
    Expr, Function, Ident, NamedWindowDefinition, NamedWindowExpr, Select, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowSpec, WindowType,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// A view body's SELECT list over `rel`, its pre-projection relation, with the
/// item each ORDER BY key sorts on. Window calls in the list, in QUALIFY and in
/// the ORDER BY keys are found by binding them; a body holding one is desugared
/// into joins and reduces. `leaf` is the body's own item leaf, `order_leaf` the
/// one its ORDER BY keys bind through; `ctx` names the surface.
pub(crate) fn bind_view_select_list<L: ItemLeaf>(
    ids: &ColIdGen,
    select: &Select,
    rel: Rc<RelExpr>,
    [leaf, order_leaf]: [&L; 2],
    ctx: &str,
    order_exprs: &[&Expr],
) -> Result<(Rc<RelExpr>, Vec<usize>), GnitzSqlError> {
    if let Some(name) = first_duplicate(select.named_window.iter().map(|d| d.0.value.as_str())) {
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
    let placed = place_order_keys(order_exprs, &mut items, ids, &WindowLeaf { inner: order_leaf, ..over })?;
    let qualify = match &select.qualify {
        Some(q) => bind_conjuncts(q, &WindowLeaf { aliases: &items, ..over })?,
        None => Vec::new(),
    };
    let win = state.into_inner();
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

/// One window call as written, its operands columns of `W`, and the column its
/// value is read through. `order` is empty for a whole-partition window.
struct Call {
    func: WinFunc,
    arg: Option<ColId>,
    partition: Vec<ColId>,
    order: Vec<TopNKey>,
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
        let ty = e.infer_ty_with(&|r| leaf.type_of(r));
        // A slot narrower than the register is written through a range check,
        // which is NULL for a value past it.
        let checked = FixedInt::from_type_code(ty.tc).is_some_and(|fi| !e.within_with(fi, &|r| leaf.type_of(r)));
        let nullable = checked || !e.never_null_with(&|r| leaf.is_nullable(r), &|r| leaf.type_of(r));
        let name = as_col(e)
            .and_then(|id| col_by_id(leaf.env(), id))
            .filter(|c| !c.def.is_hidden)
            .map_or_else(|| format!("_w{}", self.w.len()), |c| c.def.name.clone());
        let out = HirCol::new(ids.next(), ColumnDef::typed(name, ty, nullable));
        let id = out.id;
        self.w.push(ProjEntry { expr: e.clone(), out });
        id
    }

    /// `e` with every source reference replaced by its `W` column; a reference to
    /// one of `placeholders` stays.
    fn over_w<L: ItemLeaf>(
        &mut self,
        ids: &ColIdGen,
        leaf: &L,
        placeholders: &[ColId],
        e: &HirExpr,
    ) -> Result<HirExpr, GnitzSqlError> {
        e.try_rebuild(&mut |id| -> Result<HirExpr, GnitzSqlError> {
            Ok(BExpr::ColRef(if placeholders.contains(id) {
                *id
            } else {
                self.hoist(ids, leaf, &BExpr::ColRef(*id))
            }))
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

    /// The window specification a call names: written inline, or defined in
    /// the WINDOW clause (which may itself name another definition).
    fn resolve_spec<'f>(&'f self, over: &'f WindowType) -> Result<&'f WindowSpec, GnitzSqlError> {
        let inline = |s: &'f WindowSpec| {
            if s.window_name.is_some() {
                return Err(GnitzSqlError::Rejected(
                    "a window specification extending a named window (OVER (w …)) is not supported".into(),
                ));
            }
            Ok(s)
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
    /// value stands in for until the desugar joins it in. Two calls equal in
    /// function, argument and specification share one.
    fn bind_window_call(&self, f: &Function) -> Result<HirCol, GnitzSqlError> {
        let over = f.over.as_ref().expect("bind_window_call receives a windowed call");
        let (func, arg_expr) = classify_window_call(f)?;
        let spec = self.resolve_spec(over)?;
        // Operands bind through the body's leaf, so an aggregate argument in a
        // grouped body resolves to its finalize and a nested OVER is refused
        // there.
        let bind = |e: &Expr| bind_structural(e, self.inner);
        let mut arg = arg_expr.map(bind).transpose()?;
        // `window_name` is rejected by `resolve_spec`, which is what hands `spec`
        // over. Exhaustive (no `..`): a future frame or exclusion field stops the
        // build rather than being dropped into a maintained view.
        let WindowSpec {
            window_name: _,
            partition_by,
            order_by,
            window_frame,
        } = spec;
        let partition = partition_by.iter().map(bind).collect::<Result<Vec<_>, _>>()?;
        let mut order = Vec::with_capacity(order_by.len());
        for o in order_by {
            let (expr, desc, nulls_first) = parse_order_key(o, "window ORDER BY")?;
            order.push((bind(expr)?, desc, nulls_first));
        }
        // A ranking function ignores its frame; an aggregate over the whole
        // partition reads no order.
        if matches!(func, WinFunc::Agg(_)) && frame_is_whole(window_frame.as_ref())? {
            order.clear();
        }
        if matches!(func, WinFunc::Rank | WinFunc::DenseRank) && order.is_empty() {
            return Err(GnitzSqlError::Rejected(
                "RANK / DENSE_RANK need an ORDER BY in their window specification".into(),
            ));
        }
        // Over an argument that is never NULL, COUNT(e) is COUNT(*).
        let never_null = |e: &HirExpr| e.never_null_with(&|r| self.inner.is_nullable(r), &|r| self.inner.type_of(r));
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
        let order: Vec<TopNKey> = order
            .iter()
            .map(|(e, desc, nulls_first)| TopNKey {
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
            ColumnDef::typed(format!("_win{}", st.calls.len()), ty, is_nullable).hidden(),
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

impl<L: ItemLeaf> LeafBinder<ColId> for WindowLeaf<'_, L> {
    fn bind_node(&self, e: &Expr) -> Option<HirExpr> {
        self.inner.bind_node(e)
    }
    /// A source column outranks a SELECT alias of the same name; the alias
    /// table is consulted only where the body's own scope has nothing, and where
    /// it has no single answer the body's own error stands.
    fn bind_column(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        let err = match self.inner.bind_column(e) {
            Ok(bound) => return Ok(bound),
            Err(err) => err,
        };
        match output_column(e, self.aliases.iter().map(|it| &it.out.def)) {
            Ok(Some(at)) => Ok(self.aliases[at].expr.clone()),
            _ => Err(err),
        }
    }
    fn bind_function(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        self.inner.bind_function(f)
    }
    /// The one context that admits a window call: it binds to the placeholder
    /// column its value stands in for until the desugar joins it in.
    fn bind_window(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        Ok(BExpr::ColRef(self.bind_window_call(f)?.id))
    }
    /// This leaf's own placeholders; everything else is the body's.
    fn is_nullable(&self, r: &ColId) -> bool {
        self.placeholder(r)
            .map_or_else(|| self.inner.is_nullable(r), |(_, nullable)| nullable)
    }
    fn type_of(&self, r: &ColId) -> ColType {
        self.placeholder(r).map_or_else(|| self.inner.type_of(r), |(ty, _)| ty)
    }
    fn bind_subquery(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        self.inner.bind_subquery(e)
    }
}

impl<L: ItemLeaf> ItemLeaf for WindowLeaf<'_, L> {
    fn env(&self) -> &[HirCol] {
        self.inner.env()
    }
    fn project(&self, source: Rc<RelExpr>, items: Vec<ProjEntry>) -> Result<Rc<RelExpr>, GnitzSqlError> {
        self.inner.project(source, items)
    }
    fn wildcard_cols(&self, qualifier: Option<&str>) -> Result<Option<Vec<&HirCol>>, GnitzSqlError> {
        self.inner.wildcard_cols(qualifier)
    }
}

/// Classify a windowed call into its function and its unbound argument.
fn classify_window_call(f: &Function) -> Result<(WinFunc, Option<&Expr>), GnitzSqlError> {
    let call = PlainCall::check(f, CallSurface::Window)?;
    let name = single_fn_name(f).ok_or_else(|| unknown_function(f))?;
    let ranking = match name.to_ascii_lowercase().as_str() {
        "rank" => Some(WinFunc::Rank),
        "dense_rank" => Some(WinFunc::DenseRank),
        "row_number" => Some(WinFunc::RowNumber),
        _ => None,
    };
    let Some(func) = ranking else {
        if agg_func_from_name(name).is_none() {
            return Err(GnitzSqlError::Rejected(format!(
                "{}: not supported as a window function",
                name.to_ascii_uppercase()
            )));
        }
        let (agg, arg) = classify_agg_shape(call)?;
        return Ok((WinFunc::Agg(agg), arg));
    };
    call.args(&name.to_ascii_uppercase(), (0, Some(0)))?;
    Ok((func, None))
}

/// Whether a frame covers the partition whole (`true`) or folds it up to the
/// current row's peers (`false`, the default frame) — the two frames accepted.
fn frame_is_whole(frame: Option<&WindowFrame>) -> Result<bool, GnitzSqlError> {
    let Some(frame) = frame else {
        return Ok(false);
    };
    let end = frame.end_bound.as_ref().unwrap_or(&WindowFrameBound::CurrentRow);
    match (&frame.start_bound, end) {
        (WindowFrameBound::Preceding(None), WindowFrameBound::Following(None)) => Ok(true),
        (WindowFrameBound::Preceding(None), WindowFrameBound::CurrentRow) => match frame.units {
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
fn effective_order(call: &Call, key: Option<&[ColId]>) -> Vec<TopNKey> {
    let tiebreak = key
        .filter(|_| call.func == WinFunc::RowNumber)
        .into_iter()
        .flatten()
        .map(|&col| TopNKey { col, desc: false, nulls_first: false });
    let mut order: Vec<TopNKey> = Vec::new();
    for k in call.order.iter().copied().chain(tiebreak) {
        if !call.partition.contains(&k.col) && !order.iter().any(|o| o.col == k.col) {
            order.push(k);
        }
    }
    order
}

/// The slots `offset .. offset + limit` a QUALIFY conjunct `rn <= n`, `rn < n`
/// or `rn = n` over placeholder `rn` keeps of each partition, the literal on
/// either side; `None` for any other predicate, or a bound keeping no slot.
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
    let (offset, limit) = match op {
        BinOp::Le => (0, n),
        BinOp::Lt => (0, n.saturating_sub(1)),
        BinOp::Eq => (n.saturating_sub(1), 1),
        _ => return None,
    };
    (n >= 1 && limit >= 1).then_some((offset as u64, limit as u64))
}

/// The body's top-N cut: a ROW_NUMBER with an order, bounded by one QUALIFY
/// conjunct and read by nothing else — its index in `calls`, that conjunct's in
/// `qualify`, and the slots `offset .. offset + limit` the bound keeps.
fn top_n_cut(
    calls: &[Call],
    orders: &[Vec<TopNKey>],
    items: &[ProjEntry],
    qualify: &[HirExpr],
) -> Option<(usize, usize, u64, u64)> {
    calls.iter().enumerate().find_map(|(ci, call)| {
        if call.func != WinFunc::RowNumber || orders[ci].is_empty() {
            return None;
        }
        let rn = call.out.id;
        let reads = |e: &HirExpr| {
            let mut found = false;
            e.for_each_ref(&mut |id| found |= *id == rn);
            found
        };
        let (qi, (offset, limit)) = qualify
            .iter()
            .enumerate()
            .find_map(|(i, c)| Some((i, row_number_bound(c, rn)?)))?;
        let others = qualify.iter().enumerate().filter(|(i, _)| *i != qi).map(|(_, c)| c);
        let mut elsewhere = items.iter().map(|it| &it.expr).chain(others);
        (!elsewhere.any(reads)).then_some((ci, qi, offset, limit))
    })
}

/// Where an ORDER BY key lands in the band join: the one range slot (the first
/// key), the lexicographic residual (every later key), or an equality (hash)
/// key.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeySlot {
    Hash,
    Band,
    Residual,
}

/// The join-key rules a value relation's partition / order key must satisfy,
/// stated with the clause named, rather than surfacing later as a rejection of a
/// join the user never wrote.
fn check_key(def: &ColumnDef, role: &str, slot: KeySlot) -> Result<(), GnitzSqlError> {
    let tc = def.ty.tc;
    reject_float_key(tc, None, role)?;
    if slot == KeySlot::Band && tc.is_german_string() {
        return Err(GnitzSqlError::Rejected(format!(
            "{role}: a string cannot be the first ORDER BY key; its content hash is not \
             order-preserving, so it cannot bound the band join the window folds over"
        )));
    }
    if slot == KeySlot::Residual && tc.is_wide_int() {
        return Err(GnitzSqlError::Rejected(format!(
            "{role}: a 128-bit key can only be the first ORDER BY key; a later key is \
             compared in an expression, which a 128-bit value cannot enter"
        )));
    }
    if def.is_nullable {
        return Err(GnitzSqlError::Rejected(format!(
            "{role}: the key must be provably NOT NULL (a NOT NULL column, or an expression over \
             NOT NULL columns) — the window is keyed on it, and a NULL key neither groups nor matches"
        )));
    }
    Ok(())
}

/// One window specification of the desugar: its keys as `W` columns, each order
/// key with whether it descends, and the calls it serves.
struct Spec {
    partition: Vec<ColId>,
    order: Vec<(ColId, bool)>,
    calls: Vec<Call>,
}

impl Spec {
    /// The partition columns, then the order columns.
    fn keys(&self) -> impl Iterator<Item = ColId> + '_ {
        self.partition.iter().copied().chain(self.order.iter().map(|&(c, _)| c))
    }
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
        let rel = RelExpr::alias(ids, Rc::clone(input));
        let cols = rel.cols();
        let at = input
            .cols()
            .iter()
            .map(|c| c.id)
            .zip(cols.iter().map(|c| c.id))
            .collect();
        Read { rel, cols, at }
    }
    fn col(&self, src: ColId) -> HirExpr {
        BExpr::ColRef(self.at[&src])
    }
}

/// Rewrite a windowed body: cut `W` by the QUALIFY-bounded ROW_NUMBER if there is
/// one, join every remaining specification's value relation onto the outer read,
/// filter by what is left of QUALIFY and project the SELECT list.
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
    let mut orders: Vec<Vec<TopNKey>> = calls.iter().map(|c| effective_order(c, key.as_deref())).collect();

    let cut = match top_n_cut(&calls, &orders, &items, &qualify) {
        Some((ci, qi, offset, limit)) => {
            let (call, order) = (calls.remove(ci), orders.remove(ci));
            qualify.remove(qi);
            // A top-N's partition is a reduce group key: GROUP BY's rule.
            for &p in &call.partition {
                reject_float_key(def_of(&win.w, p).ty.tc, None, "window PARTITION BY")?;
            }
            Some((call.partition, order, limit, offset))
        }
        None => None,
    };

    let mut specs: Vec<Spec> = Vec::new();
    for (call, order) in calls.into_iter().zip(orders) {
        if call.func == WinFunc::RowNumber && key.is_none() {
            return Err(GnitzSqlError::Rejected(
                "ROW_NUMBER needs an input with a unique row key (a table, or a grouped or DISTINCT \
                 body); over a join body, a stream, a UNION ALL or a view keyed by a join key its rows \
                 have no total order — use RANK or DENSE_RANK, give it an ORDER BY and bound it in \
                 QUALIFY without selecting it, or window over a view of the join"
                    .into(),
            ));
        }
        for &p in &call.partition {
            check_key(def_of(&win.w, p), "window PARTITION BY", KeySlot::Hash)?;
        }
        for (i, k) in order.iter().enumerate() {
            let role = if call.order.iter().any(|written| written.col == k.col) {
                "window ORDER BY"
            } else {
                "ROW_NUMBER's tiebreak (the input's row key)"
            };
            let slot = if i == 0 { KeySlot::Band } else { KeySlot::Residual };
            check_key(def_of(&win.w, k.col), role, slot)?;
        }
        let order: Vec<(ColId, bool)> = order.iter().map(|k| (k.col, k.desc)).collect();
        match specs
            .iter_mut()
            .find(|s| s.partition == call.partition && s.order == order)
        {
            Some(spec) => spec.calls.push(call),
            None => specs.push(Spec {
                partition: call.partition.clone(),
                order,
                calls: vec![call],
            }),
        }
    }

    let items = items
        .into_iter()
        .map(|it| {
            Ok(ProjEntry {
                expr: win.over_w(ids, leaf, &placeholders, &it.expr)?,
                out: it.out,
            })
        })
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;
    let qualify = qualify
        .iter()
        .map(|q| win.over_w(ids, leaf, &placeholders, q))
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;

    // W: the narrowing projection every read of the input aliases.
    let w = leaf.project(input, win.w)?;
    let base = match cut {
        Some((partition, order, limit, offset)) => RelExpr::top_n(Rc::clone(&w), partition, order, limit, offset),
        None => Rc::clone(&w),
    };
    // Every specification's value relation, joined onto the outer read. An
    // unordered specification with no partition key is a keyless (cross) join
    // against the one global row.
    let outer = Read::of(ids, &base);
    let mut cur = Rc::clone(&outer.rel);
    for spec in &specs {
        let (rel, keys) = spec_relation(ids, &w, spec)?;
        let on = spec
            .keys()
            .zip(keys)
            .map(|(k, r)| BExpr::bin(outer.col(k), BinOp::Eq, BExpr::ColRef(r)))
            .collect();
        cur = RelExpr::join(cur, rel, JoinType::Inner, on)?;
    }

    // A `W` reference reads the outer side; a placeholder is the id its value
    // column was minted under, so it already names a column of the join.
    let to_outer = |e: &HirExpr| {
        e.try_rebuild(&mut |id| Ok::<_, GnitzSqlError>(BExpr::ColRef(outer.at.get(id).copied().unwrap_or(*id))))
    };
    let qualify = qualify.iter().map(to_outer).collect::<Result<Vec<_>, _>>()?;
    let items = items
        .into_iter()
        .map(|it| Ok(ProjEntry { expr: to_outer(&it.expr)?, out: it.out }))
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;
    Ok(RelExpr::project(RelExpr::filter(cur, qualify)?, items))
}

/// A reduce's aggregate list, with each aggregate's index handed back so a value
/// can address it.
struct Aggs<'a> {
    env: &'a [HirCol],
    ids: &'a ColIdGen,
    is_global: bool,
    list: Vec<HirAgg>,
}

impl Aggs<'_> {
    fn slot(&mut self, func: AggFunc, arg: Option<ColId>) -> Result<usize, GnitzSqlError> {
        let agg = HirAgg::new(self.ids, func, arg, self.env, self.is_global, &self.list)?;
        // `HirAgg::new` shares physical columns, so `COUNT(x)` over a NOT NULL `x` is
        // the `COUNT(*)` already listed.
        let same = |a: &HirAgg| {
            a.func == func
                && a.out.col.id == agg.out.col.id
                && a.companion.as_ref().map(|c| c.col.id) == agg.companion.as_ref().map(|c| c.col.id)
        };
        Ok(match self.list.iter().position(same) {
            Some(i) => i,
            None => {
                self.list.push(agg);
                self.list.len() - 1
            }
        })
    }

    /// Aggregate `(func, arg)`, registered on first sight, as a value column.
    fn value(&mut self, func: AggFunc, arg: Option<ColId>) -> Result<Value, GnitzSqlError> {
        let slot = self.slot(func, arg)?;
        Ok(self.list[slot].as_value())
    }
}

/// `rel`'s `keys` passed through, then each value as the column of its id.
fn present(rel: Rc<RelExpr>, keys: &[ColId], values: Vec<(ColId, Value)>) -> Rc<RelExpr> {
    let cols = rel.cols();
    let mut items = RelExpr::passthrough_items(keys.iter().map(|&k| hircol_of(&cols, k).clone()));
    items.extend(values.into_iter().map(|(id, (expr, ty, nullable))| ProjEntry {
        expr,
        out: HirCol::new(id, ColumnDef::typed("_win", ty, nullable)),
    }));
    RelExpr::project(rel, items)
}

/// One specification's value relation — keyed on the specification's keys, one
/// column per call under the call's placeholder id — and its key columns in
/// `spec.keys()` order.
fn spec_relation(ids: &ColIdGen, w: &Rc<RelExpr>, spec: &Spec) -> Result<(Rc<RelExpr>, Vec<ColId>), GnitzSqlError> {
    let wf = Read::of(ids, w);
    let keys: Vec<ColId> = spec.keys().map(|k| wf.at[&k]).collect();
    let arg = |c: &Call| c.arg.map(|a| wf.at[&a]);
    let mut g_aggs = Aggs {
        env: &wf.cols,
        ids,
        is_global: keys.is_empty(),
        list: Vec::new(),
    };

    if spec.order.is_empty() {
        let mut values = Vec::with_capacity(spec.calls.len());
        for c in &spec.calls {
            values.push((
                c.out.id,
                match c.func {
                    WinFunc::Agg(func) => g_aggs.value(func, arg(c))?,
                    // Nothing orders the partition, so every row of it is first.
                    _ => (BExpr::LitInt(1), ColType::of(TypeCode::I64), false),
                },
            ));
        }
        let g = RelExpr::reduce(Rc::clone(&wf.rel), keys.clone(), HirAgg::physical(&g_aggs.list));
        return Ok((present(g, &keys, values), keys));
    }

    // G: one row per peer group, carrying what each call folds. Over a set and a
    // total order every peer group is one row, so a ROW_NUMBER, like a DENSE_RANK,
    // counts groups and reads nothing of one.
    let mut carried: Vec<Vec<usize>> = Vec::with_capacity(spec.calls.len());
    for c in &spec.calls {
        carried.push(match c.func {
            WinFunc::Agg(AggFunc::Avg) => {
                vec![g_aggs.slot(AggFunc::Sum, arg(c))?, g_aggs.slot(AggFunc::Count, arg(c))?]
            }
            WinFunc::Agg(func) => vec![g_aggs.slot(func, arg(c))?],
            WinFunc::Rank => vec![g_aggs.slot(AggFunc::Count, None)?],
            WinFunc::DenseRank | WinFunc::RowNumber => vec![],
        });
    }
    let g_cols: Vec<ColId> = g_aggs.list.iter().map(|_| ids.next()).collect();
    let g = present(
        RelExpr::reduce(Rc::clone(&wf.rel), keys.clone(), HirAgg::physical(&g_aggs.list)),
        &keys,
        g_cols
            .iter()
            .copied()
            .zip(g_aggs.list.iter().map(HirAgg::as_value))
            .collect(),
    );

    // The band self-join: g1 is the current peer group, g2 every group of its
    // partition at or before it under each key's own direction.
    let (g1, g2) = (Read::of(ids, &g), Read::of(ids, &g));
    let cmp = |k: ColId, op: BinOp| BExpr::bin(g2.col(k), op, g1.col(k));
    let weak = |desc: bool| if desc { BinOp::Ge } else { BinOp::Le };
    let strict = |desc: bool| if desc { BinOp::Gt } else { BinOp::Lt };
    let np = spec.partition.len();
    let dirs = || keys[np..].iter().copied().zip(spec.order.iter().map(|&(_, desc)| desc));
    let mut on: Vec<HirExpr> = keys[..np].iter().map(|&k| cmp(k, BinOp::Eq)).collect();
    let (k0, desc0) = dirs().next().expect("an ordered specification has an order key");
    on.push(cmp(k0, weak(desc0)));
    if spec.order.len() > 1 {
        // `g2.O ≤lex g1.O`, which the range slot above bounds on its first key.
        let lex = dirs().rev().fold(None, |rest: Option<HirExpr>, (k, desc)| {
            Some(match rest {
                None => cmp(k, weak(desc)),
                Some(rest) => {
                    let tie = BExpr::bin(cmp(k, BinOp::Eq), BinOp::And, rest);
                    BExpr::bin(cmp(k, strict(desc)), BinOp::Or, tie)
                }
            })
        });
        on.extend(lex);
    }
    let band = RelExpr::join(Rc::clone(&g1.rel), Rc::clone(&g2.rel), JoinType::Inner, on)?;
    let band_cols = band.cols();

    // R: g2's carried values folded over the band, keyed by g1's group.
    let mut r_keys: Vec<ColId> = keys.iter().map(|k| g1.at[k]).collect();
    let mut r_aggs = Aggs {
        env: &band_cols,
        ids,
        is_global: false,
        list: Vec::new(),
    };
    let mut values = Vec::with_capacity(spec.calls.len());
    for (c, slots) in spec.calls.iter().zip(&carried) {
        let of_g2 = |slot: usize| Some(g2.at[&g_cols[slot]]);
        values.push((
            c.out.id,
            match c.func {
                WinFunc::Agg(AggFunc::Avg) => {
                    let (sum, _, nullable) = r_aggs.value(AggFunc::Sum, of_g2(slots[0]))?;
                    let (cnt, _, _) = r_aggs.value(AggFunc::Sum, of_g2(slots[1]))?;
                    (
                        crate::agg::finalize_agg_bexpr(sum, Some(cnt), AggFunc::Avg),
                        ColType::of(TypeCode::F64),
                        nullable,
                    )
                }
                WinFunc::Agg(func @ (AggFunc::Min | AggFunc::Max)) => r_aggs.value(func, of_g2(slots[0]))?,
                WinFunc::Agg(_) => r_aggs.value(AggFunc::Sum, of_g2(slots[0]))?,
                // Rows before the current group: the fold less the group's own count,
                // which is a column of g1 and so a group key of R.
                WinFunc::Rank => {
                    let own = g1.at[&g_cols[slots[0]]];
                    if !r_keys.contains(&own) {
                        r_keys.push(own);
                    }
                    let (folded, _, _) = r_aggs.value(AggFunc::Sum, of_g2(slots[0]))?;
                    let before = BExpr::bin(folded, BinOp::Sub, BExpr::ColRef(own));
                    (
                        BExpr::bin(before, BinOp::Add, BExpr::LitInt(1)),
                        ColType::of(TypeCode::I64),
                        false,
                    )
                }
                // The band holds one row per group at or before the current one.
                WinFunc::DenseRank | WinFunc::RowNumber => r_aggs.value(AggFunc::Count, None)?,
            },
        ));
    }
    let r = RelExpr::reduce(band, r_keys.clone(), HirAgg::physical(&r_aggs.list));
    let keys = r_keys[..keys.len()].to_vec();
    Ok((present(r, &keys, values), keys))
}

#[cfg(test)]
#[path = "tests/window.rs"]
mod tests;
