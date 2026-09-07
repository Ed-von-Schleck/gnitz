//! Window functions (`f(…) OVER (PARTITION BY … ORDER BY …)`), desugared at
//! bind into the join / reduce HIR the rest of the pipeline already lowers. No
//! operator is added: a window value is an aggregate over the rows of the
//! body's own pre-projection relation that share the current row's partition
//! key (and, under an ORDER BY, precede it), joined back onto every row.
//!
//! Let `W` be the relation the SELECT list projects (the FROM/WHERE tree, or
//! the grouped relation) narrowed to the columns anything references, read
//! through one `Alias` as the outer side and through another per window
//! specification. For a specification with partition columns `P`:
//!
//! * **whole partition** (no ORDER BY, or an `UNBOUNDED PRECEDING AND UNBOUNDED
//!   FOLLOWING` frame): `G = γ_P(W)` carries each aggregate per partition, and the
//!   output is `W ⋈_P G`.
//! * **cumulative** (an ORDER BY `O`; the default `RANGE … CURRENT ROW` frame):
//!   `G = γ_{P,O}(W)` carries each aggregate per peer group, `R = γ_{P,O}(G g1 ⋈
//!   G g2 ON g1.P = g2.P ∧ g2.O ≤ g1.O)` folds every peer group up to and
//!   including the current one, and the output is `W ⋈_{P,O} R`. `RANK` is
//!   `1 + Σ_{≤} cnt − own cnt`, `DENSE_RANK` the number of peer groups folded, and
//!   every aggregate is the fold of its per-group value (AVG as `Σ sum / Σ
//!   count`). The `≤` band join matches every group with itself, so the fold is
//!   never empty and needs no null-fill; a multi-column ORDER BY keys the band on
//!   its first column and refines it lexicographically in the residual.
//!
//! `ROW_NUMBER` is `RANK` over the ORDER BY extended by the input's row key (its
//! primary key, or a grouped body's group key), which is what makes it a
//! function of the Z-set: two rows identical in that extended order are one
//! element and share a number. An input whose key is a join key is rejected.
//!
//! The desugar is purely logical: `W` and `G` are shared subtrees read through
//! aliases, and the lowering's cut memo and collision rule decide what is read
//! in place (a bare table) and what is cut once to a hidden segment. Partition
//! and order keys are join keys of these shapes, so they carry the join-key
//! rules: no float, no NULL, and no string in the band slot.
//!
//! Cost, inherent to the semantics under incremental maintenance: one changed
//! row changes a partition-level value, so the output delta retracts and
//! re-emits every row of that partition; the cumulative shape additionally
//! holds the band self-join's traces over `G`, one row per peer group.

use super::bind::{bind_projection, hir_ref_nullable, type_of, ItemLeaf};
use super::{as_col, col_by_id, ColId, ColIdGen, HirAgg, HirCol, HirExpr, HirRef, JoinType, ProjEntry, RelExpr};
use crate::agg::default_agg_name;
use crate::ast_util::{
    agg_func_from_name, classify_agg_shape, peel_nested, reject_fn_qualifiers, single_fn_name, unknown_function,
};
use crate::bind::{bind_structural, LeafBinder};
use crate::error::GnitzSqlError;
use crate::ir::{AggFunc, BExpr, BinOp};
use crate::validate::{reject_duplicate_projection_names, reject_float_key_of};
use gnitz_core::{ColumnDef, TypeCode};
use sqlparser::ast::{
    Expr, Function, FunctionArguments, Ident, NamedWindowDefinition, NamedWindowExpr, OrderByExpr, OrderByOptions,
    Select, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowSpec, WindowType,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// Bind a SELECT list carrying window calls (and its QUALIFY) over `rel`, the
/// body's pre-projection relation, and desugar the windows into joins and
/// reduces. `leaf` is the body's own item leaf; `ctx` names the surface.
pub(crate) fn bind_window_final<L: ItemLeaf>(
    ids: &ColIdGen,
    select: &Select,
    rel: Rc<RelExpr>,
    leaf: &L,
    ctx: &str,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let wleaf = WindowLeaf {
        inner: leaf,
        ids,
        named: &select.named_window,
        row_key: rel.row_key(),
        state: RefCell::new(Windows::default()),
        aliases: RefCell::new(Vec::new()),
    };
    let items = bind_projection(&select.projection, &wleaf, ids, ctx)?;
    reject_duplicate_projection_names(&select.projection, items.iter().map(|e| &e.out.def), ctx)?;
    // The table exists so `QUALIFY rn = 1` can reach `… AS rn`, and the QUALIFY
    // bind below is the only read of it — published only now, so the SELECT list
    // itself does not see its own names.
    let qualify = match &select.qualify {
        Some(q) => {
            *wleaf.aliases.borrow_mut() = items
                .iter()
                .map(|it| (it.out.def.name.clone(), it.expr.clone()))
                .collect();
            Some(bind_structural(q, &wleaf)?)
        }
        None => None,
    };
    let win = wleaf.state.into_inner();
    desugar(ids, rel, leaf.env(), items, qualify, win)
}

// ── Binding ─────────────────────────────────────────────────────────────────────

/// A window function: an aggregate, or one of the two ranking functions.
/// `ROW_NUMBER` is not one of them — it is `RANK` over an ORDER BY extended by
/// the input's row key, an extension [`WindowLeaf::bind_window_call`] performs,
/// so nothing past the bind distinguishes the two.
#[derive(Clone, Copy, PartialEq, Debug)]
enum WinFunc {
    Agg(AggFunc),
    Rank,
    DenseRank,
}

/// A window specification. `K` is the operand representation: the bound
/// expression over the body's scope at bind, the `W` column after hoisting.
/// `order` is empty for a whole-partition window; each entry is the key and
/// whether it ascends.
struct Spec<K> {
    partition: Vec<K>,
    order: Vec<(K, bool)>,
}

/// One window call: its specification, function, argument (represented as in
/// [`Spec`]), and the placeholder column its value stands in for until the
/// desugar joins it in.
struct Call<K> {
    spec: usize,
    func: WinFunc,
    arg: Option<K>,
    out: HirCol,
}

#[derive(Default)]
struct Windows {
    specs: Vec<Spec<HirExpr>>,
    calls: Vec<Call<HirExpr>>,
}

/// The leaf a windowed SELECT list binds through: every window call becomes a
/// placeholder column, everything else goes to the body's own leaf.
struct WindowLeaf<'a, L> {
    inner: &'a L,
    ids: &'a ColIdGen,
    named: &'a [NamedWindowDefinition],
    /// The input's row key, when it has one — `ROW_NUMBER`'s tiebreak.
    row_key: Option<Vec<ColId>>,
    state: RefCell<Windows>,
    /// The SELECT list's own output names, filled once the projection is bound,
    /// so `QUALIFY rn = 1` reaches `ROW_NUMBER() OVER (…) AS rn`. Empty while
    /// the projection binds, which is what keeps an alias invisible to itself
    /// and lets a source column of the same name win.
    aliases: RefCell<Vec<(String, HirExpr)>>,
}

impl<L: ItemLeaf> WindowLeaf<'_, L> {
    /// The `(type, nullable)` of the placeholder `r` names, when it names one —
    /// a window value's declaration, which the body's own leaf does not know.
    fn placeholder(&self, r: &HirRef) -> Option<(TypeCode, bool)> {
        let HirRef::Col(id) = r else { return None };
        self.state
            .borrow()
            .calls
            .iter()
            .find(|c| c.out.id == *id)
            .map(|c| (c.out.def.type_code, c.out.def.is_nullable))
    }

    /// The function the call behind placeholder `id` computes.
    fn func_of(&self, id: ColId) -> Option<WinFunc> {
        self.state
            .borrow()
            .calls
            .iter()
            .find(|c| c.out.id == id)
            .map(|c| c.func)
    }

    fn never_null(&self, e: &HirExpr) -> bool {
        e.never_null_with(&|r| self.inner.is_nullable(r))
    }

    /// The window specification a call names: written inline, or defined in
    /// the WINDOW clause (which may itself name another definition).
    fn resolve_spec<'f>(&'f self, over: &'f WindowType) -> Result<&'f WindowSpec, GnitzSqlError> {
        let inline = |s: &'f WindowSpec| {
            if s.window_name.is_some() {
                return Err(GnitzSqlError::Unsupported(
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
                    GnitzSqlError::Plan(format!("window '{}' is not defined in the WINDOW clause", name.value))
                })?;
            match &def.1 {
                NamedWindowExpr::WindowSpec(s) => return inline(s),
                NamedWindowExpr::NamedWindow(next) => name = next,
            }
        }
        Err(GnitzSqlError::Plan(format!(
            "window '{}' is defined in terms of itself",
            name.value
        )))
    }

    /// Bind one `f(…) OVER (…)` call and hand back the placeholder column its
    /// value stands in for until the desugar joins it in.
    fn bind_window_call(&self, f: &Function) -> Result<HirCol, GnitzSqlError> {
        let over = f.over.as_ref().expect("bind_window_call receives a windowed call");
        let (func, arg_expr, row_number) = classify_window_call(f)?;
        let spec = self.resolve_spec(over)?;
        // Operands bind through the body's leaf, so an aggregate argument in a
        // grouped body resolves to its finalize and a nested OVER is refused
        // there.
        let arg = arg_expr.map(|e| bind_structural(e, self.inner)).transpose()?;
        // `window_name` is rejected by `resolve_spec`, which is what hands `spec`
        // over. Exhaustive (no `..`): a future frame or exclusion field stops the
        // build rather than being dropped into a maintained view.
        let WindowSpec {
            window_name: _,
            partition_by,
            order_by,
            window_frame,
        } = spec;
        let partition = partition_by
            .iter()
            .map(|e| bind_structural(e, self.inner))
            .collect::<Result<Vec<_>, _>>()?;
        let mut order = Vec::with_capacity(order_by.len());
        for o in order_by {
            let OrderByExpr { expr, options, with_fill } = o;
            if with_fill.is_some() {
                return Err(GnitzSqlError::Unsupported(
                    "window ORDER BY: WITH FILL is not supported".into(),
                ));
            }
            // `nulls_first` is inert: `check_key` below refuses a key that is not
            // provably NOT NULL, so this ORDER BY sees no NULL to place.
            let OrderByOptions { asc, nulls_first: _ } = options;
            order.push((bind_structural(expr, self.inner)?, asc.unwrap_or(true)));
        }
        if !frame_is_cumulative(window_frame.as_ref(), !order.is_empty())? {
            order.clear();
        }
        let written = order.len();
        if row_number {
            let Some(key) = &self.row_key else {
                return Err(GnitzSqlError::Unsupported(
                    "ROW_NUMBER needs an input with a unique row key (a table, or a grouped or \
                     DISTINCT body); over a join body, a stream, or a view keyed by a join key its \
                     ties have no order — use RANK or DENSE_RANK, or window over a view of the join"
                        .into(),
                ));
            };
            // The row key makes the order total, so only identical rows share a
            // number. A key the specification already fixes is dropped: it adds
            // nothing, and without it a window keyed by the row key stays whole.
            let already_fixed = |e: &HirExpr| partition.contains(e) || order.iter().any(|(written, _)| written == e);
            let tiebreak: Vec<HirExpr> = key
                .iter()
                .map(|&id| BExpr::ColRef(HirRef::Col(id)))
                .filter(|e| !already_fixed(e))
                .collect();
            order.extend(tiebreak.into_iter().map(|e| (e, true)));
        } else if matches!(func, WinFunc::Rank | WinFunc::DenseRank) && order.is_empty() {
            return Err(GnitzSqlError::Unsupported(
                "RANK / DENSE_RANK need an ORDER BY in their window specification".into(),
            ));
        }
        for e in &partition {
            self.check_key(e, "window PARTITION BY", KeySlot::Hash)?;
        }
        for (i, (e, _)) in order.iter().enumerate() {
            let role = if i < written {
                "window ORDER BY"
            } else {
                "ROW_NUMBER's tiebreak (the input's row key)"
            };
            self.check_key(e, role, if i == 0 { KeySlot::Band } else { KeySlot::Residual })?;
        }
        let (type_code, is_nullable) = self.call_typing(func, arg.as_ref())?;

        let mut st = self.state.borrow_mut();
        let spec_idx = match st
            .specs
            .iter()
            .position(|s| s.partition == partition && s.order == order)
        {
            Some(i) => i,
            None => {
                st.specs.push(Spec { partition, order });
                st.specs.len() - 1
            }
        };
        // Addressed by id only: the desugar replaces every reference to it.
        let out = HirCol::new(
            self.ids.next(),
            ColumnDef::new(format!("_win{}", st.calls.len()), type_code, is_nullable).hidden(),
        );
        st.calls.push(Call {
            spec: spec_idx,
            func,
            arg,
            out: out.clone(),
        });
        Ok(out)
    }

    /// The join-key rules a partition / order key must satisfy, stated at bind
    /// with the clause named, rather than surfacing later as a rejection of a
    /// join the user never wrote.
    fn check_key(&self, e: &HirExpr, role: &str, slot: KeySlot) -> Result<(), GnitzSqlError> {
        let tc = e.infer_type_with(&|r| self.type_of(r));
        if tc.is_float() {
            return Err(reject_float_key_of("a float-valued expression", role));
        }
        if slot == KeySlot::Band && tc.is_german_string() {
            return Err(GnitzSqlError::Unsupported(format!(
                "{role}: a string cannot be the first ORDER BY key; its content hash is not \
                 order-preserving, so it cannot bound the band join the window folds over"
            )));
        }
        if !self.never_null(e) {
            return Err(GnitzSqlError::Unsupported(format!(
                "{role}: the key must be provably NOT NULL (a NOT NULL column, or an expression over \
                 NOT NULL columns) — the window is keyed on it, and a NULL key neither groups nor matches"
            )));
        }
        Ok(())
    }

    /// A call's output type and nullability. The type is the aggregate's own;
    /// the nullability is not, because a window frame always contains the
    /// current row, so an aggregate over a NOT NULL argument is never NULL.
    fn call_typing(&self, func: WinFunc, arg: Option<&HirExpr>) -> Result<(TypeCode, bool), GnitzSqlError> {
        let WinFunc::Agg(agg) = func else {
            return Ok((TypeCode::I64, false));
        };
        let arg_def = arg.map(|e| ColumnDef::new("_arg", e.infer_type_with(&|r| self.type_of(r)), !self.never_null(e)));
        let raw = crate::agg::agg_typing(agg, arg_def.as_ref())?.ops[0].1;
        let nullable = match agg {
            AggFunc::Count => false,
            _ => arg_def.is_some_and(|d| d.is_nullable),
        };
        Ok((crate::agg::agg_view_type(agg, raw), nullable))
    }
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

impl<L: ItemLeaf> LeafBinder<HirRef> for WindowLeaf<'_, L> {
    fn bind_node(&self, e: &Expr) -> Option<HirExpr> {
        self.inner.bind_node(e)
    }
    /// A source column outranks a SELECT alias of the same name; the alias
    /// table is consulted only where the body's own scope has nothing.
    fn bind_column(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        let err = match self.inner.bind_column(e) {
            Ok(bound) => return Ok(bound),
            Err(err) => err,
        };
        if let Expr::Identifier(id) = peel_nested(e) {
            if let Some((_, expr)) = self
                .aliases
                .borrow()
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(&id.value))
            {
                return Ok(expr.clone());
            }
        }
        Err(err)
    }
    fn bind_function(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        self.inner.bind_function(f)
    }
    /// The one context that admits a window call: it binds to the placeholder
    /// column its value stands in for until the desugar joins it in.
    fn bind_window(&self, f: &Function) -> Result<HirExpr, GnitzSqlError> {
        Ok(BExpr::ColRef(HirRef::Col(self.bind_window_call(f)?.id)))
    }
    /// This leaf's own placeholders; everything else is the body's.
    fn is_nullable(&self, r: &HirRef) -> bool {
        self.placeholder(r)
            .map_or_else(|| self.inner.is_nullable(r), |(_, nullable)| nullable)
    }
    fn bind_subquery(&self, e: &Expr) -> Result<HirExpr, GnitzSqlError> {
        self.inner.bind_subquery(e)
    }
}

impl<L: ItemLeaf> ItemLeaf for WindowLeaf<'_, L> {
    fn env(&self) -> &[HirCol] {
        self.inner.env()
    }
    fn type_of(&self, r: &HirRef) -> TypeCode {
        self.placeholder(r).map_or_else(|| self.inner.type_of(r), |(tc, _)| tc)
    }
    fn wildcard_cols(&self) -> Option<Vec<&HirCol>> {
        self.inner.wildcard_cols()
    }
    fn call_item(
        &self,
        f: &Function,
        alias: &Option<String>,
        idx: usize,
    ) -> Option<Result<(HirExpr, ColumnDef), GnitzSqlError>> {
        if f.over.is_none() {
            return self.inner.call_item(f, alias, idx);
        }
        Some(self.bind_window_call(f).map(|out| {
            // A windowed aggregate is named as the same aggregate written in a
            // GROUP BY is, so one name↔aggregate table serves both.
            let name = alias.clone().unwrap_or_else(|| match self.func_of(out.id) {
                Some(WinFunc::Agg(agg)) => default_agg_name(agg, idx),
                _ => format!("_{}{idx}", single_fn_name(f).unwrap_or("window").to_ascii_lowercase()),
            });
            let def = ColumnDef::new(name, out.def.type_code, out.def.is_nullable);
            (BExpr::ColRef(HirRef::Col(out.id)), def)
        }))
    }
}

/// Classify a windowed call into its function, its unbound argument, and
/// whether it was written as `ROW_NUMBER` — which is `RANK` plus the row-key
/// tiebreak the caller appends, so it has no [`WinFunc`] of its own.
fn classify_window_call(f: &Function) -> Result<(WinFunc, Option<&Expr>, bool), GnitzSqlError> {
    reject_fn_qualifiers(f, "window functions")?;
    let name = single_fn_name(f).ok_or_else(|| unknown_function(f))?;
    let ranking = match name.to_ascii_lowercase().as_str() {
        "rank" => Some((WinFunc::Rank, false)),
        "dense_rank" => Some((WinFunc::DenseRank, false)),
        "row_number" => Some((WinFunc::Rank, true)),
        _ => None,
    };
    let Some((func, row_number)) = ranking else {
        // `bind_structural` routes every windowed call here, scalar names
        // included, so `classify_agg_shape`'s "function not supported" would be
        // false for `ABS(x) OVER (…)` — ABS is supported, just not windowed.
        if agg_func_from_name(name).is_none() {
            return Err(GnitzSqlError::Unsupported(format!(
                "{}: not supported as a window function",
                name.to_ascii_uppercase()
            )));
        }
        let (agg, arg) = classify_agg_shape(f)?;
        return Ok((WinFunc::Agg(agg), arg, false));
    };
    let no_args = match &f.args {
        FunctionArguments::None => true,
        FunctionArguments::List(list) => list.args.is_empty(),
        FunctionArguments::Subquery(_) => false,
    };
    if !no_args {
        return Err(GnitzSqlError::Unsupported(format!(
            "{}: takes no arguments",
            name.to_ascii_uppercase()
        )));
    }
    Ok((func, None, row_number))
}

/// Whether a frame folds the partition up to the current row (`true`) or covers
/// it whole (`false`). The default frame is cumulative iff there is an ORDER BY;
/// the two explicit frames accepted are those same two shapes.
fn frame_is_cumulative(frame: Option<&WindowFrame>, has_order: bool) -> Result<bool, GnitzSqlError> {
    let Some(frame) = frame else {
        return Ok(has_order);
    };
    let end = frame.end_bound.as_ref().unwrap_or(&WindowFrameBound::CurrentRow);
    match (&frame.start_bound, end) {
        (WindowFrameBound::Preceding(None), WindowFrameBound::Following(None)) => Ok(false),
        (WindowFrameBound::Preceding(None), WindowFrameBound::CurrentRow) => match frame.units {
            WindowFrameUnits::Range => Ok(has_order),
            WindowFrameUnits::Rows | WindowFrameUnits::Groups => Err(GnitzSqlError::Unsupported(
                "window frames: ROWS / GROUPS … CURRENT ROW excludes the current row's peers, which \
                 needs a total row order; use RANGE (the default frame)"
                    .into(),
            )),
        },
        _ => Err(GnitzSqlError::Unsupported(
            "window frames: only RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (the default) and \
             BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING are supported"
                .into(),
        )),
    }
}

// ── Desugar ─────────────────────────────────────────────────────────────────────

/// The columns of `W`, the relation every window reads: one per distinct
/// expression the SELECT list, QUALIFY and the window operands reference.
/// Visible and uniquely named, since `W` may be registered as a segment schema.
struct Hoist<'a> {
    ids: &'a ColIdGen,
    env: &'a [HirCol],
    placeholders: &'a [HirCol],
    items: Vec<ProjEntry>,
    /// Subquery leaves, keyed by the relation they carry: `HirRef`'s equality
    /// never matches one, so they cannot be found by scanning `items`.
    by_sub: HashMap<*const RelExpr, ColId>,
    /// Source columns, keyed by the id they were hoisted under — the `hoist`
    /// return the first pass already computes, so the rebuild reads it back
    /// rather than re-finding the entry by expression equality.
    by_col: HashMap<ColId, ColId>,
}

impl Hoist<'_> {
    /// The `W` column holding `e` — a pass-through when `e` is a bare source
    /// column, else a computed column evaluating it over the body's scope. One
    /// per distinct expression, found by scanning what is already hoisted.
    fn hoist(&mut self, e: &HirExpr) -> ColId {
        if let Some(it) = self.items.iter().find(|it| it.expr == *e) {
            return it.out.id;
        }
        let tc = e.infer_type_with(&|r| type_of(self.env, r));
        let nullable = !e.never_null_with(&|r| hir_ref_nullable(self.env, r));
        let out = HirCol::new(
            self.ids.next(),
            ColumnDef::new(format!("_w{}", self.items.len()), tc, nullable),
        );
        let id = out.id;
        self.items.push(ProjEntry { expr: e.clone(), out });
        id
    }

    /// `e` with every source reference replaced by its `W` column: a column by
    /// its pass-through, a subquery leaf by the computed column evaluating it.
    fn refs(&mut self, e: &HirExpr) -> Result<HirExpr, GnitzSqlError> {
        // `try_rebuild` takes a `Fn` and hoisting needs `&mut`, so every leaf is
        // hoisted first and the rebuild reads the finished columns.
        let mut pending: Vec<HirRef> = Vec::new();
        e.for_each_ref(&mut |r| pending.push(r.clone()));
        for r in pending {
            match r {
                HirRef::Col(id) if col_by_id(self.placeholders, id).is_some() => {}
                HirRef::Col(id) => {
                    let w = self.hoist(&BExpr::ColRef(HirRef::Col(id)));
                    self.by_col.insert(id, w);
                }
                HirRef::Subquery(s) => {
                    let key = Rc::as_ptr(&s.rel);
                    if !self.by_sub.contains_key(&key) {
                        let w = self.hoist(&BExpr::ColRef(HirRef::Subquery(s)));
                        self.by_sub.insert(key, w);
                    }
                }
            }
        }
        e.try_rebuild(&|r| -> Result<HirExpr, GnitzSqlError> {
            let id = match r {
                HirRef::Col(id) if col_by_id(self.placeholders, *id).is_some() => *id,
                HirRef::Col(id) => *self.by_col.get(id).expect("every source reference was hoisted above"),
                HirRef::Subquery(s) => self.by_sub[&Rc::as_ptr(&s.rel)],
            };
            Ok(BExpr::ColRef(HirRef::Col(id)))
        })
    }
}

/// `W` as the desugar reads it: the shared relation, and where each hoisted
/// column sits in its output. A read of it is an [`RelExpr::Alias`].
struct WRel {
    rel: Rc<RelExpr>,
    pos: HashMap<ColId, usize>,
}

/// An alias of a relation with its columns in hand, addressed by position.
struct Read {
    rel: Rc<RelExpr>,
    cols: Vec<HirCol>,
}

impl Read {
    fn of(ids: &ColIdGen, rel: &Rc<RelExpr>) -> Read {
        let rel = RelExpr::alias(ids, Rc::clone(rel));
        let cols = rel.cols();
        Read { rel, cols }
    }
    fn id(&self, i: usize) -> ColId {
        self.cols[i].id
    }
    fn col(&self, i: usize) -> HirExpr {
        BExpr::ColRef(HirRef::Col(self.id(i)))
    }
}

/// Rewrite a windowed body into joins and reduces: hoist `W`, build each
/// specification's value relation, join every one onto the outer read of `W`,
/// and project the SELECT list over the result (through the QUALIFY filter).
fn desugar(
    ids: &ColIdGen,
    input: Rc<RelExpr>,
    env: &[HirCol],
    items: Vec<ProjEntry>,
    qualify: Option<HirExpr>,
    win: Windows,
) -> Result<Rc<RelExpr>, GnitzSqlError> {
    let placeholders: Vec<HirCol> = win.calls.iter().map(|c| c.out.clone()).collect();
    let mut h = Hoist {
        ids,
        env,
        placeholders: &placeholders,
        items: Vec::new(),
        by_sub: HashMap::new(),
        by_col: HashMap::new(),
    };
    let items = items
        .into_iter()
        .map(|it| Ok(ProjEntry { expr: h.refs(&it.expr)?, out: it.out }))
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;
    let qualify = qualify.map(|q| h.refs(&q)).transpose()?;
    let calls: Vec<Call<ColId>> = win
        .calls
        .iter()
        .map(|c| Call {
            spec: c.spec,
            func: c.func,
            arg: c.arg.as_ref().map(|a| h.hoist(a)),
            out: c.out.clone(),
        })
        .collect();
    let specs: Vec<Spec<ColId>> = win
        .specs
        .iter()
        .map(|s| Spec {
            partition: s.partition.iter().map(|e| h.hoist(e)).collect(),
            order: s.order.iter().map(|(e, asc)| (h.hoist(e), *asc)).collect(),
        })
        .collect();

    // W: the input itself when it is a bare table every hoisted column passes
    // through (the lowering reads it in place), else the narrowing projection
    // (cut once to a segment every read shares).
    let in_place = match input.as_ref() {
        RelExpr::Get { cols, .. } => h
            .items
            .iter()
            .map(|it| {
                let src = as_col(&it.expr)?;
                Some((it.out.id, cols.iter().position(|c| c.id == src)?))
            })
            .collect::<Option<HashMap<_, _>>>(),
        _ => None,
    };
    let w = match in_place {
        Some(pos) => WRel { rel: Rc::clone(&input), pos },
        None => WRel {
            pos: h.items.iter().enumerate().map(|(i, it)| (it.out.id, i)).collect(),
            rel: RelExpr::project(input, h.items),
        },
    };
    let outer = Read::of(ids, &w.rel);

    // Every specification's value relation, joined onto the outer read. A
    // whole-partition window with no partition key is a keyless (cross) join
    // against the one global row.
    let mut cur = Rc::clone(&outer.rel);
    let mut values: HashMap<ColId, ColId> = HashMap::new();
    for (si, spec) in specs.iter().enumerate() {
        let mine: Vec<&Call<ColId>> = calls.iter().filter(|c| c.spec == si).collect();
        let Windowed { rel: right, keys, values: vals } = if spec.order.is_empty() {
            whole_partition(ids, &w, spec, &mine)?
        } else {
            cumulative(ids, &w, spec, &mine)?
        };
        let on = keys
            .into_iter()
            .map(|(w_col, right_id)| {
                BExpr::BinOp(
                    Box::new(outer.col(w.pos[&w_col])),
                    BinOp::Eq,
                    Box::new(BExpr::ColRef(HirRef::Col(right_id))),
                )
            })
            .collect();
        cur = RelExpr::join(cur, right, JoinType::Inner, on);
        values.extend(vals);
    }

    // The SELECT list and QUALIFY over the joined relation: a `W` reference
    // reads the outer side, a placeholder its window's value column.
    let remap = |e: &HirExpr| {
        e.try_rebuild(&|r| -> Result<HirExpr, GnitzSqlError> {
            let id = match r {
                HirRef::Col(id) => *id,
                HirRef::Subquery(_) => {
                    return Err(GnitzSqlError::Internal(
                        "window desugar: a subquery leaf escaped hoisting".into(),
                    ))
                }
            };
            let target = match (w.pos.get(&id), values.get(&id)) {
                (Some(&p), _) => outer.id(p),
                (None, Some(&v)) => v,
                (None, None) => {
                    return Err(GnitzSqlError::Internal(
                        "window desugar: a reference names neither W nor a window value".into(),
                    ))
                }
            };
            Ok(BExpr::ColRef(HirRef::Col(target)))
        })
    };
    let items = items
        .into_iter()
        .map(|it| Ok(ProjEntry { expr: remap(&it.expr)?, out: it.out }))
        .collect::<Result<Vec<_>, GnitzSqlError>>()?;
    let rel = match qualify {
        Some(q) => RelExpr::filter(cur, vec![remap(&q)?]),
        None => cur,
    };
    Ok(RelExpr::project(rel, items))
}

/// One specification's value relation: the relation itself, its join keys as
/// `(W column, value-relation column)` pairs, and each call's placeholder paired
/// with the value column that realizes it.
struct Windowed {
    rel: Rc<RelExpr>,
    keys: Vec<(ColId, ColId)>,
    values: Vec<(ColId, ColId)>,
}

/// A value column of a reduce's projection: its expression, type, nullability.
type Value = (HirExpr, TypeCode, bool);

/// A reduce's aggregate list, deduplicated by `(function, argument)`, with each
/// aggregate's index handed back so a value can address it.
struct Aggs<'a> {
    env: &'a [HirCol],
    ids: &'a ColIdGen,
    is_global: bool,
    list: Vec<HirAgg>,
}

impl Aggs<'_> {
    fn slot(&mut self, func: AggFunc, arg: Option<ColId>) -> Result<usize, GnitzSqlError> {
        if let Some(i) = self.list.iter().position(|a| a.func == func && a.arg == arg) {
            return Ok(i);
        }
        self.list
            .push(HirAgg::new(self.ids, func, arg, self.env, self.is_global)?);
        Ok(self.list.len() - 1)
    }

    /// Aggregate `(func, arg)`, registered on first sight, as a value column.
    fn value(&mut self, func: AggFunc, arg: Option<ColId>) -> Result<Value, GnitzSqlError> {
        let slot = self.slot(func, arg)?;
        Ok(self.list[slot].as_value())
    }
}

/// Present a reduce as `[_k{i}…, _{prefix}{j}…]`: visible, uniquely named
/// columns, the shape a join or an alias reads. Values are deduplicated by
/// expression, so two calls computing the same thing share a column. Returns the
/// projection with the id of each key column and of the column realizing each
/// *input* value, so nothing re-derives a position from the layout.
fn present(
    ids: &ColIdGen,
    reduce: Rc<RelExpr>,
    keys: &[ColId],
    prefix: &str,
    values: Vec<Value>,
) -> (Rc<RelExpr>, Vec<ColId>, Vec<ColId>) {
    let in_cols = reduce.cols();
    let mut items: Vec<ProjEntry> = Vec::with_capacity(keys.len() + values.len());
    let key_ids = keys
        .iter()
        .enumerate()
        .map(|(i, &k)| {
            let def = &col_by_id(&in_cols, k).expect("a reduce carries its group keys").def;
            let out = HirCol::new(
                ids.next(),
                ColumnDef::new(format!("_k{i}"), def.type_code, def.is_nullable),
            );
            let id = out.id;
            items.push(ProjEntry { expr: BExpr::ColRef(HirRef::Col(k)), out });
            id
        })
        .collect();
    let nkeys = items.len();
    let mut value_ids = Vec::with_capacity(values.len());
    for (expr, tc, nullable) in values {
        value_ids.push(match items[nkeys..].iter().find(|it| it.expr == expr) {
            Some(it) => it.out.id,
            None => {
                let out = HirCol::new(
                    ids.next(),
                    ColumnDef::new(format!("_{prefix}{}", items.len() - nkeys), tc, nullable),
                );
                let id = out.id;
                items.push(ProjEntry { expr, out });
                id
            }
        });
    }
    (RelExpr::project(reduce, items), key_ids, value_ids)
}

/// One specification's value relation: `reduce` presented as [`present`] does,
/// with its `W` keys and each call's placeholder paired to the column realizing
/// them.
fn windowed(
    ids: &ColIdGen,
    reduce: Rc<RelExpr>,
    group_cols: &[ColId],
    w_keys: Vec<ColId>,
    prefix: &str,
    calls: &[&Call<ColId>],
    per_call: Vec<Value>,
) -> Windowed {
    let (rel, key_ids, value_ids) = present(ids, reduce, group_cols, prefix, per_call);
    Windowed {
        keys: w_keys.into_iter().zip(key_ids).collect(),
        values: calls.iter().map(|c| c.out.id).zip(value_ids).collect(),
        rel,
    }
}

/// The per-partition aggregate relation `G = γ_P(W')` for a whole-partition
/// window.
fn whole_partition(
    ids: &ColIdGen,
    w: &WRel,
    spec: &Spec<ColId>,
    calls: &[&Call<ColId>],
) -> Result<Windowed, GnitzSqlError> {
    let wf = Read::of(ids, &w.rel);
    let wid = |w_col: ColId| wf.id(w.pos[&w_col]);
    let keys: Vec<ColId> = spec.partition.iter().map(|&p| wid(p)).collect();
    let mut aggs = Aggs {
        env: &wf.cols,
        ids,
        is_global: keys.is_empty(),
        list: Vec::new(),
    };
    let mut per_call = Vec::with_capacity(calls.len());
    for c in calls {
        per_call.push(match c.func {
            WinFunc::Agg(func) => aggs.value(func, c.arg.map(wid))?,
            // Nothing to order by, so every row of the partition is first. (A
            // written RANK / DENSE_RANK without an ORDER BY is rejected at bind.)
            _ => (BExpr::LitInt(1), TypeCode::I64, false),
        });
    }
    let reduce = RelExpr::reduce(wf.rel, Vec::new(), keys.clone(), aggs.list);
    Ok(windowed(
        ids,
        reduce,
        &keys,
        spec.partition.clone(),
        "g",
        calls,
        per_call,
    ))
}

/// The cumulative relation `R = γ_{P,O}(G ⋈ G)` for an ordered window.
fn cumulative(ids: &ColIdGen, w: &WRel, spec: &Spec<ColId>, calls: &[&Call<ColId>]) -> Result<Windowed, GnitzSqlError> {
    // G: one row per peer group, carrying each aggregate's per-group value.
    // RANK and ROW_NUMBER need the group's row count; DENSE_RANK only the group.
    let wf = Read::of(ids, &w.rel);
    let wid = |w_col: ColId| wf.id(w.pos[&w_col]);
    let np = spec.partition.len();
    let w_keys: Vec<ColId> = spec
        .partition
        .iter()
        .chain(spec.order.iter().map(|(o, _)| o))
        .copied()
        .collect();
    let g_keys: Vec<ColId> = w_keys.iter().map(|&c| wid(c)).collect();
    let nkeys = g_keys.len();
    let mut g_aggs = Aggs {
        env: &wf.cols,
        ids,
        is_global: false,
        list: Vec::new(),
    };
    // Per call, its G aggregate slots: one, or two for AVG (sum and count).
    let mut g_slots: Vec<Vec<usize>> = Vec::with_capacity(calls.len());
    for c in calls {
        let arg = c.arg.map(wid);
        g_slots.push(match c.func {
            WinFunc::Agg(AggFunc::Avg) => {
                let x = arg.expect("AVG has an argument");
                vec![
                    g_aggs.slot(AggFunc::Sum, Some(x))?,
                    g_aggs.slot(AggFunc::Count, Some(x))?,
                ]
            }
            WinFunc::Agg(func) => vec![g_aggs.slot(func, arg)?],
            WinFunc::Rank => vec![g_aggs.slot(AggFunc::Count, None)?],
            WinFunc::DenseRank => vec![],
        });
    }
    let g_values = g_aggs.list.iter().map(HirAgg::as_value).collect();
    let reduce = RelExpr::reduce(wf.rel, Vec::new(), g_keys.clone(), g_aggs.list);
    let (g, _, _) = present(ids, reduce, &g_keys, "g", g_values);

    // The band self-join: g1 is the current peer group, g2 every group of the
    // same partition at or before it — `g2.O ≤lex g1.O` under each key's own
    // direction.
    let (g1, g2) = (Read::of(ids, &g), Read::of(ids, &g));
    let cmp = |i: usize, op: BinOp| BExpr::BinOp(Box::new(g2.col(i)), op, Box::new(g1.col(i)));
    let weak = |asc: bool| if asc { BinOp::Le } else { BinOp::Ge };
    let strict = |asc: bool| if asc { BinOp::Lt } else { BinOp::Gt };
    let mut on: Vec<HirExpr> = (0..np).map(|i| cmp(i, BinOp::Eq)).collect();
    let dirs: Vec<bool> = spec.order.iter().map(|(_, asc)| *asc).collect();
    on.push(cmp(np, weak(dirs[0])));
    if dirs.len() > 1 {
        let last = dirs.len() - 1;
        let mut lex = cmp(np + last, weak(dirs[last]));
        for (i, &asc) in dirs.iter().enumerate().rev().skip(1) {
            let tie = BExpr::BinOp(Box::new(cmp(np + i, BinOp::Eq)), BinOp::And, Box::new(lex));
            lex = BExpr::BinOp(Box::new(cmp(np + i, strict(asc))), BinOp::Or, Box::new(tie));
        }
        on.push(lex);
    }
    let band = RelExpr::join(Rc::clone(&g1.rel), Rc::clone(&g2.rel), JoinType::Inner, on);
    let band_cols = band.cols();

    // R: fold g2's per-group values over the band, keyed by g1's group. RANK
    // subtracts the current group's own count back out, so that count is a
    // group key too (unprojected).
    let mut r_keys: Vec<ColId> = (0..nkeys).map(|i| g1.id(i)).collect();
    let mut r_aggs = Aggs {
        env: &band_cols,
        ids,
        is_global: false,
        list: Vec::new(),
    };
    let mut own_cnt_keyed = false;
    let mut per_call = Vec::with_capacity(calls.len());
    for (c, gs) in calls.iter().zip(&g_slots) {
        let v2 = |g: usize| Some(g2.id(nkeys + g));
        per_call.push(match c.func {
            WinFunc::Agg(AggFunc::Avg) => {
                let (sum, _, nullable) = r_aggs.value(AggFunc::Sum, v2(gs[0]))?;
                let (cnt, _, _) = r_aggs.value(AggFunc::Sum, v2(gs[1]))?;
                let scaled = BExpr::BinOp(Box::new(sum), BinOp::Mul, Box::new(BExpr::LitFloat(1.0)));
                (
                    BExpr::BinOp(Box::new(scaled), BinOp::Div, Box::new(cnt)),
                    TypeCode::F64,
                    nullable,
                )
            }
            WinFunc::Agg(func @ (AggFunc::Min | AggFunc::Max)) => r_aggs.value(func, v2(gs[0]))?,
            WinFunc::Agg(_) => r_aggs.value(AggFunc::Sum, v2(gs[0]))?,
            WinFunc::Rank => {
                if !own_cnt_keyed {
                    own_cnt_keyed = true;
                    r_keys.push(g1.id(nkeys + gs[0]));
                }
                let (folded, _, _) = r_aggs.value(AggFunc::Sum, v2(gs[0]))?;
                let before = BExpr::BinOp(Box::new(folded), BinOp::Sub, Box::new(g1.col(nkeys + gs[0])));
                (
                    BExpr::BinOp(Box::new(before), BinOp::Add, Box::new(BExpr::LitInt(1))),
                    TypeCode::I64,
                    false,
                )
            }
            WinFunc::DenseRank => r_aggs.value(AggFunc::Count, None)?,
        });
    }
    let projected_keys: Vec<ColId> = r_keys[..nkeys].to_vec();
    let reduce = RelExpr::reduce(band, Vec::new(), r_keys, r_aggs.list);
    Ok(windowed(ids, reduce, &projected_keys, w_keys, "r", calls, per_call))
}

#[cfg(test)]
#[path = "tests/window.rs"]
mod tests;
