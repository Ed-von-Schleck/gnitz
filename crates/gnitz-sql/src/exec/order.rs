//! Ad-hoc SELECT ordering & pagination sink: ORDER BY / OFFSET / LIMIT applied
//! as a single client-side pass over the fetched batch (base tables and views).
//!
//! The batch arrives as a client `ZSetBatch`: entries carrying an integer
//! **weight**. Entries are *not* unique by `(PK, payload)` — a reply train is
//! concatenated and never folded — and the sink does not need them to be, only
//! `weight >= 1`. A PK column orders by `memcmp` over its OPK bytes; a payload
//! column is a per-type typed compare (the shared `gnitz_wire::cmp_typed_le`),
//! its region being native little-endian. The sink
//! sorts **before** projection so an ORDER BY key absent from the projected
//! columns still resolves, then walks the sorted permutation by running
//! **logical position** — LIMIT/OFFSET count multiplicity (summed weight), never
//! Z-set entries — and gathers the surviving rows. The invariants are pinned by
//! the unit tests below.

use std::cmp::Ordering;

use crate::ast_util::{clause_position, reject_position_out_of_range};
use crate::bind::{bind_single_table, output_column};
use crate::codec::project_schema::ProjItem;
use crate::error::GnitzSqlError;
use crate::exec::batch::RowGather;
use crate::validate::order_column;
use gnitz_core::{ColumnDef, Schema, ZSetBatch, ZSetBatchView};
use gnitz_expr::{cmp_order_keys, OrderLocator, RowSource, SchemaFacts};
use sqlparser::ast::{Expr, OrderBy, OrderByExpr, OrderByKind, OrderByOptions};

// ---------------------------------------------------------------------------
// One sort key over the full pre-projection schema
// ---------------------------------------------------------------------------

/// Resolve one sort key against the pre-projection (`actual`) schema. Every
/// address the comparator needs comes from the one [`gnitz_expr::ColumnLocator`],
/// resolved once out of the O(n log n) comparator — the same record the engine's
/// worker-side ORDER BY resolves through, feeding the same [`cmp_order_keys`].
fn sort_key(schema: &Schema, ci: usize, asc: bool, nulls_first: bool) -> OrderLocator {
    OrderLocator {
        loc: SchemaFacts::locate(schema, ci),
        desc: !asc,
        nulls_first,
    }
}

/// The client-side cut a sink applies to its own result. `limit: None` is
/// unbounded.
#[derive(Clone, Copy)]
pub(crate) struct Window {
    pub(crate) offset: usize,
    pub(crate) limit: Option<usize>,
}

/// Lexicographic compare over the key list — the permutation's sort comparator.
/// [`cmp_order_keys`] is the engine's own; the tiebreak the caller appends
/// (`push_identity_tiebreak`) is what makes the order total here.
fn cmp_rows(view: &ZSetBatchView, keys: &[OrderLocator], ra: usize, rb: usize) -> Ordering {
    cmp_order_keys(keys, view, ra, view.get_null_word(ra), view, rb, view.get_null_word(rb))
}

// ---------------------------------------------------------------------------
// ORDER BY key parsing & resolution
// ---------------------------------------------------------------------------

/// What an ORDER BY key names: a 1-based visible-output position, or an
/// expression — a bare or qualified name resolving output-first, anything else
/// bound in the SELECT list's own scope and carried as a hidden column.
pub(crate) enum OrderTarget<'a> {
    Position(usize),
    Expr(&'a Expr),
}

/// A parsed ORDER BY key: its target plus resolved direction and absolute NULL
/// placement (default NULLS LAST for ASC, FIRST for DESC).
pub(crate) struct OrderKey<'a> {
    pub(crate) target: OrderTarget<'a>,
    asc: bool,
    nulls_first: bool,
}

impl OrderKey<'_> {
    fn wire(&self, col: usize) -> gnitz_wire::OrderKey {
        gnitz_wire::OrderKey {
            col: col as u16,
            desc: !self.asc,
            nulls_first: self.nulls_first,
        }
    }
}

/// The expression keys of `keys`, in key order — the list a SELECT list's binder
/// places one projection item each for, and [`wire_order`] reads back.
pub(crate) fn order_exprs<'a>(keys: &[OrderKey<'a>]) -> Vec<&'a Expr> {
    keys.iter()
        .filter_map(|k| match k.target {
            OrderTarget::Expr(e) => Some(e),
            OrderTarget::Position(_) => None,
        })
        .collect()
}

fn unsupported(what: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{what} is not supported in direct SELECT ORDER BY"))
}

/// Classify one ORDER BY expression: an integer literal is positional, by the
/// same rule GROUP BY reads; everything else is an expression.
fn order_target(e: &Expr) -> Result<OrderTarget<'_>, GnitzSqlError> {
    Ok(match clause_position(e, "ORDER BY position")? {
        Some(pos) => OrderTarget::Position(pos),
        None => OrderTarget::Expr(e),
    })
}

/// The ORDER BY clause as key specs, or none for an absent clause.
pub(crate) fn parse_order_by(order_by: Option<&OrderBy>) -> Result<Vec<OrderKey<'_>>, GnitzSqlError> {
    let Some(ob) = order_by else {
        return Ok(Vec::new());
    };
    let keys = resolve_order_by(ob)?;
    if keys.len() > gnitz_wire::MAX_ORDER_KEYS {
        return Err(GnitzSqlError::Unsupported(format!(
            "ORDER BY has more than {} keys",
            gnitz_wire::MAX_ORDER_KEYS
        )));
    }
    Ok(keys)
}

/// Parse the whole ORDER BY clause into resolved key specs, rejecting the
/// unsupported ClickHouse/DuckDB extensions as a clean `Unsupported` error.
fn resolve_order_by(ob: &OrderBy) -> Result<Vec<OrderKey<'_>>, GnitzSqlError> {
    // Exhaustive (no `..`) at each of the three levels: a dropped ORDER BY
    // modifier is a wrong result, so a future `sqlparser` field stops the build.
    let OrderBy { kind, interpolate } = ob;
    if interpolate.is_some() {
        return Err(unsupported("ORDER BY ... INTERPOLATE"));
    }
    let exprs = match kind {
        OrderByKind::Expressions(e) => e,
        OrderByKind::All(_) => return Err(unsupported("ORDER BY ALL")),
    };
    let mut keys = Vec::with_capacity(exprs.len());
    for obe in exprs {
        let OrderByExpr { expr, options, with_fill } = obe;
        if with_fill.is_some() {
            return Err(unsupported("ORDER BY ... WITH FILL"));
        }
        let OrderByOptions { asc, nulls_first } = options;
        let asc = asc.unwrap_or(true);
        keys.push(OrderKey {
            target: order_target(expr)?,
            asc,
            // Absolute default: ASC → NULLS LAST, DESC → NULLS FIRST.
            nulls_first: nulls_first.unwrap_or(!asc),
        });
    }
    Ok(keys)
}

/// The physical column index a 1-based ORDER BY position names, given the
/// physical indices of the visible columns.
fn resolve_position(pos: usize, visible: &[usize]) -> Result<usize, GnitzSqlError> {
    reject_position_out_of_range(pos, visible.len(), "ORDER BY")?;
    Ok(visible[pos - 1])
}

/// The ORDER BY keys as wire `OrderKey`s over the (server-projected) ScanSpec
/// reply columns: a position names a VISIBLE column, every other key binds
/// against the source and takes the reply slot [`emit_slot`] gives it.
pub(crate) fn resolve_read_spec_order(
    items: &mut Vec<ProjItem>,
    out_cols: &mut Vec<ColumnDef>,
    source_schema: &Schema,
    alias: &str,
    keys: &[OrderKey<'_>],
) -> Result<Vec<gnitz_wire::OrderKey>, GnitzSqlError> {
    // Visible reply columns for positional resolution (appends are hidden, so this
    // stays the SELECT-list positions even as columns are appended below).
    let visible: Vec<usize> = out_cols
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.is_hidden)
        .map(|(i, _)| i)
        .collect();
    let mut out = Vec::with_capacity(keys.len());
    for (i, key) in keys.iter().enumerate() {
        let col = match key.target {
            OrderTarget::Position(pos) => resolve_position(pos, &visible)?,
            OrderTarget::Expr(e) => match output_column(e, out_cols.iter())? {
                Some(at) => at,
                None => {
                    let item = ProjItem::from_bound(bind_single_table(e, source_schema, alias)?);
                    emit_slot(item, i, items, out_cols, source_schema)
                }
            },
        };
        out.push(key.wire(col));
    }
    Ok(out)
}

/// The reply slot emitting `item`: the projection slot already emitting it, else
/// a hidden one appended for it. The projection ships to the workers and runs on
/// every row, so reuse keeps a duplicate off the per-row path; appends are
/// hidden, so they never shift a visible position.
fn emit_slot(
    item: ProjItem,
    key: usize,
    items: &mut Vec<ProjItem>,
    out_cols: &mut Vec<ColumnDef>,
    source_schema: &Schema,
) -> usize {
    if let Some(at) = items.iter().position(|it| *it == item) {
        return at;
    }
    out_cols.push(match &item {
        ProjItem::PassThrough { src_col } => source_schema.columns[*src_col].clone().hidden(),
        ProjItem::Computed { bound_expr } => order_column(key, bound_expr.infer_ty(&source_schema.columns)),
    });
    items.push(item);
    out_cols.len() - 1
}

/// Append the deterministic identity tiebreak — every PK column in pk-list
/// order, then every payload column in schema order, all ASC NULLS FIRST — so
/// distinct logical rows tied on the ORDER BY keys order consistently across
/// worker counts and a cut is a function of the data. Matches the worker's
/// OPK-then-payload tiebreak.
///
/// **Precondition on the caller's schema: `pk_cols` must be data-derived** — a
/// base-table key, or a synthetic key that is a pure function of row content
/// (`_group_pk`, `_join_pk`, `_set_pk`). They lead the tiebreak, so a PK carrying
/// anything else decides every tie before a payload column is ever reached.
fn push_identity_tiebreak(keys: &mut Vec<OrderLocator>, schema: &Schema) {
    for &ci in &schema.pk_cols {
        keys.push(sort_key(schema, ci as usize, true, true));
    }
    for (_, ci, _) in schema.payload_columns() {
        keys.push(sort_key(schema, ci, true, true));
    }
}

// ---------------------------------------------------------------------------
// Multiplicity walk (LIMIT / OFFSET over logical rows)
// ---------------------------------------------------------------------------

/// Walk entry weights in sorted order over the logical window `[offset, hi)`
/// (`hi = offset + limit`, or `u64::MAX` when unbounded), returning each
/// surviving entry's index and its surviving weight. Entry *i* occupies
/// `[Cᵢ, Cᵢ + wᵢ)` (cumulative weight before it); its surviving weight is
/// `max(0, min(Cᵢ+wᵢ, hi) − max(Cᵢ, offset))`. This one overlap formula handles
/// OFFSET mid-entry, LIMIT mid-entry, and both cuts inside the same entry. The
/// walk requires `wᵢ ≥ 1`, which its one caller checks over the whole batch.
fn paginate(weights: &[i64], offset: u64, hi: u64) -> Vec<(usize, i64)> {
    let mut cum: u64 = 0;
    let mut out = Vec::new();
    for (i, &w) in weights.iter().enumerate() {
        let lo_i = cum;
        let hi_i = cum.saturating_add(w as u64);
        let surviving = hi_i.min(hi).saturating_sub(lo_i.max(offset));
        if surviving > 0 {
            out.push((i, surviving as i64));
        }
        cum = hi_i;
        if cum >= hi {
            break;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// The sink
// ---------------------------------------------------------------------------

/// The wire `OrderKey`s [`read_spec_finish`] windows an already-projected result
/// by. A position resolves against the visible columns; an expression key takes
/// the item `placed` records for it — one per [`order_exprs`] entry, in that
/// order — shifted past the `base` hidden columns the schema leads with.
pub(crate) fn wire_order(
    keys: &[OrderKey<'_>],
    schema: &Schema,
    placed: &[usize],
    base: usize,
) -> Result<Vec<gnitz_wire::OrderKey>, GnitzSqlError> {
    let visible: Vec<usize> = schema.visible_columns().map(|(i, _)| i).collect();
    let mut placed = placed.iter();
    keys.iter()
        .map(|k| {
            let col = match k.target {
                OrderTarget::Position(pos) => resolve_position(pos, &visible)?,
                OrderTarget::Expr(_) => {
                    let at = placed
                        .next()
                        .ok_or_else(|| GnitzSqlError::Internal("ORDER BY placements do not match the keys".into()))?;
                    base + at
                }
            };
            Ok(k.wire(col))
        })
        .collect()
}

/// Sort + window an already-server-projected ScanSpec reply by its wire
/// `OrderKey`s (whose `col` is a full reply-schema column index — fed straight to
/// `sort_key`, NOT resolved by name, so a projected-column / alias ORDER BY
/// works and a hidden appended key stays addressable), then present under the
/// identity projection (hidden columns stripped downstream). The worker's per-
/// worker top-k selects the same order (shared comparators), so this re-sort of
/// the concatenation yields the exact final window.
pub(crate) fn read_spec_finish(
    schema: Schema,
    batch: ZSetBatch,
    order_keys: &[gnitz_wire::OrderKey],
    window: Window,
) -> (Schema, ZSetBatch) {
    let sort_keys: Vec<OrderLocator> = order_keys
        .iter()
        .map(|k| sort_key(&schema, k.col as usize, !k.desc, k.nulls_first))
        .collect();
    finish_window(schema, batch, sort_keys, window)
}

/// Apply the resolved sort keys and the OFFSET/LIMIT cut — the shared tail of
/// both sinks, returning the schema unchanged. Zero-copy when there is nothing
/// to reorder, skip, or bound.
///
/// A cut over a non-total order would pick an arbitrary member of each tie
/// group, so a cut appends the identity tiebreak; every tie is then between
/// *identical* rows, and an unstable partial selection is exact however it
/// splits a tie group. Without a cut, a stable sort so ties keep their fetch
/// order. Then the logical window is walked and the survivors gathered, moving
/// each row's String/Blob cells out of the owned `full` (every source row
/// survives at most once; `full` is dropped right after). A boundary entry
/// keeps its window-clipped multiplicity.
fn finish_window(
    schema: Schema,
    full: ZSetBatch,
    mut sort_keys: Vec<OrderLocator>,
    Window { offset, limit }: Window,
) -> (Schema, ZSetBatch) {
    let has_cut = limit.is_some() || offset > 0;
    if sort_keys.is_empty() && !has_cut {
        return (schema, full);
    }
    // Bag positivity, the precondition of both steps below: the cut keeps
    // `offset + limit` entries because each covers at least one logical row, and
    // `paginate` sums weights into a running position. Checked over the whole
    // batch, before the cut can discard the violator unseen.
    debug_assert!(
        full.weights.iter().all(|&w| w > 0),
        "ordering sink: non-positive weight violates the bag invariant"
    );

    let n = full.len();
    let mut perm: Vec<usize> = (0..n).collect();
    if !sort_keys.is_empty() {
        // One region list for the whole sort: every comparison reads through it,
        // and building it per comparison would dominate the compare itself.
        let view = ZSetBatchView::new(&full, &schema);
        if has_cut {
            push_identity_tiebreak(&mut sort_keys, &schema);
            if let Some(l) = limit {
                let k = offset.saturating_add(l).min(n);
                if k == 0 {
                    perm.clear();
                } else if k < n {
                    perm.select_nth_unstable_by(k - 1, |&ra, &rb| cmp_rows(&view, &sort_keys, ra, rb));
                    perm.truncate(k);
                }
            }
            perm.sort_unstable_by(|&ra, &rb| cmp_rows(&view, &sort_keys, ra, rb));
        } else {
            perm.sort_by(|&ra, &rb| cmp_rows(&view, &sort_keys, ra, rb));
        }
    }

    let off = offset as u64;
    let hi = limit.map_or(u64::MAX, |l| off.saturating_add(l as u64));
    let ordered_weights: Vec<i64> = perm.iter().map(|&r| full.weights[r]).collect();
    let surviving_rows = paginate(&ordered_weights, off, hi);
    let gather = RowGather::new(&schema);
    let mut gathered = ZSetBatch::with_capacity(&schema, surviving_rows.len());
    for (pos, surviving) in surviving_rows {
        gather.copy(&full, perm[pos], &mut gathered);
        // The gather copied the weight verbatim; overwrite with the
        // window-clipped multiplicity (a boundary entry keeps a reduced one).
        *gathered.weights.last_mut().unwrap() = surviving;
    }
    (schema, gathered)
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
