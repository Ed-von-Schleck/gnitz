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

use crate::bind::{bind_single_table, output_column};
use crate::codec::project_schema::ProjItem;
use crate::error::GnitzSqlError;
use crate::tail::{key_slots, resolve_position, OrderKey, OrderTarget};
use crate::validate::order_column;
use std::sync::Arc;

use gnitz_core::{ColumnDef, Schema, ZSetBatch};
use gnitz_expr::{cmp_order_keys, push_identity_tiebreak, OrderLocator, RowSource, SchemaFacts};

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
fn cmp_rows(batch: &ZSetBatch, keys: &[OrderLocator], ra: usize, rb: usize) -> Ordering {
    cmp_order_keys(
        keys,
        batch,
        ra,
        batch.get_null_word(ra),
        batch,
        rb,
        batch.get_null_word(rb),
    )
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
/// by. `placed` is relative to the projection, which starts after `schema`'s PK
/// columns.
pub(crate) fn wire_order(
    keys: &[OrderKey<'_>],
    schema: &Schema,
    placed: &[usize],
) -> Result<Vec<gnitz_wire::OrderKey>, GnitzSqlError> {
    let visible: Vec<usize> = schema.visible_columns().map(|(i, _)| i).collect();
    let placed: Vec<usize> = placed.iter().map(|at| schema.pk_cols.len() + at).collect();
    let slots = key_slots(keys, &visible, &placed)?;
    Ok(keys.iter().zip(slots).map(|(k, col)| k.wire(col)).collect())
}

/// Sort and window a projected ScanSpec reply by its wire `OrderKey`s, whose
/// `col` indexes the reply schema, so a hidden appended key resolves.
pub(crate) fn read_spec_finish(
    schema: Arc<Schema>,
    batch: ZSetBatch,
    order_keys: &[gnitz_wire::OrderKey],
    window: Window,
) -> (Arc<Schema>, ZSetBatch) {
    let sort_keys: Vec<OrderLocator> = order_keys
        .iter()
        .map(|k| OrderLocator::of(SchemaFacts::locate(schema.as_ref(), k.col as usize), k))
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
    schema: Arc<Schema>,
    full: ZSetBatch,
    mut sort_keys: Vec<OrderLocator>,
    Window { offset, limit }: Window,
) -> (Arc<Schema>, ZSetBatch) {
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
        if has_cut {
            push_identity_tiebreak(&mut sort_keys, schema.as_ref());
            if let Some(l) = limit {
                let k = offset.saturating_add(l).min(n);
                if k == 0 {
                    perm.clear();
                } else if k < n {
                    perm.select_nth_unstable_by(k - 1, |&ra, &rb| cmp_rows(&full, &sort_keys, ra, rb));
                    perm.truncate(k);
                }
            }
            perm.sort_unstable_by(|&ra, &rb| cmp_rows(&full, &sort_keys, ra, rb));
        } else {
            perm.sort_by(|&ra, &rb| cmp_rows(&full, &sort_keys, ra, rb));
        }
    }

    let off = offset as u64;
    let hi = limit.map_or(u64::MAX, |l| off.saturating_add(l as u64));
    let ordered_weights: Vec<i64> = perm.iter().map(|&r| full.weights[r]).collect();
    let surviving_rows = paginate(&ordered_weights, off, hi);
    let mut gathered = ZSetBatch::with_capacity(&schema, surviving_rows.len());
    for (pos, surviving) in surviving_rows {
        // Written at the window-clipped multiplicity (a boundary entry keeps a
        // reduced one).
        gathered.copy_row_at(&full, perm[pos], surviving);
    }
    (schema, gathered)
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
