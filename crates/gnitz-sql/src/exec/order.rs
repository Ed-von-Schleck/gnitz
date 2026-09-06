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

use crate::ast_util::{clause_position, reject_position_out_of_range, single_relation_col_name};
use crate::bind::find_unique_column;
use crate::codec::project_schema::ProjItem;
use crate::error::GnitzSqlError;
use crate::exec::batch::RowGather;
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

/// Lexicographic compare over the key list — the permutation's sort comparator.
/// [`cmp_order_keys`] is the engine's own; the tiebreak the caller appends
/// (`push_identity_tiebreak`) is what makes the order total here.
fn cmp_rows(view: &ZSetBatchView, keys: &[OrderLocator], ra: usize, rb: usize) -> Ordering {
    cmp_order_keys(keys, view, ra, view.get_null_word(ra), view, rb, view.get_null_word(rb))
}

// ---------------------------------------------------------------------------
// ORDER BY key parsing & resolution
// ---------------------------------------------------------------------------

/// What an ORDER BY key names: a 1-based visible-output position or a column
/// name/alias (bare or qualified). Expressions and non-integer literals are
/// rejected before this.
enum OrderTarget {
    Position(usize),
    Name(String),
}

/// A parsed ORDER BY key: its target plus resolved direction and absolute NULL
/// placement (default NULLS LAST for ASC, FIRST for DESC).
struct OrderKey {
    target: OrderTarget,
    asc: bool,
    nulls_first: bool,
}

fn unsupported(what: &str) -> GnitzSqlError {
    GnitzSqlError::Unsupported(format!("{what} is not supported in direct SELECT ORDER BY"))
}

/// Classify one ORDER BY expression as a positional index or a column reference.
fn order_target(e: &Expr) -> Result<OrderTarget, GnitzSqlError> {
    // Integer literal → positional, by the same rule GROUP BY reads.
    if let Some(pos) = clause_position(e, "ORDER BY position")? {
        return Ok(OrderTarget::Position(pos));
    }
    // Bare or qualified (`t.col`) identifier → its name, which is resolved
    // against the read's *output* columns, not the relation's — so there is no
    // qualifier to check it against and none is read.
    if let Some(name) = single_relation_col_name(e) {
        return Ok(OrderTarget::Name(name.to_string()));
    }
    Err(GnitzSqlError::Unsupported(
        "ORDER BY supports only column references and 1-based positions".to_string(),
    ))
}

/// Parse the whole ORDER BY clause into resolved key specs, rejecting the
/// unsupported ClickHouse/DuckDB extensions as a clean `Unsupported` error.
fn resolve_order_by(ob: &OrderBy) -> Result<Vec<OrderKey>, GnitzSqlError> {
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

/// Resolve one ORDER BY key to a column index into `schema` — the batch the
/// passthrough sink sorts is already in its presentation shape (a view scan,
/// an executor result, an aggregate finish), so a name resolves against the
/// schema's columns/aliases (ambiguity → error) and a position names the n-th
/// **visible** column (a result may carry a hidden synthetic key at physical
/// index 0, so a naive `pos-1` would sort by the hidden key).
fn resolve_key_col(key: &OrderKey, schema: &Schema) -> Result<usize, GnitzSqlError> {
    match &key.target {
        OrderTarget::Position(pos) => {
            let visible: Vec<usize> = schema.visible_columns().map(|(i, _)| i).collect();
            resolve_position(*pos, &visible)
        }
        OrderTarget::Name(name) => find_unique_column(&schema.columns, name)?
            .ok_or_else(|| GnitzSqlError::Bind(format!("ORDER BY column '{name}' not found"))),
    }
}

/// The physical column index a 1-based ORDER BY position names, given the
/// physical indices of the visible columns.
fn resolve_position(pos: usize, visible: &[usize]) -> Result<usize, GnitzSqlError> {
    reject_position_out_of_range(pos, visible.len(), "ORDER BY")?;
    Ok(visible[pos - 1])
}

/// Resolve the ORDER BY clause to wire `OrderKey`s over the (server-projected)
/// ScanSpec reply columns, **reusing or appending a hidden payload column** for
/// a non-projected source key (§ the read path sorts server-side over a
/// superset; the client windows). `col` is the full reply-schema column index —
/// the worker top-k and [`read_spec_finish`] feed it straight to
/// `sort_key`. Positions resolve against the VISIBLE columns; appends are
/// hidden and never shift a visible position. An ORDER BY expression is an
/// `Unsupported` (the caller routes; the executor rejects it identically); an
/// unknown column is a `Bind` error.
pub(crate) fn resolve_read_spec_order(
    items: &mut Vec<ProjItem>,
    out_cols: &mut Vec<ColumnDef>,
    source_schema: &Schema,
    order_by: Option<&OrderBy>,
) -> Result<Vec<gnitz_wire::OrderKey>, GnitzSqlError> {
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
    // Visible reply columns for positional resolution (appends are hidden, so this
    // stays the SELECT-list positions even as columns are appended below).
    let visible: Vec<usize> = out_cols
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.is_hidden)
        .map(|(i, _)| i)
        .collect();
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let col = match &key.target {
            OrderTarget::Position(pos) => resolve_position(*pos, &visible)?,
            OrderTarget::Name(name) => {
                if let Some(ci) = find_unique_column(&*out_cols, name)? {
                    ci
                } else if let Some(src_ci) = find_unique_column(&source_schema.columns, name)? {
                    // A non-projected source column. Any pass-through of it
                    // already in the reply — the hidden-prepended PK slots, an
                    // aliased projection, an earlier ORDER BY append — carries
                    // the same data, so key on that slot; else append a hidden
                    // copy (stripped at presentation).
                    let existing = items
                        .iter()
                        .position(|it| matches!(it, ProjItem::PassThrough { src_col } if *src_col == src_ci));
                    match existing {
                        Some(pos) => pos,
                        None => {
                            items.push(ProjItem::PassThrough { src_col: src_ci });
                            out_cols.push(source_schema.columns[src_ci].clone().hidden());
                            out_cols.len() - 1
                        }
                    }
                } else {
                    return Err(GnitzSqlError::Bind(format!("ORDER BY column '{name}' not found")));
                }
            }
        };
        out.push(gnitz_wire::OrderKey {
            col: col as u16,
            desc: !key.asc,
            nulls_first: key.nulls_first,
        });
    }
    Ok(out)
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

/// Resolve the ORDER BY of an already-projected result (the aggregate finisher's
/// output) against its own output schema, to the wire `OrderKey`s
/// [`read_spec_finish`] windows by.
///
/// `col` is a full output-schema column index and `desc = !asc`, which is what
/// [`read_spec_finish`] decodes — so both sinks finish through that one function.
pub(crate) fn resolve_out_schema_order(
    order_by: Option<&OrderBy>,
    schema: &Schema,
) -> Result<Vec<gnitz_wire::OrderKey>, GnitzSqlError> {
    let Some(ob) = order_by else {
        return Ok(Vec::new());
    };
    resolve_order_by(ob)?
        .iter()
        .map(|k| {
            Ok(gnitz_wire::OrderKey {
                col: resolve_key_col(k, schema)? as u16,
                desc: !k.asc,
                nulls_first: k.nulls_first,
            })
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
    offset: usize,
    limit: Option<usize>,
) -> (Schema, ZSetBatch) {
    let sort_keys: Vec<OrderLocator> = order_keys
        .iter()
        .map(|k| sort_key(&schema, k.col as usize, !k.desc, k.nulls_first))
        .collect();
    finish_window(schema, batch, sort_keys, offset, limit)
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
    offset: usize,
    limit: Option<usize>,
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
