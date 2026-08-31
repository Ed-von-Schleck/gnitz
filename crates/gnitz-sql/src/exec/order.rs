//! Ad-hoc SELECT ordering & pagination sink: ORDER BY / OFFSET / LIMIT applied
//! as a single client-side pass over the fetched batch (base tables and views).
//!
//! The batch arrives as a client `ZSetBatch`: entries carrying an integer
//! **weight**, decoded to native little-endian (not the engine's OPK). Entries
//! are *not* unique by `(PK, payload)` — a reply train is concatenated and never
//! folded — and the sink does not need them to be, only `weight >= 1`.
//! So ordering is a per-type, per-column typed compare (the shared
//! `gnitz_wire::cmp_typed_le`); there is no client `memcmp`/OPK trick. The sink
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
use gnitz_core::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch};
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_wire::cmp_typed_le;
use sqlparser::ast::{Expr, OrderBy, OrderByKind};

// ---------------------------------------------------------------------------
// One sort key over the full pre-projection schema
// ---------------------------------------------------------------------------

/// A resolved sort key: a column index into the pre-projection (`actual`)
/// schema, its direction, its **absolute** NULL placement (not flipped by
/// DESC), and the per-column accessor facts — type, stride, PK byte offset,
/// null-bit mask — hoisted out of the comparator (they are invariant per key,
/// and the comparator runs O(n log n) times).
struct SortKey {
    ci: usize,
    asc: bool,
    nulls_first: bool,
    tc: TypeCode,
    stride: usize,
    /// The column's byte offset inside the packed PK region; `None` for a
    /// payload column.
    pk_offset: Option<usize>,
    /// Single-bit mask of the column's payload null bit; `0` for a PK column
    /// (never NULL).
    null_mask: u64,
}

impl SortKey {
    /// Every address the comparator needs comes from the one resolved-addressing
    /// record, [`ColumnLocator`] — not from four independent `is_pk_col` /
    /// `pk_byte_offset` / `payload_idx` / `wire_stride` lookups that could
    /// disagree. It is the same record the engine's worker-side ORDER BY
    /// comparator resolves through, which is what keeps the two *coordinate*-
    /// equivalent as the schema layout evolves. Only the coordinates: the bytes
    /// at them differ, because this comparator reads the in-memory `PkColumn`
    /// buffer, which is native-LE, where the engine reads an OPK region — so
    /// each comparator decodes its own side.
    fn new(schema: &Schema, ci: usize, asc: bool, nulls_first: bool) -> Self {
        let loc = SchemaFacts::locate(schema, ci);
        let (pk_offset, null_mask) = match loc {
            ColumnLocator::Pk { byte_off, .. } => (Some(byte_off as usize), 0),
            ColumnLocator::Payload { slot, .. } => (None, 1u64 << slot),
        };
        SortKey {
            ci,
            asc,
            nulls_first,
            tc: schema.columns[ci].type_code,
            stride: loc.size(),
            pk_offset,
            null_mask,
        }
    }
}

/// Whether the key's column is SQL NULL at row `i` (a PK column's mask is `0`).
fn col_is_null(batch: &ZSetBatch, key: &SortKey, i: usize) -> bool {
    batch.nulls[i] & key.null_mask != 0
}

/// Compare the **non-null** value of the key's column between rows `ra` and
/// `rb` in content order (callers null-check first). PK columns read through
/// the shared `PkColumn::col_window` accessor; payload columns dispatch
/// on their `ColData` variant. Fixed-width values compare via the shared
/// `cmp_typed_le` — dispatched on the type code, never through
/// `FixedInt::decode_le_i64`, which bit-reinterprets a full-width `U64` as
/// `i64` (correct for WHERE arithmetic, wrong for ordering: it would sort
/// `u64::MAX` first). STRING/BLOB compare byte-wise on the materialized value —
/// provably the engine's content order (`compare_german_strings` reduces to
/// plain lexicographic byte order for equal content, and no `COLLATE` exists).
fn cmp_col_value(batch: &ZSetBatch, key: &SortKey, ra: usize, rb: usize) -> Ordering {
    if let Some(off) = key.pk_offset {
        let wa = batch.pks.col_window(ra, off, key.stride);
        let wb = batch.pks.col_window(rb, off, key.stride);
        return cmp_typed_le(wa, wb, key.tc as u8);
    }
    match &batch.columns[key.ci] {
        ColData::Fixed(buf) => {
            let s = key.stride;
            cmp_typed_le(&buf[ra * s..ra * s + s], &buf[rb * s..rb * s + s], key.tc as u8)
        }
        ColData::Strings(v) => v[ra]
            .as_deref()
            .unwrap_or("")
            .as_bytes()
            .cmp(v[rb].as_deref().unwrap_or("").as_bytes()),
        ColData::Bytes(v) => v[ra].as_deref().unwrap_or(&[]).cmp(v[rb].as_deref().unwrap_or(&[])),
    }
}

/// Compare rows `ra`/`rb` under one key: absolute NULL placement, then the value
/// comparison reversed for DESC.
fn cmp_key(batch: &ZSetBatch, key: &SortKey, ra: usize, rb: usize) -> Ordering {
    match (col_is_null(batch, key, ra), col_is_null(batch, key, rb)) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if key.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if key.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {
            let ord = cmp_col_value(batch, key, ra, rb);
            if key.asc {
                ord
            } else {
                ord.reverse()
            }
        }
    }
}

/// Lexicographic compare over the key list — the permutation's sort comparator.
fn cmp_rows(batch: &ZSetBatch, keys: &[SortKey], ra: usize, rb: usize) -> Ordering {
    for key in keys {
        let ord = cmp_key(batch, key, ra, rb);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
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
    // Bare or qualified (`t.col`) identifier → name; qualifier is ignored
    // (crate-wide single-relation convention).
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
    if ob.interpolate.is_some() {
        return Err(unsupported("ORDER BY ... INTERPOLATE"));
    }
    let exprs = match &ob.kind {
        OrderByKind::Expressions(e) => e,
        OrderByKind::All(_) => return Err(unsupported("ORDER BY ALL")),
    };
    let mut keys = Vec::with_capacity(exprs.len());
    for obe in exprs {
        if obe.with_fill.is_some() {
            return Err(unsupported("ORDER BY ... WITH FILL"));
        }
        let asc = obe.options.asc.unwrap_or(true);
        // Absolute default: ASC → NULLS LAST, DESC → NULLS FIRST.
        let nulls_first = obe.options.nulls_first.unwrap_or(!asc);
        keys.push(OrderKey {
            target: order_target(&obe.expr)?,
            asc,
            nulls_first,
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
/// `SortKey::new`. Positions resolve against the VISIBLE columns; appends are
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
fn push_identity_tiebreak(keys: &mut Vec<SortKey>, schema: &Schema) {
    for &ci in &schema.pk_cols {
        keys.push(SortKey::new(schema, ci, true, true));
    }
    for (_, ci, _) in schema.payload_columns() {
        keys.push(SortKey::new(schema, ci, true, true));
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
/// `SortKey::new`, NOT resolved by name, so a projected-column / alias ORDER BY
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
    let sort_keys: Vec<SortKey> = order_keys
        .iter()
        .map(|k| SortKey::new(&schema, k.col as usize, !k.desc, k.nulls_first))
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
    mut full: ZSetBatch,
    mut sort_keys: Vec<SortKey>,
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
        if has_cut {
            push_identity_tiebreak(&mut sort_keys, &schema);
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
    let gather = RowGather::new(&schema);
    let mut gathered = ZSetBatch::with_capacity(&schema, surviving_rows.len());
    for (pos, surviving) in surviving_rows {
        gather.take(&mut full, perm[pos], &mut gathered);
        // The gather copied the weight verbatim; overwrite with the
        // window-clipped multiplicity (a boundary entry keeps a reduced one).
        *gathered.weights.last_mut().unwrap() = surviving;
    }
    (schema, gathered)
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
