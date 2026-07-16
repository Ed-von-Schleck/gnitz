//! Ad-hoc SELECT ordering & pagination sink: ORDER BY / OFFSET / LIMIT applied
//! as a single client-side pass over the fetched batch (base tables and views).
//!
//! The batch arrives as a client `ZSetBatch` — one entry per `(PK, payload)` with
//! an integer **weight**, decoded to native little-endian (not the engine's OPK).
//! So ordering is a per-type, per-column typed compare (the shared
//! `gnitz_wire::cmp_typed_le`), the same primitive the residual filter
//! needs; there is no client `memcmp`/OPK trick. The sink sorts **before**
//! projection so an ORDER BY key absent from the projected columns still
//! resolves, then walks the sorted permutation by running **logical position** —
//! LIMIT/OFFSET count multiplicity (summed weight), never Z-set entries — and
//! gathers the surviving rows. See `plans` design notes; the invariants are
//! pinned by the unit tests below.

use std::cmp::Ordering;

use crate::ast_util::{expr_usize_literal, single_relation_col_name};
use crate::bind::find_unique_column;
use crate::error::GnitzSqlError;
use crate::exec::batch::{copy_batch_row_owned, pk_col_window_at, project, resolve_projection, Projection};
use gnitz_core::{ColData, Schema, TypeCode, ZSetBatch};
use gnitz_wire::cmp_typed_le;
use sqlparser::ast::{Expr, OrderBy, OrderByKind, SelectItem, Value};

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
    fn new(schema: &Schema, ci: usize, asc: bool, nulls_first: bool) -> Self {
        let tc = schema.columns[ci].type_code;
        let is_pk = schema.is_pk_col(ci);
        SortKey {
            ci,
            asc,
            nulls_first,
            tc,
            stride: tc.wire_stride(),
            pk_offset: is_pk.then(|| schema.pk_byte_offset(ci)),
            null_mask: if is_pk { 0 } else { 1u64 << schema.payload_idx(ci) },
        }
    }
}

/// Whether the key's column is SQL NULL at row `i` (a PK column's mask is `0`).
/// The bitmap is the single NULL source across every `ColData` variant — a
/// `Fixed`/`U128s` NULL is zero-filled filler with no per-value sentinel.
fn col_is_null(batch: &ZSetBatch, key: &SortKey, i: usize) -> bool {
    batch.nulls[i] & key.null_mask != 0
}

/// Compare the **non-null** value of the key's column between rows `ra` and
/// `rb` in content order (callers null-check first). PK columns read through
/// the shared [`pk_col_window_at`] variant accessor; payload columns dispatch
/// on their `ColData` variant. Fixed-width values compare via the shared
/// `cmp_typed_le` — dispatched on the type code, never through
/// `FixedInt::decode_le_i64`, which bit-reinterprets a full-width `U64` as
/// `i64` (correct for WHERE arithmetic, wrong for ordering: it would sort
/// `u64::MAX` first). STRING/BLOB compare byte-wise on the materialized value —
/// provably the engine's content order (`compare_german_strings` reduces to
/// plain lexicographic byte order for equal content, and no `COLLATE` exists).
fn cmp_col_value(batch: &ZSetBatch, key: &SortKey, ra: usize, rb: usize) -> Ordering {
    if let Some(off) = key.pk_offset {
        let wa = pk_col_window_at(batch, off, key.stride, ra);
        let wb = pk_col_window_at(batch, off, key.stride, rb);
        return cmp_typed_le(wa.as_slice(), wb.as_slice(), key.tc as u8);
    }
    match &batch.columns[key.ci] {
        ColData::Fixed(buf) => {
            let s = key.stride;
            cmp_typed_le(&buf[ra * s..ra * s + s], &buf[rb * s..rb * s + s], key.tc as u8)
        }
        ColData::U128s(v) => cmp_typed_le(&v[ra].to_le_bytes(), &v[rb].to_le_bytes(), key.tc as u8),
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
    // Integer literal → positional, parsed by the same fallible helper as
    // `extract_limit`/`extract_offset`.
    if let Expr::Value(vws) = e {
        if matches!(&vws.value, Value::Number(..)) {
            return Ok(OrderTarget::Position(expr_usize_literal(e, "ORDER BY position")?));
        }
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

/// Resolve one ORDER BY key to a column index into the pre-projection (`actual`)
/// schema. Positional → n-th **visible** output column (mapped back through
/// `col_indices`); a name tries the output columns/aliases first, then the
/// source (pre-projection) visible columns — so a non-projected column is
/// orderable but a hidden synthetic key (`_join_pk`) is not name-addressable.
fn resolve_key_to_source_col(
    key: &OrderKey,
    proj_schema: &Schema,
    col_indices: &[usize],
    actual_schema: &Schema,
) -> Result<usize, GnitzSqlError> {
    match &key.target {
        OrderTarget::Position(pos) => {
            // 1-based over the visible output columns (a view result carries a
            // hidden column at physical index 0, so a naive `pos-1` would sort by
            // the hidden key).
            let visible: Vec<usize> = proj_schema.visible_columns().map(|(i, _)| i).collect();
            if *pos == 0 || *pos > visible.len() {
                return Err(GnitzSqlError::Unsupported(format!(
                    "ORDER BY position {pos} is out of range (1..={})",
                    visible.len()
                )));
            }
            Ok(col_indices[visible[*pos - 1]])
        }
        OrderTarget::Name(name) => {
            // (2) output column name / alias (ambiguity → error), then
            // (3) source column name (visible only).
            if let Some(out_ci) = find_unique_column(&proj_schema.columns, name)? {
                return Ok(col_indices[out_ci]);
            }
            if let Some(src_ci) = find_unique_column(&actual_schema.columns, name)? {
                return Ok(src_ci);
            }
            Err(GnitzSqlError::Bind(format!("ORDER BY column '{name}' not found")))
        }
    }
}

/// Build the full sort-key list: the ORDER BY keys, then — only when a
/// LIMIT/OFFSET cut is present — the deterministic identity tiebreak (every PK
/// column in pk-list order, then every payload column in schema order, all ASC
/// NULLS FIRST) so distinct logical rows tied on the ORDER BY keys order
/// consistently across worker counts and the cut is a function of the data.
fn build_sort_keys(
    order_keys: &[OrderKey],
    resolved: &Projection,
    actual_schema: &Schema,
    has_cut: bool,
) -> Result<Vec<SortKey>, GnitzSqlError> {
    // The projected output schema + its source-column map. The passthrough
    // (`None`) reproduces the source schema, so the identity map applies.
    let identity_indices: Vec<usize>;
    let (proj_schema, col_indices): (&Schema, &[usize]) = match resolved {
        Some((s, ci)) => (s, ci),
        None => {
            identity_indices = (0..actual_schema.columns.len()).collect();
            (actual_schema, &identity_indices)
        }
    };

    let mut keys = Vec::with_capacity(order_keys.len());
    for k in order_keys {
        let ci = resolve_key_to_source_col(k, proj_schema, col_indices, actual_schema)?;
        keys.push(SortKey::new(actual_schema, ci, k.asc, k.nulls_first));
    }

    if has_cut && !keys.is_empty() {
        for &ci in &actual_schema.pk_cols {
            keys.push(SortKey::new(actual_schema, ci, true, true));
        }
        for (_, ci, _) in actual_schema.payload_columns() {
            keys.push(SortKey::new(actual_schema, ci, true, true));
        }
    }
    Ok(keys)
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
/// walk requires `wᵢ ≥ 1` (the DBSP bag positivity invariant — base tables
/// positive, UNION ALL sums positives, EXCEPT ALL clamps via `positive_diff`).
fn paginate(weights: &[i64], offset: u64, hi: u64) -> Vec<(usize, i64)> {
    let mut cum: u64 = 0;
    let mut out = Vec::new();
    for (i, &w) in weights.iter().enumerate() {
        debug_assert!(
            w > 0,
            "ordering sink: non-positive weight {w} violates the bag invariant"
        );
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

/// Sort (ORDER BY), paginate (OFFSET/LIMIT), and project the fetched batch.
/// `full_batch` is the whole pre-projection row set; `offset` defaults to 0 and
/// `limit` is `None` when unbounded. Returns the projected schema and result
/// batch. O(result), client-side.
pub(crate) fn order_limit_project(
    projection: &[SelectItem],
    actual_schema: &Schema,
    full_batch: Option<ZSetBatch>,
    order_by: Option<&OrderBy>,
    offset: usize,
    limit: Option<usize>,
) -> Result<(Schema, ZSetBatch), GnitzSqlError> {
    let order_keys = match order_by {
        Some(ob) => resolve_order_by(ob)?,
        None => Vec::new(),
    };

    let resolved = resolve_projection(projection, actual_schema)?;

    // Zero-copy passthrough: nothing to reorder, skip, or bound. The identity
    // projection hands the batch straight back.
    if order_keys.is_empty() && offset == 0 && limit.is_none() {
        return Ok(project(resolved, actual_schema, full_batch));
    }

    let has_cut = limit.is_some() || offset > 0;
    let sort_keys = build_sort_keys(&order_keys, &resolved, actual_schema, has_cut)?;

    let mut full = full_batch.unwrap_or_else(|| ZSetBatch::new(actual_schema));
    let n = full.len();

    // Stable-sorted row permutation over the full batch.
    let mut perm: Vec<usize> = (0..n).collect();
    if !sort_keys.is_empty() {
        perm.sort_by(|&ra, &rb| cmp_rows(&full, &sort_keys, ra, rb));
    }

    // Multiplicity walk over the sorted order, then gather the survivors —
    // moving each row's String/Blob cells out of the owned `full` (every source
    // row survives at most once; `full` is dropped right after).
    let off = offset as u64;
    let hi = limit.map_or(u64::MAX, |l| off.saturating_add(l as u64));
    let ordered_weights: Vec<i64> = perm.iter().map(|&r| full.weights[r]).collect();

    let mut gathered = ZSetBatch::new(actual_schema);
    for (pos, surviving) in paginate(&ordered_weights, off, hi) {
        let row = perm[pos];
        copy_batch_row_owned(&mut full, row, &mut gathered, actual_schema);
        // `copy_batch_row_owned` copied `full.weights[row]` verbatim; overwrite
        // with the window-clipped multiplicity (a boundary entry keeps a reduced
        // one).
        *gathered.weights.last_mut().unwrap() = surviving;
    }

    Ok(project(resolved, actual_schema, Some(gathered)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::batch::pk_col_window;
    use crate::test_support::col_def;
    use gnitz_core::null_word_get;
    use sqlparser::ast::{Query, SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    // The per-type window comparison itself (`cmp_typed_le`) is pinned by
    // gnitz-wire's own tests; the tests here cover the sink built on it.

    // ---- paginate (multiplicity walk) ----

    #[test]
    fn paginate_all_weight_one_is_entry_counting() {
        let w = vec![1i64; 5];
        // LIMIT 3.
        assert_eq!(paginate(&w, 0, 3), vec![(0, 1), (1, 1), (2, 1)]);
        // OFFSET 2 LIMIT 2.
        assert_eq!(paginate(&w, 2, 4), vec![(2, 1), (3, 1)]);
    }

    #[test]
    fn paginate_boundary_entry_keeps_reduced_weight() {
        // Entries [2,3,2]; LIMIT 3 lands inside entry 1 (weight 3 → 1 survives).
        let w = vec![2i64, 3, 2];
        assert_eq!(paginate(&w, 0, 3), vec![(0, 2), (1, 1)]);
        // The surviving logical-row count is exactly 3 (bag semantics).
        assert_eq!(paginate(&w, 0, 3).iter().map(|(_, x)| *x).sum::<i64>(), 3);
    }

    #[test]
    fn paginate_offset_and_limit_inside_one_entry() {
        // Weight-5 entry, OFFSET 1 LIMIT 1 → the overlap formula yields weight 1
        // (a two-independent-passes impl gets this wrong).
        let w = vec![5i64];
        assert_eq!(paginate(&w, 1, 2), vec![(0, 1)]);
    }

    #[test]
    fn paginate_weight3_straddles_cut() {
        // A weight-3 entry straddling the window keeps only the overlapping part.
        let w = vec![3i64, 3, 3];
        // window [0,4): entry0 whole (3), entry1 clipped to 1.
        assert_eq!(paginate(&w, 0, 4), vec![(0, 3), (1, 1)]);
        // window [4,7): entry1 clipped low (2), entry2 clipped high (1).
        assert_eq!(paginate(&w, 4, 7), vec![(1, 2), (2, 1)]);
    }

    // ---- sink integration ----

    /// `(id U64 pk, v I64 nullable, s STRING nullable)`.
    fn kv_schema() -> Schema {
        Schema {
            columns: vec![
                col_def("id", TypeCode::U64, false),
                col_def("v", TypeCode::I64, true),
                col_def("s", TypeCode::String, true),
            ],
            pk_cols: vec![0],
        }
    }

    /// Push one row (`pk`, optional `v`, optional `s`, weight) into a `kv_schema` batch.
    fn push_kv(b: &mut ZSetBatch, pk: u64, v: Option<i64>, s: Option<&str>, weight: i64) {
        b.pks.push_u128(pk as u128);
        b.weights.push(weight);
        let mut null_word = 0u64;
        if v.is_none() {
            null_word |= 1 << 0; // payload idx 0 = v
        }
        if s.is_none() {
            null_word |= 1 << 1; // payload idx 1 = s
        }
        b.nulls.push(null_word);
        if let ColData::Fixed(buf) = &mut b.columns[1] {
            buf.extend_from_slice(&v.unwrap_or(0).to_le_bytes());
        }
        if let ColData::Strings(vs) = &mut b.columns[2] {
            vs.push(Some(s.unwrap_or("").to_string()));
        }
    }

    fn parse_query(sql: &str) -> Query {
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            Statement::Query(q) => *q,
            _ => panic!("not a query"),
        }
    }

    /// Run the sink for `sql` over `batch`, returning the ordered `(id, v)` pairs.
    fn run(sql: &str, schema: &Schema, batch: ZSetBatch) -> Result<Vec<(u64, Option<i64>)>, GnitzSqlError> {
        let q = parse_query(sql);
        let select = match q.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => panic!("not a select"),
        };
        // The test SQL only ever uses `LIMIT <n>`; the deeper `Expr::Value` is
        // folded into the outer pattern so no re-match of the same binding.
        let limit = match &q.limit_clause {
            Some(sqlparser::ast::LimitClause::LimitOffset {
                limit: Some(Expr::Value(vws)),
                ..
            }) => match &vws.value {
                Value::Number(n, _) => n.parse::<usize>().ok(),
                _ => None,
            },
            _ => None,
        };
        let (out_schema, out) =
            order_limit_project(&select.projection, schema, Some(batch), q.order_by.as_ref(), 0, limit)?;
        let id_ci = out_schema.pk_cols[0];
        let v_ci = out_schema.columns.iter().position(|c| c.name == "v");
        let mut rows = Vec::new();
        for i in 0..out.len() {
            let id = out.pks.get(i) as u64;
            let v = v_ci.and_then(|ci| {
                if null_word_get(out.nulls[i], out_schema.payload_idx(ci)) {
                    None
                } else if let ColData::Fixed(buf) = &out.columns[ci] {
                    Some(i64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap()))
                } else {
                    None
                }
            });
            let _ = id_ci;
            rows.push((id, v));
        }
        Ok(rows)
    }

    #[test]
    fn sink_order_by_asc_desc_and_source_col() {
        let schema = kv_schema();
        let mut b = ZSetBatch::new(&schema);
        push_kv(&mut b, 1, Some(30), Some("a"), 1);
        push_kv(&mut b, 2, Some(10), Some("b"), 1);
        push_kv(&mut b, 3, Some(20), Some("c"), 1);

        // ORDER BY a non-projected source column (`v`), ascending.
        let got = run("SELECT id FROM t ORDER BY v", &schema, b.clone()).unwrap();
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 3, 1]);
        // DESC reverses.
        let got = run("SELECT id FROM t ORDER BY v DESC", &schema, b).unwrap();
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 3, 2]);
    }

    #[test]
    fn sink_order_by_alias_and_positional() {
        let schema = kv_schema();
        let mut b = ZSetBatch::new(&schema);
        push_kv(&mut b, 1, Some(30), None, 1);
        push_kv(&mut b, 2, Some(10), None, 1);
        push_kv(&mut b, 3, Some(20), None, 1);

        // Alias resolves to the output column; sort ascending by it.
        let got = run("SELECT id, v AS val FROM t ORDER BY val", &schema, b.clone()).unwrap();
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 3, 1]);
        // Positional: 2nd output column is `v`.
        let got = run("SELECT id, v FROM t ORDER BY 2 DESC", &schema, b).unwrap();
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 3, 2]);
    }

    #[test]
    fn sink_nulls_default_and_explicit_absolute() {
        let schema = kv_schema();
        let mut b = ZSetBatch::new(&schema);
        push_kv(&mut b, 1, Some(10), None, 1);
        push_kv(&mut b, 2, None, None, 1);
        push_kv(&mut b, 3, Some(20), None, 1);

        // ASC → NULLS LAST by default.
        let got = run("SELECT id, v FROM t ORDER BY v", &schema, b.clone()).unwrap();
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![1, 3, 2]);
        // DESC → NULLS FIRST by default (placement is absolute, not value-flipped).
        let got = run("SELECT id, v FROM t ORDER BY v DESC", &schema, b.clone()).unwrap();
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 3, 1]);
        // Explicit NULLS FIRST on ASC overrides the default.
        let got = run("SELECT id, v FROM t ORDER BY v ASC NULLS FIRST", &schema, b).unwrap();
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 1, 3]);
    }

    #[test]
    fn sink_string_order_and_multikey() {
        let schema = kv_schema();
        let mut b = ZSetBatch::new(&schema);
        push_kv(&mut b, 1, Some(1), Some("banana"), 1);
        push_kv(&mut b, 2, Some(1), Some("apple"), 1);
        push_kv(&mut b, 3, Some(2), Some("apple"), 1);

        // ORDER BY v, s: (1,apple)=2, (1,banana)=1, (2,apple)=3.
        let got = run("SELECT id, v, s FROM t ORDER BY v, s", &schema, b).unwrap();
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 1, 3]);
    }

    #[test]
    fn sink_limit_counts_multiplicity() {
        let schema = kv_schema();
        let mut b = ZSetBatch::new(&schema);
        // A weight-3 entry sorts first, then a weight-1 entry.
        push_kv(&mut b, 1, Some(10), Some("x"), 3);
        push_kv(&mut b, 2, Some(20), Some("y"), 1);

        // LIMIT 2 must clip the weight-3 entry to weight 2 (2 logical rows), not
        // return 2 entries (= 4 logical rows).
        let q = parse_query("SELECT id, v FROM t ORDER BY v LIMIT 2");
        let select = match q.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => unreachable!(),
        };
        let (out_schema, out) =
            order_limit_project(&select.projection, &schema, Some(b), q.order_by.as_ref(), 0, Some(2)).unwrap();
        assert_eq!(out.len(), 1, "one entry survives");
        assert_eq!(out.weights[0], 2, "clipped to weight 2");
        let _ = out_schema;
    }

    #[test]
    fn sink_positional_over_hidden_view_schema() {
        // A view result carries a hidden key at physical index 0; position 1 must
        // name the first VISIBLE column, not the hidden key.
        let schema = Schema {
            columns: vec![
                col_def("_group_pk", TypeCode::U128, false).hidden(),
                col_def("city", TypeCode::U64, false),
                col_def("cnt", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        };
        let mut b = ZSetBatch::new(&schema);
        let mut push = |pk: u128, city: u64, cnt: i64| {
            b.pks.push_u128(pk);
            b.weights.push(1);
            b.nulls.push(0);
            if let ColData::Fixed(buf) = &mut b.columns[1] {
                buf.extend_from_slice(&city.to_le_bytes());
            }
            if let ColData::Fixed(buf) = &mut b.columns[2] {
                buf.extend_from_slice(&cnt.to_le_bytes());
            }
        };
        push(100, 30, 1);
        push(200, 10, 1);
        push(300, 20, 1);

        // ORDER BY 1 → the first visible column `city`, ascending.
        let q = parse_query("SELECT * FROM v ORDER BY 1");
        let select = match q.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => unreachable!(),
        };
        let (out_schema, out) =
            order_limit_project(&select.projection, &schema, Some(b), q.order_by.as_ref(), 0, None).unwrap();
        // Read `city` (physical col 1) in output order.
        let city_ci = out_schema.columns.iter().position(|c| c.name == "city").unwrap();
        let cities: Vec<u64> = (0..out.len())
            .map(|i| {
                let ColData::Fixed(buf) = &out.columns[city_ci] else {
                    unreachable!()
                };
                u64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap())
            })
            .collect();
        assert_eq!(cities, vec![10, 20, 30]);
    }

    #[test]
    fn sink_null_detected_via_bitmap_in_u128_column() {
        // A NULL U128 payload is zero-filled filler; NULL must be read from the
        // bitmap, not an is_none() on the value.
        let schema = Schema {
            columns: vec![col_def("id", TypeCode::U64, false), col_def("u", TypeCode::U128, true)],
            pk_cols: vec![0],
        };
        let mut b = ZSetBatch::new(&schema);
        let mut push = |pk: u128, u: Option<u128>| {
            b.pks.push_u128(pk);
            b.weights.push(1);
            b.nulls.push(if u.is_none() { 1 } else { 0 });
            if let ColData::U128s(v) = &mut b.columns[1] {
                v.push(u.unwrap_or(0));
            }
        };
        push(1, Some(5));
        push(2, None);
        push(3, Some(1));

        // ASC NULLS LAST default: 1(=1), 5, NULL → ids 3, 1, 2.
        let q = parse_query("SELECT id, u FROM t ORDER BY u");
        let select = match q.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => unreachable!(),
        };
        let (out_schema, out) =
            order_limit_project(&select.projection, &schema, Some(b), q.order_by.as_ref(), 0, None).unwrap();
        let ids: Vec<u64> = (0..out.len()).map(|i| out.pks.get(i) as u64).collect();
        assert_eq!(ids, vec![3, 1, 2]);
        let _ = out_schema;
    }

    #[test]
    fn sink_compound_pk_key_orders_by_typed_value() {
        // Compound PK (a U16, b I16); sort by the SECOND PK column `b` which holds
        // a negative — pins that the Bytes accessor + typed compare handle a
        // multi-byte LE value (256 vs 1) and a non-leading signed column.
        let schema = Schema {
            columns: vec![
                col_def("a", TypeCode::U16, false),
                col_def("b", TypeCode::I16, false),
                col_def("w", TypeCode::I64, true),
            ],
            pk_cols: vec![0, 1],
        };
        let mut b = ZSetBatch::new(&schema);
        let mut push = |a: u16, bb: i16| {
            let mut pk = [0u8; 4];
            pk[..2].copy_from_slice(&a.to_le_bytes());
            pk[2..].copy_from_slice(&bb.to_le_bytes());
            b.pks.push_bytes(&pk);
            b.weights.push(1);
            b.nulls.push(1); // w NULL (payload idx 0)
            if let ColData::Fixed(buf) = &mut b.columns[2] {
                buf.extend_from_slice(&0i64.to_le_bytes());
            }
        };
        // rows: b ∈ {1, -5, 256}; ascending signed order is -5, 1, 256.
        push(1, 1);
        push(2, -5);
        push(3, 256);

        let q = parse_query("SELECT a, b FROM t ORDER BY b");
        let select = match q.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => unreachable!(),
        };
        let (out_schema, out) =
            order_limit_project(&select.projection, &schema, Some(b), q.order_by.as_ref(), 0, None).unwrap();
        // Read `b` from the compound PK region (physical col 1).
        let bs: Vec<i16> = (0..out.len())
            .map(|i| {
                let w = pk_col_window(&out, &out_schema, 1, i);
                i16::from_le_bytes(w.as_slice().try_into().unwrap())
            })
            .collect();
        assert_eq!(bs, vec![-5, 1, 256]);
    }

    #[test]
    fn sink_zero_copy_passthrough() {
        // No ORDER BY, no OFFSET, no LIMIT → the source batch flows straight
        // through (identity projection), unchanged.
        let schema = kv_schema();
        let mut b = ZSetBatch::new(&schema);
        push_kv(&mut b, 3, Some(30), Some("c"), 1);
        push_kv(&mut b, 1, Some(10), Some("a"), 1);
        let clone = b.clone();
        let q = parse_query("SELECT * FROM t");
        let select = match q.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => unreachable!(),
        };
        let (_, out) = order_limit_project(&select.projection, &schema, Some(b), q.order_by.as_ref(), 0, None).unwrap();
        assert_eq!(out, clone, "passthrough must not touch the batch (order preserved)");
    }

    #[test]
    fn sink_rejects_unsupported_order_by_forms() {
        let schema = kv_schema();
        let mk = |sql: &str| {
            let q = parse_query(sql);
            let select = match q.body.as_ref() {
                SetExpr::Select(s) => s.clone(),
                _ => unreachable!(),
            };
            order_limit_project(
                &select.projection,
                &schema,
                Some(ZSetBatch::new(&schema)),
                q.order_by.as_ref(),
                0,
                None,
            )
            .map(|_| ())
        };
        // A non-column-ref expression key → Unsupported.
        assert!(matches!(
            mk("SELECT id, v FROM t ORDER BY v + 1"),
            Err(GnitzSqlError::Unsupported(_))
        ));
        // Positional out of range → error (position 9 > 2 output cols; position 0).
        assert!(mk("SELECT id, v FROM t ORDER BY 9").is_err());
        assert!(mk("SELECT id, v FROM t ORDER BY 0").is_err());
    }

    #[test]
    fn resolve_order_by_rejects_clickhouse_duckdb_extensions() {
        // `ORDER BY ALL` / `WITH FILL` / `INTERPOLATE` only reach `resolve_order_by`
        // as their typed AST forms under a dialect that parses them (GenericDialect
        // parses `ALL` as an identifier), so drive the arms directly.
        use sqlparser::ast::{Ident, Interpolate, OrderByExpr, OrderByOptions, WithFill};
        let ident_key = || OrderByExpr {
            expr: Expr::Identifier(Ident::new("v")),
            options: OrderByOptions::default(),
            with_fill: None,
        };

        let all = OrderBy {
            kind: OrderByKind::All(OrderByOptions::default()),
            interpolate: None,
        };
        assert!(matches!(resolve_order_by(&all), Err(GnitzSqlError::Unsupported(_))));

        let interpolate = OrderBy {
            kind: OrderByKind::Expressions(vec![ident_key()]),
            interpolate: Some(Interpolate { exprs: None }),
        };
        assert!(matches!(
            resolve_order_by(&interpolate),
            Err(GnitzSqlError::Unsupported(_))
        ));

        let with_fill = OrderBy {
            kind: OrderByKind::Expressions(vec![OrderByExpr {
                expr: Expr::Identifier(Ident::new("v")),
                options: OrderByOptions::default(),
                with_fill: Some(WithFill {
                    from: None,
                    to: None,
                    step: None,
                }),
            }]),
            interpolate: None,
        };
        assert!(matches!(
            resolve_order_by(&with_fill),
            Err(GnitzSqlError::Unsupported(_))
        ));
    }
}
