//! Parameterized bounded read (`ReadSpec`) execution — the worker half of the
//! ad-hoc SELECT scan. Runs **once per worker over one merged partition cursor**:
//! open a cursor for the bound, then per chunk `op_filter(predicate)` → the
//! sink — rows (`op_map(projection)` → bounded top-k / materialize) or the
//! aggregate hash-fold (`AdhocFold`). No DBSP circuit, no operator state, no
//! exchange.
//!
//! The reply schema arrives as the client's raw echoed wire block (decoded by the
//! worker one layer up); this module takes the decoded `SchemaDescriptor` and
//! never rebuilds the block.

use std::cmp::Ordering;

use gnitz_wire::{AggReadSpec, OrderKey, RangeDescriptor, ReadBound, ReadSink, ReadSpec, TypeCode};

use super::store_io::SourceCursor;
use super::*;
use crate::expr::{LogicalProgram, ScalarFunc};
use crate::ops::{op_filter, op_map, AdhocFold, ReindexSpec};
use crate::schema::key::opk_key;
use crate::schema::{null_bit, ColumnLocator, MAX_PK_BYTES};
use crate::storage::{cmp_col_window, compare_pk_bytes, compare_rows, ColumnarSource, PkBuf};

/// `limit_k` above which the worker materializes instead of running the bounded
/// top-k sink (a deep OFFSET ships unsorted and the client sorts). Worker
/// policy, not wire contract — the client never consults it.
const MAX_WORKER_TOPK: u64 = 65_536;

impl CatalogEngine {
    /// Execute `spec` against `target_id` on this worker's merged partition
    /// cursor, returning one keeper batch in the `reply_schema` shape. The caller
    /// (the worker dispatch arm) decoded `reply_schema` from the client's echoed
    /// wire block and replies by force-including those raw bytes; this method does
    /// the bound walk, predicate, projection, and ORDER BY / LIMIT reduction.
    ///
    /// `Err` on a corrupt program blob, a reply schema that does not match the
    /// sink's expected shape (rows: PK-stride equality; fold: the derived
    /// SyntheticFold layout), a missing wide-int index — every one a
    /// corrupt/stale frame, surfaced as a `STATUS_ERROR` reply — or the fold's
    /// per-worker group cap (a resource-exhaustion abort).
    pub(crate) fn scan_spec_family(
        &mut self,
        target_id: i64,
        spec: &ReadSpec,
        reply_schema: &SchemaDescriptor,
    ) -> Result<Batch, String> {
        let src_schema = self.table_entry(target_id)?.schema;

        // Compile the predicate once per request, exactly as the circuit
        // compiler does (`query/compiler/emit.rs`).
        let predicate = match spec.predicate.is_empty() {
            true => None,
            false => Some(compile_predicate(&spec.predicate, &src_schema)?),
        };

        let chunk_rows = self.ddl_scan_chunk_rows.max(1);
        let mut source = self.open_scan_spec_cursor(target_id, &spec.bound, &src_schema)?;

        match &spec.sink {
            ReadSink::Fold(agg) => run_scan_fold_sink(
                &mut source,
                predicate.as_ref(),
                &src_schema,
                reply_schema,
                agg,
                chunk_rows,
                self.adhoc_group_cap,
            ),
            ReadSink::Rows {
                projection,
                order,
                limit_k,
            } => {
                // op_map's identity reindex byte-copies the source OPK verbatim
                // into the reply PK region, gated on stride equality (a mismatch
                // would zero-fill it and corrupt the client's PK tiebreak). The
                // trusted planner always matches; this guards the echoed client
                // blob. (A fold sink emits a synthetic `_agg_pk` PK and never
                // byte-copies the source PK, so the guard is rows-sink-only.)
                if reply_schema.pk_stride() != src_schema.pk_stride() {
                    return Err(format!(
                        "scan_spec: reply pk_stride {} != source pk_stride {}",
                        reply_schema.pk_stride(),
                        src_schema.pk_stride()
                    ));
                }
                let projection = match projection.is_empty() {
                    true => None,
                    false => Some(compile_projection(projection, &src_schema, reply_schema)?),
                };
                Ok(run_scan_rows_sink(
                    &mut source,
                    predicate.as_ref(),
                    projection.as_ref(),
                    &src_schema,
                    reply_schema,
                    order,
                    *limit_k,
                    chunk_rows,
                ))
            }
        }
    }

    /// Open the source cursor for `bound` over `source`'s merged partitions.
    fn open_scan_spec_cursor(
        &mut self,
        source: i64,
        bound: &ReadBound,
        src_schema: &SchemaDescriptor,
    ) -> Result<ScanSpecCursor, String> {
        match bound {
            ReadBound::None => Ok(ScanSpecCursor::Source(SourceCursor::Full(Box::new(
                self.table_entry(source)?.handle.open_cursor(),
            )))),
            ReadBound::PkRange(desc) => match pk_range_keys(src_schema, desc)? {
                None => Ok(ScanSpecCursor::Source(SourceCursor::Empty)),
                Some((start, end)) => {
                    let mut cursor = self.table_entry(source)?.handle.open_cursor();
                    // `[start, end)` clamps every underlying source, so the walk
                    // is O(range): a point lookup drains exactly its group,
                    // never a boundary chunk of over-read.
                    cursor.seek_range_bytes(start.pk_bytes(), end.as_ref().map(|e| e.pk_bytes()));
                    Ok(ScanSpecCursor::Source(SourceCursor::Full(Box::new(cursor))))
                }
            },
            ReadBound::IndexRange { idx_cols, desc } => {
                let cols = gnitz_wire::unpack_pk_cols(*idx_cols);
                if !cols.is_well_formed() {
                    return Err(format!("scan_spec: malformed index column list for table {source}"));
                }
                // The range column's type picks the walk — the same split the SQL
                // layer strips conjuncts by (`TypeCode::is_wide_int`, the one
                // authority): wide → un-gated byte-exact walk, since no compiled
                // predicate can re-impose the bound; narrow → selectivity-gated
                // (may fall back to a full cursor), the conjunct rides the
                // predicate.
                let range_col = cols
                    .as_slice()
                    .get(desc.eq_vals().len())
                    .map(|&c| c as usize)
                    .filter(|&c| c < src_schema.num_columns())
                    .ok_or_else(|| format!("scan_spec: index range column out of bounds for table {source}"))?;
                let wide =
                    TypeCode::try_from_u8(src_schema.columns[range_col].type_code).is_some_and(|tc| tc.is_wide_int());
                if wide {
                    match self.open_index_range_cursor(source, cols.as_slice(), desc, 0)? {
                        None => Ok(ScanSpecCursor::Source(SourceCursor::Empty)),
                        Some(c) => Ok(ScanSpecCursor::Source(SourceCursor::Bounded(Box::new(c)))),
                    }
                } else {
                    match self.open_bounded_source(source, cols.as_slice(), desc) {
                        None => Err(format!("scan_spec: source table {source} unregistered")),
                        Some(sc) => Ok(ScanSpecCursor::Source(sc)),
                    }
                }
            }
            ReadBound::PkSet(keys) => {
                // Trust boundary: the decoder cannot know the PK arity, so the
                // schema check lands here — a hard reject, matching every other
                // malformed-frame condition (release builds must not clamp).
                if src_schema.pk_indices().len() != 1 {
                    return Err(format!(
                        "scan_spec: PkSet gather requires a single-column PK (table {source})"
                    ));
                }
                let cursor = self.table_entry(source)?.handle.open_cursor();
                let stride = src_schema.pk_stride() as usize;
                // Encode each native key to its OPK image and sort byte-wise:
                // OPK order IS typed PK order, so the gather below is one
                // monotone forward sweep regardless of wire key order. A single
                // PK column is at most 16 bytes, so the images are flat
                // fixed-size arrays (zero tail bytes never affect the order).
                let mut opk_keys: Vec<[u8; 16]> = keys
                    .iter()
                    .map(|&k| {
                        let (opk, st) = opk_key(src_schema, &k.to_le_bytes());
                        debug_assert_eq!(st, stride);
                        let mut key = [0u8; 16];
                        key[..stride].copy_from_slice(&opk[..stride]);
                        key
                    })
                    .collect();
                opk_keys.sort_unstable();
                Ok(ScanSpecCursor::PkSet(Box::new(PkSetGather {
                    cursor,
                    keys: opk_keys,
                    stride,
                    next: 0,
                    src_schema: *src_schema,
                })))
            }
        }
    }
}

/// A `pk IN (…)` gather: one live row per key, in ascending OPK order.
struct PkSetGather {
    cursor: ReadCursor,
    /// OPK images of the keys, sorted; the leading `stride` bytes are the key.
    keys: Vec<[u8; 16]>,
    stride: usize,
    next: usize,
    src_schema: SchemaDescriptor,
}

/// The per-bound chunk producer feeding the sink. Each variant yields
/// source-schema chunks of consolidated (PK, payload) groups; the sink then
/// filters, projects, and reduces them.
enum ScanSpecCursor {
    /// Full merged cursor, bounded index walk, or provably-empty bound — the
    /// shared source cursor. A PK range is a Full cursor positioned on
    /// `[start, end)` (`seek_range_bytes`), so it exhausts exactly at the cut.
    Source(SourceCursor),
    /// `pk IN (…)` gather (boxed: the cursor is ~560 bytes — one allocation per
    /// request, never per chunk).
    PkSet(Box<PkSetGather>),
}

impl ScanSpecCursor {
    /// The next up-to-`max_rows` source rows, or `None` once the bound is
    /// exhausted. A returned batch may be empty (a window of PkSet misses);
    /// `None` strictly means "no further rows".
    fn next_chunk(&mut self, max_rows: usize) -> Option<Batch> {
        match self {
            ScanSpecCursor::Source(source) => source.drain_chunk(max_rows),
            ScanSpecCursor::PkSet(g) => {
                if g.next >= g.keys.len() {
                    return None;
                }
                let cap = (g.keys.len() - g.next).min(max_rows);
                let mut out = Batch::with_schema(g.src_schema, cap);
                while g.next < g.keys.len() && out.count < max_rows {
                    let key = g.keys[g.next];
                    g.next += 1;
                    if g.cursor.advance_to_exact_live(&key[..g.stride]) {
                        let w = g.cursor.current_weight;
                        debug_assert!(w > 0, "advance_to_exact_live guarantees a positive weight");
                        g.cursor.copy_current_row_into(&mut out, w);
                    }
                }
                // An all-miss window returns an empty batch only when the key list
                // is exhausted; otherwise it advanced `next` and there is more.
                (out.count > 0 || g.next < g.keys.len()).then_some(out)
            }
        }
    }
}

/// Run the fold sink over `source`: fold every surviving chunk into per-group
/// accumulators and return the partial reduce-output rows — or `Err` when the
/// per-worker group cap is exceeded (a resource-exhaustion abort, before any
/// data frame is sent). A fold spec carries no projection: survivors fold
/// directly.
fn run_scan_fold_sink(
    source: &mut ScanSpecCursor,
    predicate: Option<&ScalarFunc>,
    src_schema: &SchemaDescriptor,
    reply_schema: &SchemaDescriptor,
    agg: &AggReadSpec,
    chunk_rows: usize,
    group_cap: usize,
) -> Result<Batch, String> {
    let mut fold = AdhocFold::new(src_schema, reply_schema, agg, group_cap)?;
    while let Some(chunk) = source.next_chunk(chunk_rows) {
        if chunk.count == 0 {
            continue;
        }
        let filtered = match predicate {
            Some(f) => op_filter(&chunk, f, src_schema),
            None => chunk,
        };
        if filtered.count == 0 {
            continue;
        }
        fold.fold_chunk(&filtered)?;
    }
    Ok(fold.finish())
}

/// Run the rows sink over `source`, returning one keeper batch in the
/// `reply_schema` shape (a superset of this worker's contribution; the client
/// re-sorts the concatenation and applies the exact window).
#[allow(clippy::too_many_arguments)]
fn run_scan_rows_sink(
    source: &mut ScanSpecCursor,
    predicate: Option<&ScalarFunc>,
    projection: Option<&ScalarFunc>,
    src_schema: &SchemaDescriptor,
    reply_schema: &SchemaDescriptor,
    order: &[OrderKey],
    limit_k: u64,
    chunk_rows: usize,
) -> Batch {
    // Bounded top-k: an ORDER BY with a small window. Everything else materializes
    // (an ORDER-BY early-stop would truncate in cursor order, not sort order).
    let topk = !order.is_empty() && limit_k > 0 && limit_k <= MAX_WORKER_TOPK;
    // No-ORDER-BY early stop: drain until the summed survivor weight reaches the
    // window. Draining in `limit_k`-sized chunks is what makes this O(limit_k)
    // rather than O(chunk) — but only when every drained row survives. With a
    // predicate, survivors ≪ drained rows and a tiny chunk degrades to
    // row-at-a-time cursor driving, so drain full chunks instead and over-read
    // at most one chunk.
    let early_stop = order.is_empty() && limit_k > 0;
    let drain_rows = if early_stop && predicate.is_none() {
        (limit_k as usize).clamp(1, chunk_rows)
    } else {
        chunk_rows
    };

    // Pre-resolve each ORDER BY key to its reply-schema locator (the top-k shape).
    let order_locs: Vec<OrderLocator> = order
        .iter()
        .map(|k| OrderLocator {
            loc: reply_schema.locate(k.col as usize),
            desc: k.desc,
            nulls_first: k.nulls_first,
        })
        .collect();

    let mut keeper = Batch::empty_with_schema(reply_schema);
    // Summed survivor weight — tracked only for the bounded shapes that read it.
    let mut summed: i64 = 0;

    while let Some(chunk) = source.next_chunk(drain_rows) {
        if chunk.count == 0 {
            continue;
        }
        let filtered = match predicate {
            Some(f) => op_filter(&chunk, f, src_schema),
            None => chunk,
        };
        if filtered.count == 0 {
            continue;
        }
        let projected = match projection {
            Some(f) => op_map(&filtered, f, src_schema, ReindexSpec::None),
            None => filtered,
        };
        if projected.count == 0 {
            continue;
        }
        if topk || early_stop {
            summed += sum_weights(&projected);
        }
        keeper.append_batch(&projected, 0, projected.count);

        if topk && summed > 2 * limit_k as i64 {
            (keeper, summed) = topk_keep(keeper, &order_locs, reply_schema, limit_k);
        }
        if early_stop && summed >= limit_k as i64 {
            break;
        }
    }

    // A final total ≤ `limit_k` provably cuts nothing — skip the sort outright.
    if topk && summed > limit_k as i64 {
        (keeper, _) = topk_keep(keeper, &order_locs, reply_schema, limit_k);
    }
    keeper
}

/// Sort `keeper` by the ORDER BY comparator and keep the smallest prefix whose
/// summed weight covers `limit_k` (the boundary row kept whole — a per-worker
/// row landing below the global cutoff needs its full weight, else a cross-worker
/// under-count results; the client window does the exact split). Returns the
/// kept batch and its summed weight.
fn topk_keep(
    keeper: Batch,
    order_locs: &[OrderLocator],
    reply_schema: &SchemaDescriptor,
    limit_k: u64,
) -> (Batch, i64) {
    if keeper.count == 0 {
        return (keeper, 0);
    }
    let mut perm: Vec<u32> = (0..keeper.count as u32).collect();
    let cmp = |a: &u32, b: &u32| scan_spec_cmp(order_locs, &keeper, *a as usize, *b as usize, reply_schema);
    // Every keeper row is a live consolidated group (weight ≥ 1 — the same
    // positivity the client's window truncation banks on), so the `limit_k`
    // comparator-smallest rows always cover the weight window: partial-select
    // them and sort only that prefix — O(n + k log k), not O(n log n) per
    // trigger — mirroring the client's own `read_spec_finish`.
    let k = (limit_k as usize).min(perm.len());
    if k < perm.len() {
        perm.select_nth_unstable_by(k - 1, cmp);
        perm.truncate(k);
    }
    perm.sort_unstable_by(cmp);

    let mut acc: i64 = 0;
    let mut cut = perm.len();
    for (i, &row) in perm.iter().enumerate() {
        acc += keeper.get_weight(row as usize);
        if acc >= limit_k as i64 {
            cut = i + 1; // keep [0, i] inclusive — the boundary row whole
            break;
        }
    }
    (
        Batch::from_indexed_rows(&keeper.as_mem_batch(), &perm[..cut], reply_schema),
        acc,
    )
}

/// One resolved ORDER BY key.
struct OrderLocator {
    loc: ColumnLocator,
    desc: bool,
    nulls_first: bool,
}

/// The worker-side ORDER BY comparator over two rows of a reply-schema batch.
/// Byte-for-byte equivalent to the client's `SortKey` order (§ shared primitives
/// `cmp_typed_le` / `compare_german_strings` / OPK tiebreak) so each worker keeps
/// a superset of its window contribution: user keys (NULLs placed absolutely per
/// `nulls_first`, values reversed for `desc`), then the deterministic OPK-then-
/// payload tiebreak.
fn scan_spec_cmp(
    order_locs: &[OrderLocator],
    b: &Batch,
    a: usize,
    c: usize,
    reply_schema: &SchemaDescriptor,
) -> Ordering {
    let a_null = b.get_null_word(a);
    let c_null = b.get_null_word(c);
    for key in order_locs {
        match key.loc {
            // PK columns are never NULL; compare the OPK byte window (order-preserving).
            ColumnLocator::Pk { byte_off, size, .. } => {
                let o = byte_off as usize;
                let sz = size as usize;
                let av = &b.get_pk_bytes(a)[o..o + sz];
                let cv = &b.get_pk_bytes(c)[o..o + sz];
                let mut ord = av.cmp(cv);
                if key.desc {
                    ord = ord.reverse();
                }
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            ColumnLocator::Payload { slot, size, type_code } => {
                let pi = slot as usize;
                let an = null_bit(a_null, pi);
                let cn = null_bit(c_null, pi);
                match (an, cn) {
                    (true, true) => continue,
                    // NULL placement is absolute (not flipped by `desc`).
                    (true, false) => {
                        return if key.nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (false, true) => {
                        return if key.nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    (false, false) => {
                        let sz = size as usize;
                        let av = b.get_col_ptr(a, pi, sz);
                        let cv = b.get_col_ptr(c, pi, sz);
                        let mut ord = cmp_col_window(av, b.blob_slice(), cv, b.blob_slice(), type_code);
                        if key.desc {
                            ord = ord.reverse();
                        }
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                }
            }
        }
    }
    // Deterministic tiebreak (never reversed): OPK bytes, then payload columns.
    match compare_pk_bytes(b.get_pk_bytes(a), b.get_pk_bytes(c)) {
        Ordering::Equal => compare_rows(reply_schema, b, a, b, c),
        ord => ord,
    }
}

fn sum_weights(batch: &Batch) -> i64 {
    (0..batch.count).map(|i| batch.get_weight(i)).sum()
}

/// The half-open OPK PK key range `[start, end)` for `range` over `schema`'s PK,
/// the base-PK sibling of `index_range_keys`: the cut → key mapping and the
/// provably-empty verdicts are the shared `range_keys_from_cuts` (§ its doc);
/// this function contributes only the PK-column group-prefix encoder and the
/// arity guard. `Ok(None)` = provably empty; `Err` = the descriptor pins every
/// PK column with no range column left.
///
/// The single-column-PK case (`n_eq == 0`, `prefix_len == pk_stride`) is the
/// mainline — `WHERE pk > 5` and the full-PK point lookup — where `After`
/// increments the whole key with carry ripple and there is no zero pad.
fn pk_range_keys(schema: &SchemaDescriptor, range: &RangeDescriptor) -> Result<Option<(PkBuf, Option<PkBuf>)>, String> {
    let eq_natives = range.eq_vals();
    let n_eq = eq_natives.len();
    let pk_col_count = schema.pk_indices().len();
    if n_eq >= pk_col_count {
        return Err(format!(
            "pk range: n_eq {n_eq} has no range column within PK arity {pk_col_count}"
        ));
    }
    // The group prefix of a cut value: OPK-encode the first `n_eq + 1` PK
    // columns (equality values then the cut value), leaving the trailing
    // columns raw-zero — the minimum OPK for any type, so `group(v)` IS
    // `pad(group(v))`.
    Ok(crate::storage::range_keys_from_cuts(
        range,
        schema.pk_stride() as usize,
        |v| {
            let mut out = [0u8; MAX_PK_BYTES];
            let mut off = 0usize;
            for (i, (_ord, _ci, col)) in schema.pk_columns().enumerate().take(n_eq + 1) {
                let cs = col.size() as usize;
                let native = if i < n_eq { eq_natives[i] } else { v };
                gnitz_wire::encode_pk_column(&native.to_le_bytes()[..cs], col.type_code, &mut out[off..off + cs]);
                off += cs;
            }
            (out, off)
        },
    ))
}

/// Decode + validate a client predicate blob against `schema`, then build its
/// predicate `ScalarFunc` — the same path the circuit compiler runs. Any failure
/// is a corrupt frame (the client pre-compiled the identical program at plan time).
fn compile_predicate(blob: &[u8], schema: &SchemaDescriptor) -> Result<ScalarFunc, String> {
    let dep = gnitz_wire::decode_expr_blob(blob).ok_or("scan_spec: corrupt predicate blob")?;
    let prog = LogicalProgram::from_wire(&dep.code, dep.num_regs, dep.result_reg, dep.const_strings)
        .and_then(|p| p.validate(Some(schema), None).map(|()| p))
        .map_err(|e| format!("scan_spec: invalid predicate program: {e:?}"))?;
    Ok(ScalarFunc::from_predicate(prog, schema))
}

/// Decode + validate a client projection (MAP) blob and build its map
/// `ScalarFunc`. `check_out` bounds every payload slot against
/// `out_schema.num_payload_cols()`, so an OOB slot is a clean `Err`, not a panic.
fn compile_projection(
    blob: &[u8],
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
) -> Result<ScalarFunc, String> {
    let dep = gnitz_wire::decode_expr_blob(blob).ok_or("scan_spec: corrupt projection blob")?;
    let prog = LogicalProgram::from_wire(&dep.code, dep.num_regs, 0, dep.const_strings)
        .map_err(|e| format!("scan_spec: invalid projection program: {e:?}"))?;
    prog.validate(Some(in_schema), Some(out_schema))
        .map_err(|e| format!("scan_spec: projection/schema mismatch: {e:?}"))?;
    Ok(ScalarFunc::from_map(prog, in_schema, out_schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::type_code;
    use crate::test_support::pk_only_schema;
    use gnitz_wire::Cut::{After, Before};

    fn opk_u64(v: u64) -> Vec<u8> {
        v.to_be_bytes().to_vec() // U64 OPK is plain big-endian
    }

    /// `pk >= 5` → `[OPK(5), +∞)`. The single-column mainline: no equality pins,
    /// `prefix_len == pk_stride`, an unbounded upper edge.
    #[test]
    fn pk_range_ge_unbounded_above() {
        let s = pk_only_schema(&[type_code::U64]);
        let d = RangeDescriptor::new(&[], Before(5), After(u64::MAX as u128));
        let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
        assert_eq!(start.pk_bytes(), opk_u64(5));
        assert!(end.is_none(), "unbounded above → end None");
    }

    /// `pk > 5` → `[OPK(6), +∞)` — the degenerate no-pad `succ` on the whole key.
    #[test]
    fn pk_range_gt_increments_whole_key() {
        let s = pk_only_schema(&[type_code::U64]);
        let d = RangeDescriptor::new(&[], After(5), After(u64::MAX as u128));
        let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
        assert_eq!(start.pk_bytes(), opk_u64(6));
        assert!(end.is_none());
    }

    /// `pk < 10` → `[OPK(0), OPK(10))`.
    #[test]
    fn pk_range_lt() {
        let s = pk_only_schema(&[type_code::U64]);
        let d = RangeDescriptor::new(&[], Before(0), Before(10));
        let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
        assert_eq!(start.pk_bytes(), opk_u64(0));
        assert_eq!(end.unwrap().pk_bytes(), opk_u64(10));
    }

    /// Full-PK point lookup `pk = 5` → `[OPK(5), OPK(6))` (degenerate cuts).
    #[test]
    fn pk_range_point_lookup() {
        let s = pk_only_schema(&[type_code::U64]);
        let d = RangeDescriptor::new(&[], Before(5), After(5));
        let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
        assert_eq!(start.pk_bytes(), opk_u64(5));
        assert_eq!(end.unwrap().pk_bytes(), opk_u64(6));
    }

    /// An inverted interval (`pk > 10 AND pk < 3`) drains to zero rows.
    #[test]
    fn pk_range_inverted_is_empty() {
        let s = pk_only_schema(&[type_code::U64]);
        let d = RangeDescriptor::new(&[], After(10), Before(3));
        assert_eq!(pk_range_keys(&s, &d).unwrap(), None);
    }

    /// A signed PK: `pk > -1` seeks to `OPK(0)` (sign-flip order); `After(i64::MAX)`
    /// overflows `succ` → unbounded above.
    #[test]
    fn pk_range_signed_i64() {
        let s = pk_only_schema(&[type_code::I64]);
        let neg1 = (-1i64 as u64) as u128;
        let d = RangeDescriptor::new(&[], After(neg1), After((i64::MAX as u64) as u128));
        let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
        let (opk0, st) = opk_key(&s, &0i64.to_le_bytes());
        assert_eq!(start.pk_bytes(), &opk0[..st]);
        assert!(end.is_none());
    }

    /// Compound PK `(a, b)` with `a = 5 AND b > 3`: the range column is `b`, so
    /// `start` seeks past `(5, 3)` and stays within the `a == 5` group.
    #[test]
    fn pk_range_compound_prefix_eq() {
        let s = pk_only_schema(&[type_code::U64, type_code::U64]);
        let d = RangeDescriptor::new(&[5], After(3), After(u64::MAX as u128));
        let (start, end) = pk_range_keys(&s, &d).unwrap().unwrap();
        // start = OPK(5,4) — the prefix `(5,3)` incremented on b.
        let (s54, st) = opk_key(&s, &{
            let mut v = Vec::new();
            v.extend_from_slice(&5u64.to_le_bytes());
            v.extend_from_slice(&4u64.to_le_bytes());
            v
        });
        assert_eq!(start.pk_bytes(), &s54[..st]);
        // end = the successor of `(5, MAX)` — carries into `a`, i.e. OPK(6, 0).
        let (s60, _) = opk_key(&s, &{
            let mut v = Vec::new();
            v.extend_from_slice(&6u64.to_le_bytes());
            v.extend_from_slice(&0u64.to_le_bytes());
            v
        });
        assert_eq!(end.unwrap().pk_bytes(), &s60[..st]);
    }

    /// `n_eq` at the PK arity leaves no range column — a trust-boundary reject.
    #[test]
    fn pk_range_no_range_column_errs() {
        let s = pk_only_schema(&[type_code::U64]);
        let d = RangeDescriptor::new(&[5], Before(0), After(0));
        assert!(pk_range_keys(&s, &d).is_err());
    }
}
