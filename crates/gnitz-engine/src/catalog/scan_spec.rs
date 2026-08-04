//! Parameterized bounded read (`ReadSpec`) execution — the worker half of the
//! ad-hoc SELECT scan. Runs **once per worker**: open a cursor for the bound,
//! then per chunk take the predicate's surviving row ranges and drive them into
//! the sink — rows (projected onto the keeper, then bounded top-k / materialize)
//! or the aggregate hash-fold (`AdhocFold`). Nothing between the source chunk
//! and the sink is materialized. No DBSP circuit, no operator state, no
//! exchange.
//!
//! A bound that names its keys — a `pk IN (…)` set, or a PK range confined to
//! one distribution prefix — reads only the partitions those keys hash into; an
//! unconfined bound merges every local partition.
//!
//! The reply schema arrives as the client's raw wire block (decoded by the
//! worker one layer up); this module takes the decoded `SchemaDescriptor`. The
//! block itself never leaves the request — the client decodes the reply against
//! its own copy.

use std::cmp::Ordering;

use gnitz_wire::{AggReadSpec, OrderKey, RangeDescriptor, ReadBound, ReadSink, ReadSpec};

use super::store_io::SourceCursor;
use super::*;
use crate::expr::ScalarFunc;
use crate::ops::AdhocFold;
use crate::query::StoreProbe;
use crate::schema::key::{compare_pk_bytes, opk_key, PkBuf};
use crate::schema::ColumnLocator;
use crate::storage::{cmp_col_window, compare_rows};
use gnitz_expr::{LogicalProgram, RowSource};

/// `limit_k` above which the worker materializes instead of running the bounded
/// top-k sink (a deep OFFSET ships unsorted and the client sorts). Worker
/// policy, not wire contract — the client never consults it.
const MAX_WORKER_TOPK: u64 = 65_536;

impl CatalogEngine {
    /// Execute `spec` against `target_id` on this worker's partitions,
    /// returning one keeper batch in the `reply_schema` shape. The caller
    /// (the worker dispatch arm) decoded `reply_schema` from the client's wire
    /// block and replies with no block at all; this method does the bound walk,
    /// predicate, projection, and ORDER BY / LIMIT reduction.
    ///
    /// `Err` on a corrupt program blob, a reply schema that does not match the
    /// sink's expected shape (rows: PK-stride equality; fold: the derived
    /// SyntheticFold layout), an index an `exact` bound needs and cannot find — every one a
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
        let group_cap = self.adhoc_group_cap;
        let mut source = self.open_scan_spec_cursor(target_id, &spec.bound, &src_schema)?;
        let ctx = ScanSinkCtx {
            // The routed cursors' store, re-read on every chunk (a cursor holds
            // no borrow on it). `None` for a borrowed system table.
            store: self.partitioned_store(target_id),
            predicate: predicate.as_ref(),
            chunk_rows,
        };

        match &spec.sink {
            ReadSink::Fold(agg) => run_scan_fold_sink(&mut source, ctx, &src_schema, reply_schema, agg, group_cap),
            ReadSink::Rows {
                projection,
                order,
                limit_k,
            } => {
                // `OrderKey.col` is a raw client `u16` — a full reply-schema
                // column index — that `run_scan_rows_sink` feeds straight to
                // `reply_schema.locate`, whose bound is a release-active
                // `assert!`. Bound it here, ahead of both reply shapes: the
                // locators are built before the topk/early-stop decision and
                // before the chunk loop, so `limit_k` and an empty table are no
                // protection.
                if let Some(k) = order.iter().find(|k| k.col as usize >= reply_schema.num_columns()) {
                    return Err(format!(
                        "scan_spec: order key column {} out of range ({} cols)",
                        k.col,
                        reply_schema.num_columns()
                    ));
                }
                // The rows sink byte-copies the source OPK verbatim into the reply
                // PK region, so the strides must agree — a mismatch would leave
                // the keeper's uninitialized tail as the client's PK tiebreak.
                // With no projection the whole row is copied region-wise off
                // `reply_schema`'s strides, so the reply must be the source schema
                // outright; the fold sink checks the same way
                // (`AdhocFold::new`). The trusted planner always matches; this
                // guards the client's blob. (A fold sink emits a synthetic
                // `_agg_pk` PK and never byte-copies the source PK, so the stride
                // half is rows-sink-only.)
                let projection = match projection.is_empty() {
                    true => {
                        if !reply_schema.same_physical_layout(&src_schema) {
                            return Err("scan_spec: identity rows reply schema differs from the source".into());
                        }
                        None
                    }
                    false => {
                        if reply_schema.pk_stride() != src_schema.pk_stride() {
                            return Err(format!(
                                "scan_spec: reply pk_stride {} != source pk_stride {}",
                                reply_schema.pk_stride(),
                                src_schema.pk_stride()
                            ));
                        }
                        Some(compile_projection(projection, &src_schema, reply_schema)?)
                    }
                };
                Ok(run_scan_rows_sink(
                    &mut source,
                    ctx,
                    projection.as_ref(),
                    reply_schema,
                    order,
                    *limit_k,
                ))
            }
        }
    }

    /// Open the source cursor for `bound` over `source` — routed to the
    /// partitions the bound's keys reach where it names them, merged over every
    /// local partition where it does not. Each arm owns its own trust-boundary
    /// rejections; see the per-bound openers below.
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
            ReadBound::PkRange(desc) => self.open_pk_range_cursor(source, desc, src_schema),
            ReadBound::IndexRange { idx_cols, exact, desc } => {
                self.open_index_bound_cursor(source, *idx_cols, *exact, desc)
            }
            ReadBound::PkSet(keys) => self.open_pk_set_gather(source, keys, src_schema),
        }
    }

    /// A base-PK range walk, clamped to `[start, end)` so it is O(range): a
    /// point lookup drains exactly its group, never a boundary chunk of
    /// over-read.
    ///
    /// A range whose every key shares the distribution prefix lives in one
    /// partition — the same test `scan_spec_partition` uses to unicast the
    /// request. `open_cursor_for_key` resolves through `slot_for_key`, never
    /// through the global partition id, so an unhashed store lands on its one
    /// child and `None` means this process holds no such partition. A range
    /// spanning partitions keeps the merge.
    fn open_pk_range_cursor(
        &mut self,
        source: i64,
        desc: &RangeDescriptor,
        src_schema: &SchemaDescriptor,
    ) -> Result<ScanSpecCursor, String> {
        let Some((start, end)) = pk_range_keys(src_schema, desc)? else {
            return Ok(ScanSpecCursor::Source(SourceCursor::Empty));
        };
        let handle = &self.table_entry(source)?.handle;
        let confined = crate::schema::key::range_shares_prefix(&start, end.as_ref(), src_schema.dist_stride() as usize);
        let mut cursor = if confined {
            match handle.open_cursor_for_key(start.pk_bytes()) {
                Some(c) => c,
                None => return Ok(ScanSpecCursor::Source(SourceCursor::Empty)),
            }
        } else {
            handle.open_cursor()
        };
        cursor.seek_range_bytes(start.pk_bytes(), end.as_ref().map(|e| e.pk_bytes()));
        Ok(ScanSpecCursor::Source(SourceCursor::Full(Box::new(cursor))))
    }

    /// A secondary-index range walk. `exact` says the SQL layer stripped the
    /// bounded conjuncts from the predicate, so this walk is the only thing
    /// applying them: run it un-gated, and fail rather than degrade if the index
    /// is gone (a full cursor would return rows nothing re-filters). Otherwise
    /// the conjuncts ride the predicate and the walk is an access optimization
    /// the selectivity gate may trade away.
    fn open_index_bound_cursor(
        &mut self,
        source: i64,
        idx_cols: u64,
        exact: bool,
        desc: &RangeDescriptor,
    ) -> Result<ScanSpecCursor, String> {
        let cols = gnitz_wire::unpack_pk_cols(idx_cols);
        if !cols.is_well_formed() {
            return Err(format!("scan_spec: malformed index column list for table {source}"));
        }
        if exact {
            Ok(match self.open_index_range_cursor(source, cols.as_slice(), desc)? {
                None => ScanSpecCursor::Source(SourceCursor::Empty),
                Some(c) => ScanSpecCursor::Source(SourceCursor::Bounded(Box::new(c))),
            })
        } else {
            self.open_bounded_source(source, cols.as_slice(), desc)
                .map(ScanSpecCursor::Source)
                .ok_or_else(|| format!("scan_spec: source table {source} unregistered"))
        }
    }

    /// A `pk IN (…)` gather. Two trust-boundary rejections land here because the
    /// decoder has no schema: the PK must be a single column, and no two wire
    /// keys may share an OPK image (`opk_key` truncates to `pk_stride`, so `5`
    /// and `5 + 2^64` are the same U64 PK — left in, the pair would emit its row
    /// twice). Both are hard rejects; release builds must not clamp.
    fn open_pk_set_gather(
        &mut self,
        source: i64,
        keys: &[u128],
        src_schema: &SchemaDescriptor,
    ) -> Result<ScanSpecCursor, String> {
        if src_schema.pk_indices().len() != 1 {
            return Err(format!(
                "scan_spec: PkSet gather requires a single-column PK (table {source})"
            ));
        }
        let handle = &self.table_entry(source)?.handle;
        let stride = src_schema.pk_stride() as usize;
        // OPK order IS typed PK order, so sorting the images byte-wise makes the
        // gather one monotone forward sweep regardless of wire key order. A
        // single PK column is at most 16 bytes, so the images are flat
        // fixed-size arrays (zero tail bytes never affect the order).
        let mut opk_keys: Vec<[u8; 16]> = keys
            .iter()
            .map(|&k| {
                let opk = opk_key(src_schema, &k.to_le_bytes());
                debug_assert_eq!(opk.len as usize, stride);
                let mut key = [0u8; 16];
                key[..stride].copy_from_slice(opk.pk_bytes());
                key
            })
            .collect();
        opk_keys.sort_unstable();
        // The sort makes a colliding pair adjacent. Checked before the ownership
        // filter so every worker returns the same verdict.
        if let Some(w) = opk_keys.windows(2).find(|w| w[0] == w[1]) {
            return Err(format!(
                "scan_spec: PkSet duplicate key {:?} (table {source})",
                &w[0][..stride]
            ));
        }
        // The probe answers a key this worker cannot reach with `None` — the
        // request is broadcast, so at W workers most of the list belongs
        // elsewhere — and routes the rest to the one partition that can hold
        // them instead of the whole store's merge.
        Ok(ScanSpecCursor::PkSet(Box::new(PkSetGather {
            cursor: handle.open_probe(),
            keys: opk_keys,
            stride,
            next: 0,
            src_schema: *src_schema,
        })))
    }
}

/// A `pk IN (…)` gather: every live row of each key's PK group, in ascending
/// OPK order.
struct PkSetGather {
    cursor: StoreProbe,
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
    /// Full cursor, bounded index walk, or provably-empty bound — the shared
    /// source cursor. A PK range is a Full cursor positioned on `[start, end)`
    /// (`seek_range_bytes`), so it exhausts exactly at the cut; when the range is
    /// confined to one distribution prefix that cursor spans one partition.
    Source(SourceCursor),
    /// `pk IN (…)` gather (boxed: `SourceCursor` is three words and this holds a
    /// key list and a schema — one allocation per request, never per chunk).
    PkSet(Box<PkSetGather>),
}

impl ScanSpecCursor {
    /// The next source rows, or `None` once the bound is exhausted. A returned
    /// batch may be empty (a window of PkSet misses, or of keys this worker holds
    /// no partition for); `None` strictly means "no further rows". `store` is the
    /// scanned relation's partitioned store, `None` for a borrowed system table.
    ///
    /// `max_rows` bounds a `Source` chunk exactly; a `PkSet` chunk tests it before
    /// each key and then drains that key's whole group, so it can overshoot to
    /// `max_rows - 1 + |largest group|`. Both sinks read `chunk.count`.
    fn next_chunk(&mut self, store: Option<&PartitionedTable>, max_rows: usize) -> Option<Batch> {
        match self {
            ScanSpecCursor::Source(source) => source.drain_chunk(store, max_rows),
            ScanSpecCursor::PkSet(g) => {
                if g.next >= g.keys.len() {
                    return None;
                }
                let cap = (g.keys.len() - g.next).min(max_rows);
                let mut out = Batch::with_capacity(g.src_schema, cap);
                while g.next < g.keys.len() && out.count < max_rows {
                    let key = &g.keys[g.next][..g.stride];
                    g.next += 1;
                    // An absent key, or one this worker holds no partition for,
                    // copies nothing: the discard the broadcast list needs.
                    g.cursor.copy_live_pk_group_into(store, key, &mut out);
                }
                // An all-miss window returns an empty batch only when the key list
                // is exhausted; otherwise it advanced `next` and there is more.
                (out.count > 0 || g.next < g.keys.len()).then_some(out)
            }
        }
    }
}

/// The per-request context both sinks read on every chunk: the routed cursors'
/// store, the compiled predicate, and the drain size.
struct ScanSinkCtx<'a> {
    store: Option<&'a PartitionedTable>,
    predicate: Option<&'a ScalarFunc>,
    chunk_rows: usize,
}

/// Run the fold sink over `source`: fold every surviving chunk into per-group
/// accumulators and return the partial reduce-output rows — or `Err` when the
/// per-worker group cap is exceeded (a resource-exhaustion abort, before any
/// data frame is sent). A fold spec carries no projection: survivors fold
/// directly.
fn run_scan_fold_sink(
    source: &mut ScanSpecCursor,
    ctx: ScanSinkCtx,
    src_schema: &SchemaDescriptor,
    reply_schema: &SchemaDescriptor,
    agg: &AggReadSpec,
    group_cap: usize,
) -> Result<Batch, String> {
    let mut fold = AdhocFold::new(src_schema, reply_schema, agg, group_cap)?;
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    while let Some(chunk) = source.next_chunk(ctx.store, ctx.chunk_rows) {
        if chunk.count == 0 {
            continue;
        }
        survivor_ranges(ctx.predicate, &chunk, &mut ranges);
        fold.fold_ranges(&chunk, &ranges)?;
    }
    Ok(fold.finish())
}

/// The filter's surviving row ranges over one chunk — the whole chunk when there
/// is no predicate. `out` is per-request scratch reused across chunks.
fn survivor_ranges(predicate: Option<&ScalarFunc>, chunk: &Batch, out: &mut Vec<(usize, usize)>) {
    match predicate {
        Some(f) => f.filter_ranges(chunk, out),
        None => {
            out.clear();
            out.push((0, chunk.count));
        }
    }
}

/// Run the rows sink over `source`, returning one keeper batch in the
/// `reply_schema` shape (a superset of this worker's contribution; the client
/// re-sorts the concatenation and applies the exact window).
///
/// Each chunk's survivor ranges are appended straight onto the keeper — there is
/// no intermediate survivor batch and no projected batch.
fn run_scan_rows_sink(
    source: &mut ScanSpecCursor,
    ctx: ScanSinkCtx,
    projection: Option<&ScalarFunc>,
    reply_schema: &SchemaDescriptor,
    order: &[OrderKey],
    limit_k: u64,
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
    let drain_rows = if early_stop && ctx.predicate.is_none() {
        (limit_k as usize).clamp(1, ctx.chunk_rows)
    } else {
        ctx.chunk_rows
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
    // Per-request scratch, reused across chunks.
    let mut ranges: Vec<(usize, usize)> = Vec::new();

    while let Some(chunk) = source.next_chunk(ctx.store, drain_rows) {
        if chunk.count == 0 {
            continue;
        }
        survivor_ranges(ctx.predicate, &chunk, &mut ranges);
        // Weigh the survivors off the source — the same weights that land in the
        // keeper, read from a contiguous region rather than row-by-row off the
        // destination. `topk` needs the chunk total; `early_stop` needs the
        // running prefix, so it can cut the range list at the first range that
        // covers the window: with survivors ≫ `limit_k` that gathers ~`limit_k`
        // rows instead of the whole chunk's survivors. Ranges are whole rows, so
        // the worker still returns a ≥ `limit_k`-weight superset and the client
        // applies the exact window. The two are mutually exclusive by
        // construction (`order` empty or not), so a top-k scan never cuts.
        if topk {
            summed += ranges.iter().map(|&(s, e)| chunk.sum_weights(s, e)).sum::<i64>();
        } else if early_stop {
            for (i, &(s, e)) in ranges.iter().enumerate() {
                summed += chunk.sum_weights(s, e);
                if summed >= limit_k as i64 {
                    ranges.truncate(i + 1);
                    break;
                }
            }
        }
        match projection {
            None => keeper.append_ranges(&chunk, &ranges),
            Some(p) => p.append_map_ranges(&chunk, &mut keeper, &ranges),
        }

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
        Batch::from_indexed_rows(&keeper.as_mem_batch(), &perm[..cut], &[], reply_schema),
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
                let an = gnitz_wire::null_word_get(a_null, pi);
                let cn = gnitz_wire::null_word_get(c_null, pi);
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
                        let mut ord = cmp_col_window(av, b.blob(), cv, b.blob(), type_code);
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
    crate::schema::key::eq_prefix_range_keys(
        range,
        schema.pk_indices().len(),
        schema.pk_stride() as usize,
        "pk range",
        |natives| {
            // Source and target column are the same here (no index promotion),
            // so the shared encoder's promote step is its identity arm. The
            // trailing PK columns stay raw-zero — the minimum OPK for any type,
            // so `group(v)` IS `pad(group(v))`.
            let cols = schema
                .pk_columns()
                .take(natives.len())
                .map(|(_, col)| (col.type_code, *col));
            crate::schema::key::encode_leading_opk(cols, natives)
        },
    )
}

/// The one partition every row matching `range` can live in, or `None` when the
/// range spans partitions (or is provably empty) — the master's confinement test,
/// which turns a broadcast into a unicast.
///
/// `partition_for_pk` hashes only `key[..dist_stride]`, so the range is confined
/// iff every key in it shares that prefix, which `range_shares_prefix` decides
/// from the range's first and last keys. Partition ids are `mix(pk) >> 56` and so
/// not monotone in key order; a prefix match over the whole range is what makes
/// the single hash of `start` speak for all of it.
pub(crate) fn scan_spec_partition(schema: &SchemaDescriptor, range: &RangeDescriptor) -> Option<usize> {
    let (start, end) = pk_range_keys(schema, range).ok().flatten()?;
    crate::schema::key::range_shares_prefix(&start, end.as_ref(), schema.dist_stride() as usize)
        .then(|| schema.partition_for_pk(start.pk_bytes()))
}

/// Decode + validate a client predicate blob against `schema`, then build its
/// predicate `ScalarFunc` — the same path the circuit compiler runs. Any failure
/// is a corrupt frame (the client pre-compiled the identical program at plan time).
fn compile_predicate(blob: &[u8], schema: &SchemaDescriptor) -> Result<ScalarFunc, String> {
    let dep = gnitz_wire::decode_expr_blob(blob).ok_or("scan_spec: corrupt predicate blob")?;
    LogicalProgram::from_wire(&dep.code, dep.num_regs, dep.result_reg, dep.const_strings)
        .and_then(|p| ScalarFunc::from_predicate(p, schema))
        .map_err(|e| format!("scan_spec: invalid predicate program: {e}"))
}

/// Decode + validate a client projection (MAP) blob and build its map
/// `ScalarFunc` — the same path the circuit compiler runs. `validate` bounds
/// every payload slot against `out_schema.num_payload_cols()` and requires the
/// program to write all of them, so an OOB or unwritten slot is a clean `Err`
/// rather than a panic or a shipped byte of the keeper's recycled tail.
fn compile_projection(
    blob: &[u8],
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
) -> Result<ScalarFunc, String> {
    let dep = gnitz_wire::decode_expr_blob(blob).ok_or("scan_spec: corrupt projection blob")?;
    LogicalProgram::from_wire(&dep.code, dep.num_regs, 0, dep.const_strings)
        .and_then(|p| ScalarFunc::from_map(p, in_schema, out_schema))
        .map_err(|e| format!("scan_spec: invalid projection program: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn};
    use crate::test_support::{opk_pk, pk_only_schema};
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
        assert_eq!(start, opk_key(&s, &0i64.to_le_bytes()));
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
        let s54 = opk_key(&s, &{
            let mut v = Vec::new();
            v.extend_from_slice(&5u64.to_le_bytes());
            v.extend_from_slice(&4u64.to_le_bytes());
            v
        });
        assert_eq!(start, s54);
        // end = the successor of `(5, MAX)` — carries into `a`, i.e. OPK(6, 0).
        let s60 = opk_key(&s, &{
            let mut v = Vec::new();
            v.extend_from_slice(&6u64.to_le_bytes());
            v.extend_from_slice(&0u64.to_le_bytes());
            v
        });
        assert_eq!(end.unwrap(), s60);
    }

    /// `n_eq` at the PK arity leaves no range column — a trust-boundary reject.
    #[test]
    fn pk_range_no_range_column_errs() {
        let s = pk_only_schema(&[type_code::U64]);
        let d = RangeDescriptor::new(&[5], Before(0), After(0));
        assert!(pk_range_keys(&s, &d).is_err());
    }

    // ── scan_spec_partition — the master's confinement test ──────────────────

    /// A full point is confined to the partition of its own PK bytes, at every PK
    /// shape — single, wide, and compound (where the point pins the leading
    /// columns through `eq_vals` and points at the last).
    #[test]
    fn scan_spec_partition_confines_a_full_point() {
        let u64s = pk_only_schema(&[type_code::U64]);
        assert_eq!(
            scan_spec_partition(&u64s, &RangeDescriptor::new(&[], Before(42), After(42))),
            Some(u64s.partition_for_pk(&opk_pk(&u64s, &[42])))
        );

        let u128s = pk_only_schema(&[type_code::U128]);
        let wide = (1u128 << 100) | 7;
        assert_eq!(
            scan_spec_partition(&u128s, &RangeDescriptor::new(&[], Before(wide), After(wide))),
            Some(u128s.partition_for_pk(&opk_pk(&u128s, &[wide])))
        );

        let comp = pk_only_schema(&[type_code::U32, type_code::U64]);
        assert_eq!(
            scan_spec_partition(&comp, &RangeDescriptor::new(&[9], Before(4), After(4))),
            Some(comp.partition_for_pk(&opk_pk(&comp, &[9, 4])))
        );
    }

    /// With a `Keyed { prefix_len: 1 }` placement every row sharing the leading
    /// column lands in one partition, so pinning it and ranging the trailing
    /// column is confined — to the same partition full points on `(a, b)` reach.
    /// At the full-PK default
    /// the same bound spans partitions.
    #[test]
    fn scan_spec_partition_follows_the_distribution_prefix() {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ];
        let prefix = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 1 });
        // `a = 7 AND b > 3` — a whole trailing-column range inside one `a` group.
        let ranged = RangeDescriptor::new(&[7], After(3), After(u64::MAX as u128));
        let want = prefix.partition_for_pk(&opk_pk(&prefix, &[7, 0]));
        assert_eq!(scan_spec_partition(&prefix, &ranged), Some(want));
        for b in [4u128, u64::MAX as u128] {
            assert_eq!(
                scan_spec_partition(&prefix, &RangeDescriptor::new(&[7], Before(b), After(b))),
                Some(want),
                "a full point on (7, {b}) shares the group's partition"
            );
        }

        let full = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 2 });
        assert_eq!(
            scan_spec_partition(&full, &ranged),
            None,
            "hashing the whole PK spreads one `a` group across partitions"
        );
    }

    /// A maximal-value point carries out of `succ`, so its range has no `end` —
    /// the all-`0xFF` last key must still confine it rather than broadcast. The
    /// signed maximum's OPK is all-`0xFF` too (sign-flip).
    #[test]
    fn scan_spec_partition_confines_a_maximal_point() {
        for tc in [type_code::U64, type_code::I64] {
            let s = pk_only_schema(&[tc]);
            let max = if tc == type_code::U64 {
                u64::MAX as u128
            } else {
                i64::MAX as u128
            };
            let d = RangeDescriptor::new(&[], Before(max), After(max));
            assert!(
                pk_range_keys(&s, &d).unwrap().unwrap().1.is_none(),
                "After(max) carries out"
            );
            assert_eq!(
                scan_spec_partition(&s, &d),
                Some(s.partition_for_pk(&opk_pk(&s, &[max])))
            );
        }
    }

    /// A range wider than one partition's key span is not confinable: partition
    /// ids are `mix(pk) >> 56`, not monotone in key order, so only a whole-range
    /// prefix match proves confinement. A provably-empty range is not confinable
    /// either — the worker answers it (a fold sink still owes its ground row).
    #[test]
    fn scan_spec_partition_declines_a_multi_key_range() {
        let s = pk_only_schema(&[type_code::U64]);
        assert_eq!(
            scan_spec_partition(&s, &RangeDescriptor::new(&[], Before(0), After(1000))),
            None
        );
        assert_eq!(
            scan_spec_partition(&s, &RangeDescriptor::new(&[], After(1000), Before(0))),
            None,
            "an inverted range is provably empty"
        );
        // Unbounded above from a non-maximal start: the last key is 0xFF…FF.
        assert_eq!(
            scan_spec_partition(&s, &RangeDescriptor::new(&[], Before(5), After(u64::MAX as u128))),
            None
        );
    }
}
