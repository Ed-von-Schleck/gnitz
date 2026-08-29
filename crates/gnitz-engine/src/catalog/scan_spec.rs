//! Parameterized bounded read (`ReadSpec`) execution — the worker half of the
//! ad-hoc SELECT scan. Runs **once per worker**: open a cursor for the bound,
//! then per chunk take the predicate's surviving row ranges and drive them into
//! the sink — rows (projected onto the keeper, then bounded top-k / materialize)
//! or the aggregate hash-fold (`AdhocFold`). Nothing between the source chunk
//! and the sink is materialized. No DBSP circuit, no operator state, no
//! exchange.
//!
//! The cursor spans this worker's whole slice of the relation; a bound that
//! names its keys narrows the walk within it. Which worker answers at all is the
//! master's question (`scan_spec_worker`), not this module's.
//!
//! The reply schema arrives as the client's raw wire block (decoded by the
//! worker one layer up); this module takes the decoded `SchemaDescriptor`. The
//! block itself never leaves the request — the client decodes the reply against
//! its own copy.

use std::cmp::Ordering;

use gnitz_wire::{AggReadSpec, Cut, OrderKey, RangeDescriptor, ReadBound, ReadSink, ReadSpec, WireFault};

use super::store_io::BoundedRead;
use super::*;
use crate::expr::{MapPlan, PkSource};
use crate::ops::AdhocFold;
use crate::schema::key::{compare_pk_bytes, opk_key, PkBuf};
use crate::schema::ColumnLocator;
use crate::storage::{compare_rows, PkSetGather, SourceCursor};
use gnitz_expr::{Evaluator, LogicalProgram};

/// `limit_k` above which the worker materializes instead of running the bounded
/// top-k sink (a deep OFFSET ships unsorted and the client sorts). Worker
/// policy, not wire contract — the client never consults it.
const MAX_WORKER_TOPK: u64 = 65_536;

impl CatalogEngine {
    /// Execute `spec` against `target_id` on this worker's slice,
    /// returning one keeper batch in the `reply_schema` shape. The caller
    /// (the worker dispatch arm) decoded `reply_schema` from the client's wire
    /// block and replies with no block at all; this method does the bound walk,
    /// predicate, projection, and ORDER BY / LIMIT reduction.
    ///
    /// `Err` on a corrupt program blob, a reply schema that does not match the
    /// sink's expected shape (rows: PK-stride equality; fold: the derived
    /// SyntheticFold layout), an index an `exact` bound needs and cannot find — every one a
    /// corrupt/stale frame, surfaced as a `STATUS_ERROR` reply — the fold's
    /// per-worker group cap (a resource-exhaustion abort), or a delta cursor
    /// below this worker's retention floor, which carries `STATUS_DELTA_EXPIRED`.
    ///
    /// `cut_tick` is the last tick round the master had emitted when it wrote
    /// this read's group, and bounds an incremental delta read above. Every other
    /// bound ignores it.
    pub fn scan_spec_family(
        &mut self,
        target_id: i64,
        spec: &ReadSpec,
        reply_schema: &SchemaDescriptor,
        cut_tick: u64,
    ) -> Result<Batch, WireFault> {
        let src_schema = self.scan_spec_source_schema(target_id, &spec.bound)?;

        // Compile the predicate once per request, exactly as the circuit
        // compiler does (`query/compiler/emit.rs`).
        let predicate = match spec.predicate.is_empty() {
            true => None,
            false => Some(compile_predicate(&spec.predicate, &src_schema)?),
        };

        let chunk_rows = self.ddl_scan_chunk_rows.max(1);
        let group_cap = self.adhoc_group_cap;
        let mut source = self.open_scan_spec_cursor(target_id, &spec.bound, &src_schema, cut_tick)?;
        let ctx = ScanSinkCtx {
            predicate: predicate.as_ref(),
            chunk_rows,
        };

        match &spec.sink {
            ReadSink::Fold(agg) => Ok(run_scan_fold_sink(
                &mut source,
                ctx,
                &src_schema,
                reply_schema,
                agg,
                group_cap,
            )?),
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
                    )
                    .into());
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
                            )
                            .into());
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

    /// The schema the bound's rows arrive in — what the predicate, the projection
    /// and the identity-layout check below are all resolved against.
    ///
    /// Taken from the **bound**, not from the registry entry: an incremental delta
    /// read walks the view's delta store, whose rows are `_tick ‖ view PK ‖ view
    /// payload`. The bootstrap arm (`after_tick = 0`) is *the sum of every delta
    /// after round 0*, which is the view's whole history — precisely what its own
    /// output store holds — so it walks that, in the view's own schema.
    ///
    /// A `Delta` bound against a relation with no feed is refused at **every**
    /// `after_tick`, zero included: the reply would otherwise promise a
    /// continuation the server cannot serve.
    fn scan_spec_source_schema(&mut self, target: i64, bound: &ReadBound) -> Result<SchemaDescriptor, String> {
        let entry = self.table_entry(target)?;
        let ReadBound::Delta { after_tick } = bound else {
            return Ok(entry.schema);
        };
        if entry.delta_bytes.is_none() {
            return Err(format!(
                "scan_spec: relation {target} carries no delta feed; \
                 create the view WITH (delta = '<size>') to subscribe to it"
            ));
        }
        if *after_tick == 0 {
            return Ok(entry.schema);
        }
        entry
            .delta
            .as_ref()
            .map(|f| f.schema)
            .ok_or_else(|| format!("scan_spec: this process holds no delta store for relation {target}"))
    }

    /// Open the source cursor for `bound` over `source`, bounded within this
    /// worker's store by whatever the bound names. Each arm owns its own
    /// trust-boundary rejections; see the per-bound openers below.
    fn open_scan_spec_cursor(
        &mut self,
        source: i64,
        bound: &ReadBound,
        src_schema: &SchemaDescriptor,
        cut_tick: u64,
    ) -> Result<SourceCursor, WireFault> {
        // A store holding skeleton rows is hydrated over the bound into one
        // in-memory run first, so the predicate, projection, ORDER BY / LIMIT and
        // aggregate sinks below run unchanged over source-schema rows and never see
        // a skeleton row.
        //
        // Capacity buys disk, not read peak: an *unbounded* `SELECT … WHERE …` here
        // gives up `scan_spec`'s otherwise-fully-streaming property and
        // materializes the hydrated relation, as a plain `Scan` of the unbounded
        // twin already does. A bounded one materializes only its bound.
        if self.table_entry(source)?.needs_hydration() {
            let rows = match bound {
                ReadBound::None => self.materialize_bounded_store(source, BoundedRead::All)?,
                ReadBound::PkRange(desc) => match pk_range_keys(src_schema, desc)? {
                    None => Batch::empty_with_schema(src_schema),
                    Some((start, end)) => self.materialize_bounded_store(
                        source,
                        BoundedRead::Range(start.pk_bytes(), end.as_ref().map(|k| k.pk_bytes())),
                    )?,
                },
                ReadBound::PkSet(keys) => {
                    let opk = pk_set_opk_keys(source, keys, src_schema)?;
                    self.materialize_bounded_store(source, BoundedRead::Keys(&opk))?
                }
                // Only base tables own index circuits, so an index bound cannot
                // name a view — the only relation kind that can hold skeletons.
                ReadBound::IndexRange { .. } => {
                    return Err(format!(
                        "scan_spec: an index bound cannot name view {source} — only base tables own index circuits"
                    )
                    .into())
                }
                // Unreachable while `capacity` and `delta` are refused together:
                // a fed view never holds a skeleton row, so this test is always
                // false for one and the delta arm below is reached
                // unconditionally. Stated as a rejection rather than as an
                // assertion because what keeps the branch out of reach is a
                // prohibition two layers away.
                ReadBound::Delta { .. } => {
                    return Err(format!(
                        "scan_spec: relation {source} holds skeleton rows, which no relation with a delta feed can"
                    )
                    .into())
                }
            };
            return Ok(SourceCursor::Full(Box::new(ReadCursor::over_batches(
                &[Rc::new(rows)],
                *src_schema,
            ))));
        }
        match bound {
            // The bootstrap arm reads the view's own store, which is what
            // `scan_spec_source_schema` already told the caller.
            ReadBound::None | ReadBound::Delta { after_tick: 0 } => {
                Ok(SourceCursor::Full(Box::new(self.table_entry(source)?.open_cursor())))
            }
            ReadBound::PkRange(desc) => Ok(self.open_pk_range_cursor(source, desc, src_schema)?),
            ReadBound::IndexRange { idx_cols, exact, desc } => {
                Ok(self.open_index_bound_cursor(source, *idx_cols, *exact, desc)?)
            }
            ReadBound::PkSet(keys) => Ok(self.open_pk_set_gather(source, keys, src_schema)?),
            ReadBound::Delta { after_tick } => self.open_delta_cursor(source, *after_tick, cut_tick),
        }
    }

    /// The `(after_tick, cut_tick]` walk over a fed view's delta store.
    ///
    /// Both ends go through `pk_range_keys`, never through arithmetic on the
    /// tick. Two bugs avoided rather than a preference: `after_tick` is
    /// client-supplied and unvalidated until here, so `after_tick + 1` at
    /// `u64::MAX` would panic in a debug build and wrap in release, where
    /// `Cut::After`'s saturation arm answers "no key space above" instead; and the
    /// keys it produces are `pk_stride` wide and zero-padded past the tick, which
    /// is what `seek_range_bytes` requires and what makes the padded key the
    /// minimum of its tick group — every fed view's key is wider than the stamp
    /// alone. A start key not below the end comes back as an empty range rather
    /// than an error, so a forged `after_tick` above the cut is inert.
    ///
    /// Refused when this worker has dropped a round the walk would have to cover.
    /// The floors are independent across workers, so one refusal refuses the read
    /// — a broadcast one worker cannot serve whole has a hole in it.
    ///
    /// The test is `after_tick < dropped_through`, and the strictness is exact
    /// rather than cautious. `dropped_through` is the **highest** `_tick` this
    /// store has dropped, and the walk covers `(after_tick, cut]`, so a dropped
    /// row falls inside it iff `dropped_through > after_tick`. A cursor sitting
    /// *at* the floor is therefore served in full: a drop can leave the tail of
    /// its highest round behind, but `Cut::After(after_tick)` starts above that
    /// round, so the surviving fragment is excluded rather than returned. Refusing
    /// it as well would strand a bootstrap whose watermark landed on the floor —
    /// it would re-read at 0, be handed the same round, and be refused again until
    /// a later tick moved it.
    fn open_delta_cursor(&mut self, source: i64, after_tick: u64, cut_tick: u64) -> Result<SourceCursor, WireFault> {
        let Some(feed) = self.table_entry(source)?.delta.as_ref() else {
            return Err(format!("scan_spec: this process holds no delta store for relation {source}").into());
        };
        let dropped_through = feed.handle.dropped_through();
        if after_tick < dropped_through {
            return Err(WireFault {
                status: gnitz_wire::STATUS_DELTA_EXPIRED,
                text: format!(
                    "delta cursor {after_tick} of relation {source} is below the \
                     retained floor {dropped_through}; re-read at 0"
                ),
            });
        }
        let desc = RangeDescriptor::new(&[], Cut::After(after_tick as u128), Cut::After(cut_tick as u128));
        Ok(Self::range_cursor(&feed.schema, &desc, |s, e| {
            feed.open_cursor_in_range(s, e)
        })?)
    }

    /// A base-PK range walk, clamped to `[start, end)` so it is O(range): a
    /// point lookup drains exactly its group, never a boundary chunk of
    /// over-read.
    ///
    /// The cursor spans this worker's whole slice of the relation; the range cut
    /// is what bounds the walk. Whether the range is confined to one worker is the
    /// master's question (`scan_spec_worker`, which turns the broadcast into a
    /// unicast), not this one's.
    fn open_pk_range_cursor(
        &mut self,
        source: i64,
        desc: &RangeDescriptor,
        src_schema: &SchemaDescriptor,
    ) -> Result<SourceCursor, String> {
        let entry = self.table_entry(source)?;
        Self::range_cursor(src_schema, desc, |s, e| entry.open_cursor_in_range(s, e))
    }

    /// A cursor cut to the half-open key range `desc` names over `schema`'s PK
    /// space, or `Empty` for a provably-empty one. The one place a range
    /// descriptor becomes a cursor cut, so the `PkRange` bound over a relation's
    /// own store and the `Delta` bound over a fed view's delta store share the
    /// zero-padded keys and the saturating `After` arm.
    ///
    /// The keys are derived before `open` runs, so it opens over the range rather
    /// than over the whole store.
    fn range_cursor(
        schema: &SchemaDescriptor,
        desc: &RangeDescriptor,
        open: impl FnOnce(&[u8], Option<&[u8]>) -> ReadCursor,
    ) -> Result<SourceCursor, String> {
        let Some((start, end_key)) = pk_range_keys(schema, desc)? else {
            return Ok(SourceCursor::Empty);
        };
        let end = end_key.as_ref().map(|e| e.pk_bytes());
        let mut cursor = open(start.pk_bytes(), end);
        cursor.seek_range_bytes(start.pk_bytes(), end);
        Ok(SourceCursor::Full(Box::new(cursor)))
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
    ) -> Result<SourceCursor, String> {
        let cols = gnitz_wire::unpack_pk_cols(idx_cols);
        if !cols.is_well_formed() {
            return Err(format!("scan_spec: malformed index column list for table {source}"));
        }
        self.open_index_source(source, cols.as_slice(), desc, !exact)
    }

    /// A `pk IN (…)` gather over an unbounded relation.
    fn open_pk_set_gather(
        &mut self,
        source: i64,
        keys: &[u128],
        src_schema: &SchemaDescriptor,
    ) -> Result<SourceCursor, String> {
        let opk = pk_set_opk_keys(source, keys, src_schema)?;
        let entry = self.table_entry(source)?;
        // A key this worker holds no row for copies nothing — the request is
        // broadcast, so at W workers most of the list belongs elsewhere.
        Ok(SourceCursor::PkSet(Box::new(PkSetGather::open(
            opk,
            *src_schema,
            |s, e| entry.open_cursor_in_range(s, e),
        ))))
    }
}

/// The OPK images of a `pk IN (…)` key list, sorted ascending and concatenated —
/// the walk order `PkSetGather` and `BoundedRead::Keys` both require. OPK order
/// IS typed PK order, so sorting the images byte-wise makes the gather one
/// monotone forward sweep regardless of wire key order.
///
/// Two trust-boundary rejections land here because the decoder has no schema:
/// the PK must be a single column, and no two wire keys may share an OPK image
/// (`opk_key` truncates to `pk_stride`, so `5` and `5 + 2^64` are the same U64
/// PK — left in, the pair would emit its row twice). Both are hard rejects;
/// release builds must not clamp.
fn pk_set_opk_keys(source: i64, keys: &[u128], src_schema: &SchemaDescriptor) -> Result<Vec<u8>, String> {
    if src_schema.pk_indices().len() != 1 {
        return Err(format!(
            "scan_spec: PkSet gather requires a single-column PK (table {source})"
        ));
    }
    let stride = src_schema.pk_stride() as usize;
    // A single PK column is at most 16 bytes, so the images sort as inline
    // fixed-size arrays — one allocation for the list and register-width
    // comparisons, at a key count that reaches `MAX_PK_SET_KEYS` per request.
    // Trailing zero bytes are identical across keys and never affect the order.
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
    let mut flat = Vec::with_capacity(opk_keys.len() * stride);
    for k in &opk_keys {
        flat.extend_from_slice(&k[..stride]);
    }
    Ok(flat)
}

/// The per-request context both sinks read on every chunk: the compiled
/// predicate and the drain size.
struct ScanSinkCtx<'a> {
    predicate: Option<&'a Evaluator>,
    chunk_rows: usize,
}

/// Run the fold sink over `source`: fold every surviving chunk into per-group
/// accumulators and return the partial reduce-output rows — or `Err` when the
/// per-worker group cap is exceeded (a resource-exhaustion abort, before any
/// data frame is sent). A fold spec carries no projection: survivors fold
/// directly.
fn run_scan_fold_sink(
    source: &mut SourceCursor,
    ctx: ScanSinkCtx,
    src_schema: &SchemaDescriptor,
    reply_schema: &SchemaDescriptor,
    agg: &AggReadSpec,
    group_cap: usize,
) -> Result<Batch, String> {
    let mut fold = AdhocFold::new(src_schema, reply_schema, agg, group_cap)?;
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    while let Some(chunk) = source.drain_chunk(ctx.chunk_rows) {
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
fn survivor_ranges(predicate: Option<&Evaluator>, chunk: &Batch, out: &mut Vec<(usize, usize)>) {
    match predicate {
        Some(f) => f.filter_ranges(&chunk.as_mem_batch(), out),
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
    source: &mut SourceCursor,
    ctx: ScanSinkCtx,
    projection: Option<&MapPlan>,
    reply_schema: &SchemaDescriptor,
    order: &[OrderKey],
    limit_k: u64,
) -> Batch {
    // The client's `limit_k` is a `u64` (`OFFSET + LIMIT`, `0` = unbounded) and
    // every test below is against a summed i64 weight, so saturate once here: an
    // unsaturated `limit_k as i64` wraps negative past `i64::MAX` and makes the
    // first survivor range satisfy the window, truncating the answer.
    let window = limit_k.min(i64::MAX as u64) as i64;
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

    while let Some(chunk) = source.drain_chunk(drain_rows) {
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
                if summed >= window {
                    ranges.truncate(i + 1);
                    break;
                }
            }
        }
        match projection {
            None => keeper.append_ranges(&chunk, &ranges),
            Some(p) => p.append_map_ranges(&chunk, &mut keeper, &ranges),
        }

        if topk && summed > 2 * window {
            (keeper, summed) = topk_keep(keeper, &order_locs, reply_schema, limit_k);
        }
        if early_stop && summed >= window {
            break;
        }
    }

    // A final total ≤ the window provably cuts nothing — skip the sort outright.
    if topk && summed > window {
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
/// Byte-for-byte equivalent to the client's `SortKey` order (the shared
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
        // PK columns are never NULL, so this is the payload-only gate. NULL
        // placement is absolute — `nulls_first` decides it, and `desc` does not
        // flip it.
        if let ColumnLocator::Payload { slot, .. } = key.loc {
            let pi = slot as usize;
            match (
                gnitz_wire::null_word_get(a_null, pi),
                gnitz_wire::null_word_get(c_null, pi),
            ) {
                (true, true) => continue,
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
                (false, false) => {}
            }
        }
        // The locator carries both the addressing and the order rule: a PK
        // column's OPK window compares raw (order-preserving), a payload column
        // through the typed dispatch that routes STRING/BLOB by content.
        let mut ord = key.loc.cmp_non_null(b, a, b, c);
        if key.desc {
            ord = ord.reverse();
        }
        if ord != Ordering::Equal {
            return ord;
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
/// provably-empty verdicts are the shared `range_keys_from_cuts`;
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

/// The one worker every row matching `range` can live on, or `None` when the
/// range spans workers (or is provably empty) — the master's confinement test,
/// which turns a broadcast into a unicast.
///
/// `worker_for_pk` hashes only `key[..dist_stride]`, so the range is confined
/// iff every key in it shares that prefix, which `range_shares_prefix` decides
/// from the range's first and last keys. An owner is a hash of the key and so
/// not monotone in key order; a prefix match over the whole range is what makes
/// the single hash of `start` speak for all of it.
pub fn scan_spec_worker(schema: &SchemaDescriptor, range: &RangeDescriptor, num_workers: usize) -> Option<usize> {
    let (start, end) = pk_range_keys(schema, range).ok().flatten()?;
    crate::schema::key::range_shares_prefix(&start, end.as_ref(), schema.dist_stride() as usize)
        .then(|| schema.worker_for_pk(start.pk_bytes(), num_workers))
}

/// Decode + validate a client predicate blob against `schema`, then build its
/// predicate `Evaluator` — the same path the circuit compiler runs. Any failure
/// is a corrupt frame (the client pre-compiled the identical program at plan time).
fn compile_predicate(blob: &[u8], schema: &SchemaDescriptor) -> Result<Evaluator, String> {
    LogicalProgram::from_blob(blob, "scan_spec predicate")
        .and_then(|p| p.resolve_filter(schema))
        .map_err(|e| format!("scan_spec: invalid predicate program: {e}"))
}

/// Decode + validate a client projection (MAP) blob and build its [`MapPlan`] —
/// the same path the circuit compiler runs. `validate` bounds every payload slot
/// against `out_schema.num_payload_cols()` and requires the program to write all
/// of them, so an OOB or unwritten slot is a clean `Err` rather than a panic or
/// a shipped byte of the keeper's recycled tail.
fn compile_projection(
    blob: &[u8],
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
) -> Result<MapPlan, String> {
    LogicalProgram::from_map_blob(blob, "scan_spec projection")
        .and_then(|p| MapPlan::from_map(p, in_schema, out_schema, PkSource::Inherit))
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

    // ── scan_spec_worker — the master's confinement test ─────────────────────

    /// Worker count the confinement tests route against.
    const NW: usize = 4;

    /// A full point is confined to the worker of its own PK bytes, at every PK
    /// shape — single, wide, and compound (where the point pins the leading
    /// columns through `eq_vals` and points at the last).
    #[test]
    fn scan_spec_worker_confines_a_full_point() {
        let u64s = pk_only_schema(&[type_code::U64]);
        assert_eq!(
            scan_spec_worker(&u64s, &RangeDescriptor::new(&[], Before(42), After(42)), NW),
            Some(u64s.worker_for_pk(&opk_pk(&u64s, &[42]), NW))
        );

        let u128s = pk_only_schema(&[type_code::U128]);
        let wide = (1u128 << 100) | 7;
        assert_eq!(
            scan_spec_worker(&u128s, &RangeDescriptor::new(&[], Before(wide), After(wide)), NW),
            Some(u128s.worker_for_pk(&opk_pk(&u128s, &[wide]), NW))
        );

        let comp = pk_only_schema(&[type_code::U32, type_code::U64]);
        assert_eq!(
            scan_spec_worker(&comp, &RangeDescriptor::new(&[9], Before(4), After(4)), NW),
            Some(comp.worker_for_pk(&opk_pk(&comp, &[9, 4]), NW))
        );
    }

    /// With a `Keyed { prefix_len: 1 }` placement every row sharing the leading
    /// column lands on one worker, so pinning it and ranging the trailing column
    /// is confined — to the same worker full points on `(a, b)` reach. At the
    /// full-PK default the same bound spans workers.
    #[test]
    fn scan_spec_worker_follows_the_distribution_prefix() {
        let cols = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 0),
        ];
        let prefix = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 1 });
        // `a = 7 AND b > 3` — a whole trailing-column range inside one `a` group.
        let ranged = RangeDescriptor::new(&[7], After(3), After(u64::MAX as u128));
        let want = prefix.worker_for_pk(&opk_pk(&prefix, &[7, 0]), NW);
        assert_eq!(scan_spec_worker(&prefix, &ranged, NW), Some(want));
        for b in [4u128, u64::MAX as u128] {
            assert_eq!(
                scan_spec_worker(&prefix, &RangeDescriptor::new(&[7], Before(b), After(b)), NW),
                Some(want),
                "a full point on (7, {b}) shares the group's worker"
            );
        }

        let full = SchemaDescriptor::new_with_placement(&cols, &[0, 1], Placement::Keyed { prefix_len: 2 });
        assert_eq!(
            scan_spec_worker(&full, &ranged, NW),
            None,
            "hashing the whole PK spreads one `a` group across workers"
        );
    }

    /// A maximal-value point carries out of `succ`, so its range has no `end` —
    /// the all-`0xFF` last key must still confine it rather than broadcast. The
    /// signed maximum's OPK is all-`0xFF` too (sign-flip).
    #[test]
    fn scan_spec_worker_confines_a_maximal_point() {
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
                scan_spec_worker(&s, &d, NW),
                Some(s.worker_for_pk(&opk_pk(&s, &[max]), NW))
            );
        }
    }

    /// A range wider than one worker's key span is not confinable: owners are a
    /// hash of the key, not monotone in key order, so only a whole-range prefix
    /// match proves confinement. A provably-empty range is not confinable
    /// either — the worker answers it (a fold sink still owes its ground row).
    #[test]
    fn scan_spec_worker_declines_a_multi_key_range() {
        let s = pk_only_schema(&[type_code::U64]);
        assert_eq!(
            scan_spec_worker(&s, &RangeDescriptor::new(&[], Before(0), After(1000)), NW),
            None
        );
        assert_eq!(
            scan_spec_worker(&s, &RangeDescriptor::new(&[], After(1000), Before(0)), NW),
            None,
            "an inverted range is provably empty"
        );
        // Unbounded above from a non-maximal start: the last key is 0xFF…FF.
        assert_eq!(
            scan_spec_worker(&s, &RangeDescriptor::new(&[], Before(5), After(u64::MAX as u128)), NW),
            None
        );
    }
}
