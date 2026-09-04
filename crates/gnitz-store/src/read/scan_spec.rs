//! Parameterized bounded read (`ReadSpec`) execution — the worker half of the
//! ad-hoc SELECT scan. Runs **once per worker**: resolve the client's frame
//! against the bound's source schema, open a cursor for the bound, then per
//! chunk take the predicate's surviving row ranges and drive them into the sink
//! — rows or the aggregate hash-fold. Nothing between the source chunk and the
//! sink is materialized. No DBSP circuit, no operator state, no exchange.
//!
//! Resolve order is **source schema → predicate → sink → open**, and the open is
//! last because it is the expensive step: a hydrating materialization for a
//! capacity-bounded view, an encode and sort of the whole key list for a
//! `PkSet`. A forged frame costs a comparison, not a walk.
//!
//! The cursor spans this worker's whole slice of the relation; a bound that
//! names its keys narrows the walk within it. Which worker answers at all is the
//! master's question (`SchemaDescriptor::confined_worker`), not this module's.
//!
//! The reply schema arrives as the client's raw wire block (decoded by the
//! worker one layer up); this module takes the decoded `SchemaDescriptor`. The
//! block itself never leaves the request — the client decodes the reply against
//! its own copy.

use std::cmp::Ordering;

use gnitz_wire::{AggReadSpec, Cut, OrderKey, RangeDescriptor, ReadBound, ReadSink, ReadSpec};

use std::rc::Rc;

use super::SkeletonHydrator;
use crate::expr::{MapPlan, PkSource};
use crate::ops::AdhocFold;
use crate::relation::RelationRegistry;
use crate::schema::key::{compare_pk_bytes, opk_key, pack_pk_be, pk_range_keys};
use crate::schema::{ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor};
use crate::storage::{compare_rows, Batch, PkSetGather, ReadCursor, SourceCursor, StoreError};
use gnitz_expr::{Evaluator, LogicalProgram};

impl RelationRegistry {
    /// Execute `spec` against `target_id` on this worker's slice,
    /// returning one keeper batch in the `reply_schema` shape. The caller
    /// (the worker dispatch arm) decoded `reply_schema` from the client's wire
    /// block and replies with no block at all; this method does the bound walk,
    /// predicate, projection, and ORDER BY / LIMIT reduction.
    ///
    /// `Err` on a corrupt program blob, a reply schema that does not match the
    /// sink's expected shape (rows: PK-stride equality; fold: the derived
    /// SyntheticFold layout), an index column list naming a column the table has
    /// not got, an index an `exact` bound needs and cannot find — every one a
    /// corrupt/stale frame, surfaced as a `STATUS_ERROR` reply — the fold's
    /// per-worker group cap (a resource-exhaustion abort), or a delta cursor
    /// below this worker's retention floor, which is [`StoreError::DeltaExpired`].
    ///
    /// `cut_tick` is the last tick round the master had emitted when it wrote
    /// this read's group, and bounds an incremental delta read above. Every other
    /// bound ignores it.
    pub fn scan_spec_family(
        &self,
        target_id: i64,
        spec: &ReadSpec,
        reply_schema: &SchemaDescriptor,
        cut_tick: u64,
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<Batch, StoreError> {
        let src_schema = self.scan_spec_source_schema(target_id, &spec.bound)?;

        // Compile the predicate once per request, exactly as the circuit
        // compiler does (`query/compiler/emit.rs`).
        let predicate = match spec.predicate.is_empty() {
            true => None,
            false => Some(compile_predicate(&spec.predicate, &src_schema)?),
        };
        let ctx = ScanSinkCtx {
            predicate: predicate.as_ref(),
            chunk_rows: self.config.scan_chunk_rows,
        };

        // Each arm resolves its sink whole, then opens. The open is spelled
        // twice rather than hoisted above the match: hoisting it is what would
        // put the walk ahead of the frame's own rejections.
        match &spec.sink {
            ReadSink::Fold(agg) => {
                // The reduce input: the pre-map's output when the fold carries
                // one, else the source itself. Everything downstream — the fold's
                // column indices, its accumulator types and its derived partial
                // layout — is resolved against this, never against `src_schema`.
                let pre = compile_fold_pre_map(agg, &src_schema)?;
                let reduce_in = pre.as_ref().map_or(&src_schema, |(s, _)| s);
                let fold = AdhocFold::new(reduce_in, reply_schema, agg, self.config.adhoc_group_cap)?;
                let mut source = self.open_scan_spec_cursor(target_id, &spec.bound, &src_schema, cut_tick, hydrator)?;
                run_scan_fold_sink(&mut source, ctx, pre.as_ref(), fold)
            }
            ReadSink::Rows {
                projection,
                order,
                limit_k,
            } => {
                let projection = resolve_rows_projection(projection, &src_schema, reply_schema)?;
                let order_locs = resolve_order_locs(order, reply_schema)?;
                let window = saturated_window(*limit_k);
                let mut source = self.open_scan_spec_cursor(target_id, &spec.bound, &src_schema, cut_tick, hydrator)?;
                Ok(match !order_locs.is_empty() && window > 0 {
                    true => topk_rows(&mut source, ctx, projection.as_ref(), reply_schema, &order_locs, window),
                    false => stream_rows(&mut source, ctx, projection.as_ref(), reply_schema, window),
                })
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
    fn scan_spec_source_schema(&self, target: i64, bound: &ReadBound) -> Result<SchemaDescriptor, StoreError> {
        let entry = self.table_entry(target)?;
        let ReadBound::Delta { after_tick } = bound else {
            return Ok(entry.schema);
        };
        if entry.budgets.delta_bytes.is_none() {
            return Err(StoreError::rejected(format!(
                "scan_spec: relation {target} carries no delta feed; \
                 create the view WITH (delta = '<size>') to subscribe to it"
            )));
        }
        if *after_tick == 0 {
            return Ok(entry.schema);
        }
        Ok(entry.delta_feed_or_err(target)?.schema)
    }

    /// Open the source cursor for `bound` over `source`, bounded within this
    /// worker's store by whatever the bound names.
    ///
    /// A walk that meets a skeleton row ([`ReadCursor::any_skeleton`]) is
    /// hydrated whole first, so no sink below ever sees one — at the cost of this
    /// module's streaming property for that read.
    fn open_scan_spec_cursor(
        &self,
        source: i64,
        bound: &ReadBound,
        src_schema: &SchemaDescriptor,
        cut_tick: u64,
        hydrator: Option<&mut dyn SkeletonHydrator>,
    ) -> Result<SourceCursor, StoreError> {
        let (cursor, keys) = match bound {
            // The delta bootstrap arm reads the view's own store, which is what
            // `scan_spec_source_schema` already told the caller.
            ReadBound::None | ReadBound::Delta { after_tick: 0 } => (self.table_entry(source)?.open_cursor(), None),
            ReadBound::PkRange(desc) => {
                let entry = self.table_entry(source)?;
                match range_cursor(src_schema, desc, |s, e| entry.open_cursor_in_range(s, e))? {
                    Some(c) => (c, None),
                    None => return Ok(SourceCursor::Empty),
                }
            }
            ReadBound::PkSet(keys) => {
                let opk = pk_set_opk_keys(source, keys, src_schema)?;
                let entry = self.table_entry(source)?;
                // A key this worker holds no row for copies nothing — the request
                // is broadcast, so at W workers most of the list belongs elsewhere.
                let gather = PkSetGather::open(opk, *src_schema, |s, e| entry.open_cursor_in_range(s, e));
                if !gather.any_skeleton() {
                    return Ok(SourceCursor::PkSet(Box::new(gather)));
                }
                let (cursor, keys) = gather.into_parts();
                (cursor, Some(keys))
            }
            // Only base tables own index circuits, so an index bound names a base
            // table, which never holds a skeleton row.
            ReadBound::IndexRange { idx_cols, exact, desc } => {
                return self.open_index_bound_cursor(source, *idx_cols, *exact, desc)
            }
            // `capacity` and `delta` are refused together, so a fed view never
            // holds a skeleton row either.
            ReadBound::Delta { after_tick } => return self.open_delta_cursor(source, *after_tick, cut_tick),
        };
        if !cursor.any_skeleton() {
            return Ok(SourceCursor::Full(Box::new(cursor)));
        }
        let rows = self.materialize_hydrated(source, cursor, keys.as_deref(), hydrator)?;
        Ok(SourceCursor::Full(Box::new(ReadCursor::over_batches(
            &[Rc::new(rows)],
            *src_schema,
        ))))
    }

    /// The `(after_tick, cut_tick]` walk over a fed view's delta store.
    ///
    /// Both ends are `Cut::After`, so the open end is exclusive and no arithmetic
    /// touches the client's `after_tick` — `after_tick + 1` at `u64::MAX` would
    /// panic in debug and wrap in release.
    ///
    /// Refused when this worker has dropped a round the walk covers; the floors
    /// are independent across workers, so one refusal refuses the read. The walk
    /// covers `(after_tick, cut]`, so a dropped row is inside it iff
    /// `dropped_through > after_tick` — a cursor sitting *at* the floor is served
    /// in full rather than being told to re-read at 0 forever.
    fn open_delta_cursor(&self, source: i64, after_tick: u64, cut_tick: u64) -> Result<SourceCursor, StoreError> {
        let feed = self.table_entry(source)?.delta_feed_or_err(source)?;
        let dropped_through = feed.dropped_through();
        if after_tick < dropped_through {
            return Err(StoreError::DeltaExpired(format!(
                "delta cursor {after_tick} of relation {source} is below the \
                 retained floor {dropped_through}; re-read at 0"
            )));
        }
        let desc = RangeDescriptor::new(&[], Cut::After(after_tick as u128), Cut::After(cut_tick as u128));
        Ok(
            match range_cursor(&feed.schema, &desc, |s, e| feed.open_cursor_in_range(s, e))? {
                Some(c) => SourceCursor::Full(Box::new(c)),
                None => SourceCursor::Empty,
            },
        )
    }

    /// A secondary-index range walk. `exact` says the SQL layer stripped the
    /// bounded conjuncts from the predicate, so this walk is the only thing
    /// applying them: run it un-gated, and fail rather than degrade if the index
    /// is gone (a full cursor would return rows nothing re-filters). Otherwise
    /// the conjuncts ride the predicate and the walk is an access optimization
    /// the selectivity gate may trade away.
    ///
    /// The column word is the client's, so it takes the same admission test every
    /// other frame carrying one takes — on both walk kinds, since a column the
    /// table has not got is a corrupt frame either way.
    fn open_index_bound_cursor(
        &self,
        source: i64,
        idx_cols: u64,
        exact: bool,
        desc: &RangeDescriptor,
    ) -> Result<SourceCursor, StoreError> {
        let cols = gnitz_wire::unpack_pk_cols(idx_cols);
        self.validate_index_cols(source, &cols, "scan_spec")?;
        // `exact`: the walk alone imposes the range. Otherwise the conjuncts ride
        // the residual predicate and the walk may be traded away.
        let walk = if exact {
            super::IndexWalk::Required
        } else {
            super::IndexWalk::Optional
        };
        self.open_index_source(source, cols.as_slice(), desc, walk)
    }
}

/// A cursor cut to the half-open key range `desc` names over `schema`'s PK
/// space, or `None` for a provably-empty one. The one place a range descriptor
/// becomes a cursor cut, so the `PkRange` bound over a relation's own store and
/// the `Delta` bound over a fed view's share the zero-padded keys and the
/// saturating `After` arm.
fn range_cursor(
    schema: &SchemaDescriptor,
    desc: &RangeDescriptor,
    open: impl FnOnce(&[u8], Option<&[u8]>) -> ReadCursor,
) -> Result<Option<ReadCursor>, StoreError> {
    let Some((start, end_key)) = pk_range_keys(schema, desc).map_err(StoreError::rejected)? else {
        return Ok(None);
    };
    let end = end_key.as_ref().map(|e| e.pk_bytes());
    let mut cursor = open(start.pk_bytes(), end);
    cursor.seek_range_bytes(start.pk_bytes(), end);
    Ok(Some(cursor))
}

/// The OPK images of a `pk IN (…)` key list, sorted ascending and concatenated —
/// the walk order `PkSetGather` requires. OPK order IS typed PK order, so sorting
/// the images makes the gather one monotone forward sweep regardless of wire key
/// order.
///
/// Two trust-boundary rejections land here because the decoder has no schema:
/// the packed key must fit the wire's 16-byte scalar form, and no two wire keys
/// may share an OPK image (`opk_key` truncates to `pk_stride`, so `5` and
/// `5 + 2^64` are the same U64 PK — left in, the pair would emit its row twice).
/// Both are hard rejects; release builds must not clamp.
fn pk_set_opk_keys(source: i64, keys: &[u128], src_schema: &SchemaDescriptor) -> Result<Vec<u8>, StoreError> {
    let stride = src_schema.pk_stride() as usize;
    if stride > gnitz_wire::NARROW_PK_MAX_BYTES {
        return Err(StoreError::rejected(format!(
            "scan_spec: PkSet gather requires a PK of at most {} bytes (table {source})",
            gnitz_wire::NARROW_PK_MAX_BYTES
        )));
    }
    // The whole packed key fits `pack_pk_be`'s left-aligned `u128` sort key
    // exactly — one allocation, and a register compare in both the sort and the
    // duplicate scan.
    let mut opk_keys: Vec<u128> = keys
        .iter()
        .map(|&k| pack_pk_be(opk_key(src_schema, &k.to_le_bytes()).pk_bytes()))
        .collect();
    opk_keys.sort_unstable();
    // Adjacent after the sort. Tested against the list rather than against what
    // was gathered, so every worker answers the same way.
    if let Some(w) = opk_keys.windows(2).find(|w| w[0] == w[1]) {
        return Err(StoreError::rejected(format!(
            "scan_spec: PkSet duplicate key {:?} (table {source})",
            &w[0].to_be_bytes()[..stride]
        )));
    }
    let mut flat = Vec::with_capacity(opk_keys.len() * stride);
    for k in &opk_keys {
        flat.extend_from_slice(&k.to_be_bytes()[..stride]);
    }
    Ok(flat)
}

/// The rows sink's projection plan, or `None` for the identity reply, with the
/// reply-schema guard each shape needs: an identity reply is copied region-wise
/// off its own strides and so must be the source schema outright, where a
/// projected one need only agree on `pk_stride`. Both guard the client's blob —
/// a stride mismatch ships the keeper's uninitialized tail as its PK tiebreak.
fn resolve_rows_projection(
    blob: &[u8],
    src_schema: &SchemaDescriptor,
    reply_schema: &SchemaDescriptor,
) -> Result<Option<MapPlan>, StoreError> {
    if blob.is_empty() {
        return match reply_schema.same_physical_layout(src_schema) {
            true => Ok(None),
            false => Err(StoreError::rejected(
                "scan_spec: identity rows reply schema differs from the source",
            )),
        };
    }
    if reply_schema.pk_stride() != src_schema.pk_stride() {
        return Err(StoreError::rejected(format!(
            "scan_spec: reply pk_stride {} != source pk_stride {}",
            reply_schema.pk_stride(),
            src_schema.pk_stride()
        )));
    }
    compile_projection(blob, src_schema, reply_schema).map(Some)
}

/// The client's `limit_k` (`OFFSET + LIMIT`; `0` = unbounded) as the summed-i64
/// weight window every sink test is against. Unsaturated the cast wraps negative
/// past `i64::MAX`, where the first survivor range satisfies the window and
/// truncates the answer.
fn saturated_window(limit_k: u64) -> i64 {
    limit_k.min(i64::MAX as u64) as i64
}

/// The per-request context both sinks read on every chunk: the compiled
/// predicate and the drain size.
struct ScanSinkCtx<'a> {
    predicate: Option<&'a Evaluator>,
    chunk_rows: usize,
}

/// The fold's reduce-input schema and the pre-map producing it, or `None` when
/// the spec carries no pre-map and the source *is* the reduce input.
///
/// The schema is derived, not shipped: the map inherits the source PK region
/// verbatim ([`PkSource::Inherit`]), so the reduce input is the source's PK
/// columns followed by the payload slots the client declared. Deriving it here
/// is what keeps the PK half unforgeable — a client cannot describe a PK region
/// the map does not actually produce.
fn compile_fold_pre_map(
    agg: &AggReadSpec,
    src_schema: &SchemaDescriptor,
) -> Result<Option<(SchemaDescriptor, MapPlan)>, StoreError> {
    let Some(pre) = &agg.pre else {
        return Ok(None);
    };
    // Through `DerivedSchema`, the front door that *rejects* what
    // `SchemaDescriptor::new` asserts on: the declared column count is the
    // client's, and an over-wide one has to be a malformed frame rather than an
    // abort inside a `const fn`. `push_pk_of` is the same
    // inherit-the-input's-key prologue every other derived schema uses.
    let mut out = DerivedSchema::new();
    let built = out.push_pk_of(src_schema).is_some()
        && pre
            .out_cols
            .iter()
            .all(|&(tc, nullable)| out.push(SchemaColumn::new(tc, nullable as u8)).is_some());
    if !built {
        return Err(StoreError::rejected(format!(
            "scan_spec fold: a pre-map declaring {} payload columns over a {}-column key \
             is not a legal reduce input",
            pre.out_cols.len(),
            src_schema.pk_indices().len()
        )));
    }
    let out_schema = out.finish();
    // `compile_projection` requires the program to write every declared payload
    // slot, so a program disagreeing with the declarations is rejected here
    // rather than folding over an unwritten column.
    let plan = compile_projection(&pre.program, src_schema, &out_schema)?;
    Ok(Some((out_schema, plan)))
}

/// Run the fold sink over `source`: fold every surviving chunk into `fold`'s
/// per-group accumulators and return the partial reduce-output rows — or `Err`
/// when the per-worker group cap is exceeded (a resource-exhaustion abort,
/// before any data frame is sent).
///
/// `pre` is the reduce's own map and the schema it produces, applied between the
/// predicate and the fold — the position the view path's circuit puts it in, and
/// fused with the filter so a chunk is never materialized twice.
fn run_scan_fold_sink(
    source: &mut SourceCursor,
    ctx: ScanSinkCtx,
    pre: Option<&(SchemaDescriptor, MapPlan)>,
    mut fold: AdhocFold,
) -> Result<Batch, StoreError> {
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    // The plan and its destination in one Option, so no arm can fold source-schema
    // rows into a fold built for the reduce input. One reusable mapped batch:
    // `clear` keeps the buffers (and resets the blob), so a wide scan pays one
    // allocation, not one per chunk.
    let mut pre = pre.map(|(schema, plan)| (plan, Batch::empty_with_schema(schema)));
    while let Some(chunk) = source.drain_chunk(ctx.chunk_rows) {
        if chunk.count == 0 {
            continue;
        }
        survivor_ranges(ctx.predicate, &chunk, &mut ranges);
        match &mut pre {
            Some((plan, dst)) => {
                dst.clear();
                plan.append_map_ranges(&chunk, dst, &ranges);
                // The map already dropped the non-survivors, so every row of
                // `dst` folds — and a chunk none survived maps to nothing.
                if dst.count > 0 {
                    fold.fold_ranges(dst, &[(0, dst.count)])?;
                }
            }
            None => fold.fold_ranges(&chunk, &ranges)?,
        }
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

/// Append one chunk's survivor ranges onto the keeper — straight, or through the
/// projection. There is no intermediate survivor batch and no projected batch.
fn append_survivors(projection: Option<&MapPlan>, chunk: &Batch, keeper: &mut Batch, ranges: &[(usize, usize)]) {
    match projection {
        None => keeper.append_ranges(&chunk.as_mem_batch(), ranges),
        Some(p) => p.append_map_ranges(chunk, keeper, ranges),
    }
}

/// The rows sink with no ORDER BY: append every survivor, stopping once the
/// summed survivor weight reaches a `window > 0`.
///
/// The cut is applied to the range list too — at the first range that covers the
/// window — so with survivors ≫ `window` that gathers ~`window` rows instead of
/// the whole chunk's survivors. Ranges are whole rows, so the worker still
/// returns a ≥ `window`-weight superset and the client applies the exact window.
fn stream_rows(
    source: &mut SourceCursor,
    ctx: ScanSinkCtx,
    projection: Option<&MapPlan>,
    reply_schema: &SchemaDescriptor,
    window: i64,
) -> Batch {
    let early_stop = window > 0;
    // `window`-sized chunks make the early stop O(window) rather than O(chunk),
    // but only while every drained row survives: with a predicate a tiny chunk
    // degrades to row-at-a-time cursor driving, so over-read a full one instead.
    let drain_rows = match early_stop && ctx.predicate.is_none() {
        true => (window as usize).clamp(1, ctx.chunk_rows),
        false => ctx.chunk_rows,
    };

    let mut keeper = Batch::empty_with_schema(reply_schema);
    let mut summed: i64 = 0;
    // Per-request scratch, reused across chunks.
    let mut ranges: Vec<(usize, usize)> = Vec::new();

    while let Some(chunk) = source.drain_chunk(drain_rows) {
        if chunk.count == 0 {
            continue;
        }
        survivor_ranges(ctx.predicate, &chunk, &mut ranges);
        if early_stop {
            // Weighed off the source — the same weights that land in the keeper,
            // read from a contiguous region rather than row-by-row off the
            // destination.
            for (i, &(s, e)) in ranges.iter().enumerate() {
                summed += chunk.sum_weights(s, e);
                if summed >= window {
                    ranges.truncate(i + 1);
                    break;
                }
            }
        }
        append_survivors(projection, &chunk, &mut keeper, &ranges);
        if early_stop && summed >= window {
            break;
        }
    }
    keeper
}

/// The rows sink with an ORDER BY and a `window > 0`: append every survivor and
/// trim the keeper back down with [`topk_keep`], at two thresholds.
fn topk_rows(
    source: &mut SourceCursor,
    ctx: ScanSinkCtx,
    projection: Option<&MapPlan>,
    reply_schema: &SchemaDescriptor,
    order_locs: &[OrderLocator],
    window: i64,
) -> Batch {
    // Mid-scan the keeper is still growing, so a trim at `window` would re-sort
    // after every chunk to shed rows the next chunk replaces. Saturating: an
    // unbounded `limit_k` leaves this unfireable rather than overflowing.
    let residency_cap = window.saturating_mul(2);
    let mut keeper = Batch::empty_with_schema(reply_schema);
    // Summed survivor weight of what the keeper currently holds.
    let mut summed: i64 = 0;
    // Per-request scratch, reused across chunks.
    let mut ranges: Vec<(usize, usize)> = Vec::new();

    while let Some(chunk) = source.drain_chunk(ctx.chunk_rows) {
        if chunk.count == 0 {
            continue;
        }
        survivor_ranges(ctx.predicate, &chunk, &mut ranges);
        // Weighed off the source — the same weights that land in the keeper,
        // read from a contiguous region rather than row-by-row off the
        // destination.
        summed += ranges.iter().map(|&(s, e)| chunk.sum_weights(s, e)).sum::<i64>();
        append_survivors(projection, &chunk, &mut keeper, &ranges);
        if summed > residency_cap {
            (keeper, summed) = topk_keep(keeper, order_locs, window);
        }
    }
    // The keeper IS the reply now, and a rows reply carrying a STRING/BLOB column
    // goes out as one frame — so shed down to the smallest superset the client
    // can still cut exactly. At or below the window it provably cuts nothing.
    if summed > window {
        (keeper, _) = topk_keep(keeper, order_locs, window);
    }
    keeper
}

/// Sort `keeper` by the ORDER BY comparator and keep the smallest prefix whose
/// summed weight covers `window`, the boundary row kept **whole** — a per-worker
/// row below the global cutoff needs its full weight, else a cross-worker
/// under-count results. Returns the kept batch and its summed weight.
///
/// Every keeper row is a live consolidated group (weight ≥ 1), so the `window`
/// comparator-smallest rows always cover it and only that prefix needs sorting.
/// Clipping the boundary row and applying OFFSET stay the client's.
fn topk_keep(keeper: Batch, order_locs: &[OrderLocator], window: i64) -> (Batch, i64) {
    if keeper.count == 0 {
        return (keeper, 0);
    }
    let mut perm: Vec<u32> = (0..keeper.count as u32).collect();
    let cmp = |a: &u32, b: &u32| scan_spec_cmp(order_locs, &keeper, *a as usize, *b as usize);
    let k = (window as usize).min(perm.len());
    if k < perm.len() {
        perm.select_nth_unstable_by(k - 1, cmp);
        perm.truncate(k);
    }
    perm.sort_unstable_by(cmp);

    let mut acc: i64 = 0;
    let mut cut = perm.len();
    for (i, &row) in perm.iter().enumerate() {
        acc += keeper.get_weight(row as usize);
        if acc >= window {
            cut = i + 1; // keep [0, i] inclusive — the boundary row whole
            break;
        }
    }
    let schema = keeper.schema;
    (
        Batch::from_indexed_rows(&keeper.as_mem_batch(), &perm[..cut], &schema),
        acc,
    )
}

/// One resolved ORDER BY key.
struct OrderLocator {
    loc: ColumnLocator,
    desc: bool,
    nulls_first: bool,
}

/// Resolve each ORDER BY key to its reply-schema locator.
///
/// `OrderKey.col` is a raw client `u16` reaching `reply_schema.locate`, whose
/// bound is a release-active `assert!` — so a forged one is caught here, ahead
/// of the open.
fn resolve_order_locs(order: &[OrderKey], reply_schema: &SchemaDescriptor) -> Result<Vec<OrderLocator>, StoreError> {
    order
        .iter()
        .map(|k| match (k.col as usize) < reply_schema.num_columns() {
            true => Ok(OrderLocator {
                loc: reply_schema.locate(k.col as usize),
                desc: k.desc,
                nulls_first: k.nulls_first,
            }),
            false => Err(StoreError::rejected(format!(
                "scan_spec: order key column {} out of range ({} cols)",
                k.col,
                reply_schema.num_columns()
            ))),
        })
        .collect()
}

/// The worker-side ORDER BY comparator over two rows of a reply-schema batch.
/// Byte-for-byte equivalent to the client's `SortKey` order (the shared
/// `cmp_typed_le` / `compare_german_strings` / OPK tiebreak) so each worker keeps
/// a superset of its window contribution: user keys (NULLs placed absolutely per
/// `nulls_first`, values reversed for `desc`), then the deterministic OPK-then-
/// payload tiebreak.
fn scan_spec_cmp(order_locs: &[OrderLocator], batch: &Batch, ra: usize, rb: usize) -> Ordering {
    let a_null = batch.get_null_word(ra);
    let b_null = batch.get_null_word(rb);
    for key in order_locs {
        // NULL placement is absolute — `nulls_first` decides it, and `desc` does
        // not flip it. A PK locator answers `false` on both sides and falls
        // through to the value compare.
        match (key.loc.is_null_word(a_null), key.loc.is_null_word(b_null)) {
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
        // The locator carries both the addressing and the order rule: a PK
        // column's OPK window compares raw (order-preserving), a payload column
        // through the typed dispatch that routes STRING/BLOB by content.
        let mut ord = key.loc.cmp_non_null(batch, ra, batch, rb);
        if key.desc {
            ord = ord.reverse();
        }
        if ord != Ordering::Equal {
            return ord;
        }
    }
    // Deterministic tiebreak (never reversed): OPK bytes, then payload columns.
    match compare_pk_bytes(batch.get_pk_bytes(ra), batch.get_pk_bytes(rb)) {
        Ordering::Equal => compare_rows(&batch.schema, batch, ra, batch, rb),
        ord => ord,
    }
}

/// Decode + validate a client predicate blob against `schema`, then build its
/// predicate `Evaluator` — the same path the circuit compiler runs. Any failure
/// is a corrupt frame (the client pre-compiled the identical program at plan time).
fn compile_predicate(blob: &[u8], schema: &SchemaDescriptor) -> Result<Evaluator, StoreError> {
    LogicalProgram::from_blob(blob, "scan_spec predicate")
        .and_then(|p| p.resolve_filter(schema))
        .map_err(|e| StoreError::rejected(format!("scan_spec: invalid predicate program: {e}")))
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
) -> Result<MapPlan, StoreError> {
    LogicalProgram::from_map_blob(blob, "scan_spec projection")
        .and_then(|p| MapPlan::from_map(p, in_schema, out_schema, PkSource::Inherit))
        .map_err(|e| StoreError::rejected(format!("scan_spec: invalid projection program: {e}")))
}

#[cfg(test)]
#[path = "tests/scan_spec.rs"]
mod tests;
