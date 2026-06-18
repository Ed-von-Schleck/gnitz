//! Master-side unique-index filter cache: the `MasterDispatcher` methods that
//! warm, query, seed, and invalidate the per-`(table, packed_cols)` `UniqueFilter`s.
//! The `UniqueFilter` / `WarmupGuard` / `UniqueIndexDesc` types and `extract_into_filter`
//! live in the parent module (shared with the preflight seed path).

use super::*;

impl MasterDispatcher {
    // -----------------------------------------------------------------------
    // Unique-index filter
    // -----------------------------------------------------------------------

    /// Collect column-extraction descriptors for every unique index on
    /// `table_id` — the filter-map key plus the span encode plan per unique
    /// circuit, the shape `extract_into_filter` consumes.
    fn unique_index_descriptors(
        &mut self, table_id: i64,
    ) -> Option<(SchemaDescriptor, Vec<UniqueIndexDesc>)> {
        let cat = unsafe { &mut *self.catalog };
        let schema = cat.get_schema_desc(table_id)?;
        let n_circuits = cat.get_index_circuit_count(table_id);

        let mut out: Vec<UniqueIndexDesc> = Vec::new();
        for ci in 0..n_circuits {
            let Some(cols) = cat.unique_index_circuit_cols(table_id, ci) else { continue };
            let idx_schema = match cat.get_index_circuit_schema(table_id, ci) {
                Some(s) => s,
                None => continue,
            };
            out.push(UniqueIndexDesc {
                packed: gnitz_wire::pack_pk_cols(cols),
                spec: IndexKeySpec::new(cols, &schema, &idx_schema),
            });
        }
        if out.is_empty() { None } else { Some((schema, out)) }
    }

    /// True if every key in `keys` is definitely absent from the filter
    /// for `(table_id, packed)`. Returns false if the filter is capped,
    /// not warm (caller is expected to warm it first), or contains any
    /// key. On false, caller must fall through to the Phase 2 broadcast.
    pub(super) fn unique_filter_all_absent(
        &self, table_id: i64, packed: u64, keys: &[PkBuf],
    ) -> bool {
        let filter = match self.unique_filters.get(&(table_id, packed)) {
            Some(f) => f,
            None => return false,
        };
        if !filter.warm || filter.capped {
            return false;
        }
        keys.iter().all(|k| !filter.values.contains(k))
    }

    /// Record every unique-index value from a successfully-flushed
    /// `batch` on `table_id` into the corresponding filters. No-op for
    /// filters that are not yet warm (warmup will pick them up), and
    /// for index circuits that are not unique.
    pub(crate) fn unique_filter_ingest_batch(&mut self, table_id: i64, batch: &Batch) {
        let (_schema, descs) = match self.unique_index_descriptors(table_id) {
            Some(x) => x,
            None => return,
        };
        let mb = batch.as_mem_batch();
        for d in descs {
            let filter = match self.unique_filters.get_mut(&(table_id, d.packed)) {
                Some(f) => f,
                None => continue, // not warm — warmup will pick this up
            };
            if filter.capped { continue; }
            extract_into_filter(filter, &mb, &d.spec);
        }
    }

    /// Drop every filter entry for `table_id`. Called on flush errors
    /// (where filter state may be out of sync with workers) and on DDL
    /// changes (DROP TABLE, DROP/CREATE INDEX).
    pub(crate) fn unique_filter_invalidate_table(&mut self, table_id: i64) {
        self.unique_filters.retain(|&(t, _), _| t != table_id);
        // The check-batch pool is a pure allocation cache keyed by table id;
        // drop the dropped table's entry so it doesn't leak across DDL cycles.
        self.check_batch_pool.remove(&table_id);
    }

    /// Remove the unique-filter entry for a single (owner_table_id, packed)
    /// pair. `packed` is the `pack_pk_cols(col_indices)` / `IDXTAB_PAY_SOURCE_COLS`
    /// value. Called on DROP INDEX so subsequent INSERTs re-trigger warmup for
    /// the now-absent index while leaving unrelated filters on the same table; a
    /// non-existent key (e.g. a non-unique FK index) is a harmless no-op.
    pub(crate) fn unique_filter_remove(&mut self, owner_id: i64, packed: u64) {
        self.unique_filters.remove(&(owner_id, packed));
    }

    // -----------------------------------------------------------------------
    // Pipelined distributed validation
    /// Async version of `ensure_unique_filters_warm`. Feeds each worker's
    /// scan reply directly into the filter(s) instead of concatenating
    /// them into a single master-side `Batch` the way `fan_out_scan_async`
    /// does — on a table with tens of millions of rows the concatenation
    /// step would peak at ~2× the total scan size and risk OOM.
    ///
    /// We intentionally go through `join_all` rather than awaiting each
    /// `await_reply` serially: `join_all`'s first poll registers every
    /// `req_id`'s waker before any worker reply can arrive, while a
    /// serial await would register them one at a time and `route_reply`
    /// would drop any reply whose waker isn't yet in `reply_wakers`
    /// (and forget to decrement `in_flight[w]`, stalling the W2M ring).
    /// The peak we do pay is one `DecodedWire` per worker during the
    /// processing loop, not the full concatenated batch.
    pub(super) async fn ensure_unique_filters_warm_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        table_id: i64,
    ) -> Result<(), String> {
        let (schema, missing, mut guard): (SchemaDescriptor, Vec<UniqueIndexDesc>, WarmupGuard) = unsafe {
            let disp = &mut *disp_ptr;
            let (schema, descs) = match disp.unique_index_descriptors(table_id) {
                Some(x) => x,
                None => return Ok(()),
            };
            let missing: Vec<UniqueIndexDesc> = descs.into_iter()
                .filter(|d| !disp.unique_filters.contains_key(&(table_id, d.packed)))
                .collect();
            if missing.is_empty() { return Ok(()); }
            for d in &missing {
                disp.unique_filters.insert((table_id, d.packed), UniqueFilter::new());
            }
            let missing_keys: Vec<u64> = missing.iter().map(|d| d.packed).collect();
            let guard = WarmupGuard {
                disp_ptr,
                table_id,
                keys: missing_keys,
                disarmed: false,
            };
            (schema, missing, guard)
        };

        // `_lease` held across the full continuation drain below; its workers
        // stream multi-frame trains, and on an early error return (or a
        // mid-scan cancellation) the lease drop discards every undrained
        // frame at the ring boundary.
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(table_id);
            let lsn = disp.next_lsn();
            disp.write_group_with_req_ids(
                table_id, lsn, 0, 0, &[], &schema, &col_names,
                0, 0, req_ids, -1, 0, None, &[],
            )
        }).await?;

        // Drain every worker's continuation-frame train into the cold filters.
        // `drain_index_scan` owns the early-return error contract (the lease
        // drop above discards any undrained frames at the ring boundary), the
        // schema guard against DDL-lagged worker replies, the zero-copy
        // `MemBatch` lifetime, and the continuation-schema-hint handling.
        let scan_result = drain_index_scan(slots, &req_ids, reactor, "scan", &schema, |mb, _| {
            let disp = unsafe { &mut *disp_ptr };
            for d in &missing {
                if let Some(filter) = disp.unique_filters.get_mut(&(table_id, d.packed)) {
                    if !filter.capped {
                        extract_into_filter(filter, mb, &d.spec);
                    }
                }
            }
            Ok(())
        }).await;

        // On success the filters are fully populated → mark warm so the
        // broadcast-skip shortcut may trust them. Disarm the guard so the
        // drop handler does not remove the now-warm entries. On failure (worker
        // crash mid-scan or cancellation) let the guard's Drop handler remove
        // the cold entries so the next validation retries warmup from scratch.
        match scan_result {
            Ok(()) => {
                let disp = unsafe { &mut *disp_ptr };
                for d in &missing {
                    if let Some(f) = disp.unique_filters.get_mut(&(table_id, d.packed)) {
                        f.warm = true;
                    }
                }
                guard.disarmed = true;
                Ok(())
            }
            Err(e) => {
                // Guard is not disarmed → Drop removes the cold entries.
                Err(e)
            }
        }
    }

    /// Seed the `(table_id, col_idx)` filter from the CREATE-time pre-flight,
    /// captured under the catalog write lock. Marks it warm so the first
    /// INSERT skips `ensure_unique_filters_warm_async`. `capped = true` (the
    /// accumulator overflowed and cleared its set whole — `seen` arrives
    /// empty) publishes a warm+capped filter: `unique_filter_all_absent` then
    /// always falls through to the broadcast — the same steady state the lazy
    /// warmup converges to, without paying a redundant full-cluster scan on
    /// the first INSERT. Symmetric counterpart of `unique_filter_remove`.
    pub(crate) fn unique_filter_seed(
        &mut self, table_id: i64, packed: u64, seen: FxHashSet<PkBuf>, capped: bool,
    ) {
        let mut filter = UniqueFilter::new();
        filter.warm = true;     // pre-flight scanned every worker under the write lock
        if capped {
            filter.capped = true;
        } else {
            filter.values = seen;   // exact distinct set; same type, move not re-hash
        }
        self.unique_filters.insert((table_id, packed), filter);
    }

}
