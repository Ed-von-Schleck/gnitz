//! Master-side unique-index filter cache: the `UniqueFilter` / `WarmupGuard` /
//! `UniqueIndexDesc` types, `extract_into_filter`, and the `MasterDispatcher`
//! methods that warm, query, seed, and invalidate the per-`(table,
//! packed_cols)` filters (the preflight seed path shares the types).

use super::*;

#[cfg(test)]
use super::preflight::{build_check_batch, build_check_batch_pkbuf, format_pk_value_bytes};

// ---------------------------------------------------------------------------
// Master-side unique-index filter
// ---------------------------------------------------------------------------
//
// For each `(table_id, unique_col_idx)` we maintain a HashSet of all
// indexed values known to exist in the unique index. Phase 2 of
// `validate_all_distributed` consults this filter before building a
// broadcast: if every new value is definitely absent from the filter,
// that index's broadcast is skipped entirely.
//
// Correctness invariants:
//   1. A value is only added to the filter AFTER fsync confirms the
//      owning batch is durable. Before fsync, the filter is NOT updated,
//      so an in-flight insert never creates a false "present" entry.
//   2. On any flush error, filters for affected tables are invalidated
//      (dropped from the map) and lazily re-warmed on next use from
//      authoritative worker state.
//   3. Warmup is lazy: the first query for an unwarmed `(tid, col)`
//      triggers a `fan_out_scan` against the main table, from which
//      the indexed-column values are extracted. Subsequent queries
//      read the HashSet directly.
//   4. Deletes are NOT removed from the filter — they leave stale
//      "possibly present" entries that cause harmless fall-through
//      broadcasts. Stale accumulation is bounded by `UNIQUE_FILTER_CAP`:
//      once exceeded, the filter flips to `capped = true` and disables
//      itself until invalidated.
//   5. The master event loop is single-threaded, so there is no race
//      between query, warmup, and ingest.

/// Maximum number of values tracked per `(table_id, packed_cols)` filter.
/// Filters that would exceed this disable themselves. Kept at 1M even though a
/// `PkBuf` key (81 bytes: `[u8; 80]` + len) is ~5× a `u128`'s 16: at the cap,
/// hashbrown's inline storage (7/8 max load → 2²¹ power-of-two buckets × 82 B)
/// allocates ≈170 MB per maxed filter, vs ≈36 MB for the old `u128` keys —
/// bounded, and only reached by a table holding 1M distinct unique values.
/// Lowering the cap would shrink the broadcast-skip reach (every unique index
/// with > cap distinct values reverts to always-broadcast on the insert hot
/// path); the cap stays the lever for a memory-constrained deployment. The
/// PkBuf width is identical for the single-column ≤16-byte and the composite
/// cases — one key type, one code path.
pub(super) const UNIQUE_FILTER_CAP: usize = 1_000_000;

pub(super) struct UniqueFilter {
    /// The OPK leading-key spans known present in the index. A `PkBuf` holds the
    /// full composite span at any width, so a `UNIQUE (a, b)` whose span exceeds
    /// 16 bytes is tracked without truncation (a truncating `u128` could prove a
    /// present key absent and wrongly skip the broadcast).
    pub(super) values: FxHashSet<PkBuf>,
    /// Maximum distinct values tracked: `UNIQUE_FILTER_CAP` in production,
    /// parameterizable so tests exercise the cap discipline cheaply.
    pub(super) cap: usize,
    /// True once the filter has exceeded `cap`. In that
    /// state `values` is cleared and the filter always reports
    /// "possibly present" (falls through to broadcast).
    pub(super) capped: bool,
    /// False until the warmup scan has fully populated `values`. While
    /// false the filter still accepts ingestion (so keys committed during
    /// the scan window are not lost) but `unique_filter_all_absent`
    /// refuses the broadcast-skip shortcut — an empty/partial filter must
    /// never be trusted to prove absence.
    pub(super) warm: bool,
}

impl UniqueFilter {
    pub(super) fn new() -> Self {
        Self::with_cap(UNIQUE_FILTER_CAP)
    }

    pub(super) fn with_cap(cap: usize) -> Self {
        UniqueFilter {
            values: FxHashSet::default(),
            cap,
            capped: false,
            warm: false,
        }
    }

    /// On overflow the set is cleared WHOLE, never truncated: a partial set
    /// would prove "absent" for a present key — a uniqueness hole.
    pub(super) fn insert(&mut self, key: PkBuf) {
        if self.capped {
            return;
        }
        self.values.insert(key);
        if self.values.len() > self.cap {
            self.values = FxHashSet::default();
            self.capped = true;
        }
    }
}

/// RAII guard that removes cold UniqueFilter entries if the warmup future is
/// dropped before it completes. Uses a raw pointer rather than `&mut Map` to
/// avoid holding a mutable reference across `.await` suspension points, which
/// would violate noalias assumptions if the committer task touches the map
/// while the master future is parked.
pub(super) struct WarmupGuard {
    pub(super) disp_ptr: *mut MasterDispatcher,
    pub(super) table_id: i64,
    /// `pack_pk_cols(col_indices)` per cold filter — the `unique_filters` map
    /// key, so the drop handler removes exactly the entries this warmup created.
    pub(super) keys: Vec<u64>,
    pub(super) disarmed: bool,
}

impl Drop for WarmupGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            unsafe {
                let disp = &mut *self.disp_ptr;
                for &packed in &self.keys {
                    disp.unique_filter_remove(self.table_id, packed);
                }
            }
        }
    }
}

// Safety: WarmupGuard is only used inside the single-threaded master event loop.
unsafe impl Send for WarmupGuard {}

/// Column-extraction descriptor for one unique index on a table, `Copy` (built
/// fresh per batch on the hot ingest path — no heap allocation). `packed`
/// (= `pack_pk_cols(col_indices)`) keys the `unique_filters` map and is the
/// exact `IDXTAB_PAY_SOURCE_COLS` value, so seed/drop/warmup all derive the
/// same key; `spec` is the per-circuit span encode plan.
#[derive(Clone, Copy)]
pub(super) struct UniqueIndexDesc {
    pub(super) packed: u64,
    pub(super) spec: IndexKeySpec,
}

/// Walk every positive-weight, non-null row of `batch` and insert the indexed
/// columns' OPK leading-key span into `filter`. Respects the filter's capped
/// state by stopping the walk once the filter caps. A row with a NULL in any
/// indexed column is skipped (`key_bytes` → false), sharing the NULL-distinct
/// key contract with the CREATE-time validator and the projection.
pub(super) fn extract_into_filter(
    filter: &mut UniqueFilter,
    batch: &crate::storage::MemBatch<'_>,
    spec: &IndexKeySpec,
) {
    let mut keybuf = PkBuf::empty(0);
    for row in 0..batch.count {
        if batch.get_weight(row) <= 0 {
            continue;
        }
        if !spec.key_bytes(batch, row, &mut keybuf) {
            continue;
        }
        filter.insert(keybuf);
        if filter.capped {
            return;
        } // stop walking once the filter caps
    }
}

impl MasterDispatcher {
    // -----------------------------------------------------------------------
    // Unique-index filter
    // -----------------------------------------------------------------------

    /// Collect column-extraction descriptors for every unique index on
    /// `table_id` — the filter-map key plus the span encode plan per unique
    /// circuit, the shape `extract_into_filter` consumes.
    fn unique_index_descriptors(&mut self, table_id: i64) -> Option<(SchemaDescriptor, Vec<UniqueIndexDesc>)> {
        let cat = unsafe { &mut *self.catalog };
        let schema = cat.get_schema_desc(table_id)?;

        // One pass over the circuit list; the span plan is the circuit's
        // precomputed `key_spec`. Uniqueness is filtered on the LIVE flag —
        // promotion/demotion flips it without rebuilding the spec.
        let out: Vec<UniqueIndexDesc> = cat
            .index_circuits(table_id)
            .iter()
            .filter(|ic| ic.is_unique)
            .map(|ic| UniqueIndexDesc {
                packed: gnitz_wire::pack_pk_cols(ic.col_indices.as_slice()),
                spec: ic.key_spec,
            })
            .collect();
        if out.is_empty() {
            None
        } else {
            Some((schema, out))
        }
    }

    /// True if every key in `keys` is definitely absent from the filter
    /// for `(table_id, packed)`. Returns false if the filter is capped,
    /// not warm (caller is expected to warm it first), or contains any
    /// key. On false, caller must fall through to the Phase 2 broadcast.
    pub(super) fn unique_filter_all_absent(&self, table_id: i64, packed: u64, keys: &[PkBuf]) -> bool {
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
            if filter.capped {
                continue;
            }
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
            let missing: Vec<UniqueIndexDesc> = descs
                .into_iter()
                .filter(|d| !disp.unique_filters.contains_key(&(table_id, d.packed)))
                .collect();
            if missing.is_empty() {
                return Ok(());
            }
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

        // Single-source a REPLICATED table's warmup scan: a fan-out would
        // stream `nw` copies of the same rows back to build one filter (a set —
        // dedup keeps it correct, but the extra `nw - 1` full-table scans are
        // pure waste).
        let unicast = replicated_unicast(disp_ptr, table_id);

        // `_lease` held across the full continuation drain below; its workers
        // stream multi-frame trains, and on an early error return (or a
        // mid-scan cancellation) the lease drop discards every undrained
        // frame at the ring boundary.
        let (slots, req_ids, _lease) =
            dispatch_scan_fanout(disp_ptr, reactor, sal_excl, unicast, |disp, req_ids, unicast| {
                let (schema, block, _safe, _stride) = disp.cached_schema_block(table_id);
                disp.write_group_with_req_ids(
                    table_id,
                    0,
                    0,
                    &[],
                    &schema,
                    &[],
                    0,
                    0,
                    req_ids,
                    unicast,
                    0,
                    Some(block.as_slice()),
                    &[],
                )
            })
            .await?;

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
        })
        .await;

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
    pub(crate) fn unique_filter_seed(&mut self, table_id: i64, packed: u64, seen: FxHashSet<PkBuf>, capped: bool) {
        let mut filter = UniqueFilter::new();
        filter.warm = true; // pre-flight scanned every worker under the write lock
        if capped {
            filter.capped = true;
        } else {
            filter.values = seen; // exact distinct set; same type, move not re-hash
        }
        self.unique_filters.insert((table_id, packed), filter);
    }
}

#[cfg(test)]
mod unique_filter_tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn};

    fn u64_schema() -> SchemaDescriptor {
        // Single U64 PK column.
        SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0])
    }

    /// OPK leading-key span of a single U64 value — the form `key_bytes`
    /// produces for a U64-promoted index column (U64 OPK == big-endian). Used to
    /// build expected filter/accumulator keys in these unit tests.
    fn span_u64(v: u64) -> PkBuf {
        PkBuf::from_bytes(&v.to_be_bytes())
    }

    fn two_col_schema() -> SchemaDescriptor {
        // PK U64 at index 0, payload U64 at index 1.
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 1),
            ],
            &[0],
        )
    }

    /// Span-extraction spec for a unique index on `cols` of `schema`, promoted
    /// via `make_index_schema` exactly as production circuit registration does.
    fn test_spec(cols: &[u32], schema: &SchemaDescriptor) -> IndexKeySpec {
        let idx_schema = crate::schema::make_index_schema(cols, schema).unwrap();
        IndexKeySpec::new(cols, schema, &idx_schema)
    }

    fn make_row_batch(schema: SchemaDescriptor, rows: &[(u128, i64, u64, i64)]) -> Batch {
        // rows: (pk, weight, null_word, payload_col1_i64_value)
        let mut batch = Batch::with_schema(schema, rows.len().max(1));
        for &(pk, weight, null_word, payload_val) in rows {
            let lo = [payload_val];
            let hi = [0u64];
            let null_ptr: *const u8 = std::ptr::null();
            let ptrs = [null_ptr];
            let lens = [0u32];
            unsafe {
                batch.append_row_simple(pk, weight, null_word, &lo, &hi, &ptrs, &lens);
            }
        }
        batch
    }

    #[test]
    fn check_batch_64_payload_cols_full_null_word() {
        // 65 columns: 1 U64 PK + 64 nullable payload cols → npc == 64, the
        // shift-by-width boundary. Must not panic (debug) and must mark every
        // payload column null (release silent-miss guard).
        let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..64 {
            cols.push(SchemaColumn::new(type_code::I64, 1));
        }
        let schema = SchemaDescriptor::new(&cols, &[0]);
        assert_eq!(schema.num_payload_cols(), 64);

        let keys = vec![42u128];
        let batch = build_check_batch(&schema, &keys, type_code::U64, None);
        assert_eq!(batch.count, 1);

        let null_word = u64::from_le_bytes(batch.null_bmp_data()[0..8].try_into().unwrap());
        assert_eq!(null_word, u64::MAX, "all 64 payload columns must be marked null");
    }

    #[test]
    fn check_batch_nonleading_pk_probe_matches_stored_opk() {
        // Base table `(label STRING, id U64 PRIMARY KEY)` — PK at column 1.
        // The FK parent fast-path passes a base-table schema whose lone PK may
        // be declared at any column position; resolving the leading key column
        // from `columns[0]` (the STRING) would encode the probe at the wrong
        // type/width and mangle the existence check.
        let cols = vec![
            SchemaColumn::new(type_code::STRING, 0),
            SchemaColumn::new(type_code::U64, 0),
        ];
        let schema = SchemaDescriptor::new(&cols, &[1]);

        let keys = vec![42u128];
        let batch = build_check_batch(&schema, &keys, type_code::U64, None);

        // The parent stores id=42 as `encode_pk_column(42, U64)` = 42u64.to_be_bytes().
        let mut expected = [0u8; 8];
        gnitz_wire::encode_pk_column(&42u64.to_le_bytes(), type_code::U64, &mut expected);
        assert_eq!(
            batch.get_pk_bytes(0),
            &expected[..],
            "probe key must equal the stored OPK PK for a non-leading PK column"
        );
    }

    #[test]
    fn filter_insert_basic() {
        let mut f = UniqueFilter::new();
        f.insert(span_u64(1));
        f.insert(span_u64(2));
        assert!(f.values.contains(&span_u64(1)));
        assert!(f.values.contains(&span_u64(2)));
        assert!(!f.capped);
    }

    #[test]
    fn filter_cap_clears_values() {
        // Exceed a small parameterized cap, verify the filter flips to
        // capped and its values HashSet is cleared whole.
        let mut f = UniqueFilter::with_cap(8);
        for k in 0..10u64 {
            f.insert(span_u64(k));
            if f.capped {
                break;
            }
        }
        assert!(f.capped, "filter should be capped after exceeding the limit");
        assert!(f.values.is_empty(), "values cleared once capped");
        // Further inserts are no-ops.
        f.insert(span_u64(99999999));
        assert!(f.values.is_empty());
    }

    #[test]
    fn extract_into_filter_pk_col() {
        // Schema: PK-only U64. Test that the PK-column locator extracts PKs.
        let schema = u64_schema();
        let batch = make_row_batch(
            schema,
            &[
                (10, 1, 0, 0),
                (20, 1, 0, 0),
                (30, -1, 0, 0), // delete row — should be skipped
            ],
        );
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
        assert!(filter.values.contains(&span_u64(10)));
        assert!(filter.values.contains(&span_u64(20)));
        assert!(!filter.values.contains(&span_u64(30)), "negative weight skipped");
    }

    #[test]
    fn extract_into_filter_signed_pk_col_uses_native_key() {
        // Single signed I64 PK indexed by itself. Its leading index key now
        // promotes to a *signed* I64 (order-preserving), so the extracted span is
        // the I64-OPK (sign-bit-flipped) of the NATIVE value — for a self-indexed
        // I64 PK that equals the source's at-rest OPK bytes, since the source type
        // already matches the index type. The extraction must build the span from
        // the *native* key (`pk_native_key`) re-encoded at the promoted index type
        // — feeding the OPK-widened `get_pk` value would double-flip and seek a
        // wrong key, hiding genuine duplicates.
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::I64, 0)], &[0]);
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-5i64).to_le_bytes(), type_code::I64, &mut opk);
        let keys = [PkBuf::from_bytes(&opk)];
        let batch = build_check_batch_pkbuf(&schema, &keys, None);

        // The promoted leading key is I64, so the filter span is the I64-OPK of
        // the native value (= the at-rest OPK bytes here).
        let promoted_span = PkBuf::from_bytes(&opk);
        // The old U64-promotion image (BE of the two's-complement u64 bits, no
        // sign flip) is a DIFFERENT, non-order-preserving span the extractor must
        // NOT hold.
        let unsigned_span = PkBuf::from_bytes(&((-5i64) as u64).to_be_bytes());
        assert_ne!(
            promoted_span, unsigned_span,
            "signed I64 OPK (sign-flipped) differs from the unsigned image"
        );

        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
        assert!(
            filter.values.contains(&promoted_span),
            "filter holds the I64-promoted native span"
        );
        assert!(
            !filter.values.contains(&unsigned_span),
            "must not hold the non-order-preserving unsigned image"
        );
    }

    #[test]
    fn extract_into_filter_payload_col_skips_nulls() {
        // Schema: PK U64, payload U64 (nullable). Test extraction by col 1.
        let schema = two_col_schema();
        let batch = make_row_batch(
            schema,
            &[
                (1, 1, 0, 100), // payload=100, not null
                (2, 1, 1, 200), // null bit set → should be skipped
                (3, 1, 0, 300),
            ],
        );
        let mut filter = UniqueFilter::new();
        // Single payload column promoted to a U64 index column (8-byte span).
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[1], &schema));
        assert!(filter.values.contains(&span_u64(100)));
        assert!(!filter.values.contains(&span_u64(200)), "null values skipped");
        assert!(filter.values.contains(&span_u64(300)));
        assert_eq!(filter.values.len(), 2);
    }

    #[test]
    fn extract_into_filter_respects_capped() {
        let schema = u64_schema();
        let batch = make_row_batch(schema, &[(10, 1, 0, 0), (20, 1, 0, 0)]);
        let mut filter = UniqueFilter::new();
        filter.capped = true;
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
        assert!(filter.values.is_empty(), "no-op on capped filter");
    }

    /// Zero workers, null catalog, dummy SAL: the unique-filter methods
    /// touch only the `unique_filters` map.
    fn filter_dispatcher() -> MasterDispatcher {
        MasterDispatcher::new(
            0,
            Vec::new(),
            std::ptr::null_mut(),
            SalWriter::new(std::ptr::null_mut(), -1, 0, Vec::new()),
            W2mReceiver::new(Vec::new()),
        )
    }

    /// The full CREATE-time seed chain on a pre-flight that overflowed the
    /// cap: the accumulator's seed is empty-because-capped, not
    /// empty-because-the-table-is-empty, so the published filter must never
    /// prove a key absent (it must fall through to the broadcast).
    #[test]
    fn capped_preflight_seed_never_proves_absence() {
        let mut acc = PreflightAccumulator::new(3);
        for k in [10u64, 20, 30, 40] {
            assert!(acc.offer(span_u64(k)), "distinct keys never flip the verdict");
        }
        let (seed, capped) = acc.into_seed();
        assert!(capped, "cap + 1 distinct keys must report capped");
        let mut disp = filter_dispatcher();
        disp.unique_filter_seed(7, 0, seed, capped);
        assert!(
            !disp.unique_filter_all_absent(7, 0, &[span_u64(10)]),
            "a capped pre-flight seed must fall through to the broadcast",
        );
    }

    /// Exactly-at-cap pre-flight: the seed is the complete distinct set, so
    /// the published filter proves absence for fresh keys and reports seeded
    /// keys as possibly present.
    #[test]
    fn at_cap_preflight_seed_proves_absence() {
        let mut acc = PreflightAccumulator::new(3);
        for k in [10u64, 20, 30] {
            assert!(acc.offer(span_u64(k)));
        }
        let (seed, capped) = acc.into_seed();
        assert!(!capped, "exactly cap distinct keys must keep the full seed");
        let mut disp = filter_dispatcher();
        disp.unique_filter_seed(7, 0, seed, capped);
        assert!(
            disp.unique_filter_all_absent(7, 0, &[span_u64(40)]),
            "fresh key is provably absent"
        );
        assert!(
            !disp.unique_filter_all_absent(7, 0, &[span_u64(20)]),
            "seeded key falls through"
        );
    }

    /// A capped seed publishes a warm+capped filter whose entry exists in
    /// `unique_filters` — so `ensure_unique_filters_warm_async`'s
    /// `contains_key` skip applies and no key is ever proven absent.
    #[test]
    fn unique_filter_seed_capped_publishes_warm_capped_entry() {
        let mut disp = filter_dispatcher();
        disp.unique_filter_seed(7, 0, FxHashSet::default(), true);
        let filter = disp.unique_filters.get(&(7, 0)).expect("entry must exist");
        assert!(filter.warm);
        assert!(filter.capped);
        assert!(filter.values.is_empty());
        assert!(!disp.unique_filter_all_absent(7, 0, &[span_u64(12345)]));
    }

    // -- drain_index_scan unit tests ------------------------------------------
    //
    // Synthetic-train pattern: anonymous-mmap W2M rings (no fork), frames
    // pre-written via W2mWriter, the drain driven by a single manual poll with
    // a noop waker. Every fixture parks all continuation frames up front, so
    // a healthy drain never returns `Pending` — a `Pending` poll IS the
    // failure signal for a phantom-continuation regression.

    struct DrainFixture {
        rings: Vec<crate::test_support::SharedRegion>,
        reactor: crate::runtime::reactor::Reactor,
        receiver: crate::runtime::w2m::W2mReceiver,
        req_ids: [u64; crate::runtime::sal::MAX_WORKERS],
    }

    const DRAIN_RING_CAPACITY: usize = 64 * 1024;

    impl DrainFixture {
        fn new(n_workers: usize) -> (Self, Vec<crate::runtime::w2m::W2mWriter>) {
            use crate::runtime::w2m::{W2mReceiver, W2mWriter};
            use crate::runtime::w2m_ring;
            let mut rings = Vec::with_capacity(n_workers);
            let mut writers = Vec::with_capacity(n_workers);
            for _ in 0..n_workers {
                let region = crate::test_support::SharedRegion::new(DRAIN_RING_CAPACITY);
                let ptr = region.ptr();
                unsafe { w2m_ring::init_region_for_tests(ptr, DRAIN_RING_CAPACITY as u64) };
                writers.push(W2mWriter::new(ptr, DRAIN_RING_CAPACITY as u64));
                rings.push(region);
            }
            let reactor = crate::runtime::reactor::Reactor::new(16).expect("reactor");
            let mut req_ids = [0u64; crate::runtime::sal::MAX_WORKERS];
            for id in req_ids[..n_workers].iter_mut() {
                *id = reactor.alloc_scan_request_id();
            }
            let receiver = W2mReceiver::new(rings.iter().map(|r| r.ptr()).collect());
            (
                DrainFixture {
                    rings,
                    reactor,
                    receiver,
                    req_ids,
                },
                writers,
            )
        }

        /// Hand the first frame of every worker to the caller (what
        /// `dispatch_scan_fanout` returns) and park every later frame so
        /// `await_scan_slot` resolves without a live worker.
        fn initial_slots(&self) -> Vec<crate::runtime::w2m::W2mSlot> {
            let n = self.rings.len();
            let mut slots = Vec::with_capacity(n);
            for w in 0..n {
                slots.push(self.receiver.try_read_slot(w).expect("first frame"));
                while let Some(cont) = self.receiver.try_read_slot(w) {
                    self.reactor.test_route_scan_slot(cont);
                }
            }
            slots
        }

        fn teardown(self, lease: ScanLease) {
            // Drop the lease before the rings: its Drop purges scan_parked,
            // which would drop any still-queued W2mSlot borrowing the
            // soon-to-be-unmapped region.
            drop(lease);
            drop(self.reactor);
            drop(self.receiver);
            // `self.rings` drops last, unmapping the regions.
        }
    }

    /// Encode one reply frame onto a test ring, mirroring the worker's reply
    /// shapes: `flags` carries the train flags (0 = single-frame reply).
    fn write_test_frame(
        writer: &crate::runtime::w2m::W2mWriter,
        req: u32,
        flags: u64,
        status: u32,
        error_msg: &[u8],
        schema: Option<&SchemaDescriptor>,
        batch: Option<&Batch>,
    ) {
        use crate::runtime::wire::{self as ipc};
        let sz = ipc::wire_size(status, error_msg, schema, None, batch, None, &[]);
        writer.send_encoded(sz, req, |buf| {
            ipc::encode_wire_into_ipc(
                buf,
                0,
                1,
                0,
                flags,
                0u128,
                0,
                0,
                status,
                error_msg,
                schema,
                None,
                batch,
                None,
                &[],
            );
        });
    }

    /// Poll a future exactly once with a noop waker; `None` on `Pending`.
    fn try_poll_once<T>(fut: impl std::future::Future<Output = T>) -> Option<T> {
        use std::task::{Context, Poll, Waker};
        let mut cx = Context::from_waker(Waker::noop());
        let mut fut = Box::pin(fut);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(r) => Some(r),
            Poll::Pending => None,
        }
    }

    /// Drive a future to completion in exactly one poll, panicking on Pending.
    fn poll_once<T>(fut: impl std::future::Future<Output = T>) -> T {
        try_poll_once(fut).unwrap_or_else(|| {
            panic!(
                "future did not complete in one poll: the drain awaited a \
             frame that is not (and will never be) parked"
            )
        })
    }

    /// A frame must still be parked for `req`: the drain returned without
    /// consuming it (the lease drop, not the drain, owns its disposal).
    fn assert_frame_still_parked(reactor: &crate::runtime::reactor::Reactor, req: u32) {
        assert!(
            try_poll_once(reactor.await_scan_slot(req)).is_some(),
            "expected an undrained parked frame — the drain consumed frames \
             past its early-return point"
        );
    }

    /// A worker fault must surface as an IMMEDIATE `Err` in a single poll —
    /// without draining the survivor's train (the caller's `ScanLease` drop
    /// owns the discard of undrained frames).
    ///
    /// Layout: worker 0 emits one `STATUS_ERROR` frame (flags 0, like
    /// `send_error`); worker 1 is healthy with a two-frame train whose
    /// `FLAG_SCAN_LAST` continuation is parked.
    ///  - If `has_more` were keyed on `FLAG_SCAN_LAST` alone (not
    ///    status-gated), worker 0's flags-0 error frame would read as "more
    ///    coming" and the first poll would return `Pending` — caught as a
    ///    failed assert instead of an infinite hang.
    ///  - If the drain still deferred the error (the pre-early-return shape),
    ///    worker 1's parked continuation would be consumed — caught by the
    ///    still-parked assert.
    #[test]
    fn drain_index_scan_errs_immediately_on_fault_frame() {
        use crate::runtime::wire::{STATUS_ERROR, STATUS_OK};

        let (fx, writers) = DrainFixture::new(2);
        let w0_req = fx.req_ids[0] as u32;
        let w1_req = fx.req_ids[1] as u32;

        write_test_frame(&writers[0], w0_req, 0, STATUS_ERROR, b"boom", None, None);
        write_test_frame(&writers[1], w1_req, FLAG_CONTINUATION, STATUS_OK, b"", None, None);
        write_test_frame(
            &writers[1],
            w1_req,
            FLAG_CONTINUATION | FLAG_SCAN_LAST,
            STATUS_OK,
            b"",
            None,
            None,
        );

        let lease = fx.reactor.scan_lease(&[w0_req, w1_req]);
        let slots = fx.initial_slots();

        // The frames carry no schema block, so `expected` is never consulted.
        let result = poll_once(drain_index_scan(
            slots,
            &fx.req_ids,
            &fx.reactor,
            "scan",
            &two_col_schema(),
            |_, _| Ok(()),
        ));
        let err = result.expect_err("worker fault must surface as Err");
        assert!(err.contains("worker 0"), "error names the faulted worker: {err}");
        assert!(err.contains("boom"), "error carries the worker message: {err}");

        // Early return: worker 1's continuation must still be parked.
        assert_frame_still_parked(&fx.reactor, w1_req);

        fx.teardown(lease);
    }

    /// Multi-frame trains from one worker merge with a single-frame (flag-free,
    /// length-1 train) reply from another: the sink sees every row, the
    /// continuation decodes against the schema hint saved from the first
    /// frame, and the flag-free frame terminates its train (the new
    /// `parse_train_header` contract — under the old "no FLAG_SCAN_LAST ⇒
    /// more" rule this test would hang at `Pending`).
    #[test]
    fn drain_index_scan_merges_chunked_and_single_frame_trains() {
        use crate::runtime::wire::STATUS_OK;

        let schema = two_col_schema();
        let chunk_a = make_row_batch(schema, &[(1, 1, 0, 10), (2, 1, 0, 20)]);
        let chunk_b = make_row_batch(schema, &[(3, 1, 0, 30)]);
        let single = make_row_batch(schema, &[(4, 1, 0, 40), (5, -1, 0, 50)]);

        let (fx, writers) = DrainFixture::new(2);
        let w0_req = fx.req_ids[0] as u32;
        let w1_req = fx.req_ids[1] as u32;

        // Worker 0: chunked train — schema on the first frame only.
        write_test_frame(
            &writers[0],
            w0_req,
            FLAG_CONTINUATION,
            STATUS_OK,
            b"",
            Some(&schema),
            Some(&chunk_a),
        );
        write_test_frame(
            &writers[0],
            w0_req,
            FLAG_CONTINUATION | FLAG_SCAN_LAST,
            STATUS_OK,
            b"",
            None,
            Some(&chunk_b),
        );
        // Worker 1: single-frame reply, no train flags (send_response shape).
        write_test_frame(&writers[1], w1_req, 0, STATUS_OK, b"", Some(&schema), Some(&single));

        let lease = fx.reactor.scan_lease(&[w0_req, w1_req]);
        let slots = fx.initial_slots();

        let mut rows: Vec<(u128, i64)> = Vec::new();
        let result = poll_once(drain_index_scan(
            slots,
            &fx.req_ids,
            &fx.reactor,
            "seek_by_index",
            &schema,
            |mb, frame_len| {
                assert!(frame_len > 0, "sink receives the raw frame byte length");
                for i in 0..mb.count {
                    rows.push((mb.get_pk(i), mb.get_weight(i)));
                }
                Ok(())
            },
        ));
        result.expect("healthy trains must drain cleanly");
        assert_eq!(
            rows,
            vec![(1, 1), (2, 1), (3, 1), (4, 1), (5, -1)],
            "every frame's rows reach the sink in worker order, weights verbatim",
        );

        fx.teardown(lease);
    }

    /// A first-frame schema that does not match `expected` must error before
    /// any row reaches the sink: the batch append helpers do not validate
    /// shape, so a DDL-lagged worker reply would otherwise be mis-decoded.
    #[test]
    fn drain_index_scan_rejects_first_frame_schema_mismatch() {
        use crate::runtime::wire::STATUS_OK;

        let wire_schema = two_col_schema();
        let expected = u64_schema(); // different column count
        let batch = make_row_batch(wire_schema, &[(1, 1, 0, 10)]);

        let (fx, writers) = DrainFixture::new(1);
        let w0_req = fx.req_ids[0] as u32;
        write_test_frame(&writers[0], w0_req, 0, STATUS_OK, b"", Some(&wire_schema), Some(&batch));

        let lease = fx.reactor.scan_lease(&[w0_req]);
        let slots = fx.initial_slots();

        let mut sink_calls = 0usize;
        let result = poll_once(drain_index_scan(
            slots,
            &fx.req_ids,
            &fx.reactor,
            "gather",
            &expected,
            |_, _| {
                sink_calls += 1;
                Ok(())
            },
        ));
        let err = result.expect_err("schema mismatch must surface as Err");
        assert!(err.contains("Schema mismatch"), "error names the mismatch: {err}");
        assert!(err.contains("worker 0"), "error names the worker: {err}");
        assert_eq!(sink_calls, 0, "no row may reach the sink under a wrong schema");

        fx.teardown(lease);
    }

    /// A sink `Err` (the reply-cap path in `fan_out_index_collect_common`)
    /// aborts the drain immediately; the train's parked continuation stays
    /// parked for the lease drop to discard.
    #[test]
    fn drain_index_scan_sink_error_aborts_drain() {
        use crate::runtime::wire::STATUS_OK;

        let schema = two_col_schema();
        let chunk = make_row_batch(schema, &[(1, 1, 0, 10)]);

        let (fx, writers) = DrainFixture::new(1);
        let w0_req = fx.req_ids[0] as u32;
        write_test_frame(
            &writers[0],
            w0_req,
            FLAG_CONTINUATION,
            STATUS_OK,
            b"",
            Some(&schema),
            Some(&chunk),
        );
        write_test_frame(
            &writers[0],
            w0_req,
            FLAG_CONTINUATION | FLAG_SCAN_LAST,
            STATUS_OK,
            b"",
            None,
            Some(&chunk),
        );

        let lease = fx.reactor.scan_lease(&[w0_req]);
        let slots = fx.initial_slots();

        let result = poll_once(drain_index_scan(
            slots,
            &fx.req_ids,
            &fx.reactor,
            "seek_by_index",
            &schema,
            |_, _| Err("seek_by_index: result exceeds the reply cap".to_string()),
        ));
        let err = result.expect_err("sink error must abort the drain");
        assert!(err.contains("reply cap"), "sink error surfaces verbatim: {err}");

        // The terminal continuation was never consumed.
        assert_frame_still_parked(&fx.reactor, w0_req);

        fx.teardown(lease);
    }

    #[test]
    fn format_pk_value_uuid_full_128_bits() {
        // A UUID with non-zero high 64 bits. The lower 64 bits alone would be
        // misread as a different (truncated) value.
        let uuid: u128 = 0x550e8400_e29b_41d4_a716_446655440000u128;
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::UUID, 0)], &[0]);

        let s = format_pk_value_bytes(&uuid.to_be_bytes(), &schema);
        // Must not be the lower-64 truncation (11975073520896 or similar).
        let truncated = format!("{}", uuid as u64);
        assert_ne!(s, truncated, "UUID must not be formatted as truncated u64");
        // Must contain the high-word hex digits.
        assert!(s.contains("550e8400"), "UUID formatting must include high bits");
    }

    #[test]
    fn format_pk_value_uuid_two_distinct_uuids_differ() {
        // Two UUIDs that differ only in the high 64 bits must produce different strings.
        let uuid_a: u128 = 0x11111111_0000_0000_0000_000000000001u128;
        let uuid_b: u128 = 0x22222222_0000_0000_0000_000000000001u128;
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::UUID, 0)], &[0]);

        let sa = format_pk_value_bytes(&uuid_a.to_be_bytes(), &schema);
        let sb = format_pk_value_bytes(&uuid_b.to_be_bytes(), &schema);
        assert_ne!(sa, sb, "UUIDs differing in high bits must format differently");
    }

    fn compound_pk_bytes(parts: &[&[u8]]) -> PkBuf {
        let mut v = Vec::new();
        for p in parts {
            v.extend_from_slice(p);
        }
        PkBuf::from_bytes(&v)
    }

    #[test]
    fn format_pk_value_bytes_wide_compound_u64x3() {
        // Three U64 columns = 24-byte PK: too wide for a u128, exercising the
        // byte-form renderer where a u128 key would truncate.
        // OPK for unsigned U64 is big-endian.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1, 2],
        );
        assert!(schema.pk_stride() > 16);
        let pk = compound_pk_bytes(&[&7u64.to_be_bytes(), &8u64.to_be_bytes(), &9u64.to_be_bytes()]);
        assert_eq!(format_pk_value_bytes(pk.pk_bytes(), &schema), "7, 8, 9");
    }

    #[test]
    fn extract_into_filter_compound_pk_extracts_single_column() {
        // (A U32, B U32) both PK, unique index on A. Two rows share A=5 but
        // differ in B. Pre-fix get_pk(i) returned the packed (A,B) key, so the
        // filter held two distinct values; the fix slices out A only.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[0, 1],
        );
        assert_eq!(schema.pk_stride(), 8);
        // PK region is OPK; for unsigned U32 that is big-endian. extract_into_filter
        // decodes OPK→native via pk_native_key, so the fixture must be OPK.
        let keys = [
            compound_pk_bytes(&[&5u32.to_be_bytes(), &1u32.to_be_bytes()]),
            compound_pk_bytes(&[&5u32.to_be_bytes(), &2u32.to_be_bytes()]),
        ];
        let batch = build_check_batch_pkbuf(&schema, &keys, None);
        let mut filter = UniqueFilter::new();
        // Index on a U32 column promotes to a U64 (8-byte) index column.
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[0], &schema));
        assert!(filter.values.contains(&span_u64(5)), "filter holds column A's value");
        assert_eq!(filter.values.len(), 1, "the shared A=5 collapses to one entry");
    }

    #[test]
    fn extract_into_filter_compound_pk_second_column_offset() {
        // Unique index on B (the second PK column at byte offset 4). Confirms
        // the locator slices the right column out of the packed key.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[0, 1],
        );
        // OPK PK region: unsigned U32 columns are stored big-endian.
        let keys = [
            compound_pk_bytes(&[&5u32.to_be_bytes(), &11u32.to_be_bytes()]),
            compound_pk_bytes(&[&6u32.to_be_bytes(), &22u32.to_be_bytes()]),
        ];
        let batch = build_check_batch_pkbuf(&schema, &keys, None);
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[1], &schema));
        assert!(filter.values.contains(&span_u64(11)));
        assert!(filter.values.contains(&span_u64(22)));
        assert_eq!(filter.values.len(), 2);
    }
}
