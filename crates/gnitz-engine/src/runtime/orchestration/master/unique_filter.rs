//! Master-side unique-index filter cache: the `UniqueFilter` / `WarmupGuard` /
//! `UniqueIndexDesc` types, `extract_into_filter`, and the `MasterDispatcher`
//! methods that warm, query, seed, and invalidate the per-`(table,
//! packed_cols)` filters (the preflight seed path shares the types).

use super::*;

#[cfg(test)]
use super::preflight::build_check_batch_pk_bytes;

// For each `(table_id, packed_cols)` we keep a set of the OPK spans known to
// exist in that unique index. The U-SEC rule (`txn_check_unique_indices`)
// consults it before building a broadcast: if every new span is definitely
// absent, that index's broadcast is skipped entirely.
//
// Correctness invariants:
//   1. A span is added only AFTER fsync confirms the owning batch is durable,
//      so an in-flight insert never creates a false "present" entry.
//   2. On any flush error the affected table's filters are dropped from the map
//      and lazily re-warmed from authoritative worker state.
//   3. Warmup is lazy: the first query for an unwarmed key scans the table
//      (`ensure_unique_filters_warm`) and extracts the indexed spans.
//   4. Deletes are NOT removed — they leave stale "possibly present" entries
//      that cause harmless fall-through broadcasts. Accumulation is bounded by
//      `UNIQUE_FILTER_CAP`: past it the filter caps and disables itself until
//      invalidated.
//   5. The master event loop is single-threaded, so query, warmup, and ingest
//      cannot race.

/// Maximum number of spans tracked per `(table_id, packed_cols)` filter; a
/// filter that would exceed it disables itself. At the cap a `PkBuf` key
/// (81 bytes) costs ≈170 MB per maxed filter — bounded, and only reached by a
/// table holding 1M distinct unique values. Lowering the cap shrinks the
/// broadcast-skip reach (every unique index past it reverts to
/// always-broadcast on the insert hot path), so it is the lever for a
/// memory-constrained deployment.
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
/// dropped before it completes.
pub(super) struct WarmupGuard<'d> {
    pub(super) disp: &'d MasterDispatcher,
    pub(super) table_id: i64,
    /// `pack_pk_cols(col_indices)` per cold filter — the `unique_filters` map
    /// key, so the drop handler removes exactly the entries this warmup created.
    pub(super) keys: Vec<u64>,
    pub(super) disarmed: bool,
}

impl Drop for WarmupGuard<'_> {
    fn drop(&mut self) {
        if !self.disarmed {
            for &packed in &self.keys {
                self.disp.unique_filter_remove(self.table_id, packed);
            }
        }
    }
}

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
    let mut keybuf = PkBuf::zeroed(0);
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
    /// circuit, the shape `extract_into_filter` consumes. Empty when the table
    /// has no unique index, and for an unknown table: `index_circuits` answers
    /// those with an empty slice.
    ///
    /// One pass over the circuit list; the span plan is the circuit's
    /// precomputed `key_spec`. Uniqueness is filtered on the LIVE flag —
    /// promotion/demotion flips it without rebuilding the spec.
    fn unique_index_descriptors(&self, table_id: i64) -> Vec<UniqueIndexDesc> {
        self.cat()
            .index_circuits(table_id)
            .iter()
            .filter(|ic| ic.is_unique)
            .map(|ic| UniqueIndexDesc {
                packed: gnitz_wire::pack_pk_cols(ic.col_indices.as_slice()),
                spec: ic.key_spec,
            })
            .collect()
    }

    /// True if every key in `keys` is definitely absent from the filter
    /// for `(table_id, packed)`. Returns false if the filter is capped,
    /// not warm (caller is expected to warm it first), or contains any
    /// key. On false, caller must fall through to the occupancy broadcast.
    pub(super) fn unique_filter_all_absent(
        &self,
        table_id: i64,
        packed: u64,
        mut keys: impl Iterator<Item = PkBuf>,
    ) -> bool {
        let filters = self.unique_filters.borrow();
        let filter = match filters.get(&(table_id, packed)) {
            Some(f) => f,
            None => return false,
        };
        if !filter.warm || filter.capped {
            return false;
        }
        keys.all(|k| !filter.values.contains(&k))
    }

    /// Record every unique-index value from a successfully-flushed
    /// `batch` on `table_id` into the corresponding filters. No-op for
    /// filters that are not yet warm (warmup will pick them up), and
    /// for index circuits that are not unique.
    pub(crate) fn unique_filter_ingest_batch(&self, table_id: i64, batch: &Batch) {
        let descs = self.unique_index_descriptors(table_id);
        if descs.is_empty() {
            return;
        }
        let mb = batch.as_mem_batch();
        let mut filters = self.unique_filters.borrow_mut();
        for d in descs {
            let Some(filter) = filters.get_mut(&(table_id, d.packed)) else {
                continue; // not warm — warmup will pick this up
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
    pub(crate) fn unique_filter_invalidate_table(&self, table_id: i64) {
        self.unique_filters.borrow_mut().retain(|&(t, _), _| t != table_id);
        // The check-batch pool is a pure allocation cache; drop every slot of the
        // dropped table so it doesn't leak across DDL cycles.
        self.check_batch_pool.borrow_mut().retain(|&(t, _), _| t != table_id);
    }

    /// Remove the unique-filter entry for a single (owner_table_id, packed)
    /// pair. `packed` is the `pack_pk_cols(col_indices)` / `IDXTAB_PAY_SOURCE_COLS`
    /// value. Called on DROP INDEX so subsequent INSERTs re-trigger warmup for
    /// the now-absent index while leaving unrelated filters on the same table; a
    /// non-existent key (e.g. a non-unique FK index) is a harmless no-op.
    pub(crate) fn unique_filter_remove(&self, owner_id: i64, packed: u64) {
        self.unique_filters.borrow_mut().remove(&(owner_id, packed));
    }

    /// Populate every not-yet-warm unique filter on `table_id` from a full scan
    /// of the committed table, feeding each worker's reply frames straight into
    /// the filters. Nothing is concatenated master-side: on a table of tens of
    /// millions of rows a merged `Batch` would peak at the whole scan size.
    pub(super) async fn ensure_unique_filters_warm(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
        table_id: i64,
    ) -> Result<(), String> {
        let (missing, mut guard): (Vec<UniqueIndexDesc>, WarmupGuard) = {
            let mut filters = disp.unique_filters.borrow_mut();
            let missing: Vec<UniqueIndexDesc> = disp
                .unique_index_descriptors(table_id)
                .into_iter()
                .filter(|d| !filters.contains_key(&(table_id, d.packed)))
                .collect();
            if missing.is_empty() {
                return Ok(());
            }
            for d in &missing {
                filters.insert((table_id, d.packed), UniqueFilter::new());
            }
            let guard = WarmupGuard {
                disp,
                table_id,
                keys: missing.iter().map(|d| d.packed).collect(),
                disarmed: false,
            };
            (missing, guard)
        };
        let schema = disp.schema_desc_for(table_id);

        // Single-source a REPLICATED table's warmup scan: a fan-out would
        // stream `nw` copies of the same rows back to build one filter (a set —
        // dedup keeps it correct, but the extra `nw - 1` full-table scans are
        // pure waste).
        let unicast = replicated_unicast(disp, table_id);

        // `_lease` held across the full continuation drain below; its workers
        // stream multi-frame trains, and on an early error return (or a
        // mid-scan cancellation) the lease drop discards every undrained
        // frame at the ring boundary.
        let (slots, req_ids, _lease) =
            dispatch_scan_fanout(disp, reactor, sal_excl, unicast, |disp, req_ids, unicast| {
                disp.write_scan_group(table_id, 0, 0, req_ids, unicast, 0, &[])
            })
            .await?;

        // Drain every worker's continuation-frame train into the cold filters.
        // `drain_index_scan` owns the early-return error contract (the lease
        // drop above discards any undrained frames at the ring boundary), the
        // schema guard against DDL-lagged worker replies, the zero-copy
        // `MemBatch` lifetime, and the continuation-schema-hint handling.
        // On failure (worker crash mid-scan or cancellation) the guard is left
        // armed, so its Drop removes the cold entries and the next validation
        // retries warmup from scratch.
        drain_index_scan(slots, &req_ids, reactor, "scan", &schema, |mb, _| {
            let mut filters = disp.unique_filters.borrow_mut();
            for d in &missing {
                if let Some(filter) = filters.get_mut(&(table_id, d.packed)) {
                    if !filter.capped {
                        extract_into_filter(filter, mb, &d.spec);
                    }
                }
            }
            Ok(())
        })
        .await?;

        // Fully populated → mark warm so the broadcast-skip shortcut may trust
        // them, and disarm the guard so its Drop leaves them in place.
        let mut filters = disp.unique_filters.borrow_mut();
        for d in &missing {
            if let Some(f) = filters.get_mut(&(table_id, d.packed)) {
                f.warm = true;
            }
        }
        guard.disarmed = true;
        Ok(())
    }

    /// Seed the `(table_id, col_idx)` filter from the CREATE-time pre-flight,
    /// captured under the catalog write lock. Marks it warm so the first
    /// INSERT skips `ensure_unique_filters_warm`. `capped = true` (the
    /// accumulator overflowed and cleared its set whole — `seen` arrives
    /// empty) publishes a warm+capped filter: `unique_filter_all_absent` then
    /// always falls through to the broadcast — the same steady state the lazy
    /// warmup converges to, without paying a redundant full-cluster scan on
    /// the first INSERT. Symmetric counterpart of `unique_filter_remove`.
    pub(crate) fn unique_filter_seed(&self, table_id: i64, packed: u64, seen: FxHashSet<PkBuf>, capped: bool) {
        let mut filter = UniqueFilter::new();
        filter.warm = true; // pre-flight scanned every worker under the write lock
        if capped {
            filter.capped = true;
        } else {
            filter.values = seen; // exact distinct set; same type, move not re-hash
        }
        self.unique_filters.borrow_mut().insert((table_id, packed), filter);
    }
}

#[cfg(test)]
mod unique_filter_tests {
    use super::super::fixtures::{compound_pk_bytes, make_row_batch, two_col_schema, u64_schema};
    use super::*;
    use crate::schema::{type_code, SchemaColumn};

    /// OPK leading-key span of a single U64 value — the form `key_bytes`
    /// produces for a U64-promoted index column (U64 OPK == big-endian). Used to
    /// build expected filter/accumulator keys in these unit tests.
    fn span_u64(v: u64) -> PkBuf {
        PkBuf::from_bytes(&v.to_be_bytes())
    }

    /// Span-extraction spec for a unique index on `cols` of `schema`, promoted
    /// via `make_index_schema` exactly as production circuit registration does.
    fn test_spec(cols: &[u32], schema: &SchemaDescriptor) -> IndexKeySpec {
        let idx_schema = crate::schema::make_index_schema(cols, schema).unwrap();
        IndexKeySpec::new(cols, schema, &idx_schema)
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
        let batch = build_check_batch_pk_bytes(&schema, keys.iter().map(|k| k.pk_bytes()), None);

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
            Rc::new(W2mReceiver::new(Vec::new())),
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
        let disp = filter_dispatcher();
        disp.unique_filter_seed(7, 0, seed, capped);
        assert!(
            !disp.unique_filter_all_absent(7, 0, [span_u64(10)].into_iter()),
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
        let disp = filter_dispatcher();
        disp.unique_filter_seed(7, 0, seed, capped);
        assert!(
            disp.unique_filter_all_absent(7, 0, [span_u64(40)].into_iter()),
            "fresh key is provably absent"
        );
        assert!(
            !disp.unique_filter_all_absent(7, 0, [span_u64(20)].into_iter()),
            "seeded key falls through"
        );
    }

    /// A capped seed publishes a warm+capped filter whose entry exists in
    /// `unique_filters` — so `ensure_unique_filters_warm`'s
    /// `contains_key` skip applies and no key is ever proven absent.
    #[test]
    fn unique_filter_seed_capped_publishes_warm_capped_entry() {
        let disp = filter_dispatcher();
        disp.unique_filter_seed(7, 0, FxHashSet::default(), true);
        let filters = disp.unique_filters.borrow();
        let filter = filters.get(&(7, 0)).expect("entry must exist");
        assert!(filter.warm);
        assert!(filter.capped);
        assert!(filter.values.is_empty());
        assert!(!disp.unique_filter_all_absent(7, 0, [span_u64(12345)].into_iter()));
    }

    #[test]
    fn extract_into_filter_compound_pk_extracts_single_column() {
        // (A U32, B U32) both PK, unique index on A. Two rows share A=5 but
        // differ in B, so the extraction must slice out A alone — the packed
        // (A,B) key would make them two distinct filter entries.
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
        let batch = build_check_batch_pk_bytes(&schema, keys.iter().map(|k| k.pk_bytes()), None);
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
        let batch = build_check_batch_pk_bytes(&schema, keys.iter().map(|k| k.pk_bytes()), None);
        let mut filter = UniqueFilter::new();
        extract_into_filter(&mut filter, &batch.as_mem_batch(), &test_spec(&[1], &schema));
        assert!(filter.values.contains(&span_u64(11)));
        assert!(filter.values.contains(&span_u64(22)));
        assert_eq!(filter.values.len(), 2);
    }
}
