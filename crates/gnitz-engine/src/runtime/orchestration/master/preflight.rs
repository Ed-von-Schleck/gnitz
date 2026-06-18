//! Distributed PK / unique-index preflight validation and violation
//! formatting: the `validate_*` pipelines, the gather/merge key streams
//! (`PreflightKeyStream` / `PreflightAccumulator` / `merge_index_scan`), and
//! the error renderers.

use super::*;

/// Per-worker state for one sorted-key stream in `merge_index_scan`.
///
/// `mb` is a zero-copy view into `slot`'s ring bytes with its lifetime
/// erased: it is valid only while `slot` is held, so `attach_frame` clears
/// `mb` before dropping or replacing `slot`, and nothing reads `mb` after the
/// stream's slot is released.
struct PreflightKeyStream {
    /// Worker index (error attribution) and scan request id (frame pulls).
    w: usize,
    req_id: u64,
    /// Ring slot backing `mb`. Holding it parks the frame's ring bytes.
    slot: Option<W2mSlot>,
    /// Zero-copy view of the current frame's key batch (`None` for an empty
    /// terminal frame or after a decode error).
    mb: Option<crate::storage::MemBatch<'static>>,
    /// Cursor into `mb`.
    row: usize,
    /// Current frame is non-terminal: status 0 and no FLAG_SCAN_LAST.
    has_more: bool,
}

impl PreflightKeyStream {
    fn new(w: usize, req_id: u64) -> Self {
        PreflightKeyStream {
            w,
            req_id,
            slot: None,
            mb: None,
            row: 0,
            has_more: false,
        }
    }

    /// Install `slot` as the current frame, decoding its keys zero-copy.
    /// A fault/corrupt/undecodable frame is an immediate `Err` — the caller
    /// unwinds to the `ScanLease` drop, which discards the undrained trains.
    fn attach_frame(&mut self, slot: W2mSlot, frame_schema: &SchemaDescriptor) -> Result<(), String> {
        // The view must die before its backing slot.
        self.mb = None;
        self.slot = None;
        self.row = 0;
        let (ctrl, has_more) = parse_train_header(&slot, self.w, "unique pre-flight")?;
        self.has_more = has_more;
        // Every frame decodes against the shared compile-time wire schema
        // (version 0): the first frame's embedded schema block equals it by
        // construction (`send_unique_preflight_keys`), and continuation
        // frames carry no schema and resolve through the hint.
        let ctrl_size = ctrl.block_size;
        let schema_hint = Some(SchemaWithVersion {
            descriptor: frame_schema,
            version: 0,
        });
        // SAFETY: the slice points into the W2M ring slot that `slot` pins
        // until dropped. `self.mb` borrows it, and every path that drops or
        // replaces `self.slot` clears `self.mb` first (top of this fn and
        // `next_key`'s terminal arm), so the view never outlives the pin. The
        // lifetime erasure exists only because slot and view live in the same
        // struct.
        let bytes: &'static [u8] = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(slot.bytes()) };
        let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(bytes, ctrl_size, ctrl, schema_hint)
            .map_err(|e| scan_decode_err(self.w, e))?;
        self.mb = zc.data_batch;
        self.slot = Some(slot);
        Ok(())
    }

    /// Yield this worker's next span, pulling continuation frames on demand.
    /// Returns `Ok(None)` once the train is terminal. The whole PK region IS
    /// the OPK leading-key span (`pk_stride == idx_key_size`), read verbatim
    /// as a `PkBuf` — no column-0 decode, so the wire is byte-transparent for
    /// a span of any width.
    async fn next_key(
        &mut self,
        frame_schema: &SchemaDescriptor,
        reactor: &crate::runtime::reactor::Reactor,
    ) -> Result<Option<PkBuf>, String> {
        loop {
            if let Some(mb) = &self.mb {
                if self.row < mb.count {
                    let key = PkBuf::from_bytes(mb.get_pk_bytes(self.row));
                    self.row += 1;
                    return Ok(Some(key));
                }
            }
            if !self.has_more {
                self.mb = None;
                self.slot = None;
                return Ok(None);
            }
            let slot = reactor.await_scan_slot(self.req_id as u32).await;
            self.attach_frame(slot, frame_schema)?;
        }
    }
}

/// Per-key accounting for the pre-flight merge: duplicate verdict + inline
/// seed collection, fed keys in globally-sorted merge order. Split from the
/// frame-pulling loop so the verdict and the all-or-nothing seed rule are
/// directly testable with a small cap.
pub(crate) struct PreflightAccumulator {
    prev: Option<PkBuf>,
    pub(crate) duplicate: bool,
    /// Seed collection reuses `UniqueFilter`'s cap discipline: on overflow
    /// `insert` clears the set WHOLE and disables itself, so the seed is
    /// complete-or-empty, never truncated — a truncated seed would publish a
    /// warm but incomplete filter whose "proven absent" answers would let a
    /// genuine duplicate skip the INSERT broadcast. Every span reaching
    /// `insert` is distinct (spans arrive sorted, so duplicates are adjacent
    /// and stop at the `prev` check).
    filter: UniqueFilter,
}

impl PreflightAccumulator {
    pub(crate) fn new(cap: usize) -> Self {
        PreflightAccumulator {
            prev: None,
            duplicate: false,
            filter: UniqueFilter::with_cap(cap),
        }
    }

    /// Offer the next span in globally-sorted merge order. Returns `false`
    /// once a duplicate is found — the verdict is monotonic, so the caller
    /// stops merging useful spans (but still drains every worker's train).
    /// Span equality (byte-equal ⟺ value-equal) replaces the old `u128` equality.
    pub(crate) fn offer(&mut self, key: PkBuf) -> bool {
        if self.duplicate {
            return false;
        }
        if self.prev == Some(key) {
            self.duplicate = true;
            return false;
        }
        self.prev = Some(key);
        self.filter.insert(key);
        true
    }

    /// The complete distinct span set plus the capped verdict. `capped = true`
    /// means the set overflowed and was cleared whole — the caller must
    /// publish a capped (always-broadcast) filter, never a warm-empty one.
    pub(crate) fn into_seed(self) -> (FxHashSet<PkBuf>, bool) {
        (self.filter.values, self.filter.capped)
    }
}

/// Streaming k-way merge over the per-worker SORTED key streams of a unique
/// pre-flight fan-out. Master memory is `O(num_workers)`: the heap, one
/// cursor and one live zero-copy frame view per worker; frame bytes the merge
/// has not reached yet stay in the fixed per-worker W2M shared-memory rings.
///
/// One adjacent-equal check (`prev == popped`) catches BOTH duplicate
/// classes: two equal keys from one worker are adjacent in its sorted run and
/// pop consecutively (within-partition), and the same value held by two
/// workers surfaces as two equal heads (cross-partition). Takes no catalog
/// lock — it only compares OPK spans (`PkBuf`) read verbatim from the frames'
/// PK regions against `frame_schema`.
///
/// Returns on the FIRST error (fault, corrupt or undecodable frame) and on
/// the first duplicate (the verdict is monotonic) — as in `drain_index_scan`,
/// without draining the remaining trains: the caller holds the `ScanLease` to
/// end of scope, so on return or cancellation the lease drop deregisters the
/// req_ids and `route_scan_slot` discards every undrained frame at the ring
/// boundary — a still-streaming worker never wedges in `send_encoded`.
async fn merge_index_scan(
    slots: Vec<W2mSlot>,
    req_ids: &[u64; crate::runtime::sal::MAX_WORKERS],
    reactor: &crate::runtime::reactor::Reactor,
    frame_schema: &SchemaDescriptor,
) -> Result<PreflightAccumulator, String> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    // Both sides agree on the frame layout by construction: `frame_schema` is
    // `unique_preflight_wire_schema` of the same idx_schema the worker encodes
    // against, so no per-stream schema capture from the first frame is needed.
    // The PK region IS the OPK leading-key span; the merge reads it verbatim
    // as a `PkBuf`.
    let nw = slots.len();
    let mut streams: Vec<PreflightKeyStream> = Vec::with_capacity(nw);

    // Seed each stream with its first frame and prime the heap with each
    // worker's minimum (next_key pulls continuations if a first frame is
    // empty but non-terminal). Ordering the heap by (span, worker) — byte-
    // lexicographic via `PkBuf: Ord` — pops equal spans adjacently regardless
    // of which workers hold them, the merge order replacing numeric `u128`.
    let mut heap: BinaryHeap<Reverse<(PkBuf, usize)>> = BinaryHeap::with_capacity(nw);
    for (w, slot) in slots.into_iter().enumerate() {
        let mut s = PreflightKeyStream::new(w, req_ids[w]);
        s.attach_frame(slot, frame_schema)?;
        if let Some(key) = s.next_key(frame_schema, reactor).await? {
            heap.push(Reverse((key, w)));
        }
        streams.push(s);
    }

    let mut acc = PreflightAccumulator::new(UNIQUE_FILTER_CAP);
    while let Some(Reverse((key, w))) = heap.pop() {
        if !acc.offer(key) {
            break;
        } // first duplicate is conclusive
        if let Some(next) = streams[w].next_key(frame_schema, reactor).await? {
            heap.push(Reverse((next, w)));
        }
    }
    Ok(acc)
}

/// Convert a narrow-PK `get_pk` (OPK-widened) value to its byte-form `PkBuf`.
/// `pk`'s OPK bytes at rest are the trailing `pk_stride` bytes of its big-endian
/// image (exactly what `extend_pk` writes); `to_le_bytes` would reverse them and
/// probe a key matching no stored row. Used at gather call sites to feed the
/// unified `Vec<PkBuf>` input once per call (not per incoming batch row).
#[inline]
fn u128_to_pkbuf(pk: u128, pk_stride: u8) -> PkBuf {
    PkBuf::from_bytes(&pk.to_be_bytes()[16 - pk_stride as usize..])
}

/// Decode an OPK leading-key span back to its native per-column values — the
/// inverse of `IndexKeySpec::write_span`, so the master stays the OPK *decoder*
/// and the worker the sole OPK *encoder*. `idx_cols` are the span's promoted
/// index columns; trailing array slots stay zero.
fn span_to_natives(span: &PkBuf, idx_cols: &[SchemaColumn]) -> [u128; gnitz_wire::PK_LIST_MAX_COLS] {
    let mut natives = [0u128; gnitz_wire::PK_LIST_MAX_COLS];
    let mut off = 0;
    for (native, col) in natives.iter_mut().zip(idx_cols) {
        let sz = col.size() as usize;
        let le = gnitz_wire::decode_pk_column_owned(&span.pk_bytes()[off..off + sz], col.type_code);
        *native = u128::from_le_bytes(le);
        off += sz;
    }
    natives
}

/// Collect the distinct non-PK parent columns referenced by `target_id`'s FK
/// children, as a projection list for `execute_gather_async`. A PK-target child
/// reads its referenced value from the packed PK on the wire and needs no
/// gather, so PK columns are excluded.
fn collect_fk_projection(
    cat: &CatalogEngine,
    target_id: i64,
    n_children: usize,
    source_schema: &SchemaDescriptor,
) -> Vec<u8> {
    let mut project: Vec<u8> = Vec::new();
    for ci in 0..n_children {
        if let Some((_, _, parent_col_idx)) = cat.get_fk_child_info(target_id, ci) {
            if !source_schema.is_pk_col(parent_col_idx) && !project.contains(&(parent_col_idx as u8)) {
                project.push(parent_col_idx as u8);
            }
        }
    }
    project
}

impl MasterDispatcher {
    // -----------------------------------------------------------------------
    //
    // A user INSERT may trigger up to O(N) distributed has-pk checks
    // (one per FK constraint, one per FK child with restrict deletes,
    // one for UPSERT PK identification, plus one or two per unique
    // secondary index). Running them sequentially costs N master↔worker
    // round trips. `execute_pipeline` writes the whole burst into SAL,
    // signals once, then collects N responses per worker in a single
    // poll loop — cursor position on the W2M ring correlates each
    // response with its originating check, so no explicit correlation
    // ID is needed. `validate_all_distributed` composes the bursts into
    // at most 2 rounds: Phase 1 is fully independent; Phase 2 depends
    // on Phase 1's UPSERT-identification result.

    /// Async equivalent of `validate_all_distributed`. Identical semantics;
    /// uses `execute_pipeline_async` + `fan_out_seek_by_index_async` so it
    /// runs without blocking the reactor.
    pub async fn validate_all_distributed_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        batch: &Batch,
        mode: WireConflictMode,
    ) -> Result<(), String> {
        let (n_fk, n_children, n_circuits, has_unique, unique_pk, source_schema) = unsafe {
            let disp = &mut *disp_ptr;
            let cat = &mut *disp.catalog;
            let n_fk = cat.get_fk_count(target_id);
            let n_children = cat.get_fk_children_count(target_id);
            let n_circuits = cat.get_index_circuit_count(target_id);
            let has_unique = cat.has_any_unique_index(target_id);
            let unique_pk = cat.table_has_unique_pk(target_id);
            let source_schema = cat
                .get_schema_desc(target_id)
                .ok_or_else(|| format!("validate_all_distributed: no schema for table {target_id}"))?;
            (n_fk, n_children, n_circuits, has_unique, unique_pk, source_schema)
        };

        let needs_pk_rejection = matches!(mode, WireConflictMode::Error) && unique_pk;

        if n_fk == 0 && n_children == 0 && !has_unique && !needs_pk_rejection {
            return Ok(());
        }

        // Wide PKs (pk_stride > 16) cannot pack into a u128 word; their
        // aggregation/identification paths key on byte-form `PkBuf` instead.
        // Narrow paths stay verbatim on u128 to avoid the per-row PkBuf init
        // cost on the common (large-INSERT) path.
        let wide = source_schema.pk_is_wide();

        // Borrowed view for the per-column locator reads (FK insert / unique
        // enforcement); zero-allocation over `batch`'s pages.
        let mb = batch.as_mem_batch();

        // Build PK aggregation. Narrow path uses the pooled u128 map and yields
        // `pk_lo_hi`; the wide path uses a local PkBuf-keyed map and yields
        // `pk_lo_hi_wide`. Both feed the UPSERT PK-identification check below.
        let mut pk_lo_hi: Option<Vec<u128>> = None;
        let mut pk_lo_hi_wide: Option<Vec<PkBuf>> = None;
        if has_unique || needs_pk_rejection {
            if !wide {
                pk_lo_hi = PK_AGG_POOL.with(|cell| -> Result<Option<Vec<u128>>, String> {
                    let mut m = cell.borrow_mut();
                    // Reclaim capacity after an unusually large batch to prevent
                    // the thread-local from growing without bound.
                    if m.capacity() > 65_536 {
                        *m = FxHashMap::default();
                    } else {
                        m.clear();
                    }
                    m.reserve(batch.count);
                    for i in 0..batch.count {
                        let w = batch.get_weight(i);
                        if w == 0 {
                            continue;
                        }
                        let entry = m.entry(batch.get_pk(i)).or_insert((0, 0));
                        entry.0 += w;
                        // A +w row is w insertions of the PK; Error mode must
                        // reject it like the w separate +1 rows it encodes.
                        if w > 0 {
                            entry.1 += if w > 1 { 2 } else { 1 };
                        }
                    }
                    if needs_pk_rejection {
                        for (&pk, &(_, pos_count)) in m.iter() {
                            if pos_count > 1 {
                                let key_str = format_pk_value(pk, &source_schema);
                                return Err(unsafe {
                                    (*disp_ptr).batch_dup_pk_err(target_id, &source_schema, &key_str)
                                });
                            }
                        }
                    }
                    let mut keys: Vec<u128> = Vec::with_capacity(m.len());
                    for (&pk, &(net_weight, _)) in m.iter() {
                        if net_weight <= 0 {
                            continue;
                        }
                        keys.push(pk);
                    }
                    Ok(Some(keys))
                })?;
            } else {
                let mut m: FxHashMap<PkBuf, (i64, u32)> =
                    FxHashMap::with_capacity_and_hasher(batch.count, Default::default());
                for i in 0..batch.count {
                    let w = batch.get_weight(i);
                    if w == 0 {
                        continue;
                    }
                    let entry = m.entry(PkBuf::from_bytes(batch.get_pk_bytes(i))).or_insert((0, 0));
                    entry.0 += w;
                    // A +w row is w insertions of the PK; Error mode must
                    // reject it like the w separate +1 rows it encodes.
                    if w > 0 {
                        entry.1 += if w > 1 { 2 } else { 1 };
                    }
                }
                if needs_pk_rejection {
                    for (pk, &(_, pos_count)) in m.iter() {
                        if pos_count > 1 {
                            let key_str = format_pk_value_bytes(pk.pk_bytes(), &source_schema);
                            return Err(unsafe { (*disp_ptr).batch_dup_pk_err(target_id, &source_schema, &key_str) });
                        }
                    }
                }
                let mut keys: Vec<PkBuf> = Vec::with_capacity(m.len());
                for (pk, &(net_weight, _)) in m.iter() {
                    if net_weight <= 0 {
                        continue;
                    }
                    keys.push(*pk);
                }
                pk_lo_hi_wide = Some(keys);
            }
        }

        // ----- Phase 1 plan -----------------------------------------------
        let mut p1_checks: Vec<PipelinedCheck> = Vec::new();
        let mut p1_labels: Vec<P1Label> = Vec::new();

        for fi in 0..n_fk {
            let (fk_col_idx, parent_table_id, parent_col_idx, parent_schema) = unsafe {
                let disp = &mut *disp_ptr;
                let cat = &mut *disp.catalog;
                let (fk_col_idx, parent_table_id, parent_col_idx) = match cat.get_fk_constraint(target_id, fi) {
                    Some(c) => c,
                    None => continue,
                };
                let parent_schema = cat
                    .get_schema_desc(parent_table_id)
                    .ok_or_else(|| format!("FK parent table {parent_table_id} schema not found"))?;
                (fk_col_idx, parent_table_id, parent_col_idx, parent_schema)
            };

            // The FK column may itself be a PK column of the child table (e.g.
            // `id BIGINT PRIMARY KEY REFERENCES parent(pid)`); `locate` resolves
            // the PK-or-payload read once, so a PK FK column reads from the
            // packed PK region instead of a (nonexistent) payload slot.
            let loc = source_schema.locate(fk_col_idx);

            let mut seen: FxHashSet<u128> = FxHashSet::default();
            let mut keys: Vec<u128> = Vec::new();

            for i in 0..batch.count {
                if batch.get_weight(i) <= 0 {
                    continue;
                }
                if loc.is_null(&mb, i) {
                    continue;
                }
                let fk_key = loc.native_key(&mb, i);
                if seen.insert(fk_key) {
                    keys.push(fk_key);
                }
            }

            if keys.is_empty() {
                continue;
            }
            let expected_count = keys.len();

            // PK fast-path only when the referenced column *is* the parent's
            // lone PK. Otherwise probe the parent's UNIQUE index by broadcast,
            // since index entries are distributed independently of the PK.
            let pk = parent_schema.pk_indices();
            let is_parent_pk = pk.len() == 1 && pk[0] as usize == parent_col_idx;

            if is_parent_pk {
                // Base table: the PK column does not promote, so its own type is
                // both the source and the leading-key type (identity encode).
                let src_type = parent_schema.columns[parent_col_idx].type_code;
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(parent_table_id) };
                let check_batch = build_check_batch(&parent_schema, &keys, src_type, pooled);
                p1_labels.push(P1Label::FkParent {
                    parent_table_id,
                    expected_count,
                });
                p1_checks.push(PipelinedCheck {
                    target_id: parent_table_id,
                    flags: FLAG_HAS_PK,
                    col_hint: 0,
                    payload: Some(CheckPayload::ScatterSource { source: check_batch }),
                    schema: parent_schema,
                });
            } else {
                let idx_schema = unsafe {
                    let cat = &mut *(*disp_ptr).catalog;
                    cat.get_index_schema_by_cols(parent_table_id, &[parent_col_idx as u32])
                        .ok_or_else(|| {
                            format!("FK check: no unique index on parent table {parent_table_id} col {parent_col_idx}")
                        })?
                };
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(parent_table_id) };
                // `loc` is the child FK column; its type drives the sign-extension
                // into the parent index's promoted leading key.
                let check_batch = build_check_batch(&idx_schema, &keys, loc.type_code(), pooled);
                p1_labels.push(P1Label::FkParent {
                    parent_table_id,
                    expected_count,
                });
                p1_checks.push(PipelinedCheck {
                    target_id: parent_table_id,
                    flags: FLAG_HAS_PK,
                    col_hint: gnitz_wire::pack_pk_cols(&[parent_col_idx as u32]),
                    payload: Some(CheckPayload::Broadcast(check_batch)),
                    schema: idx_schema,
                });
            }
        }

        // Parent DELETE → child RESTRICT. `target_id` / `source_schema` here
        // are the PARENT (the table receiving the deletes). The probe keys are
        // the *referenced parent column's* values of the removed rows, which
        // differ per child (different children may reference different columns).
        if n_children > 0 {
            // net_pk aggregation runs at batch.count scale: stay on u128 for
            // the narrow path; the wide path keys on byte-form PkBuf. The
            // *distinct* removed PKs are few, so both produce a uniform
            // `Vec<PkBuf>` consumed by the gather and per-child key building.
            let removed_pks: Vec<PkBuf> = if !wide {
                let mut net_pk: FxHashMap<u128, i64> = FxHashMap::default();
                for i in 0..batch.count {
                    let w = batch.get_weight(i);
                    if w == 0 {
                        continue;
                    }
                    *net_pk.entry(batch.get_pk(i)).or_insert(0) += w;
                }
                let stride = source_schema.pk_stride();
                net_pk
                    .into_iter()
                    .filter(|&(_, w)| w < 0)
                    .map(|(k, _)| u128_to_pkbuf(k, stride))
                    .collect()
            } else {
                let mut net_pk: FxHashMap<PkBuf, i64> = FxHashMap::default();
                for i in 0..batch.count {
                    let w = batch.get_weight(i);
                    if w == 0 {
                        continue;
                    }
                    *net_pk.entry(PkBuf::from_bytes(batch.get_pk_bytes(i))).or_insert(0) += w;
                }
                net_pk.into_iter().filter(|&(_, w)| w < 0).map(|(k, _)| k).collect()
            };

            if !removed_pks.is_empty() {
                // Resolve every non-PK referenced parent column in ONE batched
                // gather instead of one serial seek per (removed PK × child).
                let project =
                    unsafe { collect_fk_projection(&*(*disp_ptr).catalog, target_id, n_children, &source_schema) };
                let gathered = if project.is_empty() {
                    GatherMap::default()
                } else {
                    Self::execute_gather_async(disp_ptr, reactor, sal_excl, target_id, removed_pks.clone(), &project)
                        .await?
                };

                for ci in 0..n_children {
                    let (child_tid, fk_col_idx, parent_col_idx, idx_schema) = unsafe {
                        let cat = &mut *(*disp_ptr).catalog;
                        let (child_tid, fk_col_idx, parent_col_idx) = match cat.get_fk_child_info(target_id, ci) {
                            Some(info) => info,
                            None => continue,
                        };
                        let idx_schema =
                            cat.get_index_schema_by_cols(child_tid, &[fk_col_idx as u32])
                                .ok_or_else(|| {
                                    format!(
                                "FK RESTRICT check failed: no index on child table {child_tid} col {fk_col_idx}")
                                })?;
                        (child_tid, fk_col_idx, parent_col_idx, idx_schema)
                    };

                    // PK-column target → value is on the wire inside the packed
                    // PK; extract just that column. Non-PK UNIQUE target → the
                    // delete carries no payload, so read the value resolved by
                    // the gather above. A NULL or absent referenced value is
                    // omitted from the gather map and so never blocks the
                    // delete (matching the old per-row seek's skip).
                    let keys: Vec<u128> = if source_schema.is_pk_col(parent_col_idx) {
                        let col_type = source_schema.columns[parent_col_idx].type_code;
                        let col_size = source_schema.columns[parent_col_idx].size() as usize;
                        let pk_field_off = source_schema.pk_byte_offset(parent_col_idx) as usize;
                        removed_pks
                            .iter()
                            .map(|pk| pk_native_key(pk.pk_bytes(), pk_field_off, col_size, col_type))
                            .collect()
                    } else {
                        let proj_pos = project
                            .iter()
                            .position(|&c| c == parent_col_idx as u8)
                            .expect("non-PK referenced column missing from gather projection");
                        let mut vals: Vec<u128> = Vec::with_capacity(removed_pks.len());
                        for pk in &removed_pks {
                            // Zero-copy lookup via Borrow<[u8]>.
                            if let Some(row) = gathered.get(pk.pk_bytes()) {
                                if let Some(v) = row[proj_pos] {
                                    vals.push(v);
                                }
                            }
                        }
                        vals
                    };
                    if keys.is_empty() {
                        continue;
                    }

                    // The keys are the referenced parent column's values; its type
                    // drives the sign-extension into the child index's promoted
                    // leading key (equal logical values sign-extend to the same
                    // promoted image from either side's width).
                    let src_type = source_schema.columns[parent_col_idx].type_code;
                    let pooled = unsafe { (*disp_ptr).pool_pop_batch(child_tid) };
                    let check_batch = build_check_batch(&idx_schema, &keys, src_type, pooled);
                    p1_labels.push(P1Label::FkRestrict { child_tid });
                    p1_checks.push(PipelinedCheck {
                        target_id: child_tid,
                        flags: FLAG_HAS_PK,
                        col_hint: gnitz_wire::pack_pk_cols(&[fk_col_idx as u32]),
                        payload: Some(CheckPayload::Broadcast(check_batch)),
                        schema: idx_schema,
                    });
                }
            }
        }

        // UPSERT PK identification: which incoming PKs already exist in storage.
        // Routing is computed inside execute_pipeline_async; here we only build
        // the check batch (narrow u128 keys, or byte-form for wide PKs).
        let upsert_pk_batch: Option<Batch> = match (&pk_lo_hi, &pk_lo_hi_wide) {
            (Some(keys), _) if !keys.is_empty() => {
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                // `keys` are get_pk (OPK-widened) main-table PK values — write
                // their OPK bytes verbatim. `build_check_batch` is the *index*
                // builder: it would re-OPK-encode column 0 (double sign-flip for
                // signed) and zero the compound suffix, mangling the main-table PK
                // probe. This branch is narrow-only (`pk_lo_hi` is Some only when
                // `!source_schema.pk_is_wide()`), so `extend_pk` is valid.
                Some(build_check_batch_with(&source_schema, keys, pooled, |b, &k| {
                    b.extend_pk(k)
                }))
            }
            (_, Some(keys)) if !keys.is_empty() => {
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                Some(build_check_batch_pkbuf(&source_schema, keys, pooled))
            }
            _ => None,
        };
        if let Some(check_batch) = upsert_pk_batch {
            p1_labels.push(P1Label::UpsertPkId);
            p1_checks.push(PipelinedCheck {
                target_id,
                flags: FLAG_HAS_PK,
                col_hint: 0,
                payload: Some(CheckPayload::ScatterSource { source: check_batch }),
                schema: source_schema,
            });
        }

        // ----- Phase 1 execute + interpret --------------------------------
        let mut existing_pks: FxHashSet<PkBuf> = FxHashSet::default();
        if !p1_checks.is_empty() {
            let mut p1_results = Self::execute_pipeline_async(disp_ptr, reactor, sal_excl, &mut p1_checks).await?;
            unsafe {
                reclaim_check_batches(&mut *disp_ptr, &mut p1_checks);
            }

            for (idx, label) in p1_labels.iter().enumerate() {
                match label {
                    P1Label::FkParent {
                        parent_table_id,
                        expected_count,
                    } => {
                        if p1_results[idx].len() < *expected_count {
                            let (sn, tn, tsn, ttn) = unsafe {
                                let disp = &mut *disp_ptr;
                                let (s, t) = disp.get_qualified_name_owned(target_id);
                                let (ts, tt) = disp.get_qualified_name_owned(*parent_table_id);
                                (s, t, ts, tt)
                            };
                            return Err(format!(
                                "Foreign Key violation in '{sn}.{tn}': value not found in target '{tsn}.{ttn}'"
                            ));
                        }
                    }
                    P1Label::FkRestrict { child_tid } => {
                        if !p1_results[idx].is_empty() {
                            let (sn, tn, csn, ctn) = unsafe {
                                let disp = &mut *disp_ptr;
                                let (s, t) = disp.get_qualified_name_owned(target_id);
                                let (cs, ct) = disp.get_qualified_name_owned(*child_tid);
                                (s, t, cs, ct)
                            };
                            return Err(format!(
                                "Foreign Key violation: cannot delete from '{sn}.{tn}', row still referenced by '{csn}.{ctn}'",
                            ));
                        }
                    }
                    P1Label::UpsertPkId => {
                        existing_pks = std::mem::take(&mut p1_results[idx]);
                        if matches!(mode, WireConflictMode::Error) && !existing_pks.is_empty() {
                            let conflict_pk = existing_pks.iter().next().unwrap();
                            let (pk_names, sn, tn) =
                                unsafe { (*disp_ptr).pk_violation_context(target_id, &source_schema) };
                            let key_str = format_pk_value_bytes(conflict_pk.pk_bytes(), &source_schema);
                            return Err(format!(
                                "duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": Key ({pk_names})=({key_str}) already exists",
                            ));
                        }
                    }
                }
            }
        }

        // ----- Phase 2 plan (unique index checks) -------------------------
        //
        // UPDATE of a referenced non-PK UNIQUE column can orphan child rows:
        // the validation batch holds only the new `+1` row (no retraction), so
        // the Phase-1 net-negative scan never sees the retired value. Resolve
        // the OLD committed value for each updated PK and RESTRICT-check any
        // value that both changed and is still referenced. Pipelined into the
        // Phase-2 burst alongside the unique-index broadcasts.
        // (child_tid, fk_col, child_idx_schema, src_type, retired_parent_values).
        // `src_type` is the referenced parent column's type, threaded into the
        // child index's OPK seek so a signed column sign-extends correctly.
        let mut p2_restrict: Vec<(i64, u32, SchemaDescriptor, u8, Vec<u128>)> = Vec::new();
        if n_children > 0 && !existing_pks.is_empty() {
            // Resolve the OLD committed values for all updated PKs in ONE
            // batched gather, rather than one serial seek per (updated PK ×
            // child). UPDATE cannot change PK columns, so only non-PK
            // referenced columns participate.
            let project =
                unsafe { collect_fk_projection(&*(*disp_ptr).catalog, target_id, n_children, &source_schema) };

            if !project.is_empty() {
                // Batch rows that are UPDATEs: positive weight and a PK that
                // already exists in committed storage (not an INSERT). Computed
                // once; the per-child loop below indexes into this instead of
                // re-walking the whole batch and re-checking `existing_pks`.
                let update_rows: Vec<usize> = (0..batch.count)
                    .filter(|&i| batch.get_weight(i) > 0 && existing_pks.contains(batch.get_pk_bytes(i)))
                    .collect();

                let mut updated: Vec<PkBuf> = Vec::new();
                let mut seen: FxHashSet<PkBuf> = FxHashSet::default();
                for &i in &update_rows {
                    let pk = PkBuf::from_bytes(batch.get_pk_bytes(i));
                    if seen.insert(pk) {
                        updated.push(pk);
                    }
                }

                // Old values come from committed storage (validation is
                // pre-commit, under the FK table locks held by the executor,
                // so the committed parent state is static across the gather).
                let gathered =
                    Self::execute_gather_async(disp_ptr, reactor, sal_excl, target_id, updated, &project).await?;

                for ci in 0..n_children {
                    let (child_tid, fk_col_idx, parent_col_idx, idx_schema) = unsafe {
                        let cat = &mut *(*disp_ptr).catalog;
                        let (child_tid, fk_col_idx, parent_col_idx) = match cat.get_fk_child_info(target_id, ci) {
                            Some(info) => info,
                            None => continue,
                        };
                        // PK columns are immutable under UPDATE — nothing to enforce.
                        if source_schema.is_pk_col(parent_col_idx) {
                            continue;
                        }
                        let idx_schema = match cat.get_index_schema_by_cols(child_tid, &[fk_col_idx as u32]) {
                            Some(s) => s,
                            None => continue,
                        };
                        (child_tid, fk_col_idx, parent_col_idx, idx_schema)
                    };

                    // PK columns are skipped above (immutable under UPDATE), so
                    // this referenced column is always a payload column.
                    let loc = source_schema.locate(parent_col_idx);
                    let proj_pos = project
                        .iter()
                        .position(|&c| c == parent_col_idx as u8)
                        .expect("non-PK referenced column missing from gather projection");

                    let mut retired: Vec<u128> = Vec::new();
                    for &i in &update_rows {
                        let new_val = if loc.is_null(&mb, i) {
                            None
                        } else {
                            Some(loc.native_key(&mb, i))
                        };

                        // Absent committed row → omitted from the gather map →
                        // skip (matches the old per-row seek's None). Zero-copy
                        // lookup via Borrow<[u8]>.
                        let old_val = match gathered.get(batch.get_pk_bytes(i)) {
                            Some(row) => row[proj_pos],
                            None => continue,
                        };

                        // Only a changed, non-null old value can orphan child rows.
                        if old_val != new_val {
                            if let Some(v) = old_val {
                                retired.push(v);
                            }
                        }
                    }
                    if !retired.is_empty() {
                        // `loc` is the referenced parent (payload) column; its
                        // type drives the sign-extension at the seek encode.
                        p2_restrict.push((child_tid, fk_col_idx as u32, idx_schema, loc.type_code(), retired));
                    }
                }
            }
        }

        if !has_unique && p2_restrict.is_empty() {
            return Ok(());
        }

        // Lazily warm the unique-index filters. In the async path this
        // may trigger a scan; the scan is itself async.
        Self::ensure_unique_filters_warm_async(disp_ptr, reactor, sal_excl, target_id).await?;

        let mut p2_checks: Vec<PipelinedCheck> = Vec::new();
        let mut p2_labels: Vec<P2Label> = Vec::new();

        // Index-independent; computed once. `retracted_vals` allocation is reused
        // across indices (`retracted_pairs` is moved into the label below, so it
        // is rebuilt each index).
        let has_retractions = (0..batch.count).any(|i| batch.get_weight(i) < 0);
        let mut retracted_vals: FxHashSet<PkBuf> = FxHashSet::default();
        // Reused scratch the per-row OPK leading-key span is written into; each
        // `key_bytes` call overwrites it. No per-row allocation.
        let mut keybuf = PkBuf::empty(0);

        for ci in 0..n_circuits {
            let (col_indices, idx_schema) = unsafe {
                let disp = &mut *disp_ptr;
                let cat = &mut *disp.catalog;
                let cols = match cat.unique_index_circuit_cols(target_id, ci) {
                    Some(c) => c,
                    None => continue,
                };
                let idx_schema = match cat.get_index_circuit_schema(target_id, ci) {
                    Some(s) => s,
                    None => continue,
                };
                // Own the list (PkColList is Copy) so it survives the catalog
                // borrow and feeds the P2Label / error formatters.
                (PkColList::from_slice(cols), idx_schema)
            };
            let cols = col_indices.as_slice();

            // Per-circuit read/encode plan: `key_bytes` builds the OPK
            // leading-key span — null-correct (skip on any NULL) and
            // equality-correct at any width.
            let spec = IndexKeySpec::new(cols, &source_schema, &idx_schema);
            // The (table, this) filter-map key AND the worker's index-store hint —
            // a single-column hint cannot locate a composite index.
            let packed = gnitz_wire::pack_pk_cols(cols);

            // `upsert_keys` carries the row's own `is_upsert` flag (the row PK is
            // no longer read at the verify). `check_keys`/`seen` take every
            // positive row on a pure INSERT — size them to `batch.count` to skip
            // growth reallocs on the hot path; `upsert_keys` stays `Vec::new()`
            // (empty on an insert-only batch).
            let mut upsert_keys: Vec<(PkBuf, bool)> = Vec::new();
            let mut check_keys: Vec<PkBuf> = Vec::with_capacity(batch.count);
            let mut seen: FxHashSet<PkBuf> = FxHashSet::with_capacity_and_hasher(batch.count, Default::default());

            // `retracted_vals` routes a fresh-PK insertion of a retracted value
            // into the verify path (where the holder is discovered);
            // `retracted_pairs` carries the precise `(holder PK, value span)` check
            // into it. `retracted_pairs` is moved into the label below (so it is
            // rebuilt per index), and is sized only when the batch actually retracts.
            retracted_vals.clear();
            let mut retracted_pairs: FxHashSet<(PkBuf, PkBuf)> = if has_retractions {
                FxHashSet::with_capacity_and_hasher(batch.count, Default::default())
            } else {
                FxHashSet::default()
            };
            if has_retractions {
                for i in 0..batch.count {
                    if batch.get_weight(i) >= 0 {
                        continue;
                    }
                    if !spec.key_bytes(&mb, i, &mut keybuf) {
                        continue;
                    }
                    retracted_vals.insert(keybuf);
                    retracted_pairs.insert((PkBuf::from_bytes(batch.get_pk_bytes(i)), keybuf));
                }
            }

            for i in 0..batch.count {
                let w = batch.get_weight(i);
                if w <= 0 {
                    continue;
                }

                // A row NULL in any indexed column is not indexed (NULL-distinct).
                if !spec.key_bytes(&mb, i, &mut keybuf) {
                    continue;
                }

                // One row at weight w is the value w times. On a non-unique_pk
                // table that is w live instances (enforce_unique_pk collapses
                // it to one on unique_pk tables) — the same violation as w
                // separate +1 rows, which `seen` below rejects.
                if !unique_pk && w > 1 {
                    return Err(unsafe { (*disp_ptr).unique_violation_err(target_id, cols, true) });
                }

                // In-batch duplicate detection runs for ALL positive-weight
                // rows, INCLUDING UPSERTs: two rows setting the same new unique
                // value in one transaction is a violation regardless of whether
                // their PKs already exist.
                if !seen.insert(keybuf) {
                    return Err(unsafe { (*disp_ptr).unique_violation_err(target_id, cols, true) });
                }

                // UPSERT (committed PK on a unique_pk table — enforce_unique_pk
                // retracts the old row at apply) OR a fresh insertion whose value
                // is explicitly retracted in this batch (transfer onto a fresh PK):
                // both need per-holder verification. On a non-unique_pk table an
                // existing PK is NOT an upsert (no enforce_unique_pk), so it must
                // take the broadcast path where any committed hit is a violation.
                let is_upsert = unique_pk && existing_pks.contains(batch.get_pk_bytes(i));
                if is_upsert || retracted_vals.contains(&keybuf) {
                    // Carry the row's own `is_upsert`: a fresh-PK row routed here by
                    // a value in `retracted_vals` must get only the
                    // explicit-retraction exemption at the verify, never the
                    // implicit one.
                    upsert_keys.push((keybuf, is_upsert));
                    continue;
                }
                check_keys.push(keybuf);
            }

            // Unique check batches zero-pad each OPK span to the index PK stride
            // (leading span = the value, suffix = zero — byte-identical to the old
            // single-column `build_check_batch`): `padded` is sound because
            // `key_bytes` keeps the tail past the span zero.
            let stride = idx_schema.pk_stride() as usize;

            if !check_keys.is_empty() {
                let skip_broadcast = unsafe { (*disp_ptr).unique_filter_all_absent(target_id, packed, &check_keys) };

                if !skip_broadcast {
                    let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                    let chk_batch = build_check_batch_with(&idx_schema, &check_keys, pooled, |b, k| {
                        b.extend_pk_bytes(k.padded(stride))
                    });
                    p2_labels.push(P2Label::NonUpsert { col_indices });
                    p2_checks.push(PipelinedCheck {
                        target_id,
                        flags: FLAG_HAS_PK,
                        col_hint: packed,
                        payload: Some(CheckPayload::Broadcast(chk_batch)),
                        schema: idx_schema,
                    });
                }
            }

            if !upsert_keys.is_empty() {
                let pooled = unsafe { (*disp_ptr).pool_pop_batch(target_id) };
                let u_batch = build_check_batch_with(&idx_schema, &upsert_keys, pooled, |b, (k, _)| {
                    b.extend_pk_bytes(k.padded(stride))
                });
                p2_labels.push(P2Label::Upsert {
                    col_indices,
                    upsert_keys,
                    retracted_pairs,
                });
                p2_checks.push(PipelinedCheck {
                    target_id,
                    flags: FLAG_HAS_PK,
                    col_hint: packed,
                    payload: Some(CheckPayload::Broadcast(u_batch)),
                    schema: idx_schema,
                });
            }
        }

        // RESTRICT probes for referenced UNIQUE values retired by UPDATE.
        for (child_tid, fk_col, idx_schema, src_type, keys) in p2_restrict {
            let pooled = unsafe { (*disp_ptr).pool_pop_batch(child_tid) };
            let chk_batch = build_check_batch(&idx_schema, &keys, src_type, pooled);
            p2_labels.push(P2Label::FkRestrict { child_tid });
            p2_checks.push(PipelinedCheck {
                target_id: child_tid,
                flags: FLAG_HAS_PK,
                col_hint: gnitz_wire::pack_pk_cols(&[fk_col]),
                payload: Some(CheckPayload::Broadcast(chk_batch)),
                schema: idx_schema,
            });
        }

        if p2_checks.is_empty() {
            return Ok(());
        }

        let p2_results = Self::execute_pipeline_async(disp_ptr, reactor, sal_excl, &mut p2_checks).await?;
        unsafe {
            reclaim_check_batches(&mut *disp_ptr, &mut p2_checks);
        }

        for (idx, label) in p2_labels.iter().enumerate() {
            match label {
                P2Label::NonUpsert { col_indices } => {
                    if !p2_results[idx].is_empty() {
                        return Err(unsafe {
                            (*disp_ptr).unique_violation_err(target_id, col_indices.as_slice(), false)
                        });
                    }
                }
                P2Label::FkRestrict { child_tid } => {
                    if !p2_results[idx].is_empty() {
                        let (sn, tn, csn, ctn) = unsafe {
                            let disp = &mut *disp_ptr;
                            let (s, t) = disp.get_qualified_name_owned(target_id);
                            let (cs, ct) = disp.get_qualified_name_owned(*child_tid);
                            (s, t, cs, ct)
                        };
                        return Err(format!(
                            "Foreign Key violation: cannot update '{sn}.{tn}', row still referenced by '{csn}.{ctn}'",
                        ));
                    }
                }
                P2Label::Upsert {
                    col_indices,
                    upsert_keys,
                    retracted_pairs,
                } => {
                    // Fan out all seeks before collecting: sal_excl is released
                    // before each reply wait, so the seek futures run
                    // concurrently rather than serializing one RTT per key.
                    let occupied = &p2_results[idx];
                    // The positionally paired check still holds the index schema
                    // (reclaim_check_batches takes only the payload).
                    let idx_schema = &p2_checks[idx].schema;
                    let stride = idx_schema.pk_stride() as usize;
                    let idx_cols = &idx_schema.columns[..col_indices.as_slice().len()];
                    // Each pending entry carries the index-value span and the row's
                    // own `is_upsert` flag; the row PK no longer participates (the
                    // same-PK case is subsumed by `existing_pks.contains(found_pk)`).
                    let mut pending: Vec<(PkBuf, bool)> = Vec::new();
                    let mut futs = Vec::new();
                    for &(key_pk, is_upsert) in upsert_keys {
                        // occupied holds index PKs at full stride, zero-suffixed
                        // (the probe wrote the OPK span padded to the stride).
                        // `key_pk` is already that OPK span — pad to the stride and
                        // test directly; do NOT re-OPK-encode an encoded span.
                        if !occupied.contains(key_pk.padded(stride)) {
                            continue;
                        }
                        pending.push((key_pk, is_upsert));
                        // async fn futures are !Unpin; box-pin for join_all_unpin.
                        // One homogeneous future type (all inputs Copy, no `dyn`):
                        // `seek_unique_holder` gates arity internally.
                        futs.push(Box::pin(Self::seek_unique_holder(
                            disp_ptr,
                            reactor,
                            sal_excl,
                            target_id,
                            *col_indices,
                            span_to_natives(&key_pk, idx_cols),
                        )));
                    }
                    let holders = crate::runtime::reactor::join_all_unpin(futs).await;
                    for ((key_pk, is_upsert), holder_result) in pending.into_iter().zip(holders) {
                        // A unique index yields at most one holder; the merged
                        // `Option<PkBuf>` is the committed holder's source PK.
                        let Some(found_pk) = holder_result? else { continue };
                        // The committed holder is acceptable only if it releases
                        // this value in this batch. Implicit release (holder is
                        // itself an upserted PK, so enforce_unique_pk retracts its
                        // committed row — covers a same-PK re-upsert and a bulk
                        // shift) applies ONLY when the colliding row is also an
                        // upsert: `is_upsert` implies `unique_pk &&
                        // existing_pks.contains(row_pk)`. A fresh-PK row routed here
                        // by `retracted_vals` has is_upsert=false, so it cannot ride
                        // the holder's implicit retraction and land a second live
                        // holder. Explicit release (holder retracts this exact
                        // (PK, value span) pair) needs no such gate and admits a
                        // transfer onto a fresh PK. `seen` already barred two live
                        // rows sharing the value.
                        let exempt = (is_upsert && existing_pks.contains(found_pk.pk_bytes()))
                            || retracted_pairs.contains(&(found_pk, key_pk));
                        if !exempt {
                            return Err(unsafe {
                                (*disp_ptr).unique_violation_err(target_id, col_indices.as_slice(), false)
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Pre-flight global uniqueness check for CREATE UNIQUE INDEX, distributed:
    /// each worker projects its committed partition to the indexed columns' OPK
    /// leading-key spans, sorts them locally (byte-lexicographic), and streams
    /// the SORTED spans back; the master runs a streaming k-way merge
    /// (`merge_index_scan`) whose single adjacent-equal check catches both
    /// within-partition and cross-partition duplicates that no per-worker
    /// `backfill_index` can see. Master memory is `O(num_workers)` plus the
    /// (≤ cap) filter seed — never the table's distinct-key cardinality. The OPK
    /// leading-key span is lossless and injective for every type a unique index
    /// permits (`index_key_type` rejects floats/STRING/BLOB), so byte equality ⟺
    /// index-value equality and byte-lexicographic order is a valid merge order
    /// at any width — replacing the old numeric `u128` order, which a composite
    /// key has no meaningful value for.
    ///
    /// On success the index is safe to commit and broadcast and the returned
    /// distinct span set seeds the master's unique filter; on failure the
    /// caller returns a client error and never broadcasts, so no worker
    /// reaches the fatal `DdlSync` backfill path.
    ///
    /// MUST run inside the DDL critical section (committer barrier drained,
    /// catalog write lock held) and BEFORE the IDX_TAB +1 is appended/broadcast,
    /// so the scanned snapshot is exactly the data each worker will later
    /// backfill and no concurrent INSERT can be ordered between the snapshot and
    /// the backfill.
    ///
    /// An unknown table yields an empty set (nothing to validate).
    pub async fn validate_unique_index_create_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        owner_id: i64,
        col_indices: &[u32],
    ) -> Result<(FxHashSet<PkBuf>, bool), String> {
        let (idx_schema, packed) = unsafe {
            let cat = &mut *(*disp_ptr).catalog;
            let owner_schema = match cat.get_schema_desc(owner_id) {
                Some(s) => s,
                None => return Ok((FxHashSet::default(), false)),
            };
            // Trivial-uniqueness short-circuit, generalised to the compound PK:
            // a composite index whose columns equal the table's enforced-unique
            // PK (in any order) can never collide, so the scan is skipped.
            // `group_cols_eq_pk` is order-insensitive set equality and returns
            // false for an out-of-range index, so no separate bounds check is
            // needed; it subsumes the old sole-PK-column case exactly. The
            // empty uncapped seed is NOT the committed value set (the scan is
            // skipped), but warm-empty stays sound: value-equality is
            // PK-equality here, and rows whose PK is committed never reach
            // `check_keys` (Error mode fails the Phase-1 PK check; upsert mode
            // routes them to the always-broadcast upsert path), so every
            // `check_keys` key carries a fresh PK — hence a fresh value — and
            // "absent" is always the correct verdict.
            if cat.table_has_unique_pk(owner_id) && owner_schema.group_cols_eq_pk(col_indices) {
                return Ok((FxHashSet::default(), false));
            }
            // Build the index schema (the circuit is not registered until this
            // pre-flight succeeds) for the merge's reply-frame layout and the
            // promoted per-column widths. Identical inputs to each worker's own
            // build, so the frame schema agrees by construction. `packed` is
            // the column list the worker resolves the seek by.
            (
                crate::catalog::make_index_schema(col_indices, &owner_schema)?,
                gnitz_wire::pack_pk_cols(col_indices),
            )
        };
        let frame_schema = unique_preflight_wire_schema(&idx_schema, col_indices.len());

        // Fan out the pre-flight command (the packed column list rides in
        // seek_col_idx); each worker answers with its sorted-span
        // continuation-frame train. `_lease` held to end of scope: when the
        // merge returns early (error or duplicate verdict) the lease drop
        // discards the undrained trains at the ring boundary.
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(owner_id);
            let lsn = disp.next_lsn();
            disp.write_group_with_req_ids(
                owner_id,
                lsn,
                FLAG_UNIQUE_PREFLIGHT,
                0,
                &[],
                &schema,
                &col_names,
                0,
                packed,
                req_ids,
                -1,
                0,
                None,
                &[],
            )
        })
        .await?;

        let merged = merge_index_scan(slots, &req_ids, reactor, &frame_schema).await?;
        if merged.duplicate {
            return Err(unsafe { (*disp_ptr).unique_create_dup_err(owner_id, col_indices) });
        }
        Ok(merged.into_seed())
    }

    /// Delegate unique-index violation formatting to the catalog, the single
    /// source of truth for the message text and the table/column name lookups.
    /// `col_indices` is the index's full column list (composite-aware).
    fn unique_violation_err(&mut self, target_id: i64, col_indices: &[u32], in_batch: bool) -> String {
        unsafe { (*self.catalog).unique_violation_err(target_id, col_indices, in_batch) }
    }

    /// Delegate `CREATE UNIQUE INDEX` duplicate-value rejection to the catalog.
    fn unique_create_dup_err(&mut self, owner_id: i64, col_indices: &[u32]) -> String {
        unsafe { (*self.catalog).unique_create_dup_err(owner_id, col_indices) }
    }

    /// Build the `(pk_names_joined, schema_name, table_name)` triple used
    /// in PG-style "violates unique constraint \"{sn}_{tn}_pkey\": ... key
    /// ({pk_names_joined})=(...)" error messages. Compound PKs join column
    /// names with commas in declaration order.
    fn pk_violation_context(&mut self, target_id: i64, schema: &SchemaDescriptor) -> (String, String, String) {
        let names: Vec<String> = schema
            .pk_indices()
            .iter()
            .map(|&ci| self.get_col_name(target_id, ci as usize))
            .collect();
        let (sn, tn) = self.get_qualified_name_owned(target_id);
        (names.join(", "), sn, tn)
    }

    /// Format the "two rows in one batch share a PK" rejection message.
    /// `key_str` is the already-rendered offending key (narrow or wide).
    fn batch_dup_pk_err(&mut self, target_id: i64, schema: &SchemaDescriptor, key_str: &str) -> String {
        let (pk_names, sn, tn) = self.pk_violation_context(target_id, schema);
        format!(
            "duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": Batch contains multiple rows with key ({pk_names})=({key_str})",
        )
    }
}
