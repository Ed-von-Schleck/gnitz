use std::num::NonZeroU64;

use super::*;

/// Cached schema wire block plus derived properties needed by the SAL
/// ingest fast path. Returned by `get_cached_schema_wire_block`; all four
/// fields share the same invalidation lifecycle (DDL on the owning table
/// drops every entry together).
pub struct CachedSchemaWire {
    pub block: Rc<Vec<u8>>,
    pub version: u16,
    /// True when every column has a fixed-width 8-aligned stride and no
    /// German-string (STRING or BLOB) columns. Drives the `scatter_wire_group` fast path.
    pub wire_safe: bool,
    /// Sum of pk_stride + 8 (weight) + 8 (null_bmp) + every payload column's
    /// stride. Only meaningful when `wire_safe`.
    pub wire_row_fixed_stride: u32,
}

const SYS_TABLE_IDS: &[i64] = &[
    SCHEMA_TAB_ID, TABLE_TAB_ID, VIEW_TAB_ID, COL_TAB_ID, IDX_TAB_ID,
    DEP_TAB_ID, SEQ_TAB_ID,
    CIRCUIT_NODES_TAB_ID, CIRCUIT_EDGES_TAB_ID, CIRCUIT_NODE_COLUMNS_TAB_ID,
];

/// The only handle the DDL/imperative layer has on catalog state: submit one
/// system-family delta. It cannot name a `sys_*` table, so DDL code physically
/// cannot mutate a dependent family except by emitting a delta that flows
/// through the single precheck → persist → `fire_hooks` → broadcast path —
/// identical on live apply, WAL replay, and worker sync. The cascades that drop
/// columns/indices/circuit rows are the applier's declared reaction to a
/// retraction, fired from inside `fire_hooks`, not the emitter's concern.
pub(crate) trait CatalogDeltaSink {
    /// Apply one system-family delta and enqueue it for broadcast: precheck →
    /// storage write → `fire_hooks` → enqueue. The batch is taken by value so
    /// the applier moves it straight into `pending_broadcasts` (one storage
    /// clone, no hooks clone).
    fn submit(&mut self, family: SysFamily, batch: Batch) -> Result<(), String>;

    /// Apply locally without enqueuing a broadcast. ONLY for rows the workers
    /// already produce themselves (FK indices auto-created from the same
    /// `TABLE_TAB` delta) and for rollback compensation. Re-broadcasting these
    /// would deliver phantom deltas. Documented and audited; not a general escape.
    fn submit_local(&mut self, family: SysFamily, batch: Batch) -> Result<(), String>;
}

impl CatalogDeltaSink for CatalogEngine {
    fn submit(&mut self, family: SysFamily, mut batch: Batch) -> Result<(), String> {
        if self.ctx.in_rollback() {
            // During rollback all cascade writes must bypass pending_broadcasts
            // so no compensating row is re-broadcast to workers.
            return self.submit_local(family, batch);
        }
        let id = family.id();
        // Reject BEFORE any WAL write; avoids dangling retractions that would
        // later trip replay_catalog.
        self.precheck_sys_ingest(id, &batch)?;
        // Apply + fire hooks, pinning this zone's writes to the DDL zone LSN.
        self.apply_local(family, &mut batch, self.ctx.ddl_zone_lsn())?;

        // Enqueue after hooks so nested cascade pushes land first and the
        // executor broadcasts children → parent. Drop empty batches so
        // worker-side no-op cascades don't accumulate unread entries.
        if batch.count > 0 {
            self.pending_broadcasts.push((id, batch));
        }
        Ok(())
    }

    fn submit_local(&mut self, family: SysFamily, mut batch: Batch) -> Result<(), String> {
        // No LSN pin: local applies are rollback compensation or rows the
        // workers already produce, neither of which owns this zone's durability.
        // `None` is deliberate even while a DDL zone is active — these rows are
        // not in the SAL, so pinning their family's current_lsn would advance the
        // recovery dedup watermark with no matching SAL group.
        self.apply_local(family, &mut batch, None)
        // Deliberately no push to pending_broadcasts.
    }
}

impl CatalogEngine {
    // -- System table accessor -----------------------------------------------

    /// Map a system table ID to a mutable reference. Returns None for unknown IDs.
    pub(crate) fn sys_table(&self, table_id: i64) -> Option<&Table> {
        match table_id {
            SCHEMA_TAB_ID => Some(&self.sys_schemas),
            TABLE_TAB_ID => Some(&self.sys_tables),
            VIEW_TAB_ID => Some(&self.sys_views),
            COL_TAB_ID => Some(&self.sys_columns),
            IDX_TAB_ID => Some(&self.sys_indices),
            DEP_TAB_ID => Some(&self.sys_view_deps),
            SEQ_TAB_ID => Some(&self.sys_sequences),
            CIRCUIT_NODES_TAB_ID => Some(&self.sys_circuit_nodes),
            CIRCUIT_EDGES_TAB_ID => Some(&self.sys_circuit_edges),
            CIRCUIT_NODE_COLUMNS_TAB_ID => Some(&self.sys_circuit_node_columns),
            _ => None,
        }
    }

    pub(crate) fn sys_table_mut(&mut self, table_id: i64) -> Option<&mut Table> {
        match table_id {
            SCHEMA_TAB_ID => Some(&mut self.sys_schemas),
            TABLE_TAB_ID => Some(&mut self.sys_tables),
            VIEW_TAB_ID => Some(&mut self.sys_views),
            COL_TAB_ID => Some(&mut self.sys_columns),
            IDX_TAB_ID => Some(&mut self.sys_indices),
            DEP_TAB_ID => Some(&mut self.sys_view_deps),
            SEQ_TAB_ID => Some(&mut self.sys_sequences),
            CIRCUIT_NODES_TAB_ID => Some(&mut self.sys_circuit_nodes),
            CIRCUIT_EDGES_TAB_ID => Some(&mut self.sys_circuit_edges),
            CIRCUIT_NODE_COLUMNS_TAB_ID => Some(&mut self.sys_circuit_node_columns),
            _ => None,
        }
    }

    /// Apply one delta to its family's storage and fire the reaction hooks —
    /// the shared tail of [`CatalogDeltaSink::submit`] / `submit_local`. When
    /// `pin_lsn` is `Some(lsn)` it pins the family's `current_lsn` to `lsn.get()`
    /// (the DDL zone LSN) so recovery's dedup check (`msg.lsn <= flushed`) matches
    /// the SAL group LSN. Pins never regress the counter: the zone allocator
    /// takes `max(ingest_lsn, max_table_current_lsn()) + 1`, so even counters
    /// drifted by un-pinned auto-bump ingests sit strictly below every newly
    /// allocated zone LSN. Does NOT broadcast.
    fn apply_local(&mut self, family: SysFamily, batch: &mut Batch, pin_lsn: Option<NonZeroU64>) -> Result<(), String> {
        let id = family.id();
        let table = self.sys_table_mut(id)
            .expect("SysFamily::id() maps to a known sys table");
        ingest_batch_into(table, batch);
        if let Some(lsn) = pin_lsn {
            table.current_lsn = lsn.get();
        }
        batch.set_schema(sys_tab_schema(id));
        self.fire_hooks(family, batch)
    }

    // -- Server ingestion / scan / seek / flush -----------------------------

    /// Ingest a batch into a table family (unique_pk + store + index projection + hooks).
    /// System tables go through the [`CatalogDeltaSink::submit`] applied-delta
    /// path (precheck → ingest → hooks → broadcast-queue). User tables delegate
    /// to `DagEngine::ingest_by_ref`. This `&Batch` entry serves external/wire
    /// callers that hold a borrow; the DDL emitters call `submit` directly with
    /// an owned batch to skip the clone.
    pub fn ingest_to_family(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            let family = SysFamily::from_id(table_id)
                .ok_or_else(|| format!("Unknown system family {}", table_id))?;
            self.submit(family, batch.clone())
        } else {
            let rc = self.dag.ingest_by_ref(table_id, batch);
            if rc < 0 {
                Err(format!("ingest_to_family failed for table_id={} rc={}", table_id, rc))
            } else {
                Ok(())
            }
        }
    }

    /// Emit the single retraction delta for `pk` in `family`: seek the live row,
    /// copy it with weight −1, and submit it through the one applied-delta path.
    /// The drop cascade is the applier's reaction to that −1 (fired from
    /// `fire_hooks`), not the caller's concern. Uses the immutable `sys_table`
    /// accessor because `retract_single_row` only reads; the `submit` move comes
    /// after. `retract_single_row` returns an empty batch when the PK is absent
    /// or already retracted; emitters resolve the friendly "does not exist"
    /// message from the caches before calling, so the `count == 0` arm only
    /// fires on cache/storage divergence.
    /// Only the test-only direct DDL drop paths (`ddl.rs`) retract engine-side;
    /// production retractions arrive as wire deltas.
    #[cfg(test)]
    pub(crate) fn submit_retraction(&mut self, family: SysFamily, pk: u128) -> Result<(), String> {
        let schema = sys_tab_schema(family.id());
        let batch = {
            let table = self.sys_table(family.id())
                .expect("SysFamily::id() maps to a known sys table");
            retract_single_row(table, &schema, pk)
        };
        if batch.count == 0 {
            return Err("Entity does not exist in catalog".into());
        }
        self.submit(family, batch)
    }

    /// Reject a CREATE whose qualified `schema.name` collides with an existing
    /// entity. Re-ingesting the same row (e.g. a pk-col update of `self_id`) is
    /// allowed; only a genuinely different entity under that name is rejected.
    /// Also resolves `sid`, erroring if the schema does not exist.
    fn precheck_qname_unique(&self, sid: i64, name: &str, self_id: i64) -> Result<(), String> {
        let schema_name = self.caches.schema_by_id.get(&sid)
            .ok_or_else(|| format!("Schema with ID {} does not exist", sid))?;
        let qualified = format!("{}.{}", schema_name, name);
        if let Some(&existing) = self.caches.entity_by_qname.get(&qualified) {
            if existing != self_id {
                return Err(format!("Table or view already exists: {}", qualified));
            }
        }
        Ok(())
    }

    /// Visit every positive-weight `sys_indices` row whose owner and **exact
    /// column list** match `(owner_id, cols)`, invoking `f(index_id, is_unique)`
    /// for each. Centralises the IDX_TAB cursor walk shared by the DROP INDEX
    /// uniqueness checks (the drop-time FK guard in `precheck_sys_ingest` and the
    /// post-retraction circuit demotion in `hook_index_register`). The persisted
    /// `source_cols` field is the packed `u64` (flag bit 63 set for the packed
    /// form), so decode it via `unpack_pk_cols` and compare ordered lists — a
    /// bare compare would never match a packed row. Rows that have already netted
    /// to zero weight are skipped by the cursor.
    pub(crate) fn for_each_index_on_cols(
        &self, owner_id: i64, cols: &[u32], mut f: impl FnMut(i64, bool),
    ) {
        let mut cursor = self.sys_indices.open_cursor();
        while cursor.cursor.valid {
            if cursor.cursor.current_weight > 0 {
                let row_owner = cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64;
                let row_cols  = gnitz_wire::unpack_pk_cols(
                    cursor_read_u64(&cursor, IDXTAB_COL_SOURCE_COLS));
                if row_owner == owner_id && row_cols.as_slice() == cols {
                    let row_id  = cursor.cursor.current_key as u64 as i64;
                    let is_uniq = cursor_read_u64(&cursor, IDXTAB_COL_IS_UNIQUE) != 0;
                    f(row_id, is_uniq);
                }
            }
            cursor.cursor.advance();
        }
    }

    /// Validate a system-table write before any mutation (memtable or hooks).
    /// Covers both positive-weight (CREATE) invariants and negative-weight (DROP)
    /// integrity guards so that no invalid state is ever written.
    fn precheck_sys_ingest(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        if table_id != SCHEMA_TAB_ID
            && table_id != TABLE_TAB_ID
            && table_id != VIEW_TAB_ID
            && table_id != IDX_TAB_ID
        {
            return Ok(());
        }

        // -- Positive-weight (CREATE) checks ----------------------------------
        for i in 0..batch.count {
            if batch.get_weight(i) <= 0 { continue; }

            match table_id {
                SCHEMA_TAB_ID => {
                    let name = self.read_batch_string(batch, i, 0);
                    if self.caches.schema_by_name.contains_key(&name) {
                        return Err(format!("Schema already exists: {}", name));
                    }
                }
                TABLE_TAB_ID => {
                    let tid = batch.get_pk(i) as i64;
                    let sid = self.read_batch_u64(batch, i, 0) as i64;
                    let name = self.read_batch_string(batch, i, 1);
                    let pk = unpack_pk_cols(self.read_batch_u64(batch, i, 3));

                    // Collect ColumnDefs, rejecting any gap/duplicate in the
                    // column-index sequence (would mismap columns downstream).
                    let col_defs = self.scan_column_defs(tid, true)?;

                    if col_defs.is_empty() {
                        return Err(format!(
                            "catalog invariant violated: table '{}' (tid={}) registered \
                             before its column records. COL_TAB writes must precede \
                             TABLE_TAB writes (see hooks.rs dispatch doc).",
                            name, tid));
                    }
                    validate_pk_cols(&col_defs, &pk)?;
                    if col_defs.len() > crate::schema::MAX_COLUMNS {
                        return Err(format!(
                            "table '{}' (tid={}) has {} columns (max {})",
                            name, tid, col_defs.len(), crate::schema::MAX_COLUMNS));
                    }

                    let first_pk = pk.as_slice()[0];
                    let self_pk_type = col_defs[first_pk as usize].type_code;
                    for (col_idx, cd) in col_defs.iter().enumerate() {
                        if cd.fk_table_id != 0 {
                            self.validate_fk_column(cd, col_idx, tid, first_pk, self_pk_type)?;
                        }
                    }

                    self.precheck_qname_unique(sid, &name, tid)?;
                }
                VIEW_TAB_ID => {
                    let vid = batch.get_pk(i) as i64;
                    let sid = self.read_batch_u64(batch, i, 0) as i64;
                    let name = self.read_batch_string(batch, i, 1);

                    // Reject any gap/duplicate in the column-index sequence;
                    // only the count is needed here.
                    let col_count = self.scan_column_defs(vid, true)?.len();
                    if col_count == 0 {
                        return Err(format!(
                            "catalog invariant violated: view '{}' (vid={}) registered \
                             before its column records. COL_TAB writes must precede \
                             VIEW_TAB writes (see hooks.rs dispatch doc).",
                            name, vid));
                    }
                    // Reject an over-wide view here — before apply_entity_by_qname
                    // mutates the caches — so a rejected DDL leaves clean state,
                    // mirroring the TABLE_TAB precheck above. hook_view_register
                    // carries the same guard as the build_schema_from_col_defs
                    // assert backstop.
                    if col_count > crate::schema::MAX_COLUMNS {
                        return Err(format!(
                            "view '{}' (vid={}) has {} columns (max {})",
                            name, vid, col_count, crate::schema::MAX_COLUMNS));
                    }
                    self.precheck_qname_unique(sid, &name, vid)?;
                }
                IDX_TAB_ID => {
                    let owner_id = self.read_batch_u64(batch, i, 0) as i64;
                    // source_cols carries pack_pk_cols(&col_indices); decode and
                    // validate each column (a single-column index is the 1-element
                    // degenerate case).
                    let cols = gnitz_wire::unpack_pk_cols(self.read_batch_u64(batch, i, 2));
                    let index_name = self.read_batch_string(batch, i, 3);
                    if !cols.is_well_formed() {
                        return Err(format!(
                            "Index: column list count {} out of range 1..={}",
                            cols.decoded_count(), gnitz_wire::PK_LIST_MAX_COLS));
                    }

                    let entry = self.dag.tables.get(&owner_id)
                        .ok_or_else(|| format!("Index: owner table {} not found", owner_id))?;

                    // Only base tables can own a secondary index: index
                    // projection runs only on the base-table DML paths
                    // (`ingest_store_and_indices`); view deltas land via the
                    // circuit-evaluation terminal-view moves, which never
                    // project into `index_circuits`. An index on a view would
                    // backfill once and then silently serve stale results. The
                    // SQL binder rejects this by name resolution; this precheck
                    // rejects a raw wire push before the row is persisted or
                    // broadcast, and `hook_index_register` re-checks for the
                    // paths that skip precheck (boot replay, worker ddl_sync).
                    if !entry.kind.is_base_table() {
                        return Err(format!("Index: owner {} is not a base table", owner_id));
                    }

                    // Bounds, per-column eligibility (STRING/BLOB/float), and
                    // arity/stride limits, identical to what registration will
                    // enforce — only the table-name context is added here.
                    make_index_schema(cols.as_slice(), &entry.schema).map_err(|e| {
                        format!(
                            "{} for table '{}' (tid={})",
                            e,
                            self.caches.entity_by_id.get(&owner_id)
                                .map(|(_, n)| n.as_str()).unwrap_or("?"),
                            owner_id)
                    })?;

                    let idx_id = batch.get_pk(i) as i64;
                    if let Some(&existing) = self.caches.index_by_name.get(&index_name) {
                        if existing != idx_id {
                            return Err(format!("Index already exists: {}", index_name));
                        }
                    }
                }
                _ => {}
            }
        }

        // -- Negative-weight (DROP) checks ------------------------------------
        let mut drop_ids: Vec<i64> = Vec::new();
        for i in 0..batch.count {
            if batch.get_weight(i) < 0 {
                drop_ids.push(batch.get_pk(i) as i64);
            }
        }
        if drop_ids.is_empty() {
            return Ok(());
        }
        drop_ids.sort_unstable();
        drop_ids.dedup();

        // A UNIQUE index referenced by a FK, and any internal `__fk_` index the
        // RESTRICT seek depends on, are load-bearing — block their drop. A
        // DROP TABLE cascade legitimately retracts the owner's own indices, so
        // it is exempt (the table drop already passed its own precheck).
        if table_id == IDX_TAB_ID {
            if self.ctx.in_cascade_drop() {
                return Ok(());
            }
            for &idx_id in &drop_ids {
                if let Some(name) = self.caches.index_by_id.get(&idx_id) {
                    if name.contains(FK_INDEX_INFIX) {
                        return Err("Integrity violation: cannot drop an internal FK index".into());
                    }
                }
                let (owner_id, cols) = {
                    let mut cursor = self.sys_indices.open_cursor();
                    // sys_indices has a single U64 PK; OPK == big-endian.
                    cursor.cursor.seek_bytes(&(idx_id as u64).to_be_bytes());
                    if !cursor.cursor.valid || cursor.cursor.current_key as u64 != idx_id as u64 {
                        continue;
                    }
                    (cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64,
                     gnitz_wire::unpack_pk_cols(cursor_read_u64(&cursor, IDXTAB_COL_SOURCE_COLS)))
                };
                // FK backing is single-column: a composite index never satisfies
                // a single-column FK/uniqueness requirement, so dropping
                // one is never blocked by the FK-target guard.
                if cols.as_slice().len() != 1 { continue; }
                let src_col = cols.as_slice()[0] as usize;
                if self.fk_children_of(owner_id).iter().any(|r| r.parent_col_idx == src_col) {
                    // The FK target column must retain uniqueness for FK child
                    // inserts to validate. The drop is allowed when uniqueness is
                    // structurally preserved: the column is the lone PK (the PK
                    // itself guarantees uniqueness), or another unique secondary
                    // index survives the drop.
                    let is_lone_pk = self.dag.tables.get(&owner_id).is_some_and(|e| {
                        let pk = e.schema.pk_indices();
                        pk.len() == 1 && pk[0] as usize == src_col
                    });
                    if !is_lone_pk {
                        // Scan sys_indices (pre-drop: the rows being dropped are
                        // still present) for any other unique index on this column
                        // that would survive (exclude every id in this drop batch).
                        let mut unique_remains = false;
                        self.for_each_index_on_cols(owner_id, &[src_col as u32], |row_id, is_uniq| {
                            if is_uniq && drop_ids.binary_search(&row_id).is_err() {
                                unique_remains = true;
                            }
                        });
                        if !unique_remains {
                            let (sn, tn) = self.caches.entity_by_id.get(&owner_id)
                                .cloned().unwrap_or_default();
                            return Err(format!(
                                "Integrity violation: index on '{sn}.{tn}' is referenced by a \
                                 foreign key and no unique index would remain on the column"));
                        }
                    }
                }
            }
            return Ok(());
        }

        if table_id == TABLE_TAB_ID {
            for &tid in &drop_ids {
                // A FK child being co-dropped in this same batch is
                // self-resolving — only a child *outside* the batch blocks the
                // drop. Mirrors the view-dependency binary_search filter below.
                let blocking = self.fk_children_of(tid)
                    .iter()
                    .find(|r| drop_ids.binary_search(&r.child_tid).is_err());
                if let Some(r) = blocking {
                    let (sn, tn) = self.caches.entity_by_id.get(&r.child_tid)
                        .cloned().unwrap_or_default();
                    return Err(format!(
                        "Integrity violation: table referenced by '{}.{}'", sn, tn
                    ));
                }
            }
        }

        // The view-dependency guard applies only to TABLE/VIEW drops. A
        // SCHEMA_TAB drop carries the schema id, and schema ids (allocated from
        // FIRST_USER_SCHEMA_ID = 3) share an i64 space with table ids (from
        // FIRST_USER_TABLE_ID = 16): as both counters climb, a schema id
        // eventually equals an earlier table id. dep_map is keyed by *table*
        // id, so probing it with a schema id would spuriously match an
        // unrelated table's dependents. A schema's own members were already
        // dropped (and individually dep-checked) by the drop_schema cascade
        // before this row is emitted, so the schema row itself needs no guard.
        if table_id == TABLE_TAB_ID || table_id == VIEW_TAB_ID {
            let dep_map = self.dag.get_dep_map();
            for &id in &drop_ids {
                if let Some(dependents) = dep_map.get(&id) {
                    // A dependent that is itself being dropped in this same
                    // batch is self-resolving — only an *outside* dependent
                    // blocks the drop. drop_ids is sorted+deduped above, so
                    // binary_search is O(N log M) vs the O(N·M) of `contains`.
                    let still_active = dependents.iter()
                        .any(|&dep_id| drop_ids.binary_search(&dep_id).is_err());
                    if still_active {
                        let (sn, tn) = self.caches.entity_by_id.get(&id)
                            .cloned().unwrap_or_default();
                        return Err(format!("View dependency: entity '{}.{}'", sn, tn));
                    }
                }
            }
        }
        Ok(())
    }

    /// Drain the pending-broadcast queue. Master calls this once per
    /// top-level DDL and forwards each entry to `broadcast_ddl`. Workers
    /// receive system-table changes via FLAG_DDL_SYNC → `ddl_sync`, which
    /// bypasses `ingest_to_family` entirely, so the queue stays empty there.
    pub fn drain_pending_broadcasts(&mut self) -> Vec<(i64, Batch)> {
        std::mem::take(&mut self.pending_broadcasts)
    }

    /// Physically remove a batch of queued directory paths. An existence guard
    /// keeps a re-queued path (drop applied, dir already gone) quiet.
    fn remove_queued_dirs(dirs: Vec<String>) {
        for dir in dirs {
            if std::path::Path::new(&dir).exists() {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
    }

    /// Physically remove the directories queued by table/view/index drop
    /// hooks. The executor calls this only after the DDL zone's fdatasync
    /// confirms the drop is durable — see `pending_dir_deletions`.
    pub fn drain_pending_dir_deletions(&mut self) {
        Self::remove_queued_dirs(std::mem::take(&mut self.pending_dir_deletions));
    }

    /// Drop the queued directory paths *without* deleting them. Used when a DDL
    /// fails or to clear stale entries left by a prior failed DDL — the entity
    /// did not durably drop, so its files must survive.
    pub fn discard_pending_dir_deletions(&mut self) {
        self.pending_dir_deletions.clear();
    }

    /// Move durably-dropped directories from the in-flight DDL queue into the
    /// checkpoint-gated queue instead of removing them now. See
    /// `checkpoint_gated_deletions`. Used on the DROP-success path where worker
    /// processes may still be applying the entity's CREATE.
    pub fn defer_pending_dir_deletions(&mut self) {
        self.checkpoint_gated_deletions
            .append(&mut self.pending_dir_deletions);
    }

    /// Physically remove every checkpoint-gated directory. SAFE only at a
    /// checkpoint boundary, after the per-worker FLAG_FLUSH ACKs prove all
    /// workers consumed past the DROP that queued each entry.
    pub fn drain_checkpoint_gated_deletions(&mut self) {
        Self::remove_queued_dirs(std::mem::take(&mut self.checkpoint_gated_deletions));
    }

    /// Cancel a pending removal of `dir` from *both* deletion queues. Required
    /// when an entity whose on-disk path is *name-based* (not `<name>_<tid>`) is
    /// recreated before the gating checkpoint drains the queue: only schemas
    /// have name-based paths (`<base>/<schema>`), so a `DROP SCHEMA s` +
    /// `CREATE SCHEMA s` would otherwise leave `<base>/s` queued, and the next
    /// checkpoint's `remove_dir_all` would wipe the recreated schema and every
    /// new table beneath it. Tables/indices encode a monotonic tid in their
    /// path, so a recreate never collides with a gated entry and needs no
    /// cancellation.
    ///
    /// Both queues are filtered: in normal operation the DROP and CREATE are
    /// separate DDL RPCs, so the DROP's entry was already moved to the gated
    /// queue by its own `defer` and clearing `pending_dir_deletions` is a no-op.
    /// During SAL recovery, however, a replayed `DROP s` and a replayed
    /// `CREATE s` land in the *same* `pending_dir_deletions` with no intervening
    /// `defer`; the CREATE's own pre-stage `truncate` removes only its push and
    /// leaves the DROP's `<base>/s` — the live, recreated path — in the queue.
    /// Clearing it here lets the recreating hook reclaim that residue before the
    /// boot-time `gc_orphan_directories` drain (or any later checkpoint) can
    /// `remove_dir_all` the live schema and the tables beneath it.
    pub fn cancel_gated_deletion(&mut self, dir: &str) {
        self.checkpoint_gated_deletions.retain(|d| d != dir);
        self.pending_dir_deletions.retain(|d| d != dir);
    }

    /// Remove table, view, and index directories on disk that belong to no live
    /// entity — the residue of a DROP whose checkpoint-gated deletion was lost to
    /// a crash before the next checkpoint drained it. Best-effort: a failure to
    /// remove one orphan is logged and never aborts recovery.
    ///
    /// Two reclamation mechanisms run here:
    /// - A schema-scoped path scan removes orphan table/view/index dirs under
    ///   every live schema (the drop-flushed-but-gating-checkpoint-missed window,
    ///   where the entity is absent from both the shard scan and the SAL, so
    ///   nothing re-queues its dir).
    /// - A `drain_pending_dir_deletions` removes every dir that SAL replay
    ///   re-queued (the drop-committed-to-SAL-but-unflushed window), including a
    ///   dropped schema subtree the path scan cannot reach because the schema is
    ///   gone from `schema_by_id`.
    ///
    /// Must run only after BOTH shard replay (`replay_catalog`) and SAL replay
    /// (`recover_system_tables_from_sal`) have populated `dag.tables`; otherwise a
    /// table whose CREATE committed to the SAL but was not yet flushed would be
    /// absent from `dag.tables` and its live directory wrongly deleted.
    ///
    /// Requires the `cancel_gated_deletion` fix that also clears
    /// `pending_dir_deletions`; without it the drain could remove a recreated
    /// same-name schema whose live path SAL replay left in the queue.
    pub(crate) fn gc_orphan_directories(&mut self) {
        // Full on-disk path of every live table/view (user + system).
        let live_tables: rustc_hash::FxHashSet<&str> =
            self.dag.tables.values().map(|e| e.directory.as_str()).collect();

        // Full on-disk path of every live index: `<owner_dir>/idx_<idx_id>`.
        let mut live_indices: rustc_hash::FxHashSet<String> = rustc_hash::FxHashSet::default();
        for (owner_id, idx_ids) in &self.caches.indices_by_owner {
            if let Some(owner) = self.dag.tables.get(owner_id) {
                for idx_id in idx_ids {
                    live_indices.insert(index_dir(&owner.directory, *idx_id));
                }
            }
        }

        // Scan only schemas the catalog knows about. We never enumerate
        // `base_dir` for unknown directories: a schema path is an arbitrary user
        // name with no structural marker (`<base>/<schema>`), so removing an
        // unrecognized entry could wipe unrelated host data if base_dir is
        // shared. The real system catalog (`<base>/_system_catalog`) is never a
        // registered schema name, so it is never reached; the `_system`/`public`
        // logical-schema dirs are scanned but the system tables live under
        // `_system_catalog`, so nothing system-owned is ever a candidate.
        for schema_name in self.caches.schema_by_id.values() {
            let schema_dir = schema_dir(&self.base_dir, schema_name);
            for name in subdir_names(&schema_dir) {
                let full = format!("{}/{}", schema_dir, name);

                if live_tables.contains(full.as_str()) {
                    // Live table/view: sweep orphaned `idx_<id>` sub-dirs left by
                    // a standalone DROP INDEX whose gated deletion was lost to a
                    // crash.
                    for idx_name in subdir_names(&full) {
                        if !is_index_dir_name(&idx_name) {
                            continue;
                        }
                        let idx_full = format!("{}/{}", full, idx_name);
                        if live_indices.contains(&idx_full) {
                            continue;
                        }
                        match std::fs::remove_dir_all(&idx_full) {
                            Ok(()) => gnitz_debug!("recovery: removed orphan index dir {}", idx_full),
                            Err(e) => gnitz_debug!("recovery: failed to remove orphan index dir {}: {}", idx_full, e),
                        }
                    }
                    continue;
                }

                // Defense in depth: only `<something>_<digits>` dirs — the shape
                // of table (`<name>_<tid>`) and view (`view_<name>_<vid>`)
                // creation — are eligible for removal. Never touch an unexpected
                // entry. The only component that writes a directory directly
                // under a schema dir is table/view creation, so a table-shaped
                // name absent from `live_tables` is necessarily an orphaned drop.
                if !is_table_dir_name(&name) {
                    continue;
                }
                match std::fs::remove_dir_all(&full) {
                    Ok(()) => gnitz_debug!("recovery: removed orphan table/view dir {}", full),
                    Err(e) => gnitz_debug!("recovery: failed to remove orphan dir {}: {}", full, e),
                }
            }
        }

        // SAL replay of any DROP fired hooks that re-pushed the dropped directory
        // onto `pending_dir_deletions` (the committed-but-unflushed crash window).
        // The schema-scoped scan above already removed the orphans under live
        // schemas, but a dropped *schema*'s subtree is unreachable by that scan
        // (the schema is gone from `schema_by_id`). Physically remove everything
        // the replay re-queued so those dirs are reclaimed and no recovery residue
        // is carried into the first DDL/checkpoint. Safe because the
        // `cancel_gated_deletion` fix guarantees no recreated same-name (live)
        // schema path survives in the queue.
        self.drain_pending_dir_deletions();
    }

    // -----------------------------------------------------------------------
    // Stage-A compensation (DDL rollback)
    // -----------------------------------------------------------------------

    /// Topological creation priority for rollback sort. Lower = earlier in the
    /// dependency chain (created first, destroyed last). Priorities must be
    /// distinct for TABLE_TAB and VIEW_TAB so the relative order is stable.
    fn rollback_topo_priority(tid: i64) -> u8 {
        match tid {
            SCHEMA_TAB_ID                => 0,
            COL_TAB_ID                   => 1,
            DEP_TAB_ID                   => 2,
            CIRCUIT_NODES_TAB_ID         => 3,
            CIRCUIT_EDGES_TAB_ID         => 4,
            CIRCUIT_NODE_COLUMNS_TAB_ID  => 5,
            TABLE_TAB_ID                 => 6,
            VIEW_TAB_ID                  => 7,
            IDX_TAB_ID                   => 8,
            _                            => 99,
        }
    }

    /// Negate every row's weight in-place.
    pub(crate) fn negate_batch_in_place(batch: &mut Batch) {
        for i in 0..batch.count {
            let w = batch.get_weight(i);
            let negated = (-w).to_le_bytes();
            let off = i * 8;
            batch.weight_data_mut()[off..off + 8].copy_from_slice(&negated);
        }
    }

    /// Compensate a failed Stage-A DDL: undo every in-memory mutation that
    /// was applied before the failure so the catalog is exactly as it was.
    ///
    /// Called from the executor's Stage-A error arm. `original_batch` is the
    /// batch that was passed to `ingest_to_family` before the failure.
    pub(crate) fn compensate_stage_a(
        &mut self,
        target_id: i64,
        original_batch: &Batch,
    ) {
        let mut rollback_list = self.drain_pending_broadcasts();

        // pending_broadcasts is children-before-parents: the top-level batch is
        // pushed last (after fire_hooks returns Ok). It is present iff fire_hooks
        // succeeded (evaluate_dag then panicked); absent iff fire_hooks itself
        // failed. Use iter().any() for robustness over .last().
        let top_level_present = rollback_list.iter().any(|(tid, _)| *tid == target_id);
        if !top_level_present {
            let mut b = original_batch.clone();
            b.set_schema(sys_tab_schema(target_id));
            rollback_list.push((target_id, b));
        }

        let is_create = (0..original_batch.count)
            .any(|i| original_batch.get_weight(i) > 0);

        debug_assert!(
            original_batch.count == 0
                || (0..original_batch.count).all(|i| original_batch.get_weight(i) > 0)
                || (0..original_batch.count).all(|i| original_batch.get_weight(i) < 0),
            "compensate_stage_a assumes a weight-homogeneous top-level batch; a mixed \
             CREATE/DROP batch would misclassify the rollback direction and mishandle \
             pending_dir_deletions"
        );

        // For CREATE rollback: dependents unregistered before dependencies — DESCENDING.
        // For DROP rollback: dependencies restored before dependents — ASCENDING.
        if is_create {
            rollback_list.sort_by_key(|(tid, _)| std::cmp::Reverse(Self::rollback_topo_priority(*tid)));
        } else {
            rollback_list.sort_by_key(|(tid, _)| Self::rollback_topo_priority(*tid));
        }

        // Replay each with negated weight through the no-broadcast path.
        // fire_hooks still fires so caches, dag.tables, and pending_dir_deletions
        // are updated. The rollback gate in `submit` ensures any cascade that
        // calls back into `submit` also bypasses broadcasts.
        let result = self.with_rollback_compensation(|s| -> Result<(), String> {
            for (tid, mut batch) in rollback_list {
                Self::negate_batch_in_place(&mut batch);
                let family = SysFamily::from_id(tid)
                    .ok_or_else(|| format!("rollback: unknown system family {}", tid))?;
                s.submit_local(family, batch)?;
            }
            Ok(())
        });

        // For CREATE: drain cleans up any pre-staged directories from hooks.
        // For DROP: discard keeps the entity files on disk (drop was not durable).
        if is_create {
            self.drain_pending_dir_deletions();
        } else {
            self.discard_pending_dir_deletions();
        }

        result.unwrap_or_else(|e| {
            gnitz_fatal_abort!(
                "Stage-A DDL compensation failed — catalog cannot be restored; \
                 aborting to prevent serving a diverged catalog. Cause: {}", e
            );
        });
    }

    /// Ingest a user-table batch and return the effective delta (after unique_pk
    /// dedup).  Used by multi-worker push where the worker needs the effective
    /// batch for later DAG evaluation but does NOT evaluate immediately.
    /// System tables are NOT supported (use `ingest_to_family` for those).
    pub fn ingest_returning_effective(
        &mut self, table_id: i64, batch: Batch,
    ) -> Result<Batch, String> {
        if table_id < FIRST_USER_TABLE_ID {
            return Err("ingest_returning_effective not supported for system tables".to_string());
        }
        let (rc, effective_opt) = self.dag.ingest_returning_effective(table_id, batch);
        if rc < 0 {
            return Err(format!("ingest failed for table_id={} rc={}", table_id, rc));
        }
        match effective_opt {
            Some(eff) => Ok(eff),
            None => Err(format!("ingest returned no effective batch for table_id={}", table_id)),
        }
    }

    /// Scan all positive-weight rows from a table.
    pub fn scan_family(&mut self, table_id: i64) -> Result<Rc<Batch>, String> {
        let schema = if table_id < FIRST_USER_TABLE_ID {
            sys_tab_schema(table_id)
        } else {
            self.dag.tables.get(&table_id)
                .map(|e| e.schema)
                .ok_or_else(|| format!("Unknown table_id {}", table_id))?
        };
        // The CIRCUIT_* tables are now SQL-introspectable: every operator's
        // parameter shape is expressible in catalog schema.
        if let Some(table) = self.sys_table_mut(table_id) {
            return Ok(table.full_scan());
        }
        Ok(self.scan_store(table_id, &schema))
    }

    /// Point lookup by PK. Returns a single-row batch if found, None otherwise.
    pub fn seek_family(&mut self, table_id: i64, pk: u128) -> Result<Option<Batch>, String> {
        let schema = if table_id < FIRST_USER_TABLE_ID {
            sys_tab_schema(table_id)
        } else {
            self.dag.tables.get(&table_id)
                .map(|e| e.schema)
                .ok_or_else(|| format!("Unknown table_id {}", table_id))?
        };

        // Create cursor and seek. The new CircuitNodes/CircuitEdges/
        // CircuitNodeColumns layout is SQL-introspectable; only unrecognised
        // system table IDs return None.
        let mut cursor = if table_id < FIRST_USER_TABLE_ID {
            if let Some(table) = self.sys_table_mut(table_id) {
                table.open_cursor()
            } else {
                return Ok(None);
            }
        } else {
            self.dag.tables.get(&table_id).unwrap().handle.open_cursor()
        };

        let (opk, stride) = crate::storage::opk_key(&schema, pk);
        cursor.cursor.seek_bytes(&opk[..stride]);
        if !cursor.cursor.valid { return Ok(None); }
        if cursor.cursor.current_pk_bytes() != &opk[..stride] {
            return Ok(None);
        }
        if cursor.cursor.current_weight <= 0 { return Ok(None); }

        let mut batch = Batch::with_schema(schema, 1);
        self.copy_cursor_row_to_batch(&cursor, &mut batch);
        Ok(Some(batch))
    }

    /// Byte-keyed sibling of [`seek_family`]: point lookup by full PK bytes,
    /// correct for `pk_stride > 16` where a `u128` key cannot encode the PK.
    /// Mirrors `seek_family` exactly — open the merged cursor, `seek_bytes`,
    /// confirm exact PK and positive weight.
    pub fn seek_family_bytes(&mut self, table_id: i64, pk: &[u8]) -> Result<Option<Batch>, String> {
        let schema = if table_id < FIRST_USER_TABLE_ID {
            sys_tab_schema(table_id)
        } else {
            self.dag.tables.get(&table_id)
                .map(|e| e.schema)
                .ok_or_else(|| format!("Unknown table_id {}", table_id))?
        };

        let mut cursor = if table_id < FIRST_USER_TABLE_ID {
            match self.sys_table_mut(table_id) {
                Some(table) => table.open_cursor(),
                None => return Ok(None),
            }
        } else {
            self.dag.tables.get(&table_id).unwrap().handle.open_cursor()
        };

        cursor.cursor.seek_bytes(pk);
        if !cursor.cursor.valid { return Ok(None); }
        if cursor.cursor.current_pk_bytes() != pk { return Ok(None); }
        if cursor.cursor.current_weight <= 0 { return Ok(None); }

        let mut batch = Batch::with_schema(schema, 1);
        self.copy_cursor_row_to_batch(&cursor, &mut batch);
        Ok(Some(batch))
    }

    /// Batched point lookup. Open one cursor on `table_id` and seek each PK in
    /// `pks` (verbatim OPK bytes), appending the stored row (weight 1) for every
    /// present, live key into a result batch projected to `project`. Each `seek`
    /// re-probes every source independently, so order is not required for
    /// correctness; passing `pks` ascending keeps the per-source binary-search
    /// probes monotonic for better cache locality.
    /// Absent / retracted keys are skipped — identical to `seek_family`'s
    /// single-key `None` — so a removed PK with no committed row contributes
    /// nothing. `project` lists the parent column indices to return (all
    /// non-PK scalar columns); an empty `project` returns PK-only rows.
    ///
    /// Reuses one cursor across all keys (cheaper than N `seek_family` calls,
    /// each of which re-opens a cursor). Projection keeps the result scalar-
    /// only — FK-referenced columns are never STRING/BLOB — so the blob arena
    /// is never touched. Works for both narrow and wide PKs: the OPK bytes are
    /// seeked verbatim, with no native→OPK re-encode.
    pub fn gather_family_bytes(
        &mut self,
        table_id: i64,
        pks: &[crate::storage::PkBuf],
        project: &[u8],
    ) -> Result<Batch, String> {
        let schema = self.dag.tables.get(&table_id)
            .map(|e| e.schema)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;
        let result_schema = project_schema(&schema, project);
        let mut out = Batch::with_schema(result_schema, pks.len());
        let mut cursor = self.dag.tables.get(&table_id).unwrap().handle.open_cursor();
        for pk in pks {
            let bytes = pk.pk_bytes();
            cursor.cursor.seek_bytes(bytes);
            if !cursor.cursor.valid { continue; }
            if cursor.cursor.current_pk_bytes() != bytes { continue; }
            if cursor.cursor.current_weight <= 0 { continue; }
            copy_cursor_cols_to_batch(&cursor, &mut out, &schema, project);
        }
        Ok(out)
    }

    /// Index-assisted lookup: prefix-scan the index by `natives` — the native
    /// key values of the leading `natives.len()` indexed columns
    /// (`natives.len()` may be `< col_indices.len()` for a leading-prefix
    /// scan) — reconstruct the source PK from the index PK suffix, and resolve
    /// to the source rows.
    ///
    /// Rows with a NULL in ANY indexed column are absent from the index
    /// (`batch_project_index` skips them), so a prefix scan returns only rows
    /// whose trailing indexed columns are all non-NULL — the SQL planner must
    /// not serve a prefix predicate from an index whose uncovered trailing
    /// columns are nullable.
    pub fn seek_by_index(&mut self, table_id: i64, col_indices: &[u32], natives: &[u128])
        -> Result<Option<Batch>, String>
    {
        let entry = self.dag.tables.get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;

        let ic = entry.index_circuits.iter()
            .find(|ic| ic.col_indices.as_slice() == col_indices)
            .ok_or_else(|| format!("No index on cols {:?} for table {}", col_indices, table_id))?;

        let src_pk_stride = entry.schema.pk_stride() as usize;
        // The full leading-key region width = sum over ALL indexed columns'
        // promoted widths; the source PK suffix begins there.
        let idx_key_size: usize = (0..col_indices.len())
            .map(|i| ic.index_schema.columns[i].size() as usize)
            .sum();

        // OPK-encode each supplied native into its slot. The index PK region is
        // OPK-at-rest, so the prefix must be order-preserving to match stored
        // entries (`batch_project_index` encodes identically).
        let (opk, prefix_len) = crate::schema::index_opk_prefix_composite(
            natives, &ic.index_schema.columns[..natives.len()],
        );
        let opk_prefix = &opk[..prefix_len];

        let idx_table = ic.table_mut();
        let mut cursor = idx_table.open_cursor();

        // Seek to the first positive-weight match, then walk forward with
        // `walk_to_positive_with_prefix` after each consumed entry. Re-calling
        // `seek_first_positive_with_prefix` inside the loop would re-seek and
        // re-find the same entry forever — an orphaned index entry (positive
        // weight, no source row) would spin.
        // A non-unique indexed value matches multiple rows; accumulate ALL of
        // them on this worker, not just the first. (A unique index yields one
        // match, so this is equivalent there.) Index entries co-locate with
        // their source rows (partitioned by source PK), so a value's matches can
        // be spread across workers: this returns one worker's partial set and
        // the master (`fan_out_seek_by_index_collect_async`) merges across all.
        let mut result: Option<Batch> = None;
        let mut hit = cursor.cursor.seek_first_positive_with_prefix(opk_prefix);
        while hit {
            // Copy the source PK out of the index PK suffix into a fixed buffer
            // before the `&mut self` resolve below releases the cursor borrow.
            // Widened to MAX_PK_BYTES so wide (`src_pk_stride > 16`) sources
            // resolve via `seek_family_bytes`.
            let current_pk = cursor.cursor.current_pk_bytes();
            let mut src_pk_buf = [0u8; crate::schema::MAX_PK_BYTES];
            src_pk_buf[..src_pk_stride].copy_from_slice(
                &current_pk[idx_key_size..idx_key_size + src_pk_stride],
            );
            if let Some(batch) = self.seek_family_bytes(table_id, &src_pk_buf[..src_pk_stride])? {
                match result.as_mut() {
                    Some(acc) => acc.append_batch(&batch, 0, batch.count),
                    None => result = Some(batch),
                }
            }
            cursor.cursor.advance();
            hit = cursor.cursor.walk_to_positive_with_prefix(opk_prefix);
        }
        Ok(result)
    }

    /// Flush a table's WAL.
    pub fn flush_family(&mut self, table_id: i64) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            if let Some(table) = self.sys_table_mut(table_id) {
                table.flush().map_err(|e| format!("flush error: {}", e))?;
                // Compact so L0 shards don't accumulate without bound across
                // DDL-heavy sessions (system catalog tables are scanned on every
                // boot and DDL op).
                table.compact_if_needed().map_err(|e| format!("compaction error: {:?}", e))?;
            }
            Ok(())
        } else {
            let rc = self.dag.flush(table_id);
            if rc < 0 {
                Err(format!("flush failed for table_id={} rc={}", table_id, rc))
            } else {
                Ok(())
            }
        }
    }

    /// Phase 1 across a table family. System tables flush inline (legacy
    /// path; they checkpoint during DDL, not at `flush_all`).
    pub fn flush_family_prepare(
        &mut self,
        table_id: i64,
    ) -> Result<Vec<(usize, crate::storage::FlushWork)>, String> {
        if table_id < FIRST_USER_TABLE_ID {
            if let Some(table) = self.sys_table_mut(table_id) {
                table.flush().map_err(|e| format!("flush error: {}", e))?;
                table.compact_if_needed().map_err(|e| format!("compaction error: {:?}", e))?;
            }
            Ok(Vec::new())
        } else {
            self.dag.flush_prepare(table_id)
        }
    }

    /// Phase 3 across a table family. Returns dirfds to fsync.
    pub fn flush_family_commit_batch(
        &mut self,
        table_id: i64,
        works: Vec<(usize, crate::storage::FlushWork)>,
    ) -> Result<Vec<libc::c_int>, String> {
        if table_id < FIRST_USER_TABLE_ID {
            // System tables commit inline; no FlushWork should arrive here.
            debug_assert!(works.is_empty());
            Ok(Vec::new())
        } else {
            self.dag.flush_commit_batch(table_id, works)
        }
    }

    /// Worker DDL sync: memonly ingest into system table + fire hooks.
    /// Workers receive DDL deltas from master and need to update their registry
    /// without writing to WAL (master owns durability).
    pub fn ddl_sync(&mut self, table_id: i64, batch: Batch) -> Result<(), String> {
        let family = SysFamily::from_id(table_id)
            .ok_or_else(|| "ddl_sync only for system tables".to_string())?;
        // Fire hooks first (borrow only); the ingest below moves `batch`.
        // Hooks have no observable ordering dependency on the storage write.
        self.fire_hooks(family, &batch)?;
        let table = self.sys_table_mut(table_id)
            .ok_or_else(|| format!("Unknown system table_id {}", table_id))?;
        let _ = table.ingest_owned_batch_memonly(batch);
        Ok(())
    }

    /// Raw store ingest: SAL recovery path — no unique_pk, no hooks, no index projection.
    pub fn raw_store_ingest(&mut self, table_id: i64, batch: Batch) -> Result<(), String> {
        let entry = self.dag.tables.get(&table_id)
            .ok_or_else(|| format!("Unknown table_id {}", table_id))?;
        let _ = entry.handle.ingest_owned_batch(batch);
        Ok(())
    }

    /// SAL recovery replay — unique_pk-aware. Routes user-table batches
    /// through the full `ingest_returning_effective` path so that
    /// retractions (which carry zero-padded payloads on the wire) are
    /// resolved against the actual stored payload instead of being
    /// added as orphaned rows. The retract-and-insert pattern in
    /// `enforce_unique_pk_partitioned` makes the replay idempotent
    /// w.r.t. already-flushed data.
    ///
    /// Index shards see duplicate `(+1, -1)` projections when a batch
    /// is replayed after already having been flushed. These consolidate
    /// to zero on read and are pruned at the next compaction.
    pub fn replay_ingest(&mut self, table_id: i64, batch: Batch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            // System tables: use raw ingest (no unique_pk semantics).
            return self.raw_store_ingest(table_id, batch);
        }
        let (rc, _effective) = self.dag.ingest_returning_effective(table_id, batch);
        if rc < 0 {
            return Err(format!("replay_ingest failed for table_id={} rc={}", table_id, rc));
        }
        Ok(())
    }

    // -- Partition management (for multi-worker fork) -------------------------

    /// Set active partition range for user tables.
    pub fn set_active_partitions(&mut self, start: u32, end: u32) {
        self.active_part_start = start;
        self.active_part_end = end;
    }

    /// Close all partitions in user tables (master after fork). System tables
    /// hold Borrowed (non-partitioned) handles, so the filter is the handle.
    pub fn close_user_table_partitions(&mut self) {
        for entry in self.dag.tables.values() {
            if let Some(ptable) = entry.handle.as_partitioned_mut() {
                ptable.close_all_partitions();
            }
        }
    }

    /// Trim worker partitions to assigned range. System tables hold Borrowed
    /// (non-partitioned) handles, so the filter is the handle.
    pub fn trim_worker_partitions(&mut self, start: u32, end: u32) {
        for entry in self.dag.tables.values() {
            if let Some(ptable) = entry.handle.as_partitioned_mut() {
                ptable.close_partitions_outside(start, end);
            }
        }
    }

    /// Invalidate all cached plans.
    pub fn invalidate_all_plans(&mut self) {
        self.dag.invalidate_all();
    }

    // -- FK / index metadata queries (for distributed validation) -------------

    /// Number of FK constraints on a table.
    pub fn get_fk_count(&self, table_id: i64) -> usize {
        self.caches.fk_by_child.get(&table_id).map(|v| v.len()).unwrap_or(0)
    }

    /// Get FK constraint at index: (fk_col_idx, target_table_id, target_col_idx).
    pub fn get_fk_constraint(&self, table_id: i64, idx: usize) -> Option<(usize, i64, usize)> {
        self.caches.fk_by_child.get(&table_id)
            .and_then(|v| v.get(idx))
            .map(|c| (c.fk_col_idx, c.target_table_id, c.target_col_idx))
    }

    /// Number of index circuits on a table.
    pub fn get_index_circuit_count(&self, table_id: i64) -> usize {
        self.dag.tables.get(&table_id)
            .map(|e| e.index_circuits.len())
            .unwrap_or(0)
    }

    /// Get index circuit info at index: (col_indices, is_unique). Production
    /// consumers go through `unique_index_circuit_col` / `index_circuit_for_cols`;
    /// only the catalog tests enumerate raw circuit info.
    #[cfg(test)]
    pub fn get_index_circuit_info(&self, table_id: i64, idx: usize)
        -> Option<(PkColList, bool)>
    {
        let entry = self.dag.tables.get(&table_id)?;
        let ic = entry.index_circuits.get(idx)?;
        Some((ic.col_indices, ic.is_unique))
    }

    /// The source column of the unique circuit at position `idx`, `None` when
    /// the circuit is missing or non-unique. The unique-enforcement machinery
    /// (routing cache, filters, has_pk pre-checks) iterates circuits through
    /// this single accessor — see `IndexCircuitEntry::unique_col`.
    pub fn unique_index_circuit_col(&self, table_id: i64, idx: usize) -> Option<u32> {
        self.dag.tables.get(&table_id)?.index_circuits.get(idx)?.unique_col()
    }

    /// Get index store handle for an exact column list (for worker has_pk via
    /// a unique index — always single-column today).
    pub fn get_index_store_handle(&self, table_id: i64, cols: &[u32]) -> *const Table {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_indices.as_slice() == cols))
            .map(|ic| ic.table_mut() as *const Table)
            .unwrap_or(std::ptr::null())
    }

    /// Get the SchemaDescriptor for the index circuit at position idx.
    pub fn get_index_circuit_schema(&self, table_id: i64, idx: usize) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.get(idx))
            .map(|ic| ic.index_schema)
    }

    /// Number of child tables that reference `parent_id` via FK.
    pub fn get_fk_children_count(&self, parent_id: i64) -> usize {
        self.caches.fk_by_parent.get(&parent_id).map(|v| v.len()).unwrap_or(0)
    }

    /// The secondary index circuit on `col_idx` of `table_id`, if one exists.
    /// Single source of truth for "does this column have an index, and is it
    /// unique": the SEEK_BY_INDEX handler matches the `Option` once — `None`
    /// answers STATUS_NO_INDEX (so the SQL planner falls back to a scan or a
    /// CREATE INDEX hint without a prior catalog probe), `Some(ic)` routes by
    /// `ic.is_unique` (unicast a unique match to one worker, broadcast-and-merge
    /// a non-unique one). Returning the entry means callers scan the circuit
    /// list once and cannot ask whether a non-existent index is unique.
    pub fn index_circuit_for_cols(
        &self, table_id: i64, cols: &[u32],
    ) -> Option<&crate::dag::IndexCircuitEntry> {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_indices.as_slice() == cols))
    }

    /// True if the table has at least one unique secondary index circuit.
    /// Used to decide whether distributed unique-index validation is needed.
    /// Non-unique circuits (e.g. FK indices) do not count.
    pub fn has_any_unique_index(&self, table_id: i64) -> bool {
        self.dag.tables.get(&table_id)
            .map(|e| e.index_circuits.iter().any(|ic| ic.is_unique))
            .unwrap_or(false)
    }

    /// True if the table was created with `unique_pk=true`. Used by the
    /// distributed validator to decide whether Error-mode inserts need
    /// an against-store PK rejection broadcast.
    pub fn table_has_unique_pk(&self, table_id: i64) -> bool {
        self.dag.tables.get(&table_id)
            .map(|e| e.unique_pk())
            .unwrap_or(false)
    }

    /// True when `col_idx` is the table's SOLE PK column AND the table was
    /// created with `unique_pk`, so PK uniqueness is enforced on every ingest
    /// path (`enforce_unique_pk_partitioned` in Update mode,
    /// `needs_pk_rejection` in Error mode, recovery via `replay_ingest`) and
    /// no two live rows can share the column's value. Both halves are
    /// load-bearing: without `unique_pk` none of that enforcement runs, so
    /// even a sole PK column can carry duplicates across live rows, and a
    /// member of a compound PK is never trivially unique.
    pub fn col_is_enforced_unique_pk(&self, table_id: i64, col_idx: u32) -> bool {
        self.dag.tables.get(&table_id)
            .map(|e| e.unique_pk() && e.schema.pk_indices() == [col_idx])
            .unwrap_or(false)
    }

    /// Get child info at index: (child_table_id, fk_col_idx, parent_col_idx).
    pub fn get_fk_child_info(&self, parent_id: i64, idx: usize) -> Option<(i64, usize, usize)> {
        self.caches.fk_by_parent.get(&parent_id)
            .and_then(|v| v.get(idx))
            .map(|r| (r.child_tid, r.fk_col_idx, r.parent_col_idx))
    }

    /// Get the index schema for a specific column list's index on a table.
    pub fn get_index_schema_by_cols(&self, table_id: i64, cols: &[u32]) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_indices.as_slice() == cols))
            .map(|ic| ic.index_schema)
    }

    /// Get column names for a table/view. Cached after first lookup.
    pub fn get_column_names(&mut self, table_id: i64) -> Vec<String> {
        if let Some(names) = self.caches.col_names.get(&table_id) {
            return names.clone();
        }
        let names: Vec<String> = self.read_column_defs(table_id)
            .into_iter().map(|cd| cd.name).collect();
        self.caches.col_names.insert(table_id, names.clone());
        names
    }

    /// Get column names as byte vectors. Backed by col_names cache; lazy-populated.
    pub fn get_col_names_bytes(&mut self, table_id: i64) -> Rc<Vec<Vec<u8>>> {
        if let Some(bytes) = self.caches.col_names_bytes.get(&table_id) {
            return bytes.clone();
        }
        let names = self.get_column_names(table_id);
        let bytes = Rc::new(names.iter().map(|n| n.as_bytes().to_vec()).collect::<Vec<_>>());
        self.caches.col_names_bytes.insert(table_id, bytes.clone());
        bytes
    }

    /// Return the cached encoded schema wire block, current schema version,
    /// and derived wire properties (`wire_safe`, `wire_row_fixed_stride`)
    /// for `table_id`, or `None` if the block isn't yet cached. Wire props
    /// are paired with the block so they share invalidation.
    pub fn get_cached_schema_wire_block(&self, table_id: i64) -> Option<CachedSchemaWire> {
        let (block, wire_safe, wire_row_fixed_stride) =
            self.caches.schema_wire_cache.get(&table_id)?.clone();
        let version = self.caches.get_schema_version(table_id);
        Some(CachedSchemaWire { block, version, wire_safe, wire_row_fixed_stride })
    }

    /// Return the current schema version for `table_id` (1 if unknown).
    pub fn get_schema_version(&self, table_id: i64) -> u16 {
        self.caches.get_schema_version(table_id)
    }

    /// Return the current index-metadata version for `table_id` (1 if unknown).
    pub fn get_index_version(&self, table_id: i64) -> u8 {
        self.caches.get_index_version(table_id)
    }

    /// Store an encoded schema wire block in the cache, along with its
    /// derived wire properties. The two are written together so the
    /// invalidation in `clear_col_cache_no_bump` keeps them consistent.
    pub fn set_schema_wire_block(
        &mut self,
        table_id: i64,
        block: Rc<Vec<u8>>,
        wire_safe: bool,
        wire_row_fixed_stride: u32,
    ) {
        self.caches.schema_wire_cache.insert(table_id, (block, wire_safe, wire_row_fixed_stride));
    }

    /// Return the full set of table IDs that must be locked together for a
    /// write to `table_id`, sorted ascending to guarantee deadlock-free
    /// acquisition. Includes `table_id` itself plus all FK parents (to
    /// guard concurrent parent DELETE) and FK children (to guard concurrent
    /// child INSERT during a parent DELETE). Returns an empty vec if this
    /// table requires no lock at all.
    pub fn fk_lock_set(&self, table_id: i64) -> Vec<i64> {
        if !self.caches.needs_lock.contains(&table_id) {
            return Vec::new();
        }
        let mut tids = vec![table_id];
        if let Some(constraints) = self.caches.fk_by_child.get(&table_id) {
            for c in constraints {
                tids.push(c.target_table_id);
            }
        }
        if let Some(children) = self.caches.fk_by_parent.get(&table_id) {
            for r in children {
                tids.push(r.child_tid);
            }
        }
        tids.sort_unstable();
        tids.dedup();
        tids
    }

    // -- Store handle accessors -----------------------------------------------

    /// Get raw PartitionedTable handle for a user table.
    pub fn get_ptable_handle(&self, table_id: i64) -> Option<*mut PartitionedTable> {
        self.dag.tables.get(&table_id).and_then(|e| {
            match &e.handle {
                StoreHandle::Partitioned(cell) => Some(unsafe { &mut **cell.get() as *mut PartitionedTable }),
                _ => None,
            }
        })
    }

    /// Get schema descriptor for a table.
    pub fn get_schema_desc(&self, table_id: i64) -> Option<SchemaDescriptor> {
        if table_id > 0 && table_id < FIRST_USER_TABLE_ID {
            Some(sys_tab_schema(table_id))
        } else {
            self.dag.tables.get(&table_id).map(|e| e.schema)
        }
    }

    /// Get a raw mutable pointer to the DagEngine.
    pub fn get_dag_ptr(&mut self) -> *mut DagEngine {
        &mut self.dag as *mut DagEngine
    }

    // -- Iteration helpers ----------------------------------------------------

    /// Collect all user table IDs.
    pub fn iter_user_table_ids(&self) -> Vec<i64> {
        self.dag.tables.keys()
            .filter(|&&tid| tid >= FIRST_USER_TABLE_ID)
            .copied()
            .collect()
    }

    /// Get max flushed LSN for a table. Recovery itself reads the bulk map
    /// from `collect_all_flushed_lsns`; this single-table form is test-only.
    #[cfg(test)]
    pub fn get_max_flushed_lsn(&self, table_id: i64) -> u64 {
        if table_id > 0 && table_id < FIRST_USER_TABLE_ID {
            return self.sys_table_current_lsn(table_id);
        }
        let entry = match self.dag.tables.get(&table_id) {
            Some(e) => e,
            None => return 0,
        };
        entry.handle.current_lsn()
    }

    /// Read `current_lsn` from a system table by id. Returns 0 for unknown
    /// ids. Implemented without `&mut self` so recovery callers can hold
    /// other shared state.
    fn sys_table_current_lsn(&self, table_id: i64) -> u64 {
        let table: &Table = match table_id {
            SCHEMA_TAB_ID => &self.sys_schemas,
            TABLE_TAB_ID => &self.sys_tables,
            VIEW_TAB_ID => &self.sys_views,
            COL_TAB_ID => &self.sys_columns,
            IDX_TAB_ID => &self.sys_indices,
            DEP_TAB_ID => &self.sys_view_deps,
            SEQ_TAB_ID => &self.sys_sequences,
            CIRCUIT_NODES_TAB_ID => &self.sys_circuit_nodes,
            CIRCUIT_EDGES_TAB_ID => &self.sys_circuit_edges,
            CIRCUIT_NODE_COLUMNS_TAB_ID => &self.sys_circuit_node_columns,
            _ => return 0,
        };
        table.current_lsn
    }

    /// Build a map of every known table id → max flushed LSN, covering
    /// both system tables and user tables. Recovery uses this as the
    /// dedup filter for the unified two-pass walk.
    pub fn collect_all_flushed_lsns(&self) -> std::collections::HashMap<i64, u64> {
        let mut map = std::collections::HashMap::new();
        for &tid in SYS_TABLE_IDS {
            map.insert(tid, self.sys_table_current_lsn(tid));
        }
        for (&tid, entry) in self.dag.tables.iter() {
            if tid >= FIRST_USER_TABLE_ID {
                // recovery_lsn (min across partitions), not current_lsn (max):
                // a partial family flush must not over-skip a lagging
                // partition's still-in-SAL rows. The allocator below keeps the
                // max via max_table_current_lsn for its upper-bound needs.
                map.insert(tid, entry.handle.recovery_lsn());
            }
        }
        map
    }

    /// Maximum `current_lsn` across all tables — system and user. The
    /// executor seeds `shared.ingest_lsn` from this at boot and the DDL
    /// zone allocator re-reads it per DDL, so every allocated zone LSN is
    /// strictly greater than each table's current counter: no recovery
    /// watermark a checkpoint persisted can cover a committed-but-unflushed
    /// zone, and a failed zone's pinned LSN is never reused.
    pub fn max_table_current_lsn(&self) -> u64 {
        let mut max_lsn = 0u64;
        for &tid in SYS_TABLE_IDS {
            max_lsn = max_lsn.max(self.sys_table_current_lsn(tid));
        }
        for entry in self.dag.tables.values() {
            max_lsn = max_lsn.max(entry.handle.current_lsn());
        }
        max_lsn
    }

    // -- Read column definitions from sys_columns --------------------------

    /// Scan sys_columns for every positive-weight column record owned by
    /// `owner_id`, in column-index order. When `check_contiguity` is set the
    /// column indices (lower 9 bits of the packed PK) must run 0,1,2,… with no
    /// gap or duplicate — a gap would silently mismap columns in
    /// `build_schema_from_col_defs`, so the create-precheck path rejects it.
    /// The non-checking form is infallible by construction.
    pub(crate) fn scan_column_defs(
        &self,
        owner_id: i64,
        check_contiguity: bool,
    ) -> Result<Vec<ColumnDef>, String> {
        let start_pk = pack_column_id(owner_id, 0);
        let end_pk = pack_column_id(owner_id + 1, 0);
        let mut cursor = self.sys_columns.open_cursor();
        // sys_columns has a single U64 PK; OPK == big-endian.
        cursor.cursor.seek_bytes(&start_pk.to_be_bytes());

        let mut defs = Vec::new();
        let mut expected: i64 = 0;
        while cursor.cursor.valid {
            let pk = cursor.cursor.current_key as u64;
            if pk >= end_pk { break; }
            if cursor.cursor.current_weight > 0 {
                if check_contiguity {
                    // pack_column_id layout: owner_id << 9 | col_idx (lower 9 bits).
                    let actual = (pk & 0x1FF) as i64;
                    if actual != expected {
                        return Err(format!(
                            "entity (owner_id={}): column records are non-contiguous; \
                             expected index {}, got {}",
                            owner_id, expected, actual));
                    }
                    expected += 1;
                }
                defs.push(ColumnDef {
                    name:        cursor_read_string(&cursor, COLTAB_COL_NAME),
                    type_code:   cursor_read_u64(&cursor, COLTAB_COL_TYPE_CODE) as u8,
                    is_nullable: cursor_read_u64(&cursor, COLTAB_COL_IS_NULLABLE) != 0,
                    fk_table_id: cursor_read_u64(&cursor, COLTAB_COL_FK_TABLE_ID) as i64,
                    fk_col_idx:  cursor_read_u64(&cursor, COLTAB_COL_FK_COL_IDX) as u32,
                });
            }
            cursor.cursor.advance();
        }
        Ok(defs)
    }

    pub(crate) fn read_column_defs(&self, owner_id: i64) -> Vec<ColumnDef> {
        self.scan_column_defs(owner_id, false).unwrap()
    }

    pub(crate) fn build_schema_from_col_defs(&self, col_defs: &[ColumnDef], pk_cols: &[u32]) -> SchemaDescriptor {
        assert!(
            col_defs.len() <= crate::schema::MAX_COLUMNS,
            "build_schema_from_col_defs: too many columns ({}) for entity (type_codes: {:?})",
            col_defs.len(),
            col_defs.iter().map(|c| c.type_code).collect::<Vec<_>>(),
        );
        let mut cols = [zero_col(); crate::schema::MAX_COLUMNS];
        for (i, cd) in col_defs.iter().enumerate() {
            cols[i] = SchemaColumn::new(cd.type_code, if cd.is_nullable { 1 } else { 0 });
        }
        SchemaDescriptor::new(&cols[..col_defs.len()], pk_cols)
    }

    // -- FK constraint queries ---------------------------------------------

    /// Returns all child tables that have FK constraints targeting `parent_id`.
    pub(crate) fn fk_children_of(&self, parent_id: i64) -> &[FkParentRef] {
        self.caches.fk_by_parent.get(&parent_id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    // -- Batch field readers -----------------------------------------------

    pub(crate) fn read_batch_u64(&self, batch: &Batch, row: usize, payload_col: usize) -> u64 {
        let off = row * 8;
        let col = batch.col_data(payload_col);
        if off + 8 > col.len() { return 0; }
        u64::from_le_bytes(col[off..off + 8].try_into().unwrap_or([0; 8]))
    }

    pub(crate) fn read_batch_string(&self, batch: &Batch, row: usize, payload_col: usize) -> String {
        let off = row * 16;
        let data = batch.col_data(payload_col);
        if off + 16 > data.len() { return String::new(); }
        let st: [u8; 16] = data[off..off + 16].try_into().unwrap_or([0; 16]);
        let bytes = crate::schema::decode_german_string(&st, &batch.blob);
        String::from_utf8(bytes).unwrap_or_default()
    }

    // -- Registry query methods -----------------------------------------------

    pub fn has_id(&self, table_id: i64) -> bool {
        self.dag.tables.contains_key(&table_id)
    }

    // The following registry getters are exercised only by the catalog tests;
    // production code reads the caches/DAG entries directly.
    #[cfg(test)]
    pub fn get_schema(&self, table_id: i64) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id).map(|e| e.schema)
    }

    #[cfg(test)]
    pub fn get_schema_name_by_id(&self, schema_id: i64) -> &str {
        self.caches.schema_by_id.get(&schema_id).map(|s| s.as_str()).unwrap_or("")
    }

    #[cfg(test)]
    pub fn has_schema(&self, name: &str) -> bool {
        self.caches.schema_by_name.contains_key(name)
    }

    #[cfg(test)]
    pub fn get_schema_id(&self, name: &str) -> i64 {
        self.caches.schema_by_name.get(name).copied().unwrap_or(-1)
    }

    #[cfg(test)]
    pub fn schema_is_empty(&self, schema_name: &str) -> bool {
        let sid = match self.caches.schema_by_name.get(schema_name) {
            Some(&sid) => sid,
            None => return true,
        };
        let t_empty = self.caches.tables_by_schema.get(&sid).map(|s| s.is_empty()).unwrap_or(true);
        let v_empty = self.caches.views_by_schema.get(&sid).map(|s| s.is_empty()).unwrap_or(true);
        t_empty && v_empty
    }

    pub fn allocate_schema_id(&mut self) -> i64 {
        let sid = self.next_schema_id;
        self.next_schema_id += 1;
        sid
    }

    pub fn allocate_table_id(&mut self) -> i64 {
        let tid = self.next_table_id;
        self.next_table_id += 1;
        tid
    }

    pub fn allocate_index_id(&mut self) -> i64 {
        let iid = self.next_index_id;
        self.next_index_id += 1;
        iid
    }

    pub fn get_qualified_name(&self, table_id: i64) -> Option<(&str, &str)> {
        self.caches.entity_by_id.get(&table_id).map(|(s, t)| (s.as_str(), t.as_str()))
    }

    /// `(schema, table, column)` names for `(table_id, col_idx)`, each falling
    /// back to `"?"` when the catalog has no entry. The fallback is defensive:
    /// the entity always exists on the constraint-violation paths that format
    /// these names.
    fn qualified_col_names(&mut self, table_id: i64, col_idx: usize)
        -> (String, String, String)
    {
        let col = self.get_column_names(table_id)
            .get(col_idx).cloned().unwrap_or_else(|| "?".to_string());
        let (sn, tn) = self.get_qualified_name(table_id).unwrap_or(("?", "?"));
        (sn.to_string(), tn.to_string(), col)
    }

    /// Format a unique-index constraint violation naming the qualified table and
    /// offending column. `in_batch` appends the "duplicate in batch" qualifier
    /// used when two rows of one ingest batch collide, versus a collision with
    /// already-committed data. Single source of truth for this message — the
    /// distributed path (`MasterDispatcher`) delegates here.
    pub(crate) fn unique_violation_err(
        &mut self, table_id: i64, col_idx: usize, in_batch: bool,
    ) -> String {
        let (sn, tn, col) = self.qualified_col_names(table_id, col_idx);
        if in_batch {
            format!("Unique index violation on '{sn}.{tn}' column '{col}': duplicate in batch")
        } else {
            format!("Unique index violation on '{sn}.{tn}' column '{col}'")
        }
    }

    /// Format the `CREATE UNIQUE INDEX` rejection raised when the target column
    /// already holds duplicate values. Same single-source-of-truth contract as
    /// [`Self::unique_violation_err`].
    pub(crate) fn unique_create_dup_err(&mut self, table_id: i64, col_idx: usize) -> String {
        let (sn, tn, col) = self.qualified_col_names(table_id, col_idx);
        format!(
            "cannot create unique index on '{sn}.{tn}' column '{col}': column contains duplicate values"
        )
    }

    #[cfg(test)]
    pub fn get_by_name(&self, schema_name: &str, table_name: &str) -> Option<i64> {
        let qualified = format!("{}.{}", schema_name, table_name);
        self.caches.entity_by_qname.get(&qualified).copied()
    }

    #[cfg(test)]
    pub fn has_index_by_name(&self, name: &str) -> bool {
        self.caches.index_by_name.contains_key(name)
    }

    // -- Sequence management -----------------------------------------------

    pub fn advance_sequence(&mut self, seq_id: i64, old_val: i64, new_val: i64) {
        let schema = seq_tab_schema();
        let mut bb = BatchBuilder::new(schema);
        // Retract old
        bb.begin_row(seq_id as u128, -1);
        bb.put_u64(old_val as u64);
        bb.end_row();
        // Insert new
        bb.begin_row(seq_id as u128, 1);
        bb.put_u64(new_val as u64);
        bb.end_row();
        let batch = bb.finish();
        ingest_batch_into(&mut self.sys_sequences, &batch);
    }

    // -- Scan store -------------------------------------------------------

    pub(crate) fn scan_store(&mut self, table_id: i64, schema: &SchemaDescriptor) -> Rc<Batch> {
        let entry = match self.dag.tables.get(&table_id) {
            Some(e) => e,
            None => return Rc::new(Batch::empty_with_schema(schema)),
        };
        entry.handle.open_cursor().cursor.materialize()
    }

    /// Cursor-returning sibling of `scan_store` for callers that stream the
    /// relation chunk-wise (`drain_chunk`) instead of materializing it whole.
    /// `None` when the table is unregistered — callers treat that as empty.
    /// User tables only; system tables keep `scan_family`.
    ///
    /// The handle owns its sources via `Rc`, so it stays valid while the
    /// caller mutates OTHER relations (index table, view family) between
    /// chunks; the scanned relation itself must not be written mid-loop.
    pub(crate) fn open_store_cursor(&mut self, table_id: i64) -> Option<CursorHandle> {
        self.dag.tables.get(&table_id).map(|e| e.handle.open_cursor())
    }
}

/// Build the schema for a `gather_family` result: the PK columns of `schema`
/// (in pk-list order, so the packed PK round-trips identically) followed by
/// the projected columns in `project` order as payload. `project` must list
/// only non-PK columns (PK members are resolved from the packed PK without a
/// gather); a projected PK column would be emitted twice.
fn project_schema(schema: &SchemaDescriptor, project: &[u8]) -> SchemaDescriptor {
    let mut cols: Vec<SchemaColumn> = Vec::with_capacity(schema.pk_indices().len() + project.len());
    let mut pk_idx: Vec<u32> = Vec::with_capacity(schema.pk_indices().len());
    for (_, _, col) in schema.pk_columns() {
        pk_idx.push(cols.len() as u32);
        cols.push(*col);
    }
    for &p in project {
        debug_assert!(!schema.is_pk_col(p as usize),
            "project_schema: projected column {} is a PK column", p);
        cols.push(schema.columns[p as usize]);
    }
    SchemaDescriptor::new(&cols, &pk_idx)
}

/// Projecting sibling of `copy_cursor_row_with_weight`: append the cursor's
/// current row to `out` (which has the `project_schema` layout) with weight 1,
/// copying only the columns named in `project`. The projected payload column
/// at position `k` corresponds to source column `project[k]`; the projected
/// null bit `k` mirrors the source row's null bit for that column. Projected
/// columns are scalar, so no blob relocation is required.
fn copy_cursor_cols_to_batch(
    cursor: &CursorHandle,
    out: &mut Batch,
    src_schema: &SchemaDescriptor,
    project: &[u8],
) {
    // `current_pk_bytes()` is the verbatim OPK PK region for any width, and the
    // read cursor always tracks it regardless of stride. For narrow PKs it is
    // byte-identical to the old `extend_pk(current_key)` round-trip
    // (`current_key == widen_pk_be(current_pk_bytes)`), so one path serves both.
    out.extend_pk_bytes(cursor.cursor.current_pk_bytes());
    out.extend_weight(&1i64.to_le_bytes());

    let src_null = cursor.cursor.current_null_word;
    let mut proj_null = 0u64;
    for (k, &p) in project.iter().enumerate() {
        // The projection is master-built and excludes PK columns
        // (`collect_fk_projection` skips `is_pk_col`; `project_schema` asserts
        // it one frame up), so every projected column has a payload slot.
        let pi = src_schema.try_payload_idx(p as usize)
            .expect("FK projection excludes PK columns");
        if src_null & (1u64 << pi) != 0 {
            proj_null |= 1u64 << k;
        }
    }
    out.extend_null_bmp(&proj_null.to_le_bytes());

    for (k, &p) in project.iter().enumerate() {
        let col_size = src_schema.columns[p as usize].size() as usize;
        let ptr = cursor.cursor.col_ptr(p as usize, col_size);
        if !ptr.is_null() {
            let data = unsafe { std::slice::from_raw_parts(ptr, col_size) };
            out.extend_col(k, data);
        } else {
            out.fill_col_zero(k, col_size);
        }
    }
    out.count += 1;
}
