//! Catalog write-path spine — the single ingest/apply pipeline every
//! system-table mutation flows through: `submit` → `precheck_sys_ingest`
//! → `apply_local` → `fire_hooks`, plus the broadcast queue, the
//! directory-deletion queues, and Stage-A (DDL rollback) compensation.
//! `fire_hooks` lives in `hooks.rs`; `SysFamily` / `ApplyContext` in
//! `sys_tables.rs` / `apply_context.rs`. No second ingest entry point may
//! skip this precheck/hooks path.

use std::num::NonZeroU64;

use super::*;

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
    fn submit(&mut self, family: SysFamily, batch: Batch) -> Result<(), String> {
        if self.ctx.in_rollback() {
            // During rollback all cascade writes must bypass pending_broadcasts
            // so no compensating row is re-broadcast to workers.
            return self.submit_local(family, batch);
        }
        // `submit` is the composition of the two steps the DDL_TXN handler drives
        // separately per bundle family: precheck (no mutation) then
        // apply-and-enqueue. Cascade callers (hooks) keep the atomic composition.
        self.precheck_family(family.id(), &batch)?;
        self.apply_and_enqueue_family(family.id(), batch)
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
    /// the SAL group LSN. Pins never regress the counter: every zone is reserved
    /// with a floor that dominates the pinned family's `current_lsn`
    /// (`ZoneLsnAllocator::reserve`), so even counters drifted by un-pinned
    /// auto-bump ingests sit strictly below the zone LSN pinning them. Does NOT
    /// broadcast.
    fn apply_local(&mut self, family: SysFamily, batch: &mut Batch, pin_lsn: Option<NonZeroU64>) -> Result<(), String> {
        let id = family.id();
        let table = self
            .sys_table_mut(id)
            .expect("SysFamily::id() maps to a known sys table");
        // Live DDL's one sound non-fatal exit: a failure here is pre-broadcast
        // (nothing durable, nothing broadcast, client not ACKed), so Stage-A
        // compensation can unwind it. Propagate rather than abort.
        table
            .ingest_borrowed_batch(batch)
            .map_err(|e| format!("apply_local: sys-table ingest failed (family={id}): {e}"))?;
        if let Some(lsn) = pin_lsn {
            table.current_lsn = lsn.get();
        }
        batch.set_schema(sys_tab_schema(id));
        self.fire_hooks(family, batch)
    }

    // -- System ingestion entry + precheck / broadcast / dir-deletion -------

    /// Ingest a batch into a table family (unique_pk + store + index projection + hooks).
    /// System tables go through the [`CatalogDeltaSink::submit`] applied-delta
    /// path (precheck → ingest → hooks → broadcast-queue). User tables delegate
    /// to `DagEngine::ingest_by_ref`. This `&Batch` entry serves external/wire
    /// callers that hold a borrow; the DDL emitters call `submit` directly with
    /// an owned batch to skip the clone.
    pub fn ingest_to_family(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            let family = SysFamily::from_id(table_id).ok_or_else(|| format!("Unknown system family {table_id}"))?;
            self.submit(family, batch.clone())
        } else {
            let rc = self.dag.ingest_by_ref(table_id, batch);
            if rc < 0 {
                Err(format!("ingest_to_family failed for table_id={table_id} rc={rc}"))
            } else {
                Ok(())
            }
        }
    }

    /// Precheck one system-family delta (the CREATE/DROP invariant guards) with
    /// no mutation — the read-only half of [`CatalogDeltaSink::submit`]. Exposed
    /// so the `DDL_TXN` handler can precheck a family, set its rollback marker,
    /// then apply it, leaving the marker `None` iff the precheck failed (so a
    /// precheck rejection reconstructs no ghost row on rollback).
    pub(crate) fn precheck_family(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        self.precheck_sys_ingest(table_id, batch)
    }

    /// Apply one system-family delta to storage, fire its hooks, and enqueue it
    /// for broadcast — the mutating half of [`CatalogDeltaSink::submit`], pinned
    /// to the open DDL zone LSN. Takes the batch by value so it moves straight
    /// into `pending_broadcasts` (one storage clone, no hooks clone). Enqueue
    /// happens after hooks so nested cascade pushes land first and the executor
    /// broadcasts children → parent; empty batches are dropped so worker-side
    /// no-op cascades don't accumulate unread entries.
    pub(crate) fn apply_and_enqueue_family(&mut self, table_id: i64, mut batch: Batch) -> Result<(), String> {
        let family = SysFamily::from_id(table_id).ok_or_else(|| format!("Unknown system family {table_id}"))?;
        self.apply_local(family, &mut batch, self.ctx.ddl_zone_lsn())?;
        if batch.count > 0 {
            self.pending_broadcasts.push((table_id, batch));
        }
        Ok(())
    }

    /// The schema descriptor for a system-family `tid`, or `None` if `tid` is not
    /// a system family. Unlike `get_schema_desc`, this never panics on an unknown
    /// id in the system range, so the `DDL_TXN` decoder can reject a bogus family
    /// tid before decoding its wal-block slice into a `Batch`.
    pub(crate) fn sys_family_schema(&self, tid: i64) -> Option<SchemaDescriptor> {
        SysFamily::from_id(tid).map(|f| sys_tab_schema(f.id()))
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
            let table = self
                .sys_table(family.id())
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
        let schema_name = self
            .caches
            .schema_by_id
            .get(&sid)
            .ok_or_else(|| format!("Schema with ID {sid} does not exist"))?;
        let qualified = format!("{schema_name}.{name}");
        if let Some(&existing) = self.caches.entity_by_qname.get(&qualified) {
            if existing != self_id {
                return Err(format!("Table or view already exists: {qualified}"));
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
    pub(crate) fn for_each_index_on_cols(&self, owner_id: i64, cols: &[u32], mut f: impl FnMut(i64, bool)) {
        let mut cursor = self.sys_indices.open_cursor();
        while cursor.cursor.valid {
            if cursor.cursor.current_weight > 0 {
                let row_owner = cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64;
                let row_cols = gnitz_wire::unpack_pk_cols(cursor_read_u64(&cursor, IDXTAB_COL_SOURCE_COLS));
                if row_owner == owner_id && row_cols.as_slice() == cols {
                    let row_id = cursor.cursor.current_key_narrow() as u64 as i64;
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
        if table_id != SCHEMA_TAB_ID && table_id != TABLE_TAB_ID && table_id != VIEW_TAB_ID && table_id != IDX_TAB_ID {
            return Ok(());
        }

        // -- Positive-weight (CREATE) checks ----------------------------------
        for i in 0..batch.count {
            if batch.get_weight(i) <= 0 {
                continue;
            }

            match table_id {
                SCHEMA_TAB_ID => {
                    let name = self.read_batch_string(batch, i, 0);
                    if self.caches.schema_by_name.contains_key(&name) {
                        return Err(format!("Schema already exists: {name}"));
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
                            "catalog invariant violated: table '{name}' (tid={tid}) registered \
                             before its column records. COL_TAB writes must precede \
                             TABLE_TAB writes (see hooks.rs dispatch doc)."
                        ));
                    }
                    validate_pk_cols(&col_defs, &pk)?;
                    if col_defs.len() > crate::schema::MAX_COLUMNS {
                        return Err(format!(
                            "table '{}' (tid={}) has {} columns (max {})",
                            name,
                            tid,
                            col_defs.len(),
                            crate::schema::MAX_COLUMNS
                        ));
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
                            "catalog invariant violated: view '{name}' (vid={vid}) registered \
                             before its column records. COL_TAB writes must precede \
                             VIEW_TAB writes (see hooks.rs dispatch doc)."
                        ));
                    }
                    // Reject an over-wide view here — before apply_entity_by_qname
                    // mutates the caches — so a rejected DDL leaves clean state,
                    // mirroring the TABLE_TAB precheck above. hook_view_register
                    // carries the same guard as the build_schema_from_col_defs
                    // assert backstop.
                    if col_count > crate::schema::MAX_COLUMNS {
                        return Err(format!(
                            "view '{}' (vid={}) has {} columns (max {})",
                            name,
                            vid,
                            col_count,
                            crate::schema::MAX_COLUMNS
                        ));
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
                            cols.decoded_count(),
                            gnitz_wire::PK_LIST_MAX_COLS
                        ));
                    }

                    let entry = self
                        .dag
                        .tables
                        .get(&owner_id)
                        .ok_or_else(|| format!("Index: owner table {owner_id} not found"))?;

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
                        return Err(format!("Index: owner {owner_id} is not a base table"));
                    }

                    // Bounds, per-column eligibility (STRING/BLOB/float), and
                    // arity/stride limits, identical to what registration will
                    // enforce — only the table-name context is added here.
                    make_index_schema(cols.as_slice(), &entry.schema).map_err(|e| {
                        format!(
                            "{} for table '{}' (tid={})",
                            e,
                            self.caches
                                .entity_by_id
                                .get(&owner_id)
                                .map(|(_, n)| n.as_str())
                                .unwrap_or("?"),
                            owner_id
                        )
                    })?;

                    let idx_id = batch.get_pk(i) as i64;
                    if let Some(&existing) = self.caches.index_by_name.get(&index_name) {
                        if existing != idx_id {
                            return Err(format!("Index already exists: {index_name}"));
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

        // First DROP arm: reject a SCHEMA_TAB -1 on a non-empty schema. This is
        // the engine-side, caller-agnostic half of the DROP SCHEMA member
        // cascade — it runs before any WAL write, so a rejected non-empty drop
        // queues no dir deletion and retracts no rows, converting the silent
        // member-orphan into a loud error. The production client cascade
        // (`GnitzClient::drop_schema`) and the `#[cfg(test)]` engine cascade both
        // drop every member as prior, separate submissions, so by the time the
        // schema row reaches here its member caches for that sid are empty and
        // the guard passes. No cascade exemption is needed (unlike the IDX_TAB
        // guard below): a SCHEMA_TAB -1 is never submitted from inside an engine
        // cascade.
        if table_id == SCHEMA_TAB_ID {
            for &sid in &drop_ids {
                let n = self.schema_member_count(sid);
                if n > 0 {
                    return Err(format!("Schema not empty: {n} relation(s) remain; drop them first"));
                }
            }
            return Ok(());
        }

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
                    if !cursor.cursor.valid || cursor.cursor.current_key_narrow() as u64 != idx_id as u64 {
                        continue;
                    }
                    (
                        cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64,
                        gnitz_wire::unpack_pk_cols(cursor_read_u64(&cursor, IDXTAB_COL_SOURCE_COLS)),
                    )
                };
                // FK backing is single-column: a composite index never satisfies
                // a single-column FK/uniqueness requirement, so dropping
                // one is never blocked by the FK-target guard.
                if cols.as_slice().len() != 1 {
                    continue;
                }
                let src_col = cols.as_slice()[0] as usize;
                if self
                    .fk_children_of(owner_id)
                    .iter()
                    .any(|r| r.parent_col_idx == src_col)
                {
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
                            let (sn, tn) = self.caches.entity_by_id.get(&owner_id).cloned().unwrap_or_default();
                            return Err(format!(
                                "Integrity violation: index on '{sn}.{tn}' is referenced by a \
                                 foreign key and no unique index would remain on the column"
                            ));
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
                let blocking = self
                    .fk_children_of(tid)
                    .iter()
                    .find(|r| drop_ids.binary_search(&r.child_tid).is_err());
                if let Some(r) = blocking {
                    let (sn, tn) = self.caches.entity_by_id.get(&r.child_tid).cloned().unwrap_or_default();
                    return Err(format!("Integrity violation: table referenced by '{sn}.{tn}'"));
                }
            }
        }

        // The view-dependency guard applies only to TABLE/VIEW drops, never to a
        // SCHEMA_TAB drop. A schema drop is gated separately by the member-count
        // arm at the top of this section (which returns before reaching here); it
        // must not be dep-probed, because schema ids (allocated from
        // FIRST_USER_SCHEMA_ID = 3) share an i64 space with table ids (from
        // FIRST_USER_TABLE_ID = 16) — as both counters climb, a schema id
        // eventually equals an earlier table id, so probing the table-keyed
        // dep_map with a schema id would spuriously match an unrelated table's
        // dependents.
        if table_id == TABLE_TAB_ID || table_id == VIEW_TAB_ID {
            let dep_map = self.dag.get_dep_map();
            for &id in &drop_ids {
                if let Some(dependents) = dep_map.get(&id) {
                    // A dependent that is itself being dropped in this same
                    // batch is self-resolving — only an *outside* dependent
                    // blocks the drop. drop_ids is sorted+deduped above, so
                    // binary_search is O(N log M) vs the O(N·M) of `contains`.
                    let still_active = dependents
                        .iter()
                        .any(|&dep_id| drop_ids.binary_search(&dep_id).is_err());
                    if still_active {
                        let (sn, tn) = self.caches.entity_by_id.get(&id).cloned().unwrap_or_default();
                        return Err(format!("View dependency: entity '{sn}.{tn}'"));
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
    pub(crate) fn drain_pending_dir_deletions(&mut self) {
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
        self.checkpoint_gated_deletions.append(&mut self.pending_dir_deletions);
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
    pub(crate) fn cancel_gated_deletion(&mut self, dir: &str) {
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
        let live_tables: rustc_hash::FxHashSet<&str> = self.dag.tables.values().map(|e| e.directory.as_str()).collect();

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
                let full = format!("{schema_dir}/{name}");

                if live_tables.contains(full.as_str()) {
                    // Live table/view: sweep orphaned `idx_<id>` sub-dirs left by
                    // a standalone DROP INDEX whose gated deletion was lost to a
                    // crash.
                    for idx_name in subdir_names(&full) {
                        if !is_index_dir_name(&idx_name) {
                            continue;
                        }
                        let idx_full = format!("{full}/{idx_name}");
                        if live_indices.contains(&idx_full) {
                            remove_stale_index_rank_dirs(&idx_full);
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

    /// Topological creation priority for the catalog family order. Lower =
    /// earlier in the dependency chain (created first, destroyed last). Used both
    /// to order the `DDL_TXN` handler's ascending forward ingest (so every
    /// register/index hook sees its dependencies already in the memtable) and to
    /// order rollback's descending negate. Priorities must be distinct for
    /// TABLE_TAB and VIEW_TAB so the relative order is stable.
    pub(crate) fn catalog_topo_priority(tid: i64) -> u8 {
        match tid {
            SCHEMA_TAB_ID => 0,
            COL_TAB_ID => 1,
            DEP_TAB_ID => 2,
            CIRCUIT_NODES_TAB_ID => 3,
            CIRCUIT_EDGES_TAB_ID => 4,
            CIRCUIT_NODE_COLUMNS_TAB_ID => 5,
            TABLE_TAB_ID => 6,
            VIEW_TAB_ID => 7,
            IDX_TAB_ID => 8,
            _ => 99,
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

    /// Compensate a failed `DDL_TXN` bundle: undo every in-memory mutation that
    /// was applied before the failure so the catalog is exactly as it was, in
    /// master memory, before any worker sees a byte.
    ///
    /// The drained `pending_broadcasts` already holds every family that was
    /// applied **and enqueued** (the families before the failing one). The
    /// handler additionally passes the single family that was applied but **not
    /// yet enqueued** — a hook/panic failure inside `apply_and_enqueue_family` —
    /// as `applied_not_enqueued`, so it too is negated. On a **precheck** failure
    /// the handler passes `None`: nothing was applied for that family, so nothing
    /// is reconstructed and **no ghost `-1` is written**. At most one family is
    /// ever applied-not-enqueued, so `Option` is the exact type.
    pub(crate) fn compensate_stage_a(&mut self, applied_not_enqueued: Option<(i64, Batch)>) {
        let mut rollback_list = self.drain_pending_broadcasts();

        if let Some((tid, mut batch)) = applied_not_enqueued {
            batch.set_schema(sys_tab_schema(tid));
            rollback_list.push((tid, batch));
        }

        // Precheck-failed first family: nothing applied, trivial no-op.
        if rollback_list.is_empty() {
            return;
        }

        // Derive the rollback direction from the (weight-uniform) list: a bundle
        // is either a CREATE (all +1) or a DROP (all -1).
        let is_create = rollback_list
            .iter()
            .any(|(_, b)| (0..b.count).any(|i| b.get_weight(i) > 0));

        debug_assert!(
            rollback_list
                .iter()
                .all(|(_, b)| (0..b.count).all(|i| b.get_weight(i) > 0))
                || rollback_list
                    .iter()
                    .all(|(_, b)| (0..b.count).all(|i| b.get_weight(i) < 0)),
            "compensate_stage_a assumes a weight-homogeneous bundle; a mixed \
             CREATE/DROP bundle would misclassify the rollback direction and mishandle \
             pending_dir_deletions"
        );

        // For CREATE rollback: dependents unregistered before dependencies — DESCENDING.
        // For DROP rollback: dependencies restored before dependents — ASCENDING.
        if is_create {
            rollback_list.sort_by_key(|(tid, _)| std::cmp::Reverse(Self::catalog_topo_priority(*tid)));
        } else {
            rollback_list.sort_by_key(|(tid, _)| Self::catalog_topo_priority(*tid));
        }

        // Replay each with negated weight through the no-broadcast path.
        // fire_hooks still fires so caches, dag.tables, and pending_dir_deletions
        // are updated. The rollback gate in `submit` ensures any cascade that
        // calls back into `submit` also bypasses broadcasts.
        let result = self.with_rollback_compensation(|s| -> Result<(), String> {
            for (tid, mut batch) in rollback_list {
                Self::negate_batch_in_place(&mut batch);
                let family = SysFamily::from_id(tid).ok_or_else(|| format!("rollback: unknown system family {tid}"))?;
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
                 aborting to prevent serving a diverged catalog. Cause: {}",
                e
            );
        });
    }
}
