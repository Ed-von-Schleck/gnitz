//! FK / secondary-index metadata queries (for distributed validation),
//! the store-handle and schema-descriptor accessors, the cached
//! schema-wire block, and unique-violation message formatting.

use super::*;

impl CatalogEngine {
    // -- Engine state, reached only through these ----------------------------
    //
    // `CatalogEngine` declares no `pub` field. A public field is assignable by
    // anyone who can name it and offers no place to state what a caller owes, so
    // every out-of-crate reach into this engine's own state comes through an
    // accessor, and each one is where the next field behind it stops being
    // reachable.

    /// The relation DAG: registered tables, views and their compiled circuits.
    pub fn dag(&self) -> &DagEngine {
        &self.dag
    }

    /// [`Self::dag`] for the callers that register, flush or re-order relations.
    /// The caches this engine maintains index into the DAG by id, so mutations
    /// here must leave the id set alone unless they go through a hook.
    pub fn dag_mut(&mut self) -> &mut DagEngine {
        &mut self.dag
    }

    /// Enter the DDL zone at `lsn`: every store LSN written until
    /// [`Self::close_ddl_zone`] is stamped with it, so one crash-recovery unit
    /// covers the whole DDL. Nesting is not supported — a second open before the
    /// close overwrites the first.
    pub fn open_ddl_zone(&mut self, lsn: std::num::NonZeroU64) {
        self.ctx.open_ddl_zone(lsn);
    }

    /// Leave the DDL zone and reset the rollback flag. Must run on both the
    /// success and the compensated-failure path: a zone left open stamps every
    /// later ingest with a stale LSN.
    pub fn close_ddl_zone(&mut self) {
        self.ctx.close_ddl_zone();
    }

    /// The checkpoint generation durably recorded in `SEQ_ID_CHECKPOINT_GEN`.
    pub fn durable_generation(&self) -> u64 {
        self.durable_generation
    }

    /// The generation a manifest must carry to be resumed from. Equal to
    /// [`Self::durable_generation`] except across the recovery-start bump.
    pub fn resume_generation(&self) -> u64 {
        self.resume_generation
    }

    /// The data directory this engine's relations live under.
    pub fn base_dir(&self) -> &str {
        &self.base_dir
    }

    /// Rows per `drain_chunk` call on every chunked scan this engine drives.
    pub fn ddl_scan_chunk_rows(&self) -> usize {
        self.ddl_scan_chunk_rows
    }

    /// The high-water mark of user SERIAL sequence `seq_id` (== the table id) —
    /// the last id handed out. `None` when the sequence has never advanced.
    pub fn user_sequence(&self, seq_id: i64) -> Option<i64> {
        self.user_sequences.get(&seq_id).copied()
    }

    /// Whether `view_id`'s checkpointed output was rejected at boot and must be
    /// rebuilt rather than resumed.
    pub fn view_is_invalid(&self, view_id: i64) -> bool {
        self.invalid_views.contains(&view_id)
    }

    /// Every view id [`Self::view_is_invalid`] holds for, in no defined order.
    pub fn invalid_views(&self) -> impl Iterator<Item = i64> + '_ {
        self.invalid_views.iter().copied()
    }

    /// Drop `view_id` from the invalid set once its output has been rebuilt.
    /// Returns whether it was there.
    pub fn clear_invalid_view(&mut self, view_id: i64) -> bool {
        self.invalid_views.remove(&view_id)
    }

    // -- FK / index metadata queries (for distributed validation) -------------

    /// All FK edges where `table_id` is the child (empty when none).
    pub fn fk_constraints_of(&self, table_id: i64) -> &[FkEdge] {
        self.caches
            .fk_by_child
            .get(&table_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// All index circuits on a table (empty when none) — the one-pass
    /// accessor for consumers that walk every circuit (e.g. the master's
    /// unique-filter descriptors).
    pub fn index_circuits(&self, table_id: i64) -> &[crate::query::IndexCircuitEntry] {
        self.dag
            .tables
            .get(&table_id)
            .map(|e| e.index_circuits.as_slice())
            .unwrap_or(&[])
    }

    /// Get index circuit info at index: (col_indices, is_unique). Production
    /// consumers go through `index_circuits` / `index_circuit_for_cols`; only
    /// the catalog tests enumerate raw circuit info.
    #[cfg(test)]
    pub(crate) fn get_index_circuit_info(&self, table_id: i64, idx: usize) -> Option<(PkColList, bool)> {
        let entry = self.dag.tables.get(&table_id)?;
        let ic = entry.index_circuits.get(idx)?;
        Some((ic.col_indices, ic.is_unique))
    }

    /// Reject a column list that is malformed or names a column outside
    /// `table_id`'s schema. The one admission test for every frame carrying a
    /// `pack_pk_cols` word: the master applies it as an early client-facing
    /// reject, the worker as its trust boundary, and both render the same error.
    pub fn validate_index_cols(&self, table_id: i64, cols: &PkColList, op: &str) -> Result<(), String> {
        let in_range = |s: &SchemaDescriptor| {
            cols.is_well_formed() && cols.as_slice().iter().all(|&c| (c as usize) < s.num_columns())
        };
        match self.get_schema_desc(table_id) {
            Some(s) if in_range(&s) => Ok(()),
            _ => Err(format!("{op}: invalid column list for table {table_id}")),
        }
    }

    /// The secondary index circuit on `cols` of `table_id`, if one exists. The
    /// SEEK_BY_INDEX handler matches the `Option` once — `None` answers
    /// STATUS_NO_INDEX (so the SQL planner falls back to a scan or a CREATE INDEX
    /// hint without a prior catalog probe), `Some` broadcasts the seek.
    pub fn index_circuit_for_cols(&self, table_id: i64, cols: &[u32]) -> Option<&crate::query::IndexCircuitEntry> {
        self.dag.tables.get(&table_id)?.index_circuit_on(cols)
    }

    /// True if the table has at least one unique secondary index circuit.
    /// Used to decide whether distributed unique-index validation is needed.
    /// Non-unique circuits (e.g. FK indices) do not count.
    pub fn has_any_unique_index(&self, table_id: i64) -> bool {
        self.index_circuits(table_id).iter().any(|ic| ic.is_unique)
    }

    /// Return the cached schema wire entry (block, version, wire-safety) for
    /// `table_id`, or `None` if the block isn't yet cached.
    pub fn get_cached_schema_wire_block(&self, table_id: i64) -> Option<SchemaWireEntry> {
        self.caches.schema_wire_cache.get(&table_id).cloned()
    }

    /// Return the current schema version for `table_id` (1 if unknown).
    pub fn get_schema_version(&self, table_id: i64) -> u16 {
        self.caches.get_schema_version(table_id)
    }

    /// Store an encoded schema wire block in the cache, with the version and
    /// wire-safety it was built at — written together so the invalidation in
    /// `clear_col_cache_no_bump` keeps them consistent.
    pub(crate) fn set_schema_wire_block(&mut self, table_id: i64, entry: SchemaWireEntry) {
        self.caches.schema_wire_cache.insert(table_id, entry);
    }

    /// The full set of table IDs that must be locked together for a write to
    /// `table_id`, sorted ascending to guarantee deadlock-free acquisition:
    /// `table_id` itself plus all FK parents (to guard concurrent parent
    /// DELETE) and FK children (to guard concurrent child INSERT during a
    /// parent DELETE). Empty if this table requires no lock at all.
    /// Materialized at DDL time by `recompute_needs_lock`; the borrow is
    /// sound on the push path because the caller holds the push read lock.
    pub fn fk_lock_set(&self, table_id: i64) -> &[i64] {
        self.caches
            .needs_lock
            .get(&table_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Does `table_id` carry a constraint whose validation reads committed
    /// state? The disjunction of the validation rules' own gates. A new
    /// constraint kind adds its term here and to the rule that enforces it —
    /// the master's per-table overlay fold is built on exactly this predicate,
    /// and a rule reading an overlay outside it indexes a missing entry.
    pub fn has_row_constraints(&self, table_id: i64) -> bool {
        !self.fk_constraints_of(table_id).is_empty()
            || !self.fk_children_of(table_id).is_empty()
            || self.has_any_unique_index(table_id)
    }

    /// Does validating a write of `mode` to `table_id` read committed state?
    /// When false `validate_txn_distributed` finds nothing to check and skips
    /// the write entirely; the executor reads it for the same reason plus one of
    /// its own — such a write may hold its table lock shared, since nothing it
    /// does can be invalidated by a concurrent write to the same table.
    pub fn push_reads_committed_state(&self, table_id: i64, mode: gnitz_wire::WireConflictMode) -> bool {
        self.has_row_constraints(table_id) || matches!(mode, gnitz_wire::WireConflictMode::Error)
    }

    // -- Store handle accessors -----------------------------------------------

    /// A user relation's own store, or `None` if the relation is absent,
    /// detached, or a `Borrowed` system table.
    pub fn get_store_handle(&self, table_id: i64) -> Option<&Table> {
        self.dag.tables.get(&table_id).and_then(|e| e.handle.as_owned())
    }

    /// Whether `tid`'s store came back from a checkpoint manifest at this open,
    /// rather than being erased or created empty. `false` for a relation this
    /// process holds no owned store for.
    pub fn store_resumed(&self, tid: i64) -> bool {
        self.dag
            .tables
            .get(&tid)
            .and_then(|e| e.handle.as_owned())
            .is_some_and(|t| t.resumed_from_checkpoint())
    }

    /// Get schema descriptor for a table. Registry-uniform: system tables are
    /// pre-registered before any caller can run, and an unknown id in the
    /// system range (the 8-10 gap) resolves to a graceful `None` instead of a
    /// panic.
    pub fn get_schema_desc(&self, table_id: i64) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id).map(|e| e.schema)
    }

    /// The on-disk directory of a user table (`{base_dir}/{schema}/{name}_{tid}`),
    /// the parent of its child store subdirs (`ChildAddr`). Guaranteed to exist on
    /// the data filesystem once the table is created, so it anchors an
    /// `O_TMPFILE` spill (e.g. the CREATE UNIQUE INDEX pre-flight external sort)
    /// onto the same disk as the table's data. `None` for an unknown table.
    pub fn table_directory(&self, table_id: i64) -> Option<&str> {
        self.dag.tables.get(&table_id).map(|e| e.directory.as_str())
    }

    // -- FK constraint queries ---------------------------------------------

    /// All FK edges where `parent_id` is the parent (empty when none).
    pub fn fk_children_of(&self, parent_id: i64) -> &[FkEdge] {
        self.caches
            .fk_by_parent
            .get(&parent_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// `(schema, table, columns)` names for `(table_id, col_indices)`, each
    /// falling back to `"?"` when the catalog has no entry. The `columns` field
    /// joins every named column with `, ` (a composite `UNIQUE (a, b)` renders
    /// `"a, b"`). The fallback is defensive: the entity always exists on the
    /// constraint-violation paths that format these names. Goes through
    /// `read_column_defs` so these messages read the same cached column defs as
    /// every other name consumer, instead of opening a second COL_TAB scan.
    fn qualified_col_names(&mut self, table_id: i64, col_indices: &[u32]) -> (&str, &str, String) {
        let defs = self.read_column_defs(table_id);
        let col = col_indices
            .iter()
            .map(|&ci| defs.get(ci as usize).map_or("?", |d| d.name.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        let (sn, tn) = self.qualified_name_or_unknown(table_id);
        (sn, tn, col)
    }

    /// Format a unique-index constraint violation naming the qualified table and
    /// offending column(s). `in_batch` appends the "duplicate in batch" qualifier
    /// used when two rows of one ingest batch collide, versus a collision with
    /// already-committed data. Single source of truth for this message — the
    /// distributed path (`MasterDispatcher`) delegates here. A composite index
    /// passes its full `col_indices`, joined as `(a, b)`.
    pub fn unique_violation_err(&mut self, table_id: i64, col_indices: &[u32], in_batch: bool) -> String {
        let (sn, tn, col) = self.qualified_col_names(table_id, col_indices);
        if in_batch {
            format!("Unique index violation on '{sn}.{tn}' column '{col}': duplicate in batch")
        } else {
            format!("Unique index violation on '{sn}.{tn}' column '{col}'")
        }
    }

    /// Format the `CREATE UNIQUE INDEX` rejection raised when the target
    /// column(s) already hold duplicate values. Same single-source-of-truth
    /// contract as [`Self::unique_violation_err`].
    pub fn unique_create_dup_err(&mut self, table_id: i64, col_indices: &[u32]) -> String {
        let (sn, tn, col) = self.qualified_col_names(table_id, col_indices);
        format!("cannot create unique index on '{sn}.{tn}' column '{col}': column contains duplicate values")
    }

    /// Format the PK-uniqueness rejection, PG-style. `key_str` is the
    /// already-rendered offending key. `in_batch` distinguishes two rows of one
    /// ingest batch sharing a PK from a collision with committed data.
    pub fn pk_violation_err(&mut self, table_id: i64, pk_indices: &[u32], key_str: &str, in_batch: bool) -> String {
        let (sn, tn, cols) = self.qualified_col_names(table_id, pk_indices);
        let what = if in_batch {
            format!("Batch contains multiple rows with key ({cols})=({key_str})")
        } else {
            format!("Key ({cols})=({key_str}) already exists")
        };
        format!("duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": {what}")
    }

    /// Format "an inserted child row references a value the parent does not
    /// hold". Shared by the inline DDL-time check and the distributed pre-flight.
    pub fn fk_missing_err(&self, child_tid: i64, parent_tid: i64) -> String {
        let (sn, tn) = self.qualified_name_or_unknown(child_tid);
        let (tsn, ttn) = self.qualified_name_or_unknown(parent_tid);
        format!("Foreign Key violation in '{sn}.{tn}': value not found in target '{tsn}.{ttn}'")
    }

    /// Format "a row a child still references cannot be removed". `verb` names
    /// what the parent write was doing: `"delete from"` when the row goes away,
    /// `"update"` when the referenced value changes under it.
    pub fn fk_restrict_err(&self, parent_tid: i64, child_tid: i64, verb: &str) -> String {
        let (sn, tn) = self.qualified_name_or_unknown(parent_tid);
        let (csn, ctn) = self.qualified_name_or_unknown(child_tid);
        format!("Foreign Key violation: cannot {verb} '{sn}.{tn}', row still referenced by '{csn}.{ctn}'")
    }

    /// `table_id`'s schema, or the one "the catalog has no schema for a table a
    /// live request names" error. Every caller is a fail-stop — reaching it
    /// means the catalog diverged from the request that named the table — so
    /// `op` labels which path observed the divergence.
    pub fn schema_or_err(&self, table_id: i64, op: &str) -> Result<SchemaDescriptor, String> {
        self.get_schema_desc(table_id)
            .ok_or_else(|| format!("{op}: no schema for table {table_id}"))
    }
}
