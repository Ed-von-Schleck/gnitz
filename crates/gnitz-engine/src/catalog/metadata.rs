//! FK / secondary-index metadata queries (for distributed validation),
//! the store-handle and schema-descriptor accessors, the cached
//! schema-wire block, and unique-violation message formatting.

use super::*;

impl CatalogEngine {
    // -- FK / index metadata queries (for distributed validation) -------------

    /// All FK edges where `table_id` is the child (empty when none).
    pub(crate) fn fk_constraints_of(&self, table_id: i64) -> &[FkEdge] {
        self.caches
            .fk_by_child
            .get(&table_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// All index circuits on a table (empty when none) — the one-pass
    /// accessor for consumers that walk every circuit (e.g. the master's
    /// unique-filter descriptors).
    pub(crate) fn index_circuits(&self, table_id: i64) -> &[crate::query::IndexCircuitEntry] {
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

    /// Get the index schema for a specific column list's index on a table.
    pub fn get_index_schema_by_cols(&self, table_id: i64, cols: &[u32]) -> Option<SchemaDescriptor> {
        self.index_circuit_for_cols(table_id, cols).map(|ic| ic.index_schema)
    }

    /// Return the cached schema wire entry (block, version, and derived wire
    /// properties) for `table_id`, or `None` if the block isn't yet cached.
    pub fn get_cached_schema_wire_block(&self, table_id: i64) -> Option<SchemaWireEntry> {
        self.caches.schema_wire_cache.get(&table_id).cloned()
    }

    /// Return the current schema version for `table_id` (1 if unknown).
    pub fn get_schema_version(&self, table_id: i64) -> u16 {
        self.caches.get_schema_version(table_id)
    }

    /// Store an encoded schema wire block in the cache, along with its
    /// derived wire properties. Written together so the invalidation in
    /// `clear_col_cache_no_bump` keeps them consistent.
    pub fn set_schema_wire_block(&mut self, table_id: i64, entry: SchemaWireEntry) {
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

    /// Does validating a write of `mode` to `table_id` read committed state?
    /// The disjunction of the four validation rules' own gates, so when it is
    /// false `validate_txn_distributed` finds nothing to check and skips the
    /// write entirely; the executor reads it for the same reason plus one of
    /// its own — such a write may hold its table lock shared, since nothing it
    /// does can be invalidated by a concurrent write to the same table. A new
    /// constraint kind adds its term here and to the rule that enforces it.
    pub fn push_reads_committed_state(&self, table_id: i64, mode: gnitz_wire::WireConflictMode) -> bool {
        !self.fk_constraints_of(table_id).is_empty()
            || !self.fk_children_of(table_id).is_empty()
            || self.has_any_unique_index(table_id)
            || matches!(mode, gnitz_wire::WireConflictMode::Error)
    }

    // -- Store handle accessors -----------------------------------------------

    /// Get a `&mut Table` for a user relation's own store, or `None` if the
    /// relation is absent, detached, or a `Borrowed` system table.
    ///
    /// SAFETY: hands out `&mut` from `&self` through the same `UnsafeCell`
    /// contract as [`StoreHandle::as_owned_mut`] — no aliasing `&mut` into
    /// the same store may be live across the call.
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn get_store_handle(&self, table_id: i64) -> Option<&mut Table> {
        self.dag.tables.get(&table_id).and_then(|e| e.handle.as_owned_mut())
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
    pub(crate) fn table_directory(&self, table_id: i64) -> Option<&str> {
        self.dag.tables.get(&table_id).map(|e| e.directory.as_str())
    }

    /// Get a raw mutable pointer to the DagEngine.
    pub(crate) fn get_dag_ptr(&mut self) -> *mut DagEngine {
        &mut self.dag as *mut DagEngine
    }

    // -- FK constraint queries ---------------------------------------------

    /// All FK edges where `parent_id` is the parent (empty when none).
    pub(crate) fn fk_children_of(&self, parent_id: i64) -> &[FkEdge] {
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
    /// constraint-violation paths that format these names. The defs are an `Rc`
    /// snapshot, so they do not borrow-conflict with the later name lookup.
    fn qualified_col_names(&mut self, table_id: i64, col_indices: &[u32]) -> (String, String, String) {
        let defs = self.read_column_defs(table_id);
        let col = col_indices
            .iter()
            .map(|&ci| defs.get(ci as usize).map_or("?", |d| d.name.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        let (sn, tn) = self.qualified_name_or_unknown(table_id);
        (sn.to_string(), tn.to_string(), col)
    }

    /// Format a unique-index constraint violation naming the qualified table and
    /// offending column(s). `in_batch` appends the "duplicate in batch" qualifier
    /// used when two rows of one ingest batch collide, versus a collision with
    /// already-committed data. Single source of truth for this message — the
    /// distributed path (`MasterDispatcher`) delegates here. A composite index
    /// passes its full `col_indices`, joined as `(a, b)`.
    pub(crate) fn unique_violation_err(&mut self, table_id: i64, col_indices: &[u32], in_batch: bool) -> String {
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
    pub(crate) fn unique_create_dup_err(&mut self, table_id: i64, col_indices: &[u32]) -> String {
        let (sn, tn, col) = self.qualified_col_names(table_id, col_indices);
        format!("cannot create unique index on '{sn}.{tn}' column '{col}': column contains duplicate values")
    }

    /// Format the PK-uniqueness rejection, PG-style. `key_str` is the
    /// already-rendered offending key. `in_batch` distinguishes two rows of one
    /// ingest batch sharing a PK from a collision with committed data.
    pub(crate) fn pk_violation_err(
        &mut self,
        table_id: i64,
        pk_indices: &[u32],
        key_str: &str,
        in_batch: bool,
    ) -> String {
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
    pub(crate) fn fk_missing_err(&self, child_tid: i64, parent_tid: i64) -> String {
        let (sn, tn) = self.qualified_name_or_unknown(child_tid);
        let (tsn, ttn) = self.qualified_name_or_unknown(parent_tid);
        format!("Foreign Key violation in '{sn}.{tn}': value not found in target '{tsn}.{ttn}'")
    }

    /// Format "a row a child still references cannot be removed". `verb` names
    /// what the parent write was doing: `"delete from"` when the row goes away,
    /// `"update"` when the referenced value changes under it.
    pub(crate) fn fk_restrict_err(&self, parent_tid: i64, child_tid: i64, verb: &str) -> String {
        let (sn, tn) = self.qualified_name_or_unknown(parent_tid);
        let (csn, ctn) = self.qualified_name_or_unknown(child_tid);
        format!("Foreign Key violation: cannot {verb} '{sn}.{tn}', row still referenced by '{csn}.{ctn}'")
    }
}
