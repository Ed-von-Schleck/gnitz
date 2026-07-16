//! FK / secondary-index metadata queries (for distributed validation),
//! the store-handle and schema-descriptor accessors, the cached
//! schema-wire block, and unique-violation message formatting.

use super::*;

/// The cached schema wire data (block + derived wire properties, see
/// [`crate::catalog::cache::SchemaWireEntry`]) paired with the table's current
/// schema version. Returned by `get_cached_schema_wire_block`; the entry and
/// version share the same invalidation lifecycle (DDL on the owning table
/// drops the entry and bumps the version together).
pub struct CachedSchemaWire {
    pub entry: crate::catalog::cache::SchemaWireEntry,
    pub version: u16,
}

impl CatalogEngine {
    // -- FK / index metadata queries (for distributed validation) -------------

    /// All FK constraints on child table `table_id` (empty when none).
    pub(crate) fn fk_constraints_of(&self, table_id: i64) -> &[FkConstraint] {
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

    /// Number of index circuits on a table.
    pub fn get_index_circuit_count(&self, table_id: i64) -> usize {
        self.dag
            .tables
            .get(&table_id)
            .map(|e| e.index_circuits.len())
            .unwrap_or(0)
    }

    /// Get index circuit info at index: (col_indices, is_unique). Production
    /// consumers go through `unique_index_circuit_cols` / `index_circuit_for_cols`;
    /// only the catalog tests enumerate raw circuit info.
    #[cfg(test)]
    pub(crate) fn get_index_circuit_info(&self, table_id: i64, idx: usize) -> Option<(PkColList, bool)> {
        let entry = self.dag.tables.get(&table_id)?;
        let ic = entry.index_circuits.get(idx)?;
        Some((ic.col_indices, ic.is_unique))
    }

    /// The source column list of the unique circuit at position `idx`, `None`
    /// when the circuit is missing or non-unique. The unique-enforcement
    /// machinery (routing cache, filters, has_pk pre-checks) iterates circuits
    /// through this single accessor — see `IndexCircuitEntry::unique_cols`. The
    /// slice has length ≥ 1 (composite UNIQUE yields the full ordered list).
    pub fn unique_index_circuit_cols(&self, table_id: i64, idx: usize) -> Option<&[u32]> {
        self.dag.tables.get(&table_id)?.index_circuits.get(idx)?.unique_cols()
    }

    /// Get index store handle for an exact column list (for worker has_pk via
    /// a unique index, single- or multi-column).
    pub(crate) fn get_index_store_handle(&self, table_id: i64, cols: &[u32]) -> *const Table {
        self.index_circuit_for_cols(table_id, cols)
            .map(|ic| ic.table_mut() as *const Table)
            .unwrap_or(std::ptr::null())
    }

    /// Get the SchemaDescriptor for the index circuit at position idx.
    pub fn get_index_circuit_schema(&self, table_id: i64, idx: usize) -> Option<SchemaDescriptor> {
        self.dag
            .tables
            .get(&table_id)
            .and_then(|e| e.index_circuits.get(idx))
            .map(|ic| ic.index_schema)
    }

    /// The secondary index circuit on `col_idx` of `table_id`, if one exists.
    /// Single source of truth for "does this column have an index, and is it
    /// unique": the SEEK_BY_INDEX handler matches the `Option` once — `None`
    /// answers STATUS_NO_INDEX (so the SQL planner falls back to a scan or a
    /// CREATE INDEX hint without a prior catalog probe), `Some(ic)` routes by
    /// `ic.is_unique` (unicast a unique match to one worker, broadcast-and-merge
    /// a non-unique one). Returning the entry means callers scan the circuit
    /// list once and cannot ask whether a non-existent index is unique.
    pub fn index_circuit_for_cols(&self, table_id: i64, cols: &[u32]) -> Option<&crate::query::IndexCircuitEntry> {
        self.dag
            .tables
            .get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_indices.as_slice() == cols))
    }

    /// True if the table has at least one unique secondary index circuit.
    /// Used to decide whether distributed unique-index validation is needed.
    /// Non-unique circuits (e.g. FK indices) do not count.
    pub fn has_any_unique_index(&self, table_id: i64) -> bool {
        self.dag
            .tables
            .get(&table_id)
            .map(|e| e.index_circuits.iter().any(|ic| ic.is_unique))
            .unwrap_or(false)
    }

    /// True if the table was created with `unique_pk=true`. Used by the
    /// distributed validator to decide whether Error-mode inserts need
    /// an against-store PK rejection broadcast.
    pub fn table_has_unique_pk(&self, table_id: i64) -> bool {
        self.dag.tables.get(&table_id).map(|e| e.unique_pk()).unwrap_or(false)
    }

    /// Get the index schema for a specific column list's index on a table.
    pub fn get_index_schema_by_cols(&self, table_id: i64, cols: &[u32]) -> Option<SchemaDescriptor> {
        self.index_circuit_for_cols(table_id, cols).map(|ic| ic.index_schema)
    }

    /// Get column names for a table/view. Cached after first lookup; the same
    /// COL_TAB read fills the column-def and hidden-column-bitmask caches.
    /// Returns an `Rc` snapshot — callers routinely touch the catalog while
    /// holding it, so a borrow would not do.
    pub fn get_column_names(&mut self, table_id: i64) -> Rc<Vec<String>> {
        if let Some(names) = self.caches.col_names.get(&table_id) {
            return names.clone();
        }
        self.fill_column_caches(table_id);
        self.caches.col_names.get(&table_id).expect("just filled").clone()
    }

    /// Hidden-column bitmask for a table/view (bit N ⇔ column N is a hidden key
    /// slot). Shares the COL_TAB read and invalidation with `get_column_names`.
    pub fn get_col_hidden_mask(&mut self, table_id: i64) -> u128 {
        if let Some(&mask) = self.caches.col_hidden.get(&table_id) {
            return mask;
        }
        self.fill_column_caches(table_id);
        self.caches.col_hidden.get(&table_id).copied().unwrap_or(0)
    }

    /// Get column names as byte vectors. Backed by the column caches; lazy-populated.
    pub fn get_col_names_bytes(&mut self, table_id: i64) -> Rc<Vec<Vec<u8>>> {
        if let Some(bytes) = self.caches.col_names_bytes.get(&table_id) {
            return bytes.clone();
        }
        self.fill_column_caches(table_id);
        self.caches.col_names_bytes.get(&table_id).expect("just filled").clone()
    }

    /// Return the cached schema wire entry (block + derived wire properties)
    /// with the current schema version for `table_id`, or `None` if the block
    /// isn't yet cached. Wire props are paired with the block so they share
    /// invalidation.
    pub fn get_cached_schema_wire_block(&self, table_id: i64) -> Option<CachedSchemaWire> {
        let entry = self.caches.schema_wire_cache.get(&table_id)?;
        Some(CachedSchemaWire {
            entry: entry.clone(),
            version: self.caches.get_schema_version(table_id),
        })
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
    /// derived wire properties. Written together so the invalidation in
    /// `clear_col_cache_no_bump` keeps them consistent.
    pub fn set_schema_wire_block(&mut self, table_id: i64, entry: crate::catalog::cache::SchemaWireEntry) {
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

    // -- Store handle accessors -----------------------------------------------

    /// Get a `&mut PartitionedTable` for a user table, or `None` if the table is
    /// absent or not partitioned (e.g. a `Borrowed` system table).
    ///
    /// SAFETY: hands out `&mut` from `&self` through the same `UnsafeCell`
    /// contract as [`StoreHandle::as_partitioned_mut`] — no aliasing `&mut` into
    /// the same store may be live across the call.
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn get_ptable_handle(&self, table_id: i64) -> Option<&mut PartitionedTable> {
        self.dag
            .tables
            .get(&table_id)
            .and_then(|e| e.handle.as_partitioned_mut())
    }

    /// Get schema descriptor for a table. Registry-uniform: system tables are
    /// pre-registered before any caller can run, and an unknown id in the
    /// system range (the 8-10 gap) resolves to a graceful `None` instead of a
    /// panic.
    pub fn get_schema_desc(&self, table_id: i64) -> Option<SchemaDescriptor> {
        self.dag.tables.get(&table_id).map(|e| e.schema)
    }

    /// The on-disk directory of a user table (`{base_dir}/{schema}/{name}_{tid}`),
    /// the parent of its per-partition `part_{p}` subdirs. Guaranteed to exist on
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

    /// Returns all child tables that have FK constraints targeting `parent_id`.
    pub(crate) fn fk_children_of(&self, parent_id: i64) -> &[FkParentRef] {
        self.caches
            .fk_by_parent
            .get(&parent_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// `(schema, table, columns)` names for `(table_id, col_indices)`, each
    /// falling back to `"?"` when the catalog has no entry. The `columns` field
    /// joins every named column with `, ` (a composite `UNIQUE (a, b)` renders
    /// `"a, b"`; a single-column index renders identically to before). The
    /// fallback is defensive: the entity always exists on the
    /// constraint-violation paths that format these names. `get_column_names`
    /// returns an owned `Vec`, so it does not borrow-conflict with the later
    /// `get_qualified_name`.
    fn qualified_col_names(&mut self, table_id: i64, col_indices: &[u32]) -> (String, String, String) {
        let names = self.get_column_names(table_id);
        let col = col_indices
            .iter()
            .map(|&ci| names.get(ci as usize).cloned().unwrap_or_else(|| "?".to_string()))
            .collect::<Vec<_>>()
            .join(", ");
        let (sn, tn) = self.get_qualified_name(table_id).unwrap_or(("?", "?"));
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
}
