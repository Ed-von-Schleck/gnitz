//! FK / secondary-index metadata queries (for distributed validation),
//! the store-handle and schema-descriptor accessors, the cached
//! schema-wire block, and unique-violation message formatting.

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

impl CatalogEngine {
    // -- FK / index metadata queries (for distributed validation) -------------

    /// Number of FK constraints on a table.
    pub fn get_fk_count(&self, table_id: i64) -> usize {
        self.caches.fk_by_child.get(&table_id).map(|v| v.len()).unwrap_or(0)
    }

    /// Get FK constraint at index: (fk_col_idx, target_table_id, target_col_idx).
    pub fn get_fk_constraint(&self, table_id: i64, idx: usize) -> Option<(usize, i64, usize)> {
        self.caches
            .fk_by_child
            .get(&table_id)
            .and_then(|v| v.get(idx))
            .map(|c| (c.fk_col_idx, c.target_table_id, c.target_col_idx))
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
        self.dag
            .tables
            .get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_indices.as_slice() == cols))
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

    /// Get child info at index: (child_table_id, fk_col_idx, parent_col_idx).
    pub fn get_fk_child_info(&self, parent_id: i64, idx: usize) -> Option<(i64, usize, usize)> {
        self.caches
            .fk_by_parent
            .get(&parent_id)
            .and_then(|v| v.get(idx))
            .map(|r| (r.child_tid, r.fk_col_idx, r.parent_col_idx))
    }

    /// Get the index schema for a specific column list's index on a table.
    pub fn get_index_schema_by_cols(&self, table_id: i64, cols: &[u32]) -> Option<SchemaDescriptor> {
        self.dag
            .tables
            .get(&table_id)
            .and_then(|e| e.index_circuits.iter().find(|ic| ic.col_indices.as_slice() == cols))
            .map(|ic| ic.index_schema)
    }

    /// Get column names for a table/view. Cached after first lookup.
    pub fn get_column_names(&mut self, table_id: i64) -> Vec<String> {
        if let Some(names) = self.caches.col_names.get(&table_id) {
            return names.clone();
        }
        let names: Vec<String> = self.read_column_defs(table_id).into_iter().map(|cd| cd.name).collect();
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
        let (block, wire_safe, wire_row_fixed_stride) = self.caches.schema_wire_cache.get(&table_id)?.clone();
        let version = self.caches.get_schema_version(table_id);
        Some(CachedSchemaWire {
            block,
            version,
            wire_safe,
            wire_row_fixed_stride,
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
    /// derived wire properties. The two are written together so the
    /// invalidation in `clear_col_cache_no_bump` keeps them consistent.
    pub fn set_schema_wire_block(
        &mut self,
        table_id: i64,
        block: Rc<Vec<u8>>,
        wire_safe: bool,
        wire_row_fixed_stride: u32,
    ) {
        self.caches
            .schema_wire_cache
            .insert(table_id, (block, wire_safe, wire_row_fixed_stride));
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
    pub(crate) fn get_ptable_handle(&self, table_id: i64) -> Option<*mut PartitionedTable> {
        self.dag.tables.get(&table_id).and_then(|e| match &e.handle {
            StoreHandle::Partitioned(cell) => Some(unsafe { &mut **cell.get() as *mut PartitionedTable }),
            _ => None,
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
