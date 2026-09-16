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

    /// Which relations exist, and the stores behind them. There are no thin
    /// delegators beside it: a `CatalogEngine::kind` would hide the rung
    /// its answer came from, and every call site has to be rewritten either way.
    pub(crate) fn registry(&self) -> &RelationRegistry {
        &self.registry
    }

    /// The DBSP layer's view state: plan cache, per-view metadata and the dependency map.
    pub(crate) fn dag(&self) -> &DagEngine {
        &self.dag
    }

    /// [`Self::registry`] for the callers that register, ingest into or flush a
    /// relation.
    pub(crate) fn registry_mut(&mut self) -> &mut RelationRegistry {
        &mut self.registry
    }

    /// `worker_count << 32 | STATE_FORMAT` for the count this process launched
    /// with — what a persisted record of derived state must carry to be honoured.
    pub(in crate::catalog) fn launched_topology(&self) -> u64 {
        super::registry::topology_word(self.registry.slot().of)
    }

    /// Whether persisted derived state was written under this boot's topology.
    /// Half of every resume verdict, and what the registry's `resume_enabled` is
    /// latched from.
    pub(crate) fn topology_matches(&self) -> bool {
        self.recorded_topology == self.launched_topology()
    }

    /// Both halves at once, proven disjoint here rather than asserted at each
    /// caller. The epoch entries need it: they read the plan cache while
    /// ingesting into the registry.
    ///
    /// **A caller that outlives the borrow — the server launders this pair
    /// through raw pointers, so its dispatch loop can serve pushes and reads
    /// while an epoch is parked — owes one thing: no relation may be registered
    /// or unregistered for the duration.** A `DdlSync` arriving mid-wait is
    /// queued and replayed after the epoch returns, which is what keeps the
    /// registry's map from rehashing under a live reference into it.
    pub(crate) fn dag_and_registry_mut(&mut self) -> (&mut DagEngine, &mut RelationRegistry) {
        (&mut self.dag, &mut self.registry)
    }

    /// The checkpoint generation.
    pub(crate) fn durable_generation(&self) -> u64 {
        self.durable_generation
    }

    /// The data directory this engine's relations live under.
    pub(crate) fn base_dir(&self) -> &str {
        &self.base_dir
    }

    /// Whether `view_id`'s checkpointed output was rejected at boot and must be
    /// rebuilt rather than resumed.
    pub(crate) fn view_is_invalid(&self, view_id: i64) -> bool {
        self.invalid_views.contains(&view_id)
    }

    /// Every view id [`Self::view_is_invalid`] holds for, in no defined order.
    pub(crate) fn invalid_views(&self) -> impl Iterator<Item = i64> + '_ {
        self.invalid_views.iter().copied()
    }

    /// Drop `view_id` from the invalid set once its output has been rebuilt.
    /// Returns whether it was there.
    pub(crate) fn clear_invalid_view(&mut self, view_id: i64) -> bool {
        self.invalid_views.remove(&view_id)
    }

    // -- FK / index metadata queries (for distributed validation) -------------

    /// All FK edges where `table_id` is the child (empty when none).
    pub(crate) fn fk_constraints_of(&self, table_id: i64) -> &[FkEdge] {
        self.caches
            .fk_by_child
            .get(&table_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Return the cached schema wire entry (block, version) for
    /// `table_id`, or `None` if the block isn't yet cached.
    pub(crate) fn get_cached_schema_wire_block(&self, table_id: i64) -> Option<SchemaWireEntry> {
        self.caches.schema_wire_cache.get(&table_id).cloned()
    }

    /// Return the current schema version for `table_id` (1 if unknown).
    pub(crate) fn get_schema_version(&self, table_id: i64) -> u16 {
        self.caches.get_schema_version(table_id)
    }

    /// Store an encoded schema wire block in the cache with the version it was
    /// built at — written together so the invalidation in
    /// `clear_col_cache_no_bump` keeps them consistent.
    pub(in crate::catalog) fn set_schema_wire_block(&mut self, table_id: i64, entry: SchemaWireEntry) {
        self.caches.schema_wire_cache.insert(table_id, entry);
    }

    /// The full set of table IDs that must be locked together for a write to
    /// `table_id`, sorted ascending to guarantee deadlock-free acquisition:
    /// `table_id` itself plus all FK parents (to guard concurrent parent
    /// DELETE) and FK children (to guard concurrent child INSERT during a
    /// parent DELETE). Empty if this table requires no lock at all.
    /// Materialized at DDL time by `recompute_needs_lock`; the borrow is
    /// sound on the push path because the caller holds the push read lock.
    pub(crate) fn fk_lock_set(&self, table_id: i64) -> &[i64] {
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
    pub(crate) fn has_row_constraints(&self, table_id: i64) -> bool {
        !self.fk_constraints_of(table_id).is_empty()
            || !self.fk_children_of(table_id).is_empty()
            || self.registry.relation(table_id).is_some_and(Relation::has_unique_index)
    }

    /// Does validating a write of `mode` to `table_id` read committed state?
    /// When false `validate_txn_distributed` finds nothing to check and skips
    /// the write entirely; the executor reads it for the same reason plus one of
    /// its own — such a write may hold its table lock shared, since nothing it
    /// does can be invalidated by a concurrent write to the same table.
    pub(crate) fn push_reads_committed_state(&self, table_id: i64, mode: gnitz_wire::WireConflictMode) -> bool {
        self.has_row_constraints(table_id) || matches!(mode, gnitz_wire::WireConflictMode::Error)
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

    /// `(index_id, is_unique)` of every live `sys_indices` row on exactly
    /// `(owner_id, cols)`, read from storage rather than the caches.
    pub(super) fn indices_on_cols(&self, owner_id: i64, cols: &[u32]) -> Vec<(i64, bool)> {
        let Some(ids) = self.caches.indices_by_owner.get(&owner_id) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for &idx_id in ids {
            let Some(sr) = self.live_sys_row(SysFamily::Index, idx_id) else {
                continue;
            };
            let (src, ri) = sr.source();
            // A malformed word matches no column list, so it is simply not this
            // owner's index — the register hook is where such a row is refused.
            let Ok((row_owner, row_cols, props)) = read_idx_tab_row(src, ri) else {
                continue;
            };
            if row_owner == owner_id && row_cols.as_slice() == cols {
                out.push((idx_id, props.is_unique));
            }
        }
        out
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
    /// already-committed data. A composite index passes its full `col_indices`,
    /// joined as `(a, b)`.
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

    /// Format the PK-uniqueness rejection, PG-style, from the offending key's raw
    /// OPK bytes. Taking the schema rather than a pre-rendered key plus its
    /// `pk_indices()` is what keeps the column names and the values a caller
    /// cannot pair from two different schemas. `in_batch` distinguishes two rows
    /// of one ingest batch sharing a PK from a collision with committed data.
    pub(crate) fn pk_violation_err(
        &mut self,
        table_id: i64,
        schema: &SchemaDescriptor,
        pk_bytes: &[u8],
        in_batch: bool,
    ) -> String {
        let key_str = &schema.format_pk_bytes(pk_bytes);
        let (sn, tn, cols) = self.qualified_col_names(table_id, schema.pk_indices());
        let what = if in_batch {
            format!("Batch contains multiple rows with key ({cols})=({key_str})")
        } else {
            format!("Key ({cols})=({key_str}) already exists")
        };
        format!("duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": {what}")
    }

    /// Format "an inserted child row references a value the parent does not
    /// hold".
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
