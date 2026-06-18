//! Multi-worker partition management (fork / trim / rehome / replication)
//! and the flushed-LSN bookkeeping that recovery and the DDL zone
//! allocator read.

use super::*;

const SYS_TABLE_IDS: &[i64] = &[
    SCHEMA_TAB_ID, TABLE_TAB_ID, VIEW_TAB_ID, COL_TAB_ID, IDX_TAB_ID,
    DEP_TAB_ID, SEQ_TAB_ID,
    CIRCUIT_NODES_TAB_ID, CIRCUIT_EDGES_TAB_ID, CIRCUIT_NODE_COLUMNS_TAB_ID,
];

impl CatalogEngine {
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
    ///
    /// **Single-partition stores are exempt.** A `num_partitions == 1` store
    /// (a replicated base table's full copy, or a replicated-derived view's local
    /// slice) holds its whole local dataset at partition 0, built across the fork
    /// at `[0, 1)`. A worker whose range excludes 0 (every worker but worker 0)
    /// would otherwise drop it here, and the subsequent FLAG_PUSH replay would
    /// no-op into an empty `tables` vec — silent data loss after every reboot with
    /// W > 1. Exempting them keeps the copy on every worker, which is the whole
    /// point of replication.
    pub fn trim_worker_partitions(&mut self, start: u32, end: u32) {
        for entry in self.dag.tables.values() {
            if let Some(ptable) = entry.handle.as_partitioned_mut() {
                if ptable.num_partitions() == 1 { continue; }
                ptable.close_partitions_outside(start, end);
            }
        }
    }

    /// Re-home every inherited single-partition store to THIS worker's own
    /// partition index `part_start`. The pre-fork master builds a replicated base
    /// table (or replicated-derived view) at `part_0`; workers inherit that
    /// `part_0` store across the fork. Because all workers share the data
    /// directory, leaving them at `part_0` collides on flush (every worker writing
    /// the same shard files). Each worker therefore rebuilds the store at
    /// `part_{part_start}`. The inherited store is empty — the master ingests no
    /// user data — so nothing is lost; the subsequent FLAG_PUSH replay fills the
    /// re-homed store. Called post-fork after `set_active_partitions`, before the
    /// user-data replay. (The live CREATE path already builds the store at the
    /// worker's own `part_start`, so it needs no re-home; this only repairs the
    /// recovery inherit.)
    pub fn rehome_single_partition_stores(&mut self, part_start: u32) -> Result<(), String> {
        // Worker 0's `part_start` is 0 — the inherited store already lives at
        // `part_0`, so its re-home target is its current home. Skip the no-op
        // rebuild (and the empty-store allocation it would do).
        if part_start == 0 {
            return Ok(());
        }
        let tids: Vec<i64> = self.dag.tables.iter()
            .filter(|(_, e)| e.handle.as_partitioned_mut()
                .map(|p| p.num_partitions() == 1).unwrap_or(false))
            .map(|(&tid, _)| tid)
            .collect();
        for tid in tids {
            let entry = self.dag.tables.get_mut(&tid).expect("tid taken from iter");
            let pt = PartitionedTable::new(
                &entry.directory, "", entry.schema, tid as u32,
                1, entry.kind.persistence(), part_start, part_start + 1,
                crate::storage::partition_arena_size(1),
            ).map_err(|e| format!("rehome single-partition store tid={tid}: {e:?}"))?;
            entry.handle = StoreHandle::Partitioned(std::cell::UnsafeCell::new(Box::new(pt)));
        }
        Ok(())
    }

    /// True iff a read of relation `id` (base table or view) must be
    /// **single-sourced** because its output is replicated — a full identical
    /// copy lives on every worker. A base table carries the flag on its schema;
    /// a view is replicated iff all its sources are (design §4.2). Consulted by
    /// the scan dispatch so a replicated relation is read from one worker instead
    /// of gathering N identical copies. SEEK already unicasts to one worker, so it
    /// needs no equivalent check.
    pub fn relation_output_is_replicated(&mut self, id: i64) -> bool {
        match self.dag.tables.get(&id) {
            Some(e) if e.kind.is_base_table() => return e.schema.replicated(),
            Some(_) => {} // view — fall through (releases the `tables` borrow)
            None => return false,
        }
        self.dag.view_all_sources_replicated(id)
    }

    /// Invalidate all cached plans.
    pub fn invalidate_all_plans(&mut self) {
        self.dag.invalidate_all();
    }

    /// Get max flushed LSN for a table. Recovery itself reads the bulk map
    /// from `collect_all_flushed_lsns`; this single-table form is test-only.
    #[cfg(test)]
    pub(crate) fn get_max_flushed_lsn(&self, table_id: i64) -> u64 {
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
}
