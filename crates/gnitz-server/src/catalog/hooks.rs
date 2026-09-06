use super::*;

use gnitz_wire::{SCHEMATAB_PAY_NAME, SEQTAB_PAY_VALUE};
use rustc_hash::FxHashMap;

/// What `register_relation` needs to build and register one relation: the
/// values decoded off its TABLE_TAB / VIEW_TAB row, plus the placement its own
/// family derives.
struct RelationRegistration {
    kind: RelationKind,
    id: i64,
    schema_id: i64,
    name: String,
    pk: PkColList,
    placement: Placement,
    /// The `WITH (…)` byte budgets; both `None` for a base table and for a plain
    /// view.
    budgets: ViewBudgets,
}

impl CatalogEngine {
    // -- Hook processing ---------------------------------------------------
    //
    // Dispatch is a static per-system-table sequence, and the order of calls
    // is the dependency order. The `apply_*` / `hook_*` split is described in
    // the module doc.
    //
    // Cross-family ordering contract: a relation's COL_TAB rows must be readable
    // from `sys_columns` STORAGE when its register hook runs, or the hook
    // registers a relation whose schema build finds no columns. Live DDL applies
    // a bundle in ascending `topo_priority`; boot replay opens every sys store
    // first.
    //
    // A hook re-asserts a precheck guard only where the verdict cannot depend on
    // application order and the hook already holds the guard's input.
    pub(in crate::catalog) fn fire_hooks(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        let reordered = Self::canonicalize_for_hooks(batch, family);
        let batch = reordered.as_ref().unwrap_or(batch);
        match family {
            SysFamily::Schema => {
                self.apply_schema_caches(batch);
                self.hook_schema_dir(batch)?;
            }
            SysFamily::Table => {
                self.apply_entity_caches(family, batch);
                self.apply_schema_members(batch);
                self.hook_relation_register(family, batch)?;
                self.relock_from_table_delta(batch);
                self.hook_cascade_fk(batch)?;
            }
            SysFamily::View => {
                self.apply_entity_caches(family, batch);
                self.apply_schema_members(batch);
                self.hook_relation_register(family, batch)?;
            }
            SysFamily::Column => {
                self.apply_col_names_invalidate(batch);
                self.apply_fk_edges_and_locks(batch);
                // MUST be last — after `apply_col_names_invalidate` evicts the
                // cached col defs — so the DROP NOT NULL descriptor rebuild reads
                // the post-ALTER column defs.
                self.hook_column_alter(batch)?;
            }
            SysFamily::Index => {
                self.apply_index_caches(batch);
                self.hook_index_register(batch)?;
            }
            // Restore the user-sequence high-water from a durably-committed or
            // SAL-replayed advance so a committed SERIAL id is never re-issued.
            SysFamily::Sequence => self.hook_sequence_register(batch),
            // The dependency map is derived from the `ScanDelta` nodes, so a
            // circuit-node write restates the graph. The circuit itself is
            // loaded by `load_circuit`, not by hooks.
            SysFamily::CircuitNodes => self.dag.invalidate_dep_map(),
        }
        Ok(())
    }

    /// The view every handler sees: retractions before insertions. `None` when
    /// `batch` already is, which is every forward producer — only
    /// `compensate_stage_a`'s in-place negation reverses a pair.
    ///
    /// Batch-wide, not per-PK: `apply_entity_caches`' `-1` arm reads the outgoing
    /// name from the cache, so an ALTER VIEW whose new vid's `+1` came first would
    /// unmap the *new* view's qualified name.
    fn canonicalize_for_hooks(batch: &Batch, family: SysFamily) -> Option<Batch> {
        // `false < true`, so "sorted by (weight > 0)" *is* "retractions first".
        if (0..batch.len()).is_sorted_by_key(|i| batch.get_weight(i) > 0) {
            return None;
        }
        // Total, because `precheck_family` rejects a delta carrying a zero weight.
        let mut idx: Vec<u32> = (0..batch.len())
            .filter(|&i| batch.get_weight(i) < 0)
            .map(|i| i as u32)
            .collect();
        idx.extend((0..batch.len()).filter(|&i| batch.get_weight(i) > 0).map(|i| i as u32));
        Some(Batch::from_indexed_rows(&batch.as_mem_batch(), &idx, family.schema()))
    }

    /// Fold a `sys_sequences` advance into the in-memory `user_sequences` map.
    /// Fires via `submit` (live durable range advances and SAL replay at
    /// recovery). Catalog sequences (`seq_id < FIRST_USER_TABLE_ID`) recover via
    /// the object-id hooks instead, so they are skipped — the guard is defensive
    /// since `advance_sequence` bypasses `submit` and never reaches here.
    fn hook_sequence_register(&mut self, batch: &Batch) {
        for i in 0..batch.len() {
            if batch.get_weight(i) <= 0 {
                continue;
            }
            let seq_id = batch.get_pk(i) as i64;
            let hw = payload_u64(batch, i, SEQTAB_PAY_VALUE) as i64;
            self.observe_user_sequence(seq_id, hw);
        }
    }

    // -- Hook handlers ---------------------------------------------------------

    fn hook_schema_dir(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.len() {
            let weight = batch.get_weight(i);
            let name = payload_string(batch, i, SCHEMATAB_PAY_NAME);
            let path = schema_dir(&self.base_dir, &name);
            if weight > 0 {
                // A prior DROP SCHEMA may have queued this exact (name-based)
                // path for checkpoint-gated removal; recreating it now must
                // cancel that, or the gating checkpoint would wipe the new
                // schema and its tables.
                self.cancel_gated_deletion(&path);
                // A Stage-A rollback of this CREATE re-fires the hook with the
                // row negated, and the else-arm below queues the path for the
                // compensation's own drain — so the directory needs no staging.
                ensure_dir(&path)?;
                let _ = fsync_dir(&path);
                let _ = fsync_dir(&self.base_dir);
            } else {
                // Drained only on the master live-DDL path, where
                // `precheck_schema_family`'s CAS has proved this payload name is
                // the retracted schema's own.
                self.pending_dir_deletions.push(path);
            }
        }
        Ok(())
    }

    /// Build a relation's store and enter it in the registry — the `+1` half of
    /// [`hook_relation_register`](Self::hook_relation_register), over the values
    /// its per-family builder decoded.
    fn register_relation(&mut self, reg: RelationRegistration) -> Result<(), String> {
        let RelationRegistration {
            kind,
            id,
            schema_id,
            name,
            pk,
            placement,
            budgets,
        } = reg;
        let col_defs = self.read_column_defs(id);
        let schema_name = self.caches.schema_by_id.get(&schema_id).cloned().unwrap_or_default();
        let directory = relation_dir(&self.base_dir, &schema_name, kind, id);
        let schema = build_schema_from_col_defs(kind, &col_defs, pk.as_slice(), placement)
            .map_err(|e| format!("{} '{name}' (id={id}) {e}", kind.noun()))?;
        gnitz_debug!(
            "catalog: creating {} dir={} name={} id={} workers={}",
            kind.noun(),
            directory,
            name,
            id,
            self.registry.num_workers()
        );
        // Bound to this open because it needs the previous child set, which the
        // open would shadow and `reconcile_child_dirs` then deletes. Views are
        // exempt: a worker-count change invalidates every one, so
        // `rebuild_invalid_views` refills them from base.
        if self.ctx.mode() == ApplyMode::Replay && kind.is_base_table() {
            gnitz_store::storage::repartition_relation(&directory, &schema, id as u32, self.registry.num_workers())?;
        }
        // `register` owns the staged-directory reclaim and the parent fsync.
        self.registry
            .register(RelationSpec { id, kind, schema, directory, budgets })?;
        raise_id_counter(&mut self.next_table_id, id);
        Ok(())
    }

    /// Tear a relation out of the registry — the shared net-dead `-1` half of
    /// the two register hooks. `cascade` retracts the dependent system rows the
    /// relation owns (indices/columns for a table, circuit/columns for a
    /// view) and runs before the unregister, while the entry is still resolvable.
    ///
    /// The version counters are purged AFTER the cascade: its own
    /// `apply_index_caches` / `invalidate_col_names` bumps would otherwise
    /// `or_insert` them straight back.
    fn drop_relation(&mut self, id: i64, cascade: impl FnOnce(&mut Self) -> Result<(), String>) -> Result<(), String> {
        let Some(directory) = self.registry.entry(id).map(|e| e.directory.clone()) else {
            return Ok(());
        };
        cascade(self)?;
        let CatalogEngine { registry, dag, .. } = self;
        dag.unregister_table(registry, id);
        self.pending_dir_deletions.push(directory);
        self.caches.purge_schema_version(id);
        Ok(())
    }

    /// The TABLE_TAB / VIEW_TAB register hook. One loop for both families: a
    /// row's side effect is gated on its NET live state, not its own sign, so a
    /// rename pair — net-live before and after — fires neither the `+1`
    /// registration nor the `-1` teardown, in any row order and on every
    /// application path. Only the per-row registration values and the teardown
    /// cascade differ by family.
    fn hook_relation_register(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        for i in self.relation_row_order(family, batch) {
            let weight = batch.get_weight(i);
            let id = batch.get_pk(i) as i64;
            // Storage is applied before hooks fire, so a rename pair reads
            // net-live and a genuine drop reads net-dead — including at boot
            // replay, where the pair has already folded to `+1`.
            let net_live = self.sys_store(family).has_pk_bytes(batch.get_pk_bytes(i));

            if weight > 0 {
                // System tables are pre-registered by `register_system_table_families`
                // before `replay_catalog` fires hooks, so their rows show up here
                // with the DAG already populated; a rename pair's `+1` likewise
                // finds the id already registered. Skip to avoid double-register.
                // A `+1` whose net folds to dead (a create cancelled within its
                // own bundle) registers nothing.
                if !net_live || self.registry.has_id(id) {
                    continue;
                }
                let reg = if family == SysFamily::Table {
                    let (schema_id, name, pk, kind, placement) =
                        read_table_tab_row(batch, i).map_err(|e| format!("{e} (tid={id})"))?;
                    RelationRegistration {
                        kind,
                        id,
                        schema_id,
                        name,
                        pk,
                        placement,
                        budgets: ViewBudgets::default(),
                    }
                } else {
                    self.view_registration(batch, i, id)?
                };
                self.register_relation(reg)?;
                // Registration leaves a view EMPTY. Filling it is the server's:
                // `backfill_views_in_dep_order` for a live CREATE,
                // checkpoint resume, or the master's invalid-view rebuild at boot.
                // Filling here would double-count against all three.
            } else if !net_live {
                // A genuine drop. A rename pair's `-1` is net-live, so this
                // teardown is skipped and the registration survives untouched.
                self.drop_relation(id, |s| s.cascade_relation_children(family, id))?;
            }
        }
        Ok(())
    }

    /// The rows of a relation delta in the order the register hook must process
    /// them: retractions first, then the live rows in dependency order.
    /// Registering a view reads its sources' stamped `Placement` and `depth`, and
    /// nothing about the order the hook is handed — a wire bundle's row order, or
    /// boot replay's walk of VIEW_TAB in PK order — puts a view after the views
    /// it scans. Without this a view is replicated before a restart and
    /// partitioned after.
    /// A table's registration reads no other relation, so TABLE_TAB keeps row
    /// order.
    fn relation_row_order(&mut self, family: SysFamily, batch: &Batch) -> Vec<usize> {
        let mut rows: Vec<usize> = (0..batch.len()).collect();
        if family != SysFamily::View {
            return rows;
        }
        // `canonicalize_for_hooks` already put the retractions first, so the live
        // rows are the suffix from here.
        let first_live = rows.partition_point(|&i| batch.get_weight(i) <= 0);
        let ids: Vec<i64> = rows[first_live..].iter().map(|&i| batch.get_pk(i) as i64).collect();
        let rank: FxHashMap<i64, usize> = self
            .dag
            .order_by_view_deps(&self.registry, &ids)
            .into_iter()
            .enumerate()
            .map(|(r, vid)| (vid, r))
            .collect();
        // Stable, so a malformed batch's duplicate `+1`s on one id keep row
        // order among themselves.
        rows[first_live..].sort_by_key(|&i| rank[&(batch.get_pk(i) as i64)]);
        rows
    }

    /// The registration values for VIEW_TAB row `i`: placement folded out of the
    /// view's sources' own stamped placements, plus the `WITH` budgets.
    ///
    /// The view's physical PK is the persisted leading-k column list: a single
    /// synthetic hash column for join/set-op/distinct views, or the source PK
    /// passed through (0..k) for a plain projection over a compound-PK table.
    fn view_registration(&mut self, batch: &Batch, i: usize, vid: i64) -> Result<RelationRegistration, String> {
        let (schema_id, name, pk, budgets, owner_view_id) = read_view_tab_row(batch, i)?;
        // The circuit's `circuit_nodes` are persisted before this VIEW_TAB row,
        // so `get_source_ids` resolves here. Re-check for the paths that skip the
        // precheck (boot replay, worker `ddl_sync`).
        let source_ids = self.dag.get_source_ids(&self.registry, vid);
        self.validate_view_options(vid, &name, budgets, owner_view_id, &source_ids)?;
        // Stamping the fold is what makes placement transitive:
        // `relation_row_order` registers this view after its sources, so a view
        // over it reads the answer back off one value.
        let CatalogEngine { registry, dag, .. } = self;
        let placement = dag.view_placement(registry, vid, &source_ids, pk.as_slice().len());
        Ok(RelationRegistration {
            kind: RelationKind::View,
            id: vid,
            schema_id,
            name,
            pk,
            placement,
            budgets,
        })
    }

    /// Retract the dependent system rows a dropped relation owns: its indices
    /// (a table) or its circuit (a view), plus its column records either way.
    ///
    /// `precheck_family` rejects FK/view-dep-blocked drops before the `-1` row
    /// reaches the WAL, so the cascade never has to ask whether the drop is
    /// allowed.
    ///
    /// A dropped view also retracts the internal chain segments it owns — the
    /// client neither names nor knows them, so this is their sole retractor.
    /// Terminates because segments are flat: every one names the user view as
    /// its owner, so a segment owns none.
    ///
    /// Under compensation ONLY the index cascade runs: the column, circuit and
    /// segment rows are in compensation's own drained set, so cascading them too
    /// would retract each twice → a net `-1` ghost. FK auto-indices are the dual
    /// — `submit_local` never enqueues them, so this cascade is theirs too.
    fn cascade_relation_children(&mut self, family: SysFamily, id: i64) -> Result<(), String> {
        if family == SysFamily::Table {
            self.cascade_retract_indices(id)?;
        }
        if self.ctx.in_rollback() {
            return Ok(());
        }
        if family == SysFamily::View {
            self.cascade_retract_circuit(id)?;
            self.cascade_retract_segments(id)?;
        }
        self.cascade_retract_columns(id)
    }

    fn cascade_retract_segments(&mut self, owner_id: i64) -> Result<(), String> {
        // Copied out: the submit below re-enters `apply_entity_caches`, which
        // removes from this very list.
        let Some(ids) = self.caches.segments_by_owner.get(&owner_id) else {
            return Ok(());
        };
        let ids: Vec<u128> = ids.iter().map(|&id| id as u128).collect();
        let schema = SysFamily::View.schema();
        let batch = retract_pk_list(self.sys_store(SysFamily::View), schema, ids);
        if !batch.is_empty() {
            self.submit_cascade(SysFamily::View, batch)?;
        }
        Ok(())
    }

    fn cascade_retract_indices(&mut self, owner_id: i64) -> Result<(), String> {
        // Copied out: the submit below re-enters `apply_index_caches`, which
        // removes from this very list.
        let Some(ids) = self.caches.indices_by_owner.get(&owner_id) else {
            return Ok(());
        };
        let ids: Vec<u128> = ids.iter().map(|&id| id as u128).collect();
        let schema = SysFamily::Index.schema();
        let batch = retract_pk_list(self.sys_store(SysFamily::Index), schema, ids);
        if !batch.is_empty() {
            self.submit_cascade(SysFamily::Index, batch)?;
        }
        Ok(())
    }

    fn cascade_retract_columns(&mut self, owner_id: i64) -> Result<(), String> {
        let schema = SysFamily::Column.schema();
        let (start_pk, end_pk) = column_id_band(owner_id);
        let start = sys_opk(schema, start_pk as u128);
        let end = sys_opk(schema, end_pk as u128);
        let batch = retract_key_range(
            self.sys_store(SysFamily::Column),
            schema,
            start.pk_bytes(),
            end.pk_bytes(),
        );
        if !batch.is_empty() {
            self.submit_cascade(SysFamily::Column, batch)?;
        }
        Ok(())
    }

    /// True if `col_idx` appends a *trailing* column to registered relation
    /// `owner_id`. `precheck_column_append` admits exactly this shape and
    /// `hook_column_alter` recognizes an append by the same test, so the worker
    /// `ddl_sync` and SAL-replay paths — which bypass precheck entirely — act on
    /// exactly what the master admitted.
    pub(super) fn is_trailing_col_append(&self, owner_id: i64, col_idx: u64) -> bool {
        self.registry
            .entry(owner_id)
            .is_some_and(|e| col_idx as usize == e.schema.num_columns())
    }

    /// Column-ALTER side effect: rebuild the owner's descriptor from the
    /// (freshly invalidated) column defs and publish it into the store in place.
    /// A `is_nullable 0→1` flip (DROP NOT NULL) moves the whole-schema payload
    /// comparator `FixedIntNonnull → Generic`, and a trailing append (ADD COLUMN)
    /// grows the region count; RENAME/DROP COLUMN reach the rebuild and no-op
    /// there, since neither changes a descriptor field.
    ///
    /// The trigger is the batch's per-PK shape against the owner's
    /// currently-registered arity, which is what makes it right for the
    /// compensation path, where the pair arrives negated. Replay carries no
    /// transition to react to, so it returns before the scan rather than through it.
    fn hook_column_alter(&mut self, batch: &Batch) -> Result<(), String> {
        if self.ctx.mode() == ApplyMode::Replay {
            return Ok(());
        }
        // COL_TAB PK = pack_col_id. `pk_signatures` skips `w == 0`, so "no `-1`"
        // is "carries only `+1`s".
        for sig in pk_signatures(batch) {
            let (owner, col_idx) = gnitz_wire::unpack_col_id(sig.pk as u64);
            let owner = owner as i64;
            let is_append = sig.neg.is_none() && self.is_trailing_col_append(owner, col_idx);
            if !(sig.is_pair() || is_append) {
                continue;
            }
            let Some(cur) = self
                .registry
                .entry(owner)
                .filter(|e| e.kind.is_base_table())
                .map(|e| e.schema)
            else {
                continue;
            };
            // Rebuild from the post-invalidate col defs. `cur.placement()` is
            // construction, not a workaround for `eq` ignoring it: without it the
            // rebuild is stamped KEYED_DEFAULT and loses CLUSTER BY / REPLICATED.
            // Only an is_nullable 0→1 flip or a trailing append changes the
            // descriptor; when it does, publish it into the store.
            let col_defs = self.read_column_defs(owner);
            let rebuilt =
                build_schema_from_col_defs(RelationKind::BaseTable, &col_defs, cur.pk_indices(), cur.placement())
                    .map_err(|e| format!("column ALTER on table id={owner}: {e}"))?;
            if rebuilt != cur {
                let CatalogEngine { registry, dag, .. } = self;
                dag.swap_table_schema(registry, owner, rebuilt)?;
            }
        }
        Ok(())
    }

    fn cascade_retract_circuit(&mut self, vid: i64) -> Result<(), String> {
        let family = SysFamily::CircuitNodes;
        let schema = family.schema();
        // The family uses the compound PK `(view_id, node_id)`, so one view's
        // rows are the key band `[(vid, 0), (vid + 1, 0))`.
        let start = circuit_opk(schema, vid, 0);
        let end = circuit_opk(schema, vid + 1, 0);
        let batch = retract_key_range(self.sys_store(family), schema, start.pk_bytes(), end.pk_bytes());
        if !batch.is_empty() {
            self.submit_cascade(family, batch)?;
        }
        Ok(())
    }

    /// The IDX_TAB register hook: a sign dispatch over the batch, mirroring
    /// `hook_relation_register`. `apply_index_caches` has already populated the
    /// name-indexed caches from `IDXTAB_PAY_NAME`, so neither half below reads
    /// that string.
    fn hook_index_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.len() {
            let idx_id = batch.get_pk(i) as i64;
            let (owner_id, cols, props) =
                read_idx_tab_row(batch, i).map_err(|rule| format!("index {idx_id}: column list {rule}"))?;
            if batch.get_weight(i) > 0 {
                self.register_index(idx_id, owner_id, &cols, props.is_unique)?;
            } else {
                self.unregister_index(owner_id, cols.as_slice());
            }
        }
        Ok(())
    }

    /// Enter an index circuit for a `+1` IDX_TAB row — which opens this
    /// process's store — and fill it: the `+1` half of
    /// [`Self::hook_index_register`].
    ///
    /// One circuit per column list (dedup by ordered list). An incumbent circuit
    /// means no second store is opened, but a UNIQUE newcomer over a non-unique
    /// incumbent promotes it. Promotion is order-independent — the circuit is
    /// unique iff ANY index on the column list is unique — so replay reconstructs
    /// an identical result whatever order the index ids arrive in.
    fn register_index(&mut self, idx_id: i64, owner_id: i64, cols: &PkColList, is_unique: bool) -> Result<(), String> {
        // Keep worker next_index_id in sync with master-assigned IDs so that
        // create_fk_indices → allocate_index_id never collides with an explicit
        // user index that was broadcast via IDX_TAB +1.
        raise_id_counter(&mut self.next_index_id, idx_id);

        // Boot replay and worker ddl_sync reach this hook without
        // `precheck_family`, so re-run the shared registration guards here (see
        // `validate_index_registration`). Resolve the owner entry once for
        // everything below.
        let entry = self.validate_index_registration(owner_id)?;
        let owner_dir = entry.directory.clone();

        if let Some(was_unique) = entry.index_circuit_on(cols.as_slice()).map(|ic| ic.is_unique) {
            if is_unique && !was_unique {
                self.promote_index_to_unique(owner_id, cols.as_slice());
            }
            return Ok(());
        }

        let idx_dir = index_dir(&owner_dir, idx_id);
        let cols = *cols;

        // A failed CREATE INDEX must leave nothing on disk: the stage removes
        // `idx_dir` recursively, child subdirs included. The stage is a local,
        // not an entry in `pending_dir_deletions`, so that queue keeps one
        // meaning — directories of *dropped* entities, which a rollback must
        // therefore keep. `add_index` bounds-checks and promotes every column
        // (defence in depth at the catalog trust boundary; a crafted wire row
        // could name an out-of-range or ineligible column).
        staged_dir(&idx_dir, || {
            self.registry.add_index(owner_id, idx_id, cols.as_slice(), is_unique)?;
            let resumed = self
                .registry
                .index_circuit_for_cols(owner_id, cols.as_slice())
                .is_some_and(IndexCircuitEntry::resumed_from_checkpoint);
            // The master never populates its index copies (they stay permanently
            // empty; distributed HAS_PK/seek probes union the workers' slice-local
            // copies). Workers and standalone backfill from their local base slice
            // — unless the store resumed from a checkpointed manifest, which
            // already holds those rows.
            if !self.ctx.in_rollback() && !self.is_master && !resumed {
                if let Err(e) = self.backfill_index(owner_id, cols.as_slice()) {
                    // The circuit was entered before the backfill so the
                    // projection could ingest through it; a failed CREATE INDEX
                    // leaves no circuit.
                    self.registry.remove_index_circuit(owner_id, cols.as_slice());
                    return Err(e);
                }
            }
            Ok(())
        })
    }

    /// Demote or destroy `owner_id`'s circuit on `cols` after a `-1` IDX_TAB row
    /// — the `-1` half of [`Self::hook_index_register`]. The retraction is
    /// already applied to sys_indices when hooks fire, so its net weight is 0 and
    /// the scan below sees only what survives it.
    fn unregister_index(&mut self, owner_id: i64, cols: &[u32]) {
        let mut remains: Option<bool> = None;
        self.for_each_index_on_cols(owner_id, cols, |_row_id, is_uniq| {
            *remains.get_or_insert(false) |= is_uniq;
        });
        if let Some(remains_unique) = remains {
            // Another index (e.g. the FK auto-index) still covers this column
            // list. Demote the circuit rather than destroying it.
            self.registry
                .set_index_circuit_uniqueness(owner_id, cols, remains_unique);
            return;
        }
        // No index remains on the column list — drop the circuit. The directory
        // path uses the *creating* index_id, not the dropped one: when a second
        // index promoted an incumbent circuit, the real directory on disk carries
        // the first registrant's id.
        let creating = self
            .registry
            .entry(owner_id)
            .and_then(|e| e.index_circuit_on(cols).map(|ic| (e.directory.clone(), ic.index_id)));
        if let Some((owner_dir, creating_idx_id)) = creating {
            self.registry.remove_index_circuit(owner_id, cols);
            self.pending_dir_deletions.push(index_dir(&owner_dir, creating_idx_id));
        }
    }

    fn hook_cascade_fk(&mut self, batch: &Batch) -> Result<(), String> {
        // Live only. Replay and compensation both restore the persisted IDX_TAB
        // rows themselves, so minting here would duplicate them under fresh ids.
        // Gated before the fold, which is otherwise a pass over every table in the
        // database on the boot full scan.
        if self.ctx.mode() != ApplyMode::Live {
            return Ok(());
        }
        // Paired tids excluded: an auto-index name embeds the *current* table
        // name, so a rename's `+1` would mint a second one its by-name dedup
        // cannot suppress. Read off the batch, because this hook also fires where
        // no master-threaded set exists (worker `ddl_sync`, SAL-tail recovery).
        for tid in family_pks_by_sign(batch, true) {
            self.create_fk_indices(tid)?;
        }
        Ok(())
    }
}
