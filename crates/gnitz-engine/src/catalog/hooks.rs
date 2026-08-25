use super::*;

use crate::foundation::fault::Seam;
use crate::schema::make_index_schema;
use gnitz_wire::{SCHEMATAB_PAY_NAME, SEQTAB_PAY_VALUE};
use rustc_hash::FxHashMap;

/// `GNITZ_INJECT_TABLE_CREATE_DELAY_MS`: stall a user table's create between its
/// directory and its child subdir, so a concurrent DROP races it.
static TABLE_CREATE_DELAY: Seam = Seam::new("GNITZ_INJECT_TABLE_CREATE_DELAY_MS");

/// What `register_relation` needs to build and register one relation: the
/// values decoded off its TABLE_TAB / VIEW_TAB row, plus the placement and
/// depth its own family derives.
struct RelationRegistration {
    kind: RelationKind,
    id: i64,
    schema_id: i64,
    name: String,
    pk: PkColList,
    placement: Placement,
    depth: i32,
    /// The `WITH (…)` byte budgets; both `None` for a base table and for a plain
    /// view.
    budgets: crate::query::ViewBudgets,
}

impl CatalogEngine {
    // -- Hook processing ---------------------------------------------------
    //
    // Dispatch is a static per-system-table sequence, and the order of calls
    // is the dependency order. The `apply_*` / `hook_*` split is described in
    // the module doc.
    //
    // Cross-sys-table ordering contract (required by `hook_relation_register`,
    // which reads sys_columns via `read_column_defs`):
    //
    //   COL_TAB writes MUST precede TABLE_TAB / VIEW_TAB writes.
    //
    // Where this is enforced:
    //   * Live DDL: every catalog write arrives as one `FLAG_DDL_TXN` bundle,
    //     and the server ingest loop (`handle_ddl_txn`) applies the bundle's
    //     families in ascending `SysFamily::topo_priority` order under one zone —
    //     COL_TAB before the TABLE_TAB/VIEW_TAB register hook that reads
    //     it. The client send order is irrelevant; the server sorts.
    //   * Wire: the SAL is a single FIFO; worker `FLAG_DDL_SYNC` dispatch
    //     preserves master broadcast order.
    //   * Replay (`bootstrap.rs::replay_catalog`): TABLE_TAB is replayed
    //     before COL_TAB, but `read_column_defs` reads sys_columns storage
    //     directly (loaded at open time), not the cache, so the ordering
    //     holds.
    pub(crate) fn fire_hooks(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        // Dispatch every handler over a sign-partitioned view (retractions
        // before insertions) so a rewrite pair applies in retract-then-insert
        // order for every fold, on the forward and the (negated) compensation
        // path alike. A weight-homogeneous CREATE/DROP bundle is single-sign, so
        // the partition is identity and `sign_partition_batch` returns `None` (no
        // copy); only a mixed-sign bundle (a rename pair) is reordered into a
        // private copy — the received `batch` (broadcast to workers, negated on
        // rollback) is left untouched.
        let reordered = Self::sign_partition_batch(batch, &family.schema());
        let batch = reordered.as_ref().unwrap_or(batch);
        // Exhaustive over `SysFamily`: a newly-added family is a compile error
        // here, not a silently-skipped `_` arm. The no-op families
        // (Sequence/Circuit*) are named, not swallowed by a wildcard.
        match family {
            SysFamily::Schema => {
                self.apply_schema_caches(batch);
                self.hook_schema_dir(batch)?;
            }
            SysFamily::Table => {
                self.apply_entity_caches(batch);
                self.apply_schema_members(batch);
                self.hook_relation_register(SysFamily::Table, batch)?;
                self.relock_from_table_delta(batch);
                self.hook_cascade_fk(batch)?;
            }
            SysFamily::View => {
                self.apply_entity_caches(batch);
                self.apply_schema_members(batch);
                self.hook_relation_register(SysFamily::View, batch)?;
            }
            SysFamily::Column => {
                self.apply_col_names_invalidate(batch);
                self.apply_fk_constraints(batch);
                self.relock_from_column_delta(batch);
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
            // circuit-node write restates the graph. Edges and node columns
            // carry no dependency and the circuit itself is loaded by
            // `load_circuit`, not by hooks.
            SysFamily::CircuitNodes => self.dag.invalidate_dep_map(),
            SysFamily::CircuitEdges | SysFamily::CircuitNodeColumns => {}
        }
        Ok(())
    }

    /// A sign-partitioned copy of `batch` — all `weight < 0` rows first (in
    /// original order), then all `weight > 0` rows — when it carries both signs;
    /// `None` when weight-homogeneous (the partition is identity, so the caller
    /// dispatches the original with no copy). This restores the natural Z-set
    /// application order (every retraction observes the pre-bundle state for its
    /// key) for every `apply_*` fold at once, on the forward path and on the
    /// compensation path (where in-place negation flips a client's `-1`-first
    /// pair to `+1`-first, which this re-canonicalizes by the batch's *current*
    /// signs). Catalog bundles are ±1, so the `from_indexed_rows` gather (which
    /// asserts nonzero weights) is sound.
    fn sign_partition_batch(batch: &Batch, schema: &SchemaDescriptor) -> Option<Batch> {
        let (mut has_neg, mut has_pos) = (false, false);
        for i in 0..batch.count {
            let w = batch.get_weight(i);
            has_neg |= w < 0;
            has_pos |= w > 0;
            if has_neg && has_pos {
                break;
            }
        }
        if !(has_neg && has_pos) {
            return None;
        }
        let mut indices: Vec<u32> = Vec::with_capacity(batch.count);
        indices.extend((0..batch.count).filter(|&i| batch.get_weight(i) < 0).map(|i| i as u32));
        indices.extend((0..batch.count).filter(|&i| batch.get_weight(i) > 0).map(|i| i as u32));
        Some(Batch::from_indexed_rows(&batch.as_mem_batch(), &indices, schema))
    }

    /// Whether `pk_bytes` (the OPK key) names a *net-live* row in `family`'s
    /// store — net positive weight through the merged cursor. Storage is applied
    /// before hooks fire, so a rename pair's shared PK reads net-live (its `-1`
    /// and `+1` are both present) while a genuine drop reads net-dead. The gate
    /// for the reconciling register hooks: a different question than
    /// batch-local pair detection, and robust even to the consolidated sys tables
    /// at boot replay (where a rename pair has folded to net `+1`, no `-1`
    /// surviving) and to a multi-op recovery batch on one id.
    fn sys_pk_is_live(&self, family: SysFamily, pk_bytes: &[u8]) -> bool {
        self.sys_store(family).open_cursor().advance_to_exact_live(pk_bytes)
    }

    /// Fold a `sys_sequences` advance into the in-memory `user_sequences` map.
    /// Fires via `submit` (live durable range advances and SAL replay at
    /// recovery). Catalog sequences (`seq_id < FIRST_USER_TABLE_ID`) recover via
    /// the object-id hooks instead, so they are skipped — the guard is defensive
    /// since `advance_sequence` bypasses `submit` and never reaches here.
    fn hook_sequence_register(&mut self, batch: &Batch) {
        for i in 0..batch.count {
            if batch.get_weight(i) <= 0 {
                continue;
            }
            let seq_id = batch.get_pk(i) as i64;
            let hw = batch.read_payload_u64(i, SEQTAB_PAY_VALUE) as i64;
            self.observe_user_sequence(seq_id, hw);
        }
    }

    // -- Hook handlers ---------------------------------------------------------

    fn hook_schema_dir(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let weight = batch.get_weight(i);
            let name = batch.read_payload_string(i, SCHEMATAB_PAY_NAME);
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
                // Best-effort, here and at every other catalog `fsync_dir`: the
                // schema's own row is in the fsynced SAL, and the boot replay
                // re-creates whatever directory it names, so a lost directory
                // entry costs a re-`mkdir` and never a row.
                let _ = fsync_dir(&path);
                let _ = fsync_dir(&self.base_dir);
            } else {
                // Queue for deletion on successful DROP SCHEMA and on CREATE SCHEMA
                // rollback when compensate_stage_a re-fires this hook with -1.
                // The path comes from this row's payload name and the queue is
                // drained by `remove_dir_all`. Only the master live-DDL path acts
                // on it (workers discard theirs), and there
                // `precheck_schema_family`'s CAS has proved the name is the
                // retracted schema's own.
                self.pending_dir_deletions.push(path);
            }
        }
        Ok(())
    }

    /// Build this process's store for a top-level relation: one `Table` under
    /// `w{rank}of{num_workers}`. Recovery is derived from `kind`, so a relation
    /// cannot be (e.g.) ephemeral but SAL-replayed. Only user relations are built
    /// here — system catalog tables are plain single `Table`s built at bootstrap.
    ///
    /// The child is homed at THIS process's own worker rank, so a live CREATE on
    /// each worker post-fork builds a distinct dir directly. The post-fork master
    /// owns no store at all: it registers the relation `Detached` so it and
    /// worker 0 do not both hold a live `Table` on `w0of{W}`.
    ///
    /// Both storeless cases are decided here rather than at the callers, so a new
    /// caller cannot build a `Table` for one of them.
    ///
    /// A fed view's **delta store** is opened here too, and returned with the
    /// relation's own: every path that opens a relation's store comes through
    /// here, so a feed cannot come back missing from a rehome or a rebuild — the
    /// same reason the capacity is stamped here. Its derived schema is built
    /// ahead of the storeless early returns, so the one limit that can refuse a
    /// feed is refused identically on every process.
    ///
    /// Crash-cleanup of the directory is the caller's: this takes `&self` and is
    /// pure construction.
    pub(crate) fn build_relation_store(
        &self,
        kind: RelationKind,
        directory: &str,
        id: i64,
        schema: SchemaDescriptor,
        budgets: crate::query::ViewBudgets,
    ) -> Result<RelationStores, String> {
        // Above every early return below, so it runs on **every** process: the
        // post-fork master opens no user store at all, and a limit first noticed
        // on a worker would be a fatal abort taken after the client was told the
        // CREATE succeeded.
        let delta = budgets
            .delta_bytes
            .map(|budget| {
                crate::schema::make_delta_schema(&schema)
                    .map(|delta_schema| (budget, delta_schema))
                    .ok_or_else(|| format!("view {id} has too many columns to carry a delta feed"))
            })
            .transpose()?;

        // Above `ensure_dir`: a storeless kind owns no store in any process, so no
        // directory is created for one.
        let recovery = match kind {
            RelationKind::Stream => {
                return Ok(RelationStores {
                    handle: StoreHandle::Detached,
                    delta: None,
                })
            }
            // A view's output store and its operator traces resume from the
            // manifest the ephemeral checkpoint round stamped, or are rebuilt.
            RelationKind::View => self.rederive_source(),
            RelationKind::SystemCatalog | RelationKind::BaseTable => RecoverySource::SalReplay,
        };
        ensure_dir(directory)?;
        if !self.owns_stores {
            return Ok(RelationStores {
                handle: StoreHandle::Detached,
                delta: None,
            });
        }

        // Widen the window where the table dir exists but its child subdir does
        // not, so a concurrent master remove_dir_all (DROP) deterministically
        // races this create. User tables only.
        if kind.is_base_table() {
            if let Some(ms) = TABLE_CREATE_DELAY.count() {
                std::thread::sleep(std::time::Duration::from_millis(ms));
            }
        }

        let child = ChildAddr::this_worker(self.num_workers);
        let mut table = Table::new(&child.dir(directory), schema, id as u32, recovery)
            .map_err(|e| format!("Failed to open relation {id}: error {e} (dir={directory})"))?;
        // Every store this worker opens for a relation comes through here, so a
        // bounded view cannot come back unbounded from a rehome or a rebuild.
        table.set_capacity(budgets.capacity_bytes);
        Ok(RelationStores {
            handle: StoreHandle::Owned(std::cell::UnsafeCell::new(Box::new(table))),
            delta: delta
                .map(|(budget, s)| Self::build_delta_store(directory, id, s, budget))
                .transpose()?,
        })
    }

    /// This worker's delta store for a fed view, under `delta_w{rank}` of the
    /// view's own directory.
    ///
    /// Erased at open — `RecoverySource::Rederive { resume_at: None }`, the "never
    /// resume" spelling the enum already carries, which unlinks the manifest and
    /// erases the shards. That is the intent, not an omission: no delta expresses
    /// what a boot does to a view over a stream (it returns to the value it would
    /// have if the stream had never received a row) or to an invalidated view
    /// (rebuilt from base), and a restart mints a fresh boot nonce, so every cursor
    /// a client holds stops matching and it re-reads at `after_tick = 0`.
    fn build_delta_store(
        directory: &str,
        id: i64,
        delta_schema: SchemaDescriptor,
        budget: u64,
    ) -> Result<Box<DeltaFeed>, String> {
        let child = ChildAddr::delta_for_this_worker();
        let mut table = Table::new(
            &child.dir(directory),
            delta_schema,
            id as u32,
            RecoverySource::Rederive { resume_at: None },
        )
        .map_err(|e| format!("Failed to open delta store of view {id}: error {e} (dir={directory})"))?;
        table.set_delta_budget(budget);
        Ok(Box::new(DeltaFeed {
            schema: delta_schema,
            handle: StoreHandle::Owned(std::cell::UnsafeCell::new(Box::new(table))),
        }))
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
            depth,
            budgets,
        } = reg;
        let col_defs = self.read_column_defs(id);
        validate_relation_defs(kind, id, &name, &col_defs, &pk)?;

        let schema_name = self.caches.schema_by_id.get(&schema_id).cloned().unwrap_or_default();
        let directory = relation_dir(&self.base_dir, &schema_name, kind, id);
        let schema = build_schema_from_col_defs(&col_defs, pk.as_slice(), placement)?;
        gnitz_debug!(
            "catalog: creating {} dir={} name={} id={} workers={}",
            kind.noun(),
            directory,
            name,
            id,
            self.num_workers
        );
        let handle = if self.ctx.is_live() {
            // A live CREATE is what creates the relation directory, so it is
            // staged for crash-cleanup: if the rest of Stage-A fails,
            // `compensate_stage_a`'s drain removes what was made here.
            let staged = directory.clone();
            self.with_staged_dir(staged, |s| {
                s.build_relation_store(kind, &directory, id, schema, budgets)
            })?
        } else {
            // A boot replay: the directory already holds the relation's rows, so
            // it is never staged — an error here leaves them for the next boot to
            // retry. The relayout is bound to this open rather than run as its own
            // pass because it needs the previous child set, which opening the
            // store would shadow and `reconcile_child_dirs` deletes right
            // afterwards. Views are exempt: a worker-count change invalidates
            // every one of them, so `rebuild_invalid_views` refills them from base
            // and relaying their state would be waste.
            if kind.is_base_table() {
                crate::storage::repartition_relation(&directory, &schema, id as u32, self.num_workers)?;
            }
            self.build_relation_store(kind, &directory, id, schema, budgets)?
        };
        // Only a live CREATE adds an entry to the schema dir, and only a kind that
        // owns a store creates one at all — a boot replay reopens a directory that
        // is already there, so there is nothing new to make durable. A storeless
        // kind's definition rides on the system tables like any other row.
        if self.ctx.is_live() && kind.owns_store() {
            let _ = fsync_dir(&schema_dir(&self.base_dir, &schema_name));
        }
        self.dag.register_table(
            id,
            crate::query::TableEntry::new(handle, schema, kind, depth, directory, budgets),
        );
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
        let Some(directory) = self.dag.tables.get(&id).map(|e| e.directory.clone()) else {
            return Ok(());
        };
        cascade(self)?;
        self.dag.unregister_table(id);
        self.pending_dir_deletions.push(directory);
        self.caches.purge_schema_version(id);
        Ok(())
    }

    /// The TABLE_TAB / VIEW_TAB register hook. One loop for both families: a
    /// row's side effect is gated on its NET live state (storage is applied
    /// before hooks fire), not its own sign, so a rename pair — net-live before
    /// and after — fires neither the `+1` registration nor the `-1` teardown, in
    /// any row order and on every application path. Only the per-row
    /// registration values and the teardown cascade differ by family.
    fn hook_relation_register(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        for i in self.relation_row_order(family, batch) {
            let weight = batch.get_weight(i);
            let id = batch.get_pk(i) as i64;
            let net_live = self.sys_pk_is_live(family, batch.get_pk_bytes(i));

            if weight > 0 {
                // System tables are pre-registered by `register_system_table_families`
                // before `replay_catalog` fires hooks, so their rows show up here
                // with the DAG already populated; a rename pair's `+1` likewise
                // finds the id already registered. Skip to avoid double-register.
                // A `+1` whose net folds to dead (a create cancelled within its
                // own bundle) registers nothing.
                if !net_live || self.dag.tables.contains_key(&id) {
                    continue;
                }
                let reg = match family {
                    SysFamily::Table => Self::table_registration(batch, i, id)?,
                    SysFamily::View => self.view_registration(batch, i, id)?,
                    // Named rather than folded into the view arm: decoding any
                    // other family's row against the VIEW_TAB layout would
                    // register a relation out of unrelated bytes.
                    other => return Err(format!("{} is not a relation family", other.name())),
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
    /// neither order the hook is handed is dependency order — boot replay walks
    /// VIEW_TAB in PK order, in which a chain's user-named view sorts *before*
    /// the hidden segments it scans (its id is minted before the body is bound).
    /// Without this a view is replicated before a restart and partitioned after.
    /// A table's registration reads no other relation, so TABLE_TAB keeps row
    /// order.
    fn relation_row_order(&mut self, family: SysFamily, batch: &Batch) -> Vec<usize> {
        if family != SysFamily::View {
            return (0..batch.count).collect();
        }
        let mut rows: Vec<usize> = (0..batch.count).filter(|&i| batch.get_weight(i) <= 0).collect();
        let mut live: Vec<usize> = (0..batch.count).filter(|&i| batch.get_weight(i) > 0).collect();
        if live.len() > 1 {
            let ids: Vec<i64> = live.iter().map(|&i| batch.get_pk(i) as i64).collect();
            let rank: FxHashMap<i64, usize> = self
                .dag
                .order_by_view_deps(&ids)
                .into_iter()
                .enumerate()
                .map(|(r, vid)| (vid, r))
                .collect();
            // Stable, so a malformed batch's duplicate `+1`s on one id keep row
            // order among themselves.
            live.sort_by_key(|&i| rank[&(batch.get_pk(i) as i64)]);
        }
        rows.append(&mut live);
        rows
    }

    /// The registration values for TABLE_TAB row `i`: placement folded out of
    /// `TABLE_TAB.flags`, at depth 0, never capacity-bounded.
    fn table_registration(batch: &Batch, i: usize, tid: i64) -> Result<RelationRegistration, String> {
        let (schema_id, name, pk, flags) = read_table_tab_row(batch, i);
        // The one value that answers where this table's rows live — the store
        // shape, the write scatter, the read routing, and the co-partition
        // analyzers all read it back off the schema. `new_with_placement` clamps
        // an out-of-range prefix, so a crafted flag cannot index out of bounds.
        let placement = Placement::from_table_flags(flags)
            .map_err(|e| format!("catalog invariant violated: table '{name}' (tid={tid}) is {e}."))?;
        Ok(RelationRegistration {
            kind: RelationKind::from_table_flags(flags),
            id: tid,
            schema_id,
            name,
            pk,
            placement,
            depth: 0,
            budgets: crate::query::ViewBudgets::default(),
        })
    }

    /// The registration values for VIEW_TAB row `i`: placement folded out of the
    /// view's sources' own stamped placements, at one past their deepest, plus
    /// the capacity clause and the rules on what a bounded view may read.
    ///
    /// The view's physical PK is the persisted leading-k column list: a single
    /// synthetic hash column for join/set-op/distinct views, or the source PK
    /// passed through (0..k) for a plain projection over a compound-PK table.
    /// `validate_relation_defs`' over-wide rejection is genuinely reachable from
    /// here — a compound-PK plain projection prepends the k source PK columns, so
    /// `SELECT *` over a wide compound-PK table can cross MAX_COLUMNS.
    fn view_registration(&mut self, batch: &Batch, i: usize, vid: i64) -> Result<RelationRegistration, String> {
        let (schema_id, name, pk, capacity, delta) = read_view_tab_row(batch, i);
        let budgets = crate::query::ViewBudgets {
            capacity_bytes: (capacity != 0).then_some(capacity),
            delta_bytes: (delta != 0).then_some(delta),
        };
        // Trust-boundary re-check, in the style of the placement rejection in
        // `table_registration`: a hidden chain segment is an internal relation
        // the planner mints, never something an option clause may name.
        if (budgets.capacity_bytes.is_some() || budgets.delta_bytes.is_some())
            && name.starts_with(gnitz_wire::HIDDEN_VIEW_PREFIX)
        {
            return Err(format!(
                "catalog invariant violated: hidden segment '{name}' (vid={vid}) carries a WITH option."
            ));
        }
        // The two budgets are refused together, here as well as at the SQL layer:
        // a bounded view's `Delta(0)` cannot be a function of the tick round, and
        // the whole feed contract is that it is. A read over a partly-dehydrated
        // view hydrates its missing keys from the *source relation's live store*,
        // which `handle_push` advances outside any tick, so the bootstrap would
        // report round `T` while already carrying a push the next poll delivers
        // again as round `T+1` — double weight, no error, no row-set difference.
        if budgets.capacity_bytes.is_some() && budgets.delta_bytes.is_some() {
            return Err(format!(
                "view '{name}' (vid={vid}) declares both `capacity` and `delta`; \
                 a capacity-bounded view cannot carry a delta feed"
            ));
        }
        // The circuit's `circuit_nodes` are persisted before this VIEW_TAB row,
        // so `get_source_ids` resolves here.
        let source_ids = self.dag.get_source_ids(vid);
        // What a view may not read. Both rules trace to skeleton rows being
        // recomputed from the *source* store: a bounded view's own store is
        // skeletonized, and a stream's holds nothing to recompute from.
        // `ScanDelta` is the only external-source opcode in the wire vocabulary,
        // so `source_ids` covers every source of every circuit, a hand-built
        // circuit-builder-API view included. Within-bundle hidden segments are
        // unresolvable here and never carry a capacity.
        for &src in &source_ids {
            let Some(e) = self.dag.tables.get(&src) else { continue };
            if e.capacity_bytes.is_some() {
                return Err(format!(
                    "view '{name}' (vid={vid}) reads relation {src}, which is a \
                     capacity-bounded view; views cannot be created over one"
                ));
            }
            if budgets.capacity_bytes.is_some() && e.kind == RelationKind::Stream {
                return Err(format!(
                    "view '{name}' (vid={vid}) reads relation {src}, which is a stream; \
                     a capacity-bounded view cannot be created over one"
                ));
            }
        }
        // Where this view's rows live, folded from its sources' own stamped
        // placements. Stamping the fold is what makes the property transitive —
        // `relation_row_order` registers this view after its sources, so a view
        // over it reads the answer right here — and reading one value is what
        // keeps the store shape, the read routing, and the co-partition analyzers
        // from disagreeing.
        let placement = self.dag.view_placement(vid, &source_ids, pk.as_slice().len());
        let depth = source_ids
            .iter()
            .filter_map(|id| self.dag.tables.get(id))
            .map(|e| e.depth + 1)
            .max()
            .unwrap_or(0);
        Ok(RelationRegistration {
            kind: RelationKind::View,
            id: vid,
            schema_id,
            name,
            pk,
            placement,
            depth,
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
    /// Under an atomic CREATE the COL_TAB and circuit rows are applied via the
    /// enqueuing path, so they are in compensation's drained set and negated
    /// directly. CREATE rollback replays descending topo, so the relation's `-1`
    /// fires this cascade BEFORE those drained `-1`s: an unguarded cascade would
    /// retract them, then the drained `-1`s would retract them again → a net `-1`
    /// ghost. Skipping them during rollback leaves compensation's direct negate
    /// as the sole retractor. The unguarded index cascade is the dual: FK
    /// auto-indices use `submit_local` — never enqueued — so this cascade is
    /// their only retractor and must run.
    fn cascade_relation_children(&mut self, family: SysFamily, id: i64) -> Result<(), String> {
        if family == SysFamily::Table {
            self.cascade_retract_indices(id)?;
        } else if !self.ctx.in_rollback() {
            self.cascade_retract_circuit(id)?;
        }
        if !self.ctx.in_rollback() {
            self.cascade_retract_columns(id)?;
        }
        Ok(())
    }

    fn cascade_retract_indices(&mut self, owner_id: i64) -> Result<(), String> {
        // Clone the idx_id list before any mutation so submit → apply_index_caches
        // can safely remove entries from indices_by_owner as each retraction fires.
        let idx_ids: Vec<i64> = match self.caches.indices_by_owner.get(&owner_id) {
            Some(ids) if !ids.is_empty() => ids.clone(),
            _ => return Ok(()),
        };
        let schema = SysFamily::Index.schema();
        for idx_id in idx_ids {
            let batch = retract_single_row(self.sys_store(SysFamily::Index), &schema, idx_id as u128);
            if batch.count > 0 {
                self.submit_cascade(SysFamily::Index, batch)?;
            }
        }
        Ok(())
    }

    fn cascade_retract_columns(&mut self, owner_id: i64) -> Result<(), String> {
        let schema = SysFamily::Column.schema();
        let (start_pk, end_pk) = column_id_band(owner_id);
        let start = sys_opk(&schema, start_pk as u128);
        let end = sys_opk(&schema, end_pk as u128);
        let batch = retract_key_range(
            self.sys_store(SysFamily::Column),
            &schema,
            start.pk_bytes(),
            end.pk_bytes(),
        );
        if batch.count > 0 {
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
        self.dag
            .tables
            .get(&owner_id)
            .is_some_and(|e| col_idx as usize == e.schema.num_columns())
    }

    /// Column-ALTER side effect: rebuild the owner's descriptor from the
    /// (freshly invalidated) column defs and publish it into the store in place.
    /// A `is_nullable 0→1` flip (DROP NOT NULL) moves the whole-schema payload
    /// comparator `FixedIntNonnull → Generic`, and a trailing append (ADD COLUMN)
    /// grows the region count; RENAME/DROP COLUMN reach the rebuild and no-op
    /// there, since neither changes a descriptor field.
    ///
    /// The trigger is derived from the batch shape, never from context: the hook
    /// fires on live apply, worker sync and boot replay alike with no flag to
    /// tell them apart. It acts only on a rewrite pair, or on a trailing append —
    /// an unpaired `+1` at its registered owner's current column count. Naming
    /// the append shape rather than merely "carries a `+1`" is what keeps
    /// `replay_catalog`'s boot full-scan, which hands over every live COL_TAB row
    /// at `+1`, from rebuilding every base table's descriptor.
    fn hook_column_alter(&mut self, batch: &Batch) -> Result<(), String> {
        // Distinct owner tids carrying a `-1`/`+1` pair or a trailing append
        // (COL_TAB PK = pack_col_id).
        let mut owners: Vec<i64> = Vec::new();
        for sig in pk_signatures(batch).iter() {
            let (owner, col_idx) = gnitz_wire::unpack_col_id(sig.pk as u64);
            let owner = owner as i64;
            let is_append = sig.pos.is_some() && sig.neg.is_none() && self.is_trailing_col_append(owner, col_idx);
            if !(sig.is_pair() || is_append) {
                continue;
            }
            if !owners.contains(&owner) {
                owners.push(owner);
            }
        }
        for owner in owners {
            let Some(entry) = self.dag.tables.get(&owner) else {
                continue;
            };
            if !entry.kind.is_base_table() {
                continue;
            }
            let cur = entry.schema;
            // Rebuild from the post-invalidate col defs. `cur.placement()` is
            // construction, not a workaround for `eq` ignoring it: without it the
            // rebuild is stamped KEYED_DEFAULT and loses CLUSTER BY / REPLICATED.
            // Only an is_nullable 0→1 flip or a trailing append changes the
            // descriptor; when it does, publish it into the store.
            let col_defs = self.read_column_defs(owner);
            let rebuilt = build_schema_from_col_defs(&col_defs, cur.pk_indices(), cur.placement())
                .map_err(|e| format!("column ALTER on table id={owner}: {e}"))?;
            if rebuilt != cur {
                self.dag.swap_table_schema(owner, rebuilt)?;
            }
        }
        Ok(())
    }

    fn cascade_retract_circuit(&mut self, vid: i64) -> Result<(), String> {
        for family in [
            SysFamily::CircuitNodes,
            SysFamily::CircuitEdges,
            SysFamily::CircuitNodeColumns,
        ] {
            let schema = family.schema();
            // These families use the compound PK `(view_id, sub)`, so one view's
            // rows are the key band `[(vid, 0), (vid + 1, 0))`. `sys_opk` takes
            // the native value in pk-list column order, so `view_id` (column 0)
            // occupies the LOW u128 half — the byte-order dual of the
            // `(vid << 64) | sub` image `Batch::extend_pk` writes.
            let start = sys_opk(&schema, vid as u64 as u128);
            let end = sys_opk(&schema, vid as u64 as u128 + 1);
            let batch = retract_key_range(self.sys_store(family), &schema, start.pk_bytes(), end.pk_bytes());
            if batch.count > 0 {
                self.submit(family, batch)?;
            }
        }
        Ok(())
    }

    /// The IDX_TAB register hook: a sign dispatch over the batch, mirroring
    /// `hook_relation_register`. The index name lives in the row's
    /// `IDXTAB_PAY_NAME` slot and `apply_index_caches` has already populated the
    /// name-indexed caches from it, so neither half below reads the string — it
    /// would just be a wasted allocation.
    fn hook_index_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            let idx_id = batch.get_pk(i) as i64;
            let (owner_id, cols, is_unique) = read_idx_tab_row(batch, i);
            if batch.get_weight(i) > 0 {
                self.register_index(idx_id, owner_id, &cols, is_unique)?;
            } else {
                self.unregister_index(owner_id, cols.as_slice());
            }
        }
        Ok(())
    }

    /// Build an index table for a `+1` IDX_TAB row, fill it, and enter its
    /// circuit — the `+1` half of [`Self::hook_index_register`].
    ///
    /// One circuit per column list (dedup by ordered list). An incumbent circuit
    /// means no second table is built, but a UNIQUE newcomer over a non-unique
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
        let entry = self.validate_index_registration(owner_id, cols)?;
        let owner_schema = entry.schema;
        let owner_dir = entry.directory.clone();

        if let Some(was_unique) = entry.index_circuit_on(cols.as_slice()).map(|ic| ic.is_unique) {
            if is_unique && !was_unique {
                self.promote_index_to_unique(owner_id, &owner_schema, cols.as_slice())?;
            }
            return Ok(());
        }

        // make_index_schema bounds-checks and promotes every column (defence in
        // depth at the catalog trust boundary; a crafted wire row could name an
        // out-of-range or ineligible column).
        let idx_schema = make_index_schema(cols.as_slice(), &owner_schema)?;
        let idx_dir = index_dir(&owner_dir, idx_id);
        let cols = *cols;

        // Staged before Table::new: if any step below fails, the stage removes
        // `idx_dir` recursively — child subdirs included — so a failed CREATE
        // INDEX leaves nothing on disk.
        self.with_staged_dir(idx_dir.clone(), |s| {
            let mut idx_table_box = Box::new(s.new_index_table(&idx_dir, idx_id, idx_schema)?);
            let idx_table_ptr = &mut *idx_table_box as *mut Table;
            // The master never populates its index copies (they stay permanently
            // empty; distributed HAS_PK/seek probes union the workers' slice-local
            // copies). Workers and standalone backfill from their local base slice
            // — unless the table just resumed from a checkpointed manifest, which
            // already holds those rows.
            if !s.ctx.in_rollback()
                && !crate::foundation::worker_ctx::is_master()
                && !idx_table_box.resumed_from_checkpoint()
            {
                s.backfill_index(
                    owner_id,
                    &owner_schema,
                    cols.as_slice(),
                    idx_table_ptr,
                    &idx_schema,
                    // Duplicates are only re-checked on a first apply; a replayed
                    // index's data passed the check when it was originally written.
                    is_unique && s.ctx.is_live(),
                )?;
            }
            s.dag
                .add_index_circuit(owner_id, cols.as_slice(), idx_id, idx_table_box, idx_schema, is_unique);
            Ok(())
        })
    }

    /// Demote or destroy `owner_id`'s circuit on `cols` after a `-1` IDX_TAB row
    /// — the `-1` half of [`Self::hook_index_register`]. The retraction is
    /// already applied to sys_indices when hooks fire, so its net weight is 0 and
    /// the scan below sees only what survives it.
    fn unregister_index(&mut self, owner_id: i64, cols: &[u32]) {
        let (mut has_any, mut remains_unique) = (false, false);
        self.for_each_index_on_cols(owner_id, cols, |_row_id, is_uniq| {
            has_any = true;
            remains_unique |= is_uniq;
        });
        if has_any {
            // Another index (e.g. the FK auto-index) still covers this column
            // list. Demote the circuit rather than destroying it.
            self.dag.set_index_circuit_uniqueness(owner_id, cols, remains_unique);
            return;
        }
        // No index remains on the column list — drop the circuit. The directory
        // path uses the *creating* index_id, not the dropped one: when a second
        // index promoted an incumbent circuit, the real directory on disk carries
        // the first registrant's id.
        let creating = self
            .dag
            .tables
            .get(&owner_id)
            .and_then(|e| e.index_circuit_on(cols).map(|ic| (e.directory.clone(), ic.index_id)));
        if let Some((owner_dir, creating_idx_id)) = creating {
            self.dag.remove_index_circuit(owner_id, cols);
            self.pending_dir_deletions.push(index_dir(&owner_dir, creating_idx_id));
        }
    }

    fn hook_cascade_fk(&mut self, batch: &Batch) -> Result<(), String> {
        // During CREATE TABLE rollback the TABLE +1 compensation re-registers the
        // table; running hook_cascade_fk here would allocate new index IDs for FK
        // indices that conflict with the IDX +1 rows the topological replay
        // restores a moment later.
        if self.ctx.in_rollback() {
            return Ok(());
        }
        // A rename pair's `+1` must NOT re-mint this table's FK
        // auto-indices — the auto-index name embeds the *current* (new) table
        // name, so its by-name dedup cannot suppress the duplicate a rename
        // would produce for a non-PK FK column. `family_pks_by_sign` excludes
        // exactly those paired tids. It reads the batch alone rather than a
        // master-threaded set, because this hook also fires on worker `ddl_sync`
        // and master SAL-tail recovery, where such a set would be absent.
        for tid in family_pks_by_sign(batch, true) {
            // Live-only: the boot shard replay restores the persisted IDX_TAB
            // rows itself, and it replays TABLE_TAB first — so `index_by_name` is
            // still empty here and an ungated run would mint duplicate FK indices
            // under fresh ids alongside them.
            if self.ctx.is_live() {
                self.create_fk_indices(tid)?;
            }
        }
        Ok(())
    }
}
