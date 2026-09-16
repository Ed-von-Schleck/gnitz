use super::cache::RelationEntry;
use super::*;
use std::collections::hash_map::Entry;

impl CatalogEngine {
    // -- Hook processing ---------------------------------------------------
    //
    // Call order is dependency order. A relation's COL_TAB rows must be in storage
    // when its register hook runs: live DDL applies ascending `topo_priority`, and
    // boot replay opens every sys store first.
    pub(in crate::catalog) fn fire_hooks(
        &mut self,
        family: SysFamily,
        batch: &Batch,
        on: OnRegister,
    ) -> Result<(), String> {
        if family.allocates_ids() {
            for i in batch.live_rows() {
                let id = family.leading_id(batch.get_pk(i));
                self.next_id = self.next_id.max(id.saturating_add(1));
            }
        }
        match family {
            SysFamily::Schema => self.apply_schema_caches(batch),
            SysFamily::Table | SysFamily::View => {
                self.apply_entity_caches(batch);
                self.hook_relation_register(family, batch, on)?;
            }
            SysFamily::Column => self.hook_column_change(batch)?,
            SysFamily::Index => {
                self.apply_index_caches(batch);
                self.hook_index_register(batch)?;
            }
            // `_sequences` rows drive no cache.
            SysFamily::Sequence => {}
            // The dependency map is maintained from circuit-row deltas; the circuit itself is
            // read by `load_circuit` when a view compiles.
            SysFamily::CircuitNodes => self.dag.apply_circuit_delta(batch),
        }
        Ok(())
    }

    // -- Hook handlers ---------------------------------------------------------

    /// Build a relation's store and enter it in the registry — the create half of
    /// [`hook_relation_register`](Self::hook_relation_register), over the values
    /// its per-family builder decoded.
    fn register_relation(&mut self, reg: RelationRegistration<'_>, on: OnRegister) -> Result<(), String> {
        let RelationRegistration {
            kind,
            id,
            schema_id: _,
            name,
            pk,
            placement,
            props,
        } = reg;
        let col_defs = self.read_column_defs(id);
        let directory = relation_dir(&self.base_dir, kind, id);
        let schema = build_schema_from_col_defs(kind, &col_defs, pk.as_slice(), placement)
            .map_err(|e| format!("{} '{name}' (id={id}) {e}", kind.noun()))?;
        gnitz_debug!(
            "catalog: creating {} dir={} name={} id={} workers={}",
            kind.noun(),
            directory,
            name,
            id,
            self.registry.slot().of
        );
        // `register` owns the boot relayout, the staged-directory reclaim and the
        // parent fsync.
        self.registry
            .register(RelationSpec { id, kind, schema, directory, props }, on)?;
        self.enter_relation(id, kind, &schema, &col_defs);
        // Derived, not stored: every process builds the same FK circuits from the same
        // column records.
        for ci in self.fk_circuit_cols(id) {
            self.registry
                .add_index(id, ci as i64, &[ci as u32], false)
                .map_err(|e| format!("{} '{name}' (id={id}) FK index on column {ci}: {e}", kind.noun()))?;
        }
        Ok(())
    }

    /// Enter registered relation `id`'s [`RelationEntry`], and index the FK edges it
    /// declares by parent. Only a base table's column records declare one.
    pub(in crate::catalog) fn enter_relation(
        &mut self,
        id: i64,
        kind: RelationKind,
        schema: &SchemaDescriptor,
        defs: &[ColumnDef],
    ) {
        let fks: Vec<FkEdge> = defs
            .iter()
            .enumerate()
            .filter(|(_, cd)| kind.is_base_table() && cd.fk_table_id != 0)
            .map(|(ci, cd)| FkEdge {
                child_tid: id,
                fk_col: ci,
                parent_tid: cd.fk_table_id,
                parent_col: cd.fk_col_idx as usize,
            })
            .collect();
        for e in &fks {
            self.caches.fk_by_parent.entry(e.parent_tid).or_default().push(*e);
        }
        self.caches
            .relations
            .insert(id, RelationEntry::new(id, schema, defs, fks));
    }

    /// The FK columns of `id` that carry a derived index circuit: those outside its
    /// PK, whose region already stores them.
    fn fk_circuit_cols(&self, id: i64) -> Vec<usize> {
        let Some(schema) = self.registry.relation(id).map(Relation::schema) else {
            return Vec::new();
        };
        self.fk_constraints_of(id)
            .iter()
            .map(|e| e.fk_col)
            .filter(|&c| !schema.is_pk_col(c))
            .collect()
    }

    /// Tear relation `id` out of the registry. Its owned system rows are retracted by
    /// the rows that dropped it (see `submit`), never from here; its directory is
    /// left to the orphan sweep.
    fn unregister_relation(&mut self, id: i64) {
        if !self.registry.has_id(id) {
            return;
        }
        self.dag.unregister_table(&mut self.registry, id);
        self.caches.fk_by_parent.remove(&id);
        if let Some(entry) = self.caches.relations.remove(&id) {
            for e in entry.fks {
                if let Entry::Occupied(mut edges) = self.caches.fk_by_parent.entry(e.parent_tid) {
                    edges.get_mut().retain(|x| x.child_tid != id);
                    if edges.get().is_empty() {
                        edges.remove();
                    }
                }
            }
        }
    }

    /// The TABLE_TAB / VIEW_TAB register hook: a PK carrying one sign is a drop or a
    /// create; a rename pair carries both and is neither.
    fn hook_relation_register(&mut self, family: SysFamily, batch: &Batch, on: OnRegister) -> Result<(), String> {
        let mut creates: Vec<(i64, usize)> = Vec::new();
        for sig in pk_signatures(family, batch) {
            match (sig.neg, sig.pos) {
                (Some(_), None) => self.unregister_relation(sig.leading),
                (None, Some(row)) => creates.push((sig.leading, row)),
                _ => {}
            }
        }
        // Registering a view reads its sources' stamped placement, and ids ascend
        // along every scan edge, so id order registers a view after the views it
        // scans.
        creates.sort_unstable_by_key(|c| c.0);
        for (id, row) in creates {
            if self.registry.has_id(id) {
                continue;
            }
            let reg = if family == SysFamily::Table {
                read_table_tab_row(batch, row).map_err(|e| format!("{e} (tid={id})"))?
            } else {
                self.view_registration(batch, row, id)?
            };
            self.register_relation(reg, on)?;
            // A view registers empty; the backfill, checkpoint resume or boot rebuild
            // fills it.
        }
        Ok(())
    }

    /// The registration values for VIEW_TAB row `i`: placement folded out of the
    /// view's sources' own stamped placements, plus the `WITH` options.
    ///
    /// The view's physical PK is the persisted leading-k column list: a single
    /// synthetic hash column for join/set-op/distinct views, or the source PK
    /// passed through (0..k) for a plain projection over a compound-PK table.
    fn view_registration<'a>(
        &mut self,
        batch: &'a Batch,
        i: usize,
        vid: i64,
    ) -> Result<RelationRegistration<'a>, String> {
        let ViewRegistration {
            schema_id,
            name,
            pk,
            props,
            owner_view_id,
        } = read_view_tab_row(batch, i).map_err(|e| format!("{e} (vid={vid})"))?;
        // The circuit's `circuit_nodes` are persisted before this VIEW_TAB row,
        // so `get_source_ids` resolves here. Re-check for the paths that skip the
        // precheck (boot replay, worker `ddl_sync`).
        let source_ids = self.dag.get_source_ids(vid);
        self.validate_view_options(vid, name, props, owner_view_id, &source_ids)?;
        // Stamping the fold is what makes placement transitive:
        // `hook_relation_register` registers this view after its sources, so a view
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
            props,
        })
    }

    /// Apply a column ALTER (or its compensation) to every registered owner.
    fn hook_column_change(&mut self, batch: &Batch) -> Result<(), String> {
        let mut owners: Vec<i64> = (0..batch.len())
            .map(|i| SysFamily::Column.leading_id(batch.get_pk(i)))
            .collect();
        owners.sort_unstable();
        owners.dedup();
        for owner in owners {
            let Some((kind, cur)) = self.registry.relation(owner).map(|e| (e.kind(), e.schema())) else {
                continue;
            };
            let defs = self.read_column_defs(owner);
            let schema = if kind.is_base_table() {
                let rebuilt =
                    build_schema_from_col_defs(RelationKind::BaseTable, &defs, cur.pk_indices(), cur.placement())
                        .map_err(|e| format!("column ALTER on table id={owner}: {e}"))?;
                if rebuilt != cur {
                    let CatalogEngine { registry, dag, .. } = self;
                    dag.swap_schema(registry, owner, rebuilt)?;
                }
                rebuilt
            } else {
                cur
            };
            self.caches
                .relations
                .get_mut(&owner)
                .expect("every registered relation has an entry")
                .reschema(owner, &schema, &defs);
        }
        Ok(())
    }

    /// The IDX_TAB register hook: a sign dispatch over the batch.
    /// `apply_index_caches` has already populated the name-indexed caches from
    /// `IDXTAB_PAY_NAME`, so neither half below reads that string.
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
        // Boot replay and worker ddl_sync reach this hook without
        // `precheck_family`, so re-run the shared registration guards here (see
        // `validate_index_registration`). Resolve the owner entry once for
        // everything below.
        let entry = self.validate_index_registration(owner_id)?;

        if let Some(was_unique) = entry.index_on(cols.as_slice()).map(|ic| ic.is_unique()) {
            // No second store: the index schema does not depend on `is_unique`,
            // and the duplicate check is the master's pre-flight, already run.
            if is_unique && !was_unique {
                self.registry.set_index_unique(owner_id, cols.as_slice(), true);
            }
            return Ok(());
        }

        let idx_dir = self
            .registry
            .index_dir(owner_id, idx_id)
            .expect("the owner resolved above");
        let cols = *cols;

        // `add_index` bounds-checks every column: a crafted wire row could name an
        // out-of-range or ineligible one.
        staged_dir(&idx_dir, || {
            self.registry.add_index(owner_id, idx_id, cols.as_slice(), is_unique)?;
            let resumed = self
                .registry
                .relation(owner_id)
                .and_then(|r| r.index_on(cols.as_slice()))
                .is_some_and(SecondaryIndex::resumed);
            // The master never populates its index copies (they stay permanently
            // empty; distributed HAS_PK/seek probes union the workers' slice-local
            // copies). Workers and standalone backfill from their local base slice
            // — unless the store resumed from a checkpointed manifest, which
            // already holds those rows.
            if !self.is_master && !resumed {
                if let Err(e) = self.backfill_index(owner_id, cols.as_slice()) {
                    // The circuit was entered before the backfill so the
                    // projection could ingest through it; a failed CREATE INDEX
                    // leaves no circuit. `staged_dir` reclaims its directory.
                    self.registry.remove_index(owner_id, cols.as_slice());
                    return Err(e);
                }
            }
            Ok(())
        })
    }

    /// Demote or destroy `owner_id`'s circuit on `cols` after a `-1` IDX_TAB row. It
    /// survives while another index row or the owner's FK columns still cover `cols`.
    fn unregister_index(&mut self, owner_id: i64, cols: &[u32]) {
        let survivors = self.indices_on_cols(owner_id, cols);
        let fk_circuit = matches!(cols, [c] if self.fk_circuit_cols(owner_id).contains(&(*c as usize)));
        if survivors.is_empty() && !fk_circuit {
            self.registry.remove_index(owner_id, cols);
        } else {
            self.registry
                .set_index_unique(owner_id, cols, survivors.iter().any(|&(_, u)| u));
        }
    }
}
