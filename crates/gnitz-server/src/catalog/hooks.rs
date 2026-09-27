use super::cache::RelationEntry;
use super::*;
use gnitz_expr::SchemaFacts;
use std::collections::hash_map::Entry;

impl CatalogEngine {
    // -- Hook processing ---------------------------------------------------
    //
    // Call order is dependency order. A relation's COL_TAB rows must be in storage
    // when its register hook runs: live DDL applies ascending `topo_priority`, and
    // boot replay opens every sys store first.
    pub(in crate::catalog) fn fire_hooks(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        self.raise_next_id(family, batch, batch.live_rows());
        match family {
            SysFamily::Schema => self.apply_schema_caches(batch),
            SysFamily::Table | SysFamily::View => {
                self.apply_entity_caches(batch);
                self.hook_relation_register(family, batch)?;
            }
            SysFamily::Column => self.hook_column_change(batch)?,
            SysFamily::Index => {
                self.apply_index_caches(batch);
                self.hook_index_register(batch)?;
            }
            // `_sequences` rows drive no cache.
            SysFamily::Sequence => {}
            SysFamily::CircuitNodes => self.dag.apply_circuit_delta(batch),
        }
        Ok(())
    }

    // -- Hook handlers ---------------------------------------------------------

    /// Build a relation's store and enter it in the registry — the create half of
    /// [`hook_relation_register`](Self::hook_relation_register), over the values
    /// its per-family builder decoded.
    fn register_relation(&mut self, reg: RelationRegistration<'_>) -> Result<(), String> {
        let RelationRegistration {
            kind,
            id,
            schema_id: _,
            name,
            pk,
            placement,
            facts,
        } = reg;
        let col_defs = self.read_column_defs(id)?;
        let schema = build_schema_from_col_defs(kind, &col_defs, pk.as_slice(), placement)
            .map_err(|e| format!("{} '{name}' (id={id}) {e}", kind.noun()))?;
        gnitz_debug!(
            "catalog: creating {} name={} id={} workers={}",
            kind.noun(),
            name,
            id,
            self.registry.slot().of
        );
        self.registry.register(RelationSpec { id, kind, schema })?;
        self.enter_relation(id, kind, schema.pk_indices(), &col_defs, facts);
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
        pk: &[u32],
        defs: &[ColumnDef],
        facts: RelFacts,
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
            .insert(id, RelationEntry::new(pk, defs, fks, facts));
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
        // The DAG can hold a view whose store failed to register.
        self.dag.forget(id);
        if !self.registry.has_id(id) {
            return;
        }
        self.registry.unregister(id);
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
    fn hook_relation_register(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
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
            self.register_relation(reg)?;
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
            pk_repeats,
        } = read_view_tab_row(batch, i).map_err(|e| format!("{e} (vid={vid})"))?;
        // Re-checked for the paths that skip the precheck: boot replay, a
        // worker's `ddl_sync`.
        self.validate_view_options(vid, name, props, owner_view_id)?;
        let placement = self
            .dag
            .register_view(&self.registry, vid, pk.as_slice().len())
            .map_err(|e| format!("{e} (vid={vid})"))?;
        Ok(RelationRegistration {
            kind: RelationKind::View(props),
            id: vid,
            schema_id,
            name,
            pk,
            placement,
            facts: RelFacts { pk_repeats, serial: false },
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
            let defs = self.read_column_defs(owner)?;
            if kind.is_base_table() {
                let rebuilt =
                    build_schema_from_col_defs(RelationKind::BaseTable, &defs, cur.pk_indices(), cur.placement())
                        .map_err(|e| format!("column ALTER on table id={owner}: {e}"))?;
                if rebuilt != cur {
                    self.reject_if_dependent_views(owner, "column ALTER")?;
                    self.registry.swap_schema(owner, rebuilt)?;
                }
            }
            self.caches
                .relations
                .get_mut(&owner)
                .expect("every registered relation has an entry")
                .reschema(cur.pk_indices(), &defs);
        }
        Ok(())
    }

    /// The IDX_TAB register hook: a sign dispatch over the batch.
    /// `apply_index_caches` has already populated the name-indexed caches from
    /// `IDXTAB_PAY_NAME`, so neither half below reads that string.
    fn hook_index_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.len() {
            let idx_id = batch.get_pk(i) as i64;
            let (owner_id, cols, props) = read_idx_tab_row(batch, i).map_err(|e| format!("index {idx_id}: {e}"))?;
            if batch.get_weight(i) > 0 {
                self.registry
                    .add_index(owner_id, idx_id, cols.as_slice(), props.is_unique)?;
            } else {
                self.registry.release_index(owner_id, idx_id);
            }
        }
        Ok(())
    }
}
