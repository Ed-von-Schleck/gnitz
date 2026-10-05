use super::cache::{encode_record, RelationEntry};
use super::*;
use gnitz_expr::ColumnTable;
use gnitz_wire::TableDistribution;
use std::collections::hash_map::Entry;

impl CatalogEngine {
    // -- Hook processing ---------------------------------------------------

    /// Run `family`'s name index and its register hook over `batch`. A registration
    /// reads the relation's COL_TAB rows, and a view's its CIRCUIT_TAB row, from the
    /// store, so the caller applies those families first.
    pub(in crate::catalog) fn fire_hooks(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        self.raise_next_id(family, batch);
        let hooked = match family {
            SysFamily::Schema => {
                self.apply_schema_names(batch);
                Ok(())
            }
            SysFamily::Table | SysFamily::View => {
                self.apply_relation_names(batch);
                self.hook_relation_register(family, batch)
            }
            SysFamily::Column => self.hook_column_change(batch),
            SysFamily::Index => self.hook_index_register(batch),
            // `_sequences` rows drive no cache; a view's registration reads its `_circuits` row.
            SysFamily::Sequence | SysFamily::Circuit => return Ok(()),
        };
        // Every family that reaches here can change what a RESOLVE answers.
        self.caches.resolve_tokens.get_mut().clear();
        hooked
    }

    // -- Hook handlers ---------------------------------------------------------

    /// Build a relation's store and enter it in the registry — the create half of
    /// [`hook_relation_register`](Self::hook_relation_register). Messages are bare
    /// predicates; the caller names the relation.
    fn register_relation(&mut self, rel: &RelRow<'_>) -> Result<(), String> {
        let col_defs = self.read_column_defs(rel.id)?;
        let schema = build_schema_from_col_defs(rel.kind, &col_defs, rel.pk.as_slice())?;
        let placement = match rel.detail {
            RelDetail::View { owner_view_id, .. } => {
                let placement = self.dag.register_view(&self.registry, rel.id, &schema, owner_view_id)?;
                for &src in self.dag.sources_of(rel.id) {
                    // A capacity-bounded view is a leaf: its store holds skeleton rows, so
                    // nothing may scan it.
                    if self.registry.relation(src).is_some_and(|r| r.kind().is_bounded()) {
                        return Err(format!(
                            "reads '{}', which is a capacity-bounded view; views cannot be created over one",
                            self.qualified_name(src)
                        ));
                    }
                    // A segment keeps its rows only for the build of its chain, so only
                    // that chain may scan it — and a capacity-bounded view, which
                    // recomputes a key from the rows of what it scans, not at all.
                    let chain = self.dag.chain_of(src);
                    if chain != src && (chain != self.dag.chain_of(rel.id) || rel.kind.is_bounded()) {
                        return Err(format!(
                            "reads {src}, a segment of view {chain} that holds no rows for it"
                        ));
                    }
                }
                placement
            }
            RelDetail::Table {
                distribution: TableDistribution::Replicated,
                ..
            } => Placement::Replicated,
            RelDetail::Table {
                distribution: TableDistribution::Keyed { prefix_len: 0 },
                ..
            } => Placement::full_pk(&schema),
            RelDetail::Table {
                distribution: TableDistribution::Keyed { prefix_len },
                ..
            } => Placement::keyed(&schema, prefix_len as usize),
        };
        gnitz_debug!(
            "catalog: creating {} name={} id={} workers={}",
            rel.kind.noun(),
            rel.name,
            rel.id,
            self.registry.slot().of
        );
        self.registry.register(RelationSpec {
            id: rel.id,
            kind: rel.kind,
            schema,
            placement,
            pk_repeats: rel.pk_repeats(),
        })?;
        self.enter_relation(rel.id, schema.pk_cols(), &col_defs, rel.serial());
        // Derived, not stored: every process builds the same FK indexes from the same
        // column records. Every FK column carries one, a PK column included — a
        // parent's RESTRICT probe reads the child through it.
        for (ci, _) in col_defs.iter().enumerate().filter(|(_, cd)| cd.fk.is_some()) {
            self.registry
                .add_index(
                    rel.id,
                    IndexClaim::ForeignKey,
                    gnitz_wire::PkColList::from_slice(&[ci as u32]),
                )
                .map_err(|e| format!("FK index on column {ci}: {e}"))?;
        }
        Ok(())
    }

    /// Enter registered relation `id`'s [`RelationEntry`], and index the FK edges it
    /// declares by parent.
    pub(in crate::catalog) fn enter_relation(&mut self, id: u64, pk: &[u32], defs: &[CatalogColumn], serial: bool) {
        let fks: Vec<FkEdge> = defs
            .iter()
            .enumerate()
            .filter_map(|(ci, cd)| {
                cd.fk.map(|fk| FkEdge {
                    child_tid: id,
                    fk_col: ci,
                    parent_tid: fk.table_id,
                    parent_col: fk.col as usize,
                })
            })
            .collect();
        for e in &fks {
            self.caches.fk_by_parent.entry(e.parent_tid).or_default().push(*e);
        }
        let record = encode_record(pk, defs);
        self.caches.relations.insert(id, RelationEntry { record, fks, serial });
    }

    /// Tear relation `id` out of the registry and the DAG. Its owned system rows are
    /// retracted by the rows that dropped it (see `submit`), never from here; its
    /// directory is left to the orphan sweep.
    fn unregister_relation(&mut self, id: u64) {
        self.dag.forget(id);
        self.registry.unregister(id);
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
        let mut creates: Vec<(u64, usize)> = Vec::new();
        for sig in pk_signatures(family, batch) {
            match (sig.neg, sig.pos) {
                (Some(_), None) => self.unregister_relation(sig.leading),
                (None, Some(row)) => creates.push((sig.leading, row)),
                _ => {}
            }
        }
        // Registering a view reads its sources' placements, and ids ascend
        // along every scan edge, so id order registers a view after the views it
        // scans.
        creates.sort_unstable_by_key(|c| c.0);
        for (id, row) in creates {
            // A system family's own TABLE_TAB row names a relation registered at open.
            if self.registry.has_id(id) {
                continue;
            }
            let rel = read_rel_row(family, batch, row)?;
            // A view registers empty; the backfill, checkpoint resume or boot rebuild
            // fills it.
            self.register_relation(&rel).map_err(|e| format!("{rel} {e}"))?;
        }
        Ok(())
    }

    /// Apply a column ALTER (or its compensation) to every registered owner.
    fn hook_column_change(&mut self, batch: &Batch) -> Result<(), String> {
        let owners = IdSet::new((0..batch.len()).map(|i| SysFamily::Column.leading_id(batch.get_pk(i))));
        for &owner in owners.ids() {
            let Some((kind, cur)) = self.registry.relation(owner).map(|e| (e.kind(), e.schema())) else {
                continue;
            };
            let defs = self.read_column_defs(owner)?;
            let rebuilt = build_schema_from_col_defs(kind, &defs, cur.pk_cols())
                .map_err(|e| format!("column ALTER on table id={owner}: {e}"))?;
            if rebuilt != cur {
                self.registry.swap_schema(owner, rebuilt)?;
            }
            self.caches
                .relations
                .get_mut(&owner)
                .expect("every registered relation has an entry")
                .record = encode_record(cur.pk_cols(), &defs);
        }
        Ok(())
    }

    /// The IDX_TAB register hook: a `+1` adds the index's claim on its column list,
    /// a `-1` releases it.
    fn hook_index_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.len() {
            let idx_id = batch.get_pk(i) as u64;
            let (owner_id, cols, unique) = read_idx_tab_row(batch, i).map_err(|e| format!("index {idx_id}: {e}"))?;
            if batch.get_weight(i) > 0 {
                self.registry
                    .add_index(owner_id, IndexClaim::Index { id: idx_id, unique }, cols)?;
            } else {
                self.registry.release_index(owner_id, idx_id);
            }
        }
        Ok(())
    }
}
