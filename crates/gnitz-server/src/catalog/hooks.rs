use gnitz_store::relation::{IndexClaim, RelationSpec};
use gnitz_wire::{IdxTabRow, TableDistribution, ViewParts};
use gnitz_zset::algebra::Placement;
use gnitz_zset::repr::Batch;
use std::rc::Rc;

use super::cache::RelationEntry;
use super::constraints::FkEdge;
use super::sys_reads::IdSet;
use super::sys_tables::{
    build_schema_from_col_defs, encode_record, pk_signatures, read_rel_row, PkShape, RelDetail, RelRow, SysFamily,
};
use super::CatalogEngine;

impl CatalogEngine {
    // -- Hook processing ---------------------------------------------------

    /// Run `family`'s register hook over `batch`, and a relation family's name
    /// index. A registration reads the relation's COL_TAB rows, and a view's its
    /// CIRCUIT_TAB row, from the store, so the caller applies those families first.
    pub(in crate::catalog) fn fire_hooks(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        let hooked = match family {
            // A schema's name is read from its row, and a schema is never renamed;
            // `sequences` rows drive no cache; a view's registration reads its
            // `circuits` row.
            SysFamily::Schema | SysFamily::Sequence | SysFamily::Circuit => return Ok(()),
            SysFamily::Table | SysFamily::View => {
                self.caches.apply_relation_names(batch);
                self.hook_relation_register(family, batch)
            }
            SysFamily::Column => self.hook_column_change(batch),
            SysFamily::Index => self.hook_index_register(batch),
        };
        // Every family that reaches here can change what a RESOLVE answers.
        for entry in self.caches.relations.values_mut() {
            *entry.token.get_mut() = 0;
        }
        hooked
    }

    // -- Hook handlers ---------------------------------------------------------

    /// Build a relation's store and enter it in the registry — the create half of
    /// [`hook_relation_register`](Self::hook_relation_register). Messages are bare
    /// predicates; the caller names the relation.
    fn register_relation(&mut self, rel: &RelRow<'_>) -> Result<(), String> {
        let col_defs = self.read_column_defs(rel.id)?;
        let schema = build_schema_from_col_defs(rel.kind(), &col_defs, rel.pk.as_slice())?;
        let (placement, pk_repeats, compress, serial) = match rel.detail {
            RelDetail::View(ViewParts { owner, pk_repeats, props }) => {
                let circuit = self.read_circuit(rel.id)?;
                let chain = owner.unwrap_or(rel.id);
                for src in circuit.sources() {
                    if SysFamily::from_id(src).is_some_and(SysFamily::master_only) {
                        return Err(format!(
                            "reads '{}', which holds no set a view can scan",
                            self.qualified_name(src)
                        ));
                    }
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
                    let holder = self.dag.chain_of(src);
                    if holder != src && (holder != chain || rel.kind().is_bounded()) {
                        return Err(format!(
                            "reads {src}, a segment of view {holder} that holds no rows for it"
                        ));
                    }
                }
                let placement = self.dag.register_view(
                    &self.registry,
                    rel.id,
                    &schema,
                    rel.kind().is_bounded(),
                    chain,
                    &circuit,
                )?;
                (placement, pk_repeats, props.compress, false)
            }
            RelDetail::Table(t) => {
                let placement = match t.distribution {
                    TableDistribution::Replicated => Placement::Replicated,
                    TableDistribution::Keyed { prefix_len: 0 } => Placement::full_pk(&schema),
                    TableDistribution::Keyed { prefix_len } => Placement::keyed(&schema, prefix_len as usize),
                };
                // A base table's PK is unique by `enforce_unique_pk`; a stream's is not.
                (placement, t.stream, t.compress, t.serial)
            }
        };
        gnitz_debug!(
            "catalog: creating {} name={} id={} workers={}",
            rel.kind().noun(),
            rel.name,
            rel.id,
            self.registry.slot().of
        );
        let fks: Vec<FkEdge> = col_defs
            .iter()
            .enumerate()
            .filter_map(|(ci, cd)| {
                cd.fk.map(|fk| FkEdge {
                    child_tid: rel.id,
                    fk_col: ci,
                    parent_tid: fk.table_id,
                    parent_col: fk.col as usize,
                    on_delete: fk.on_delete,
                })
            })
            .collect();
        let record = Rc::from(encode_record(schema.pk_cols(), &col_defs));
        self.registry.register(RelationSpec {
            id: rel.id,
            kind: rel.kind(),
            schema,
            placement,
            pk_repeats,
            compress,
        })?;
        if rel.kind().is_view() {
            self.registry.declare_traces(rel.id, self.dag.state_layout(rel.id))?;
        }
        self.caches.enter(rel.id, RelationEntry::new(record, fks, serial));
        // Derived, not stored: every process builds the same FK indexes from the same
        // column records. Every FK column carries one, a PK column included.
        for e in &self.caches.relations[&rel.id].fks {
            self.registry
                .add_index(rel.id, IndexClaim::ForeignKey, e.child_index_cols())
                .map_err(|err| format!("FK index on column {}: {err}", e.fk_col))?;
        }
        Ok(())
    }

    /// Tear relation `id` out of the registry and the DAG. Its owned system rows are
    /// retracted by the rows that dropped it (see `submit`), never from here; its
    /// directory is left to the orphan sweep.
    fn unregister_relation(&mut self, id: u64) {
        self.dag.forget(id);
        self.registry.unregister(id);
        self.caches.remove(id);
    }

    /// The TABLE_TAB / VIEW_TAB register hook: a PK carrying one sign is a drop or a
    /// create; a rename pair carries both and is neither.
    fn hook_relation_register(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        let mut creates: Vec<(u64, usize)> = Vec::new();
        for sig in pk_signatures(batch) {
            match sig.shape() {
                PkShape::Drop => self.unregister_relation(sig.leading),
                PkShape::Create => creates.push((sig.leading, sig.row)),
                PkShape::Pair => {}
            }
        }
        // Registering a view reads its sources' placements, and ids ascend
        // along every scan edge, so id order registers a view after the views it
        // scans.
        creates.sort_unstable_by_key(|c| c.0);
        for (id, row) in creates {
            // A system family's own TABLE_TAB row names a relation registered at open.
            if SysFamily::from_id(id).is_some() {
                continue;
            }
            let rel = read_rel_row(family, batch, row)?;
            // A view registers empty; the backfill, checkpoint resume or boot rebuild
            // fills it.
            self.register_relation(&rel).map_err(|e| format!("{rel} {e}"))?;
        }
        Ok(())
    }

    /// Apply a column ALTER (or its undo) to every registered owner.
    fn hook_column_change(&mut self, batch: &Batch) -> Result<(), String> {
        let owners = IdSet::new((0..batch.len()).map(|i| gnitz_wire::sys_leading_id(batch.get_pk_bytes(i))));
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
                .record = Rc::from(encode_record(cur.pk_cols(), &defs));
        }
        Ok(())
    }

    /// The IDX_TAB register hook: a `+1` adds the index's claim on its column list,
    /// a `-1` releases it.
    fn hook_index_register(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.len() {
            let idx_id = gnitz_wire::sys_leading_id(batch.get_pk_bytes(i));
            let r = IdxTabRow::read(batch, i).map_err(|e| format!("index {idx_id}: {e}"))?;
            let claim = IndexClaim::Index { id: idx_id, unique: r.is_unique };
            if batch.get_weight(i) > 0 {
                self.registry.add_index(r.owner_id, claim, r.source_col_idx)?;
            } else {
                self.registry.release_index(r.owner_id, claim);
            }
        }
        Ok(())
    }
}
