//! Opening a relation's stores and entering it in the registry.

use super::dirs::remove_children;
use super::*;
use gnitz_foundation::fault::Seam;

/// `GNITZ_INJECT_TABLE_CREATE_DELAY_MS`: stall a user table's create between its
/// directory and its child subdir, so a concurrent DROP races it.
static TABLE_CREATE_DELAY: Seam = Seam::new("GNITZ_INJECT_TABLE_CREATE_DELAY_MS");

impl RelationRegistry {
    /// Enter a relation, its stores opened fresh.
    pub fn register(&mut self, spec: RelationSpec) -> Result<(), StoreError> {
        let stores = self.build_relation_store(spec, false)?;
        self.enter(spec, stores);
        Ok(())
    }

    /// Enter a view whose rows open from the manifest already in its directory;
    /// `Err`, entering nothing, otherwise.
    pub fn reopen_view(&mut self, spec: RelationSpec) -> Result<(), StoreError> {
        let stores = self.build_relation_store(spec, true)?;
        if !stores.0.held().resumed_from_checkpoint() {
            return Err(StoreError::rejected(format!(
                "view {} did not reopen from its manifest",
                spec.id
            )));
        }
        self.enter(spec, stores);
        Ok(())
    }

    fn enter(&mut self, spec: RelationSpec, (store, delta): (Store, Option<Box<Table>>)) {
        let RelationSpec { id, kind, .. } = spec;
        let prev = self.tables.insert(
            id,
            Relation {
                id,
                store,
                delta,
                indexes: Vec::new(),
                kind,
            },
        );
        debug_assert!(prev.is_none(), "relation {id} registered twice");
    }

    /// This process's stores for a relation: its rows, and a fed view's deltas. A
    /// view's rows resume from a checkpoint manifest iff `resume`.
    pub(super) fn build_relation_store(
        &self,
        spec: RelationSpec,
        resume: bool,
    ) -> Result<(Store, Option<Box<Table>>), StoreError> {
        let RelationSpec { id, kind, schema } = spec;
        let absent = || Ok((Store::Absent(Box::new(schema)), None));
        let (recovery, budgets, feed) = match kind {
            RelationKind::Stream => return absent(),
            // Its store is the relation directory itself, outside the per-slot child
            // layout that a worker-count change relays and reclaims.
            RelationKind::SystemCatalog => {
                let dir = relation_dir(&self.base_dir, id);
                let table = Table::new(&dir, schema, RecoverySource::SalReplay, self.store_budgets())
                    .map_err(|e| StoreError::storage(format!("open store '{dir}'"), e))?;
                return Ok((Store::Held(Box::new(table)), None));
            }
            // The master opens no user store, but it still creates the relation's
            // directory, so the directory exists by the time the DDL is acknowledged.
            _ if !self.residency.owns_stores() => {
                ensure_dir(&relation_dir(&self.base_dir, id))?;
                return absent();
            }
            RelationKind::BaseTable => {
                if let Some(ms) = TABLE_CREATE_DELAY.count() {
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                }
                (RecoverySource::SalReplay, self.store_budgets(), None)
            }
            RelationKind::View(p) => {
                if !resume {
                    // The traces a previous compile left here are rebuilt with the rows they
                    // integrate; one the next compile does not declare would never be erased.
                    let slot = self.slot;
                    remove_children(&relation_dir(&self.base_dir, id), |c| {
                        matches!(c.kind, ChildKind::Scratch(_)) && c.slot == slot
                    })
                    .map_err(|e| StoreError::storage(format!("remove the operator traces of view {id}"), e))?;
                }
                (
                    self.rederive_source(resume),
                    self.store_budgets().bounded(p.capacity_bytes()),
                    p.delta_bytes(),
                )
            }
        };
        let rows = self.open_child(id, ChildKind::Rows, schema, recovery, budgets)?;
        let delta = match feed {
            Some(budget) if schema.placement().counts_on(self.slot.rank) => {
                // Admitted by the catalog precheck; a host registering outside it gets the refusal here.
                let delta_schema = crate::schema::make_delta_schema(&schema).ok_or_else(|| {
                    StoreError::rejected(format!("view {id} has too many columns to carry a delta feed"))
                })?;
                // Erased at open: no delta expresses what a boot does to a view, so every cursor restarts.
                let table = self.open_child(
                    id,
                    ChildKind::Delta,
                    delta_schema,
                    self.rederive_source(false),
                    self.store_budgets().delta(budget),
                )?;
                Some(Box::new(table))
            }
            _ => None,
        };
        Ok((Store::Held(Box::new(rows)), delta))
    }

    /// This process's `kind` child store of relation `id`.
    pub(super) fn open_child(
        &self,
        id: i64,
        kind: ChildKind<'_>,
        schema: SchemaDescriptor,
        recovery: RecoverySource,
        budgets: StoreBudgets,
    ) -> Result<Table, StoreError> {
        let dir = ChildAddr { kind, slot: self.slot }.dir(&relation_dir(&self.base_dir, id));
        Table::new(&dir, schema, recovery, budgets).map_err(|e| StoreError::storage(format!("open store '{dir}'"), e))
    }
}
