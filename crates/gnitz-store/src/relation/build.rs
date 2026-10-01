//! Opening a relation's stores and entering it in the registry.

use super::dirs::remove_children;
use super::*;
use gnitz_foundation::fault::Seam;

/// `GNITZ_INJECT_TABLE_CREATE_DELAY_MS`: stall a user table's create between its
/// directory and its child subdir, so a concurrent DROP races it.
static TABLE_CREATE_DELAY: Seam = Seam::new("GNITZ_INJECT_TABLE_CREATE_DELAY_MS");

impl RelationRegistry {
    /// Enter a relation, its stores opened fresh.
    pub fn register(&mut self, spec: RelationSpec) -> Result<(), String> {
        let stores = self.build_relation_store(spec, false)?;
        self.enter(spec, stores);
        Ok(())
    }

    /// Enter a view whose rows open from the manifest already in its directory;
    /// `Err`, entering nothing, otherwise.
    pub fn reopen_view(&mut self, spec: RelationSpec) -> Result<(), String> {
        let stores = self.build_relation_store(spec, true)?;
        self.enter(spec, stores);
        Ok(())
    }

    fn enter(&mut self, spec: RelationSpec, (store, delta): (Store, Option<Box<Table>>)) {
        let RelationSpec { id, kind, placement, .. } = spec;
        let prev = self.tables.insert(
            id,
            Relation {
                id,
                store,
                delta,
                indexes: Vec::new(),
                kind,
                placement,
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
    ) -> Result<(Store, Option<Box<Table>>), String> {
        let RelationSpec { id, kind, schema, placement } = spec;
        let absent = || Ok((Store::Absent(Box::new(schema)), None));
        let (recovery, budgets, feed) = match kind {
            RelationKind::Stream => return absent(),
            // Its store is the relation directory itself, outside the per-slot child
            // layout that a worker-count change relays and reclaims.
            RelationKind::SystemCatalog => {
                let dir = relation_dir(&self.base_dir, id);
                let table = Table::new(&dir, schema, RecoverySource::SalReplay, self.store_budgets())
                    .map_err(|e| format!("open store '{dir}': {e}"))?;
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
                    .map_err(|e| format!("remove the operator traces of view {id}: {e}"))?;
                }
                (
                    self.rederive_source(resume),
                    self.store_budgets().bounded(p.capacity_bytes()),
                    p.delta_bytes(),
                )
            }
        };
        let rows = self.open_child_as(id, ChildKind::Rows, schema, recovery, budgets)?;
        let delta = match feed {
            Some(budget) if placement.counts_on(self.slot.rank) => {
                // Admitted by the catalog precheck; a host registering outside it gets the refusal here.
                let delta_schema = super::delta::make_delta_schema(&schema)
                    .ok_or_else(|| format!("view {id} has too many columns to carry a delta feed"))?;
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

    /// [`Self::open_child`], `Err` unless the store opened under `recovery` itself —
    /// a resume that found no manifest at its generation.
    pub(super) fn open_child_as(
        &self,
        id: u64,
        kind: ChildKind<'_>,
        schema: SchemaDescriptor,
        recovery: RecoverySource,
        budgets: StoreBudgets,
    ) -> Result<Table, String> {
        let table = self.open_child(id, kind, schema, recovery, budgets)?;
        if table.recovery_source() != recovery {
            return Err(format!(
                "relation {id}: {kind:?} store did not resume from its manifest"
            ));
        }
        Ok(table)
    }

    /// This process's `kind` child store of relation `id`.
    pub(super) fn open_child(
        &self,
        id: u64,
        kind: ChildKind<'_>,
        schema: SchemaDescriptor,
        recovery: RecoverySource,
        budgets: StoreBudgets,
    ) -> Result<Table, String> {
        let dir = self.child_dir(id, kind);
        Table::new(&dir, schema, recovery, budgets).map_err(|e| format!("open store '{dir}': {e}"))
    }
}
