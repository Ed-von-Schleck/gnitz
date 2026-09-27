//! Opening a relation's stores and entering it in the registry.

use super::*;
use crate::storage::ChildKind;
use gnitz_foundation::fault::Seam;

/// `GNITZ_INJECT_TABLE_CREATE_DELAY_MS`: stall a user table's create between its
/// directory and its child subdir, so a concurrent DROP races it.
static TABLE_CREATE_DELAY: Seam = Seam::new("GNITZ_INJECT_TABLE_CREATE_DELAY_MS");

impl RelationRegistry {
    /// Enter a relation and open its stores.
    pub fn register(&mut self, spec: RelationSpec) -> Result<(), StoreError> {
        self.enter(spec, false)
    }

    /// Enter a view whose rows open from the manifest already in its directory;
    /// `Err`, entering nothing, otherwise.
    pub fn reopen_view(&mut self, spec: RelationSpec) -> Result<(), StoreError> {
        let id = spec.id;
        self.enter(spec, true)?;
        if self.relation(id).is_some_and(Relation::resumed) {
            return Ok(());
        }
        self.unregister(id);
        Err(StoreError::rejected(format!(
            "view {id} did not reopen from its manifest"
        )))
    }

    fn enter(&mut self, spec: RelationSpec, from_manifest: bool) -> Result<(), StoreError> {
        let RelationSpec { id, kind, schema } = spec;
        let (store, delta) = self.build_relation_store(kind, id, schema, from_manifest)?;
        let relation = Relation {
            id,
            store,
            delta,
            indexes: Vec::new(),
            kind,
        };
        self.tables.insert(id, relation);
        Ok(())
    }

    /// This process's stores for a relation: its rows, and a fed view's deltas.
    pub(crate) fn build_relation_store(
        &self,
        kind: RelationKind,
        id: i64,
        schema: SchemaDescriptor,
        from_manifest: bool,
    ) -> Result<(Store, Option<Box<Table>>), StoreError> {
        let props = match kind {
            RelationKind::View(p) => p,
            _ => ViewProps::Plain,
        };
        // On every process, the master included: a limit first noticed on a
        // worker would abort after the CREATE was acknowledged.
        let delta = props
            .delta_bytes()
            .map(|budget| {
                crate::schema::make_delta_schema(&schema)
                    .map(|delta_schema| (budget, delta_schema))
                    .ok_or_else(|| {
                        StoreError::rejected(format!("view {id} has too many columns to carry a delta feed"))
                    })
            })
            .transpose()?;

        let recovery = match kind {
            RelationKind::Stream => return Ok((Store::Absent(Box::new(schema)), None)),
            // A view's output store and its operator traces resume from the
            // manifest the ephemeral checkpoint round stamped, or are rebuilt.
            RelationKind::View(_) if from_manifest => self.rederive_source(true),
            RelationKind::View(_) if self.non_resumable.contains(&id) => self.rederive_source(false),
            RelationKind::View(_) => self.rederive_source(self.resume_enabled),
            RelationKind::SystemCatalog | RelationKind::BaseTable => RecoverySource::SalReplay,
        };
        let directory = &relation_dir(&self.base_dir, id);
        ensure_dir(directory)?;
        if kind != RelationKind::SystemCatalog && !self.residency.owns_stores() {
            return Ok((Store::Absent(Box::new(schema)), None));
        }

        // Widen the window where the table dir exists but its child subdir does
        // not, so a DROP of the table deterministically races this create. User
        // tables only.
        if kind.is_base_table() {
            if let Some(ms) = TABLE_CREATE_DELAY.count() {
                std::thread::sleep(std::time::Duration::from_millis(ms));
            }
        }

        let child_dir = match kind {
            RelationKind::SystemCatalog => directory.to_string(),
            _ => ChildAddr { kind: ChildKind::Rows, slot: self.slot }.dir(directory),
        };
        let table = Table::new(
            &child_dir,
            schema,
            recovery,
            self.store_budgets().bounded(props.capacity_bytes()),
        )
        .map_err(|e| StoreError::storage(format!("open relation {id} (dir={directory})"), e))?;
        let serves_feed = schema.placement().counts_on(self.slot.rank);
        let delta = delta
            .filter(|_| serves_feed)
            .map(|(budget, s)| self.build_delta_store(directory, id, s, budget))
            .transpose()?;
        Ok((Store::Held(Box::new(table)), delta))
    }

    /// This worker's delta store for a fed view. Erased at open: no delta
    /// expresses what a boot does to a view, so every cursor restarts.
    fn build_delta_store(
        &self,
        directory: &str,
        id: i64,
        delta_schema: SchemaDescriptor,
        budget: u64,
    ) -> Result<Box<Table>, StoreError> {
        let child = ChildAddr { kind: ChildKind::Delta, slot: self.slot };
        let table = Table::new(
            &child.dir(directory),
            delta_schema,
            RecoverySource::Rederive { resume_at: None },
            self.store_budgets().delta(budget),
        )
        .map_err(|e| StoreError::storage(format!("open delta store of view {id} (dir={directory})"), e))?;
        Ok(Box::new(table))
    }
}
