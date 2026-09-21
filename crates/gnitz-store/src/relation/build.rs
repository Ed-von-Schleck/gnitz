//! Opening a relation's stores and entering it in the registry.

use super::*;
use crate::schema::Placement;
use crate::storage::ChildKind;
use gnitz_foundation::fault::Seam;

/// `GNITZ_INJECT_TABLE_CREATE_DELAY_MS`: stall a user table's create between its
/// directory and its child subdir, so a concurrent DROP races it.
static TABLE_CREATE_DELAY: Seam = Seam::new("GNITZ_INJECT_TABLE_CREATE_DELAY_MS");

impl RelationRegistry {
    /// Enter a relation and open its stores.
    pub fn register(&mut self, spec: RelationSpec) -> Result<(), StoreError> {
        let RelationSpec { id, kind, schema, props } = spec;
        let directory = relation_dir(&self.base_dir, kind, id);
        let (store, delta) = self.build_relation_store(kind, &directory, id, schema, props)?;
        let relation = Relation {
            id,
            store,
            delta,
            indexes: Vec::new(),
            kind,
            directory,
            props,
        };
        self.tables.insert(id, relation);
        Ok(())
    }

    /// This process's stores for a relation: its rows, and a fed view's deltas.
    pub(crate) fn build_relation_store(
        &self,
        kind: RelationKind,
        directory: &str,
        id: i64,
        schema: SchemaDescriptor,
        props: ViewProps,
    ) -> Result<(Store, Option<Box<Store>>), StoreError> {
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
            RelationKind::Stream => return Ok((Store::detached(schema), None)),
            // A view's output store and its operator traces resume from the
            // manifest the ephemeral checkpoint round stamped, or are rebuilt.
            RelationKind::View if self.non_resumable.contains(&id) => RecoverySource::Rederive { resume_at: None },
            RelationKind::View => self.rederive_source(),
            RelationKind::SystemCatalog | RelationKind::BaseTable => RecoverySource::SalReplay,
        };
        ensure_dir(directory)?;
        if kind != RelationKind::SystemCatalog && !self.residency.owns_stores() {
            return Ok((Store::detached(schema), None));
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
        let serves_feed = !schema.placement().is_replicated() || self.slot.rank == Placement::REPLICA_OWNER;
        let delta = delta
            .filter(|_| serves_feed)
            .map(|(budget, s)| self.build_delta_store(directory, id, s, budget))
            .transpose()?;
        Ok((Store::owned(Box::new(table), schema), delta))
    }

    /// This worker's delta store for a fed view. Erased at open: no delta
    /// expresses what a boot does to a view, so every cursor restarts.
    fn build_delta_store(
        &self,
        directory: &str,
        id: i64,
        delta_schema: SchemaDescriptor,
        budget: u64,
    ) -> Result<Box<Store>, StoreError> {
        let child = ChildAddr { kind: ChildKind::Delta, slot: self.slot };
        let table = Table::new(
            &child.dir(directory),
            delta_schema,
            RecoverySource::Rederive { resume_at: None },
            self.store_budgets().delta(budget),
        )
        .map_err(|e| StoreError::storage(format!("open delta store of view {id} (dir={directory})"), e))?;
        Ok(Box::new(Store::owned(Box::new(table), delta_schema)))
    }
}
