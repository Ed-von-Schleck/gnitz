//! Opening a relation's stores and entering it in the registry.

use super::dirs::ensure_dir;
use super::*;
use gnitz_foundation::fault::Seam;

/// `GNITZ_INJECT_TABLE_CREATE_DELAY_MS`: stall a user table's create between its
/// directory and its child subdir, so a concurrent DROP races it.
static TABLE_CREATE_DELAY: Seam = Seam::new("GNITZ_INJECT_TABLE_CREATE_DELAY_MS");

impl RelationRegistry {
    /// [`Self::register_at`] with a view's rows rebuilt.
    pub fn register(&mut self, spec: RelationSpec) -> Result<(), String> {
        self.register_at(spec, None)
    }

    /// Enter a relation. A view's rows open from the manifest at
    /// `Some(generation)` already in its directory, `Err` entering nothing when
    /// there is none, and are rebuilt at `None`.
    pub fn register_at(&mut self, spec: RelationSpec, resume_at: Option<u64>) -> Result<(), String> {
        let (store, feed) = self.build_relation_store(spec, resume_at)?;
        let RelationSpec {
            id,
            kind,
            placement,
            pk_repeats,
            compress,
            ..
        } = spec;
        let prev = self.tables.insert(
            id,
            Relation {
                id,
                store,
                unsealed: None,
                feed,
                indexes: Vec::new(),
                traces: Box::default(),
                kind,
                placement,
                pk_repeats,
                compress,
            },
        );
        debug_assert!(prev.is_none(), "relation {id} registered twice");
        Ok(())
    }

    /// This process's stores for a relation: its rows, and a fed view's feed. A
    /// view's rows resume from the manifest at `resume_at`, or are rebuilt.
    pub(super) fn build_relation_store(
        &self,
        spec: RelationSpec,
        resume_at: Option<u64>,
    ) -> Result<(Store, Option<Feed>), String> {
        let RelationSpec {
            id, kind, schema, placement, compress, ..
        } = spec;
        let absent = || Ok((Store::Absent(Box::new(schema)), None));
        let (recovery, capacity, feed) = match kind {
            RelationKind::Stream => return absent(),
            // Its store is the relation directory itself, outside the per-slot child
            // layout that a worker-count change relays and reclaims.
            RelationKind::SystemCatalog => {
                let dir = relation_dir(&self.base_dir, id);
                let table = Table::new(
                    &dir,
                    schema,
                    RecoverySource::SalReplay,
                    self.config.ram_tier_bytes,
                    None,
                    false,
                )
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
                (RecoverySource::SalReplay, None, None)
            }
            RelationKind::View(p) => (
                RecoverySource::Rederive { resume_at },
                p.capacity_bytes(),
                p.delta_bytes(),
            ),
        };
        let rows = self.open_child_as(id, ChildKind::Rows, schema, recovery, capacity, compress)?;
        // Empty at every open: no delta expresses what a boot does to a view, so every cursor restarts.
        let feed = feed.filter(|_| placement.counts_on(self.slot.rank)).map(Feed::new);
        Ok((Store::Held(Box::new(rows)), feed))
    }

    /// This process's store of the index on `owner` over `cols`, resumed from its
    /// manifest at `resume_at` when it carries one.
    pub(super) fn open_index_store(
        &self,
        owner: u64,
        cols: PkColList,
        schema: SchemaDescriptor,
        resume_at: Option<u64>,
    ) -> Result<Store, String> {
        let recovery = RecoverySource::Rederive { resume_at };
        let table = self.open_child(owner, ChildKind::Index(cols), schema, recovery, None, false)?;
        Ok(Store::Held(Box::new(table)))
    }

    /// [`Self::open_child`], `Err` for a store asked to resume that did not.
    pub(super) fn open_child_as(
        &self,
        id: u64,
        kind: ChildKind<'_>,
        schema: SchemaDescriptor,
        recovery: RecoverySource,
        capacity: Option<u64>,
        compress: bool,
    ) -> Result<Table, String> {
        let table = self.open_child(id, kind, schema, recovery, capacity, compress)?;
        // A resume that found no manifest at its generation.
        if matches!(recovery, RecoverySource::Rederive { resume_at: Some(_) }) && table.loaded_mark().is_none() {
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
        capacity: Option<u64>,
        compress: bool,
    ) -> Result<Table, String> {
        let dir = self.child_dir(id, kind);
        Table::new(&dir, schema, recovery, self.config.ram_tier_bytes, capacity, compress)
            .map_err(|e| format!("open store '{dir}': {e}"))
    }
}

#[cfg(test)]
#[path = "tests/build.rs"]
mod tests;
