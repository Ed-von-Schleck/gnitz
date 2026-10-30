//! Opening a relation's stores — at its registration, and again after the fork
//! for the process that holds them — and entering it and its indexes in the
//! registry.

use super::dirs::ensure_dir;
use super::*;
use gnitz_foundation::fault::Seam;

/// `GNITZ_INJECT_TABLE_CREATE_DELAY_MS`: stall a user table's create between its
/// directory and its child subdir, so a concurrent DROP races it.
static TABLE_CREATE_DELAY: Seam = Seam::new("GNITZ_INJECT_TABLE_CREATE_DELAY_MS");

impl RelationRegistry {
    /// [`Self::register_at`] with no index, every store rebuilt.
    pub fn register(&mut self, spec: RelationSpec, schema: SchemaDescriptor) -> Result<(), String> {
        self.register_at(spec, schema, &[], None).map(drop)
    }

    /// Enter a relation, at `schema`, with an index on each `(claim, cols)` of
    /// `indexes`, all or nothing. At `Some(generation)` a view's rows open from the
    /// manifest at that generation already in its directory, `Err` when there is
    /// none, and each index resumes from its own manifest at the same generation
    /// or is filled from the rows; at `None` everything is rebuilt. Returns how
    /// many indexes it filled.
    pub fn register_at(
        &mut self,
        spec: RelationSpec,
        schema: SchemaDescriptor,
        indexes: &[(IndexClaim, PkColList)],
        resume_at: Option<u64>,
    ) -> Result<usize, String> {
        let id = spec.id;
        if self.relations.contains_key(&id) {
            return Err(format!("relation {id} is already registered"));
        }
        let rows = self.build_rows(spec, schema, resume_at)?;
        self.relations.insert(id, Relation { spec, rows, indexes: Vec::new() });
        let filled = self.enter_indexes(id, indexes, resume_at);
        if filled.is_err() {
            self.relations.remove(&id);
        }
        filled
    }

    /// Enter a secondary index on `owner` for each `(claim, cols)`, the new ones
    /// filled by one scan of this process's slice of the owner, or add a claim
    /// to the one already on `cols`. On `Err` nothing is entered. A claim's
    /// `unique` is trusted: a duplicate can straddle two workers' slices, so
    /// only the caller can check it.
    pub fn add_indexes(&mut self, owner: u64, indexes: &[(IndexClaim, PkColList)]) -> Result<(), String> {
        // Always rebuilt: a manifest a released index left on `cols` is stale.
        self.enter_indexes(owner, indexes, None).map(drop)
    }

    /// Enter `indexes` on `owner`, each as [`Self::add_indexes`] does, their new
    /// stores resumed at `resume_at` and the ones that did not filled by one scan
    /// of the owner. On `Err` nothing is entered. Returns how many it filled.
    fn enter_indexes(
        &mut self,
        owner: u64,
        indexes: &[(IndexClaim, PkColList)],
        resume_at: Option<u64>,
    ) -> Result<usize, String> {
        let mut claimed: Vec<(usize, IndexClaim)> = Vec::new();
        let mut opened: Vec<SecondaryIndex> = Vec::new();
        for &(claim, cols) in indexes {
            let unique = matches!(claim, IndexClaim::Index { unique: true, .. });
            let entry = self.index_owner(owner, unique)?;
            if let Some(at) = entry.indexes.iter().position(|ix| ix.cols == cols) {
                debug_assert!(!entry.indexes[at].claims.contains(&claim), "{claim:?} claimed twice");
                claimed.push((at, claim));
                continue;
            }
            if let Some(ix) = opened.iter_mut().find(|ix| ix.cols == cols) {
                ix.claims.push(claim);
                continue;
            }
            let owner_schema = entry.schema();
            let fail = |e: String| format!("index on columns {:?} of relation {owner}: {e}", cols.as_slice());
            let key_spec = KeySpec::new(cols.as_slice(), &owner_schema).map_err(fail)?;
            // One that does not resume opens empty, for the fill below.
            let table = if self.residency.publishes(entry.kind()) {
                let dir = self.child_dir(owner, ChildKind::Index(cols));
                let recovery = RecoverySource::Rederive { resume_at };
                let table = self
                    .open_store(&dir, key_spec.index_schema(&owner_schema), recovery, None, false)
                    .map_err(fail)?;
                Some(Box::new(table))
            } else {
                None
            };
            opened.push(SecondaryIndex {
                cols,
                table,
                key_spec,
                claims: vec![claim],
                covers_pk: owner_schema.covers_pk(cols.as_slice()),
            });
        }
        let chunk_rows = self.config.scan_chunk_rows;
        let entry = self.relations.get_mut(&owner).expect("resolved above");
        let mut targets: Vec<&mut SecondaryIndex> = opened
            .iter_mut()
            .filter(|ix| ix.table.is_some() && !ix.resumed())
            .collect();
        let filled = targets.len();
        if filled > 0 {
            // One chunked scan of the owner fills every target.
            let mut source = entry.rows.held().open_cursor(Cut::Now);
            while let Some(chunk) = source.drain_chunk(chunk_rows) {
                for ix in targets.iter_mut() {
                    ix.project_and_ingest(&chunk).map_err(|e| {
                        format!(
                            "fill index on columns {:?} of relation {owner}: {e}",
                            ix.cols.as_slice()
                        )
                    })?;
                }
            }
        }
        for (at, claim) in claimed {
            entry.indexes[at].claims.push(claim);
        }
        entry.indexes.append(&mut opened);
        Ok(filled)
    }

    /// Become rank `rank` as `residency`: enter every user relation again, as
    /// [`Self::register_at`] at `resume_at(id)` with the index claims it holds,
    /// now opening this process's stores. Returns how many indexes it filled.
    pub fn open_stores(
        &mut self,
        rank: u32,
        residency: Residency,
        resume_at: impl Fn(u64) -> Option<u64>,
    ) -> Result<usize, String> {
        assert_eq!(
            self.residency,
            Residency::Master,
            "open_stores runs once, on a master registry"
        );
        assert_ne!(residency, Residency::Master);
        self.slot = Slot::new(rank, self.slot.of);
        self.residency = residency;
        let ids: Vec<u64> = self
            .relations
            .iter()
            .filter(|(_, e)| e.kind() != RelationKind::SystemCatalog)
            .map(|(&id, _)| id)
            .collect();
        let mut filled = 0usize;
        for id in ids {
            let Relation { spec, rows, indexes } = self.relations.remove(&id).expect("listed above");
            let claims: Vec<_> = indexes
                .iter()
                .flat_map(|ix| ix.claims.iter().map(|&claim| (claim, ix.cols)))
                .collect();
            filled += self.register_at(spec, *rows.schema(), &claims, resume_at(id))?;
        }
        // A worker applies the same catalog deltas as the master, but only the
        // master writes the system tables' shards.
        self.held_rows(|k| !residency.publishes(k)).for_each(Table::hold_in_ram);
        Ok(filled)
    }

    /// The system families' `table id → replay floor` their stores opened with:
    /// the floors of the master's pre-fork SAL walk.
    pub fn system_replay_floors(&self) -> std::collections::HashMap<u64, u64> {
        self.relations
            .iter()
            .filter(|(_, entry)| entry.kind() == RelationKind::SystemCatalog)
            .map(|(&id, entry)| (id, entry.table().loaded_mark().unwrap_or(0)))
            .collect()
    }

    /// This process's rows of a relation: its store and a fed view's feed. A
    /// view's rows resume from the manifest at `resume_at`, or are rebuilt.
    fn build_rows(&self, spec: RelationSpec, schema: SchemaDescriptor, resume_at: Option<u64>) -> Result<Rows, String> {
        let RelationSpec { id, kind, placement, compress, .. } = spec;
        let (recovery, capacity, feed) = match kind {
            RelationKind::Stream => return Ok(Rows::Stream { schema: Box::new(schema), unsealed: None }),
            // Its store is the relation directory itself, outside the per-slot child
            // layout that a worker-count change relays and reclaims.
            RelationKind::SystemCatalog => {
                let dir = relation_dir(&self.base_dir, id);
                let table = self.open_store(&dir, schema, RecoverySource::SalReplay, None, false)?;
                return Ok(Rows::Held { table: Box::new(table), feed: None });
            }
            // The master opens no user store, but it still creates the relation's
            // directory, so the directory exists by the time the DDL is acknowledged.
            _ if !self.residency.publishes(kind) => {
                ensure_dir(&relation_dir(&self.base_dir, id))?;
                return Ok(Rows::Absent(Box::new(schema)));
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
        let table = self.open_child(id, ChildKind::Rows, schema, recovery, capacity, compress)?;
        // Empty at every open: no delta expresses what a boot does to a view, so every cursor restarts.
        let feed = feed.filter(|_| placement.counts_on(self.slot.rank)).map(Feed::new);
        Ok(Rows::Held { table: Box::new(table), feed })
    }

    /// This process's `kind` child store of relation `id`; `Err` for one told to
    /// resume that found no manifest at that generation.
    pub(super) fn open_child(
        &self,
        id: u64,
        kind: ChildKind<'_>,
        schema: SchemaDescriptor,
        recovery: RecoverySource,
        capacity: Option<u64>,
        compress: bool,
    ) -> Result<Table, String> {
        let table = self.open_store(&self.child_dir(id, kind), schema, recovery, capacity, compress)?;
        if matches!(recovery, RecoverySource::Rederive { resume_at: Some(_) }) && table.loaded_mark().is_none() {
            return Err(format!(
                "relation {id}: {kind:?} store did not resume from its manifest"
            ));
        }
        Ok(table)
    }

    /// The store at `dir`, its RAM tier this registry's.
    fn open_store(
        &self,
        dir: &str,
        schema: SchemaDescriptor,
        recovery: RecoverySource,
        capacity: Option<u64>,
        compress: bool,
    ) -> Result<Table, String> {
        Table::new(dir, schema, recovery, self.config.ram_tier_bytes, capacity, compress)
            .map_err(|e| format!("open store '{dir}': {e}"))
    }
}

#[cfg(test)]
#[path = "tests/build.rs"]
mod tests;
