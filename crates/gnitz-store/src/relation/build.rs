//! Opening a relation's stores — at its registration, and again after the fork
//! for the process that holds them — and entering it and its indexes in the
//! registry.

use super::dirs::ensure_dir;
use super::*;
use gnitz_foundation::fault::Count;

/// `GNITZ_INJECT_TABLE_CREATE_DELAY_MS`: stall a user table's create between its
/// directory and its child subdir, so a concurrent DROP races it.
static TABLE_CREATE_DELAY: Count = Count::new("GNITZ_INJECT_TABLE_CREATE_DELAY_MS");

impl RelationRegistry {
    /// [`Self::register_at`] with no index and no trace, every store rebuilt.
    pub fn register(&mut self, spec: RelationSpec, schema: SchemaDescriptor) -> Result<(), String> {
        self.register_at(spec, schema, &[], StateLayout::default(), None)
            .map(drop)
    }

    /// Enter a relation, at `schema`, with an index on each `(claim, cols)` of
    /// `indexes`, all or nothing; a view with the operator `traces` its compiled
    /// circuit declares, which [`CircuitState::open`] opens. At
    /// `Some(generation)` a view's rows open from the manifest at that generation
    /// already in its directory, `Err` when there is none, and each index
    /// resumes from its own manifest at the same generation or is filled from
    /// the rows; at `None` everything is rebuilt. Returns how many indexes it
    /// filled.
    pub fn register_at(
        &mut self,
        spec: RelationSpec,
        schema: SchemaDescriptor,
        indexes: &[(IndexClaim, PkColList)],
        traces: StateLayout,
        resume_at: Option<u64>,
    ) -> Result<usize, String> {
        let id = spec.id;
        if self.relations.contains_key(&id) {
            return Err(format!("relation {id} is already registered"));
        }
        debug_assert!(
            traces.is_empty() || spec.kind.is_view(),
            "only a view keeps operator traces"
        );
        let rows = self.build_rows(spec, schema, resume_at)?;
        let mut relation = Relation { spec, rows, indexes: Vec::new(), traces };
        let filled = self.enter_indexes(&mut relation, indexes, resume_at)?;
        self.relations.insert(id, relation);
        Ok(filled)
    }

    /// Enter a secondary index on `owner` for each `(claim, cols)`, the new ones
    /// filled by one scan of this process's slice of the owner, or add a claim
    /// to the one already on `cols`. On `Err` nothing is entered. A claim's
    /// `unique` is trusted: a duplicate can straddle two workers' slices, so
    /// only the caller can check it.
    pub fn add_indexes(&mut self, owner: u64, indexes: &[(IndexClaim, PkColList)]) -> Result<(), String> {
        let mut relation = self.relations.remove(&owner).ok_or_else(|| Self::unregistered(owner))?;
        // Always rebuilt: a manifest a released index left on `cols` is stale.
        let entered = self.enter_indexes(&mut relation, indexes, None);
        self.relations.insert(owner, relation);
        entered.map(drop)
    }

    /// Enter `indexes` on `owner`, each as [`Self::add_indexes`] does, and
    /// [attach](Self::attach_indexes) the new ones' stores at `resume_at`. On
    /// `Err` nothing is entered. Returns how many it filled.
    fn enter_indexes(
        &self,
        owner: &mut Relation,
        indexes: &[(IndexClaim, PkColList)],
        resume_at: Option<u64>,
    ) -> Result<usize, String> {
        let schema = owner.schema();
        let mut claimed: Vec<(usize, IndexClaim)> = Vec::new();
        let mut declared: Vec<SecondaryIndex> = Vec::new();
        for &(claim, cols) in indexes {
            owner.admit_index(matches!(claim, IndexClaim::Index { unique: true, .. }))?;
            if let Some(at) = owner.indexes.iter().position(|ix| ix.cols == cols) {
                debug_assert!(!owner.indexes[at].claims.contains(&claim), "{claim:?} claimed twice");
                claimed.push((at, claim));
                continue;
            }
            if let Some(ix) = declared.iter_mut().find(|ix| ix.cols == cols) {
                ix.claims.push(claim);
                continue;
            }
            let key_spec = KeySpec::new(cols.as_slice(), &schema).map_err(|e| index_fault(owner.id(), cols, e))?;
            let store = match key_spec.leads_pk() {
                true => IndexStore::OwnerPk,
                false => IndexStore::NotHeld,
            };
            declared.push(SecondaryIndex {
                cols,
                store,
                key_spec,
                claims: vec![claim],
            });
        }
        let entered = owner.indexes.len();
        owner.indexes.append(&mut declared);
        let filled = self
            .attach_indexes(owner, resume_at)
            .inspect_err(|_| owner.indexes.truncate(entered))?;
        for (at, claim) in claimed {
            owner.indexes[at].claims.push(claim);
        }
        Ok(filled)
    }

    /// Open this process's store of each of `owner`'s indexes not yet held,
    /// where it holds `owner`'s rows, resumed at `resume_at`; the ones that did
    /// not resume are filled by one scan of the owner. Returns how many it
    /// filled.
    fn attach_indexes(&self, owner: &mut Relation, resume_at: Option<u64>) -> Result<usize, String> {
        if owner.rows.table().is_none() {
            return Ok(0);
        }
        let (id, schema) = (owner.id(), owner.schema());
        let mut targets: Vec<usize> = Vec::new();
        for (at, ix) in owner.indexes.iter_mut().enumerate() {
            if !matches!(ix.store, IndexStore::NotHeld) {
                continue;
            }
            let dir = self.child_dir(id, ChildKind::Index(ix.cols));
            let recovery = RecoverySource::Rederive { resume_at };
            let table = self
                .open_store(&dir, ix.key_spec.index_schema(&schema), recovery, None, false)
                .map_err(|e| index_fault(id, ix.cols, e))?;
            ix.store = IndexStore::Own(Box::new(table));
            if !ix.resumed() {
                targets.push(at);
            }
        }
        if !targets.is_empty() {
            // One chunked scan of the owner fills every target.
            // An entry reads its row's PK and key columns alone.
            let keys = targets
                .iter()
                .flat_map(|&at| owner.indexes[at].cols.as_slice().iter().copied());
            let mut source = owner.open_cursor(Cut::Now, schema.payload_bits(keys));
            while let Some(chunk) = source.drain_chunk(self.config.scan_chunk_rows) {
                for &at in &targets {
                    let ix = &mut owner.indexes[at];
                    ix.project_and_ingest(&chunk)
                        .map_err(|e| index_fault(id, ix.cols, format!("fill: {e}")))?;
                }
            }
        }
        Ok(targets.len())
    }

    /// Become rank `rank` as `residency`: open this process's stores of every
    /// user relation in place — its rows at `resume_at(id)`, as
    /// [`Self::register_at`] would, and its indexes'. Returns how many indexes it
    /// filled. An `Err` leaves the registry part-attached.
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
        // Out of the map while each is opened in place: an opener reads the
        // registry's own fields.
        let mut relations = std::mem::take(&mut self.relations);
        let filled = relations
            .values_mut()
            .filter(|r| r.kind() != RelationKind::SystemCatalog)
            .try_fold(0usize, |filled, relation| {
                let at = resume_at(relation.id());
                relation.rows = self.build_rows(relation.spec, relation.schema(), at)?;
                Ok::<_, String>(filled + self.attach_indexes(relation, at)?)
            });
        self.relations = relations;
        let filled = filled?;
        // A worker applies the same catalog deltas as the master, but only the
        // master writes the system tables' shards.
        self.held_rows(|k| !residency.publishes(k)).for_each(Table::hold_in_ram);
        Ok(filled)
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

/// An index's fault `e`, on columns `cols` of relation `owner`.
fn index_fault(owner: u64, cols: PkColList, e: String) -> String {
    format!("index on columns {:?} of relation {owner}: {e}", cols.as_slice())
}

#[cfg(test)]
#[path = "tests/build.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/build.rs"]
mod bench;
