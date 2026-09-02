//! Opening a relation's stores and entering it in the registry — the one site
//! that decides the child address, the recovery source, the capacity stamp and
//! whether a delta store is opened.

use super::*;
use crate::foundation::fault::Seam;

/// `GNITZ_INJECT_TABLE_CREATE_DELAY_MS`: stall a user table's create between its
/// directory and its child subdir, so a concurrent DROP races it.
static TABLE_CREATE_DELAY: Seam = Seam::new("GNITZ_INJECT_TABLE_CREATE_DELAY_MS");

impl RelationRegistry {
    /// Enter a relation and open its store — the one site that decides the child
    /// address, the `RecoverySource`, the capacity stamp and whether a delta
    /// store is opened, so the catalog (from a TABLE_TAB / VIEW_TAB row) and a
    /// mirror (from its own record) cannot drift on any of them.
    ///
    /// Opens through [`staged_dir`], so a directory this call creates is removed
    /// again if the open fails and has its parent fsynced if it succeeds.
    pub fn register(&mut self, spec: RelationSpec) -> Result<(), StoreError> {
        let stores = staged_dir(&spec.directory, || {
            self.build_relation_store(spec.kind, &spec.directory, spec.id, spec.schema, spec.budgets)
        })?;
        self.enter(spec, stores);
        Ok(())
    }

    /// Build this process's store for a top-level relation: one `Table` under
    /// this slot's `w{rank}of{n}` child. Only user relations are built here — system
    /// catalog tables are plain single `Table`s built at bootstrap.
    ///
    /// The `match kind` below is the one place a per-kind property is derived,
    /// which is what makes the nonsense combinations — durable but rebuilt from
    /// source, ephemeral but never rebuilt — unconstructable.
    ///
    /// The child is homed at THIS process's own worker rank, so a live CREATE on
    /// each worker post-fork builds a distinct dir directly. The post-fork master
    /// owns no store at all: it registers the relation `Detached` so it and
    /// worker 0 do not both hold a live `Table` on `w0of{W}`. Both storeless cases
    /// are decided here rather than at the callers, so a new caller cannot build a
    /// `Table` for one of them.
    ///
    /// A fed view's **delta store** is opened here too, and returned with the
    /// relation's own: every path that opens a relation's store comes through
    /// here, so a feed cannot come back missing from a rehome or a rebuild — the
    /// same reason the capacity is stamped here.
    ///
    /// Creates `directory` when the kind owns a store; [`staged_dir`] is what
    /// makes that creation crash-safe.
    pub(crate) fn build_relation_store(
        &self,
        kind: RelationKind,
        directory: &str,
        id: i64,
        schema: SchemaDescriptor,
        budgets: ViewBudgets,
    ) -> Result<RelationStores, StoreError> {
        // Above every early return below, so it runs on **every** process: the
        // post-fork master opens no user store at all, and a limit first noticed
        // on a worker would be a fatal abort taken after the client was told the
        // CREATE succeeded.
        let delta = budgets
            .delta_bytes
            .map(|budget| {
                crate::schema::make_delta_schema(&schema)
                    .map(|delta_schema| (budget, delta_schema))
                    .ok_or_else(|| {
                        StoreError::rejected(format!("view {id} has too many columns to carry a delta feed"))
                    })
            })
            .transpose()?;

        // Above `ensure_dir`: a storeless kind owns no store in any process, so no
        // directory is created for one.
        let recovery = match kind {
            RelationKind::Stream => return Ok(RelationStores::detached()),
            // A view's output store and its operator traces resume from the
            // manifest the ephemeral checkpoint round stamped, or are rebuilt.
            RelationKind::View => self.rederive_source(),
            RelationKind::SystemCatalog | RelationKind::BaseTable => RecoverySource::SalReplay,
        };
        ensure_dir(directory)?;
        if !self.owns_stores {
            return Ok(RelationStores::detached());
        }

        // Widen the window where the table dir exists but its child subdir does
        // not, so a concurrent master remove_dir_all (DROP) deterministically
        // races this create. User tables only.
        if kind.is_base_table() {
            if let Some(ms) = TABLE_CREATE_DELAY.count() {
                std::thread::sleep(std::time::Duration::from_millis(ms));
            }
        }

        let child = ChildAddr::worker(self.slot);
        let mut table = Table::with_budgets(&child.dir(directory), schema, id as u32, recovery, self.config.ram)
            .map_err(|e| StoreError::storage(format!("open relation {id} (dir={directory})"), e))?;
        // Every store this worker opens for a relation comes through here, so a
        // bounded view cannot come back unbounded from a rehome or a rebuild.
        table.set_capacity(budgets.capacity_bytes);
        Ok(RelationStores {
            handle: StoreHandle::owned(Box::new(table)),
            delta: delta
                .map(|(budget, s)| Self::build_delta_store(self.slot, self.config.ram, directory, id, s, budget))
                .transpose()?,
        })
    }

    /// This worker's delta store for a fed view, under `delta_w{rank}` of the
    /// view's own directory.
    ///
    /// Erased at open — `Rederive { resume_at: None }` — deliberately: no delta
    /// expresses what a boot does to a view over a stream or to an invalidated
    /// view, and a restart mints a fresh boot nonce, so every cursor a client
    /// holds stops matching and it re-reads at `after_tick = 0`.
    fn build_delta_store(
        slot: Slot,
        ram: RamBudgets,
        directory: &str,
        id: i64,
        delta_schema: SchemaDescriptor,
        budget: u64,
    ) -> Result<Box<DeltaFeed>, StoreError> {
        let child = ChildAddr::delta(slot);
        let mut table = Table::with_budgets(
            &child.dir(directory),
            delta_schema,
            id as u32,
            RecoverySource::Rederive { resume_at: None },
            ram,
        )
        .map_err(|e| StoreError::storage(format!("open delta store of view {id} (dir={directory})"), e))?;
        table.set_delta_budget(budget);
        Ok(Box::new(DeltaFeed {
            schema: delta_schema,
            handle: StoreHandle::owned(Box::new(table)),
        }))
    }
}
