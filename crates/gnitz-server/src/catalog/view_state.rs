//! Store state that needs both the registry and the DAG: the store open under
//! the boot's resume point, the ephemeral checkpoint round, the orphan-directory
//! sweep, and the hydrating read.

use gnitz_store::read::ReadRows;
use gnitz_store::relation::Residency;
use gnitz_wire::ReadSpec;

use super::CatalogEngine;

impl CatalogEngine {
    /// [`gnitz_store::relation::RelationRegistry::scan_spec`], hydrating.
    pub(crate) fn scan_spec(&mut self, target_id: u64, spec: ReadSpec, reply_layout: u64) -> Result<ReadRows, String> {
        let (registry, dag) = (&self.registry, &mut self.dag);
        registry.scan_spec(
            target_id,
            spec,
            reply_layout,
            Some(&mut |k| dag.hydrate_keys(registry, target_id, k)),
        )
    }

    /// Open this process's stores: a relation's rederived state resumes at the
    /// boot verdict's generation unless the verdict rebuilds it.
    pub(crate) fn open_stores(&mut self, rank: u32, residency: Residency) -> Result<usize, String> {
        let boot = &self.boot;
        self.registry.open_stores(rank, residency, |id| {
            (!boot.rebuild.contains(&id)).then_some(boot.resume_at)
        })
    }

    /// The ephemeral checkpoint round: persist the operator traces, output store
    /// and indexes of every view a boot can resume, and every table's indexes, at
    /// `generation`.
    pub(crate) fn flush_ephemeral_round(&mut self, generation: u64) -> Result<(), String> {
        let never = self.dag.never_resumed(&self.registry);
        let resumes = |id| !never.contains(&id);
        let states = self.dag.ephemeral_states(&self.registry, generation, resumes)?;
        self.registry.checkpoint_ephemeral(states, generation, resumes)
    }

    /// Remove every relation and index directory no live entity owns. Only sound once
    /// every worker has ACKed a round written after the last DdlSync.
    pub(crate) fn reclaim_orphan_dirs(&self) {
        if let Err(e) = self.registry.reclaim_orphan_relation_dirs(|id| self.dag.layout_of(id)) {
            gnitz_warn!("catalog: orphan directory sweep failed: {}", e);
        }
    }
}
