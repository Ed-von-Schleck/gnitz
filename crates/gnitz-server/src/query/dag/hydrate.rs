//! Per-key hydration for a capacity-bounded view: recompute a skeleton key's
//! output rows by replaying the view's own program over a key-restricted seed.

use super::*;
use gnitz_wire::PkKeys;

impl DagEngine {
    /// Every row of `view_id` at `keys`, each held by the view's store as a
    /// skeleton row: the hydrate closure of [`RelationRegistry::scan_spec`].
    pub(crate) fn hydrate_keys(
        &mut self,
        registry: &RelationRegistry,
        view_id: u64,
        keys: PkKeys,
    ) -> Result<Batch, String> {
        let view = self.registered(view_id)?;
        let replay = view
            .code
            .hydration
            .ok_or_else(|| format!("view {view_id} holds skeleton rows but was compiled unbounded"))?;
        let (vm, mut stores) = view.opened(registry, view_id, usize::MAX)?;
        vm::replay_keys(vm, &mut stores, &replay, keys)
    }
}

#[cfg(test)]
#[path = "tests/hydrate.rs"]
mod tests;
