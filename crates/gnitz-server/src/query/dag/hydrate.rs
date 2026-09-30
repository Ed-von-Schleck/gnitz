//! Per-key hydration for a capacity-bounded view: recompute a skeleton key's
//! output rows by replaying the view's own program over a key-restricted seed.

use super::*;
use crate::query::compiler::HydrationSeed;
use gnitz_store::read::SkeletonHydrator;
use gnitz_wire::PkKeys;

impl SkeletonHydrator for DagEngine {
    fn hydrate_keys(&mut self, registry: &RelationRegistry, view_id: u64, keys: PkKeys) -> Result<Batch, String> {
        let view_schema = registry.relation_or_err(view_id)?.schema();
        let DagEngine { views, unticked, .. } = self;
        let (_, ViewPlan { code, state }) = ensure_compiled(views, registry, view_id)?;
        let hydration = code
            .hydration
            .expect("a store holding skeleton rows is a bounded view's, compiled with its hydration");

        let vm = &mut code.post.vm;
        let mut gather = match hydration.seed {
            HydrationSeed::Relation(source) => {
                let since_last_tick = unticked.get(&source);
                registry.relation_or_err(source)?.gather(keys, since_last_tick)
            }
            HydrationSeed::Trace(seed_table) => state.gather(seed_table, keys),
        };
        let mut out = Batch::empty_with_schema(&view_schema);
        while let Some(seed) = gather.drain_chunk(registry.scan_chunk_rows()) {
            let produced = vm::replay_chunk(vm, state, hydration.entry, seed)?;
            out.append_above(produced.into_consolidated());
        }
        Ok(out)
    }
}

#[cfg(test)]
#[path = "tests/hydrate.rs"]
mod tests;
