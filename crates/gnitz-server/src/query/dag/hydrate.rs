//! Per-key hydration for a capacity-bounded view: recompute a skeleton key's
//! output rows by replaying the view's own program over a key-restricted seed.

use super::*;
use crate::query::compiler::HydrationSeed;
use gnitz_store::read::SkeletonHydrator;
use gnitz_store::storage::{PkSetGather, StoreError};

/// Recompute the output rows of the capacity-bounded view `view_id` for `keys`
/// — the flat concatenation of the OPK images, ascending — replaying at most
/// `scan_chunk_rows` seed rows at a time.
impl SkeletonHydrator for DagEngine {
    fn hydrate_keys(&mut self, registry: &RelationRegistry, view_id: i64, keys: Vec<u8>) -> Result<Batch, StoreError> {
        let view_schema = registry
            .relation_or_err(view_id)
            .map_err(|e| e.in_context(&format!("hydrate: view {view_id}")))?
            .schema();
        let DagEngine { views, unticked, .. } = self;
        let (_, ViewPlan { code, state }) = ensure_compiled(views, registry, view_id).map_err(StoreError::rejected)?;
        let hydration = code.hydration.ok_or_else(|| {
            StoreError::rejected(format!("hydrate: view {view_id} was not compiled as capacity-bounded"))
        })?;

        let sub = &mut code.post;
        let seed_schema = *sub.vm.program.schema_of(hydration.entry.reg());
        let mut gather = match hydration.seed {
            HydrationSeed::Relation(source) => {
                let entry = registry
                    .relation_or_err(source)
                    .map_err(|e| e.in_context(&format!("hydrate: view {view_id} source")))?;
                // The view last saw the source at its last tick.
                entry.gather(keys, unticked.get(&source))
            }
            HydrationSeed::Trace(seed_table) => {
                let state = &*state;
                PkSetGather::open(keys, seed_schema, |s, e| state.cursor_in_range(seed_table, s, e))
            }
        };
        let mut out = Batch::empty_with_schema(&view_schema);
        while let Some(seed) = gather.next_chunk(registry.scan_chunk_rows()) {
            let produced = vm::replay_chunk(&mut sub.vm, state, hydration.entry, seed)
                .map_err(|e| StoreError::rejected(format!("hydrate: view {view_id} replay failed: {e}")))?;
            out.append_above(produced.into_consolidated(&view_schema));
        }
        Ok(out)
    }
}
