//! Per-key hydration for a capacity-bounded view: recompute one key's output
//! rows by re-running the view's own compiled program over a key-restricted
//! seed.
//!
//! A bounded view's store keeps only skeleton rows past its capacity — the PK
//! and one coarse weight — so a read that touches such a key has to reproduce
//! its payload. Nothing is stored to make that possible: the hydration ground
//! truth is state the view already keeps at full fidelity. For a linear body
//! that is the source relation's own store; for an inner equi-join it is the two
//! `integrate_trace` tables the compiled circuit already maintains.
//!
//! Exactness: restriction to a key is linear and time-invariant, so it commutes
//! with integration — `σ_k(I(s)) = I(σ_k(s))` — and the inner equi-join is
//! per-key, `σ_k(A ⋈ B) = σ_k(A) ⋈ σ_k(B)`. The compiled two-term union exists
//! to produce *deltas* incrementally; the state they integrate to is that single
//! product, which is what a replay over the two trace integrals computes.

use super::*;
use crate::query::compiler::{HydrationSeed, Sides};
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
        let (_, ViewPlan { code, state }) = self.ensure_compiled(registry, view_id).map_err(StoreError::rejected)?;
        let Sides::Unexchanged { hydration: Some(hydration) } = code.sides else {
            return Err(StoreError::rejected(format!(
                "hydrate: view {view_id} was not compiled as capacity-bounded"
            )));
        };

        let sub = &mut code.post;
        let seed_schema = *sub.vm.program.schema_of(hydration.in_reg);
        let mut gather = match hydration.seed {
            HydrationSeed::Relation(source) => {
                let entry = registry
                    .relation_or_err(source)
                    .map_err(|e| e.in_context(&format!("hydrate: view {view_id} source")))?;
                // A linear view's physical PK is the leading source-PK columns,
                // byte-identical to the source PK, so the store's own keys index
                // the source directly.
                PkSetGather::open(keys, seed_schema, |s, e| entry.cursor_in_range(s, e))
            }
            HydrationSeed::Trace(seed_table) => {
                let state = &*state;
                PkSetGather::open(keys, seed_schema, |s, e| state.cursor_in_range(seed_table, s, e))
            }
        };
        let mut out = Batch::empty_with_schema(&view_schema);
        while let Some(seed) = gather.next_chunk(registry.scan_chunk_rows()) {
            let produced = vm::replay_chunk(&mut sub.vm, state, hydration.start_pc, (hydration.in_reg, seed))
                .map_err(|e| StoreError::rejected(format!("hydrate: view {view_id} replay failed: {e}")))?;
            debug_assert!(
                produced.schema().same_physical_layout(&view_schema),
                "hydration produced a batch that is not in the view's schema",
            );
            out.append_above(produced.into_consolidated(&view_schema));
        }
        Ok(out)
    }
}
