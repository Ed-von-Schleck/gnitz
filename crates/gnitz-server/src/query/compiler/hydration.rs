//! Where a capacity-bounded view's per-key replay seeds: resolved off the
//! circuit graph, then against the plan the emitter produced.

use super::*;
use crate::query::vm::ReplayEntry;
use gnitz_store::relation::RelationKind;

/// Where a bounded view's per-key replay seeds: the register it enters at, and
/// the integral the seed is gathered from.
#[derive(Clone, Copy)]
pub(in crate::query) struct Hydration {
    pub(in crate::query) entry: ReplayEntry,
    pub(in crate::query) seed: Integral,
}

const UNSUPPORTED: &str =
    "capacity-bounded view: only a filter/projection over one relation or over an inner equi-join is supported";

/// Resolve the seed against the plan the emitter produced for it. The seed is
/// gathered by the view's own keys: the output's PK region is the seeded node's,
/// which a row-local walk and an equi join's delta both keep.
pub(super) fn derive_hydration(
    circuit: &Circuit,
    registry: &RelationRegistry,
    vm: &Vm,
    regs: &[DeltaReg],
    integrals: &[Option<Integral>],
    sources: &FxHashMap<u64, Scan>,
) -> Result<Hydration, String> {
    use gnitz_wire::{JoinKind, OpNode};
    // A replay runs on one worker, and so no exchange round.
    if circuit.ops().any(|(_, op)| matches!(op, OpNode::ExchangeShard)) {
        return Err(UNSUPPORTED.into());
    }
    let origin = row_local_origin(circuit, circuit.out());
    let (in_reg, seed) = match circuit.op(origin) {
        OpNode::ScanDelta { source, .. } => {
            if registry.relation_or_err(*source)?.kind() == RelationKind::Stream {
                return Err(
                    "capacity-bounded view: a filter/projection over a stream has no stored rows to recompute from"
                        .into(),
                );
            }
            (regs[origin], Integral::Relation(*source))
        }
        OpNode::Join { kind: JoinKind::Equi } => {
            let &[da, db, ia, ib] = circuit.inputs(origin) else {
                unreachable!("a join is wired on four inputs")
            };
            // Seeding `da` alone replays `I(da) ⋈ I(db)`, which is the maintained view
            // only where each delta joins the other's whole integral.
            if (ia, ib) != (da, db) {
                return Err(UNSUPPORTED.into());
            }
            match integrals[da].expect("the join declared its integrand's integral") {
                seed @ Integral::Own(_) => (regs[da], seed),
                // The table's rows enter where its scan's delta does, ahead of
                // `da`'s re-key, whose integral the store is.
                seed @ Integral::Relation(table) => (sources[&table].reg, seed),
            }
        }
        _ => return Err(UNSUPPORTED.into()),
    };
    Ok(Hydration { entry: vm.replay_entry(in_reg)?, seed })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/hydration.rs"]
mod tests;
