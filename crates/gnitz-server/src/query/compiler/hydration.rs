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

/// Where a bounded view's replay seeds, as [`seed_node`] matched it.
#[derive(Debug, PartialEq)]
enum SeedAt {
    /// The `ScanDelta` of a linear body.
    Scan { node: NodeId, source: u64 },
    /// The integral of `delta`, side A's delta of an inner equi-join.
    Trace { delta: NodeId },
}

fn seed_node(loaded: &LoadedCircuit) -> Result<SeedAt, String> {
    use gnitz_wire::OpNode;
    // A replay runs on one worker, and so no exchange round.
    if loaded.exchange_shards().next().is_some() {
        return Err(UNSUPPORTED.into());
    }
    let origin = row_local_origin(loaded, loaded.out());
    match loaded.op(origin) {
        OpNode::ScanDelta { source, .. } => return Ok(SeedAt::Scan { node: origin, source: *source }),
        OpNode::Join { kind: gnitz_wire::JoinKind::Equi } => {}
        _ => return Err(UNSUPPORTED.into()),
    }
    let &[da, db, ia, ib] = loaded.inputs(origin) else {
        unreachable!("a join is wired on four inputs")
    };
    // Seeding `da` alone replays `I(da) ⋈ I(db)`, which is the maintained view
    // only where each delta joins the other's whole integral.
    if (ia, ib) != (da, db) {
        return Err(UNSUPPORTED.into());
    }
    Ok(SeedAt::Trace { delta: da })
}

/// Resolve the seed against the plan the emitter produced for it.
pub(super) fn derive_hydration(
    loaded: &LoadedCircuit,
    registry: &RelationRegistry,
    view_schema: &SchemaDescriptor,
    vm: &Vm,
    regs: &[DeltaReg],
    integrals: &[Option<Integral>],
) -> Result<Hydration, String> {
    let reg = |n: NodeId| regs[n];
    let (in_node, seed, keyed) = match seed_node(loaded)? {
        SeedAt::Scan { node, source } => {
            if registry
                .relation(source)
                .is_some_and(|r| r.kind() == RelationKind::Stream)
            {
                return Err(
                    "capacity-bounded view: a filter/projection over a stream has no stored rows to recompute from"
                        .into(),
                );
            }
            (
                node,
                Integral::Relation(source, gnitz_store::relation::Cut::Sealed),
                node,
            )
        }
        SeedAt::Trace { delta } => {
            let seed = integrals[delta].expect("the join declared its integrand's integral");
            let in_node = match seed {
                Integral::Own(_) => delta,
                // The relation's own rows enter where its delta does, and are
                // re-keyed by `delta`, the reindex whose integral the store is.
                Integral::Relation(..) => loaded.inputs(delta)[0],
            };
            (in_node, seed, delta)
        }
    };
    let entry = vm.replay_entry(reg(in_node))?;
    // The view's own keys gather the seed.
    if vm.schema_of(reg(keyed)).pk_stride() != view_schema.pk_stride() {
        return Err(UNSUPPORTED.into());
    }
    Ok(Hydration { entry, seed })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/hydration.rs"]
mod tests;
