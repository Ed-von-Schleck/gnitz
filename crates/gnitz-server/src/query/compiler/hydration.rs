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

/// One term `delta ⋈ z⁻¹I(integrand)` of an equi-join.
struct Term {
    delta: NodeId,
    integrand: NodeId,
    delta_is_right: bool,
}

impl Term {
    fn of(loaded: &LoadedCircuit, join: NodeId) -> Option<Term> {
        use gnitz_wire::{JoinKind, OpNode};
        let OpNode::Join { kind: JoinKind::Equi, delta_is_right } = loaded.op(join) else {
            return None;
        };
        let &[delta, integrand] = loaded.inputs(join) else {
            unreachable!("a join is wired on two inputs")
        };
        Some(Term {
            delta,
            integrand,
            delta_is_right: *delta_is_right,
        })
    }
}

/// The relations `node` reads, directly or transitively.
fn sources(loaded: &LoadedCircuit, node: NodeId) -> Vec<u64> {
    let reached = loaded.ancestors_inclusive(node);
    loaded
        .ops()
        .filter_map(|(n, op)| match op {
            gnitz_wire::OpNode::ScanDelta { source, .. } if reached[n] => Some(*source),
            _ => None,
        })
        .collect()
}

/// Where a bounded view's replay seeds, as [`seed_node`] matched it.
#[derive(Debug, PartialEq)]
enum SeedAt {
    /// The `ScanDelta` of a linear body.
    Scan { node: NodeId, source: u64 },
    /// The integral of `delta`, one delta of an inner equi-join's two-term form.
    Trace { delta: NodeId },
}

fn seed_node(loaded: &LoadedCircuit) -> Result<SeedAt, String> {
    use gnitz_wire::OpNode;
    // One replay seeds one register of one plan; an exchange splits the plan.
    if loaded.exchange_shards().next().is_some() {
        return Err(UNSUPPORTED.into());
    }
    let origin = row_local_origin(loaded, loaded.inputs(loaded.sink()?)[0]);
    match loaded.op(origin) {
        OpNode::ScanDelta { source, .. } => return Ok(SeedAt::Scan { node: origin, source: *source }),
        OpNode::Union => {}
        _ => return Err(UNSUPPORTED.into()),
    }
    let &[j1, j2] = loaded.inputs(origin) else {
        unreachable!("a union is wired on two inputs")
    };
    let (Some(t1), Some(t2)) = (Term::of(loaded, j1), Term::of(loaded, j2)) else {
        return Err(UNSUPPORTED.into());
    };
    // Seeding `t1.delta` alone replays `I(t1.delta) ⋈ I(t2.delta)`, which is the
    // maintained view only for the two-term form of one join.
    let cross_wired = t1.integrand == t2.delta && t2.integrand == t1.delta;
    let one_side_order = t1.delta_is_right != t2.delta_is_right;
    let (s1, s2) = (sources(loaded, t1.delta), sources(loaded, t2.delta));
    let single_source_per_epoch = !s1.iter().any(|s| s2.contains(s));
    if !(cross_wired && one_side_order && single_source_per_epoch) {
        return Err(UNSUPPORTED.into());
    }
    Ok(SeedAt::Trace { delta: t1.delta })
}

/// Resolve the seed against the plan the emitter produced for it.
pub(super) fn derive_hydration(
    loaded: &LoadedCircuit,
    registry: &RelationRegistry,
    view_schema: &SchemaDescriptor,
    plan: &SubPlan,
    regs: &[Option<DeltaReg>],
    integrals: &[Option<Integral>],
) -> Result<Hydration, String> {
    let reg = |n: NodeId| regs[n].expect("an exchange-free plan emits every node");
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
            (node, Integral::Source(source), node)
        }
        SeedAt::Trace { delta } => {
            let seed = integrals[delta].expect("the cross-wired term's join declared its integrand's integral");
            let in_node = match seed {
                Integral::Own(_) => delta,
                // The relation's own rows enter where its delta does, and are
                // re-keyed by `delta`, the reindex whose integral the store is.
                Integral::Source(_) => loaded.inputs(delta)[0],
            };
            (in_node, seed, delta)
        }
    };
    let entry = plan.vm.replay_entry(reg(in_node))?;
    // The view's own keys gather the seed.
    if plan.vm.schema_of(reg(keyed)).pk_stride() != view_schema.pk_stride() {
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
