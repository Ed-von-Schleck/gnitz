//! Where a capacity-bounded view's per-key replay seeds: resolved off the
//! circuit graph, then against the plan the emitter produced.

use super::*;
use gnitz_store::relation::RelationKind;

const UNSUPPORTED: &str =
    "capacity-bounded view: only a filter/projection over one relation or over an inner equi-join is supported";

const READS_OWN_STORE: &str =
    "capacity-bounded view: a reduce as its output would read the view's skeleton rows back as its trace";

/// Walk back from `from` through nodes that hand a subset of their input's
/// rows on, each on its own worker with the PK region intact, to the first
/// node that does not.
fn row_local_origin(circuit: &Circuit, mut from: NodeId) -> NodeId {
    while matches!(circuit.op(from), OpNode::Filter(_) | OpNode::Map(MapKind::Compute(_))) {
        from = circuit.inputs(from)[0];
    }
    from
}

/// A bounded view's per-key replay: the register it seeds, the integral the
/// seed is gathered from, and the clamp the gathered weights are read through,
/// resolved against the plan the emitter produced. The seed is
/// gathered by the view's own keys: the output's PK region is the seeded node's,
/// which a row-local walk and an equi join's delta both keep.
pub(super) fn derive_hydration(
    circuit: &Circuit,
    meta: &ViewMeta,
    code: &CompileOutput,
    regs: &[DeltaReg],
    integrals: &[Option<(Integral, DeltaReg, Option<ClampKind>)>],
) -> Result<Replay, String> {
    if code.vm.reads_view_store() {
        return Err(READS_OWN_STORE.into());
    }
    // A replay runs on one worker, and so no exchange round.
    if circuit.ops().any(|(_, op)| op.exchanges()) {
        return Err(UNSUPPORTED.into());
    }
    let origin = row_local_origin(circuit, circuit.out());
    let (in_reg, seed, clamp) = match circuit.op(origin) {
        OpNode::ScanDelta { source, .. } => {
            if meta.source(*source).kind() == RelationKind::Stream {
                return Err(
                    "capacity-bounded view: a filter/projection over a stream has no stored rows to recompute from"
                        .into(),
                );
            }
            // The view's rows would sit where the route puts them, and the
            // source's where its store does.
            if meta.states_route(*source) {
                return Err(UNSUPPORTED.into());
            }
            (regs[origin], Integral::Relation(*source), None)
        }
        OpNode::Join(JoinKind::Equi) => {
            let &[da, db, ia, ib] = circuit.inputs(origin) else {
                unreachable!("a join is wired on four inputs")
            };
            // Seeding `da` alone replays `I(da) ⋈ I(db)`, which is the maintained view
            // only where each delta joins the other's whole integral.
            if (ia, ib) != (da, db) {
                return Err(UNSUPPORTED.into());
            }
            let (seed, entry, clamp) = integrals[da].expect("the join declared its integrand's integral");
            (entry, seed, clamp)
        }
        _ => return Err(UNSUPPORTED.into()),
    };
    // The replay reads of a scanned source's rows what its backfill does.
    let read = match seed {
        Integral::Relation(source) => code.scan(source).read,
        Integral::Own(_) => u64::MAX,
    };
    code.vm.replay(in_reg, seed, clamp, read)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/hydration.rs"]
mod tests;
