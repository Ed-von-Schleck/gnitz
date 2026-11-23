//! Where a capacity-bounded view's per-key replay seeds: resolved off the
//! circuit graph, then against the plan the emitter produced.

use super::*;
use gnitz_store::relation::RelationKind;

const UNSUPPORTED: &str =
    "capacity-bounded view: only a filter/projection over one relation or over an inner equi-join is supported";

impl EmitCtx<'_> {
    /// A bounded view's per-key replay, as [`Vm::replay`] takes it: the register
    /// it seeds, the integral the seed is gathered from, the clamp the gathered
    /// weights are read through, and the payload columns it reads. The seed is
    /// gathered by the view's own keys: the output's PK region is the seeded
    /// node's, which a row-local walk and an equi join's delta both keep.
    pub(super) fn replay_seed(&self) -> Result<(DeltaReg, Integral, Option<ClampKind>, u64), String> {
        let circuit = self.circuit;
        // A replay runs on one worker, and so no exchange round.
        if circuit.ops().any(|(_, op)| op.exchanges()) {
            return Err(UNSUPPORTED.into());
        }
        // Walk back from the output through nodes that hand a subset of their
        // input's rows on, each on its own worker with the PK region intact, to the
        // first node that does not.
        let mut origin = circuit.out();
        while matches!(circuit.op(origin), OpNode::Filter(_) | OpNode::Map(MapKind::Compute(_))) {
            origin = circuit.inputs(origin)[0];
        }
        let (in_reg, seed, clamp) = match circuit.op(origin) {
            OpNode::ScanDelta { source, .. } => {
                let source_meta = &self.meta.sources[source];
                if source_meta.relation.kind() == RelationKind::Stream {
                    return Err(
                        "capacity-bounded view: a filter/projection over a stream has no stored rows to recompute from"
                            .into(),
                    );
                }
                // The view's rows would sit where the route puts them, and the
                // source's where its store does.
                if source_meta.route.is_some() {
                    return Err(UNSUPPORTED.into());
                }
                (self.regs[origin], Integral::Relation(*source), None)
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
                let (seed, entry, clamp) = self.integrals[da].expect("the join declared its integrand's integral");
                (entry, seed, clamp)
            }
            _ => return Err(UNSUPPORTED.into()),
        };
        // The replay reads of a scanned source's rows what its backfill does.
        let read = match seed {
            Integral::Relation(source) => {
                let (_, scan) = self
                    .sources
                    .iter()
                    .find(|(id, _)| *id == source)
                    .expect("the seed is scanned");
                scan.read
            }
            Integral::Own(_) => u64::MAX,
        };
        Ok((in_reg, seed, clamp, read))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/hydration.rs"]
mod tests;
