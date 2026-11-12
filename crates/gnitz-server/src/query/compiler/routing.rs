//! The plan-free facts of a view's circuit: how each source's delta is routed
//! into it, and the placement its store registers under. Nothing here emits.

use std::collections::BTreeMap;

use super::*;

/// Per-view circuit metadata, derived from one circuit load: every worker routes
/// its own partitions by it, so no two workers can route one round differently.
pub(super) struct ViewMeta<'a> {
    /// Each worker computes the view's whole result locally: it is replicated, or
    /// there is one worker.
    pub(super) self_contained: bool,
    /// Every scanned source, by id.
    sources: BTreeMap<u64, &'a Relation>,
    /// source table id → how a worker relays the delta this source scatters by —
    /// a share of it where the source is replicated, a round otherwise. A source
    /// absent from it does not scatter. Derived as beside other workers, whatever
    /// the worker count, so that a key no relay could route by is refused at any.
    source_routes: FxHashMap<u64, Relay>,
    /// Per node, every worker computes the same rows into it: each relation
    /// behind it is replicated and reaches it whole, and nothing on the way trims
    /// or moves its rows — or the view is self-contained.
    whole: Vec<bool>,
}

impl<'a> ViewMeta<'a> {
    /// Derive `circuit`'s routing metadata, and the placement a view of schema
    /// `view` over it registers under.
    pub(super) fn derive(
        circuit: &Circuit,
        registry: &'a RelationRegistry,
        view: &SchemaDescriptor,
    ) -> Result<(ViewMeta<'a>, Placement), String> {
        let uses = source_uses(circuit)?;
        let sources: BTreeMap<u64, &Relation> = uses
            .keys()
            .map(|&tid| Ok((tid, registry.relation_or_err(tid)?)))
            .collect::<Result<_, String>>()?;
        if let Some((tid, _)) = uses.iter().find(|(_, u)| u.reaches_join && u.key.is_none()) {
            return Err(format!("source {tid} feeds a join and states no scatter key"));
        }
        let placement = placement(circuit, &uses, &sources, view);
        let self_contained = placement.is_replicated() || registry.slot().of <= 1;

        let replicated = |tid: u64| sources[&tid].placement().is_replicated();
        let keyed = || uses.iter().filter_map(|(&tid, u)| Some((tid, u, u.key.as_deref()?)));
        // A replicated partner holds every row on every worker, so each match is
        // made once, on the other side's own worker — unless that side's delta is
        // broadcast, or the circuit also reads the replicated delta outside its
        // join terms, which counts it once per worker.
        let has_replicated_partner = keyed().any(|(tid, ..)| replicated(tid))
            && keyed().all(|(tid, u, key)| !key.is_empty() && (!replicated(tid) || !u.outside_join));

        let mut source_routes: FxHashMap<u64, Relay> = FxHashMap::default();
        for (tid, use_, key) in keyed() {
            let source = sources[&tid];
            let partner_makes_every_match = has_replicated_partner
                // A clamp over one worker's slice admits the key once per worker.
                && (replicated(tid) || !use_.set_fed);
            let plan = match key.is_empty() {
                // Every worker already holds the whole delta a broadcast would
                // hand it.
                true if replicated(tid) => continue,
                true => ScatterPlan::broadcast(),
                false => {
                    // Refused whether or not the relay is skipped below.
                    let plan = ScatterPlan::join(&source.schema(), key)
                        .map_err(|e| format!("source {tid} scatter key: {e}"))?;
                    // Only a keyed relay can skip: a broadcast's matches spread
                    // over the whole other side.
                    if partner_makes_every_match || plan.routes_to_native_owner(source.placement()) {
                        continue;
                    }
                    plan
                }
            };
            let relay = Relay { plan: Rc::new(plan), key: key.into() };
            source_routes.insert(tid, relay);
        }
        // A broadcast hands every worker the whole delta; a keyed route, its part.
        // Walked at any worker count, so that its refusals are too.
        let mut whole = whole_nodes(circuit, |tid| match source_routes.get(&tid) {
            Some(relay) => relay.key.is_empty(),
            None => replicated(tid),
        })?;
        if self_contained {
            whole.fill(true);
        }
        let meta = ViewMeta {
            self_contained,
            sources,
            source_routes,
            whole,
        };
        Ok((meta, placement))
    }

    /// The scanned source `source_id`. Panics unless the circuit scans it.
    pub(super) fn source(&self, source_id: u64) -> &'a Relation {
        self.sources[&source_id]
    }

    /// Beside other workers, `source_id`'s delta would not enter the view where
    /// the source's store places it — whatever the worker count.
    pub(super) fn states_route(&self, source_id: u64) -> bool {
        self.source_routes.contains_key(&source_id)
    }

    /// Every worker computes the same rows into `nid`.
    pub(super) fn held_whole(&self, nid: NodeId) -> bool {
        self.whole[nid]
    }

    /// How a worker relays `source_id`'s delta into this view — two views
    /// relaying one source by equal keys relay it by equal plans. `None` when
    /// that source does not scatter: its rows are already where the view needs
    /// them, and a route would name a key they were never stored under, or every
    /// worker computes the view's whole result.
    pub(super) fn source_route(&self, source_id: u64) -> Option<&Relay> {
        self.source_routes.get(&source_id).filter(|_| !self.self_contained)
    }
}

/// Where the rows of a view of schema `view` live, folded from its sources'
/// placements and where its circuit leaves its rows.
fn placement(
    circuit: &Circuit,
    uses: &BTreeMap<u64, SourceUse>,
    sources: &BTreeMap<u64, &Relation>,
    view: &SchemaDescriptor,
) -> Placement {
    // Every worker computes the whole result from its own full copies.
    if sources.values().all(|s| s.placement().is_replicated()) {
        return Placement::Replicated;
    }
    if sources.values().any(|s| !s.placement().is_key_routed()) {
        return Placement::Local;
    }
    let moves = circuit
        .ops()
        .any(|(_, op)| matches!(op, OpNode::Join(_)) || op.exchanges());
    match scanned_row_locally(circuit, circuit.out()) {
        // The view's PK region is this source's, byte for byte, and its rows sit on
        // that source's owner only while nothing relays its delta. Every source
        // stating a scatter key counts as relayed here, even one whose scatter
        // key routes to its native owner: Local is safe then.
        Some(tid) if uses[&tid].key.is_none() => sources[&tid].placement(),
        Some(_) => Placement::Local,
        // Behind a join or an exchange the rows sit on their own key's owner.
        None if moves => Placement::full_pk(view),
        None => Placement::Local,
    }
}

// ---------------------------------------------------------------------------
// Circuit walks
// ---------------------------------------------------------------------------

/// A scatter key: a `(source column, slot type)` slot list, in the order
/// the source's scan states it.
type ScatterKey = Vec<gnitz_wire::ReindexSlot>;

/// What the circuit does with one scanned source's delta.
#[derive(Default)]
struct SourceUse {
    /// The scatter key it states, in the source relation's own column indices;
    /// empty for a broadcast.
    key: Option<ScatterKey>,
    /// The scan's rows reach something other than a join operand.
    outside_join: bool,
    /// They cross a `Distinct`.
    set_fed: bool,
    /// They reach a `Join`, so the source must state a scatter key.
    reaches_join: bool,
}

/// One forward pass over the circuit: what it does with each source's delta, by
/// source in ascending order — so every process reports the same offender.
fn source_uses(circuit: &Circuit) -> Result<BTreeMap<u64, SourceUse>, String> {
    let mut uses: BTreeMap<u64, SourceUse> = BTreeMap::new();
    // Whose scan's delta flows into each node, propagated along the way.
    let mut owner: Vec<Option<u64>> = vec![None; circuit.ops().len()];

    for (nid, op) in circuit.ops() {
        for &p in circuit.inputs(nid) {
            if let Some(tid) = owner[p] {
                note_read(uses.entry(tid).or_default(), Some(op));
            }
        }
        if propagates(op) {
            owner[nid] = owner[circuit.inputs(nid)[0]];
        }
        match op {
            OpNode::ScanDelta { source, scatter, .. } => {
                let use_ = uses.entry(*source).or_default();
                if let Some(stated) = scatter {
                    if use_.key.get_or_insert_with(|| stated.clone()) != stated {
                        return Err(format!(
                            "source {source} is scanned under several distinct scatter keys"
                        ));
                    }
                }
                owner[nid] = Some(*source);
            }
            OpNode::WeightClamp(ClampKind::Distinct) => {
                if let Some(tid) = owner[nid] {
                    uses.entry(tid).or_default().set_fed = true;
                }
            }
            _ => {}
        }
    }
    if let Some(tid) = owner[circuit.out()] {
        note_read(uses.entry(tid).or_default(), None);
    }
    Ok(uses)
}

/// `op` hands its input's rows on as that input's source's delta.
fn propagates(op: &OpNode) -> bool {
    matches!(
        op,
        OpNode::Filter(_) | OpNode::Map(_) | OpNode::WeightClamp(ClampKind::Distinct)
    )
}

/// Record in `use_` that `reader` — `None` for the view's output — reads a
/// node carrying the source's delta.
fn note_read(use_: &mut SourceUse, reader: Option<&OpNode>) {
    match reader {
        Some(OpNode::Join(_)) => use_.reaches_join = true,
        Some(op) if propagates(op) => {}
        _ => use_.outside_join = true,
    }
}

/// One forward pass: per node, whether every worker computes the same rows into
/// it, where `whole_scan` answers for each source's scan. Refuses an exchange whose
/// placement no worker count could serve, so a view's acceptance never depends on
/// the worker count.
fn whole_nodes(circuit: &Circuit, whole_scan: impl Fn(u64) -> bool) -> Result<Vec<bool>, String> {
    let n = circuit.ops().len();
    let mut whole: Vec<bool> = Vec::with_capacity(n);
    // Some node it reads, directly or transitively, owes a ground row.
    let mut grounded: Vec<bool> = Vec::with_capacity(n);
    for (nid, op) in circuit.ops() {
        let inputs = circuit.inputs(nid);
        let behind_ground = inputs.iter().any(|&p| grounded[p]);
        // The ground row is minted in an epoch of its own on one worker,
        // entered behind the reduce, which no other worker runs: a round
        // behind it would be skipped, and a share would keep only the
        // minting worker's part.
        if op.exchanges() && behind_ground {
            return Err("an exchange behind a global aggregate".into());
        }
        grounded.push(behind_ground || circuit.owes_ground_row(nid));
        let own = match op {
            OpNode::ScanDelta { source, .. } => whole_scan(*source),
            _ => !op.exchanges(),
        };
        whole.push(own && inputs.iter().all(|&p| whole[p]));
    }
    Ok(whole)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/routing.rs"]
mod tests;
