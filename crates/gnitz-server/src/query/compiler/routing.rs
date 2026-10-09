//! The plan-free facts of a view's circuit: how each source's delta is routed
//! into it, and the placement its store registers under. Nothing here emits.

use std::collections::BTreeMap;
use std::rc::Rc;

use super::*;
use gnitz_wire::ClampKind;
use gnitz_zset::algebra::Placement;

/// Per-view circuit metadata, derived from one circuit load: every worker routes
/// its own partitions by it, so no two workers can route one round differently.
pub(super) struct ViewMeta {
    /// Each worker computes the view's whole result locally: it is replicated, or
    /// there is one worker.
    pub(super) self_contained: bool,
    /// source table id → the plan a worker relays the delta this source scatters
    /// by: a share of it where the source is replicated, a round otherwise.
    /// A source absent from it does not scatter; a self-contained view has none.
    source_routes: FxHashMap<u64, Rc<ScatterPlan>>,
}

impl ViewMeta {
    /// Derive `circuit`'s routing metadata, and the placement a view of schema
    /// `view` over it registers under.
    pub(super) fn derive(
        circuit: &Circuit,
        registry: &RelationRegistry,
        view: &SchemaDescriptor,
    ) -> Result<(ViewMeta, Placement), String> {
        let uses = source_uses(circuit)?;
        let sources = scanned_relations(&uses, registry)?;
        if let Some((tid, _)) = uses.iter().find(|(_, u)| u.reaches_join && u.key.is_none()) {
            return Err(format!("source {tid} feeds a join and states no scatter key"));
        }
        let placement = placement(circuit, &uses, &sources, view);
        let self_contained = placement.is_replicated() || registry.slot().of <= 1;

        let replicated = |tid: u64| sources[&tid].placement().is_replicated();
        let keyed = || uses.iter().filter_map(|(&tid, u)| Some((tid, u, u.key.as_deref()?)));
        // A replicated partner holds every row on every worker, so each match is
        // made once, on the other side's own worker — unless the circuit also
        // reads that replicated delta outside its join terms, which counts it
        // once per worker.
        let has_replicated_partner =
            keyed().any(|(tid, ..)| replicated(tid)) && keyed().all(|(tid, u, _)| !replicated(tid) || !u.outside_join);

        let mut source_routes: FxHashMap<u64, Rc<ScatterPlan>> = FxHashMap::default();
        for (tid, use_, key) in keyed() {
            let source = sources[&tid];
            let partner_makes_every_match = has_replicated_partner
                // An owner filter drops every row the relay did not place.
                && !use_.owner_trimmed
                // A clamp over one worker's slice admits the key once per worker.
                && (replicated(tid) || !use_.set_fed);
            let plan = match key.is_empty() {
                // Every worker already holds the whole delta a broadcast would
                // hand it.
                true if replicated(tid) => continue,
                true => ScatterPlan::broadcast(),
                false => {
                    // Only a keyed relay can skip: a broadcast's matches spread
                    // over the whole other side.
                    if partner_makes_every_match {
                        continue;
                    }
                    let plan = ScatterPlan::join(&source.schema(), key)
                        .map_err(|e| format!("source {tid} scatter key: {e}"))?;
                    if plan.routes_to_native_owner(source.placement()) {
                        continue;
                    }
                    plan
                }
            };
            source_routes.insert(tid, Rc::new(plan));
        }
        // Kept only beside other workers; the loop above refuses a key no relay
        // could route by at any worker count.
        if self_contained {
            source_routes.clear();
        }

        Ok((ViewMeta { self_contained, source_routes }, placement))
    }

    /// The plan a worker relays `source_id`'s delta into this view by, `None`
    /// when that source does not scatter: its rows are already where the view
    /// needs them, and a route would name a key they were never stored under.
    pub(super) fn source_route(&self, source_id: u64) -> Option<&Rc<ScatterPlan>> {
        self.source_routes.get(&source_id)
    }
}

/// Each source `uses` names: every one a registered relation.
fn scanned_relations<'r>(
    uses: &BTreeMap<u64, SourceUse>,
    registry: &'r RelationRegistry,
) -> Result<BTreeMap<u64, &'r Relation>, String> {
    uses.keys()
        .map(|&tid| Ok((tid, registry.relation_or_err(tid)?)))
        .collect()
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
        .any(|(_, op)| matches!(op, gnitz_wire::OpNode::Join { .. } | gnitz_wire::OpNode::ExchangeShard));
    match scanned_row_locally(circuit, circuit.out()) {
        // The view's PK region is this source's, byte for byte, and its rows sit on
        // that source's owner only while nothing relays its delta: only a source
        // stating a scatter key is relayed.
        Some(tid) if uses[&tid].key.is_none() && sources[&tid].schema().pk_cols().len() == view.pk_cols().len() => {
            sources[&tid].placement()
        }
        Some(_) => Placement::Local,
        // Behind a join or an exchange the rows sit on their own key's owner.
        None if moves => Placement::full_pk(view),
        None => Placement::Local,
    }
}

// ---------------------------------------------------------------------------
// Circuit walks
// ---------------------------------------------------------------------------

/// One reindex key: a `(source column, slot type)` slot list, in
/// the trace-side `ReindexPacker`'s own order.
type ReindexKey = Vec<gnitz_wire::ReindexSlot>;

/// What the circuit does with one scanned source's delta.
struct SourceUse {
    /// The scatter key it states, in the source relation's own column indices;
    /// empty for a broadcast.
    key: Option<ReindexKey>,
    /// Every direct reader of every `ScatterKey` Map over this source is a
    /// `WorkerFilter`, which keeps only rows already on their PK's owner.
    owner_trimmed: bool,
    /// The scan's rows reach something other than a join operand.
    outside_join: bool,
    /// They cross a `Distinct` on the way to a join.
    set_fed: bool,
    /// They reach a `Join`, so the source must state a scatter key.
    reaches_join: bool,
}

impl Default for SourceUse {
    fn default() -> SourceUse {
        SourceUse {
            key: None,
            // Vacuously true until a reader that is not a `WorkerFilter` appears.
            owner_trimmed: true,
            outside_join: false,
            set_fed: false,
            reaches_join: false,
        }
    }
}

/// One forward pass over the circuit: what it does with each source's delta, by
/// source in ascending order — so every process reports the same offender.
fn source_uses(circuit: &Circuit) -> Result<BTreeMap<u64, SourceUse>, String> {
    use gnitz_wire::{MapKind, OpNode, ReindexRole};

    let mut uses: BTreeMap<u64, SourceUse> = BTreeMap::new();
    // Whose scan's delta flows into each node — propagated along the way — and
    // which node IS that source's `ScatterKey` Map, read by its readers alone.
    let mut owner: Vec<Option<u64>> = vec![None; circuit.nodes().len()];
    let mut states_key: Vec<Option<u64>> = vec![None; circuit.nodes().len()];

    for (nid, op) in circuit.ops() {
        let propagates = matches!(
            op,
            OpNode::Filter(_) | OpNode::Map(_) | OpNode::WeightClamp(ClampKind::Distinct)
        );
        for &p in circuit.inputs(nid) {
            if let Some(tid) = owner[p] {
                let use_ = uses.entry(tid).or_default();
                match op {
                    OpNode::Join { .. } => use_.reaches_join = true,
                    _ if propagates => {}
                    _ => use_.outside_join = true,
                }
            }
            if let Some(tid) = states_key[p].filter(|_| !matches!(op, OpNode::WorkerFilter)) {
                uses.entry(tid).or_default().owner_trimmed = false;
            }
        }
        if propagates {
            owner[nid] = owner[circuit.inputs(nid)[0]];
        }
        match op {
            OpNode::ScanDelta { source, .. } => {
                uses.entry(*source).or_default();
                owner[nid] = Some(*source);
            }
            OpNode::Map(MapKind::Reindex {
                key,
                role: ReindexRole::ScatterKey { source_cols },
                ..
            }) => {
                // The key is stated in the columns of the source whose scan the
                // node reads, each at its key slot's type.
                let tid = owner[nid].ok_or("a scatter key over no scanned source")?;
                let stated: ReindexKey = source_cols.iter().copied().zip(key.iter().map(|s| s.1)).collect();
                let known = uses.entry(tid).or_default().key.get_or_insert_with(|| stated.clone());
                if *known != stated {
                    return Err(format!("source {tid} feeds several distinct scatter keys"));
                }
                states_key[nid] = Some(tid);
            }
            OpNode::WeightClamp(ClampKind::Distinct) => {
                if let Some(tid) = owner[nid] {
                    uses.entry(tid).or_default().set_fed = true;
                }
            }
            _ => {}
        }
    }
    // The output reads its node as any other reader does.
    if let Some(tid) = owner[circuit.out()] {
        uses.entry(tid).or_default().outside_join = true;
    }
    if let Some(tid) = states_key[circuit.out()] {
        uses.entry(tid).or_default().owner_trimmed = false;
    }
    Ok(uses)
}

/// The source of the `ScanDelta` the row-local walk back from `from` ends at.
fn scanned_row_locally(circuit: &Circuit, from: NodeId) -> Option<u64> {
    match circuit.op(row_local_origin(circuit, from)) {
        gnitz_wire::OpNode::ScanDelta { source, .. } => Some(*source),
        _ => None,
    }
}

/// True when the view's output `ExchangeShard` at `enid` moves nothing:
/// `scatter`, the plan over the shard's input, hashes the bytes the scan behind
/// it placed its rows by. The row-local walk back to that scan carries the PK
/// region through, so a prefix of the input's PK is that prefix of the scan's.
pub(super) fn skips_output_exchange(
    circuit: &Circuit,
    enid: NodeId,
    scatter: &ScatterPlan,
    meta: &ViewMeta,
    registry: &RelationRegistry,
) -> bool {
    scanned_row_locally(circuit, circuit.inputs(enid)[0])
        // A relayed delta no longer sits where its source's store places it.
        .filter(|&tid| meta.source_route(tid).is_none())
        .and_then(|tid| registry.relation(tid))
        .is_some_and(|source| scatter.routes_to_native_owner(source.placement()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/routing.rs"]
mod tests;
