//! The plan-free facts of a view's circuit: how each source's delta is routed
//! into it, the bound its backfill scan narrows by, and the placement its store
//! registers under. Nothing here emits, so it can be read without compiling.

use std::collections::BTreeMap;
use std::rc::Rc;

use super::*;
use gnitz_expr::ColumnTable;
use gnitz_wire::{ClampKind, ReadBound};
use gnitz_zset::schema::Placement;

/// How a batch reaches the workers that consume it.
#[derive(Clone)]
pub(in crate::query) enum Relay {
    /// One exchange round, each row to the owner the plan names.
    Round(Rc<ScatterPlan>),
    /// Every worker already holds the batch whole and keeps its own share; no round.
    Share(Rc<ScatterPlan>),
}

/// Per-view circuit metadata, derived from one circuit load: every worker routes
/// its own partitions by it, so no two workers can route one round differently.
pub(in crate::query) struct ViewMeta {
    /// This worker computes the view's whole result locally: it is replicated, or
    /// this process is the only worker.
    pub(in crate::query) self_contained: bool,
    /// source table id → how a worker relays the delta this source scatters.
    /// A source absent from it does not scatter; a self-contained view has none.
    source_routes: FxHashMap<u64, Relay>,
    /// source table id → the bound its backfill scan narrows by. Absent for a
    /// source scanned more than once, whose one backfill cursor feeds every scan.
    source_bounds: FxHashMap<u64, ReadBound>,
}

impl ViewMeta {
    /// Derive `loaded`'s routing metadata, and the placement a view of schema
    /// `view` over it registers under.
    pub(in crate::query) fn derive(
        loaded: &LoadedCircuit,
        registry: &RelationRegistry,
        view: &SchemaDescriptor,
    ) -> Result<(ViewMeta, Placement), String> {
        let uses = source_uses(loaded)?;
        let sources = scanned_relations(&uses, registry)?;
        if let Some((tid, _)) = uses.iter().find(|(_, u)| u.reaches_join && u.key.is_none()) {
            return Err(format!("source {tid} feeds a join and states no scatter key"));
        }
        let source_bounds = uses
            .iter()
            .filter_map(|(&tid, u)| Some((tid, u.bound.clone()?)))
            .filter(|(_, b)| *b != ReadBound::None)
            .collect();

        let has_join = loaded
            .ops()
            .any(|(_, op)| matches!(op, gnitz_wire::OpNode::Join { .. }));
        let rows = match pk_source(loaded) {
            // Its rows sit on their source's owner only while nothing relays that
            // source's delta, and only a source stating a scatter key is relayed.
            Some(tid) if uses[&tid].key.is_none() => RowHome::SourcePk(tid),
            Some(_) => RowHome::Producer,
            None if has_join || loaded.exchange_shards().next().is_some() => RowHome::OwnKey,
            None => RowHome::Producer,
        };
        let placement = placement(&sources, rows, view);
        let self_contained = placement.is_replicated() || registry.slot().of <= 1;

        let replicated = |tid: u64| sources[&tid].placement().is_replicated();
        let keyed = || uses.iter().filter_map(|(&tid, u)| Some((tid, u, u.key.as_deref()?)));
        // A replicated partner holds every row on every worker, so each match is
        // made once, on the other side's own worker — unless the circuit also
        // reads that replicated delta outside its join terms, which counts it
        // once per worker.
        let has_replicated_partner =
            keyed().any(|(tid, ..)| replicated(tid)) && keyed().all(|(tid, u, _)| !replicated(tid) || !u.outside_join);

        let mut source_routes: FxHashMap<u64, Relay> = FxHashMap::default();
        for (tid, use_, key) in keyed() {
            let source = sources[&tid];
            let partner_makes_every_match = has_replicated_partner
                // An owner filter drops every row the relay did not place.
                && !use_.owner_trimmed
                // A clamp over one worker's slice admits the key once per worker.
                && (replicated(tid) || !use_.set_fed);
            let relay = match key.is_empty() {
                // Every worker already holds the whole delta a broadcast would
                // hand it.
                true if replicated(tid) => continue,
                true => Relay::Round(Rc::new(ScatterPlan::broadcast())),
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
                    match replicated(tid) {
                        true => Relay::Share(Rc::new(plan)),
                        false => Relay::Round(Rc::new(plan)),
                    }
                }
            };
            source_routes.insert(tid, relay);
        }
        // Kept only beside other workers; the loop above refuses a key no relay
        // could route by at any worker count.
        if self_contained {
            source_routes.clear();
        }

        let meta = ViewMeta {
            self_contained,
            source_routes,
            source_bounds,
        };
        Ok((meta, placement))
    }

    /// How a worker relays `source_id`'s delta into this view, `None`
    /// when that source does not scatter: its rows are already where the view
    /// needs them, and a route would name a key they were never stored under.
    pub(in crate::query) fn source_route(&self, source_id: u64) -> Option<&Relay> {
        self.source_routes.get(&source_id)
    }

    /// The bound `source`'s backfill scan narrows by: `ReadBound::None` unless the
    /// circuit carries one.
    pub(in crate::query) fn source_bound(&self, source: u64) -> ReadBound {
        self.source_bounds.get(&source).cloned().unwrap_or(ReadBound::None)
    }
}

/// The relation whose PK region the view's output PK region is, byte for byte.
/// `None` unless a walk back from the output proves it.
fn pk_source(loaded: &LoadedCircuit) -> Option<u64> {
    scanned_row_locally(loaded, loaded.out())
}

/// The key whose owner a view's rows sit on.
enum RowHome {
    /// The PK region of this source, byte for byte.
    SourcePk(u64),
    /// The view's own key.
    OwnKey,
    /// None: they stay on the worker that produced them.
    Producer,
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
/// placements and where its circuit leaves its `rows`.
fn placement(sources: &BTreeMap<u64, &Relation>, rows: RowHome, view: &SchemaDescriptor) -> Placement {
    // Every worker computes the whole result from its own full copies.
    if sources.values().all(|s| s.placement().is_replicated()) {
        return Placement::Replicated;
    }
    if sources.values().any(|s| !s.placement().is_key_routed()) {
        return Placement::Local;
    }
    match rows {
        RowHome::OwnKey => Placement::full_pk(view),
        RowHome::SourcePk(tid) if sources[&tid].schema().pk_cols().len() == view.pk_cols().len() => {
            sources[&tid].placement()
        }
        RowHome::SourcePk(_) | RowHome::Producer => Placement::Local,
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
    /// Its backfill scan's bound, `ReadBound::None` once a second `ScanDelta`
    /// names the source — one cursor feeds every scan.
    bound: Option<ReadBound>,
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
            bound: None,
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
fn source_uses(loaded: &LoadedCircuit) -> Result<BTreeMap<u64, SourceUse>, String> {
    use gnitz_wire::{MapKind, OpNode, ReindexRole};

    let mut uses: BTreeMap<u64, SourceUse> = BTreeMap::new();
    // Whose scan's delta flows into each node — propagated along the way — and
    // which node IS that source's `ScatterKey` Map, read by its readers alone.
    let mut owner: Vec<Option<u64>> = vec![None; loaded.len()];
    let mut states_key: Vec<Option<u64>> = vec![None; loaded.len()];

    for (nid, op) in loaded.ops() {
        let propagates = matches!(
            op,
            OpNode::Filter(_) | OpNode::Map(_) | OpNode::WeightClamp(ClampKind::Distinct)
        );
        for &p in loaded.inputs(nid) {
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
            owner[nid] = owner[loaded.inputs(nid)[0]];
        }
        match op {
            OpNode::ScanDelta { source, bound } => {
                let tid = *source;
                let use_ = uses.entry(tid).or_default();
                use_.bound = Some(match use_.bound {
                    None => bound.clone(),
                    Some(_) => ReadBound::None,
                });
                owner[nid] = Some(tid);
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
    if let Some(tid) = owner[loaded.out()] {
        uses.entry(tid).or_default().outside_join = true;
    }
    if let Some(tid) = states_key[loaded.out()] {
        uses.entry(tid).or_default().owner_trimmed = false;
    }
    Ok(uses)
}

/// The source of the `ScanDelta` the row-local walk back from `from` ends at.
fn scanned_row_locally(loaded: &LoadedCircuit, from: NodeId) -> Option<u64> {
    match loaded.op(row_local_origin(loaded, from)) {
        gnitz_wire::OpNode::ScanDelta { source, .. } => Some(*source),
        _ => None,
    }
}

/// True when the view's output `ExchangeShard` at `enid` moves nothing:
/// `scatter`, the plan over the shard's input, hashes the bytes the scan behind
/// it placed its rows by. The row-local walk back to that scan carries the PK
/// region through, so a prefix of the input's PK is that prefix of the scan's.
pub(super) fn skips_output_exchange(
    loaded: &LoadedCircuit,
    enid: NodeId,
    scatter: &ScatterPlan,
    registry: &RelationRegistry,
) -> bool {
    scanned_row_locally(loaded, loaded.inputs(enid)[0])
        .and_then(|tid| registry.relation(tid))
        .is_some_and(|source| scatter.routes_to_native_owner(source.placement()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/routing.rs"]
mod tests;
