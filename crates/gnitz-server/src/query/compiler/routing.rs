//! The plan-free facts of a view's circuit: how each source's delta is routed
//! into it, the placement its store registers under, and the shape facts the
//! worker dispatch and the backfill cursor read. Nothing here emits, so it can
//! be read without compiling.

use super::*;
use gnitz_store::schema::Placement;
use gnitz_wire::ReadBound;

/// How each worker routes its partition of one source's delta into a view.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RelayRoute {
    /// Pure range join (`n_eq == 0`) or cross join: the matches are spread over
    /// the whole other side, so every worker needs the full delta and trims to
    /// its owned slice (`WorkerFilter`) before integrating.
    Broadcast,
    /// Scatter by a join key, already truncated to the routing prefix and
    /// mirroring the trace-side reindex Map slot-for-slot. A band join
    /// (`n_eq >= 1`) routes by the equality prefix alone, dropping the trailing
    /// range slot, so equal eq-values co-partition both sides and the range
    /// probe stays partition-local.
    JoinKey(Box<[gnitz_wire::ReindexSlot]>),
}

impl RelayRoute {
    /// The scatter this route splits a partition by; `None` for a broadcast.
    pub(crate) fn spec(&self) -> Option<ScatterSpec<'_>> {
        match self {
            RelayRoute::Broadcast => None,
            RelayRoute::JoinKey(slots) => Some(ScatterSpec::JoinKey(slots)),
        }
    }
}

/// The source id a round carries when it is not a source's own scatter: a
/// side's relayed output, which routes by the view's own shard columns.
pub(crate) const OUTPUT_RELAY: u64 = 0;

/// Per-view circuit metadata, derived from one circuit load: every worker routes
/// its own partitions by it, so no two workers can route one round differently.
pub(crate) struct ViewMeta {
    /// source table id → how a worker routes the delta this source scatters.
    /// A source absent from it does not scatter.
    source_routes: FxHashMap<u64, RelayRoute>,
    /// The view's own shard columns; its output relay routes each row to the
    /// owner of the output PK `op_reduce` keys each group by.
    output_shard_cols: Box<[u32]>,
    /// The circuit's one `ExchangeShard` is a proven no-op: every row it would
    /// move already sits on the worker owning its distribution key.
    pub(in crate::query) skips_exchange: bool,
    /// source table id → the bound its backfill scan narrows by. Absent for a
    /// source scanned more than once, whose one backfill cursor feeds every scan.
    source_bounds: FxHashMap<u64, ReadBound>,
}

impl ViewMeta {
    /// Derive `loaded`'s routing metadata, and the placement a view of `pk_arity`
    /// PK columns over it registers under.
    pub(in crate::query) fn derive(
        loaded: &LoadedCircuit,
        registry: &RelationRegistry,
        pk_arity: usize,
    ) -> Result<(ViewMeta, Placement), String> {
        let (uses, circuit_relay) = source_uses(loaded)?;
        let schemas = scanned_schemas(&uses, registry)?;
        // The lowest offender, so every process reports the same one.
        if let Some(tid) = uses
            .iter()
            .filter(|(_, u)| u.reaches_join && u.key.is_none())
            .map(|(&tid, _)| tid)
            .min()
        {
            return Err(format!("source {tid} feeds a join and states no scatter key"));
        }
        let source_bounds = uses
            .iter()
            .filter_map(|(&tid, u)| Some((tid, u.bound.clone()?)))
            .filter(|(_, b)| *b != ReadBound::None)
            .collect();

        let shards: Vec<(NodeId, &[u32])> = loaded.exchange_shards().collect();
        let rows = match pk_source(loaded) {
            Some(tid) => RowHome::SourcePk(tid),
            None if !shards.is_empty() || circuit_relay.is_some() => RowHome::OwnKey,
            None => RowHome::Producer,
        };
        let placement = placement(&schemas, rows, pk_arity);
        let (skips_exchange, output_shard_cols): (bool, Box<[u32]>) = match shards[..] {
            [] => (false, Box::default()),
            [(enid, cols)] => (skips_output_exchange(loaded, enid, cols, &schemas), Box::from(cols)),
            [.., (_, cols)] => (false, Box::from(cols)),
        };

        let replicated = |tid: u64| schemas[&tid].placement().is_replicated();
        let keyed = || uses.iter().filter_map(|(&tid, u)| Some((tid, u, u.key.as_deref()?)));
        // A replicated partner holds every row on every worker, so each match is
        // made once, on the other side's own worker — unless the circuit also
        // reads that replicated delta outside its join terms, which counts it
        // once per worker.
        let has_replicated_partner =
            keyed().any(|(tid, ..)| replicated(tid)) && keyed().all(|(tid, u, _)| !replicated(tid) || !u.outside_join);

        let mut source_routes: FxHashMap<u64, RelayRoute> = FxHashMap::default();
        for (tid, use_, key) in keyed() {
            // An owner-trimmed source routes by the whole key it states, because
            // that is the key its filter keeps rows on.
            let relay = match use_.owner_trimmed {
                true => JoinRelay::WholeKey,
                false => circuit_relay.unwrap_or(JoinRelay::WholeKey),
            };
            let route = join_route(key, relay)?;
            let schema = schemas[&tid];
            let partner_makes_every_match = has_replicated_partner
                // An owner filter drops every row the relay did not place.
                && !use_.owner_trimmed
                // A clamp over one worker's slice admits the key once per worker.
                && (replicated(tid) || !use_.set_fed);
            let skips = match &route {
                // A broadcast's matches spread over the whole other side.
                RelayRoute::Broadcast => false,
                RelayRoute::JoinKey(k) => {
                    ScatterSpec::JoinKey(k).routes_to_native_owner(&schema) || partner_makes_every_match
                }
            };
            if skips {
                continue;
            }
            // Every worker scatters by this key mid-round, where a refusal aborts
            // it.
            if let RelayRoute::JoinKey(slots) = &route {
                ScatterSpec::JoinKey(slots)
                    .check(&schema)
                    .map_err(|e| format!("source {tid} scatter key: {e}"))?;
            }
            source_routes.insert(tid, route);
        }

        let meta = ViewMeta {
            source_routes,
            output_shard_cols,
            skips_exchange,
            source_bounds,
        };
        Ok((meta, placement))
    }

    /// How a worker routes `source_id`'s delta into this view, `None`
    /// when that source does not scatter: its rows are already where the view
    /// needs them, and a route would name a key they were never stored under.
    pub(crate) fn source_route(&self, source_id: u64) -> Option<&RelayRoute> {
        self.source_routes.get(&source_id)
    }

    /// The bound `source`'s backfill scan narrows by: `ReadBound::None` unless the
    /// circuit carries one.
    pub(crate) fn source_bound(&self, source: u64) -> ReadBound {
        self.source_bounds.get(&source).cloned().unwrap_or(ReadBound::None)
    }

    /// The columns a side's relayed output is routed by: to the owner of the
    /// output PK `op_reduce` keys each group by.
    pub(crate) fn output_shard_cols(&self) -> &[u32] {
        &self.output_shard_cols
    }
}

/// The relation whose PK region the view's output PK region is, byte for byte.
/// `None` unless a walk back from the sink proves it.
fn pk_source(loaded: &LoadedCircuit) -> Option<u64> {
    loaded
        .sink()
        .ok()
        .and_then(|sink| scan_through_row_local(loaded, sink))
        .map(|(tid, _)| tid)
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

/// The schema of each source `uses` names: every one a scanned, registered
/// relation.
fn scanned_schemas(
    uses: &FxHashMap<u64, SourceUse>,
    registry: &RelationRegistry,
) -> Result<FxHashMap<u64, SchemaDescriptor>, String> {
    let mut ids: Vec<u64> = uses.keys().copied().collect();
    // Ascending, so every process reports the same offender.
    ids.sort_unstable();
    ids.into_iter()
        .map(|tid| {
            if uses[&tid].bound.is_none() {
                return Err(format!("source {tid} states a scatter key but is not scanned"));
            }
            let relation = registry.relation_or_err(tid)?;
            Ok((tid, relation.schema()))
        })
        .collect()
}

/// Where a view's rows live, folded from its sources' placements and where its
/// circuit leaves its `rows`.
fn placement(sources: &FxHashMap<u64, SchemaDescriptor>, rows: RowHome, pk_arity: usize) -> Placement {
    if sources.is_empty() {
        return Placement::KEYED_DEFAULT;
    }
    // Every worker computes the whole result from its own full copies.
    if sources.values().all(|s| s.placement().is_replicated()) {
        return Placement::Replicated;
    }
    if sources.values().any(|s| !s.placement().is_key_routed()) {
        return Placement::Local;
    }
    let mut only = sources.iter();
    let (Some((&src, schema)), None) = (only.next(), only.next()) else {
        return Placement::KEYED_DEFAULT;
    };
    match rows {
        RowHome::OwnKey => Placement::KEYED_DEFAULT,
        RowHome::SourcePk(tid) if tid == src && schema.pk_indices().len() == pk_arity => schema.placement(),
        RowHome::SourcePk(_) | RowHome::Producer => Placement::Local,
    }
}

/// The route a source carrying a reindex key takes. `key` is that key,
/// `(column, promotion target)` per slot, in trace-side reindex order.
fn join_route(key: &[gnitz_wire::ReindexSlot], relay: JoinRelay) -> Result<RelayRoute, String> {
    let route_len = match relay {
        JoinRelay::Broadcast => return Ok(RelayRoute::Broadcast),
        JoinRelay::WholeKey => key.len(),
        // A circuit is client-supplied, so a wider `n_eq` is refused, not sliced.
        JoinRelay::EqPrefix { n_eq } if key.len() == n_eq as usize + 1 => n_eq as usize,
        JoinRelay::EqPrefix { .. } => {
            return Err("band join: n_eq does not match the source's reindex key arity".into())
        }
    };
    Ok(RelayRoute::JoinKey(Box::from(&key[..route_len])))
}

/// True iff the view's output `ExchangeShard` at `enid` would move nothing: it
/// shards by a key its scan's own distribution already routes rows to.
fn skips_output_exchange(
    loaded: &LoadedCircuit,
    enid: NodeId,
    shard_cols: &[u32],
    schemas: &FxHashMap<u64, SchemaDescriptor>,
) -> bool {
    let Some((tid, mapped)) = scan_through_row_local(loaded, enid) else {
        return false;
    };
    let schema = schemas[&tid];
    // Behind a map, shard column `c` is source PK column `c`, or a payload slot.
    let source_cols: Option<Vec<u32>> = match mapped {
        false => Some(shard_cols.to_vec()),
        true => shard_cols
            .iter()
            .map(|&c| schema.pk_indices().get(c as usize).copied())
            .collect(),
    };
    source_cols.is_some_and(|cols| ScatterSpec::GroupKey(&cols).routes_to_native_owner(&schema))
}

// ---------------------------------------------------------------------------
// Circuit walks
// ---------------------------------------------------------------------------

/// One reindex key: a `(source column, carried promotion target)` slot list, in
/// the trace-side `ReindexPacker`'s own order.
type ReindexKey = Vec<gnitz_wire::ReindexSlot>;

/// What the circuit does with one source's delta. Entries come from both
/// `ScanDelta` nodes and `ScatterKey` reindex Maps, which name a source
/// independently — neither implies the other.
struct SourceUse {
    /// The scatter key it states, in the source relation's own column indices.
    key: Option<ReindexKey>,
    /// Its backfill scan's bound: `None` until a `ScanDelta` names the source,
    /// and `ReadBound::None` once a second does — one cursor feeds every scan.
    bound: Option<ReadBound>,
    /// Every direct reader of every `ScatterKey` Map naming this source is a
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

/// One forward pass over the circuit: what it does with each source's delta, and
/// the relay its joins call for — `None` for a circuit without a join.
fn source_uses(loaded: &LoadedCircuit) -> Result<(FxHashMap<u64, SourceUse>, Option<JoinRelay>), String> {
    use gnitz_wire::{MapKind, OpNode, ReindexRole};

    let mut uses: FxHashMap<u64, SourceUse> = FxHashMap::default();
    let mut circuit_relay: Option<JoinRelay> = None;
    // Whose scan's delta flows into each node — propagated along the way — and
    // which node IS that source's `ScatterKey` Map, read by its readers alone.
    let mut owner: Vec<Option<u64>> = vec![None; loaded.len()];
    let mut states_key: Vec<Option<u64>> = vec![None; loaded.len()];

    for (nid, op) in loaded.ops() {
        let propagates = matches!(
            op,
            OpNode::Filter(_) | OpNode::Map(_) | OpNode::IntegrateTrace | OpNode::Distinct
        );
        for p in loaded.inputs(nid).iter() {
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
            owner[nid] = owner[loaded.inputs(nid).unary()];
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
                role: ReindexRole::ScatterKey { source, source_key },
                ..
            }) => {
                let tid = *source;
                let key = uses
                    .entry(tid)
                    .or_default()
                    .key
                    .get_or_insert_with(|| source_key.clone());
                if key != source_key {
                    return Err(format!("source {source} feeds several distinct scatter keys"));
                }
                states_key[nid] = Some(tid);
            }
            OpNode::Distinct => {
                if let Some(tid) = owner[nid] {
                    uses.entry(tid).or_default().set_fed = true;
                }
            }
            OpNode::Join { kind, .. } => {
                let this = relay_of(*kind);
                if circuit_relay.is_some_and(|r| r != this) {
                    return Err("circuit joins need different relays".into());
                }
                circuit_relay = Some(this);
            }
            _ => {}
        }
    }
    Ok((uses, circuit_relay))
}

/// The `ScanDelta` the row-local walk back from `enid`'s input ends at: `(table
/// id, whether a Map was crossed)`.
fn scan_through_row_local(loaded: &LoadedCircuit, enid: NodeId) -> Option<(u64, bool)> {
    let (origin, mapped) = row_local_origin(loaded, loaded.inputs(enid).unary());
    match loaded.op(origin) {
        gnitz_wire::OpNode::ScanDelta { source, .. } => Some(((*source), mapped)),
        _ => None,
    }
}

/// How a view's join, if it has one, needs its inputs placed — what a worker
/// routes a source's delta by, and whether any source can skip the scatter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinRelay {
    /// An equi join, whose matches share a key — as a GROUP BY's groups do.
    WholeKey,
    /// A band join, whose matches share the `n_eq` leading equality slots.
    EqPrefix { n_eq: u8 },
    /// A pure range or cross join, whose matches share nothing.
    Broadcast,
}

/// The relay one `Join` node's kind calls for.
fn relay_of(kind: gnitz_wire::JoinKind) -> JoinRelay {
    use gnitz_wire::JoinKind;
    match kind {
        JoinKind::Equi => JoinRelay::WholeKey,
        JoinKind::Range { n_eq: 0, .. } | JoinKind::Cross => JoinRelay::Broadcast,
        JoinKind::Range { n_eq, .. } => JoinRelay::EqPrefix { n_eq },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/routing.rs"]
mod tests;
