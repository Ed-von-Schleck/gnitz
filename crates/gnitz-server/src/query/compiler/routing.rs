//! The plan-free facts of a view's circuit: how each source's delta is routed
//! into it, and the shape facts the worker dispatch, the placement query and the
//! backfill cursor read. Nothing here emits, so the master can ask without compiling.

use super::*;
use gnitz_store::schema::Placement;
use gnitz_wire::ReadBound;
use rustc_hash::FxHashSet;
use std::collections::hash_map::Entry;

/// How the master relay routes one source's delta into a view.
pub(crate) enum RelayRoute {
    /// Pure range join (`n_eq == 0`) or cross join: the matches are spread over
    /// the whole other side, so every worker needs the full delta and trims to
    /// its owned slice (`WorkerFilter`) before integrating.
    Broadcast,
    /// Scatter by the view's own shard columns, under the null-distinct group
    /// fold `op_reduce` keys its output with.
    GroupKey(Box<[u32]>),
    /// Scatter by a join key, already truncated to the routing prefix and
    /// mirroring the trace-side reindex Map slot-for-slot. A band join
    /// (`n_eq >= 1`) routes by the equality prefix alone, dropping the trailing
    /// range slot, so equal eq-values co-partition both sides and the range
    /// probe stays partition-local.
    JoinKey(Box<[gnitz_wire::ReindexSlot]>),
}

/// source table id → the join/group reindex key its scans feed.
type JoinShardMap = FxHashMap<i64, ReindexKey>;

/// The source id a round carries when it is not a source's own scatter: a
/// side's relayed output, which routes by the view's own shard columns.
pub(crate) const OUTPUT_RELAY: i64 = 0;

/// Per-view circuit metadata, derived from one circuit load — the master's relay
/// key and the worker's scatter set together, so the two processes cannot derive
/// disagreeing halves. `derive` reads the circuit only through
/// [`LoadedCircuit::ops`], which orders identically in both.
pub(crate) struct ViewMeta {
    /// source table id → how the master routes the delta this source scatters.
    /// A source absent from it does not scatter.
    source_routes: FxHashMap<i64, RelayRoute>,
    /// The view's own shard columns, which its output relay routes by.
    output_route: RelayRoute,
    /// The circuit's one `ExchangeShard` is a proven no-op: every row it would
    /// move already sits on the worker owning its distribution key.
    pub(in crate::query) skips_exchange: bool,
    /// The relation whose PK region this view's output PK region is, byte for
    /// byte. `None` unless a walk back from the sink proves it.
    pub(in crate::query) pk_source: Option<i64>,
    /// The circuit moves rows off the worker that produced them: it carries an
    /// output `ExchangeShard`, or a `Join`, whose inputs the runtime join-shard
    /// scatter repartitions without an `ExchangeShard` node.
    exchanges: bool,
    /// source table id → the bound its backfill scan narrows by. Absent for a
    /// source scanned twice, whose one backfill cursor feeds both scans.
    pub(in crate::query) source_bounds: FxHashMap<i64, ReadBound>,
}

impl ViewMeta {
    /// An empty fixture, enough to occupy a memo entry.
    #[cfg(test)]
    pub(in crate::query) fn empty() -> ViewMeta {
        ViewMeta {
            source_routes: FxHashMap::default(),
            output_route: group_key_route(None),
            skips_exchange: false,
            pk_source: None,
            exchanges: false,
            source_bounds: FxHashMap::default(),
        }
    }

    /// Derive `loaded`'s routing metadata. `Err` when the circuit is malformed
    /// or carries an unscatterable source — there is no default route, since a
    /// key nothing was stored under would scatter a delta away from the traces
    /// it must meet.
    ///
    /// `registry` supplies what the circuit alone cannot: co-partitioning and the
    /// output-shard elision both test a shard key against a *source relation's*
    /// distribution prefix. `Err` when a source feeding a join states no route
    /// key, when it states two distinct ones, or when the key does not route
    /// against the relation it names.
    pub(in crate::query) fn derive(loaded: &LoadedCircuit, registry: &RelationRegistry) -> Result<ViewMeta, String> {
        let join_relay = circuit_join_relay(loaded)?;
        let (seqs, untrimmed) = stated_routes(loaded)?;

        let mut outside_joins: FxHashSet<i64> = FxHashSet::default();
        let mut set_fed: FxHashSet<i64> = FxHashSet::default();
        // `ReadBound::None` once a source is seen twice.
        let mut bounds: FxHashMap<i64, ReadBound> = FxHashMap::default();
        for (nid, op) in loaded.ops() {
            let gnitz_wire::OpNode::ScanDelta { source, bound } = op else {
                continue;
            };
            let tid = *source as i64;
            match bounds.entry(tid) {
                Entry::Vacant(e) => {
                    e.insert(bound.clone());
                }
                Entry::Occupied(mut e) => *e.get_mut() = ReadBound::None,
            }
            let consumed = scan_consumption(loaded, nid);
            if !consumed.only_joins {
                outside_joins.insert(tid);
            }
            if consumed.set_fed {
                set_fed.insert(tid);
            }
            if consumed.joined && !seqs.contains_key(&tid) {
                return Err(format!("source {source} feeds a join and states no scatter key"));
            }
        }
        let source_bounds = bounds.into_iter().filter(|(_, b)| *b != ReadBound::None).collect();

        let shards: Vec<(NodeId, &[u32])> = loaded.exchange_shards().collect();
        let exchanges = !shards.is_empty() || join_relay.is_some();
        let pk_source = loaded
            .sink()
            .ok()
            .and_then(|sink| scan_through_row_local(loaded, sink))
            .map(|(tid, _)| tid);
        let join_relay = join_relay.unwrap_or(JoinRelay::WholeKey);
        // Every scatter key of the source is read only by owner filters.
        let owner_trimmed = |tid: &i64| !untrimmed.contains(tid);
        // A range or cross join's matches spread over the whole other side, so
        // no source distribution places them: nothing co-partitions, and every
        // source relays — except an owner-trimmed one already on its PK's owner.
        let co_partitioned = match join_relay {
            JoinRelay::WholeKey => compute_co_partitioned(&seqs, registry, &outside_joins, &set_fed),
            JoinRelay::EqPrefix { .. } => FxHashSet::default(),
            JoinRelay::Broadcast => seqs
                .iter()
                .filter(|&(tid, cols)| owner_trimmed(tid) && native_partition_is(registry, *tid, cols))
                .map(|(&tid, _)| tid)
                .collect(),
        };

        let skips_exchange = match shards[..] {
            [(enid, cols)] => skips_output_exchange(loaded, enid, cols, registry),
            _ => false,
        };
        let shard_cols: Option<Box<[u32]>> = shards.last().map(|&(_, cols)| Box::from(cols));
        let source_routes = seqs
            .into_iter()
            .filter(|(tid, _)| !co_partitioned.contains(tid))
            .map(|(tid, seq)| {
                let route = match join_relay == JoinRelay::Broadcast && owner_trimmed(&tid) {
                    // Routes each row to the worker its owner filter keeps it on.
                    true => join_route(seq, JoinRelay::WholeKey),
                    false => join_route(seq, join_relay),
                }?;
                // The relay scatters by this key mid-round, where a refusal aborts
                // the master.
                if let (RelayRoute::JoinKey(slots), Some(schema)) =
                    (&route, registry.relation(tid).map(Relation::schema))
                {
                    ScatterSpec::JoinKey(slots)
                        .check(&schema)
                        .map_err(|e| format!("source {tid} scatter key: {e}"))?;
                }
                Ok((tid, route))
            })
            .collect::<Result<_, String>>()?;

        Ok(ViewMeta {
            source_routes,
            output_route: group_key_route(shard_cols),
            skips_exchange,
            pk_source,
            exchanges,
            source_bounds,
        })
    }

    /// How the master relay routes `source_id`'s delta into this view, `None`
    /// when that source does not scatter: its rows are already where the view
    /// needs them, and a route would name a key they were never stored under.
    pub(crate) fn source_route(&self, source_id: i64) -> Option<&RelayRoute> {
        self.source_routes.get(&source_id)
    }

    /// How a side's relayed output is routed: by the view's own shard columns.
    pub(crate) fn output_route(&self) -> &RelayRoute {
        &self.output_route
    }

    /// The rows land where the circuit's own exchange put them, rather than
    /// where a source's PK region did.
    pub(in crate::query) fn places_rows_by_own_key(&self) -> bool {
        self.pk_source.is_none() && self.exchanges
    }

    /// True iff `source_id`'s delta must go through the join scatter: it carries a
    /// join/group reindex key, and neither its native distribution matching that
    /// key nor a replicated partner met only through join terms makes the
    /// exchange unnecessary.
    pub(in crate::query) fn scatters(&self, source_id: i64) -> bool {
        self.source_route(source_id).is_some()
    }
}

/// The view's shard columns, consistent with `op_reduce`'s output PK. `None` —
/// no `ExchangeShard` in the circuit — routes by `∅`.
fn group_key_route(shard_cols: Option<Box<[u32]>>) -> RelayRoute {
    RelayRoute::GroupKey(shard_cols.unwrap_or_else(|| Box::from([])))
}

/// The route a source carrying a reindex key takes. `pairs` is that key,
/// `(column, promotion target)` per slot, in trace-side reindex order.
fn join_route(pairs: ReindexKey, relay: JoinRelay) -> Result<RelayRoute, String> {
    let route_len = match relay {
        JoinRelay::Broadcast => return Ok(RelayRoute::Broadcast),
        JoinRelay::WholeKey => pairs.len(),
        // A band join's reindex key is `[eq…, range]`; a circuit is client-supplied,
        // so a wider `n_eq` is refused rather than sliced.
        JoinRelay::EqPrefix { n_eq } if pairs.len() == n_eq as usize + 1 => n_eq as usize,
        JoinRelay::EqPrefix { .. } => {
            return Err("band join: n_eq does not match the source's reindex key arity".into())
        }
    };
    Ok(RelayRoute::JoinKey(Box::from(&pairs[..route_len])))
}

/// True iff `cols` is **exactly** `schema`'s distribution prefix, so a derived
/// operator keyed by it co-partitions with the relation and its exchange can be
/// skipped. Rows of a non-`Keyed` relation are not placed by `worker_for_pk` at
/// all, so no shard key names where they already are.
///
/// Exact, never a super-prefix: a super-prefix would let the two sides of a join
/// skip at different widths, hashing equal keys to different workers and
/// silently dropping matches.
fn shard_cols_match_dist_key(schema: &SchemaDescriptor, cols: &[u32]) -> bool {
    let Placement::Keyed { prefix_len } = schema.placement() else {
        return false;
    };
    let k = prefix_len as usize;
    cols.len() == k && cols == &schema.pk_indices()[..k]
}

/// True iff `tid`'s rows already sit on the worker `key` routes them to: the key is
/// exactly its distribution prefix, unpromoted.
fn native_partition_is(registry: &RelationRegistry, tid: i64, key: &ReindexKey) -> bool {
    // A carried target means the slot is wider than the source column, so
    // native PK partitions do not align with the T-width trace key. Partner of
    // `ScatterKey::new`'s own no-target gate: relaxing one alone would let a
    // promoted side skip while its partner scatters at the wider `T`.
    if key.iter().any(|(_, t)| t.is_some()) {
        return false;
    }
    let cols: Vec<u32> = key.iter().map(|&(c, _)| c).collect();
    registry
        .relation(tid)
        .map(Relation::schema)
        .is_some_and(|schema| shard_cols_match_dist_key(&schema, &cols))
}

/// The sources whose delta may skip the join scatter. `outside_joins` holds the
/// sources the circuit also consumes other than as a join operand; `set_fed` those
/// it clamps to a set before a join reads them.
fn compute_co_partitioned(
    join_shard_map: &JoinShardMap,
    registry: &RelationRegistry,
    outside_joins: &FxHashSet<i64>,
    set_fed: &FxHashSet<i64>,
) -> FxHashSet<i64> {
    // A replicated participant met only through join terms holds every row on
    // every worker, so it joins its partner wherever the partner's rows are, and
    // each match is made once, on the partner's worker. A replicated delta the
    // circuit also uses directly would count once per worker, so it withdraws the
    // skip. A partitioned source clamped to a set scatters by its key regardless:
    // a clamp over one worker's slice of a key admits the key once per worker. Not folded into
    // `shard_cols_match_dist_key`, which must stay the pure prefix predicate for
    // the partner-less skip path.
    let replicated = |tid: i64| {
        registry
            .relation(tid)
            .is_some_and(|r| r.schema().placement().is_replicated())
    };
    let partner_local = join_shard_map.keys().any(|&tid| replicated(tid))
        && join_shard_map
            .keys()
            .all(|tid| !replicated(*tid) || !outside_joins.contains(tid));
    join_shard_map
        .iter()
        .filter(|&(&tid, cols)| {
            registry.relation(tid).is_some()
                && ((partner_local && (replicated(tid) || !set_fed.contains(&tid)))
                    || native_partition_is(registry, tid, cols))
        })
        .map(|(&tid, _)| tid)
        .collect()
}

/// True iff a pipeline keyed by `shard_cols` re-emits rows onto the worker that
/// already owned them, so eliding the shard also elides the output IPC.
///
/// Only at the two ends of the key width. At one column the output key hashes to
/// the same `widen_pk_be` value the prefix does; at the full PK the output
/// re-emits that PK verbatim. In between, a multi-column key streams into an
/// Xxh3 fold unrelated to the prefix's, so the output lands elsewhere and the
/// addressing store loses it — a `CLUSTER BY` prefix of 2+ proper-prefix columns
/// therefore buys no locality for a `GROUP BY` on it.
fn output_route_returns_to_the_input_partition(shard_cols: &[u32], schema: &SchemaDescriptor) -> bool {
    shard_cols.len() <= 1 || shard_cols.len() == schema.pk_indices().len()
}

/// True iff the view's output `ExchangeShard` at `enid` is a no-op: its scan's
/// distribution prefix is exactly the shard key, and the output routes back to
/// that same partition.
fn skips_output_exchange(
    loaded: &LoadedCircuit,
    enid: NodeId,
    shard_cols: &[u32],
    registry: &RelationRegistry,
) -> bool {
    let Some((tid, mapped)) = scan_through_row_local(loaded, enid) else {
        return false;
    };
    registry.relation(tid).map(Relation::schema).is_some_and(|schema| {
        // Behind a map, shard column `c` is source PK column `c`, or a payload slot.
        let source_cols: Option<Vec<u32>> = match mapped {
            false => Some(shard_cols.to_vec()),
            true => shard_cols
                .iter()
                .map(|&c| schema.pk_indices().get(c as usize).copied())
                .collect(),
        };
        source_cols.is_some_and(|cols| shard_cols_match_dist_key(&schema, &cols))
            && output_route_returns_to_the_input_partition(shard_cols, &schema)
    })
}

// ---------------------------------------------------------------------------
// Circuit walks
// ---------------------------------------------------------------------------

/// One reindex key: a `(source column, carried promotion target)` slot list, in
/// the trace-side `ReindexPacker`'s own order.
type ReindexKey = Vec<gnitz_wire::ReindexSlot>;

/// Each source's stated scatter key, and the sources whose key is read by
/// something other than a `WorkerFilter`. `Err` on two distinct keys for one
/// source: one trace side would be keyed by the other.
fn stated_routes(loaded: &LoadedCircuit) -> Result<(JoinShardMap, FxHashSet<i64>), String> {
    let (mut seqs, mut untrimmed) = (JoinShardMap::default(), FxHashSet::default());
    for (nid, op) in loaded.ops() {
        let gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex {
            role: gnitz_wire::ReindexRole::ScatterKey { source, source_key },
            ..
        }) = op
        else {
            continue;
        };
        let tid = *source as i64;
        match seqs.entry(tid) {
            Entry::Vacant(e) => {
                e.insert(source_key.clone());
            }
            Entry::Occupied(e) if e.get() != source_key => {
                return Err(format!("source {source} feeds several distinct scatter keys"))
            }
            Entry::Occupied(_) => {}
        }
        let mut readers = (nid + 1..loaded.len()).filter(|&r| loaded.inputs(r).iter().any(|p| p == nid));
        if !readers.all(|r| matches!(loaded.op(r), gnitz_wire::OpNode::WorkerFilter)) {
            untrimmed.insert(tid);
        }
    }
    Ok((seqs, untrimmed))
}

/// How the rows one scan emits are consumed.
struct ScanConsumption {
    /// They reach nothing past a `Join` but as that join's operand.
    only_joins: bool,
    /// They cross a `Distinct` on the way to a join.
    set_fed: bool,
    /// They reach a `Join`, so the source must state its scatter key.
    joined: bool,
}

/// Walk forward from the scan at `scan_nid` through `Filter`, `Map`,
/// `IntegrateTrace` and `Distinct`, stopping at a `Join`; any other consumer uses
/// the delta itself rather than a join term over it.
fn scan_consumption(loaded: &LoadedCircuit, scan_nid: NodeId) -> ScanConsumption {
    let mut reached = vec![false; loaded.len()];
    reached[scan_nid] = true;
    let mut consumed = ScanConsumption {
        only_joins: true,
        set_fed: false,
        joined: false,
    };
    for nid in scan_nid + 1..loaded.len() {
        if !loaded.inputs(nid).iter().any(|p| reached[p]) {
            continue;
        }
        match loaded.op(nid) {
            gnitz_wire::OpNode::Join { .. } => consumed.joined = true,
            gnitz_wire::OpNode::Filter(_) | gnitz_wire::OpNode::Map(_) | gnitz_wire::OpNode::IntegrateTrace => {
                reached[nid] = true
            }
            gnitz_wire::OpNode::Distinct => {
                reached[nid] = true;
                consumed.set_fed = true;
            }
            _ => consumed.only_joins = false,
        }
    }
    consumed
}

/// Walk back from `enid` through nodes that keep rows on their worker and the PK
/// region verbatim, to its `ScanDelta`: `(table id, whether a map was crossed)`.
/// Every input names an earlier node, so no visited guard.
fn scan_through_row_local(loaded: &LoadedCircuit, enid: NodeId) -> Option<(i64, bool)> {
    let (mut cur, mut mapped) = (enid, false);
    loop {
        // Bail on a fan-in: a multi-input node (Union, set op) draws from more
        // than one source, so no single table's distribution prefix governs the
        // shard key and it can never co-partition.
        let NodeInputs::Unary(src_nid) = *loaded.inputs(cur) else {
            return None;
        };
        match loaded.op(src_nid) {
            gnitz_wire::OpNode::ScanDelta { source: t, .. } => return Some((*t as i64, mapped)),
            gnitz_wire::OpNode::Filter(_) => {}
            gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Projection(_) | gnitz_wire::MapKind::Compute(_)) => {
                mapped = true
            }
            _ => return None,
        }
        cur = src_nid;
    }
}

/// How a view's join, if it has one, needs its inputs placed — what the master
/// relay routes a source's delta by, and whether any source can skip the
/// scatter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinRelay {
    /// Route by the whole reindex key: an equi join, whose matches share a key
    /// (a GROUP BY routes by its group key the same way).
    WholeKey,
    /// A band join: route by the `n_eq` leading equality slots alone, dropping
    /// the range slot, so equal eq-values co-partition and the range probe
    /// stays partition-local.
    EqPrefix { n_eq: u8 },
    /// A pure range join or a cross join: the matches spread over the whole
    /// other side, so every worker needs the full delta and a `WorkerFilter`
    /// trims what it integrates.
    Broadcast,
}

/// The join relay a circuit's `Join` nodes call for, `None` for a circuit
/// without a join. `Err` when two joins call for different relays: the view's
/// sources are routed by one.
fn circuit_join_relay(loaded: &LoadedCircuit) -> Result<Option<JoinRelay>, String> {
    use gnitz_wire::{JoinKind, OpNode};
    let mut relay = None;
    for (_, op) in loaded.ops() {
        let OpNode::Join { kind, .. } = op else { continue };
        let this = match kind {
            JoinKind::Equi => JoinRelay::WholeKey,
            JoinKind::Range { n_eq: 0, .. } | JoinKind::Cross => JoinRelay::Broadcast,
            JoinKind::Range { n_eq, .. } => JoinRelay::EqPrefix { n_eq: *n_eq },
        };
        if relay.is_some_and(|r| r != this) {
            return Err("circuit joins need different relays".into());
        }
        relay = Some(this);
    }
    Ok(relay)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/routing.rs"]
mod tests;
