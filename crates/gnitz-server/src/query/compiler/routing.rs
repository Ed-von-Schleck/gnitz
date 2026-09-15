//! The plan-free facts of a view's circuit: how each source's delta is routed
//! into it, and the shape facts the worker dispatch, the placement query and the
//! backfill cursor read. Nothing here emits, so the master can ask without compiling.

use super::*;
use gnitz_store::schema::Placement;
use gnitz_wire::ReadBound;
use rustc_hash::FxHashSet;
use std::collections::hash_map::Entry;
use std::rc::Rc;

/// How the master relay routes one source's delta into a view.
pub(crate) enum RelayRoute {
    /// The source feeds several distinct reindex keys: no single key
    /// co-partitions it with the trace sides, so the round must be refused
    /// rather than routed by a key nothing was stored under.
    NoSingleKey,
    /// Pure range join (`n_eq == 0`) or cross join: the matches are spread over
    /// the whole other side, so every worker needs the full delta and trims to
    /// its owned slice (`WorkerFilter`) before integrating.
    Broadcast,
    /// Scatter by the view's own shard columns, under the null-distinct group
    /// fold `op_reduce` keys its output with.
    GroupKey(Rc<[u32]>),
    /// Scatter by a join key, already truncated to the routing prefix and
    /// mirroring the trace-side reindex Map slot-for-slot. A band join
    /// (`n_eq >= 1`) routes by the equality prefix alone, dropping the trailing
    /// range slot, so equal eq-values co-partition both sides and the range
    /// probe stays partition-local.
    JoinKey(Rc<[gnitz_wire::ReindexSlot]>),
}

/// source table id → the join/group reindex key its scans feed.
type JoinShardMap = FxHashMap<i64, ReindexKey>;

/// What one source gets: how the master routes its delta, and whether the worker
/// must send it through the scatter at all.
struct SourceRelay {
    route: RelayRoute,
    scatter: bool,
}

/// Per-view circuit metadata, derived from one circuit load — the master's relay
/// key and the worker's scatter set together, so the two processes cannot derive
/// disagreeing halves. `derive` reads the circuit only through
/// [`LoadedCircuit::ops`], which orders identically in both.
pub(crate) struct ViewMeta {
    /// source table id → that source's relay entry. A source absent from this
    /// map carries no reindex key of its own and takes `default_route`.
    source_routes: FxHashMap<i64, SourceRelay>,
    /// The route of everything absent from `source_routes`, including the output
    /// relay (`source_id == 0`): the view's own shard columns under `GroupKey`.
    default_route: RelayRoute,
    /// The circuit's one `ExchangeShard` is a proven no-op: every row it would
    /// move already sits on the worker owning its distribution key.
    pub(in crate::query) skips_exchange: bool,
    /// The circuit moves rows off the worker that produced them: it carries an
    /// output `ExchangeShard`, or a `Join`, whose inputs the runtime join-shard
    /// scatter repartitions without an `ExchangeShard` node.
    pub(in crate::query) repartitions: bool,
    /// source table id → the bound its backfill scan narrows by. Absent for a
    /// source scanned twice, whose one backfill cursor feeds both scans.
    pub(in crate::query) source_bounds: FxHashMap<i64, ReadBound>,
}

impl ViewMeta {
    /// An empty fixture, enough to occupy a memo slot.
    #[cfg(test)]
    pub(in crate::query) fn empty() -> ViewMeta {
        ViewMeta {
            source_routes: FxHashMap::default(),
            default_route: group_key_route(None),
            skips_exchange: false,
            repartitions: false,
            source_bounds: FxHashMap::default(),
        }
    }

    /// Load `view_id`'s circuit and derive its metadata. `None` when the circuit
    /// cannot be read, is malformed, or carries an unscatterable source — there
    /// is no default route, since a key nothing was stored under would scatter a
    /// delta away from the traces it must meet.
    pub(in crate::query) fn for_view(registry: &RelationRegistry, view_id: i64) -> Option<ViewMeta> {
        let loaded = load::load_circuit(registry, view_id as u64).ok()?;
        ViewMeta::derive(&loaded, registry).ok()
    }

    /// `registry` supplies what the circuit alone cannot: co-partitioning and the
    /// output-shard elision both test a shard key against a *source relation's*
    /// distribution prefix. `Err` when a source's reindex maps carry no route
    /// key, so its delta cannot be scattered.
    pub(in crate::query) fn derive(loaded: &LoadedCircuit, registry: &RelationRegistry) -> Result<ViewMeta, String> {
        // source → the distinct key sequences its scans feed. Deduped across scan
        // nodes as well as within one, so two scans on a source carrying the same
        // key resolve to one sequence rather than tripping the pack-key gate below.
        let mut seqs: FxHashMap<i64, Vec<ReindexKey>> = FxHashMap::default();
        let mut outside_joins: FxHashSet<i64> = FxHashSet::default();
        let mut set_fed: FxHashSet<i64> = FxHashSet::default();
        let mut untrimmed: FxHashSet<i64> = FxHashSet::default();
        // `ReadBound::None` once a source is seen twice.
        let mut bounds: FxHashMap<i64, ReadBound> = FxHashMap::default();
        for (nid, op) in loaded.ops() {
            let gnitz_wire::OpNode::ScanDelta { source, bound } = op else {
                continue;
            };
            match bounds.entry(*source as i64) {
                Entry::Vacant(e) => {
                    e.insert(bound.clone());
                }
                Entry::Occupied(mut e) => *e.get_mut() = ReadBound::None,
            }
            let consumed = scan_consumption(loaded, nid);
            if !consumed.only_joins {
                outside_joins.insert(*source as i64);
            }
            if consumed.set_fed {
                set_fed.insert(*source as i64);
            }
            let (node_seqs, orphaned, owner_trimmed) = scatter_key_of_scan(loaded, nid);
            if orphaned {
                return Err("a source's reindex maps carry no route key, so its delta cannot be scattered".into());
            }
            if !owner_trimmed {
                untrimmed.insert(*source as i64);
            }
            // Every live circuit holds one scan to one sequence, so a second means
            // a newly-constructible shape that wants a real plan, not a refusal.
            debug_assert!(
                node_seqs.len() <= 1,
                "scan {nid} feeds {} distinct scatter keys; no single pack key routes it",
                node_seqs.len(),
            );
            let acc = seqs.entry(*source as i64).or_default();
            for seq in node_seqs {
                if !acc.contains(&seq) {
                    acc.push(seq);
                }
            }
        }
        seqs.retain(|_, s| !s.is_empty());
        let source_bounds = bounds.into_iter().filter(|(_, b)| *b != ReadBound::None).collect();

        // The co-partition prefix test wants the sequences CONCATENATED, and wants
        // a conservative refusal when a source carries more than one key: the
        // concatenation matches no source's distribution prefix, so that source
        // correctly goes through the exchange.
        let concatenated: JoinShardMap = seqs
            .iter()
            .map(|(&tid, s)| (tid, s.iter().flatten().copied().collect()))
            .collect();
        let join_relay = circuit_join_relay(loaded)?;
        let shards: Vec<(NodeId, &[u32])> = loaded.exchange_shards().collect();
        let repartitions = !shards.is_empty() || join_relay.is_some();
        let join_relay = join_relay.unwrap_or(JoinRelay::WholeKey);
        // Every scatter key of the source is read only by owner filters.
        let owner_trimmed = |tid: &i64| !untrimmed.contains(tid);
        // A range or cross join's matches spread over the whole other side, so
        // no source distribution places them: nothing co-partitions, and every
        // source relays — except an owner-trimmed one already on its PK's owner.
        let co_partitioned = match join_relay {
            JoinRelay::WholeKey => compute_co_partitioned(&concatenated, registry, &outside_joins, &set_fed),
            JoinRelay::EqPrefix { .. } => FxHashSet::default(),
            JoinRelay::Broadcast => concatenated
                .iter()
                .filter(|&(tid, cols)| owner_trimmed(tid) && native_partition_is(registry, *tid, cols))
                .map(|(&tid, _)| tid)
                .collect(),
        };

        let skips_exchange = match shards[..] {
            [(enid, cols)] => skips_output_exchange(loaded, enid, cols, registry),
            _ => false,
        };
        let shard_cols: Option<Rc<[u32]>> = shards.last().map(|&(_, cols)| Rc::from(cols));
        let source_routes = seqs
            .into_iter()
            .map(|(tid, s)| {
                // The relay packs ONE sequence or refuses, never the concatenation
                // `compute_co_partitioned` reads: the delta would scatter by
                // `pack(a ‖ b)` while each trace side is keyed by `pack(a)` or
                // `pack(b)`, dropping every match silently.
                let route = match <[_; 1]>::try_from(s) {
                    // Routes each row to the worker its owner filter keeps it on.
                    Ok([seq]) if join_relay == JoinRelay::Broadcast && owner_trimmed(&tid) => {
                        join_route(seq, JoinRelay::WholeKey)
                    }
                    Ok([seq]) => join_route(seq, join_relay),
                    Err(_) => RelayRoute::NoSingleKey,
                };
                let relay = SourceRelay {
                    route,
                    scatter: !co_partitioned.contains(&tid),
                };
                (tid, relay)
            })
            .collect();

        Ok(ViewMeta {
            source_routes,
            default_route: group_key_route(shard_cols),
            skips_exchange,
            repartitions,
            source_bounds,
        })
    }

    /// How the master relay routes `source_id`'s delta into this view.
    pub(crate) fn relay_route(&self, source_id: i64) -> &RelayRoute {
        self.source_routes
            .get(&source_id)
            .map_or(&self.default_route, |relay| &relay.route)
    }

    /// True iff `source_id`'s delta must go through the join scatter: it carries a
    /// join/group reindex key, and neither its native distribution matching that
    /// key nor a replicated partner met only through join terms makes the
    /// exchange unnecessary.
    pub(in crate::query) fn scatters(&self, source_id: i64) -> bool {
        self.source_routes.get(&source_id).is_some_and(|relay| relay.scatter)
    }

    /// True iff this view's relay routing holds a key for `id` — not every source
    /// it scans, so it answers eviction (over-eviction is safe) and nothing that
    /// must enumerate sources.
    pub(in crate::query) fn routes_source(&self, id: i64) -> bool {
        self.source_routes.contains_key(&id)
    }
}

/// The view's shard columns, consistent with `op_reduce`'s output PK. `None` —
/// no `ExchangeShard` in the circuit — routes by `∅`.
fn group_key_route(shard_cols: Option<Rc<[u32]>>) -> RelayRoute {
    RelayRoute::GroupKey(shard_cols.unwrap_or_else(|| Rc::from([])))
}

/// The route a source carrying a reindex key takes. `pairs` is that key,
/// `(column, promotion target)` per slot, in trace-side reindex order.
fn join_route(pairs: ReindexKey, relay: JoinRelay) -> RelayRoute {
    let route_len = match relay {
        JoinRelay::Broadcast => return RelayRoute::Broadcast,
        JoinRelay::WholeKey => pairs.len(),
        JoinRelay::EqPrefix { n_eq } => {
            debug_assert!(
                pairs.len() == n_eq as usize + 1,
                "band-join reindex key = [eq…, range]: len must be n_eq + 1"
            );
            n_eq as usize
        }
    };
    RelayRoute::JoinKey(Rc::from(&pairs[..route_len]))
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

/// The scatter key of the source scanned at `scan_nid`: one sequence per
/// `ScatterKey` reindex `Map` reachable forward through `Filter`s.
///
/// Which reindex is a source's join/group key is the planner's to state
/// ([`gnitz_wire::ReindexRole`]); the engine could only guess it from graph
/// shape. The second return says the walk found only `Auxiliary` ones — a
/// planner call site that forgot its role, which `ViewMeta::derive` turns
/// into a failed compile rather than silently-unscattered rows. The third says
/// every `ScatterKey` map found is read by `WorkerFilter`s alone.
fn scatter_key_of_scan(loaded: &LoadedCircuit, scan_nid: NodeId) -> (Vec<ReindexKey>, bool, bool) {
    // A node is reached when one of its inputs is and its operator propagates;
    // one forward pass suffices because every input names an earlier node.
    let mut reached = vec![false; loaded.len()];
    reached[scan_nid] = true;
    // One entry per distinct key sequence — an identical one reached again is
    // added once. Duplicate columns WITHIN a sequence are preserved, so the result
    // mirrors the trace-side `ReindexPacker` slot-for-slot.
    let mut seqs: Vec<ReindexKey> = Vec::new();
    let mut saw_auxiliary = false;
    let mut owner_trimmed = true;
    for nid in scan_nid + 1..loaded.len() {
        if !loaded.inputs(nid).iter().any(|p| reached[p]) {
            continue;
        }
        match loaded.op(nid) {
            gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Reindex { key, role, .. }) => {
                if *role != gnitz_wire::ReindexRole::ScatterKey {
                    saw_auxiliary = true;
                    continue;
                }
                let mut readers = (nid + 1..loaded.len()).filter(|&r| loaded.inputs(r).iter().any(|p| p == nid));
                owner_trimmed &= readers.all(|r| matches!(loaded.op(r), gnitz_wire::OpNode::WorkerFilter));
                if !seqs.contains(key) {
                    seqs.push(key.clone());
                }
            }
            gnitz_wire::OpNode::Filter(_) => reached[nid] = true,
            _ => {}
        }
    }
    let orphaned = seqs.is_empty() && saw_auxiliary;
    (seqs, orphaned, owner_trimmed)
}

/// How the rows one scan emits are consumed.
struct ScanConsumption {
    /// They reach nothing past a `Join` but as that join's operand.
    only_joins: bool,
    /// They cross a `Distinct` on the way to a join.
    set_fed: bool,
}

/// Walk forward from the scan at `scan_nid` through `Filter`, `Map`,
/// `IntegrateTrace` and `Distinct`, stopping at a `Join`; any other consumer uses
/// the delta itself rather than a join term over it.
fn scan_consumption(loaded: &LoadedCircuit, scan_nid: NodeId) -> ScanConsumption {
    let mut reached = vec![false; loaded.len()];
    reached[scan_nid] = true;
    let mut consumed = ScanConsumption { only_joins: true, set_fed: false };
    for nid in scan_nid + 1..loaded.len() {
        if !loaded.inputs(nid).iter().any(|p| reached[p]) {
            continue;
        }
        match loaded.op(nid) {
            gnitz_wire::OpNode::Join(_) => {}
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

/// Walk back from the `ExchangeShard` at `enid` through nodes that keep rows on
/// their worker and the PK region verbatim, to its `ScanDelta`: `(table id, whether
/// a map was crossed)`. Every input names an earlier node, so no visited guard.
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
        let OpNode::Join(kind) = op else { continue };
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
