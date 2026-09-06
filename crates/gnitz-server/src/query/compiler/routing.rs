//! What the runtime must know about a view's circuit that is not its
//! instructions: how each source's delta is routed into it, plus the three shape
//! facts the worker dispatch and the placement query branch on. Nothing here
//! emits, so the master answers a routing question without running
//! `compile_view`, which stamps scratch tables with the caller's rank.

use super::*;
use gnitz_store::schema::Placement;
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
type JoinShardMap = HashMap<i64, load::ReindexKey>;

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
    /// The sink-nearest `ExchangeShard` is a proven no-op: every row it would
    /// move already sits on the worker owning its distribution key. Not itself a
    /// statement that the circuit is one-sided — the worker dispatch conjoins
    /// that before eliding the output IPC.
    pub(in crate::query) skips_exchange: bool,
    /// The circuit moves rows off the worker that produced them: it carries an
    /// output `ExchangeShard`, or a `Join`, whose inputs the runtime join-shard
    /// scatter repartitions without an `ExchangeShard` node.
    pub(in crate::query) repartitions: bool,
}

impl ViewMeta {
    /// The answer for a circuit that could not be read or is cyclic: no exchange
    /// skip, no repartition, every source routed by the empty key. Every metadata
    /// query then takes its conservative branch instead of walking a graph that
    /// is not there.
    pub(in crate::query) fn nothing_special() -> ViewMeta {
        ViewMeta {
            source_routes: FxHashMap::default(),
            default_route: group_key_route(None),
            skips_exchange: false,
            repartitions: false,
        }
    }

    /// Load `view_id`'s circuit and derive its metadata. A circuit that cannot be
    /// read, is malformed, or carries an unscatterable source falls back to
    /// [`ViewMeta::nothing_special`] — the same circuits `compile_view` rejects,
    /// so no view that runs is metadata-less.
    pub(in crate::query) fn for_view(host: &dyn SchemaSource, view_id: i64) -> ViewMeta {
        load::load_circuit(host, view_id as u64)
            .ok()
            .and_then(|loaded| ViewMeta::derive(&loaded, host).ok())
            .unwrap_or_else(ViewMeta::nothing_special)
    }

    /// `host` supplies what the circuit alone cannot: co-partitioning and the
    /// output-shard elision both test a shard key against a *source relation's*
    /// distribution prefix. `Err` when a source's reindex maps carry no route
    /// key, so its delta cannot be scattered.
    pub(in crate::query) fn derive(loaded: &LoadedCircuit, host: &dyn SchemaSource) -> Result<ViewMeta, CompileError> {
        // source → the distinct key sequences its scans feed. Deduped across scan
        // nodes as well as within one, so two scans on a source carrying the same
        // key resolve to one sequence rather than tripping the pack-key gate below.
        let mut seqs: HashMap<i64, Vec<load::ReindexKey>> = HashMap::new();
        for (nid, op) in loaded.ops() {
            let gnitz_wire::OpNode::ScanDelta { source, .. } = op else {
                continue;
            };
            let (node_seqs, orphaned) = load::scatter_key_of_scan(loaded, nid);
            // Not conditioned on the circuit carrying a `Join`: a GROUP BY or
            // PK-redistribution circuit's group reindex routes just as much.
            if orphaned {
                return Err(CompileError::Rejected(
                    "a source's reindex maps carry no route key, so its delta cannot be scattered",
                ));
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

        // The co-partition prefix test wants the sequences CONCATENATED, and wants
        // a conservative refusal when a source carries more than one key: the
        // concatenation matches no source's distribution prefix, so that source
        // correctly goes through the exchange.
        let concatenated: JoinShardMap = seqs
            .iter()
            .map(|(&tid, s)| (tid, s.iter().flatten().copied().collect()))
            .collect();
        let join_relay = load::circuit_join_relay(loaded);
        // A range or cross join's matches spread over the whole other side, so
        // no source distribution places them: nothing co-partitions, and every
        // source relays.
        let co_partitioned = match join_relay {
            load::JoinRelay::WholeKey => compute_co_partitioned(&concatenated, host),
            load::JoinRelay::EqPrefix { .. } | load::JoinRelay::Broadcast => HashSet::new(),
        };

        let shard = load::output_exchange_shard(loaded);
        let skips_exchange = shard
            .as_ref()
            .is_some_and(|(enid, cols)| skips_output_exchange(loaded, *enid, cols, host));
        let shard_cols: Option<Rc<[u32]>> = shard.map(|(_, cols)| Rc::from(cols));
        let repartitions =
            shard_cols.is_some() || loaded.ops().any(|(_, op)| matches!(op, gnitz_wire::OpNode::Join(_)));
        let source_routes = seqs
            .into_iter()
            .map(|(tid, s)| {
                // The relay packs ONE sequence or refuses, never the concatenation
                // `compute_co_partitioned` reads: the delta would scatter by
                // `pack(a ‖ b)` while each trace side is keyed by `pack(a)` or
                // `pack(b)`, dropping every match silently.
                let route = match <[_; 1]>::try_from(s) {
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
        })
    }

    /// How the master relay routes `source_id`'s delta into this view.
    pub(crate) fn relay_route(&self, source_id: i64) -> &RelayRoute {
        self.source_routes
            .get(&source_id)
            .map_or(&self.default_route, |relay| &relay.route)
    }

    /// True iff `source_id`'s delta must go through the join scatter: it carries a
    /// join/group reindex key, and its native distribution does not already match
    /// that key (nor is its partner replicated, which makes the exchange
    /// unnecessary either way).
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
fn join_route(pairs: load::ReindexKey, relay: load::JoinRelay) -> RelayRoute {
    let route_len = match relay {
        load::JoinRelay::Broadcast => return RelayRoute::Broadcast,
        load::JoinRelay::WholeKey => pairs.len(),
        load::JoinRelay::EqPrefix { n_eq } => {
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

fn compute_co_partitioned(join_shard_map: &JoinShardMap, host: &dyn SchemaSource) -> HashSet<i64> {
    // One replicated participant makes every participant skip: the replicated
    // side already has every row on every worker, and its partner can therefore
    // stay in its own PK partitioning and `cogroup` against the full local copy.
    // Not folded into `shard_cols_match_dist_key`, which must stay the pure
    // prefix predicate for the partner-less skip path.
    let any_replicated = join_shard_map
        .keys()
        .any(|&tid| host.schema_of(tid).is_some_and(|s| s.placement().is_replicated()));
    let mut co_partitioned = HashSet::new();
    for (&tid, cols) in join_shard_map {
        let Some(ext_schema) = host.schema_of(tid) else {
            continue;
        };

        if any_replicated {
            co_partitioned.insert(tid);
            continue;
        }

        // A carried target means the slot is wider than the source column, so
        // native PK partitions do not align with the T-width trace key. Partner of
        // `ScatterKey::new`'s own no-target gate: relaxing one alone would let a
        // promoted side skip while its partner scatters at the wider `T`.
        if cols.iter().any(|(_, t)| t.is_some()) {
            continue;
        }
        let col_indices: Vec<u32> = cols.iter().map(|&(c, _)| c).collect();
        if shard_cols_match_dist_key(&ext_schema, &col_indices) {
            co_partitioned.insert(tid);
        }
    }
    co_partitioned
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

/// True iff the view's output `ExchangeShard` at `enid` is a no-op: it reads a
/// scan — through any Filter chain — whose distribution prefix is exactly the
/// shard key, and the pipeline's output routes back to that same partition.
fn skips_output_exchange(loaded: &LoadedCircuit, enid: i32, shard_cols: &[u32], host: &dyn SchemaSource) -> bool {
    let Some(tid) = scan_tid_through_filters(loaded, enid) else {
        return false;
    };
    host.schema_of(tid).is_some_and(|schema| {
        shard_cols_match_dist_key(&schema, shard_cols)
            && output_route_returns_to_the_input_partition(shard_cols, &schema)
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/routing.rs"]
mod tests;
