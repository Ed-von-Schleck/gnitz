//! What the compiler derives from a circuit besides its instructions:
//! [`CircuitFacts`] — the routing and shape bundle the runtime reads off a
//! view.

use super::*;
use gnitz_store::schema::Placement;

/// Everything the runtime needs to know about a view's circuit that is not the
/// executable plan: its routing and its shape.
///
/// One producer, because the master derives the relay's key in its process while
/// the worker derives the scatter set in its own, and deriving them apart let
/// them disagree. Order-sensitive facts are therefore read off `loaded.ordered`,
/// never off `nodes`, whose iteration order differs per process.
pub(in crate::query) struct CircuitFacts {
    /// source table id → the one key the master relay may pack and route that
    /// source's delta by, or `None` when its scans feed several distinct keys:
    /// no single pack key then co-partitions with the trace sides, and the relay
    /// must refuse the round rather than fall back to the view's
    /// `ExchangeShard` columns, which route by a different key again.
    pub keys: HashMap<i64, Option<Vec<(u32, u8)>>>,
    /// The `keys` sources whose deltas must actually go through the scatter:
    /// less those whose native distribution already aligns with the key (or
    /// whose partner is replicated, making the exchange unnecessary either way).
    pub scatter_sources: FxHashSet<i64>,
    /// The sink-nearest `ExchangeShard`'s columns — the routing key of every
    /// source that carries no reindex key of its own — and `None` when the circuit
    /// carries no `ExchangeShard` at all. The two states are distinct: an ungrouped
    /// global aggregate shards on `∅`, a real exchange that funnels every row onto
    /// `worker_for_key(V₀)`.
    pub shard_cols: Option<Vec<u32>>,
    /// `Some(n_eq)` iff the view is a non-equi (range / band) join.
    pub range_join_n_eq: Option<u8>,
    /// The circuit carries a `Join` node.
    pub has_join: bool,
    /// The output `ExchangeShard` is a proven no-op, so the output IPC is elided.
    pub skips_exchange: bool,
}

impl CircuitFacts {
    /// `ext_tables` supplies what the circuit alone cannot: co-partitioning and
    /// the output-shard elision both test a shard key against a *source
    /// relation's* distribution prefix.
    ///
    /// `Err` when a source's reindex maps carry no route key, so its delta cannot
    /// be scattered — returned rather than carried as a field no consumer is
    /// obliged to read.
    pub(in crate::query) fn derive(
        loaded: &LoadedCircuit,
        ext_tables: &dyn SchemaSource,
    ) -> Result<CircuitFacts, CompileError> {
        // source → the distinct sequences its scans feed. Deduped across scan nodes
        // as well as within one, so two scans on a source carrying the same key
        // resolve to one sequence rather than tripping the pack-key gate below.
        let mut seqs: HashMap<i64, Vec<Vec<(u32, u8)>>> = HashMap::new();
        for &nid in &loaded.ordered {
            let gnitz_wire::OpNode::ScanDelta { source, .. } = loaded.op(nid) else {
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
        let co_partitioned = compute_co_partitioned(&concatenated, ext_tables);
        let scatter_sources = concatenated
            .into_keys()
            .filter(|tid| !co_partitioned.contains(tid))
            .collect();

        // The relay's pack key wants ONE sequence or a refusal, never the
        // concatenation: the delta would scatter by `pack(a ‖ b)` while each trace
        // side is keyed by `pack(a)` or `pack(b)`, dropping every match silently.
        // Not SQL-reachable, but a hand-built wire circuit can express it — hence a
        // refusal here and not the `debug_assert` above.
        let keys = seqs
            .into_iter()
            .map(|(tid, mut s)| (tid, (s.len() == 1).then(|| s.pop().expect("len == 1"))))
            .collect();

        let shard = load::output_exchange_shard(loaded);
        Ok(CircuitFacts {
            keys,
            scatter_sources,
            skips_exchange: shard
                .as_ref()
                .is_some_and(|(enid, cols)| skips_output_exchange(loaded, *enid, cols, ext_tables)),
            shard_cols: shard.map(|(_, cols)| cols),
            range_join_n_eq: load::circuit_range_join_n_eq(loaded),
            has_join: loaded
                .nodes
                .values()
                .any(|op| matches!(op, gnitz_wire::OpNode::Join(_))),
        })
    }
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

pub(super) fn compute_co_partitioned(join_shard_map: &JoinShardMap, ext_tables: &dyn SchemaSource) -> HashSet<i64> {
    // One replicated participant makes every participant skip: the replicated
    // side already has every row on every worker, and its partner can therefore
    // stay in its own PK partitioning and `cogroup` against the full local copy.
    // Held here rather than inside `shard_cols_match_dist_key`, which the
    // partner-less single-source skip path also uses and which must stay the pure
    // prefix predicate.
    let any_replicated = join_shard_map
        .keys()
        .any(|&tid| ext_tables.schema_of(tid).is_some_and(|s| s.placement().is_replicated()));
    let mut co_partitioned = HashSet::new();
    for (&tid, cols) in join_shard_map {
        let Some(ext_schema) = ext_tables.schema_of(tid) else {
            continue;
        };

        if any_replicated {
            co_partitioned.insert(tid);
            continue;
        }

        // A non-zero carried tc means the slot is wider than the source column, so
        // native PK partitions do not align with the T-width trace key. Partner of
        // `ScatterKey::new`'s own `tc == 0` gate: relaxing one alone would let a
        // promoted side skip while its partner scatters at the wider `T`.
        if cols.iter().any(|&(_, tc)| tc != 0) {
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
fn skips_output_exchange(loaded: &LoadedCircuit, enid: i32, shard_cols: &[u32], ext_tables: &dyn SchemaSource) -> bool {
    let Some(tid) = scan_tid_through_filters(loaded, enid) else {
        return false;
    };
    ext_tables.schema_of(tid).is_some_and(|schema| {
        shard_cols_match_dist_key(&schema, shard_cols)
            && output_route_returns_to_the_input_partition(shard_cols, &schema)
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/optimize.rs"]
mod tests;
