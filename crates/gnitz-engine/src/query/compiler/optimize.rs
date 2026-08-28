//! What the compiler derives from a circuit besides its instructions:
//! [`CircuitFacts`] — the routing, shape and scan-hint bundle the runtime reads
//! off a view — plus the distinct elision the emitter consults and the promotion
//! validators both of them lean on.

use super::*;
use crate::schema::Placement;

/// Everything the runtime needs to know about a view's circuit that is not the
/// executable plan: its routing, its shape, and its source scan's access hint.
///
/// One struct with one producer because these are projections of one circuit, and
/// deriving them apart let them disagree — the master derives the relay's key in
/// its process while the worker derives the scatter set in its own. Anything
/// order-sensitive is therefore read off `loaded.ordered` and never off `nodes`,
/// whose iteration order differs per process.
pub(crate) struct CircuitFacts {
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
    /// The `(source table id, secondary-index range)` the planner pushed onto the
    /// primary source's `ScanDelta`.
    pub source_bound: Option<(i64, gnitz_wire::ScanBound)>,
}

impl CircuitFacts {
    /// Derive every fact, in the one place they are derived.
    ///
    /// `ext_tables` is what the routing half needs and the circuit alone cannot
    /// supply: co-partitioning and the output-shard elision both test a shard key
    /// against a *source relation's* distribution prefix.
    ///
    /// `Err` when a source's reindex maps carry no route key: its delta cannot be
    /// scattered. Returned rather than carried as a field, because a field is a
    /// verdict every consumer has to remember to read.
    pub(crate) fn derive(loaded: &LoadedCircuit, ext_tables: &dyn SchemaSource) -> Result<CircuitFacts, CompileError> {
        // source → the distinct sequences its scans feed. Deduping ACROSS scan
        // nodes mirrors the within-node dedup in `scatter_key_of_scan`: two scans
        // on one source carrying the same key must still resolve to one sequence,
        // or the pack-key gate below would newly refuse a round that routes fine
        // today.
        let mut seqs: HashMap<i64, Vec<Vec<(u32, u8)>>> = HashMap::new();
        for &nid in &loaded.ordered {
            let Some(gnitz_wire::OpNode::ScanDelta { source, .. }) = loaded.nodes.get(&nid) else {
                continue;
            };
            let (node_seqs, orphaned) = load::scatter_key_of_scan(loaded, nid);
            // A scan reaching reindex maps of which none is a `ScatterKey` is what
            // a planner call site that forgot its role produces — the likelier of
            // the two possible mistakes, and silently-unscattered rows if honoured.
            // Deliberately not conditioned on the circuit carrying a `Join`: a
            // GROUP BY or PK-redistribution circuit's group reindex is equally
            // load-bearing for routing.
            if orphaned {
                return Err(CompileError::Rejected(
                    "a source's reindex maps carry no route key, so its delta cannot be scattered",
                ));
            }
            // WITHIN one scan the `ScatterKey` role filter and the
            // identical-sequence dedup appear to hold every live circuit to one
            // sequence, so a second one means the shape became constructible and
            // wants a real plan, not a silent refusal. Across scan nodes it is only
            // a tripwire, not a rule — see the pack-key projection below.
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
        // concatenation: with sequences `a` and `b` the delta would scatter by
        // `pack(a ‖ b)` while each trace side is keyed by `pack(a)` or `pack(b)`,
        // so the two would never co-partition and matches would drop silently.
        // Refusing is the only safe answer, and it is the relay's to report.
        //
        // Two scans of one source carrying DIFFERENT keys is not SQL-reachable —
        // the planner wraps a repeated tid in a pass-through segment before
        // lowering a join — but a hand-built wire circuit can express it, and
        // refusing is what this walk owes it. Deliberately NOT a `debug_assert`,
        // unlike the per-scan tripwire above.
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
            source_bound: load::circuit_source_bound(loaded),
        })
    }
}

/// True iff `cols` is **exactly** `schema`'s distribution prefix —
/// `pk_indices()[..k]` in PK order, where `k` is its `Keyed` prefix length. That
/// means a derived operator co-partitions with the relation (the exchange router
/// hashes the same leading `dist_stride` OPK bytes), so its network exchange can
/// be skipped. A non-`Keyed` relation's rows are not placed by `worker_for_pk`
/// at all, so no shard key names where they already are; a replicated *join*
/// source still skips, through [`compute_co_partitioned`]'s replication arm.
///
/// **Exact `== k`, never a super-prefix.** A super-prefix gate would let the two
/// sides of a join skip at *different* prefix widths, hashing equal join keys to
/// different workers so the elided exchange silently drops matches. A side whose
/// join-key length differs from its own `k` instead exchanges and repartitions to
/// the full key, reconverging with the other side.
/// `cluster_by_super_prefix_join_safety_multiworker` in
/// `gnitz-sql/tests/planner_cluster_by.rs` exercises this.
fn shard_cols_match_dist_key(schema: &SchemaDescriptor, cols: &[u32]) -> bool {
    let Placement::Keyed { prefix_len } = schema.placement() else {
        return false;
    };
    let k = prefix_len as usize;
    cols.len() == k && cols == &schema.pk_indices()[..k]
}

pub(super) fn compute_co_partitioned(join_shard_map: &JoinShardMap, ext_tables: &dyn SchemaSource) -> HashSet<i64> {
    // Replication skip, computed once for the whole join: if ANY participating
    // source is replicated, EVERY participant skips its exchange. This deliberately
    // does NOT widen `shard_cols_match_dist_key` (the pure prefix predicate also
    // used by the partner-less single-source view-skip path, which must keep its
    // exact semantics).
    //   * a REPLICATED source always skips — its full copy is on every worker, so
    //     its delta and trace are already present everywhere (the write broadcast
    //     did the work the exchange would have);
    //   * a partitioned source whose join PARTNER is replicated also skips — it
    //     stays in its own PK partitioning and `cogroup`s against the full local
    //     dim copy, so no exchange is needed on either side. This is the case
    //     hash co-partitioning cannot serve: the fact need not be distributed by
    //     the join key, so one fact can join many replicated dims.
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

        // A non-zero carried tc means the slot width differs from the source,
        // so native PK partitions do not align with the T-width trace key — the
        // source must go through the exchange even if its PK matches the key.
        // This is the partner of `ScatterKey::new`'s own `tc == 0` gate:
        // relaxing one alone would let a promoted side skip its exchange while
        // its partner scatters at the wider `T`, silently dropping matches.
        if cols.iter().any(|&(_, tc)| tc != 0) {
            continue;
        }
        // Co-partitioned only when the shard (= join) key is EXACTLY the
        // source's distribution prefix in PK order (`pk_indices[..k]`). For a
        // default full-PK table that is the whole PK; for a `CLUSTER BY prefix`
        // table it is the leading `k` columns rows are actually hashed by. The
        // match is EXACT (`cols.len() == k`), never a super-prefix: a
        // super-prefix skip could route the two join sides at mismatched
        // widths and silently drop matches (see `shard_cols_match_dist_key`).
        let col_indices: Vec<u32> = cols.iter().map(|&(c, _)| c).collect();
        if shard_cols_match_dist_key(&ext_schema, &col_indices) {
            co_partitioned.insert(tid);
        }
    }
    co_partitioned
}

/// True iff the view's output `ExchangeShard` at `enid` is a no-op (every row
/// already on the worker owning its distribution key) and the output IPC can be
/// skipped: the shard reads a scan — through any Filter chain — whose distribution prefix
/// (`pk_indices[..k]`) is exactly the shard key, AND the pipeline's output
/// reproduces that shard key as its own route key.
///
/// The second half is what the first does not imply. Eliding the shard also
/// elides the output IPC (`skip_output_exchange`), so the output stays on the
/// worker that owned the input prefix's partition — sound only if the output
/// routes back to that same partition. That holds at the two ends of `k` and
/// nowhere between:
///
/// - `k == 1`: the output key is that one column's route key, the same
///   `widen_pk_be` value `worker_for_pk_bytes` derives from the prefix
///   (routing is invariant under left zero-padding at `len <= 16`).
/// - `k == |PK|`: the output re-emits the source PK verbatim.
/// - `1 < k < |PK|`: a multi-column key streams into an Xxh3 fold whose u128 is
///   unrelated to the prefix's, so the output lands on another worker and the
///   addressing store loses it.
///
/// So a `CLUSTER BY` prefix of 2+ proper-prefix columns buys no locality for a
/// `GROUP BY` on that prefix. Testing `k` rather than the consuming node's
/// `ReduceOutKey` keeps this independent of which operator the shard feeds: the
/// shard key *is* the group key (`reduce_multi` hands one column slice to both),
/// and `ReduceOutKey` alone would be the wrong discriminator anyway — a signed or
/// narrow single prefix column keys `SyntheticFold` and is nonetheless correct.
fn skips_output_exchange(loaded: &LoadedCircuit, enid: i32, shard_cols: &[u32], ext_tables: &dyn SchemaSource) -> bool {
    let Some(tid) = scan_tid_through_filters(loaded, enid) else {
        return false;
    };
    ext_tables.schema_of(tid).is_some_and(|schema| {
        shard_cols_match_dist_key(&schema, shard_cols)
            && (shard_cols.len() <= 1 || shard_cols.len() == schema.pk_indices().len())
    })
}

// ---------------------------------------------------------------------------
// Optimization passes
// ---------------------------------------------------------------------------

/// Distinct nodes elided because their input is already distinct. One forward
/// pass along the topological order, maintaining the set of nodes whose output
/// is known distinct: a Reduce or Distinct establishes it; a Filter preserves
/// it; a Map preserves it unless it re-keys the PK (an equijoin pre-index
/// reindex or a full-row HashRow), which invalidates upstream distinctness.
pub(super) fn compute_skip_nodes(loaded: &LoadedCircuit) -> HashSet<i32> {
    let mut distinct_at: HashSet<i32> = HashSet::new();
    let mut skip = HashSet::new();
    for &nid in &loaded.ordered {
        // Every arm below is a unary operator, so its one input is where the
        // property it preserves or establishes comes from.
        let input_distinct = |nid: i32| distinct_at.contains(&loaded.inputs(nid).unary());
        match loaded.nodes.get(&nid) {
            Some(gnitz_wire::OpNode::Reduce { .. }) => {
                distinct_at.insert(nid);
            }
            Some(gnitz_wire::OpNode::Distinct) => {
                if input_distinct(nid) {
                    skip.insert(nid);
                }
                distinct_at.insert(nid);
            }
            Some(gnitz_wire::OpNode::Filter(_)) => {
                if input_distinct(nid) {
                    distinct_at.insert(nid);
                }
            }
            Some(gnitz_wire::OpNode::Map(mk)) => {
                let re_keys = matches!(
                    mk,
                    gnitz_wire::MapKind::Reindex { .. } | gnitz_wire::MapKind::HashRow(..)
                );
                if !re_keys && input_distinct(nid) {
                    distinct_at.insert(nid);
                }
            }
            _ => {}
        }
    }
    skip
}

/// True iff any carried target in `target_tcs` is invalid for its source column
/// in `cols` under `valid`, the domain predicate of the promotion's destination.
/// A violation means a corrupt/forged catalog; callers abort the compile cleanly
/// rather than panic/truncate in the copy kernels. Callers must have
/// range-checked `cols` first. A zero target carries no promotion and is always
/// accepted. Shared body of [`key_promotion_invalid`] and
/// [`payload_promotion_invalid`], which differ only in `valid`.
fn promotion_invalid(
    cols: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
    valid: impl Fn(u8, u8) -> bool,
) -> bool {
    cols.iter().enumerate().any(|(i, &c)| {
        let t = target_tcs.get(i).copied().unwrap_or(0);
        t != 0 && !valid(schema.columns[c as usize].type_code, t)
    })
}

/// The reindex **key** domain: a carried target `t` must be exactly the
/// promotion the planner derives for a key of this source type. Rather than
/// re-deriving the sign/width ladder by hand (and drifting from the planner),
/// validate against the single shared rule — `t` is a value-preserving promotion
/// of `src` iff `join_key_common_type` maps the pair `(src, t)` back to `t`. The
/// promotion is idempotent — promoting a source against its own carried target
/// is a no-op — so this is exactly the planner's `carried_reindex_tc` contract
/// read back (a `#[test]` pins the idempotency). It also screens PK-ineligible
/// targets for free, since the function only ever yields PK-eligible types, and
/// admits the 16-byte OPK targets the key region supports.
pub(super) fn key_promotion_invalid(cols: &[u32], target_tcs: &[u8], schema: &SchemaDescriptor) -> bool {
    promotion_invalid(cols, target_tcs, schema, |src, t| {
        gnitz_wire::join_key_common_type(src, t) == Some(t)
    })
}

/// The **payload** copy domain: the ≤8-byte fixed-int widen, which is the only
/// promotion the copy kernel supports. Identical to the rule `check_copy_types`
/// holds a COPY_COL destination to — the HashRow payload widen is that same
/// kernel — so it is narrower than [`key_promotion_invalid`], not a mode of it.
pub(super) fn payload_promotion_invalid(cols: &[u32], target_tcs: &[u8], schema: &SchemaDescriptor) -> bool {
    promotion_invalid(cols, target_tcs, schema, gnitz_wire::is_widening_promotion)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn};

    /// 3-column compound PK `(U32, U64, U64)` + one payload, so the columns have
    /// distinct widths and a prefix stride is unambiguous.
    fn three_col_pk_schema(dist_k: u8) -> SchemaDescriptor {
        three_col_placed(Placement::Keyed { prefix_len: dist_k })
    }

    fn three_col_placed(placement: Placement) -> SchemaDescriptor {
        SchemaDescriptor::new_with_placement(
            &[
                SchemaColumn::new(type_code::U32, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1, 2],
            placement,
        )
    }

    #[test]
    fn shard_cols_match_dist_key_is_exact_prefix() {
        let k1 = three_col_pk_schema(1); // CLUSTER BY col0
                                         // Exact prefix at k=1 matches; the full PK and a super-prefix do not.
        assert!(shard_cols_match_dist_key(&k1, &[0]));
        assert!(!shard_cols_match_dist_key(&k1, &[0, 1]), "super-prefix must NOT match");
        assert!(!shard_cols_match_dist_key(&k1, &[0, 1, 2]));
        assert!(!shard_cols_match_dist_key(&k1, &[1]), "non-leading column");
        assert!(!shard_cols_match_dist_key(&k1, &[]));

        // Default (full-PK) schema: dist key is the whole PK, exactly.
        let full = three_col_pk_schema(0);
        assert!(shard_cols_match_dist_key(&full, &[0, 1, 2]));
        assert!(
            !shard_cols_match_dist_key(&full, &[0]),
            "a single component is not the full key"
        );
        assert!(!shard_cols_match_dist_key(&full, &[0, 1]));

        // k=2 matches exactly [0,1], not [0] and not [0,1,2].
        let k2 = three_col_pk_schema(2);
        assert!(shard_cols_match_dist_key(&k2, &[0, 1]));
        assert!(!shard_cols_match_dist_key(&k2, &[0]));
        assert!(!shard_cols_match_dist_key(&k2, &[0, 1, 2]));
    }

    /// A relation whose rows are not placed by `worker_for_pk` has no shard
    /// key that names where they already are — whatever its PK columns look
    /// like. This is what stops a co-partition/exchange elision from firing onto
    /// an unkeyed source.
    #[test]
    fn shard_cols_never_match_an_unkeyed_placement() {
        for p in [Placement::Replicated, Placement::Local] {
            let s = three_col_placed(p);
            assert!(!shard_cols_match_dist_key(&s, &[0, 1, 2]), "{p:?}: full PK");
            assert!(!shard_cols_match_dist_key(&s, &[0]), "{p:?}: leading column");
            assert!(!shard_cols_match_dist_key(&s, &[]), "{p:?}: empty key");
        }
    }

    #[test]
    fn test_compute_co_partitioned_strict_full_pk_sequence() {
        // Compound PK (a, b) at columns 0, 1; column 2 is payload.
        let compound = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        let ext: ExtTables = HashMap::from([(7, compound)]);
        let co = |cols: Vec<(u32, u8)>| {
            let mut m = HashMap::new();
            m.insert(7i64, cols);
            compute_co_partitioned(&m, &ext).contains(&7)
        };
        // Only the exact PK sequence in schema order co-partitions.
        assert!(
            co(vec![(0, 0), (1, 0)]),
            "shard [pk0, pk1] equals pk_indices() → co-partitioned"
        );
        assert!(!co(vec![(0, 0)]), "shard [pk0] alone is not the full PK");
        assert!(!co(vec![(1, 0)]), "shard [pk1] alone is not the full PK");
        assert!(!co(vec![(1, 0), (0, 0)]), "permuted [pk1, pk0] != pk_indices() order");
        // A promoted key (non-zero carried tc) never co-partitions: native PK
        // partitions are at the source width, not the T-wide trace key.
        assert!(
            !co(vec![(0, type_code::I64), (1, 0)]),
            "a promoted PK slot must go through the exchange"
        );

        // Single-PK source: [pk] stays co-partitioned (no regression).
        let single = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let ext1: ExtTables = HashMap::from([(9, single)]);
        let mut m = HashMap::new();
        m.insert(9i64, vec![(0, 0)]);
        assert!(
            compute_co_partitioned(&m, &ext1).contains(&9),
            "single-PK shard [pk] stays co-partitioned"
        );
    }

    #[test]
    fn test_compute_co_partitioned_replicated() {
        // Two single-PK (U64) join sides; the join key is a NON-PK payload column
        // (col 1), so neither side's shard key matches its distribution prefix —
        // the only reason to skip the exchange is replication.
        const COLS: [SchemaColumn; 2] = [
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ];
        let base = || SchemaDescriptor::new(&COLS, &[0]);
        let replicated = SchemaDescriptor::new_with_placement(&COLS, &[0], crate::schema::Placement::Replicated);
        let join_on_payload = || {
            let mut m = HashMap::new();
            m.insert(7i64, vec![(1u32, 0u8)]); // dim  shards on payload col 1
            m.insert(8i64, vec![(1u32, 0u8)]); // fact shards on payload col 1
            m
        };

        // partitioned ⋈ partitioned on a non-PK key: neither side skips.
        let ext_pp: ExtTables = HashMap::from([(7, base()), (8, base())]);
        let co = compute_co_partitioned(&join_on_payload(), &ext_pp);
        assert!(
            !co.contains(&7) && !co.contains(&8),
            "two partitioned sides on a non-PK key both go through the exchange"
        );

        // partitioned fact ⋈ REPLICATED dim: BOTH skip — the dim because it is
        // replicated, the fact because its join partner is replicated (it stays in
        // its own PK partitioning and joins the full local dim copy).
        let ext_pr: ExtTables = HashMap::from([(7, replicated), (8, base())]);
        let co = compute_co_partitioned(&join_on_payload(), &ext_pr);
        assert!(co.contains(&7), "a replicated source always skips its exchange");
        assert!(
            co.contains(&8),
            "a partitioned fact skips when its partner is replicated"
        );

        // replicated ⋈ replicated: both skip (output is replicated; single-sourced on read).
        let ext_rr: ExtTables = HashMap::from([(7, replicated), (8, replicated)]);
        let co = compute_co_partitioned(&join_on_payload(), &ext_rr);
        assert!(
            co.contains(&7) && co.contains(&8),
            "replicated ⋈ replicated: both sides skip"
        );

        // A replicated source skips even with a promoted (non-zero tc) key: the
        // write broadcast already placed its full trace on every worker, so the
        // tc-promotion exchange gate (which blocks a partitioned source) does not apply.
        let ext_r: ExtTables = HashMap::from([(7, replicated)]);
        let mut promoted = HashMap::new();
        promoted.insert(7i64, vec![(0u32, type_code::I64)]);
        assert!(
            compute_co_partitioned(&promoted, &ext_r).contains(&7),
            "replicated source skips regardless of carried type-promotion"
        );
    }

    // ── compute_scatter_routing covers ScanDelta (SQL-planner join pattern) ──

    /// The routing walk must find ScanDelta → Map(reindex) chains.
    #[test]
    fn test_scatter_routing_scan_delta() {
        use gnitz_wire::{MapKind, OpNode};

        // Minimal two-sided SQL join circuit skeleton:
        //   ScanDelta(left_tid=10) → Map(reindex_col=1) → Join → IntegrateSink
        //   ScanDelta(right_tid=20) → Map(reindex_col=0) → Join
        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(
            1,
            OpNode::Map(MapKind::Reindex {
                program: dummy_blob.clone(),
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(2, scan_delta(20));
        nodes.insert(
            3,
            OpNode::Map(MapKind::Reindex {
                program: dummy_blob,
                reindex_cols: vec![0],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(4, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(5, OpNode::IntegrateSink);
        let edges = vec![
            (0, 1, PORT_IN),
            (2, 3, PORT_IN),
            (1, 4, PORT_IN_A),
            (3, 4, PORT_TRACE),
            (4, 5, PORT_IN),
        ];
        let loaded = loaded_for_test(nodes, edges);

        let keys = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;

        assert_eq!(
            keys.get(&10),
            Some(&Some(vec![(1, 0)])),
            "left side (source 10) must map to reindex_col=1"
        );
        assert_eq!(
            keys.get(&20),
            Some(&Some(vec![(0, 0)])),
            "right side (source 20) must map to reindex_col=0"
        );
    }

    #[test]
    fn test_scatter_routing_through_filter() {
        use gnitz_wire::{MapKind, OpNode};
        // ScanDelta(42) → Filter → Map(reindex_col=1) → Join → IntegrateSink.
        // The reindex Map is two hops from the scan (a Filter sits between),
        // so the one-hop lookup misses it; BFS through Filter must find it.
        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(42));
        nodes.insert(1, OpNode::Filter(Some(dummy_blob.clone())));
        nodes.insert(
            2,
            OpNode::Map(MapKind::Reindex {
                program: dummy_blob,
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(3, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(4, OpNode::IntegrateSink);
        nodes.insert(5, OpNode::IntegrateTrace);
        let edges = vec![
            (0, 1, PORT_IN), // ScanDelta → Filter
            (1, 2, PORT_IN), // Filter → reindex Map
            (2, 3, PORT_IN_A),
            (2, 5, PORT_IN), // reindex Map → its own integral
            (5, 3, PORT_TRACE),
            (3, 4, PORT_IN),
        ];
        let loaded = loaded_for_test(nodes, edges);

        let keys = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;
        assert_eq!(
            keys.get(&42),
            Some(&Some(vec![(1, 0)])),
            "ScanDelta → Filter → Map(reindex) must map source 42 to col 1"
        );
    }

    /// Two `ScanDelta` nodes on ONE source with DIFFERENT scatter keys. Under the
    /// old `loaded.nodes` walk, last-writer-wins over a per-process `RandomState`
    /// order picked one of the two at random — and the master and the worker
    /// derive their halves of the routing in separate processes. Accumulating
    /// over `loaded.ordered` makes the answer the same everywhere: two sequences,
    /// so no single pack key routes the source and the relay refuses.
    #[test]
    fn test_scatter_routing_two_keys_one_source_is_deterministic() {
        use gnitz_wire::{MapKind, OpNode};

        let dummy_blob = dummy_expr_blob();
        let reindex = |col: u32, blob| {
            OpNode::Map(MapKind::Reindex {
                program: blob,
                reindex_cols: vec![col],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            })
        };
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, reindex(1, dummy_blob.clone()));
        nodes.insert(2, scan_delta(10));
        nodes.insert(3, reindex(2, dummy_blob));
        nodes.insert(4, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(5, OpNode::IntegrateSink);
        let edges = vec![
            (0, 1, PORT_IN),
            (2, 3, PORT_IN),
            (1, 4, PORT_IN_A),
            (3, 4, PORT_TRACE),
            (4, 5, PORT_IN),
        ];
        let loaded = loaded_for_test(nodes, edges);

        let keys = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;
        assert_eq!(
            keys.get(&10),
            Some(&None),
            "two distinct keys on one source must refuse, not pick one at random"
        );
    }

    /// The same shape with the SAME key on both scans must still route. Today's
    /// last-writer-wins resolves it to the one correct sequence, so a naive
    /// accumulation that skipped the cross-node dedup would newly produce two
    /// sequences and refuse a round that works — a regression the rewrite must
    /// not introduce.
    #[test]
    fn test_scatter_routing_repeated_key_one_source_still_routes() {
        use gnitz_wire::{MapKind, OpNode};

        let dummy_blob = dummy_expr_blob();
        let reindex = |blob| {
            OpNode::Map(MapKind::Reindex {
                program: blob,
                reindex_cols: vec![1],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            })
        };
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, reindex(dummy_blob.clone()));
        nodes.insert(2, scan_delta(10));
        nodes.insert(3, reindex(dummy_blob));
        nodes.insert(4, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(5, OpNode::IntegrateSink);
        let edges = vec![
            (0, 1, PORT_IN),
            (2, 3, PORT_IN),
            (1, 4, PORT_IN_A),
            (3, 4, PORT_TRACE),
            (4, 5, PORT_IN),
        ];
        let loaded = loaded_for_test(nodes, edges);

        let keys = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;
        assert_eq!(
            keys.get(&10),
            Some(&Some(vec![(1, 0)])),
            "one key reached twice is still one key"
        );
    }

    /// A source whose path to the join carries no reindex Map stays out of the map.
    #[test]
    fn test_scatter_routing_unreindexed_trace_side_absent() {
        use gnitz_wire::{MapKind, OpNode};

        let dummy_blob = dummy_expr_blob();
        let mut nodes = HashMap::new();
        nodes.insert(0, scan_delta(10));
        nodes.insert(1, scan_delta(20));
        nodes.insert(
            2,
            OpNode::Map(MapKind::Reindex {
                program: dummy_blob,
                reindex_cols: vec![2],
                reindex_target_tcs: vec![],
                role: gnitz_wire::ReindexRole::ScatterKey,
            }),
        );
        nodes.insert(3, OpNode::Join(gnitz_wire::JoinKind::DeltaTrace));
        nodes.insert(4, OpNode::IntegrateSink);
        nodes.insert(5, OpNode::IntegrateTrace);
        let edges = vec![
            (0, 2, PORT_IN),    // ScanDelta → reindex Map
            (1, 5, PORT_IN),    // ScanDelta(20) → IntegrateTrace (no reindex)
            (5, 3, PORT_TRACE), // trace → join trace port
            (2, 3, PORT_IN_A),
            (3, 4, PORT_IN),
        ];
        let loaded = loaded_for_test(nodes, edges);

        let map = CircuitFacts::derive(&loaded, &ExtTables::default()).unwrap().keys;

        // ScanDelta(10) → Map(reindex_col=2) must be found.
        assert_eq!(
            map.get(&10),
            Some(&Some(vec![(2, 0)])),
            "ScanDelta source must be in the routing map"
        );
        // Source 20 has no downstream reindex Map — must NOT appear.
        assert!(
            !map.contains_key(&20),
            "a source with no reindex Map must not be in the routing map"
        );
    }
}
