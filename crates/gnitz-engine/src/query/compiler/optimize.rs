//! Annotation + optimization passes and the schema-construction helpers:
//! co-partition analysis, distinct elision, and the join/reduce/map output
//! schemas.

use super::*;

/// The node feeding `nid` on `port`, or `None` if there is no such input edge (a
/// malformed circuit). Every "who produces this operand" question in the
/// compiler resolves through this one lookup.
pub(super) fn input_on_port(loaded: &LoadedCircuit, nid: i32, port: i32) -> Option<i32> {
    loaded
        .incoming
        .get(&nid)
        .and_then(|ins| ins.iter().find(|&&(_, p)| p == port))
        .map(|&(src, _)| src)
}

/// A view's join/group scatter routing: both projections of ONE walk over the
/// circuit's `ScanDelta` nodes. They answer different questions about the same
/// fact — the distinct `ScatterKey` sequences each source's scans feed — and
/// deriving them apart let them disagree.
pub(crate) struct ScatterRouting {
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
}

/// Derive a view's [`ScatterRouting`].
///
/// Walks `loaded.ordered` — total over `nodes`, else `topo_sorted` would have
/// returned `Cycle` — and NOT `loaded.nodes`, whose `RandomState` iteration order
/// differs per process: the master derives `keys` in its process while the worker
/// derives `scatter_sources` in its own, so a per-process order could route the
/// two differently.
pub(crate) fn compute_scatter_routing(loaded: &LoadedCircuit, ext_tables: &ExtTables) -> ScatterRouting {
    // source → the distinct sequences its scans feed. Deduping ACROSS scan nodes
    // mirrors the within-node dedup in `scatter_key_of_scan`: two scans on one
    // source carrying the same key must still resolve to one sequence, or the
    // pack-key gate below would newly refuse a round that routes fine today.
    let mut seqs: HashMap<i64, Vec<Vec<(u32, u8)>>> = HashMap::new();
    for &nid in &loaded.ordered {
        let Some(gnitz_wire::OpNode::ScanDelta { source, .. }) = loaded.nodes.get(&nid) else {
            continue;
        };
        let node_seqs = load::scatter_key_of_scan(loaded, nid).0;
        // WITHIN one scan the `ScatterKey` role filter and the identical-sequence
        // dedup appear to hold every live circuit to one sequence, so a second one
        // means the shape became constructible and wants a real plan, not a silent
        // refusal. Across scan nodes it is only a tripwire, not a rule — see the
        // pack-key projection below.
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

    // The co-partition prefix test wants the sequences CONCATENATED, and wants a
    // conservative refusal when a source carries more than one key: the
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
    // `pack(a ‖ b)` while each trace side is keyed by `pack(a)` or `pack(b)`, so
    // the two would never co-partition and matches would drop silently. Refusing
    // is the only safe answer, and it is the relay's to report.
    //
    // Two scans of one source carrying DIFFERENT keys is not SQL-reachable — the
    // planner wraps a repeated tid in a pass-through segment before lowering a
    // join — but a hand-built wire circuit can express it, and refusing is what
    // this walk owes it. Deliberately NOT a `debug_assert`, unlike the per-scan
    // tripwire above.
    let keys = seqs
        .into_iter()
        .map(|(tid, mut s)| (tid, (s.len() == 1).then(|| s.pop().expect("len == 1"))))
        .collect();

    ScatterRouting { keys, scatter_sources }
}

pub(super) fn compute_co_partitioned(join_shard_map: &JoinShardMap, ext_tables: &ExtTables) -> HashSet<i64> {
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
    let any_replicated = join_shard_map.keys().any(|tid| {
        ext_tables
            .get(tid)
            .is_some_and(|schema| schema.placement().is_replicated())
    });
    let mut co_partitioned = HashSet::new();
    for (&tid, cols) in join_shard_map {
        let Some(ext_schema) = ext_tables.get(&tid) else {
            continue;
        };

        if any_replicated {
            co_partitioned.insert(tid);
            continue;
        }

        // A non-zero carried tc means the slot width differs from the source,
        // so native PK partitions do not align with the T-width trace key — the
        // source must go through the exchange even if its PK matches the key.
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
        if ext_schema.shard_cols_match_dist_key(&col_indices) {
            co_partitioned.insert(tid);
        }
    }
    co_partitioned
}

/// True iff the view's output `ExchangeShard` is a no-op (every row already on
/// the worker owning its distribution key) and the output IPC can be skipped:
/// the shard reads a scan — through any Filter chain — whose distribution prefix
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
pub(crate) fn compute_skips_exchange(loaded: &LoadedCircuit, ext_tables: &ExtTables) -> bool {
    let Some((enid, shard_cols)) = super::output_exchange_shard(loaded) else {
        return false;
    };
    let Some(tid) = scan_tid_through_filters(loaded, enid) else {
        return false;
    };
    ext_tables.get(&tid).is_some_and(|schema| {
        schema.shard_cols_match_dist_key(&shard_cols)
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
        let input_distinct = input_on_port(loaded, nid, PORT_IN).is_some_and(|src| distinct_at.contains(&src));
        match loaded.nodes.get(&nid) {
            Some(gnitz_wire::OpNode::Reduce { .. }) => {
                distinct_at.insert(nid);
            }
            Some(gnitz_wire::OpNode::Distinct) => {
                if input_distinct {
                    skip.insert(nid);
                }
                distinct_at.insert(nid);
            }
            Some(gnitz_wire::OpNode::Filter(_)) => {
                if input_distinct {
                    distinct_at.insert(nid);
                }
            }
            Some(gnitz_wire::OpNode::Map(mk)) => {
                let has_reindex = match mk {
                    gnitz_wire::MapKind::Expression { reindex_cols, .. } => !reindex_cols.is_empty(),
                    gnitz_wire::MapKind::HashRow(..) => true,
                    _ => false,
                };
                if !has_reindex && input_distinct {
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
    cols: &[u16],
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
pub(super) fn key_promotion_invalid(cols: &[u16], target_tcs: &[u8], schema: &SchemaDescriptor) -> bool {
    promotion_invalid(cols, target_tcs, schema, |src, t| {
        gnitz_wire::join_key_common_type(src, t) == Some(t)
    })
}

/// The **payload** copy domain: the ≤8-byte fixed-int widen, which is the only
/// promotion the copy kernel supports. Identical to the rule `check_copy_types`
/// holds a COPY_COL destination to — the HashRow payload widen is that same
/// kernel — so it is narrower than [`key_promotion_invalid`], not a mode of it.
pub(super) fn payload_promotion_invalid(cols: &[u16], target_tcs: &[u8], schema: &SchemaDescriptor) -> bool {
    promotion_invalid(cols, target_tcs, schema, gnitz_wire::is_widening_promotion)
}

pub(super) fn agg_value_idx_eligible(tc: TypeCode) -> bool {
    // The exact order-encodable AVI value set: a narrow (<=8B) fixed int or float.
    // Reuse the canonical predicates instead of a negative variant allow-list, so a
    // future TypeCode is AVI-ineligible by default until explicitly classified.
    // Load-bearing: the excluded types are exactly what keep the `unreachable!` arms
    // of `encode_ordered`/`decode_ordered` (ops/util.rs) unreachable — change the
    // *form* of this predicate freely, but never widen its accepted set without
    // updating those arms.
    is_fixed_int(tc as u8) || tc.is_float()
}
