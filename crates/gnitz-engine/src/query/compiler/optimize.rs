//! Annotation + optimization passes and the schema-construction helpers:
//! co-partition analysis, distinct elision, and the join/reduce/map output
//! schemas.

use super::*;

/// The first (usually sole) node feeding `nid`, if any.
pub(super) fn first_input(loaded: &LoadedCircuit, nid: i32) -> Option<i32> {
    loaded.incoming.get(&nid).and_then(|v| v.first()).map(|&(src, _)| src)
}

pub(crate) fn compute_join_shard_map(loaded: &LoadedCircuit) -> JoinShardMap {
    let mut join_shard_map = HashMap::new();
    for (&nid, op) in &loaded.nodes {
        if let gnitz_wire::OpNode::ScanDelta { source, .. } = op {
            let rcs = reindex_cols_through_filters(loaded, nid);
            if !rcs.is_empty() {
                join_shard_map.insert(*source as i64, rcs);
            }
        }
    }
    join_shard_map
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
        let col_indices: Vec<i32> = cols.iter().map(|&(c, _)| c).collect();
        if ext_schema.shard_cols_match_dist_key(&col_indices) {
            co_partitioned.insert(tid);
        }
    }
    co_partitioned
}

/// The circuit's join-routing annotations: the all-sources join-shard map
/// (source tid → reindex `(col, promotion tc)` pairs) and the co-partition set —
/// the sources whose deltas may skip the join scatter because their native
/// distribution already aligns with the join key (or a replicated partner makes
/// the exchange unnecessary).
pub(super) fn annotate(loaded: &LoadedCircuit, ext_tables: &ExtTables) -> (JoinShardMap, HashSet<i64>) {
    let join_shard_map = compute_join_shard_map(loaded);
    let co_partitioned = compute_co_partitioned(&join_shard_map, ext_tables);
    (join_shard_map, co_partitioned)
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
pub(super) fn compute_skips_exchange(loaded: &LoadedCircuit, ext_tables: &ExtTables) -> bool {
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
        let input_distinct = first_input(loaded, nid).is_some_and(|src| distinct_at.contains(&src));
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

// ---------------------------------------------------------------------------
// Schema construction helpers
// ---------------------------------------------------------------------------

/// `None` when the merged column count would overflow the fixed `[_; 65]` schema
/// array — a crafted/corrupt `Join` node; the caller fails the compile rather
/// than aborting. (The outer-join null-fill columns are appended by
/// [`null_extend_output_schema`], not here.)
pub(super) fn merge_schemas_for_join(left: &SchemaDescriptor, right: &SchemaDescriptor) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(left)?;
    for (_, c) in left.payload_columns().chain(right.payload_columns()) {
        b.push(*c)?;
    }
    Some(b.finish())
}

/// Output schema of a plain projection Map: the input's PK columns, then the
/// non-PK sources in `src_indices` order.
///
/// `None` when the merged column count would overflow the fixed `[_; 65]` schema
/// array — a crafted/corrupt `Projection` node; the caller fails the compile
/// rather than aborting. Self-protecting like [`merge_schemas_for_join`], and
/// for the same reason: the bound is `pk_len + payload_n`, so a caller checking
/// only `src_indices.len()` is short by the PK count. `src_indices` may repeat an
/// index, so its length alone does not bound the payload count either.
pub(super) fn build_map_output_schema(input: &SchemaDescriptor, src_indices: &[i32]) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(input)?;
    for &idx in src_indices {
        let i = idx as usize;
        if !input.is_pk_col(i) {
            b.push(input.columns[i])?;
        }
    }
    Some(b.finish())
}

/// True iff any carried cross-width promotion target in `target_tcs` is invalid
/// for its source column in `cols`. A carried target `t` must be exactly the
/// promotion the planner derives for a key of this source type: rather than
/// re-deriving the sign/width ladder by hand (and drifting from the planner),
/// validate against the single shared rule — `t` is a value-preserving
/// promotion of `src` iff `join_key_common_type` maps the pair `(src, t)` back
/// to `t`. The promotion is idempotent — promoting a source against its own
/// carried target is a no-op — so this is exactly the planner's
/// `carried_reindex_tc` contract read back (a `#[test]` pins the idempotency).
/// It also screens PK-ineligible targets for free, since the function only
/// ever yields PK-eligible types. `fixed_int_only` additionally restricts
/// targets to the ≤8-byte fixed-int domain (the HashRow payload widen is the
/// only copy path that supports it). A violation means a corrupt/forged
/// catalog; callers abort the compile cleanly rather than panic/truncate in
/// the copy kernels. Callers must have range-checked `cols` first.
pub(super) fn reindex_promotion_invalid(
    cols: &[u16],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
    fixed_int_only: bool,
) -> bool {
    cols.iter().enumerate().any(|(i, &c)| {
        let t = target_tcs.get(i).copied().unwrap_or(0);
        t != 0 && {
            let src = schema.columns[c as usize].type_code;
            if fixed_int_only {
                // Identical to the rule `check_copy_types` holds a COPY_COL
                // destination to — the payload widen is the same kernel.
                !gnitz_wire::is_widening_promotion(src, t)
            } else {
                // The reindex key path also admits the 16-byte OPK targets, so
                // it takes the promotion ladder without the fixed-int clause.
                gnitz_wire::join_key_common_type(src, t) != Some(t)
            }
        }
    })
}

/// Output schema of a HashRow (set-op full-row identity) Map: a synthetic U128
/// PK at slot 0, then the projected payload columns. `target_tcs[j] != 0`
/// promotes payload column `j` to that ≤8-byte integer type (cross-width
/// set-op coercion) — `new` re-derives size/signedness for the promoted type —
/// keeping THIS SIDE's nullability. Per-side, not the operator-merged view
/// nullability: an INTERSECT/EXCEPT leaf is `distinct`-ed on its own before
/// the tuple-tightening combine, so its row comparator must classify by what
/// this side can actually emit.
/// `None` when the synthetic PK plus the projected columns would overflow the
/// fixed `[_; 65]` schema array — self-protecting like [`merge_schemas_for_join`]
/// and [`build_map_output_schema`], so no caller owns the bound.
pub(super) fn hashrow_output_schema(
    in_schema: &SchemaDescriptor,
    proj_cols: &[u16],
    target_tcs: &[u8],
) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk(SchemaColumn::new(type_code::U128, 0))?;
    for (j, &c) in proj_cols.iter().enumerate() {
        let src = in_schema.columns[c as usize];
        let tgt = target_tcs.get(j).copied().unwrap_or(0);
        let out_tc = if tgt != 0 { tgt } else { src.type_code };
        b.push(SchemaColumn::new(out_tc, src.nullable))?;
    }
    Some(b.finish())
}

/// Output schema of an outer-join NULL_EXTEND: the input schema verbatim (PK
/// region unchanged), then one nullable column per null-fill `type_codes` entry.
/// `decode_op_node` rejects an undecodable type code, so every entry is a real
/// column type.
/// `None` when the merged column count would overflow the fixed `[_; 65]` schema
/// array — self-protecting like the sibling builders.
pub(super) fn null_extend_output_schema(in_schema: &SchemaDescriptor, type_codes: &[u8]) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(in_schema)?;
    for (_, c) in in_schema.payload_columns() {
        b.push(*c)?;
    }
    for &tc in type_codes {
        b.push(SchemaColumn::new(tc, 1))?;
    }
    Some(b.finish())
}

/// Build the full output schema of a reindex Map: the synthetic PK column(s)
/// derived from `reindex_cols` (in key order), followed by the kept payload
/// columns. Each PK slot's width is `gnitz_wire::resolve_reindex_type` — the
/// carried cross-width promotion target `T_i` when non-zero, else the per-column
/// default policy (a ≤8-byte integer key keeps its native width; everything else —
/// U128/UUID, the STRING/BLOB content hash, PK-ineligible floats — becomes U128).
/// This is the same carried-or-derive rule the planner's `_join_pk` stamp uses,
/// so the engine and catalog strides stay in lockstep. Narrowing is safe for
/// every view: reindex traces are non-durable and re-derived from the source.
///
/// `payload_cols` places exactly `in_schema.columns[payload_cols[i]]` at payload
/// slot `i` — the source columns the reindex program copies, derived from the
/// program (and range-checked) by `emit_node`. A join side whose program skips a
/// dead source column thus stops persisting it in the trace.
/// `None` when the synthetic PK plus the kept payload columns would overflow the
/// fixed `[_; 65]` schema array — self-protecting like the sibling builders, so
/// the bound lives with the array rather than at each caller.
pub(super) fn reindex_output_schema(
    in_schema: &SchemaDescriptor,
    reindex_cols: &[u16],
    target_tcs: &[u8],
    payload_cols: &[u16],
) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    for (i, &c) in reindex_cols.iter().enumerate() {
        let out_tc = gnitz_wire::resolve_reindex_type(
            in_schema.columns[c as usize].type_code,
            target_tcs.get(i).copied().unwrap_or(0),
        );
        // decode (the catalog trust boundary) already rejects a non-PK-eligible
        // carried tc; this is the engine-internal backstop that a planner/compiler
        // bug cannot stamp a float or other ineligible type into the PK region.
        debug_assert!(
            gnitz_wire::is_pk_eligible(out_tc),
            "reindex output type code {out_tc} is not PK-eligible"
        );
        b.push_pk(SchemaColumn::new(out_tc, 0))?; // PK region: nullable = 0
    }
    for &c in payload_cols {
        b.push(in_schema.columns[c as usize])?;
    }
    Some(b.finish())
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

/// The combined AVI stores its key as a fixed-width byte prefix
/// `group_cols ‖ ordinal(u8) ‖ av_encoded`. A group key is byte-form-eligible
/// iff every group column is a non-nullable, fixed-width, non-float scalar (a
/// valid PK-column type) and the composite key — group stride **plus the
/// 1-byte ordinal plus the av value** — fits the composite PK budget
/// (`MAX_PK_COLUMNS` columns, `MAX_PK_BYTES` bytes). The byte-form cursor
/// (drive, seek, consolidation) orders by `compare_pk_bytes`, so any stride up
/// to the engine PK limit is wide-safe; only column type/count and the byte
/// budget gate eligibility. The empty global key (`gcols = []`) stays eligible
/// (`0 + 2 ≤ MAX_PK_COLUMNS`), so a global MIN/MAX always resolves via the index.
pub(super) fn avi_group_key_eligible(schema: &SchemaDescriptor, gcols: &[u32]) -> bool {
    // group cols + ordinal column + av column must fit the PK-column budget.
    if gcols.len() + 2 > crate::schema::MAX_PK_COLUMNS {
        return false;
    }
    let mut stride = 0usize;
    for &c in gcols {
        let col = &schema.columns[c as usize];
        if col.nullable != 0 {
            return false;
        }
        // A byte-prefix group key is exactly a valid PK-column type: fixed-width,
        // non-float, byte-comparable. Reuse the single PK-eligibility predicate
        // rather than re-listing the variants here.
        if !TypeCode::from_validated_u8(col.type_code).is_pk_eligible() {
            return false; // STRING / BLOB / F32 / F64 — fall back to trace scan
        }
        stride += col.size() as usize;
    }
    // + 1 ordinal byte + the order-encoded value.
    let key_bytes = stride + 1 + crate::ops::AVI_AV_BYTES;
    key_bytes <= crate::schema::MAX_PK_BYTES
}
