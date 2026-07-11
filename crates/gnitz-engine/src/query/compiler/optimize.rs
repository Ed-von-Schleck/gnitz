//! Annotation + optimization passes and the schema-construction helpers:
//! co-partition analysis, distinct elision, the reduce-finalize fold, and the
//! join/reduce/map output schemas.

use super::*;
use gnitz_wire::ReduceOutKey;

/// The first (usually sole) node feeding `nid`, if any.
pub(super) fn first_input(loaded: &LoadedCircuit, nid: i32) -> Option<i32> {
    loaded.incoming.get(&nid).and_then(|v| v.first()).map(|&(src, _)| src)
}

pub(crate) fn compute_join_shard_map(loaded: &LoadedCircuit) -> JoinShardMap {
    let mut join_shard_map = HashMap::new();
    for (&nid, op) in &loaded.nodes {
        if let gnitz_wire::OpNode::ScanDelta(tid) = op {
            let rcs = reindex_cols_through_filters(loaded, nid);
            if !rcs.is_empty() {
                join_shard_map.insert(*tid as i64, rcs);
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
    //     dim copy, so no exchange is needed on either side (design §4.5). This is
    //     the case hash co-partitioning cannot serve: the fact need not be
    //     distributed by the join key, so one fact can join many replicated dims.
    let any_replicated = ext_tables
        .iter()
        .any(|(tid, schema)| schema.replicated() && join_shard_map.contains_key(tid));
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
/// the shard reads a scan — through any Filter chain — whose distribution
/// prefix (`pk_indices[..k]`) is exactly the shard key. For a default full-PK
/// table that is the strict full-PK case; for a `CLUSTER BY prefix` table it
/// also lets a (possibly filtered) `GROUP BY prefix` / reduce run locally —
/// every row for a group value already lives on one worker — since this
/// governs every single-source `ExchangeShard` view, not just joins.
pub(super) fn compute_skips_exchange(loaded: &LoadedCircuit, ext_tables: &ExtTables) -> bool {
    let Some((enid, shard_cols)) = loaded.nodes.iter().find_map(|(&nid, op)| match op {
        gnitz_wire::OpNode::ExchangeShard { shard_cols } => {
            Some((nid, shard_cols.iter().map(|&c| c as i32).collect::<Vec<_>>()))
        }
        _ => None,
    }) else {
        return false;
    };
    let Some(tid) = scan_tid_through_filters(loaded, enid) else {
        return false;
    };
    ext_tables
        .get(&tid)
        .is_some_and(|schema| schema.shard_cols_match_dist_key(&shard_cols))
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

/// Fold budget: inline a finalize MAP into its reduce node only when its program
/// is at most this many instructions; larger finalize MAPs stay a separate node
/// so a reduce op never carries an unbounded inline program.
const MAX_FOLD_FINALIZE_INSTRS: usize = 15;

/// The sole consuming MAP of `reduce_nid` when it is fold-eligible, with its
/// decoded finalize program. Consulted by `emit_reduce`, which absorbs the MAP
/// into the reduce's finalize slot (saving one intermediate batch
/// materialization per tick) and marks the MAP node folded.
///
/// Eligibility: the reduce has exactly one outgoing edge, to a reindex-free
/// `Map(Expression)` whose only input is the reduce, and whose program is small
/// enough and neither const-using nor a plain block copy.
pub(super) fn reduce_finalize_fold(loaded: &LoadedCircuit, reduce_nid: i32) -> Option<(i32, LogicalProgram)> {
    let outs = loaded.outgoing.get(&reduce_nid)?;
    if outs.len() != 1 {
        return None;
    }
    let map_nid = outs[0].0;
    let blob = match loaded.nodes.get(&map_nid) {
        Some(gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
            program, reindex_cols, ..
        })) if reindex_cols.is_empty() => program,
        _ => return None,
    };
    if loaded.incoming.get(&map_nid).map(|v| v.len()).unwrap_or(0) != 1 {
        return None;
    }
    let dep = gnitz_wire::decode_expr_blob(blob)?;
    // Empty const pool (structural check): a const-using finalize returns `Err`
    // (`StrColConst` vs the empty pool) → not folded → compiled normally through
    // the Map path with its real pool, which also removes a latent crash (the
    // empty pool used to be folded in and panic at eval).
    let prog = LogicalProgram::from_wire(&dep.code, dep.num_regs, 0, Vec::new()).ok()?;
    // Skip folding any clean block copy regardless of offset; the authoritative
    // identity-MAP elision (with the schema-checked PK offset) handles it.
    if prog.sequential_copy_base().is_some() {
        return None;
    }
    if prog.len() > MAX_FOLD_FINALIZE_INSTRS {
        return None;
    }
    Some((map_nid, prog))
}

// ---------------------------------------------------------------------------
// Schema construction helpers
// ---------------------------------------------------------------------------

/// Copy `schema`'s PK columns to `cols[..pk_count]` and populate `pk_idx`
/// with `[0, 1, ..., pk_count - 1]`. Returns `pk_count`. PK columns always
/// occupy the leading positions of the output schema, so the new PK
/// indices are dense and identical to the loop counter.
pub(super) fn copy_pk_columns_into(
    schema: &SchemaDescriptor,
    cols: &mut [SchemaColumn],
    pk_idx: &mut [u32; crate::schema::MAX_PK_COLUMNS],
) -> usize {
    let mut k = 0;
    for (_, _, c) in schema.pk_columns() {
        cols[k] = *c;
        pk_idx[k] = k as u32;
        k += 1;
    }
    k
}

/// Whether a join merges the right side's payload columns as-is or marks them
/// nullable (outer-join null-fill).
#[derive(Clone, Copy)]
pub(super) enum JoinNullFill {
    None,
    RightNullable,
}

/// `None` when the merged column count would overflow the fixed `[_; 65]` schema
/// array — a crafted/corrupt `NullExtend` or `Join` node; the caller fails the
/// compile rather than aborting.
pub(super) fn merge_schemas_for_join(
    left: &SchemaDescriptor,
    right: &SchemaDescriptor,
    fill: JoinNullFill,
) -> Option<SchemaDescriptor> {
    let total = left.num_columns() + right.num_payload_cols();
    if total > crate::schema::MAX_COLUMNS {
        return None;
    }
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let mut pk_idx = [0u32; crate::schema::MAX_PK_COLUMNS];
    let pk_len = copy_pk_columns_into(left, &mut cols, &mut pk_idx);
    let mut n = pk_len;
    for (_, _, c) in left.payload_columns() {
        cols[n] = *c;
        n += 1;
    }
    for (_, _, c) in right.payload_columns() {
        let mut c = *c;
        if matches!(fill, JoinNullFill::RightNullable) {
            c.nullable = 1;
        }
        cols[n] = c;
        n += 1;
    }
    Some(SchemaDescriptor::new(&cols[..n], &pk_idx[..pk_len]))
}

pub(super) fn build_map_output_schema(input: &SchemaDescriptor, src_indices: &[i32]) -> SchemaDescriptor {
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let mut pk_idx = [0u32; crate::schema::MAX_PK_COLUMNS];
    let pk_len = copy_pk_columns_into(input, &mut cols, &mut pk_idx);
    let mut n = pk_len;
    for &idx in src_indices {
        let i = idx as usize;
        if !input.is_pk_col(i) {
            cols[n] = input.columns[i];
            n += 1;
        }
    }
    SchemaDescriptor::new(&cols[..n], &pk_idx[..pk_len])
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
pub(super) fn reindex_output_schema(
    in_schema: &SchemaDescriptor,
    reindex_cols: &[u16],
    target_tcs: &[u8],
    payload_cols: &[u16],
) -> SchemaDescriptor {
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let pk_n = reindex_cols.len();
    let payload_n = payload_cols.len();
    // Self-protecting: a future caller that skips `emit_node`'s guard would
    // otherwise hit a bare slice OOB in the `cols[pk_n + i]` writes below.
    assert!(
        pk_n + payload_n <= crate::schema::MAX_COLUMNS,
        "reindex_output_schema: {pk_n} + {payload_n} columns exceed MAX_COLUMNS ({})",
        crate::schema::MAX_COLUMNS,
    );
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
        cols[i] = SchemaColumn::new(out_tc, 0); // PK region: nullable = 0
    }
    for (i, &c) in payload_cols.iter().enumerate() {
        cols[pk_n + i] = in_schema.columns[c as usize];
    }
    let pk_idx: Vec<u32> = (0..pk_n as u32).collect();
    SchemaDescriptor::new(&cols[..pk_n + payload_n], &pk_idx)
}

/// Engine adapter over the single shared typing rule
/// (`gnitz_wire::agg_output_type`); `AggOp::Null` — the grouped no-aggregate
/// placeholder, which never leaves the engine — types I64 like COUNT.
pub(super) const fn agg_output_type(agg_op: AggOp, col_type_code: TypeCode) -> u8 {
    let func = match agg_op {
        AggOp::Count | AggOp::Null => gnitz_wire::AggFunc::Count,
        AggOp::CountNonNull => gnitz_wire::AggFunc::CountNonNull,
        AggOp::SumZero => gnitz_wire::AggFunc::SumZero,
        AggOp::Sum => gnitz_wire::AggFunc::Sum,
        AggOp::Min => gnitz_wire::AggFunc::Min,
        AggOp::Max => gnitz_wire::AggFunc::Max,
    };
    gnitz_wire::agg_output_type(func, col_type_code as u8)
}

/// Build the reduce output schema by **obeying** the planner's shipped
/// `out_key`. The caller (`emit_reduce`) validates `out_key` against the input
/// schema (`SchemaDescriptor::reduce_out_key`) before calling this, so the
/// three arms are byte-identical to what the planner laid out.
pub(super) fn build_reduce_output_schema(
    input: &SchemaDescriptor,
    group_cols: &[u32],
    agg_descs: &[AggDescriptor],
    out_key: ReduceOutKey,
) -> SchemaDescriptor {
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let mut n = 0;
    let mut pk_idx = [0u32; crate::schema::MAX_PK_COLUMNS];
    let mut pk_len = 0usize;

    match out_key {
        ReduceOutKey::PkPermutation => {
            // Output PK region mirrors the source's PK byte layout: walk
            // `pk_columns()` in pk-list order rather than `group_cols` order.
            let k = copy_pk_columns_into(input, &mut cols, &mut pk_idx);
            n = k;
            pk_len = k;
        }
        ReduceOutKey::SingleNaturalCol => {
            // A single non-PK-or-PK natural group column keyed directly (e.g.
            // GROUP BY a U64 payload column, where `pk_columns()` would name the
            // wrong column).
            cols[n] = input.columns[group_cols[0] as usize];
            pk_idx[pk_len] = n as u32;
            pk_len += 1;
            n += 1;
        }
        ReduceOutKey::SyntheticFold => {
            // Synthetic U128 PK, group columns as payload.
            cols[n] = SchemaColumn::new(type_code::U128, 0);
            pk_idx[pk_len] = n as u32;
            pk_len += 1;
            n += 1;
            for &gc in group_cols {
                cols[n] = input.columns[gc as usize];
                n += 1;
            }
        }
    }
    // Aggregate results (same for all arms)
    for ad in agg_descs {
        cols[n] = SchemaColumn::new(agg_output_type(ad.agg_op, ad.col_type_code), 0);
        n += 1;
    }
    SchemaDescriptor::new(&cols[..n], &pk_idx[..pk_len])
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
