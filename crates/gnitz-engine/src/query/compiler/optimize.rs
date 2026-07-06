//! Annotation + optimization passes and the schema-construction helpers:
//! `annotate`, `opt_*`, co-partition analysis, and join/reduce/map output schemas.

use super::*;

pub(super) fn compute_join_shard_map(loaded: &LoadedCircuit) -> HashMap<i64, Vec<(i32, u8)>> {
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

pub(super) fn compute_co_partitioned(
    join_shard_map: &HashMap<i64, Vec<(i32, u8)>>,
    ext_tables: &[ExternalTable],
) -> HashSet<i64> {
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
        .any(|t| t.schema.replicated() && join_shard_map.contains_key(&t.table_id));
    let mut co_partitioned = HashSet::new();
    for (&tid, cols) in join_shard_map {
        let Some(ext) = ext_tables.iter().find(|t| t.table_id == tid) else {
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
        if ext.schema.shard_cols_match_dist_key(&col_indices) {
            co_partitioned.insert(tid);
        }
    }
    co_partitioned
}

pub(super) fn propagate_distinct(loaded: &LoadedCircuit, ann: &mut Annotation) {
    for &nid in &loaded.ordered {
        match loaded.nodes.get(&nid) {
            Some(gnitz_wire::OpNode::Reduce { .. }) | Some(gnitz_wire::OpNode::Distinct) => {
                ann.is_distinct_at.insert(nid);
            }
            Some(gnitz_wire::OpNode::Filter(_)) => {
                let in_nids = loaded.incoming.get(&nid).cloned().unwrap_or_default();
                if !in_nids.is_empty() && ann.is_distinct_at.contains(&in_nids[0].0) {
                    ann.is_distinct_at.insert(nid);
                }
            }
            Some(gnitz_wire::OpNode::Map(mk)) => {
                // A reindex (equijoin pre-index or full-row HashRow) rewrites the
                // PK, so upstream distinctness no longer holds at this node.
                let has_reindex = match mk {
                    gnitz_wire::MapKind::Expression { reindex_cols, .. } => !reindex_cols.is_empty(),
                    gnitz_wire::MapKind::HashRow(..) => true,
                    _ => false,
                };
                if !has_reindex {
                    let in_nids = loaded.incoming.get(&nid).cloned().unwrap_or_default();
                    if !in_nids.is_empty() && ann.is_distinct_at.contains(&in_nids[0].0) {
                        ann.is_distinct_at.insert(nid);
                    }
                }
            }
            _ => {}
        }
    }
}

pub(super) fn annotate(loaded: &LoadedCircuit, ext_tables: &[ExternalTable]) -> Annotation {
    let join_shard_map = compute_join_shard_map(loaded);
    let co_partitioned = compute_co_partitioned(&join_shard_map, ext_tables);
    let mut ann = Annotation {
        co_partitioned,
        is_distinct_at: HashSet::new(),
    };
    propagate_distinct(loaded, &mut ann);
    ann
}

// ---------------------------------------------------------------------------
// Optimization passes
// ---------------------------------------------------------------------------

/// Split fold programs and rewrite state between pre- and post-exchange plans.
///
/// `opt_fold_reduce_map` records programs for ALL REDUCE nodes in the graph.
/// When the graph is split at EXCHANGE_SHARD, only the programs for REDUCE
/// nodes in each plan half are valid for that half. This function partitions
/// `progs` and re-indexes `fold_finalize` so each plan half gets zero-based
/// indices into its own program slice.
#[allow(clippy::vec_box)]
pub(super) fn split_fold_programs(
    rw: Rewrites,
    progs: Vec<Box<LogicalProgram>>,
    pre_nids: &HashSet<i32>,
) -> (Rewrites, Vec<Box<LogicalProgram>>, Rewrites, Vec<Box<LogicalProgram>>) {
    // Invert fold_finalize: old program index → reduce_nid
    let mut idx_to_nid: HashMap<usize, i32> = HashMap::new();
    for (&nid, &idx) in &rw.fold_finalize {
        idx_to_nid.insert(idx, nid);
    }

    let mut pre_progs: Vec<Box<LogicalProgram>> = Vec::new();
    let mut post_progs: Vec<Box<LogicalProgram>> = Vec::new();
    let mut pre_fold: HashMap<i32, usize> = HashMap::new();
    let mut post_fold: HashMap<i32, usize> = HashMap::new();

    for (old_idx, prog) in progs.into_iter().enumerate() {
        if let Some(&nid) = idx_to_nid.get(&old_idx) {
            if pre_nids.contains(&nid) {
                let new_idx = pre_progs.len();
                pre_progs.push(prog);
                pre_fold.insert(nid, new_idx);
            } else {
                let new_idx = post_progs.len();
                post_progs.push(prog);
                post_fold.insert(nid, new_idx);
            }
        }
    }

    let rw_pre = Rewrites {
        skip_nodes: rw.skip_nodes.clone(),
        fold_finalize: pre_fold,
        folded_maps: rw.folded_maps.clone(),
    };
    let rw_post = Rewrites {
        skip_nodes: rw.skip_nodes,
        fold_finalize: post_fold,
        folded_maps: rw.folded_maps,
    };

    (rw_pre, pre_progs, rw_post, post_progs)
}

pub(super) fn opt_distinct(loaded: &LoadedCircuit, ann: &Annotation, rw: &mut Rewrites) {
    for (&nid, op) in &loaded.nodes {
        if matches!(op, gnitz_wire::OpNode::Distinct) {
            let in_nids = loaded.incoming.get(&nid).cloned().unwrap_or_default();
            if !in_nids.is_empty() && ann.is_distinct_at.contains(&in_nids[0].0) {
                rw.skip_nodes.insert(nid);
            }
        }
    }
}

/// Fold budget: inline a finalize MAP into its reduce node only when its program
/// is at most this many instructions; larger finalize MAPs stay a separate node
/// so a reduce op never carries an unbounded inline program.
const MAX_FOLD_FINALIZE_INSTRS: usize = 15;

#[allow(clippy::vec_box, clippy::ptr_arg)]
pub(super) fn opt_fold_reduce_map(
    loaded: &LoadedCircuit,
    rw: &mut Rewrites,
    owned_expr_progs: &mut Vec<Box<LogicalProgram>>,
) {
    for (&nid, op) in &loaded.nodes {
        let blob = match op {
            gnitz_wire::OpNode::Map(gnitz_wire::MapKind::Expression {
                program, reindex_cols, ..
            }) if reindex_cols.is_empty() => program,
            _ => continue,
        };
        let in_nids = loaded.incoming.get(&nid).cloned().unwrap_or_default();
        if in_nids.len() != 1 {
            continue;
        }
        let reduce_nid = in_nids[0].0;
        if !matches!(loaded.nodes.get(&reduce_nid), Some(gnitz_wire::OpNode::Reduce { .. })) {
            continue;
        }
        let consumers = loaded.consumers.get(&reduce_nid).cloned().unwrap_or_default();
        if consumers.len() != 1 {
            continue;
        }
        let dep = match decode_expr_blob(blob) {
            Some(d) => d,
            None => continue,
        };
        let prog = LogicalProgram::from_wire(&dep.code, dep.num_regs, 0, Vec::new());
        // Skip folding any clean block copy regardless of offset; the
        // authoritative identity-MAP elision below (with the schema-checked PK
        // offset) handles it. No register schema exists in this early pass, so we
        // cannot supply the PK count here.
        if prog.sequential_copy_base().is_some() {
            continue;
        }
        if prog.len() <= MAX_FOLD_FINALIZE_INSTRS {
            let idx = owned_expr_progs.len();
            owned_expr_progs.push(Box::new(prog));
            rw.fold_finalize.insert(reduce_nid, idx);
            rw.folded_maps.insert(nid, reduce_nid);
        }
    }
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

pub(super) fn merge_schemas_for_join(
    left: &SchemaDescriptor,
    right: &SchemaDescriptor,
    fill: JoinNullFill,
) -> SchemaDescriptor {
    let total = left.num_columns() + right.num_payload_cols();
    assert!(
        total <= crate::schema::MAX_COLUMNS,
        "join output schema exceeds {}-column limit: {} + payload({}) = {}",
        crate::schema::MAX_COLUMNS,
        left.num_columns(),
        right.num_payload_cols(),
        total
    );
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
    SchemaDescriptor::new(&cols[..n], &pk_idx[..pk_len])
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

/// Determine the output type for an aggregate function.
///   COUNT, COUNT_NON_NULL → I64
///   SUM on float → F64, else I64  (SUM can overflow the source width)
///   MIN/MAX on float → F64; on a ≤8-byte integer → that source type; else I64
///
/// MIN/MAX *select* an existing row, so a ≤8-byte integer extremum is itself a
/// value of the source type T and is always representable in it — `MIN(INT)` is
/// `INT`, `MAX(SMALLINT UNSIGNED)` is `SMALLINT UNSIGNED`, etc. Those keep their
/// own type: the row emitters serialize the accumulator at the output column
/// width, so a narrow column writes the correct low bytes, and the width-gated
/// trace read-back (`readback_agg_bits`) reconstructs the 8-byte accumulator
/// from that width. The old U64 special case folds into this rule — the source
/// type simply *is* `U64`. Any non-float, non-≤8-byte-integer source (STRING /
/// 16-byte types) falls to the `I64` arm, but only as a total-function default
/// for the output schema this builds: a MIN/MAX over such a source is rejected at
/// compile — by the SQL binder upstream, and by `emit_reduce`'s order-encodability
/// guard on the low-level circuit API that bypasses the binder — so that schema is
/// discarded (`build_plan` returns `None`) and the `I64` arm never reaches
/// execution. The accumulator order-encodes via `encode_ordered`, which has no key
/// for these types at all. SUM over a U64 source is typed **U64**: the i64
/// `wrapping_add` accumulator's bit pattern already *is* the true sum mod 2^64,
/// same 8-byte width, so the label is the only choice — and U64 lets a downstream
/// unsigned compare re-seed correctly (like MIN/MAX preserving their source type).
/// A narrow unsigned source (U8/U16/U32) still widens to I64 (its sum stays
/// < 2^63, so signed order is correct). Mirrored by the SQL planner's
/// `agg_result_type`.
pub(super) const fn agg_output_type(agg_op: AggOp, col_type_code: TypeCode) -> u8 {
    match agg_op {
        // SumZero sums integer count/sum columns; like COUNT it produces I64.
        AggOp::Count | AggOp::CountNonNull | AggOp::Null | AggOp::SumZero => type_code::I64,
        AggOp::Sum => {
            if col_type_code.is_float() {
                type_code::F64
            } else if col_type_code as u8 == type_code::U64 {
                type_code::U64
            } else {
                type_code::I64
            }
        }
        AggOp::Min | AggOp::Max => {
            if col_type_code.is_float() {
                type_code::F64
            } else if is_fixed_int(col_type_code as u8) {
                col_type_code as u8
            } else {
                type_code::I64
            }
        }
    }
}

pub(super) fn build_reduce_output_schema(
    input: &SchemaDescriptor,
    group_cols: &[i32],
    agg_descs: &[AggDescriptor],
) -> SchemaDescriptor {
    let mut cols = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let mut n = 0;
    let mut pk_idx = [0u32; crate::schema::MAX_PK_COLUMNS];
    let mut pk_len = 0usize;

    let group_cols_u32: Vec<u32> = group_cols.iter().map(|&i| i as u32).collect();
    let group_set_eq_pk = input.group_cols_eq_pk(&group_cols_u32);
    let use_natural_pk = group_set_eq_pk || crate::ops::is_single_col_natural_pk(input, &group_cols_u32);

    if use_natural_pk {
        // Output PK region mirrors the source's PK byte layout: walk
        // `pk_columns()` in pk-list order rather than `group_cols` order.
        // For single-column natural PK on a non-PK column (e.g. GROUP BY
        // a U64 payload column), `pk_columns()` would name the wrong
        // column, so fall back to copying that one group column.
        if group_set_eq_pk {
            let k = copy_pk_columns_into(input, &mut cols, &mut pk_idx);
            n = k;
            pk_len = k;
        } else {
            cols[n] = input.columns[group_cols[0] as usize];
            pk_idx[pk_len] = n as u32;
            pk_len += 1;
            n += 1;
        }
    } else {
        // Synthetic U128 PK
        cols[n] = SchemaColumn::new(type_code::U128, 0);
        pk_idx[pk_len] = n as u32;
        pk_len += 1;
        n += 1;
        // Group columns
        for &gc in group_cols {
            cols[n] = input.columns[gc as usize];
            n += 1;
        }
    }
    // Aggregate results (same for both branches)
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
