//! Exchange relay/scatter: `op_repartition_batch(es_mode)`,
//! `op_relay_scatter_consolidated(_mode)`, `op_relay_scatter`,
//! `op_relay_broadcast`, `op_multi_scatter`.

use std::cell::RefCell;
use std::cmp::Ordering;

use crate::schema::SchemaDescriptor;
use crate::storage::{
    compare_pk_ordering, partition_for_pk_bytes, pk_sort_key, scatter_multi_source, write_to_batch, Batch,
    ConsolidatedBatch, MemBatch,
};

use super::router::{build_w_map, compound_join_packer, key_is_promoted, route_nonpk_row, RouteMode};
// `SCATTER_INDICES` + the `ReindexPacker`, plus `hash_row_for_partition`,
// `partition_for_key`, and `compare_pk_bytes`, are reached only from the
// `#[cfg(test)]` scatter helpers / co-partition tests below; production relay paths
// build their own worker-row scratch (`WORKER_ROWS`), route through
// `route_nonpk_row` / `partition_for_pk_bytes`, and settle PK ties with the
// canonical `compare_pk_ordering`.
#[cfg(test)]
use super::super::reindex::ReindexPacker;
#[cfg(test)]
use super::router::{compute_worker_indices, hash_row_for_partition, worker_for_partition, SCATTER_INDICES};
#[cfg(test)]
use crate::storage::{compare_pk_bytes, partition_for_key};

// Thread-local pool: reuse Vec<Vec<(u8,u32)>> worker-row scratch across calls.
thread_local! {
    static WORKER_ROWS: RefCell<Vec<Vec<(u8, u32)>>> = const { RefCell::new(Vec::new()) };
}

fn mem_batch_blob_cap(mem_batches: &[Option<MemBatch>]) -> usize {
    mem_batches
        .iter()
        .filter_map(|o| o.as_ref())
        .map(|mb| mb.blob.len())
        .sum::<usize>()
        .max(1)
}

#[cfg(test)]
pub(super) fn op_repartition_batch(
    batch: &Batch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    gnitz_debug!(
        "op_repartition_batch: count={} num_workers={}",
        batch.count,
        num_workers
    );
    let worker_indices = compute_worker_indices(batch, col_indices, schema, num_workers);
    let mb = batch.as_mem_batch();
    // Single source by definition; mirror the production single-source rule.
    // `compute_worker_indices` does not return the routing decision, so recompute
    // `is_pk_routing` here (a cheap slice compare).
    let is_pk_routing = col_indices == schema.pk_indices();
    let mut out: Vec<Batch> = worker_indices
        .into_iter()
        .map(|indices| {
            if !indices.is_empty() {
                Batch::from_indexed_rows(&mb, &indices, schema)
            } else {
                Batch::empty_with_schema(schema)
            }
        })
        .collect();
    if is_pk_routing {
        for b in out.iter_mut() {
            if b.count > 0 {
                b.sorted = batch.sorted;
                b.consolidated = batch.consolidated;
            }
        }
    }
    out
}

pub(crate) fn op_repartition_batches_mode(
    sources: &[Option<&Batch>],
    col_indices: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
    num_workers: usize,
    mode: RouteMode,
) -> Vec<Batch> {
    gnitz_debug!(
        "op_repartition_batches_mode: sources={} mode={:?}",
        sources.iter().filter(|s| matches!(s, Some(sb) if sb.count > 0)).count(),
        mode,
    );
    assert!(
        sources.len() <= 256,
        "source index must fit in u8 (got {})",
        sources.len()
    );
    let w_map = build_w_map(num_workers);

    let mem_batches: Vec<Option<MemBatch>> = sources
        .iter()
        .map(|opt| match opt {
            Some(s) if s.count > 0 => Some(s.as_mem_batch()),
            _ => None,
        })
        .collect();

    let total_blob = mem_batch_blob_cap(&mem_batches);

    WORKER_ROWS.with(|pool| {
        let mut worker_rows = pool.borrow_mut();
        if worker_rows.len() < num_workers {
            worker_rows.resize_with(num_workers, Vec::new);
        }
        for w in 0..num_workers {
            worker_rows[w].clear();
        }

        // Same hoist as `fill_worker_indices`: wide-PK single-column routing
        // and compound-PK routing both go through `partition_for_pk_bytes`
        // so the partition matches `PartitionedTable::local_index`. Mixing
        // the hash-combine path with PK-existence checks would route rows
        // to the wrong worker.
        // A promoted join key must not take the native-PK fast-path: its source PK
        // bytes are at the narrow width while the trace `_join_pk` is `T`-wide, so
        // the two would land on different workers. Route it through the packer.
        let is_pk_routing = col_indices == schema.pk_indices() && !key_is_promoted(target_tcs);
        // Compound/promoted JoinPromote routes by the packed `_join_pk` (see
        // `compound_join_packer`); the `pack_buf` is hoisted out of the row loop
        // because `pack_into` fully overwrites `buf[..out_stride]` each row.
        let join_packer = compound_join_packer(mode, col_indices, target_tcs, schema);
        let mut pack_buf = [0u8; gnitz_wire::MAX_PK_BYTES];
        for (si, mb_opt) in mem_batches.iter().enumerate() {
            let mb = match mb_opt {
                Some(m) => m,
                None => continue,
            };
            if is_pk_routing {
                for i in 0..mb.count {
                    let partition = partition_for_pk_bytes(mb.get_pk_bytes(i));
                    worker_rows[w_map[partition]].push((si as u8, i as u32));
                }
            } else {
                for i in 0..mb.count {
                    let partition = route_nonpk_row(mb, i, &join_packer, &mut pack_buf, col_indices, schema, mode);
                    worker_rows[w_map[partition]].push((si as u8, i as u32));
                }
            }
        }

        let total_rows: usize = worker_rows[..num_workers].iter().map(|v| v.len()).sum();
        let mut out: Vec<Batch> = (0..num_workers)
            .map(|w| {
                if worker_rows[w].is_empty() {
                    return Batch::empty_with_schema(schema);
                }
                let blob_cap = (total_blob * worker_rows[w].len() / total_rows).max(1);
                write_to_batch(schema, worker_rows[w].len(), blob_cap, |writer| {
                    scatter_multi_source(&mem_batches, &worker_rows[w], writer);
                })
            })
            .collect();

        // PK-routed, single-source repartition preserves source order and
        // distinctness per worker (a PK group never splits across workers, so a
        // worker's sub-batch is an in-order subset of one sorted source).
        // Multi-source scatter is per-source-concatenated — not globally sorted
        // — so it propagates nothing, and non-PK routing destroys PK order.
        if is_pk_routing {
            let mut contributing = sources.iter().filter_map(|s| *s).filter(|b| b.count > 0);
            if let (Some(src), None) = (contributing.next(), contributing.next()) {
                for b in out.iter_mut() {
                    if b.count > 0 {
                        b.sorted = src.sorted;
                        b.consolidated = src.consolidated;
                    }
                }
            }
        }
        out
    })
}

/// Unified K-way merge-walk skeleton — one body, one comparator, every PK width.
/// `order_cache[si]` holds each active source's order-preserving `pk_sort_key`
/// (the leading-≤16 OPK bytes packed big-endian: the whole key for `pk_stride ≤ 16`,
/// the order-preserving prefix for `> 16`; `pk_sort_key` never panics, unlike
/// `get_pk`, which debug_asserts `stride ≤ 16`). The winner each step is the source
/// whose cached key is smallest — the register compare is the common fast path, and
/// a key tie defers to the canonical `compare_pk_ordering` on the live OPK bytes,
/// whose own `len > 16` guard no-ops a `≤ 16` tie (the cached key is then the whole
/// PK, so equal keys are byte-equal) and reserves the byte tiebreak for a `> 16`
/// prefix collision — then on ascending source index (deterministic under
/// `swap_remove`'s scramble of `active_sources`). `route` partitions each emitted
/// row through `partition_for_pk_bytes` (or the group/packer path) — byte-correct at
/// any width. No width fork, no `get_pk`, no `pk_cache`.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn relay_walk_inner<'a, Route>(
    mem_batches: &[Option<MemBatch<'a>>],
    w_map: &[usize; 256],
    worker_rows: &mut [Vec<(u8, u32)>],
    order_cache: &mut [u128; 256],
    mut cursors: [u32; 256],
    mut active_sources: [u8; 256],
    mut num_active: usize,
    mut route: Route,
) where
    Route: FnMut(&MemBatch<'a>, usize) -> usize,
{
    while num_active > 0 {
        if num_active == 1 {
            // Bulk-drain the sole remaining source without PK comparisons.
            let si = active_sources[0] as usize;
            let mb = mem_batches[si].as_ref().unwrap();
            for row in cursors[si] as usize..mb.count {
                worker_rows[w_map[route(mb, row)]].push((si as u8, row as u32));
            }
            return;
        }

        // Winner = smallest cached `pk_sort_key`, a cache tie settled by the
        // comparator below, then by ascending source index.
        let mut best_pos = 0usize;
        let mut best_si = active_sources[0];
        let mut best_key = order_cache[best_si as usize];
        #[allow(clippy::needless_range_loop)]
        for pos in 1..num_active {
            let si = active_sources[pos];
            let key = order_cache[si as usize];
            let ord = match key.cmp(&best_key) {
                // Cached-key tie: settle on the live OPK bytes through the canonical
                // comparator. Its `len > 16` guard no-ops a `≤ 16` tie (the cached
                // key is the whole PK, so equal keys are byte-equal) and runs the
                // memcmp only on a `> 16` prefix collision.
                Ordering::Equal => compare_pk_ordering(
                    mem_batches[si as usize]
                        .as_ref()
                        .unwrap()
                        .get_pk_bytes(cursors[si as usize] as usize),
                    mem_batches[best_si as usize]
                        .as_ref()
                        .unwrap()
                        .get_pk_bytes(cursors[best_si as usize] as usize),
                ),
                other => other,
            };
            if ord == Ordering::Less || (ord == Ordering::Equal && si < best_si) {
                best_pos = pos;
                best_si = si;
                best_key = key;
            }
        }

        let best_si = best_si as usize;
        let row = cursors[best_si] as usize;
        cursors[best_si] += 1;
        let mb = mem_batches[best_si].as_ref().unwrap();
        worker_rows[w_map[route(mb, row)]].push((best_si as u8, row as u32));

        let new_cur = cursors[best_si] as usize;
        if new_cur == mb.count {
            // Swap-remove the exhausted source in O(1).
            num_active -= 1;
            active_sources[best_pos] = active_sources[num_active];
        } else {
            order_cache[best_si] = pk_sort_key(mb.get_pk_bytes(new_cur));
        }
    }
}

/// Scatter pre-consolidated sources to workers in PK order using a K-cursor merge walk.
/// Fills `worker_rows[0..num_workers]` with PK-ordered (si, row) index pairs.
/// No post-sort needed. Caller holds `WORKER_ROWS` borrow_mut.
fn relay_scatter_merge_walk(
    mem_batches: &[Option<MemBatch<'_>>],
    col_indices: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
    num_workers: usize,
    worker_rows: &mut Vec<Vec<(u8, u32)>>,
    mode: RouteMode,
) {
    assert!(
        mem_batches.len() <= 256,
        "source index must fit in u8 (got {})",
        mem_batches.len()
    );
    let w_map = build_w_map(num_workers);
    let cursors = [0u32; 256];
    let mut order_cache = [0u128; 256];
    let mut active_sources = [0u8; 256];
    let mut num_active: usize = 0;

    if worker_rows.len() < num_workers {
        worker_rows.resize_with(num_workers, Vec::new);
    }
    worker_rows[..num_workers].iter_mut().for_each(Vec::clear);

    // One order-preserving winner key per active source, valid at every PK width:
    // `pk_sort_key` packs the leading ≤16 OPK bytes big-endian — the whole key for
    // stride ≤ 16, the order-preserving prefix for > 16. Never panics (unlike
    // `get_pk`, which debug_asserts stride ≤ 16).
    for (si, mb_opt) in mem_batches.iter().enumerate() {
        if let Some(mb) = mb_opt {
            if mb.count > 0 {
                order_cache[si] = pk_sort_key(mb.get_pk_bytes(0));
                active_sources[num_active] = si as u8;
                num_active += 1;
            }
        }
    }

    // Strict sequence equality (see `fill_worker_indices`): set equality would
    // route a permuted compound PK to a different worker than the schema-order
    // `partition_for_pk_bytes` on the other join side.
    // A promoted join key must not take the native-PK fast-path (its source PK is
    // at the narrow width, the trace `_join_pk` is `T`-wide); route it through the
    // packer so the two sides co-partition.
    let is_pk_routing = col_indices == schema.pk_indices() && !key_is_promoted(target_tcs);

    // Compound/promoted JoinPromote routes by the packed `_join_pk` (see
    // `compound_join_packer`) — the consolidated join path, exactly where
    // co-partition must hold.
    let join_packer = compound_join_packer(mode, col_indices, target_tcs, schema);

    // Two routes, picked once (never per row): the PK fast path is a bare
    // `partition_for_pk_bytes` over the OPK bytes — byte-identical to the old narrow
    // `partition_for_key(get_pk)` route (both reduce to `mix(widen_pk_be(bytes))`)
    // and the old wide route. The non-PK path defers to the shared `route_nonpk_row`
    // (the pack-or-hash decision), reusing one hoisted `pack_buf` across rows; the
    // `&mut pack_buf` capture is why `relay_walk_inner` takes `FnMut`, not `Fn`.
    if is_pk_routing {
        let route_pk = |mb: &MemBatch, row: usize| partition_for_pk_bytes(mb.get_pk_bytes(row));
        relay_walk_inner(
            mem_batches,
            &w_map,
            worker_rows,
            &mut order_cache,
            cursors,
            active_sources,
            num_active,
            route_pk,
        );
    } else {
        let mut pack_buf = [0u8; gnitz_wire::MAX_PK_BYTES];
        let route_group = |mb: &MemBatch, row: usize| {
            route_nonpk_row(mb, row, &join_packer, &mut pack_buf, col_indices, schema, mode)
        };
        relay_walk_inner(
            mem_batches,
            &w_map,
            worker_rows,
            &mut order_cache,
            cursors,
            active_sources,
            num_active,
            route_group,
        );
    }
}

/// Scatter pre-consolidated batches across workers using a merge-walk.
/// All sources must satisfy the consolidated invariant (sorted, no duplicate PKs).
/// Output batches are sorted but not consolidated (duplicate PKs can appear across sources).
#[cfg(test)]
pub(super) fn op_relay_scatter_consolidated(
    sources: &[Option<&ConsolidatedBatch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    op_relay_scatter_consolidated_mode(sources, col_indices, &[], schema, num_workers, RouteMode::GroupKey)
}

pub(crate) fn op_relay_scatter_consolidated_mode(
    sources: &[Option<&ConsolidatedBatch>],
    col_indices: &[u32],
    target_tcs: &[u8],
    schema: &SchemaDescriptor,
    num_workers: usize,
    mode: RouteMode,
) -> Vec<Batch> {
    let mem_batches: Vec<Option<MemBatch>> = sources
        .iter()
        .map(|opt| match opt {
            Some(s) if s.count > 0 => Some(s.as_mem_batch()),
            _ => None,
        })
        .collect();
    gnitz_debug!(
        "op_relay_scatter_consolidated: sources={}",
        mem_batches.iter().filter(|o| o.is_some()).count(),
    );
    if mem_batches.iter().all(|o| o.is_none()) {
        return (0..num_workers).map(|_| Batch::empty_with_schema(schema)).collect();
    }
    let total_blob: usize = mem_batch_blob_cap(&mem_batches);

    // The merge-walk yields globally sorted output across all sources. With
    // exactly one contributing source no cross-source duplicate PK is possible,
    // so the per-worker output is also consolidated; with ≥2 sources the same PK
    // can appear in two sources (e.g. an insert in one, a retraction in another),
    // so consolidated stays false.
    let single_source = mem_batches.iter().filter(|o| o.is_some()).count() == 1;

    WORKER_ROWS.with(|pool| {
        let mut worker_rows = pool.borrow_mut();
        relay_scatter_merge_walk(
            &mem_batches,
            col_indices,
            target_tcs,
            schema,
            num_workers,
            &mut worker_rows,
            mode,
        );

        let total_rows: usize = worker_rows[..num_workers].iter().map(|v| v.len()).sum();
        (0..num_workers)
            .map(|w| {
                if worker_rows[w].is_empty() {
                    return Batch::empty_with_schema(schema);
                }
                let blob_cap = (total_blob * worker_rows[w].len() / total_rows).max(1);
                let mut b = write_to_batch(schema, worker_rows[w].len(), blob_cap, |writer| {
                    scatter_multi_source(&mem_batches, &worker_rows[w], writer);
                });
                b.sorted = true;
                if single_source {
                    b.consolidated = true;
                }
                b
            })
            .collect()
    })
}

/// Broadcast relay: deliver the FULL delta to every worker. The per-worker
/// source slices are disjoint (each is one worker's base-table-PK-partitioned
/// slice), so their concatenation is the full delta with no duplication; that
/// concatenation is then cloned to each of `num_workers` destinations. The
/// range-join probe needs the whole delta on every worker — a range match can
/// live on any worker's trace — which the equality scatter (one destination per
/// row) cannot deliver. Sibling of `op_repartition_batches_mode` /
/// `op_relay_scatter_consolidated_mode`, but without `col_indices` / `RouteMode`
/// (broadcast routes nothing). All destination batches are byte-identical.
pub(crate) fn op_relay_broadcast(
    sources: &[Option<&Batch>],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    let total: usize = sources.iter().flatten().map(|b| b.count).sum();
    if total == 0 {
        return (0..num_workers).map(|_| Batch::empty_with_schema(schema)).collect();
    }
    // Concatenate the disjoint slices into the full delta once (append_batch
    // relocates each source's blob, so independent source blobs stay valid).
    let mut full = Batch::with_schema(*schema, total);
    for src in sources.iter().flatten() {
        if src.count > 0 {
            full.append_batch(src, 0, src.count);
        }
    }
    // Clone to every worker; the last destination takes ownership of `full`.
    let mut out: Vec<Batch> = (1..num_workers).map(|_| full.clone_batch()).collect();
    out.push(full);
    out
}

/// Scatter non-consolidated batches across workers by hashing each row.
#[cfg(test)]
pub(super) fn op_relay_scatter(
    sources: &[Option<&Batch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    op_repartition_batches_mode(sources, col_indices, &[], schema, num_workers, RouteMode::GroupKey)
}

/// Scatter a single batch across workers using multiple independent column
/// specifications in one pass.  Returns one `Vec<Batch>` per spec.
///
/// Only used in tests; retained as `#[cfg(test)]` so the helper doesn't
/// bloat the production binary. Routes non-PK specs by `RouteMode::GroupKey`
/// only — it does NOT build a `ReindexPacker`, so it does not co-partition a
/// join-promoted compound key. A hand-assembled compound-join co-partition test
/// must drive `op_repartition_batches_mode` / `op_relay_scatter_consolidated_mode`
/// with `RouteMode::JoinPromote`, not this helper.
#[cfg(test)]
pub(super) fn op_multi_scatter(
    batch: &Batch,
    col_specs: &[&[u32]],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Vec<Batch>> {
    let n = batch.count;
    let n_specs = col_specs.len();
    let mb = batch.as_mem_batch();
    let w_map = build_w_map(num_workers);
    let needed = n_specs * num_workers;

    SCATTER_INDICES.with(|pool| {
        let mut flat_indices = pool.borrow_mut();
        if flat_indices.len() < needed {
            flat_indices.resize_with(needed, Vec::new);
        }
        for slot in flat_indices[..needed].iter_mut() {
            slot.clear();
        }

        for si in 0..n_specs {
            let spec = col_specs[si];
            let is_pk_routing = spec == schema.pk_indices();
            if is_pk_routing {
                // Distribution-prefix route, exactly as `fill_worker_indices`.
                for i in 0..n {
                    let partition = schema.partition_for_pk(mb.get_pk_bytes(i));
                    flat_indices[si * num_workers + w_map[partition]].push(i as u32);
                }
            } else {
                for i in 0..n {
                    let partition = hash_row_for_partition(&mb, i, spec, schema, RouteMode::GroupKey);
                    flat_indices[si * num_workers + w_map[partition]].push(i as u32);
                }
            }
        }

        let mut results: Vec<Vec<Batch>> = (0..n_specs)
            .map(|_| (0..num_workers).map(|_| Batch::empty_with_schema(schema)).collect())
            .collect();

        for si in 0..n_specs {
            for w in 0..num_workers {
                let indices = &flat_indices[si * num_workers + w];
                if !indices.is_empty() {
                    results[si][w] = Batch::from_indexed_rows(&mb, indices, schema);
                }
            }
        }

        // Flag propagation: PK-spec sub-batches inherit sorted/consolidated from source.
        for si in 0..n_specs {
            let spec = col_specs[si];
            if spec == schema.pk_indices() {
                if crate::storage::ConsolidatedBatch::from_batch_ref(batch).is_some() {
                    for batch in results[si].iter_mut().take(num_workers) {
                        if batch.count > 0 {
                            batch.sorted = true;
                            batch.consolidated = true;
                        }
                    }
                } else if batch.sorted {
                    for batch in results[si].iter_mut().take(num_workers) {
                        if batch.count > 0 {
                            batch.sorted = true;
                        }
                    }
                }
            }
        }

        results
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, SHORT_STRING_THRESHOLD};
    use crate::test_support::{make_wide_batch, opk_pk, wide_pk_3xu64_schema};

    #[test]
    #[should_panic(expected = "source index must fit in u8")]
    fn test_op_repartition_batches_rejects_over_256_sources() {
        // The source index is stored in a u8; > 256 sources would truncate it
        // silently in release builds. The guard must be a hard assert.
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        let sources: Vec<Option<&Batch>> = vec![None; 257];
        let _ = op_repartition_batches_mode(&sources, &[0], &[], &schema, 4, RouteMode::GroupKey);
    }

    fn make_schema_u64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_schema_u128_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_schema_u64_string() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        )
    }

    fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn make_batch_str(schema: &SchemaDescriptor, rows: &[(u64, i64, &str)]) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, s) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());

            let bytes = s.as_bytes();
            let length = bytes.len() as u32;
            let mut gs = [0u8; 16];
            gs[0..4].copy_from_slice(&length.to_le_bytes());
            if bytes.len() <= SHORT_STRING_THRESHOLD {
                let copy_len = bytes.len().min(12);
                gs[4..4 + copy_len].copy_from_slice(&bytes[..copy_len]);
            } else {
                gs[4..8].copy_from_slice(&bytes[..4]);
                let offset = b.blob.len() as u64;
                gs[8..16].copy_from_slice(&offset.to_le_bytes());
                b.blob.extend_from_slice(bytes);
            }
            b.extend_col(0, &gs);
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn total_rows(batches: &[Batch]) -> usize {
        batches.iter().map(|b| b.count).sum()
    }

    #[test]
    fn test_relay_scatter_wide_pk_order_and_routing() {
        let schema = wide_pk_3xu64_schema();
        let num_workers = 4;
        // (1,1,*) prefix-twins span the three sources: they share the leading
        // BE(1)++BE(1) 16-byte OPK prefix and differ only in the trailing column,
        // so the unified `relay_walk_inner` comparator must fall through its cached
        // 16-byte-prefix register compare to the `compare_pk_bytes` byte tiebreak
        // (the twins are byte-distinct, so that fallback is decisive — never Equal).
        // The multi-byte values are LE/BE order inversions — c2 ∈ {1,2,256,257}
        // and c0 ∈ {2,256} — so OPK encoding is load-bearing for the builder's
        // sorted assert (a dropped BE flip sorts 256 before 1/2 and trips it).
        // Additionally, b1 and b2 both carry PK (3,3,3) with DISTINCT payloads
        // (99 vs 31): a byte-EQUAL wide PK whose `compare_pk_bytes` fallback returns
        // Equal, exercising the source-index tiebreak at wide width.
        let b0 = make_wide_batch(
            &schema,
            &[
                (0, 0, 0, 1, 10),
                (1, 1, 1, 1, 11),
                (1, 1, 256, 1, 12), // shares BE(1)++BE(1) prefix with (1,1,1); c2 multi-byte
            ],
        );
        let b1 = make_wide_batch(
            &schema,
            &[
                (1, 1, 257, 1, 20), // third prefix-twin
                (2, 2, 2, 1, 21),
                (3, 3, 3, 1, 99),   // byte-equal twin of b2's (3,3,3); distinct payload 99
                (256, 0, 0, 1, 22), // c0 multi-byte: under LE this sorts before (2,2,2)
            ],
        );
        let b2 = make_wide_batch(
            &schema,
            &[
                (1, 1, 2, 1, 30), // fourth prefix-twin
                (3, 3, 3, 1, 31), // byte-equal twin of b1's (3,3,3); distinct payload 31
                (7, 0, 0, 1, 32),
            ],
        );
        // sources[1] = b1 (si=1), sources[2] = b2 (si=2): the tiebreak must emit
        // b1's (3,3,3) copy before b2's.
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1), Some(&b2)];
        let result = op_relay_scatter_consolidated(&sources, schema.pk_indices(), &schema, num_workers);

        assert_eq!(total_rows(&result), 10);
        for (w, sb) in result.iter().enumerate() {
            // (a) Non-decreasing under the column-aware comparator.
            for r in 1..sb.count {
                let prev = sb.get_pk_bytes(r - 1);
                let cur = sb.get_pk_bytes(r);
                assert_ne!(
                    compare_pk_bytes(prev, cur),
                    std::cmp::Ordering::Greater,
                    "worker {w} row {r} out of order",
                );
            }
            // (b) Every row routed to partition_for_pk_bytes's worker.
            for r in 0..sb.count {
                let pk = sb.get_pk_bytes(r);
                let expected = worker_for_partition(partition_for_pk_bytes(pk), num_workers);
                assert_eq!(expected, w, "wide PK routed to wrong worker");
            }
        }

        // (c) Byte-equal wide-PK source-index tiebreak. b1 (si=1) and b2 (si=2)
        // both carry PK (3,3,3): equal cached prefix AND equal full OPK bytes, so
        // the comparator's `compare_pk_bytes` fallback returns Equal and the
        // ascending-source-index tiebreak decides. Both copies co-locate (equal PK
        // → equal partition) and emit adjacently in merge order, so collecting the
        // (3,3,3) payloads across workers yields b1's 99 before b2's 31.
        let pk_333 = opk_pk(&schema, &[3, 3, 3]);
        let mut payloads_333 = Vec::new();
        for sb in &result {
            for r in 0..sb.count {
                if sb.get_pk_bytes(r) == pk_333.as_slice() {
                    payloads_333.push(i64::from_le_bytes(sb.col_data(0)[r * 8..r * 8 + 8].try_into().unwrap()));
                }
            }
        }
        assert_eq!(
            payloads_333,
            vec![99, 31],
            "byte-equal wide PK (3,3,3) must emit b1's si=1 copy (payload 99) before b2's si=2 copy (payload 31)",
        );
    }

    /// Compound (U64, U64) PK schema: pk_stride = 16 (narrow), pk_count = 2.
    /// Exercises the column-aware cached-key comparator: a raw packed-u128 `<` over
    /// the wrong byte order would put the trailing column in the high bits and
    /// reverse column priority — but `order_cache` packs the OPK image, so it does
    /// not.
    fn make_narrow_compound_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        )
    }

    /// Pack (c0, c1) into a u128 the way `MemBatch::get_pk` would read a
    /// 16-byte PK region: low 8 bytes = c0, high 8 bytes = c1.
    fn mk_compound_pk(c0: u64, c1: u64) -> u128 {
        ((c1 as u128) << 64) | (c0 as u128)
    }

    fn make_narrow_compound_batch(schema: &SchemaDescriptor, rows: &[(u128, i64, i64)]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            // `mk_compound_pk` packs c0 in the low 8 bytes and c1 in the high 8.
            // The compound PK at rest is OPK = col0_BE ++ col1_BE, so encode the
            // two native column values through extend_pk_opk rather than writing
            // the raw u128 (which would byte-reverse the column order).
            let c0 = pk as u64 as u128;
            let c1 = (pk >> 64) as u64 as u128;
            b.extend_pk_opk(schema, &[c0, c1]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_relay_scatter_narrow_compound_pk_order_and_routing() {
        // (U64, U64) compound PK: low-8-byte collisions force the column-aware
        // comparator. (1, 10) packed = (10<<64)|1, (2, 0) packed = 2; raw u128
        // `<` would order (2, 0) before (1, 10), but lexicographic order says
        // (1, 10) < (2, 0). Likewise (1, 5) < (1, 10) < (1, 15) lexicographically.
        let schema = make_narrow_compound_schema();
        // A 16-byte key: the cached `pk_sort_key` is the whole PK, so the comparator
        // decides on the register compare alone and never runs the byte tiebreak.
        assert!(!schema.pk_is_wide(), "pk_stride must be 16 (narrow)");
        assert!(schema.pk_indices().len() > 1, "fixture is a compound (multi-column) PK");
        let num_workers = 4;
        let b0 = make_narrow_compound_batch(
            &schema,
            &[
                (mk_compound_pk(1, 10), 1, 11),
                (mk_compound_pk(2, 0), 1, 12),
                (mk_compound_pk(5, 9), 1, 13),
            ],
        );
        let b1 = make_narrow_compound_batch(
            &schema,
            &[
                (mk_compound_pk(1, 5), 1, 21),
                (mk_compound_pk(1, 15), 1, 22),
                (mk_compound_pk(3, 3), 1, 23),
            ],
        );
        let b2 = make_narrow_compound_batch(
            &schema,
            &[
                (mk_compound_pk(1, 7), 1, 31),
                (mk_compound_pk(2, 2), 1, 32),
                (mk_compound_pk(7, 0), 1, 33),
            ],
        );
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1), Some(&b2)];
        // col_indices = [0, 1] is the full PK set: exercises the compound-PK-set
        // is_pk_routing path (route_pk via partition_for_pk_bytes over the OPK bytes).
        let result = op_relay_scatter_consolidated(&sources, &[0u32, 1u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 9);
        for (w, sb) in result.iter().enumerate() {
            // (a) Non-decreasing under the column-aware comparator.
            for r in 1..sb.count {
                let prev = sb.get_pk_bytes(r - 1);
                let cur = sb.get_pk_bytes(r);
                assert_ne!(
                    compare_pk_bytes(prev, cur),
                    Ordering::Greater,
                    "worker {w} row {r} out of order (compound PK)",
                );
            }
            // (b) Routing matches partition_for_pk_bytes (the canonical PK route
            // for narrow PKs via fill_worker_indices' is_compound_pk path).
            for r in 0..sb.count {
                let pk = sb.get_pk_bytes(r);
                let expected = worker_for_partition(partition_for_pk_bytes(pk), num_workers);
                assert_eq!(expected, w, "compound PK routed to wrong worker");
            }
        }
    }

    fn make_schema_i64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_i64_batch(schema: &SchemaDescriptor, rows: &[(i64, i64, i64)]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            // Signed PK at rest is OPK (big-endian, sign-bit flipped), so
            // memcmp == signed order. Encode through extend_pk_opk, not the
            // raw right-aligned `extend_pk` (which would store native bytes).
            b.extend_pk_opk(schema, &[(pk as u64) as u128]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_relay_scatter_i64_pk_signed_ordering() {
        // Native I64 PK with negative values. Each output batch must be
        // sorted under canonical signed order — raw u128 ordering would put
        // -1 (0xFFFF...) after +1 and break the `sorted = true` invariant set
        // by op_relay_scatter_consolidated.
        let schema = make_schema_i64_i64();
        let num_workers = 4;
        // Each source sorted ascending under signed I64 order.
        let b0 = make_i64_batch(&schema, &[(-100, 1, 10), (-1, 1, 11), (5, 1, 12)]);
        let b1 = make_i64_batch(&schema, &[(-50, 1, 20), (0, 1, 21), (100, 1, 22)]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter_consolidated(&sources, &[0u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 6);
        for (w, sb) in result.iter().enumerate() {
            assert!(sb.sorted, "worker {w} output not marked sorted");
            for r in 1..sb.count {
                let prev = sb.get_pk(r - 1);
                let cur = sb.get_pk(r);
                assert_ne!(
                    compare_pk_bytes(sb.get_pk_bytes(r - 1), sb.get_pk_bytes(r)),
                    Ordering::Greater,
                    "worker {w} row {r}: signed pks out of order (prev={prev:#x} cur={cur:#x})",
                );
            }
        }
    }

    #[test]
    fn test_relay_scatter_wide_pk_single_source_bulk_drain() {
        let schema = wide_pk_3xu64_schema();
        let num_workers = 4;
        // Exactly one active source → the num_active == 1 bulk-drain path.
        // One value past 255 (c0 = 256) so the drain routes a realistic wide OPK
        // and the shared builder's sorted assert covers this path, which has no
        // ordering check of its own (under LE, 256 would sort before (7,8,9)).
        let b0 = make_wide_batch(
            &schema,
            &[
                (1, 2, 3, 1, 1),
                (4, 5, 6, 1, 2),
                (7, 8, 9, 1, 3),
                (256, 11, 12, 1, 4), // c0 multi-byte: under LE this sorts before (7,8,9)
            ],
        );
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0)];
        let result = op_relay_scatter_consolidated(&sources, schema.pk_indices(), &schema, num_workers);

        assert_eq!(total_rows(&result), 4);
        for (w, sb) in result.iter().enumerate() {
            for r in 0..sb.count {
                let pk = sb.get_pk_bytes(r);
                let expected = worker_for_partition(partition_for_pk_bytes(pk), num_workers);
                assert_eq!(expected, w, "bulk-drain wide PK routed to wrong worker");
            }
        }
    }

    #[test]
    fn test_repartition_batch_pk_routing() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let pk_vals: &[u64] = &[1, 7, 42, 100, 255, 1024, 65537, 999983];

        let mut b = Batch::with_schema(schema, pk_vals.len());

        for &pk in pk_vals {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), pk_vals.len());

        for &pk in pk_vals {
            let expected = worker_for_partition(partition_for_key(pk as u128), num_workers);
            let found = (0..sub_batches[expected].count).any(|r| (sub_batches[expected].get_pk(r) as u64) == pk);
            assert!(found, "pk={pk} not found in worker {expected}");
        }
    }

    #[test]
    fn test_repartition_batch_u128_pk() {
        let schema = make_schema_u128_i64();
        let num_workers = 4;
        let pks: &[u128] = &[
            1u128 << 64,
            (0xCAFE_BABEu128 << 64) | 0xDEAD_BEEF,
            u128::MAX,
            (7u128 << 64) | 42,
        ];

        let n = pks.len();
        let mut b = Batch::with_schema(schema, n);

        for &pk in pks {
            b.extend_pk(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), n);

        for &pk in pks {
            let expected = worker_for_partition(partition_for_key(pk), num_workers);
            let found = (0..sub_batches[expected].count).any(|r| sub_batches[expected].get_pk(r) == pk);
            assert!(found, "pk={pk} not in worker {expected}");
        }
    }

    #[test]
    fn test_repartition_batch_group_col() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let same_val: i64 = 42;

        let mut b = Batch::with_schema(schema, 4);

        for pk in [1u64, 2, 3, 4] {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &same_val.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), 4);
        let non_empty = sub_batches.iter().filter(|sb| sb.count > 0).count();
        assert_eq!(non_empty, 1, "all rows with same group key must go to one worker");
    }

    #[test]
    fn test_repartition_batch_string_col() {
        let schema = make_schema_u64_string();
        let num_workers = 4;

        // Short string "hello" (≤ 12 bytes): two rows must go to same worker
        let b = make_batch_str(&schema, &[(1, 1, "hello"), (2, 1, "hello"), (3, 1, "world")]);
        let sub_batches = op_repartition_batch(&b, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), 3);

        let mut worker_of_1 = None;
        for (w, batch) in sub_batches.iter().enumerate().take(num_workers) {
            for r in 0..batch.count {
                if (batch.get_pk(r) as u64) == 1 {
                    worker_of_1 = Some(w);
                }
            }
        }
        let w1 = worker_of_1.expect("pk=1 must be in some worker");
        let pk2_same = (0..sub_batches[w1].count).any(|r| (sub_batches[w1].get_pk(r) as u64) == 2);
        assert!(pk2_same, "same short string 'hello' must route to same worker");

        // Long string (> 12 bytes): two rows must go to same worker
        let long_str = "this is a longer string for heap";
        let b2 = make_batch_str(&schema, &[(10, 1, long_str), (11, 1, long_str)]);
        let sub2 = op_repartition_batch(&b2, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub2), 2);

        let mut worker_of_10 = None;
        for (w, batch) in sub2.iter().enumerate().take(num_workers) {
            for r in 0..batch.count {
                if (batch.get_pk(r) as u64) == 10 {
                    worker_of_10 = Some(w);
                }
            }
        }
        let w10 = worker_of_10.expect("pk=10 must be in some worker");
        let pk11_same = (0..sub2[w10].count).any(|r| (sub2[w10].get_pk(r) as u64) == 11);
        assert!(pk11_same, "same long string must route to same worker");
    }

    #[test]
    fn test_repartition_batch_pk_routing_propagates_flags() {
        let schema = make_schema_u64_i64(); // PK = col 0 (U64), payload = col 1 (I64)
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        assert!(b.sorted && b.consolidated);

        // PK routing (col 0 == pk_indices()): single source ⇒ flags propagate.
        let pk_routed = op_repartition_batch(&b, &[0u32], &schema, 4);
        for sb in pk_routed.iter().filter(|s| s.count > 0) {
            assert!(sb.sorted, "PK-routed sub-batch must inherit sorted");
            assert!(sb.consolidated, "PK-routed sub-batch must inherit consolidated");
        }

        // Non-PK routing (col 1): hash distribution destroys PK order ⇒ no flags.
        let hash_routed = op_repartition_batch(&b, &[1u32], &schema, 4);
        for sb in hash_routed.iter().filter(|s| s.count > 0) {
            assert!(!sb.sorted, "non-PK-routed sub-batch must not claim sorted");
            assert!(!sb.consolidated, "non-PK-routed sub-batch must not claim consolidated");
        }
    }

    #[test]
    fn test_repartition_batches_pk_routing_single_vs_multi_source() {
        let schema = make_schema_u64_i64();
        // Single contributing source, PK routing: propagate both flags.
        let b0 = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let single: Vec<Option<&Batch>> = vec![Some(&b0)];
        let out = op_repartition_batches_mode(&single, &[0u32], &[], &schema, 4, RouteMode::GroupKey);
        for sb in out.iter().filter(|s| s.count > 0) {
            assert!(sb.sorted, "single-source PK-routed must inherit sorted");
            assert!(sb.consolidated, "single-source PK-routed must inherit consolidated");
        }

        // Two contributing sources, PK routing: per-source-concatenated output is
        // not globally sorted, so nothing propagates.
        let b1 = make_batch(&schema, &[(4, 1, 40), (5, 1, 50), (6, 1, 60)]);
        let multi: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
        let out = op_repartition_batches_mode(&multi, &[0u32], &[], &schema, 4, RouteMode::GroupKey);
        for sb in out.iter().filter(|s| s.count > 0) {
            assert!(!sb.sorted, "multi-source scatter must not claim sorted");
            assert!(!sb.consolidated, "multi-source scatter must not claim consolidated");
        }
    }

    #[test]
    fn test_repartition_batch_sorted_not_consolidated_pk_routing() {
        let schema = make_schema_u64_i64();
        let mut b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]).into_inner();
        // Sorted but not consolidated source: only `sorted` may propagate.
        b.consolidated = false;
        assert!(b.sorted && !b.consolidated);

        let out = op_repartition_batch(&b, &[0u32], &schema, 4);
        for sb in out.iter().filter(|s| s.count > 0) {
            assert!(sb.sorted, "PK-routed sub-batch inherits sorted");
            assert!(!sb.consolidated, "must not invent consolidated");
        }
    }

    #[test]
    fn test_relay_scatter_consolidated_path() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let b0 = make_batch(&schema, &[(1, 1, 10), (5, 1, 50), (9, 1, 90)]);
        let b1 = make_batch(&schema, &[(2, 1, 20), (6, 1, 60), (10, 1, 100)]);

        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter_consolidated(&sources, &[0u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 6);
        for sb in &result {
            if sb.count > 0 {
                assert!(
                    !sb.consolidated,
                    "merged path uses PK-only comparison, must not claim consolidated"
                );
                assert!(sb.sorted, "merged path output must be sorted");
            }
        }

        // Single contributing source: no cross-source duplicate PK possible, so
        // the output is consolidated as well as sorted.
        let single: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0)];
        let result = op_relay_scatter_consolidated(&single, &[0u32], &schema, num_workers);
        assert_eq!(total_rows(&result), 3);
        for sb in result.iter().filter(|s| s.count > 0) {
            assert!(sb.sorted, "single-source merge output must be sorted");
            assert!(sb.consolidated, "single-source merge output must be consolidated");
        }
    }

    #[test]
    fn test_relay_scatter_fallback_path() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let b0 = make_batch(&schema, &[(1, 1, 10)]);
        let b1 = make_batch(&schema, &[(2, 1, 20)]);

        let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter(&sources, &[0u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 2);
        for sb in &result {
            if sb.count > 0 {
                assert!(
                    !sb.consolidated,
                    "non-consolidated path output must not be consolidated"
                );
            }
        }
    }

    #[test]
    fn test_multi_scatter_pk_flag_propagation() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        assert!(b.sorted && b.consolidated);

        let specs: Vec<&[u32]> = vec![&[0u32], &[1u32]];
        let result = op_multi_scatter(&b, &specs, &schema, num_workers);

        // PK spec (specs[0]): sub-batches must inherit sorted + consolidated
        for sb in &result[0] {
            if sb.count > 0 {
                assert!(sb.sorted, "PK spec sub-batch must inherit sorted");
                assert!(sb.consolidated, "PK spec sub-batch must inherit consolidated");
            }
        }
        // Non-PK spec (specs[1]): must NOT inherit
        for sb in &result[1] {
            if sb.count > 0 {
                assert!(!sb.sorted, "non-PK spec sub-batch must not inherit sorted");
                assert!(!sb.consolidated, "non-PK spec sub-batch must not inherit consolidated");
            }
        }
    }

    #[test]
    fn test_repartition_row_count() {
        let schema = make_schema_u64_i64();
        let rows: Vec<(u64, i64, i64)> = (1u64..=100).map(|i| (i, 1, i as i64 * 10)).collect();
        let b = make_batch(&schema, &rows);

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, 4);
        assert_eq!(total_rows(&sub_batches), 100, "total rows must equal input count");
    }

    #[test]
    fn test_repartition_routing_contract() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let vals: Vec<i64> = (0..64i64).map(|i| i * 997 + 1).collect();

        let mut b = Batch::with_schema(schema, vals.len());

        for (i, &v) in vals.iter().enumerate() {
            b.extend_pk((i + 1) as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &v.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), vals.len());

        for &v in &vals {
            // Routing by a payload column uses the canonical route key (signed
            // columns are sign-flipped via payload_route_key) so a payload FK
            // routes identically to the same value stored as a PK column.
            let route_key = crate::schema::payload_route_key(&v.to_le_bytes(), 0, 8, type_code::I64);
            let expected_partition = partition_for_key(route_key);
            let expected_worker = worker_for_partition(expected_partition, num_workers);
            let found = (0..sub_batches[expected_worker].count).any(|r| {
                i64::from_le_bytes(
                    sub_batches[expected_worker].col_data(0)[r * 8..r * 8 + 8]
                        .try_into()
                        .unwrap(),
                ) == v
            });
            assert!(found, "val={v} should be in worker {expected_worker}");
        }
    }

    #[test]
    fn test_repartition_merged_duplicate_rows() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        // Two sources with identical (PK=1, val=10) rows — merged output must NOT
        // claim consolidated because it uses PK-only comparison.
        let b0 = make_batch(&schema, &[(1, 1, 10)]);
        let b1 = make_batch(&schema, &[(1, 1, 10)]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter_consolidated(&sources, &[0u32], &schema, num_workers);

        assert_eq!(total_rows(&result), 2, "both rows must appear in output");
        for sb in &result {
            if sb.count > 0 {
                assert!(!sb.consolidated, "merged path must not claim consolidated");
            }
        }
    }

    #[test]
    fn test_distinct_update_same_pk() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();

        // Trace: (PK=1, val=100, w=+1) — a row inserted in a previous tick.
        let trace_batch = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut cursor_handle = CursorHandle::from_owned(&[trace_batch], schema);

        // Delta: UPDATE PK=1 sets val=100 → 200.
        // _enforce_unique_pk emits (PK=1, val=100, w=-1) and (PK=1, val=200, w=+1).
        // Both rows have the same PK but different payloads; sorted by payload ascending.
        let delta = make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]);

        let (out, _consolidated) = crate::ops::op_distinct(delta.into_inner(), cursor_handle.cursor_mut(), &schema);

        assert_eq!(
            out.count, 2,
            "expected 2 output rows after same-PK update, got {}",
            out.count
        );

        assert_eq!((out.get_pk(0) as u64), 1);
        let val0 = i64::from_le_bytes(out.col_data(0)[0..8].try_into().unwrap());
        assert_eq!(val0, 100);
        assert_eq!(out.get_weight(0), -1);

        assert_eq!((out.get_pk(1) as u64), 1);
        let val1 = i64::from_le_bytes(out.col_data(0)[8..16].try_into().unwrap());
        assert_eq!(val1, 200);
        assert_eq!(out.get_weight(1), 1);
    }

    #[test]
    fn test_relay_scatter_consolidated_output_order() {
        let schema = make_schema_u64_i64();
        // Source 0: PKs 1, 3, 5 (odd); source 1: PKs 2, 4, 6 (even).
        // After merge-walk each worker's batch must be PK-sorted.
        let b0 = make_batch(&schema, &[(1, 1, 0), (3, 1, 0), (5, 1, 0)]);
        let b1 = make_batch(&schema, &[(2, 1, 0), (4, 1, 0), (6, 1, 0)]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter_consolidated(&sources, &[0u32], &schema, 4);
        for sb in &result {
            for r in 1..sb.count {
                assert!(
                    sb.get_pk(r) >= sb.get_pk(r - 1),
                    "output must be PK-sorted (non-decreasing) within each worker"
                );
            }
        }
    }

    #[test]
    fn test_relay_scatter_consolidated_tie_breaking() {
        // Both sources emit PK=1. Source 0's row must appear before source 1's row
        // in every worker's output (ascending si tie-break).
        let schema = make_schema_u64_i64();
        let b0 = make_batch(&schema, &[(1, 1, 10)]);
        let b1 = make_batch(&schema, &[(1, 1, 20)]);
        let sources: Vec<Option<&ConsolidatedBatch>> = vec![Some(&b0), Some(&b1)];
        let result = op_relay_scatter_consolidated(&sources, &[0u32], &schema, 4);
        for sb in &result {
            if sb.count >= 2 {
                assert_eq!(sb.get_pk(0), sb.get_pk(1));
                let val0 = i64::from_le_bytes(sb.col_data(0)[0..8].try_into().unwrap());
                let val1 = i64::from_le_bytes(sb.col_data(0)[8..16].try_into().unwrap());
                assert_eq!(val0, 10, "source 0 row must come first");
                assert_eq!(val1, 20, "source 1 row must come second");
            }
        }
    }

    #[test]
    fn test_op_repartition_batches_compound_pk_routes_by_bytes() {
        // op_repartition_batches_mode must route compound-PK rows by raw PK
        // bytes (partition_for_pk_bytes), matching PartitionedTable::
        // local_index. The pre-fix code only branched on (single_pk &&
        // wide), falling through to hash_row_for_partition for narrow
        // compound PKs — which would route to a different worker than
        // the data lives on.
        let schema = make_narrow_compound_schema();
        let num_workers = 4;
        let b0 = make_narrow_compound_batch(
            &schema,
            &[
                (mk_compound_pk(1, 10), 1, 11),
                (mk_compound_pk(2, 0), 1, 12),
                (mk_compound_pk(5, 9), 1, 13),
            ],
        );
        let b1 = make_narrow_compound_batch(
            &schema,
            &[
                (mk_compound_pk(1, 5), 1, 21),
                (mk_compound_pk(3, 3), 1, 23),
                (mk_compound_pk(7, 0), 1, 33),
            ],
        );
        let sources: Vec<Option<&Batch>> = vec![Some(&b0), Some(&b1)];
        // col_indices = [0, 1] = the full PK set → compound-PK routing.
        let sub_batches =
            op_repartition_batches_mode(&sources, &[0u32, 1u32], &[], &schema, num_workers, RouteMode::GroupKey);
        assert_eq!(total_rows(&sub_batches), 6);
        for (w, sb) in sub_batches.iter().enumerate() {
            for r in 0..sb.count {
                let pk = sb.get_pk_bytes(r);
                let expected = worker_for_partition(partition_for_pk_bytes(pk), num_workers);
                assert_eq!(expected, w, "compound-PK row routed to wrong worker");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Compound JoinPromote scatter co-partition (both production functions)
    // -----------------------------------------------------------------------

    /// Schema with a compound, non-PK join key spanning a signed I64 and a U128
    /// column (packed stride 24 > 16). The PK is a separate U64 so the join key
    /// is NOT the PK and routing goes through the `route_group` / packer arm.
    fn make_join_key_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),  // col0: PK
                SchemaColumn::new(type_code::I64, 0),  // col1: signed join key part
                SchemaColumn::new(type_code::U128, 0), // col2: wide join key part
            ],
            &[0],
        )
    }

    fn make_join_key_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, u128)]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, c1, c2) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &c1.to_le_bytes()); // I64 payload (pi 0)
            b.extend_col(1, &c2.to_le_bytes()); // U128 payload (pi 1)
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_compound_join_promote_scatter_copartitions_both_functions() {
        // A compound JoinPromote scatter must route every row by the SAME packed
        // OPK bytes the reindex Map writes as `_join_pk` — i.e. by
        // `partition_for_pk_bytes(ReindexPacker::pack(cols, row))`, NOT by
        // `extract_group_key`. Exercised through BOTH production scatter
        // functions: the non-consolidated row loop in `op_repartition_batches_mode`
        // and the consolidated merge-walk's `route_group` in
        // `op_relay_scatter_consolidated_mode` — the latter through both the
        // single-source bulk-drain AND the multi-source K-way winner-select loop
        // (the merge body the unified walk rewrites). Covers signed-negative and
        // >16-byte composite keys, including a duplicate join key that must
        // co-locate (the no-dropped-rows / matching-keys-together guarantee).
        let schema = make_join_key_schema();
        let cols = [1u32, 2u32]; // (I64, U128) join key — non-PK, stride 24.
        let num_workers = 4;
        // PK ascending (consolidated invariant). Rows 0 and 3 share the join key
        // (-5, 100) → must land on the same worker.
        let rows: &[(u64, i64, u128)] = &[
            (1, -5, 100),
            (2, 7, 100),
            (3, -5, 100),
            (4, i64::MIN, 0),
            (5, 3, 0xdead_beef_cafe_0001),
            (6, -1, 1),
        ];
        let cb = make_join_key_batch(&schema, rows);

        let packer = ReindexPacker::new(&schema, &cols, &[]);
        let expected_worker = |sb: &Batch, r: usize| -> usize {
            let mut buf = [0u8; gnitz_wire::MAX_PK_BYTES];
            packer.pack_into(&mut buf[..packer.out_stride], &sb.as_mem_batch(), r);
            worker_for_partition(partition_for_pk_bytes(&buf[..packer.out_stride]), num_workers)
        };

        // Helper: assert every output row routed to the worker its packed key
        // dictates, no rows dropped, and the shared-key rows co-located.
        let check = |result: &[Batch], label: &str| {
            assert_eq!(total_rows(result), rows.len(), "{label}: no dropped rows");
            let mut worker_of_pk = std::collections::HashMap::new();
            for (w, sb) in result.iter().enumerate() {
                for r in 0..sb.count {
                    assert_eq!(expected_worker(sb, r), w, "{label}: row routed to wrong worker");
                    worker_of_pk.insert(sb.get_pk(r) as u64, w);
                }
            }
            // Rows with pk=1 and pk=3 share the join key (-5, 100): same worker.
            assert_eq!(
                worker_of_pk.get(&1),
                worker_of_pk.get(&3),
                "{label}: matching join keys must co-locate",
            );
        };

        // (a) Non-consolidated path.
        let repart =
            op_repartition_batches_mode(&[Some(&cb)], &cols, &[], &schema, num_workers, RouteMode::JoinPromote);
        check(&repart, "op_repartition_batches_mode");

        // (b) Consolidated merge-walk path, single source → bulk-drain `route_group`.
        let consol =
            op_relay_scatter_consolidated_mode(&[Some(&cb)], &cols, &[], &schema, num_workers, RouteMode::JoinPromote);
        check(&consol, "op_relay_scatter_consolidated_mode");

        // (c) Consolidated merge-walk path, TWO sources → the K-way winner-select
        // loop drives `route_group` (the body the unified walk rewrites). Split the
        // PK-ascending rows by parity into two sorted, consolidated sources so the
        // walk genuinely interleaves them; pk=1 and pk=3 (join key (-5,100)) both
        // live in the odd source yet must still co-locate by packed `_join_pk`.
        let cb_odd = make_join_key_batch(&schema, &[rows[0], rows[2], rows[4]]); // pk 1,3,5
        let cb_even = make_join_key_batch(&schema, &[rows[1], rows[3], rows[5]]); // pk 2,4,6
        let consol_multi = op_relay_scatter_consolidated_mode(
            &[Some(&cb_odd), Some(&cb_even)],
            &cols,
            &[],
            &schema,
            num_workers,
            RouteMode::JoinPromote,
        );
        check(
            &consol_multi,
            "op_relay_scatter_consolidated_mode (2 sources, K-way route_group)",
        );
    }

    /// A SINGLE promoted join key (arity 1, carried `T != 0`) must also route
    /// through the `ReindexPacker` — the packer fires on promotion, not just on a
    /// compound key. Covers both a payload key (`Narrow` I32 → I64, including a
    /// negative value so sign-extension is exercised) and a key that is the source
    /// PK (`Pk` arm), where the native-PK fast-path must be GATED OFF so the row
    /// routes by its `T`-wide `_join_pk`, not its narrow PK bytes.
    #[test]
    fn test_single_key_promote_scatter_copartitions() {
        let num_workers = 4;

        // ---- (1) Payload key: [U64 PK, I32 payload], reindex col1 → I64. ----
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I32, 0),
            ],
            &[0],
        );
        // PK ascending; rows pk=1 and pk=4 share key -5 → must co-locate.
        let rows: &[(u64, i32)] = &[(1, -5), (2, 7), (3, i32::MIN), (4, -5), (5, 0), (6, -1)];
        let mut b = Batch::with_schema(schema, rows.len());
        for &(pk, key) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &key.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        let cb = ConsolidatedBatch::new_unchecked(b);
        let cols = [1u32];
        let targets = [type_code::I64];

        let packer = ReindexPacker::new(&schema, &cols, &targets);
        let expected_worker = |sb: &Batch, r: usize| -> usize {
            let mut buf = [0u8; gnitz_wire::MAX_PK_BYTES];
            packer.pack_into(&mut buf[..packer.out_stride], &sb.as_mem_batch(), r);
            worker_for_partition(partition_for_pk_bytes(&buf[..packer.out_stride]), num_workers)
        };
        let check = |result: &[Batch], label: &str| {
            assert_eq!(total_rows(result), rows.len(), "{label}: no dropped rows");
            let mut worker_of_pk = std::collections::HashMap::new();
            for (w, sb) in result.iter().enumerate() {
                for r in 0..sb.count {
                    assert_eq!(
                        expected_worker(sb, r),
                        w,
                        "{label}: promoted single key routed to wrong worker"
                    );
                    worker_of_pk.insert(sb.get_pk(r) as u64, w);
                }
            }
            assert_eq!(
                worker_of_pk.get(&1),
                worker_of_pk.get(&4),
                "{label}: equal promoted keys must co-locate"
            );
        };
        let repart = op_repartition_batches_mode(
            &[Some(&cb)],
            &cols,
            &targets,
            &schema,
            num_workers,
            RouteMode::JoinPromote,
        );
        check(&repart, "payload-key op_repartition_batches_mode");
        let consol = op_relay_scatter_consolidated_mode(
            &[Some(&cb)],
            &cols,
            &targets,
            &schema,
            num_workers,
            RouteMode::JoinPromote,
        );
        check(&consol, "payload-key op_relay_scatter_consolidated_mode");

        // ---- (2) PK key: [I32 PK, U64 payload], reindex col0 → I64. The native
        // PK fast-path must be gated off so routing uses the packed I64 key. ----
        let pk_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        let pk_rows: &[(i32, u64)] = &[(-5, 9), (-1, 9), (3, 9), (i32::MIN, 9)];
        let mut pb = Batch::with_schema(pk_schema, pk_rows.len());
        for &(pk, v) in pk_rows {
            let mut opk = [0u8; 4];
            gnitz_wire::encode_pk_column(&pk.to_le_bytes(), type_code::I32, &mut opk);
            pb.extend_pk_bytes(&opk);
            pb.extend_weight(&1i64.to_le_bytes());
            pb.extend_null_bmp(&0u64.to_le_bytes());
            pb.extend_col(0, &v.to_le_bytes());
            pb.count += 1;
        }
        pb.sorted = true;
        pb.consolidated = true;
        let pk_cb = ConsolidatedBatch::new_unchecked(pb);
        let pk_cols = [0u32];
        let pk_targets = [type_code::I64];
        let pk_packer = ReindexPacker::new(&pk_schema, &pk_cols, &pk_targets);
        let pk_repart = op_repartition_batches_mode(
            &[Some(&pk_cb)],
            &pk_cols,
            &pk_targets,
            &pk_schema,
            num_workers,
            RouteMode::JoinPromote,
        );
        assert_eq!(total_rows(&pk_repart), pk_rows.len(), "PK-key: no dropped rows");
        for (w, sb) in pk_repart.iter().enumerate() {
            for r in 0..sb.count {
                let mut buf = [0u8; gnitz_wire::MAX_PK_BYTES];
                pk_packer.pack_into(&mut buf[..pk_packer.out_stride], &sb.as_mem_batch(), r);
                assert_eq!(
                    worker_for_partition(partition_for_pk_bytes(&buf[..pk_packer.out_stride]), num_workers),
                    w,
                    "PK-key fast-path must be gated: routed by native PK not packed T"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Regression-guard microbench for the retained `order_cache`
    // -----------------------------------------------------------------------

    /// The cache-FREE merge walk: the alternative the relay would collapse to if
    /// `order_cache` were dropped — one body, but the winner scan calls the
    /// canonical `compare_pk_ordering` on live OPK bytes every comparison (exactly
    /// what the sibling loser-tree merges in `repr/merge.rs` / `repr/heap.rs` /
    /// `read_cursor` do), instead of a register compare of a precomputed key.
    /// Byte-for-byte the same output as `relay_walk_inner`; it exists only so
    /// `relay_scatter_merge_walk_bench` can measure what the cache buys on the
    /// relay's O(K) linear winner-scan.
    fn relay_walk_cachefree(
        mem_batches: &[Option<MemBatch<'_>>],
        w_map: &[usize; 256],
        worker_rows: &mut [Vec<(u8, u32)>],
        mut cursors: [u32; 256],
        mut active_sources: [u8; 256],
        mut num_active: usize,
    ) {
        while num_active > 0 {
            if num_active == 1 {
                let si = active_sources[0] as usize;
                let mb = mem_batches[si].as_ref().unwrap();
                for row in cursors[si] as usize..mb.count {
                    worker_rows[w_map[partition_for_pk_bytes(mb.get_pk_bytes(row))]].push((si as u8, row as u32));
                }
                return;
            }
            let mut best_pos = 0usize;
            let mut best_si = active_sources[0];
            let mut best_bytes = mem_batches[best_si as usize]
                .as_ref()
                .unwrap()
                .get_pk_bytes(cursors[best_si as usize] as usize);
            #[allow(clippy::needless_range_loop)]
            for pos in 1..num_active {
                let si = active_sources[pos];
                let bytes = mem_batches[si as usize]
                    .as_ref()
                    .unwrap()
                    .get_pk_bytes(cursors[si as usize] as usize);
                let ord = compare_pk_ordering(bytes, best_bytes);
                if ord == Ordering::Less || (ord == Ordering::Equal && si < best_si) {
                    best_pos = pos;
                    best_si = si;
                    best_bytes = bytes;
                }
            }
            let best_si = best_si as usize;
            let row = cursors[best_si] as usize;
            cursors[best_si] += 1;
            let mb = mem_batches[best_si].as_ref().unwrap();
            worker_rows[w_map[partition_for_pk_bytes(mb.get_pk_bytes(row))]].push((best_si as u8, row as u32));
            let new_cur = cursors[best_si] as usize;
            if new_cur == mb.count {
                num_active -= 1;
                active_sources[best_pos] = active_sources[num_active];
            }
        }
    }

    /// Isolated merge-walk bench: K=8 interleaved pre-consolidated sources, timing
    /// ONLY the winner-scan + route + index push (no `write_to_batch` /
    /// `scatter_multi_source` row copy, which otherwise dominates and masks the
    /// walk). Reports cached (`relay_walk_inner`) vs cache-free
    /// (`relay_walk_cachefree`) rows/s per stride. This is what justifies keeping
    /// `order_cache`: the cached register-compare must beat re-packing the OPK key
    /// every comparison. Last measured ~1.6x at stride 8/16 and ~3.3x at stride 24
    /// (where cache-free pays the full 16-byte prefix pack on every pairwise
    /// compare), so dropping the cache to match the loser-tree siblings would
    /// regress this O(K) linear winner-scan substantially.
    /// `cd crates && cargo test -p gnitz-engine --release relay_scatter_merge_walk_bench -- --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore]
    fn relay_scatter_merge_walk_bench() {
        use std::hint::black_box;
        use std::time::{Duration, Instant};

        const K: usize = 8;
        const N: usize = 100_016; // divisible by K
        const ITERS: usize = 40;
        let num_workers = 4;
        let per_src = N / K;

        // Run both walk variants over the same K sources, assert identical output,
        // and time each. `build` keeps the three stride builders' batches alive.
        let bench = |label: &str, cbs: &[ConsolidatedBatch]| {
            let mem_batches: Vec<Option<MemBatch>> = cbs.iter().map(|cb| Some(cb.as_mem_batch())).collect();
            let total: usize = mem_batches.iter().flatten().map(|m| m.count).sum();
            let w_map = build_w_map(num_workers);
            let cursors = [0u32; 256];
            let mut active_sources = [0u8; 256];
            let mut num_active = 0usize;
            for (si, mb) in mem_batches.iter().enumerate() {
                if mb.as_ref().is_some_and(|m| m.count > 0) {
                    active_sources[num_active] = si as u8;
                    num_active += 1;
                }
            }
            let route_pk = |mb: &MemBatch, row: usize| partition_for_pk_bytes(mb.get_pk_bytes(row));
            let mut order_cache = [0u128; 256];
            let reset_cache = |oc: &mut [u128; 256]| {
                for &s8 in active_sources.iter().take(num_active) {
                    let s = s8 as usize;
                    oc[s] = pk_sort_key(mem_batches[s].as_ref().unwrap().get_pk_bytes(0));
                }
            };
            let mut wr_a: Vec<Vec<(u8, u32)>> = (0..num_workers).map(|_| Vec::with_capacity(total)).collect();
            let mut wr_b: Vec<Vec<(u8, u32)>> = (0..num_workers).map(|_| Vec::with_capacity(total)).collect();

            // Correctness: cached and cache-free must agree byte-for-byte.
            reset_cache(&mut order_cache);
            relay_walk_inner(
                &mem_batches,
                &w_map,
                &mut wr_a,
                &mut order_cache,
                cursors,
                active_sources,
                num_active,
                route_pk,
            );
            relay_walk_cachefree(&mem_batches, &w_map, &mut wr_b, cursors, active_sources, num_active);
            assert_eq!(wr_a, wr_b, "{label}: cached vs cache-free walk diverged");

            let mut dur_a = Duration::ZERO;
            let mut dur_b = Duration::ZERO;
            for _ in 0..ITERS {
                wr_a.iter_mut().for_each(Vec::clear);
                reset_cache(&mut order_cache);
                let t = Instant::now();
                relay_walk_inner(
                    &mem_batches,
                    &w_map,
                    &mut wr_a,
                    &mut order_cache,
                    cursors,
                    active_sources,
                    num_active,
                    route_pk,
                );
                dur_a += t.elapsed();
                black_box(&wr_a);

                wr_b.iter_mut().for_each(Vec::clear);
                let t = Instant::now();
                relay_walk_cachefree(&mem_batches, &w_map, &mut wr_b, cursors, active_sources, num_active);
                dur_b += t.elapsed();
                black_box(&wr_b);
            }
            let rps = |d: Duration| (total * ITERS) as f64 / d.as_secs_f64() / 1e6;
            println!(
                "{label:>10}: cached {:6.1} Mrows/s   cache-free {:6.1} Mrows/s   cached/cache-free {:.2}x",
                rps(dur_a),
                rps(dur_b),
                rps(dur_a) / rps(dur_b),
            );
        };

        // stride 8 — single U64 PK.
        let s8 = make_schema_u64_i64();
        let b8: Vec<ConsolidatedBatch> = (0..K)
            .map(|s| {
                let rows: Vec<(u64, i64, i64)> = (0..per_src).map(|j| ((s + K * j) as u64, 1, j as i64)).collect();
                make_batch(&s8, &rows)
            })
            .collect();
        bench("stride 8", &b8);

        // stride 16 — (U64, U64) compound PK. c0 = global index (ascending), c1 = 0;
        // `make_narrow_compound_batch` reads the low 64 bits as c0 (see `mk_compound_pk`).
        let s16 = make_narrow_compound_schema();
        let b16: Vec<ConsolidatedBatch> = (0..K)
            .map(|s| {
                let rows: Vec<(u128, i64, i64)> = (0..per_src).map(|j| ((s + K * j) as u128, 1, j as i64)).collect();
                make_narrow_compound_batch(&s16, &rows)
            })
            .collect();
        bench("stride 16", &b16);

        // stride 24 — (U64, U64, U64) wide PK. c0 = global index (ascending), c1=c2=0.
        let s24 = wide_pk_3xu64_schema();
        let b24: Vec<ConsolidatedBatch> = (0..K)
            .map(|s| {
                let rows: Vec<(u64, u64, u64, i64, i64)> =
                    (0..per_src).map(|j| ((s + K * j) as u64, 0, 0, 1, j as i64)).collect();
                make_wide_batch(&s24, &rows)
            })
            .collect();
        bench("stride 24", &b24);
    }
}
