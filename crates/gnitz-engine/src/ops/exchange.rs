//! Exchange repartitioning: PartitionRouter, op_repartition_batch, op_relay_scatter_consolidated, op_relay_scatter, op_multi_scatter.

use std::cell::RefCell;

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ConsolidatedBatch, MemBatch, partition_for_key};

use super::util::{extract_group_key, payload_idx};

// ---------------------------------------------------------------------------
// Exchange repartitioning
// ---------------------------------------------------------------------------

// Thread-local pool: reuse Vec<Vec<u32>> index scratch across calls so
// steady-state repartition/scatter ops allocate nothing for routing tables.
thread_local! {
    static SCATTER_INDICES: RefCell<Vec<Vec<u32>>> = const { RefCell::new(Vec::new()) };
}

/// Build a 256-entry partition→worker lookup table, hoisting the division out
/// of the per-row loop. `partition_for_key` always returns values in 0..=255.
#[inline]
fn build_w_map(num_workers: usize) -> [usize; 256] {
    let mut map = [0usize; 256];
    let chunk = 256 / num_workers;
    for (p, item) in map.iter_mut().enumerate() {
        *item = (p / chunk).min(num_workers - 1);
    }
    map
}

#[inline]
pub fn worker_for_partition(partition: usize, num_workers: usize) -> usize {
    let chunk = 256 / num_workers;
    (partition / chunk).min(num_workers - 1)
}

// ---------------------------------------------------------------------------
// Partition routing cache
// ---------------------------------------------------------------------------

/// Master-side routing cache: maps (table_id, col_idx, key_lo) → worker.
///
/// Populated by `record_routing` after repartitioning unique-indexed columns.
/// Lets the master unicast index seeks instead of broadcasting on cache hit.
pub struct PartitionRouter {
    index_routing: std::collections::HashMap<(u32, u32, u64), u32>,
}

impl PartitionRouter {
    pub fn new() -> Self {
        PartitionRouter {
            index_routing: std::collections::HashMap::new(),
        }
    }

    /// Returns the worker for a given index key, or -1 on cache miss.
    pub fn worker_for_index_key(&self, tid: u32, col_idx: u32, key_lo: u64) -> i32 {
        match self.index_routing.get(&(tid, col_idx, key_lo)) {
            Some(&w) => w as i32,
            None => -1,
        }
    }

    /// Scan every row in `batch` and record or retract its index key → worker mapping.
    /// Rows with negative weight retract; non-negative weight records.
    pub fn record_routing(
        &mut self,
        batch: &Batch,
        schema: &SchemaDescriptor,
        tid: u32,
        col_idx: u32,
        worker: u32,
    ) {
        let mb = batch.as_mem_batch();
        let pki = schema.pk_index as usize;
        for row in 0..batch.count {
            let weight = batch.get_weight(row);
            let key_lo = if col_idx as usize == pki {
                mb.get_pk(row) as u64
            } else {
                let pi = payload_idx(col_idx as usize, pki);
                let col = &schema.columns[col_idx as usize];
                let col_size = col.size as usize;
                if col.type_code == crate::schema::type_code::STRING {
                    // Hash string content to u64 (same approach as extract_group_key)
                    let struct_bytes = mb.get_col_ptr(row, pi, 16);
                    let length = crate::util::read_u32_le(struct_bytes, 0) as usize;
                    if length == 0 {
                        0u64
                    } else if length <= crate::schema::SHORT_STRING_THRESHOLD {
                        crate::xxh::checksum(&struct_bytes[4..4 + length])
                    } else {
                        let heap_offset = u64::from_le_bytes(struct_bytes[8..16].try_into().unwrap()) as usize;
                        crate::xxh::checksum(&mb.blob[heap_offset..heap_offset + length])
                    }
                } else if col.type_code == crate::schema::type_code::U128 {
                    // XOR the two u64 halves
                    let bytes = mb.get_col_ptr(row, pi, 16);
                    let lo = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
                    let hi = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
                    lo ^ hi
                } else {
                    let bytes = mb.get_col_ptr(row, pi, col_size);
                    let mut buf = [0u8; 8];
                    buf[..col_size].copy_from_slice(bytes);
                    u64::from_le_bytes(buf)
                }
            };
            let map_key = (tid, col_idx, key_lo);
            if weight < 0 {
                self.index_routing.remove(&map_key);
            } else {
                self.index_routing.insert(map_key, worker);
            }
        }
    }
}

fn hash_row_for_partition(
    mb: &MemBatch,
    row: usize,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
) -> usize {
    let pki = schema.pk_index as usize;
    if col_indices.len() == 1 && col_indices[0] as usize == pki {
        return partition_for_key(mb.get_pk(row));
    }
    let (lo, hi) = extract_group_key(mb, row, schema, col_indices);
    partition_for_key(crate::util::make_pk(lo, hi))
}

pub fn op_repartition_batch(
    batch: &Batch,
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    gnitz_debug!("op_repartition_batch: count={} num_workers={}", batch.count, num_workers);
    let npc = schema.num_columns as usize - 1;
    let n = batch.count;
    let mb = batch.as_mem_batch();
    let w_map = build_w_map(num_workers);

    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        if worker_indices.len() < num_workers {
            worker_indices.resize_with(num_workers, Vec::new);
        }
        for w in 0..num_workers { worker_indices[w].clear(); }

        for i in 0..n {
            let partition = hash_row_for_partition(&mb, i, col_indices, schema);
            worker_indices[w_map[partition]].push(i as u32);
        }

        let mut result: Vec<Option<Batch>> = (0..num_workers).map(|_| None).collect();
        for w in 0..num_workers {
            if !worker_indices[w].is_empty() {
                result[w] = Some(Batch::from_indexed_rows(&mb, &worker_indices[w], schema));
            }
        }
        result
            .into_iter()
            .map(|opt| opt.unwrap_or_else(|| Batch::empty(npc)))
            .collect()
    })
}

pub fn op_repartition_batches(
    sources: &[Option<&Batch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    gnitz_debug!("op_repartition_batches: sources={} num_workers={}", sources.len(), num_workers);
    let npc = schema.num_columns as usize - 1;
    let w_map = build_w_map(num_workers);
    let mut dest: Vec<Option<Batch>> = (0..num_workers).map(|_| None).collect();

    SCATTER_INDICES.with(|pool| {
        let mut worker_indices = pool.borrow_mut();
        if worker_indices.len() < num_workers {
            worker_indices.resize_with(num_workers, Vec::new);
        }

        for src_opt in sources {
            let src = match src_opt {
                Some(s) if s.count > 0 => *s,
                _ => continue,
            };
            let mb = src.as_mem_batch();
            for w in 0..num_workers { worker_indices[w].clear(); }
            for i in 0..src.count {
                let partition = hash_row_for_partition(&mb, i, col_indices, schema);
                worker_indices[w_map[partition]].push(i as u32);
            }
            for w in 0..num_workers {
                if !worker_indices[w].is_empty() {
                    let d = dest[w].get_or_insert_with(|| {
                        Batch::with_schema(*schema, worker_indices[w].len())
                    });
                    d.append_indexed_rows(&mb, &worker_indices[w], schema);
                }
            }
        }
    });

    dest.into_iter()
        .map(|opt| opt.unwrap_or_else(|| Batch::empty(npc)))
        .collect()
}

pub fn op_repartition_batches_merged(
    sources: &[Option<&Batch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    gnitz_debug!(
        "op_repartition_batches_merged: sources={} num_workers={}",
        sources.len(),
        num_workers
    );
    let npc = schema.num_columns as usize - 1;
    let n = sources.len();
    let w_map = build_w_map(num_workers);
    let mut cursors: Vec<usize> = vec![0; n];
    let mut dest: Vec<Option<Batch>> = (0..num_workers).map(|_| None).collect();

    // Pre-compute MemBatch views to avoid per-row allocation in as_mem_batch()
    let mem_batches: Vec<Option<MemBatch>> = sources
        .iter()
        .map(|src_opt| match src_opt {
            Some(s) if s.count > 0 => Some(s.as_mem_batch()),
            _ => None,
        })
        .collect();

    loop {
        let mut min_w: Option<usize> = None;
        for w in 0..n {
            if mem_batches[w].is_none() {
                continue;
            }
            let src = sources[w].as_ref().unwrap();
            let idx = cursors[w];
            if idx >= src.count {
                continue;
            }
            match min_w {
                None => {
                    min_w = Some(w);
                }
                Some(mw) => {
                    let min_src = sources[mw].as_ref().unwrap();
                    let min_idx = cursors[mw];
                    if src.get_pk(idx) < min_src.get_pk(min_idx) {
                        min_w = Some(w);
                    }
                }
            }
        }
        let min_w = match min_w {
            None => break,
            Some(w) => w,
        };

        let src = sources[min_w].as_ref().unwrap();
        let src_idx = cursors[min_w];
        let mb = mem_batches[min_w].as_ref().unwrap();
        let partition = hash_row_for_partition(mb, src_idx, col_indices, schema);
        let dw = w_map[partition];

        let d = dest[dw].get_or_insert_with(|| Batch::with_schema(*schema, 16));
        d.append_batch(src, src_idx, src_idx + 1);
        cursors[min_w] += 1;
    }

    dest.into_iter()
        .map(|opt| match opt {
            Some(mut d) => {
                d.sorted = true;
                d.consolidated = false;
                d.set_schema(*schema);
                d
            }
            None => Batch::empty(npc),
        })
        .collect()
}

/// Scatter pre-consolidated batches across workers using a merge-walk.
/// All sources must satisfy the consolidated invariant (sorted, no duplicate PKs).
/// Output batches are sorted but not consolidated (duplicate PKs can appear across sources).
pub fn op_relay_scatter_consolidated(
    sources: &[Option<&ConsolidatedBatch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    let has_any = sources.iter().any(|src_opt| matches!(src_opt, Some(s) if s.count > 0));
    gnitz_debug!(
        "op_relay_scatter_consolidated: sources={}",
        sources.iter().filter(|s| matches!(s, Some(sb) if sb.count > 0)).count(),
    );
    if !has_any {
        let npc = schema.num_columns as usize - 1;
        return (0..num_workers).map(|_| Batch::empty(npc)).collect();
    }
    let plain: Vec<Option<&Batch>> = sources
        .iter()
        .copied()
        .map(|opt| opt.map(|cb| -> &Batch { cb }))
        .collect();
    op_repartition_batches_merged(&plain, col_indices, schema, num_workers)
}

/// Scatter non-consolidated batches across workers by hashing each row.
pub fn op_relay_scatter(
    sources: &[Option<&Batch>],
    col_indices: &[u32],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Batch> {
    gnitz_debug!(
        "op_relay_scatter: sources={}",
        sources.iter().filter(|s| matches!(s, Some(sb) if sb.count > 0)).count(),
    );
    op_repartition_batches(sources, col_indices, schema, num_workers)
}

/// Scatter a single batch across workers using multiple independent column
/// specifications in one pass.  Returns one `Vec<Batch>` per spec.
///
/// Only used in tests; retained as `#[cfg(test)]` so the helper doesn't
/// bloat the production binary.
#[cfg(test)]
pub fn op_multi_scatter(
    batch: &Batch,
    col_specs: &[&[u32]],
    schema: &SchemaDescriptor,
    num_workers: usize,
) -> Vec<Vec<Batch>> {
    let n = batch.count;
    let n_specs = col_specs.len();
    let npc = schema.num_columns as usize - 1;
    let pki = schema.pk_index as usize;
    let mb = batch.as_mem_batch();
    let w_map = build_w_map(num_workers);
    let needed = n_specs * num_workers;

    SCATTER_INDICES.with(|pool| {
        let mut flat_indices = pool.borrow_mut();
        if flat_indices.len() < needed {
            flat_indices.resize_with(needed, Vec::new);
        }
        for slot in flat_indices[..needed].iter_mut() { slot.clear(); }

        for i in 0..n {
            for si in 0..n_specs {
                let partition = hash_row_for_partition(&mb, i, col_specs[si], schema);
                flat_indices[si * num_workers + w_map[partition]].push(i as u32);
            }
        }

        let mut results: Vec<Vec<Batch>> = (0..n_specs)
            .map(|_| (0..num_workers).map(|_| Batch::empty(npc)).collect())
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
            if spec.len() == 1 && spec[0] as usize == pki {
                if crate::storage::ConsolidatedBatch::from_batch_ref(batch).is_some() {
                    for w in 0..num_workers {
                        if results[si][w].count > 0 {
                            results[si][w].sorted = true;
                            results[si][w].consolidated = true;
                        }
                    }
                } else if batch.sorted {
                    for w in 0..num_workers {
                        if results[si][w].count > 0 {
                            results[si][w].sorted = true;
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
    use crate::schema::{SchemaColumn, SchemaDescriptor, SHORT_STRING_THRESHOLD, type_code};

    fn make_schema_u64_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0,
            size: 0,
            nullable: 0,
            _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U64,
            size: 8,
            nullable: 0,
            _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: type_code::I64,
            size: 8,
            nullable: 0,
            _pad: 0,
        };
        SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns,
        }
    }

    fn make_schema_u128_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0,
            size: 0,
            nullable: 0,
            _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U128,
            size: 16,
            nullable: 0,
            _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: type_code::I64,
            size: 8,
            nullable: 0,
            _pad: 0,
        };
        SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns,
        }
    }

    fn make_schema_u64_string() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0,
            size: 0,
            nullable: 0,
            _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U64,
            size: 8,
            nullable: 0,
            _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: type_code::STRING,
            size: 16,
            nullable: 0,
            _pad: 0,
        };
        SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns,
        }
    }

    fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, val) in rows {
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
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
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
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
    fn test_repartition_batch_pk_routing() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let pk_vals: &[u64] = &[1, 7, 42, 100, 255, 1024, 65537, 999983];

        let mut b = Batch::with_schema(schema, pk_vals.len());

        for &pk in pk_vals {
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), pk_vals.len());

        for &pk in pk_vals {
            let expected = worker_for_partition(
                partition_for_key(pk as u128),
                num_workers,
            );
            let found = (0..sub_batches[expected].count)
                .any(|r| (sub_batches[expected].get_pk(r) as u64) == pk);
            assert!(found, "pk={pk} not found in worker {expected}");
        }
    }

    #[test]
    fn test_repartition_batch_u128_pk() {
        let schema = make_schema_u128_i64();
        let num_workers = 4;
        let pk_pairs: &[(u64, u64)] = &[
            (0, 1),
            (0xDEAD_BEEF, 0xCAFE_BABE),
            (u64::MAX, u64::MAX),
            (42, 7),
        ];

        let n = pk_pairs.len();
        let mut b = Batch::with_schema(schema, n);

        for &(lo, hi) in pk_pairs {
            b.extend_pk_lo(&lo.to_le_bytes());
            b.extend_pk_hi(&hi.to_le_bytes());
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), n);

        for &(lo, hi) in pk_pairs {
            let expected = worker_for_partition(
                partition_for_key(crate::util::make_pk(lo, hi)),
                num_workers,
            );
            let found = (0..sub_batches[expected].count).any(|r| {
                sub_batches[expected].get_pk(r) == crate::util::make_pk(lo, hi)
            });
            assert!(found, "pk=({lo},{hi}) not in worker {expected}");
        }
    }

    #[test]
    fn test_repartition_batch_group_col() {
        let schema = make_schema_u64_i64();
        let num_workers = 4;
        let same_val: i64 = 42;

        let mut b = Batch::with_schema(schema, 4);

        for pk in [1u64, 2, 3, 4] {
            b.extend_pk_lo(&pk.to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
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
        for w in 0..num_workers {
            for r in 0..sub_batches[w].count {
                if (sub_batches[w].get_pk(r) as u64) == 1 {
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
        for w in 0..num_workers {
            for r in 0..sub2[w].count {
                if (sub2[w].get_pk(r) as u64) == 10 {
                    worker_of_10 = Some(w);
                }
            }
        }
        let w10 = worker_of_10.expect("pk=10 must be in some worker");
        let pk11_same = (0..sub2[w10].count).any(|r| (sub2[w10].get_pk(r) as u64) == 11);
        assert!(pk11_same, "same long string must route to same worker");
    }

    #[test]
    fn test_repartition_batch_no_consolidation() {
        let schema = make_schema_u64_i64();
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        assert!(b.sorted && b.consolidated);

        let sub_batches = op_repartition_batch(&b, &[0u32], &schema, 4);
        for sb in &sub_batches {
            if sb.count > 0 {
                assert!(!sb.consolidated, "repartition must not propagate consolidated");
                assert!(!sb.sorted, "repartition must not propagate sorted");
            }
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
                assert!(!sb.consolidated, "merged path uses PK-only comparison, must not claim consolidated");
                assert!(sb.sorted, "merged path output must be sorted");
            }
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
                assert!(!sb.consolidated, "non-consolidated path output must not be consolidated");
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
            b.extend_pk_lo(&((i + 1) as u64).to_le_bytes());
            b.extend_pk_hi(&0u64.to_le_bytes());
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &v.to_le_bytes());
            b.count += 1;
        }

        let sub_batches = op_repartition_batch(&b, &[1u32], &schema, num_workers);
        assert_eq!(total_rows(&sub_batches), vals.len());

        for &v in &vals {
            let expected_partition = partition_for_key(v as u128);
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
    fn partition_router_basic() {
        let schema = make_schema_u64_i64();
        let mut router = PartitionRouter::new();

        let b = make_batch(&schema, &[(42, 1, 0)]);
        router.record_routing(&b, &schema, 1, 0, 3);
        assert_eq!(router.worker_for_index_key(1, 0, 42), 3);
        assert_eq!(router.worker_for_index_key(1, 0, 99), -1);

        // Retract: negative weight removes the entry
        let b2 = make_batch(&schema, &[(42, -1, 0)]);
        router.record_routing(&b2, &schema, 1, 0, 3);
        assert_eq!(router.worker_for_index_key(1, 0, 42), -1);
    }

    #[test]
    fn test_partition_router_string_col() {
        let schema = make_schema_u64_string();
        let mut router = PartitionRouter::new();

        // Short string
        let b = make_batch_str(&schema, &[(1, 1, "hello")]);
        router.record_routing(&b, &schema, 1, 1, 2);
        // Must not panic — the old code would overflow copying 16 bytes into 8-byte buffer.
        // The key_lo is a hash, so we just check it was stored and is retrievable.
        // record_routing uses col_idx=1 (STRING column), so look up the hashed key.
        let mb = b.as_mem_batch();
        let struct_bytes = mb.get_col_ptr(0, 0, 16); // payload_idx(1, 0) = 0
        let length = crate::util::read_u32_le(struct_bytes, 0) as usize;
        let hashed_key = crate::xxh::checksum(&struct_bytes[4..4 + length]);
        assert_eq!(router.worker_for_index_key(1, 1, hashed_key), 2);

        // Long string (> 12 bytes)
        let b2 = make_batch_str(&schema, &[(2, 1, "a_long_string_value")]);
        router.record_routing(&b2, &schema, 1, 1, 3);
        // Must not panic
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
        use std::sync::Arc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: (PK=1, val=100, w=+1) — a row inserted in a previous tick.
        let trace_batch = Arc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut cursor_handle = CursorHandle::from_owned(&[trace_batch], schema);

        // Delta: UPDATE PK=1 sets val=100 → 200.
        // _enforce_unique_pk emits (PK=1, val=100, w=-1) and (PK=1, val=200, w=+1).
        // Both rows have the same PK but different payloads; sorted by payload ascending.
        let delta = make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]);

        let (out, _consolidated) =
            crate::ops::op_distinct(delta.into_inner(), cursor_handle.cursor_mut(), &schema);

        assert_eq!(
            out.count,
            2,
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
}
