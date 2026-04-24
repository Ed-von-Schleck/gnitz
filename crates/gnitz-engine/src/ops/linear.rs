//! Linear operators: op_filter, op_map, op_negate, op_union.
//! Also includes PK promotion for reindex (GROUP BY).

use crate::schema::{SchemaColumn, SchemaDescriptor, SHORT_STRING_THRESHOLD, type_code};
use crate::storage::{Batch, ConsolidatedBatch, MemBatch};
use crate::expr::ScalarFuncKind;
use crate::xxh;


// ---------------------------------------------------------------------------
// Linear operators
// ---------------------------------------------------------------------------

/// Filter: retain rows where predicate returns true.
/// Uses contiguous-range bulk copy for efficiency.
/// For batches of ≥ 16 rows with an Interpreted predicate, evaluates all rows
/// via the batch evaluator and scans the resulting bitmask.
pub fn op_filter(
    batch: &Batch,
    func: &ScalarFuncKind,
    schema: &SchemaDescriptor,
) -> Batch {
    let n = batch.count;
    if n == 0 {
        let npc = schema.num_columns as usize - 1;
        return Batch::empty(npc);
    }

    let mb = batch.as_mem_batch();
    let mut output = Batch::with_schema(*schema, n);

    // If the batch has out-of-line string data, pre-clone the blob so that
    // 16-byte string structs can be copied verbatim (no per-row relocation).
    // Offsets inside the structs remain valid because both blobs are identical.
    let blob_passthrough = !batch.blob.is_empty()
        && (0..schema.num_columns as usize)
            .any(|ci| schema.columns[ci].type_code == type_code::STRING);
    if blob_passthrough {
        output.share_blob_from(batch);
    }

    let append_range = |out: &mut Batch, start: usize, end: usize| {
        if blob_passthrough {
            out.append_batch_no_blob_reloc(batch, start, end);
        } else {
            out.append_batch(batch, start, end);
        }
    };

    // Batch evaluator path: evaluate all n rows, scan resulting bitmask.
    if let Some(bits) = func.filter_batch_bits(&mb, n, schema) {
        let mut range_start: isize = -1;
        let words = n.div_ceil(64);
        for (w, &word) in bits[..words].iter().enumerate() {
            let row_base = w * 64;
            let chunk = (row_base + 64).min(n) - row_base;

            if word == 0 {
                if range_start >= 0 {
                    append_range(&mut output, range_start as usize, row_base);
                    range_start = -1;
                }
            } else if word == u64::MAX || (chunk < 64 && word == (1u64 << chunk) - 1) {
                if range_start < 0 {
                    range_start = row_base as isize;
                }
            } else {
                for i in 0..chunk {
                    let passes = (word >> i) & 1 != 0;
                    let abs = row_base + i;
                    if passes {
                        if range_start < 0 { range_start = abs as isize; }
                    } else if range_start >= 0 {
                        append_range(&mut output, range_start as usize, abs);
                        range_start = -1;
                    }
                }
            }
        }
        if range_start >= 0 {
            append_range(&mut output, range_start as usize, n);
        }
    } else {
        // Per-row path (small batches or PassAll)
        let mut range_start: isize = -1;
        for i in 0..n {
            if func.evaluate_predicate(&mb, i, schema) {
                if range_start < 0 {
                    range_start = i as isize;
                }
            } else if range_start >= 0 {
                append_range(&mut output, range_start as usize, i);
                range_start = -1;
            }
        }
        if range_start >= 0 {
            append_range(&mut output, range_start as usize, n);
        }
    }

    // append_batch resets sorted+consolidated to false; restore them based on input.
    if ConsolidatedBatch::from_batch_ref(batch).is_some() {
        output.sorted = true;
        output.consolidated = true;
    } else {
        output.sorted = batch.sorted;
        output.consolidated = false;
    }

    gnitz_debug!("op_filter: in={} out={} func={}", n, output.count, func.kind_name());
    output
}

/// Map: transform batch via scalar function.
/// If reindex_col >= 0, computes new PK from that column value (GROUP BY).
pub fn op_map(
    batch: &Batch,
    func: &ScalarFuncKind,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    reindex_col: i32,
) -> Batch {
    if batch.count == 0 {
        let out_npc = out_schema.num_columns as usize - 1;
        return Batch::empty(out_npc);
    }

    if reindex_col < 0 {
        // Batch path
        let mut result = func.evaluate_map_batch(batch, in_schema, out_schema);
        result.sorted = batch.sorted;
        gnitz_debug!("op_map: in={} out={} reindex=-1 func={}", batch.count, result.count, func.kind_name());
        return result;
    }

    // Per-row path with reindex
    let in_mb = batch.as_mem_batch();
    let ri_col = reindex_col as usize;

    // Evaluate the map batch (without reindex) to get column data
    let mut output = func.evaluate_map_batch(batch, in_schema, out_schema);

    // Overwrite PK with promoted values from reindex column
    for row in 0..output.count {
        let pk = promote_col_to_pk(&in_mb, row, ri_col, in_schema);
        output.set_pk_at(row, pk);
    }

    output.sorted = false;
    output.consolidated = false;
    gnitz_debug!("op_map: in={} out={} reindex={} func={}", batch.count, output.count, reindex_col, func.kind_name());
    output
}

/// Negate: flip the sign of every weight.
pub fn op_negate(batch: &Batch) -> Batch {
    if batch.count == 0 {
        return Batch::empty(batch.num_payload_cols());
    }

    // clone_batch copies all column regions and the blob verbatim — no
    // per-string relocation is needed since we keep the same blob content.
    let mut output = batch.clone_batch();
    // Negate weights in-place.
    let weights = output.weight_data_mut();
    for i in 0..batch.count {
        let off = i * 8;
        let w = i64::from_le_bytes(weights[off..off + 8].try_into().unwrap());
        weights[off..off + 8].copy_from_slice(&(-w).to_le_bytes());
    }

    // clone_batch preserves sorted and consolidated; negating weights does not change element
    // identity, so both invariants survive as-is.
    gnitz_debug!("op_negate: count={}", batch.count);
    output
}

/// Union: algebraic addition of two Z-Set streams.
/// When both inputs are sorted, performs O(N) merge preserving sort order.
pub fn op_union(
    batch_a: &Batch,
    batch_b: Option<&Batch>,
    schema: &SchemaDescriptor,
) -> Batch {
    let b = match batch_b {
        Some(b) if b.count > 0 => b,
        _ => {
            // Identity copy of batch_a; clone_batch preserves sorted and consolidated.
            let output = batch_a.clone_batch();
            gnitz_debug!("op_union: a={} b=0 identity", batch_a.count);
            return output;
        }
    };

    if batch_a.sorted && b.sorted {
        return op_union_merge(batch_a, b, schema);
    }

    // Unsorted: concatenate
    let mut output = Batch::with_schema(*schema, batch_a.count + b.count);
    output.append_batch(batch_a, 0, batch_a.count);
    output.append_batch(b, 0, b.count);
    output.sorted = false;
    output.consolidated = false;
    gnitz_debug!("op_union: a={} b={} out={} concat", batch_a.count, b.count, output.count);
    output
}

/// Sorted merge of two sorted batches with contiguous-run batching.
fn op_union_merge(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> Batch {
    let n_a = batch_a.count;
    let n_b = batch_b.count;
    let mut output = Batch::with_schema(*schema, n_a + n_b);

    let mb_a = batch_a.as_mem_batch();
    let mb_b = batch_b.as_mem_batch();

    let mut i = 0usize;
    let mut j = 0usize;
    // run_src: 0=batch_a, 1=batch_b, -1=no current run
    let mut run_src: i32 = -1;
    let mut run_a_start = 0usize;
    let mut run_b_start = 0usize;

    while i < n_a && j < n_b {
        let a_pk = batch_a.get_pk(i);
        let b_pk = batch_b.get_pk(j);

        if a_pk < b_pk {
            if run_src != 0 {
                if run_src == 1 {
                    output.append_batch(batch_b, run_b_start, j);
                }
                run_src = 0;
                run_a_start = i;
            }
            i += 1;
        } else if b_pk < a_pk {
            if run_src != 1 {
                if run_src == 0 {
                    output.append_batch(batch_a, run_a_start, i);
                }
                run_src = 1;
                run_b_start = j;
            }
            j += 1;
        } else {
            // Equal PKs: flush pending run, then merge-sort the equal-PK
            // sub-ranges from both batches by payload to preserve (PK, payload)
            // sort order.
            if run_src == 0 {
                output.append_batch(batch_a, run_a_start, i);
            } else if run_src == 1 {
                output.append_batch(batch_b, run_b_start, j);
            }
            run_src = -1;

            // Find the end of the equal-PK run in each batch.
            let mut i_end = i + 1;
            while i_end < n_a
                && batch_a.get_pk(i_end) == a_pk
            {
                i_end += 1;
            }
            let mut j_end = j + 1;
            while j_end < n_b
                && batch_b.get_pk(j_end) == b_pk
            {
                j_end += 1;
            }

            // Merge the two sub-ranges by payload order.
            let (mut ia, mut jb) = (i, j);
            while ia < i_end && jb < j_end {
                if crate::storage::compare_rows(schema, &mb_a, ia, &mb_b, jb)
                    != std::cmp::Ordering::Greater
                {
                    output.append_batch(batch_a, ia, ia + 1);
                    ia += 1;
                } else {
                    output.append_batch(batch_b, jb, jb + 1);
                    jb += 1;
                }
            }
            if ia < i_end {
                output.append_batch(batch_a, ia, i_end);
            }
            if jb < j_end {
                output.append_batch(batch_b, jb, j_end);
            }

            i = i_end;
            j = j_end;
        }
    }

    // Flush final run
    if run_src == 0 {
        output.append_batch(batch_a, run_a_start, i);
    } else if run_src == 1 {
        output.append_batch(batch_b, run_b_start, j);
    }

    // Remaining
    if i < n_a {
        output.append_batch(batch_a, i, n_a);
    }
    if j < n_b {
        output.append_batch(batch_b, j, n_b);
    }

    output.sorted = true;
    output.consolidated = false;
    gnitz_debug!("op_union: a={} b={} out={} sorted_merge", n_a, n_b, output.count);
    output
}

/// Null-extend: copy input batch and append N null-filled payload columns.
/// Used in LEFT JOIN decomposition to convert anti-join output (left-only rows)
/// into null-filled outer join rows.
pub fn op_null_extend(
    batch: &Batch,
    in_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let in_npc = in_schema.num_columns as usize - 1;
    let right_npc = right_schema.num_columns as usize - 1;
    let out_npc = in_npc + right_npc;
    let n = batch.count;

    if n == 0 {
        return Batch::empty(out_npc);
    }

    // Build a merged schema for the output batch.
    let mut out_columns = [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64];
    let mut ci_out = 0;
    for ci in 0..in_schema.num_columns as usize {
        out_columns[ci_out] = in_schema.columns[ci];
        ci_out += 1;
    }
    let right_pk_index = right_schema.pk_index as usize;
    for ci in 0..right_schema.num_columns as usize {
        if ci == right_pk_index { continue; }
        out_columns[ci_out] = right_schema.columns[ci];
        ci_out += 1;
    }
    let out_schema = SchemaDescriptor {
        num_columns: ci_out as u32,
        pk_index: in_schema.pk_index,
        columns: out_columns,
    };

    let mut output = Batch::with_schema(out_schema, n);
    output.count = n;

    // Copy system columns
    output.pk_lo_data_mut().copy_from_slice(batch.pk_lo_data());
    output.pk_hi_data_mut().copy_from_slice(batch.pk_hi_data());
    output.weight_data_mut().copy_from_slice(batch.weight_data());

    // Copy input payload columns
    for pi in 0..in_npc {
        let in_ci = if pi < in_schema.pk_index as usize { pi } else { pi + 1 };
        let stride = in_schema.columns[in_ci].size as usize;
        output.col_data_mut(pi).copy_from_slice(&batch.col_data(pi)[..n * stride]);
    }

    // Right-side payload columns are already zero-filled by with_schema.

    // Set null bits for all appended right-side columns
    let right_null_bits = if right_npc < 64 {
        (1u64 << right_npc) - 1
    } else {
        u64::MAX
    };
    let shift = in_npc;
    for row in 0..n {
        let off = row * 8;
        let in_null = u64::from_le_bytes(batch.null_bmp_data()[off..off + 8].try_into().unwrap());
        let out_null = in_null | (right_null_bits << shift);
        output.null_bmp_data_mut()[off..off + 8].copy_from_slice(&out_null.to_le_bytes());
    }

    output.sorted = batch.sorted;
    output.consolidated = batch.consolidated;
    gnitz_debug!("op_null_extend: in={} right_npc={}", n, right_npc);
    output
}

// ---------------------------------------------------------------------------
// PK promotion for reindex (GROUP BY)
// ---------------------------------------------------------------------------

/// Murmur3 64-bit finalizer.
#[inline]
fn mix64(mut v: u64) -> u64 {
    v ^= v >> 33;
    v = v.wrapping_mul(0xFF51AFD7ED558CCD);
    v ^= v >> 33;
    v = v.wrapping_mul(0xC4CEB9FE1A85EC53);
    v ^= v >> 33;
    v
}

/// Promote a column value to a 128-bit PK for reindexing.
pub(super) fn promote_col_to_pk(
    batch: &MemBatch,
    row: usize,
    col_idx: usize,
    schema: &SchemaDescriptor,
) -> u128 {
    let tc = schema.columns[col_idx].type_code;
    let pki = schema.pk_index as usize;

    // If reindexing by the PK column itself, read from the batch's PK.
    if col_idx == pki {
        return batch.get_pk(row);
    }

    match tc {
        type_code::U128 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 16);
            u128::from_le_bytes(ptr[0..16].try_into().unwrap())
        }
        type_code::U64 | type_code::I64 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 8);
            let val = u64::from_le_bytes(ptr.try_into().unwrap());
            val as u128
        }
        type_code::STRING => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let struct_bytes = batch.get_col_ptr(row, pi, 16);
            let length = crate::util::read_u32_le(struct_bytes, 0) as usize;
            if length == 0 {
                return 0;
            }
            // Hash the string bytes
            let h = if length <= SHORT_STRING_THRESHOLD {
                // Inline: prefix bytes at struct[4..4+length]
                xxh::checksum(&struct_bytes[4..4 + length])
            } else {
                let heap_offset =
                    u64::from_le_bytes(struct_bytes[8..16].try_into().unwrap()) as usize;
                xxh::checksum(&batch.blob[heap_offset..heap_offset + length])
            };
            let h_hi = mix64(h);
            ((h_hi as u128) << 64) | (h as u128)
        }
        type_code::F64 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 8);
            let bits = u64::from_le_bytes(ptr.try_into().unwrap());
            bits as u128
        }
        type_code::F32 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 4);
            let bits = u32::from_le_bytes(ptr.try_into().unwrap()) as u64;
            bits as u128
        }
        _ => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 8);
            let val = u64::from_le_bytes(ptr.try_into().unwrap());
            val as u128
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use crate::storage::Batch;

    fn make_schema_u64_i64() -> SchemaDescriptor {
        let mut columns = [SchemaColumn {
            type_code: 0, size: 0, nullable: 0, _pad: 0,
        }; 64];
        columns[0] = SchemaColumn {
            type_code: type_code::U64, size: 8, nullable: 0, _pad: 0,
        };
        columns[1] = SchemaColumn {
            type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
        };
        SchemaDescriptor { num_columns: 2, pk_index: 0, columns }
    }

    fn make_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64)],
    ) -> Batch {
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
        b
    }

    fn get_payload_i64(b: &Batch, row: usize) -> i64 {
        crate::util::read_i64_le(b.col_data(0), row * 8)
    }

    // -----------------------------------------------------------------------
    // Union merge sort-invariant tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_union_merge_same_pk_payload_order() {
        // batch_a has val=20, batch_b has val=10 — output must be [10, 20]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 10)]);
        let out = op_union(&a, Some(&b), &schema);
        assert_eq!(out.count, 2);
        assert!(out.sorted);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
    }

    #[test]
    fn test_union_merge_same_pk_multiple_entries() {
        // a: [(1,1,20),(1,1,30)], b: [(1,1,10),(1,1,25)] → payloads [10,20,25,30]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20), (1, 1, 30)]);
        let b = make_batch(&schema, &[(1, 1, 10), (1, 1, 25)]);
        let out = op_union(&a, Some(&b), &schema);
        assert_eq!(out.count, 4);
        assert!(out.sorted);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert_eq!(get_payload_i64(&out, 2), 25);
        assert_eq!(get_payload_i64(&out, 3), 30);
    }

    #[test]
    fn test_union_merge_mixed_same_diff_pk() {
        // a: [(1,1,20),(3,1,300)], b: [(1,1,10),(2,1,200)]
        // output PKs [1,1,2,3], vals [10,20,200,300]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20), (3, 1, 300)]);
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 200)]);
        let out = op_union(&a, Some(&b), &schema);
        assert_eq!(out.count, 4);
        assert!(out.sorted);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!((out.get_pk(1) as u64), 1);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert_eq!((out.get_pk(2) as u64), 2);
        assert_eq!(get_payload_i64(&out, 2), 200);
        assert_eq!((out.get_pk(3) as u64), 3);
        assert_eq!(get_payload_i64(&out, 3), 300);
    }

    #[test]
    fn test_union_merge_same_pk_equal_payload() {
        // Same (PK, payload), opposite weights — must be adjacent for consolidation
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10)]);
        let b = make_batch(&schema, &[(1, -1, 10)]);
        let out = op_union(&a, Some(&b), &schema);
        assert_eq!(out.count, 2);
        assert!(out.sorted);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 10);
    }

    // -----------------------------------------------------------------------
    // op_filter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_filter_basic() {
        use crate::expr::{ExprProgram, Plan};

        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 5), (2, 1, 15), (3, 1, 25)]);

        let code = vec![
            1i64, 0, 1, 0,  // LOAD_COL_INT r0 = col[1]
            3, 1, 10, 0,    // LOAD_CONST r1 = 10
            17, 2, 0, 1,    // CMP_GT r2 = (r0 > r1)
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let func = ScalarFuncKind::Plan(Plan::from_predicate(prog, schema.pk_index));

        let out = op_filter(&batch, &func, &schema);
        assert_eq!(out.count, 2, "only pk=2 and pk=3 pass val>10");
        assert_eq!((out.get_pk(0) as u64), 2);
        assert_eq!((out.get_pk(1) as u64), 3);
    }

    #[test]
    fn test_op_filter_consolidated_flag() {
        use crate::expr::{ExprProgram, Plan};

        let code = vec![
            3i64, 0, 1, 0,  // LOAD_CONST r0 = 1 (always true)
        ];
        let prog = ExprProgram::new(code, 1, 0, vec![]);
        let schema = make_schema_u64_i64();
        let func = ScalarFuncKind::Plan(Plan::from_predicate(prog, schema.pk_index));

        let mut batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        batch.consolidated = true;

        let out = op_filter(&batch, &func, &schema);
        assert_eq!(out.count, 2);
        assert!(out.consolidated, "consolidated input + pass-all → consolidated output");
        assert!(out.sorted);
    }

    // -----------------------------------------------------------------------
    // op_negate tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_negate_weights() {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 3, 10), (2, -1, 20)]);
        let out = op_negate(&batch);
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), -3);
        assert_eq!(out.get_weight(1), 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert!(out.consolidated);
    }

    // -----------------------------------------------------------------------
    // op_map tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_map_empty_batch() {
        use crate::expr::Plan;
        let schema = make_schema_u64_i64();
        let empty_batch = Batch::empty(1);

        let func = ScalarFuncKind::Plan(Plan::from_projection(
            &[1], &[type_code::I64], schema.pk_index,
        ));
        let out = op_map(&empty_batch, &func, &schema, &schema, -1);
        assert_eq!(out.count, 0);
    }
}
