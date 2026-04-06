//! Linear operators: op_filter, op_map, op_negate, op_union.
//! Also includes PK promotion for reindex (GROUP BY).

use crate::compact::{SchemaDescriptor, SHORT_STRING_THRESHOLD, type_code};
use crate::memtable::OwnedBatch;
use crate::merge::MemBatch;
use crate::scalar_func::ScalarFuncKind;
use crate::xxh;

use super::util::pk_lt;

// ---------------------------------------------------------------------------
// Linear operators
// ---------------------------------------------------------------------------

/// Filter: retain rows where predicate returns true.
/// Uses contiguous-range bulk copy for efficiency.
pub fn op_filter(
    batch: &OwnedBatch,
    func: &ScalarFuncKind,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let n = batch.count;
    if n == 0 {
        let npc = schema.num_columns as usize - 1;
        return OwnedBatch::empty(npc);
    }

    let mb = batch.as_mem_batch();
    let mut output = OwnedBatch::with_schema(*schema, n);
    output.count = 0;

    let mut range_start: isize = -1;
    for i in 0..n {
        if func.evaluate_predicate(&mb, i, schema) {
            if range_start < 0 {
                range_start = i as isize;
            }
        } else {
            if range_start >= 0 {
                output.append_batch(batch, range_start as usize, i);
                range_start = -1;
            }
        }
    }
    if range_start >= 0 {
        output.append_batch(batch, range_start as usize, n);
    }

    if batch.consolidated {
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
    batch: &OwnedBatch,
    func: &ScalarFuncKind,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    reindex_col: i32,
) -> OwnedBatch {
    if batch.count == 0 {
        let out_npc = out_schema.num_columns as usize - 1;
        return OwnedBatch::empty(out_npc);
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
    let mut output = OwnedBatch::with_schema(*out_schema, batch.count);
    output.count = 0;

    // First evaluate the map batch (without reindex) to get column data
    let mapped = func.evaluate_map_batch(batch, in_schema, out_schema);

    // Then overwrite PK with promoted values from reindex column
    output.pk_hi = vec![0u8; mapped.count * 8]; // will be overwritten
    output.pk_lo = vec![0u8; mapped.count * 8];
    output.weight = mapped.weight;
    output.null_bmp = mapped.null_bmp;
    output.col_data = mapped.col_data;
    output.blob = mapped.blob;
    output.count = mapped.count;

    for row in 0..batch.count {
        let (pk_lo, pk_hi) = promote_col_to_pk(&in_mb, row, ri_col, in_schema);
        output.pk_lo[row * 8..row * 8 + 8].copy_from_slice(&pk_lo.to_le_bytes());
        output.pk_hi[row * 8..row * 8 + 8].copy_from_slice(&pk_hi.to_le_bytes());
    }

    output.sorted = false;
    output.consolidated = false;
    gnitz_debug!("op_map: in={} out={} reindex={} func={}", batch.count, output.count, reindex_col, func.kind_name());
    output
}

/// Negate: flip the sign of every weight.
pub fn op_negate(batch: &OwnedBatch) -> OwnedBatch {
    if batch.count == 0 {
        return OwnedBatch::empty(batch.col_data.len());
    }

    let mut output = OwnedBatch::empty(batch.col_data.len());
    output.schema = batch.schema;
    output.append_batch_negated(batch, 0, batch.count);

    if batch.consolidated {
        output.sorted = true;
        output.consolidated = true;
    } else {
        output.sorted = batch.sorted;
        output.consolidated = false;
    }
    gnitz_debug!("op_negate: count={}", batch.count);
    output
}

/// Union: algebraic addition of two Z-Set streams.
/// When both inputs are sorted, performs O(N) merge preserving sort order.
pub fn op_union(
    batch_a: &OwnedBatch,
    batch_b: Option<&OwnedBatch>,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let b = match batch_b {
        Some(b) if b.count > 0 => b,
        _ => {
            // Identity copy of batch_a
            let mut output = batch_a.clone_batch();
            if batch_a.consolidated {
                output.sorted = true;
                output.consolidated = true;
            } else {
                output.sorted = batch_a.sorted;
            }
            gnitz_debug!("op_union: a={} b=0 identity", batch_a.count);
            return output;
        }
    };

    if batch_a.sorted && b.sorted {
        return op_union_merge(batch_a, b, schema);
    }

    // Unsorted: concatenate
    let mut output = OwnedBatch::with_schema(*schema, batch_a.count + b.count);
    output.count = 0;
    output.append_batch(batch_a, 0, batch_a.count);
    output.append_batch(b, 0, b.count);
    output.sorted = false;
    output.consolidated = false;
    gnitz_debug!("op_union: a={} b={} out={} concat", batch_a.count, b.count, output.count);
    output
}

/// Sorted merge of two sorted batches with contiguous-run batching.
fn op_union_merge(
    batch_a: &OwnedBatch,
    batch_b: &OwnedBatch,
    schema: &SchemaDescriptor,
) -> OwnedBatch {
    let n_a = batch_a.count;
    let n_b = batch_b.count;
    let mut output = OwnedBatch::with_schema(*schema, n_a + n_b);
    output.count = 0;

    let mb_a = batch_a.as_mem_batch();
    let mb_b = batch_b.as_mem_batch();

    let mut i = 0usize;
    let mut j = 0usize;
    // run_src: 0=batch_a, 1=batch_b, -1=no current run
    let mut run_src: i32 = -1;
    let mut run_a_start = 0usize;
    let mut run_b_start = 0usize;

    while i < n_a && j < n_b {
        let a_lo = batch_a.get_pk_lo(i);
        let a_hi = batch_a.get_pk_hi(i);
        let b_lo = batch_b.get_pk_lo(j);
        let b_hi = batch_b.get_pk_hi(j);

        if pk_lt(a_lo, a_hi, b_lo, b_hi) {
            if run_src != 0 {
                if run_src == 1 {
                    output.append_batch(batch_b, run_b_start, j);
                }
                run_src = 0;
                run_a_start = i;
            }
            i += 1;
        } else if pk_lt(b_lo, b_hi, a_lo, a_hi) {
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
                && batch_a.get_pk_lo(i_end) == a_lo
                && batch_a.get_pk_hi(i_end) == a_hi
            {
                i_end += 1;
            }
            let mut j_end = j + 1;
            while j_end < n_b
                && batch_b.get_pk_lo(j_end) == b_lo
                && batch_b.get_pk_hi(j_end) == b_hi
            {
                j_end += 1;
            }

            // Merge the two sub-ranges by payload order.
            let (mut ia, mut jb) = (i, j);
            while ia < i_end && jb < j_end {
                if crate::columnar::compare_rows(schema, &mb_a, ia, &mb_b, jb)
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

/// Promote a column value to a (lo, hi) PK pair for reindexing.
pub(super) fn promote_col_to_pk(
    batch: &MemBatch,
    row: usize,
    col_idx: usize,
    schema: &SchemaDescriptor,
) -> (u64, u64) {
    let tc = schema.columns[col_idx].type_code;
    let pki = schema.pk_index as usize;

    // If reindexing by the PK column itself, read from pk_lo/pk_hi directly.
    if col_idx == pki {
        let lo = crate::util::read_u64_le(batch.pk_lo, row * 8);
        let hi = crate::util::read_u64_le(batch.pk_hi, row * 8);
        return (lo, hi);
    }

    match tc {
        type_code::U128 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 16);
            let lo = u64::from_le_bytes(ptr[0..8].try_into().unwrap());
            let hi = u64::from_le_bytes(ptr[8..16].try_into().unwrap());
            (lo, hi)
        }
        type_code::U64 | type_code::I64 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 8);
            let val = u64::from_le_bytes(ptr.try_into().unwrap());
            (val, 0)
        }
        type_code::STRING => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let struct_bytes = batch.get_col_ptr(row, pi, 16);
            let length = crate::util::read_u32_le(struct_bytes, 0) as usize;
            if length == 0 {
                return (0, 0);
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
            (h, h_hi)
        }
        type_code::F64 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 8);
            let bits = u64::from_le_bytes(ptr.try_into().unwrap());
            (bits, 0)
        }
        type_code::F32 => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 4);
            let bits = u32::from_le_bytes(ptr.try_into().unwrap()) as u64;
            (bits, 0)
        }
        _ => {
            let pi = if col_idx < pki { col_idx } else { col_idx - 1 };
            let ptr = batch.get_col_ptr(row, pi, 8);
            let val = u64::from_le_bytes(ptr.try_into().unwrap());
            (val, 0)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::{SchemaColumn, SchemaDescriptor, type_code};
    use crate::memtable::OwnedBatch;

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
    ) -> OwnedBatch {
        let n = rows.len();
        let mut b = OwnedBatch::with_schema(*schema, n.max(1));
        b.count = 0;
        for &(pk, w, val) in rows {
            b.pk_lo.extend_from_slice(&pk.to_le_bytes());
            b.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            b.weight.extend_from_slice(&w.to_le_bytes());
            b.null_bmp.extend_from_slice(&0u64.to_le_bytes());
            b.col_data[0].extend_from_slice(&val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    fn get_payload_i64(b: &OwnedBatch, row: usize) -> i64 {
        crate::util::read_i64_le(&b.col_data[0], row * 8)
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
        assert_eq!(out.get_pk_lo(0), 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(out.get_pk_lo(1), 1);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert_eq!(out.get_pk_lo(2), 2);
        assert_eq!(get_payload_i64(&out, 2), 200);
        assert_eq!(out.get_pk_lo(3), 3);
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
        use crate::scalar_func::{Plan, ScalarFuncKind};
        use crate::expr::ExprProgram;

        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 5), (2, 1, 15), (3, 1, 25)]);

        let code = vec![
            1i64, 0, 1, 0,  // LOAD_COL_INT r0 = col[1]
            3, 1, 10, 0,    // LOAD_CONST r1 = 10
            17, 2, 0, 1,    // CMP_GT r2 = (r0 > r1)
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let func = ScalarFuncKind::Plan(Plan::from_predicate(prog));

        let out = op_filter(&batch, &func, &schema);
        assert_eq!(out.count, 2, "only pk=2 and pk=3 pass val>10");
        assert_eq!(out.get_pk_lo(0), 2);
        assert_eq!(out.get_pk_lo(1), 3);
    }

    #[test]
    fn test_op_filter_consolidated_flag() {
        use crate::scalar_func::{Plan, ScalarFuncKind};
        use crate::expr::ExprProgram;

        let code = vec![
            3i64, 0, 1, 0,  // LOAD_CONST r0 = 1 (always true)
        ];
        let prog = ExprProgram::new(code, 1, 0, vec![]);
        let func = ScalarFuncKind::Plan(Plan::from_predicate(prog));

        let schema = make_schema_u64_i64();
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
        use crate::scalar_func::{Plan, ScalarFuncKind};

        let schema = make_schema_u64_i64();
        let empty_batch = OwnedBatch::empty(1);

        let func = ScalarFuncKind::Plan(Plan::from_projection(
            &[1], &[type_code::I64], schema.pk_index,
        ));
        let out = op_map(&empty_batch, &func, &schema, &schema, -1);
        assert_eq!(out.count, 0);
    }
}
