//! Linear operators: op_filter, op_map, op_negate, op_union, op_null_extend.
//!
//! PK-promotion for reindex (GROUP BY / equijoin / set-op keys) lives in the
//! sibling `reindex` module; `op_map` calls into it for the reindex paths.

use std::cmp::Ordering;

use crate::expr::ScalarFunc;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::storage::{with_payload_cmp, Batch, Layout, MemBatch};

use super::cogroup::{cogroup_union, BatchCursor};
use super::reindex::{reindex_hash_row, ReindexPacker};

// ---------------------------------------------------------------------------
// Linear operators
// ---------------------------------------------------------------------------

/// Filter: retain rows where predicate returns true.
/// Uses contiguous-range bulk copy for efficiency.
pub fn op_filter(batch: &Batch, func: &ScalarFunc, schema: &SchemaDescriptor) -> Batch {
    let n = batch.count;
    if n == 0 {
        return Batch::empty_with_schema(schema);
    }

    let mb = batch.as_mem_batch();
    let mut output = Batch::with_schema(*schema, n);

    // When the schema has a STRING/BLOB column, copy 16-byte string structs
    // verbatim (no per-row relocation); offsets inside the structs stay valid
    // because both blobs are identical. No `!batch.blob.is_empty()` guard: a
    // batch whose strings are all short (≤12 bytes, stored inline) has an empty
    // blob, but the bulk copy is still correct — short strings are self-contained
    // in their struct and an empty shared blob is a no-op for the absent long
    // strings. Gating on a non-empty blob needlessly dropped all-short-string
    // batches to the slow `relocate_string_cell` path.
    let blob_passthrough = schema.has_german_string();
    if blob_passthrough {
        output.share_blob_from(batch);
    }

    func.run_filter(&mb, n, |start, end| {
        if blob_passthrough {
            output.append_batch_no_blob_reloc(batch, start, end);
        } else {
            output.append_batch(batch, start, end);
        }
    });

    // The appends above downgraded `output` to `Raw`. A filtered subset preserves
    // the input's order, weights, and (PK, payload) distinctness, so it carries
    // exactly the input's layout — a faithful propagate (no re-verify).
    output.inherit_layout(batch);

    gnitz_debug!("op_filter: in={} out={}", n, output.count);
    output
}

/// Map: transform batch via scalar function, then stamp the output PK region.
///
/// `reindex_hash` and a non-empty `reindex_cols` are mutually exclusive — each
/// selects what overwrites `evaluate_map_batch`'s bulk PK copy:
/// - `reindex_hash`: set each PK to a hash of the full output row (all payload
///   columns) for EXCEPT/INTERSECT/DISTINCT full-row set identity.
/// - non-empty `reindex_cols`: pack the listed source columns' OPK bytes
///   contiguously into the output PK (the `_join_pk` for an equijoin /
///   GROUP BY repartition).
/// - neither: plain batch map — the bulk PK copy stands.
#[allow(clippy::too_many_arguments)]
pub fn op_map(
    batch: &Batch,
    func: &ScalarFunc,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    reindex_cols: &[u32],
    target_tcs: &[u8],
    reindex_hash: bool,
    branch_id: u8,
) -> Batch {
    debug_assert!(
        !reindex_hash || reindex_cols.is_empty(),
        "op_map: reindex_hash and reindex_cols are mutually exclusive",
    );
    if batch.count == 0 {
        return Batch::empty_with_schema(out_schema);
    }

    let mut output = func.evaluate_map_batch(batch, out_schema);
    debug_assert_eq!(
        output.count, batch.count,
        "MAP output row count must equal input row count",
    );
    if reindex_hash {
        // Set each PK to a hash of the full output row so rows with identical
        // content collide and distinct content does not (EXCEPT/INTERSECT/
        // DISTINCT full-row set identity).
        reindex_hash_row(out_schema, &mut output, branch_id);
    } else if !reindex_cols.is_empty() {
        // Pack each reindex column's OPK bytes contiguously into the output PK.
        // The SAME packer routes the exchange scatter (via `ScatterKey`), so the
        // reindexed `_join_pk` and the delta scatter co-partition byte-for-byte.
        ReindexPacker::new(in_schema, reindex_cols, target_tcs).promote_into(&batch.as_mem_batch(), &mut output);
    }

    // `evaluate_map_batch` returns `Raw`, and every path keeps it there: a
    // reindex/hash stamp only rewrites PK bytes (no layout raise), and a plain
    // projection stays `Raw` because a payload-reordering projection over a
    // duplicate-PK input can break (PK, payload) order (the D1 fail-safe — only
    // `into_consolidated` would trust `sorted`, and it re-sorts a `Raw` batch).
    gnitz_debug!(
        "op_map: in={} out={} reindex_hash={} reindex_cols={}",
        batch.count,
        output.count,
        reindex_hash,
        reindex_cols.len(),
    );
    output
}

/// Negate: flip the sign of every weight.
pub fn op_negate(batch: &Batch) -> Batch {
    if batch.count == 0 {
        return batch.empty_like();
    }

    // clone_batch copies all column regions and the blob verbatim — no
    // per-string relocation is needed since we keep the same blob content.
    let mut output = batch.clone_batch();
    output.map_weights(i64::wrapping_neg);

    // clone_batch preserves sorted and consolidated; negating weights does not change element
    // identity, so both invariants survive as-is.
    gnitz_debug!("op_negate: count={}", batch.count);
    output
}

/// Union: algebraic addition of two Z-Set streams.
/// When both inputs are sorted, performs O(N) merge preserving sort order.
pub fn op_union(batch_a: Batch, batch_b: Option<&Batch>, schema: &SchemaDescriptor) -> Batch {
    let b = match batch_b {
        Some(b) if b.count > 0 => b,
        // O(1) pass-through: no allocation, sorted/consolidated preserved.
        _ => {
            gnitz_debug!("op_union: a={} b=0 identity", batch_a.count);
            return batch_a;
        }
    };
    if batch_a.count == 0 {
        return b.clone_batch();
    }

    if batch_a.sorted_verified(schema) && b.sorted_verified(schema) {
        return op_union_merge(&batch_a, b, schema);
    }

    // Unsorted: concatenate (the appends leave `output` `Raw`).
    let mut output = Batch::with_schema(*schema, batch_a.count + b.count);
    output.append_batch(&batch_a, 0, batch_a.count);
    output.append_batch(b, 0, b.count);
    gnitz_debug!(
        "op_union: a={} b={} out={} concat",
        batch_a.count,
        b.count,
        output.count
    );
    output
}

/// Sorted merge of two sorted batches with contiguous-run batching.
fn op_union_merge(batch_a: &Batch, batch_b: &Batch, schema: &SchemaDescriptor) -> Batch {
    with_payload_cmp!(schema, op_union_merge_inner, batch_a, batch_b, schema)
}

#[inline]
fn op_union_merge_inner<RowCmp>(batch_a: &Batch, batch_b: &Batch, schema: &SchemaDescriptor, row_cmp: RowCmp) -> Batch
where
    RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy,
{
    let n_a = batch_a.count;
    let n_b = batch_b.count;
    let mut output = Batch::with_schema(*schema, n_a + n_b);

    let mb_a = batch_a.as_mem_batch();
    let mb_b = batch_b.as_mem_batch();

    let mut a = BatchCursor::new(batch_a);
    let mut b = BatchCursor::new(batch_b);

    // Symmetric full-outer co-group (Z-Set `+`: every row from both sides). The
    // skeleton bulk-appends each single-source run itself; only the shared-PK
    // groups need the payload merge below, which coalesces contiguous
    // single-source runs into one append each (the row count per group is often
    // single-digit and `append_batch` has fixed per-call offset overhead).
    cogroup_union(&mut a, &mut b, &mut output, |out, ra, rb| {
        let (i, i_end) = (ra.start, ra.end);
        let (j, j_end) = (rb.start, rb.end);
        let (mut ia, mut jb) = (i, j);
        if ia < i_end && jb < j_end {
            let mut prev_a = row_cmp(schema, &mb_a, ia, &mb_b, jb) != Ordering::Greater;
            let mut run_start = if prev_a { ia } else { jb };
            if prev_a {
                ia += 1;
            } else {
                jb += 1;
            }
            while ia < i_end && jb < j_end {
                let pick_a = row_cmp(schema, &mb_a, ia, &mb_b, jb) != Ordering::Greater;
                if pick_a != prev_a {
                    if prev_a {
                        out.append_batch(batch_a, run_start, ia);
                        run_start = jb;
                    } else {
                        out.append_batch(batch_b, run_start, jb);
                        run_start = ia;
                    }
                    prev_a = pick_a;
                }
                if pick_a {
                    ia += 1;
                } else {
                    jb += 1;
                }
            }
            // Flush the in-progress run, folding its side's still-unpicked
            // tail (rows the loop never reached because the *other* side
            // exhausted first) into the same append. The opposite side then
            // drains as a separate run.
            if prev_a {
                out.append_batch(batch_a, run_start, i_end);
                if jb < j_end {
                    out.append_batch(batch_b, jb, j_end);
                }
            } else {
                out.append_batch(batch_b, run_start, j_end);
                if ia < i_end {
                    out.append_batch(batch_a, ia, i_end);
                }
            }
        } else {
            if ia < i_end {
                out.append_batch(batch_a, ia, i_end);
            }
            if jb < j_end {
                out.append_batch(batch_b, jb, j_end);
            }
        }
    });

    // Payload-aware merge of two sorted inputs ⇒ genuinely (PK, payload)-sorted,
    // but unfolded (Z-Set `+` does not sum weights). Certify `Sorted`.
    output.certify_layout(Layout::Sorted, schema);
    gnitz_debug!("op_union: a={} b={} out={} sorted_merge", n_a, n_b, output.count);
    output
}

/// Null-extend: copy input batch and append N null-filled payload columns.
/// Used by the LEFT JOIN null-fill to extend the unmatched preserved rows
/// (ν = positive_part(A − π_A(inner))) with NULL right columns.
pub fn op_null_extend(batch: &Batch, in_schema: &SchemaDescriptor, right_schema: &SchemaDescriptor) -> Batch {
    assert!(
        in_schema.num_columns() + right_schema.num_payload_cols() <= crate::schema::MAX_COLUMNS,
        "op_null_extend: combined column count {} + {} exceeds MAX_COLUMNS={}",
        in_schema.num_columns(),
        right_schema.num_payload_cols(),
        crate::schema::MAX_COLUMNS,
    );

    let in_npc = in_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let n = batch.count;

    // Build the merged schema for the output batch (also the shape of the
    // zero-row early return — output PK region is identical to the input PK).
    let mut out_columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let mut ci_out = 0;
    for ci in 0..in_schema.num_columns() {
        out_columns[ci_out] = in_schema.columns[ci];
        ci_out += 1;
    }
    for (_rpi, _ci, col) in right_schema.payload_columns() {
        out_columns[ci_out] = *col;
        ci_out += 1;
    }
    let out_schema = SchemaDescriptor::new(&out_columns[..ci_out], in_schema.pk_indices());

    if n == 0 {
        return Batch::empty_with_schema(&out_schema);
    }

    let mut output = Batch::with_schema(out_schema, n);
    output.count = n;

    // Propagate the input blob so long (> 12 byte) STRING/BLOB values whose
    // 16-byte structs are copied verbatim below still resolve against the
    // shared heap. Without this they would point into an empty blob. No
    // `!batch.blob.is_empty()` guard, same as `op_filter`: sharing an empty
    // blob is a no-op and all-short-string batches are still correct.
    if in_schema.has_german_string() {
        output.share_blob_from(batch);
    }

    // Copy system columns
    output.pk_data_mut().copy_from_slice(batch.pk_data());
    output.weight_data_mut().copy_from_slice(batch.weight_data());

    // Copy input payload columns
    for (pi, _ci, col) in in_schema.payload_columns() {
        let stride = col.size() as usize;
        output
            .col_data_mut(pi)
            .copy_from_slice(&batch.col_data(pi)[..n * stride]);
    }

    // Right-side payload columns are already zero-filled by with_schema.

    // Set null bits for all appended right-side columns
    let right_null_bits = super::util::all_payload_null_mask(right_npc);
    for row in 0..n {
        let off = row * 8;
        let in_null = u64::from_le_bytes(batch.null_bmp_data()[off..off + 8].try_into().unwrap());
        let out_null = super::util::merge_null_words(in_null, right_null_bits, in_npc);
        output.null_bmp_data_mut()[off..off + 8].copy_from_slice(&out_null.to_le_bytes());
    }

    // Null-extending appends columns without touching PK order or weights, so the
    // output carries exactly the input's layout (faithful propagate, no re-verify).
    output.inherit_layout(batch);
    gnitz_debug!("op_null_extend: in={} right_npc={}", n, right_npc);
    output
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::{Batch, Layout};
    use crate::test_support::{
        make_batch_i64pk, make_schema_i64pk_i64, make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string,
        make_wide_batch, wide_pk_3xu64_schema,
    };

    fn make_schema_u64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, i64)]) -> Batch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));
        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.certify_layout(Layout::Consolidated, schema);
        b
    }

    fn get_payload_i64(b: &Batch, row: usize) -> i64 {
        crate::foundation::codec::read_i64_le(b.col_data(0), row * 8)
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
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 2);
        assert!(out.is_sorted());
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
    }

    /// Skewed tiny ⋃ huge union: the gallop skip jumps the long b-only prefix in
    /// one `advance_to`, and the shared PK=5 carries interleaved payloads on BOTH
    /// sides ({100,300} ⋃ {200,400}). Output must be the full union multiset,
    /// (PK, payload)-sorted (Z-Set `+` keeps every row, weights unfolded).
    #[test]
    fn test_union_merge_skewed_interleaved_payloads() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(5, 1, 100), (5, 1, 300)]); // tiny side
        let b = make_batch(
            &schema,
            &[
                (1, 1, 10),
                (2, 1, 20),
                (3, 1, 30),
                (4, 1, 40),
                (5, 1, 200),
                (5, 1, 400),
                (6, 1, 60),
                (7, 1, 70),
            ],
        ); // huge side; PK=5 interleaves with a
        let out = op_union(a, Some(&b), &schema);

        let got: Vec<(u64, i64)> = (0..out.count)
            .map(|r| (out.get_pk(r) as u64, get_payload_i64(&out, r)))
            .collect();
        let want: Vec<(u64, i64)> = vec![
            (1, 10),
            (2, 20),
            (3, 30),
            (4, 40),
            (5, 100),
            (5, 200),
            (5, 300),
            (5, 400),
            (6, 60),
            (7, 70),
        ];
        assert_eq!(got, want, "full union, (PK, payload)-sorted");
        assert!(out.is_sorted());
        assert!(!out.is_consolidated(), "Z-Set + does not fold weights");
    }

    #[test]
    fn test_union_merge_same_pk_multiple_entries() {
        // a: [(1,1,20),(1,1,30)], b: [(1,1,10),(1,1,25)] → payloads [10,20,25,30]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20), (1, 1, 30)]);
        let b = make_batch(&schema, &[(1, 1, 10), (1, 1, 25)]);
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 4);
        assert!(out.is_sorted());
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
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 4);
        assert!(out.is_sorted());
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
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 2);
        assert!(out.is_sorted());
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 10);
    }

    /// Read a STRING/BLOB payload column's value back out as a `String`.
    /// Mirrors join.rs's `read_str_payload`; short (≤12 byte) values live inline
    /// in the 16-byte German-string struct, long ones in the blob heap.
    fn read_str_col(batch: &Batch, col: usize, row: usize) -> String {
        let off = row * 16;
        let gs = &batch.col_data(col)[off..off + 16];
        let length = u32::from_le_bytes(gs[0..4].try_into().unwrap()) as usize;
        if length == 0 {
            return String::new();
        }
        if length <= crate::schema::SHORT_STRING_THRESHOLD {
            String::from_utf8_lossy(&gs[4..4 + length]).to_string()
        } else {
            let blob_off = u64::from_le_bytes(gs[8..16].try_into().unwrap()) as usize;
            String::from_utf8_lossy(&batch.blob[blob_off..blob_off + length]).to_string()
        }
    }

    /// Build a `Batch` for a `(U64 pk, STRING payload)` schema from
    /// `(pk, weight, &str)` rows. Short (≤12 byte) strings are stored inline;
    /// the resulting batch is marked sorted+consolidated (caller supplies rows
    /// already in (PK, payload) order).
    fn make_str_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, &str)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, s) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            let gs = crate::test_support::german_string(s.as_bytes(), &mut b.blob);
            b.extend_col(0, &gs);
            b.count += 1;
        }
        b.certify_layout(Layout::Consolidated, schema);
        b
    }

    /// Pin: the GENERIC `RowCmp` arm (`compare_rows`, German-string comparison)
    /// must drive `op_union_merge_inner`'s shared-PK payload merge. A
    /// `(U64 pk, STRING payload)` schema resolves to `PayloadCmpKind::Generic`,
    /// so `with_payload_cmp!` threads the string-aware comparator into the merge.
    /// Two rows share PK=1 with payloads "banana" (in a) and "apple" (in b); the
    /// union must produce both rows (PK, payload)-sorted: "apple" before
    /// "banana". The fixed-int comparator would treat these German-string structs
    /// as raw 8-byte integers and order them by the wrong bytes (or merge them),
    /// so a fixed-int dispatch through this path flips the string order.
    #[test]
    fn test_union_merge_generic_rowcmp_shared_pk_string_payload() {
        let schema = make_schema_pk_u64_payload_string();
        // Guard: the schema must select the GENERIC comparator. If a future
        // change moved STRING into the fixed-int fast path, this pin would no
        // longer exercise the generic arm — fail loudly here instead.
        assert_eq!(
            schema.payload_cmp,
            crate::schema::PayloadCmpKind::Generic,
            "U64+STRING schema must use the GENERIC payload comparator",
        );

        let a = make_str_batch(&schema, &[(1, 1, "banana")]);
        let b = make_str_batch(&schema, &[(1, 1, "apple")]);
        let out = op_union(a, Some(&b), &schema);

        assert_eq!(out.count, 2, "Z-Set + keeps both shared-PK rows");
        assert!(out.is_sorted());
        // Both rows carry PK=1.
        assert_eq!(out.get_pk(0) as u64, 1);
        assert_eq!(out.get_pk(1) as u64, 1);
        // (PK, payload) order: German-string compare puts "apple" before "banana".
        assert_eq!(read_str_col(&out, 0, 0), "apple", "row0 payload (string-sorted)");
        assert_eq!(read_str_col(&out, 0, 1), "banana", "row1 payload (string-sorted)");
    }

    // -----------------------------------------------------------------------
    // op_filter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_filter_basic() {
        use crate::expr::{CmpOp, LogicalInstr, LogicalProgram};

        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 5), (2, 1, 15), (3, 1, 25)]);

        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: 1 }, // r0 = col[1]
            LogicalInstr::LoadConst { dst: 1, val: 10 }, // r1 = 10
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            }, // r2 = (r0 > r1)
        ];
        let func = ScalarFunc::from_predicate(LogicalProgram::new(instrs, 3, 2, vec![]), &schema);

        let out = op_filter(&batch, &func, &schema);
        assert_eq!(out.count, 2, "only pk=2 and pk=3 pass val>10");
        assert_eq!((out.get_pk(0) as u64), 2);
        assert_eq!((out.get_pk(1) as u64), 3);
    }

    #[test]
    fn test_op_filter_consolidated_flag() {
        use crate::expr::{LogicalInstr, LogicalProgram};

        let instrs = vec![
            LogicalInstr::LoadConst { dst: 0, val: 1 }, // always true
        ];
        let schema = make_schema_u64_i64();
        let func = ScalarFunc::from_predicate(LogicalProgram::new(instrs, 1, 0, vec![]), &schema);

        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);

        let out = op_filter(&batch, &func, &schema);
        assert_eq!(out.count, 2);
        assert!(
            out.is_consolidated(),
            "consolidated input + pass-all → consolidated output"
        );
        assert!(out.is_sorted());
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
        assert!(out.is_consolidated());
    }

    // -----------------------------------------------------------------------
    // op_map tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_map_empty_batch() {
        use crate::expr::LogicalProgram;
        let schema = make_schema_u64_i64();
        let empty_batch = Batch::empty_with_schema(&schema);

        let func = ScalarFunc::from_map(LogicalProgram::copy_cols(&[(1, type_code::I64)]), &schema, &schema);
        let out = op_map(&empty_batch, &func, &schema, &schema, &[], &[], false, 0);
        assert_eq!(out.count, 0);
    }

    // -----------------------------------------------------------------------
    // Wide-PK union merge (pk_stride > 16)
    // -----------------------------------------------------------------------

    fn wide_pk_triple(b: &Batch, row: usize) -> (u64, u64, u64) {
        let pk = b.get_pk_bytes(row);
        // OPK encodes each U64 column big-endian (the §6 at-rest layout), so
        // the columns must be read back big-endian.
        (
            u64::from_be_bytes(pk[0..8].try_into().unwrap()),
            u64::from_be_bytes(pk[8..16].try_into().unwrap()),
            u64::from_be_bytes(pk[16..24].try_into().unwrap()),
        )
    }

    #[test]
    fn test_op_union_merge_wide_pk() {
        // Regression: op_union on wide-PK (pk_stride=24) batches previously
        // panicked in get_pk -> widen_pk_le. Verify it merges correctly:
        // sorted by (PK, payload), all rows present, equal-PK groups
        // payload-sorted, weights not summed (union, not consolidation).
        let schema = wide_pk_3xu64_schema();
        let a = make_wide_batch(&schema, &[(0, 0, 1, 1, 20), (0, 0, 3, 1, 300)]);
        let b = make_wide_batch(&schema, &[(0, 0, 1, 1, 10), (0, 0, 2, 1, 200)]);

        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 4);
        assert!(out.is_sorted());
        assert!(!out.is_consolidated());

        assert_eq!(wide_pk_triple(&out, 0), (0, 0, 1));
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(wide_pk_triple(&out, 1), (0, 0, 1));
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert_eq!(wide_pk_triple(&out, 2), (0, 0, 2));
        assert_eq!(get_payload_i64(&out, 2), 200);
        assert_eq!(wide_pk_triple(&out, 3), (0, 0, 3));
        assert_eq!(get_payload_i64(&out, 3), 300);
    }

    #[test]
    fn test_op_union_empty_a_returns_b() {
        // batch_a empty, batch_b non-empty → result equals batch_b content.
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[]);
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 2);
        assert_eq!(out.get_pk(0) as u64, 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(out.get_pk(1) as u64, 2);
        assert_eq!(get_payload_i64(&out, 1), 20);
    }

    #[test]
    fn test_op_union_b_none_passthrough() {
        // batch_b None → batch_a returned verbatim (sorted/consolidated kept).
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10)]);
        let out = op_union(a, None, &schema);
        assert_eq!(out.count, 1);
        assert!(out.is_sorted() && out.is_consolidated());
        assert_eq!(get_payload_i64(&out, 0), 10);
    }

    // -----------------------------------------------------------------------
    // op_union_merge signed I64 PK ordering (item 33)
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_union_merge_signed_i64_pk() {
        // Signed PK has high bit set for negatives. Raw-u128 comparison (narrow
        // path) would sort negatives after positives. The merge must produce
        // signed ascending order.
        let schema = make_schema_i64pk_i64();
        let a = make_batch_i64pk(&schema, &[(-5, 1, 100), (0, 1, 200), (7, 1, 300)]);
        let b = make_batch_i64pk(&schema, &[(-1, 1, 400), (3, 1, 500)]);
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 5);
        assert!(out.is_sorted());
        let pks: Vec<i64> = (0..out.count)
            .map(|i| {
                let mut le = [0u8; 8];
                gnitz_wire::decode_pk_column(out.get_pk_bytes(i), type_code::I64, &mut le);
                i64::from_le_bytes(le)
            })
            .collect();
        assert_eq!(pks, vec![-5, -1, 0, 3, 7], "signed ascending PK order");
    }

    // -----------------------------------------------------------------------
    // op_null_extend blob propagation + combined-column guard
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_null_extend_blob_propagation() {
        // Regression: op_null_extend did not propagate the input blob, so a
        // long (> 12 byte) string in the output resolved against an empty
        // blob and returned garbage.
        let in_schema = make_schema_pk_u64_payload_string();
        let mut b = Batch::with_schema(in_schema, 1);
        let long_str: &[u8] = b"a-fairly-long-string-value"; // 26 bytes > 12
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let gs = crate::test_support::german_string(long_str, &mut b.blob);
        b.extend_col(0, &gs);
        b.count += 1;

        // Right side: a single I64 payload column.
        let right_schema = make_schema_u64_i64();
        let out = op_null_extend(&b, &in_schema, &right_schema);

        assert!(!out.blob.is_empty(), "output blob must be propagated");

        // Resolve the long string from the output's STRING column (col 0).
        let struct_bytes = out.col_data(0);
        let length = u32::from_le_bytes(struct_bytes[0..4].try_into().unwrap()) as usize;
        let off = u64::from_le_bytes(struct_bytes[8..16].try_into().unwrap()) as usize;
        let resolved = crate::schema::long_string_bytes(&out.blob, off, length);
        assert_eq!(resolved, long_str, "long string must resolve to the original");
    }

    #[test]
    #[should_panic(expected = "op_null_extend: combined column count")]
    fn test_op_null_extend_combined_column_guard() {
        // Regression: op_null_extend wrote in_schema.num_columns() +
        // right_schema.num_payload_cols() entries into a [_; MAX_COLUMNS]
        // stack array with no bounds check. Combined > MAX_COLUMNS must
        // panic with a clear message, not a generic index-out-of-bounds.
        let mut in_cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..40 {
            in_cols.push(SchemaColumn::new(type_code::I64, 0));
        }
        let in_schema = SchemaDescriptor::new(&in_cols, &[0]);

        let mut right_cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..40 {
            right_cols.push(SchemaColumn::new(type_code::I64, 0));
        }
        let right_schema = SchemaDescriptor::new(&right_cols, &[0]);

        // 41 + 40 = 81 > MAX_COLUMNS (65).
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        for pi in 0..in_schema.num_payload_cols() {
            b.extend_col(pi, &0i64.to_le_bytes());
        }
        b.count += 1;
        let _ = op_null_extend(&b, &in_schema, &right_schema);
    }

    #[test]
    fn test_op_filter_blob_passthrough_long_value() {
        // op_filter over a batch with a BLOB payload column holding a long
        // (> 12 byte) value. The output BLOB must resolve to the original.
        let schema = make_schema_pk_u64_payload_blob();
        let mut b = Batch::with_schema(schema, 2);
        let long_blob: &[u8] = b"a-fairly-long-blob-value-xyz"; // 28 bytes > 12
                                                                // Row 0: long blob, val passes always-true filter.
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let gs = crate::test_support::german_string(long_blob, &mut b.blob);
        b.extend_col(0, &gs);
        b.count += 1;
        b.certify_layout(Layout::Consolidated, &schema);

        let func = always_true_func(&schema);
        let out = op_filter(&b, &func, &schema);
        assert_eq!(out.count, 1);
        assert!(!out.blob.is_empty(), "BLOB filter output must carry the blob buffer");
        let struct_bytes = out.col_data(0);
        let length = u32::from_le_bytes(struct_bytes[0..4].try_into().unwrap()) as usize;
        let off = u64::from_le_bytes(struct_bytes[8..16].try_into().unwrap()) as usize;
        let resolved = crate::schema::long_string_bytes(&out.blob, off, length);
        assert_eq!(resolved, long_blob, "long BLOB must resolve to the original bytes");
    }

    // -----------------------------------------------------------------------
    // Stride consistency on early-exit empty batches (item 3)
    // -----------------------------------------------------------------------

    fn always_true_func(schema: &SchemaDescriptor) -> ScalarFunc {
        use crate::expr::{LogicalInstr, LogicalProgram};
        let instrs = vec![LogicalInstr::LoadConst { dst: 0, val: 1 }]; // always true
        ScalarFunc::from_predicate(LogicalProgram::new(instrs, 1, 0, vec![]), schema)
    }

    #[test]
    fn test_op_filter_empty_stride_u64() {
        // U64 PK → stride 8. Early-exit empty batch must carry the schema
        // stride, not a hardcoded 16.
        let schema = make_schema_u64_i64();
        let empty = make_batch(&schema, &[]);
        assert_eq!(empty.count, 0);
        let func = always_true_func(&schema);
        let out = op_filter(&empty, &func, &schema);
        assert_eq!(out.count, 0);
        assert_eq!(
            out.pk_stride(),
            8,
            "op_filter empty must use schema stride 8 for U64 PK"
        );
    }

    #[test]
    fn test_op_negate_empty_stride_u64() {
        let schema = make_schema_u64_i64();
        let empty = make_batch(&schema, &[]);
        let out = op_negate(&empty);
        assert_eq!(out.count, 0);
        assert_eq!(
            out.pk_stride(),
            8,
            "op_negate empty must use schema stride 8 for U64 PK"
        );
    }

    #[test]
    fn test_op_null_extend_empty_stride_u64() {
        let in_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        let empty = make_batch(&in_schema, &[]);
        let out = op_null_extend(&empty, &in_schema, &right_schema);
        assert_eq!(out.count, 0);
        assert_eq!(
            out.pk_stride(),
            8,
            "op_null_extend empty must use combined-schema stride 8"
        );
    }

    // -----------------------------------------------------------------------
    // op_null_extend shift guard (item 4a)
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_null_extend_shift_guard_64_payload_cols() {
        // in_npc == 64 (65-column schema, 1 PK col) and right_npc == 0.
        // shift == 64 makes `right_null_bits << 64` panic in debug builds even
        // though right_null_bits is 0.
        let mut in_cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..64 {
            in_cols.push(SchemaColumn::new(type_code::I64, 0));
        }
        let in_schema = SchemaDescriptor::new(&in_cols, &[0]);
        assert_eq!(in_schema.num_payload_cols(), 64);

        // Right schema: PK only, no payload columns → right_npc == 0.
        let right_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        assert_eq!(right_schema.num_payload_cols(), 0);

        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        for pi in 0..in_schema.num_payload_cols() {
            b.extend_col(pi, &0i64.to_le_bytes());
        }
        b.count += 1;

        // Must not panic.
        let out = op_null_extend(&b, &in_schema, &right_schema);
        assert_eq!(out.count, 1);
        let in_null = u64::from_le_bytes(b.null_bmp_data()[0..8].try_into().unwrap());
        let out_null = u64::from_le_bytes(out.null_bmp_data()[0..8].try_into().unwrap());
        assert_eq!(out_null, in_null, "no right cols → output null word equals input");
    }

    // -----------------------------------------------------------------------
    // op_negate i64::MIN (item 46)
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_negate_i64_min_no_panic() {
        // -i64::MIN overflows; debug builds panic, release wraps silently.
        // wrapping_neg must leave i64::MIN unchanged without panicking.
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, i64::MIN, 10), (2, 5, 20)]);
        let out = op_negate(&batch);
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), i64::MIN, "wrapping_neg(i64::MIN) == i64::MIN");
        assert_eq!(out.get_weight(1), -5);
    }

    #[test]
    fn test_op_map_with_reindex_promotes_payload_to_pk() {
        use crate::expr::LogicalProgram;
        // op_map with reindex_col >= 0 rewrites the output PK by reading the
        // referenced column through the reindex packer. Verifies (1) every row's
        // output PK matches the source column value, (2) the resulting
        // batch is correctly marked unsorted/unconsolidated (sort order on
        // the new PK is not preserved by the row-by-row promote).

        // Input: PK u64, payload i64. Reindex on the payload (col 1) — the
        // new output PK is each row's payload value.
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 200), (2, 1, 100), (3, 1, 300)]);

        // Projection plan: output keeps the same single payload column.
        let func = ScalarFunc::from_map(LogicalProgram::copy_cols(&[(1, type_code::I64)]), &schema, &schema);

        let out = op_map(
            &batch,
            &func,
            &schema,
            &schema,
            /* reindex_cols = */ &[1],
            &[],
            false,
            0,
        );
        assert_eq!(out.count, 3);
        // Each output row's PK is the sign-aware OPK image of its source payload
        // value (col 1 is I64): `widen_pk_be(encode_pk_column(v))`, i.e. the value
        // with its sign bit flipped, matching how extract_col_key routes the same
        // value. A raw-native `== 200` assertion would falsely fail signed reindex.
        let opk_i64 = |v: i64| ((v as u64) ^ 0x8000_0000_0000_0000) as u128;
        assert_eq!(out.get_pk(0), opk_i64(200));
        assert_eq!(out.get_pk(1), opk_i64(100));
        assert_eq!(out.get_pk(2), opk_i64(300));
        // Payload itself is unchanged by the projection.
        assert_eq!(get_payload_i64(&out, 0), 200);
        assert_eq!(get_payload_i64(&out, 1), 100);
        assert_eq!(get_payload_i64(&out, 2), 300);
        // Reindex destroys PK order — output must be marked accordingly.
        assert!(!out.is_sorted(), "reindex output must not be marked sorted");
        assert!(!out.is_consolidated(), "reindex output must not be marked consolidated");
    }

    // -----------------------------------------------------------------------
    // op_filter
    // -----------------------------------------------------------------------

    /// Per-row differential oracle for the batch filter path: 20 rows (≥
    /// THRESHOLD=16, so the batch evaluator is taken) filtered by `col[1] > 10`
    /// must keep exactly the rows whose payload exceeds 10, in input PK order.
    #[test]
    fn test_filter_batch_matches_per_row() {
        use crate::expr::{CmpOp, LogicalInstr, LogicalProgram, ScalarFunc};

        let schema = make_schema_u64_i64();
        // (pk, weight, payload); a row passes iff payload > 10.
        let batch = make_batch(
            &schema,
            &[
                (1, 1, 5),    // fail
                (2, 1, 15),   // pass
                (3, 1, 25),   // pass
                (4, 1, 10),   // fail (= not >)
                (5, 1, 20),   // pass
                (6, 1, 3),    // fail
                (7, 1, 30),   // pass
                (8, 1, 10),   // fail
                (9, 1, 11),   // pass
                (10, 1, 0),   // fail
                (11, 1, 50),  // pass
                (12, 1, 9),   // fail
                (13, 1, 12),  // pass
                (14, 1, 8),   // fail
                (15, 1, 100), // pass
                (16, 1, 10),  // fail
                (17, 1, 1),   // fail
                (18, 1, 13),  // pass
                (19, 1, 7),   // fail
                (20, 1, 22),  // pass
            ],
        );

        // Predicate: col[1] > 10
        let instrs = vec![
            LogicalInstr::LoadColInt { dst: 0, col: 1 },
            LogicalInstr::LoadConst { dst: 1, val: 10 },
            LogicalInstr::Cmp {
                op: CmpOp::Gt,
                dst: 2,
                a: 0,
                b: 1,
            },
        ];
        let func = ScalarFunc::from_predicate(LogicalProgram::new(instrs, 3, 2, vec![]), &schema);

        let out = op_filter(&batch, &func, &schema);
        // pk=2(15), 3(25), 5(20), 7(30), 9(11), 11(50), 13(12), 15(100), 18(13), 20(22)
        assert_eq!(out.count, 10, "expected 10 rows with val > 10");
        assert_eq!(out.get_pk(0) as u64, 2);
        assert_eq!(out.get_pk(1) as u64, 3);
        assert_eq!(out.get_pk(2) as u64, 5);
        assert_eq!(out.get_pk(3) as u64, 7);
        assert_eq!(out.get_pk(4) as u64, 9);
        assert_eq!(out.get_pk(5) as u64, 11);
        assert_eq!(out.get_pk(6) as u64, 13);
        assert_eq!(out.get_pk(7) as u64, 15);
        assert_eq!(out.get_pk(8) as u64, 18);
        assert_eq!(out.get_pk(9) as u64, 20);
    }
}
