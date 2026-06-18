//! Delta-delta joins: anti-join, semi-join, inner join (ΔA ⋈ ΔB).
//!
//! Both operands are deltas in the same epoch; matching is a (PK, payload)
//! co-group over a `BatchCursor` on B.

use std::cmp::Ordering;

use crate::schema::SchemaDescriptor;
use crate::storage::{with_payload_cmp, Batch, ConsolidatedBatch, MemBatch};

use super::super::cogroup::{cogroup_intersection, cogroup_left, BatchCursor};
use super::rowwrite::write_join_row_from_batches;

// ---------------------------------------------------------------------------
// Anti-join / Semi-join delta-delta
// ---------------------------------------------------------------------------

/// Emit batch_a rows whose (PK, payload) has NO positive-weight match in batch_b.
/// For SQL EXCEPT: a row is excluded only if B has a matching (PK, payload), not just PK.
pub fn op_anti_join_delta_delta(batch_a: &Batch, batch_b: &Batch, schema: &SchemaDescriptor) -> ConsolidatedBatch {
    ConsolidatedBatch::new_unchecked(filter_join_dd_with_payload(batch_a, batch_b, schema))
}

/// Emit batch_a rows whose PK HAS a positive-weight match in batch_b.
pub fn op_semi_join_delta_delta(batch_a: &Batch, batch_b: &Batch, schema: &SchemaDescriptor) -> ConsolidatedBatch {
    ConsolidatedBatch::new_unchecked(semi_join_dd(batch_a, batch_b, schema))
}

/// PK-only semi-join DD: emit each A PK group iff B has a positive-weight row at
/// that PK. `cogroup_intersection` over a `BatchCursor` on B visits only shared
/// PKs (galloping past the gaps — the skewed tiny ΔB ⋈ huge ΔA case gets the same
/// speedup the delta-trace joins do), and the callback brackets B's equal-PK
/// group by index to test presence.
fn semi_join_dd(batch_a: &Batch, batch_b: &Batch, schema: &SchemaDescriptor) -> Batch {
    let npc = schema.num_payload_cols();
    let cs_a = Batch::consolidate_if_needed(batch_a, schema);
    let cs_b = Batch::consolidate_if_needed(batch_b, schema);
    let ca: &Batch = cs_a.as_deref().unwrap_or(batch_a);
    let cb: &Batch = cs_b.as_deref().unwrap_or(batch_b);
    let n_a = ca.count;
    if n_a == 0 {
        return Batch::empty(npc, schema.pk_stride());
    }

    let mut output = Batch::empty_with_schema(schema);
    let mut bc = BatchCursor::new(cb);

    cogroup_intersection(ca, &mut bc, |_key, a_range, m| {
        // Intersection ⇒ m is at B's equal-PK group; bracket and test presence.
        let b_end = m.group_end();
        let present = (m.pos..b_end).any(|j| cb.get_weight(j) > 0);
        m.seek(b_end);
        if present {
            output.append_batch(ca, a_range.start, a_range.end);
        }
    });

    output.sorted = true;
    output.consolidated = true;
    gnitz_debug!("op_semi_join_dd: a={} b={} out={}", n_a, cb.count, output.count);
    output
}

/// Payload-aware anti-join DD: excludes A rows only when B has a matching
/// (PK, payload) with positive weight. Used for SQL EXCEPT.
fn filter_join_dd_with_payload(batch_a: &Batch, batch_b: &Batch, schema: &SchemaDescriptor) -> Batch {
    let cs_a = Batch::consolidate_if_needed(batch_a, schema);
    let cs_b = Batch::consolidate_if_needed(batch_b, schema);
    let ca: &Batch = cs_a.as_deref().unwrap_or(batch_a);
    let cb: &Batch = cs_b.as_deref().unwrap_or(batch_b);

    with_payload_cmp!(schema, filter_join_dd_with_payload_inner, ca, cb, schema)
}

#[inline]
fn filter_join_dd_with_payload_inner<RowCmp>(
    ca: &Batch,
    cb: &Batch,
    schema: &SchemaDescriptor,
    row_cmp: RowCmp,
) -> Batch
where
    RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy,
{
    let npc = schema.num_payload_cols();
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 {
        return Batch::empty(npc, schema.pk_stride());
    }

    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    let mut output = Batch::empty_with_schema(schema);
    let mut bc = BatchCursor::new(cb);

    // `cogroup_left` visits every A PK group (anti emits per A row whether or not
    // B matches). Inside each group, a (PK, payload) sub-merge walks A's rows and
    // B's equal-PK group in lockstep — both payload-sorted — emitting each A row
    // with no positive-weight (PK, payload) match in B, coalescing unmatched runs.
    cogroup_left(ca, &mut bc, |key, a_range, m| {
        // B's equal-PK group, or an empty range when B lacks the key (cogroup_left
        // lands m at lower_bound(key), which may be a later key's row).
        let has_b = m.pos < cb.count && cb.get_pk_bytes(m.pos) == key;
        let (b_start, b_end) = if has_b { (m.pos, m.group_end()) } else { (m.pos, m.pos) };
        let mut scan_b = b_start;
        let mut unmatched_start: Option<usize> = None;
        for idx_a_row in a_range.clone() {
            // Reuse the comparison that ends the advance: when the loop breaks
            // with scan_b in range, `ord` already holds the (≥ Equal) result for
            // the current B row, so the match test needs no second row_cmp.
            let mut ord = Ordering::Greater;
            while scan_b < b_end {
                ord = row_cmp(schema, &mb_b, scan_b, &mb_a, idx_a_row);
                if ord != Ordering::Less {
                    break;
                }
                scan_b += 1;
            }
            let matched = scan_b < b_end && ord == Ordering::Equal && cb.get_weight(scan_b) > 0;
            if !matched {
                if unmatched_start.is_none() {
                    unmatched_start = Some(idx_a_row);
                }
            } else if let Some(rs) = unmatched_start.take() {
                output.append_batch(ca, rs, idx_a_row);
            }
        }
        if let Some(rs) = unmatched_start.take() {
            output.append_batch(ca, rs, a_range.end);
        }
        m.seek(b_end);
    });

    output.sorted = true;
    output.consolidated = true;
    gnitz_debug!("op_anti_join_dd: a={} b={} out={}", n_a, n_b, output.count);
    output
}

// ---------------------------------------------------------------------------
// Inner join delta-delta
// ---------------------------------------------------------------------------

/// Delta-Delta inner join: ΔA ⋈ ΔB. Cartesian product per matching PK group.
/// Output schema: [left_PK, left_payload..., right_payload...].
/// Weight = w_a * w_b (wrapping). Zero-weight pairs are skipped.
pub fn op_join_delta_delta(
    batch_a: &Batch,
    batch_b: &Batch,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let left_npc = left_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let out_npc = left_npc + right_npc;

    let cs_a = Batch::consolidate_if_needed(batch_a, left_schema);
    let cs_b = Batch::consolidate_if_needed(batch_b, right_schema);
    let ca: &Batch = cs_a.as_deref().unwrap_or(batch_a);
    let cb: &Batch = cs_b.as_deref().unwrap_or(batch_b);
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 || n_b == 0 {
        gnitz_debug!("op_join_dd: a={} b={} out=0", n_a, n_b);
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);
    let mut bc = BatchCursor::new(cb);

    // `cogroup_intersection` over a `BatchCursor` on B emits at shared PKs only,
    // galloping past the gaps (the skewed tiny ⋈ huge case gets the same speedup
    // the delta-trace joins get). Inside each group, bracket B's equal-PK group
    // by index and emit the A-major × B-inner cartesian product (left payload
    // ascending then right payload ascending ⇒ output stays (PK, payload)-sorted).
    cogroup_intersection(ca, &mut bc, |_key, a_range, m| {
        let b_start = m.pos;
        let b_end = m.group_end();
        for i in a_range {
            let wa = ca.get_weight(i);
            if wa == 0 {
                continue;
            }
            for j in b_start..b_end {
                let wb = cb.get_weight(j);
                let w_out = wa.wrapping_mul(wb);
                if w_out != 0 {
                    write_join_row_from_batches(&mut output, &mb_a, i, &mb_b, j, w_out, left_schema, right_schema);
                }
            }
        }
        m.seek(b_end);
    });

    output.sorted = true;
    gnitz_debug!("op_join_dd: a={} b={} out={}", n_a, n_b, output.count);
    output
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::test_common::*;
    use super::*;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, SHORT_STRING_THRESHOLD};
    use crate::storage::{Batch, ConsolidatedBatch};
    use crate::test_support::{make_wide_batch, wide_pk_3xu64_schema};

    #[test]
    fn test_anti_join_dd_basic() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        // B has PK=2 but different payload (200 vs 20) → payload-aware: no match
        let b = make_batch(&schema, &[(2, 1, 200)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // All 3 rows survive (different payloads)
        assert_eq!(out.count, 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_anti_join_dd_basic_matching_payload() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        // B has PK=2 with SAME payload (20) → exact match → excluded
        let b = make_batch(&schema, &[(2, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!((out.get_pk(1) as u64), 3);
    }

    #[test]
    fn test_anti_join_dd_empty_right() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let b = make_batch(&schema, &[]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!((out.get_pk(1) as u64), 2);
    }

    #[test]
    fn test_anti_join_dd_full_overlap() {
        let schema = make_schema_u64_i64();
        // Same PK AND same payload → full match → all excluded
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_anti_join_dd_negative_weight_b() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        // PK=1: B has negative weight → should NOT suppress A rows
        // PK=2: B has matching payload (20) with positive weight → excluded
        let b = make_batch(&schema, &[(1, -1, 10), (2, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 1);
        assert_eq!((out.get_pk(0) as u64), 1);
    }

    /// Pin: the GENERIC `RowCmp` arm (`compare_rows`, German-string comparison)
    /// must drive `filter_join_dd_with_payload_inner`'s shared-PK (PK, payload)
    /// sub-merge. A `(U64 pk, STRING payload)` schema resolves to
    /// `PayloadCmpKind::Generic`, so `with_payload_cmp!` threads the
    /// string-aware comparator into the anti-join.
    ///
    /// A = [(1,1,"drop"), (1,1,"keep")] (payload-sorted: "drop" < "keep"),
    /// B = [(1,1,"drop")]. The anti-join (EXCEPT) excludes an A row only when B
    /// has a *matching (PK, payload)*: "drop" matches and is dropped; "keep" has
    /// no match and survives. The fixed-int comparator treats the German-string
    /// structs as raw 8-byte integers — it cannot tell "drop" from "keep"
    /// correctly, so it either suppresses the wrong row (dropping "keep") or
    /// fails to suppress "drop", changing the surviving string.
    #[test]
    fn test_anti_join_dd_generic_rowcmp_same_pk_string() {
        let schema = make_schema_u64_string();
        // Guard: the schema must select the GENERIC comparator, else this pin
        // would not exercise the string-aware arm.
        assert_eq!(
            schema.payload_cmp,
            crate::schema::PayloadCmpKind::Generic,
            "U64+STRING schema must use the GENERIC payload comparator",
        );

        // Rows supplied in (PK, payload) order ("drop" < "keep") so the
        // sorted+consolidated flags `make_batch_str` sets are truthful.
        let a = make_batch_str(&schema, &[(1, 1, "drop"), (1, 1, "keep")]);
        let b = make_batch_str(&schema, &[(1, 1, "drop")]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);

        assert_eq!(out.count, 1, "only the unmatched A row survives EXCEPT");
        assert_eq!(out.get_pk(0) as u64, 1);
        // The surviving row is "keep" — "drop" matched B and was excluded.
        assert_eq!(
            read_str_payload(&out, 0, 0),
            "keep",
            "generic string compare must keep \"keep\" and drop \"drop\"",
        );
    }

    // -----------------------------------------------------------------------
    // Semi-join DD tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_semi_join_dd_basic() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let b = make_batch(&schema, &[(2, 1, 200), (3, 1, 300)]);
        let out = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 2);
        assert_eq!((out.get_pk(1) as u64), 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_anti_semi_complement_matching_payloads() {
        // When B has matching payloads, anti + semi = total
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
        let b = make_batch(&schema, &[(2, 1, 20), (4, 1, 40)]);
        let total = a.count;
        let anti = op_anti_join_delta_delta(&a, &b, &schema);
        let semi = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(anti.count + semi.count, total);
    }

    // -----------------------------------------------------------------------
    // Join DD tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_cartesian() {
        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        // PK=1: 2 rows in a, 3 rows in b → 6 output rows
        let a = make_batch(&left_schema, &[(1, 1, 10), (1, 2, 20)]);
        let b = make_batch(&right_schema, &[(1, 1, 100), (1, 3, 300), (1, -1, 500)]);
        let out = op_join_delta_delta(&a, &b, &left_schema, &right_schema);
        // 2 × 3 = 6, but check for zero-weight elimination:
        // (w=1*1=1), (w=1*3=3), (w=1*-1=-1), (w=2*1=2), (w=2*3=6), (w=2*-1=-2)
        // All non-zero → 6 rows
        assert_eq!(out.count, 6);
        assert!(out.sorted);

        // Check weights: row 0 = (a[0] × b[0]) = 1*1 = 1
        assert_eq!(out.get_weight(0), 1);
        // row 1 = (a[0] × b[1]) = 1*3 = 3
        assert_eq!(out.get_weight(1), 3);
    }

    #[test]
    fn test_join_dd_string_columns() {
        let left_schema = make_schema_u64_string();
        let right_schema = make_schema_u64_string();
        let a = make_batch_str(&left_schema, &[(1, 1, "hello")]);
        let b = make_batch_str(&right_schema, &[(1, 1, "world_longer_than_twelve")]);
        let out = op_join_delta_delta(&a, &b, &left_schema, &right_schema);
        assert_eq!(out.count, 1);
        // Left payload = col 0, right payload = col 1
        let left_str = read_str_payload(&out, 0, 0);
        let right_str = read_str_payload(&out, 1, 0);
        assert_eq!(left_str, "hello");
        assert_eq!(right_str, "world_longer_than_twelve");
    }

    #[test]
    fn test_join_dd_empty_inputs() {
        let ls = make_schema_u64_i64();
        let rs = make_schema_u64_i64();

        let out1 = op_join_delta_delta(&make_batch(&ls, &[]), &make_batch(&rs, &[(1, 1, 100)]), &ls, &rs);
        assert_eq!(out1.count, 0);

        let out2 = op_join_delta_delta(&make_batch(&rs, &[(1, 1, 100)]), &make_batch(&ls, &[]), &ls, &rs);
        assert_eq!(out2.count, 0);

        let out3 = op_join_delta_delta(&make_batch(&ls, &[]), &make_batch(&rs, &[]), &ls, &rs);
        assert_eq!(out3.count, 0);
    }

    #[test]
    fn test_join_dd_zero_weight_skip() {
        let ls = make_schema_u64_i64();
        let rs = make_schema_u64_i64();
        // a has weight=0 after consolidation wouldn't happen, but test the skip
        let a = make_batch(&ls, &[(1, 2, 10)]);
        // b has a pair that nets to zero weight after consolidation
        let mut b = Batch::with_schema(rs, 2);

        for &(pk, w, val) in &[(1u64, 1i64, 100i64), (1u64, -1i64, 100i64)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = false;

        let out = op_join_delta_delta(&a, &b, &ls, &rs);
        // b consolidates to empty (1 + -1 = 0 for same pk+payload) → no match
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_join_dd_wide_pk_works() {
        // Wide PK: 3× U64 = 24-byte stride. The byte-API port handles all widths
        // without panicking and produces correct Cartesian product output.
        let schema = wide_pk_3xu64_schema();
        let a = make_wide_batch(&schema, &[(1, 0, 0, 1, 7)]);
        // Self-join: (1,0,0) × (1,0,0) → one output row with weight 1*1=1.
        let out = op_join_delta_delta(&a, &a, &schema, &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_weight(0), 1);
    }

    #[test]
    fn test_anti_join_dd_same_pk_different_payload() {
        let schema = make_schema_u64_i64();
        // A: two rows with PK=1, different payloads
        let a = make_batch(&schema, &[(1, 1, 10), (1, 1, 20)]);
        // B: one row with PK=1, payload=20
        let b = make_batch(&schema, &[(1, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // Only (PK=1, val=10) should survive — (PK=1, val=20) matches B exactly
        assert_eq!(out.count, 1);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
    }

    #[test]
    fn test_anti_join_dd_same_pk_same_payload() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10)]);
        let b = make_batch(&schema, &[(1, 1, 10)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // Exact payload match → output should be empty
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_semi_join_dd_unchanged() {
        // Semi-join DD should still use PK-only semantics
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (1, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 999)]); // different payload but same PK
        let out = op_semi_join_delta_delta(&a, &b, &schema);
        // PK-only: both A rows match B's PK → 2 rows
        assert_eq!(out.count, 2);
    }

    // -----------------------------------------------------------------------
    // BLOB payload copy in join writers (item 2)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_blob_left_and_right_payload() {
        // BLOB payload columns on both sides of a DD join. write_left_payload
        // (left) and the right-side copy in write_join_row_from_batches must
        // relocate the blob, not copy the 16-byte struct verbatim.
        let left_schema = make_schema_u64_blob();
        let right_schema = make_schema_u64_blob();
        let left_blob: &[u8] = b"left-long-blob-value-beyond-twelve";
        let right_blob: &[u8] = b"right-long-blob-value-beyond-twelve";
        let a = make_batch_blob(&left_schema, &[(1, 1, left_blob)]);
        let b = make_batch_blob(&right_schema, &[(1, 1, right_blob)]);
        let out = op_join_delta_delta(&a, &b, &left_schema, &right_schema);
        assert_eq!(out.count, 1);
        assert!(!out.blob.is_empty(), "join output must carry blob buffer");
        // Left BLOB at out col 0, right BLOB at out col 1.
        let resolve = |col: usize| -> Vec<u8> {
            let gs = &out.col_data(col)[0..16];
            let len = u32::from_le_bytes(gs[0..4].try_into().unwrap()) as usize;
            let off = u64::from_le_bytes(gs[8..16].try_into().unwrap()) as usize;
            crate::schema::long_string_bytes(&out.blob, off, len).to_vec()
        };
        assert_eq!(resolve(0), left_blob, "left BLOB must resolve correctly");
        assert_eq!(resolve(1), right_blob, "right BLOB must resolve correctly");
    }

    // -----------------------------------------------------------------------
    // Stride consistency on early-exit empty join batches (item 3)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_empty_stride_u64() {
        let ls = make_schema_u64_i64();
        let rs = make_schema_u64_i64();
        let out = op_join_delta_delta(&make_batch(&ls, &[]), &make_batch(&rs, &[(1, 1, 1)]), &ls, &rs);
        assert_eq!(out.count, 0);
        assert_eq!(out.pk_stride(), 8, "empty join output must use schema PK stride 8");
    }

    #[test]
    fn test_anti_join_dd_empty_stride_u64() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[]);
        let b = make_batch(&schema, &[(1, 1, 1)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 0);
        assert_eq!(out.pk_stride(), 8);
    }

    // -----------------------------------------------------------------------
    // Shift guards for 64-payload-column left schema (items 4b, 4c, 4d)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_signed_pk_match_found() {
        // left=[2], right sorted signed [-3, 2]. Shared key = 2. Raw-u128
        // comparison advances the left pointer past 2 (since 2 < (-3 as u128))
        // and misses the match. compare_pk_bytes dispatch must find it.
        let ls = make_schema_signed();
        let rs = make_schema_signed();
        let a = make_signed_batch(&ls, &[(2, 1, 10)]);
        let b = make_signed_batch(&rs, &[(-3, 1, 30), (2, 1, 20)]);
        let out = op_join_delta_delta(&a, &b, &ls, &rs);
        assert_eq!(out.count, 1, "key 2 is shared and must join");
        assert_eq!(signed_pk_i64(&out, 0), 2);
        assert_eq!(get_payload_i64(&out, 0), 10);
    }

    #[test]
    fn test_semi_anti_join_dd_signed_pk() {
        let schema = make_schema_signed();
        // a sorted signed: -3, 2. b sorted signed: -3.
        let a = make_signed_batch(&schema, &[(-3, 1, 1), (2, 1, 2)]);
        let b = make_signed_batch(&schema, &[(-3, 1, 1)]);
        let semi = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(semi.count, 1, "only -3 shares a PK");
        assert_eq!(signed_pk_i64(&semi, 0), -3);
        let anti = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(anti.count, 1, "2 is not in b");
        assert_eq!(signed_pk_i64(&anti, 0), 2);
    }

    #[test]
    fn test_join_dd_compound_pk_match_found() {
        // Compound (c0,c1). left=[(1,0)], right sorted [(0,1),(1,0)]. Shared
        // key (1,0). Raw-u128 packs c1 high, so (1,0)=1 < (0,1)=1<<64; the
        // walk would advance left past the match. Byte dispatch finds it.
        let schema = make_schema_compound();
        let a = make_compound_batch(&schema, &[(1, 0, 1, 10)]);
        let b = make_compound_batch(&schema, &[(0, 1, 1, 30), (1, 0, 1, 20)]);
        let out = op_join_delta_delta(&a, &b, &schema, &schema);
        assert_eq!(out.count, 1, "compound key (1,0) is shared");
        assert_eq!(out.get_pk_bytes(0), &compound_pk_bytes(1, 0));
    }

    // -----------------------------------------------------------------------
    // Wide-PK test helpers
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_wide_pk_cartesian() {
        // Two wide-PK batches with two matching PKs.
        let schema = wide_pk_3xu64_schema();
        let a = make_wide_batch(&schema, &[(1, 0, 0, 1, 10), (2, 0, 0, 1, 20)]);
        let b = make_wide_batch(&schema, &[(1, 0, 0, 1, 100), (2, 0, 0, 1, 200)]);
        let out = op_join_delta_delta(&a, &b, &schema, &schema);
        // Both PKs match: (1,0,0)×(1,0,0) and (2,0,0)×(2,0,0) → 2 output rows.
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(out.get_weight(1), 1);
    }

    #[test]
    fn test_semi_join_dd_wide_pk() {
        // Wide-PK semi-join: A has (1,0,0) and (3,0,0); B has (1,0,0) only.
        // Output: only (1,0,0) from A.
        let schema = wide_pk_3xu64_schema();
        let a = make_wide_batch(&schema, &[(1, 0, 0, 1, 10), (3, 0, 0, 1, 30)]);
        let b = make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]);
        let out = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 0, 0).as_slice());
    }

    #[test]
    fn test_anti_join_dd_wide_pk() {
        // Wide-PK payload-aware anti-join (EXCEPT): A row excluded only when B has
        // an exact (PK, payload) match with positive weight.
        // A has (1,0,0,payload=10) and (3,0,0,payload=30); B has (1,0,0,payload=10).
        // Output: only (3,0,0) from A (the (1,0,0) row is excluded by exact match).
        let schema = wide_pk_3xu64_schema();
        let a = make_wide_batch(&schema, &[(1, 0, 0, 1, 10), (3, 0, 0, 1, 30)]);
        let b = make_wide_batch(&schema, &[(1, 0, 0, 1, 10)]); // same payload as A's first row
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 3, 0, 0).as_slice());
    }

    // -----------------------------------------------------------------------
    // Signed narrow PK ordering (I64 negative values)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_signed_narrow_negative_pks() {
        // Single-column I64 PK: -3, -1, 2. Two batches. Assert (-1,-1) matches.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let left = make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10), (2, 1, 20)]);
        let right = make_signed_batch(&schema, &[(-1, 1, 100), (4, 1, 400)]);
        let out = op_join_delta_delta(&left, &right, &schema, &schema);
        // Only -1 matches on both sides.
        assert_eq!(out.count, 1);
        assert_eq!(signed_pk_i64(&out, 0), -1i64);
        assert_eq!(out.get_weight(0), 1);
    }

    // -----------------------------------------------------------------------
    // Anti-join DD monotonic scan_b correctness (item 30)
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dd_large_same_pk_group() {
        // Single PK group with many payloads. A's payloads 0..50 even; B's
        // payloads 0..50 even with positive weight. Anti-join keeps A rows
        // whose payload is NOT in B. Build A = payloads {0,2,4,...,98},
        // B = payloads {0,4,8,...,96}. Survivors: payloads in A not in B.
        let schema = make_schema_u64_i64();
        let a_rows: Vec<(u64, i64, i64)> = (0..50).map(|k| (1u64, 1i64, (k * 2) as i64)).collect();
        let b_rows: Vec<(u64, i64, i64)> = (0..25).map(|k| (1u64, 1i64, (k * 4) as i64)).collect();
        let a = make_batch(&schema, &a_rows);
        let b = make_batch(&schema, &b_rows);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // Reference: A payload p survives iff p not in B (multiples of 4).
        let expected: Vec<i64> = (0..50).map(|k| (k * 2) as i64).filter(|p| p % 4 != 0).collect();
        assert_eq!(out.count, expected.len());
        for (i, &p) in expected.iter().enumerate() {
            assert_eq!(get_payload_i64(&out, i), p, "row {i}");
        }
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

    fn read_str_payload(batch: &Batch, col: usize, row: usize) -> String {
        let off = row * 16;
        let gs = &batch.col_data(col)[off..off + 16];
        let length = u32::from_le_bytes(gs[0..4].try_into().unwrap()) as usize;
        if length == 0 {
            return String::new();
        }
        if length <= SHORT_STRING_THRESHOLD {
            String::from_utf8_lossy(&gs[4..4 + length]).to_string()
        } else {
            let blob_offset = u64::from_le_bytes(gs[8..16].try_into().unwrap()) as usize;
            String::from_utf8_lossy(&batch.blob[blob_offset..blob_offset + length]).to_string()
        }
    }

    fn make_schema_u64_blob() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::BLOB, 0),
            ],
            &[0],
        )
    }

    fn make_batch_blob(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8])]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, bytes) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            let length = bytes.len() as u32;
            let mut gs = [0u8; 16];
            gs[0..4].copy_from_slice(&length.to_le_bytes());
            if bytes.len() <= SHORT_STRING_THRESHOLD {
                gs[4..4 + bytes.len()].copy_from_slice(bytes);
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
}
