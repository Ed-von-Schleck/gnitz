//! Delta-trace joins: anti-join, semi-join, inner join, outer join.
//!
//! Each delta row is matched against the integrated trace (`z⁻¹(I)`) via a
//! co-group on the join key. Output schema: `[left_PK, left_payload…, right_payload…]`.

use crate::schema::SchemaDescriptor;
use crate::storage::{scatter_copy, write_to_batch, Batch, ConsolidatedBatch, ReadCursor};

use super::super::cogroup::{cogroup_intersection, cogroup_left};
use super::rowwrite::{write_join_row, write_join_row_null_right};

thread_local! {
    /// Reused emit-index scratch for the delta-trace anti/semi joins. Cleared
    /// per operator call rather than allocating a fresh `Vec<u32>` each tick —
    /// same hold-across-work, no-cap shape as exchange's pools. The borrow is
    /// confined to one operator body; `scatter_copy` does not re-enter.
    static JOIN_EMIT_INDICES: std::cell::RefCell<Vec<u32>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

// ---------------------------------------------------------------------------
// Anti-join delta-trace
// ---------------------------------------------------------------------------

/// Emit delta rows whose key has NO positive-weight match in the trace.
/// `cogroup_left` visits every delta group (the match side galloping-skips to
/// it); the group is emitted iff `group_has_positive` reports no live row in the
/// trace group — i.e. the key is absent from Distinct(trace).
pub fn op_anti_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> ConsolidatedBatch {
    let npc = schema.num_payload_cols();
    let cs = Batch::consolidate_if_needed(delta, schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
    }

    JOIN_EMIT_INDICES.with(|pool| {
        let mut emit_indices = pool.borrow_mut();
        emit_indices.clear();

        cogroup_left(consolidated, cursor, |key, range, m| {
            if !m.group_has_positive(key) {
                for i in range {
                    emit_indices.push(i as u32);
                }
            }
        });

        if emit_indices.is_empty() {
            return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
        }

        let mb = consolidated.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut output = write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
            scatter_copy(&mb, &emit_indices, &[], writer);
        });
        // emit_indices ascend (groups visited in key order, ranges ascending),
        // so rows come out in consolidated-delta order: (PK, payload)-sorted.
        output.sorted = true;
        output.consolidated = true;
        ConsolidatedBatch::new_unchecked(output)
    })
}

// ---------------------------------------------------------------------------
// Semi-join delta-trace
// ---------------------------------------------------------------------------

/// Emit delta rows whose key HAS a positive-weight match in the trace.
/// `cogroup_intersection` visits only shared keys (both pointers galloping-skip
/// to catch up, optimal whichever side is larger — replacing the old size
/// selector + swapped path); the delta group is emitted iff `group_has_positive`
/// reports a live row in the trace group.
pub fn op_semi_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> ConsolidatedBatch {
    let npc = schema.num_payload_cols();
    let cs = Batch::consolidate_if_needed(delta, schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
    }

    JOIN_EMIT_INDICES.with(|pool| {
        let mut emit_indices = pool.borrow_mut();
        emit_indices.clear();

        cogroup_intersection(consolidated, cursor, |key, range, m| {
            if m.group_has_positive(key) {
                for i in range {
                    emit_indices.push(i as u32);
                }
            }
        });

        if emit_indices.is_empty() {
            return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
        }

        let mb = consolidated.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut output = write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
            scatter_copy(&mb, &emit_indices, &[], writer);
        });
        output.sorted = true;
        output.consolidated = true;
        ConsolidatedBatch::new_unchecked(output)
    })
}

// ---------------------------------------------------------------------------
// Inner join delta-trace
// ---------------------------------------------------------------------------

/// Join delta rows against trace. Output schema: [left_PK, left_payload..., right_payload...].
///
/// `cogroup_intersection` on the join key: the match (trace) side is forward-only,
/// so each trace group is walked once and producted against the whole
/// (random-access) delta group `[i, j)` — trace-major emission. Galloping skips
/// make this optimal in both former selector regimes (huge delta + tiny trace,
/// and the reverse) with no `rewind` / `estimated_length` / size selector.
pub fn op_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let out_npc = left_schema.num_payload_cols() + right_schema.num_payload_cols();

    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    let delta_mb = consolidated.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    cogroup_intersection(consolidated, cursor, |key, range, m| {
        m.for_each_pk_group_row(key, |c| {
            let w_trace = c.current_weight;
            for i in range.clone() {
                let w_out = consolidated.get_weight(i).wrapping_mul(w_trace);
                if w_out != 0 {
                    write_join_row(&mut output, &delta_mb, i, c, w_out, left_schema, right_schema);
                }
            }
        });
    });

    // Trace-major cartesian emission is PK-sorted but NOT (PK, payload)-sorted
    // (left payloads interleave across trace entries); downstream re-sorts.
    output.sorted = false;
    output
}

// ---------------------------------------------------------------------------
// Outer join delta-trace
// ---------------------------------------------------------------------------

/// Left outer join: like inner join, but a delta group with no positive-weight
/// trace match additionally emits null-filled right sides. `cogroup_left` visits
/// every delta group; `matched` is a property of the **trace group** (any
/// positive-weight row), shared by every delta row in the group, so the
/// null-fill decision is made once per group after the (trace-major) product.
pub fn op_join_delta_trace_outer(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let out_npc = left_schema.num_payload_cols() + right_schema.num_payload_cols();

    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    let delta_mb = consolidated.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    cogroup_left(consolidated, cursor, |key, range, m| {
        // Walk the trace group once: product against the whole delta group and
        // record whether any positive-weight trace row exists. A tombstone
        // (weight ≤ 0) still products but does NOT suppress the null-fill.
        let mut matched = false;
        m.for_each_pk_group_row(key, |c| {
            let w_trace = c.current_weight;
            if w_trace > 0 {
                matched = true;
            }
            for i in range.clone() {
                let w_out = consolidated.get_weight(i).wrapping_mul(w_trace);
                if w_out != 0 {
                    write_join_row(&mut output, &delta_mb, i, c, w_out, left_schema, right_schema);
                }
            }
        });
        if !matched {
            for i in range {
                write_join_row_null_right(
                    &mut output,
                    &delta_mb,
                    i,
                    consolidated.get_weight(i),
                    left_schema,
                    right_schema,
                );
            }
        }
    });

    output.sorted = false;
    output
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::test_common::*;
    use super::*;
    use crate::foundation::codec::read_i64_le;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::{Batch, ConsolidatedBatch};
    use crate::test_support::{make_wide_batch, wide_pk_3xu64_schema};

    #[test]
    fn test_anti_join_dt_basic() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();

        // Trace: pk=2 and pk=4 exist
        let trace = Rc::new(make_batch(&schema, &[(2, 1, 200), (4, 1, 400)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: pk=1,2,3
        let delta = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // pk=2 is in trace, so output should be pk=1 and pk=3
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!((out.get_pk(1) as u64), 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_semi_join_dt_basic() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();

        // Trace: pk=2 and pk=3 exist with positive weight
        let trace = Rc::new(make_batch(&schema, &[(2, 1, 200), (3, 1, 300)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: pk=1,2,3,4
        let delta = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only pk=2 and pk=3 have trace matches
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 2);
        assert_eq!((out.get_pk(1) as u64), 3);
    }

    /// Compound PK: the co-group compares trace PK vs delta PK by bytes. Raw
    /// u128 order is last-column-major, so a byte compare (not u128) is required
    /// or the scan mislocates and matches/anti-matches are wrong.
    #[test]
    fn test_semi_anti_join_dt_compound_pk() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_compound();
        // Trace (storage order): (1,5) and (2,3).
        let trace = Rc::new(make_compound_batch(&schema, &[(1, 5, 1, 100), (2, 3, 1, 200)]).into_inner());

        // Delta (storage order): (1,5), (2,3), (3,1).
        let delta = make_compound_batch(&schema, &[(1, 5, 1, 10), (2, 3, 1, 20), (3, 1, 1, 30)]);

        let mut ch = CursorHandle::from_owned(&[Rc::clone(&trace)], schema);
        let semi = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(semi.count, 2, "semi: (1,5) and (2,3) match the trace");
        assert_eq!(semi.get_pk_bytes(0), &compound_pk_bytes(1, 5));
        assert_eq!(semi.get_pk_bytes(1), &compound_pk_bytes(2, 3));

        let mut ch2 = CursorHandle::from_owned(&[trace], schema);
        let anti = op_anti_join_delta_trace(&delta, ch2.cursor_mut(), &schema);
        assert_eq!(anti.count, 1, "anti: only (3,1) is absent from the trace");
        assert_eq!(anti.get_pk_bytes(0), &compound_pk_bytes(3, 1));
    }

    /// Signed single-column PK: negatives sort first in storage but last in raw
    /// u128, so the advance loop must use `current_pk_cmp`.
    #[test]
    fn test_semi_anti_join_dt_signed_pk() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_signed();
        // Trace (signed order): -3, -1.
        let trace = Rc::new(make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10)]).into_inner());

        // Delta (signed order): -3, -1, 2.
        let delta = make_signed_batch(&schema, &[(-3, 1, 1), (-1, 1, 2), (2, 1, 3)]);

        let mut ch = CursorHandle::from_owned(&[Rc::clone(&trace)], schema);
        let semi = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(semi.count, 2, "semi: -3 and -1 match the trace");
        assert_eq!(signed_pk_i64(&semi, 0), -3);
        assert_eq!(signed_pk_i64(&semi, 1), -1);

        let mut ch2 = CursorHandle::from_owned(&[trace], schema);
        let anti = op_anti_join_delta_trace(&delta, ch2.cursor_mut(), &schema);
        assert_eq!(anti.count, 1, "anti: only 2 is absent from the trace");
        assert_eq!(signed_pk_i64(&anti, 0), 2);
    }

    #[test]
    fn test_semi_join_dt_nonconsolidated() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();

        // Trace: pk=1 exists
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Non-consolidated delta: pk=1 appears twice (w=2 and w=3)
        let mut delta = Batch::with_schema(schema, 2);
        for (pk, w, val) in [(1u64, 2i64, 10i64), (1u64, 3i64, 10i64)] {
            delta.extend_pk(pk as u128);
            delta.extend_weight(&w.to_le_bytes());
            delta.extend_null_bmp(&0u64.to_le_bytes());
            delta.extend_col(0, &val.to_le_bytes());
            delta.count += 1;
        }
        delta.sorted = true;
        delta.consolidated = false;

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // After consolidation of delta: pk=1 w=5 → matches trace → 1 row
        assert_eq!(out.count, 1);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(out.get_weight(0), 5);
    }

    // -----------------------------------------------------------------------
    // op_join_delta_trace_outer
    // -----------------------------------------------------------------------

    #[test]
    fn test_outer_join_null_fill() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();

        // Right trace: pk=1 val=100
        let trace = Rc::new(make_batch(&right_schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);

        // Delta (left): pk=1 val=10 (matches), pk=2 val=20 (no match → null fill)
        let delta = make_batch(&left_schema, &[(1, 1, 10), (2, 1, 20)]);

        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &left_schema, &right_schema);

        assert_eq!(out.count, 2);
        // pk=1: matched, left payload=10, right payload=100
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(get_payload_i64(&out, 0), 10); // left val

        // pk=2: no match, left payload=20, right columns null-filled
        assert_eq!((out.get_pk(1) as u64), 2);
        assert_eq!(get_payload_i64(&out, 1), 20); // left val
                                                  // Right column (payload col 1) should be null
        let null_word = u64::from_le_bytes(out.null_bmp_data()[8..16].try_into().unwrap());
        assert!(
            null_word & 2 != 0,
            "right payload column (bit 1) should be null for non-matching row"
        );
    }

    /// Inner join delta×trace on a narrow (I32, 4-byte) signed PK. Every delta
    /// key has a trace match; all must be found, including the smallest key
    /// (regression guard for the byte-seek path at sub-8-byte stride).
    #[test]
    fn test_join_dt_i32_key_all_match() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let left_schema = make_schema_i32();
        let right_schema = make_schema_i32();
        // Trace (right): keys 1 and 2, both present.
        let trace = Rc::new(make_i32_batch(&right_schema, &[(1, 1, 100), (2, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        // Delta (left): keys 1 and 2.
        let delta = make_i32_batch(&left_schema, &[(1, 1, 10), (2, 1, 20)]);

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &left_schema, &right_schema);
        assert_eq!(out.count, 2, "both I32 keys must join (smallest key not dropped)");
        assert_eq!(out.get_pk(0) as i32, 1);
        assert_eq!(out.get_pk(1) as i32, 2);
    }

    #[test]
    fn test_join_dt_many_to_many_output_unsorted() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Many-to-many inner join: the unified co-group walks each trace group
        // once and products it against the whole delta group, emitting the
        // cartesian product in trace-major order — left payloads interleave out
        // of (PK, payload) order, so the output must be marked unsorted.
        let schema = make_schema_u64_i64();
        // Trace: PK=1 with two right payloads (100, 200).
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        // Delta: PK=1 with three left payloads.
        let delta = make_batch(&schema, &[(1, 1, 10), (1, 1, 20), (1, 1, 30)]).into_inner();

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &schema, &schema);
        assert_eq!(out.count, 6, "3 left × 2 right = 6 join outputs");

        // col 0 = left payload, col 1 = right payload (all PKs equal). The sorted
        // flag may be true ONLY if the rows are actually in (left, right) order.
        let pairs: Vec<(i64, i64)> = (0..out.count)
            .map(|r| (read_i64_le(out.col_data(0), r * 8), read_i64_le(out.col_data(1), r * 8)))
            .collect();
        let actually_sorted = pairs.windows(2).all(|w| w[0] <= w[1]);
        assert!(
            !out.sorted || actually_sorted,
            "many-to-many join marked output sorted but payload order is {pairs:?}",
        );
    }

    // -----------------------------------------------------------------------
    // Range join DT
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_same_pk_different_payload() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();

        // Trace: PK=1 exists with positive weight
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: two rows with PK=1 but different payloads (both should be matched)
        let delta = make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]);

        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Both rows have PK=1 which is in the trace → anti-join emits 0 rows
        assert_eq!(out.count, 0, "both same-PK rows should be matched by trace");
    }

    #[test]
    fn test_semi_join_dt_same_pk_different_payload() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();

        // Trace: PK=1 exists with positive weight
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: two rows with PK=1 but different payloads (both should be matched)
        let delta = make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Both rows have PK=1 which is in the trace → semi-join emits 2 rows
        assert_eq!(out.count, 2, "both same-PK rows should be emitted by semi-join");
    }

    // -----------------------------------------------------------------------
    // Semi-join DT: large-delta regime weight inflation + u128::MAX sentinel
    // -----------------------------------------------------------------------

    #[test]
    fn test_semi_join_dt_large_delta_max_pk() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();

        // Trace: PK=u64::MAX with positive weight — previously the sentinel
        // last_pk = u128::MAX would match this on first occurrence and skip it.
        let max_pk = u64::MAX;
        let trace = Rc::new(make_batch(&schema, &[(max_pk, 1, 0)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Build 25 delta rows: a large delta against a 1-row trace
        let mut rows: Vec<(u64, i64, i64)> = (1u64..=24).map(|i| (i, 1, i as i64)).collect();
        rows.push((max_pk, 1, 0));
        rows.sort_by_key(|r| r.0);
        let delta = make_batch(&schema, &rows);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only PK=u64::MAX from delta matches — must emit exactly 1 row
        assert_eq!(out.count, 1, "PK=u64::MAX must match (large delta vs tiny trace)");
        assert_eq!((out.get_pk(0) as u64), max_pk);
    }

    #[test]
    fn test_semi_join_dt_no_weight_inflation() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;

        let schema = make_schema_u64_i64();

        // Trace: 3 entries for PK=1 with different payloads
        // This triggers the trace-has-many scenario
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100), (1, 1, 200), (1, 1, 300)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: 25 rows — a large delta against a 3-row (same-PK) trace
        let mut rows: Vec<(u64, i64, i64)> = (1u64..=25).map(|pk| (pk, 1, pk as i64 * 10)).collect();
        rows.sort_by_key(|r| r.0);
        let delta = make_batch(&schema, &rows);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only PK=1 from delta matches — should emit exactly 1 row, not 3
        assert_eq!(
            out.count, 1,
            "PK=1 should appear once, not inflated by trace duplicates"
        );
        assert_eq!((out.get_pk(0) as u64), 1);
    }

    // -----------------------------------------------------------------------
    // Anti-join DD: payload-aware matching
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_uncompacted_tombstone_then_live() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        let schema = make_schema_u64_i64();
        // Trace for PK=1 has two entries (sorted by payload): a tombstone
        // (payload 100, w=-1) then a live row (payload 200, w=+1). Net: PK=1
        // is present. Anti-join must NOT emit the delta row for PK=1.
        let trace = Rc::new(make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        let delta = make_batch(&schema, &[(1, 1, 5)]);
        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "PK=1 is in trace (net positive via second entry)");
    }

    #[test]
    fn test_semi_join_dt_uncompacted_tombstone_then_live() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        let schema = make_schema_u64_i64();
        let trace = Rc::new(make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        let delta = make_batch(&schema, &[(1, 1, 5)]);
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 1, "PK=1 has a positive-weight trace entry → emitted");
    }

    #[test]
    fn test_anti_join_dt_duplicate_pk_group() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        let schema = make_schema_u64_i64();
        // Trace: PK=1 present (positive).
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        // Two consolidated delta rows with same PK=1, distinct payloads.
        let delta = make_batch(&schema, &[(1, 1, 10), (1, 1, 20)]);
        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "both PK=1 rows excluded");
    }

    // -----------------------------------------------------------------------
    // Outer join uncompacted trace (item 25)
    // -----------------------------------------------------------------------

    #[test]
    fn test_outer_join_uncompacted_no_positive_entry() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        // Trace has a single tombstone entry for PK=1 (w=-1). No positive
        // entry → the key is NOT in Distinct(trace) → null-fill must be emitted.
        let trace = Rc::new(make_batch(&right_schema, &[(1, -1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        let delta = make_batch(&left_schema, &[(1, 1, 10)]);
        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &left_schema, &right_schema);
        // Expect: one inner row (w = 1 * -1 = -1) AND one null-extended row (w=+1).
        assert_eq!(
            out.count, 2,
            "inner row for the tombstone + null-fill (no positive match)"
        );
        let mut weights: Vec<i64> = (0..out.count).map(|i| out.get_weight(i)).collect();
        weights.sort();
        assert_eq!(weights, vec![-1, 1]);
        // The null-filled row must have the right payload column null.
        let null_filled = (0..out.count).find(|&i| {
            let nw = u64::from_le_bytes(out.null_bmp_data()[i * 8..i * 8 + 8].try_into().unwrap());
            nw & 2 != 0
        });
        assert!(null_filled.is_some(), "a null-extended row must be present");
    }

    #[test]
    fn test_outer_join_uncompacted_positive_entry_no_null_fill() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        // Trace for PK=1: a live entry (w=+1) and a tombstone (w=-1). The key
        // IS in Distinct(trace) → no null-fill.
        let trace = Rc::new(make_batch(&right_schema, &[(1, 1, 100), (1, -1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        let delta = make_batch(&left_schema, &[(1, 1, 10)]);
        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &left_schema, &right_schema);
        assert_eq!(out.count, 2, "two inner rows, no null-fill");
        for i in 0..out.count {
            let nw = u64::from_le_bytes(out.null_bmp_data()[i * 8..i * 8 + 8].try_into().unwrap());
            assert_eq!(nw & 2, 0, "no row should have a null right column");
        }
    }

    // -----------------------------------------------------------------------
    // Delta-delta join signed PK ordering (item 39)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dt_wide_pk_multiset_delta() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Two delta rows sharing the same wide PK but different payloads (multiset
        // delta). One trace row for that PK. Inner join must produce 2 output rows.
        // The co-group hands the whole same-PK delta group to the callback at
        // once, producted against the once-walked trace group (no re-seek).
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let mut delta = Batch::with_schema(schema, 2);
        // Row 0: pk=(1,0,0) payload=10 w=+1
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &10i64.to_le_bytes());
        delta.count += 1;
        // Row 1: pk=(1,0,0) payload=20 w=+1 — same PK, different payload
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &20i64.to_le_bytes());
        delta.count += 1;
        delta.sorted = true;

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &schema, &schema);
        // 2 delta rows × 1 trace row = 2 output rows
        assert_eq!(out.count, 2, "multiset delta: expected 2 join outputs");
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(out.get_weight(1), 1);
    }

    // -----------------------------------------------------------------------
    // Wide-PK inner-join prefix-collision test
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dt_wide_pk_prefix_collision() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Two wide-PK rows sharing the same first 16 OPK bytes (columns 0 and 1
        // identical) but differing in column 2. The co-group must treat them
        // as distinct keys, not conflate via u128 prefix.
        // Row A: pk=(1,1,2), Row B: pk=(1,1,9).
        // Trace: Row A (+1). Delta: Row A (+1) and Row B (+1).
        // Large delta (2 rows) vs 1-row trace.
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 2, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let delta_cb = make_wide_batch(
            &schema,
            &[
                (1, 1, 2, 1, 200), // matches trace pk
                (1, 1, 9, 1, 300), // different pk — must NOT match
            ],
        );
        let delta = delta_cb.into_inner();

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &schema, &schema);
        // Only Row A matches (delta row 0 × trace row 0). Row B has no trace entry.
        assert_eq!(out.count, 1, "prefix-collision: only matching PK should produce output");
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 2).as_slice());
        assert_eq!(out.get_weight(0), 1);
    }

    // -----------------------------------------------------------------------
    // Wide-PK outer join multiset delta
    // -----------------------------------------------------------------------

    #[test]
    fn test_outer_join_dt_wide_pk_multiset_delta() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Same multiset delta setup as the inner-join test, but for outer join.
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let mut delta = Batch::with_schema(schema, 2);
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &10i64.to_le_bytes());
        delta.count += 1;
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &20i64.to_le_bytes());
        delta.count += 1;
        delta.sorted = true;

        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &schema, &schema);
        // Both delta rows match → 2 joined output rows, no null-fills.
        assert_eq!(out.count, 2, "outer multiset: expected 2 matched outputs");
        for i in 0..out.count {
            assert_eq!(out.get_weight(i), 1);
            // PK must be (1,0,0)
            assert_eq!(out.get_pk_bytes(i), wide_pk_bytes(&schema, 1, 0, 0).as_slice());
        }
    }

    // -----------------------------------------------------------------------
    // Wide-PK anti/semi-join delta-trace
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_wide_pk() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Delta: (1,1,2) and (1,1,9). Trace: (1,1,9) only.
        // Anti-join: emit delta rows whose PK is NOT in trace → only (1,1,2).
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 9, 1, 50)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let delta_cb = make_wide_batch(&schema, &[(1, 1, 2, 1, 10), (1, 1, 9, 1, 20)]);
        let delta = delta_cb.into_inner();
        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 2).as_slice());
    }

    #[test]
    fn test_semi_join_dt_wide_pk() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Delta: (1,1,2) and (1,1,9). Trace: (1,1,9) only.
        // Semi-join: emit delta rows whose PK IS in trace → only (1,1,9).
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 9, 1, 50)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let delta_cb = make_wide_batch(&schema, &[(1, 1, 2, 1, 10), (1, 1, 9, 1, 20)]);
        let delta = delta_cb.into_inner();
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 9).as_slice());
    }

    // -----------------------------------------------------------------------
    // Wide-PK semi-join (large delta) — prefix collision and negative weight
    // -----------------------------------------------------------------------

    #[test]
    fn test_semi_join_dt_wide_pk_prefix_collision() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Prefix-collision pair: same first 16 OPK bytes (c0=1,c1=1), differ
        // in c2. Large delta vs tiny trace exercises the galloping skip.
        // Trace has (1,1,0) only. Delta has (1,1,0) and (1,1,1<<56).
        // Only (1,1,0) should be semi-joined.
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 0, 1, 99)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        // Large c2 differs starting at byte 17 of the OPK key.
        let c2_large = 1u64 << 56;
        let delta_cb = make_wide_batch(
            &schema,
            &[
                (1, 1, 0, 1, 10),
                (1, 1, c2_large, 1, 20),
                (2, 0, 0, 1, 30), // extra row so the delta outnumbers the 1-row trace
            ],
        );
        let delta = delta_cb.into_inner();
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only (1,1,0) matches the trace.
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 0).as_slice());
    }

    #[test]
    fn test_semi_join_dt_wide_pk_negative_weight_trace() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Trace entry for a wide PK with weight <= 0 must be skipped (no emit).
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(5, 5, 5, -1, 99)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        // Large delta vs a 1-row trace.
        let delta_cb = make_wide_batch(&schema, &[(5, 5, 5, 1, 10), (6, 6, 6, 1, 20)]);
        let delta = delta_cb.into_inner();
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Negative-weight trace row must not produce any output.
        assert_eq!(out.count, 0, "negative-weight trace row must be skipped");
    }

    // -----------------------------------------------------------------------
    // Wide-PK delta-delta family
    // -----------------------------------------------------------------------

    fn make_schema_i32() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_i32_batch(schema: &SchemaDescriptor, rows: &[(i32, i64, i64)]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk((pk as u32) as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }
}
