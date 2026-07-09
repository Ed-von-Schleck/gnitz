//! Delta-trace inner join.
//!
//! Each delta row is matched against the integrated trace (`z⁻¹(I)`) via a
//! co-group on the join key. Output schema: `[left_PK, left_payload…, right_payload…]`.

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, ReadCursor};

use super::super::cogroup::cogroup_intersection;
use super::rowwrite::write_join_row;

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
    let consolidated: &Batch = cs.as_ref().unwrap_or(delta);
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
    // (left payloads interleave across trace entries) and is unfolded; `empty_joined`
    // leaves both flags clear, downstream re-sorts and consolidates.
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
    use crate::storage::{Batch, Layout};
    use crate::test_support::{make_wide_batch, wide_pk_3xu64_schema};

    /// Inner join delta×trace on a narrow (I32, 4-byte) signed PK. Every delta
    /// key has a trace match; all must be found, including the smallest key
    /// (regression guard for the byte-seek path at sub-8-byte stride).
    #[test]
    fn test_join_dt_i32_key_all_match() {
        use crate::storage::ReadCursor;
        use std::rc::Rc;

        let left_schema = make_schema_i32();
        let right_schema = make_schema_i32();
        // Trace (right): keys 1 and 2, both present.
        let trace = Rc::new(make_i32_batch(&right_schema, &[(1, 1, 100), (2, 1, 200)]));
        let mut ch = ReadCursor::from_owned(&[trace], right_schema);
        // Delta (left): keys 1 and 2.
        let delta = make_i32_batch(&left_schema, &[(1, 1, 10), (2, 1, 20)]);

        let out = op_join_delta_trace(&delta, &mut ch, &left_schema, &right_schema);
        assert_eq!(out.count, 2, "both I32 keys must join (smallest key not dropped)");
        assert_eq!(out.get_pk(0) as i32, 1);
        assert_eq!(out.get_pk(1) as i32, 2);
    }

    #[test]
    fn test_join_dt_many_to_many_output_unsorted() {
        use crate::storage::ReadCursor;
        use std::rc::Rc;
        // Many-to-many inner join: the unified co-group walks each trace group
        // once and products it against the whole delta group, emitting the
        // cartesian product in trace-major order — left payloads interleave out
        // of (PK, payload) order, so the output must be marked unsorted.
        let schema = make_schema_u64_i64();
        // Trace: PK=1 with two right payloads (100, 200).
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]));
        let mut ch = ReadCursor::from_owned(&[trace], schema);
        // Delta: PK=1 with three left payloads.
        let delta = make_batch(&schema, &[(1, 1, 10), (1, 1, 20), (1, 1, 30)]);

        let out = op_join_delta_trace(&delta, &mut ch, &schema, &schema);
        assert_eq!(out.count, 6, "3 left × 2 right = 6 join outputs");

        // col 0 = left payload, col 1 = right payload (all PKs equal). The sorted
        // flag may be true ONLY if the rows are actually in (left, right) order.
        let pairs: Vec<(i64, i64)> = (0..out.count)
            .map(|r| (read_i64_le(out.col_data(0), r * 8), read_i64_le(out.col_data(1), r * 8)))
            .collect();
        let actually_sorted = pairs.windows(2).all(|w| w[0] <= w[1]);
        assert!(
            !out.is_sorted() || actually_sorted,
            "many-to-many join marked output sorted but payload order is {pairs:?}",
        );
    }

    // -----------------------------------------------------------------------
    // Wide-PK inner-join multiset delta
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dt_wide_pk_multiset_delta() {
        use crate::storage::ReadCursor;
        use std::rc::Rc;
        // Two delta rows sharing the same wide PK but different payloads (multiset
        // delta). One trace row for that PK. Inner join must produce 2 output rows.
        // The co-group hands the whole same-PK delta group to the callback at
        // once, producted against the once-walked trace group (no re-seek).
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]));
        let mut ch = ReadCursor::from_owned(&[trace_batch], schema);

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
        delta.certify_layout(Layout::Sorted, &schema);

        let out = op_join_delta_trace(&delta, &mut ch, &schema, &schema);
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
        use crate::storage::ReadCursor;
        use std::rc::Rc;
        // Two wide-PK rows sharing the same first 16 OPK bytes (columns 0 and 1
        // identical) but differing in column 2. The co-group must treat them
        // as distinct keys, not conflate via u128 prefix.
        // Row A: pk=(1,1,2), Row B: pk=(1,1,9).
        // Trace: Row A (+1). Delta: Row A (+1) and Row B (+1).
        // Large delta (2 rows) vs 1-row trace.
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 2, 1, 100)]));
        let mut ch = ReadCursor::from_owned(&[trace_batch], schema);

        let delta = make_wide_batch(
            &schema,
            &[
                (1, 1, 2, 1, 200), // matches trace pk
                (1, 1, 9, 1, 300), // different pk — must NOT match
            ],
        );

        let out = op_join_delta_trace(&delta, &mut ch, &schema, &schema);
        // Only Row A matches (delta row 0 × trace row 0). Row B has no trace entry.
        assert_eq!(out.count, 1, "prefix-collision: only matching PK should produce output");
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 2).as_slice());
        assert_eq!(out.get_weight(0), 1);
    }

    fn make_schema_i32() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_i32_batch(schema: &SchemaDescriptor, rows: &[(i32, i64, i64)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk((pk as u32) as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.certify_layout(Layout::Consolidated, schema);
        b
    }
}
