//! Linear operators: filter, negate, union.
//!
//! The other two live with the batch mechanics they are: MAP is
//! `crate::expr::MapPlan::evaluate_map_batch`, null-extend
//! `Batch::widened_with_null_tail`.

use gnitz_expr::Evaluator;

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, Layout};

// ---------------------------------------------------------------------------
// Linear operators
// ---------------------------------------------------------------------------

/// Filter: retain rows where predicate returns true.
/// Uses contiguous-range bulk copy for efficiency.
pub(crate) fn op_filter(batch: &Batch, pred: &Evaluator, schema: &SchemaDescriptor) -> Batch {
    // The DAG pushes an empty placeholder every epoch, and `filter_ranges` takes
    // its scratch borrow and sizes it before the morsel loop.
    if batch.count == 0 {
        return Batch::empty_with_schema(schema);
    }

    // A per-call `Vec`: measured against a reused one it is a wash. `filter_ranges`
    // lends `out` so a *chunked* scan can carry one list; this caller has one batch.
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    pred.filter_ranges(&batch.as_mem_batch(), &mut ranges);
    Batch::from_ranges(batch, &ranges, schema)
}

/// Negate: flip the sign of every weight. `wrapping_neg` because `i64::MIN` must
/// not panic; element identity is untouched, so the layout claim carries over.
pub(crate) fn op_negate(mut batch: Batch) -> Batch {
    batch.map_weights(i64::wrapping_neg);
    batch
}

/// Union: algebraic addition of two Z-Set streams; sorted inputs take an O(N)
/// merge. `out_schema` is the UNION's own, not either input's — the merge
/// certifies `Sorted` under it, so a narrower comparator would leave a false
/// order claim for `into_consolidated` to trust.
pub(crate) fn op_union(batch_a: Batch, batch_b: &Batch, out_schema: &SchemaDescriptor) -> Batch {
    if batch_b.count == 0 {
        // O(1) pass-through: no allocation, sorted/consolidated preserved.
        gnitz_debug!("op_union: a={} b=0 identity", batch_a.count);
        return batch_a;
    }
    if batch_a.count == 0 {
        return batch_b.clone_batch();
    }

    if batch_a.sorted_verified(out_schema) && batch_b.sorted_verified(out_schema) {
        let mut output = batch_a.merged_sorted(batch_b, out_schema);
        // A payload-aware merge of two sorted inputs is genuinely
        // (PK, payload)-sorted, but unfolded (Z-Set `+` does not sum weights).
        output.certify_layout(Layout::Sorted, out_schema);
        gnitz_debug!(
            "op_union: a={} b={} out={} sorted_merge",
            batch_a.count,
            batch_b.count,
            output.count
        );
        return output;
    }

    // Unsorted: concatenate (the appends leave `output` `Raw`).
    let output = batch_a.concatenated(batch_b, out_schema);
    gnitz_debug!(
        "op_union: a={} b={} out={} concat",
        batch_a.count,
        batch_b.count,
        output.count
    );
    output
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        make_batch, make_batch_bytes, make_batch_i64pk, make_schema_i64pk_i64, make_schema_pk_u64_payload_string,
        make_schema_u64_i64, make_wide_batch, opk_pk, wide_pk_3xu64_schema,
    };

    fn get_payload_i64(b: &Batch, row: usize) -> i64 {
        gnitz_wire::read_i64_le(b.col_data(0), row * 8)
    }

    // -----------------------------------------------------------------------
    // Union merge sort-invariant tests
    // -----------------------------------------------------------------------

    /// The order oracle: every row of both sides exactly once, at its own weight,
    /// in (PK, payload) order. Compared unsorted, so it pins the emitted order.
    /// Covers disjoint keys, a shared key, a skewed gallop on each side, an
    /// all-equal PK, and each empty side.
    #[test]
    fn union_emits_every_row_both_sides() {
        let schema = make_schema_u64_i64();
        type Rows = &'static [(u64, i64, i64)];
        let cases: &[(Rows, Rows)] = &[
            (
                &[(1, 1, 10), (3, 1, 30), (5, 1, 50)],
                &[(2, 1, 20), (3, 1, 33), (4, 1, 40)],
            ),
            // tiny ∪ huge: the b-side gallop
            (&[(1, 1, 1)], &[(2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5), (6, 1, 6)]),
            // huge ∪ tiny: the a-side gallop
            (&[(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5)], &[(3, 1, 9)]),
            // all equal, multi-payload, mixed weights
            (&[(7, 1, 70), (7, 1, 71)], &[(7, -2, 72), (7, 1, 73)]),
            (&[(1, 1, 1), (2, 1, 2)], &[]),
            (&[], &[(1, 1, 1), (2, 1, 2)]),
        ];

        for (ai, bi) in cases {
            let out = op_union(make_batch(&schema, ai), &make_batch(&schema, bi), &schema);

            // (pk, payload, weight), in (PK, payload) order. A stable sort over
            // `a` then `b` breaks a (PK, payload) tie a-first, which is what the
            // merge's `!= Greater` pick does.
            let mut want: Vec<(u64, i64, i64)> = ai.iter().chain(bi.iter()).map(|&(pk, w, v)| (pk, v, w)).collect();
            want.sort_by_key(|&(pk, v, _)| (pk, v));

            let got: Vec<(u64, i64, i64)> = (0..out.count)
                .map(|r| (out.get_pk(r) as u64, get_payload_i64(&out, r), out.get_weight(r)))
                .collect();
            assert_eq!(got, want, "union a={ai:?} b={bi:?}");
        }
    }

    #[test]
    fn test_union_merge_same_pk_payload_order() {
        // batch_a has val=20, batch_b has val=10 — output must be [10, 20]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 10)]);
        let out = op_union(a, &b, &schema);
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
        let out = op_union(a, &b, &schema);

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
    fn test_union_merge_same_pk_equal_payload() {
        // Same (PK, payload), opposite weights — must be adjacent for consolidation
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10)]);
        let b = make_batch(&schema, &[(1, -1, 10)]);
        let out = op_union(a, &b, &schema);
        assert_eq!(out.count, 2);
        assert!(out.is_sorted());
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 10);
    }

    /// Pin: the GENERIC arm (`compare_rows`, German-string comparison) must drive
    /// the shared-PK payload interleave. The fixed-int comparator would read these
    /// 16-byte structs as raw integers and order "banana" before "apple".
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

        let a = make_batch_bytes(&schema, &[(1, 1, b"banana")]);
        let b = make_batch_bytes(&schema, &[(1, 1, b"apple")]);
        let out = op_union(a, &b, &schema);

        assert_eq!(out.count, 2, "Z-Set + keeps both shared-PK rows");
        assert!(out.is_sorted());
        // Both rows carry PK=1.
        assert_eq!(out.get_pk(0) as u64, 1);
        assert_eq!(out.get_pk(1) as u64, 1);
        // (PK, payload) order: German-string compare puts "apple" before "banana".
        assert_eq!(out.read_payload_string(0, 0), "apple", "row0 payload (string-sorted)");
        assert_eq!(out.read_payload_string(1, 0), "banana", "row1 payload (string-sorted)");
    }

    // -----------------------------------------------------------------------
    // Wide-PK union merge (pk_stride > 16)
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_union_merge_wide_pk() {
        // Regression: op_union on wide-PK (pk_stride=24) batches previously
        // panicked in get_pk -> widen_pk_le. Verify it merges correctly:
        // sorted by (PK, payload), all rows present, equal-PK groups
        // payload-sorted, weights not summed (union, not consolidation).
        let schema = wide_pk_3xu64_schema();
        let a = make_wide_batch(&schema, &[(0, 0, 1, 1, 20), (0, 0, 3, 1, 300)]);
        let b = make_wide_batch(&schema, &[(0, 0, 1, 1, 10), (0, 0, 2, 1, 200)]);

        let out = op_union(a, &b, &schema);
        assert_eq!(out.count, 4);
        assert!(out.is_sorted());
        assert!(!out.is_consolidated());

        let pk = |c: u128| opk_pk(&schema, &[0, 0, c]);
        assert_eq!(out.get_pk_bytes(0), &pk(1)[..]);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(out.get_pk_bytes(1), &pk(1)[..]);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert_eq!(out.get_pk_bytes(2), &pk(2)[..]);
        assert_eq!(get_payload_i64(&out, 2), 200);
        assert_eq!(out.get_pk_bytes(3), &pk(3)[..]);
        assert_eq!(get_payload_i64(&out, 3), 300);
    }

    #[test]
    fn test_op_union_empty_a_returns_b() {
        // batch_a empty, batch_b non-empty → result equals batch_b content.
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[]);
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let out = op_union(a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!(out.get_pk(0) as u64, 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(out.get_pk(1) as u64, 2);
        assert_eq!(get_payload_i64(&out, 1), 20);
    }

    #[test]
    fn test_op_union_empty_b_passthrough() {
        // batch_b empty → batch_a returned verbatim (sorted/consolidated kept).
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10)]);
        let out = op_union(a, &make_batch(&schema, &[]), &schema);
        assert_eq!(out.count, 1);
        assert!(out.is_sorted() && out.is_consolidated());
        assert_eq!(get_payload_i64(&out, 0), 10);
    }

    // -----------------------------------------------------------------------
    // op_union_merge signed I64 PK ordering
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_union_merge_signed_i64_pk() {
        // Signed PK columns are sign-flipped into the OPK bytes, so the merge's
        // plain byte comparison must yield signed ascending order — this pins
        // that the union merge reads the PK through that encoding.
        let schema = make_schema_i64pk_i64();
        let a = make_batch_i64pk(&schema, &[(-5, 1, 100), (0, 1, 200), (7, 1, 300)]);
        let b = make_batch_i64pk(&schema, &[(-1, 1, 400), (3, 1, 500)]);
        let out = op_union(a, &b, &schema);
        assert_eq!(out.count, 5);
        assert!(out.is_sorted());
        let pks: Vec<i64> = (0..out.count)
            .map(|i| crate::test_support::opk_pk_i64(out.get_pk_bytes(i)))
            .collect();
        assert_eq!(pks, vec![-5, -1, 0, 3, 7], "signed ascending PK order");
    }

    // -----------------------------------------------------------------------
    // op_filter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_filter_consolidated_flag() {
        use gnitz_expr::{LogicalInstr, LogicalProgram};

        let instrs = vec![
            LogicalInstr::LoadConst { dst: 0, val: 1 }, // always true
        ];
        let schema = make_schema_u64_i64();
        let func = LogicalProgram::new(instrs, 1, 0, vec![])
            .resolve_filter(&schema)
            .unwrap();

        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);

        let out = op_filter(&batch, &func, &schema);
        assert_eq!(out.count, 2);
        assert!(
            out.is_consolidated(),
            "consolidated input + pass-all → consolidated output"
        );
        assert!(out.is_sorted());
    }

    /// Per-row differential oracle for the batch filter path: 20 rows filtered by
    /// `col[1] > 10` must keep exactly the rows whose payload exceeds 10, in
    /// input PK order.
    #[test]
    fn test_filter_batch_matches_per_row() {
        use gnitz_expr::{CmpOp, LogicalInstr, LogicalProgram};

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
        let func = LogicalProgram::new(instrs, 3, 2, vec![])
            .resolve_filter(&schema)
            .unwrap();

        let out = op_filter(&batch, &func, &schema);
        // pk=2(15), 3(25), 5(20), 7(30), 9(11), 11(50), 13(12), 15(100), 18(13), 20(22)
        assert_eq!(out.count, 10, "expected 10 rows with val > 10");
        let pks: Vec<u64> = (0..out.count).map(|r| out.get_pk(r) as u64).collect();
        assert_eq!(pks, vec![2, 3, 5, 7, 9, 11, 13, 15, 18, 20]);
    }

    // -----------------------------------------------------------------------
    // op_negate tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_negate_weights() {
        let schema = make_schema_u64_i64();
        let out = op_negate(make_batch(&schema, &[(1, 3, 10), (2, -1, 20)]));
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), -3);
        assert_eq!(out.get_weight(1), 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert!(out.is_consolidated());
    }

    #[test]
    fn test_op_negate_i64_min_no_panic() {
        // -i64::MIN overflows; debug builds panic, release wraps silently.
        // wrapping_neg must leave i64::MIN unchanged without panicking.
        let schema = make_schema_u64_i64();
        let out = op_negate(make_batch(&schema, &[(1, i64::MIN, 10), (2, 5, 20)]));
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), i64::MIN, "wrapping_neg(i64::MIN) == i64::MIN");
        assert_eq!(out.get_weight(1), -5);
    }
}
