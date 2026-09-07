use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::payload_string;
use crate::test_support::{
    make_batch, make_batch_bytes, make_batch_opk, make_batch_raw, make_schema_pk_u64_payload_string,
    make_schema_u64_i64, opk_pk, pk_payload_schema,
};
use gnitz_wire::read_i64_le;

/// A union case row: an index into the shape's key list, a weight, and a payload.
type Rows = &'static [(usize, i64, i64)];

/// The PK-shape axis of the union merge: the schema's PK column types, and the
/// eight key values a case row's index selects. Only the last PK column varies —
/// the leading ones stay zero — so the key order is the index order at every
/// stride, and the signed shape puts negatives below zero.
const SHAPES: [(&[u8], [i128; 8]); 4] = [
    (&[type_code::U64], [0, 1, 2, 3, 4, 5, 6, 7]),
    (
        &[type_code::U64, type_code::U64, type_code::U64],
        [0, 1, 2, 3, 4, 5, 6, 7],
    ),
    (
        &[type_code::U64, type_code::U16, type_code::U8],
        [0, 1, 2, 3, 4, 5, 6, 7],
    ),
    (&[type_code::I64], [-4, -3, -2, -1, 0, 1, 2, 3]),
];

/// The OPK key a case row's index names: `keys[i]` in the schema's last PK
/// column, the earlier ones zero.
fn key_of(schema: &SchemaDescriptor, keys: &[i128; 8], i: usize) -> Vec<u8> {
    let n = schema.pk_columns().count();
    let mut vals = vec![0u128; n];
    vals[n - 1] = keys[i] as u128;
    opk_pk(schema, &vals)
}

/// A `Consolidated` batch over `schema` from case rows, which must already be in
/// (PK, payload) order.
fn batch(schema: &SchemaDescriptor, keys: &[i128; 8], rows: Rows) -> Batch {
    let pks: Vec<Vec<u8>> = rows.iter().map(|&(i, ..)| key_of(schema, keys, i)).collect();
    let opk: Vec<(&[u8], i64, i64)> = pks.iter().zip(rows).map(|(k, &(_, w, v))| (&k[..], w, v)).collect();
    let mut b = make_batch_opk(schema, &opk);
    b.certify_layout(Layout::Consolidated, schema);
    b
}

/// Union of two consolidated batches is Z-Set `+` with the fold: the two inputs'
/// multiset grouped by (OPK bytes, payload), each group's weights summed, and
/// net-zero groups dropped — in (PK, payload) order, at every PK stride. The
/// oracle below is that definition, so it pins the emitted order and the folded
/// weights, not just the contents.
#[test]
fn union_folds_both_sides_into_one_zset_in_pk_payload_order() {
    let cases: &[(Rows, Rows)] = &[
        // disjoint keys plus one shared
        (
            &[(1, 1, 10), (3, 1, 30), (5, 1, 50)],
            &[(2, 1, 20), (3, 1, 33), (4, 1, 40)],
        ),
        // tiny ∪ huge: the b-side gallop
        (&[(1, 1, 1)], &[(2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5), (6, 1, 6)]),
        // huge ∪ tiny: the a-side gallop
        (&[(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 1, 4), (5, 1, 5)], &[(3, 1, 9)]),
        // one key throughout, multiple payloads, mixed weights
        (&[(7, 1, 70), (7, 1, 71)], &[(7, -2, 72), (7, 1, 73)]),
        // skewed, with both sides' payloads interleaving at the shared key 5
        (
            &[(5, 1, 100), (5, 1, 300)],
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
        ),
        // a shared key whose payloads order each way round
        (&[(1, 1, 10)], &[(1, 1, 20)]),
        (&[(1, 1, 20)], &[(1, 1, 10)]),
        // equal (PK, payload) at opposite weights: the element cancels outright,
        // so the union emits nothing
        (&[(1, 1, 10)], &[(1, -1, 10)]),
        // a shared key where one payload folds to a non-zero weight and another
        // cancels, with a third payload on each side left alone
        (
            &[(2, 1, 10), (2, 3, 20), (2, 1, 30)],
            &[(2, 2, 10), (2, -3, 20), (2, 1, 40)],
        ),
        (&[(1, 1, 1), (2, 1, 2)], &[]),
        (&[], &[(1, 1, 1), (2, 1, 2)]),
    ];

    for (tcs, keys) in SHAPES {
        let schema = pk_payload_schema(tcs);
        for (ai, bi) in cases {
            let out = op_union(batch(&schema, &keys, ai), &batch(&schema, &keys, bi), &schema);

            // (pk bytes, payload) is the element identity; sum the weights of
            // each group and drop the ones that cancel.
            let mut want: Vec<(Vec<u8>, i64, i64)> = Vec::new();
            for &(i, w, v) in ai.iter().chain(bi.iter()) {
                let k = key_of(&schema, &keys, i);
                match want.iter_mut().find(|e| e.0 == k && e.1 == v) {
                    Some(e) => e.2 += w,
                    None => want.push((k, v, w)),
                }
            }
            want.retain(|e| e.2 != 0);
            want.sort_by(|x, y| (&x.0, x.1).cmp(&(&y.0, y.1)));
            let got: Vec<(Vec<u8>, i64, i64)> = (0..out.count)
                .map(|r| {
                    (
                        out.get_pk_bytes(r).to_vec(),
                        read_i64_le(out.col_data(0), r * 8),
                        out.get_weight(r),
                    )
                })
                .collect();

            assert_eq!(got, want, "{tcs:?}: a={ai:?} b={bi:?}");
            // Consolidated in, consolidated out — the merge folds, so nothing
            // downstream has to.
            assert!(out.is_consolidated(), "{tcs:?}: a={ai:?} b={bi:?}");
        }
    }
}

/// The shared-PK payload interleave must run through the GENERIC arm
/// (`compare_rows`, German-string comparison). The fixed-int comparator would
/// read these 16-byte cells as raw integers and order "banana" before "apple".
#[test]
fn union_orders_shared_pk_string_payloads_through_the_generic_comparator() {
    let schema = make_schema_pk_u64_payload_string();
    // If a future change moved STRING into the fixed-int fast path this would
    // stop exercising the generic arm — fail loudly here instead.
    assert_eq!(
        schema.payload_cmp,
        crate::schema::PayloadCmpKind::Generic,
        "U64+STRING must select the GENERIC payload comparator",
    );

    let out = op_union(
        make_batch_bytes(&schema, &[(1, 1, b"banana")]),
        &make_batch_bytes(&schema, &[(1, 1, b"apple")]),
        &schema,
    );

    assert_eq!(out.count, 2, "distinct payloads at one PK stay two elements");
    assert!(out.is_consolidated());
    assert_eq!(out.get_pk(0) as u64, 1);
    assert_eq!(out.get_pk(1) as u64, 1);
    assert_eq!(payload_string(&out, 0, 0), "apple");
    assert_eq!(payload_string(&out, 1, 0), "banana");
}

/// Neither side consolidated: the merge is unavailable, so `op_union`
/// concatenates and leaves the result `Raw` for a downstream fold. This is the
/// shape `UNION ALL` over a join or reduce output takes — both emit `Raw`
/// batches.
#[test]
fn union_concatenates_unconsolidated_inputs_and_leaves_them_raw() {
    let schema = make_schema_u64_i64();
    let a = make_batch_raw(&schema, &[(3, 1, 30), (1, 1, 10)]);
    let b = make_batch_raw(&schema, &[(2, 1, 20)]);

    let out = op_union(a, &b, &schema);
    assert_eq!(out.layout(), Layout::Raw);
    let got: Vec<(u64, i64)> = (0..out.count)
        .map(|r| (out.get_pk(r) as u64, read_i64_le(out.col_data(0), r * 8)))
        .collect();
    assert_eq!(
        got,
        vec![(3, 30), (1, 10), (2, 20)],
        "a's rows then b's, order untouched"
    );
}

/// Per-row differential oracle for the filter's contiguous-range bulk copy:
/// `col[1] > 10` keeps exactly the rows whose payload exceeds 10, in input PK
/// order, with runs of length 1, 2 and 3 on both sides of the predicate. Filter
/// is linear, so a consolidated input stays consolidated.
#[test]
fn filter_keeps_exactly_the_matching_rows() {
    use gnitz_expr::{CmpOp, LogicalInstr, LogicalProgram, Reg};

    let schema = make_schema_u64_i64();
    let rows: &[(u64, i64, i64)] = &[
        (1, 1, 5),
        (2, 1, 15),
        (3, 1, 25),
        (4, 1, 10), // 10 fails: the predicate is strict
        (5, 1, 20),
        (6, 1, 3),
        (7, 1, 8),
        (8, 1, 30),
        (9, 1, 11),
        (10, 1, 12),
        (11, 1, 0),
    ];
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadConst { val: 10 },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
    ];
    let func = LogicalProgram::new(instrs, Vec::new(), Some(Reg(2)), vec![])
        .resolve_filter(&schema)
        .unwrap();

    let out = op_filter(&make_batch(&schema, rows), &func, &schema).expect("a selective filter copies");
    let got: Vec<u64> = (0..out.count).map(|r| out.get_pk(r) as u64).collect();
    let want: Vec<u64> = rows.iter().filter(|&&(_, _, v)| v > 10).map(|&(pk, ..)| pk).collect();
    assert_eq!(got, want);
    assert!(out.is_consolidated());

    // Every row passing is answered with `None`, so the caller hands its own
    // input through rather than paying a whole-batch copy for a no-op.
    let all_pass = LogicalProgram::new(
        vec![
            LogicalInstr::LoadColInt { col: 1 },
            LogicalInstr::LoadConst { val: -1 },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        ],
        Vec::new(),
        Some(Reg(2)),
        vec![],
    )
    .resolve_filter(&schema)
    .unwrap();
    assert!(op_filter(&make_batch(&schema, rows), &all_pass, &schema).is_none());
}

/// Negate is the Z-Set group inverse: every weight flips sign and nothing else
/// moves. `i64::MIN` is its own inverse in ℤ/2⁶⁴, so `wrapping_neg` leaves it
/// where it is instead of overflowing.
#[test]
fn negate_flips_every_weight() {
    let schema = make_schema_u64_i64();
    let out = op_negate(make_batch(&schema, &[(1, 3, 10), (2, -1, 20), (3, i64::MIN, 30)]));

    let got: Vec<(i64, i64)> = (0..out.count)
        .map(|r| (out.get_weight(r), read_i64_le(out.col_data(0), r * 8)))
        .collect();
    assert_eq!(got, vec![(-3, 10), (1, 20), (i64::MIN, 30)]);
    assert!(out.is_consolidated());
}

/// One side's `(pk, weight, payload)` row generator, indexed by row number.
type RowGen<'a> = &'a dyn Fn(usize) -> (u64, i64, i64);

/// Release-only microbench for `op_union`'s two-way merge, over the shapes its
/// three arms see. `shared_pk_interleave` and `shared_pk_fold` put every row in
/// an equal-PK group, which is the arm that folds; `alt1` alternates single rows
/// (what a set operation's uniform hashed `_set_pk` produces) and `runs4096` is
/// the `store_io` shape — long PK-disjoint runs. The last two stay entirely in
/// the galloping arms, which the fold does not touch, so they are the controls.
///
/// `cd crates && cargo test -p gnitz-store --release union_merge_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn union_merge_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    const N: usize = 500_000;
    const ITERS: usize = 20;
    const RUN: usize = 4096;
    let schema = make_schema_u64_i64();

    // Each side is built strictly (PK, payload)-ascending, so `make_batch`'s
    // `Consolidated` certification is honest and `op_union` takes its merge.
    let side = |f: RowGen| -> Batch { make_batch(&schema, &(0..N).map(f).collect::<Vec<_>>()) };

    let cases: [(&str, RowGen, RowGen); 4] = [
        // Every PK shared, 8 payloads a side, payloads interleaving one for one:
        // the fold arm's per-row loop with a side switch at every step.
        (
            "shared_pk_interleave",
            &|i| ((i / 8) as u64, 1, (2 * (i % 8)) as i64),
            &|i| ((i / 8) as u64, 1, (2 * (i % 8) + 1) as i64),
        ),
        // Every (PK, payload) shared: every step folds and appends one row.
        ("shared_pk_fold", &|i| ((i / 8) as u64, 1, (i % 8) as i64), &|i| {
            ((i / 8) as u64, 1, (i % 8) as i64)
        }),
        // Disjoint PKs alternating one for one: the galloping arms at run 1.
        ("alt1", &|i| (2 * i as u64, 1, i as i64), &|i| {
            (2 * i as u64 + 1, 1, i as i64)
        }),
        // Disjoint PKs in guard-sized blocks: the galloping arms at run 4096.
        (
            "runs4096",
            &|i| ((2 * (i / RUN) * RUN + i % RUN) as u64, 1, i as i64),
            &|i| ((2 * (i / RUN) * RUN + RUN + i % RUN) as u64, 1, i as i64),
        ),
    ];

    for (label, fa, fb) in cases {
        let (a, b) = (side(fa), side(fb));
        let t = Instant::now();
        let mut acc = 0usize;
        for _ in 0..ITERS {
            acc += black_box(op_union(a.clone_batch(), &b, &schema).count);
        }
        let secs = t.elapsed().as_secs_f64();
        println!(
            "union_merge/{label}: {:.1} Mrows/s ({} in-rows × {ITERS} iters in {secs:.3}s, out {})",
            (2 * N * ITERS) as f64 / secs / 1e6,
            2 * N,
            acc / ITERS,
        );
    }
}

// ── Derived output schemas ──────────────────────────────────────────────

/// `union_nullability_merge` ORs the two inputs' per-column nullability, so a
/// null-carrying side reclassifies the output from the null-blind
/// `FixedIntNonnull` fast comparator to the null-aware `Generic` one.
#[test]
fn union_merges_nullability_and_reclassifies_the_comparator() {
    use crate::schema::PayloadCmpKind;
    let nonnull = pk_payload_schema(&[type_code::U128]);
    let nullable = SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U128, 0),
            SchemaColumn::new(type_code::I64, 1),
        ],
        &[0],
    );

    // Both non-nullable stays on the fast path; either side nullable forces
    // `Generic`, and the OR is symmetric.
    for (a, b, want_nullable) in [(nonnull, nonnull, 0u8), (nonnull, nullable, 1), (nullable, nonnull, 1)] {
        let m = union_nullability_merge(&a, &b).expect("shared layout");
        assert_eq!(m.columns[1].nullable, want_nullable);
        assert_eq!(
            m.payload_cmp,
            if want_nullable == 1 {
                PayloadCmpKind::Generic
            } else {
                PayloadCmpKind::FixedIntNonnull
            }
        );
    }
}

/// A mismatched pair is the whole layout contract, and in release there is
/// nothing else: adopting `a`'s schema would let `op_union` read `b`'s bytes
/// through it.
#[test]
fn union_of_mismatched_input_layouts_is_rejected() {
    let a = pk_payload_schema(&[type_code::U128]);
    assert_eq!(
        union_nullability_merge(&a, &make_schema_u64_i64())
            .expect_err("mismatched layouts")
            .to_string(),
        "union: inputs do not share a physical layout"
    );
}

/// The null-extend output is the input schema plus one nullable column per
/// fill slot, so the bound is on the *merged* width — a list-length bound alone
/// would miss a near-max-width input taking a short extension over the limit.
#[test]
fn a_null_extend_overflowing_the_merged_schema_is_rejected() {
    const GUARD: &str = "null-extend: merged schema exceeds MAX_COLUMNS";
    let extend = |s: &SchemaDescriptor, n: usize| null_extend_output_schema(s, &vec![type_code::I64; n]);
    let narrow = make_schema_u64_i64();
    let out = extend(&narrow, 1).expect("a short type_codes list extends cleanly");
    assert_eq!(out.num_columns(), narrow.num_columns() + 1);
    assert_eq!(out.columns[out.num_columns() - 1].nullable, 1);
    // MAX_COLUMNS type_codes overflow the fixed schema array on their own.
    assert_eq!(
        extend(&narrow, crate::schema::MAX_COLUMNS)
            .expect_err("overflow")
            .to_string(),
        GUARD
    );
    // 64 + 2 > 65: the merged width, which a bound on the list length misses.
    let wide = {
        let mut cols = [SchemaColumn::new(type_code::I64, 0); 64];
        cols[0] = SchemaColumn::new(type_code::U64, 0);
        SchemaDescriptor::new(&cols, &[0])
    };
    assert_eq!(extend(&wide, 2).expect_err("overflow").to_string(), GUARD);
}
