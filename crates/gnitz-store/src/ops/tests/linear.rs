use super::*;
use crate::schema::type_code;
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

/// Union is Z-Set `+`: every row of both sides survives at its own weight, in
/// (PK, payload) order, with nothing folded. The oracle is the two inputs'
/// multiset sorted by (OPK bytes, payload) — a stable sort with `a` first, which
/// is the tie-break the merge's `!= Greater` pick makes — so it pins the emitted
/// order and not just the contents, at every PK stride.
#[test]
fn union_emits_every_row_of_both_sides_in_pk_payload_order() {
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
        // equal (PK, payload) at opposite weights: they must land adjacent, so a
        // later consolidation cancels them
        (&[(1, 1, 10)], &[(1, -1, 10)]),
        (&[(1, 1, 1), (2, 1, 2)], &[]),
        (&[], &[(1, 1, 1), (2, 1, 2)]),
    ];

    for (tcs, keys) in SHAPES {
        let schema = pk_payload_schema(tcs);
        for (ai, bi) in cases {
            let out = op_union(batch(&schema, &keys, ai), &batch(&schema, &keys, bi), &schema);

            let mut want: Vec<(Vec<u8>, i64, i64)> = ai
                .iter()
                .chain(bi.iter())
                .map(|&(i, w, v)| (key_of(&schema, &keys, i), v, w))
                .collect();
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
            assert!(out.is_sorted());
            // A merge of two non-empty sides is unfolded, so only the
            // pass-through of an empty side keeps the `Consolidated` claim.
            assert_eq!(
                out.is_consolidated(),
                ai.is_empty() || bi.is_empty(),
                "{tcs:?}: a={ai:?} b={bi:?}",
            );
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

    assert_eq!(out.count, 2, "Z-Set + keeps both shared-PK rows");
    assert!(out.is_sorted());
    assert_eq!(out.get_pk(0) as u64, 1);
    assert_eq!(out.get_pk(1) as u64, 1);
    assert_eq!(payload_string(&out, 0, 0), "apple");
    assert_eq!(payload_string(&out, 1, 0), "banana");
}

/// Neither side sorted: the merge is unavailable, so `op_union` concatenates and
/// leaves the result `Raw` for a downstream re-sort. This is the shape `UNION
/// ALL` over a join or reduce output takes — both emit `Raw` batches.
#[test]
fn union_concatenates_unsorted_inputs_and_leaves_them_raw() {
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

    let out = op_filter(&make_batch(&schema, rows), &func, &schema);
    let got: Vec<u64> = (0..out.count).map(|r| out.get_pk(r) as u64).collect();
    let want: Vec<u64> = rows.iter().filter(|&&(_, _, v)| v > 10).map(|&(pk, ..)| pk).collect();
    assert_eq!(got, want);
    assert!(out.is_consolidated() && out.is_sorted());
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
