use super::*;
use crate::ops::group_key::GroupOutKey;
use crate::schema::{ground_owner, Placement, SchemaColumn, TypeCode, MAX_PK_BYTES};
use crate::storage::BatchBuilder;
use crate::test_support::{make_schema_u64_i64, pk_u64_two_i64_schema, self_typed_slots, weighted_rows};
use std::hint::black_box;

/// PK `(U32, I32, U64, U64)` — 24 bytes, so the whole PK routes by the wide
/// hash and every proper prefix by its narrow image — over
/// `[I64, I64 NULL, STRING NULL, U128, I16]` at columns 4..=8.
fn schema(placement: Placement) -> SchemaDescriptor {
    use TypeCode::*;
    let cols = [U32, I32, U64, U64, I64, I64, String, U128, I16];
    let cols: Vec<SchemaColumn> = (0..)
        .zip(cols)
        .map(|(c, tc)| SchemaColumn::new(tc, c == 5 || c == 6))
        .collect();
    SchemaDescriptor::new(&cols, &[0, 1, 2, 3]).with_placement(placement)
}

/// Rows past two packer chunks: every key column repeats values across rows,
/// both signs, a NULL beside a zero and beside an empty string, an inline and a
/// heap string, and weights of both signs and zero.
fn batch(schema: &SchemaDescriptor) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for i in 0..600u64 {
        let neg = |m: u64| (i % m) as i64 - (m / 2) as i64;
        b.begin_row_opk(
            &[(i % 5) as u128, neg(7) as u128, (i * 31 + 7) as u128, (i % 3) as u128],
            [1, 2, 0, -1][(i % 4) as usize],
        );
        b.put_int(neg(9) as u128);
        b.put_opt_int((i % 5 != 0).then(|| neg(3) as u128));
        match i % 4 {
            0 => b.put_null(),
            1 => b.put_string(""),
            2 => b.put_string("inline"),
            _ => b.put_string("a string past the inline length"),
        }
        b.put_int(u128::from(i % 7) << 100 | 1);
        b.put_int(neg(11) as u128);
        b.end_row();
    }
    b.finish()
}

/// `plan` sends each live row of `b` to its `owner` of `nw` workers and no
/// weight-0 row anywhere, as one ascending list per worker, and hands each rank
/// the same rows as its share.
fn assert_routes(plan: &ScatterPlan, b: &Batch, nw: usize, what: &str, owner: impl Fn(&MemBatch, usize) -> usize) {
    let mb = b.as_mem_batch();
    let mut want = vec![Vec::new(); nw];
    for row in (0..mb.count).filter(|&r| mb.get_weight(r) != 0) {
        want[owner(&mb, row)].push(row as u32);
    }
    // A pool left over from a wider round.
    let mut pool = vec![vec![7u32]; nw + 1];
    assert_eq!(plan.route(b, &mut pool, nw), &want[..], "{what}");
    for (rank, rows) in want.iter().enumerate() {
        assert_eq!(
            weighted_rows(&plan.share(b, Slot::new(rank as u32, nw as u32))),
            weighted_rows(&b.ascending_subset(rows)),
            "{what}: rank {rank}'s share"
        );
    }
}

const NW: usize = 4;

/// A group scatter sends each row to the owner of the output PK the reduce keys
/// its group by: a PK prefix, one column's image, or the NULL-distinct fold.
#[test]
fn a_group_scatter_routes_each_row_to_its_output_pks_owner() {
    let schema = schema(Placement::KEYED_DEFAULT);
    let (b, empty) = (batch(&schema), Batch::empty_with_schema(&schema));
    for cols in [
        &[][..],
        &[0],
        &[0, 1, 2, 3],
        &[1],
        &[4],
        &[7],
        &[5],
        &[6],
        &[0, 1],
        &[1, 0],
        &[3, 0, 1, 2],
        &[4, 0],
        &[5, 6, 8],
    ] {
        let (gk, _) = GroupOutKey::new(&schema, cols, []).expect("the fixture group keys");
        let plan = ScatterPlan::group(&schema, cols).expect("the fixture key routes");
        let owner = |mb: &MemBatch, row| worker_for_pk_bytes(gk.out_pk(mb, row).bytes(), NW);
        assert_routes(&plan, &b, NW, &format!("group {cols:?}"), owner);
        assert_routes(&plan, &empty, NW, &format!("group {cols:?} of no rows"), owner);
    }
}

/// A scatter keyed by no columns sends every row to [`ground_owner`] — the
/// worker the compiler lets seed a global aggregate's ground row.
#[test]
fn a_keyless_group_scatter_routes_every_row_to_the_ground_owner() {
    let schema = schema(Placement::KEYED_DEFAULT);
    let (b, plan) = (batch(&schema), ScatterPlan::group(&schema, &[]).unwrap());
    for nw in [1, 2, 3, 4, 7, 16, 64] {
        assert_routes(&plan, &b, nw, &format!("nw={nw}"), |_, _| ground_owner(nw));
    }
}

/// A join scatter sends each row where the `_join_pk` the reindex Map packs for
/// it hashes, and runs the packer only for a key that is not the row's own OPK
/// bytes: a promoted slot, string content, or several columns off the PK prefix.
#[test]
fn a_join_scatter_routes_each_row_to_its_packed_keys_owner() {
    use TypeCode::{I64, U128};
    let schema = schema(Placement::KEYED_DEFAULT);
    let b = batch(&schema);
    let own = |cols: &[u32]| self_typed_slots(&schema, cols);
    for (slots, packed) in [
        (own(&[0]), false),
        (own(&[0, 1]), false),
        (own(&[0, 1, 2, 3]), false),
        (own(&[1]), false),
        (own(&[4]), false),
        (own(&[5]), false),
        (own(&[7]), false),
        (own(&[1, 0]), true),
        (own(&[4, 7]), true),
        (vec![(1, I64)], true),
        (vec![(8, I64), (4, I64)], true),
        (own(&[6]), true),
        (vec![(5, I64), (6, U128)], true),
    ] {
        let plan = ScatterPlan::join(&schema, &slots).expect("the fixture key routes");
        assert_eq!(matches!(plan.0, Key::Packed(_)), packed, "{slots:?}");
        let packer = ReindexPacker::new(&schema, &slots).expect("the fixture key packs");
        assert_routes(&plan, &b, NW, &format!("join {slots:?}"), |mb, row| {
            worker_for_pk_bytes(packer.pack_prefix(&mut [0u8; MAX_PK_BYTES], mb, row), NW)
        });
    }
}

/// The worker filter keeps a rank's share by the whole PK, whatever prefix the
/// schema distributes by.
#[test]
fn the_worker_filter_keeps_the_whole_pk_share() {
    let schema = schema(Placement::Keyed { prefix_len: 1 });
    let b = batch(&schema);
    let whole_pk = ScatterPlan::group(&schema, &[0, 1, 2, 3]).unwrap();
    for nw in [1, NW as u32] {
        for rank in 0..nw {
            let slot = Slot::new(rank, nw);
            assert_eq!(
                weighted_rows(&op_worker_filter(&b, slot)),
                weighted_rows(&whole_pk.share(&b, slot)),
                "rank {rank} of {nw}"
            );
        }
    }
}

/// The route arrives off a client-pushed circuit row, so every shape the schema
/// cannot route is refused rather than panicking inside `locate` or the packer.
#[test]
fn a_key_the_schema_cannot_route_is_refused() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::F64, true),
        ],
        &[0],
    );
    for (plan, why) in [
        (
            ScatterPlan::group(&schema, &[7]),
            "group key: column 7 out of range (2 cols)",
        ),
        (
            ScatterPlan::group(&schema, &[1]),
            "group key: column 1 is a float, which has no order-preserving key image",
        ),
        (
            ScatterPlan::join(&schema, &[(7, TypeCode::I64)]),
            "reindex key: column 7 out of range (2 cols)",
        ),
        (
            ScatterPlan::join(&schema, &[(1, TypeCode::U128)]),
            "reindex key: column 1 is a float, which has no order-preserving key image",
        ),
        (
            ScatterPlan::join(&schema, &[(0, TypeCode::U32)]),
            "reindex key: column 0 of type U64 does not pack at U32",
        ),
    ] {
        assert_eq!(plan.err().as_deref(), Some(why));
    }
}

// ── routes_to_native_owner: the exchange-elision predicate ──────────────

/// The predicate accepts exactly a key that hashes the bytes `worker_for_pk`
/// placed the relation's rows by — a join key over the whole distribution
/// prefix, and a group key there only where its output PK is those bytes, one
/// column or the whole PK — and every plan it accepts does route each row to its
/// table's own worker.
#[test]
fn a_plan_routes_natively_exactly_over_the_distribution_prefix() {
    let pk = [0u32, 1, 2, 3];
    for (placement, dist) in [
        (Placement::Keyed { prefix_len: 1 }, Some(&pk[..1])),
        (Placement::Keyed { prefix_len: 2 }, Some(&pk[..2])),
        (Placement::Keyed { prefix_len: 3 }, Some(&pk[..3])),
        (Placement::KEYED_DEFAULT, Some(&pk[..])),
        (Placement::Replicated, None),
        (Placement::Local, None),
    ] {
        let schema = schema(placement);
        let b = batch(&schema);
        for cols in [&[][..], &[0], &[0, 1], &[0, 1, 2], &[0, 1, 2, 3], &[1], &[1, 0]] {
            let on_prefix = dist == Some(cols);
            let natural = cols.len() == 1 || cols.len() == pk.len();
            for (kind, plan, want) in [
                ("group", ScatterPlan::group(&schema, cols), on_prefix && natural),
                (
                    "join",
                    ScatterPlan::join(&schema, &self_typed_slots(&schema, cols)),
                    on_prefix,
                ),
            ] {
                let what = format!("{placement:?}, {kind} {cols:?}");
                let plan = plan.expect("the fixture key routes");
                assert_eq!(plan.routes_to_native_owner(&schema), want, "{what}");
                if want {
                    assert_routes(&plan, &b, NW, &what, |mb, row| {
                        schema.worker_for_pk(mb.get_pk_bytes(row), NW)
                    });
                }
            }
        }
        // A widening promotion moves a slot off its source column's width.
        if let Some(dist) = dist {
            let mut promoted = self_typed_slots(&schema, dist);
            promoted[0].1 = TypeCode::U64;
            assert!(
                !ScatterPlan::join(&schema, &promoted).is_ok_and(|p| p.routes_to_native_owner(&schema)),
                "{placement:?}: promoted"
            );
        }
    }
}

/// The refusal of a group key over a several-column proper prefix is about the
/// hash, not the width: the fold does land rows off their table's worker.
#[test]
fn a_proper_prefix_group_key_routes_off_the_tables_worker() {
    let schema = schema(Placement::Keyed { prefix_len: 2 });
    let b = batch(&schema);
    let mb = b.as_mem_batch();
    let (gk, _) = GroupOutKey::new(&schema, &[0, 1], []).unwrap();
    assert!(
        (0..mb.count).any(|row| worker_for_pk_bytes(gk.out_pk(&mb, row).bytes(), NW)
            != schema.worker_for_pk(mb.get_pk_bytes(row), NW)),
        "the fold must disagree with the prefix hash somewhere"
    );
}

/// A slot typed at the storage integer of a DATE, TIMESTAMP or DECIMAL PK
/// column packs that column's own OPK bytes, so it keeps the unpacked route.
#[test]
fn a_layout_identical_promotion_routes_natively() {
    for (tc, slot) in [
        (TypeCode::Date, TypeCode::I32),
        (TypeCode::Timestamp, TypeCode::I64),
        (TypeCode::Decimal, TypeCode::I64),
    ] {
        let s = SchemaDescriptor::new(
            &[SchemaColumn::new(tc, false), SchemaColumn::new(TypeCode::I64, false)],
            &[0],
        );
        assert!(
            ScatterPlan::join(&s, &[(0, slot)]).is_ok_and(|p| p.routes_to_native_owner(&s)),
            "{tc:?} typed {slot:?}"
        );
    }
}

/// `n` rows over a U64-PK, all-I64-payload `schema`, PKs `0..n`, each payload a
/// spread function of the PK.
fn bench_stripe(schema: &SchemaDescriptor, n: usize) -> Batch {
    let mut b = BatchBuilder::new(*schema);
    for pk in 0..n as u64 {
        b.begin_row(pk as u128, 1);
        for c in 1..=schema.num_payload_cols() as i64 {
            b.put_int((pk as i64).wrapping_mul(2_654_435_761 + c) as u128);
        }
        b.end_row();
    }
    b.finish()
}

/// Instructions per [`ScatterPlan::route`] call, and per row, for each routing
/// kernel: on small deltas, where the per-call fixed cost a round adds shows, and
/// at 1M rows.
///
/// `cd crates && cargo test -p gnitz-store --release exchange_route_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn exchange_route_bench() {
    let instructions = gnitz_foundation::perf::Counter::instructions().unwrap();
    let (one, two) = (make_schema_u64_i64(), pk_u64_two_i64_schema());
    for (name, schema, plan) in [
        ("pk", &one, ScatterPlan::group(&one, &[0])),
        ("image", &one, ScatterPlan::group(&one, &[1])),
        (
            "packed",
            &two,
            ScatterPlan::join(&two, &[(1, TypeCode::I64), (2, TypeCode::I64)]),
        ),
        ("fold", &two, ScatterPlan::group(&two, &[1, 2])),
    ] {
        let plan = plan.unwrap();
        for (n, workers, iters) in [(1, 4, 10_000), (64, 16, 10_000), (1024, 16, 10_000), (1_000_000, 4, 20)] {
            let batch = bench_stripe(schema, n);
            let mut pool = Vec::new();
            let routed: usize = plan.route(&batch, &mut pool, workers).iter().map(Vec::len).sum();
            assert_eq!(routed, n, "{name}: the route dropped rows");
            let ((), i) = instructions.measure(|| {
                for _ in 0..iters {
                    black_box(plan.route(&batch, &mut pool, workers));
                }
            });
            println!(
                "{name}: {n} rows -> {workers} workers: {} instructions per call, {:.1} per row",
                i / iters,
                i as f64 / (iters as usize * n) as f64
            );
        }
    }
}
