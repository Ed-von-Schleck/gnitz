use super::*;
use crate::algebra::group_key::GroupOutKey;
use crate::algebra::{ground_owner, MapPlan, Placement};
use crate::repr::BatchBuilder;
use crate::schema::SchemaColumn;
use crate::test_support::{pk_payload_schema, self_typed_slots, u64_pk_schema, weighted_rows};
use gnitz_wire::{MapKind, NullKeys, TypeCode};

/// PK `(U32, I32, U64, U64)` — 24 bytes, so the whole PK routes by the wide
/// hash and every proper prefix by its narrow image — over
/// `[I64, I64 NULL, STRING NULL, U128, I16]` at columns 4..=8.
fn schema() -> SchemaDescriptor {
    use TypeCode::*;
    let cols = [U32, I32, U64, U64, I64, I64, String, U128, I16];
    let cols: Vec<SchemaColumn> = (0..)
        .zip(cols)
        .map(|(c, tc)| SchemaColumn::new(tc, c == 5 || c == 6))
        .collect();
    SchemaDescriptor::new(&cols, &[0, 1, 2, 3])
}

/// Rows past two packer chunks: every key column repeats values across rows,
/// both signs, a NULL beside a zero and beside an empty string, an inline and a
/// heap string, and weights of both signs and zero.
fn batch(schema: &SchemaDescriptor) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for i in 0..600u64 {
        let neg = |m: u64| (i % m) as i64 - (m / 2) as i64;
        b.begin_row_natives(
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

/// At every worker count of a cluster, `plan` sends each nonzero-weight row of
/// `b` to its `owner` and no weight-0 row anywhere, as one ascending list per
/// worker — a broadcast as one list — and hands each rank the rows of its list
/// as its share.
fn assert_routes(plan: &ScatterPlan, b: &Batch, what: &str, owner: impl Fn(&MemBatch, usize, usize) -> usize) {
    let mb = b.as_mem_batch();
    for nw in [1, 2, 4, 7] {
        let mut want = vec![Vec::new(); if plan.is_broadcast() { 1 } else { nw }];
        for row in (0..mb.len()).filter(|&r| mb.get_weight(r) != 0) {
            want[owner(&mb, row, nw)].push(row as u32);
        }
        // A pool left over from a wider round.
        let mut pool = vec![vec![7u32]; nw + 1];
        assert_eq!(
            plan.route(&b.as_mem_batch(), &mut pool, nw),
            &want[..],
            "{what}, {nw} workers"
        );
        for rank in 0..nw {
            assert_eq!(
                weighted_rows(&plan.share(b, Slot::new(rank as u32, nw as u32))),
                weighted_rows(&b.ascending_subset(&want[rank % want.len()])),
                "{what}: rank {rank} of {nw}'s share"
            );
        }
    }
}

/// A group scatter sends each row to the owner of the output PK the reduce keys
/// its group by: a PK range, one column's image, the packed key, or the
/// NULL-distinct fold — over no columns [`ground_owner`], the worker the
/// compiler lets seed a global aggregate's ground row.
#[test]
fn a_group_scatter_routes_each_row_to_its_output_pks_owner() {
    let schema = schema();
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
        let owner = |mb: &MemBatch, row, nw| {
            let owner = worker_for_pk_bytes(gk.out_pk(mb, row).bytes(), nw);
            assert!(!cols.is_empty() || owner == ground_owner(nw), "the ground row's owner");
            owner
        };
        assert_routes(&plan, &b, &format!("group {cols:?}"), owner);
        assert_routes(&plan, &empty, &format!("group {cols:?} of no rows"), owner);
    }
}

/// A relation's own placement routes each row where it lives: a PK prefix of
/// any byte width where those bytes hash — a narrow one by its image, at every
/// cell width, and a wide one by its digest — and a replicated relation's rows
/// to every worker.
#[test]
fn the_native_plan_routes_each_row_by_its_placement() {
    let schema = schema();
    let b = batch(&schema);
    let keyed = (1..=schema.pk_stride()).map(|n| Placement::Keyed { dist_stride: n as u8 });
    for placement in keyed.chain([Placement::Replicated]) {
        assert_routes(
            &ScatterPlan::native(placement),
            &b,
            &format!("{placement:?}"),
            |mb, row, nw| placement.owner(mb.get_pk_bytes(row), nw).unwrap_or(0),
        );
    }
}

/// A join scatter sends each row where the `_join_pk` the reindex Map stamps on
/// it hashes, so the scattered delta and the reindexed trace co-partition; it
/// runs the packer only for a key that is not the row's own OPK bytes: a
/// promoted slot, string content, or several columns that are not consecutive
/// PK columns.
#[test]
fn a_join_scatter_routes_each_row_to_its_reindexed_owner() {
    use TypeCode::{I64, U128};
    let schema = schema();
    let b = batch(&schema);
    let own = |cols: &[u32]| self_typed_slots(&schema, cols);
    for (slots, packed) in [
        (own(&[0]), false),
        (own(&[0, 1]), false),
        (own(&[0, 1, 2, 3]), false),
        (own(&[1]), false),
        (own(&[1, 2]), false),
        (own(&[2, 3]), false),
        (own(&[1, 2, 3]), false),
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
        assert_eq!(matches!(plan.0, Some(GroupKey::Packed(_))), packed, "{slots:?}");
        let reindex = MapKind::Reindex {
            keep: vec![],
            key: slots.clone(),
            nulls: NullKeys::Keep,
        };
        let mut map = MapPlan::from_wire(&schema, &reindex).expect("the fixture key packs");
        let (keyed, at) = (map.evaluate_map_batch(&b), Placement::full_pk(map.output_schema()));
        assert_routes(&plan, &b, &format!("join {slots:?}"), |_, row, nw| {
            at.owner(keyed.get_pk_bytes(row), nw).unwrap()
        });
    }
}

/// The route arrives off a client-pushed circuit row, so every shape the schema
/// cannot route is refused rather than panicking inside `locate` or the packer.
#[test]
fn a_key_the_schema_cannot_route_is_refused() {
    let schema = u64_pk_schema(&[SchemaColumn::new(TypeCode::F64, true)]);
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

/// The predicate accepts exactly the plans that hash the bytes the placement
/// places rows by, and each plan it accepts routes every row to its owner.
#[test]
fn a_plan_routes_natively_exactly_over_the_distribution_prefix() {
    let pk = [0u32, 1, 2, 3];
    let schema = schema();
    let b = batch(&schema);
    for (placement, dist) in [
        (Placement::keyed(&schema, 1), Some(&pk[..1])),
        (Placement::keyed(&schema, 2), Some(&pk[..2])),
        (Placement::keyed(&schema, 3), Some(&pk[..3])),
        (Placement::full_pk(&schema), Some(&pk[..])),
        (Placement::Replicated, None),
        (Placement::Local, None),
    ] {
        for cols in [&[][..], &[0], &[0, 1], &[0, 1, 2], &[0, 1, 2, 3], &[1], &[1, 0]] {
            let on_prefix = dist == Some(cols);
            for (kind, plan) in [
                ("group", ScatterPlan::group(&schema, cols)),
                ("join", ScatterPlan::join(&schema, &self_typed_slots(&schema, cols))),
            ] {
                let what = format!("{placement:?}, {kind} {cols:?}");
                let plan = plan.expect("the fixture key routes");
                assert_eq!(plan.routes_to_native_owner(placement), on_prefix, "{what}");
                if on_prefix {
                    assert_routes(&plan, &b, &what, |mb, row, nw| {
                        placement.owner(mb.get_pk_bytes(row), nw).unwrap()
                    });
                }
            }
        }
        // A widening promotion moves a slot off its source column's width.
        if let Some(dist) = dist {
            let mut promoted = self_typed_slots(&schema, dist);
            promoted[0].1 = TypeCode::U64;
            assert!(
                !ScatterPlan::join(&schema, &promoted).is_ok_and(|p| p.routes_to_native_owner(placement)),
                "{placement:?}: promoted"
            );
        }
    }
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
        let s = pk_payload_schema(&[tc]);
        assert!(
            ScatterPlan::join(&s, &[(0, slot)]).is_ok_and(|p| p.routes_to_native_owner(Placement::full_pk(&s))),
            "{tc:?} typed {slot:?}"
        );
    }
}

/// A `Local` relation's rows have no key owner, so no native plan exists.
#[test]
#[should_panic(expected = "a Local relation's rows have no key owner")]
fn the_native_plan_of_a_local_relation_panics() {
    ScatterPlan::native(Placement::Local);
}
