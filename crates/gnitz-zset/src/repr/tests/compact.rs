use super::super::batch::REG_NULL_BMP;
use super::super::encoding::Encoding;
use super::super::layout::{spans_of, ShardHeader};
use super::super::shard_file::ShardWriteOpts;
use super::*;
use crate::test_support::{
    arb_fold_case, assert_folds, fold_batch, fold_schemas, make_batch, make_schema_u64_i64, map_shard, FoldRow,
};
use gnitz_wire::PkBuf;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::rc::Rc;

/// Every `(guard_key, skeleton, batch)` [`merge_and_route`] emits, in order.
fn route(
    shards: &[Rc<MappedShard>],
    guards: &[PkBuf],
    dehydrate: bool,
    schema: &SchemaDescriptor,
) -> Vec<(PkBuf, bool, Batch)> {
    let inputs: Vec<&MappedShard> = shards.iter().map(|s| &**s).collect();
    let mut out = Vec::new();
    merge_and_route(&inputs, guards, dehydrate, schema, &mut |gk, skeleton, batch| {
        out.push((gk, skeleton, batch));
        Ok(())
    })
    .unwrap();
    out
}

/// Guard routing is a saturating slot lookup: slot 0 owns every key below the
/// first guard key, and the last guard owns everything above it.
#[test]
fn guard_slot_saturates_at_both_ends() {
    let cases: &[(&[u64], u64, usize)] = &[
        (&[], 42, 0),
        (&[0], 0, 0),
        (&[0], 999, 0),
        (&[0, 100, 200], 50, 0),
        (&[0, 100, 200], 100, 1),
        (&[0, 100, 200], 150, 1),
        (&[0, 100, 200], 200, 2),
        (&[0, 100, 200], 999, 2),
        (&[200, 400], 50, 0),
    ];
    for &(guards, key, want) in cases {
        let keys: Vec<[u8; 8]> = guards.iter().map(|g| g.to_be_bytes()).collect();
        assert_eq!(
            guard_slot(&keys, &key.to_be_bytes(), |g| &g[..]),
            want,
            "guards={guards:?} key={key}"
        );
    }
}

/// An empty guard list would drop every survivor while the caller went on to
/// clear the source tier.
#[test]
#[should_panic(expected = "at least one guard")]
fn merge_and_route_rejects_empty_guards() {
    route(&[], &[], false, &make_schema_u64_i64());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Each guard receives its [`guard_slot`] share of the inputs' Z-set sum —
    /// per-PK sums for a skeleton guard — and a share folding to nothing emits nothing.
    #[test]
    fn each_guard_receives_its_share_of_the_zset_sum(
        (si, rows) in arb_fold_case(),
        picks in prop::collection::vec(any::<prop::sample::Index>(), 0..5),
        below_all in any::<bool>(),
        dehydrate in any::<bool>(),
    ) {
        let s = fold_schemas()[si];
        let runs: Vec<Batch> = rows
            .chunks(7)
            .map(|c| fold_batch(&s, c).into_consolidated())
            .filter(|b| b.count > 0)
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let shards: Vec<Rc<MappedShard>> = runs
            .iter()
            .enumerate()
            .map(|(i, b)| map_shard(&dir.path().join(format!("{i}.db")), b, ShardWriteOpts::default()))
            .collect();

        // Guard keys drawn from the rows' own keys, so boundaries land on live
        // PK groups; `below_all` adds one under every key, leaving guard 0 empty.
        let mut keys: Vec<PkBuf> = picks
            .iter()
            .filter(|_| !rows.is_empty())
            .map(|i| PkBuf::from_bytes(&i.get(&rows).0))
            .collect();
        if below_all || keys.is_empty() {
            keys.push(PkBuf::zeroed(s.pk_stride()));
        }
        keys.sort_unstable();
        keys.dedup();
        let owner = |pk: &[u8]| guard_slot(&keys, pk, PkBuf::pk_bytes);
        let share = |g: usize| -> Vec<FoldRow> { rows.iter().filter(|r| owner(&r.0) == g).cloned().collect() };
        let per_pk = |g: usize| -> BTreeMap<Vec<u8>, i64> {
            let mut m = BTreeMap::new();
            for (pk, w, ..) in share(g) {
                *m.entry(pk).or_insert(0) += w;
            }
            m
        };
        // A skeleton fold asserts its per-PK sums are never negative, which a
        // positive integral guarantees; these rows carry no such guarantee.
        let skeleton = dehydrate && (0..keys.len()).all(|g| per_pk(g).values().all(|&w| w >= 0));

        let mut emitted = route(&shards, &keys, skeleton, &s).into_iter().peekable();
        for (g, &key) in keys.iter().enumerate() {
            let what = format!("guard {g} (skeleton {skeleton})");
            let want_skeleton: Vec<(Vec<u8>, i64)> = per_pk(g).into_iter().filter(|&(_, w)| w != 0).collect();
            let hydrated_want = fold_batch(&s, &share(g));
            let expect_any = if skeleton { !want_skeleton.is_empty() } else { hydrated_want.clone().into_consolidated().count > 0 };
            let got = emitted.next_if(|(k, ..)| *k == key);
            prop_assert_eq!(got.is_some(), expect_any, "{}: emitted", what);
            let Some((_, got_skeleton, batch)) = got else { continue };
            prop_assert_eq!(got_skeleton, skeleton);
            if skeleton {
                prop_assert_eq!(*batch.schema(), s.pk_only());
                let rows: Vec<(Vec<u8>, i64)> =
                    (0..batch.count).map(|r| (batch.get_pk_bytes(r).to_vec(), batch.get_weight(r))).collect();
                prop_assert_eq!(rows, want_skeleton, "{}", what);
            } else {
                assert_folds(&[hydrated_want], &batch, &what);
            }
        }
        prop_assert!(emitted.next().is_none(), "a guard emitted out of order");
    }
}

/// A skeleton output declares itself: zero payload arity, the skeleton flag,
/// and a null region collapsed to one `Constant` element for the whole file.
#[test]
fn a_skeleton_output_is_payload_free_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let src = map_shard(
        &dir.path().join("in.db"),
        &make_batch(&schema, &[(1, 1, 10), (1, 2, 20), (2, 5, 30)]),
        ShardWriteOpts::default(),
    );
    let [(_, true, batch)] = &route(&[src], &[PkBuf::zeroed(8)], true, &schema)[..] else {
        panic!("one skeleton guard, one output");
    };
    let path = dir.path().join("out.db");
    batch
        .write_as_shard(
            path.to_str().unwrap(),
            ShardWriteOpts { skeleton: true, ..Default::default() },
        )
        .unwrap();
    let raw = std::fs::read(&path).unwrap();
    let header = ShardHeader::read(&raw).unwrap();
    assert_eq!(header.file_npc, 0, "zero payload arity");
    assert!(header.skeleton, "declared skeleton");
    let nulls = &spans_of(&raw)[REG_NULL_BMP];
    assert_eq!((nulls.size, nulls.encoding), (8, Encoding::Constant));
}

/// A skeleton input makes every output a skeleton, asked or not: its rows carry
/// no payload to write back full width.
#[test]
fn a_skeleton_input_makes_every_output_a_skeleton() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let hydrated = make_batch(&schema, &[(1, 1, 10), (9, 1, 90)]);
    let [(_, true, coarse)] = &route(
        &[map_shard(
            &dir.path().join("a.db"),
            &hydrated,
            ShardWriteOpts::default(),
        )],
        &[PkBuf::zeroed(8)],
        true,
        &schema,
    )[..] else {
        panic!("one skeleton output");
    };
    // Written PK-only, read back under the relation's schema as a store does.
    let path = dir.path().join("b.db");
    let opts = ShardWriteOpts { skeleton: true, ..Default::default() };
    coarse.write_as_shard(path.to_str().unwrap(), opts).unwrap();
    let skeleton = Rc::new(MappedShard::open(path.to_str().unwrap(), &schema).unwrap());
    let fresh = map_shard(
        &dir.path().join("c.db"),
        &make_batch(&schema, &[(1, 2, 11), (5, 1, 50)]),
        ShardWriteOpts::default(),
    );

    let guards = [PkBuf::zeroed(8), PkBuf::from_bytes(&5u64.to_be_bytes())];
    let out = route(&[skeleton, fresh], &guards, false, &schema);
    let rows: Vec<(bool, Vec<(u64, i64)>)> = out
        .iter()
        .map(|(_, skel, b)| {
            assert_eq!(*b.schema(), schema.pk_only());
            let rows = (0..b.count)
                .map(|r| {
                    (
                        u64::from_be_bytes(b.get_pk_bytes(r).try_into().unwrap()),
                        b.get_weight(r),
                    )
                })
                .collect();
            (*skel, rows)
        })
        .collect();
    assert_eq!(rows, [(true, vec![(1, 3)]), (true, vec![(5, 1), (9, 1)])]);
}

/// Compaction over packed inputs that all hold the same keys, so the merge breaks
/// every PK tie on the payload, at several source × guard counts; then with every
/// guard a skeleton.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_compaction_bench() {
    use crate::repr::BatchBuilder;
    use crate::test_support::{pk_u64_two_i64_schema, settled_rss};
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const TOTAL: usize = 1 << 20;
    let schema = pk_u64_two_i64_schema();
    let dir = tempfile::tempdir().unwrap();
    let (cycles, instructions) = (Counter::cycles().unwrap(), Counter::instructions().unwrap());
    for (sources, guards, skeleton) in [(4, 1, false), (32, 32, false), (64, 64, false), (32, 32, true)] {
        let per = TOTAL / sources;
        let inputs: Vec<Rc<MappedShard>> = (0..sources)
            .map(|s| {
                let mut b = BatchBuilder::new(&schema);
                for i in 0..per {
                    b.begin_row(i as u128, 1);
                    b.put_int(s as u128);
                    b.put_int(3 * i as u128);
                    b.end_row();
                }
                let name = format!("in_{sources}_{guards}_{skeleton}_{s}.db");
                map_shard(&dir.path().join(name), &b.finish(), ShardWriteOpts::default())
            })
            .collect();
        let guard_keys: Vec<PkBuf> = (0..guards)
            .map(|g| PkBuf::from_bytes(&((g * per / guards) as u64).to_be_bytes()))
            .collect();
        let inputs: Vec<&MappedShard> = inputs.iter().map(|s| &**s).collect();
        let rss0 = settled_rss();
        let (((), i), c) = cycles.measure(|| {
            instructions.measure(|| {
                merge_and_route(&inputs, &guard_keys, skeleton, &schema, &mut |_, _, batch| {
                    black_box(batch);
                    Ok(())
                })
                .unwrap()
            })
        });
        println!(
            "{sources} sources x {guards} guards{}: {i} instructions, {c} cycles; {} bytes retained",
            if skeleton { ", all skeleton" } else { "" },
            settled_rss().saturating_sub(rss0),
        );
    }
}
