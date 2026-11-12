use super::super::encoding::Encoding;
use super::super::file::ShardWriteOpts;
use super::super::layout::{spans_of, ShardHeader};
use super::*;
use crate::repr::{merge_consolidated, BatchBuilder};
use crate::schema::SchemaColumn;
use crate::test_support::{
    arb_fold_case, assert_folds, fold_batch, make_batch, make_schema_pk_u64_payload_string, make_schema_u64_i64,
    make_string_batch, map_coded_shard, map_shard, prose, u64_pk_schema, FoldRow,
};
use gnitz_wire::{PkBuf, TypeCode, REG_NULL_BMP};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::rc::Rc;

/// Every `(guard_key, skeleton, batch)` a [`GuardMerge`] into `guards`
/// answers, in order.
fn route(shards: &[Rc<MappedShard>], guards: &[PkBuf], dehydrate: bool) -> Vec<(PkBuf, bool, Batch)> {
    let mut merge = GuardMerge::begin(shards.to_vec(), guards[1..].to_vec(), dehydrate).unwrap();
    (guards.iter())
        .filter_map(|&key| {
            let run = merge.next(None)?;
            let batch = run.plain().expect("a merge given no codebook is plain").clone();
            Some((key, run.skeleton, batch))
        })
        .collect()
}

/// The one run a merge of `shards` into one destination writes.
fn whole_run<'a>(shards: &[Rc<MappedShard>], dehydrate: bool, codebook: Option<&'a Codebook>) -> GuardRun<'a> {
    let mut merge = GuardMerge::begin(shards.to_vec(), Vec::new(), dehydrate).unwrap();
    merge.next(codebook).expect("rows survive")
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

/// A merge begins only over inputs whose bodies match their checksums.
#[test]
fn a_merge_refuses_a_corrupt_input() {
    let (dir, other) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64, i64)> = (1..=100).map(|i| (i, 1, i as i64)).collect();
    let intact = map_shard(&dir.path().join("s.db"), &make_batch(&schema, &rows));
    let mut image = std::fs::read(dir.path().join("s.db")).unwrap();
    *image.last_mut().unwrap() ^= 1;
    // The same basename, which the prefix digest is seeded with.
    let path = other.path().join("s.db");
    std::fs::write(&path, &image).unwrap();
    let corrupt = Rc::new(MappedShard::open(path.to_str().unwrap(), &schema).unwrap());
    assert!(GuardMerge::begin(vec![Rc::clone(&intact)], Vec::new(), false).is_ok());
    assert_eq!(
        GuardMerge::begin(vec![intact, corrupt], Vec::new(), false).err(),
        Some(StorageError::Corrupt("body checksum"))
    );
}

/// A fence below the last one would merge rows a second time.
#[test]
#[should_panic(expected = "fences ascend")]
fn a_descending_guard_key_panics() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64, i64)> = (1..=100).map(|i| (i, 1, i as i64)).collect();
    let shard = map_shard(&dir.path().join("s.db"), &make_batch(&schema, &rows));
    let keys = [60u64, 30].map(|k| PkBuf::from_bytes(&k.to_be_bytes())).to_vec();
    let _ = GuardMerge::begin(vec![shard], keys, false);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Each guard receives its [`guard_slot`] share of the inputs' Z-set sum —
    /// per-PK sums for a skeleton guard — and a share folding to nothing emits nothing.
    #[test]
    fn each_guard_receives_its_share_of_the_zset_sum(
        (s, rows) in arb_fold_case(),
        picks in prop::collection::vec(any::<prop::sample::Index>(), 0..5),
        below_all in any::<bool>(),
        dehydrate in any::<bool>(),
    ) {
        let runs: Vec<Batch> = rows
            .chunks(7)
            .map(|c| fold_batch(&s, c).into_consolidated())
            .filter(|b| !b.is_empty())
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let shards: Vec<Rc<MappedShard>> = runs
            .iter()
            .enumerate()
            .map(|(i, b)| map_shard(&dir.path().join(format!("{i}.db")), b))
            .collect();
        prop_assume!(!shards.is_empty());

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

        let mut emitted = route(&shards, &keys, skeleton).into_iter().peekable();
        for (g, &key) in keys.iter().enumerate() {
            let what = format!("guard {g} (skeleton {skeleton})");
            let want_skeleton: Vec<(Vec<u8>, i64)> = per_pk(g).into_iter().filter(|&(_, w)| w != 0).collect();
            let hydrated_want = fold_batch(&s, &share(g));
            let expect_any = if skeleton { !want_skeleton.is_empty() } else { !hydrated_want.clone().into_consolidated().is_empty() };
            let got = emitted.next_if(|(k, ..)| *k == key);
            prop_assert_eq!(got.is_some(), expect_any, "{}: emitted", what);
            let Some((_, got_skeleton, batch)) = got else { continue };
            prop_assert_eq!(got_skeleton, skeleton);
            if skeleton {
                prop_assert_eq!(*batch.schema(), s.pk_only());
                let rows: Vec<(Vec<u8>, i64)> =
                    (0..batch.len()).map(|r| (batch.get_pk_bytes(r).to_vec(), batch.get_weight(r))).collect();
                prop_assert_eq!(rows, want_skeleton, "{}", what);
            } else {
                assert_folds(&[hydrated_want], &batch, &what);
            }
        }
        prop_assert!(emitted.next().is_none(), "a guard emitted out of order");
    }
}

/// `(pk, weight, value)` of each row of a `make_schema_u64_i64` batch.
fn u64_i64_rows(b: &Batch) -> Vec<(u64, i64, i64)> {
    (0..b.len())
        .map(|r| {
            let pk = u64::from_be_bytes(b.get_pk_bytes(r).try_into().unwrap());
            let v = i64::from_le_bytes(b.get_col_ptr(r, 0, 8).try_into().unwrap());
            (pk, b.get_weight(r), v)
        })
        .collect()
}

/// Inputs that ascend without overlap, an empty window between them, fold to
/// what the merge of them is; a key one input ends at and the next begins at
/// still sums the weights of an element both hold; and a dehydrating fold
/// still folds a hydrated input's rows that share a PK.
#[test]
fn ascending_inputs_fold_as_their_merge() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let low = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (2, 2, 21), (3, 1, 30)]);
    let high = make_batch(&schema, &[(7, 1, 70), (8, 3, 80)]);
    let meeting = make_batch(&schema, &[(3, 1, 30), (3, 1, 31), (4, 1, 40)]);
    let shard = |name: &str, b: &Batch| map_shard(&dir.path().join(name), b);
    let (low_s, high_s, meeting_s) = (
        shard("low.db", &low),
        shard("high.db", &high),
        shard("meeting.db", &meeting),
    );
    let guards = [PkBuf::zeroed(8), PkBuf::from_bytes(&5u64.to_be_bytes())];
    let merged = |batches: &[&Batch]| {
        let mems: Vec<_> = batches.iter().map(|b| b.as_mem_batch()).collect();
        u64_i64_rows(&merge_consolidated(&mems, &schema))
    };
    let folded = |shards: &[Rc<MappedShard>]| -> Vec<(u64, i64, i64)> {
        route(shards, &guards, false)
            .iter()
            .flat_map(|(.., b)| u64_i64_rows(b))
            .collect()
    };

    // Below 5 only `low` holds rows; from 5 `top` precedes `high` but lies above it.
    let top = make_batch(&schema, &[(9, 1, 90)]);
    assert_eq!(
        folded(&[Rc::clone(&low_s), shard("top.db", &top), Rc::clone(&high_s)]),
        merged(&[&low, &top, &high])
    );
    let want = merged(&[&low, &meeting]);
    assert!(want.contains(&(3, 2, 30)), "premise: one element in both");
    assert_eq!(folded(&[Rc::clone(&low_s), meeting_s]), want);

    let coarse: Vec<Vec<(u64, i64)>> = route(&[low_s, high_s], &guards, true)
        .iter()
        .map(|(_, skel, b)| {
            assert!(*skel);
            (0..b.len())
                .map(|r| {
                    (
                        u64::from_be_bytes(b.get_pk_bytes(r).try_into().unwrap()),
                        b.get_weight(r),
                    )
                })
                .collect()
        })
        .collect();
    assert_eq!(coarse, [vec![(1, 1), (2, 3), (3, 1)], vec![(7, 1), (8, 3)]]);
}

/// A skeleton output declares itself on disk — zero payload arity, the skeleton
/// flag, and a null region collapsed to one `Constant` element for the whole
/// file — and a skeleton input makes every output a skeleton, asked or not: its
/// rows carry no payload to write back full width.
#[test]
fn a_skeleton_input_makes_every_output_a_skeleton() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let hydrated = make_batch(&schema, &[(1, 1, 10), (9, 1, 90)]);
    let [(_, true, coarse)] = &route(
        &[map_shard(&dir.path().join("a.db"), &hydrated)],
        &[PkBuf::zeroed(8)],
        true,
    )[..] else {
        panic!("one skeleton output");
    };
    // Written PK-only, read back under the relation's schema as a store does.
    let path = dir.path().join("b.db");
    coarse
        .write_shard_over(coarse, path.to_str().unwrap(), ShardWriteOpts::default(), true)
        .unwrap();
    let raw = std::fs::read(&path).unwrap();
    let header = ShardHeader::read(&raw).unwrap();
    assert_eq!((header.file_npc, header.skeleton), (0, true));
    let nulls = &spans_of(&raw)[REG_NULL_BMP];
    assert_eq!((nulls.size, nulls.encoding), (8, Encoding::Constant));
    let skeleton = Rc::new(MappedShard::open(path.to_str().unwrap(), &schema).unwrap());
    let fresh = map_shard(
        &dir.path().join("c.db"),
        &make_batch(&schema, &[(1, 2, 11), (5, 1, 50)]),
    );

    let guards = [PkBuf::zeroed(8), PkBuf::from_bytes(&5u64.to_be_bytes())];
    let out = route(&[skeleton, fresh], &guards, false);
    let rows: Vec<(bool, Vec<(u64, i64)>)> = out
        .iter()
        .map(|(_, skel, b)| {
            assert_eq!(*b.schema(), schema.pk_only());
            let rows = (0..b.len())
                .map(|r| (gnitz_wire::widen_pk_be(b.get_pk_bytes(r)) as u64, b.get_weight(r)))
                .collect();
            (*skel, rows)
        })
        .collect();
    assert_eq!(rows, [(true, vec![(1, 3)]), (true, vec![(5, 1), (9, 1)])]);
}

/// Each row of `src` as `(pk, weight, value)`, its weight `weight`'s.
fn string_rows(src: &impl RowSource, weight: impl Fn(usize) -> i64) -> Vec<(u64, i64, Vec<u8>)> {
    (0..src.row_count())
        .map(|r| {
            let pk = u64::from_be_bytes(src.get_pk_bytes(r).try_into().unwrap());
            (pk, weight(r), gnitz_wire::payload_bytes(src, r, 0).into_owned())
        })
        .collect()
}

/// The bytes `src`'s heap stores for row `row`'s long value.
fn stored(src: &impl RowSource, row: usize) -> Vec<u8> {
    let cell = src.german_cell(row, 0);
    gnitz_wire::german_string_value(cell, src.heap())
        .unwrap()
        .stored()
        .to_vec()
}

/// A merge given a codebook writes its run coded under it: a value read from a
/// shard of that table keeps its codes, one of another table's or of a plain
/// shard is coded, and a retraction cancels the row it retracts, coded under one
/// table or under two — the rows are the plain merge's, weights and all.
#[test]
fn a_coded_merge_keeps_the_codes_of_its_own_table() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    let values: Vec<String> = (0..1000).map(prose).collect();
    let rows = |pks: std::ops::RangeInclusive<u64>, w: i64| -> Batch {
        let rows: Vec<(u64, i64, &[u8])> = pks.map(|pk| (pk, w, values[pk as usize].as_bytes())).collect();
        make_string_batch(&rows)
    };
    let own = rows(1..=300, 1);
    let book = Codebook::for_batch(None, &own).unwrap();
    let theirs = rows(301..=600, 1);
    let other = Codebook::for_batch(None, &theirs).unwrap();
    assert_ne!(other.table().digest(), book.table().digest(), "premise: two tables");
    // Retractions of `own`'s first rows under its table, of `theirs`' under
    // none, and rows of no table yet.
    let (gone, fresh) = (rows(1..=50, -1), rows(601..=900, 1));
    let late = merge_consolidated(&[gone.as_mem_batch(), fresh.as_mem_batch()], &schema);
    let retracted = rows(301..=320, -1);
    let shards = [
        map_coded_shard(&dir.path().join("own.db"), &own, &book),
        map_coded_shard(&dir.path().join("theirs.db"), &theirs, &other),
        map_coded_shard(&dir.path().join("late.db"), &late, &book),
        map_shard(&dir.path().join("retracted.db"), &retracted),
    ];

    let plain = whole_run(&shards, false, None);
    let coded = whole_run(&shards, false, Some(&book));
    let plain = plain.plain().unwrap();
    let want = string_rows(plain, |r| plain.get_weight(r));
    assert_eq!(want.len(), 250 + 280 + 300);
    let cancelled = |pk: u64| pk <= 50 || (301..=320).contains(&pk);
    assert!(
        want.iter().all(|&(pk, w, _)| !cancelled(pk) && w == 1),
        "the retractions cancelled"
    );
    assert_eq!(string_rows(&coded, |r| coded.batch.get_weight(r)), want);

    // Run rows 0..250 are `own`'s rows 50..300 and keep their codes; run row 250
    // is `theirs`' row 20, coded anew under the merge's table.
    for (run_row, own_row) in (0..250).zip(50..300) {
        assert_eq!(
            stored(&coded, run_row),
            stored(&*shards[0], own_row),
            "own row {own_row}"
        );
    }
    let mut recoded = Vec::new();
    book.encode_into(values[321].as_bytes(), &mut recoded);
    assert_eq!(stored(&coded, 250), recoded);
    assert_ne!(
        stored(&*shards[1], 20),
        recoded,
        "premise: the other table codes it otherwise"
    );
}

/// Windows that share no key and ascend, in whatever order the shards come, are
/// the run in key order, each guard its own share: the rows and long values the
/// merge path writes, a coded shard's values decoded into the plain run.
#[test]
fn ascending_windows_are_the_merge_of_their_rows() {
    let dir = tempfile::tempdir().unwrap();
    let values: Vec<String> = (0..1000).map(prose).collect();
    let rows = |pks: std::ops::RangeInclusive<u64>, w: i64| -> Batch {
        let rows: Vec<(u64, i64, &[u8])> = pks.map(|pk| (pk, w, values[pk as usize].as_bytes())).collect();
        make_string_batch(&rows)
    };
    let (low, mid, high) = (rows(1..=300, 1), rows(301..=600, 2), rows(601..=900, 1));
    let book = Codebook::for_batch(None, &low).unwrap();
    let shards = [
        map_shard(&dir.path().join("high.db"), &high),
        map_coded_shard(&dir.path().join("low.db"), &low, &book),
        map_shard(&dir.path().join("mid.db"), &mid),
    ];
    let weight = |pk: u64| if (301..=600).contains(&pk) { 2 } else { 1 };
    let want = |pks: std::ops::RangeInclusive<u64>| -> Vec<(u64, i64, Vec<u8>)> {
        pks.map(|pk| (pk, weight(pk), values[pk as usize].as_bytes().to_vec()))
            .collect()
    };

    let fences = vec![PkBuf::from_bytes(&450u64.to_be_bytes())];
    let mut merge = GuardMerge::begin(shards.to_vec(), fences, false).unwrap();
    for pks in [1..=449, 450..=900] {
        let run = merge.next(None).unwrap();
        let batch = run.plain().unwrap();
        assert!(batch.is_consolidated());
        assert_eq!(
            string_rows(batch, |r| batch.get_weight(r)),
            want(pks.clone()),
            "{pks:?}"
        );
    }
    // The coded run of the same windows holds the same rows.
    let coded = whole_run(&shards, false, Some(&book));
    assert_eq!(string_rows(&coded, |r| coded.batch.get_weight(r)), want(1..=900));
}

/// The run [`GuardMerge::merged`] writes for the windows of the merge's next
/// destination, and the windows; the merge itself is left where it was.
fn merged_run<'a>(merge: &GuardMerge, codebook: Option<&'a Codebook>) -> (GuardRun<'a>, Vec<std::ops::Range<usize>>) {
    let windows = merge.windows();
    let batch = merge.merged(windows.clone(), codebook).expect("rows survive");
    let run = GuardRun::new(batch, &merge.out_schema, codebook, merge.skeleton);
    (run, windows)
}

/// A row region by region: the key, the weight, the null word, and per payload
/// column its cell, or a string's value and the bytes its heap stores for it.
type RunRow = (Vec<u8>, i64, u64, Vec<Vec<u8>>);

/// Each row of `run` as a [`RunRow`].
fn run_rows(run: &GuardRun<'_>) -> Vec<RunRow> {
    let schema = *run.batch.schema();
    (0..run.row_count())
        .map(|r| {
            let cells = (schema.payload_columns())
                .flat_map(|(pi, col)| match col.type_code.is_german_string() {
                    false => vec![run.get_col_ptr(r, pi, col.size()).to_vec()],
                    true => {
                        let cell = run.german_cell(r, pi);
                        let stored = gnitz_wire::german_string_value(cell, run.heap())
                            .map_or_else(Vec::new, |v| v.stored().to_vec());
                        vec![gnitz_wire::payload_bytes(run, r, pi).into_owned(), stored]
                    }
                })
                .collect();
            (
                run.get_pk_bytes(r).to_vec(),
                run.batch.get_weight(r),
                run.get_null_word(r),
                cells,
            )
        })
        .collect()
}

/// Windows that share no key and ascend are decoded into the run a merge of
/// them writes: under a codebook, every region and every value's stored bytes
/// — codes kept for a shard of the merge's table, coded anew for another's and
/// a plain one's — in a heap no larger; and as skeleton rows over skeleton
/// shards.
#[test]
fn ascending_windows_decode_to_the_run_their_merge_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (int, string) = (
        SchemaColumn::new(TypeCode::I64, false),
        SchemaColumn::new(TypeCode::String, false),
    );
    let schema = u64_pk_schema(&[int, string, SchemaColumn::new(TypeCode::I64, true), string]);
    let block = |pks: std::ops::Range<u64>, w: i64| -> Batch {
        let mut b = BatchBuilder::new(&schema);
        // Two rows per PK, apart only in a long string: their order is the
        // values' content order, which the coded run's codes do not keep.
        for pk in pks {
            for tail in ["", "~"] {
                b.begin_row(pk as u128, w);
                b.put_int(pk as u128 * 3);
                b.put_string(&(prose(pk as usize) + tail));
                b.put_opt_int((pk % 3 != 0).then_some(pk as u128));
                b.put_string(&prose(pk as usize % 17));
                b.end_row();
            }
        }
        b.finish()
    };
    let blocks = [
        block(0..200, 1),
        block(200..450, 2),
        block(450..700, -1),
        block(700..1000, 1),
    ];
    let book = Codebook::for_batch(None, &blocks[0]).unwrap();
    let other = Codebook::for_batch(None, &blocks[2]).unwrap();
    assert_ne!(other.table().digest(), book.table().digest(), "premise: two tables");
    let path = |name: &str| dir.path().join(name);
    let shards = vec![
        map_coded_shard(&path("c.db"), &blocks[2], &other),
        map_shard(&path("d.db"), &blocks[3]),
        map_coded_shard(&path("a.db"), &blocks[0], &book),
        map_coded_shard(&path("b.db"), &blocks[1], &book),
    ];
    let fences = [300u64, 320].map(|k| PkBuf::from_bytes(&k.to_be_bytes())).to_vec();
    let fold = |shards: &[Rc<MappedShard>], codebook: Option<&Codebook>| {
        let mut merge = GuardMerge::begin(shards.to_vec(), fences.clone(), false).unwrap();
        while !merge.done() {
            let (want, windows) = merged_run(&merge, codebook);
            assert!(merge.in_order(&windows).is_some(), "premise: the windows need no merge");
            let at = merge.next;
            let got = merge.next(codebook).unwrap();
            assert_eq!(got.batch.is_consolidated(), codebook.is_none(), "destination {at}");
            assert_eq!(run_rows(&got), run_rows(&want), "destination {at}");
            assert!(got.batch.blob().len() <= want.batch.blob().len(), "destination {at}");
            assert_eq!(got.coded.map(|c| c.1), want.coded.map(|c| c.1));
        }
    };
    fold(&shards, Some(&book));
    fold(&shards, None);

    // A skeleton fold sums no retraction.
    let skeletons: Vec<Rc<MappedShard>> = (0..)
        .zip(shards.iter().filter(|s| s.retraction_rows() == 0))
        .map(|(s, shard)| {
            let run = whole_run(std::slice::from_ref(shard), true, None);
            let path = path(&format!("skeleton_{s}.db"));
            let path = path.to_str().unwrap();
            (run.batch
                .write_shard_over(&run.batch, path, ShardWriteOpts::default(), true))
            .unwrap();
            Rc::new(MappedShard::open(path, &schema).unwrap())
        })
        .collect();
    let mut merge = GuardMerge::begin(skeletons, fences, false).unwrap();
    while !merge.done() {
        let (want, windows) = merged_run(&merge, None);
        assert!(merge.in_order(&windows).is_some(), "premise: the windows need no merge");
        let at = merge.next;
        let got = merge.next(Some(&book)).unwrap();
        assert!(got.skeleton && got.coded.is_none());
        assert_eq!(*got.batch.schema(), schema.pk_only());
        assert_eq!(run_rows(&got), run_rows(&want), "destination {at}");
    }
}
