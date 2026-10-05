use super::*;
use crate::repr::batch_pool::tls_pool::MAX_POOLED_BYTES;
use crate::repr::shard_file::ShardWriteOpts;
use crate::repr::{Batch, BatchBuilder, MappedShard};
use crate::schema::{SchemaColumn, TypeCode};
use crate::test_support::{
    arb_fold_case, assert_folds, create_read_cursor, fold_batch, fold_schemas, make_batch_u128,
    make_schema_pk_u64_payload_string, make_schema_u128_i64, make_schema_u64_i64, map_shard, payload0_i64, row_key,
    u64_pk_schema, weighted_rows, zset_of, RowKey,
};
use gnitz_wire::PkKeys;
use proptest::prelude::*;
use std::rc::Rc;

/// A consolidated `Rc<Batch>` over [`make_schema_u128_i64`]: `(pk, weight, payload)`
/// rows, (PK, payload)-sorted.
fn u128_run(rows: &[(u128, i64, i64)]) -> Rc<Batch> {
    Rc::new(make_batch_u128(&make_schema_u128_i64(), rows))
}

/// `(pk, weight, payload)` of every row a walk from here emits, over a
/// single-I64-payload schema of stride ≤ 16.
fn walk(c: &mut ReadCursor) -> Vec<(u128, i64, i64)> {
    let mut rows = Vec::new();
    while c.valid {
        let (src, row) = c.current_row_source();
        rows.push((c.current_key_narrow(), c.current_weight, payload0_i64(src, row)));
        c.advance();
    }
    rows
}

/// The cursor's current row, as its identity and net weight.
fn current(c: &ReadCursor) -> (RowKey, i64) {
    let mut one = Batch::with_capacity(&c.schema, 1);
    c.copy_current_row_into(&mut one, c.current_weight);
    weighted_rows(&one).remove(0)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Every read verb over a cursor is a view of one Z-set: the sum of its runs,
    /// in (PK, payload) order.
    #[test]
    fn every_read_verb_is_a_view_of_the_zset_sum(
        (si, rows) in arb_fold_case(),
        run_len in 1usize..12,
        as_shard in any::<u16>(),
        probes in prop::collection::vec((any::<prop::sample::Index>(), -1i8..=1), 0..8),
        chunk in 1usize..6,
    ) {
        let s = fold_schemas()[si];
        let stride = s.pk_stride();
        let dir = tempfile::tempdir().unwrap();
        let batches: Vec<Batch> = rows.chunks(run_len).map(|c| fold_batch(&s, c).into_consolidated()).collect();
        let runs: Vec<Run> = batches
            .iter()
            .enumerate()
            .map(|(i, b)| match as_shard >> (i % 16) & 1 == 1 && b.count > 0 {
                true => Run::Shard(map_shard(
                    &dir.path().join(format!("{i}.db")),
                    b)),
                false => Run::Mem(Rc::new(b.clone())),
            })
            .collect();
        let open = || from_runs(runs.iter().cloned(), s, runs.len());

        let mat = open().materialize();
        assert_folds(&batches, &mat, "materialize");
        let want = weighted_rows(&mat);
        let pk = |r: usize| mat.get_pk_bytes(r);
        let rows_where = |f: &dyn Fn(&[u8], i64) -> bool| -> Vec<(RowKey, i64)> {
            (0..mat.count).filter(|&r| f(pk(r), mat.get_weight(r))).map(|r| want[r].clone()).collect()
        };

        // The walk, with its size estimate bounding what is left of it.
        let mut c = open();
        let mut walked = Vec::new();
        while c.valid {
            prop_assert!(c.estimated_length() >= want.len() - walked.len());
            prop_assert!(!c.current_is_skeleton());
            walked.push(current(&c));
            c.advance();
        }
        prop_assert_eq!(c.estimated_length(), 0);
        prop_assert_eq!(&walked, &want);

        let mut c = open();
        let mut drained = Vec::new();
        while let Some(b) = c.drain_chunk(chunk) {
            prop_assert!(0 < b.count && b.count <= chunk && b.is_consolidated());
            drained.extend(weighted_rows(&b));
        }
        prop_assert_eq!(&drained, &want);

        // Element weights, probed with each run's own rows — an element one run
        // holds may have folded away in the sum.
        let sum = zset_of(&mat, &s);
        for b in &batches {
            let mut got = Vec::new();
            open().for_each_mem_row_weight(&b.as_mem_batch(), |i, w| got.push((i, w)));
            let want_w: Vec<(usize, i64)> =
                (0..b.count).map(|i| (i, sum.get(&row_key(b, &s, i)).copied().unwrap_or(0))).collect();
            prop_assert_eq!(got, want_w);
        }

        let mut keys: Vec<Vec<u8>> = vec![vec![0; stride], vec![0xff; stride]];
        for (i, d) in probes.iter().filter(|_| !rows.is_empty()) {
            let mut k = i.get(&rows).0.clone();
            k[stride - 1] = k[stride - 1].wrapping_add_signed(*d);
            keys.push(k);
        }

        // Point seeks, absolute and galloping from wherever the last probe left
        // the cursor — forward, equal or backward.
        let mut adv = open();
        for k in &keys {
            let lower = (0..mat.count).find(|&r| pk(r) >= &k[..]);
            let mut fresh = open();
            fresh.seek_bytes(k);
            adv.advance_to(k);
            for c in [&fresh, &adv] {
                prop_assert_eq!(c.valid, lower.is_some());
                if let Some(r) = lower {
                    prop_assert_eq!(current(c), want[r].clone());
                }
            }
            let mut group = Vec::new();
            fresh.for_each_pk_group_row(k, |c| group.push(current(c)));
            prop_assert_eq!(group, rows_where(&|p, _| p == &k[..]));

            for n in [1, stride] {
                let want = rows_where(&|p, w| p[..n] == k[..n] && w > 0);
                let mut positive = Vec::new();
                open().for_each_positive_with_prefix(&k[..n], |c| positive.push(current(c)));
                prop_assert_eq!(&positive, &want);
            }
        }

        // Range seeks: exactly the window, and an estimate between the live rows
        // and the raw entries it spans.
        for w in keys.windows(2) {
            let (lo, hi) = if w[0] <= w[1] { (&w[0], &w[1]) } else { (&w[1], &w[0]) };
            let in_range = |p: &[u8]| &lo[..] <= p && p < &hi[..];
            let mut c = open();
            c.seek_range_bytes(lo, Some(hi));
            let estimate = c.estimated_length();
            let got = weighted_rows(&c.materialize());
            let raw: usize = batches.iter().map(|b| (0..b.count).filter(|&r| in_range(b.get_pk_bytes(r))).count()).sum();
            prop_assert!(got.len() <= estimate && estimate <= raw);
            prop_assert_eq!(got, rows_where(&|p, _| in_range(p)));
        }

        keys.sort();
        keys.dedup();

        // A capped gather takes each key's positive rows up to the cap, whole
        // PKs and leading bytes of one alike.
        for n in [1, stride] {
            let mut prefixes: Vec<&[u8]> = keys.iter().map(|k| &k[..n]).collect();
            prefixes.dedup();
            for max in [0, 1, 3] {
                let want: Vec<_> = prefixes
                    .iter()
                    .flat_map(|prefix| rows_where(&|p, w| &p[..n] == *prefix && w > 0).into_iter().take(max))
                    .collect();
                let keys = PkKeys::from_sorted(n, prefixes.concat());
                let mut capped = Vec::new();
                PkSetGather::over_runs(runs.iter().cloned(), s, runs.len(), keys)
                    .for_each_positive_capped(max, |c| capped.push(current(c)));
                prop_assert_eq!(capped, want);
            }
        }

        // A key-set gather, drained across chunk boundaries.
        let mut gather = PkSetGather::new(open(), PkKeys::from_sorted(stride, keys.concat()));
        let mut gathered = Vec::new();
        while let Some(b) = gather.drain_chunk(chunk) {
            prop_assert!(b.count > 0);
            gathered.extend(weighted_rows(&b));
        }
        prop_assert_eq!(gathered, rows_where(&|p, _| keys.iter().any(|k| &k[..] == p)));
    }
}

/// The merge mode follows the live source set: a seek narrows it, and `rewind`
/// or a backward `advance_to` re-livens a source a range seek emptied.
#[test]
fn mode_follows_the_live_source_set() {
    let schema = make_schema_u128_i64();
    // Source `s` holds PKs `s·100 + 1 ..= s·100 + 40`, payload `pk · 10`.
    let b: Vec<Rc<Batch>> = (0..4u128)
        .map(|s| {
            u128_run(
                &(1..=40)
                    .map(|i| (s * 100 + i, 1, (s * 100 + i) as i64 * 10))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    let rows = |lo: u128, hi: u128| -> Vec<(u128, i64, i64)> { (lo..=hi).map(|pk| (pk, 1, pk as i64 * 10)).collect() };
    let opk = |pk: u128| pk.to_be_bytes();

    let mut c = create_read_cursor(&b, &[], schema);
    assert!(c.mode.is_none());
    c.seek_range_bytes(&opk(301), Some(&opk(311)));
    assert_eq!(c.mode, Some(3), "range inside source 3");
    assert_eq!(c.sources.len(), 4, "no source is destroyed");
    assert_eq!(walk(&mut c), rows(301, 310));

    // A backward `advance_to` brings back every source up to its clamped count.
    c.advance_to(&opk(101));
    assert!(c.mode.is_none(), "sources 1 and 2 are live again");
    assert_eq!(walk(&mut c), [rows(101, 140), rows(201, 240), rows(301, 310)].concat());

    // A window spanning exactly two sources still merges through the tree.
    let mut c = create_read_cursor(&b, &[], schema);
    c.seek_range_bytes(&opk(220), Some(&opk(320)));
    assert!(c.mode.is_none());
    assert_eq!(walk(&mut c), [rows(220, 240), rows(301, 319)].concat());

    // The unbounded seek reaches the same collapse, and `rewind` re-livens all.
    let mut c = create_read_cursor(&b, &[], schema);
    c.seek_bytes(&opk(301));
    assert_eq!(c.mode, Some(3));
    assert_eq!(walk(&mut c), rows(301, 340));
    c.rewind();
    assert!(c.mode.is_none(), "rewind re-livens every source");
    assert_eq!(walk(&mut c).len(), 4 * 40);

    // A window covering nothing: no rows, no drain, no panic.
    let mut c = create_read_cursor(&b, &[], schema);
    c.seek_range_bytes(&opk(41), Some(&opk(51)));
    assert!(c.mode.is_none());
    assert!(!c.valid, "no live source, so the merge drives to invalid");
    assert!(c.drain_chunk(usize::MAX).is_none());
}

/// A consolidated U64 PK / STRING run of `pk → "{pk:0>len}"` over the
/// ascending `pks` at `weight`.
fn string_run(pks: impl Iterator<Item = u64>, len: usize, weight: i64) -> Batch {
    let mut bb = BatchBuilder::new(&make_schema_pk_u64_payload_string());
    for pk in pks {
        bb.begin_row(pk as u128, weight);
        bb.put_string(&format!("{pk:0>len$}"));
        bb.end_row();
    }
    bb.finish().into_consolidated()
}

/// A drain whose inputs nearly all cancel reserves heap for the survivors alone.
#[test]
fn materialize_reserves_for_the_survivors() {
    // Same keys and payloads at opposite weights, but for key 0.
    let inserts = Rc::new(string_run(0..4096, 512, 1));
    let retracts = Rc::new(string_run(1..4096, 512, -1));
    let heap = inserts.blob.len() + retracts.blob.len();
    assert!(heap > MAX_POOLED_BYTES, "must exceed MAX_POOLED_BYTES: {heap}");

    let batch = create_read_cursor(&[inserts, retracts], &[], make_schema_pk_u64_payload_string()).materialize();
    assert_eq!(batch.count, 1, "all but the first key cancels");
    assert_eq!(batch.blob.len(), 512, "one surviving string");
    assert!(
        batch.blob.capacity() <= MAX_POOLED_BYTES,
        "the reservation must not cover the cancelled rows (MAX_POOLED_BYTES): {}",
        batch.blob.capacity(),
    );
}

/// Each chunk of a drain reserves heap for its own rows only.
#[test]
fn drain_chunk_blob_reservation_stays_o_chunk() {
    // Interleaved keys, so both sources stay live and the drain runs through the
    // merge rather than the single-source bulk copy.
    let even = Rc::new(string_run((0..4096).map(|i| 2 * i), 512, 1));
    let odd = Rc::new(string_run((0..4096).map(|i| 2 * i + 1), 512, 1));
    let total_blob = even.blob.len() + odd.blob.len();
    assert!(total_blob > MAX_POOLED_BYTES, "{total_blob}");

    let mut cursor = create_read_cursor(&[even, odd], &[], make_schema_pk_u64_payload_string());
    assert!(cursor.mode.is_none(), "both sources must stay live");
    let mut rows = 0usize;
    while let Some(chunk) = cursor.drain_chunk(512) {
        rows += chunk.count;
        assert!(
            chunk.blob.capacity() <= MAX_POOLED_BYTES,
            "chunk reserved {} blob bytes of a {total_blob}-byte relation (MAX_POOLED_BYTES)",
            chunk.blob.capacity(),
        );
    }
    assert_eq!(rows, 2 * 4096);
}

/// A bounded read that one shard of several covers drains through the
/// single-source path and carries only its own rows' heap bytes.
#[test]
fn bounded_string_read_carries_only_its_own_rows() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    let shards: Vec<Rc<MappedShard>> = (0..2u64)
        .map(|s| {
            let run = string_run((1..=100).map(|i| s * 10_000 + i), 40, 1);
            map_shard(&dir.path().join(format!("s{s}.db")), &run)
        })
        .collect();

    let mut c = create_read_cursor(&[], &shards, schema);
    c.seek_range_bytes(&10_001u64.to_be_bytes(), Some(&10_004u64.to_be_bytes()));
    assert_eq!(c.mode, Some(1));
    let batch = c.drain_chunk(usize::MAX).expect("shard 1 window");
    assert_eq!(batch.blob.len(), 3 * 40, "only the drained rows' strings");
    let strings: Vec<Vec<u8>> = (0..batch.count)
        .map(|i| gnitz_expr::payload_bytes(&batch, i, 0).to_vec())
        .collect();
    let want: Vec<Vec<u8>> = (10_001..10_004u64)
        .map(|pk| format!("{pk:0>40}").into_bytes())
        .collect();
    assert_eq!(strings, want);
    assert!(!c.valid, "the window is fully drained");
}

/// A PK group holding a skeleton row collapses to one coarse row under both
/// payload comparators, beside a hydrated row that compares `Equal` to it; a
/// split drain hands that row's key back and copies only the hydrated groups.
#[test]
fn a_skeleton_row_coarsens_its_whole_pk_group() {
    let dir = tempfile::tempdir().unwrap();
    let nullable_string = u64_pk_schema(SchemaColumn::new(TypeCode::String, true));
    for (name, schema) in [("fixedint", make_schema_u64_i64()), ("generic", nullable_string)] {
        // PK 1: skeleton (coarse +3) plus two newer hydrated rows, one of which
        // compares `Equal` to a skeleton row under this arm. PK 2: hydrated only.
        // PK 3: skeleton whose coarse weight cancels.
        let mut sk = BatchBuilder::new(&schema.pk_only());
        for (pk, w) in [(1u128, 3), (3, 2)] {
            sk.begin_row(pk, w);
            sk.end_row();
        }
        // Opened under the view schema, as every reader sees a skeleton shard.
        let sk_path = dir.path().join(format!("{name}_sk.db"));
        let sk_path = sk_path.to_str().unwrap();
        let opts = ShardWriteOpts { skeleton: true, ..Default::default() };
        sk.finish().write_as_shard(sk_path, opts).unwrap();
        let sk = Rc::new(MappedShard::open(sk_path, &schema).unwrap());
        let mut bb = BatchBuilder::new(&schema);
        for (pk, w, v) in [(1u128, 5, None), (1, 7, Some(9)), (2, 4, Some(1)), (3, -2, None)] {
            bb.begin_row(pk, w);
            match (name, v) {
                ("fixedint", v) => bb.put_int(v.unwrap_or(0)),
                (_, Some(v)) => bb.put_string(&v.to_string()),
                (_, None) => bb.put_null(),
            }
            bb.end_row();
        }
        let mem = Rc::new(bb.finish().into_consolidated());

        let mut c = create_read_cursor(std::slice::from_ref(&mem), std::slice::from_ref(&sk), schema);
        let mut groups = Vec::new();
        while c.valid {
            groups.push((c.current_key_narrow(), c.current_weight, c.current_is_skeleton()));
            c.advance();
        }
        assert_eq!(
            groups,
            vec![(1, 15, true), (2, 4, false)],
            "{name}: PK 1 folds to one coarse row (3+5+7), PK 3 ghosts at 2-2",
        );

        let mut skeletons = SkeletonKeys::default();
        let live = create_read_cursor(&[mem], &[sk], schema)
            .drain_live_chunk(usize::MAX, &mut skeletons)
            .unwrap();
        assert_eq!(skeletons.keys, 1u64.to_be_bytes(), "{name}");
        assert_eq!(
            (live.count, live.get_pk_bytes(0), live.get_weight(0)),
            (1, &2u64.to_be_bytes()[..], 4),
            "{name}"
        );
    }
}
