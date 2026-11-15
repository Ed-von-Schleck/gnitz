use super::*;
use crate::repr::pool::MAX_POOLED_BYTES;

/// Carrying two whole heaps charges each source's rows the output leaves out —
/// survivors interleaved across both — and every kept string reads back.
#[test]
fn materialize_carrying_charges_each_sources_dropped_rows() {
    use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_string, read_strings};
    let schema = make_schema_pk_u64_payload_string();
    // Eight rows a side, one dropped from each: too little to make either
    // carried heap wasteful.
    let side = |first: u64, fill: u8, dropped: &'static [u8]| {
        let kept = [fill; 40];
        let mut rows: Vec<(u64, i64, &[u8])> = (0..8).map(|i| (first + 2 * i, 1, &kept[..])).collect();
        rows[3].2 = dropped;
        make_batch_bytes(&schema, &rows)
    };
    let (a, b) = (side(1, b'a', &[b'x'; 30]), side(2, b'b', &[b'y'; 50]));
    let sources = [a.as_mem_batch(), b.as_mem_batch()];
    let rows: Vec<Pick> = (0..8)
        .filter(|&r| r != 3)
        .flat_map(|r| [(0, r, 1), (1, r, 1)])
        .collect();
    let out = materialize_carrying(&sources, &schema, &rows);
    assert_eq!(out.dead_heap(), 30 + 50);
    assert_eq!(out.blob().len(), a.blob().len() + b.blob().len());
    let want: Vec<Vec<u8>> = (0..14).map(|i| vec![[b'a', b'b'][i % 2]; 40]).collect();
    assert_eq!(read_strings(&out), want);
}

/// Six sources of six payload columns, past the inline sizes of the per-source
/// views, the payload column table and the kept counts: every pick reads back
/// in pick order at its weight, whether its source's heap is carried whole or
/// relocated cell by cell.
#[test]
fn materialize_carrying_reads_past_the_inline_tables() {
    use crate::repr::BatchBuilder;
    use crate::schema::SchemaColumn;
    use crate::test_support::{u64_pk_schema, weighted_rows};
    use gnitz_wire::TypeCode;
    const SOURCES: u64 = 6;
    const ROWS: u64 = 48;
    let mut cols = vec![SchemaColumn::new(TypeCode::I64, false); 5];
    cols.push(SchemaColumn::new(TypeCode::String, false));
    let schema = u64_pk_schema(&cols);
    assert!(SOURCES as usize * schema.num_payload_cols() > 32);
    let put = |b: &mut BatchBuilder, key: u64, weight: i64| {
        b.begin_row(key as u128, weight);
        for c in 0..5 {
            b.put_int((key * 10 + c) as u128);
        }
        b.put_string(&format!("a string past the inline prefix {key:04}"));
        b.end_row();
    };
    // Source `s` holds the keys `s` mod `SOURCES`.
    let batches: Vec<Batch> = (0..SOURCES)
        .map(|s| {
            let mut b = BatchBuilder::new(&schema);
            for key in (s..ROWS).step_by(SOURCES as usize) {
                put(&mut b, key, 1);
            }
            b.finish()
        })
        .collect();
    let sources: Vec<MemBatch> = batches.iter().map(Batch::as_mem_batch).collect();
    // The last source keeps one row of eight, so its heap is relocated; the
    // others keep every row and carry theirs.
    let picked = |key: u64| key % SOURCES != SOURCES - 1 || key < SOURCES;
    let rows: Vec<Pick> = (0..ROWS)
        .filter(|&key| picked(key))
        .map(|key| ((key % SOURCES) as u32, (key / SOURCES) as u32, 1 + (key % 3) as i64))
        .collect();
    let out = materialize_carrying(&sources, &schema, &rows);
    let mut want = BatchBuilder::new(&schema);
    for key in (0..ROWS).filter(|&key| picked(key)) {
        put(&mut want, key, 1 + (key % 3) as i64);
    }
    assert_eq!(weighted_rows(&out), weighted_rows(&want.finish()));
    let carried: usize = batches[..SOURCES as usize - 1].iter().map(|b| b.blob().len()).sum();
    assert!(out.blob().len() >= carried && out.blob().len() < carried + batches[SOURCES as usize - 1].blob().len());
    assert_eq!(out.dead_heap(), 0);
    out.debug_verify_dead_heap();
}

/// [`gather_rows`] reads the same rows whichever way its sources hold their
/// regions — in place through one `ColPtr` per source, or through each row
/// accessor — each at its pick's weight, a column a file predates read NULL.
#[test]
fn gather_rows_reads_every_backing_alike() {
    use crate::repr::{BatchBuilder, MappedShard, Run};
    use crate::schema::SchemaColumn;
    use crate::test_support::{map_shard, u64_pk_schema, weighted_rows};
    use gnitz_wire::{TypeCode, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK};
    use std::rc::Rc;
    // Five sources hoist ten fixed-region pointers, past the inline table's eight.
    const SOURCES: usize = 5;
    const ROWS: usize = 300;
    let col = SchemaColumn::new;
    let narrow = u64_pk_schema(&[col(TypeCode::I64, false), col(TypeCode::String, false)]);
    let wide = u64_pk_schema(&[
        col(TypeCode::I64, false),
        col(TypeCode::String, false),
        col(TypeCode::I64, true),
    ]);
    // Source `s` holds the keys `s` mod `SOURCES`. `spread` values no encoding
    // shrinks, so a shard stores them as they stand; the rest pack.
    let source = |schema: &SchemaDescriptor, s: usize, spread: bool| {
        let mut b = BatchBuilder::new(schema);
        for key in (s..ROWS).step_by(SOURCES) {
            b.begin_row(key as u128, 1);
            b.put_int(match spread {
                true => (key as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as u128,
                false => key as u128,
            });
            b.put_string(&format!("gathered-string-{key:08}"));
            if schema.num_payload_cols() == 3 {
                b.put_null();
            }
            b.end_row();
        }
        let mut b = b.finish();
        b.certify_consolidated();
        b
    };
    let dir = tempfile::tempdir().unwrap();
    let shard = |name: &str, b: &Batch| map_shard(&dir.path().join(format!("{name}.db")), b);
    let picks: Vec<Pick> = (0..ROWS)
        .map(|key| ((key % SOURCES) as u32, (key / SOURCES) as u32, 2))
        .collect();

    for spread in [true, false] {
        let mem: Vec<Run> = (0..SOURCES)
            .map(|s| Run::Mem(Rc::new(source(&wide, s, spread))))
            .collect();
        // One pick at a time is too few to read in place: each goes through
        // the row accessors.
        let want: Vec<_> = picks
            .chunks(1)
            .flat_map(|pick| weighted_rows(&gather_rows(&mem, &wide, pick, 0)))
            .collect();
        assert!(want.iter().all(|&(_, w)| w == 2));
        let shards: Vec<Rc<MappedShard>> = (0..SOURCES)
            .map(|s| shard(&format!("wide_{spread}_{s}"), &source(&wide, s, spread)))
            .collect();
        let windows: Vec<_> = shards.iter().map(|s| s.window(0..s.row_count())).collect();
        let narrowed_shards: Vec<MappedShard> = (0..SOURCES)
            .map(|s| shard(&format!("narrow_{spread}_{s}"), &source(&narrow, s, spread)))
            .map(|s| s.rebind(&wide).unwrap())
            .collect();
        let narrowed: Vec<Run> = narrowed_shards
            .iter()
            .map(|s| Run::Shard(Rc::new(s.rebind(&wide).unwrap())))
            .collect();
        let narrowed_windows: Vec<_> = narrowed_shards.iter().map(|s| s.window(0..s.row_count())).collect();
        let mut mixed: Vec<Run> = shards.iter().cloned().map(Run::Shard).collect();
        mixed[1] = mem[1].clone();
        // What each set reads in place.
        let (pk, nulls, p0, p2) = (REG_PK, REG_NULL_BMP, REG_PAYLOAD_START, REG_PAYLOAD_START + 2);
        assert!(mem
            .iter()
            .all(|m| m.in_place(pk).is_some() && m.in_place(nulls).is_some() && m.in_place(p0).is_some()));
        assert!(windows
            .iter()
            .all(|w| w.in_place(pk).is_some() && w.in_place(nulls).is_some() && w.in_place(p2).is_some()));
        assert!(windows.iter().all(|w| w.in_place(p0).is_some() == spread));
        // A column the file predates is padded NULL, its fixed regions still
        // read in place.
        assert!(narrowed_windows.iter().all(|n| n.null_pad_mask() == 1 << 2
            && n.in_place(pk).is_some()
            && n.in_place(nulls).is_some()
            && n.in_place(p2).is_some()));
        let got = [
            gather_rows(&mem, &wide, &picks, 0),
            gather_rows(&windows, &wide, &picks, 0),
            gather_rows(&narrowed, &wide, &picks, 0),
            gather_rows(&narrowed_windows, &wide, &picks, 0),
            gather_rows(&mixed, &wide, &picks, 0),
        ];
        for (i, got) in got.iter().enumerate() {
            assert_eq!(weighted_rows(got), want, "spread={spread}, backing {i}");
        }
    }
}

/// The thread's pick scratch serves consolidations, key-set gathers, cursor
/// drains and clamps in any interleaving: a list grown past the retention cap is dropped, and
/// each later user starts from an empty list, so no output carries another
/// call's picks.
#[test]
fn pick_scratch_interleaves_across_kernels() {
    use crate::repr::{from_runs, PkSetGather, Run};
    use crate::stream::{op_weight_clamp, History};
    use crate::test_support::{make_batch, make_batch_raw, make_schema_u64_i64, weighted_rows, TestTrace};
    use gnitz_wire::ClampKind::Distinct;
    use gnitz_wire::PkKeys;
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    let schema = make_schema_u64_i64();
    // `rows` as an unconsolidated batch, and the consolidated batch the
    // reference fold of its weights per (PK, payload) builds.
    let fold = |rows: &[(u64, i64, i64)]| {
        let mut net: BTreeMap<(u64, i64), i64> = BTreeMap::new();
        for &(pk, w, v) in rows {
            *net.entry((pk, v)).or_default() += w;
        }
        let want: Vec<(u64, i64, i64)> = net
            .into_iter()
            .filter(|e| e.1 != 0)
            .map(|((pk, v), w)| (pk, w, v))
            .collect();
        (make_batch_raw(&schema, rows), make_batch(&schema, &want))
    };
    // Descending PKs, each row twice and a third of them cancelled: the batch
    // must sort and fold.
    let shuffled = |n: u64| -> Vec<(u64, i64, i64)> {
        (0..n)
            .rev()
            .flat_map(|i| {
                [
                    (i, 1, (i % 7) as i64),
                    (i, if i % 3 == 0 { -1 } else { 2 }, (i % 7) as i64),
                ]
            })
            .collect()
    };
    let scratch_capacity = || {
        PICKS.with(|c| {
            let picks = c.take();
            let cap = picks.capacity();
            c.set(picks);
            cap
        })
    };

    let big = (MAX_POOLED_BYTES / size_of::<Pick>()) as u64 + 1000;
    let (mut got, want) = fold(&shuffled(big));
    got.consolidate_in_place();
    assert_eq!(weighted_rows(&got), weighted_rows(&want));
    assert_eq!(scratch_capacity(), 0, "a list past the cap is not retained");

    // Keys 0..64 at payload `key`, and the even ones a second time at `-1 - key`.
    let stored = || {
        [
            make_batch(&schema, &(0..64).map(|i| (i, 1, i as i64)).collect::<Vec<_>>()),
            make_batch(
                &schema,
                &(0..64).step_by(2).map(|i| (i, 1, -1 - i as i64)).collect::<Vec<_>>(),
            ),
        ]
    };
    let mut trace = TestTrace::new(schema);
    stored().into_iter().for_each(|b| trace.ingest(b));
    for round in 1..6u64 {
        let (mut got, want) = fold(&shuffled(round * 5));
        got.consolidate_in_place();
        assert_eq!(weighted_rows(&got), weighted_rows(&want));
        assert!(scratch_capacity() > 0, "a small list is retained");

        let (unclaimed, want) = fold(&shuffled(round * 3));
        assert_eq!(weighted_rows(&unclaimed.to_consolidated()), weighted_rows(&want));

        // Every `round`-th key from 0..64; even keys hold two rows.
        let keys: Vec<u64> = (0..64).step_by(round as usize).collect();
        let pks: Vec<u8> = keys.iter().flat_map(|k| k.to_be_bytes()).collect();
        let cursor = from_runs(stored().into_iter().map(|b| Run::Mem(Rc::new(b))), schema);
        let mut gather = PkSetGather::over(cursor, PkKeys::from_sorted(8, pks));
        let mut gathered = Vec::new();
        while let Some(b) = gather.drain_chunk(3) {
            gathered.extend(weighted_rows(&b));
        }
        let mut want_rows: Vec<(u64, i64, i64)> = Vec::new();
        for &k in &keys {
            if k % 2 == 0 {
                want_rows.push((k, 1, -1 - k as i64));
            }
            want_rows.push((k, 1, k as i64));
        }
        want_rows.sort_by_key(|&(pk, _, v)| (pk, v));
        assert_eq!(gathered, weighted_rows(&make_batch(&schema, &want_rows)));

        // Both runs merged by a plain cursor, drained in chunks of `round + 1`.
        let mut cursor = from_runs(stored().into_iter().map(|b| Run::Mem(Rc::new(b))), schema);
        let mut drained = Vec::new();
        while let Some(b) = cursor.drain_chunk(round as usize + 1) {
            drained.extend(weighted_rows(&b));
        }
        let mut want_rows: Vec<(u64, i64, i64)> = (0..64).map(|i| (i, 1, i as i64)).collect();
        want_rows.extend((0..64).step_by(2).map(|i| (i, 1, -1 - i as i64)));
        want_rows.sort_by_key(|&(pk, _, v)| (pk, v));
        assert_eq!(drained, weighted_rows(&make_batch(&schema, &want_rows)));

        // Keys 64.. are absent from the trace: distinct passes the delta through.
        let fresh = make_batch(&schema, &(64..64 + round).map(|i| (i, 1, 0)).collect::<Vec<_>>());
        let open = &mut |f: &[u8], l: &[u8]| trace.cursor_within(f, l);
        let out = op_weight_clamp(Cow::Borrowed(&fresh), History::Before(open), Distinct);
        assert_eq!(weighted_rows(&out), weighted_rows(&fresh));

        // Re-adding present elements emits nothing for them; the absent one
        // past them is emitted.
        let mut mixed: Vec<(u64, i64, i64)> = (0..round).map(|i| (i, 1, i as i64)).collect();
        mixed.push((100, 1, 0));
        let mixed = make_batch(&schema, &mixed);
        let open = &mut |f: &[u8], l: &[u8]| trace.cursor_within(f, l);
        let out = op_weight_clamp(Cow::Borrowed(&mixed), History::Before(open), Distinct);
        assert_eq!(weighted_rows(&out), weighted_rows(&make_batch(&schema, &[(100, 1, 0)])));
    }
}
