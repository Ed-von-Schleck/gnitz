use super::super::batch::REG_PAYLOAD_START;
use super::*;
use crate::repr::{merge_consolidated, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{
    encode_to_wire_vec, make_batch, make_batch_bytes, make_schema_pk_u64_payload_string, make_schema_u64_i64,
    pk_u64_two_i64_schema, weighted_rows,
};

/// Where region `r` of a `rows`-row block over `schema` starts.
fn region_offset(schema: &SchemaDescriptor, rows: usize, r: usize) -> usize {
    wal::WAL_HEADER_SIZE + schema.region_start(r, rows)
}

/// The foreign decode refuses a non-canonical string cell the engine's own decode admits.
#[test]
fn a_foreign_decode_refuses_a_non_canonical_string_cell() {
    let schema = make_schema_pk_u64_payload_string();
    let long: &[u8] = b"a string long enough to spill";
    let clean = encode_to_wire_vec(&make_batch_bytes(&schema, &[(1, 1, b"short"), (2, 1, long)]));
    assert_eq!(Batch::decode_foreign_wal_block(&clean, &schema).map(|b| b.len()), Ok(2));

    let cells = region_offset(&schema, 2, REG_PAYLOAD_START);
    let forgeries: [fn(&mut [u8]); 2] = [
        // Row 0 is "short" (5 bytes): its suffix padding starts at byte 9.
        |cell| cell[15] = 0xAA,
        // Row 1 is long: point its heap offset past the blob.
        |cell| gnitz_wire::write_u64_le(cell, 8, 1 << 20),
    ];
    for (row, forge) in forgeries.into_iter().enumerate() {
        let mut buf = clean.clone();
        forge(&mut buf[cells + row * 16..cells + (row + 1) * 16]);
        assert!(Batch::decode_from_wal_block(&buf, &schema).is_ok(), "row {row}");
        assert_eq!(
            Batch::decode_foreign_wal_block(&buf, &schema).err(),
            Some("data WAL German string is not in canonical form"),
            "row {row}"
        );
    }
}

/// The foreign decode refuses the two NULL encodings the schema does not admit:
/// a NULL over a non-zero cell, and a null bit on a NOT NULL column — each over
/// a block that breaks that rule alone.
#[test]
fn a_foreign_decode_refuses_a_null_the_schema_does_not_admit() {
    // `(U64 pk, I64 NOT NULL, I64 NULL)`: the nullable column is payload slot 1.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    // Row 2's NOT NULL cell is zero, the NULL encoding should its bit be set.
    let block = |not_null_bit: bool| {
        let mut b = BatchBuilder::new(&schema);
        for pk in [1u128, 2] {
            b.begin_row(pk, 1);
            if pk == 2 && not_null_bit {
                b.put_null();
            } else {
                b.put_int((pk - 1) * 10);
            }
            b.put_null();
            b.end_row();
        }
        encode_to_wire_vec(&b.finish())
    };
    let clean = block(false);
    assert_eq!(Batch::decode_foreign_wal_block(&clean, &schema).map(|b| b.len()), Ok(2));

    let cell = region_offset(&schema, 2, REG_PAYLOAD_START + 1) + 8;
    let mut valued_null = clean.clone();
    valued_null[cell..cell + 8].fill(0xFF);
    for (forged, why) in [
        (valued_null, "a non-zero cell under a NULL"),
        (block(true), "a null bit on a NOT NULL column"),
    ] {
        assert!(Batch::decode_from_wal_block(&forged, &schema).is_ok(), "{why}");
        assert_eq!(Batch::decode_foreign_wal_block(&forged, &schema).err(), Some(why));
    }
}

/// Every encoder round-trips through the validated decode at narrow and odd PK
/// and payload strides, where a block's regions start unaligned.
#[test]
fn every_encoder_round_trips_at_narrow_strides() {
    use TypeCode::*;
    for (pk, payload) in [
        (&[U8][..], I16),
        (&[U16], U8),
        (&[U32], I32),
        (&[U64, U32], U16),
        (&[U64], I64),
    ] {
        let mut cols: Vec<SchemaColumn> = pk.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
        cols.extend([SchemaColumn::new(payload, true), SchemaColumn::new(String, false)]);
        let key: Vec<u32> = (0..pk.len() as u32).collect();
        let schema = SchemaDescriptor::new(&cols, &key);
        let mut b = BatchBuilder::new(&schema);
        for i in 0..5u128 {
            b.begin_row_opk(&vec![i + 1; pk.len()], [1, -2, 3][i as usize % 3]);
            b.put_opt_int((i % 2 == 0).then_some(i * 7));
            b.put_string(&format!("row {i} holds a string past the inline limit"));
            b.end_row();
        }
        let src = b.finish();
        let stride = schema.pk_stride();
        let decode = |block: &[u8]| weighted_rows(&Batch::decode_foreign_wal_block(block, &schema).unwrap());

        assert_eq!(
            decode(&encode_to_wire_vec(&src)),
            weighted_rows(&src),
            "stride {stride}: whole"
        );

        let mut buf = vec![0u8; src.wire_whole().unwrap().byte_size()];
        let range = src.wire_rows_within(1, usize::MAX).unwrap();
        let n = range.encode(&mut buf);
        assert_eq!(n, range.byte_size(), "stride {stride}: range size");
        assert_eq!(decode(&buf[..n]), weighted_rows(&src)[1..], "stride {stride}: range");

        let listed = src.wire_listed(&[4, 0, 2]).unwrap();
        let n = listed.encode(&mut buf);
        assert_eq!(n, listed.byte_size(), "stride {stride}: scattered size");
        let picked = src.indexed_rows(&[4, 0, 2]);
        assert_eq!(decode(&buf[..n]), weighted_rows(&picked), "stride {stride}: scattered");
    }
}

/// Encode then decode 1-row and 100-row blocks of a (u64 PK, two i64 payload)
/// schema: the per-block constant of the framer and the decoder.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn wal_block_bench() {
    use std::hint::black_box;
    const ITERS: usize = 1_000_000;
    let schema = pk_u64_two_i64_schema();
    for rows in [1usize, 100] {
        let mut b = BatchBuilder::new(&schema);
        for i in 0..rows as u64 {
            b.begin_row(i as u128, 1);
            b.put_int(i as u128);
            b.put_int((i * 3) as u128);
            b.end_row();
        }
        let batch = b.finish();
        let mut buf = vec![0u8; batch.wire_whole().unwrap().byte_size()];

        let t = crate::test_support::bench_time(ITERS, || {
            let n = black_box(&batch).wire_whole().unwrap().encode(black_box(&mut buf));
            let block = MemBatch::of_wal_block(black_box(&buf[..n]), &schema).unwrap();
            black_box(block.count);
        });
        println!(
            "wal block {rows} rows: encode + decode {:.1} ns",
            t.as_nanos() as f64 / ITERS as f64
        );
    }
}

/// `(pk, value)` rows at weight 1 over a U64 PK and one STRING payload column.
/// Values past `SHORT_STRING_THRESHOLD` live in the heap.
fn string_batch(rows: &[(u64, impl AsRef<str>)]) -> Batch {
    let mut b = BatchBuilder::new(&make_schema_pk_u64_payload_string());
    for (pk, v) in rows {
        b.begin_row(*pk as u128, 1);
        b.put_string(v.as_ref());
        b.end_row();
    }
    b.finish()
}

/// A row run, and a scattered prefix, is the longest run whose block — sized as
/// `encode_listed` writes it under span dedup — fits; each reads back its rows.
/// No rows is no block.
#[test]
fn row_runs_and_scattered_prefixes_take_the_longest_run_that_fits() {
    let one = string_batch(&[(1, "v".repeat(200))]);
    // Rows naming one span, as a join's fan-out writes them.
    let mut shared = Batch::with_capacity(one.schema(), 20);
    shared
        .append_session(20)
        .push_ranges(&one.as_mem_batch(), None, &[(0, 1); 20]);
    let mut mixed = vec![(0u64, "a".repeat(64))];
    mixed.extend((1..30).map(|i| (i, "abcdefghijkl".to_string())));
    let fixed: Vec<(u64, i64, i64)> = (1..=30).map(|i| (i, 1, i as i64)).collect();
    for b in [
        make_batch(&make_schema_u64_i64(), &fixed),
        string_batch(&(0..30u64).map(|i| (i, "abcdefghijkl")).collect::<Vec<_>>()),
        string_batch(&mixed),
        string_batch(&(0..20u64).map(|i| (i, format!("{i:-<200}"))).collect::<Vec<_>>()),
        string_batch(&[(1, "w".repeat(4096)), (2, "w".repeat(4096))]),
        shared,
    ] {
        let whole = b.wire_whole().unwrap().byte_size();
        let mut buf = vec![0u8; 2 * whole];
        let size = |rows: &[u32]| b.encode_listed(rows, 0, &mut vec![0; 2 * whole], true).unwrap();
        assert!(b.wire_rows_within(b.len(), usize::MAX).is_none());
        assert!(Batch::empty_with_schema(b.schema()).wire_whole().is_none());
        let read_back = |block: &[u8]| weighted_rows(&Batch::decode_foreign_wal_block(block, b.schema()).unwrap());
        for start in [0, 1] {
            let rest = b.len() - start;
            let run = |k: usize| (start as u32..(start + k) as u32).collect::<Vec<_>>();
            let longest = |cap: usize| (1..=rest).take_while(|&k| size(&run(k)) <= cap).last();
            let k3 = size(&run(3.min(rest)));
            for budget in [64, size(&run(1)), k3 - 1, k3, usize::MAX] {
                let rows = b.wire_rows_within(start, budget).unwrap();
                let want = longest(budget).unwrap_or(1);
                assert_eq!(rows.rows(), want, "start {start} budget {budget}");
                let n = rows.encode(&mut buf);
                assert_eq!(n, rows.byte_size());
                assert_eq!(read_back(&buf[..n]), weighted_rows(&b)[start..start + want]);
            }
            let all = run(rest);
            for cap in [size(&run(1)) - 1, size(&run(2.min(rest))), size(&run(2.min(rest))) + 1] {
                let mut out = vec![0u8; cap];
                let got = b.encode_scattered_prefix(&all, &mut out);
                let want = longest(cap).map(|k| (k, size(&run(k))));
                assert_eq!(got, want, "start {start} cap {cap}");
                if let Some((k, n)) = got {
                    assert_eq!(read_back(&out[..n]), weighted_rows(&b)[start..start + k]);
                    let mut short = vec![0u8; n - 1];
                    assert!(b.encode_listed(&run(k), 0, &mut short, true).is_none());
                }
            }
        }
    }
}

/// A listed pick's size is the bytes it encodes, and its block decodes to the
/// listed rows in list order with no dead heap byte — whether its strings go
/// out a copy per cell or, for rows naming one span, once.
#[test]
fn a_listed_pick_sizes_and_encodes_its_rows_in_list_order() {
    let one = string_batch(&[(1, "v".repeat(200))]);
    // Rows naming one span, as a join's fan-out writes them.
    let mut shared = Batch::with_capacity(one.schema(), 20);
    shared
        .append_session(20)
        .push_ranges(&one.as_mem_batch(), None, &[(0, 1); 20]);
    assert_eq!(shared.blob.len(), 200, "precondition: one span for every row");
    let fixed: Vec<(u64, i64, i64)> = (1..=20).map(|i| (i, 1, i as i64)).collect();
    for b in [
        make_batch(&make_schema_u64_i64(), &fixed),
        string_batch(&(0..20u64).map(|i| (i, format!("{i:-<200}"))).collect::<Vec<_>>()),
        shared.clone(),
    ] {
        for list in [&[0u32, 3, 7, 19][..], &[19, 3, 0], &[5]] {
            let rows = b.wire_listed(list).unwrap();
            assert_eq!(rows.rows(), list.len());
            let mut buf = vec![0u8; rows.byte_size()];
            assert_eq!(
                rows.encode(&mut buf),
                buf.len(),
                "{list:?}: the size is the bytes written"
            );
            let decoded = Batch::decode_foreign_wal_block(&buf, b.schema()).unwrap();
            assert_eq!(
                weighted_rows(&decoded),
                weighted_rows(&b.indexed_rows(list)),
                "{list:?}"
            );
            assert_eq!(decoded.dead_heap, 0, "{list:?}: every heap byte is referenced");
        }
        assert!(b.wire_listed(&[]).is_none());
    }
    let three = shared.wire_listed(&[0, 1, 2]).unwrap();
    assert_eq!(
        three.byte_size(),
        wal::WAL_HEADER_SIZE + 3 * shared.schema().row_width() + 200,
        "three rows naming one span carry it once"
    );
}

/// A WAL block viewed in place is an arena of exactly its rows: at row counts
/// on both sides of an owned arena's rounding it copies, appends and merges as
/// the batch it was encoded from.
#[test]
fn a_viewed_wal_block_reads_as_its_batch() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U8, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::U32, false),
        ],
        &[0],
    );
    for rows in [1usize, 3, 8, 13] {
        let mut b = BatchBuilder::new(&schema);
        for i in 0..rows as u128 {
            b.begin_row(i + 1, 1 + (i % 3) as i64);
            b.put_int(i % 200);
            b.put_opt_int((i % 4 != 0).then_some(i * 11));
            b.put_int(i * 1000);
            b.end_row();
        }
        let mut src = b.finish();
        src.certify_consolidated();
        let block = encode_to_wire_vec(&src);
        let mb = MemBatch::of_wal_block(&block, &schema).unwrap();
        assert_eq!((mb.len(), mb.cap), (rows, rows));
        let want = weighted_rows(&src);

        assert_eq!(weighted_rows(&Batch::from_mem_batch(&mb)), want, "{rows} rows: copy");

        let mut one = Batch::empty_with_schema(&schema);
        one.append_ranges(&mb, &[(0, rows)]);
        assert_eq!(weighted_rows(&one), want, "{rows} rows: one range");

        let cut = rows / 2;
        let mut two = Batch::empty_with_schema(&schema);
        two.append_ranges(&mb, &[(0, cut), (cut, rows)]);
        assert_eq!(weighted_rows(&two), want, "{rows} rows: two ranges");

        let merged = merge_consolidated(&[mb.clone(), mb.clone()], &schema);
        let doubled: Vec<_> = want.iter().map(|(row, w)| (row.clone(), 2 * w)).collect();
        assert_eq!(weighted_rows(&merged), doubled, "{rows} rows: merge");
    }
}

/// An engine block states its batch's dead-byte bound and the engine decode
/// adopts it.
#[test]
fn dead_heap_round_trips_an_engine_block() {
    let schema = make_schema_pk_u64_payload_string();
    let mut b = make_batch_bytes(&schema, &[(1, 1, &[b'a'; 20]), (2, 1, &[b'b'; 30])]);
    b.blob.extend_from_slice(&[0; 7]);
    b.dead_heap = 7;
    let block = encode_to_wire_vec(&b);
    let decoded = Batch::decode_from_wal_block(&block, &schema).unwrap();
    assert_eq!((decoded.dead_heap, decoded.blob.len()), (7, b.blob.len()));
}

/// A foreign block's header is not trusted: the decode measures the heap,
/// counting a span two cells share once, the overlap of two spans once, and an
/// unreferenced tail whole.
#[test]
fn a_foreign_decode_measures_the_dead_heap_exactly() {
    let schema = make_schema_pk_u64_payload_string();
    // Longer than one word of the span bitset.
    let heap: Vec<u8> = (0..200u8).map(|b| b % 128).collect();
    let cell = |start: usize, len: usize| {
        let mut c = gnitz_wire::encode_german_string(&heap[start..start + len], &mut Vec::new());
        gnitz_wire::write_u64_le(&mut c, 8, start as u64);
        c
    };
    // [0, 20) twice, [10, 30) overlapping it, and [50, 150): bytes [30, 50) and
    // [150, 200) are dead.
    let cells = [cell(0, 20), cell(0, 20), cell(10, 20), cell(50, 100)].concat();
    let pks: Vec<u8> = (1..=4u64).flat_map(|k| k.to_be_bytes()).collect();
    let weights: Vec<u8> = (0..4).flat_map(|_| 1i64.to_le_bytes()).collect();
    let nulls = [0u8; 32];
    let regions: [&[u8]; 5] = [&pks, &weights, &nulls, &cells, &heap];
    for claimed in [0, 5, heap.len()] {
        let mut block = Vec::new();
        wal::append_block(&regions, claimed, &mut block);
        let decoded = Batch::decode_foreign_wal_block(&block, &schema).unwrap();
        assert_eq!(decoded.dead_heap, 70, "header claimed {claimed}");
        assert_eq!(
            Batch::decode_from_wal_block(&block, &schema).unwrap().dead_heap,
            claimed
        );
    }
}

/// A pushed block decodes iff every STRING cell is UTF-8, as a per-cell
/// `from_utf8` judges it, over random blocks mixing valid pieces and invalid
/// ones. A BLOB column between the two STRING columns may hold anything.
#[test]
fn foreign_decode_utf8_matches_per_cell_oracle() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::Blob, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
    const PIECES: [&[u8]; 12] = [
        b"a",
        b"xyz",
        "é".as_bytes(),
        "€".as_bytes(),
        "𝄞".as_bytes(),
        &[0x80],
        &[0xFF],
        &[0xC3],
        &[0xE2, 0x82],
        &[0xED, 0xA0, 0x80],
        &[0xA9],
        b"0123456789",
    ];
    let mut rng = crate::test_support::Rng::new(0x9E37_79B9_7F4A_7C15);
    let mut rnd = |m: u64| rng.gen_range(m);
    let (mut ok_n, mut bad_n) = (0, 0);
    for _ in 0..20000 {
        let rows = 1 + rnd(4);
        let mut b = BatchBuilder::new(&schema);
        let mut valid = true;
        for i in 0..rows {
            b.begin_row(i as u128, 1);
            for col in 0..3 {
                // Mostly valid pieces, so a block is often wholly valid.
                let n = rnd(8);
                let mut v = Vec::new();
                for _ in 0..n {
                    let k = if rnd(6) == 0 {
                        rnd(12)
                    } else {
                        [0, 1, 2, 3, 4, 11][rnd(6) as usize]
                    };
                    v.extend_from_slice(PIECES[k as usize]);
                }
                if col != 1 {
                    valid &= std::str::from_utf8(&v).is_ok();
                }
                b.put_blob(&v);
            }
            b.end_row();
        }
        let block = encode_to_wire_vec(&b.finish());
        let got = Batch::decode_foreign_wal_block(&block, &schema);
        assert_eq!(got.is_ok(), valid, "{:?}", got.err());
        if valid {
            ok_n += 1
        } else {
            bad_n += 1
        }
    }
    assert!(ok_n > 2000 && bad_n > 2000, "{ok_n} valid, {bad_n} invalid");
}

/// The block's STRING contents are checked as separate values: no character
/// may span two cells, or a cell and the heap bytes beside it.
#[test]
fn foreign_decode_utf8_spans_split_across_cells() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
            SchemaColumn::new(TypeCode::Blob, false),
        ],
        &[0],
    );
    let accepts = |rows: &[(&[u8], &[u8])]| {
        let mut b = BatchBuilder::new(&schema);
        for (i, (s, x)) in rows.iter().enumerate() {
            b.begin_row(i as u128, 1);
            b.put_blob(s);
            b.put_blob(x);
            b.end_row();
        }
        Batch::decode_foreign_wal_block(&encode_to_wire_vec(&b.finish()), &schema).is_ok()
    };
    let long = |pre: &[u8], post: &[u8]| [pre, b"0123456789abcdef", post].concat();
    // Valid, long and short, with the heap wholly UTF-8.
    assert!(accepts(&[(&long(b"", "é".as_bytes()), b"x"), ("aé€".as_bytes(), b"")]));
    // A long STRING starting mid-character after a BLOB ending in a lead byte:
    // the heap reads as UTF-8 across the seam.
    assert!(!accepts(&[(b"", &long(b"", &[0xC3])), (&long(&[0xA9], b""), b"")]));
    // A long STRING ending in a lead byte before a BLOB starting with a
    // continuation byte.
    assert!(!accepts(&[(&long(b"", &[0xC3]), &long(&[0xA9], b""))]));
    // Two full short cells whose contents join into one character.
    assert!(!accepts(&[(b"0123456789\xE2\x82", b""), (b"\xAC", b"")]));
    // The heap not UTF-8 (a BLOB), the STRING cells valid, and then not.
    assert!(accepts(&[(&long(b"", "é".as_bytes()), &[0xFF; 20])]));
    assert!(!accepts(&[(&long(b"", &[0xFF]), &[0xFF; 20])]));
}

/// Retired instructions for `decode_foreign_wal_block` over 1000 rows of one
/// STRING column. `GNITZ_BENCH_SHAPE` picks the value; difference two
/// `GNITZ_BENCH_PASSES` counts under `perf stat -e instructions:u`.
#[test]
#[ignore]
fn foreign_decode_string_bench() {
    let shape = std::env::var("GNITZ_BENCH_SHAPE").unwrap_or_else(|_| "s12".to_string());
    let passes: usize = std::env::var("GNITZ_BENCH_PASSES").map_or(1, |p| p.parse().unwrap());
    let unit = match shape.as_str() {
        "s12" => "abcdefghijkl".to_string(),
        "u9" => "aé€bxy".to_string(),
        "s64" => "abcdefgh".repeat(8),
        "m64" => format!("{}é", "abcdefgh".repeat(7)),
        "u64" => "aéb€".repeat(8),
        "s512" => "abcdefgh".repeat(64),
        "u512" => "aéb€".repeat(73),
        other => panic!("GNITZ_BENCH_SHAPE must be s12/u9/s64/m64/u64/s512/u512, got {other:?}"),
    };
    let schema = make_schema_pk_u64_payload_string();
    let mut b = BatchBuilder::new(&schema);
    for i in 0..1000u64 {
        b.begin_row(i as u128, 1);
        let mut v = unit.clone().into_bytes();
        v[0] = b'0' + (i % 10) as u8;
        b.put_string(std::str::from_utf8(&v).unwrap());
        b.end_row();
    }
    let block = encode_to_wire_vec(&b.finish());
    let mut acc = 0usize;
    for _ in 0..passes {
        acc += Batch::decode_foreign_wal_block(std::hint::black_box(&block), &schema)
            .unwrap()
            .len();
    }
    println!(
        "foreign_decode_string_bench shape={shape} passes={passes} acc={}",
        std::hint::black_box(acc)
    );
}

/// Retired instructions to drain a 10⁵-row batch frame by frame at a 64 KiB
/// budget, each frame sized by `wire_rows_within` and then encoded. Two shapes:
/// every row a long 40-byte string, and a wide fixed row whose string is short
/// on all but one row in 64.
/// `#[ignore]`; run release:
///   cargo test -p gnitz-zset --release reply_chunk_strings_bench -- --ignored --nocapture --test-threads=1
#[test]
#[ignore]
fn reply_chunk_strings_bench() {
    const N: usize = 100_000;
    const BUDGET: usize = 64 << 10;

    let long = string_batch(&(0..N as u64).map(|i| (i, format!("{i:040}"))).collect::<Vec<_>>());

    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend([SchemaColumn::new(TypeCode::I64, false); 6]);
    cols.push(SchemaColumn::new(TypeCode::String, false));
    let wide_schema = SchemaDescriptor::new(&cols, &[0]);
    let mut b = BatchBuilder::new(&wide_schema);
    for i in 0..N as u64 {
        b.begin_row(i as u128, 1);
        for c in 0..6u64 {
            b.put_int(i.wrapping_mul(2_654_435_761 + c) as u128);
        }
        match i % 64 {
            0 => b.put_string(&format!("{i:040}")),
            _ => b.put_string(&format!("s{}", i % 1000)),
        }
        b.end_row();
    }
    let wide = b.finish();

    let counter = gnitz_foundation::perf::Counter::instructions().expect("instructions counter");
    let mut buf = vec![0u8; 2 * BUDGET];
    for (name, batch) in [("long", &long), ("wide_short", &wide)] {
        let ((frames, bytes), instructions) = counter.measure(|| {
            let (mut start, mut frames, mut bytes) = (0, 0, 0);
            while let Some(frame) = batch.wire_rows_within(start, BUDGET) {
                bytes += frame.encode(&mut buf);
                start += frame.rows();
                frames += 1;
            }
            (frames, bytes)
        });
        std::hint::black_box(&buf);
        println!("reply_chunk_strings_bench {name}: {instructions} instr, {frames} frames, {bytes} bytes");
    }
}
