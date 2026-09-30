use super::super::batch::REG_PAYLOAD_START;
use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::BatchBuilder;
use crate::test_support::{
    encode_to_wire_vec, make_batch_bytes, make_schema_pk_u64_payload_string, pk_u64_two_i64_schema, weighted_rows,
};

/// Where region `r` of a `rows`-row block over `schema` starts.
fn region_offset(schema: &SchemaDescriptor, rows: usize, r: usize) -> usize {
    let (strides, nr) = strides_from_schema(schema);
    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    wire_offsets(&strides, nr as usize, rows, &mut offsets);
    offsets[r]
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
        let mut b = BatchBuilder::new(schema);
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

/// A zero-row block decodes to an empty batch, dropping any heap bytes it
/// declares: no cell can resolve against a heap at zero rows.
#[test]
fn a_zero_row_block_decodes_to_an_empty_batch() {
    let schema = make_schema_pk_u64_payload_string();
    let mut empty = Batch::empty_with_schema(&schema);
    empty.blob.extend_from_slice(b"heap bytes no row references");
    empty.dead_heap = empty.blob.len();
    let block = encode_to_wire_vec(&empty);

    let decoded = Batch::decode_from_wal_block(&block, &schema).expect("a zero-row block decodes");
    assert_eq!(decoded.len(), 0);
    assert!(decoded.blob.is_empty(), "a zero-row block carries no heap");
}

/// Every encoder round-trips through the validated decode at narrow and odd PK
/// and payload strides. The block packs its regions unaligned while a decoded
/// arena 8-aligns them, which is where the two can disagree.
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
        let mut b = BatchBuilder::new(schema);
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

        let mut buf = vec![0u8; src.wire_byte_size()];
        let frame = src.wire_frame_within(1, 0, src.wire_byte_size());
        let n = src.encode_frame(&frame, &mut buf);
        assert_eq!(decode(&buf[..n]), weighted_rows(&src)[1..], "stride {stride}: frame");

        let n = src.encode_scattered_to_wire(&[4, 0, 2], &mut buf).unwrap();
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
        let mut b = BatchBuilder::new(schema);
        for i in 0..rows as u64 {
            b.begin_row(i as u128, 1);
            b.put_int(i as u128);
            b.put_int((i * 3) as u128);
            b.end_row();
        }
        let batch = b.finish();
        let mut buf = vec![0u8; batch.wire_byte_size()];

        let t = crate::test_support::bench_time(ITERS, || {
            let n = black_box(&batch).encode_to_wire(black_box(&mut buf));
            let block = WalBlock::parse(black_box(&buf[..n]), &schema).unwrap();
            black_box(block.view().count);
        });
        println!(
            "wal block {rows} rows: encode + decode {:.1} ns",
            t.as_nanos() as f64 / ITERS as f64
        );
    }
}

/// Rows the chunker takes from `start` under `budget`.
fn chunk_rows(b: &Batch, start: usize, overhead: usize, budget: usize) -> usize {
    b.wire_frame_within(start, overhead, budget).rows()
}

// ---------------------------------------------------------------------------
// Reply chunking: wire_frame_within
// ---------------------------------------------------------------------------

/// `(pk, value)` rows at weight 1 over a U64 PK and one STRING payload column.
/// Values past `SHORT_STRING_THRESHOLD` live in the heap.
fn string_batch(rows: &[(u64, impl AsRef<str>)]) -> Batch {
    let mut b = BatchBuilder::new(make_schema_pk_u64_payload_string());
    for (pk, v) in rows {
        b.begin_row(*pk as u128, 1);
        b.put_string(v.as_ref());
        b.end_row();
    }
    b.finish()
}

/// A frame inside a heap-bearing batch encodes its rows' spans and no others,
/// in exactly the bytes it was sized at.
#[test]
fn a_frame_carries_only_its_spans() {
    let src = string_batch(&[
        (1, "the first long value"),
        (2, "the second long value"),
        (3, "the third long value"),
        (4, "the fourth long value"),
    ]);
    let budget = src.wire_byte_size_range(2) + "the second long value".len() + "the third long value".len();
    let frame = src.wire_frame_within(1, 0, budget);
    assert_eq!((frame.rows(), src.wire_frame_size(&frame)), (2, budget));

    let mut buf = vec![0u8; src.wire_byte_size()];
    let n = src.encode_frame(&frame, &mut buf);
    assert_eq!(n, budget);
    let chunk = Batch::decode_foreign_wal_block(&buf[..n], src.schema()).unwrap();
    assert_eq!(chunk.len(), 2);
    assert_eq!(chunk.blob.len(), budget - src.wire_byte_size_range(2));
    for (i, want) in [(0usize, "the second long value"), (1, "the third long value")] {
        assert_eq!(gnitz_expr::payload_string(&chunk, i, 0), want);
        assert_eq!(chunk.get_pk(i), (i + 2) as u128);
        assert_eq!(chunk.get_weight(i), 1);
    }
}

/// Rows sharing one heap span, as a join's fan-out writes them, cost that span
/// once.
#[test]
fn wire_frame_within_counts_a_shared_span_once() {
    const N: usize = 20;
    let one = string_batch(&[(1, "v".repeat(200))]);
    // One relocating append session, so its blob cache dedups the repeated
    // range: every row of `shared` points at the same span of its own heap.
    let mut shared = Batch::with_capacity(one.schema(), N);
    shared
        .append_session(N)
        .push_ranges(&one.as_mem_batch(), None, &[(0, 1); N]);

    let distinct = string_batch(&(0..N as u64).map(|i| (i, format!("{i:-<200}"))).collect::<Vec<_>>());

    let budget = shared.wire_byte_size();
    assert_eq!(chunk_rows(&shared, 0, 0, budget), N);
    assert!(
        chunk_rows(&distinct, 0, 0, budget) < N,
        "the same budget cannot hold {N} private 200-byte copies"
    );
}

/// A short (inline) string costs no heap bytes, both in a heapless batch and on
/// the forward walk past a long row.
#[test]
fn wire_frame_within_does_not_collapse_on_short_strings() {
    let all_short = string_batch(&(0..30u64).map(|i| (i, "abcdefghijkl")).collect::<Vec<_>>());
    assert!(all_short.blob.is_empty(), "12-byte values stay inline");

    let mut mixed_rows: Vec<(u64, String)> = vec![(0, "a".repeat(64))];
    mixed_rows.extend((1..30u64).map(|i| (i, "abcdefghijkl".to_string())));
    let mixed = string_batch(&mixed_rows);
    assert!(!mixed.blob.is_empty(), "the one long value takes the forward walk");

    // Room for ten rows beside the block header.
    let budget = all_short.wire_byte_size_range(10);
    assert_eq!(chunk_rows(&all_short, 0, 0, budget), 10);
    assert_eq!(
        chunk_rows(&mixed, 1, 0, budget),
        10,
        "the short rows past the long one cost their fixed width and nothing more"
    );

    for (b, start) in [(&all_short, 0), (&mixed, 1)] {
        let frame = b.wire_frame_within(start, 0, budget);
        assert_eq!(b.wire_frame_size(&frame), b.wire_byte_size_range(10), "no heap byte");
    }
}

/// A row too wide for the budget still ships: the chunk carries it alone rather
/// than coming back empty, and the caller sizes what it got.
#[test]
fn wire_frame_within_never_returns_an_empty_frame() {
    let batch = string_batch(&[(1, "w".repeat(4096)), (2, "w".repeat(4096))]);
    assert_eq!(chunk_rows(&batch, 0, 0, 64), 1);
}

// ---------------------------------------------------------------------------
// Scattered blocks: encode_scattered_to_wire, encode_scattered_prefix
// ---------------------------------------------------------------------------

/// A scatter of a heap-bearing batch relocates the strings its rows reference
/// into the block's own heap, so a peer's validated decode reads every row back
/// in the order the indices named.
#[test]
fn a_scattered_block_carries_its_own_heap() {
    let values: Vec<(u64, String)> = (0..6u64).map(|i| (i, format!("{i:->40}"))).collect();
    let src = string_batch(&values);
    let indices = [4u32, 1, 3];
    let mut out = vec![0u8; src.wire_byte_size()];
    let written = src
        .encode_scattered_to_wire(&indices, &mut out)
        .expect("the whole batch's size fits");
    let mut exact = vec![0u8; written];
    assert_eq!(
        src.encode_scattered_prefix(&indices, &mut exact),
        Some((indices.len(), written)),
        "the fit charges exactly the heap the encoder writes"
    );
    let got = Batch::decode_foreign_wal_block(&out[..written], src.schema()).expect("a canonical block");
    assert_eq!(got.len(), indices.len());
    assert!(got.blob.len() < src.blob.len(), "only the selected rows' spans travel");
    for (i, &idx) in indices.iter().enumerate() {
        assert_eq!(got.get_pk(i), idx as u128);
        assert_eq!(got.get_weight(i), 1);
        assert_eq!(gnitz_expr::payload_string(&got, i, 0), values[idx as usize].1);
    }
}

/// `None`, not a truncated block, when either the fixed regions or the heap
/// behind them overflow `out`.
#[test]
fn a_scattered_block_that_overflows_its_buffer_is_refused() {
    let src = string_batch(&[(1, "x".repeat(100)), (2, "y".repeat(100))]);
    let fixed = src.wire_byte_size_range(2);
    let mut out = vec![0u8; fixed - 1];
    assert!(
        src.encode_scattered_to_wire(&[0, 1], &mut out).is_none(),
        "the fixed regions overflow"
    );
    let mut out = vec![0u8; fixed + 150];
    assert!(
        src.encode_scattered_to_wire(&[0, 1], &mut out).is_none(),
        "the heap overflows"
    );
    let mut out = vec![0u8; fixed + 150];
    assert_eq!(
        src.encode_scattered_prefix(&[0, 1], &mut out),
        Some((1, src.wire_byte_size_range(1) + 100)),
        "the prefix is the one row whose heap fits"
    );
    let mut out = vec![0u8; fixed + 200];
    assert_eq!(src.encode_scattered_to_wire(&[0, 1], &mut out), Some(fixed + 200));
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
    let heap: Vec<u8> = (0..62u8).collect();
    let cell = |start: usize, len: usize| {
        let mut c = [0u8; 16];
        c[..4].copy_from_slice(&(len as u32).to_le_bytes());
        c[4..8].copy_from_slice(&heap[start..start + 4]);
        c[8..].copy_from_slice(&(start as u64).to_le_bytes());
        c
    };
    // [0, 20) twice, [10, 30) overlapping it: bytes [30, 62) are dead.
    let cells = [cell(0, 20), cell(0, 20), cell(10, 20)].concat();
    let pks: Vec<u8> = (1..=3u64).flat_map(|k| k.to_be_bytes()).collect();
    let weights: Vec<u8> = (0..3).flat_map(|_| 1i64.to_le_bytes()).collect();
    let nulls = [0u8; 24];
    let regions: [&[u8]; 5] = [&pks, &weights, &nulls, &cells, &heap];
    for claimed in [0, 5, heap.len()] {
        let mut block = Vec::new();
        wal::append_block(&regions, claimed, &mut block);
        let decoded = Batch::decode_foreign_wal_block(&block, &schema).unwrap();
        assert_eq!(decoded.dead_heap, 32, "header claimed {claimed}");
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
    let mut rng = crate::test_rng::Rng::new(0x9E37_79B9_7F4A_7C15);
    let mut rnd = |m: u64| rng.gen_range(m);
    let (mut ok_n, mut bad_n) = (0, 0);
    for _ in 0..20000 {
        let rows = 1 + rnd(4);
        let mut b = BatchBuilder::new(schema);
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
        let mut b = BatchBuilder::new(schema);
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
    let mut b = BatchBuilder::new(schema);
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
/// budget, each frame sized by `wire_frame_within` and then encoded. Two shapes:
/// every row a long 40-byte string, and a wide fixed row whose string is short
/// on all but one row in 64.
/// `#[ignore]`; run release:
///   cargo test -p gnitz-store --release reply_chunk_strings_bench -- --ignored --nocapture --test-threads=1
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
    let mut b = BatchBuilder::new(wide_schema);
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
            while start < batch.len() {
                let frame = batch.wire_frame_within(start, 0, BUDGET);
                bytes += batch.encode_frame(&frame, &mut buf);
                start = frame.end();
                frames += 1;
            }
            (frames, bytes)
        });
        std::hint::black_box(&buf);
        println!("reply_chunk_strings_bench {name}: {instructions} instr, {frames} frames, {bytes} bytes");
    }
}
