use super::super::batch::REG_PAYLOAD_START;
use super::*;
use crate::schema::{SchemaDescriptor, TypeCode};
use crate::test_support::{encode_to_wire_vec, make_batch_raw, pk_payload_schema};

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
    use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_string};
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

/// The foreign decode refuses a NULL over a non-zero cell.
#[test]
fn a_foreign_decode_refuses_a_non_zero_cell_under_a_null() {
    use crate::schema::SchemaColumn;
    // `(U64 pk, I64 NOT NULL, I64 NULL)`: the nullable column is payload slot 1.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let mut b = super::super::batch_builder::BatchBuilder::new(schema);
    for pk in [1u128, 2] {
        b.begin_row(pk, 1);
        b.put_int(pk * 10);
        b.put_null();
        b.end_row();
    }
    let clean = encode_to_wire_vec(&b.finish());
    assert_eq!(Batch::decode_foreign_wal_block(&clean, &schema).map(|b| b.len()), Ok(2));
    let cell = region_offset(&schema, 2, REG_PAYLOAD_START + 1) + 8;
    let mut forged = clean.clone();
    forged[cell..cell + 8].fill(0xFF);
    assert!(Batch::decode_from_wal_block(&forged, &schema).is_ok());
    assert_eq!(
        Batch::decode_foreign_wal_block(&forged, &schema).err(),
        Some("a non-zero cell under a NULL")
    );

    // A NOT NULL-only schema carries no null bit to test under.
    let not_null = pk_payload_schema(&[TypeCode::U64]);
    let block = encode_to_wire_vec(&make_batch_raw(&not_null, &[(1, 1, -1), (2, 1, 0)]));
    assert_eq!(
        Batch::decode_foreign_wal_block(&block, &not_null).map(|b| b.len()),
        Ok(2)
    );
}

/// A forged `ROWS` or `HEAP_LEN`, or a block cut short of its `SIZE`, is
/// refused; a genuinely empty block decodes.
#[test]
fn decode_from_wal_block_rejects_header_forgeries() {
    let schema = pk_payload_schema(&[TypeCode::U64]);
    let clean = encode_to_wire_vec(&make_batch_raw(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]));
    let decode = |buf: &[u8]| {
        let mut offsets = [0usize; MAX_BATCH_REGIONS];
        decode_mem_batch_from_wal_block(buf, &schema, &mut offsets).err()
    };
    let mismatch = Some("block size does not match its rows and heap");
    for (off, forged) in [
        (wal::WAL_OFF_ROWS, 0),
        (wal::WAL_OFF_ROWS, 2),
        (wal::WAL_OFF_ROWS, 1000),
        (wal::WAL_OFF_HEAP_LEN, 8),
    ] {
        let mut buf = clean.clone();
        gnitz_wire::write_u32_le(&mut buf, off, forged);
        assert_eq!(decode(&buf), mismatch, "header word at {off} forged to {forged}");
    }
    assert_eq!(decode(&clean[..clean.len() - 1]), Some("declared size past buffer"));

    let empty = encode_to_wire_vec(&Batch::empty_with_schema(&schema));
    let decoded = Batch::decode_from_wal_block(&empty, &schema).expect("an empty block decodes");
    assert_eq!(decoded.count, 0);
}

/// A zero-row block declaring heap bytes decodes to an empty heap: no cell can
/// resolve against a heap at zero rows.
#[test]
fn a_zero_row_block_carrying_heap_bytes_decodes_to_an_empty_heap() {
    use crate::test_support::make_schema_pk_u64_payload_string;
    let schema = make_schema_pk_u64_payload_string();
    let mut empty = Batch::empty_with_schema(&schema);
    empty.blob.extend_from_slice(b"heap bytes no row references");
    empty.dead_heap = empty.blob.len();
    let block = encode_to_wire_vec(&empty);

    let decoded = Batch::decode_from_wal_block(&block, &schema).expect("a zero-row block decodes");
    assert_eq!(decoded.len(), 0);
    assert!(decoded.blob.is_empty(), "a zero-row block carries no heap");
}

/// Encode then decode 1-row and 100-row blocks of a (u64 PK, two i64 payload)
/// schema: the per-block constant of the framer and the decoder.
#[test]
#[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
fn wal_block_bench() {
    use std::hint::black_box;
    use std::time::Instant;
    const ITERS: usize = 1_000_000;
    let schema = SchemaDescriptor::new(
        &[
            crate::schema::SchemaColumn::new(TypeCode::U64, false),
            crate::schema::SchemaColumn::new(TypeCode::I64, false),
            crate::schema::SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    for rows in [1usize, 100] {
        let mut b = super::super::batch_builder::BatchBuilder::new(schema);
        for i in 0..rows as u64 {
            b.begin_row(i as u128, 1);
            b.put_int(i as u128);
            b.put_int((i * 3) as u128);
            b.end_row();
        }
        let batch = b.finish();
        let mut buf = vec![0u8; batch.wire_byte_size()];
        let mut offsets = [0usize; MAX_BATCH_REGIONS];

        let t = Instant::now();
        for _ in 0..ITERS {
            let n = black_box(&batch).encode_to_wire(black_box(&mut buf));
            let mb = decode_mem_batch_from_wal_block(black_box(&buf[..n]), &schema, &mut offsets).unwrap();
            black_box(mb.count);
        }
        println!(
            "wal block {rows} rows: encode + decode {:.1} ns",
            t.elapsed().as_nanos() as f64 / ITERS as f64
        );
    }
}

/// Rows the chunker takes from `start` under `budget`.
fn chunk_rows(b: &Batch, start: usize, overhead: usize, budget: usize) -> usize {
    b.wire_chunk_within(start, overhead, budget).0.rows()
}

// ---------------------------------------------------------------------------
// Reply chunking: wire_chunk_within
// ---------------------------------------------------------------------------

/// A U64 PK with one STRING payload column, and `(pk, value)` rows at weight 1
/// over it. Values past `SHORT_STRING_THRESHOLD` live in the heap.
fn string_rows(rows: &[(u64, &str)]) -> (SchemaDescriptor, Batch) {
    let schema = crate::test_support::u64_pk_schema(crate::schema::SchemaColumn::new(TypeCode::String, false));
    let mut b = super::super::batch_builder::BatchBuilder::new(schema);
    for &(pk, v) in rows {
        b.begin_row(pk as u128, 1);
        b.put_string(v);
        b.end_row();
    }
    (schema, b.finish())
}

/// A strict subrange of a heap-bearing batch ships a heap holding only the spans
/// its own rows reference, keeps every cell's content, and keeps the source's
/// layout claim — which is what the frame encoder stamps into the wire flags.
#[test]
fn a_compacted_subrange_carries_only_its_spans_and_the_layout() {
    let (_schema, mut src) = string_rows(&[
        (1, "the first long value"),
        (2, "the second long value"),
        (3, "the third long value"),
        (4, "the fourth long value"),
    ]);
    src.certify_layout(super::super::batch::Layout::Consolidated);

    let chunk = src.compacted(1..3);
    assert_eq!(chunk.len(), 2);
    assert!(
        chunk.blob.len() < src.blob.len(),
        "a 2-of-4 subrange must carry only its own spans ({} vs {})",
        chunk.blob.len(),
        src.blob.len()
    );
    for (i, want) in [(0usize, "the second long value"), (1, "the third long value")] {
        assert_eq!(gnitz_expr::payload_string(&chunk, i, 0), want);
        assert_eq!(chunk.get_pk(i), (i + 2) as u128);
        assert_eq!(chunk.get_weight(i), 1);
    }
    assert_eq!(
        chunk.layout(),
        src.layout(),
        "a contiguous subrange of an ordered, ghost-free batch is both"
    );
}

/// Rows pointing at ONE source span cost that span once — the shape a join's
/// fan-out produces, where `scatter_copy` writes a left row's string once and
/// points every output row at it. Counting per row instead would size each frame
/// as if every row carried a private copy, and emit far too many frames.
#[test]
fn wire_chunk_within_counts_a_shared_span_once() {
    const N: usize = 20;
    let (schema, one) = string_rows(&[(1, &"v".repeat(200))]);
    // One append session, so its blob cache dedups the repeated range: every
    // row of `shared` points at the same span of `shared`'s own heap.
    let mut shared = Batch::with_capacity(&schema, N);
    shared.append_ranges(&one.as_mem_batch(), &[(0, 1); N]);

    let distinct_rows: Vec<(u64, String)> = (0..N as u64).map(|i| (i, format!("{i:-<200}"))).collect();
    let (_, distinct) = string_rows(&distinct_rows.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>());

    let budget = shared.wire_byte_size();
    assert_eq!(chunk_rows(&shared, 0, 0, budget), N);
    assert!(
        chunk_rows(&distinct, 0, 0, budget) < N,
        "the same budget cannot hold {N} private 200-byte copies"
    );
}

/// A short (inline) string contributes no heap bytes. Reading a heap extent off
/// one instead — its bytes 8..16 are content, not an offset — yields a bogus
/// span and one row per frame. Both arms are covered: an all-short batch has an
/// empty heap and inverts exactly, and one long row puts the rest on the
/// forward walk.
#[test]
fn wire_chunk_within_does_not_collapse_on_short_strings() {
    let short_rows: Vec<(u64, &str)> = (0..30u64).map(|i| (i, "abcdefghijkl")).collect();
    let (_, all_short) = string_rows(&short_rows);
    assert!(all_short.blob.is_empty(), "12-byte values stay inline");

    let mut mixed_rows: Vec<(u64, String)> = vec![(0, "a".repeat(64))];
    mixed_rows.extend((1..30u64).map(|i| (i, "abcdefghijkl".to_string())));
    let (_, mixed) = string_rows(&mixed_rows.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>());
    assert!(!mixed.blob.is_empty(), "the one long value takes the forward walk");

    // Room for ten rows beside the block header.
    let budget = all_short.wire_byte_size_range(10);
    assert_eq!(chunk_rows(&all_short, 0, 0, budget), 10);
    assert_eq!(
        chunk_rows(&mixed, 1, 0, budget),
        10,
        "the short rows past the long one cost their fixed width and nothing more"
    );

    // An empty heap is framed off the source; a live one relocates.
    assert!(matches!(
        all_short.wire_chunk_within(0, 0, budget).0,
        super::WireChunk::Range { .. }
    ));
    assert!(matches!(
        mixed.wire_chunk_within(1, 0, budget).0,
        super::WireChunk::Owned(_)
    ));
}

/// A row too wide for the budget still ships: the chunk carries it alone rather
/// than coming back empty, and the caller sizes what it got.
#[test]
fn wire_chunk_within_never_returns_an_empty_chunk() {
    let (_, batch) = string_rows(&[(1, &"w".repeat(4096)), (2, &"w".repeat(4096))]);
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
    let (schema, src) = string_rows(&values.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>());
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
    let got = Batch::decode_foreign_wal_block(&out[..written], &schema).expect("a canonical block");
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
    let (_, src) = string_rows(&[(1, &"x".repeat(100)), (2, &"y".repeat(100))]);
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
    use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_string};
    let schema = make_schema_pk_u64_payload_string();
    let mut b = make_batch_bytes(&schema, &[(1, 1, &[b'a'; 20]), (2, 1, &[b'b'; 30])]);
    b.blob.extend_from_slice(&[0; 7]);
    b.dead_heap = 7;
    let block = encode_to_wire_vec(&b);
    assert_eq!(gnitz_wire::read_u32_le(&block, wal::WAL_OFF_HEAP_DEAD), 7);
    let decoded = Batch::decode_from_wal_block(&block, &schema).unwrap();
    assert_eq!((decoded.dead_heap, decoded.blob.len()), (7, b.blob.len()));
}

/// A foreign block's header is not trusted: the decode measures the heap,
/// counting a span two cells share once, the overlap of two spans once, and an
/// unreferenced tail whole.
#[test]
fn a_foreign_decode_measures_the_dead_heap_exactly() {
    use crate::test_support::make_schema_pk_u64_payload_string;
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
        let mut block = vec![0u8; wal::block_size(&regions)];
        wal::write_block(&regions, claimed, &mut block);
        let decoded = Batch::decode_foreign_wal_block(&block, &schema).unwrap();
        assert_eq!(decoded.dead_heap, 32, "header claimed {claimed}");
        assert_eq!(
            Batch::decode_from_wal_block(&block, &schema).unwrap().dead_heap,
            claimed
        );
    }
}

/// A block declaring more dead heap bytes than it has heap is refused.
#[test]
fn a_block_declaring_more_dead_than_heap_is_refused() {
    use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_string};
    let schema = make_schema_pk_u64_payload_string();
    let b = make_batch_bytes(&schema, &[(1, 1, &[b'a'; 20])]);
    let mut block = encode_to_wire_vec(&b);
    gnitz_wire::write_u32_le(&mut block, wal::WAL_OFF_HEAP_DEAD, b.blob.len() as u32 + 1);
    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    assert_eq!(
        decode_mem_batch_from_wal_block(&block, &schema, &mut offsets).err(),
        Some("block declares more dead heap than heap")
    );
}
