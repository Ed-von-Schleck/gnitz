use super::super::batch::{REG_PAYLOAD_START, REG_WEIGHT};
use super::*;
use crate::schema::{type_code, SchemaDescriptor};
use crate::test_support::{make_batch_raw, pk_payload_schema};

/// A one-row batch encoded to a WAL block, ready to have its directory forged.
fn encoded_block(schema: &SchemaDescriptor) -> Vec<u8> {
    make_batch_raw(schema, &[(42, 1, 7)]).encode_to_wire_vec(1, false)
}

/// The foreign decode refuses a non-canonical string cell the engine's own decode admits.
#[test]
fn a_foreign_decode_refuses_a_non_canonical_string_cell() {
    use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_string};
    let schema = make_schema_pk_u64_payload_string();
    let long: &[u8] = b"a string long enough to spill";
    let clean = make_batch_bytes(&schema, &[(1, 1, b"short"), (2, 1, long)]).encode_to_wire_vec(1, false);
    assert_eq!(Batch::decode_foreign_wal_block(&clean, &schema).map(|b| b.len()), Ok(2));

    let (cells, _) = gnitz_wire::wal::dir_entry(&clean, REG_PAYLOAD_START);
    let forgeries: [fn(&mut [u8]); 2] = [
        // Row 0 is "short" (5 bytes): its suffix padding starts at byte 9.
        |cell| cell[15] = 0xAA,
        // Row 1 is long: point its heap offset past the blob.
        |cell| gnitz_wire::write_u64_le(cell, 8, 1 << 20),
    ];
    for (row, forge) in forgeries.into_iter().enumerate() {
        let mut buf = clean.clone();
        forge(&mut buf[cells + row * 16..cells + (row + 1) * 16]);
        assert!(Batch::decode_from_wal_block(&buf, &schema, false).is_ok(), "row {row}");
        assert_eq!(
            Batch::decode_foreign_wal_block(&buf, &schema).err(),
            Some("data WAL German string is not in canonical form"),
            "row {row}"
        );
    }
}

/// A fixed region whose directory size disagrees with what the schema implies
/// for the row count is refused, rather than decoded against a mis-sized region.
#[test]
fn decode_from_wal_block_rejects_mismatched_region_sizes() {
    let schema = pk_payload_schema(&[type_code::U64]); // pk_stride = 8
    for (region, forged) in [(REG_PK, 24u32), (REG_WEIGHT, 4)] {
        let mut buf = encoded_block(&schema);
        let size_off = gnitz_wire::wal::dir_entry_offset(region) + 4;
        buf[size_off..size_off + 4].copy_from_slice(&forged.to_le_bytes());
        let r = Batch::decode_from_wal_block(&buf, &schema, false);
        assert_eq!(r.err(), Some("data WAL region size mismatch"), "region {region}");
    }
}

/// `COUNT` and `NUM_REGIONS` live in the 32-byte header, outside the block's
/// own checksum, so `n -> 0` is a single-bit flip whenever `n` is a power of
/// two. Only the exact region-size relation rejects them. A genuinely empty
/// block is the negative control: `count == 0` with regions to match decodes.
#[test]
fn decode_from_wal_block_rejects_header_count_forgeries() {
    let schema = pk_payload_schema(&[type_code::U64]);
    let clean = make_batch_raw(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]).encode_to_wire_vec(7, true);
    for forged in [0u32, 2, 1000] {
        let mut buf = clean.clone();
        gnitz_wire::write_u32_le(&mut buf, gnitz_wire::WAL_OFF_COUNT, forged);
        assert_eq!(
            Batch::decode_from_wal_block(&buf, &schema, true).err(),
            Some("data WAL region size mismatch"),
            "COUNT 3 -> {forged} must be rejected by the exact region-size relation"
        );
    }

    let mut buf = clean.clone();
    gnitz_wire::write_u32_le(&mut buf, gnitz_wire::WAL_OFF_NUM_REGIONS, 4);
    assert!(
        Batch::decode_from_wal_block(&buf, &schema, true).is_err(),
        "a forged region count must be rejected"
    );

    let empty = Batch::empty_with_schema(&schema).encode_to_wire_vec(7, true);
    let decoded = Batch::decode_from_wal_block(&empty, &schema, true).expect("an empty block decodes");
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
    let block = empty.encode_to_wire_vec(7, true);

    let decoded = Batch::decode_from_wal_block(&block, &schema, true).expect("a zero-row block decodes");
    assert_eq!(decoded.len(), 0);
    assert!(decoded.blob.is_empty(), "a zero-row block carries no heap");
}

/// The variable-length blob region is the one whose extent the schema cannot
/// predict. When `[off, off + size)` overruns the block, the decoder must never
/// reach the point of resolving strings against a heap that is not there.
/// `verify_checksum = false`: the unverified IPC path is the one this guards.
#[test]
fn decode_mem_batch_rejects_blob_region_past_block() {
    let schema = pk_payload_schema(&[type_code::U64]);
    let mut buf = encoded_block(&schema);
    let entry = gnitz_wire::wal::dir_entry_offset(REG_PAYLOAD_START + schema.num_payload_cols());
    let block_end = buf.len() as u32;
    buf[entry..entry + 4].copy_from_slice(&block_end.to_le_bytes());
    buf[entry + 4..entry + 8].copy_from_slice(&8u32.to_le_bytes());

    let mut offsets = [0usize; MAX_BATCH_REGIONS];
    let r = decode_mem_batch_from_wal_block(&buf, &schema, false, &mut offsets);
    assert_eq!(r.err(), Some("data WAL block invalid"));
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
    let schema = crate::test_support::u64_pk_schema(crate::schema::SchemaColumn::new(type_code::STRING, 0));
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
fn wire_chunk_compacts_the_heap_and_inherits_the_layout() {
    let (_schema, mut src) = string_rows(&[
        (1, "the first long value"),
        (2, "the second long value"),
        (3, "the third long value"),
        (4, "the fourth long value"),
    ]);
    src.certify_layout(super::super::batch::Layout::Consolidated);

    let chunk = src.wire_chunk(1, 2);
    assert_eq!(chunk.len(), 2);
    assert!(
        chunk.blob.len() < src.blob.len(),
        "a 2-of-4 subrange must carry only its own spans ({} vs {})",
        chunk.blob.len(),
        src.blob.len()
    );
    for (i, want) in [(0usize, "the second long value"), (1, "the third long value")] {
        assert_eq!(crate::storage::payload_string(&chunk, i, 0), want);
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
