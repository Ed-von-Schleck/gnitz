use super::*;
use crate::schema::{ColumnTable, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::BatchBuilder;
use crate::test_support::{le_cell, make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string, opk_pk};

/// `packer`'s key for each of the first `n` rows of `src`, through `pack_rows`.
fn packed_keys<B: BatchView>(packer: &ReindexPacker, src: &B, n: usize) -> Vec<Vec<u8>> {
    let stride = packer.out_stride;
    let mut region = vec![0u8; n * stride];
    packer.pack_rows(&mut region, stride, src, 0, n);
    region.chunks(stride).map(<[u8]>::to_vec).collect()
}

/// Worker count the co-partition pins route against. Any count works — the
/// property is that producer and consumer agree — but the wide-arm formula
/// pin needs a fixed one to recompute against.
const NW: usize = 4;

// -----------------------------------------------------------------------
// The content-hash arm's own contract
// -----------------------------------------------------------------------

#[test]
fn a_string_key_packs_its_content_hash() {
    // Two rows: one short ("foo", inline) and one long string (> 12 bytes,
    // stored in blob), so both German-string layouts execute.
    let schema = make_schema_pk_u64_payload_string();
    let mut b = BatchBuilder::new(schema);
    let contents: [&[u8]; 2] = [b"foo", b"hello-world-xyz"];
    for (r, content) in contents.iter().enumerate() {
        b.begin_row(r as u128 + 1, 1i64);
        b.put_blob(content);
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();

    let packer = ReindexPacker::new(&schema, &[(1, TypeCode::U128)]).unwrap();
    for (row, content) in contents.iter().enumerate() {
        let mut buf = [0u8; 16];
        packer.pack_into(&mut buf, &mb, row);
        assert_eq!(buf, gnitz_wire::checksum_128(content).to_be_bytes(), "row {row}");
    }
}

// -----------------------------------------------------------------------
// FoldCols::key_row — the one-shot and the streaming form are one digest
// -----------------------------------------------------------------------

/// The fold's stack and scratch arms hash the same bytes.
#[test]
fn hash_fold_stack_arm_matches_the_scratch_arm() {
    /// The same columns folded the other way — the arm `FoldCols::new` did not pick.
    fn flipped(f: &FoldCols) -> FoldCols {
        FoldCols { locs: f.locs.clone(), inline: !f.inline }
    }

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),  // 0: PK
            SchemaColumn::new(TypeCode::I32, false),  // 1: NOT NULL payload  (slot 0)
            SchemaColumn::new(TypeCode::U64, true),   // 2: nullable payload  (slot 1)
            SchemaColumn::new(TypeCode::U128, false), // 3: wide payload      (slot 2)
            SchemaColumn::new(TypeCode::U16, true),   // 4: nullable payload  (slot 3)
        ],
        &[0],
    );
    let mut b = BatchBuilder::new(schema);
    // Row 0: nothing NULL. Row 1: the two nullable columns NULL, so both the
    // marker-only arm and the marker+route-key arm run in one row.
    for (pk, nulls) in [(7u128, false), (8u128, true)] {
        b.begin_row(pk, 1);
        b.put_int((-9i32) as u128);
        match nulls {
            true => b.put_null(),
            false => b.put_int(42),
        }
        b.put_int(u128::MAX - 3);
        match nulls {
            true => b.put_null(),
            false => b.put_int(5),
        }
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();

    let all: Vec<ColumnLocator> = (0..5).map(|c| schema.locate(c)).collect();
    // Every arity from the global aggregate's empty fold up to the full set.
    for k in 0..=all.len() {
        let f = FoldCols::new(all[..k].to_vec());
        assert!(f.inline, "arity {k} is fixed-width and fits the scratch");
        let g = flipped(&f);
        for row in 0..2 {
            let null_word = mb.get_null_word(row);
            assert_eq!(
                f.key_row(&mb, row, null_word),
                g.key_row(&mb, row, null_word),
                "arity {k}, row {row}"
            );
        }
    }

    // The two shapes that must take the scratch: variable-length content, and a
    // group set wider than the stack buffer.
    let gs = make_schema_pk_u64_payload_string();
    assert!(
        !FoldCols::new(vec![gs.locate(1)]).inline,
        "a German-string column takes the scratch"
    );
    assert!(
        !FoldCols::new(vec![all[1]; FOLD_INLINE_COLS + 1]).inline,
        "a group set past the stack buffer takes the scratch"
    );
}

// -----------------------------------------------------------------------
// ReindexPacker::output_schema — the layout the per-row packer writes through
// -----------------------------------------------------------------------

#[test]
fn packer_output_schema_pk_width_policy() {
    // (key column type, expected output PK type, expected pk_stride)
    let cases = [
        (TypeCode::U64, TypeCode::U64, 8usize),
        (TypeCode::I32, TypeCode::I32, 4),
        (TypeCode::U16, TypeCode::U16, 2),
        (TypeCode::String, TypeCode::U128, 16),
        (TypeCode::Blob, TypeCode::U128, 16),
        (TypeCode::U128, TypeCode::U128, 16),
        (TypeCode::UUID, TypeCode::U128, 16),
    ];
    for (key_tc, want_tc, want_stride) in cases {
        // in_schema: [U64 PK, <key col>]; reindex on the payload col so the
        // PK-ineligible key types (STRING/BLOB/float) are exercisable as keys.
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(key_tc, false),
            ],
            &[0],
        );
        let node_schema = ReindexPacker::new(&in_schema, &[(1, key_tc.reindex_output_type())])
            .unwrap()
            .output_schema(&in_schema, &[0, 1])
            .unwrap();
        assert_eq!(node_schema.columns[0].type_code, want_tc, "key {key_tc} → PK type");
        assert_eq!(node_schema.pk_stride(), want_stride, "key {key_tc} → pk_stride");
    }
    // A float key is refused: its raw bits are equality-incorrect (±0.0).
    for float_tc in [TypeCode::F32, TypeCode::F64] {
        let in_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(float_tc, false),
            ],
            &[0],
        );
        assert!(
            ReindexPacker::new(&in_schema, &[(1, float_tc.reindex_output_type())]).is_err(),
            "a float reindex key must be refused, not packed"
        );
        assert!(
            ReindexPacker::new_group_key(&in_schema, &[1], &[]).is_err(),
            "a float group key must be refused, not packed"
        );
    }
}

#[test]
fn packer_output_schema_compound() {
    // in_schema: [U64 pk, I32, U128]; reindex on (col1 I32, col2 U128).
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::U128, false),
        ],
        &[0],
    );
    let out = ReindexPacker::new(&in_schema, &[(1, TypeCode::I32), (2, TypeCode::U128)])
        .unwrap()
        .output_schema(&in_schema, &[0, 1, 2])
        .unwrap();
    assert_eq!(out.pk_cols(), &[0, 1], "2-slot compound PK");
    assert_eq!(out.columns[0].type_code, TypeCode::I32, "slot0 keeps I32 native width");
    assert_eq!(out.columns[1].type_code, TypeCode::U128, "slot1 U128");
    assert_eq!(out.pk_stride(), 4 + 16, "compound stride = Σ slot widths");
    // Input columns follow the synthetic PK slots.
    assert_eq!(out.num_columns(), 2 + 3);
    assert_eq!(out.columns[2].type_code, TypeCode::U64);
    assert_eq!(out.columns[3].type_code, TypeCode::I32);
    assert_eq!(out.columns[4].type_code, TypeCode::U128);
}

#[test]
fn packer_output_schema_cross_width_promotes() {
    // in_schema: [U64 pk, I32, I64]; reindex on (col1 I32, col2 I64) with
    // slot 0 promoted to I64 and slot 1 self-typed.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let out = ReindexPacker::new(&in_schema, &[(1, TypeCode::I64), (2, TypeCode::I64)])
        .unwrap()
        .output_schema(&in_schema, &[0, 1, 2])
        .unwrap();
    assert_eq!(out.columns[0].type_code, TypeCode::I64, "slot0 promoted to I64");
    assert_eq!(out.columns[1].type_code, TypeCode::I64, "slot1 self-typed I64");
    assert_eq!(out.pk_stride(), 8 + 8, "both slots 8 bytes after promotion");
}

#[test]
fn packer_output_schema_payload_prune() {
    // in_schema: [U64 pk, I32, U128, I16]; reindex on col1; keep payload {0, 3}.
    let in_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I16, false),
        ],
        &[0],
    );
    let out = ReindexPacker::new(&in_schema, &[(1, TypeCode::I32)])
        .unwrap()
        .output_schema(&in_schema, &[0, 3])
        .unwrap();
    assert_eq!(out.pk_cols(), &[0], "single synthetic PK slot");
    assert_eq!(out.columns[0].type_code, TypeCode::I32, "PK slot = reindex col1 (I32)");
    // Only the two kept payload columns follow — not all four input columns.
    assert_eq!(out.num_columns(), 1 + 2, "1 PK + 2 kept payload");
    assert_eq!(out.columns[1].type_code, TypeCode::U64, "kept payload col 0");
    assert_eq!(out.columns[2].type_code, TypeCode::I16, "kept payload col 3");
}

// -----------------------------------------------------------------------
// ReindexPacker — multi-column / compound reindex packing
// -----------------------------------------------------------------------

#[test]
fn test_reindex_packer_multi_column_bytes() {
    // Compound key spanning every slot shape: a non-leading PK column (offset
    // 8), a sign-flipped I32 payload, and a 16-byte U128 payload.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::U128, false),
        ],
        &[0, 1],
    );
    let pk0: u64 = 0x0102_0304_0506_0708;
    let pk1: u64 = 0xA0B0_C0D0_E0F0_0102;
    let iv: i32 = -3;
    let uv: u128 = 0xdead_beef_cafe_1234_5678_9abc_def0_0001;

    let mut b = BatchBuilder::new(schema);
    b.begin_row_opk(&[pk0 as u128, pk1 as u128], 1i64);
    b.put_int(iv as u128);
    b.put_int(uv);
    b.end_row();
    let b = b.finish();
    let mb = b.as_mem_batch();

    let packer = ReindexPacker::new(&schema, &[(1, TypeCode::U64), (2, TypeCode::I32), (3, TypeCode::U128)]).unwrap();
    // out_stride = 8 (Pk U64) + 4 (I32) + 16 (U128) = 28.
    assert_eq!(packer.out_stride, 8 + 4 + 16);

    let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
    packer.pack_into(&mut buf[..packer.out_stride], &mb, 0);

    // Expected: each column's OPK bytes concatenated at its offset.
    let mut want = Vec::new();
    want.extend_from_slice(&pk1.to_be_bytes()); // col1 Pk: BE(pk1) verbatim
    let mut i32_opk = [0u8; 4];
    gnitz_wire::encode_pk_column(&iv.to_le_bytes(), TypeCode::I32, &mut i32_opk);
    want.extend_from_slice(&i32_opk); // col2: sign-aware OPK
    assert_eq!(i32_opk[0], 0x7F, "I32 -3 OPK leading byte is sign-flipped (0x7F)");
    want.extend_from_slice(&uv.to_be_bytes()); // col3 Wide: BE(u128)
    assert_eq!(&buf[..packer.out_stride], &want[..], "packed compound key bytes");
}

#[test]
fn test_reindex_packer_arity1_byte_identity() {
    // The content-hash arm: a STRING and a BLOB key both pack to the
    // big-endian image of `checksum_128` over the column's content. The
    // integer and PK-placement arms are the proptest's; this is the arm it
    // cannot generate (`arb_pk_type` yields PK-eligible integers).
    for schema in [make_schema_pk_u64_payload_string(), make_schema_pk_u64_payload_blob()] {
        // Three rows with distinct content, one of them empty, exercising the
        // per-row read.
        let contents: [&[u8]; 3] = [b"abc", b"", b"hello-world-xyz"];
        let mut b = BatchBuilder::new(schema);
        for (r, content) in contents.iter().enumerate() {
            b.begin_row((r + 1) as u128 * 11, 1i64);
            b.put_blob(content);
            b.end_row();
        }
        let b = b.finish();
        let mb = b.as_mem_batch();

        let packer = ReindexPacker::new(&schema, &[(1, TypeCode::U128)]).unwrap();
        assert_eq!(packer.out_stride, 16, "a content-hash key is a 16-byte U128 slot");
        let keys = packed_keys(&packer, &mb, 3);

        for (row, content) in contents.iter().enumerate() {
            let want = gnitz_wire::checksum_128(content);
            assert_eq!(
                keys[row],
                want.to_be_bytes(),
                "{} row {row}: packed key is BE(content hash)",
                schema.columns[1].type_code,
            );
        }
        // No two contents collide, the empty one included.
        assert_ne!(keys[0], keys[2]);
        assert_ne!(keys[0], keys[1]);
    }
}

#[test]
fn test_reindex_packer_copartition_contract() {
    // The bytes one row's pack computes (pack_into into a scratch buffer) must
    // be byte-identical to the `_join_pk` stored by packed_keys,
    // so the delta scatter and the reindexed trace land on the same partition.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0],
    );
    // Reindex on (col2 U64 payload, col1 I32 payload) — a 2-column non-PK key.
    let cols = [2u32, 1u32];
    let rows: &[(u64, i32, u64)] = &[
        (1, -5, 100),
        (2, 7, 100), // same col2 as row 0, different col1
        (3, -5, 200),
        (4, i32::MIN, 0),
    ];
    let mut b = BatchBuilder::new(schema);
    for &(pk, c1, c2) in rows {
        b.begin_row(pk as u128, 1i64);
        b.put_int(c1 as u128);
        b.put_int(c2 as u128);
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();

    let packer = ReindexPacker::new(&schema, &crate::test_support::self_typed_slots(&schema, &cols)).unwrap();
    let keys = packed_keys(&packer, &mb, rows.len());

    for (row, key) in keys.iter().enumerate() {
        let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
        packer.pack_into(&mut buf[..packer.out_stride], &mb, row);
        // Trace side (stored _join_pk) == scatter side (scratch buffer).
        assert_eq!(key, &buf[..packer.out_stride], "row {row} key bytes");
    }
    // Rows 0 and 1 share col2 but differ in col1 → distinct keys.
    assert_ne!(keys[0], keys[1]);
}

#[test]
fn test_reindex_packer_copartition_contract_wide() {
    // WIDE-branch (24-byte key) co-partition pin: three independent builders
    // must agree on the bytes — the scatter (`pack_into`), the trace store
    // (`packed_keys`), and the ingest OPK encoder (`opk_pk`, which never
    // touches `ReindexPacker`) — and the formula pin below catches a fork in
    // the wide routing arm, which byte-equality alone cannot.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false), // PK (not part of the key)
            SchemaColumn::new(TypeCode::U64, false), // c1 payload → key slot 0
            SchemaColumn::new(TypeCode::U64, false), // c2 payload → key slot 1
            SchemaColumn::new(TypeCode::U64, false), // c3 payload → key slot 2
        ],
        &[0],
    );
    // Non-trivial, high-entropy column values (so a forked hash seed/shift in
    // the wide arm lands on a different bucket with overwhelming probability).
    let key: [u64; 3] = [0x0102_0304_0506_0708, 0xA0B0_C0D0_E0F0_0102, 0xdead_beef_cafe_1234];
    let mut b = BatchBuilder::new(schema);
    b.begin_row(42u128, 1i64);
    b.put_int((key[0]) as u128);
    b.put_int((key[1]) as u128);
    b.put_int((key[2]) as u128);
    b.end_row();
    let b = b.finish();
    let mb = b.as_mem_batch();

    // Reindex on the three U64 payload columns → a 24-byte (3×U64) OPK key.
    let packer = ReindexPacker::new(&schema, &[(1, TypeCode::U64), (2, TypeCode::U64), (3, TypeCode::U64)]).unwrap();
    assert_eq!(packer.out_stride, 24, "3×U64 reindex key must be 24 bytes (wide)");

    // The reindex output schema = natural 3×U64 PK (what the reindex map stamps and
    // what the trace store holds); identical layout to `wide_pk_3xu64_schema`
    // minus the trailing payload.
    let out_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
        ],
        &[0, 1, 2],
    );
    assert!(out_schema.pk_stride() > 16, "test invariant: 24-byte key is wide");

    // PATH 1 — trace store: the stamped `_join_pk`.
    let consumer = &packed_keys(&packer, &mb, 1)[0][..];

    // PATH 2 — exchange scatter: pack_into into a scratch buffer.
    let mut buf = [0u8; crate::schema::MAX_PK_BYTES];
    packer.pack_into(&mut buf[..packer.out_stride], &mb, 0);
    let producer = &buf[..packer.out_stride];

    // PATH 3 — storage/ingest OPK encoder (no ReindexPacker involved at all).
    let oracle = opk_pk(&out_schema, &[key[0] as u128, key[1] as u128, key[2] as u128]);

    // (1) BYTE-EQUALITY teeth: all three independent builders agree, and the
    // key is genuinely wide (> 16 bytes).
    assert_eq!(consumer.len(), 24, "consumer key is the 24-byte wide region");
    assert!(consumer.len() > 16, "wide branch requires key len > 16");
    assert_eq!(producer, consumer, "scatter (pack_into) == trace store (_join_pk)");
    assert_eq!(consumer, oracle.as_slice(), "trace store == ingest OPK encoder");
    assert_eq!(producer, oracle.as_slice(), "scatter == ingest OPK encoder");

    // (2) CO-PARTITION teeth: producer and consumer route to the same worker
    // through the WIDE arm of worker_for_pk_bytes.
    let p_consumer = crate::schema::worker_for_pk_bytes(consumer, NW);
    let p_producer = crate::schema::worker_for_pk_bytes(producer, NW);
    let p_oracle = crate::schema::worker_for_pk_bytes(oracle.as_slice(), NW);
    assert_eq!(p_producer, p_consumer, "producer/consumer co-partition (wide)");
    assert_eq!(p_consumer, p_oracle, "trace store / ingest co-partition (wide)");

    // (3) WIDE-ARM FORMULA pin: the owner is the multiply-shift re-bucketing
    // of XXH3-64 over the OPK bytes, recomputed here. A forked seed or shift
    // would still keep producer == consumer — both call the same function —
    // so only an independent reference catches it.
    let expected = ((gnitz_wire::checksum(consumer) as u128 * NW as u128) >> 64) as usize;
    assert_eq!(p_consumer, expected, "wide owner == ((xxh3_64(opk) * W) >> 64)");
    assert!(expected < NW, "the owner is a launched worker");
}

#[test]
fn test_reindex_packer_null_key_determinism() {
    // A NULL value in a nullable (unsigned) reindex key column is canonically
    // zeroed at the source; the packer reads those zeros (ignoring the null
    // bitmap) and OPK-encodes them. Two distinct rows both NULL in the key
    // column must pack that slot identically (all-zero for an unsigned key).
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U32, true), // nullable U32 key
        ],
        &[0],
    );
    let mut b = BatchBuilder::new(schema);
    // Row 0 and row 1: distinct PK, both NULL in col1 (slot zeroed, null bit set).
    for pk in [10u128, 20u128] {
        b.begin_row(pk, 1);
        b.put_null();
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();

    let packer = ReindexPacker::new(&schema, &[(1, TypeCode::U32)]).unwrap();
    assert_eq!(packer.out_stride, 4); // U32 key → 4-byte slot

    let mut buf0 = [0u8; crate::schema::MAX_PK_BYTES];
    let mut buf1 = [0u8; crate::schema::MAX_PK_BYTES];
    packer.pack_into(&mut buf0[..packer.out_stride], &mb, 0);
    packer.pack_into(&mut buf1[..packer.out_stride], &mb, 1);

    assert_eq!(&buf0[..4], &[0u8; 4], "NULL unsigned key slot is all-zero");
    assert_eq!(&buf0[..4], &buf1[..4], "two NULL-key rows pack identically");
}

// -----------------------------------------------------------------------
// ReindexPacker::new_group_key — the presence bitmap
// -----------------------------------------------------------------------

#[test]
fn test_group_key_bitmap_bit_positions() {
    // Two packed group columns, only the second nullable: the bitmap must set
    // **bit 1**, not bit 0. Getting it wrong silently merges a NULL group with
    // a `0` group, and the end-to-end coverage groups on one nullable column,
    // where every wrong position still lands on bit 0.
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false), // A: NOT NULL   → packed slot 0
            SchemaColumn::new(TypeCode::U32, true),  // B: nullable   → packed slot 1
        ],
        &[0],
    );
    // Row 0: B is NULL (payload slot 1 → null-word bit 1). Row 1: B == 0.
    let mut b = BatchBuilder::new(schema);
    for (pk, b_null) in [(10u128, true), (20u128, false)] {
        b.begin_row(pk, 1);
        b.put_int(7); // A, same in both rows
        match b_null {
            true => b.put_null(), // B: the canonical zero under a NULL
            false => b.put_int(0),
        }
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();

    let packer = ReindexPacker::new_group_key(&schema, &[1, 2], &[])
        .expect("integer group columns")
        .0;
    assert_eq!(packer.out_stride, 1 + 8 + 4, "bitmap ++ I64 slot ++ U32 slot");

    let mut null_row = [0u8; crate::schema::MAX_PK_BYTES];
    let mut zero_row = [0u8; crate::schema::MAX_PK_BYTES];
    packer.pack_into(&mut null_row[..packer.out_stride], &mb, 0);
    packer.pack_into(&mut zero_row[..packer.out_stride], &mb, 1);

    assert_eq!(null_row[0], 0b10, "NULL in packed column 1 sets bit 1, not bit 0");
    assert_eq!(zero_row[0], 0, "no NULL, no bits");
    // The NULL slot is zeroed and B == 0 encodes to zeros, so the bitmap byte
    // is the *only* thing separating a NULL group from a `0` group.
    assert_eq!(
        &null_row[1..packer.out_stride],
        &zero_row[1..packer.out_stride],
        "the two rows differ in nothing but the bitmap"
    );
    assert_ne!(
        &null_row[..packer.out_stride],
        &zero_row[..packer.out_stride],
        "a NULL group must not collide with a 0 group"
    );
}

/// A group key packs every byte of its slot, bitmap and fold included, so
/// `pack_rows` over a reused buffer leaves nothing of the previous rows.
#[test]
fn a_group_key_overwrites_its_whole_slot() {
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend([SchemaColumn::new(TypeCode::I32, true); 6]);
    let schema = SchemaDescriptor::new(&cols, &[0]);
    let mut b = BatchBuilder::new(schema);
    for row in 0..40u64 {
        b.begin_row(row as u128, 1);
        for c in 0..6 {
            match (row + c) % 5 {
                0 => b.put_null(),
                v => b.put_int(v as u128),
            }
        }
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();
    // Two packed columns under a bitmap, and a key whose tail folds.
    for group in [&[1u32, 2][..], &[1, 2, 3, 4, 5, 6]] {
        let packer = ReindexPacker::new_group_key(&schema, group, &[]).unwrap().0;
        let stride = packer.out_stride;
        let mut dirty = vec![0xA5u8; mb.count * stride];
        packer.pack_rows(&mut dirty, stride, &mb, 0, mb.count);
        for (row, key) in dirty.chunks_exact(stride).enumerate() {
            let mut clean = vec![0u8; stride];
            packer.pack_into(&mut clean, &mb, row);
            assert_eq!(key, &clean[..], "{group:?} row {row}");
        }
    }
}

// -----------------------------------------------------------------------
// ReindexPacker::pack_into — property test over every PK-eligible type
// -----------------------------------------------------------------------

mod pack_proptest {
    use super::*;
    use crate::test_support::{arb_pk_type, pk_only_schema};
    use proptest::prelude::*;

    /// A legal promoted slot type: the widest slot of the source's own
    /// signedness. For the already-widest codes that is the self-typed slot, so
    /// the two passes below are always legal, not always distinct.
    fn promoted_type(tc: TypeCode) -> TypeCode {
        use TypeCode as T;
        match tc {
            TypeCode::U8 | TypeCode::U16 | TypeCode::U32 | TypeCode::U64 => T::U64,
            TypeCode::I8
            | TypeCode::I16
            | TypeCode::I32
            | TypeCode::I64
            | TypeCode::Date
            | TypeCode::Timestamp
            | TypeCode::Decimal => T::I64,
            TypeCode::I128 => T::I128,
            _ => T::U128, // U128, UUID
        }
    }

    /// `(column type codes, one native-LE value per column)`. 1..=MAX_PK_COLUMNS
    /// columns over every PK-eligible code, at every width combination — the
    /// surface the running-sum slot offsets live on.
    fn arb_key_case() -> impl Strategy<Value = (Vec<TypeCode>, Vec<Vec<u8>>)> {
        prop::collection::vec(arb_pk_type(), 1..=crate::schema::MAX_PK_COLUMNS).prop_flat_map(|types| {
            let vals: Vec<_> = types
                .iter()
                .map(|&t| prop::collection::vec(any::<u8>(), t.wire_stride()))
                .collect();
            (Just(types), vals)
        })
    }

    proptest! {
        /// At every arity and every PK-eligible type, self-typed and promoted:
        /// each slot equals its own wire encoder's output at the offset the
        /// running sum put it, **and** the two placements agree with each
        /// other. The second does not follow from the first — PK and payload
        /// placement go through different primitives — and it is the
        /// co-partition contract this file exists for.
        #[test]
        fn reindex_pack_matches_wire_encoders((types, vals) in arb_key_case()) {
            // Payload placement: [U64 PK, c0 .. cn-1], key = the payload columns.
            let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
            cols.extend(types.iter().map(|&tc| SchemaColumn::new(tc, false)));
            let pay_schema = SchemaDescriptor::new(&cols, &[0]);
            let mut pb = BatchBuilder::new(pay_schema);
            pb.begin_row(1, 1);
            for v in &vals {
                pb.put_int(le_cell(v));
            }
            pb.end_row();
            let pb = pb.finish();
            let pay_mb = pb.as_mem_batch();

            // PK placement: the same columns, all of them PK columns, OPK at rest.
            let pk_schema = pk_only_schema(&types);
            let mut opk = Vec::new();
            for (i, v) in vals.iter().enumerate() {
                let mut slot = vec![0u8; v.len()];
                gnitz_wire::encode_pk_column(v, types[i], &mut slot);
                opk.extend_from_slice(&slot);
            }
            let mut kb = BatchBuilder::new(pk_schema);
            kb.begin_row_bytes(&opk, 1i64);
            kb.end_row();
            let kb = kb.finish();
            let pk_mb = kb.as_mem_batch();

            for promoted in [false, true] {
                let target = |tc: TypeCode| if promoted { promoted_type(tc) } else { tc.reindex_output_type() };
                let pay_key: Vec<gnitz_wire::ReindexSlot> =
                    types.iter().enumerate().map(|(i, &tc)| (i as u32 + 1, target(tc))).collect();
                let pk_key: Vec<gnitz_wire::ReindexSlot> =
                    types.iter().enumerate().map(|(i, &tc)| (i as u32, target(tc))).collect();
                let pay_packer = ReindexPacker::new(&pay_schema, &pay_key).unwrap();
                let pk_packer = ReindexPacker::new(&pk_schema, &pk_key).unwrap();
                let stride = pay_packer.out_stride;
                prop_assert_eq!(stride, pk_packer.out_stride);

                let mut pay_buf = [0u8; crate::schema::MAX_PK_BYTES];
                let mut pk_buf = [0u8; crate::schema::MAX_PK_BYTES];
                pay_packer.pack_into(&mut pay_buf[..stride], &pay_mb, 0);
                pk_packer.pack_into(&mut pk_buf[..stride], &pk_mb, 0);

                // (1) Absolute.
                let mut off = 0usize;
                for (i, &tc) in types.iter().enumerate() {
                    let out_tc = target(tc);
                    let w = out_tc.wire_stride();
                    let src_w = tc.wire_stride();

                    let mut native = [0u8; 16];
                    native[..src_w].copy_from_slice(&vals[i]);
                    let image = gnitz_wire::key_image(tc, u128::from_le_bytes(native));
                    let want = gnitz_wire::encode_pk_images([(tc, out_tc, image)]);
                    prop_assert_eq!(&pay_buf[off..off + w], want.pk_bytes(), "payload slot {}", i);
                    prop_assert_eq!(&pk_buf[off..off + w], want.pk_bytes(), "pk slot {}", i);

                    off += w;
                }
                prop_assert_eq!(off, stride, "slot widths sum to out_stride");

                // (2) Cross-placement.
                prop_assert_eq!(&pay_buf[..stride], &pk_buf[..stride]);
            }
        }

        /// `pack_rows` is `pack_into` row by row, over a window at an arbitrary
        /// start, for both placements, self-typed and promoted.
        #[test]
        fn pack_rows_is_pack_into_per_row(
            (types, rows, bytes, start) in prop::collection::vec(arb_pk_type(), 1..=crate::schema::MAX_PK_COLUMNS)
                .prop_flat_map(|types| {
                    let width: usize = types.iter().map(|t| t.wire_stride()).sum();
                    (Just(types), 1usize..600).prop_flat_map(move |(types, rows)| {
                        (Just(types), Just(rows), prop::collection::vec(any::<u8>(), rows * width), 0..rows)
                    })
                })
        ) {
            let widths: Vec<usize> = types.iter().map(|t| t.wire_stride()).collect();
            let row_width: usize = widths.iter().sum();
            let cell = |r: usize, i: usize| {
                let at = r * row_width + widths[..i].iter().sum::<usize>();
                &bytes[at..at + widths[i]]
            };
            let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
            cols.extend(types.iter().map(|&tc| SchemaColumn::new(tc, false)));
            let pay_schema = SchemaDescriptor::new(&cols, &[0]);
            let pk_schema = pk_only_schema(&types);
            let (mut pb, mut kb) = (BatchBuilder::new(pay_schema), BatchBuilder::new(pk_schema));
            for r in 0..rows {
                pb.begin_row(r as u128, 1);
                let mut opk = Vec::new();
                for (i, &tc) in types.iter().enumerate() {
                    pb.put_int(le_cell(cell(r, i)));
                    let mut slot = vec![0u8; widths[i]];
                    gnitz_wire::encode_pk_column(cell(r, i), tc, &mut slot);
                    opk.extend_from_slice(&slot);
                }
                pb.end_row();
                kb.begin_row_bytes(&opk, 1);
                kb.end_row();
            }
            let (pb, kb) = (pb.finish(), kb.finish());
            let n = rows - start;
            for promoted in [false, true] {
                let target = |tc: TypeCode| if promoted { promoted_type(tc) } else { tc.reindex_output_type() };
                for (schema, batch, first) in [(&pay_schema, &pb, 1u32), (&pk_schema, &kb, 0)] {
                    let key: Vec<gnitz_wire::ReindexSlot> =
                        types.iter().enumerate().map(|(i, &tc)| (i as u32 + first, target(tc))).collect();
                    let packer = ReindexPacker::new(schema, &key).unwrap();
                    let stride = packer.out_stride;
                    let mb = batch.as_mem_batch();
                    let mut got = vec![0xA5u8; n * stride];
                    packer.pack_rows(&mut got, stride, &mb, start, n);
                    let mut want = vec![0u8; n * stride];
                    for (i, key) in want.chunks_exact_mut(stride).enumerate() {
                        packer.pack_into(key, &mb, start + i);
                    }
                    prop_assert_eq!(got, want);
                }
            }
        }
    }
}

/// Release-only microbench for `pack_rows` in `KEY_CHUNK`-row chunks, as
/// `for_each_key` runs it, over join- and group-key shapes.
/// `cd crates && cargo test -p gnitz-store --release reindex_pack_bench -- --ignored --nocapture --test-threads=1`
/// `REINDEX_PACK=<name>` times one shape alone, for `perf stat`.
#[test]
#[ignore]
fn reindex_pack_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    const N: usize = 1_000_000;
    const ITERS: usize = 20;

    let join_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
    let mut jb = BatchBuilder::new(join_schema);
    for i in 0..N as u64 {
        jb.begin_row(i as u128, 1i64);
        jb.put_int((i.wrapping_mul(2_654_435_761)) as u128);
        jb.put_int((i.wrapping_mul(0x9E37_79B9_7F4A_7C15)) as u128);
        jb.put_int((!i) as u128);
        jb.put_int((i as i32).wrapping_mul(-3) as u32 as u128);
        jb.put_blob(format!("key-{}", i % 50_000).as_bytes());
        jb.end_row();
    }
    let jb = jb.finish();
    let jmb = jb.as_mem_batch();
    let join3 = ReindexPacker::new(
        &join_schema,
        &[(1, TypeCode::U64), (2, TypeCode::U64), (3, TypeCode::U64)],
    )
    .unwrap();
    assert_eq!(join3.out_stride, 24);
    let promoted = ReindexPacker::new(&join_schema, &[(4, TypeCode::I64)]).unwrap();
    let string = ReindexPacker::new(&join_schema, &[(5, TypeCode::U128)]).unwrap();

    // --- Nullable 2-column group key: [U64 PK, I64, U32 NULL], group on (1, 2).
    let grp_schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::U32, true),
        ],
        &[0],
    );
    let mut gb = BatchBuilder::new(grp_schema);
    for i in 0..N as u64 {
        gb.begin_row(i as u128, 1);
        gb.put_int((i as i64).wrapping_mul(-7) as u128);
        // Every 8th row is NULL in the nullable group column.
        match i % 8 {
            0 => gb.put_null(),
            _ => gb.put_int(i as u32 as u128),
        }
        gb.end_row();
    }
    let gb = gb.finish();
    let gmb = gb.as_mem_batch();
    let group2 = ReindexPacker::new_group_key(&grp_schema, &[1, 2], &[])
        .expect("integer group columns")
        .0;

    for (name, packer, mb) in [
        ("join3", &join3, &jmb),
        ("promoted-i32", &promoted, &jmb),
        ("string", &string, &jmb),
        ("group2-nullable", &group2, &gmb),
    ] {
        if std::env::var("REINDEX_PACK").is_ok_and(|o| o != name) {
            continue;
        }
        let stride = packer.out_stride;
        let mut buf = vec![0u8; KEY_CHUNK * stride];
        // Warm up.
        packer.pack_rows(&mut buf, stride, mb, 0, KEY_CHUNK);

        let t = Instant::now();
        let mut acc = 0u64;
        for _ in 0..ITERS {
            for start in (0..N).step_by(KEY_CHUNK) {
                let n = (N - start).min(KEY_CHUNK);
                packer.pack_rows(&mut buf, stride, mb, start, n);
                acc = acc.wrapping_add(black_box(buf[0]) as u64);
            }
        }
        let secs = t.elapsed().as_secs_f64();
        println!(
            "reindex_pack_bench[{name}]: {:.1} Mrows/s ({N} rows x {ITERS} iters in {secs:.3}s, stride {stride}, checksum {acc})",
            (N * ITERS) as f64 / secs / 1e6,
        );
    }
}
