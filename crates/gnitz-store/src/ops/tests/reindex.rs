use super::*;
use crate::schema::{ColumnTable, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::BatchBuilder;
use crate::test_support::{le_cell, make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string, u64_pk_schema};

/// `packer`'s key for each of the first `n` rows of `src`, through `pack_rows`.
fn packed_keys<B: BatchView>(packer: &ReindexPacker, src: &B, n: usize) -> Vec<Vec<u8>> {
    let stride = packer.out_stride;
    let mut region = vec![0u8; n * stride];
    packer.pack_rows(&mut region, stride, src, 0, n);
    region.chunks(stride).map(<[u8]>::to_vec).collect()
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
        assert!(f.inline, "arity {k} is fixed-width and fits the stack buffer");
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

/// The synthetic PK is one slot per key column at its slot type — a
/// content-hash key a U128 — packed at the slots' summed width, and the kept
/// input columns follow it as payload.
#[test]
fn packer_output_schema_is_the_key_slots_then_the_kept_columns() {
    use TypeCode::*;
    /// `(input payload types after a U64 PK, key slots, kept columns, output types)`.
    type Case = (
        &'static [TypeCode],
        &'static [gnitz_wire::ReindexSlot],
        &'static [u32],
        &'static [TypeCode],
    );
    let cases: [Case; 7] = [
        (&[I32], &[(1, I32)], &[0, 1], &[I32, U64, I32]),
        (&[U16], &[(1, U16)], &[1], &[U16, U16]),
        (&[String], &[(1, U128)], &[0, 1], &[U128, U64, String]),
        (&[Blob], &[(1, U128)], &[1], &[U128, Blob]),
        (&[UUID], &[(1, U128)], &[1], &[U128, UUID]),
        (
            &[I32, U128],
            &[(1, I32), (2, U128)],
            &[0, 1, 2],
            &[I32, U128, U64, I32, U128],
        ),
        (&[I32, I64, I16], &[(1, I64), (2, I64)], &[0, 3], &[I64, I64, U64, I16]),
    ];
    for (payload, key, keep, want) in cases {
        let mut cols = vec![SchemaColumn::new(U64, false)];
        cols.extend(payload.iter().map(|&tc| SchemaColumn::new(tc, false)));
        let in_schema = SchemaDescriptor::new(&cols, &[0]);
        let packer = ReindexPacker::new(&in_schema, key).unwrap();
        let out = packer.output_schema(&in_schema, keep).unwrap();
        let types: Vec<TypeCode> = (0..out.num_columns()).map(|c| out.columns[c].type_code).collect();
        assert_eq!(types, want, "{payload:?} keyed on {key:?}");
        assert_eq!(out.pk_cols(), (0..key.len() as u32).collect::<Vec<_>>(), "{payload:?}");
        let slots: usize = key.iter().map(|&(_, t)| t.wire_stride()).sum();
        assert_eq!((out.pk_stride(), packer.out_stride), (slots, slots), "{payload:?}");
    }
    // A float key is refused: its raw bits are equality-incorrect (±0.0).
    for float_tc in [F32, F64] {
        let in_schema = u64_pk_schema(SchemaColumn::new(float_tc, false));
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

// -----------------------------------------------------------------------
// ReindexPacker — the content-hash arm
// -----------------------------------------------------------------------

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

    proptest! {
        /// At every arity and every PK-eligible type, self-typed and promoted,
        /// and in both placements — the key columns as payload and as the PK,
        /// which go through different primitives — `pack_rows` over a window at
        /// an arbitrary start writes each row's key as `pack_into` does, and
        /// each slot as its own wire encoder does at the offset the running sum
        /// put it. The placements agreeing is the co-partition contract.
        #[test]
        fn pack_rows_is_the_wire_encoding_in_both_placements(
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
                let natives: Vec<u128> = (0..types.len()).map(|i| le_cell(cell(r, i))).collect();
                pb.begin_row(r as u128, 1);
                for &v in &natives {
                    pb.put_int(v);
                }
                pb.end_row();
                kb.begin_row_opk(&natives, 1);
                kb.end_row();
            }
            let (pb, kb) = (pb.finish(), kb.finish());
            let n = rows - start;
            for promoted in [false, true] {
                let target = |tc: TypeCode| if promoted { promoted_type(tc) } else { tc.reindex_output_type() };
                let want: Vec<u8> = (start..rows)
                    .flat_map(|r| {
                        let images = types.iter().enumerate().map(|(i, &tc)| {
                            (tc, target(tc), gnitz_wire::key_image(tc, le_cell(cell(r, i))))
                        });
                        gnitz_wire::encode_pk_images(images).pk_bytes().to_vec()
                    })
                    .collect();
                for (schema, batch, first) in [(&pay_schema, &pb, 1u32), (&pk_schema, &kb, 0)] {
                    let key: Vec<gnitz_wire::ReindexSlot> =
                        types.iter().enumerate().map(|(i, &tc)| (i as u32 + first, target(tc))).collect();
                    let packer = ReindexPacker::new(schema, &key).unwrap();
                    let stride = packer.out_stride;
                    prop_assert_eq!(n * stride, want.len());
                    let mb = batch.as_mem_batch();
                    let mut got = vec![0xA5u8; n * stride];
                    packer.pack_rows(&mut got, stride, &mb, start, n);
                    prop_assert_eq!(&got, &want, "first key column {}", first);
                    // An identity key is each column's own OPK image, end to end:
                    // every unpromoted one, which the scatter then routes unpacked.
                    let own = packer.identity_columns().map(|locs| -> Vec<u8> {
                        (start..rows)
                            .flat_map(|r| locs.iter().map(move |l| (r, *l)))
                            .flat_map(|(r, l)| l.opk_image(&mb, r).to_be_bytes()[16 - l.size()..].to_vec())
                            .collect()
                    });
                    prop_assert!(own.is_some() || promoted, "an unpromoted key is an identity");
                    prop_assert!(own.is_none_or(|own| own == want), "first key column {}", first);
                    for (i, row) in want.chunks_exact(stride).enumerate() {
                        let mut one = vec![0u8; stride];
                        packer.pack_into(&mut one, &mb, start + i);
                        prop_assert_eq!(&one[..], row);
                    }
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
