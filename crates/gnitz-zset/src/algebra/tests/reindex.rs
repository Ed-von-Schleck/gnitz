use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{ColumnTable, SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{le_cell, make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string, u64_pk_schema};

/// `packer`'s key for each of the first `n` rows of `src`, through `pack_rows`.
fn packed_keys<B: BatchView>(packer: &ReindexPacker, src: &B, n: usize) -> Vec<Vec<u8>> {
    let stride = packer.out_stride;
    let mut region = vec![0u8; n * stride];
    packer.pack_rows(&mut region, stride, src, &[(0, n)]);
    region.chunks(stride).map(<[u8]>::to_vec).collect()
}

/// `native`, a `tc` value in its low bytes, sign- or zero-extended to 128 bits.
fn extended(tc: TypeCode, native: u128) -> u128 {
    let shift = 128 - 8 * tc.wire_stride() as u32;
    match tc.is_signed_int() {
        true => ((native << shift) as i128 >> shift) as u128,
        false => native << shift >> shift,
    }
}

/// The OPK bytes of `tc`'s value `native` in a slot of type `target`.
fn slot_bytes(tc: TypeCode, native: u128, target: TypeCode) -> Vec<u8> {
    let mut slot = vec![0u8; target.wire_stride()];
    gnitz_wire::store_opk(&mut slot, extended(tc, native), target.is_signed_int());
    slot
}

/// `promote_image` re-biases a column's image to the slot type's: the bytes the
/// value itself encodes to there, for every promotion the engine performs — an
/// FK's domain widening and a join key's packing at its common slot.
#[test]
fn promote_image_is_the_values_image_at_the_slot_type() {
    const PATTERNS: &[u128] = &[
        0,
        1,
        2,
        0x7F,
        0x80,
        0xFF,
        0x100,
        i64::MAX as u128,
        1 << 63,
        u64::MAX as u128,
        1 << 64,
        i128::MAX as u128,
        1 << 127,
        u128::MAX,
    ];
    let pk_types: Vec<TypeCode> = TypeCode::ALL.iter().copied().filter(|t| t.is_pk_eligible()).collect();
    for &src in &pk_types {
        for &target in &pk_types {
            if !(src == target || src.int_domain_fits(target) || src.packs_at(target)) {
                continue;
            }
            for &p in PATTERNS {
                let mut got = vec![0u8; target.wire_stride()];
                let image = promote_image(gnitz_wire::key_image(src, p), src, target);
                gnitz_wire::store_opk(&mut got, image, false);
                assert_eq!(got, slot_bytes(src, p, target), "src={src} target={target} p={p:#x}");
            }
        }
    }
}

/// Equal values on both sides of an accepted join key pair pack byte-identically
/// at the common slot, so they co-partition and match; and distinct values never
/// collide — including the native-byte aliasing trap (U8 255 and I8 -1 share all
/// 0xFF native bytes).
#[test]
fn every_accepted_join_key_pair_copartitions() {
    const PROBES: &[i128] = &[
        i128::MIN,
        i64::MIN as i128,
        i32::MIN as i128,
        -32768,
        -129,
        -128,
        -56,
        -1,
        0,
        1,
        127,
        128,
        200,
        255,
        256,
        32767,
        65535,
        i32::MAX as i128,
        u32::MAX as i128,
        i64::MAX as i128,
        u64::MAX as i128,
        i128::MAX,
    ];
    /// `[min, max]` of a key type's values; a UUID keys as the U128 it is stored as.
    fn key_bounds(tc: TypeCode) -> (i128, u128) {
        match (tc.wire_stride(), tc.is_signed_int()) {
            (16, true) => (i128::MIN, i128::MAX as u128),
            (16, false) => (0, u128::MAX),
            (w, true) => (-(1i128 << (8 * w - 1)), (1u128 << (8 * w - 1)) - 1),
            (w, false) => (0, (1u128 << (8 * w)) - 1),
        }
    }
    let pk: Vec<TypeCode> = TypeCode::ALL.iter().copied().filter(|t| t.is_pk_eligible()).collect();
    for &l in &pk {
        for &r in &pk {
            let Ok(t) = l.join_key_common_type(r) else { continue };
            let keys: Vec<(i128, Vec<u8>)> = [l, r]
                .into_iter()
                .flat_map(|tc| {
                    let (lo, hi) = key_bounds(tc);
                    PROBES
                        .iter()
                        .filter(move |&&v| lo <= v && (v < 0 || v as u128 <= hi))
                        .map(move |&v| {
                            let mut key = vec![0u8; t.wire_stride()];
                            let image = promote_image(gnitz_wire::key_image(tc, v as u128), tc, t);
                            gnitz_wire::store_opk(&mut key, image, false);
                            (v, key)
                        })
                })
                .collect();
            for (va, ka) in &keys {
                for (vb, kb) in &keys {
                    assert_eq!(ka == kb, va == vb, "{l}/{r} at {t}: {va} vs {vb}");
                }
            }
        }
    }
}

// -----------------------------------------------------------------------
// FoldCols::key_row — its two byte layouts
// -----------------------------------------------------------------------

/// Under either layout two rows fold to one key exactly when they agree on
/// every folded column, a NULL told from a zero; the fold of no columns is V₀.
#[test]
fn each_fold_layout_keys_a_row_by_its_columns() {
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
    // One PK, and every combination of the payload values: a NULL beside a zero
    // in each nullable column.
    let mut rows: Vec<[Option<u128>; 4]> = Vec::new();
    for a in [0, (-9i32) as u32 as u128] {
        for b in [None, Some(0), Some(42)] {
            for c in [0, u128::MAX - 3] {
                for d in [None, Some(0), Some(5)] {
                    rows.push([Some(a), b, Some(c), d]);
                }
            }
        }
    }
    let mut b = BatchBuilder::new(&schema);
    for row in &rows {
        b.begin_row(7, 1);
        row.iter().for_each(|&cell| b.put_opt_int(cell));
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();

    let all: Vec<ColumnLocator> = (0..5).map(|c| schema.locate(c)).collect();
    let empty = FoldCols::new(Vec::new());
    assert_eq!(
        empty.key_row(&mb, 0, mb.get_null_word(0)),
        gnitz_wire::global_group_key()
    );
    for k in 1..all.len() {
        let stack = FoldCols::new(all[1..=k].to_vec());
        assert!(stack.inline, "arity {k} is fixed-width and fits the stack buffer");
        for (arm, f) in [("stack", &stack), ("scratch", &flipped(&stack))] {
            let key = |row: usize| f.key_row(&mb, row, mb.get_null_word(row));
            for x in 0..rows.len() {
                for y in 0..rows.len() {
                    assert_eq!(
                        key(x) == key(y),
                        rows[x][..k] == rows[y][..k],
                        "{arm}, arity {k}: {:?} against {:?}",
                        rows[x],
                        rows[y]
                    );
                }
            }
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
        let mut b = BatchBuilder::new(&schema);
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
    let mut b = BatchBuilder::new(&schema);
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
    let mut b = BatchBuilder::new(&schema);
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
        packer.pack_rows(&mut dirty, stride, &mb, &[(0, mb.count)]);
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
        /// an arbitrary start, whole or cut into two runs around a skipped row,
        /// writes each row's key as `pack_into` does, and
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
            let (mut pb, mut kb) = (BatchBuilder::new(&pay_schema), BatchBuilder::new(&pk_schema));
            for r in 0..rows {
                let natives: Vec<u128> = (0..types.len()).map(|i| le_cell(cell(r, i))).collect();
                pb.begin_row(r as u128, 1);
                for &v in &natives {
                    pb.put_int(v);
                }
                pb.end_row();
                kb.begin_row_natives(&natives, 1);
                kb.end_row();
            }
            let (pb, kb) = (pb.finish(), kb.finish());
            let n = rows - start;
            for promoted in [false, true] {
                let target = |tc: TypeCode| if promoted { promoted_type(tc) } else { tc.reindex_output_type() };
                let want: Vec<u8> = (start..rows)
                    .flat_map(|r| (0..types.len()).map(move |i| (r, i)))
                    .flat_map(|(r, i)| slot_bytes(types[i], le_cell(cell(r, i)), target(types[i])))
                    .collect();
                for (schema, batch, first) in [(&pay_schema, &pb, 1u32), (&pk_schema, &kb, 0)] {
                    let key: Vec<gnitz_wire::ReindexSlot> =
                        types.iter().enumerate().map(|(i, &tc)| (i as u32 + first, target(tc))).collect();
                    let packer = ReindexPacker::new(schema, &key).unwrap();
                    let stride = packer.out_stride;
                    prop_assert_eq!(n * stride, want.len());
                    let mb = batch.as_mem_batch();
                    let mut got = vec![0xA5u8; n * stride];
                    packer.pack_rows(&mut got, stride, &mb, &[(start, rows)]);
                    prop_assert_eq!(&got, &want, "first key column {}", first);
                    if n >= 3 {
                        // Every row but the window's second.
                        let mut cut = vec![0xA5u8; (n - 1) * stride];
                        packer.pack_rows(&mut cut, stride, &mb, &[(start, start + 1), (start + 2, rows)]);
                        prop_assert_eq!(&cut[..stride], &want[..stride]);
                        prop_assert_eq!(&cut[stride..], &want[2 * stride..]);
                    }
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

/// Over PKs of every suffix width and a nullable indexed column of every
/// indexable width: the projection holds exactly the rows `write_span` admits,
/// each as its span followed by its own PK, in source order — and `append_spans`
/// the same rows' spans, zero to the end of a wider slot.
#[test]
fn index_entries_are_each_admitted_rows_span_and_pk() {
    use crate::repr::BatchBuilder;
    use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode, MAX_PK_BYTES};
    use TypeCode::{I128, I16, I64, U128, U32, U64, U8};
    let mut rng = crate::test_support::Rng::new(0x1dc5);
    let pks: [&[TypeCode]; 6] = [&[U32, I64], &[U32], &[U64], &[U128], &[U64, U128], &[U128, U128]];
    for (pk, tc) in pks
        .into_iter()
        .flat_map(|pk| [U8, I16, U32, I64, U128, I128].map(|tc| (pk, tc)))
    {
        let k = pk.len() as u32;
        let mut cols: Vec<SchemaColumn> = pk.iter().map(|&t| SchemaColumn::new(t, false)).collect();
        cols.extend([SchemaColumn::new(tc, true), SchemaColumn::new(TypeCode::I32, false)]);
        let owner = SchemaDescriptor::new(&cols, &(0..k).collect::<Vec<_>>());
        let width = tc.wire_stride();
        let mut b = BatchBuilder::new(&owner);
        for row in 0..300u64 {
            // Runs of live rows broken by ghosts and by NULLs.
            let weight = [1, 1, 1, 0, -2, 1][(row % 6) as usize];
            let mut natives = vec![row as u128];
            natives.extend((1..k).map(|_| rng.next_u64() as u128));
            b.begin_row_natives(&natives, weight);
            let v = rng.gen_u128();
            match rng.gen_range(5) {
                0 => b.put_null(),
                _ if width == 16 => b.put_int(v),
                _ => b.put_int(v & ((1u128 << (8 * width)) - 1)),
            }
            b.put_int(row as u128);
            b.end_row();
        }
        let src = b.finish();
        let mb = src.as_mem_batch();
        for indexed in [&[k][..], &[k, k + 1], &[k - 1, k]] {
            let (spec, idx_schema) = crate::schema::index_spec_and_schema(indexed, &owner).unwrap();
            let mut want: Vec<(Vec<u8>, i64)> = Vec::new();
            for row in 0..src.len() {
                let mut e = [0u8; MAX_PK_BYTES];
                if src.get_weight(row) != 0 && spec.write_span(&mb, row, &mut e) {
                    let mut e = e[..spec.key_size()].to_vec();
                    e.extend_from_slice(src.get_pk_bytes(row));
                    want.push((e, src.get_weight(row)));
                }
            }
            let got = index_entries(&src, &spec, &idx_schema);
            let got: Vec<(Vec<u8>, i64)> = (0..got.len())
                .map(|r| (got.get_pk_bytes(r).to_vec(), got.get_weight(r)))
                .collect();
            assert_eq!(got, want, "{pk:?} {tc} indexed on {indexed:?}");

            let (key_size, slot) = (spec.key_size(), spec.key_size().next_multiple_of(8));
            let mut spans = vec![0xEEu8; 3];
            append_spans(&mut spans, slot, &mb, &spec, |w| w > 0);
            let want_spans: Vec<u8> = want
                .iter()
                .filter(|(_, w)| *w > 0)
                .flat_map(|(e, _)| {
                    let mut record = e[..key_size].to_vec();
                    record.resize(slot, 0);
                    record
                })
                .collect();
            assert_eq!(spans[..3], [0xEE; 3], "{pk:?} {tc} indexed on {indexed:?}");
            assert_eq!(spans[3..], want_spans, "{pk:?} {tc} indexed on {indexed:?}");
        }
    }
}

/// A nullable group key packs the same bytes a column at a time as a row at a
/// time, at every integer width, signed and unsigned.
#[test]
fn a_nullable_group_key_packs_by_column_as_by_row() {
    use crate::repr::BatchBuilder;
    use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode, MAX_PK_BYTES};
    let tcs = [
        TypeCode::U8,
        TypeCode::I8,
        TypeCode::U16,
        TypeCode::I16,
        TypeCode::U32,
        TypeCode::I32,
        TypeCode::U64,
        TypeCode::I64,
        TypeCode::U128,
        TypeCode::I128,
    ];
    let mut rng = crate::test_support::Rng::new(0xb17);
    for pair in tcs.windows(2) {
        for nullable in [[true, true], [true, false], [false, true]] {
            let cols = [
                SchemaColumn::new(TypeCode::U64, false),
                SchemaColumn::new(pair[0], nullable[0]),
                SchemaColumn::new(pair[1], nullable[1]),
            ];
            let schema = SchemaDescriptor::new(&cols, &[0]);
            const N: usize = 700;
            let mut b = BatchBuilder::new(&schema);
            for row in 0..N {
                b.begin_row(row as u128, 1);
                for (col, nullable) in cols[1..].iter().zip(nullable) {
                    let v = rng.gen_u128();
                    let w = col.size() as usize;
                    match nullable && rng.gen_range(4) == 0 {
                        true => b.put_null(),
                        false if w == 16 => b.put_int(v),
                        false => b.put_int(v & ((1u128 << (8 * w)) - 1)),
                    }
                }
                b.end_row();
            }
            let batch = b.finish();
            let mb = batch.as_mem_batch();
            let (packer, _) = ReindexPacker::new_group_key(&schema, &[1, 2], &[]).unwrap();
            assert!(packer.has_bitmap && packer.packs_by_column());
            // Wider than the key, as an index entry's slot is.
            let stride = packer.out_stride + 3;
            let mut by_col = vec![0xEEu8; N * stride];
            // Two runs around a skipped row.
            let runs = [(0, 300), (301, N)];
            let mut cut = vec![0xEEu8; (N - 1) * stride];
            packer.pack_rows(&mut cut, stride, &mb, &runs);
            for (key, row) in cut.chunks_exact(stride).zip((0..300).chain(301..N)) {
                let mut by_row = [0u8; MAX_PK_BYTES];
                let want = packer.pack_prefix(&mut by_row, &mb, row);
                assert_eq!(&key[..packer.out_stride], want, "{pair:?} {nullable:?} cut row {row}");
            }
            packer.pack_rows(&mut by_col, stride, &mb, &[(0, N)]);
            for (row, key) in by_col.chunks_exact(stride).enumerate() {
                let mut by_row = [0u8; MAX_PK_BYTES];
                let want = packer.pack_prefix(&mut by_row, &mb, row);
                assert_eq!(&key[..packer.out_stride], want, "{pair:?} {nullable:?} row {row}");
            }
        }
    }
}
