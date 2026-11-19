use super::*;
use crate::repr::{BatchBuilder, MemBatch};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    STRS, edge_native, le_cell, make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string, random_schema,
    u64_pk_schema,
};
use gnitz_foundation::rng::Rng;
use gnitz_wire::TypeCode;

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
    gnitz_wire::store_opk(&mut slot, extended(tc, native) ^ gnitz_wire::opk_bias(target));
    slot
}

/// Two rows fold to one key exactly when they agree on every folded column, a
/// NULL told from a zero and from an empty string. Up to `FOLD_INLINE_COLS`
/// columns fold on the stack, a wider set through the scratch; a repeated
/// column shares its null bit.
#[test]
fn a_fold_keys_a_row_by_its_columns() {
    use TypeCode::*;
    let schema = u64_pk_schema(&[
        SchemaColumn::new(I32, false),
        SchemaColumn::new(U64, true),
        SchemaColumn::new(U128, false),
        SchemaColumn::new(U16, true),
        SchemaColumn::new(String, true),
    ]);
    // Every combination of the payload values: a NULL beside a zero in each
    // nullable column, the string as its index in `STRS`.
    let mut rows: Vec<[Option<u128>; 5]> = Vec::new();
    for a in [0, (-9i32) as u32 as u128] {
        for b in [None, Some(0), Some(42)] {
            for c in [0, u128::MAX - 3] {
                for d in [None, Some(0), Some(5)] {
                    for s in [None, Some(0), Some(8)] {
                        rows.push([Some(a), b, Some(c), d, s]);
                    }
                }
            }
        }
    }
    let mut b = BatchBuilder::new(&schema);
    for row in &rows {
        b.begin_row(7, 1);
        row[..4].iter().for_each(|&cell| {
            b.put_opt_int(cell);
        });
        match row[4] {
            Some(i) => {
                b.put_string(STRS[i as usize]);
            }
            None => {
                b.put_null();
            }
        }
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();

    for cols in [
        &[1][..],
        &[1, 2],
        &[1, 2, 3],
        &[1, 2, 3, 4],
        &[1, 2, 3, 4, 5],
        &[5, 2],
        &[1, 2, 3, 4, 1, 2],
    ] {
        let f = FoldCols::new(cols.iter().map(|&c| schema.locate(c)).collect());
        let keys: Vec<u128> = (0..rows.len()).map(|r| f.key_row(&mb, r)).collect();
        let vals = |r: usize| -> Vec<Option<u128>> { cols.iter().map(|&c| rows[r][c - 1]).collect() };
        for x in 0..rows.len() {
            for y in 0..rows.len() {
                assert_eq!(
                    keys[x] == keys[y],
                    vals(x) == vals(y),
                    "{cols:?}: {:?} against {:?}",
                    rows[x],
                    rows[y]
                );
            }
        }
    }
}

// -----------------------------------------------------------------------
// KeyPacker::output_schema — the layout the per-row packer writes through
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
        let cols: Vec<SchemaColumn> = payload.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
        let in_schema = u64_pk_schema(&cols);
        let packer = KeyPacker::new(&in_schema, key).unwrap();
        let out = packer.output_schema(&in_schema, keep).unwrap();
        let types: Vec<TypeCode> = out.columns().iter().map(|c| c.type_code).collect();
        assert_eq!(types, want, "{payload:?} keyed on {key:?}");
        assert_eq!(out.pk_cols(), (0..key.len() as u32).collect::<Vec<_>>(), "{payload:?}");
        let slots: usize = key.iter().map(|&(_, t)| t.wire_stride()).sum();
        assert_eq!((out.pk_stride(), packer.out_stride), (slots, slots), "{payload:?}");
    }
}

// -----------------------------------------------------------------------
// KeyPacker — the content-hash arm
// -----------------------------------------------------------------------

/// A STRING or BLOB key packs the big-endian `checksum_128` of its content,
/// the empty content included: the arm the property's integer types never take.
#[test]
fn a_content_key_packs_its_content_hash() {
    let contents: [&[u8]; 3] = [b"abc", b"", b"hello-world-xyz"];
    for schema in [make_schema_pk_u64_payload_string(), make_schema_pk_u64_payload_blob()] {
        let mut b = BatchBuilder::new(&schema);
        for (r, content) in contents.iter().enumerate() {
            b.begin_row(r as u128, 1);
            b.put_blob(content);
            b.end_row();
        }
        let b = b.finish();
        let packer = KeyPacker::new(&schema, &[(1, TypeCode::U128)]).unwrap();
        let want: Vec<u128> = contents.iter().map(|c| gnitz_wire::checksum_128(c)).collect();
        let mut got = vec![0; contents.len()];
        packer.identities_over(&b.as_mem_batch(), &[(0, contents.len())], &mut got);
        assert_eq!(got, want, "{}", schema.columns()[1].type_code);
    }
}

/// A key holding content-hashed slots — nullable and not, beside integer slots
/// and with columns past the budget folded — packs by `pack_rows` over windows
/// and dirty padded slots exactly as `pack_into` packs each row.
#[test]
fn pack_rows_hashes_content_slots_as_a_row_at_a_time() {
    use TypeCode::*;
    let schema = u64_pk_schema(&[
        SchemaColumn::new(I64, true),
        SchemaColumn::new(String, true),
        SchemaColumn::new(U32, false),
        SchemaColumn::new(String, false),
        SchemaColumn::new(Blob, true),
    ]);
    let rows = 300;
    let mut b = BatchBuilder::new(&schema);
    for r in 0..rows {
        b.begin_row(r as u128, 1);
        b.put_opt_int((r % 3 != 0).then_some((r as i64 - 150) as u64 as u128));
        match r % 4 {
            0 => {
                b.put_null();
            }
            k => {
                b.put_string(STRS[(r + k) % STRS.len()]);
            }
        }
        b.put_int(r as u128 * 7);
        b.put_string(STRS[r % STRS.len()]);
        match r % 5 {
            0 => {
                b.put_null();
            }
            _ => {
                b.put_blob(STRS[(r * 3) % STRS.len()].as_bytes());
            }
        }
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();
    let windows: [&[(usize, usize)]; 3] = [&[(0, rows)], &[(17, 18), (40, 299)], &[(5, 5), (250, rows)]];
    for group in [
        &[2][..],
        &[4],
        &[4, 3],
        &[2, 1],
        &[1, 2, 3],
        &[2, 4, 5],
        &[1, 2, 3, 4, 5],
    ] {
        let (packer, _) = KeyPacker::new_group_key(&schema, group, &[]).unwrap();
        for runs in windows {
            let want: Vec<usize> = runs.iter().flat_map(|&(s, e)| s..e).collect();
            for pad in [0, 3] {
                let stride = packer.out_stride + pad;
                let mut got = vec![0xA5u8; want.len() * stride];
                packer.pack_rows(&mut got, stride, &mb, runs);
                for (key, &row) in got.chunks_exact(stride).zip(&want) {
                    let mut one = vec![0u8; packer.out_stride];
                    packer.pack_into(&mut one, &mb, row);
                    assert_eq!(&key[..packer.out_stride], &one[..], "{group:?} row {row} pad {pad}");
                }
            }
        }
    }
}

// -----------------------------------------------------------------------
// KeyPacker::pack_into — property test over every PK-eligible type
// -----------------------------------------------------------------------

mod pack_proptest {
    use super::*;
    use crate::test_support::pk_only_schema;
    use gnitz_wire::{MAX_PK_BYTES, MAX_PK_COLUMNS};
    use proptest::prelude::*;
    use proptest::sample::Index;

    /// The PK-eligible type codes.
    fn arb_pk_type() -> impl Strategy<Value = TypeCode> {
        prop::sample::select(
            TypeCode::ALL
                .iter()
                .copied()
                .filter(|t| t.is_pk_eligible())
                .collect::<Vec<_>>(),
        )
    }

    /// `packer`'s keys of the rows of `runs`, packed by `pack_rows` over dirty
    /// slots `pad` bytes wider than the key and by `pack_prefix` one at a time,
    /// are each `want` of its row.
    fn assert_packs(
        packer: &KeyPacker,
        mb: &MemBatch,
        runs: &[(usize, usize)],
        want: impl Fn(usize) -> Vec<u8>,
        what: &str,
    ) -> Result<(), TestCaseError> {
        let rows: Vec<usize> = runs.iter().flat_map(|&(s, e)| s..e).collect();
        for pad in [0, 3] {
            let stride = packer.out_stride + pad;
            let mut got = vec![0xA5u8; rows.len() * stride];
            packer.pack_rows(&mut got, stride, mb, runs);
            for (key, &row) in got.chunks_exact(stride).zip(&rows) {
                prop_assert_eq!(
                    &key[..packer.out_stride],
                    &want(row)[..],
                    "{} row {} pad {}",
                    what,
                    row,
                    pad
                );
            }
        }
        for &row in &rows {
            let mut one = [0u8; MAX_PK_BYTES];
            prop_assert_eq!(
                packer.pack_prefix(&mut one, mb, row),
                &want(row)[..],
                "{} row {}",
                what,
                row
            );
        }
        Ok(())
    }

    proptest! {
        /// At every arity, every PK-eligible type and every slot type it packs
        /// at, and in both placements — the key columns as payload and as the
        /// PK, which go through different primitives — `pack_rows` over a window
        /// at an arbitrary start, whole or cut around a skipped row, writes each
        /// slot as the value's own wire encoding at the slot type. Both sides of
        /// a join key pair pack at their common slot, so equal values
        /// co-partition. A group key over the same columns, some nullable,
        /// packs a column at a time as a row at a time: the bitmap, the zeroed
        /// NULL slots and the fold of the columns past the budget.
        #[test]
        fn pack_rows_is_the_wire_encoding_in_both_placements(
            (cols, rows, bytes, nulls, start) in prop::collection::vec(
                (arb_pk_type(), any::<Index>(), any::<bool>()),
                1..=MAX_PK_COLUMNS,
            )
            .prop_flat_map(|cols| {
                let width: usize = cols.iter().map(|c| c.0.wire_stride()).sum();
                (Just(cols), 1usize..600).prop_flat_map(move |(cols, rows)| {
                    let bytes = prop::collection::vec(any::<u8>(), rows * width);
                    (Just(cols), Just(rows), bytes, prop::collection::vec(any::<u8>(), rows), 0..rows)
                })
            })
        ) {
            let types: Vec<TypeCode> = cols.iter().map(|c| c.0).collect();
            let slots: Vec<TypeCode> = cols
                .iter()
                .map(|&(tc, pick, _)| {
                    let at: Vec<TypeCode> = TypeCode::ALL.iter().copied().filter(|&t| t.is_pk_eligible() && tc.packs_at(t)).collect();
                    *pick.get(&at)
                })
                .collect();
            let widths: Vec<usize> = types.iter().map(|t| t.wire_stride()).collect();
            let row_width: usize = widths.iter().sum();
            let native = |r: usize, i: usize| {
                let at = r * row_width + widths[..i].iter().sum::<usize>();
                le_cell(&bytes[at..at + widths[i]])
            };
            let is_null = |r: usize, i: usize| cols[i].2 && nulls[r] >> i & 1 == 1;
            let payload = |nullable: bool| -> SchemaDescriptor {
                let cols: Vec<SchemaColumn> = cols.iter().map(|&(tc, _, n)| SchemaColumn::new(tc, nullable && n)).collect();
                u64_pk_schema(&cols)
            };
            let (pay_schema, null_schema, pk_schema) = (payload(false), payload(true), pk_only_schema(&types));
            let mut pb = BatchBuilder::new(&pay_schema);
            let mut nb = BatchBuilder::new(&null_schema);
            let mut kb = BatchBuilder::new(&pk_schema);
            for r in 0..rows {
                let natives: Vec<u128> = (0..types.len()).map(|i| native(r, i)).collect();
                pb.begin_row(r as u128, 1);
                nb.begin_row(r as u128, 1);
                for (i, &v) in natives.iter().enumerate() {
                    pb.put_int(v);
                    nb.put_opt_int((!is_null(r, i)).then_some(v));
                }
                pb.end_row();
                nb.end_row();
                kb.begin_row_natives(&natives, 1);
                kb.end_row();
            }
            let (pb, nb, kb) = (pb.finish(), nb.finish(), kb.finish());
            let window = [(start, rows)];
            let cut = [(start, start + 1), (start + 2, rows)];
            let windows: Vec<&[(usize, usize)]> = match rows - start >= 3 {
                true => vec![&window, &cut],
                false => vec![&window],
            };

            let want = |r: usize| -> Vec<u8> {
                (0..types.len()).flat_map(|i| slot_bytes(types[i], native(r, i), slots[i])).collect()
            };
            for (schema, batch, first) in [(&pay_schema, &pb, 1u32), (&pk_schema, &kb, 0)] {
                let key: Vec<gnitz_wire::ReindexSlot> = (first..).zip(slots.iter().copied()).collect();
                let packer = KeyPacker::new(schema, &key).unwrap();
                let mb = batch.as_mem_batch();
                for runs in &windows {
                    assert_packs(&packer, &mb, runs, want, &format!("{key:?} first key column {first}"))?;
                }
                // An identity key is each column's own OPK image, end to end:
                // every self-typed one, which the scatter then routes unpacked.
                let self_typed = types.iter().zip(&slots).all(|(t, &s)| t.reindex_output_type() == s);
                let own = packer.identity_columns();
                prop_assert!(own.is_some() || !self_typed, "a self-typed key is an identity");
                if let Some(locs) = own {
                    for r in start..rows {
                        let own: Vec<u8> = locs.iter().flat_map(|l| l.opk_image(&mb, r).to_be_bytes()[16 - l.size()..].to_vec()).collect();
                        prop_assert_eq!(own, want(r), "row {}", r);
                    }
                }
            }

            let group: Vec<u32> = (1..=types.len() as u32).collect();
            let (packer, _) = KeyPacker::new_group_key(&null_schema, &group, &[]).unwrap();
            let mb = nb.as_mem_batch();
            let by_row = |r: usize| {
                let mut key = vec![0u8; packer.out_stride];
                packer.pack_into(&mut key, &mb, r);
                key
            };
            for runs in &windows {
                assert_packs(&packer, &mb, runs, by_row, "group key")?;
            }
        }
    }
}

/// The chunk kernel digests each row exactly as `key_row` does — the digest is a
/// persisted key — over random schemas: PK columns at any offset of a compound
/// PK, payload columns of every integer width, signed and not, nullable or not,
/// strings among them, and folds of one to eight columns (repeats allowed), so
/// a string is assembled on the stack and through the scratch; over chunks that
/// start anywhere and end short.
#[test]
fn keys_chunk_digests_each_row_as_key_row_does() {
    use TypeCode::*;
    let mut rng = Rng::new(0xf01d);
    let payload = [U8, I8, U16, I16, U32, I32, U64, I64, U128, I128, String];
    let mut paths = std::collections::HashSet::new();
    for _ in 0..200 {
        let s = random_schema(&mut rng, &payload, true);
        let n = 1 + rng.gen_range(600) as usize;
        let mut bb = BatchBuilder::new(&s);
        for _ in 0..n {
            let pk: Vec<u128> = s
                .pk_cols()
                .iter()
                .map(|&c| edge_native(&mut rng, s.columns()[c as usize].type_code))
                .collect();
            bb.begin_row_natives(&pk, 1);
            for c in (0..s.num_columns()).filter(|&c| !s.is_pk_col(c)) {
                let col = s.columns()[c];
                if col.nullable && rng.gen_range(4) == 0 {
                    bb.put_null();
                } else if col.type_code.is_german_string() {
                    bb.put_string(STRS[rng.gen_range(STRS.len() as u64) as usize]);
                } else if rng.gen_range(2) == 0 {
                    bb.put_int(edge_native(&mut rng, col.type_code));
                } else {
                    bb.put_int(rng.gen_u128() & gnitz_wire::image_mask(col.type_code.wire_stride()));
                }
            }
            bb.end_row();
        }
        let b = bb.finish();
        let mb = b.as_mem_batch();
        let k = 1 + rng.gen_range(8) as usize;
        let cols: Vec<usize> = (0..k).map(|_| rng.gen_range(s.num_columns() as u64) as usize).collect();
        let f = FoldCols::new(cols.iter().map(|&c| s.locate(c)).collect());
        let has_string = cols.iter().any(|&c| s.columns()[c].type_code.is_german_string());
        paths.insert((has_string, k > FOLD_INLINE_COLS));
        let want: Vec<u128> = (0..n).map(|r| f.key_row(&mb, r)).collect();
        let mut all = Vec::new();
        f.for_each_chunk(&mb, &[(0, n)], |start, keys| {
            assert_eq!(start, all.len(), "chunks in row order");
            all.extend_from_slice(keys);
        });
        let types: Vec<TypeCode> = s.columns().iter().map(|c| c.type_code).collect();
        assert_eq!(all, want, "{cols:?} over {types:?}");
        let start = rng.gen_range(n as u64) as usize;
        let len = (rng.gen_range((n - start) as u64 + 1) as usize).min(FOLD_CHUNK);
        let mut chunk = vec![0u128; len];
        f.keys_chunk(&mb, start, &mut chunk);
        assert_eq!(chunk, want[start..start + len], "{cols:?} from row {start}");
    }
    assert_eq!(
        paths.len(),
        4,
        "every assembly reached, with and without a string: {paths:?}"
    );
}
