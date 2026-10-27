use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, SchemaDescriptor};
use crate::test_support::{
    le_cell, make_schema_pk_u64_payload_blob, make_schema_pk_u64_payload_string, u64_pk_schema, STRS,
};
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
    gnitz_wire::store_opk(&mut slot, extended(tc, native), target.is_signed_int());
    slot
}

/// Two rows fold to one key exactly when they agree on every folded column, a
/// NULL told from a zero and from an empty string. Up to `FOLD_INLINE_COLS`
/// fixed-width columns fold on the stack, a wider set or one holding a string
/// through the scratch. The fold of no columns is V₀.
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
        row[..4].iter().for_each(|&cell| b.put_opt_int(cell));
        match row[4] {
            Some(i) => b.put_string(STRS[i as usize]),
            None => b.put_null(),
        }
        b.end_row();
    }
    let b = b.finish();
    let mb = b.as_mem_batch();

    let empty = FoldCols::new(Vec::new());
    assert_eq!(empty.key_row(&mb, 0), gnitz_wire::global_group_key());
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
        assert_eq!(
            f.inline,
            cols.len() <= FOLD_INLINE_COLS && !cols.contains(&5),
            "{cols:?}"
        );
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
        let cols: Vec<SchemaColumn> = payload.iter().map(|&tc| SchemaColumn::new(tc, false)).collect();
        let in_schema = u64_pk_schema(&cols);
        let packer = ReindexPacker::new(&in_schema, key).unwrap();
        let out = packer.output_schema(&in_schema, keep).unwrap();
        let types: Vec<TypeCode> = (0..out.num_columns()).map(|c| out.columns()[c].type_code).collect();
        assert_eq!(types, want, "{payload:?} keyed on {key:?}");
        assert_eq!(out.pk_cols(), (0..key.len() as u32).collect::<Vec<_>>(), "{payload:?}");
        let slots: usize = key.iter().map(|&(_, t)| t.wire_stride()).sum();
        assert_eq!((out.pk_stride(), packer.out_stride), (slots, slots), "{payload:?}");
    }
}

// -----------------------------------------------------------------------
// ReindexPacker — the content-hash arm
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
        let packer = ReindexPacker::new(&schema, &[(1, TypeCode::U128)]).unwrap();
        let want: Vec<u8> = contents
            .iter()
            .flat_map(|c| gnitz_wire::checksum_128(c).to_be_bytes())
            .collect();
        assert_eq!(
            packer.keys(&b.as_mem_batch()),
            want,
            "{}",
            schema.columns()[1].type_code
        );
    }
}

// -----------------------------------------------------------------------
// ReindexPacker::pack_into — property test over every PK-eligible type
// -----------------------------------------------------------------------

mod pack_proptest {
    use super::*;
    use crate::test_support::{arb_pk_type, pk_only_schema};
    use gnitz_wire::{MAX_PK_BYTES, MAX_PK_COLUMNS};
    use proptest::prelude::*;
    use proptest::sample::Index;

    /// `packer`'s keys of the rows of `runs`, packed by `pack_rows` over dirty
    /// slots `pad` bytes wider than the key and by `pack_prefix` one at a time,
    /// are each `want` of its row.
    fn assert_packs(
        packer: &ReindexPacker,
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
                let want = want(row);
                prop_assert_eq!(&key[..packer.out_stride], &want[..], "{} row {} pad {}", what, row, pad);
                let mut one = [0u8; MAX_PK_BYTES];
                prop_assert_eq!(packer.pack_prefix(&mut one, mb, row), &want[..], "{} row {}", what, row);
            }
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
                let packer = ReindexPacker::new(schema, &key).unwrap();
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
            let (packer, _) = ReindexPacker::new_group_key(&null_schema, &group, &[]).unwrap();
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

/// Over PKs of every suffix width and a nullable indexed column of every
/// indexable width: the projection holds exactly the rows `KeySpec::span` admits,
/// each as its span followed by its own PK, in source order — and `spans` the
/// positive-weight rows' spans.
#[test]
fn index_entries_are_each_admitted_rows_span_and_pk() {
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
                _ => b.put_int(v & gnitz_wire::image_mask(width)),
            }
            b.put_int(row as u128);
            b.end_row();
        }
        let src = b.finish();
        let mb = src.as_mem_batch();
        for indexed in [&[k][..], &[k, k + 1], &[k - 1, k]] {
            let (spec, idx_schema) = crate::schema::key::KeySpec::new(indexed, &owner)
                .map(|spec| (spec, spec.index_schema(&owner)))
                .unwrap();
            let mut want: Vec<(Vec<u8>, i64)> = Vec::new();
            for row in 0..src.len() {
                if let Some(span) = spec.span(&mb, row).filter(|_| src.get_weight(row) != 0) {
                    let mut e = span.pk_bytes().to_vec();
                    e.extend_from_slice(src.get_pk_bytes(row));
                    want.push((e, src.get_weight(row)));
                }
            }
            let got = index_entries(&src, &spec, &idx_schema);
            let got: Vec<(Vec<u8>, i64)> = (0..got.len())
                .map(|r| (got.get_pk_bytes(r).to_vec(), got.get_weight(r)))
                .collect();
            assert_eq!(got, want, "{pk:?} {tc} indexed on {indexed:?}");

            let key_size = spec.key_size();
            let want_spans: Vec<u8> = want
                .iter()
                .filter(|(_, w)| *w > 0)
                .flat_map(|(e, _)| e[..key_size].to_vec())
                .collect();
            assert_eq!(spans(&mb, &spec), want_spans, "{pk:?} {tc} indexed on {indexed:?}");
        }
    }
}
