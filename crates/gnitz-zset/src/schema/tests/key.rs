use super::*;
use crate::repr::{BatchBuilder, MemBatch};
use crate::schema::{index_spec_and_schema, SchemaColumn, SchemaDescriptor, TypeCode, MAX_PK_BYTES};
use crate::test_support::Rng;
use crate::test_support::{le_cell, pk_only_schema};
use gnitz_wire::{cmp_col_window, image_mask, key_image, widen_pk_be};

fn col(tc: TypeCode) -> SchemaColumn {
    SchemaColumn::new(tc, false)
}

// ---------------------------------------------------------------------------
// The OPK encoding and the byte primitives over it
// ---------------------------------------------------------------------------

/// The typed lexicographic order of two native little-endian PK tuples, laid
/// out in PK-list order: each column by its own type's order.
fn typed_cmp(s: &SchemaDescriptor, a: &[u8], b: &[u8]) -> Ordering {
    let mut off = 0;
    s.pk_columns()
        .map(|(_, c)| {
            let r = off..off + c.size() as usize;
            off = r.end;
            cmp_col_window(&a[r.clone()], &[], &b[r], &[], c.type_code)
        })
        .find(|o| o.is_ne())
        .unwrap_or(Ordering::Equal)
}

/// Over random compound PKs — 1..=4 columns of any PK-eligible type, the PK list
/// in random order — and native tuples sharing a random-length byte prefix, so
/// later columns and bytes past 16 decide often: a `memcmp` of the OPK keys is
/// the typed order, decoding inverts encoding, and the column-wise encoder
/// writes the same key.
#[test]
fn opk_is_an_order_preserving_bijection() {
    let pk_types: Vec<TypeCode> = TypeCode::ALL.iter().copied().filter(|t| t.is_pk_eligible()).collect();
    let mut rng = Rng::new(0x09C4B);
    for _ in 0..5000 {
        let cols: Vec<SchemaColumn> = (0..1 + rng.gen_range(4)).map(|_| col(rng.pick(&pk_types))).collect();
        let mut pk: Vec<u32> = (0..cols.len() as u32).collect();
        rng.shuffle(&mut pk);
        let s = SchemaDescriptor::new(&cols, &pk);
        let a: Vec<u8> = (0..s.pk_stride()).map(|_| rng.next_u64() as u8).collect();
        let mut b = a.clone();
        for x in &mut b[rng.gen_range(a.len() as u64 + 1) as usize..] {
            *x = rng.next_u64() as u8;
        }

        let (oa, ob) = (s.opk_key(&a), s.opk_key(&b));
        assert_eq!(
            compare_pk_bytes(oa.pk_bytes(), ob.pk_bytes()),
            typed_cmp(&s, &a, &b),
            "{s:?}: {a:?} vs {b:?}"
        );
        assert_eq!(&s.native_le_key(oa.pk_bytes())[..a.len()], &a[..], "{s:?}");
        let mut off = 0;
        let natives: Vec<u128> = s
            .pk_columns()
            .map(|(_, c)| {
                off += c.size() as usize;
                le_cell(&a[off - c.size() as usize..off])
            })
            .collect();
        assert_eq!(s.opk_key_cols(&natives), oa, "{s:?}");
    }
}

/// Every byte primitive over OPK keys, at every PK width and over key pairs
/// sharing a prefix of every length: each compares, packs or sort-keys exactly
/// as `memcmp` orders the bytes, and `sort_indices` sorts records by it.
#[test]
fn opk_byte_primitives_agree_with_memcmp_at_every_width() {
    fn naive(pk: &[u8]) -> u128 {
        let take = pk.len().min(16);
        let mut buf = [0u8; 16];
        buf[..take].copy_from_slice(&pk[..take]);
        u128::from_be_bytes(buf)
    }
    let mut rng = Rng::new(0xB17E5);
    for w in 0..=MAX_PK_BYTES {
        for _ in 0..16 {
            let a: Vec<u8> = (0..w).map(|_| rng.next_u64() as u8).collect();
            assert_eq!(pack_pk_be(&a), naive(&a), "w={w}");
            assert_eq!(leading_u64(&a), (naive(&a[..w.min(8)]) >> 64) as u64, "w={w}");
            // Same width taxonomy as `widen_pk_be`, opposite alignment.
            if (1..=16).contains(&w) {
                assert_eq!(pack_pk_be(&a), widen_pk_be(&a) << (8 * (16 - w)), "w={w}");
            }
            for pos in 0..=w {
                let mut b = a.clone();
                if pos < w {
                    b[pos] = rng.next_u64() as u8;
                }
                let want = a.cmp(&b);
                assert_eq!(compare_pk_ordering(&a, &b), want, "w={w} pos={pos}");
                assert_eq!(pk_bytes_eq(&a, &b), want.is_eq(), "w={w} pos={pos}");
                pk_width_dispatch!(w, |K| assert_eq!(
                    Ord::cmp(&K::from_opk(&a[..]), &K::from_opk(&b[..])),
                    want,
                    "w={w} pos={pos}"
                ));
            }
        }
        if w > 0 {
            // A four-letter alphabet, so records tie and share prefixes.
            let flat: Vec<u8> = (0..64 * w).map(|_| rng.next_u64() as u8 & 3).collect();
            let rec = |i: u32| &flat[i as usize * w..(i as usize + 1) * w];
            let mut idx = Vec::new();
            sort_indices(&flat, w, &mut idx);
            assert!(idx.windows(2).all(|p| rec(p[0]) <= rec(p[1])), "w={w}");
            idx.sort_unstable();
            assert!(idx.iter().copied().eq(0..64), "w={w}: a permutation");
        }
    }
}

/// `probe_key` hashes the whole span: keys sharing their leading 16 bytes stay
/// apart.
#[test]
fn probe_key_reads_past_the_leading_16_bytes() {
    let wide = |tail: u8| [[7u8; 16].as_slice(), &[tail; 8]].concat();
    assert_ne!(probe_key(&wide(3)), probe_key(&wide(4)));
}

// ---------------------------------------------------------------------------
// Key ranges and confinement
// ---------------------------------------------------------------------------

/// `increment_key_in_place` is `+1` with carry and reports a carry-out;
/// `decrement_key_in_place` undoes it — over every key of up to two bytes.
#[test]
fn key_increment_and_decrement_are_inverse_steps() {
    for w in 0..=2usize {
        let be = |x: u32| x.to_be_bytes()[4 - w..].to_vec();
        let max = (1u32 << (8 * w)) - 1;
        for v in 0..=max {
            let mut k = be(v);
            assert_eq!(increment_key_in_place(&mut k), v != max, "w={w} v={v}");
            assert_eq!(k, be(if v == max { 0 } else { v + 1 }), "w={w} v={v}");
            if v != max {
                decrement_key_in_place(&mut k);
                assert_eq!(k, be(v), "w={w} v={v}");
            }
        }
    }
}

/// A narrow key's image, right-aligned at the key's width, is the key.
#[test]
fn narrow_pk_opk_inverts_the_widening() {
    let mut rng = Rng::new(0x0A11_6E00);
    for _ in 0..2000 {
        let bytes = rng.gen_u128().to_be_bytes();
        for w in 1..=16 {
            let opk = &bytes[..w];
            assert_eq!(NarrowPkOpk::new(widen_pk_be(opk), w).bytes(), opk, "width {w}");
        }
    }
}

// ---------------------------------------------------------------------------
// Index key spans
// ---------------------------------------------------------------------------

/// An index schema is the promoted key columns then the source PK, all in the
/// PK, and its span schema the promoted key columns alone. An arity past
/// `MAX_PK_COLUMNS` is an `Err`.
#[test]
fn index_schema_is_the_promoted_key_then_the_source_pk() {
    use TypeCode::{I16, I32, I64, I8, U128, U32, U64};
    let src = SchemaDescriptor::new(&[col(U64), col(U32), col(U128)], &[0]);
    let (spec, index) = index_spec_and_schema(&[1, 2], &src).unwrap();
    assert_eq!(index, pk_only_schema(&[U64, U128, U64]));
    assert_eq!(spec.span_schema(), pk_only_schema(&[U64, U128]));
    for t in [I8, I16, I32, I64] {
        let src = SchemaDescriptor::new(&[col(U64), col(t)], &[0]);
        let (spec, index) = index_spec_and_schema(&[1], &src).unwrap();
        assert_eq!(index, pk_only_schema(&[I64, U64]), "{t}");
        assert_eq!(spec.span_schema(), pk_only_schema(&[I64]), "{t}");
    }

    let wide = SchemaDescriptor::new(&[col(U64); MAX_PK_COLUMNS + 1], &[0, 1]);
    let key: Vec<u32> = (2..=MAX_PK_COLUMNS as u32).collect();
    assert!(index_spec_and_schema(&key[1..], &wide).is_ok(), "arity MAX_PK_COLUMNS");
    assert!(index_spec_and_schema(&key, &wide).is_err(), "arity MAX_PK_COLUMNS + 1");
}

/// The span `write_span` writes for a row: each indexed column decoded to its
/// native value and encoded through the seek side. `None` is the NULL skip.
fn write_span_reference(
    owner: &SchemaDescriptor,
    spec: &KeySpec,
    cols: &[u32],
    mb: &MemBatch<'_>,
    row: usize,
) -> Option<PkBuf> {
    let mut images = Vec::with_capacity(cols.len());
    for &c in cols {
        let loc = owner.locate(c as usize);
        if loc.is_null(mb, row) {
            return None;
        }
        let mut scratch = [0u8; 16];
        images.push(key_image(
            loc.type_code(),
            le_cell(loc.native_le_bytes(mb, row, &mut scratch)),
        ));
    }
    Some(spec.seek_prefix(&images))
}

/// Over a compound PK, so an indexed PK column sits at a non-zero byte offset,
/// and a nullable payload column indexed leading and trailing: `write_span`
/// writes the reference span and skips exactly the row NULL in an indexed
/// column.
#[test]
fn write_span_matches_the_reference_on_compound_null_and_entry_shapes() {
    let src = SchemaDescriptor::new(
        &[
            col(TypeCode::U32),
            col(TypeCode::I64),
            SchemaColumn::new(TypeCode::I32, true),
        ],
        &[0, 1],
    );
    let mut bb = BatchBuilder::new(&src);
    for (i, (a, v)) in [(7u32, -1i64), (0, 0), (u32::MAX, i64::MIN), (3, i64::MAX)]
        .into_iter()
        .enumerate()
    {
        bb.begin_row_opk(&[a as u128, v as u64 as u128], 1);
        bb.put_int(-(i as i32) as u128);
        bb.end_row();
    }
    bb.begin_row_opk(&[9, 5], 1);
    bb.put_null();
    bb.end_row();
    let b = bb.finish();
    let null_row = b.len() - 1;
    let mb = b.as_mem_batch();
    let stride = src.pk_stride();

    for cols in [[1u32, 2], [2, 1]] {
        let (spec, idx) = crate::schema::index_spec_and_schema(&cols, &src).unwrap();
        // The width `split_entry` splits a stored entry at.
        assert_eq!(idx.pk_stride(), spec.key_size() + stride);
        for row in 0..b.len() {
            let mut span = [0u8; MAX_PK_BYTES];
            let written = spec.write_span(&mb, row, &mut span);
            assert_eq!(
                written,
                row != null_row,
                "{cols:?} row={row}: only the NULL row is skipped"
            );
            let want = write_span_reference(&src, &spec, &cols, &mb, row);
            assert_eq!(
                want.as_ref().map(|w| w.pk_bytes()),
                written.then(|| &span[..spec.key_size()]),
                "{cols:?} row={row}"
            );
        }
    }
}

/// For every indexable type, whether the column is payload or the table's PK:
/// the span the write path stores for a value is the span a seek for that value
/// builds, and spans sort as the values do.
#[test]
fn index_spans_equal_the_seek_prefix_and_sort_as_the_values() {
    for &t in TypeCode::ALL
        .iter()
        .filter(|t| gnitz_wire::index_key_type(**t).is_some())
    {
        let sz = t.wire_stride();
        let mask = image_mask(sz);
        let top = 1u128 << (8 * sz - 1);
        // Natives in ascending typed order.
        let values = match t.is_signed_int() {
            true => [top, mask, 0, 1, mask >> 1],
            false => [0, 1, top - 1, top, mask],
        };
        let payload_src = SchemaDescriptor::new(&[col(TypeCode::U64), col(t)], &[0]);
        let pk_src = SchemaDescriptor::new(&[col(t), col(TypeCode::U64)], &[0]);
        let mut prev: Option<Vec<u8>> = None;
        for native in values {
            let seek = KeySpec::new(&[1], &payload_src)
                .unwrap()
                .seek_prefix(&[key_image(t, native)]);
            for (src, c) in [(payload_src, 1u32), (pk_src, 0)] {
                let mut bb = BatchBuilder::new(&src);
                bb.begin_row_opk(&[if c == 0 { native } else { 1 }], 1);
                bb.put_int(if c == 0 { 0 } else { native });
                bb.end_row();
                let b = bb.finish();
                let spec = KeySpec::new(&[c], &src).unwrap();
                let mut entry = [0u8; MAX_PK_BYTES];
                assert!(spec.write_span(&b.as_mem_batch(), 0, &mut entry));
                assert_eq!(&entry[..spec.key_size()], seek.pk_bytes(), "{t} {native:#x}");
            }
            if let Some(p) = &prev {
                assert!(p[..] < *seek.pk_bytes(), "{t}: spans out of value order at {native:#x}");
            }
            prev = Some(seek.pk_bytes().to_vec());
        }
    }
}

/// The indirect sort of a flat record buffer. Sweeps OPK strides at a
/// chunk-sized `n` and a large one.
#[test]
#[ignore = "microbenchmark; run explicitly with --release --ignored --nocapture"]
fn sort_indices_bench() {
    use crate::test_support::bench_time_each;

    const ITERS: usize = 3;
    for &n in &[65_536usize, 4 << 20] {
        for &stride in &[4usize, 8, 12, 16, 24, 40] {
            for seq in [true, false] {
                let mut rng = Rng::new(0x5EED_0000 + n as u64 + stride as u64);
                let mut flat = vec![0u8; n * stride];
                for (i, rec) in flat.chunks_mut(stride).enumerate() {
                    for chunk in rec.chunks_mut(8) {
                        let bytes = rng.next_u64().to_be_bytes();
                        chunk.copy_from_slice(&bytes[..chunk.len()]);
                    }
                    // Distinct ids in the leading bytes: neighbours share a prefix.
                    if seq {
                        let id = (i as u64).wrapping_mul(0x9E37_79B9) % n as u64;
                        let be = id.to_be_bytes();
                        let w = stride.min(8);
                        rec[..w].copy_from_slice(&be[8 - w..]);
                    }
                }
                let elapsed = bench_time_each(ITERS, Vec::new, |mut idx| {
                    sort_indices(&flat, stride, &mut idx);
                    std::hint::black_box(&idx);
                });
                let ns = elapsed.as_secs_f64() * 1e9 / (ITERS as f64 * n as f64);
                let shape = if seq { "id" } else { "rnd" };
                println!("  n={n:<8} stride={stride:<3} {shape:<3} {ns:7.2} ns/record");
            }
        }
    }
}
