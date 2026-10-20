use super::*;
use crate::repr::BatchBuilder;
use crate::schema::MAX_PK_BYTES;
use crate::test_support::Rng;
use crate::test_support::{edge_native, pk_only_schema, random_schema};
use gnitz_wire::{cmp_col_window, key_image, PkColList};

/// Every byte primitive over OPK keys, at every PK width and over key pairs
/// sharing a prefix of every length: each packs, compares or sort-keys exactly
/// as `memcmp` orders the bytes.
#[test]
fn opk_byte_primitives_agree_with_memcmp_at_every_width() {
    let mut rng = Rng::new(0xB17E5);
    for w in 0..=MAX_PK_BYTES {
        for _ in 0..16 {
            let a: Vec<u8> = (0..w).map(|_| rng.next_u64() as u8).collect();
            // The leading 16 bytes, zero-padded, read big-endian.
            let mut lead = [0u8; 16];
            lead[..w.min(16)].copy_from_slice(&a[..w.min(16)]);
            assert_eq!(pack_pk_be(&a), u128::from_be_bytes(lead), "w={w}");
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
    }
}

/// Over random schemas — any column types and nullability, the PK anywhere and
/// in any order — and a random list of distinct PK-eligible columns within the
/// arity budget, PK or nullable payload, over rows of edge values:
/// - `span` skips exactly the rows NULL in an indexed column, and is otherwise
///   the band start a point walk over the indexed values' key images seeks to;
/// - spans `memcmp` as the indexed values' typed lexicographic order;
/// - the span schema is the key columns at their own types, the index schema
///   those then the source PK, so an entry splits at the span's width.
#[test]
fn key_spans_encode_the_typed_order_of_their_columns() {
    let mut rng = Rng::new(0x09C4B);
    for _ in 0..2000 {
        let s = random_schema(&mut rng, TypeCode::ALL, true);
        let tc = |c: usize| s.columns()[c].type_code;

        // Each row's natives by column, `None` for NULL or an unindexable value.
        let mut natives: Vec<Vec<Option<u128>>> = Vec::new();
        let mut bb = BatchBuilder::new(&s);
        for _ in 0..8 {
            let mut row: Vec<Option<u128>> = (0..s.num_columns()).map(|_| None).collect();
            for (c, _) in s.pk_columns() {
                row[c] = Some(edge_native(&mut rng, tc(c)));
            }
            let pk: Vec<u128> = s.pk_columns().map(|(c, _)| row[c].unwrap()).collect();
            bb.begin_row_natives(&pk, 1);
            // Payload slots run in schema order.
            for c in (0..s.num_columns()).filter(|&c| !s.is_pk_col(c)) {
                let col = s.columns()[c];
                if col.nullable && rng.gen_range(4) == 0 {
                    bb.put_null();
                } else if col.type_code.is_pk_eligible() {
                    let v = edge_native(&mut rng, col.type_code);
                    bb.put_int(v);
                    row[c] = Some(v);
                } else if col.type_code.is_german_string() {
                    bb.put_blob(b"x");
                } else {
                    bb.put_float(0.0);
                }
            }
            bb.end_row();
            natives.push(row);
        }
        let b = bb.finish();
        let mb = b.as_mem_batch();

        let budget = MAX_PK_COLUMNS - s.pk_cols().len();
        if budget == 0 {
            continue;
        }
        let mut cols: Vec<u32> = (0..s.num_columns() as u32)
            .filter(|&c| tc(c as usize).is_pk_eligible())
            .collect();
        rng.shuffle(&mut cols);
        cols.truncate(1 + rng.gen_range(budget.min(cols.len()) as u64) as usize);
        let spec = KeySpec::new(&cols, &s).unwrap();
        let types: Vec<TypeCode> = cols.iter().map(|&c| tc(c as usize)).collect();
        let span_schema = spec.span_schema();
        assert_eq!(span_schema, pk_only_schema(&types), "{s:?} {cols:?}");
        let span_cols: Vec<u32> = (0..cols.len() as u32).collect();
        let src_pk = s.pk_columns().map(|(c, _)| tc(c));
        let index = spec.index_schema(&s);
        assert_eq!(index, pk_only_schema(&[types.clone(), src_pk.collect()].concat()));
        assert_eq!(index.pk_stride(), spec.key_size() + s.pk_stride(), "{s:?} {cols:?}");

        let key = |row: usize| -> Option<Vec<u128>> { cols.iter().map(|&c| natives[row][c as usize]).collect() };
        let mut spans: Vec<(Vec<u128>, Vec<u8>)> = Vec::new();
        for row in 0..b.len() {
            let span = spec.span(&mb, row);
            let want = key(row);
            assert_eq!(
                span.is_some(),
                want.is_some(),
                "{s:?} {cols:?} row={row}: only a NULL row is skipped"
            );
            if let (Some(span), Some(vals)) = (span, want) {
                let images: Vec<u128> = vals.iter().zip(&types).map(|(&v, &t)| key_image(t, v)).collect();
                let (&last, eq) = images.split_last().unwrap();
                let point = KeyRange::point(PkColList::from_slice(&span_cols), eq, last);
                let (start, _) = range_band(&span_schema, &point).expect("a point walk holds its key");
                assert_eq!(span.pk_bytes(), start.pk_bytes(), "{s:?} {cols:?} row={row}");
                spans.push((vals, span.pk_bytes().to_vec()));
            }
        }
        for (va, sa) in &spans {
            for (vb, sb) in &spans {
                let typed = va
                    .iter()
                    .zip(vb)
                    .zip(&types)
                    .map(|((a, b), &t)| {
                        let w = t.wire_stride();
                        cmp_col_window(&a.to_le_bytes()[..w], &b.to_le_bytes()[..w], t)
                    })
                    .find(|o| o.is_ne())
                    .unwrap_or(Ordering::Equal);
                assert_eq!(sa.cmp(sb), typed, "{s:?} {cols:?}: {va:?} vs {vb:?}");
            }
        }
    }
}

/// A key column list follows the PK rule with nullability waived, under the
/// index's role prefix, and the index record — the key columns, then the
/// owner's PK — the PK arity limit.
#[test]
fn key_spec_refuses_a_list_past_the_pk_rule_or_the_arity_budget() {
    let wide = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false); MAX_PK_COLUMNS + 1], &[0, 1]);
    let key: Vec<u32> = (2..=MAX_PK_COLUMNS as u32).collect();
    assert!(KeySpec::new(&key[1..], &wide).is_ok(), "arity MAX_PK_COLUMNS");
    assert!(KeySpec::new(&key, &wide).is_err(), "arity MAX_PK_COLUMNS + 1");
    let err = KeySpec::new(&[2, 2], &wide).err().expect("a repeat is refused");
    assert!(
        err.starts_with("Index: column list ") && err.contains("names column 2 twice"),
        "{err}"
    );
}
