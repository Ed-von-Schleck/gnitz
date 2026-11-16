use std::cmp::Ordering;

use super::*;
use crate::repr::BatchBuilder;
use crate::schema::{SchemaColumn, range_band};
use crate::test_support::{Rng, edge_native, pk_only_schema, random_schema};
use gnitz_wire::{KeyRange, MAX_PK_COLUMNS, PkColList, TypeCode, cmp_col_window, key_image};

/// Over PKs of every suffix width and a nullable indexed column of every
/// indexable width: the projection holds exactly the rows `KeySpec::span` admits,
/// each as its span followed by its own PK, in source order — and `extend_spans`
/// the positive-weight rows' spans.
#[test]
fn index_entries_are_each_admitted_rows_span_and_pk() {
    use TypeCode::{I16, I64, I128, U8, U32, U64, U128};
    let mut rng = Rng::new(0x1dc5);
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
            let (idx_schema, spec) = KeySpec::new(indexed, &owner)
                .map(|spec| (spec.index_schema(&owner), spec))
                .unwrap();
            let mut want: Vec<(Vec<u8>, i64)> = Vec::new();
            for row in 0..src.len() {
                if src.get_weight(row) != 0
                    && let Some(span) = spec.span(&mb, row)
                {
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
            let mut spans = Vec::new();
            extend_spans(&mut spans, &mb, &spec);
            assert_eq!(spans, want_spans, "{pk:?} {tc} indexed on {indexed:?}");
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
            for &c in s.pk_cols() {
                let c = c as usize;
                row[c] = Some(edge_native(&mut rng, tc(c)));
            }
            let pk: Vec<u128> = s.pk_cols().iter().map(|&c| row[c as usize].unwrap()).collect();
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
        let src_pk = s.pk_columns().map(|c| c.type_code);
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
                let point = KeyRange::point(PkColList::from_slice(&span_cols), &images);
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
