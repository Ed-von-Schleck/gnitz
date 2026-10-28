use super::*;
use crate::repr::BatchBuilder;
use crate::schema::SchemaColumn;
use gnitz_wire::TypeCode;

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
