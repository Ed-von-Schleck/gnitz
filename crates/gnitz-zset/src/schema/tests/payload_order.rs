use super::*;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::SchemaColumn;
use crate::test_support::{FLOATS, STRS, edge_native, opk_pk, random_schema, row_key};
use gnitz_foundation::rng::Rng;
use gnitz_wire::{ScalarKind, TypeCode, key_image};

/// A payload cell by its type's order: an integer by its key image, a float by
/// its `total_cmp` image, a string by its bytes. One column always yields one
/// variant, so the order across variants never decides.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Cell {
    Image(u128),
    Bytes(Vec<u8>),
}

/// A row's model: its OPK bytes, then its cells in payload order, NULL as `None`
/// — which `Option`'s order puts below every value.
type Model = (Vec<u8>, Vec<Option<Cell>>);

/// Put a value from a small pool per type into the current payload column of
/// `col`'s type, so equal cells are common, and return its model.
fn random_cell(rng: &mut Rng, col: &SchemaColumn, bb: &mut BatchBuilder) -> Option<Cell> {
    let tc = col.type_code;
    if col.nullable && rng.gen_range(4) == 0 {
        bb.put_null();
        return None;
    }
    Some(match ScalarKind::from_type_code(tc) {
        _ if tc.is_german_string() => {
            let v = rng.pick(&STRS);
            bb.put_string(v);
            Cell::Bytes(v.as_bytes().to_vec())
        }
        Some(kind @ (ScalarKind::F32 | ScalarKind::F64)) => {
            let v = rng.pick(&FLOATS);
            bb.put_float(v);
            let bits = match kind {
                ScalarKind::F32 => (v as f32).to_bits() as u64,
                _ => v.to_bits(),
            };
            Cell::Image(kind.order_image(bits) as u128)
        }
        _ => {
            let v = edge_native(rng, tc);
            bb.put_int(v);
            Cell::Image(key_image(tc, v))
        }
    })
}

/// One row over `s` in a batch of its own, so each row's strings live in a blob
/// arena of their own, as a shard's and a `MemBatch`'s do.
fn random_row(rng: &mut Rng, s: &SchemaDescriptor) -> (Batch, Model) {
    let natives: Vec<u128> = s.pk_columns().map(|_| rng.pick(&[0, 1, u128::MAX])).collect();
    let pk = opk_pk(s, &natives);
    let mut bb = BatchBuilder::new(s);
    bb.begin_row_bytes(&pk, 1);
    let cells = s.payload_columns().map(|(_, c)| random_cell(rng, c, &mut bb)).collect();
    bb.end_row();
    (bb.finish(), (pk, cells))
}

/// Over random schemas — any payload types and nullability, the PK anywhere and
/// in any order — the (PK, payload) order is the PK bytes, then each payload
/// cell by its type's order; `Equal` is exactly row identity; and the fast
/// comparator, which every other schema selects, orders as the generic one.
#[test]
fn row_order_is_the_pk_bytes_then_each_typed_payload_cell() {
    let fixed_ints: Vec<TypeCode> = TypeCode::ALL.iter().copied().filter(|t| t.is_fixed_int()).collect();
    let mut rng = Rng::new(0x000D_E20F);
    for i in 0..4000 {
        // Every other schema is one the fast comparator serves.
        let s = match i % 2 {
            0 => random_schema(&mut rng, TypeCode::ALL, true),
            _ => random_schema(&mut rng, &fixed_ints, false),
        };
        assert!(i % 2 == 0 || s.payload_is_fixed_int_nonnull(), "{s:?}");
        let (a, ma) = random_row(&mut rng, &s);
        let (b, mb) = random_row(&mut rng, &s);
        let payload = Generic.compare(&s, &a, 0, &b, 0);
        assert_eq!(payload, ma.1.cmp(&mb.1), "{s:?}: {ma:?} vs {mb:?}");
        let full = Generic.compare_full(&s, &a, 0, &b, 0);
        assert_eq!(full, ma.cmp(&mb), "{s:?}: {ma:?} vs {mb:?}");
        assert_eq!(
            with_payload_cmp!(s, |p| p.compare(&s, &a, 0, &b, 0)),
            payload,
            "{s:?}: {ma:?} vs {mb:?}"
        );
        assert_eq!(
            full.is_eq(),
            row_key(&a, &s, 0) == row_key(&b, &s, 0),
            "{s:?}: {ma:?} vs {mb:?}"
        );
    }
}
