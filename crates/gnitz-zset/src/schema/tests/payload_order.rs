use super::*;
use crate::repr::{Batch, BatchBuilder};
use crate::schema::SchemaColumn;
use crate::schema::TypeCode;
use crate::test_support::Rng;
use crate::test_support::{le_cell, opk_pk, random_schema, row_key};

/// A payload cell by its type's own order: integers numerically at their
/// signedness, floats by `total_cmp`, strings and blobs bytewise. One column
/// always yields one variant, so the order across variants never decides.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Cell {
    Signed(i128),
    Unsigned(u128),
    /// The `total_cmp` key of the float's bits.
    Float(u64),
    Bytes(Vec<u8>),
}

/// A row's model: its OPK bytes, then its cells in payload order, NULL as `None`
/// — which `Option`'s order puts below every value.
type Model = (Vec<u8>, Vec<Option<Cell>>);

/// Draw a value `put` into the current payload column of `col`'s type, from a
/// small pool per type, so equal cells are common and a tie falls through to
/// the next column.
fn random_cell(rng: &mut Rng, col: &SchemaColumn, bb: &mut BatchBuilder) -> Option<Cell> {
    if col.nullable && rng.gen_range(4) == 0 {
        bb.put_null();
        return None;
    }
    let w = col.size();
    let tc = col.type_code;
    Some(if tc.is_german_string() {
        let long = |c: u8| [&[b'x'; 20][..], &[c]].concat();
        let v = rng.pick(&[vec![], b"a".to_vec(), b"ab".to_vec(), long(b'a'), long(b'b')]);
        bb.put_blob(&v);
        Cell::Bytes(v)
    } else if matches!(tc, TypeCode::F32 | TypeCode::F64) {
        let v = rng.pick(&[0.0, -0.0, 1.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY]);
        bb.put_float(v);
        let (bits, top) = match w {
            4 => ((v as f32).to_bits() as u64, 31),
            _ => (v.to_bits(), 63),
        };
        let key = if bits >> top == 1 { !bits } else { bits | 1 << top };
        Cell::Float(key & u64::MAX >> (63 - top))
    } else {
        // Zero, one, all-ones, and the signed extremes, as little-endian cells.
        let mut le = vec![0u8; w];
        match rng.gen_range(5) {
            0 => {}
            1 => le[0] = 1,
            2 => le.fill(0xFF),
            3 => {
                le.fill(0xFF);
                le[w - 1] = 0x7F;
            }
            _ => le[w - 1] = 0x80,
        }
        bb.put_int(le_cell(&le));
        let negative = tc.is_signed_int() && le[w - 1] & 0x80 != 0;
        let mut v = [if negative { 0xFF } else { 0 }; 16];
        v[..w].copy_from_slice(&le);
        match tc.is_signed_int() {
            true => Cell::Signed(i128::from_le_bytes(v)),
            false => Cell::Unsigned(u128::from_le_bytes(v)),
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

fn selected<O: PayloadOrder>(s: &SchemaDescriptor, a: &Batch, b: &Batch, order: O) -> Ordering {
    order.compare(s, a, 0, b, 0)
}

/// Over random schemas — any payload types and nullability, the PK anywhere and
/// in any order — the (PK, payload) order is the PK bytes, then each payload cell
/// by its type's order; `Equal` is exactly row identity; and the comparator the
/// schema selects orders as the generic one. Every schema whose payload is
/// non-null fixed-width integers selects the fast one.
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
        let (a, ma) = random_row(&mut rng, &s);
        let (b, mb) = random_row(&mut rng, &s);
        let payload = compare_rows(&s, &a, 0, &b, 0);
        assert_eq!(payload, ma.1.cmp(&mb.1), "{s:?}: {ma:?} vs {mb:?}");
        assert_eq!(
            compare_full_rows(&s, &a, 0, &b, 0),
            ma.cmp(&mb),
            "{s:?}: {ma:?} vs {mb:?}"
        );
        assert_eq!(
            with_payload_cmp!(s, selected, &s, &a, &b),
            payload,
            "{s:?}: {ma:?} vs {mb:?}"
        );
        assert_eq!(
            ma == mb,
            row_key(&a, &s, 0) == row_key(&b, &s, 0),
            "{s:?}: {ma:?} vs {mb:?}"
        );
        if s.payload_columns()
            .all(|(_, c)| !c.nullable && c.type_code.is_fixed_int())
        {
            assert!(s.payload_is_fixed_int_nonnull(), "{s:?}");
        }
    }
}
