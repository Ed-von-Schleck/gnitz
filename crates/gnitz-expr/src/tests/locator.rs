//! Resolved-addressing reads, driven through a non-engine [`crate::BatchView`].

use crate::test_support::{TestView, PATTERNS};
use crate::{order_bits, ColumnLocator, SchemaColumn, SchemaDescriptor};
use gnitz_wire::{FixedInt, ScalarKind, TypeCode};

/// Every locator read of every key type, from a key column and from a payload
/// slot holding the same values, against the value's own encoding.
#[test]
fn every_locator_read_agrees_with_the_values_encoding() {
    for &tc in TypeCode::ALL.iter().filter(|t| t.is_pk_eligible()) {
        let w = tc.wire_stride();
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(TypeCode::U16, false), // key column 0
                SchemaColumn::new(tc, false),            // key column 1, at byte 2
                SchemaColumn::new(TypeCode::I64, true),  // payload slot 0
                SchemaColumn::new(tc, true),             // payload slot 1
            ],
            &[0, 1],
        );
        let natives: Vec<Vec<u8>> = PATTERNS.iter().map(|p| p.to_le_bytes()[..w].to_vec()).collect();
        let mut v = TestView::for_schema(&schema, PATTERNS.len());
        for (row, &p) in PATTERNS.iter().enumerate() {
            v.set_native(&schema, row, 1, p);
            v.set_native(&schema, row, 3, p);
            v.set_null_word(row, u64::from(row % 2 == 0) | u64::from(row % 3 == 0) << 1);
        }
        let (from_pk, from_payload) = (schema.locate(1), schema.locate(3));
        assert!(matches!(from_pk, ColumnLocator::Pk { byte_off: 2, .. }), "{from_pk:?}");
        for loc in [from_pk, from_payload] {
            let arm = if loc == from_pk { "pk" } else { "payload" };
            for (row, native) in natives.iter().enumerate() {
                let label = format!("{tc} {arm} row {row}");
                assert_eq!(
                    loc.opk_image(&v, row),
                    gnitz_wire::key_image(tc, PATTERNS[row]),
                    "{label}"
                );
                assert_eq!(loc.native_le_bytes(&v, row, &mut [0u8; 16]), &native[..], "{label}");
                assert_eq!(loc.is_null(&v, row), loc == from_payload && row % 3 == 0, "{label}");
                let Some(fi) = FixedInt::from_type_code(tc) else {
                    continue;
                };
                assert_eq!(loc.decode_i64(&v, row, fi), fi.decode_le_i64(native), "{label}");
                let kind = ScalarKind::Int(fi);
                assert_eq!(
                    order_bits(&loc, &v, row, kind),
                    kind.order_image(fi.decode_le_i64(native) as u64),
                    "{label}"
                );
            }
            for i in 0..natives.len() {
                for j in 0..natives.len() {
                    assert_eq!(
                        loc.cmp_non_null(&v, i, &v, j),
                        loc.opk_image(&v, i).cmp(&loc.opk_image(&v, j)),
                        "{tc} {arm} rows {i}, {j}"
                    );
                }
            }
        }
    }
}
