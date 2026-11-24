use super::*;
use crate::schema::SchemaColumn;
use crate::test_support::u64_pk_schema;
use gnitz_foundation::rng::mix;
use gnitz_wire::TypeCode;

/// [`ColMoves`]' null words against a bit-by-bit move over a window spanning several
/// blocks: a source that cannot carry a bit — a PK column or a NOT NULL slot —
/// clears its slot.
#[test]
fn col_moves_carry_each_copied_null_bit_into_its_slot() {
    // A U64 PK, then payload slots 0, 2 and 3 admitting NULL and slot 1 NOT NULL.
    let col = |nullable| SchemaColumn::new(TypeCode::I64, nullable);
    let schema = u64_pk_schema(&[col(true), col(false), col(true), col(true)]);
    let pk = schema.locate(0);
    let payload = |pi: usize| schema.payload_locator(pi);
    let copies = |srcs: &[ColumnLocator]| -> Vec<ColCopy> {
        srcs.iter()
            .enumerate()
            .map(|(slot, &src)| ColCopy { src, slot, width: 8 })
            .collect()
    };
    let nullable = schema.nullable_payload_slots();
    let (src_start, dst_base) = (3, 5);
    for (srcs, shape) in [
        (vec![pk, payload(1)], "Zero"),
        (vec![payload(0), payload(1), payload(2)], "Mask"),
        (vec![payload(2), payload(0), pk, payload(3), payload(1)], "Permute"),
    ] {
        let moves = ColMoves::new(copies(&srcs), &schema);
        for n in [0, 1, 255, 256, 257, 600] {
            let src_word = |row: usize| mix(row as u64 + 1).rotate_left(row as u32 % 64);
            let src: Vec<u8> = (0..src_start + n).flat_map(|row| src_word(row).to_le_bytes()).collect();
            let mut dst = vec![0xAAu8; (dst_base + n + 1) * 8];
            moves.write_null_rows(&src, src_start, &mut dst, dst_base, n);
            for row in 0..dst_base + n + 1 {
                let want = match row.checked_sub(dst_base).filter(|&r| r < n) {
                    None => u64::from_le_bytes([0xAA; 8]),
                    Some(r) => moves.copies().iter().fold(0u64, |w, c| match c.src {
                        ColumnLocator::Payload { slot, .. } if nullable >> slot & 1 != 0 => {
                            w | (src_word(src_start + r) >> slot & 1) << c.slot
                        }
                        _ => w,
                    }),
                };
                assert_eq!(gnitz_wire::read_u64_le(&dst, row * 8), want, "{shape}: n={n} row {row}");
            }
        }
    }
}

/// The string slots are the input payload slots a copy reads a German string
/// from; a fixed-width or PK source adds none.
#[test]
fn col_moves_name_the_string_slots_they_copy() {
    use TypeCode::{Blob, I64, String, U128};
    // Payload slots: 0 U128, 1-2 I64, 3 STRING, 4 I64, 5 BLOB.
    let schema = u64_pk_schema(&[
        SchemaColumn::new(U128, true),
        SchemaColumn::new(I64, false),
        SchemaColumn::new(I64, true),
        SchemaColumn::new(String, true),
        SchemaColumn::new(I64, false),
        SchemaColumn::new(Blob, false),
    ]);
    let srcs = [
        schema.payload_locator(3),
        schema.locate(0),
        schema.payload_locator(0),
        schema.payload_locator(5),
    ];
    let copies = srcs
        .iter()
        .enumerate()
        .map(|(slot, &src)| ColCopy { src, slot, width: src.size() })
        .collect();
    assert_eq!(ColMoves::new(copies, &schema).string_slots(), 1 << 3 | 1 << 5);
}
