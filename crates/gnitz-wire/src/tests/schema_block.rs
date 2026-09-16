use super::*;
use crate::{TypeCode, PK_LIST_MAX_COLS};

fn col(tc: TypeCode, name: &str, pk_pos: Option<u8>, nullable: bool) -> SchemaBlockCol<'_> {
    SchemaBlockCol {
        type_code: tc as u8,
        meta: ColMeta { nullable, pk_pos, ..Default::default() },
        name: name.as_bytes(),
    }
}

/// The decode error `block` yields, without needing `SchemaBlock: Debug`.
fn decode_err(block: &[u8], max_pk: usize) -> &'static str {
    SchemaBlock::decode(block, false, max_pk)
        .err()
        .expect("expected a rejection")
}

fn simple() -> Vec<u8> {
    let cols = [
        col(TypeCode::U64, "id", Some(0), false),
        col(TypeCode::String, "a_rather_long_column_name", None, true),
    ];
    encode(7, &cols)
}

#[test]
fn roundtrips_shape_names_and_pk_order() {
    let cols = [
        col(TypeCode::U64, "b", Some(1), false),
        col(TypeCode::I32, "a", Some(0), false),
        col(TypeCode::String, "payload_name_over_twelve", None, true),
    ];
    let block = encode(42, &cols);
    let sb = SchemaBlock::decode(&block, false, MAX_PK_COLUMNS).unwrap();
    assert_eq!(sb.num_columns(), 3);
    assert_eq!(sb.columns().collect::<Vec<_>>(), cols.to_vec());
    // Declared order `(a, b)`, not column order `(b, a)`.
    assert_eq!(sb.pk_indices(), &[1, 0]);
}

/// A header claiming more regions than the buffer can hold must be rejected
/// before any `dir_entry` indexes past the end.
#[test]
fn rejects_a_directory_that_overflows_the_buffer() {
    let mut block = simple();
    crate::write_u32_le(&mut block, crate::WAL_OFF_NUM_REGIONS, 100_000);
    assert_eq!(
        decode_err(&block, MAX_PK_COLUMNS),
        "schema block directory overflows buffer"
    );
}

#[test]
fn encoded_len_matches_what_encode_writes() {
    let cols = [
        col(TypeCode::U64, "id", Some(0), false),
        col(TypeCode::String, "spills_into_the_blob_heap", None, false),
    ];
    assert_eq!(encode(1, &cols).len(), encoded_len(&cols));
}

/// A block with fewer regions than the meta-schema shape. Every region this
/// decoder reads still fits, so a lower-bound check would accept it.
#[test]
fn rejects_a_block_with_the_wrong_region_count() {
    let block = simple();
    let n = 5u32; // pk, weight, null, type_code, flags — no name, no blob
    let mut short = block.clone();
    crate::write_u32_le(&mut short, crate::WAL_OFF_NUM_REGIONS, n);
    assert_eq!(decode_err(&short, MAX_PK_COLUMNS), "schema block region count mismatch");
}

/// `max_pk` is the consumer's own limit: the same block is admissible to the
/// engine (5) and not to the client (4).
#[test]
fn pk_arity_is_bounded_by_the_caller_not_a_shared_cap() {
    let cols: Vec<SchemaBlockCol> = (0..MAX_PK_COLUMNS)
        .map(|i| SchemaBlockCol {
            type_code: TypeCode::U64 as u8,
            meta: ColMeta {
                pk_pos: Some(i as u8),
                ..Default::default()
            },
            name: b"k",
        })
        .collect();
    let block = encode(1, &cols);
    assert_eq!(
        SchemaBlock::decode(&block, false, MAX_PK_COLUMNS)
            .unwrap()
            .pk_indices()
            .len(),
        MAX_PK_COLUMNS
    );
    assert_eq!(decode_err(&block, PK_LIST_MAX_COLS), "too many PK columns");
}

#[test]
fn rejects_a_nullable_or_ineligible_pk_column() {
    let nullable = [col(TypeCode::U64, "id", Some(0), true)];
    assert_eq!(
        decode_err(&encode(1, &nullable), MAX_PK_COLUMNS),
        "PK column must be non-nullable"
    );
    let float = [col(TypeCode::F64, "id", Some(0), false)];
    assert_eq!(
        decode_err(&encode(1, &float), MAX_PK_COLUMNS),
        "PK column type not PK-eligible"
    );
}

#[test]
fn rejects_a_block_with_no_pk_column() {
    let cols = [col(TypeCode::U64, "a", None, false)];
    assert_eq!(decode_err(&encode(1, &cols), MAX_PK_COLUMNS), "no PK column");
}

/// One forgery: a mutation of a parsed block, given its region offsets.
type Forge = fn(&mut [u8], &[u64]);

/// [`simple`] with `mutate` applied against its parsed region offsets.
fn forged(mutate: Forge) -> Vec<u8> {
    let mut block = simple();
    let mut offs = [0u64; wal::MAX_WIRE_REGIONS];
    let mut sizes = [0u32; wal::MAX_WIRE_REGIONS];
    wal::validate_and_parse(&block, &mut offs, &mut sizes, false).unwrap();
    mutate(&mut block, &offs);
    block
}

/// Every guard that reads the block body, against the forgery that trips
/// it. The message is what separates them: a merged table asserting only "an
/// error" would pass on the wrong guard.
#[test]
fn each_schema_guard_rejects_its_own_forgery() {
    let cases: [(&str, Forge); 4] = [
        // A type_code word whose low byte names a valid type but whose high
        // bits are set. Truncating to `u8` accepts it; the peer's `u8`-typed
        // decode rejects it — so the two ends would disagree on which blocks
        // are legal.
        ("schema: invalid type code", |b, offs| {
            write_u64_le(b, offs[REG_TYPE_CODE] as usize, 0x0000_0001_0000_0002)
        }),
        // The block asserts no column field is null; a set bit would mean the
        // type, flags or name of that column is absent from the cells the
        // decoder nonetheless reads.
        ("schema block declares a null column field", |b, offs| {
            write_u64_le(b, offs[REG_NULL_BMP] as usize, 1)
        }),
        ("schema col_idx not in monotonic order", |b, offs| {
            let pk_off = offs[REG_PK] as usize;
            b[pk_off..pk_off + 8].copy_from_slice(&5u64.to_be_bytes());
        }),
        // Row 1's name is the long one; push its heap offset past the heap.
        ("schema name cell is not in canonical form", |b, offs| {
            write_u64_le(b, offs[REG_NAME] as usize + 16 + 8, 1 << 20)
        }),
    ];
    for (want, forge) in cases {
        assert_eq!(decode_err(&forged(forge), MAX_PK_COLUMNS), want);
    }
}

/// The per-column meta word both ends encode and decode: every field
/// round-trips, and a non-PK column reports no position however the other bits
/// are set.
#[test]
fn column_meta_roundtrip() {
    for nullable in [false, true] {
        for hidden in [false, true] {
            for serial in [false, true] {
                for pk_pos in [None, Some(0u8), Some(1), Some(3), Some(u8::MAX)] {
                    // Scale 0 is every non-DECIMAL column, and `u8::MAX` fills
                    // the field — so neither edge may bleed into a neighbour.
                    for scale in [0u8, 7, u8::MAX] {
                        let m = ColMeta { nullable, hidden, serial, scale, pk_pos };
                        assert_eq!(m.pack() & !DEFINED_BITS, 0);
                        assert_eq!(ColMeta::from_flags(m.pack()), m);
                    }
                }
            }
        }
    }
}
