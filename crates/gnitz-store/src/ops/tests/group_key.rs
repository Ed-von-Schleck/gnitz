use super::*;
use crate::schema::{type_code, SchemaColumn};
use crate::storage::Batch;

// ---------------------------------------------------------------------------
// Group key
// ---------------------------------------------------------------------------

/// The canonical single-column group key must be the value's OPK image (native
/// for unsigned, sign-flipped for signed) and must be the *same* image whether
/// the column is a PK column or a payload column. A distributed join routes one
/// side by its PK and the other by a payload FK; were the two to disagree, equal
/// keys would land on different workers and the join would drop rows.
#[test]
fn single_col_group_key_is_the_opk_image_from_either_side() {
    // The group key of the value `le` held in column 1 of `[U64 pk, <tc>]`,
    // with column 1 either a second PK column or the sole payload column.
    let key = |tc: u8, le: &[u8], col1_is_pk: bool| -> u128 {
        let cols = [SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)];
        let schema = SchemaDescriptor::new(&cols, if col1_is_pk { &[0, 1] } else { &[0] });
        let mut b = Batch::with_capacity(&schema, 1);
        if col1_is_pk {
            let mut native = [0u8; 16];
            native[..le.len()].copy_from_slice(le);
            b.extend_pk_opk(&[0, u128::from_le_bytes(native)]);
        } else {
            b.extend_pk(0u128);
        }
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        if !col1_is_pk {
            b.extend_col(schema.try_payload_idx(1).unwrap(), le);
        }
        b.count += 1;
        GroupKeyCols::new(&schema, &[1]).key_row(&b.as_mem_batch(), 0)
    };

    for (tc, vals) in [
        (type_code::I32, vec![1i128, -1, 100, i32::MIN as i128, i32::MAX as i128]),
        (type_code::I64, vec![0, -1, i64::MIN as i128, i64::MAX as i128]),
        (type_code::U16, vec![0, 1, 0xBEEF, u16::MAX as i128]),
        (type_code::U64, vec![0, 1, u64::MAX as i128]),
    ] {
        let width = SchemaColumn::new(tc, 0).size() as usize;
        for v in vals {
            let le = &(v as u128).to_le_bytes()[..width];
            // Signed columns are sign-flipped into the OPK image; unsigned ones
            // pass through, so the OPK image is the native value.
            let want = if gnitz_wire::is_signed_int(tc) {
                (v + (1i128 << (8 * width - 1))) as u128
            } else {
                v as u128
            };
            assert_eq!(key(tc, le, true), want, "PK-column key for {tc} v={v}");
            assert_eq!(key(tc, le, false), want, "payload-column key for {tc} v={v}");
        }
    }
}
