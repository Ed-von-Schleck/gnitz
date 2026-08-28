use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

// Co-partition invariant (bug #2 regression): a single narrow-int
// routing/group column must yield the canonical OPK key — the value an
// OPK PK column produces via `widen_pk_be` (native for unsigned, sign-
// flipped for signed). Pre-OPK both PK and payload sides used the raw
// native value; after the flip the PK side is sign-flipped, so a signed
// payload FK must match it or a distributed join silently drops rows.
#[test]
fn group_key_single_narrow_int_canonical_widen() {
    use super::GroupKeyCols;
    use crate::storage::Batch as B;

    // Group key for `le` (native LE bytes) stored as a payload column at
    // idx 1 (U64 PK + tested column), routed by col 1.
    let key_as_payload = |tc: u8, le: &[u8]| -> u128 {
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0), SchemaColumn::new(tc, 0)], &[0]);
        let pi = schema.try_payload_idx(1).unwrap();
        let mut b = B::with_capacity(schema, 1);
        b.extend_pk(0u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(pi, le);
        b.count += 1;
        GroupKeyCols::new(&schema, &[1]).key_row(&b.as_mem_batch(), 0)
    };

    // Signed I32: canonical key is the sign-flipped value (top bit toggled
    // on the native bits), NOT the old zero-extended native value.
    for (v, expected) in [
        (1i32, 0x8000_0001u128),
        (-1, 0x7FFF_FFFF),
        (2, 0x8000_0002),
        (100, 0x8000_0064),
        (i32::MIN, 0x0000_0000),
        (i32::MAX, 0xFFFF_FFFF),
    ] {
        assert_eq!(
            key_as_payload(type_code::I32, &v.to_le_bytes()),
            expected,
            "I32 v={v}: canonical sign-flipped key",
        );
    }

    // Unsigned U16: canonical key equals the native value (OPK == native).
    for v in [0u16, 1, 0xBEEF, u16::MAX] {
        assert_eq!(key_as_payload(type_code::U16, &v.to_le_bytes()), v as u128);
    }
}
