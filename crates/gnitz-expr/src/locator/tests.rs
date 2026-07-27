//! Resolved-addressing reads, driven through a non-engine [`crate::BatchView`].

use crate::test_support::locator_fixture as fixture;
use crate::ColumnLocator;
use gnitz_wire::type_code as tc;

#[test]
fn pk_locator_reads_decode_the_opk_sign_flip() {
    let v = fixture();
    // Trailing signed column of a compound PK — non-zero `byte_off`.
    let signed = ColumnLocator::Pk {
        byte_off: 4,
        size: 8,
        type_code: tc::I64,
    };
    // Leading unsigned column.
    let unsigned = ColumnLocator::Pk {
        byte_off: 0,
        size: 4,
        type_code: tc::U32,
    };
    assert_eq!(signed.size(), 8);
    assert_eq!(signed.type_code(), tc::I64);
    // `bytes` is the at-rest OPK image: big-endian with the sign bit flipped.
    let mut want = [0u8; 8];
    gnitz_wire::encode_pk_column(&(-1i64).to_le_bytes(), tc::I64, &mut want);
    assert_eq!(signed.bytes(&v, 0), &want[..]);
    assert_eq!(unsigned.bytes(&v, 0), &7u32.to_be_bytes()[..]);
    // `native_le_bytes` undoes it.
    let mut scratch = [0u8; 16];
    assert_eq!(signed.native_le_bytes(&v, 0, &mut scratch), &(-1i64).to_le_bytes()[..]);
    assert_eq!(signed.native_le_bytes(&v, 2, &mut scratch), &i64::MIN.to_le_bytes()[..]);
    assert_eq!(
        unsigned.native_le_bytes(&v, 2, &mut scratch),
        &u32::MAX.to_le_bytes()[..]
    );
    // `native_key` is the zero-extended two's-complement value.
    assert_eq!(signed.native_key(&v, 0), (-1i64) as u64 as u128);
    assert_eq!(signed.native_key(&v, 2), i64::MIN as u64 as u128);
    assert_eq!(unsigned.native_key(&v, 0), 7u128);
    // `route_key` is the widened OPK image — sign-flipped, so -1 lands below 0.
    assert!(signed.route_key(&v, 2) < signed.route_key(&v, 0));
    assert!(signed.route_key(&v, 0) < signed.route_key(&v, 1));
    assert_eq!(unsigned.route_key(&v, 0), 7u128);
}

#[test]
fn payload_locator_reads_are_verbatim_native_le() {
    let v = fixture();
    let narrow = ColumnLocator::Payload {
        slot: 0,
        size: 4,
        type_code: tc::I32,
    };
    let wide = ColumnLocator::Payload {
        slot: 1,
        size: 16,
        type_code: tc::U128,
    };
    assert_eq!(narrow.bytes(&v, 1), &(-3i32).to_le_bytes()[..]);
    let mut scratch = [0u8; 16];
    assert_eq!(narrow.native_le_bytes(&v, 1, &mut scratch), &(-3i32).to_le_bytes()[..]);
    assert_eq!(narrow.native_key(&v, 1), (-3i32) as u32 as u128);
    // A 16-byte cell reads all 16 bytes, not a zero-extended low 8.
    assert_eq!(wide.bytes(&v, 2), &(1u128 << 100).to_le_bytes()[..]);
    assert_eq!(wide.native_key(&v, 2), 1u128 << 100);
    assert_eq!(wide.route_key(&v, 2), 1u128 << 100);
}

#[test]
fn is_null_reads_the_addressed_slot_bit() {
    let mut v = fixture();
    v.set_null(1, 2);
    let slot0 = ColumnLocator::Payload {
        slot: 0,
        size: 4,
        type_code: tc::I32,
    };
    let slot2 = ColumnLocator::Payload {
        slot: 2,
        size: 8,
        type_code: tc::U64,
    };
    let pk = ColumnLocator::Pk {
        byte_off: 0,
        size: 4,
        type_code: tc::U32,
    };
    assert!(slot2.is_null(&v, 1));
    assert!(!slot2.is_null(&v, 0));
    assert!(!slot0.is_null(&v, 1), "a set bit at slot 2 must not read as slot 0");
    assert!(!pk.is_null(&v, 1), "PK columns are never null");
}
