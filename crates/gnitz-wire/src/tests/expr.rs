use super::*;

#[test]
fn round_trip_empty_program() {
    let blob = encode_expr_blob(0, 0, &[], &[]);
    let dec = decode_expr_blob(&blob).unwrap();
    assert_eq!(dec.num_regs, 0);
    assert_eq!(dec.result_reg, 0);
    assert!(dec.code.is_empty());
    assert!(dec.const_strings.is_empty());
}

#[test]
fn round_trip_program_with_strings() {
    let code = [1u32, 2, 3, 4, 5, 6, 7, 8];
    // Empty string, multi-byte UTF-8, and a non-UTF-8 byte string (byte-transparency).
    let s0: &[u8] = b"alpha";
    let s1: &[u8] = b"";
    let s2: &[u8] = "längre sträng".as_bytes();
    let s3: &[u8] = &[0xFF, 0x00, 0xFE, 0x80];
    let blob = encode_expr_blob(5, 4, &code, &[s0, s1, s2, s3]);
    let dec = decode_expr_blob(&blob).unwrap();
    assert_eq!(dec.num_regs, 5);
    assert_eq!(dec.result_reg, 4);
    assert_eq!(dec.code, code);
    assert_eq!(
        dec.const_strings,
        vec![s0.to_vec(), s1.to_vec(), s2.to_vec(), s3.to_vec()]
    );
}

/// A valid empty program round-trips through `encode_expr_blob`; the reject
/// tests below mutate a clone of this so each differs from a valid blob by
/// exactly one flaw.
fn valid_empty() -> Vec<u8> {
    encode_expr_blob(0, 0, &[], &[])
}

#[test]
fn rejects_bad_magic() {
    let mut b = valid_empty();
    b[0] ^= 0xFF;
    assert!(decode_expr_blob(&b).is_none());
}

#[test]
fn rejects_bad_version() {
    let mut b = valid_empty();
    b[4] = EXPR_BLOB_VERSION + 1;
    assert!(decode_expr_blob(&b).is_none());
}

#[test]
fn rejects_nonzero_reserved() {
    for off in [5usize, 10, 11] {
        let mut b = valid_empty();
        b[off] = 1;
        assert!(decode_expr_blob(&b).is_none(), "reserved byte {off} must be zero");
    }
}

#[test]
fn rejects_unaligned_code_length() {
    let mut b = valid_empty();
    // n lives at [12..16]; 3 is not a multiple of 4.
    b[12..16].copy_from_slice(&3u32.to_le_bytes());
    assert!(decode_expr_blob(&b).is_none());
}

#[test]
fn rejects_truncation_before_code() {
    // n = 4 words (16 bytes) declared, but no code bytes present.
    let mut b = valid_empty();
    b[12..16].copy_from_slice(&4u32.to_le_bytes());
    assert!(decode_expr_blob(&b).is_none());
}

#[test]
fn rejects_truncation_mid_string() {
    // One string of length 5 declared, but fewer than 5 bytes follow.
    let mut b = encode_expr_blob(0, 0, &[], &[]);
    // Overwrite the trailing string count (last 4 bytes) to 1, then append a
    // length prefix of 5 with no payload.
    let len = b.len();
    b[len - 4..len].copy_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&5u32.to_le_bytes());
    b.extend_from_slice(&[0xAA, 0xBB]); // only 2 of 5 bytes
    assert!(decode_expr_blob(&b).is_none());
}

#[test]
fn rejects_huge_s_count() {
    // s_count = u32::MAX with no string bytes must return None, not OOM.
    let mut b = encode_expr_blob(0, 0, &[], &[]);
    let len = b.len();
    b[len - 4..len].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_expr_blob(&b).is_none(), "huge s_count must be rejected");
}

#[test]
fn rejects_s_count_with_no_remaining_bytes() {
    // s_count = 1 but no string length prefix bytes remaining → None.
    let mut b = encode_expr_blob(0, 0, &[], &[]);
    let len = b.len();
    b[len - 4..len].copy_from_slice(&1u32.to_le_bytes());
    assert!(
        decode_expr_blob(&b).is_none(),
        "s_count with too few bytes must be rejected"
    );
}

#[test]
fn accepts_valid_empty_program() {
    assert!(
        decode_expr_blob(&valid_empty()).is_some(),
        "valid empty program must decode"
    );
}

#[test]
fn load_const_round_trips() {
    for v in [
        0i64,
        1,
        -1,
        i64::MIN,
        i64::MAX,
        0x0000_0001_0000_0000,
        -0x0000_0001_0000_0000,
    ] {
        let (a1, a2) = encode_load_const(v);
        assert_eq!(decode_load_const(a1, a2), v, "load_const round-trip for {v}");
    }
}

#[test]
fn operand_pair_round_trip() {
    for a in 0u32..64 {
        for b in 0u32..64 {
            assert_eq!(unpack_operand_pair(pack_operand_pair(a, b)), (a as u16, b as u16));
        }
    }
}
