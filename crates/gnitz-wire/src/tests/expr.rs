use super::*;

#[test]
fn round_trip_program_with_strings() {
    let code = [1u32, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let sinks = [0u32, 7, 1, 9];
    // Empty string, multi-byte UTF-8, and a non-UTF-8 byte string (byte-transparency).
    let s0: &[u8] = b"alpha";
    let s1: &[u8] = b"";
    let s2: &[u8] = "längre sträng".as_bytes();
    let s3: &[u8] = &[0xFF, 0x00, 0xFE, 0x80];
    let blob = encode_expr_blob(4, &code, &sinks, &[s0, s1, s2, s3]);
    let dec = decode_expr_blob(&blob).unwrap();
    assert_eq!(dec.result_reg, 4);
    assert_eq!(dec.code, code);
    assert_eq!(dec.sinks, sinks);
    assert_eq!(
        dec.const_strings,
        vec![s0.to_vec(), s1.to_vec(), s2.to_vec(), s3.to_vec()]
    );

    // The degenerate program is a valid one, not an absence.
    let dec = decode_expr_blob(&valid_empty()).expect("a valid empty program must decode");
    assert_eq!(dec.result_reg, 0);
    assert!(dec.code.is_empty() && dec.sinks.is_empty() && dec.const_strings.is_empty());
}

/// A valid empty program; the rejection table mutates a clone of this, so each
/// case differs from a decodable blob by exactly one flaw.
fn valid_empty() -> Vec<u8> {
    encode_expr_blob(0, &[], &[], &[] as &[&[u8]])
}

/// Every guard in `decode_expr_blob`, against the forgery that trips it. All of
/// them answer `None` — the program blob is carried opaquely by the circuit
/// codec, so a malformed one has no error text of its own to separate the cases;
/// the table row is what names which guard is under test.
#[test]
fn each_decode_guard_rejects_its_own_forgery() {
    let poke = |off: usize, val: u8| {
        let mut b = valid_empty();
        b[off] = val;
        b
    };
    // `n` (the code word count) lives at [12..16].
    let code_len = |n: u32| {
        let mut b = valid_empty();
        crate::write_u32_le(&mut b, 12, n);
        b
    };
    // The trailing string count is the last 4 bytes of an empty program.
    let s_count = |n: u32| {
        let mut b = valid_empty();
        let len = b.len();
        crate::write_u32_le(&mut b, len - 4, n);
        b
    };
    let short_string = {
        let mut b = s_count(1);
        b.extend_from_slice(&5u32.to_le_bytes());
        b.extend_from_slice(&[0xAA, 0xBB]); // only 2 of the 5 declared bytes
        b
    };
    let trailing = {
        let mut b = valid_empty();
        b.push(0);
        b
    };

    let cases: &[(&str, Vec<u8>)] = &[
        ("bad magic", poke(0, valid_empty()[0] ^ 0xFF)),
        ("bad version", poke(4, EXPR_BLOB_VERSION + 1)),
        ("reserved byte 5", poke(5, 1)),
        ("reserved byte 10", poke(10, 1)),
        ("reserved byte 11", poke(11, 1)),
        // 3 is not a whole instruction, which is five words.
        ("unaligned code length", code_len(3)),
        // One instruction declared, no code bytes present.
        ("truncated before code", code_len(5)),
        // A corrupt count must not drive a huge `with_capacity`.
        ("huge string count", s_count(u32::MAX)),
        ("string count with no bytes", s_count(1)),
        ("truncated mid string", short_string),
        // The trailing-garbage guard: `expect_consumed`, which no truncation
        // case reaches — those trip the reader first.
        ("trailing bytes", trailing),
    ];
    for (what, blob) in cases {
        assert!(decode_expr_blob(blob).is_none(), "{what} must be rejected");
    }
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
