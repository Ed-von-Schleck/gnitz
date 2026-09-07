use super::*;

#[test]
fn round_trip_program_with_strings() {
    let code = [[1u32, 2, 3, 4, 5], [6, 7, 8, 9, 10]];
    let sinks = [[0u32, 7], [1, 9]];
    // Empty string, multi-byte UTF-8, and a non-UTF-8 byte string (byte-transparency).
    let s0: &[u8] = b"alpha";
    let s1: &[u8] = b"";
    let s2: &[u8] = "längre sträng".as_bytes();
    let s3: &[u8] = &[0xFF, 0x00, 0xFE, 0x80];
    let blob = encode_expr_blob(4, code.into_iter(), sinks.into_iter(), &[s0, s1, s2, s3]);
    let dec = decode_expr_blob(&blob).unwrap();
    assert_eq!(dec.output, 4);
    // The regions come back as the bytes they were written as: the framing
    // counts words, and never reassembles one.
    assert_eq!(dec.code, crate::as_le_bytes(&code.concat()));
    assert_eq!(dec.sinks, crate::as_le_bytes(&sinks.concat()));
    assert_eq!(dec.const_strings, vec![s0, s1, s2, s3]);

    // The degenerate program is a valid one, not an absence.
    let empty = valid_empty();
    let dec = decode_expr_blob(&empty).expect("a valid empty program must decode");
    assert_eq!(dec.output, 0);
    assert!(dec.code.is_empty() && dec.sinks.is_empty() && dec.const_strings.is_empty());
}

/// A valid empty program; the rejection table mutates a clone of this, so each
/// case differs from a decodable blob by exactly one flaw.
fn valid_empty() -> Vec<u8> {
    encode_expr_blob(0, std::iter::empty(), std::iter::empty(), &[] as &[&[u8]])
}

/// Every guard in `decode_expr_blob`, against the forgery that trips it — and the
/// message it answers with, so a corrupt blob is diagnosable from the log alone.
#[test]
fn each_decode_guard_rejects_its_own_forgery() {
    // The three region counts of an empty program: code at [4..8], sinks at
    // [8..12], strings at [12..16].
    let count = |off: usize, n: u32| {
        let mut b = valid_empty();
        crate::write_u32_le(&mut b, off, n);
        b
    };
    let short_string = {
        let mut b = count(12, 1);
        b.extend_from_slice(&5u32.to_le_bytes());
        b.extend_from_slice(&[0xAA, 0xBB]); // only 2 of the 5 declared bytes
        b
    };
    let trailing = {
        let mut b = valid_empty();
        b.push(0);
        b
    };

    let cases: &[(&str, Vec<u8>, &str)] = &[
        // One instruction declared, no code bytes present.
        ("truncated before code", count(4, 5), "truncated"),
        ("truncated before sinks", count(8, 2), "truncated"),
        // A corrupt count must not drive a huge `with_capacity`.
        ("huge string count", count(12, u32::MAX), "string count"),
        ("string count with no bytes", count(12, 1), "string count"),
        ("truncated mid string", short_string, "truncated"),
        // The trailing-garbage guard: `expect_consumed`, which no truncation
        // case reaches — those trip the reader first.
        ("trailing bytes", trailing, "trailing"),
    ];
    for (what, blob, want) in cases {
        let msg = decode_expr_blob(blob).expect_err(&format!("{what} must be rejected"));
        assert!(
            msg.starts_with("expr blob: ") && msg.contains(want),
            "{what}: message must name the format and the fault, got: {msg}"
        );
    }
}
