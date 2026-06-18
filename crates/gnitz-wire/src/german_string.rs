//! German string codec: 16-byte inline/blob string representation.

/// Threshold for inline German String storage (bytes).
pub const SHORT_STRING_THRESHOLD: usize = 12;

/// Encode a byte slice as a 16-byte German String struct, appending overflow
/// data to `blob`.
///
/// Layout:
///   [0..4]  length (u32 LE)
///   [4..8]  prefix — first min(4, len) bytes, zero-padded
///   [8..16] if len ≤ 12: suffix bytes [4..len], zero-padded
///           if len > 12: blob arena offset (u64 LE)
pub fn encode_german_string(s: &[u8], blob: &mut Vec<u8>) -> [u8; 16] {
    let len = s.len();
    // The 4-byte length field caps a string at u32::MAX bytes. Silently
    // truncating `len as u32` would corrupt this persisted format, so reject
    // an over-long string outright (a release-build assert, not debug-only).
    assert!(
        len <= u32::MAX as usize,
        "encode_german_string: length {len} exceeds u32::MAX"
    );
    let mut st = [0u8; 16];
    st[0..4].copy_from_slice(&(len as u32).to_le_bytes());
    if len == 0 {
        return st;
    }
    let pfx = len.min(4);
    st[4..4 + pfx].copy_from_slice(&s[..pfx]);
    if len <= SHORT_STRING_THRESHOLD {
        if len > 4 {
            st[8..8 + (len - 4)].copy_from_slice(&s[4..len]);
        }
    } else {
        let off = blob.len();
        blob.extend_from_slice(s);
        st[8..16].copy_from_slice(&(off as u64).to_le_bytes());
    }
    st
}

/// Decode a 16-byte German String struct into raw bytes, or `None` if a
/// long string's blob offset/length overruns `blob`.
pub fn try_decode_german_string(st: &[u8; 16], blob: &[u8]) -> Option<Vec<u8>> {
    let len = u32::from_le_bytes(st[0..4].try_into().unwrap()) as usize;
    if len == 0 {
        return Some(Vec::new());
    }
    if len <= SHORT_STRING_THRESHOLD {
        let mut out = Vec::with_capacity(len);
        let pfx = len.min(4);
        out.extend_from_slice(&st[4..4 + pfx]);
        if len > 4 {
            out.extend_from_slice(&st[8..8 + (len - 4)]);
        }
        Some(out)
    } else {
        // Resolve the blob slice entirely in u64 space before narrowing to
        // usize: a 64-bit wire offset would silently truncate under `as usize`
        // on a 32-bit target, and the truncated value could pass the bounds
        // check while pointing at the wrong range. Once `end <= blob.len()`
        // holds, both `off` and `end` fit in usize losslessly.
        let off = u64::from_le_bytes(st[8..16].try_into().unwrap());
        let end = off.checked_add(len as u64)?;
        if end > blob.len() as u64 {
            return None;
        }
        Some(blob[off as usize..end as usize].to_vec())
    }
}

/// Decode a 16-byte German String struct into raw bytes.
/// `blob` is the shared blob arena. Panics if a long string's offset/length
/// overruns `blob`; use `try_decode_german_string` on untrusted input.
pub fn decode_german_string(st: &[u8; 16], blob: &[u8]) -> Vec<u8> {
    try_decode_german_string(st, blob).expect("decode_german_string: blob offset/length out of bounds")
}

#[cfg(test)]
mod german_string_tests {
    use super::*;

    fn roundtrip(s: &[u8]) {
        let mut blob = Vec::new();
        let st = encode_german_string(s, &mut blob);
        assert_eq!(
            try_decode_german_string(&st, &blob).as_deref(),
            Some(s),
            "try_decode roundtrip failed for len {}",
            s.len(),
        );
        assert_eq!(
            decode_german_string(&st, &blob),
            s,
            "decode roundtrip failed for len {}",
            s.len()
        );
    }

    #[test]
    fn roundtrip_across_length_boundaries() {
        roundtrip(b""); // empty
        roundtrip(b"a"); // 1 (prefix only)
        roundtrip(b"abcd"); // 4 (prefix exactly full)
        roundtrip(b"abcde"); // 5 (prefix + 1 suffix byte)
        roundtrip(b"abcdefghijkl"); // 12 == SHORT_STRING_THRESHOLD (fully inline)
        roundtrip(b"abcdefghijklm"); // 13 (first length that spills to blob)
        roundtrip(&vec![0xABu8; 1000]); // long blob string
    }

    #[test]
    fn roundtrip_long_string_after_prefix_in_blob() {
        // A non-empty blob prefix exercises the offset (not just offset 0).
        let mut blob = vec![0u8; 7];
        let payload = vec![0x5Au8; 40];
        let st = encode_german_string(&payload, &mut blob);
        assert_eq!(try_decode_german_string(&st, &blob).as_deref(), Some(&payload[..]));
    }

    #[test]
    fn try_decode_rejects_offset_plus_len_past_blob() {
        // len > threshold so the decoder reads from the blob; offset 0 but the
        // blob is shorter than len.
        let mut st = [0u8; 16];
        st[0..4].copy_from_slice(&100u32.to_le_bytes());
        st[8..16].copy_from_slice(&0u64.to_le_bytes());
        let blob = vec![0u8; 50];
        assert_eq!(try_decode_german_string(&st, &blob), None);
    }

    #[test]
    fn try_decode_rejects_offset_at_blob_end() {
        let mut st = [0u8; 16];
        st[0..4].copy_from_slice(&13u32.to_le_bytes());
        st[8..16].copy_from_slice(&50u64.to_le_bytes()); // offset == blob.len()
        let blob = vec![0u8; 50];
        assert_eq!(try_decode_german_string(&st, &blob), None);
    }

    #[test]
    fn try_decode_rejects_offset_overflow() {
        // offset + len overflows u64; checked_add must catch it rather than
        // wrapping to a small in-bounds value.
        let mut st = [0u8; 16];
        st[0..4].copy_from_slice(&20u32.to_le_bytes());
        st[8..16].copy_from_slice(&(u64::MAX - 5).to_le_bytes());
        let blob = vec![0u8; 50];
        assert_eq!(try_decode_german_string(&st, &blob), None);
    }

    #[test]
    fn try_decode_long_string_exact_fit() {
        let payload = vec![0x33u8; 30];
        let mut blob = Vec::new();
        let st = encode_german_string(&payload, &mut blob);
        assert_eq!(blob.len(), 30, "exact-fit precondition");
        assert_eq!(try_decode_german_string(&st, &blob).as_deref(), Some(&payload[..]));
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn decode_panics_on_out_of_bounds() {
        let mut st = [0u8; 16];
        st[0..4].copy_from_slice(&100u32.to_le_bytes());
        let blob = vec![0u8; 10];
        let _ = decode_german_string(&st, &blob);
    }
}
