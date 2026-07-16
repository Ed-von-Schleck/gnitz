//! Expression bytecode: opcodes, operand packing, and blob framing.

pub const EXPR_LOAD_COL_INT: u32 = 1;
pub const EXPR_LOAD_COL_FLOAT: u32 = 2;
pub const EXPR_LOAD_CONST: u32 = 3;
pub const EXPR_INT_ADD: u32 = 4;
pub const EXPR_INT_SUB: u32 = 5;
pub const EXPR_INT_MUL: u32 = 6;
pub const EXPR_INT_DIV: u32 = 7;
pub const EXPR_INT_MOD: u32 = 8;
pub const EXPR_INT_NEG: u32 = 9;
pub const EXPR_FLOAT_ADD: u32 = 10;
pub const EXPR_FLOAT_SUB: u32 = 11;
pub const EXPR_FLOAT_MUL: u32 = 12;
pub const EXPR_FLOAT_DIV: u32 = 13;
pub const EXPR_FLOAT_NEG: u32 = 14;
pub const EXPR_CMP_EQ: u32 = 15;
pub const EXPR_CMP_NE: u32 = 16;
pub const EXPR_CMP_GT: u32 = 17;
pub const EXPR_CMP_GE: u32 = 18;
pub const EXPR_CMP_LT: u32 = 19;
pub const EXPR_CMP_LE: u32 = 20;
pub const EXPR_FCMP_EQ: u32 = 21;
pub const EXPR_FCMP_NE: u32 = 22;
pub const EXPR_FCMP_GT: u32 = 23;
pub const EXPR_FCMP_GE: u32 = 24;
pub const EXPR_FCMP_LT: u32 = 25;
pub const EXPR_FCMP_LE: u32 = 26;
pub const EXPR_BOOL_AND: u32 = 27;
pub const EXPR_BOOL_OR: u32 = 28;
pub const EXPR_BOOL_NOT: u32 = 29;
pub const EXPR_IS_NULL: u32 = 30;
pub const EXPR_IS_NOT_NULL: u32 = 31;
pub const EXPR_EMIT: u32 = 32;
pub const EXPR_INT_TO_FLOAT: u32 = 33;
pub const EXPR_COPY_COL: u32 = 34;
/// Conditional select (SQL CASE blend): `[EXPR_SELECT, dst, cond, a | (b << 16)]`.
/// Three register sources — `cond`, `a`, `b` — packed into two operand words:
/// `cond` occupies word `a1` alone, while `a`/`b` are packed as the low/high 16
/// bits of word `a2`. This is the only opcode that packs two registers into one
/// word (LOAD_CONST packs a 64-bit *value*, not registers; see the decode site
/// in `gnitz-engine`'s `program.rs`). Rows where `cond` is non-NULL and truthy
/// take `a`'s value + null bit; all other rows (false **or NULL** cond) take
/// `b`'s. Carries a value, never a boolean classification.
pub const EXPR_SELECT: u32 = 35;
/// Materialize a NULL value: `[EXPR_LOAD_NULL, dst, 0, 0]` — value 0, null bit
/// set for every row. Backs `CASE` without `ELSE` (→ `ELSE NULL`) and `NULLIF`.
pub const EXPR_LOAD_NULL: u32 = 36;
pub const EXPR_STR_COL_EQ_CONST: u32 = 40;
pub const EXPR_STR_COL_LT_CONST: u32 = 41;
pub const EXPR_STR_COL_LE_CONST: u32 = 42;
pub const EXPR_STR_COL_EQ_COL: u32 = 43;
pub const EXPR_STR_COL_LT_COL: u32 = 44;
pub const EXPR_STR_COL_LE_COL: u32 = 45;

// ---------------------------------------------------------------------------
// Blob framing constants and operand packing
// ---------------------------------------------------------------------------

/// Wire-format magic for serialised expr blobs: ASCII "EXPR".
const EXPR_BLOB_MAGIC: u32 = 0x5258_5045;
/// Current wire-format version for expr blobs.
const EXPR_BLOB_VERSION: u8 = 1;
/// Fixed header width, in bytes.
const EXPR_BLOB_HEADER_SIZE: usize = 16;

/// `EXPR_LOAD_CONST` splits a 64-bit value across its two operand words
/// (`a1` = low 32 bits, `a2` = high 32 bits).
#[inline]
pub const fn encode_load_const(v: i64) -> (u32, u32) {
    (v as u32, (v >> 32) as u32)
}
#[inline]
pub const fn decode_load_const(a1: u32, a2: u32) -> i64 {
    ((a2 as i64) << 32) | (a1 as i64 & 0xFFFF_FFFF)
}

/// `EXPR_SELECT` packs source registers `a` and `b` as the low and high 16 bits of its
/// second operand word (`cond` occupies the first word alone). The only two-register
/// packing in the encoding.
#[inline]
pub const fn encode_select_operands(a: u32, b: u32) -> u32 {
    (a & 0xFFFF) | ((b & 0xFFFF) << 16)
}
#[inline]
pub const fn decode_select_operands(w: u32) -> (u16, u16) {
    ((w & 0xFFFF) as u16, (w >> 16) as u16)
}

/// A decoded expr blob. `code` and the string bytes are copied out of the input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprBlob {
    pub num_regs: u32,
    pub result_reg: u32,
    pub code: Vec<u32>,
    pub const_strings: Vec<Vec<u8>>,
}

/// Serialise an expr program. Layout (all little-endian):
///
/// ```text
/// 0   4   magic "EXPR" (u32)
/// 4   1   version (u8)
/// 5   1   reserved (must be 0)
/// 6   2   num_regs (u16)
/// 8   2   result_reg (u16)
/// 10  2   reserved (must be 0)
/// 12  4   code word count N (u32; multiple of 4)
/// 16  4N  code words (u32 each)
/// ..  4   string count S (u32)
/// ..  S × { 4-byte length L, L bytes }
/// ```
pub fn encode_expr_blob(num_regs: u32, result_reg: u32, code: &[u32], const_strings: &[&[u8]]) -> Vec<u8> {
    debug_assert!(code.len().is_multiple_of(4), "code length {} not 4-aligned", code.len());
    let mut buf = Vec::with_capacity(
        EXPR_BLOB_HEADER_SIZE + code.len() * 4 + 4 + const_strings.iter().map(|s| 4 + s.len()).sum::<usize>(),
    );
    buf.extend_from_slice(&EXPR_BLOB_MAGIC.to_le_bytes());
    buf.push(EXPR_BLOB_VERSION);
    buf.push(0); // reserved
    buf.extend_from_slice(&(num_regs as u16).to_le_bytes());
    buf.extend_from_slice(&(result_reg as u16).to_le_bytes());
    buf.extend_from_slice(&[0, 0]); // reserved
    buf.extend_from_slice(&(code.len() as u32).to_le_bytes());
    for &word in code {
        buf.extend_from_slice(&word.to_le_bytes());
    }
    buf.extend_from_slice(&(const_strings.len() as u32).to_le_bytes());
    for &s in const_strings {
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s);
    }
    buf
}

/// Inverse of `encode_expr_blob`. Validates magic, version, reserved bytes, code-length
/// alignment, region lengths, and the string-count OOM bound. Does **not** validate program
/// semantics (opcodes, register operands, column indices) — that is the decoder-consumer's job.
pub fn decode_expr_blob(blob: &[u8]) -> Option<ExprBlob> {
    if blob.len() < EXPR_BLOB_HEADER_SIZE {
        return None;
    }
    if u32::from_le_bytes(blob[0..4].try_into().unwrap()) != EXPR_BLOB_MAGIC {
        return None;
    }
    if blob[4] != EXPR_BLOB_VERSION {
        return None;
    }
    if blob[5] != 0 || blob[10] != 0 || blob[11] != 0 {
        return None;
    }
    let num_regs = u16::from_le_bytes(blob[6..8].try_into().unwrap()) as u32;
    let result_reg = u16::from_le_bytes(blob[8..10].try_into().unwrap()) as u32;
    let n = u32::from_le_bytes(blob[12..16].try_into().unwrap());
    if n % 4 != 0 {
        return None;
    }
    let code_bytes = (n as usize) * 4;
    let code_end = EXPR_BLOB_HEADER_SIZE + code_bytes;
    if blob.len() < code_end + 4 {
        return None;
    }
    let code: Vec<u32> = blob[EXPR_BLOB_HEADER_SIZE..code_end]
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    let s_count = u32::from_le_bytes(blob[code_end..code_end + 4].try_into().unwrap());
    let mut cur = code_end + 4;
    // Each string costs at least its 4-byte length prefix; bound s_count against the
    // remaining bytes before reserving, so a corrupt count can't drive a huge with_capacity.
    if (s_count as usize) > blob.len().saturating_sub(cur) / 4 {
        return None;
    }
    let mut const_strings = Vec::with_capacity(s_count as usize);
    for _ in 0..s_count {
        if blob.len() < cur + 4 {
            return None;
        }
        let l = u32::from_le_bytes(blob[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        if blob.len() < cur + l {
            return None;
        }
        const_strings.push(blob[cur..cur + l].to_vec());
        cur += l;
    }
    Some(ExprBlob {
        num_regs,
        result_reg,
        code,
        const_strings,
    })
}

#[cfg(test)]
mod tests {
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
    fn select_operands_round_trip() {
        for a in 0u32..64 {
            for b in 0u32..64 {
                assert_eq!(
                    decode_select_operands(encode_select_operands(a, b)),
                    (a as u16, b as u16)
                );
            }
        }
    }
}
