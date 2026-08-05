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
/// Copy an input column verbatim into an output payload slot:
/// `[EXPR_COPY_COL, 0, src_col, out_payload]`. Word 1 is unused — the engine
/// resolves the source locator (PK byte window or dense payload slot) and both
/// widths from the schemas it validates the program against, so a restated type
/// code could only ever disagree with them.
pub const EXPR_COPY_COL: u32 = 34;
/// Conditional select (SQL CASE blend): `[EXPR_SELECT, dst, cond, a | (b << 16)]`.
/// Three register sources — `cond`, `a`, `b` — packed into two operand words:
/// `cond` occupies word `a1` alone, while `a`/`b` are packed as the low/high 16
/// bits of word `a2` by [`pack_operand_pair`]. Rows where `cond` is non-NULL and truthy
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
/// Integer set membership: `[EXPR_INT_IN_SET, dst, value_reg, set_idx]`. Tests
/// register `value_reg`'s i64 image for membership in the i64 pool at const
/// index `set_idx` (packed `N × 8-byte LE`), writing a 0/1 boolean into `dst`.
/// The pool's wire order is not a contract — the engine sorts it at resolve and
/// binary-searches the sorted copy. NULL input propagates to NULL. `set_idx` is
/// a const-pool index (full u32, like `EXPR_STR_COL_*_CONST`), not a register.
pub const EXPR_INT_IN_SET: u32 = 46;

// Numeric scalar functions and numeric CAST. Codes 37-39 fill the gap the string
// compares left after LOAD_NULL; the rest continue past INT_IN_SET, keeping the
// space dense.

/// `[EXPR_INT_ABS, dst, a, 0]` — `wrapping_abs` on the i64 register, so
/// `ABS(i64::MIN)` is `i64::MIN`. Same width in, same width out: no NULL.
pub const EXPR_INT_ABS: u32 = 37;
/// Pure float unary transforms: `[op, dst, a, 0]`. Each is its operand's IEEE
/// result and produces no NULL of its own. ROUND is `round_ties_even`.
pub const EXPR_FLOAT_ABS: u32 = 38;
pub const EXPR_FLOAT_FLOOR: u32 = 39;
pub const EXPR_FLOAT_CEIL: u32 = 47;
pub const EXPR_FLOAT_ROUND: u32 = 48;
pub const EXPR_FLOAT_TRUNC: u32 = 49;
/// Truncate-toward-zero float→int cast with a target range check:
/// `[EXPR_FLOAT_TO_INT, dst, a, target_tc]`. NaN, ±∞ and an out-of-range
/// truncated value all produce NULL. `target_tc` is a `TypeCode` discriminant
/// riding the `a2` word, not a register — the same one-word-payload convention
/// `EXPR_INT_IN_SET` uses for its pool index.
pub const EXPR_FLOAT_TO_INT: u32 = 50;
/// Integer domain cast: `[EXPR_INT_CAST, dst, a, target_tc]`. The register bits
/// pass through unchanged when the value is in the target's domain and the row
/// is NULLed otherwise; the source is read as signed or unsigned according to
/// the resolve-time U64 tracking of `a`. `target_tc` as for `EXPR_FLOAT_TO_INT`.
pub const EXPR_INT_CAST: u32 = 51;
/// Round through f32 precision: `[EXPR_FLOAT_TO_F32, dst, a, 0]`. NULL iff the
/// source is finite and the rounded result is not. NaN and ±∞ pass through
/// (both representable); underflow to ±0 passes through.
pub const EXPR_FLOAT_TO_F32: u32 = 52;
/// Null-skipping 2-ary extremum: `[op, dst, a, b]`. A NULL operand yields the
/// other operand; the result is NULL only when both are. The integer pair
/// compares signed or unsigned per the U64 tracking of either operand; the
/// float pair compares by `f64::total_cmp`, never `==` or `partial_cmp`.
pub const EXPR_INT_MAX2: u32 = 53;
pub const EXPR_INT_MIN2: u32 = 54;
pub const EXPR_FLOAT_MAX2: u32 = 55;
pub const EXPR_FLOAT_MIN2: u32 = 56;

// String values in the register file. Codes 57-75 continue past the numeric
// scalar functions, keeping the space dense. These read and write a *string*
// register — a second class over the same register index space, so the null
// bits and boolean masks address both classes unchanged.

/// `[EXPR_LOAD_COL_STR, dst, col, 0]` — a German-string column; nulls come from
/// the batch bitmap.
pub const EXPR_LOAD_COL_STR: u32 = 57;
/// `[EXPR_LOAD_CONST_STR, dst, const_idx, 0]` — a const-pool entry's raw bytes.
pub const EXPR_LOAD_CONST_STR: u32 = 58;
/// `[EXPR_LOAD_NULL_STR, dst, 0, 0]` — the string-register twin of
/// `EXPR_LOAD_NULL`: an empty view with the null bit set for every row.
pub const EXPR_LOAD_NULL_STR: u32 = 59;
/// String CASE blend; operand packing identical to `EXPR_SELECT`
/// (`[op, dst, cond, a | b << 16]`, [`pack_operand_pair`]). `cond` is
/// a scalar register; `a`/`b` are string registers, as is `dst`.
pub const EXPR_STR_SELECT: u32 = 60;
/// `[op, dst, a, b]` — compare two string registers, writing 0/1 into the
/// *scalar* register `dst`. Byte-lexicographic (`[u8]::cmp` over the content
/// bytes), the same order `compare_german_strings` imposes on canonical cells.
pub const EXPR_STR_CMP_EQ: u32 = 61;
pub const EXPR_STR_CMP_LT: u32 = 62;
pub const EXPR_STR_CMP_LE: u32 = 63;
/// `[op, dst, a, 0]` — string length into the scalar register `dst`, in bytes
/// (`OCTET_LENGTH`) or characters (`LENGTH`: the count of non-continuation
/// bytes, which on valid UTF-8 is the codepoint count).
pub const EXPR_STR_LEN_BYTES: u32 = 64;
pub const EXPR_STR_LEN_CHARS: u32 = 65;
/// `[op, dst, a, 0]` — ASCII-only case fold (`a-z`/`A-Z`); every other byte
/// passes through, so a multibyte UTF-8 sequence is unchanged.
pub const EXPR_STR_UPPER: u32 = 66;
pub const EXPR_STR_LOWER: u32 = 67;
/// `[EXPR_STR_SUBSTR, dst, src_reg, start_reg | len_reg << 16]`; `len_reg =
/// 0xFFFF` ⇒ no FOR clause (to the end of the string). 0xFFFF is unreachable as
/// a real register (`MAX_REGS = 64`). The window is a half-open character range
/// `[start, start + len)`, 1-based, intersected with the string; a negative
/// length is NULL.
pub const EXPR_STR_SUBSTR: u32 = 68;
/// The `len_reg` value that means "no FOR clause". `MAX_REGS` is 64, so no real
/// register can collide with it, and `EXPR_STR_SUBSTR` needs no second opcode
/// for the two-argument form.
pub const STR_SUBSTR_NO_LEN: u32 = 0xFFFF;
/// `[EXPR_STR_TRIM, dst, src_reg | mode << 16, set_const_idx]`, where `mode` is a
/// [`TrimMode`]. The const-pool entry is the raw byte set to strip.
pub const EXPR_STR_TRIM: u32 = 69;

/// Which end(s) `EXPR_STR_TRIM` strips. The one definition of the mode word both
/// the planner and the engine encode against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrimMode {
    Both = 0,
    Leading = 1,
    Trailing = 2,
}

impl TrimMode {
    /// The mode word to pack beside the source register.
    #[inline]
    pub const fn to_wire(self) -> u32 {
        self as u32
    }

    /// The mode a wire word names, or `None` for an out-of-range one.
    #[inline]
    pub const fn from_wire(mode: u32) -> Option<Self> {
        match mode {
            0 => Some(TrimMode::Both),
            1 => Some(TrimMode::Leading),
            2 => Some(TrimMode::Trailing),
            _ => None,
        }
    }

    #[inline]
    pub fn trims_start(self) -> bool {
        matches!(self, TrimMode::Both | TrimMode::Leading)
    }

    #[inline]
    pub fn trims_end(self) -> bool {
        matches!(self, TrimMode::Both | TrimMode::Trailing)
    }
}
/// `[EXPR_STR_CONCAT, dst, a, b]` — SQL `||`: NULL-propagating.
pub const EXPR_STR_CONCAT: u32 = 70;
/// CONCAT fold step: a NULL `b` contributes the empty string; a NULL `a` (the
/// accumulator) propagates — the asymmetry that carries the `u32::MAX`
/// overflow-NULL through a left fold.
pub const EXPR_STR_CONCAT_NN: u32 = 71;
/// `[op, dst, a, 0]` — numeric register to decimal text. The integer form reads
/// its source signed or unsigned per the resolve-time U64 tracking; the float
/// form is the shortest round-trip decimal, switched to scientific notation
/// outside `[1e-4, 1e15)` so the output stays bounded (Rust's positional
/// `Display` renders `1e300` as 301 digits), and spells the non-finite values
/// `Infinity` / `-Infinity` / `NaN` as PostgreSQL does.
pub const EXPR_INT_TO_STR: u32 = 72;
pub const EXPR_FLOAT_TO_STR: u32 = 73;
/// `[EXPR_STR_TO_INT, dst, a, target_tc]` — parse ASCII decimal (surrounding
/// whitespace and an optional sign allowed, nothing else) into the scalar
/// register `dst`. An unparsable string or an out-of-range value is NULL, never
/// a wrap. `target_tc` rides the `a2` word as for `EXPR_INT_CAST`.
pub const EXPR_STR_TO_INT: u32 = 74;
/// `[EXPR_STR_TO_FLOAT, dst, a, 0]` — parse into an f64 scalar register; any
/// failure (including non-UTF-8 bytes) is NULL.
pub const EXPR_STR_TO_FLOAT: u32 = 75;

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

/// Two 16-bit operands in one 32-bit word, low half first. Used by
/// `EXPR_SELECT` / `EXPR_STR_SELECT` (`a | b`) and `EXPR_STR_SUBSTR`
/// (`start_reg | len_reg`) in the `a2` word, and by `EXPR_STR_TRIM`
/// (`src_reg | mode`) in `a1`. The second half is not always a register:
/// TRIM's is a mode word and SUBSTR's may be [`STR_SUBSTR_NO_LEN`].
#[inline]
pub const fn pack_operand_pair(a: u32, b: u32) -> u32 {
    (a & 0xFFFF) | ((b & 0xFFFF) << 16)
}
#[inline]
pub const fn unpack_operand_pair(w: u32) -> (u16, u16) {
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
    if crate::read_u32_le(blob, 0) != EXPR_BLOB_MAGIC {
        return None;
    }
    if blob[4] != EXPR_BLOB_VERSION {
        return None;
    }
    if blob[5] != 0 || blob[10] != 0 || blob[11] != 0 {
        return None;
    }
    let num_regs = crate::read_u16_le(blob, 6) as u32;
    let result_reg = crate::read_u16_le(blob, 8) as u32;
    let n = crate::read_u32_le(blob, 12);
    if !n.is_multiple_of(4) {
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
    let s_count = crate::read_u32_le(blob, code_end);
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
        let l = crate::read_u32_le(blob, cur) as usize;
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
    fn operand_pair_round_trip() {
        for a in 0u32..64 {
            for b in 0u32..64 {
                assert_eq!(unpack_operand_pair(pack_operand_pair(a, b)), (a as u16, b as u16));
            }
        }
    }
}
