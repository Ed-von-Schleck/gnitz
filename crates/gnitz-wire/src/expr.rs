//! Expression bytecode: opcodes, operand packing, and blob framing.

use crate::codec::Writer;

wire_enum! {
    /// The expression opcode space — every `[op, a1, a2]` triple's first
    /// word. Declared as a wire enum so `LogicalProgram::decode_triple` matches
    /// exhaustively: adding an opcode here is a compile error there until it
    /// gets a decode arm, which a `pub const` space could never force.
    ///
    /// Each variant's doc states its operand layout. Codes are neither dense nor
    /// in declaration order: later additions filled earlier gaps, and 32 / 34
    /// are the two output opcodes' graves — a program's output slots ride the
    /// blob's own sink region, not this space.
    pub enum ExprOp: u32 {
        LoadColInt = 1,
        LoadColFloat = 2,
        LoadConst = 3,
        IntAdd = 4,
        IntSub = 5,
        IntMul = 6,
        IntDiv = 7,
        IntMod = 8,
        IntNeg = 9,
        FloatAdd = 10,
        FloatSub = 11,
        FloatMul = 12,
        FloatDiv = 13,
        FloatNeg = 14,
        CmpEq = 15,
        CmpNe = 16,
        CmpGt = 17,
        CmpGe = 18,
        CmpLt = 19,
        CmpLe = 20,
        FcmpEq = 21,
        FcmpNe = 22,
        FcmpGt = 23,
        FcmpGe = 24,
        FcmpLt = 25,
        FcmpLe = 26,
        BoolAnd = 27,
        BoolOr = 28,
        BoolNot = 29,
        IsNull = 30,
        IsNotNull = 31,
        IntToFloat = 33,
        /// Conditional select (SQL CASE blend): `[ExprOp::Select, cond, a | (b << 16)]`.
        /// Three register sources — `cond`, `a`, `b` — packed into two operand words:
        /// `cond` occupies word `a1` alone, while `a`/`b` are packed as the low/high 16
        /// bits of word `a2` by [`pack_operand_pair`]. Rows where `cond` is non-NULL and truthy
        /// take `a`'s value + null bit; all other rows (false **or NULL** cond) take
        /// `b`'s. Carries a value, never a boolean classification.
        Select = 35,
        /// Materialize a NULL value: `[ExprOp::LoadNull, 0, 0]` — value 0, null bit
        /// set for every row. Backs `CASE` without `ELSE` (→ `ELSE NULL`) and `NULLIF`.
        LoadNull = 36,
        StrColEqConst = 40,
        StrColLtConst = 41,
        StrColLeConst = 42,
        StrColEqCol = 43,
        StrColLtCol = 44,
        StrColLeCol = 45,
        /// Integer set membership: `[ExprOp::IntInSet, value_reg, set_idx]`. Tests
        /// register `value_reg`'s i64 image for membership in the i64 pool at const
        /// index `set_idx` (packed `N × 8-byte LE`), writing a 0/1 boolean.
        /// The pool's wire order is not a contract — the engine sorts it at resolve and
        /// binary-searches the sorted copy. NULL input propagates to NULL. `set_idx` is
        /// a const-pool index (full u32, like `ExprOp::StrCol*Const`), not a register.
        IntInSet = 46,

        // Numeric scalar functions and numeric CAST. Codes 37-39 fill the gap the string
        // compares left after LOAD_NULL; the rest continue past INT_IN_SET, keeping the
        // space dense.

        /// `[ExprOp::IntAbs, a, 0]` — `wrapping_abs` on the i64 register, so
        /// `ABS(i64::MIN)` is `i64::MIN`. Same width in, same width out: no NULL.
        IntAbs = 37,
        /// Pure float unary transforms: `[op, a, 0]`. Each is its operand's IEEE
        /// result and produces no NULL of its own. ROUND is `round_ties_even`.
        FloatAbs = 38,
        FloatFloor = 39,
        FloatCeil = 47,
        FloatRound = 48,
        FloatTrunc = 49,
        /// Truncate-toward-zero float→int cast with a target range check:
        /// `[ExprOp::FloatToInt, a, target_tc]`. NaN, ±∞ and an out-of-range
        /// truncated value all produce NULL. `target_tc` is a `TypeCode` discriminant
        /// riding the `a2` word, not a register — the same one-word-payload convention
        /// `ExprOp::IntInSet` uses for its pool index.
        FloatToInt = 50,
        /// Integer domain cast: `[ExprOp::IntCast, a, target_tc]`. The register bits
        /// pass through unchanged when the value is in the target's domain and the row
        /// is NULLed otherwise; the source is read as signed or unsigned according to
        /// the resolve-time U64 tracking of `a`. `target_tc` as for `ExprOp::FloatToInt`.
        IntCast = 51,
        /// Round through f32 precision: `[ExprOp::FloatToF32, a, 0]`. NULL iff the
        /// source is finite and the rounded result is not. NaN and ±∞ pass through
        /// (both representable); underflow to ±0 passes through.
        FloatToF32 = 52,
        /// Null-skipping 2-ary extremum: `[op, a, b]`. A NULL operand yields the
        /// other operand; the result is NULL only when both are. The integer pair
        /// compares signed or unsigned per the U64 tracking of either operand; the
        /// float pair compares by `f64::total_cmp`, never `==` or `partial_cmp`.
        IntMax2 = 53,
        IntMin2 = 54,
        FloatMax2 = 55,
        FloatMin2 = 56,

        // String values in the register file. Codes 57-75 continue past the numeric
        // scalar functions, keeping the space dense. These read and write a *string*
        // register — a second class over the same register index space, so the null
        // bits and boolean masks address both classes unchanged.

        /// `[ExprOp::LoadColStr, col, 0]` — a German-string column; nulls come from
        /// the batch bitmap.
        LoadColStr = 57,
        /// `[ExprOp::LoadConstStr, const_idx, 0]` — a const-pool entry's raw bytes.
        LoadConstStr = 58,
        /// `[ExprOp::LoadNullStr, 0, 0]` — the string-register twin of
        /// `ExprOp::LoadNull`: an empty view with the null bit set for every row.
        LoadNullStr = 59,
        /// String CASE blend; operand packing identical to `ExprOp::Select`
        /// (`[op, cond, a | b << 16]`, [`pack_operand_pair`]). `cond` is
        /// a scalar register; `a`/`b` and the result are string registers.
        StrSelect = 60,
        /// `[op, a, b]` — compare two string registers, writing 0/1 into the
        /// *scalar* register. Byte-lexicographic (`[u8]::cmp` over the content
        /// bytes), the same order `compare_german_strings` imposes on canonical cells.
        StrCmpEq = 61,
        StrCmpLt = 62,
        StrCmpLe = 63,
        /// `[op, a, 0]` — string length into a scalar register, in bytes
        /// (`OCTET_LENGTH`) or characters (`LENGTH`: the count of non-continuation
        /// bytes, which on valid UTF-8 is the codepoint count).
        StrLenBytes = 64,
        StrLenChars = 65,
        /// `[op, a, 0]` — ASCII-only case fold (`a-z`/`A-Z`); every other byte
        /// passes through, so a multibyte UTF-8 sequence is unchanged.
        StrUpper = 66,
        StrLower = 67,
        /// `[ExprOp::StrSubstr, src_reg, start_reg | len_reg << 16]`; `len_reg =
        /// 0xFFFF` ⇒ no FOR clause (to the end of the string). 0xFFFF is unreachable as
        /// a real register (`MAX_REGS = 64`). The window is a half-open character range
        /// `[start, start + len)`, 1-based, intersected with the string; a negative
        /// length is NULL.
        StrSubstr = 68,
        /// `[ExprOp::StrTrim, src_reg | mode << 16, set_const_idx]`, where `mode` is a
        /// [`TrimMode`]. The const-pool entry is the raw byte set to strip.
        StrTrim = 69,

        /// `[ExprOp::StrConcat, a, b]` — SQL `||`: NULL-propagating.
        StrConcat = 70,
        /// CONCAT fold step: a NULL `b` contributes the empty string; a NULL `a` (the
        /// accumulator) propagates — the asymmetry that carries the `u32::MAX`
        /// overflow-NULL through a left fold.
        StrConcatNn = 71,
        /// `[op, a, 0]` — numeric register to decimal text. The integer form reads
        /// its source signed or unsigned per the resolve-time U64 tracking; the float
        /// form is the shortest round-trip decimal, switched to scientific notation
        /// outside `[1e-4, 1e15)` so the output stays bounded (Rust's positional
        /// `Display` renders `1e300` as 301 digits), and spells the non-finite values
        /// `Infinity` / `-Infinity` / `NaN` as PostgreSQL does.
        IntToStr = 72,
        FloatToStr = 73,
        /// `[ExprOp::StrToInt, a, target_tc]` — parse ASCII decimal (surrounding
        /// whitespace and an optional sign allowed, nothing else) into the scalar
        /// register. An unparsable string or an out-of-range value is NULL, never
        /// a wrap. `target_tc` rides the `a2` word as for `ExprOp::IntCast`.
        StrToInt = 74,
        /// `[ExprOp::StrToFloat, a, 0]` — parse into an f64 scalar register; any
        /// failure (including non-UTF-8 bytes) is NULL.
        StrToFloat = 75,
        /// `[ExprOp::StrLike, src_reg | escape << 16, pat_idx]` — SQL LIKE over a
        /// string register, writing 0/1 into a *scalar* register, the
        /// [`ExprOp::StrTrim`] shape. `escape` is the escape character, or 0 to disable
        /// escaping; the const-pool entry at `pat_idx` is the raw pattern bytes. The
        /// matcher is compiled at resolve, never per row.
        StrLike = 76,
        /// ASCII-case-insensitive LIKE, same operand shape.
        StrIlike = 77,
        /// `[op, a, 0]` — `IS [NOT] NULL` over a register of either class,
        /// reading only its null lane; 0/1 into a scalar register, never NULL.
        /// `IsNull` / `IsNotNull` read the batch bitmap without a load, and
        /// alone serve a column no load opcode admits (U128, BLOB).
        IsNullReg = 78,
        IsNotNullReg = 79,
        /// `[op, a, 0]` — the float unaries past the rounding family: each is
        /// the operand's IEEE result (`SQRT(-1)` is NaN, `LN(0)` is -inf), so
        /// none produces a NULL of its own.
        FloatSqrt = 80,
        FloatLn = 81,
        FloatLog10 = 82,
        FloatExp = 83,
        /// `[op, a, 0]` — SIGN: -1 / 0 / 1 in the operand's own domain. The
        /// integer form reads its source signed or unsigned per the
        /// resolve-time U64 tracking (an unsigned value is never negative);
        /// the float form spells NaN's sign as NaN.
        FloatSign = 84,
        IntSign = 85,
        /// `[ExprOp::FloatPow, a, b]` — `POWER(a, b)` over two float registers,
        /// IEEE `powf`.
        FloatPow = 86,
        /// `[op, src, n]` — LEFT / RIGHT: the first (last) `n` characters of
        /// the string register `src`; a negative `n` drops that many from the
        /// other end instead, as PostgreSQL reads it. A sub-view of the source;
        /// never NULL of its own.
        StrLeft = 87,
        StrRight = 88,
        /// `[ExprOp::StrPos, hay, needle]` — the 1-based *character* index of
        /// the first occurrence of `needle` in `hay` into a scalar register, 0
        /// when absent, 1 for an empty needle.
        StrPos = 89,
        /// `[ExprOp::StrReverse, a, 0]` — the characters in reverse order, a
        /// fresh arena copy.
        StrReverse = 90,
        /// `[ExprOp::StrReplace, s, from | (to << 16)]` — every non-overlapping
        /// occurrence of `from` in `s` replaced by `to`, left to right. An empty
        /// `from` leaves `s` unchanged. A result past `u32::MAX` bytes is NULL,
        /// `ExprOp::StrConcat`'s rule.
        StrReplace = 91,
        /// `[op, s, n | (fill << 16)]` — LPAD / RPAD: `s` padded on the left
        /// (right) with `fill` repeated to `n` *characters*; a longer `s` is
        /// truncated to its first `n` characters, `n <= 0` is the empty string,
        /// an empty `fill` pads nothing. A result past `u32::MAX` bytes is NULL.
        StrLpad = 92,
        StrRpad = 93,
        /// `[ExprOp::StrSplitPart, s, delim | (n << 16)]` — the `n`-th field of
        /// `s` split on `delim`, 1-based, from the right when negative; empty
        /// past the last field, NULL for `n = 0`, and the whole string for field
        /// ±1 of an empty `delim`. A sub-view of the source.
        StrSplitPart = 94,
    }
}

/// The `len_reg` value that means "no FOR clause", so `ExprOp::StrSubstr` needs no
/// second opcode for the two-argument form. It must stay above the evaluator's
/// register limit, which lives a crate away and pins the relationship with a
/// `const` assert against this value.
pub const STR_SUBSTR_NO_LEN: u32 = 0xFFFF;

wire_enum! {
    /// Which end(s) `ExprOp::StrTrim` strips. The one definition of the mode word
    /// both the planner and the engine encode against.
    pub enum TrimMode: u32 {
        Both = 0,
        Leading = 1,
        Trailing = 2,
    }
}

impl TrimMode {
    #[inline]
    pub fn trims_start(self) -> bool {
        matches!(self, TrimMode::Both | TrimMode::Leading)
    }

    #[inline]
    pub fn trims_end(self) -> bool {
        matches!(self, TrimMode::Both | TrimMode::Trailing)
    }
}

// ---------------------------------------------------------------------------
// Blob framing constants and operand packing
// ---------------------------------------------------------------------------

/// Wire-format magic for serialised expr blobs. Written little-endian, so the
/// bytes on disk read `EPXR` — the constant's nibbles are not in ASCII order.
/// Spelled out because a reader hexdumping a blob will not find "EXPR".
const EXPR_BLOB_MAGIC: u32 = 0x5258_5045;
/// Current wire-format version for expr blobs.
const EXPR_BLOB_VERSION: u8 = 2;
/// Fixed header width, in bytes.
const EXPR_BLOB_HEADER_SIZE: usize = 16;

/// `ExprOp::LoadConst` splits a 64-bit value across its two operand words
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
/// `ExprOp::Select` / `ExprOp::StrSelect` (`a | b`) and `ExprOp::StrSubstr`
/// (`start_reg | len_reg`) in the `a2` word, and by `ExprOp::StrTrim`
/// (`src_reg | mode`) and `ExprOp::StrLike` (`src_reg | escape`) in `a1`. The
/// second half is not always a register: TRIM's is a mode word, LIKE's an escape
/// byte, and SUBSTR's may be [`STR_SUBSTR_NO_LEN`].
#[inline]
pub const fn pack_operand_pair(a: u32, b: u32) -> u32 {
    (a & 0xFFFF) | ((b & 0xFFFF) << 16)
}
#[inline]
pub const fn unpack_operand_pair(w: u32) -> (u16, u16) {
    ((w & 0xFFFF) as u16, (w >> 16) as u16)
}

/// A decoded expr blob. `code`, `sinks` and the string bytes are copied out of
/// the input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprBlob {
    pub result_reg: u32,
    pub code: Vec<u32>,
    pub sinks: Vec<u32>,
    pub const_strings: Vec<Vec<u8>>,
}

/// Serialise an expr program. Layout (all little-endian):
///
/// ```text
/// 0   4   magic "EXPR" (u32)
/// 4   1   version (u8)
/// 5   1   reserved (must be 0)
/// 6   2   reserved (must be 0)
/// 8   2   result_reg (u16)
/// 10  2   reserved (must be 0)
/// 12  4   code word count N (u32; multiple of 3)
/// 16  4N  code words (u32 each)
/// ..  4   sink word count M (u32; multiple of 2)
/// ..  4M  sink words (u32 each)
/// ..  4   string count S (u32)
/// ..  S × { 4-byte length L, L bytes }
/// ```
///
/// The register count is not carried: a register is the index of the
/// instruction that writes it, so the code region's length *is* the register
/// file's size.
pub fn encode_expr_blob(result_reg: u32, code: &[u32], sinks: &[u32], const_strings: &[impl AsRef<[u8]>]) -> Vec<u8> {
    debug_assert!(code.len().is_multiple_of(3), "code length {} not 3-aligned", code.len());
    debug_assert!(
        sinks.len().is_multiple_of(2),
        "sink length {} not 2-aligned",
        sinks.len()
    );
    let mut w = Writer::with_capacity(
        EXPR_BLOB_HEADER_SIZE
            + (code.len() + sinks.len() + 2) * 4
            + const_strings.iter().map(|s| 4 + s.as_ref().len()).sum::<usize>(),
    );
    w.u32(EXPR_BLOB_MAGIC)
        .u8(EXPR_BLOB_VERSION)
        .u8(0) // reserved
        .u16(0) // reserved
        .u16(result_reg as u16)
        .u16(0) // reserved
        .u32(code.len() as u32);
    for &word in code {
        w.u32(word);
    }
    w.u32(sinks.len() as u32);
    for &word in sinks {
        w.u32(word);
    }
    w.u32(const_strings.len() as u32);
    for s in const_strings {
        w.bytes32(s.as_ref());
    }
    w.into_vec()
}

/// Inverse of `encode_expr_blob`. Validates magic, version, reserved bytes, code-length
/// alignment, region lengths, and the string-count OOM bound. Does **not** validate program
/// semantics (opcodes, register operands, column indices) — that is the decoder-consumer's job.
pub fn decode_expr_blob(blob: &[u8]) -> Option<ExprBlob> {
    let mut r = crate::codec::Reader::new(blob, "expr blob");
    if r.u32().ok()? != EXPR_BLOB_MAGIC || r.u8().ok()? != EXPR_BLOB_VERSION || r.u8().ok()? != 0 {
        return None;
    }
    if r.u16().ok()? != 0 {
        return None; // reserved
    }
    let result_reg = r.u16().ok()? as u32;
    if r.u16().ok()? != 0 {
        return None; // reserved
    }
    // One length-prefixed u32 region, its count held to `align` words per entry.
    let mut words = |align: u32| -> Option<Vec<u32>> {
        let n = r.u32().ok()?;
        if !n.is_multiple_of(align) {
            return None;
        }
        Some(
            r.take(n as usize * 4)
                .ok()?
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
                .collect(),
        )
    };
    let code = words(3)?;
    let sinks = words(2)?;
    let s_count = r.u32().ok()? as usize;
    // Each string costs at least its 4-byte length prefix; bound s_count against the
    // remaining bytes before reserving, so a corrupt count can't drive a huge with_capacity.
    if s_count > r.remaining() / 4 {
        return None;
    }
    let mut const_strings = Vec::with_capacity(s_count);
    for _ in 0..s_count {
        const_strings.push(r.bytes32().ok()?.to_vec());
    }
    r.expect_consumed().ok()?;
    Some(ExprBlob {
        result_reg,
        code,
        sinks,
        const_strings,
    })
}

#[cfg(test)]
#[path = "tests/expr.rs"]
mod tests;
