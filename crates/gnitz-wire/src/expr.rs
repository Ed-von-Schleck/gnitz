//! Expression bytecode: opcodes, operand packing, and blob framing.

use crate::codec::Writer;

wire_enum! {
    /// One variant per `LogicalInstr` variant, stating **wire identity only** —
    /// every rule about what an instruction means lives on the `LogicalInstr`
    /// variant this mirrors. An operator or flag that variant already carries as
    /// a field rides the selector, never a second opcode.
    ///
    /// An instruction is `[opcode, selector, a1, a2, a3]`. The selector names
    /// which member of the opcode's family this is — a comparison or arithmetic
    /// operator, a cast target, a TRIM mode, or a 0/1 flag — and is **0** for an
    /// opcode with no family, which the decoder enforces. The three operand
    /// words are whole `u32`s, one operand each; unused ones are 0 and ignored.
    /// The one operand spanning two words is `LoadConst`'s `i64`, through
    /// [`encode_load_const`] / [`decode_load_const`].
    ///
    /// A wire enum so `LogicalProgram::decode_instr` matches exhaustively: a new
    /// opcode is a compile error there until it gets a decode arm — which
    /// discriminants on `LogicalInstr` itself would lose.
    pub enum ExprOp: u32 {
        LoadColInt = 1,
        LoadColFloat = 2,
        LoadConst = 3,
        IntArith = 4,
        FloatArith = 5,
        Cmp = 6,
        FCmp = 7,
        IntToFloat = 8,
        FloatUnary = 9,
        IntUnary = 10,
        FloatToInt = 11,
        IntCast = 12,
        FloatToF32 = 13,
        IntMinMax2 = 14,
        FloatMinMax2 = 15,
        Select = 16,
        LoadNull = 17,
        BoolBinary = 18,
        BoolNot = 19,
        IsNull = 20,
        IsNullReg = 21,
        StrColConst = 22,
        StrColCol = 23,
        IntInSet = 24,
        LoadColStr = 25,
        LoadConstStr = 26,
        LoadNullStr = 27,
        StrSelect = 28,
        StrCmp = 29,
        StrLen = 30,
        StrCase = 31,
        StrSubstr = 32,
        StrTrim = 33,
        StrLike = 34,
        StrConcat = 35,
        IntToStr = 36,
        FloatToStr = 37,
        StrToInt = 38,
        StrToFloat = 39,
        StrSide = 40,
        StrPos = 41,
        StrReverse = 42,
        StrReplace = 43,
        StrPad = 44,
        StrSplitPart = 45,
    }
}

wire_enum! {
    /// What one sink pair `[kind, value]` names. Sinks ride the blob's own
    /// region, so this space is disjoint from [`ExprOp`]'s.
    pub enum SinkKind: u32 {
        /// Copy input column `value` verbatim.
        Col = 0,
        /// Store register `value`.
        Reg = 1,
    }
}

wire_enum! {
    /// Which end(s) `ExprOp::StrTrim` strips — its selector. The one definition
    /// of that word both the planner and the engine encode against.
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
pub(crate) const EXPR_BLOB_VERSION: u8 = 3;
/// Fixed header width, in bytes.
const EXPR_BLOB_HEADER_SIZE: usize = 16;

/// `ExprOp::LoadConst`'s `i64` across two operand words (`a1` = low 32 bits,
/// `a2` = high 32 bits) — the one operand wider than a word.
#[inline]
pub const fn encode_load_const(v: i64) -> (u32, u32) {
    (v as u32, (v >> 32) as u32)
}
#[inline]
pub const fn decode_load_const(a1: u32, a2: u32) -> i64 {
    ((a2 as i64) << 32) | (a1 as i64 & 0xFFFF_FFFF)
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
/// 12  4   code word count N (u32; multiple of 5)
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
    debug_assert!(code.len().is_multiple_of(5), "code length {} not 5-aligned", code.len());
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
    let code = words(5)?;
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
    Some(ExprBlob { result_reg, code, sinks, const_strings })
}

#[cfg(test)]
#[path = "tests/expr.rs"]
mod tests;
