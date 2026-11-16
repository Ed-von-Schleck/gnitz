//! The expression blob: the wire ids [`ExprOp`] and [`SinkKind`], the two
//! instruction tables, and the framing; only this module reads them.

use crate::calendar::CalendarOp;
use crate::program::MAX_REGS;
use crate::{
    CmpOp, ConstIdx, ExprValidateErr, FloatArithOp, FloatUnaryOp, IntArithOp, IntUnaryOp, LogicalInstr, LogicalProgram,
    Reg, Sink, TrimMode,
};
use gnitz_wire::{FixedInt, TypeCode, Writer, decode_all};

/// The const pool's entry cap: each of at most [`MAX_REGS`] instructions names
/// at most one entry, so a larger pool holds an entry no instruction reads.
const MAX_CONST_POOL: usize = MAX_REGS;

/// One wire instruction, `[opcode, selector, a1, a2, a3]`.
const INSTR_WORDS: usize = 5;
/// One wire sink pair, `[kind, value]`.
const SINK_WORDS: usize = 2;
/// The two above as byte strides. One constant per region serves as both the
/// item type's array size and the region's stride, so the blob's entry counts
/// and the arrays they frame cannot disagree.
const INSTR_BYTES: usize = INSTR_WORDS * 4;
const SINK_BYTES: usize = SINK_WORDS * 4;

// ---------------------------------------------------------------------------
// The wire instruction vocabulary
// ---------------------------------------------------------------------------

gnitz_wire::wire_enum! {
    /// One variant per `LogicalInstr` variant, stating **wire identity only** —
    /// every rule about what an instruction means lives on the `LogicalInstr`
    /// variant this mirrors. An operator or flag the variant carries as a field
    /// rides the selector rather than a second opcode — except Calendar's
    /// `micros`, which rides a2 because the selector holds its CalendarOp.
    ///
    /// An instruction is `[opcode, selector, a1, a2, a3]`. The selector names
    /// which member of the opcode's family this is — a comparison or arithmetic
    /// operator, a cast target, a TRIM mode, or a 0/1 flag — and is **0** for an
    /// opcode with no family, which the decoder enforces. The three operand
    /// words are whole `u32`s, one operand each; unused ones are 0 and ignored.
    /// The one operand spanning two words is `LoadConst`'s `i64`, through
    /// `encode_load_const` / `decode_load_const`.
    ///
    /// A wire enum so [`LogicalInstr::from_wire`] matches exhaustively: a new
    /// opcode is a compile error there until it gets a decode arm — which
    /// discriminants on `LogicalInstr` itself would lose.
    pub(crate) enum ExprOp: u32 {
        LoadCol = 1,
        LoadConst = 2,
        IntArith = 3,
        FloatArith = 4,
        Cmp = 5,
        FCmp = 6,
        IntToFloat = 7,
        FloatUnary = 8,
        IntUnary = 9,
        FloatToInt = 10,
        IntCast = 11,
        FloatToF32 = 12,
        IntMinMax2 = 13,
        FloatMinMax2 = 14,
        Select = 15,
        LoadNull = 16,
        BoolBinary = 17,
        BoolNot = 18,
        IsNull = 19,
        IsNullReg = 20,
        StrColConst = 21,
        StrColCol = 22,
        IntInSet = 23,
        LoadColStr = 24,
        LoadConstStr = 25,
        LoadNullStr = 26,
        StrSelect = 27,
        StrCmp = 28,
        StrLen = 29,
        StrCase = 30,
        StrSubstr = 31,
        StrTrim = 32,
        StrLike = 33,
        StrConcat = 34,
        IntToStr = 35,
        FloatToStr = 36,
        StrToInt = 37,
        StrToFloat = 38,
        StrSide = 39,
        StrPos = 40,
        StrReverse = 41,
        StrReplace = 42,
        StrPad = 43,
        StrSplitPart = 44,
        Calendar = 45,
        StrColPrefix = 46,
    }
}

gnitz_wire::wire_enum! {
    /// What one sink pair `[kind, value]` names. Sinks ride the blob's own
    /// region, so this space is disjoint from [`ExprOp`]'s.
    pub(crate) enum SinkKind: u32 {
        /// Copy input column `value` verbatim.
        Col = 0,
        /// Store register `value`.
        Reg = 1,
    }
}

/// `ExprOp::LoadConst`'s `i64` across two operand words (`a1` = low 32 bits,
/// `a2` = high 32 bits) — the one operand wider than a word.
#[inline]
const fn encode_load_const(v: i64) -> (u32, u32) {
    (v as u32, (v >> 32) as u32)
}
#[inline]
const fn decode_load_const(a1: u32, a2: u32) -> i64 {
    ((a2 as i64) << 32) | (a1 as i64 & 0xFFFF_FFFF)
}

impl Reg {
    /// A wire register word, saturating past `u16` to a register the file never
    /// holds rather than wrapping into one it does.
    fn from_wire(w: u32) -> Self {
        const { assert!(MAX_REGS < u16::MAX as usize) };
        Reg(u16::try_from(w).unwrap_or(u16::MAX))
    }
}

// ---------------------------------------------------------------------------
// The two instruction tables
// ---------------------------------------------------------------------------

impl LogicalInstr {
    /// Serialise to the wire instruction `[op, selector, a1, a2, a3]`, the exact
    /// inverse of [`Self::from_wire`] — the two are the only statements of the
    /// word layout, held together by the round-trip test. Unused words are 0,
    /// matching what the decoder ignores.
    fn to_wire(self) -> [u32; INSTR_WORDS] {
        use LogicalInstr as L;
        // Every arm is `[op, selector, a1, a2, a3]`; these shorten the common
        // shapes. `sel` is 0 for an opcode that names no family.
        let bin = |op: ExprOp, sel: u32, a: Reg, b: Reg| [op.as_wire(), sel, a.0 as u32, b.0 as u32, 0];
        let un = |op: ExprOp, sel: u32, a: Reg| [op.as_wire(), sel, a.0 as u32, 0, 0];
        let col = |op: ExprOp, sel: u32, c: u32| [op.as_wire(), sel, c, 0, 0];
        match self {
            L::LoadCol { col: c } => col(ExprOp::LoadCol, 0, c),
            L::LoadConst { val, unsigned } => {
                let (lo, hi) = encode_load_const(val);
                [ExprOp::LoadConst.as_wire(), unsigned as u32, lo, hi, 0]
            }
            L::IntArith { op, a, b } => bin(ExprOp::IntArith, op.as_wire(), a, b),
            L::FloatArith { op, a, b } => bin(ExprOp::FloatArith, op.as_wire(), a, b),
            L::Cmp { op, a, b } => bin(ExprOp::Cmp, op.as_wire(), a, b),
            L::FCmp { op, a, b } => bin(ExprOp::FCmp, op.as_wire(), a, b),
            L::IntToFloat { a } => un(ExprOp::IntToFloat, 0, a),
            L::FloatUnary { op, a } => un(ExprOp::FloatUnary, op.as_wire(), a),
            L::IntUnary { op, a } => un(ExprOp::IntUnary, op.as_wire(), a),
            L::Calendar { op, a, micros } => [ExprOp::Calendar.as_wire(), op.as_wire(), a.0 as u32, micros as u32, 0],
            L::FloatToInt { a, fi } => un(ExprOp::FloatToInt, fi.type_code().as_wire() as u32, a),
            L::IntCast { a, fi } => un(ExprOp::IntCast, fi.type_code().as_wire() as u32, a),
            L::FloatToF32 { a } => un(ExprOp::FloatToF32, 0, a),
            L::IntMinMax2 { a, b, is_max } => bin(ExprOp::IntMinMax2, is_max as u32, a, b),
            L::FloatMinMax2 { a, b, is_max } => bin(ExprOp::FloatMinMax2, is_max as u32, a, b),
            L::Select { cond, a, b } => [ExprOp::Select.as_wire(), 0, cond.0 as u32, a.0 as u32, b.0 as u32],
            L::LoadNull => [ExprOp::LoadNull.as_wire(), 0, 0, 0, 0],
            L::BoolBinary { a, b, is_or } => bin(ExprOp::BoolBinary, is_or as u32, a, b),
            L::BoolNot { a } => un(ExprOp::BoolNot, 0, a),
            L::IsNull { col: c, invert } => col(ExprOp::IsNull, invert as u32, c),
            L::IsNullReg { a, invert } => un(ExprOp::IsNullReg, invert as u32, a),
            L::StrColConst { op, col: c, const_idx } => {
                [ExprOp::StrColConst.as_wire(), op.as_wire(), c, const_idx.0, 0]
            }
            L::StrColCol { op, col_a, col_b } => [ExprOp::StrColCol.as_wire(), op.as_wire(), col_a, col_b, 0],
            L::StrColPrefix { col: c, const_idx } => [ExprOp::StrColPrefix.as_wire(), 0, c, const_idx.0, 0],
            L::IntInSet { value_reg, set_idx } => [ExprOp::IntInSet.as_wire(), 0, value_reg.0 as u32, set_idx.0, 0],
            L::LoadColStr { col: c } => col(ExprOp::LoadColStr, 0, c),
            L::LoadConstStr { const_idx } => col(ExprOp::LoadConstStr, 0, const_idx.0),
            L::LoadNullStr => [ExprOp::LoadNullStr.as_wire(), 0, 0, 0, 0],
            L::StrSelect { cond, a, b } => [ExprOp::StrSelect.as_wire(), 0, cond.0 as u32, a.0 as u32, b.0 as u32],
            L::StrCmp { op, a, b } => bin(ExprOp::StrCmp, op.as_wire(), a, b),
            L::StrLen { a, chars } => un(ExprOp::StrLen, chars as u32, a),
            L::StrCase { a, upper } => un(ExprOp::StrCase, upper as u32, a),
            // The absent FOR clause is `u32::MAX`, which no register index can be.
            L::StrSubstr { src, start_reg, len_reg } => [
                ExprOp::StrSubstr.as_wire(),
                0,
                src.0 as u32,
                start_reg.0 as u32,
                len_reg.map_or(u32::MAX, |r| r.0 as u32),
            ],
            L::StrTrim { a, mode, set_idx } => [ExprOp::StrTrim.as_wire(), mode.as_wire(), a.0 as u32, set_idx.0, 0],
            L::StrLike { src, pat_idx, ci } => [ExprOp::StrLike.as_wire(), ci as u32, src.0 as u32, pat_idx.0, 0],
            L::StrConcat { a, b, skip_null } => bin(ExprOp::StrConcat, skip_null as u32, a, b),
            L::IntToStr { a } => un(ExprOp::IntToStr, 0, a),
            L::FloatToStr { a, f32 } => un(ExprOp::FloatToStr, f32 as u32, a),
            L::StrToInt { a, fi } => un(ExprOp::StrToInt, fi.type_code().as_wire() as u32, a),
            L::StrToFloat { a } => un(ExprOp::StrToFloat, 0, a),
            L::StrSide { src, n_reg, left } => bin(ExprOp::StrSide, left as u32, src, n_reg),
            L::StrPos { hay, needle } => bin(ExprOp::StrPos, 0, hay, needle),
            L::StrReverse { a } => un(ExprOp::StrReverse, 0, a),
            L::StrReplace { s, from, to } => [ExprOp::StrReplace.as_wire(), 0, s.0 as u32, from.0 as u32, to.0 as u32],
            L::StrPad { s, n_reg, fill, left } => [
                ExprOp::StrPad.as_wire(),
                left as u32,
                s.0 as u32,
                n_reg.0 as u32,
                fill.0 as u32,
            ],
            L::StrSplitPart { s, delim, n_reg } => [
                ExprOp::StrSplitPart.as_wire(),
                0,
                s.0 as u32,
                delim.0 as u32,
                n_reg.0 as u32,
            ],
        }
    }

    /// Decode one wire instruction, the exact inverse of [`Self::to_wire`].
    /// Client-controlled words, so a bad one is rejected rather than panicked.
    ///
    /// The match over [`ExprOp`] is exhaustive with **no `_` arm**, and every
    /// opcode narrows its selector — through its family's `from_wire`, or
    /// through `no_sel`. Together that makes the accepted `(op, selector)` set
    /// computable from the enums instead of from a hand-synced table.
    ///
    /// Not a bijection: unused operand words are ignored.
    fn from_wire(w: [u32; INSTR_WORDS]) -> Result<LogicalInstr, ExprValidateErr> {
        use LogicalInstr as L;
        let [opw, sel, w2, w3, w4] = w;
        let op = ExprOp::from_wire(opw).ok_or(ExprValidateErr::UnknownOpcode(opw))?;
        let (a, b, c) = (Reg::from_wire(w2), Reg::from_wire(w3), Reg::from_wire(w4));
        // The error for a selector outside its opcode's family; flag and
        // cast_target report the same one.
        let bad_sel = || ExprValidateErr::BadSelector { op: opw, selector: sel };
        // An opcode that names no family carries selector 0 and nothing else.
        let no_sel = |i: L| if sel == 0 { Ok(i) } else { Err(bad_sel()) };
        let cmp_op = || CmpOp::from_wire(sel).ok_or_else(bad_sel);
        Ok(match op {
            ExprOp::LoadCol => no_sel(L::LoadCol { col: w2 })?,
            ExprOp::LoadConst => L::LoadConst {
                val: decode_load_const(w2, w3),
                unsigned: flag(opw, sel)?,
            },
            ExprOp::IntArith => L::IntArith {
                op: IntArithOp::from_wire(sel).ok_or_else(bad_sel)?,
                a,
                b,
            },
            ExprOp::FloatArith => L::FloatArith {
                op: FloatArithOp::from_wire(sel).ok_or_else(bad_sel)?,
                a,
                b,
            },
            ExprOp::Cmp => L::Cmp { op: cmp_op()?, a, b },
            ExprOp::FCmp => L::FCmp { op: cmp_op()?, a, b },
            ExprOp::IntToFloat => no_sel(L::IntToFloat { a })?,
            ExprOp::FloatUnary => L::FloatUnary {
                op: FloatUnaryOp::from_wire(sel).ok_or_else(bad_sel)?,
                a,
            },
            ExprOp::IntUnary => L::IntUnary {
                op: IntUnaryOp::from_wire(sel).ok_or_else(bad_sel)?,
                a,
            },
            ExprOp::Calendar => L::Calendar {
                op: CalendarOp::from_wire(sel).ok_or_else(bad_sel)?,
                a,
                micros: flag(opw, w3)?,
            },
            // `cast_target` sees the full u32: a forged high-bit selector must be
            // rejected, not silently truncated into a valid type code.
            ExprOp::FloatToInt => L::FloatToInt { a, fi: cast_target(opw, sel)? },
            ExprOp::IntCast => L::IntCast { a, fi: cast_target(opw, sel)? },
            ExprOp::FloatToF32 => no_sel(L::FloatToF32 { a })?,
            ExprOp::IntMinMax2 => L::IntMinMax2 { a, b, is_max: flag(opw, sel)? },
            ExprOp::FloatMinMax2 => L::FloatMinMax2 { a, b, is_max: flag(opw, sel)? },
            ExprOp::Select => no_sel(L::Select { cond: a, a: b, b: c })?,
            ExprOp::LoadNull => no_sel(L::LoadNull)?,
            ExprOp::BoolBinary => L::BoolBinary { a, b, is_or: flag(opw, sel)? },
            ExprOp::BoolNot => no_sel(L::BoolNot { a })?,
            ExprOp::IsNull => L::IsNull { col: w2, invert: flag(opw, sel)? },
            ExprOp::IsNullReg => L::IsNullReg { a, invert: flag(opw, sel)? },
            ExprOp::StrColConst => L::StrColConst {
                op: cmp_op()?,
                col: w2,
                const_idx: ConstIdx(w3),
            },
            ExprOp::StrColCol => L::StrColCol { op: cmp_op()?, col_a: w2, col_b: w3 },
            ExprOp::StrColPrefix => no_sel(L::StrColPrefix { col: w2, const_idx: ConstIdx(w3) })?,
            ExprOp::IntInSet => no_sel(L::IntInSet { value_reg: a, set_idx: ConstIdx(w3) })?,
            ExprOp::LoadColStr => no_sel(L::LoadColStr { col: w2 })?,
            ExprOp::LoadConstStr => no_sel(L::LoadConstStr { const_idx: ConstIdx(w2) })?,
            ExprOp::LoadNullStr => no_sel(L::LoadNullStr)?,
            ExprOp::StrSelect => no_sel(L::StrSelect { cond: a, a: b, b: c })?,
            ExprOp::StrCmp => L::StrCmp { op: cmp_op()?, a, b },
            ExprOp::StrLen => L::StrLen { a, chars: flag(opw, sel)? },
            ExprOp::StrCase => L::StrCase { a, upper: flag(opw, sel)? },
            ExprOp::StrSubstr => no_sel(L::StrSubstr {
                src: a,
                start_reg: b,
                len_reg: (w4 != u32::MAX).then_some(c),
            })?,
            ExprOp::StrTrim => L::StrTrim {
                a,
                mode: TrimMode::from_wire(sel).ok_or_else(bad_sel)?,
                set_idx: ConstIdx(w3),
            },
            ExprOp::StrLike => L::StrLike {
                src: a,
                pat_idx: ConstIdx(w3),
                ci: flag(opw, sel)?,
            },
            ExprOp::StrConcat => L::StrConcat { a, b, skip_null: flag(opw, sel)? },
            ExprOp::IntToStr => no_sel(L::IntToStr { a })?,
            ExprOp::FloatToStr => L::FloatToStr { a, f32: flag(opw, sel)? },
            ExprOp::StrToInt => L::StrToInt { a, fi: cast_target(opw, sel)? },
            ExprOp::StrToFloat => no_sel(L::StrToFloat { a })?,
            ExprOp::StrSide => L::StrSide { src: a, n_reg: b, left: flag(opw, sel)? },
            ExprOp::StrPos => no_sel(L::StrPos { hay: a, needle: b })?,
            ExprOp::StrReverse => no_sel(L::StrReverse { a })?,
            ExprOp::StrReplace => no_sel(L::StrReplace { s: a, from: b, to: c })?,
            ExprOp::StrPad => L::StrPad {
                s: a,
                n_reg: b,
                fill: c,
                left: flag(opw, sel)?,
            },
            ExprOp::StrSplitPart => no_sel(L::StrSplitPart { s: a, delim: b, n_reg: c })?,
        })
    }
}

impl Sink {
    /// Serialise to the wire pair `[kind, value]`, the inverse of
    /// [`Self::from_wire`].
    fn to_wire(self) -> [u32; SINK_WORDS] {
        match self {
            Sink::Col(src_col) => [SinkKind::Col.as_wire(), src_col],
            Sink::Reg(r) => [SinkKind::Reg.as_wire(), r.0 as u32],
        }
    }

    /// Decode one sink pair `[kind, value]`, the inverse of [`Self::to_wire`].
    /// The sink region is its own space, so a bad kind is its own error rather
    /// than an [`ExprOp`] that does not exist.
    fn from_wire([kind, value]: [u32; SINK_WORDS]) -> Result<Sink, ExprValidateErr> {
        match SinkKind::from_wire(kind).ok_or(ExprValidateErr::BadSinkKind(kind))? {
            SinkKind::Col => Ok(Sink::Col(value)),
            SinkKind::Reg => Ok(Sink::Reg(Reg::from_wire(value))),
        }
    }
}

/// The three cast opcodes' selector as the fixed-int target it names. A forged
/// code must not reach eval, where it would index a bounds table that has no arm
/// for it.
fn cast_target(op: u32, selector: u32) -> Result<FixedInt, ExprValidateErr> {
    u8::try_from(selector)
        .ok()
        .and_then(TypeCode::from_wire)
        // A temporal code names no cast target of its own: the encoder spells
        // the storage type, so a decoded selector must be one too.
        .and_then(FixedInt::exact)
        .ok_or(ExprValidateErr::BadSelector { op, selector })
}

/// A two-member family's selector as the boolean field the instruction carries
/// it in.
fn flag(op: u32, selector: u32) -> Result<bool, ExprValidateErr> {
    match selector {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ExprValidateErr::BadSelector { op, selector }),
    }
}

// ---------------------------------------------------------------------------
// The blob framing
// ---------------------------------------------------------------------------

/// Serialise an expr program. Layout (all little-endian):
///
/// ```text
/// 0   4   instruction count N
/// 4   20N instruction words
/// ..  4   sink count M
/// ..  8M  sink words
/// ..  4   const-pool count S
/// ..  S × { 4-byte length L, L bytes }
/// ```
///
/// The register count is not carried: a register is the index of the
/// instruction that writes it, so N *is* the register file's size.
fn encode_expr_blob(
    code: impl ExactSizeIterator<Item = [u32; INSTR_WORDS]>,
    sinks: impl ExactSizeIterator<Item = [u32; SINK_WORDS]>,
    const_strings: &[Vec<u8>],
) -> Vec<u8> {
    let (n, m) = (code.len(), sinks.len());
    // The exact encoded length, so nothing reallocates.
    let mut w = Writer::with_capacity(
        12 + n * INSTR_BYTES + m * SINK_BYTES + const_strings.iter().map(|s| 4 + s.len()).sum::<usize>(),
    );
    w.u32(n as u32);
    for instr in code {
        for word in instr {
            w.u32(word);
        }
    }
    w.u32(m as u32);
    for sink in sinks {
        for word in sink {
            w.u32(word);
        }
    }
    w.u32(const_strings.len() as u32);
    for s in const_strings {
        w.bytes32(s);
    }
    w.into_vec()
}

impl LogicalProgram {
    /// Serialise to the wire blob — the inverse of [`Self::from_blob`], and the
    /// one encode.
    pub fn to_blob_bytes(&self) -> Vec<u8> {
        encode_expr_blob(
            self.instrs().iter().copied().map(LogicalInstr::to_wire),
            self.sinks().iter().copied().map(Sink::to_wire),
            self.const_strings(),
        )
    }

    /// The program as a wire map writing payload slots `out_cols`, each the type
    /// and nullability declared for that slot.
    pub fn to_map(&self, out_cols: Vec<(TypeCode, bool)>) -> gnitz_wire::ComputeMap {
        gnitz_wire::ComputeMap { program: self.to_blob_bytes(), out_cols }
    }

    /// A program blob: the header walked, then the regions lowered into the
    /// typed logical form — the inverse of [`Self::to_blob_bytes`].
    ///
    /// Every count is bounded against the bytes present before any cap applies,
    /// so a forged count reports truncation rather than a limit it never reached.
    pub fn from_blob(blob: &[u8]) -> Result<Self, ExprValidateErr> {
        let (code, sinks, const_strings) = decode_all(blob, "expr blob", |r| {
            let n = r.u32()? as usize;
            let code = r.take(n * INSTR_BYTES)?;
            let m = r.u32()? as usize;
            let sinks = r.take(m * SINK_BYTES)?;
            if m > gnitz_wire::MAX_COLUMNS {
                return Err(format!("sink count {m} exceeds {}", gnitz_wire::MAX_COLUMNS));
            }
            // No fixed-stride region to bound this count, so it is capped as
            // declared.
            let s = r.u32()? as usize;
            if s > MAX_CONST_POOL {
                return Err(format!("declared const-pool count {s} exceeds {MAX_CONST_POOL}"));
            }
            // Never pre-sized: no allocation here is sized by a number the
            // sender chose.
            let mut const_strings: Vec<Vec<u8>> = Vec::new();
            for _ in 0..s {
                const_strings.push(r.bytes32()?.to_vec());
            }
            Ok((code, sinks, const_strings))
        })
        .map_err(ExprValidateErr::CorruptBlob)?;
        let n = code.len() / INSTR_BYTES;
        if n > MAX_REGS {
            return Err(ExprValidateErr::TooManyRegs(n as u32));
        }

        // Each region was taken at a multiple of its stride, so no tail remains.
        let instrs = code
            .as_chunks::<INSTR_BYTES>()
            .0
            .iter()
            .map(|t| LogicalInstr::from_wire(std::array::from_fn(|i| gnitz_wire::read_u32_le(t, 4 * i))))
            .collect::<Result<Vec<_>, _>>()?;
        let sinks = sinks
            .as_chunks::<SINK_BYTES>()
            .0
            .iter()
            .map(|t| Sink::from_wire(std::array::from_fn(|i| gnitz_wire::read_u32_le(t, 4 * i))))
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(instrs, sinks, const_strings)
    }
}

#[cfg(test)]
#[path = "tests/blob.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/blob.rs"]
mod bench;
