//! [`ExprBuilder`]: the emitter that turns a client-side expression into an
//! expression program, and [`ExprProgram`], the wire blob it produces.
//!
//! The builder accumulates typed [`LogicalInstr`]s and serialises them once, in
//! [`ExprBuilder::build`], through `LogicalInstr::to_wire`. It therefore holds
//! **no** knowledge of the wire word layout: which operand word carries a packed
//! pair, which opcode a flag selects, and where a cast target rides are stated
//! once in `to_wire` and read back once in `LogicalProgram::decode_quad` — both
//! in `program.rs`, where the drift tests that hold them together also live.
//!
//! Every method here is infallible: an operand a wire word could forge — a cast
//! target, TRIM's mode, LIKE's escape — is a typed parameter, so the builder
//! cannot express one, and the structural rules are decided when the
//! instructions are assembled into a [`LogicalProgram`]. A caller builds first
//! and is told what is unsupported afterwards.

use crate::{CmpOp, ExprValidateErr, LogicalInstr, LogicalInstr as L, LogicalProgram, StrOp};
use gnitz_wire::{FixedInt, TrimMode};

/// A compiled expression program: a flat list of 4-word instructions
/// (opcode, dst_reg, arg1, arg2) plus metadata for embedding in filter params.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExprProgram {
    pub num_regs: u32,
    pub result_reg: u32,
    pub code: Vec<u32>,
    /// The byte-transparent const pool: length-prefixed raw byte strings. Holds
    /// german-string cells (string comparisons) and packed sorted-i64 arrays
    /// (`INT_IN_SET` value pools) alike — each entry is read by the opcode that
    /// indexes it, never as text.
    pub const_strings: Vec<Vec<u8>>,
}

impl ExprProgram {
    /// Serialise to a self-contained byte sequence (magic "EXPR"), suitable for a BLOB column.
    pub fn encode(&self) -> Vec<u8> {
        let strs: Vec<&[u8]> = self.const_strings.iter().map(Vec::as_slice).collect();
        gnitz_wire::encode_expr_blob(self.num_regs, self.result_reg, &self.code, &strs)
    }
}

/// Builds an expression program with automatic register allocation.
#[derive(Default)]
pub struct ExprBuilder {
    instrs: Vec<LogicalInstr>,
    next_reg: u32,
    const_strings: Vec<Vec<u8>>,
}

impl ExprBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a destination register, push the instruction `mk` builds into it,
    /// and return it. Every value-producing method below is one call to this, so
    /// none of them touches the register counter directly.
    fn push(&mut self, mk: impl FnOnce(u16) -> LogicalInstr) -> u32 {
        let dst = self.next_reg;
        self.next_reg += 1;
        self.instrs.push(mk(dst as u16));
        dst
    }

    /// Push an instruction that allocates no register — the two output opcodes.
    fn push_void(&mut self, instr: LogicalInstr) {
        self.instrs.push(instr);
    }

    /// A two-operand instruction. Registers are `u32` on this API and `u16` in
    /// an instruction, and this is the one place that narrows them, so no method
    /// below spells a cast.
    fn bin(&mut self, mk: impl FnOnce(u16, u16, u16) -> LogicalInstr, a: u32, b: u32) -> u32 {
        self.push(|dst| mk(dst, a as u16, b as u16))
    }

    /// A one-operand instruction — [`Self::bin`]'s unary twin.
    fn un(&mut self, mk: impl FnOnce(u16, u16) -> LogicalInstr, a: u32) -> u32 {
        self.push(|dst| mk(dst, a as u16))
    }

    // --- Column and constant loads ---

    pub fn load_col_int(&mut self, col_idx: usize) -> u32 {
        self.push(|dst| L::LoadColInt {
            dst,
            col: col_idx as u32,
        })
    }

    pub fn load_col_float(&mut self, col_idx: usize) -> u32 {
        self.push(|dst| L::LoadColFloat {
            dst,
            col: col_idx as u32,
        })
    }

    pub fn load_const(&mut self, value: i64) -> u32 {
        self.push(|dst| L::LoadConst { dst, val: value })
    }

    // --- Integer arithmetic ---

    pub fn add(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::IntAdd { dst, a, b }, a, b)
    }

    pub fn sub(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::IntSub { dst, a, b }, a, b)
    }

    pub fn mul(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::IntMul { dst, a, b }, a, b)
    }

    pub fn div(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::IntDiv { dst, a, b }, a, b)
    }

    pub fn modulo(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::IntMod { dst, a, b }, a, b)
    }

    /// Both pure integer unary transforms, the operator carried as data — the
    /// shape [`LogicalInstr::IntUnary`] already stores it in.
    pub fn int_unary(&mut self, op: crate::IntUnaryOp, a: u32) -> u32 {
        self.un(|dst, a| L::IntUnary { op, dst, a }, a)
    }

    // --- Float arithmetic ---

    pub fn float_add(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::FloatAdd { dst, a, b }, a, b)
    }

    pub fn float_sub(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::FloatSub { dst, a, b }, a, b)
    }

    pub fn float_mul(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::FloatMul { dst, a, b }, a, b)
    }

    pub fn float_div(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::FloatDiv { dst, a, b }, a, b)
    }

    /// Every pure float unary transform, the operator carried as data.
    pub fn float_unary(&mut self, op: crate::FloatUnaryOp, a: u32) -> u32 {
        self.un(|dst, a| L::FloatUnary { op, dst, a }, a)
    }

    // --- Comparison ---

    /// Integer compare. The operator is a parameter rather than one method per
    /// operator: [`LogicalInstr::Cmp`] already carries it as data, and so does
    /// every consumer downstream.
    pub fn cmp(&mut self, op: CmpOp, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::Cmp { op, dst, a, b }, a, b)
    }

    /// IEEE-754 compare — [`Self::cmp`]'s float twin.
    pub fn fcmp(&mut self, op: CmpOp, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::FCmp { op, dst, a, b }, a, b)
    }

    // --- Boolean ---

    pub fn bool_and(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::BoolAnd { dst, a, b }, a, b)
    }

    pub fn bool_or(&mut self, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::BoolOr { dst, a, b }, a, b)
    }

    pub fn bool_not(&mut self, a: u32) -> u32 {
        self.un(|dst, a| L::BoolNot { dst, a }, a)
    }

    // --- Null checks ---

    /// `IS NULL`, or `IS NOT NULL` when `invert`.
    pub fn is_null(&mut self, col_idx: usize, invert: bool) -> u32 {
        self.push(|dst| L::IsNull {
            dst,
            col: col_idx as u32,
            invert,
        })
    }

    // --- Numeric conversion and scalar functions ---

    pub fn int_to_float(&mut self, src: u32) -> u32 {
        self.un(|dst, a| L::IntToFloat { dst, a }, src)
    }

    pub fn float_to_f32(&mut self, src: u32) -> u32 {
        self.un(|dst, a| L::FloatToF32 { dst, a }, src)
    }

    pub fn int_cast(&mut self, src: u32, to: FixedInt) -> u32 {
        self.push(|dst| L::IntCast {
            dst,
            a: src as u16,
            fi: to,
        })
    }

    pub fn float_to_int(&mut self, src: u32, to: FixedInt) -> u32 {
        self.push(|dst| L::FloatToInt {
            dst,
            a: src as u16,
            fi: to,
        })
    }

    /// 2-ary integer extremum; `is_max` picks MAX over MIN.
    pub fn int_minmax2(&mut self, is_max: bool, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::IntMinMax2 { dst, a, b, is_max }, a, b)
    }

    /// 2-ary float extremum; `is_max` picks MAX over MIN.
    pub fn float_minmax2(&mut self, is_max: bool, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::FloatMinMax2 { dst, a, b, is_max }, a, b)
    }

    // --- Conditional ---

    pub fn select(&mut self, cond: u32, a: u32, b: u32) -> u32 {
        self.bin(
            |dst, a, b| L::Select {
                dst,
                cond: cond as u16,
                a,
                b,
            },
            a,
            b,
        )
    }

    /// Materialize a NULL value into a fresh register (value 0, null bit set).
    pub fn load_null(&mut self) -> u32 {
        self.push(|dst| L::LoadNull { dst })
    }

    // --- Output opcodes ---

    pub fn emit_col(&mut self, src_reg: u32, payload_col_idx: u32) {
        self.push_void(L::Emit {
            src: src_reg as u16,
            out: payload_col_idx,
        });
    }

    /// Copy input column `src_col_idx` verbatim into output payload slot
    /// `payload_col_idx`. No type operand: the engine resolves the source
    /// locator (PK byte window or dense payload slot) and both widths from the
    /// schemas it validates the program against, so a restated type code could
    /// only ever disagree with them.
    pub fn copy_col(&mut self, src_col_idx: u32, payload_col_idx: u32) {
        self.push_void(L::CopyCol {
            src_col: src_col_idx,
            out: payload_col_idx,
        });
    }

    // --- Const pool (byte-transparent) ---

    /// Push a raw byte string into the const pool and return its index. The pool
    /// is byte-transparent: german-string cells and packed i64 sets share it,
    /// each interpreted by the opcode that indexes it. Every call pushes a fresh
    /// entry — no cross-call dedup.
    fn add_const_bytes(&mut self, bytes: Vec<u8>) -> u32 {
        let idx = self.const_strings.len() as u32;
        self.const_strings.push(bytes);
        idx
    }

    pub fn add_const_string(&mut self, s: String) -> u32 {
        self.add_const_bytes(s.into_bytes())
    }

    /// Push an i64 value pool for `INT_IN_SET`, packed as `N × 8-byte LE`, and
    /// return its const index. Sorting is not a wire contract: the engine's
    /// `resolve` sorts the decoded pool before binary-searching it, because set
    /// membership does not depend on order and trusting the client here would
    /// turn a skewed pool into a wrong answer. Callers still sort (and dedup) to
    /// keep the pool small.
    pub fn add_const_int_set(&mut self, values: &[i64]) -> u32 {
        self.add_const_bytes(gnitz_wire::as_le_bytes(values).to_vec())
    }

    // --- Integer set membership ---

    /// `value_reg IN <set at const_idx>` → a fresh 0/1 boolean register. NULL
    /// input propagates to NULL (the engine copies the operand's null word).
    pub fn int_in_set(&mut self, value_reg: u32, const_idx: u32) -> u32 {
        self.push(|dst| L::IntInSet {
            dst,
            value_reg: value_reg as u16,
            set_idx: const_idx,
        })
    }

    // --- Fused German-string column comparisons ---

    /// A string column against a const-pool entry, compared as 16-byte cells.
    pub fn str_col_const(&mut self, op: StrOp, col_idx: usize, const_idx: u32) -> u32 {
        self.push(|dst| L::StrColConst {
            op,
            dst,
            col: col_idx as u32,
            const_idx,
        })
    }

    /// Two string columns against each other, compared as 16-byte cells.
    pub fn str_col_col(&mut self, op: StrOp, col_a: usize, col_b: usize) -> u32 {
        self.push(|dst| L::StrColCol {
            op,
            dst,
            col_a: col_a as u32,
            col_b: col_b as u32,
        })
    }

    // --- String registers ---

    pub fn load_col_str(&mut self, col_idx: usize) -> u32 {
        self.push(|dst| L::LoadColStr {
            dst,
            col: col_idx as u32,
        })
    }

    pub fn load_const_str(&mut self, const_idx: u32) -> u32 {
        self.push(|dst| L::LoadConstStr { dst, const_idx })
    }

    /// Materialize a NULL *string* into a fresh register. A string context must
    /// emit this rather than [`Self::load_null`], so the register's class
    /// matches what its consumers read.
    pub fn load_null_str(&mut self) -> u32 {
        self.push(|dst| L::LoadNullStr { dst })
    }

    pub fn str_select(&mut self, cond: u32, a: u32, b: u32) -> u32 {
        self.bin(
            |dst, a, b| L::StrSelect {
                dst,
                cond: cond as u16,
                a,
                b,
            },
            a,
            b,
        )
    }

    /// Compare two string registers into a fresh boolean register.
    pub fn str_cmp(&mut self, op: StrOp, a: u32, b: u32) -> u32 {
        self.bin(|dst, a, b| L::StrCmp { op, dst, a, b }, a, b)
    }

    /// Length in bytes, or in characters when `chars`.
    pub fn str_len(&mut self, a: u32, chars: bool) -> u32 {
        self.push(|dst| L::StrLen {
            dst,
            a: a as u16,
            chars,
        })
    }

    /// ASCII case fold; `upper` picks UPPER over LOWER.
    pub fn str_case(&mut self, a: u32, upper: bool) -> u32 {
        self.push(|dst| L::StrCase {
            dst,
            a: a as u16,
            upper,
        })
    }

    /// `len = None` is the no-FOR form, which runs to the end of the string.
    pub fn str_substr(&mut self, src: u32, start: u32, len: Option<u32>) -> u32 {
        self.push(|dst| L::StrSubstr {
            dst,
            src: src as u16,
            start_reg: start as u16,
            len_reg: len.map(|l| l as u16),
        })
    }

    /// `set_idx` names the const-pool entry holding the bytes to strip.
    pub fn str_trim(&mut self, src: u32, mode: TrimMode, set_idx: u32) -> u32 {
        self.push(|dst| L::StrTrim {
            dst,
            a: src as u16,
            mode,
            set_idx,
        })
    }

    /// `src [I]LIKE <pattern at pat_idx>` → a fresh 0/1 boolean register.
    /// `escape = None` disables escaping and rides as byte 0; `pat_idx` names the
    /// const-pool entry holding the raw pattern bytes. `ci` picks ILIKE's
    /// ASCII-case-insensitive form.
    pub fn str_like(&mut self, src: u32, escape: Option<u8>, pat_idx: u32, ci: bool) -> u32 {
        self.push(|dst| L::StrLike {
            dst,
            src: src as u16,
            escape,
            pat_idx,
            ci,
        })
    }

    /// String concatenation. `skip_null = false` is SQL `||` (NULL in either
    /// operand, NULL out); `true` is one `CONCAT(…)` fold step, where a NULL `b`
    /// contributes the empty string while a NULL accumulator still propagates.
    pub fn str_concat(&mut self, a: u32, b: u32, skip_null: bool) -> u32 {
        self.bin(|dst, a, b| L::StrConcat { dst, a, b, skip_null }, a, b)
    }

    pub fn int_to_str(&mut self, a: u32) -> u32 {
        self.un(|dst, a| L::IntToStr { dst, a }, a)
    }

    pub fn float_to_str(&mut self, a: u32) -> u32 {
        self.un(|dst, a| L::FloatToStr { dst, a }, a)
    }

    pub fn str_to_int(&mut self, a: u32, to: FixedInt) -> u32 {
        self.push(|dst| L::StrToInt {
            dst,
            a: a as u16,
            fi: to,
        })
    }

    pub fn str_to_float(&mut self, a: u32) -> u32 {
        self.un(|dst, a| L::StrToFloat { dst, a }, a)
    }

    /// Serialise to the wire form, for a program that will be shipped to the
    /// engine. A caller that resolves the program in-process wants
    /// [`Self::build_logical`] instead, which skips the encode entirely.
    pub fn build(self, result_reg: u32) -> ExprProgram {
        ExprProgram {
            num_regs: self.next_reg,
            result_reg,
            code: self.instrs.iter().copied().flat_map(LogicalInstr::to_wire).collect(),
            const_strings: self.const_strings,
        }
    }

    /// The typed program directly, skipping the wire round trip a client-side
    /// caller would otherwise pay to resolve its own expression: the builder
    /// already holds the instructions `from_wire` would have to decode back out.
    /// Fallible for the same reasons `from_wire` is — the structural rules
    /// (register file limit, single assignment, const-pool bounds) are checked
    /// here.
    pub fn build_logical(self, result_reg: u32) -> Result<LogicalProgram, ExprValidateErr> {
        LogicalProgram::from_instrs(self.instrs, self.next_reg, result_reg, self.const_strings)
    }
}

#[cfg(test)]
#[path = "tests/builder.rs"]
mod tests;
