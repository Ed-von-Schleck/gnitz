//! [`ExprBuilder`]: the emitter that turns a client-side expression into an
//! expression program, and [`ExprProgram`], the wire blob it produces.
//!
//! The builder accumulates typed [`LogicalInstr`]s and serialises them once, in
//! [`ExprBuilder::build`], through [`LogicalInstr::to_wire`]. It therefore holds
//! **no** knowledge of the wire word layout: which operand word carries a packed
//! pair, which opcode a flag selects, and where a cast target rides are stated
//! once in `to_wire` and read back once in
//! [`LogicalProgram::from_wire`](crate::LogicalProgram::from_wire). The
//! round-trip test at the bottom of this file is what holds those two together.
//!
//! Everything reachable from here is infallible: a program's *validity* is
//! decided when it is assembled into a [`LogicalProgram`], so a caller can build
//! first and be told what is unsupported afterwards.

use crate::{CmpOp, ExprValidateErr, LogicalInstr as L, LogicalProgram, StrOp};
use gnitz_wire::{TrimMode, TypeCode};

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

type LogicalInstr = L;

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

    pub fn int_cast(&mut self, src: u32, to: TypeCode) -> u32 {
        self.push(|dst| L::IntCast {
            dst,
            a: src as u16,
            tc: to as u32,
        })
    }

    pub fn float_to_int(&mut self, src: u32, to: TypeCode) -> u32 {
        self.push(|dst| L::FloatToInt {
            dst,
            a: src as u16,
            tc: to as u32,
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
        let mut bytes = Vec::with_capacity(values.len() * 8);
        for v in values {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        self.add_const_bytes(bytes)
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
            mode: mode.as_wire(),
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
            escape: escape.unwrap_or(0) as u32,
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

    pub fn str_to_int(&mut self, a: u32, to: TypeCode) -> u32 {
        self.push(|dst| L::StrToInt {
            dst,
            a: a as u16,
            tc: to as u32,
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
            code: self.instrs.iter().flat_map(LogicalInstr::to_wire).collect(),
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
mod tests {
    use super::*;
    use crate::{FloatUnaryOp, IntUnaryOp, LogicalProgram};
    use std::collections::BTreeSet;

    /// One instance of every [`LogicalInstr`] variant, with a distinct value in
    /// every field so a swapped pair cannot round-trip by coincidence.
    ///
    /// The registers are deliberately small and the programs below are never
    /// resolved: what is under test is the word layout alone, so the list need
    /// not be a type-coherent or even a validatable program.
    fn every_variant() -> Vec<LogicalInstr> {
        use LogicalInstr as L;
        let mut v = vec![
            L::LoadColInt { dst: 1, col: 2 },
            L::LoadColFloat { dst: 3, col: 4 },
            L::LoadConst {
                dst: 5,
                val: -1_234_567_890_123,
            },
            L::IntAdd { dst: 6, a: 7, b: 8 },
            L::IntSub { dst: 9, a: 10, b: 11 },
            L::IntMul { dst: 12, a: 13, b: 14 },
            L::IntDiv { dst: 15, a: 16, b: 17 },
            L::IntMod { dst: 18, a: 19, b: 20 },
            L::FloatAdd { dst: 21, a: 22, b: 23 },
            L::FloatSub { dst: 24, a: 25, b: 26 },
            L::FloatMul { dst: 27, a: 28, b: 29 },
            L::FloatDiv { dst: 30, a: 31, b: 32 },
            L::IntToFloat { dst: 33, a: 34 },
            L::FloatToF32 { dst: 35, a: 36 },
            L::FloatToInt {
                dst: 37,
                a: 38,
                tc: TypeCode::I16 as u32,
            },
            L::IntCast {
                dst: 39,
                a: 40,
                tc: TypeCode::I32 as u32,
            },
            L::Select {
                dst: 41,
                cond: 42,
                a: 43,
                b: 44,
            },
            L::LoadNull { dst: 45 },
            L::BoolAnd { dst: 46, a: 47, b: 48 },
            L::BoolOr { dst: 49, a: 50, b: 51 },
            L::BoolNot { dst: 52, a: 53 },
            L::IsNull {
                dst: 54,
                col: 55,
                invert: false,
            },
            L::IsNull {
                dst: 56,
                col: 57,
                invert: true,
            },
            L::IntInSet {
                dst: 58,
                value_reg: 59,
                set_idx: 60,
            },
            L::LoadColStr { dst: 61, col: 62 },
            L::LoadConstStr { dst: 63, const_idx: 64 },
            L::LoadNullStr { dst: 65 },
            L::StrSelect {
                dst: 66,
                cond: 67,
                a: 68,
                b: 69,
            },
            // The two packed-pair families sit in *different* operand words —
            // SELECT/SUBSTR in a2, TRIM/LIKE in a1 — which is the single
            // per-opcode fact the encoder and decoder can silently disagree on.
            L::StrSubstr {
                dst: 70,
                src: 71,
                start_reg: 72,
                len_reg: Some(73),
            },
            L::StrSubstr {
                dst: 74,
                src: 75,
                start_reg: 76,
                len_reg: None,
            },
            L::StrTrim {
                dst: 77,
                a: 78,
                mode: TrimMode::Leading.as_wire(),
                set_idx: 79,
            },
            L::StrLike {
                dst: 80,
                src: 81,
                escape: b'!' as u32,
                pat_idx: 82,
                ci: false,
            },
            L::StrLike {
                dst: 83,
                src: 84,
                escape: 0,
                pat_idx: 85,
                ci: true,
            },
            L::IntToStr { dst: 86, a: 87 },
            L::FloatToStr { dst: 88, a: 89 },
            L::StrToInt {
                dst: 90,
                a: 91,
                tc: TypeCode::I64 as u32,
            },
            L::StrToFloat { dst: 92, a: 93 },
            L::CopyCol { src_col: 94, out: 95 },
            L::Emit { src: 96, out: 97 },
        ];
        // The operator- and flag-parameterized families, every value of each.
        for op in [CmpOp::Eq, CmpOp::Ne, CmpOp::Gt, CmpOp::Ge, CmpOp::Lt, CmpOp::Le] {
            v.push(L::Cmp {
                op,
                dst: 100,
                a: 101,
                b: 102,
            });
            v.push(L::FCmp {
                op,
                dst: 103,
                a: 104,
                b: 105,
            });
        }
        for op in [StrOp::Eq, StrOp::Lt, StrOp::Le] {
            v.push(L::StrColConst {
                op,
                dst: 106,
                col: 107,
                const_idx: 108,
            });
            v.push(L::StrColCol {
                op,
                dst: 109,
                col_a: 110,
                col_b: 111,
            });
            v.push(L::StrCmp {
                op,
                dst: 112,
                a: 113,
                b: 114,
            });
        }
        for op in [IntUnaryOp::Neg, IntUnaryOp::Abs] {
            v.push(L::IntUnary { op, dst: 115, a: 116 });
        }
        for op in [
            FloatUnaryOp::Neg,
            FloatUnaryOp::Abs,
            FloatUnaryOp::Floor,
            FloatUnaryOp::Ceil,
            FloatUnaryOp::Round,
            FloatUnaryOp::Trunc,
        ] {
            v.push(L::FloatUnary { op, dst: 117, a: 118 });
        }
        for is_max in [true, false] {
            v.push(L::IntMinMax2 {
                dst: 119,
                a: 120,
                b: 121,
                is_max,
            });
            v.push(L::FloatMinMax2 {
                dst: 122,
                a: 123,
                b: 124,
                is_max,
            });
        }
        for chars in [false, true] {
            v.push(L::StrLen {
                dst: 125,
                a: 126,
                chars,
            });
        }
        for upper in [true, false] {
            v.push(L::StrCase {
                dst: 127,
                a: 128,
                upper,
            });
        }
        for skip_null in [false, true] {
            v.push(L::StrConcat {
                dst: 129,
                a: 130,
                b: 131,
                skip_null,
            });
        }
        v
    }

    /// `from_wire ∘ to_wire == id`. The encoder and the decoder are the only two
    /// statements of the wire word layout, and this is what binds them: a swapped
    /// operand pair, a flag encoded into the wrong opcode, or a cast target on
    /// the wrong word all fail here.
    #[test]
    fn every_instruction_round_trips_through_the_wire_form() {
        let want = every_variant();
        let code: Vec<u32> = want.iter().flat_map(LogicalInstr::to_wire).collect();
        // `from_wire` runs the structure-only validation, which this deliberately
        // ill-formed fixture cannot pass — so decode the quads directly.
        let got: Vec<LogicalInstr> = code
            .chunks_exact(4)
            .map(|q| LogicalProgram::decode_quad(q).expect("to_wire emits a decodable opcode"))
            .collect();
        assert_eq!(got, want);
    }

    /// Every opcode the decoder accepts must be reachable from the encoder. An
    /// opcode only one side knows is drift the type system cannot see: the engine
    /// would accept a program no client can produce, or reject one it can.
    #[test]
    fn the_encoder_reaches_every_opcode_the_decoder_accepts() {
        let emitted: BTreeSet<u32> = every_variant().iter().map(|i| i.to_wire()[0]).collect();
        // An opcode is "accepted" iff `decode_quad` maps it; every arm reads its
        // operands without inspecting them, so an all-zero instruction probes the
        // opcode table alone. Swept over the whole `u16` rather than a range that
        // merely covers today's opcodes: a decoder arm added above the sweep would
        // be invisible here, which is the one drift this test exists to catch.
        let accepted: BTreeSet<u32> = (0..=u32::from(u16::MAX))
            .filter(|&op| LogicalProgram::decode_quad(&[op, 0, 0, 0]).is_ok())
            .collect();
        assert_eq!(emitted, accepted, "encoded opcodes vs. opcodes the decoder accepts");
    }

    /// The builder's own output must decode back to the instructions it recorded,
    /// which is what makes [`ExprBuilder::build`] and
    /// [`ExprBuilder::build_logical`] two views of one program rather than two
    /// programs.
    #[test]
    fn build_and_build_logical_describe_the_same_program() {
        // Built twice rather than cloned: the builder is consumed by both exits,
        // and a `Clone` derive kept only for a test would be a use nothing in
        // production has.
        let build = || {
            let mut b = ExprBuilder::new();
            let c = b.load_const(1_234_567_890_123);
            let col = b.load_col_int(0);
            let cond = b.cmp(CmpOp::Gt, col, c);
            let sel = b.select(cond, col, c);
            (b, sel)
        };
        let (b, sel) = build();
        let wire = b.build(sel);
        let (b, sel) = build();
        let logical = b.build_logical(sel).expect("a well-formed program");

        let decoded = LogicalProgram::from_wire(&wire.code, wire.num_regs, wire.result_reg, wire.const_strings)
            .expect("the builder must only emit decodable programs");
        assert_eq!(decoded.instrs(), logical.instrs());
    }

    #[test]
    fn encode_round_trips_through_wire_decoder() {
        let mut b = ExprBuilder::new();
        let c = b.load_const(1_234_567_890_123);
        let col = b.load_col_int(0);
        let cond = b.cmp(CmpOp::Gt, col, c);
        let s_idx = b.add_const_string("längre sträng".to_string());
        let _ = b.str_col_const(StrOp::Eq, 1, s_idx);
        let _ = b.add_const_string(String::new());
        let sel = b.select(cond, col, c);
        let prog = b.build(sel);

        let blob = prog.encode();
        let dec = gnitz_wire::decode_expr_blob(&blob).unwrap();
        assert_eq!(dec.num_regs, prog.num_regs);
        assert_eq!(dec.result_reg, prog.result_reg);
        assert_eq!(dec.code, prog.code);
        // Both sides are now `Vec<Vec<u8>>` — compare the byte-transparent pool directly.
        assert_eq!(dec.const_strings, prog.const_strings);
    }
}
