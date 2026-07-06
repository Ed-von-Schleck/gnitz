//! Compiled scalar-expression programs.
//!
//! Two typed forms: `LogicalProgram` (`LogicalInstr`, logical column indices —
//! the shape the wire blob lowers into) and `ResolvedProgram` (`Instr`, resolved
//! payload/PK indices — the evaluable form). `LogicalProgram::resolve` consumes
//! the former and produces the latter. Instruction meaning is carried by the
//! type: a missing or mis-routed opcode is a compile error, not a silent
//! miscompute.

use crate::foundation::codec::read_u32_le;
use crate::schema::{german_string_tail, SchemaDescriptor};
// Wire opcodes (1–46) the client emits, brought in as engine-local `u32`
// constants for the `from_wire` lowering match. gnitz-wire exposes its raw opcode
// constants only through `cast_consts!`, so this is the single access point.
gnitz_wire::cast_consts! { u32;
    EXPR_LOAD_COL_INT, EXPR_LOAD_COL_FLOAT, EXPR_LOAD_CONST,
    EXPR_INT_ADD, EXPR_INT_SUB, EXPR_INT_MUL, EXPR_INT_DIV, EXPR_INT_MOD, EXPR_INT_NEG,
    EXPR_FLOAT_ADD, EXPR_FLOAT_SUB, EXPR_FLOAT_MUL, EXPR_FLOAT_DIV, EXPR_FLOAT_NEG,
    EXPR_CMP_EQ, EXPR_CMP_NE, EXPR_CMP_GT, EXPR_CMP_GE, EXPR_CMP_LT, EXPR_CMP_LE,
    EXPR_FCMP_EQ, EXPR_FCMP_NE, EXPR_FCMP_GT, EXPR_FCMP_GE, EXPR_FCMP_LT, EXPR_FCMP_LE,
    EXPR_BOOL_AND, EXPR_BOOL_OR, EXPR_BOOL_NOT,
    EXPR_IS_NULL, EXPR_IS_NOT_NULL,
    EXPR_EMIT, EXPR_INT_TO_FLOAT, EXPR_COPY_COL,
    EXPR_STR_COL_EQ_CONST, EXPR_STR_COL_LT_CONST, EXPR_STR_COL_LE_CONST,
    EXPR_STR_COL_EQ_COL, EXPR_STR_COL_LT_COL, EXPR_STR_COL_LE_COL,
    EXPR_EMIT_NULL,
}

// ---------------------------------------------------------------------------
// Typed instruction operands
// ---------------------------------------------------------------------------

/// Comparison operator, shared by integer (`Cmp`) and float (`FCmp`) compares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CmpOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// German-string comparison operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StrOp {
    Eq,
    Lt,
    Le,
}

/// Source of a `CopyCol`: a dense payload column, or a single column within the
/// OPK PK region addressed by its byte offset. Replaces the old negative
/// `-(off)-1` sentinel packed into the operand slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColSrc {
    Payload(u8),
    Pk { off: u8 },
}

// ---------------------------------------------------------------------------
// LogicalInstr — the wire-mirroring form (logical column indices)
// ---------------------------------------------------------------------------

/// One instruction with logical (schema) column indices, mirroring the wire
/// opcodes the client emits. `LogicalProgram::resolve` lowers each into `Instr`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LogicalInstr {
    LoadColInt {
        dst: u16,
        col: u32,
    },
    LoadColFloat {
        dst: u16,
        col: u32,
    },
    LoadConst {
        dst: u16,
        val: i64,
    },
    IntAdd {
        dst: u16,
        a: u16,
        b: u16,
    },
    IntSub {
        dst: u16,
        a: u16,
        b: u16,
    },
    IntMul {
        dst: u16,
        a: u16,
        b: u16,
    },
    IntDiv {
        dst: u16,
        a: u16,
        b: u16,
    },
    IntMod {
        dst: u16,
        a: u16,
        b: u16,
    },
    IntNeg {
        dst: u16,
        a: u16,
    },
    FloatAdd {
        dst: u16,
        a: u16,
        b: u16,
    },
    FloatSub {
        dst: u16,
        a: u16,
        b: u16,
    },
    FloatMul {
        dst: u16,
        a: u16,
        b: u16,
    },
    FloatDiv {
        dst: u16,
        a: u16,
        b: u16,
    },
    FloatNeg {
        dst: u16,
        a: u16,
    },
    Cmp {
        op: CmpOp,
        dst: u16,
        a: u16,
        b: u16,
    },
    FCmp {
        op: CmpOp,
        dst: u16,
        a: u16,
        b: u16,
    },
    IntToFloat {
        dst: u16,
        a: u16,
    },
    BoolAnd {
        dst: u16,
        a: u16,
        b: u16,
    },
    BoolOr {
        dst: u16,
        a: u16,
        b: u16,
    },
    BoolNot {
        dst: u16,
        a: u16,
    },
    IsNull {
        dst: u16,
        col: u32,
    },
    IsNotNull {
        dst: u16,
        col: u32,
    },
    StrColConst {
        op: StrOp,
        dst: u16,
        col: u32,
        const_idx: u32,
    },
    StrColCol {
        op: StrOp,
        dst: u16,
        col_a: u32,
        col_b: u32,
    },
    CopyCol {
        src_col: u32,
        out: u32,
        tc: u8,
    },
    Emit {
        src: u16,
        out: u32,
    },
    EmitNull {
        out: u32,
    },
}

// ---------------------------------------------------------------------------
// Instr — the resolved/evaluable form (physical payload/PK indices)
// ---------------------------------------------------------------------------

/// One resolved, evaluable instruction. `signed` flags carry the result of the
/// per-register U64 type tracking: `signed: false` selects the unsigned path on
/// `Cmp`/`IntDiv`/`IntMod`/`IntToFloat`, reinterpreting the i64 register as u64.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Instr {
    LoadPayloadInt {
        dst: u16,
        pi: u8,
    },
    LoadPayloadFloat {
        dst: u16,
        pi: u8,
    },
    /// PK-region integer load: the addressed OPK column at byte `off`, width
    /// `size`; `signed` selects the sign-flip decode, `tc` drives it.
    LoadPk {
        dst: u16,
        off: u8,
        size: u8,
        tc: u8,
        signed: bool,
    },
    LoadConst {
        dst: u16,
        val: i64,
    },
    IntAdd {
        dst: u16,
        a: u16,
        b: u16,
    },
    IntSub {
        dst: u16,
        a: u16,
        b: u16,
    },
    IntMul {
        dst: u16,
        a: u16,
        b: u16,
    },
    IntDiv {
        dst: u16,
        a: u16,
        b: u16,
        signed: bool,
    },
    IntMod {
        dst: u16,
        a: u16,
        b: u16,
        signed: bool,
    },
    Cmp {
        op: CmpOp,
        dst: u16,
        a: u16,
        b: u16,
        signed: bool,
    },
    FCmp {
        op: CmpOp,
        dst: u16,
        a: u16,
        b: u16,
    },
    FloatAdd {
        dst: u16,
        a: u16,
        b: u16,
    },
    FloatSub {
        dst: u16,
        a: u16,
        b: u16,
    },
    FloatMul {
        dst: u16,
        a: u16,
        b: u16,
    },
    FloatDiv {
        dst: u16,
        a: u16,
        b: u16,
    },
    IntNeg {
        dst: u16,
        a: u16,
    },
    FloatNeg {
        dst: u16,
        a: u16,
    },
    IntToFloat {
        dst: u16,
        a: u16,
        signed: bool,
    },
    BoolAnd {
        dst: u16,
        a: u16,
        b: u16,
    },
    BoolOr {
        dst: u16,
        a: u16,
        b: u16,
    },
    BoolNot {
        dst: u16,
        a: u16,
    },
    IsNull {
        dst: u16,
        pi: u8,
    },
    IsNotNull {
        dst: u16,
        pi: u8,
    },
    StrColConst {
        op: StrOp,
        dst: u16,
        pi: u8,
        const_idx: u32,
    },
    StrColCol {
        op: StrOp,
        dst: u16,
        pi_a: u8,
        pi_b: u8,
    },
    CopyCol {
        out: u32,
        src: ColSrc,
        tc: u8,
    },
    Emit {
        src: u16,
        out: u32,
    },
    EmitNull {
        out: u32,
    },
}

/// Classification of each output-producing instruction, in program order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutputColKind {
    /// Column copied verbatim from the input batch.
    CopyCol { src: ColSrc, out_payload: u32, tc: u8 },
    /// Computed register written to an output payload column.
    Emit { reg: usize, out_payload: usize },
    /// Always-NULL output column.
    EmitNull { out_payload: u32 },
}

impl OutputColKind {
    /// Destination payload column this instruction writes. For the finalize path
    /// it equals the dense output position; for the general MAP path it is the
    /// authoritative scatter target.
    pub(crate) fn out_payload(&self) -> usize {
        match *self {
            OutputColKind::CopyCol { out_payload, .. } | OutputColKind::EmitNull { out_payload } => {
                out_payload as usize
            }
            OutputColKind::Emit { out_payload, .. } => out_payload,
        }
    }
}

// ---------------------------------------------------------------------------
// LogicalProgram — pre-resolve container
// ---------------------------------------------------------------------------

pub struct LogicalProgram {
    instrs: Vec<LogicalInstr>,
    num_regs: u32,
    result_reg: u32,
    const_strings: Vec<Vec<u8>>,
}

impl LogicalProgram {
    /// Build from typed instructions, validating the construction-time
    /// invariants. Used by `from_wire` and the test builders.
    pub(crate) fn new(instrs: Vec<LogicalInstr>, num_regs: u32, result_reg: u32, const_strings: Vec<Vec<u8>>) -> Self {
        // The BOOL_AND/BOOL_OR 3VL paths and null-bit propagation operate on
        // `u64` words indexed by register, so the register file is capped at 64.
        assert!(
            num_regs <= 64,
            "LogicalProgram: num_regs={num_regs} exceeds the 64-register limit"
        );
        assert!(
            num_regs == 0 || result_reg < num_regs,
            "LogicalProgram: result_reg={result_reg} >= num_regs={num_regs}"
        );
        gnitz_debug!(
            "expr_program: instrs={} regs={} consts={}",
            instrs.len(),
            num_regs,
            const_strings.len()
        );
        use LogicalInstr as L;
        for instr in &instrs {
            match *instr {
                // Binary ALU: SSA (dst must not alias a source — `reg3` borrows
                // three distinct register slots) + bounds on dst, a, b.
                L::IntAdd { dst, a, b }
                | L::IntSub { dst, a, b }
                | L::IntMul { dst, a, b }
                | L::IntDiv { dst, a, b }
                | L::IntMod { dst, a, b }
                | L::FloatAdd { dst, a, b }
                | L::FloatSub { dst, a, b }
                | L::FloatMul { dst, a, b }
                | L::FloatDiv { dst, a, b }
                | L::Cmp { dst, a, b, .. }
                | L::FCmp { dst, a, b, .. }
                | L::BoolAnd { dst, a, b }
                | L::BoolOr { dst, a, b } => {
                    assert!(
                        dst != a && dst != b,
                        "LogicalProgram: register aliasing dst={dst} a={a} b={b}"
                    );
                    assert_reg(dst, num_regs);
                    assert_reg(a, num_regs);
                    assert_reg(b, num_regs);
                }
                // Unary register readers that write dst: bounds dst, a.
                L::IntNeg { dst, a } | L::FloatNeg { dst, a } | L::IntToFloat { dst, a } | L::BoolNot { dst, a } => {
                    assert_reg(dst, num_regs);
                    assert_reg(a, num_regs);
                }
                // EMIT reads a source register; no dst register.
                L::Emit { src, .. } => assert_reg(src, num_regs),
                // dst-writers whose other operands are column / const indices.
                L::LoadColInt { dst, .. }
                | L::LoadColFloat { dst, .. }
                | L::LoadConst { dst, .. }
                | L::IsNull { dst, .. }
                | L::IsNotNull { dst, .. }
                | L::StrColConst { dst, .. }
                | L::StrColCol { dst, .. } => assert_reg(dst, num_regs),
                // No register operands.
                L::CopyCol { .. } | L::EmitNull { .. } => {}
            }
        }
        LogicalProgram {
            instrs,
            num_regs,
            result_reg,
            const_strings,
        }
    }

    /// Lower a wire expr blob (flat u32 quads `[op, dst, a1, a2]`) into the
    /// typed logical form. The single point that knows the wire encoding.
    pub(crate) fn from_wire(code: &[u32], num_regs: u32, result_reg: u32, const_strings: Vec<Vec<u8>>) -> Self {
        assert_eq!(
            code.len() % 4,
            0,
            "from_wire: code length {} is not a multiple of 4",
            code.len()
        );
        let mut instrs = Vec::with_capacity(code.len() / 4);
        for q in code.chunks_exact(4) {
            let op = q[0];
            let dst = q[1] as u16;
            let a = q[2] as u16;
            let b = q[3] as u16;
            // Map a wire compare opcode to its operator; both closures capture this
            // instruction's dst/a/b so the per-opcode arms below stay one-liners.
            let cmp = |op| LogicalInstr::Cmp { op, dst, a, b };
            let fcmp = |op| LogicalInstr::FCmp { op, dst, a, b };
            instrs.push(match op {
                EXPR_LOAD_COL_INT => LogicalInstr::LoadColInt { dst, col: q[2] },
                EXPR_LOAD_COL_FLOAT => LogicalInstr::LoadColFloat { dst, col: q[2] },
                // 64-bit constant split low/high across the two operand words.
                EXPR_LOAD_CONST => LogicalInstr::LoadConst {
                    dst,
                    val: ((q[3] as i64) << 32) | (q[2] as i64 & 0xFFFF_FFFF),
                },
                EXPR_INT_ADD => LogicalInstr::IntAdd { dst, a, b },
                EXPR_INT_SUB => LogicalInstr::IntSub { dst, a, b },
                EXPR_INT_MUL => LogicalInstr::IntMul { dst, a, b },
                EXPR_INT_DIV => LogicalInstr::IntDiv { dst, a, b },
                EXPR_INT_MOD => LogicalInstr::IntMod { dst, a, b },
                EXPR_INT_NEG => LogicalInstr::IntNeg { dst, a },
                EXPR_FLOAT_ADD => LogicalInstr::FloatAdd { dst, a, b },
                EXPR_FLOAT_SUB => LogicalInstr::FloatSub { dst, a, b },
                EXPR_FLOAT_MUL => LogicalInstr::FloatMul { dst, a, b },
                EXPR_FLOAT_DIV => LogicalInstr::FloatDiv { dst, a, b },
                EXPR_FLOAT_NEG => LogicalInstr::FloatNeg { dst, a },
                EXPR_CMP_EQ => cmp(CmpOp::Eq),
                EXPR_CMP_NE => cmp(CmpOp::Ne),
                EXPR_CMP_GT => cmp(CmpOp::Gt),
                EXPR_CMP_GE => cmp(CmpOp::Ge),
                EXPR_CMP_LT => cmp(CmpOp::Lt),
                EXPR_CMP_LE => cmp(CmpOp::Le),
                EXPR_FCMP_EQ => fcmp(CmpOp::Eq),
                EXPR_FCMP_NE => fcmp(CmpOp::Ne),
                EXPR_FCMP_GT => fcmp(CmpOp::Gt),
                EXPR_FCMP_GE => fcmp(CmpOp::Ge),
                EXPR_FCMP_LT => fcmp(CmpOp::Lt),
                EXPR_FCMP_LE => fcmp(CmpOp::Le),
                EXPR_BOOL_AND => LogicalInstr::BoolAnd { dst, a, b },
                EXPR_BOOL_OR => LogicalInstr::BoolOr { dst, a, b },
                EXPR_BOOL_NOT => LogicalInstr::BoolNot { dst, a },
                EXPR_IS_NULL => LogicalInstr::IsNull { dst, col: q[2] },
                EXPR_IS_NOT_NULL => LogicalInstr::IsNotNull { dst, col: q[2] },
                EXPR_EMIT => LogicalInstr::Emit { src: a, out: q[3] },
                EXPR_INT_TO_FLOAT => LogicalInstr::IntToFloat { dst, a },
                EXPR_COPY_COL => LogicalInstr::CopyCol {
                    src_col: q[2],
                    out: q[3],
                    tc: q[1] as u8,
                },
                EXPR_STR_COL_EQ_CONST => LogicalInstr::StrColConst {
                    op: StrOp::Eq,
                    dst,
                    col: q[2],
                    const_idx: q[3],
                },
                EXPR_STR_COL_LT_CONST => LogicalInstr::StrColConst {
                    op: StrOp::Lt,
                    dst,
                    col: q[2],
                    const_idx: q[3],
                },
                EXPR_STR_COL_LE_CONST => LogicalInstr::StrColConst {
                    op: StrOp::Le,
                    dst,
                    col: q[2],
                    const_idx: q[3],
                },
                EXPR_STR_COL_EQ_COL => LogicalInstr::StrColCol {
                    op: StrOp::Eq,
                    dst,
                    col_a: q[2],
                    col_b: q[3],
                },
                EXPR_STR_COL_LT_COL => LogicalInstr::StrColCol {
                    op: StrOp::Lt,
                    dst,
                    col_a: q[2],
                    col_b: q[3],
                },
                EXPR_STR_COL_LE_COL => LogicalInstr::StrColCol {
                    op: StrOp::Le,
                    dst,
                    col_a: q[2],
                    col_b: q[3],
                },
                EXPR_EMIT_NULL => LogicalInstr::EmitNull { out: q[2] },
                _ => panic!("from_wire: unknown expr opcode {op}"),
            });
        }
        LogicalProgram::new(instrs, num_regs, result_reg, const_strings)
    }

    pub(crate) fn len(&self) -> usize {
        self.instrs.len()
    }

    /// If every instruction is `CopyCol` writing dense payload outputs
    /// `out = [0, 1, 2, …]` (in instruction order), return the copies' source
    /// columns — the program's payload copy list, from which `emit_node` derives
    /// a reindex MAP's output payload schema. `None` for any other shape (a
    /// compute instruction, or a permuted/offset destination).
    pub(crate) fn payload_copy_srcs(&self) -> Option<Vec<u32>> {
        self.instrs
            .iter()
            .enumerate()
            .map(|(i, instr)| match *instr {
                LogicalInstr::CopyCol { src_col, out, .. } if out == i as u32 => Some(src_col),
                _ => None,
            })
            .collect()
    }

    /// If every instruction is `CopyCol` forming one contiguous block copy
    /// `src = [base, base+1, …]` → `out = [0, 1, 2, …]`, return `Some(base)`:
    /// the count of leading columns the program skips (the PK region a finalize
    /// / identity MAP inherits verbatim rather than copying). Otherwise `None`.
    ///
    /// Both checks are load-bearing — sequential sources AND dense destinations;
    /// a permuted-destination program is a real permutation, not an identity.
    pub(crate) fn sequential_copy_base(&self) -> Option<usize> {
        let srcs = self.payload_copy_srcs()?;
        let &base = srcs.first()?;
        let ok = srcs.iter().enumerate().all(|(i, &s)| s == base + i as u32);
        ok.then_some(base as usize)
    }

    /// Lower to the resolved form and classify register roles for the given
    /// context (`is_filter = true` keeps `result_reg` eligible for bit_only).
    /// Consuming: a `Vec<LogicalInstr>` cannot be mutated in place into a
    /// `Vec<Instr>`. Preserves the per-register U64 signed→unsigned tracking.
    pub(crate) fn resolve(self, schema: &SchemaDescriptor, is_filter: bool) -> ResolvedProgram {
        use crate::schema::type_code;
        use Instr as I;
        use LogicalInstr as L;
        // Per-register type code of the most recently produced integer value.
        // Drives the signed→unsigned variant for U64 operands, whose i64 bit
        // pattern is negative for values >= 2^63. 0 = unknown (treated signed).
        let mut reg_tc = [0u8; 64];
        let is_u64 = |tc: u8| tc == type_code::U64;
        let mut instrs = Vec::with_capacity(self.instrs.len());
        for li in self.instrs {
            match li {
                L::LoadColInt { dst, col } => {
                    let ci = col as usize;
                    let tc = schema.columns[ci].type_code;
                    if schema.is_pk_col(ci) {
                        instrs.push(I::LoadPk {
                            dst,
                            off: schema.pk_byte_offset(ci),
                            size: schema.columns[ci].size(),
                            tc,
                            signed: crate::schema::is_signed_int(tc),
                        });
                    } else {
                        instrs.push(I::LoadPayloadInt {
                            dst,
                            pi: schema.payload_mapping_byte(ci),
                        });
                    }
                    reg_tc[dst as usize] = tc;
                }
                L::LoadColFloat { dst, col } => {
                    let ci = col as usize;
                    debug_assert!(
                        !schema.is_pk_col(ci),
                        "resolve: LOAD_COL_FLOAT references PK column {ci} (PK is never float)"
                    );
                    instrs.push(I::LoadPayloadFloat {
                        dst,
                        pi: schema.payload_mapping_byte(ci),
                    });
                    reg_tc[dst as usize] = 0;
                }
                L::LoadConst { dst, val } => {
                    instrs.push(I::LoadConst { dst, val });
                    reg_tc[dst as usize] = 0;
                }
                L::IntAdd { dst, a, b } => {
                    instrs.push(I::IntAdd { dst, a, b });
                    reg_tc[dst as usize] = propagate_u64(&reg_tc, a, b);
                }
                L::IntSub { dst, a, b } => {
                    instrs.push(I::IntSub { dst, a, b });
                    reg_tc[dst as usize] = propagate_u64(&reg_tc, a, b);
                }
                L::IntMul { dst, a, b } => {
                    instrs.push(I::IntMul { dst, a, b });
                    reg_tc[dst as usize] = propagate_u64(&reg_tc, a, b);
                }
                L::IntDiv { dst, a, b } => {
                    let u = any_u64(&reg_tc, a, b);
                    instrs.push(I::IntDiv { dst, a, b, signed: !u });
                    reg_tc[dst as usize] = if u { type_code::U64 } else { 0 };
                }
                L::IntMod { dst, a, b } => {
                    let u = any_u64(&reg_tc, a, b);
                    instrs.push(I::IntMod { dst, a, b, signed: !u });
                    reg_tc[dst as usize] = if u { type_code::U64 } else { 0 };
                }
                L::IntNeg { dst, a } => {
                    instrs.push(I::IntNeg { dst, a });
                    reg_tc[dst as usize] = reg_tc[a as usize];
                }
                L::FloatAdd { dst, a, b } => instrs.push(I::FloatAdd { dst, a, b }),
                L::FloatSub { dst, a, b } => instrs.push(I::FloatSub { dst, a, b }),
                L::FloatMul { dst, a, b } => instrs.push(I::FloatMul { dst, a, b }),
                L::FloatDiv { dst, a, b } => instrs.push(I::FloatDiv { dst, a, b }),
                L::FloatNeg { dst, a } => instrs.push(I::FloatNeg { dst, a }),
                L::Cmp { op, dst, a, b } => {
                    // EQ/NE are bit-identical signed/unsigned; ordered compares
                    // pick the unsigned form when either operand is U64.
                    let signed = matches!(op, CmpOp::Eq | CmpOp::Ne) || !any_u64(&reg_tc, a, b);
                    instrs.push(I::Cmp { op, dst, a, b, signed });
                    reg_tc[dst as usize] = 0;
                }
                L::FCmp { op, dst, a, b } => instrs.push(I::FCmp { op, dst, a, b }),
                L::IntToFloat { dst, a } => {
                    let signed = !is_u64(reg_tc[a as usize]);
                    instrs.push(I::IntToFloat { dst, a, signed });
                    reg_tc[dst as usize] = 0;
                }
                L::BoolAnd { dst, a, b } => instrs.push(I::BoolAnd { dst, a, b }),
                L::BoolOr { dst, a, b } => instrs.push(I::BoolOr { dst, a, b }),
                L::BoolNot { dst, a } => instrs.push(I::BoolNot { dst, a }),
                L::IsNull { dst, col } => instrs.push(I::IsNull {
                    dst,
                    pi: schema.payload_mapping_byte(col as usize),
                }),
                L::IsNotNull { dst, col } => instrs.push(I::IsNotNull {
                    dst,
                    pi: schema.payload_mapping_byte(col as usize),
                }),
                L::StrColConst {
                    op,
                    dst,
                    col,
                    const_idx,
                } => {
                    instrs.push(I::StrColConst {
                        op,
                        dst,
                        pi: schema.payload_mapping_byte(col as usize),
                        const_idx,
                    });
                }
                L::StrColCol { op, dst, col_a, col_b } => {
                    instrs.push(I::StrColCol {
                        op,
                        dst,
                        pi_a: schema.payload_mapping_byte(col_a as usize),
                        pi_b: schema.payload_mapping_byte(col_b as usize),
                    });
                }
                L::CopyCol { src_col, out, tc } => {
                    let ci = src_col as usize;
                    let src = if schema.is_pk_col(ci) {
                        ColSrc::Pk {
                            off: schema.pk_byte_offset(ci),
                        }
                    } else {
                        ColSrc::Payload(schema.payload_mapping_byte(ci))
                    };
                    instrs.push(I::CopyCol { out, src, tc });
                }
                L::Emit { src, out } => instrs.push(I::Emit { src, out }),
                L::EmitNull { out } => instrs.push(I::EmitNull { out }),
            }
        }
        let payload_col_info: Vec<(u8, u8)> = schema
            .payload_columns()
            .map(|(_, _, col)| (col.size(), col.type_code))
            .collect();
        // Precompute each string constant's compare key once (see field docs).
        let const_prefixes: Vec<u32> = self.const_strings.iter().map(|s| compute_prefix(s)).collect();
        let const_lengths: Vec<u32> = self.const_strings.iter().map(|s| s.len() as u32).collect();
        let mut prog = ResolvedProgram {
            instrs,
            num_regs: self.num_regs,
            result_reg: self.result_reg,
            const_strings: self.const_strings,
            const_prefixes,
            const_lengths,
            payload_col_info,
            bit_only_mask: 0,
            bool_input_mask: 0,
            chain_trigger_mask: 0,
        };
        prog.classify(is_filter);
        prog.compute_and_chain(is_filter);
        prog
    }
}

#[inline]
fn assert_reg(r: u16, num_regs: u32) {
    assert!(
        (r as u32) < num_regs,
        "LogicalProgram: register {r} out of range (num_regs={num_regs})"
    );
}

/// True iff either operand register currently holds a U64 value — the single
/// rule that drives every signed→unsigned variant selection in `resolve`.
#[inline]
fn any_u64(reg_tc: &[u8; 64], a: u16, b: u16) -> bool {
    let u64_tc = crate::schema::type_code::U64;
    reg_tc[a as usize] == u64_tc || reg_tc[b as usize] == u64_tc
}

#[inline]
fn propagate_u64(reg_tc: &[u8; 64], a: u16, b: u16) -> u8 {
    if any_u64(reg_tc, a, b) {
        crate::schema::type_code::U64
    } else {
        0
    }
}

// ---------------------------------------------------------------------------
// ResolvedProgram — the evaluable form
// ---------------------------------------------------------------------------

pub struct ResolvedProgram {
    pub(in crate::expr) instrs: Vec<Instr>,
    pub(in crate::expr) num_regs: u32,
    pub(in crate::expr) result_reg: u32,
    pub(in crate::expr) const_strings: Vec<Vec<u8>>,
    /// Per-constant precomputed 4-byte BE prefix and byte length, indexed by
    /// `const_idx` — hoisted out of the per-morsel `col <op> 'const'` compare loop.
    pub(in crate::expr) const_prefixes: Vec<u32>,
    pub(in crate::expr) const_lengths: Vec<u32>,
    /// (size, type_code) for each physical payload column, in payload order.
    pub(in crate::expr) payload_col_info: Vec<(u8, u8)>,
    /// Bit `r` set iff register `r` is only consumed by boolean ops, so its
    /// producer can skip the i64 unpack into `regs[r]`.
    pub(in crate::expr) bit_only_mask: u64,
    /// Bit `r` set iff register `r` is read by a boolean consumer; the producer
    /// must populate `bool_bits[r]`.
    pub(in crate::expr) bool_input_mask: u64,
    /// Destination registers of the non-terminal ANDs in the one result-terminal
    /// AND chain: bit `r` set means "if the AND writing register `r` is all
    /// definite-FALSE for the morsel, the filter result is too — write the terminal
    /// (`result_reg`) all-FALSE and stop". 0 for non-filter programs and programs
    /// with no such chain. Every register is `< num_regs ≤ 64` (asserted in
    /// `LogicalProgram::new`), so a u64 indexed by register suffices.
    pub(in crate::expr) chain_trigger_mask: u64,
}

/// Invoke `f` once per register this instruction reads.
fn each_reg_read(i: &Instr, mut f: impl FnMut(u16)) {
    use Instr::*;
    match *i {
        IntAdd { a, b, .. }
        | IntSub { a, b, .. }
        | IntMul { a, b, .. }
        | IntDiv { a, b, .. }
        | IntMod { a, b, .. }
        | FloatAdd { a, b, .. }
        | FloatSub { a, b, .. }
        | FloatMul { a, b, .. }
        | FloatDiv { a, b, .. }
        | Cmp { a, b, .. }
        | FCmp { a, b, .. }
        | BoolAnd { a, b, .. }
        | BoolOr { a, b, .. } => {
            f(a);
            f(b);
        }
        IntNeg { a, .. } | FloatNeg { a, .. } | IntToFloat { a, .. } | BoolNot { a, .. } => f(a),
        Emit { src, .. } => f(src),
        LoadPayloadInt { .. }
        | LoadPayloadFloat { .. }
        | LoadPk { .. }
        | LoadConst { .. }
        | IsNull { .. }
        | IsNotNull { .. }
        | StrColConst { .. }
        | StrColCol { .. }
        | CopyCol { .. }
        | EmitNull { .. } => {}
    }
}

impl ResolvedProgram {
    #[inline]
    pub(in crate::expr) fn is_bit_only(&self, reg: usize) -> bool {
        (self.bit_only_mask >> reg) & 1 != 0
    }

    /// True iff `reg`'s producer must write `bool_bits[reg]` — either because a
    /// downstream BOOL consumer reads it, or because `reg` is bit_only and
    /// `run_filter` reads `bool_bits[result_reg]` directly.
    #[inline]
    pub(in crate::expr) fn needs_bool_pack(&self, reg: usize) -> bool {
        ((self.bit_only_mask | self.bool_input_mask) >> reg) & 1 != 0
    }

    /// Populate `bit_only_mask` / `bool_input_mask` from the resolved program.
    pub(in crate::expr) fn classify(&mut self, is_filter: bool) {
        let (bit_only, bool_input) = self.classify_registers(is_filter);
        self.bit_only_mask = bit_only;
        self.bool_input_mask = bool_input;
    }

    /// Detect the one result-terminal AND chain and record, in `chain_trigger_mask`,
    /// the destination registers of its non-terminal ANDs. At runtime, when such an
    /// AND is all definite-FALSE for a morsel, `eval_batch` writes the terminal
    /// (`result_reg`) all-FALSE and breaks (see the `BoolAnd` nullable arm). Only
    /// the accumulator spine is walked, so an inner AND reached through a
    /// `BoolNot`/`BoolOr` operand is never marked — forcing FALSE under those would
    /// be a miscompile.
    pub(in crate::expr) fn compute_and_chain(&mut self, is_filter: bool) {
        self.chain_trigger_mask = 0;
        let n = self.instrs.len();
        // Filter-only; need ≥ 3 instrs for a ≥ 2-AND chain. A filter has one
        // register per instruction, so n == num_regs ≤ 64 (asserted), keeping
        // `pc as u8` and the register-indexed scratch arrays in range.
        if !is_filter || self.num_regs == 0 || !(3..=64).contains(&n) {
            return;
        }
        // Terminal = last instruction = expression root; must be an AND on result_reg.
        let Instr::BoolAnd {
            dst: term_dst,
            a: term_a,
            b: term_b,
        } = self.instrs[n - 1]
        else {
            return;
        };
        if term_dst as u32 != self.result_reg {
            return;
        }
        // Single-assignment (alloc_reg never reuses): each register has one writer.
        // Track only AND writers — a spine link must be an AND, so a non-MAX slot
        // already means "written by an AND".
        let mut and_writer = [u8::MAX; 64];
        let mut use_count = [0u8; 64];
        for (pc, ins) in self.instrs.iter().enumerate() {
            if let Instr::BoolAnd { dst, .. } = *ins {
                and_writer[dst as usize] = pc as u8;
            }
            each_reg_read(ins, |r| use_count[r as usize] = use_count[r as usize].saturating_add(1));
        }
        // A spine link is an AND-written register used exactly once (clean chain).
        let is_link = |r: u16| and_writer[r as usize] != u8::MAX && use_count[r as usize] == 1;
        // Walk the accumulator spine; mark every non-terminal chain AND by its dst.
        let mut mask = 0u64;
        let (mut a, mut b) = (term_a, term_b);
        loop {
            let acc = if is_link(a) {
                a
            } else if is_link(b) {
                b
            } else {
                break;
            };
            mask |= 1u64 << acc;
            let w = and_writer[acc as usize] as usize;
            let Instr::BoolAnd { a: na, b: nb, .. } = self.instrs[w] else {
                break;
            };
            a = na;
            b = nb;
        }
        self.chain_trigger_mask = mask;
    }

    /// Classify each register's role on the nullable-arm hot path. Returns
    /// `(bit_only_mask, bool_input_mask)`. `is_filter = true` keeps `result_reg`
    /// eligible for bit_only (the `run_filter` fast path reads `bool_bits` of it
    /// directly); for maps it is demoted to keep any stray `regs[result_reg]`
    /// read live.
    pub(in crate::expr) fn classify_registers(&self, is_filter: bool) -> (u64, u64) {
        use Instr::*;
        let mut bool_produced: u64 = 0;
        let mut non_bool_read: u64 = 0;
        let mut bool_input: u64 = 0;
        for instr in &self.instrs {
            match *instr {
                // Bool producers that are also binary register readers.
                Cmp { dst, a, b, .. } | FCmp { dst, a, b, .. } => {
                    debug_assert!((dst as usize) < 64, "classify: dst {dst} >= 64");
                    debug_assert!((a as usize) < 64 && (b as usize) < 64, "classify: src {a}/{b} >= 64");
                    bool_produced |= 1u64 << dst;
                    non_bool_read |= (1u64 << a) | (1u64 << b);
                }
                // Binary register readers (non-bool-producing).
                IntAdd { a, b, .. }
                | IntSub { a, b, .. }
                | IntMul { a, b, .. }
                | IntDiv { a, b, .. }
                | IntMod { a, b, .. }
                | FloatAdd { a, b, .. }
                | FloatSub { a, b, .. }
                | FloatMul { a, b, .. }
                | FloatDiv { a, b, .. } => {
                    debug_assert!((a as usize) < 64 && (b as usize) < 64, "classify: src {a}/{b} >= 64");
                    non_bool_read |= (1u64 << a) | (1u64 << b);
                }
                // Bool producers whose operands are payload columns, not regs.
                StrColConst { dst, .. } | StrColCol { dst, .. } | IsNull { dst, .. } | IsNotNull { dst, .. } => {
                    debug_assert!((dst as usize) < 64, "classify: dst {dst} >= 64");
                    bool_produced |= 1u64 << dst;
                }
                // Binary bool consumers: producer + bool_input (not non_bool_read).
                BoolAnd { dst, a, b } | BoolOr { dst, a, b } => {
                    debug_assert!((dst as usize) < 64, "classify: dst {dst} >= 64");
                    debug_assert!(
                        (a as usize) < 64 && (b as usize) < 64,
                        "classify: BOOL src {a}/{b} >= 64"
                    );
                    bool_produced |= 1u64 << dst;
                    bool_input |= (1u64 << a) | (1u64 << b);
                }
                // Unary bool consumer.
                BoolNot { dst, a } => {
                    debug_assert!((dst as usize) < 64, "classify: dst {dst} >= 64");
                    debug_assert!((a as usize) < 64, "classify: BOOL src {a} >= 64");
                    bool_produced |= 1u64 << dst;
                    bool_input |= 1u64 << a;
                }
                // Unary register readers (non-bool).
                IntNeg { a, .. } | FloatNeg { a, .. } | IntToFloat { a, .. } => {
                    debug_assert!((a as usize) < 64, "classify: src {a} >= 64");
                    non_bool_read |= 1u64 << a;
                }
                Emit { src, .. } => {
                    debug_assert!((src as usize) < 64, "classify: EMIT src {src} >= 64");
                    non_bool_read |= 1u64 << src;
                }
                // No register reads / not bool.
                LoadPayloadInt { .. }
                | LoadPayloadFloat { .. }
                | LoadPk { .. }
                | LoadConst { .. }
                | CopyCol { .. }
                | EmitNull { .. } => {}
            }
        }
        let mut bit_only = bool_produced & !non_bool_read;
        if !is_filter && self.num_regs > 0 {
            bit_only &= !(1u64 << self.result_reg as usize);
        }
        (bit_only, bool_input)
    }

    /// Classify each output-producing instruction, in program order.
    pub(in crate::expr) fn classify_output_cols(&self) -> Vec<OutputColKind> {
        use Instr::*;
        let mut out_cols = Vec::new();
        for instr in &self.instrs {
            match *instr {
                CopyCol { src, out, tc } => out_cols.push(OutputColKind::CopyCol {
                    src,
                    out_payload: out,
                    tc,
                }),
                Emit { src, out } => out_cols.push(OutputColKind::Emit {
                    reg: src as usize,
                    out_payload: out as usize,
                }),
                EmitNull { out } => out_cols.push(OutputColKind::EmitNull { out_payload: out }),
                // Subset-select: only the three output variants matter here.
                _ => {}
            }
        }
        out_cols
    }

    /// Returns true if no instruction in this program can produce a NULL result
    /// (so the evaluator can skip null-bit tracking entirely).
    pub(in crate::expr) fn is_strictly_non_nullable(&self, schema: &SchemaDescriptor) -> bool {
        use Instr::*;
        let nullable_payload = |pi: u8| -> bool {
            let a = pi as usize;
            a < schema.num_payload_cols() && schema.columns[schema.payload_col_idx(a)].nullable != 0
        };
        for instr in &self.instrs {
            match *instr {
                // Division/modulo produce NULL on a zero divisor.
                IntDiv { .. } | IntMod { .. } | FloatDiv { .. } => return false,
                // IS_NULL / IS_NOT_NULL read the batch null bits.
                IsNull { .. } | IsNotNull { .. } => return false,
                // Column reads: null when the underlying column is nullable.
                LoadPayloadInt { pi, .. } | LoadPayloadFloat { pi, .. } | StrColConst { pi, .. }
                    if nullable_payload(pi) =>
                {
                    return false
                }
                StrColCol { pi_a, pi_b, .. } if nullable_payload(pi_a) || nullable_payload(pi_b) => return false,
                // Exhaustive remainder (no `_` wildcard): a future null-producing
                // variant must be classified here, not silently treated as safe.
                LoadPayloadInt { .. }
                | LoadPayloadFloat { .. }
                | StrColConst { .. }
                | StrColCol { .. }
                | LoadPk { .. }
                | LoadConst { .. }
                | IntAdd { .. }
                | IntSub { .. }
                | IntMul { .. }
                | IntNeg { .. }
                | FloatAdd { .. }
                | FloatSub { .. }
                | FloatMul { .. }
                | FloatNeg { .. }
                | Cmp { .. }
                | FCmp { .. }
                | IntToFloat { .. }
                | BoolAnd { .. }
                | BoolOr { .. }
                | BoolNot { .. }
                | CopyCol { .. }
                | Emit { .. }
                | EmitNull { .. } => {}
            }
        }
        true
    }
}

// ---------------------------------------------------------------------------
// String comparison helpers (column vs constant)
// ---------------------------------------------------------------------------

/// 4-byte prefix of `s` as big-endian u32 for ordered comparison. Short strings
/// zero-pad on the right so integer `<`/`>` matches lexicographic byte order.
pub(in crate::expr) fn compute_prefix(s: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    let n = s.len().min(4);
    buf[..n].copy_from_slice(&s[..n]);
    u32::from_be_bytes(buf)
}

/// Compare a German String column value against a constant byte string.
pub(in crate::expr) fn compare_col_string_vs_const(
    struct_bytes: &[u8],
    blob: &[u8],
    const_bytes: &[u8],
    const_prefix: u32,
    const_len: u32,
) -> std::cmp::Ordering {
    let col_len = read_u32_le(struct_bytes, 0) as usize;
    let c_len = const_len as usize;
    let min_len = col_len.min(c_len);

    // Single big-endian integer comparison of the 4-byte prefix. Both sides
    // zero-pad bytes beyond their length, so no masking is needed.
    let col_pfx = u32::from_be_bytes(struct_bytes[4..8].try_into().unwrap());
    if col_pfx != const_prefix {
        return col_pfx.cmp(&const_prefix);
    }

    if min_len <= 4 {
        return col_len.cmp(&c_len);
    }

    // Bulk suffix comparison — vectorised memcmp via [u8]::cmp.
    let col_tail = german_string_tail(struct_bytes, blob, col_len, min_len);
    match col_tail.cmp(&const_bytes[4..min_len]) {
        std::cmp::Ordering::Equal => col_len.cmp(&c_len),
        ord => ord,
    }
}
