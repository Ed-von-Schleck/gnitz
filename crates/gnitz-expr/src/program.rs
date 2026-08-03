//! Compiled scalar-expression programs.
//!
//! Two typed forms: `LogicalProgram` (`LogicalInstr`, logical column indices —
//! the shape the wire blob lowers into) and `ResolvedProgram` (`Instr`, resolved
//! payload/PK indices — the evaluable form). `LogicalProgram::resolve` consumes
//! the former and produces the latter. Instruction meaning is carried by the
//! type: a missing or mis-routed opcode is a compile error, not a silent
//! miscompute.

use crate::{ColumnLocator, SchemaFacts};
use gnitz_wire::{encode_german_string, FixedInt, TypeCode};
use std::fmt;
// Wire opcodes (1–46) the client emits, matched as arms in `from_wire`. They are
// `pub const … : u32` in gnitz-wire, so a plain `use` binds them for pattern use.
use gnitz_wire::{
    EXPR_BOOL_AND, EXPR_BOOL_NOT, EXPR_BOOL_OR, EXPR_CMP_EQ, EXPR_CMP_GE, EXPR_CMP_GT, EXPR_CMP_LE, EXPR_CMP_LT,
    EXPR_CMP_NE, EXPR_COPY_COL, EXPR_EMIT, EXPR_FCMP_EQ, EXPR_FCMP_GE, EXPR_FCMP_GT, EXPR_FCMP_LE, EXPR_FCMP_LT,
    EXPR_FCMP_NE, EXPR_FLOAT_ADD, EXPR_FLOAT_DIV, EXPR_FLOAT_MUL, EXPR_FLOAT_NEG, EXPR_FLOAT_SUB, EXPR_INT_ADD,
    EXPR_INT_DIV, EXPR_INT_IN_SET, EXPR_INT_MOD, EXPR_INT_MUL, EXPR_INT_NEG, EXPR_INT_SUB, EXPR_INT_TO_FLOAT,
    EXPR_IS_NOT_NULL, EXPR_IS_NULL, EXPR_LOAD_COL_FLOAT, EXPR_LOAD_COL_INT, EXPR_LOAD_CONST, EXPR_LOAD_NULL,
    EXPR_SELECT, EXPR_STR_COL_EQ_COL, EXPR_STR_COL_EQ_CONST, EXPR_STR_COL_LE_COL, EXPR_STR_COL_LE_CONST,
    EXPR_STR_COL_LT_COL, EXPR_STR_COL_LT_CONST,
};

/// The register file is capped at 64: the BOOL_AND/BOOL_OR 3VL paths, the
/// null-bit propagation, and every register-indexed mask (`bit_only_mask`,
/// `bool_pack_mask`, `chain_trigger_mask`) address registers by bit in a `u64`.
/// Public because a rejection message states the limit, and the number a caller
/// prints must be the one [`LogicalProgram::from_wire`] enforces.
pub const MAX_REGS: usize = u64::BITS as usize;

/// Why a client-authored expr program was rejected at compile — a diagnostic for
/// the recovery log, and a value consumers compare and match on (a rejected
/// predicate is asserted to be exactly `PredicateWithoutResultReg`, and
/// `TooManyRegs(n)`'s payload names how many registers the program asked for).
/// Renaming or re-shaping a variant is a visible break, not an internal detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprValidateErr {
    UnknownOpcode(u32),
    TooManyRegs(u32),
    ResultRegOutOfRange { result_reg: u32, num_regs: u32 },
    RegOutOfRange { reg: u16, num_regs: u32 },
    RegisterAliasing { dst: u16, a: u16, b: u16 },
    ConstIdxOutOfRange { const_idx: u32, n: usize },
    IntSetNotAligned { set_idx: u32, len: usize },
    ColOutOfRange { col: u32, num_columns: usize },
    ColNotPayload { col: u32 },
    ColKindMismatch { col: u32, type_code: u8, want: ColKind },
    CopyTypeMismatch { col: u32, src_tc: u8, out: u32, out_tc: u8 },
    EmitSlotNotEightBytes { out: u32, type_code: u8 },
    OutputIdxOutOfRange { out: u32, num_payload_cols: usize },
    OutputSlotUnwritten { written: u64, num_payload_cols: usize },
    PredicateWithoutResultReg,
}

/// The client-facing rendering. Lives on the type so the planner's `Unsupported`
/// and the engine's compile rejection print the same wording, and so the limit
/// printed is the one [`LogicalProgram::from_wire`] enforces. `TooManyRegs` gets
/// a sentence naming that limit — it is the one variant a working query can hit;
/// the rest are internal-shape violations with no user action, rendered as
/// `Debug`.
impl fmt::Display for ExprValidateErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExprValidateErr::TooManyRegs(n) => {
                write!(
                    f,
                    "expression needs {n} registers; the limit is {MAX_REGS} — split the predicate"
                )
            }
            other => write!(f, "{other:?}"),
        }
    }
}

/// What an opcode's kernel requires of a column operand — a *region* requirement
/// (may it be a PK column?) and a *type* requirement, which are independent: the
/// integer load kernels have a PK arm, while the payload-only kernels address a
/// column through the dense payload index a PK column has no value for (the
/// `pi = 255` sentinel).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColKind {
    /// PK or payload, any type — a `CopyCol` source.
    AnyCol,
    /// PK or payload, fixed-width integer.
    FixedInt,
    /// Payload only, any type — the null-bitmap readers touch nothing else, so
    /// U128 and STRING are legitimate.
    AnyPayload,
    /// Payload only, IEEE-754.
    Float,
    /// Payload only, the 16-byte German-string layout.
    GermanString,
}

impl ColKind {
    /// True iff a PK column is unusable here.
    fn payload_only(self) -> bool {
        matches!(self, ColKind::AnyPayload | ColKind::Float | ColKind::GermanString)
    }
}

// ---------------------------------------------------------------------------
// Typed instruction operands
// ---------------------------------------------------------------------------

/// Comparison operator, shared by integer (`Cmp`) and float (`FCmp`) compares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// German-string comparison operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrOp {
    Eq,
    Lt,
    Le,
}

// ---------------------------------------------------------------------------
// LogicalInstr — the wire-mirroring form (logical column indices)
// ---------------------------------------------------------------------------

/// One instruction with logical (schema) column indices, mirroring the wire
/// opcodes the client emits. `LogicalProgram::resolve` lowers each into `Instr`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalInstr {
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
    /// SQL CASE blend: `dst` takes `a`'s value + null bit where `cond` is
    /// non-NULL and truthy, else `b`'s. Carries a value, never a boolean.
    Select {
        dst: u16,
        cond: u16,
        a: u16,
        b: u16,
    },
    /// Always-NULL value into `dst` (value 0, null bit set): CASE without ELSE,
    /// NULLIF's match branch.
    LoadNull {
        dst: u16,
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
    /// Integer set membership: `dst = value_reg ∈ set[set_idx]`. `set_idx` is a
    /// const-pool index (`u32`, like `StrColConst.const_idx`) — the packed
    /// sorted-i64 pool, decoded once at `resolve`.
    IntInSet {
        dst: u16,
        value_reg: u16,
        set_idx: u32,
    },
    CopyCol {
        src_col: u32,
        out: u32,
    },
    Emit {
        src: u16,
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
    /// Payload integer load. `fi` *is* the eight-arm decode the kernel dispatches
    /// on, established once at resolve time (`validate` pins the column to
    /// `ColKind::FixedInt`), so the row loop carries no wildcard arm.
    LoadPayloadInt {
        dst: u16,
        pi: u8,
        fi: FixedInt,
    },
    /// Payload float load. `wide` is the F64-vs-F32 decode selector; `validate`
    /// pins the column to `ColKind::Float`, so the two arms are total.
    LoadPayloadFloat {
        dst: u16,
        pi: u8,
        wide: bool,
    },
    /// PK-region integer load: the addressed OPK column at byte `off`. `off`
    /// cannot come from `fi` — it is the column's offset within the OPK region,
    /// not its width.
    LoadPk {
        dst: u16,
        off: u8,
        fi: FixedInt,
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
    /// SQL CASE blend (resolved): identical to the logical form — blends raw i64
    /// bit patterns, so no `signed` flag is needed (the branch producers already
    /// carry the correct value; §4's float unification is the lowering's job).
    Select {
        dst: u16,
        cond: u16,
        a: u16,
        b: u16,
    },
    LoadNull {
        dst: u16,
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
    /// Integer set membership: `dst = value_reg ∈ int_sets[set_idx]`. The pool is
    /// decoded once at `resolve` into `ResolvedProgram.int_sets` (sorted
    /// ascending, signed i64); `set_idx` indexes that vector, not the const pool.
    IntInSet {
        dst: u16,
        value_reg: u16,
        set_idx: u32,
    },
    /// Verbatim column copy into output payload slot `out`. `src` carries the
    /// PK-vs-payload distinction plus width/type — the one resolved-column
    /// record (`schema::ColumnLocator`) shared with the reduce/index paths.
    CopyCol {
        out: u32,
        src: ColumnLocator,
    },
    Emit {
        src: u16,
        out: u32,
    },
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
    /// Build from typed instructions. The compiler and test builders trust their
    /// own construction, so a validation failure here is a compiler bug, not
    /// client input — `validate` (structure only; no schema) panics rather than
    /// returns. This preserves the all-profiles register-valid / alias-free
    /// guarantee `reg3`/`reg4`'s raw split borrows depend on.
    pub fn new(instrs: Vec<LogicalInstr>, num_regs: u32, result_reg: u32, const_strings: Vec<Vec<u8>>) -> Self {
        Self::assembled(instrs, num_regs, result_reg, const_strings)
            .unwrap_or_else(|e| panic!("compiler-built LogicalProgram is invalid: {e:?}"))
    }

    /// Assemble the struct and run the structure-only `validate(None, None)` that
    /// upholds the all-profiles register-valid / alias-free invariant `reg3`/`reg4`
    /// rely on. Shared by `new` (which unwraps — a failure is a compiler bug) and
    /// `from_wire` (which propagates — a failure is bad client input).
    fn assembled(
        instrs: Vec<LogicalInstr>,
        num_regs: u32,
        result_reg: u32,
        const_strings: Vec<Vec<u8>>,
    ) -> Result<Self, ExprValidateErr> {
        let prog = LogicalProgram {
            instrs,
            num_regs,
            result_reg,
            const_strings,
        };
        prog.validate(None, None)?;
        Ok(prog)
    }

    /// A pure projection: `copies[i] = src_col` copies logical input column
    /// `src_col` into dense output payload slot `i`. The source type is derived in
    /// `resolve` from the schema. The register-free (`num_regs == 0`) shape a map
    /// consumer turns into verbatim column moves and nothing else. The one
    /// wire-free constructor a *production* caller uses — the circuit compiler
    /// builds projections with it; [`LogicalProgram::new`] is reached only from
    /// hand-written test programs.
    pub fn copy_cols(copies: &[u32]) -> Self {
        let instrs = copies
            .iter()
            .enumerate()
            .map(|(out, &src_col)| LogicalInstr::CopyCol {
                src_col,
                out: out as u32,
            })
            .collect();
        LogicalProgram::new(instrs, 0, 0, Vec::new())
    }

    /// Lower a wire expr blob (flat u32 quads `[op, dst, a1, a2]`) into the
    /// typed logical form. The single point that knows the wire encoding.
    /// Client-controlled: an unknown opcode or a structurally-invalid program
    /// (bad register, alias, const index) is rejected rather than panicked. The
    /// structure-only `validate(None, None)` preserves the all-profiles
    /// register-valid / alias-free invariant for every `from_wire` output.
    pub fn from_wire(
        code: &[u32],
        num_regs: u32,
        result_reg: u32,
        const_strings: Vec<Vec<u8>>,
    ) -> Result<Self, ExprValidateErr> {
        debug_assert_eq!(
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
                EXPR_LOAD_CONST => LogicalInstr::LoadConst {
                    dst,
                    val: gnitz_wire::decode_load_const(q[2], q[3]),
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
                EXPR_SELECT => {
                    let (sa, sb) = gnitz_wire::decode_select_operands(q[3]);
                    LogicalInstr::Select {
                        dst,
                        cond: q[2] as u16,
                        a: sa,
                        b: sb,
                    }
                }
                EXPR_LOAD_NULL => LogicalInstr::LoadNull { dst },
                EXPR_COPY_COL => LogicalInstr::CopyCol {
                    src_col: q[2],
                    out: q[3],
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
                // `value_reg` rides the `a` slot (`q[2] as u16`); `set_idx` takes
                // the full `q[3]` u32 const index, never truncated to u16.
                EXPR_INT_IN_SET => LogicalInstr::IntInSet {
                    dst,
                    value_reg: a,
                    set_idx: q[3],
                },
                _ => return Err(ExprValidateErr::UnknownOpcode(op)),
            });
        }
        Self::assembled(instrs, num_regs, result_reg, const_strings)
    }

    /// If every instruction is `CopyCol` writing dense payload outputs
    /// `out = [0, 1, 2, …]` (in instruction order), return the copies' source
    /// columns — the program's payload copy list, from which a caller derives a
    /// reindex MAP's output payload schema. `None` for any other shape (a compute
    /// instruction, or a permuted/offset destination).
    pub fn payload_copy_srcs(&self) -> Option<Vec<u32>> {
        self.instrs
            .iter()
            .enumerate()
            .map(|(i, instr)| match *instr {
                LogicalInstr::CopyCol { src_col, out } if out == i as u32 => Some(src_col),
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
    pub fn sequential_copy_base(&self) -> Option<usize> {
        let srcs = self.payload_copy_srcs()?;
        let &base = srcs.first()?;
        let ok = srcs.iter().enumerate().all(|(i, &s)| s == base + i as u32);
        ok.then_some(base as usize)
    }

    /// Lower to the resolved form, then run the three one-shot analyses over the
    /// resolved instruction stream — nullability, register roles, AND-chain — for
    /// the given context (`is_filter = true` keeps `result_reg` eligible for
    /// bit_only). Consuming: a `Vec<LogicalInstr>` cannot be mutated in place into
    /// a `Vec<Instr>`. Preserves the per-register U64 signed→unsigned tracking.
    pub(crate) fn resolve_program(self, schema: &dyn SchemaFacts, is_filter: bool) -> ResolvedProgram {
        use gnitz_wire::type_code;
        use Instr as I;
        use LogicalInstr as L;
        // Does this register currently hold a U64 value? That is the whole
        // question the per-register tracking answers: it drives every
        // signed→unsigned variant selection, because a U64 >= 2^63 has a
        // negative i64 bit pattern. Unknown counts as not-U64, i.e. signed.
        let mut reg_u64 = [false; MAX_REGS];
        // Payload slot of a payload-only opcode's column operand. `validate`'s
        // `ColKind::payload_only` rule rejects a PK column here and callers
        // validate before resolving, so the `None` arm
        // is unreachable for any program that ever reaches a batch; it keeps the
        // sentinel so an unvalidated program (tests) trips the kernels' own
        // assertions instead of silently addressing payload slot 0.
        let payload_slot = |ci: usize| schema.payload_slot(ci).unwrap_or(crate::PAYLOAD_MAPPING_PK_SENTINEL);
        let mut instrs = Vec::with_capacity(self.instrs.len());
        // Decoded `INT_IN_SET` pools, indexed by the resolved `set_idx`. Decoded
        // once here (never per row); each `IntInSet` re-points its `set_idx` at
        // its slot in this vector.
        let mut int_sets: Vec<Vec<i64>> = Vec::new();
        for li in self.instrs {
            match li {
                L::LoadColInt { dst, col } => {
                    // One query: the locator already carries the type code the
                    // per-register tracking wants, so asking `col_type_code`
                    // too would make the two answers a divergence risk.
                    let loc = schema.locate(col as usize);
                    // Total on a validated program: `validate` runs
                    // `check_col(.., ColKind::FixedInt)` on every `LoadColInt`,
                    // and that predicate is `gnitz_wire::is_fixed_int` — the same
                    // eight codes `from_type_code` answers `Some` for. It covers
                    // the PK arm too (`ColKind::FixedInt` is not payload-only, so
                    // a U128/UUID PK is rejected as `ColKindMismatch`), which is
                    // what makes the kernel's wide-column wildcard unnecessary.
                    let fi = FixedInt::from_type_code(TypeCode::from_validated_u8(loc.type_code()))
                        .expect("validated LoadColInt names a fixed-int column");
                    instrs.push(match loc {
                        ColumnLocator::Pk { byte_off, .. } => I::LoadPk { dst, off: byte_off, fi },
                        ColumnLocator::Payload { slot, .. } => I::LoadPayloadInt { dst, pi: slot, fi },
                    });
                    reg_u64[dst as usize] = loc.type_code() == type_code::U64;
                }
                L::LoadColFloat { dst, col } => {
                    // Total on a validated program, the same shape as the
                    // `LoadColInt` arm above: `validate` runs
                    // `check_col(.., ColKind::Float)`, so the column is F32 or F64
                    // and nothing else. Stated positively so an unvalidated
                    // program panics here rather than reading 8 bytes out of a
                    // 4-byte region.
                    let wide = match schema.col_type_code(col as usize) {
                        type_code::F64 => true,
                        type_code::F32 => false,
                        other => unreachable!("validated LoadColFloat names F32/F64, got {other}"),
                    };
                    instrs.push(I::LoadPayloadFloat {
                        dst,
                        pi: payload_slot(col as usize),
                        wide,
                    });
                    reg_u64[dst as usize] = false;
                }
                L::LoadConst { dst, val } => {
                    instrs.push(I::LoadConst { dst, val });
                    reg_u64[dst as usize] = false;
                }
                L::IntAdd { dst, a, b } => {
                    instrs.push(I::IntAdd { dst, a, b });
                    reg_u64[dst as usize] = reg_u64[a as usize] || reg_u64[b as usize];
                }
                L::IntSub { dst, a, b } => {
                    instrs.push(I::IntSub { dst, a, b });
                    reg_u64[dst as usize] = reg_u64[a as usize] || reg_u64[b as usize];
                }
                L::IntMul { dst, a, b } => {
                    instrs.push(I::IntMul { dst, a, b });
                    reg_u64[dst as usize] = reg_u64[a as usize] || reg_u64[b as usize];
                }
                L::IntDiv { dst, a, b } => {
                    let u = reg_u64[a as usize] || reg_u64[b as usize];
                    instrs.push(I::IntDiv { dst, a, b, signed: !u });
                    reg_u64[dst as usize] = u;
                }
                L::IntMod { dst, a, b } => {
                    let u = reg_u64[a as usize] || reg_u64[b as usize];
                    instrs.push(I::IntMod { dst, a, b, signed: !u });
                    reg_u64[dst as usize] = u;
                }
                L::IntNeg { dst, a } => {
                    instrs.push(I::IntNeg { dst, a });
                    reg_u64[dst as usize] = reg_u64[a as usize];
                }
                L::FloatAdd { dst, a, b } => instrs.push(I::FloatAdd { dst, a, b }),
                L::FloatSub { dst, a, b } => instrs.push(I::FloatSub { dst, a, b }),
                L::FloatMul { dst, a, b } => instrs.push(I::FloatMul { dst, a, b }),
                L::FloatDiv { dst, a, b } => instrs.push(I::FloatDiv { dst, a, b }),
                L::FloatNeg { dst, a } => instrs.push(I::FloatNeg { dst, a }),
                L::Cmp { op, dst, a, b } => {
                    // EQ/NE are bit-identical signed/unsigned; ordered compares
                    // pick the unsigned form when either operand is U64.
                    let signed = matches!(op, CmpOp::Eq | CmpOp::Ne) || !(reg_u64[a as usize] || reg_u64[b as usize]);
                    instrs.push(I::Cmp { op, dst, a, b, signed });
                    reg_u64[dst as usize] = false;
                }
                L::FCmp { op, dst, a, b } => instrs.push(I::FCmp { op, dst, a, b }),
                L::IntToFloat { dst, a } => {
                    let signed = !reg_u64[a as usize];
                    instrs.push(I::IntToFloat { dst, a, signed });
                    reg_u64[dst as usize] = false;
                }
                L::Select { dst, cond, a, b } => {
                    // U64-ness flows through Select exactly as through IntAdd: the
                    // result is U64 if *either* branch is U64, so a downstream
                    // ordered compare / div / int_to_float on the CASE result picks
                    // the unsigned variant and values >= 2^63 order correctly.
                    instrs.push(I::Select { dst, cond, a, b });
                    reg_u64[dst as usize] = reg_u64[a as usize] || reg_u64[b as usize];
                }
                L::LoadNull { dst } => {
                    instrs.push(I::LoadNull { dst });
                    reg_u64[dst as usize] = false;
                }
                L::BoolAnd { dst, a, b } => instrs.push(I::BoolAnd { dst, a, b }),
                L::BoolOr { dst, a, b } => instrs.push(I::BoolOr { dst, a, b }),
                L::BoolNot { dst, a } => instrs.push(I::BoolNot { dst, a }),
                L::IsNull { dst, col } => instrs.push(I::IsNull {
                    dst,
                    pi: payload_slot(col as usize),
                }),
                L::IsNotNull { dst, col } => instrs.push(I::IsNotNull {
                    dst,
                    pi: payload_slot(col as usize),
                }),
                L::StrColConst {
                    op,
                    dst,
                    col,
                    const_idx,
                } => instrs.push(I::StrColConst {
                    op,
                    dst,
                    pi: payload_slot(col as usize),
                    const_idx,
                }),
                L::StrColCol { op, dst, col_a, col_b } => instrs.push(I::StrColCol {
                    op,
                    dst,
                    pi_a: payload_slot(col_a as usize),
                    pi_b: payload_slot(col_b as usize),
                }),
                L::IntInSet {
                    dst,
                    value_reg,
                    set_idx,
                } => {
                    // Decode the packed pool once, here — never per row. Immutable
                    // read (not `mem::take`): `resolve` runs on client-controlled
                    // wire input, and `validate` does not enforce one-const-index-
                    // per-opcode, so a blob that shares an index between two
                    // opcodes must stay inert, not corrupt the other's slot.
                    let mut set: Vec<i64> = self.const_strings[set_idx as usize]
                        .chunks_exact(8)
                        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
                        .collect();
                    // The kernel binary-searches this pool, so ascending order is a
                    // correctness precondition. Establish it here rather than trust
                    // the client to have sorted it: set membership does not depend on
                    // order, so sorting a skewed pool is always right, where rejecting
                    // it would only turn a wrong answer into an error. Once per
                    // compile; for an honest client the pool is already sorted.
                    set.sort_unstable();
                    let new_idx = int_sets.len() as u32;
                    int_sets.push(set);
                    instrs.push(I::IntInSet {
                        dst,
                        value_reg,
                        set_idx: new_idx,
                    });
                }
                L::CopyCol { src_col, out } => {
                    // The source's location, width, and type — dropped from the wire
                    // `CopyCol` — resolve to the one canonical record.
                    instrs.push(I::CopyCol {
                        out,
                        src: schema.locate(src_col as usize),
                    });
                }
                L::Emit { src, out } => instrs.push(I::Emit { src, out }),
            }
        }
        // Encode each string constant once into a 16-byte German-string cell
        // over one shared const blob, so the str-vs-const eval arm compares it
        // through `gnitz_wire::compare_german_strings` — no per-morsel
        // re-derivation and no parallel prefix/length tables.
        let mut const_blob: Vec<u8> = Vec::new();
        let const_cells: Vec<[u8; 16]> = self
            .const_strings
            .iter()
            .map(|s| encode_german_string(s, &mut const_blob))
            .collect();
        // Three independent passes over `instrs`, so the struct is built once,
        // fully resolved — there is no moment where a mask field is a placeholder.
        let RegisterRoles {
            bit_only,
            bool_input,
            use_count,
        } = classify_registers(&instrs, self.result_reg, is_filter);
        ResolvedProgram {
            no_nulls: is_strictly_non_nullable(&instrs, schema),
            bit_only_mask: bit_only,
            bool_pack_mask: bit_only | bool_input,
            chain_trigger_mask: and_chain_mask(&instrs, self.result_reg, is_filter, &use_count),
            instrs,
            num_regs: self.num_regs,
            result_reg: self.result_reg,
            const_cells,
            const_blob,
            int_sets,
        }
    }

    /// Validate as a filter predicate: the schema-aware pass, plus the one rule
    /// only a filter has — it must own a result register. `num_regs == 0` is
    /// legitimate for a map (`copy_cols` builds exactly that shape), so
    /// [`Self::validate`] cannot reject it; as a *filter* it is a corrupt frame
    /// with no result to read, not a filter that passes nothing.
    pub(crate) fn validate_predicate(&self, schema: &dyn SchemaFacts) -> Result<(), ExprValidateErr> {
        self.validate(Some(schema), None)?;
        if self.num_regs == 0 {
            return Err(ExprValidateErr::PredicateWithoutResultReg);
        }
        Ok(())
    }

    /// Validate every value a client-authored program controls that reaches a
    /// panicking / OOB / truncating site, in one exhaustive `match self` (a new
    /// opcode cannot silently bypass a bound). `in_schema` / `out_schema` are
    /// `None` for the structure-only pass (register-file limits, SSA
    /// anti-aliasing, const-pool index) `new`/`from_wire` run before any schema
    /// exists; passing a schema additionally validates the column / output
    /// operands the blob-derived resolve entries index. `None` schemas skip the
    /// corresponding checks — a filter (no output plan) passes `out_schema =
    /// None`, so output opcodes (eval no-ops there) are not checked.
    ///
    /// With an `out_schema` this is also where **output coverage** is decided:
    /// every declared payload slot must be written by exactly one `CopyCol` or
    /// `Emit`. Map output batches are provisioned uninitialized, so an unwritten
    /// slot ships recycled bytes rather than a zero. Checked by popcount rather
    /// than by length, so a duplicate destination is caught too (it leaves
    /// another slot's bit clear).
    pub(crate) fn validate(
        &self,
        in_schema: Option<&dyn SchemaFacts>,
        out_schema: Option<&dyn SchemaFacts>,
    ) -> Result<(), ExprValidateErr> {
        use ExprValidateErr as E;
        use LogicalInstr as L;
        // Bit per written output payload slot; `check_out` bounds every `out`
        // below `num_payload_cols() <= 64`, so no shift can overflow.
        let mut written = 0u64;
        let num_regs = self.num_regs;
        if num_regs as usize > MAX_REGS {
            return Err(E::TooManyRegs(num_regs));
        }
        if num_regs != 0 && self.result_reg >= num_regs {
            return Err(E::ResultRegOutOfRange {
                result_reg: self.result_reg,
                num_regs,
            });
        }
        // Bound `out` against the output payload width and record it as written
        // (output opcodes only).
        let mut check_out = |out: u32| -> Result<(), E> {
            if let Some(os) = out_schema {
                if out as usize >= os.num_payload_cols() {
                    return Err(E::OutputIdxOutOfRange {
                        out,
                        num_payload_cols: os.num_payload_cols(),
                    });
                }
                written |= 1u64 << out;
            }
            Ok(())
        };
        for instr in &self.instrs {
            match *instr {
                // Binary register ops (the 13 opcodes `reg3` splits): bound every
                // register operand, then SSA anti-aliasing (dst ≠ a, dst ≠ b).
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
                    check_reg(dst, num_regs)?;
                    check_reg(a, num_regs)?;
                    check_reg(b, num_regs)?;
                    if dst == a || dst == b {
                        return Err(E::RegisterAliasing { dst, a, b });
                    }
                }
                // Ternary select (`reg4`'s raw split): dst distinct from every source.
                L::Select { dst, cond, a, b } => {
                    check_reg(dst, num_regs)?;
                    check_reg(cond, num_regs)?;
                    check_reg(a, num_regs)?;
                    check_reg(b, num_regs)?;
                    if dst == cond || dst == a || dst == b {
                        return Err(E::RegisterAliasing { dst, a, b });
                    }
                }
                // Unary register readers that write dst.
                L::IntNeg { dst, a } | L::FloatNeg { dst, a } | L::IntToFloat { dst, a } | L::BoolNot { dst, a } => {
                    check_reg(dst, num_regs)?;
                    check_reg(a, num_regs)?;
                }
                // EMIT reads a source register and writes an output payload slot.
                L::Emit { src, out } => {
                    check_reg(src, num_regs)?;
                    check_out(out)?;
                    check_emit_slot(out_schema, out)?;
                }
                // LoadColInt: the payload and PK integer load kernels have arms only
                // for the eight fixed-width integer types. A float column has a
                // loadable *width* but not a loadable *type* (its bits would be
                // reinterpreted as an integer), and an unknown type code reports
                // width 8 and would slip through a width test.
                L::LoadColInt { dst, col } => {
                    check_reg(dst, num_regs)?;
                    check_col(in_schema, col, ColKind::FixedInt)?;
                }
                // LoadColFloat: the float load kernel branches on width alone, so a
                // 1- or 2-byte integer column makes it slice an 8-byte stride out of
                // a narrower region.
                L::LoadColFloat { dst, col } => {
                    check_reg(dst, num_regs)?;
                    check_col(in_schema, col, ColKind::Float)?;
                }
                L::IsNull { dst, col } | L::IsNotNull { dst, col } => {
                    check_reg(dst, num_regs)?;
                    check_col(in_schema, col, ColKind::AnyPayload)?;
                }
                L::StrColConst {
                    dst, col, const_idx, ..
                } => {
                    check_reg(dst, num_regs)?;
                    if const_idx as usize >= self.const_strings.len() {
                        return Err(E::ConstIdxOutOfRange {
                            const_idx,
                            n: self.const_strings.len(),
                        });
                    }
                    // The compare reads 16-byte German-string cells; a narrower
                    // column makes `col_data(pi, 16)` over-read its region.
                    check_col(in_schema, col, ColKind::GermanString)?;
                }
                // Set membership over the register `value_reg`; `set_idx` is a
                // const-pool index whose entry must be a whole number of 8-byte
                // i64s (the `len % 8` check turns a truncating pool entry into a
                // clean `Rejected` instead of a silent `chunks_exact` tail-drop).
                L::IntInSet {
                    dst,
                    value_reg,
                    set_idx,
                } => {
                    check_reg(value_reg, num_regs)?;
                    check_reg(dst, num_regs)?;
                    if set_idx as usize >= self.const_strings.len() {
                        return Err(E::ConstIdxOutOfRange {
                            const_idx: set_idx,
                            n: self.const_strings.len(),
                        });
                    }
                    let len = self.const_strings[set_idx as usize].len();
                    if !len.is_multiple_of(8) {
                        return Err(E::IntSetNotAligned { set_idx, len });
                    }
                }
                L::StrColCol { dst, col_a, col_b, .. } => {
                    check_reg(dst, num_regs)?;
                    check_col(in_schema, col_a, ColKind::GermanString)?;
                    check_col(in_schema, col_b, ColKind::GermanString)?;
                }
                // dst-writers whose other operands are data / none.
                L::LoadConst { dst, .. } | L::LoadNull { dst } => check_reg(dst, num_regs)?,
                // CopyCol: any (payload or PK) source column, one output payload
                // slot that can hold it.
                L::CopyCol { src_col, out } => {
                    check_col(in_schema, src_col, ColKind::AnyCol)?;
                    check_out(out)?;
                    check_copy_types(in_schema, out_schema, src_col, out)?;
                }
            }
        }
        if let Some(os) = out_schema {
            if written.count_ones() as usize != os.num_payload_cols() {
                return Err(E::OutputSlotUnwritten {
                    written,
                    num_payload_cols: os.num_payload_cols(),
                });
            }
        }
        Ok(())
    }
}

/// A register operand is in range iff `< num_regs`.
fn check_reg(r: u16, num_regs: u32) -> Result<(), ExprValidateErr> {
    if (r as u32) < num_regs {
        Ok(())
    } else {
        Err(ExprValidateErr::RegOutOfRange { reg: r, num_regs })
    }
}

/// The one column-operand check: range, then payload-ness, then the type class
/// the opcode's kernel can decode — in that order, so a stronger requirement can
/// never be tested against an unbounded index. Skipped entirely when no
/// `in_schema` is supplied (the structure-only pass).
///
/// The bound is `num_columns()`, not `MAX_COLUMNS`: the `[num_columns, 65)` zone
/// reads a zeroed schema slot. The kernels dispatch on a column's type without
/// re-checking it, so this is the only place a client blob is held to the
/// contract.
fn check_col(in_schema: Option<&dyn SchemaFacts>, col: u32, need: ColKind) -> Result<(), ExprValidateErr> {
    let Some(s) = in_schema else { return Ok(()) };
    if col as usize >= s.num_columns() {
        return Err(ExprValidateErr::ColOutOfRange {
            col,
            num_columns: s.num_columns(),
        });
    }
    if need.payload_only() && s.is_pk_col(col as usize) {
        return Err(ExprValidateErr::ColNotPayload { col });
    }
    let type_code = s.col_type_code(col as usize);
    let ok = match need {
        ColKind::AnyCol | ColKind::AnyPayload => true,
        ColKind::FixedInt => gnitz_wire::is_fixed_int(type_code),
        ColKind::Float => gnitz_wire::is_float(type_code),
        ColKind::GermanString => gnitz_wire::is_german_string(type_code),
    };
    if ok {
        Ok(())
    } else {
        Err(ExprValidateErr::ColKindMismatch {
            col,
            type_code,
            want: need,
        })
    }
}

/// A COPY_COL destination slot must hold its source verbatim. `copy_column`
/// byte-copies at equal width and otherwise `widen_native_le`s a narrower integer
/// into a wider slot — there is no narrowing and no representation change.
fn check_copy_types(
    in_schema: Option<&dyn SchemaFacts>,
    out_schema: Option<&dyn SchemaFacts>,
    src_col: u32,
    out: u32,
) -> Result<(), ExprValidateErr> {
    let (Some(is), Some(os)) = (in_schema, out_schema) else {
        return Ok(());
    };
    let src_tc = is.col_type_code(src_col as usize);
    let out_tc = os.col_type_code(os.payload_col_idx(out as usize));
    let ok = src_tc == out_tc || gnitz_wire::is_widening_promotion(src_tc, out_tc);
    if ok {
        Ok(())
    } else {
        Err(ExprValidateErr::CopyTypeMismatch {
            col: src_col,
            src_tc,
            out,
            out_tc,
        })
    }
}

/// EMIT stores a whole 8-byte register image, so its destination slot is 8 bytes
/// — no narrowing (which truncates an i64 and shears an f64) and no widening
/// (which runs off the end of `to_le_bytes()`). A stride rule, not a type rule:
/// `I64`, `U64` and `F64` are all legal targets and `validate` cannot tell which
/// the register holds. Both tests are needed and their order does not matter:
/// `wire_stride` reports 8 for an undecodable type code, so the width test alone
/// would admit one.
fn check_emit_slot(out_schema: Option<&dyn SchemaFacts>, out: u32) -> Result<(), ExprValidateErr> {
    let Some(os) = out_schema else { return Ok(()) };
    // `col_type_code`, like its two sibling checks — not `locate`, whose extra
    // work (a release-active bound assert, plus an O(pk_count) OPK-offset walk
    // for a PK column) buys nothing here: `size()` IS `wire_stride(type_code)`.
    let type_code = os.col_type_code(os.payload_col_idx(out as usize));
    if !gnitz_wire::is_valid_type_code(type_code) || gnitz_wire::wire_stride(type_code) != 8 {
        return Err(ExprValidateErr::EmitSlotNotEightBytes { out, type_code });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ResolvedProgram — the evaluable form
// ---------------------------------------------------------------------------

pub(crate) struct ResolvedProgram {
    pub(crate) instrs: Vec<Instr>,
    pub(crate) num_regs: u32,
    /// The register holding the filter verdict. Filter-only state: a map's is
    /// meaningless (both map construction sites hardcode 0 into the wire field)
    /// and never read — the only consumers are the `is_filter` arms below and
    /// the filter entry points, and [`LogicalProgram::validate_predicate`]
    /// rejects a register-free program so a filter's is always in range.
    pub(crate) result_reg: u32,
    /// Per-constant 16-byte German-string cell (indexed by `const_idx`) over
    /// the one shared `const_blob` — encoded once at resolve, compared by
    /// `gnitz_wire::compare_german_strings`.
    pub(crate) const_cells: Vec<[u8; 16]>,
    pub(crate) const_blob: Vec<u8>,
    /// Decoded `INT_IN_SET` value pools, indexed by the resolved `set_idx`. Each
    /// pool is sorted ascending in signed-i64 `Ord` by `resolve` — the wire order
    /// is not trusted — so `eval_batch` binary-searches it directly. Duplicates
    /// are left in place; `binary_search` is correct over them.
    pub(crate) int_sets: Vec<Vec<i64>>,
    /// True iff no instruction can produce a NULL against the schema this program
    /// was resolved against, so the evaluator skips null-bit tracking entirely.
    /// Resolved once — the answer is only meaningful for that one schema, since
    /// the payload indices in `instrs` were assigned from it.
    pub(crate) no_nulls: bool,
    /// Bit `r` set iff register `r` is only consumed by boolean ops, so its
    /// producer can skip the i64 unpack into `regs[r]`.
    bit_only_mask: u64,
    /// Bit `r` set iff `r`'s producer must write `bool_bits[r]` — either a
    /// downstream boolean consumer reads it, or `r` is bit_only and the filter
    /// reads `bool_bits[result_reg]` directly. Stored as the union rather than
    /// its two halves: `needs_bool_pack` is the only reader and runs per
    /// instruction per morsel.
    bool_pack_mask: u64,
    /// Destination registers of the non-terminal ANDs in the one result-terminal
    /// AND chain: bit `r` set means "if the AND writing register `r` is all
    /// definite-FALSE for the morsel, the filter result is too — write the terminal
    /// (`result_reg`) all-FALSE and stop". 0 for non-filter programs and programs
    /// with no such chain. Every register is `< num_regs ≤ 64` (asserted in
    /// `LogicalProgram::new`), so a u64 indexed by register suffices.
    pub(crate) chain_trigger_mask: u64,
}

impl ResolvedProgram {
    /// Per instruction per morsel, from the BOOL arms of `eval_batch`.
    #[inline(always)]
    pub(crate) fn is_bit_only(&self, reg: usize) -> bool {
        (self.bit_only_mask >> reg) & 1 != 0
    }

    /// True iff `reg`'s producer must write `bool_bits[reg]`. Per instruction
    /// per morsel, from `maybe_pack_bool_bits`.
    #[inline(always)]
    pub(crate) fn needs_bool_pack(&self, reg: usize) -> bool {
        (self.bool_pack_mask >> reg) & 1 != 0
    }
}

/// Detect the one result-terminal AND chain and return, as `chain_trigger_mask`,
/// the destination registers of its non-terminal ANDs. At runtime, when such an
/// AND is all definite-FALSE for a morsel, `eval_batch` writes the terminal
/// (`result_reg`) all-FALSE and breaks (see the `BoolAnd` nullable arm). Only
/// the accumulator spine is walked, so an inner AND reached through a
/// `BoolNot`/`BoolOr` operand is never marked — forcing FALSE under those would
/// be a miscompile.
///
/// `use_count` comes from [`classify_registers`], which already visits every
/// register read: a second exhaustive walk could silently under-count an opcode
/// that gained an operand, and an under-counted register reads as a clean chain
/// link when it is not.
fn and_chain_mask(instrs: &[Instr], result_reg: u32, is_filter: bool, use_count: &[u8; MAX_REGS]) -> u64 {
    let n = instrs.len();
    // Filter-only; need ≥ 3 instrs for a ≥ 2-AND chain. A filter has one
    // register per instruction, so n == num_regs ≤ MAX_REGS (asserted),
    // keeping `pc as u8` and the register-indexed scratch array in range.
    if !is_filter || !(3..=MAX_REGS).contains(&n) {
        return 0;
    }
    // Terminal = last instruction = expression root; must be an AND on result_reg.
    let Instr::BoolAnd {
        dst: term_dst,
        a: term_a,
        b: term_b,
    } = instrs[n - 1]
    else {
        return 0;
    };
    if term_dst as u32 != result_reg {
        return 0;
    }
    // Single-assignment (alloc_reg never reuses): each register has one writer.
    // Track only AND writers — a spine link must be an AND, so a non-MAX slot
    // already means "written by an AND".
    let mut and_writer = [u8::MAX; MAX_REGS];
    for (pc, ins) in instrs.iter().enumerate() {
        if let Instr::BoolAnd { dst, .. } = *ins {
            and_writer[dst as usize] = pc as u8;
        }
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
        let Instr::BoolAnd { a: na, b: nb, .. } = instrs[w] else {
            break;
        };
        a = na;
        b = nb;
    }
    mask
}

/// What [`classify_registers`] derives in its one pass over the instruction
/// stream.
struct RegisterRoles {
    /// Bit `r` set iff `r` is only consumed by boolean ops, so its producer can
    /// skip the i64 unpack into `regs[r]`.
    bit_only: u64,
    /// Bit `r` set iff a boolean consumer reads `r`, so its producer must
    /// populate `bool_bits[r]`.
    bool_input: u64,
    /// How many instructions read each register, saturating at 255.
    use_count: [u8; MAX_REGS],
}

/// Classify each register's role on the nullable-arm hot path, and count every
/// register read while doing it. A filter's `result_reg` is forced to be a bool
/// input (the filter fast path reads `bool_bits` of it directly). A map has no
/// result register to force — its only register consumer is `Emit`, which
/// already marks its source non-bool.
///
/// This is the **one** exhaustive walk over register operands: `use_count` is
/// accumulated here rather than by a second visitor, so an opcode that gains an
/// operand cannot be classified correctly and counted wrong.
///
/// Every `1u64 << reg` below is in range: `LogicalProgram::new` asserts
/// `num_regs <= MAX_REGS` and bounds every register operand by `num_regs`.
fn classify_registers(instrs: &[Instr], result_reg: u32, is_filter: bool) -> RegisterRoles {
    use Instr::*;
    let mut bool_produced: u64 = 0;
    let mut non_bool_read: u64 = 0;
    let mut bool_input: u64 = 0;
    let mut use_count = [0u8; MAX_REGS];
    // Record each register read and OR it into the mask naming how it is read.
    macro_rules! read {
        ($mask:ident, $($r:expr),+) => {{ $(
            use_count[$r as usize] = use_count[$r as usize].saturating_add(1);
            $mask |= 1u64 << $r;
        )+ }};
    }
    for instr in instrs {
        match *instr {
            // Bool producers that are also binary register readers.
            Cmp { dst, a, b, .. } | FCmp { dst, a, b, .. } => {
                bool_produced |= 1u64 << dst;
                read!(non_bool_read, a, b);
            }
            // Bool producer reading one integer register (its value operand).
            IntInSet { dst, value_reg, .. } => {
                bool_produced |= 1u64 << dst;
                read!(non_bool_read, value_reg);
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
                read!(non_bool_read, a, b);
            }
            // Bool producers whose operands are payload columns, not regs.
            StrColConst { dst, .. } | StrColCol { dst, .. } | IsNull { dst, .. } | IsNotNull { dst, .. } => {
                bool_produced |= 1u64 << dst;
            }
            // Binary bool consumers: producer + bool_input (not non_bool_read).
            BoolAnd { dst, a, b } | BoolOr { dst, a, b } => {
                bool_produced |= 1u64 << dst;
                read!(bool_input, a, b);
            }
            // Unary bool consumer.
            BoolNot { dst, a } => {
                bool_produced |= 1u64 << dst;
                read!(bool_input, a);
            }
            // Unary register readers (non-bool).
            IntNeg { a, .. } | FloatNeg { a, .. } | IntToFloat { a, .. } => {
                read!(non_bool_read, a);
            }
            // Ternary select: `cond` is read as a boolean (its producer must
            // pack `bool_bits`), `a`/`b` as values. `dst` carries a value, so
            // it is deliberately in neither set — keeping it out of
            // `bool_produced` keeps it out of `bit_only`.
            Select { cond, a, b, .. } => {
                read!(bool_input, cond);
                read!(non_bool_read, a, b);
            }
            Emit { src, .. } => {
                read!(non_bool_read, src);
            }
            // No register reads / not bool.
            LoadPayloadInt { .. }
            | LoadPayloadFloat { .. }
            | LoadPk { .. }
            | LoadConst { .. }
            | LoadNull { .. }
            | CopyCol { .. } => {}
        }
    }
    if is_filter {
        // The filter's nullable arm consumes the result as packed bits
        // (a word-level `bool_bits & !null_bits` merge), so the result
        // producer must populate `bool_bits` whatever opcode it is —
        // marking it a bool input routes every non-bool producer through
        // `maybe_pack_bool_bits`.
        bool_input |= 1u64 << result_reg as usize;
    }
    RegisterRoles {
        bit_only: bool_produced & !non_bool_read,
        bool_input,
        use_count,
    }
}

/// Returns true if no instruction in `instrs` can produce a NULL result (so the
/// evaluator can skip null-bit tracking entirely). Called once, from `resolve`,
/// against the schema the program was resolved against — the only schema for
/// which the answer means anything (`pi` operands come from it).
fn is_strictly_non_nullable(instrs: &[Instr], schema: &dyn SchemaFacts) -> bool {
    use Instr::*;
    let nullable_payload = |pi: u8| -> bool {
        let a = pi as usize;
        a < schema.num_payload_cols() && schema.col_nullable(schema.payload_col_idx(a))
    };
    for instr in instrs {
        match *instr {
                // Division/modulo produce NULL on a zero divisor.
                IntDiv { .. } | IntMod { .. } | FloatDiv { .. } => return false,
                // IS_NULL / IS_NOT_NULL read the batch null bits.
                IsNull { .. } | IsNotNull { .. } => return false,
                // LoadNull manufactures a NULL for every row — forces the nullable path.
                LoadNull { .. } => return false,
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
                // Set membership introduces no NULL beyond its input register; a
                // nullable source column is already disqualified at its `Load`.
                | IntInSet { .. }
                | IntToFloat { .. }
                // Select only copies branch values — any NULL a branch can
                // produce is already accounted for by that branch's own producer
                // (a nullable branch forces `no_nulls` off there), so Select adds
                // no NULL of its own. LoadNull is handled above (returns false).
                | Select { .. }
                | BoolAnd { .. }
                | BoolOr { .. }
                | BoolNot { .. }
                | CopyCol { .. }
                | Emit { .. } => {}
        }
    }
    true
}

#[cfg(test)]
mod tests;
