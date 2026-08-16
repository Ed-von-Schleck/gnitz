//! Compiled scalar-expression programs.
//!
//! Two typed forms: `LogicalProgram` (`LogicalInstr`, logical column indices —
//! the shape the wire blob lowers into) and `ResolvedProgram` (`Instr`, resolved
//! payload/PK indices — the evaluable form). `LogicalProgram::resolve` consumes
//! the former and produces the latter. Instruction meaning is carried by the
//! type: a missing or mis-routed opcode is a compile error, not a silent
//! miscompute.

use crate::like::LikeMatcher;
use crate::{ColumnLocator, SchemaFacts};
use gnitz_wire::{encode_german_string, FixedInt, TrimMode, TypeCode};
use std::fmt;
// Wire opcodes (1–46) the client emits, matched as arms in `from_wire`. They are
// `pub const … : u32` in gnitz-wire, so a plain `use` binds them for pattern use.
use gnitz_wire::{
    EXPR_BOOL_AND, EXPR_BOOL_NOT, EXPR_BOOL_OR, EXPR_CMP_EQ, EXPR_CMP_GE, EXPR_CMP_GT, EXPR_CMP_LE, EXPR_CMP_LT,
    EXPR_CMP_NE, EXPR_COPY_COL, EXPR_EMIT, EXPR_FCMP_EQ, EXPR_FCMP_GE, EXPR_FCMP_GT, EXPR_FCMP_LE, EXPR_FCMP_LT,
    EXPR_FCMP_NE, EXPR_FLOAT_ABS, EXPR_FLOAT_ADD, EXPR_FLOAT_CEIL, EXPR_FLOAT_DIV, EXPR_FLOAT_FLOOR, EXPR_FLOAT_MAX2,
    EXPR_FLOAT_MIN2, EXPR_FLOAT_MUL, EXPR_FLOAT_NEG, EXPR_FLOAT_ROUND, EXPR_FLOAT_SUB, EXPR_FLOAT_TO_F32,
    EXPR_FLOAT_TO_INT, EXPR_FLOAT_TO_STR, EXPR_FLOAT_TRUNC, EXPR_INT_ABS, EXPR_INT_ADD, EXPR_INT_CAST, EXPR_INT_DIV,
    EXPR_INT_IN_SET, EXPR_INT_MAX2, EXPR_INT_MIN2, EXPR_INT_MOD, EXPR_INT_MUL, EXPR_INT_NEG, EXPR_INT_SUB,
    EXPR_INT_TO_FLOAT, EXPR_INT_TO_STR, EXPR_IS_NOT_NULL, EXPR_IS_NULL, EXPR_LOAD_COL_FLOAT, EXPR_LOAD_COL_INT,
    EXPR_LOAD_COL_STR, EXPR_LOAD_CONST, EXPR_LOAD_CONST_STR, EXPR_LOAD_NULL, EXPR_LOAD_NULL_STR, EXPR_SELECT,
    EXPR_STR_CMP_EQ, EXPR_STR_CMP_LE, EXPR_STR_CMP_LT, EXPR_STR_COL_EQ_COL, EXPR_STR_COL_EQ_CONST, EXPR_STR_COL_LE_COL,
    EXPR_STR_COL_LE_CONST, EXPR_STR_COL_LT_COL, EXPR_STR_COL_LT_CONST, EXPR_STR_CONCAT, EXPR_STR_CONCAT_NN,
    EXPR_STR_ILIKE, EXPR_STR_LEN_BYTES, EXPR_STR_LEN_CHARS, EXPR_STR_LIKE, EXPR_STR_LOWER, EXPR_STR_SELECT,
    EXPR_STR_SUBSTR, EXPR_STR_TO_FLOAT, EXPR_STR_TO_INT, EXPR_STR_TRIM, EXPR_STR_UPPER,
};

/// The register file is capped at 64: the BOOL_AND/BOOL_OR 3VL paths, the
/// null-bit propagation, and every register-indexed mask — `bit_only_mask` and
/// `bool_pack_mask` on the resolved program, `written_regs` and `str_class` in
/// `validate` — address registers by bit in a `u64`.
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
    RegisterAliasing { dst: u16, reg: u16 },
    RegRewrite { reg: u16 },
    RegClassMismatch { reg: u16 },
    ConstIdxOutOfRange { const_idx: u32, n: usize },
    IntSetNotAligned { set_idx: u32, len: usize },
    ColOutOfRange { col: u32, num_columns: usize },
    ColNotPayload { col: u32 },
    ColKindMismatch { col: u32, type_code: u8, want: ColKind },
    CopyTypeMismatch { col: u32, src_tc: u8, out: u32, out_tc: u8 },
    EmitSlotNotEightBytes { out: u32, type_code: u8 },
    EmitClassMismatch { out: u32, type_code: u8 },
    OutputIdxOutOfRange { out: u32, num_payload_cols: usize },
    OutputSlotUnwritten { written: u64, num_payload_cols: usize },
    PredicateWithoutResultReg,
    BadCastTarget { tc: u32 },
    BadTrimMode { mode: u32 },
    BadLikeEscape { escape: u32 },
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

    /// True iff the destination register is NULL exactly where this column is.
    ///
    /// It follows from the type requirement: an opcode decodes a column's value
    /// only if it knows the type, and decoding is what makes the register
    /// inherit the null bit. The two untyped kinds are the null-bitmap readers
    /// (`IS [NOT] NULL`) and the columnar `CopyCol`, neither of which puts the
    /// value in a register — `CopyCol` still propagates its source's null bit,
    /// but columnar, outside the register file.
    fn carries_null_to_register(self) -> bool {
        !matches!(self, ColKind::AnyCol | ColKind::AnyPayload)
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

/// Pure float unary transform: its operand's IEEE result, propagating the
/// operand's null bit and producing no NULL of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FloatUnaryOp {
    Neg,
    Abs,
    Floor,
    Ceil,
    Round,
    Trunc,
}

/// Pure integer unary transform: same width in, same width out (both are
/// `wrapping_*`, so `-i64::MIN` and `ABS(i64::MIN)` are `i64::MIN`), and the
/// operand's U64 tracking carries to the result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntUnaryOp {
    Neg,
    Abs,
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
    FloatUnary {
        op: FloatUnaryOp,
        dst: u16,
        a: u16,
    },
    IntUnary {
        op: IntUnaryOp,
        dst: u16,
        a: u16,
    },
    /// `tc` is the raw wire word here; `validate` narrows it to a fixed-int code.
    FloatToInt {
        dst: u16,
        a: u16,
        tc: u32,
    },
    IntCast {
        dst: u16,
        a: u16,
        tc: u32,
    },
    FloatToF32 {
        dst: u16,
        a: u16,
    },
    IntMinMax2 {
        dst: u16,
        a: u16,
        b: u16,
        is_max: bool,
    },
    FloatMinMax2 {
        dst: u16,
        a: u16,
        b: u16,
        is_max: bool,
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
    /// const-pool index (`u32`, like this enum's `StrColConst.const_idx`) — the
    /// packed sorted-i64 pool, decoded once at `resolve`.
    IntInSet {
        dst: u16,
        value_reg: u16,
        set_idx: u32,
    },
    LoadColStr {
        dst: u16,
        col: u32,
    },
    LoadConstStr {
        dst: u16,
        const_idx: u32,
    },
    /// Always-NULL string into `dst` — the string-class twin of
    /// [`LogicalInstr::LoadNull`], which a string context must emit instead so
    /// its consumers' operand class matches.
    LoadNullStr {
        dst: u16,
    },
    /// String CASE blend. `cond` is a scalar register; `a`/`b`/`dst` are string.
    StrSelect {
        dst: u16,
        cond: u16,
        a: u16,
        b: u16,
    },
    /// String compare writing a boolean into the *scalar* register `dst`.
    StrCmp {
        op: StrOp,
        dst: u16,
        a: u16,
        b: u16,
    },
    /// Length into the *scalar* register `dst`: bytes, or `chars` — the count of
    /// non-continuation bytes, which on valid UTF-8 is the codepoint count.
    StrLen {
        dst: u16,
        a: u16,
        chars: bool,
    },
    /// ASCII-only case fold (`a-z`/`A-Z`); every other byte passes through, so a
    /// multibyte UTF-8 sequence is unchanged.
    StrCase {
        dst: u16,
        a: u16,
        upper: bool,
    },
    /// Half-open character window `[start, start + len)`, 1-based, intersected
    /// with the string. `len_reg = None` runs to the end; a negative length is
    /// NULL.
    StrSubstr {
        dst: u16,
        src: u16,
        start_reg: u16,
        len_reg: Option<u16>,
    },
    /// `mode` is the raw wire word here; `validate` narrows it to a [`TrimMode`].
    StrTrim {
        dst: u16,
        a: u16,
        mode: u32,
        set_idx: u32,
    },
    /// SQL LIKE writing a boolean into the *scalar* register `dst`. `pat_idx` is
    /// a const-pool index naming the raw pattern bytes and `escape` the escape
    /// character (0 = escaping disabled), still a raw wire word until `validate`
    /// narrows it — TRIM's mode shape. `ci` is ILIKE's ASCII-only case folding,
    /// carried from the opcode.
    StrLike {
        dst: u16,
        src: u16,
        escape: u32,
        pat_idx: u32,
        ci: bool,
    },
    /// `skip_null` is CONCAT's asymmetric null rule: a NULL `b` contributes the
    /// empty string, a NULL `a` (the fold accumulator) still propagates. `false`
    /// is SQL `||` — NULL in either operand, NULL out.
    StrConcat {
        dst: u16,
        a: u16,
        b: u16,
        skip_null: bool,
    },
    IntToStr {
        dst: u16,
        a: u16,
    },
    FloatToStr {
        dst: u16,
        a: u16,
    },
    /// `tc` is the raw wire word here; `validate` narrows it to a fixed-int code.
    StrToInt {
        dst: u16,
        a: u16,
        tc: u32,
    },
    StrToFloat {
        dst: u16,
        a: u16,
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
///
/// Operands and null behaviour are classified on [`LogicalInstr`], by
/// [`operands`]. A variant here with no 1:1 logical counterpart therefore
/// inherits both from whichever opcode lowers into it, and must match it in
/// each.
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
    /// Payload F32 load, widened to the register's f64 image. An F64 column
    /// needs no kernel of its own — it lowers to `LoadPayloadInt` with `I64`.
    LoadPayloadF32 {
        dst: u16,
        pi: u8,
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
    IntToFloat {
        dst: u16,
        a: u16,
        signed: bool,
    },
    FloatUnary {
        op: FloatUnaryOp,
        dst: u16,
        a: u16,
    },
    IntUnary {
        op: IntUnaryOp,
        dst: u16,
        a: u16,
    },
    /// `fi` is the validated fixed-int target `resolve_program` narrowed the
    /// wire type code to, so the kernel's bounds lookup is total.
    FloatToInt {
        dst: u16,
        a: u16,
        fi: FixedInt,
    },
    IntCast {
        dst: u16,
        a: u16,
        fi: FixedInt,
        src_signed: bool,
    },
    FloatToF32 {
        dst: u16,
        a: u16,
    },
    IntMinMax2 {
        dst: u16,
        a: u16,
        b: u16,
        is_max: bool,
        signed: bool,
    },
    FloatMinMax2 {
        dst: u16,
        a: u16,
        b: u16,
        is_max: bool,
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
    /// A German-string column against a constant. The constant is encoded at
    /// `resolve` into `ResolvedProgram.const_cells`; `cell_idx` indexes that
    /// vector, not the const pool.
    StrColConst {
        op: StrOp,
        dst: u16,
        pi: u8,
        cell_idx: u32,
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
    /// A German-string column into a string register. `pi` is the payload slot,
    /// resolved from the logical column index.
    LoadColStr {
        dst: u16,
        pi: u8,
    },
    /// The const's span in `ResolvedProgram::const_arena`, baked at resolve —
    /// the const index is known there, so no span table survives to eval.
    LoadConstStr {
        dst: u16,
        off: u32,
        len: u32,
    },
    LoadNullStr {
        dst: u16,
    },
    StrSelect {
        dst: u16,
        cond: u16,
        a: u16,
        b: u16,
    },
    StrCmp {
        op: StrOp,
        dst: u16,
        a: u16,
        b: u16,
    },
    StrLen {
        dst: u16,
        a: u16,
        chars: bool,
    },
    StrCase {
        dst: u16,
        a: u16,
        upper: bool,
    },
    /// `start_signed`/`len_signed` carry the resolve-time U64 tracking, as
    /// `IntCast.src_signed` does: the window is computed in i128, and widening a
    /// U64-tracked register as signed would make a large start negative.
    StrSubstr {
        dst: u16,
        src: u16,
        start_reg: u16,
        len_reg: Option<u16>,
        start_signed: bool,
        len_signed: bool,
    },
    /// `set_idx` indexes `ResolvedProgram::trim_sets` — the 256-bit membership
    /// table decoded once at resolve — not the const pool.
    StrTrim {
        dst: u16,
        a: u16,
        mode: TrimMode,
        set_idx: u32,
    },
    /// `matcher_idx` indexes `ResolvedProgram::like_matchers` — the pattern
    /// compiled once at resolve — not the const pool.
    StrLike {
        dst: u16,
        src: u16,
        matcher_idx: u32,
    },
    StrConcat {
        dst: u16,
        a: u16,
        b: u16,
        skip_null: bool,
    },
    IntToStr {
        dst: u16,
        a: u16,
        signed: bool,
    },
    FloatToStr {
        dst: u16,
        a: u16,
    },
    /// `fi` is the validated fixed-int target `resolve_program` narrowed the
    /// wire type code to, so the kernel's range lookup is total.
    StrToInt {
        dst: u16,
        a: u16,
        fi: FixedInt,
    },
    StrToFloat {
        dst: u16,
        a: u16,
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
    /// EMIT of a *string* register. It has no `LogicalInstr` or wire
    /// counterpart: `resolve_program` splits `Emit` by its source register's
    /// class, which is why no per-register class mask is stored.
    EmitStr {
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
            let fu = |op| LogicalInstr::FloatUnary { op, dst, a };
            let iu = |op| LogicalInstr::IntUnary { op, dst, a };
            let str_cmp = |op| LogicalInstr::StrCmp { op, dst, a, b };
            // The escape rides the `a1` word beside the source register, the
            // `EXPR_STR_TRIM` shape; `ci` lives in the opcode, so nothing
            // downstream has to re-derive it.
            let str_like = |ci| {
                let (src, escape) = gnitz_wire::unpack_operand_pair(q[2]);
                LogicalInstr::StrLike {
                    dst,
                    src,
                    escape: escape as u32,
                    pat_idx: q[3],
                    ci,
                }
            };
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
                EXPR_INT_NEG => iu(IntUnaryOp::Neg),
                EXPR_FLOAT_ADD => LogicalInstr::FloatAdd { dst, a, b },
                EXPR_FLOAT_SUB => LogicalInstr::FloatSub { dst, a, b },
                EXPR_FLOAT_MUL => LogicalInstr::FloatMul { dst, a, b },
                EXPR_FLOAT_DIV => LogicalInstr::FloatDiv { dst, a, b },
                EXPR_FLOAT_NEG => fu(FloatUnaryOp::Neg),
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
                EXPR_INT_ABS => iu(IntUnaryOp::Abs),
                EXPR_FLOAT_ABS => fu(FloatUnaryOp::Abs),
                EXPR_FLOAT_FLOOR => fu(FloatUnaryOp::Floor),
                EXPR_FLOAT_CEIL => fu(FloatUnaryOp::Ceil),
                EXPR_FLOAT_ROUND => fu(FloatUnaryOp::Round),
                EXPR_FLOAT_TRUNC => fu(FloatUnaryOp::Trunc),
                EXPR_FLOAT_TO_F32 => LogicalInstr::FloatToF32 { dst, a },
                // The full u32 rides through: a forged high-bit word must reach
                // `validate`, not be silently truncated into a valid type code.
                EXPR_FLOAT_TO_INT => LogicalInstr::FloatToInt { dst, a, tc: q[3] },
                EXPR_INT_CAST => LogicalInstr::IntCast { dst, a, tc: q[3] },
                EXPR_INT_MAX2 => LogicalInstr::IntMinMax2 {
                    dst,
                    a,
                    b,
                    is_max: true,
                },
                EXPR_INT_MIN2 => LogicalInstr::IntMinMax2 {
                    dst,
                    a,
                    b,
                    is_max: false,
                },
                EXPR_FLOAT_MAX2 => LogicalInstr::FloatMinMax2 {
                    dst,
                    a,
                    b,
                    is_max: true,
                },
                EXPR_FLOAT_MIN2 => LogicalInstr::FloatMinMax2 {
                    dst,
                    a,
                    b,
                    is_max: false,
                },
                EXPR_SELECT => {
                    let (sa, sb) = gnitz_wire::unpack_operand_pair(q[3]);
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
                EXPR_LOAD_COL_STR => LogicalInstr::LoadColStr { dst, col: q[2] },
                EXPR_LOAD_CONST_STR => LogicalInstr::LoadConstStr { dst, const_idx: q[2] },
                EXPR_LOAD_NULL_STR => LogicalInstr::LoadNullStr { dst },
                EXPR_STR_SELECT => {
                    let (sa, sb) = gnitz_wire::unpack_operand_pair(q[3]);
                    LogicalInstr::StrSelect {
                        dst,
                        cond: q[2] as u16,
                        a: sa,
                        b: sb,
                    }
                }
                EXPR_STR_CMP_EQ => str_cmp(StrOp::Eq),
                EXPR_STR_CMP_LT => str_cmp(StrOp::Lt),
                EXPR_STR_CMP_LE => str_cmp(StrOp::Le),
                EXPR_STR_LEN_BYTES => LogicalInstr::StrLen { dst, a, chars: false },
                EXPR_STR_LEN_CHARS => LogicalInstr::StrLen { dst, a, chars: true },
                EXPR_STR_UPPER => LogicalInstr::StrCase { dst, a, upper: true },
                EXPR_STR_LOWER => LogicalInstr::StrCase { dst, a, upper: false },
                EXPR_STR_SUBSTR => {
                    let (start_reg, len_word) = gnitz_wire::unpack_operand_pair(q[3]);
                    LogicalInstr::StrSubstr {
                        dst,
                        src: a,
                        start_reg,
                        len_reg: (len_word as u32 != gnitz_wire::STR_SUBSTR_NO_LEN).then_some(len_word),
                    }
                }
                EXPR_STR_TRIM => {
                    // The mode rides the `a1` word beside the source register and
                    // stays a raw u32 through to `validate`, which narrows it —
                    // the `FloatToInt`/`IntCast` cast-target shape.
                    let (src, mode) = gnitz_wire::unpack_operand_pair(q[2]);
                    LogicalInstr::StrTrim {
                        dst,
                        a: src,
                        mode: mode as u32,
                        set_idx: q[3],
                    }
                }
                EXPR_STR_LIKE => str_like(false),
                EXPR_STR_ILIKE => str_like(true),
                EXPR_STR_CONCAT => LogicalInstr::StrConcat {
                    dst,
                    a,
                    b,
                    skip_null: false,
                },
                EXPR_STR_CONCAT_NN => LogicalInstr::StrConcat {
                    dst,
                    a,
                    b,
                    skip_null: true,
                },
                EXPR_INT_TO_STR => LogicalInstr::IntToStr { dst, a },
                EXPR_FLOAT_TO_STR => LogicalInstr::FloatToStr { dst, a },
                EXPR_STR_TO_INT => LogicalInstr::StrToInt { dst, a, tc: q[3] },
                EXPR_STR_TO_FLOAT => LogicalInstr::StrToFloat { dst, a },
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

    /// Lower to the resolved form, then run the two one-shot analyses over the
    /// resolved instruction stream — nullability and register roles — for the
    /// given context (`is_filter = true` keeps `result_reg` eligible for
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
        // `validate`'s `ColKind::payload_only` rule rejects a PK column for
        // every opcode resolved through here, and the constructors validate
        // before resolving.
        let payload_slot = |ci: usize| {
            schema
                .payload_slot(ci)
                .expect("validate pinned this operand to a payload column")
        };
        let mut instrs = Vec::with_capacity(self.instrs.len());
        // Decoded `INT_IN_SET` pools, indexed by the resolved `set_idx`. Decoded
        // once here (never per row); each `IntInSet` re-points its `set_idx` at
        // its slot in this vector.
        let mut int_sets: Vec<Vec<i64>> = Vec::new();
        // Decoded `STR_TRIM` byte sets as 256-bit membership tables, addressed
        // the same way. `trim_slots` maps a const-pool index to the slot it was
        // decoded into, so two TRIMs over one set share a table.
        let mut trim_sets: Vec<[u64; 4]> = Vec::new();
        let mut trim_slots: Vec<Option<u32>> = vec![None; self.const_strings.len()];
        // LIKE patterns compiled into matchers once here, never per row. One per
        // instruction, the `int_sets` shape rather than TRIM's slot table above:
        // a matcher depends on `(pat_idx, escape, ci)`, not on `pat_idx` alone.
        let mut like_matchers: Vec<LikeMatcher> = Vec::new();
        // The one pool of constant bytes both string channels resolve against.
        // Each `LoadConstStr` bakes its span in, so a const view is an ordinary
        // arena view and `StrView` needs no third buffer discriminator; a long
        // `StrColConst` cell's heap half lands here too, reached through the
        // operand's own blob reference rather than a second program buffer.
        let mut const_arena: Vec<u8> = Vec::new();
        let mut const_spans: Vec<Option<(u32, u32)>> = vec![None; self.const_strings.len()];
        // The 16-byte German-string cells the str-vs-const eval arm compares
        // through `gnitz_wire::compare_german_strings`. `cell_slots` maps a
        // const-pool index to the cell it was encoded into.
        let mut const_cells: Vec<[u8; 16]> = Vec::new();
        let mut cell_slots: Vec<Option<u32>> = vec![None; self.const_strings.len()];
        // Resolution is 1:1 and carries every register operand through by name,
        // so masks taken off the logical stream apply unchanged to the resolved
        // one.
        let RegisterRoles { bit_only, bool_input } = register_roles(&self.instrs, self.result_reg, is_filter);
        let no_nulls = is_strictly_non_nullable(&self.instrs, schema);
        // Off the schema alone, so a column-reading opcode gets the NOT NULL
        // collapse without wiring anything of its own. A PK column has no
        // payload slot and so contributes no bit, which is the same rule that
        // makes `LoadPk`'s destination unconditionally non-null.
        let nullable_slots = (0..schema.num_columns())
            .filter(|&ci| schema.col_nullable(ci))
            .filter_map(|ci| schema.payload_slot(ci))
            .fold(0u64, |m, pi| m | 1u64 << pi);
        // Which registers hold strings, off the same operand table `validate`
        // reads. Maintained in program order so an `Emit` sees the same class
        // `validate` saw when it approved that output slot.
        let mut str_class = 0u64;
        for li in self.instrs {
            if let Some((dst, WriteAs::Str)) = operands(&li).dst {
                str_class |= 1u64 << dst;
            }
            // One logical instruction, one resolved instruction. An arm may set
            // `reg_u64[dst]` before reading `reg_u64[a]` for the value it
            // returns: `validate`'s anti-aliasing rule keeps `dst` out of the
            // same instruction's reads.
            instrs.push(match li {
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
                    reg_u64[dst as usize] = loc.type_code() == type_code::U64;
                    match loc {
                        ColumnLocator::Pk { byte_off, .. } => I::LoadPk { dst, off: byte_off, fi },
                        ColumnLocator::Payload { slot, .. } => I::LoadPayloadInt { dst, pi: slot, fi },
                    }
                }
                L::LoadColFloat { dst, col } => {
                    // `validate` pinned this column to F32/F64. Listed
                    // positively so an unvalidated program panics rather than
                    // reading 8 bytes out of a 4-byte region.
                    let pi = payload_slot(col as usize);
                    match schema.col_type_code(col as usize) {
                        type_code::F64 => I::LoadPayloadInt {
                            dst,
                            pi,
                            fi: FixedInt::I64,
                        },
                        type_code::F32 => I::LoadPayloadF32 { dst, pi },
                        other => unreachable!("validated LoadColFloat names F32/F64, got {other}"),
                    }
                }
                L::LoadConst { dst, val } => I::LoadConst { dst, val },
                L::IntAdd { dst, a, b } => {
                    reg_u64[dst as usize] = reg_u64[a as usize] || reg_u64[b as usize];
                    I::IntAdd { dst, a, b }
                }
                L::IntSub { dst, a, b } => {
                    reg_u64[dst as usize] = reg_u64[a as usize] || reg_u64[b as usize];
                    I::IntSub { dst, a, b }
                }
                L::IntMul { dst, a, b } => {
                    reg_u64[dst as usize] = reg_u64[a as usize] || reg_u64[b as usize];
                    I::IntMul { dst, a, b }
                }
                L::IntDiv { dst, a, b } => {
                    let u = reg_u64[a as usize] || reg_u64[b as usize];
                    reg_u64[dst as usize] = u;
                    I::IntDiv { dst, a, b, signed: !u }
                }
                L::IntMod { dst, a, b } => {
                    let u = reg_u64[a as usize] || reg_u64[b as usize];
                    reg_u64[dst as usize] = u;
                    I::IntMod { dst, a, b, signed: !u }
                }
                L::FloatAdd { dst, a, b } => I::FloatAdd { dst, a, b },
                L::FloatSub { dst, a, b } => I::FloatSub { dst, a, b },
                L::FloatMul { dst, a, b } => I::FloatMul { dst, a, b },
                L::FloatDiv { dst, a, b } => I::FloatDiv { dst, a, b },
                L::Cmp { op, dst, a, b } => {
                    // EQ/NE are bit-identical signed/unsigned; ordered compares
                    // pick the unsigned form when either operand is U64.
                    let signed = matches!(op, CmpOp::Eq | CmpOp::Ne) || !(reg_u64[a as usize] || reg_u64[b as usize]);
                    I::Cmp { op, dst, a, b, signed }
                }
                L::FCmp { op, dst, a, b } => I::FCmp { op, dst, a, b },
                L::FloatUnary { op, dst, a } => I::FloatUnary { op, dst, a },
                // A pure int transform keeps the operand's width and signedness,
                // so the U64 tracking carries straight through.
                L::IntUnary { op, dst, a } => {
                    reg_u64[dst as usize] = reg_u64[a as usize];
                    I::IntUnary { op, dst, a }
                }
                L::FloatToF32 { dst, a } => I::FloatToF32 { dst, a },
                // Total on a validated program, the `LoadColInt` shape above:
                // `validate` gates both opcodes' target word through
                // `gnitz_wire::is_fixed_int` — the same eight codes
                // `from_type_code` answers `Some` for.
                L::FloatToInt { dst, a, tc } => {
                    let fi = validated_cast_target(tc);
                    reg_u64[dst as usize] = fi == FixedInt::U64;
                    I::FloatToInt { dst, a, fi }
                }
                L::IntCast { dst, a, tc } => {
                    let fi = validated_cast_target(tc);
                    reg_u64[dst as usize] = fi == FixedInt::U64;
                    I::IntCast {
                        dst,
                        a,
                        fi,
                        src_signed: !reg_u64[a as usize],
                    }
                }
                L::IntMinMax2 { dst, a, b, is_max } => {
                    let u = reg_u64[a as usize] || reg_u64[b as usize];
                    reg_u64[dst as usize] = u;
                    I::IntMinMax2 {
                        dst,
                        a,
                        b,
                        is_max,
                        signed: !u,
                    }
                }
                L::FloatMinMax2 { dst, a, b, is_max } => I::FloatMinMax2 { dst, a, b, is_max },
                L::IntToFloat { dst, a } => I::IntToFloat {
                    dst,
                    a,
                    signed: !reg_u64[a as usize],
                },
                L::Select { dst, cond, a, b } => {
                    // U64-ness flows through Select exactly as through IntAdd: the
                    // result is U64 if *either* branch is U64, so a downstream
                    // ordered compare / div / int_to_float on the CASE result picks
                    // the unsigned variant and values >= 2^63 order correctly.
                    reg_u64[dst as usize] = reg_u64[a as usize] || reg_u64[b as usize];
                    I::Select { dst, cond, a, b }
                }
                L::LoadNull { dst } => I::LoadNull { dst },
                L::BoolAnd { dst, a, b } => I::BoolAnd { dst, a, b },
                L::BoolOr { dst, a, b } => I::BoolOr { dst, a, b },
                L::BoolNot { dst, a } => I::BoolNot { dst, a },
                L::IsNull { dst, col } => I::IsNull {
                    dst,
                    pi: payload_slot(col as usize),
                },
                L::IsNotNull { dst, col } => I::IsNotNull {
                    dst,
                    pi: payload_slot(col as usize),
                },
                L::StrColConst {
                    op,
                    dst,
                    col,
                    const_idx,
                } => {
                    let ci = const_idx as usize;
                    // Encoded on first reference, so a pool entry no `StrColConst`
                    // names — an `INT_IN_SET` set, a TRIM byte set — costs neither
                    // a cell nor a copy of its bytes into the arena. An IN list
                    // rides one pool entry of 8 bytes per item and nothing caps its
                    // length, so encoding it unread would park that many bytes in
                    // every cached plan for the plan's life.
                    let cell_idx = *cell_slots[ci].get_or_insert_with(|| {
                        let slot = const_cells.len() as u32;
                        const_cells.push(encode_german_string(&self.const_strings[ci], &mut const_arena));
                        slot
                    });
                    I::StrColConst {
                        op,
                        dst,
                        pi: payload_slot(col as usize),
                        cell_idx,
                    }
                }
                L::StrColCol { op, dst, col_a, col_b } => I::StrColCol {
                    op,
                    dst,
                    pi_a: payload_slot(col_a as usize),
                    pi_b: payload_slot(col_b as usize),
                },
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
                    I::IntInSet {
                        dst,
                        value_reg,
                        set_idx: new_idx,
                    }
                }
                // The source's location, width, and type — dropped from the wire
                // `CopyCol` — resolve to the one canonical record.
                L::CopyCol { src_col, out } => I::CopyCol {
                    out,
                    src: schema.locate(src_col as usize),
                },
                L::LoadColStr { dst, col } => I::LoadColStr {
                    dst,
                    pi: payload_slot(col as usize),
                },
                L::LoadConstStr { dst, const_idx } => {
                    let ci = const_idx as usize;
                    // Appended on first reference, not once per instruction: two
                    // opcodes may share a const index.
                    let (off, len) = *const_spans[ci].get_or_insert_with(|| {
                        let bytes = &self.const_strings[ci];
                        let span = (const_arena.len() as u32, bytes.len() as u32);
                        const_arena.extend_from_slice(bytes);
                        span
                    });
                    I::LoadConstStr { dst, off, len }
                }
                L::LoadNullStr { dst } => I::LoadNullStr { dst },
                L::StrSelect { dst, cond, a, b } => I::StrSelect { dst, cond, a, b },
                L::StrCmp { op, dst, a, b } => I::StrCmp { op, dst, a, b },
                L::StrLen { dst, a, chars } => I::StrLen { dst, a, chars },
                L::StrCase { dst, a, upper } => I::StrCase { dst, a, upper },
                L::StrSubstr {
                    dst,
                    src,
                    start_reg,
                    len_reg,
                } => I::StrSubstr {
                    dst,
                    src,
                    start_reg,
                    len_reg,
                    start_signed: !reg_u64[start_reg as usize],
                    len_signed: len_reg.is_none_or(|l| !reg_u64[l as usize]),
                },
                L::StrTrim { dst, a, mode, set_idx } => {
                    let si = set_idx as usize;
                    // Decoded once per distinct pool index, never per row —
                    // `LoadConstStr` shares its index the same way. Immutable
                    // read of the pool: two opcodes may name one index.
                    let new_idx = *trim_slots[si].get_or_insert_with(|| {
                        let mut table = [0u64; 4];
                        for &byte in &self.const_strings[si] {
                            table[(byte >> 6) as usize] |= 1u64 << (byte & 63);
                        }
                        let slot = trim_sets.len() as u32;
                        trim_sets.push(table);
                        slot
                    });
                    I::StrTrim {
                        dst,
                        a,
                        mode: validated_trim_mode(mode),
                        set_idx: new_idx,
                    }
                }
                L::StrLike {
                    dst,
                    src,
                    escape,
                    pat_idx,
                    ci,
                } => {
                    let pattern = &self.const_strings[pat_idx as usize];
                    let matcher_idx = like_matchers.len() as u32;
                    like_matchers.push(LikeMatcher::compile(pattern, validated_like_escape(escape), ci));
                    I::StrLike { dst, src, matcher_idx }
                }
                L::StrConcat { dst, a, b, skip_null } => I::StrConcat { dst, a, b, skip_null },
                L::IntToStr { dst, a } => I::IntToStr {
                    dst,
                    a,
                    signed: !reg_u64[a as usize],
                },
                L::FloatToStr { dst, a } => I::FloatToStr { dst, a },
                L::StrToInt { dst, a, tc } => {
                    let fi = validated_cast_target(tc);
                    reg_u64[dst as usize] = fi == FixedInt::U64;
                    I::StrToInt { dst, a, fi }
                }
                L::StrToFloat { dst, a } => I::StrToFloat { dst, a },
                L::Emit { src, out } => {
                    if (str_class >> src) & 1 != 0 {
                        I::EmitStr { src, out }
                    } else {
                        I::Emit { src, out }
                    }
                }
            });
        }
        ResolvedProgram {
            no_nulls,
            nullable_slots,
            bit_only_mask: bit_only,
            bool_pack_mask: bit_only | bool_input,
            instrs,
            num_regs: self.num_regs,
            result_reg: self.result_reg,
            const_cells,
            int_sets,
            trim_sets,
            like_matchers,
            const_arena,
            has_strings: str_class != 0,
            // Guarded on `num_regs`: `validate` bounds `result_reg` only when the
            // program allocates registers at all.
            result_is_str: self.num_regs != 0 && (str_class >> self.result_reg) & 1 != 0,
        }
    }

    /// Validate as a filter predicate: the schema-aware pass, plus the one rule
    /// only a filter has — it must own a result register. `num_regs == 0` is
    /// legitimate for a map (`copy_cols` builds exactly that shape), so
    /// [`Self::validate`] cannot reject it; as a *filter* it is a corrupt frame
    /// with no result to read, not a filter that passes nothing.
    ///
    /// The result register's class belongs here for the same reason:
    /// [`Self::validate`] takes no `is_filter` parameter, and `resolve_scalar`
    /// makes the identical `validate(Some(schema), None)` call while *wanting* a
    /// string result — that is `eval_row_str`'s whole surface. A filter reads its
    /// verdict out of `regs`/`bool_bits`, which no string arm writes, so a string
    /// `result_reg` would filter rows on recycled scratch.
    pub(crate) fn validate_predicate(&self, schema: &dyn SchemaFacts) -> Result<(), ExprValidateErr> {
        let str_class = self.validate(Some(schema), None)?;
        if self.num_regs == 0 {
            return Err(ExprValidateErr::PredicateWithoutResultReg);
        }
        if (str_class >> self.result_reg) & 1 != 0 {
            return Err(ExprValidateErr::RegClassMismatch {
                reg: self.result_reg as u16,
            });
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
    ) -> Result<u64, ExprValidateErr> {
        use ExprValidateErr as E;
        use LogicalInstr as L;
        // Bit per written output payload slot; `check_out` bounds every `out`
        // below `num_payload_cols() <= 64`, so no shift can overflow.
        let mut written = 0u64;
        // Bit per register already written, and per register holding a string.
        // Single-writer keeps the class bitmask set-only: a register's class is
        // fixed by its one writer, so a bit is never cleared.
        let mut written_regs = 0u64;
        let mut str_class = 0u64;
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
            // Bound every register and column operand off the one per-opcode
            // operand table, so no arm below restates them.
            let ops = operands(instr);
            if let Some((dst, _)) = ops.dst {
                check_reg(dst, num_regs)?;
                // No opcode may write a register it reads: `split_windows`
                // hands out a `&mut` window at `dst` beside shared windows at
                // the sources.
                if let Some(&(reg, _)) = ops.reads.iter().flatten().find(|&&(r, _)| r == dst) {
                    return Err(E::RegisterAliasing { dst, reg });
                }
            }
            for &(reg, _) in ops.reads.iter().flatten() {
                check_reg(reg, num_regs)?;
            }
            // Ahead of the arm match: `check_copy_types` indexes the schema
            // with `src_col`, so the bound must already hold there.
            for &(col, kind) in ops.cols.iter().flatten() {
                check_col(in_schema, col, kind)?;
            }
            match *instr {
                // The cast target rides the `a2` word; a forged code must not
                // reach eval, where it would index a bounds table that has no
                // arm for it.
                L::FloatToInt { tc, .. } | L::IntCast { tc, .. } | L::StrToInt { tc, .. } => {
                    if tc > u8::MAX as u32 || !gnitz_wire::is_fixed_int(tc as u8) {
                        return Err(E::BadCastTarget { tc });
                    }
                }
                // EMIT writes an output payload slot; its source's class picks
                // which slot rule applies.
                L::Emit { src, out } => {
                    check_out(out)?;
                    check_emit_slot(out_schema, out, (str_class >> src) & 1 != 0)?;
                }
                L::StrColConst { const_idx, .. } | L::LoadConstStr { const_idx, .. } => {
                    check_const_idx(const_idx, self.const_strings.len())?
                }
                // Set membership over the register `value_reg`; `set_idx` is a
                // const-pool index whose entry must be a whole number of 8-byte
                // i64s (the `len % 8` check turns a truncating pool entry into a
                // clean `Rejected` instead of a silent `chunks_exact` tail-drop).
                L::IntInSet { set_idx, .. } => {
                    check_const_idx(set_idx, self.const_strings.len())?;
                    let len = self.const_strings[set_idx as usize].len();
                    if !len.is_multiple_of(8) {
                        return Err(E::IntSetNotAligned { set_idx, len });
                    }
                }
                // The trim mode rides the `a1` word beside the source register,
                // and the byte set is a const-pool entry. The set is not held to
                // ASCII here: STRING/BLOB are byte-transparent at this layer, so
                // a byte-wise strip of arbitrary bytes is a legitimate program.
                // The SQL binder restricts *its* trim sets to ASCII, which is a
                // language rule about text, not a VM invariant.
                L::StrTrim { mode, set_idx, .. } => {
                    if TrimMode::from_wire(mode).is_none() {
                        return Err(E::BadTrimMode { mode });
                    }
                    check_const_idx(set_idx, self.const_strings.len())?;
                }
                // The escape rides the `a1` word beside the source register, so
                // the half above a byte must be clear; the pattern is a
                // const-pool entry, and an empty one is the legal `LIKE ''`.
                L::StrLike { escape, pat_idx, .. } => {
                    if escape > u8::MAX as u32 {
                        return Err(E::BadLikeEscape { escape });
                    }
                    check_const_idx(pat_idx, self.const_strings.len())?;
                }
                // The hoisted register and column checks above are all these
                // need.
                L::LoadColInt { .. }
                | L::LoadColFloat { .. }
                | L::IsNull { .. }
                | L::IsNotNull { .. }
                | L::StrColCol { .. }
                | L::LoadColStr { .. }
                | L::LoadConst { .. }
                | L::LoadNull { .. }
                | L::LoadNullStr { .. }
                | L::IntUnary { .. }
                | L::IntToFloat { .. }
                | L::BoolNot { .. }
                | L::FloatUnary { .. }
                | L::FloatToF32 { .. }
                | L::StrLen { .. }
                | L::StrCase { .. }
                | L::IntToStr { .. }
                | L::FloatToStr { .. }
                | L::StrToFloat { .. }
                | L::IntAdd { .. }
                | L::IntSub { .. }
                | L::IntMul { .. }
                | L::IntDiv { .. }
                | L::IntMod { .. }
                | L::FloatAdd { .. }
                | L::FloatSub { .. }
                | L::FloatMul { .. }
                | L::FloatDiv { .. }
                | L::Cmp { .. }
                | L::FCmp { .. }
                | L::BoolAnd { .. }
                | L::BoolOr { .. }
                | L::IntMinMax2 { .. }
                | L::FloatMinMax2 { .. }
                | L::StrCmp { .. }
                | L::StrConcat { .. }
                | L::Select { .. }
                | L::StrSelect { .. }
                | L::StrSubstr { .. } => {}
                L::CopyCol { src_col, out } => {
                    check_out(out)?;
                    check_copy_types(in_schema, out_schema, src_col, out)?;
                }
            }
            // Ordered after the match so its aliasing rejections keep the more
            // specific diagnosis: `LoadColInt dst=0` then `Select dst=0, cond=0`
            // is `RegisterAliasing`, not `RegRewrite`.
            //
            // An operand's class must be the one the opcode reads: no string
            // opcode reading a scalar register, no scalar opcode reading a string
            // one. That also rejects read-before-def of a string operand, which a
            // stale `str_views` lane would otherwise turn into another row's
            // bytes: `str_class` is filled in program order by this same walk, so
            // a read whose writer comes later sees a clear bit. `OutputClass`
            // (`Emit`'s source) has no class to be held to.
            for &(reg, read) in ops.reads.iter().flatten() {
                if let Some(want_str) = read.wants_str() {
                    if ((str_class >> reg) & 1 != 0) != want_str {
                        return Err(E::RegClassMismatch { reg });
                    }
                }
            }
            // Single assignment: one writer is what makes a register's class
            // well-defined for its whole life.
            if let Some((dst, write)) = ops.dst {
                if (written_regs >> dst) & 1 != 0 {
                    return Err(E::RegRewrite { reg: dst });
                }
                written_regs |= 1u64 << dst;
                if write == WriteAs::Str {
                    str_class |= 1u64 << dst;
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
        Ok(str_class)
    }
}

/// A const-pool index is in range iff `< n`.
fn check_const_idx(const_idx: u32, n: usize) -> Result<(), ExprValidateErr> {
    if (const_idx as usize) < n {
        Ok(())
    } else {
        Err(ExprValidateErr::ConstIdxOutOfRange { const_idx, n })
    }
}

/// How a kernel consumes a register operand.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadAs {
    /// The register's i64 / f64 image out of `regs`.
    Value,
    /// A German-string view out of `str_views`.
    Str,
    /// A packed truth bit out of `bool_bits`, never the i64 image — which is
    /// what lets a producer whose every reader is a `Bool` skip the unpack
    /// (`bit_only`).
    Bool,
    /// `Emit`'s source, whose class *selects* the destination-column rule
    /// instead of being held to one. Bounded and counted as a value read like
    /// any other; exempt from the class check alone.
    OutputClass,
}

/// What an opcode writes into its destination register. String and scalar
/// opcodes share one register index space — so the null bits and boolean masks
/// apply unchanged to both — and a register's class is fixed by its one writer.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WriteAs {
    /// A scalar value.
    Value,
    /// A scalar whose i64 image is exactly its truth bit, 0 or 1. A reader may
    /// therefore take the packed `bool_bits` bit instead, and the kernel may
    /// skip writing `regs` when the register is `bit_only`. A mask-style `-1`
    /// for true is the violation this rules out.
    Bool,
    /// A German-string view.
    Str,
}

impl ReadAs {
    /// Whether this operand's register must hold a string, or `None` for the one
    /// read `validate` does not hold to a class.
    fn wants_str(self) -> Option<bool> {
        match self {
            ReadAs::Value | ReadAs::Bool => Some(false),
            ReadAs::Str => Some(true),
            ReadAs::OutputClass => None,
        }
    }
}

/// Every operand of one opcode — the register it writes, the registers it reads,
/// the columns it addresses — each with what its kernel requires of it, plus
/// whether the kernel can manufacture a NULL of its own.
///
/// The **one** per-opcode table: `validate`, `register_roles`,
/// `is_strictly_non_nullable` and `resolve_program` all read it, so a new opcode
/// is classified once. Every arm destructures every field of every variant it
/// matches, with `_` for fields it ignores and **no `..`** — so an opcode that
/// gains an operand is a compile error here rather than an unbounded operand
/// reaching the kernels.
struct Operands {
    /// `None` for the two output opcodes, which write no register.
    dst: Option<(u16, WriteAs)>,
    reads: [Option<(u16, ReadAs)>; MAX_READS],
    cols: [Option<(u32, ColKind)>; MAX_COL_OPERANDS],
    /// True iff the kernel can turn non-NULL input into a NULL result — a zero
    /// divisor, an out-of-range cast, an unparsable text→number, a CONCAT past
    /// `u32::MAX`, `LoadNull*` on every row. A column operand's own nullability
    /// is `cols`, not this.
    makes_null: bool,
}

/// SELECT and SUBSTRING, the widest opcodes, read three registers.
const MAX_READS: usize = 3;
/// `StrColCol` compares two columns; no opcode addresses more.
const MAX_COL_OPERANDS: usize = 2;

/// The two output opcodes, which write no register.
fn nothing() -> Operands {
    Operands {
        dst: None,
        reads: [None; MAX_READS],
        cols: [None; MAX_COL_OPERANDS],
        makes_null: false,
    }
}

fn writes(dst: u16, write: WriteAs) -> Operands {
    Operands {
        dst: Some((dst, write)),
        ..nothing()
    }
}

impl Operands {
    /// Mark the kernel a NULL producer; see [`Operands::makes_null`].
    fn may_null(mut self) -> Self {
        self.makes_null = true;
        self
    }

    fn reading(mut self, reg: u16, read: ReadAs) -> Self {
        let slot = self
            .reads
            .iter()
            .position(Option::is_none)
            .expect("an opcode reads at most MAX_READS registers");
        self.reads[slot] = Some((reg, read));
        self
    }

    fn reading_opt(self, reg: Option<u16>, read: ReadAs) -> Self {
        match reg {
            Some(r) => self.reading(r, read),
            None => self,
        }
    }

    fn on_col(mut self, col: u32, kind: ColKind) -> Self {
        let slot = self
            .cols
            .iter()
            .position(Option::is_none)
            .expect("an opcode addresses at most MAX_COL_OPERANDS columns");
        self.cols[slot] = Some((col, kind));
        self
    }
}

fn operands(li: &LogicalInstr) -> Operands {
    use ColKind::{AnyCol, AnyPayload, Float, GermanString};
    use LogicalInstr as L;
    use ReadAs::{Bool as RBool, OutputClass, Str as RStr, Value as RVal};
    use WriteAs::{Bool as WBool, Str as WStr, Value as WVal};
    match *li {
        // --- Two scalar registers in, one scalar value out ---
        L::IntAdd { dst, a, b }
        | L::IntSub { dst, a, b }
        | L::IntMul { dst, a, b }
        | L::FloatAdd { dst, a, b }
        | L::FloatSub { dst, a, b }
        | L::FloatMul { dst, a, b }
        | L::IntMinMax2 { dst, a, b, is_max: _ }
        | L::FloatMinMax2 { dst, a, b, is_max: _ } => writes(dst, WVal).reading(a, RVal).reading(b, RVal),
        // A zero divisor yields NULL.
        L::IntDiv { dst, a, b } | L::IntMod { dst, a, b } | L::FloatDiv { dst, a, b } => {
            writes(dst, WVal).reading(a, RVal).reading(b, RVal).may_null()
        }
        L::Cmp { op: _, dst, a, b } | L::FCmp { op: _, dst, a, b } => {
            writes(dst, WBool).reading(a, RVal).reading(b, RVal)
        }
        L::BoolAnd { dst, a, b } | L::BoolOr { dst, a, b } => writes(dst, WBool).reading(a, RBool).reading(b, RBool),
        L::BoolNot { dst, a } => writes(dst, WBool).reading(a, RBool),
        L::IntUnary { op: _, dst, a } | L::FloatUnary { op: _, dst, a } | L::IntToFloat { dst, a } => {
            writes(dst, WVal).reading(a, RVal)
        }
        // The three narrowing casts yield NULL on an out-of-range value.
        L::FloatToF32 { dst, a } | L::FloatToInt { dst, a, tc: _ } | L::IntCast { dst, a, tc: _ } => {
            writes(dst, WVal).reading(a, RVal).may_null()
        }
        // `cond` is a truth bit; the branches are values. `dst` is a value
        // write, not a boolean one — see `WriteAs::Bool`.
        L::Select { dst, cond, a, b } => writes(dst, WVal).reading(cond, RBool).reading(a, RVal).reading(b, RVal),
        L::IntInSet {
            dst,
            value_reg,
            set_idx: _,
        } => writes(dst, WBool).reading(value_reg, RVal),
        L::LoadConst { dst, val: _ } => writes(dst, WVal),
        // A NULL on every row.
        L::LoadNull { dst } => writes(dst, WVal).may_null(),

        // --- Column operands ---
        // The integer load kernels decode only the eight fixed-width integer
        // codes. `ColKind::FixedInt` is not payload-only, so this is the one
        // column operand that may name a PK column.
        L::LoadColInt { dst, col } => writes(dst, WVal).on_col(col, ColKind::FixedInt),
        // The float load kernel branches on width alone, so a 1- or 2-byte
        // integer column would make it slice an 8-byte stride from a narrower
        // region.
        L::LoadColFloat { dst, col } => writes(dst, WVal).on_col(col, Float),
        // The null-bitmap readers decode no value, so `AnyPayload` admits U128
        // and STRING, and their boolean is definite over a NULL row.
        L::IsNull { dst, col } | L::IsNotNull { dst, col } => writes(dst, WBool).on_col(col, AnyPayload),
        // The German-string compares read 16-byte cells; a narrower column
        // would make `col_data(pi, 16)` over-read its region. The verdict is a
        // bool, but a NULL operand makes it NULL, so the null bit flows.
        L::StrColConst {
            op: _,
            dst,
            col,
            const_idx: _,
        } => writes(dst, WBool).on_col(col, GermanString),
        L::StrColCol {
            op: _,
            dst,
            col_a,
            col_b,
        } => writes(dst, WBool)
            .on_col(col_a, GermanString)
            .on_col(col_b, GermanString),
        // The string-register column load reads the same 16-byte cells the
        // `EXPR_STR_COL_*` compares do, so it carries the same requirement.
        L::LoadColStr { dst, col } => writes(dst, WStr).on_col(col, GermanString),

        // --- String registers ---
        L::LoadConstStr { dst, const_idx: _ } => writes(dst, WStr),
        // A NULL on every row.
        L::LoadNullStr { dst } => writes(dst, WStr).may_null(),
        L::IntToStr { dst, a } | L::FloatToStr { dst, a } => writes(dst, WStr).reading(a, RVal),
        L::StrLen { dst, a, chars: _ } => writes(dst, WVal).reading(a, RStr),
        // Both text→number parses yield NULL on an unparsable or out-of-range
        // value.
        L::StrToInt { dst, a, tc: _ } | L::StrToFloat { dst, a } => writes(dst, WVal).reading(a, RStr).may_null(),
        L::StrCmp { op: _, dst, a, b } => writes(dst, WBool).reading(a, RStr).reading(b, RStr),
        // One string operand plus compile-time pattern data. The verdict is a
        // definite 0/1 — LIKE introduces no NULL of its own, so the operand's
        // null bit is the only one.
        L::StrLike {
            dst,
            src,
            escape: _,
            pat_idx: _,
            ci: _,
        } => writes(dst, WBool).reading(src, RStr),
        L::StrCase { dst, a, upper: _ }
        | L::StrTrim {
            dst,
            a,
            mode: _,
            set_idx: _,
        } => writes(dst, WStr).reading(a, RStr),
        // A combined length above `u32::MAX` yields NULL, which is easy to miss
        // because CONCAT otherwise looks like a pure transform.
        L::StrConcat {
            dst,
            a,
            b,
            skip_null: _,
        } => writes(dst, WStr).reading(a, RStr).reading(b, RStr).may_null(),
        // A scalar condition blending two string branches.
        L::StrSelect { dst, cond, a, b } => writes(dst, WStr).reading(cond, RBool).reading(a, RStr).reading(b, RStr),
        // A string source with integer window bounds. SUBSTR's only NULL is a
        // negative length, so the no-FOR form makes none — the window itself
        // clamps every out-of-range endpoint.
        L::StrSubstr {
            dst,
            src,
            start_reg,
            len_reg,
        } => {
            let ops = writes(dst, WStr)
                .reading(src, RStr)
                .reading(start_reg, RVal)
                .reading_opt(len_reg, RVal);
            if len_reg.is_some() {
                ops.may_null()
            } else {
                ops
            }
        }

        // --- Output opcodes: no destination register ---
        // The copy is columnar and bypasses the register file, so any source
        // column will do and no register takes its null bit.
        L::CopyCol { src_col, out: _ } => nothing().on_col(src_col, AnyCol),
        L::Emit { src, out: _ } => nothing().reading(src, OutputClass),
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

/// The cast opcodes' raw target word as the fixed-int it names. Total on a
/// validated program: `validate` rejects any word `gnitz_wire::is_fixed_int`
/// does not accept, which is exactly the eight codes `from_type_code` answers
/// `Some` for.
fn validated_cast_target(tc: u32) -> FixedInt {
    FixedInt::from_type_code(TypeCode::from_validated_u8(tc as u8)).expect("validated cast names a fixed-int target")
}

/// `StrTrim`'s raw mode word as the mode it names. Total on a validated program:
/// `validate` rejects any other word as `BadTrimMode`.
fn validated_trim_mode(mode: u32) -> TrimMode {
    TrimMode::from_wire(mode).expect("validated StrTrim names a known mode")
}

/// `StrLike`'s raw escape word as the escape character it names, `None` for the
/// 0 that disables escaping. Total on a validated program: `validate` rejects
/// anything wider than a byte as `BadLikeEscape`.
fn validated_like_escape(escape: u32) -> Option<u8> {
    debug_assert!(escape <= u8::MAX as u32, "validated StrLike names a byte escape");
    (escape != 0).then_some(escape as u8)
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

/// EMIT's destination slot must hold what the source register's class stores: a
/// string register's 16-byte German-string cell, or a scalar register's whole
/// 8-byte image.
///
/// Within the scalar half this stays a *stride* rule rather than a type rule —
/// `I64`, `U64` and `F64` are all legal targets, and no narrowing (which
/// truncates an i64 and shears an f64) or widening (which runs off the end of
/// `to_le_bytes()`) is admitted. Both scalar tests are needed and their order
/// does not matter: `wire_stride` reports 8 for an undecodable type code, so the
/// width test alone would admit one.
fn check_emit_slot(out_schema: Option<&dyn SchemaFacts>, out: u32, is_str: bool) -> Result<(), ExprValidateErr> {
    let Some(os) = out_schema else { return Ok(()) };
    // `col_type_code`, like its two sibling checks — not `locate`, whose extra
    // work (a release-active bound assert, plus an O(pk_count) OPK-offset walk
    // for a PK column) buys nothing here: `size()` IS `wire_stride(type_code)`.
    let type_code = os.col_type_code(os.payload_col_idx(out as usize));
    if is_str != gnitz_wire::is_german_string(type_code) {
        return Err(ExprValidateErr::EmitClassMismatch { out, type_code });
    }
    if !is_str && (!gnitz_wire::is_valid_type_code(type_code) || gnitz_wire::wire_stride(type_code) != 8) {
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
    /// The 16-byte German-string cells, indexed by the resolved `cell_idx`,
    /// with any heap half in `const_arena`. Only the constants a `StrColConst`
    /// names get one — encoded once at resolve, compared by
    /// `gnitz_wire::compare_german_strings`. `cell_idx` is in range because
    /// `resolve` hands it back from the same push, not because `validate`
    /// bounds it: it is not a const-pool index and no validator sees it.
    pub(crate) const_cells: Vec<[u8; 16]>,
    /// Decoded `INT_IN_SET` value pools, indexed by the resolved `set_idx`. Each
    /// pool is sorted ascending in signed-i64 `Ord` by `resolve` — the wire order
    /// is not trusted — so `eval_batch` binary-searches it directly. Duplicates
    /// are left in place; `binary_search` is correct over them.
    pub(crate) int_sets: Vec<Vec<i64>>,
    /// Decoded `STR_TRIM` byte sets as 256-bit membership tables, indexed by the
    /// resolved `set_idx`. Held here rather than inlined into `Instr` — 32 bytes
    /// would dominate the enum.
    pub(crate) trim_sets: Vec<[u64; 4]>,
    /// LIKE / ILIKE patterns compiled into matchers at resolve, indexed by the
    /// resolved `matcher_idx`. One entry per `StrLike` instruction: the escape
    /// and the case folding are baked in, so two instructions over one pool
    /// index still get a matcher each.
    pub(crate) like_matchers: Vec<LikeMatcher>,
    /// Every constant byte the program needs at run time: the spans a
    /// `LoadConstStr` bakes in, and the heap half of each long `const_cells`
    /// entry. Holds only the constants some opcode names, so an unreferenced
    /// pool entry is not carried for the plan's life. Also the string arena's
    /// non-cleared prefix: the per-morsel reset truncates back to
    /// `const_arena.len()`, so a `LoadConstStr` span stays valid for the
    /// evaluator's life.
    pub(crate) const_arena: Vec<u8>,
    /// True iff any register holds a string. A program without one allocates no
    /// string lanes and pays a single length compare in `ensure_capacity`.
    pub(crate) has_strings: bool,
    /// True iff `result_reg` is a string register — i.e. the result must be read
    /// through [`crate::Evaluator::eval_row_str`], not `eval_row`. Kept on the
    /// program so the class travels with it; a caller tracking it alongside has
    /// nothing to stop the two drifting apart.
    pub(crate) result_is_str: bool,
    /// True iff no instruction can produce a NULL against the schema this program
    /// was resolved against, so the evaluator skips null-bit tracking entirely.
    /// Resolved once — the answer is only meaningful for that one schema, since
    /// the payload indices in `instrs` were assigned from it.
    pub(crate) no_nulls: bool,
    /// Bit `N` set iff payload slot `N`'s column admits NULL in the schema this
    /// program was resolved against. A column read from a slot outside this mask
    /// contributes no null bit, so its destination register's null words are
    /// cleared instead of gathered per row.
    ///
    /// Carries the same schema caveat as [`Self::no_nulls`], and rests on the
    /// same assumption it does: a `NOT NULL` column's declared flag is believed
    /// over the bit the batch carries for it.
    pub(crate) nullable_slots: u64,
    /// Bit `r` set iff register `r` is only consumed by boolean ops, so its
    /// producer can skip the i64 unpack into `regs[r]`.
    bit_only_mask: u64,
    /// Bit `r` set iff `r`'s producer must write `bool_bits[r]` — either a
    /// downstream boolean consumer reads it, or `r` is bit_only and the filter
    /// reads `bool_bits[result_reg]` directly. Stored as the union rather than
    /// its two halves: `needs_bool_pack` is the only reader and runs per
    /// instruction per morsel.
    bool_pack_mask: u64,
}

impl ResolvedProgram {
    /// Per instruction per morsel, from the BOOL arms of `eval_batch`.
    pub(crate) fn is_bit_only(&self, reg: usize) -> bool {
        (self.bit_only_mask >> reg) & 1 != 0
    }

    /// True iff `reg`'s producer must write `bool_bits[reg]`. Per instruction
    /// per morsel, from `maybe_pack_bool_bits`.
    pub(crate) fn needs_bool_pack(&self, reg: usize) -> bool {
        (self.bool_pack_mask >> reg) & 1 != 0
    }
}

/// What [`register_roles`] derives in its one pass over the instruction stream.
struct RegisterRoles {
    /// Bit `r` set iff `r` is only consumed by boolean ops, so its producer can
    /// skip the i64 unpack into `regs[r]`.
    bit_only: u64,
    /// Bit `r` set iff a boolean consumer reads `r`, so its producer must
    /// populate `bool_bits[r]`.
    bool_input: u64,
}

/// Classify each register's role on the nullable-arm hot path, off the one
/// operand table.
///
/// A filter's `result_reg` is forced to be a bool input: the filter's nullable
/// arm consumes the result as packed bits, so its producer must populate
/// `bool_bits` whatever opcode it is. A map's result register needs no such
/// force — `Emit` already marks its source non-bool — and a bare scalar's may
/// legitimately stay `bit_only`.
///
/// Every `1u64 << reg` below is in range: `LogicalProgram::new` asserts
/// `num_regs <= MAX_REGS` and bounds every register operand by `num_regs`.
fn register_roles(instrs: &[LogicalInstr], result_reg: u32, is_filter: bool) -> RegisterRoles {
    let (mut bool_produced, mut non_bool_read, mut bool_input) = (0u64, 0u64, 0u64);
    for li in instrs {
        let ops = operands(li);
        if let Some((dst, WriteAs::Bool)) = ops.dst {
            bool_produced |= 1u64 << dst;
        }
        for &(reg, read) in ops.reads.iter().flatten() {
            let bit = 1u64 << reg;
            if read == ReadAs::Bool {
                bool_input |= bit;
            } else {
                non_bool_read |= bit;
            }
        }
    }
    if is_filter {
        bool_input |= 1u64 << result_reg as usize;
    }
    RegisterRoles {
        bit_only: bool_produced & !non_bool_read,
        bool_input,
    }
}

/// True iff no instruction can produce a NULL against `schema`, so the evaluator
/// skips null-bit tracking entirely. Called once, from `resolve`, against the
/// schema the program is about to be resolved against — the only schema for
/// which the answer means anything.
///
/// A PK column operand never contributes: the null bitmap is payload-indexed, so
/// the PK region carries no null bit for a load to inherit.
fn is_strictly_non_nullable(instrs: &[LogicalInstr], schema: &dyn SchemaFacts) -> bool {
    !instrs.iter().map(operands).any(|ops| {
        ops.makes_null
            || ops.cols.iter().flatten().any(|&(col, kind)| {
                kind.carries_null_to_register() && !schema.is_pk_col(col as usize) && schema.col_nullable(col as usize)
            })
    })
}

#[cfg(test)]
mod tests;
