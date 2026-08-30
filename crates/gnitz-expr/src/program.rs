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
use gnitz_wire::{encode_german_string, ExprOp, FixedInt, TrimMode, TypeCode};
use std::fmt;

/// The register file is capped at 64: the BOOL_AND/BOOL_OR 3VL paths, the
/// null-bit propagation, and every register-indexed mask address registers by
/// bit in a `u64`. Public because [`ExprValidateErr`]'s `TooManyRegs` rendering
/// names the limit.
pub const MAX_REGS: usize = u64::BITS as usize;

// `STR_SUBSTR_NO_LEN` is an in-band "no FOR clause" marker in a register field,
// so it has to sit above every register index. gnitz-wire cannot see this limit
// and states it in prose; this is where the two meet.
const _: () = assert!(
    MAX_REGS as u32 <= gnitz_wire::STR_SUBSTR_NO_LEN,
    "STR_SUBSTR_NO_LEN must stay above every real register index",
);

/// Why a client-authored expr program was rejected at compile — a diagnostic for
/// the recovery log. Production consumers only render it (the SQL planner wraps
/// it as `Unsupported`, the engine's compiler carries it into `RejectedExpr`),
/// so a variant's payload exists to make that text name the offending operand.
/// Only tests discriminate the variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprValidateErr {
    UnknownOpcode(u32),
    TooManyRegs(u32),
    ResultRegOutOfRange {
        result_reg: u32,
        num_regs: u32,
    },
    RegOutOfRange {
        reg: u16,
        num_regs: u32,
    },
    RegisterAliasing {
        dst: u16,
        reg: u16,
    },
    RegRewrite {
        reg: u16,
    },
    RegReadBeforeWrite {
        reg: u16,
    },
    RegClassMismatch {
        reg: u16,
    },
    ConstIdxOutOfRange {
        const_idx: u32,
        n: usize,
    },
    IntSetNotAligned {
        set_idx: u32,
        len: usize,
    },
    ColOutOfRange {
        col: u32,
        num_columns: usize,
    },
    ColNotPayload {
        col: u32,
    },
    ColKindMismatch {
        col: u32,
        type_code: u8,
        want: &'static str,
    },
    CopyTypeMismatch {
        col: u32,
        src_tc: u8,
        out: u32,
        out_tc: u8,
    },
    EmitSlotNotEightBytes {
        out: u32,
        type_code: u8,
    },
    EmitClassMismatch {
        out: u32,
        type_code: u8,
    },
    OutputIdxOutOfRange {
        out: u32,
        num_payload_cols: usize,
    },
    OutputSlotUnwritten {
        written: u64,
        num_payload_cols: usize,
    },
    ResultRegRequired,
    CorruptBlob(&'static str),
    BadCastTarget {
        tc: u32,
    },
    BadTrimMode {
        mode: u32,
    },
    BadLikeEscape {
        escape: u32,
    },
}

/// The client-facing rendering. Lives on the type so the planner's `Unsupported`
/// and the engine's compile rejection print the same wording, and so the limit
/// printed is the one [`LogicalProgram::from_wire`] enforces. The three variants
/// a working query can hit get sentences; the rest are internal-shape violations
/// with no user action, rendered as `Debug`.
impl fmt::Display for ExprValidateErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExprValidateErr::TooManyRegs(n) => {
                write!(
                    f,
                    "expression needs {n} registers; the limit is {MAX_REGS} — split the predicate"
                )
            }
            ExprValidateErr::ColKindMismatch { col, type_code, want } => {
                write!(
                    f,
                    "column {col} (type code {type_code}) cannot be used here; this operator needs {want}"
                )
            }
            ExprValidateErr::ColNotPayload { col } => {
                write!(
                    f,
                    "column {col} is part of the primary key; this operator needs a payload column"
                )
            }
            other => write!(f, "{other:?}"),
        }
    }
}

/// A column-type predicate paired with the phrase a `ColKindMismatch` renders
/// for it — the two halves of one requirement, so neither can be widened without
/// the other.
type ColTypeTest = (fn(u8) -> bool, &'static str);

/// What an opcode's kernel requires of a column operand. A `Col` name admits a
/// PK column; a `Payload` name does not — the kernels behind those address a
/// column through a dense payload index, which a PK column has none of.
#[derive(Clone, Copy)]
enum ColKind {
    /// PK or payload, any type — a `CopyCol` source. The kernel decodes no
    /// value, so U128 and STRING are legitimate.
    AnyCol,
    /// PK or payload, one of the eight `FixedInt` codes.
    FixedIntCol,
    /// Payload only, any type — the null-bitmap readers touch nothing else.
    AnyPayload,
    /// Payload only, IEEE-754.
    FloatPayload,
    /// Payload only, the 16-byte German-string layout.
    StringPayload,
}

impl ColKind {
    /// The type predicate this kind imposes, with the phrase a `ColKindMismatch`
    /// renders — `None` for the kinds that impose none, which is why those have
    /// no sentence. Predicate and wording in one arm, so a widened predicate
    /// cannot keep the old sentence.
    fn type_test(self) -> Option<ColTypeTest> {
        match self {
            Self::AnyCol | Self::AnyPayload => None,
            Self::FixedIntCol => Some((gnitz_wire::is_fixed_int, "a fixed-width integer column")),
            Self::FloatPayload => Some((gnitz_wire::is_float, "a floating-point column")),
            Self::StringPayload => Some((gnitz_wire::is_german_string, "a string or blob column")),
        }
    }

    /// True iff a PK column is unusable here.
    fn payload_only(self) -> bool {
        matches!(self, Self::AnyPayload | Self::FloatPayload | Self::StringPayload)
    }

    /// True iff the kernel decodes this column into a register, so the register
    /// inherits the column's null bit. A kind names a type exactly when its
    /// kernel decodes one, which is why this is [`Self::type_test`].
    fn carries_null_to_register(self) -> bool {
        self.type_test().is_some()
    }
}

// `FloatPayload` and `StringPayload` are restrictions, not tautologies, only
// while no PK-eligible type is a float or a German string — otherwise they would
// silently under-approximate their kernels' domain. gnitz-wire owns the PK
// admission rule; this is where the two meet.
const _: () = {
    let mut i = 0;
    while i < TypeCode::ALL.len() {
        let t = TypeCode::ALL[i];
        assert!(
            !(t.is_pk_eligible() && (t.is_float() || t.is_german_string())),
            "a PK-eligible float or German string would make ColKind's payload-only type tests vacuous",
        );
        i += 1;
    }
};

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
    FloatToInt {
        dst: u16,
        a: u16,
        fi: FixedInt,
    },
    IntCast {
        dst: u16,
        a: u16,
        fi: FixedInt,
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
    /// `IS NULL`, or `IS NOT NULL` when `invert`. The two read the same
    /// null-bitmap bit and differ only in its polarity, so they share one
    /// variant and one kernel — the `StrLen { chars }` / `StrCase { upper }`
    /// shape.
    IsNull {
        dst: u16,
        col: u32,
        invert: bool,
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
    StrTrim {
        dst: u16,
        a: u16,
        mode: TrimMode,
        set_idx: u32,
    },
    /// SQL LIKE writing a boolean into the *scalar* register `dst`. `pat_idx` is
    /// a const-pool index naming the raw pattern bytes and `escape` the escape
    /// character, `None` where escaping is disabled. `ci` is ILIKE's ASCII-only
    /// case folding, carried from the opcode.
    StrLike {
        dst: u16,
        src: u16,
        escape: Option<u8>,
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
    StrToInt {
        dst: u16,
        a: u16,
        fi: FixedInt,
    },
    StrToFloat {
        dst: u16,
        a: u16,
    },
    CopyCol {
        src_col: u32,
        out: u32,
    },
    /// Store a register into output payload slot `out`. `is_str` is the source
    /// register's class, which `resolve_program` reads off `str_class` — it has
    /// no `LogicalInstr` or wire counterpart, which is why no per-register class
    /// mask survives to eval.
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
#[derive(Clone, Copy, Debug)]
pub(crate) enum Instr {
    /// Payload integer load. `fi` *is* the eight-arm decode the kernel dispatches
    /// on, established once at resolve time (`validate` pins the column to
    /// `ColKind::FixedIntCol`), so the row loop carries no wildcard arm.
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
    /// `fi` is the fixed-int target `decode_quad` narrowed the wire type code
    /// to, so the kernel's bounds lookup is total.
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
        invert: bool,
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
    /// resolved from the logical column index; `buf` is the column region's
    /// buffer slot, assigned by `batch::str_col_slot`.
    LoadColStr {
        dst: u16,
        pi: u8,
        buf: u32,
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
    /// `fi` is the fixed-int target `decode_quad` narrowed the wire type code
    /// to, so the kernel's range lookup is total.
    StrToInt {
        dst: u16,
        a: u16,
        fi: FixedInt,
    },
    StrToFloat {
        dst: u16,
        a: u16,
    },
}

impl LogicalInstr {
    /// Serialise to the wire quad `[op, dst, a1, a2]` — the exact inverse of
    /// [`LogicalProgram::from_wire`]'s decode, and the **one** place the operand
    /// word layout is written — which word carries a packed pair, which opcode a
    /// flag selects, where a cast target rides. Stated here once and read back
    /// there once; the round-trip test is what holds the two together.
    ///
    /// Unused words are 0, matching what the decoder ignores.
    pub(crate) fn to_wire(self) -> [u32; 4] {
        use LogicalInstr as L;
        // Every arm is `[op, dst, a1, a2]`; these shorten the common shapes.
        let bin = |op: ExprOp, dst: u16, a: u16, b: u16| [op.as_wire(), dst as u32, a as u32, b as u32];
        let un = |op, dst: u16, a: u16| bin(op, dst, a, 0);
        let col = |op: ExprOp, dst: u16, c: u32| [op.as_wire(), dst as u32, c, 0];
        match self {
            L::LoadColInt { dst, col: c } => col(ExprOp::LoadColInt, dst, c),
            L::LoadColFloat { dst, col: c } => col(ExprOp::LoadColFloat, dst, c),
            L::LoadConst { dst, val } => {
                let (a1, a2) = gnitz_wire::encode_load_const(val);
                [ExprOp::LoadConst.as_wire(), dst as u32, a1, a2]
            }
            L::IntAdd { dst, a, b } => bin(ExprOp::IntAdd, dst, a, b),
            L::IntSub { dst, a, b } => bin(ExprOp::IntSub, dst, a, b),
            L::IntMul { dst, a, b } => bin(ExprOp::IntMul, dst, a, b),
            L::IntDiv { dst, a, b } => bin(ExprOp::IntDiv, dst, a, b),
            L::IntMod { dst, a, b } => bin(ExprOp::IntMod, dst, a, b),
            L::FloatAdd { dst, a, b } => bin(ExprOp::FloatAdd, dst, a, b),
            L::FloatSub { dst, a, b } => bin(ExprOp::FloatSub, dst, a, b),
            L::FloatMul { dst, a, b } => bin(ExprOp::FloatMul, dst, a, b),
            L::FloatDiv { dst, a, b } => bin(ExprOp::FloatDiv, dst, a, b),
            L::Cmp { op, dst, a, b } => bin(
                match op {
                    CmpOp::Eq => ExprOp::CmpEq,
                    CmpOp::Ne => ExprOp::CmpNe,
                    CmpOp::Gt => ExprOp::CmpGt,
                    CmpOp::Ge => ExprOp::CmpGe,
                    CmpOp::Lt => ExprOp::CmpLt,
                    CmpOp::Le => ExprOp::CmpLe,
                },
                dst,
                a,
                b,
            ),
            L::FCmp { op, dst, a, b } => bin(
                match op {
                    CmpOp::Eq => ExprOp::FcmpEq,
                    CmpOp::Ne => ExprOp::FcmpNe,
                    CmpOp::Gt => ExprOp::FcmpGt,
                    CmpOp::Ge => ExprOp::FcmpGe,
                    CmpOp::Lt => ExprOp::FcmpLt,
                    CmpOp::Le => ExprOp::FcmpLe,
                },
                dst,
                a,
                b,
            ),
            L::IntToFloat { dst, a } => un(ExprOp::IntToFloat, dst, a),
            L::FloatUnary { op, dst, a } => un(
                match op {
                    FloatUnaryOp::Neg => ExprOp::FloatNeg,
                    FloatUnaryOp::Abs => ExprOp::FloatAbs,
                    FloatUnaryOp::Floor => ExprOp::FloatFloor,
                    FloatUnaryOp::Ceil => ExprOp::FloatCeil,
                    FloatUnaryOp::Round => ExprOp::FloatRound,
                    FloatUnaryOp::Trunc => ExprOp::FloatTrunc,
                },
                dst,
                a,
            ),
            L::IntUnary { op, dst, a } => un(
                match op {
                    IntUnaryOp::Neg => ExprOp::IntNeg,
                    IntUnaryOp::Abs => ExprOp::IntAbs,
                },
                dst,
                a,
            ),
            L::FloatToInt { dst, a, fi } => [
                ExprOp::FloatToInt.as_wire(),
                dst as u32,
                a as u32,
                fi.type_code() as u32,
            ],
            L::IntCast { dst, a, fi } => [ExprOp::IntCast.as_wire(), dst as u32, a as u32, fi.type_code() as u32],
            L::FloatToF32 { dst, a } => un(ExprOp::FloatToF32, dst, a),
            L::IntMinMax2 { dst, a, b, is_max } => {
                bin(if is_max { ExprOp::IntMax2 } else { ExprOp::IntMin2 }, dst, a, b)
            }
            L::FloatMinMax2 { dst, a, b, is_max } => {
                bin(if is_max { ExprOp::FloatMax2 } else { ExprOp::FloatMin2 }, dst, a, b)
            }
            // SELECT and STR_SELECT pack `(a, b)` into the a2 word.
            L::Select { dst, cond, a, b } => [
                ExprOp::Select.as_wire(),
                dst as u32,
                cond as u32,
                gnitz_wire::pack_operand_pair(a as u32, b as u32),
            ],
            L::StrSelect { dst, cond, a, b } => [
                ExprOp::StrSelect.as_wire(),
                dst as u32,
                cond as u32,
                gnitz_wire::pack_operand_pair(a as u32, b as u32),
            ],
            L::LoadNull { dst } => [ExprOp::LoadNull.as_wire(), dst as u32, 0, 0],
            L::LoadNullStr { dst } => [ExprOp::LoadNullStr.as_wire(), dst as u32, 0, 0],
            L::BoolAnd { dst, a, b } => bin(ExprOp::BoolAnd, dst, a, b),
            L::BoolOr { dst, a, b } => bin(ExprOp::BoolOr, dst, a, b),
            L::BoolNot { dst, a } => un(ExprOp::BoolNot, dst, a),
            L::IsNull { dst, col: c, invert } => col(if invert { ExprOp::IsNotNull } else { ExprOp::IsNull }, dst, c),
            L::StrColConst {
                op,
                dst,
                col: c,
                const_idx,
            } => [
                match op {
                    StrOp::Eq => ExprOp::StrColEqConst,
                    StrOp::Lt => ExprOp::StrColLtConst,
                    StrOp::Le => ExprOp::StrColLeConst,
                }
                .as_wire(),
                dst as u32,
                c,
                const_idx,
            ],
            L::StrColCol { op, dst, col_a, col_b } => [
                match op {
                    StrOp::Eq => ExprOp::StrColEqCol,
                    StrOp::Lt => ExprOp::StrColLtCol,
                    StrOp::Le => ExprOp::StrColLeCol,
                }
                .as_wire(),
                dst as u32,
                col_a,
                col_b,
            ],
            L::IntInSet {
                dst,
                value_reg,
                set_idx,
            } => [ExprOp::IntInSet.as_wire(), dst as u32, value_reg as u32, set_idx],
            L::LoadColStr { dst, col: c } => col(ExprOp::LoadColStr, dst, c),
            L::LoadConstStr { dst, const_idx } => col(ExprOp::LoadConstStr, dst, const_idx),
            L::StrCmp { op, dst, a, b } => bin(
                match op {
                    StrOp::Eq => ExprOp::StrCmpEq,
                    StrOp::Lt => ExprOp::StrCmpLt,
                    StrOp::Le => ExprOp::StrCmpLe,
                },
                dst,
                a,
                b,
            ),
            L::StrLen { dst, a, chars } => un(
                if chars {
                    ExprOp::StrLenChars
                } else {
                    ExprOp::StrLenBytes
                },
                dst,
                a,
            ),
            L::StrCase { dst, a, upper } => un(if upper { ExprOp::StrUpper } else { ExprOp::StrLower }, dst, a),
            // SUBSTR packs `(start, len)` into a2; the no-FOR form rides the
            // `STR_SUBSTR_NO_LEN` sentinel.
            L::StrSubstr {
                dst,
                src,
                start_reg,
                len_reg,
            } => [
                ExprOp::StrSubstr.as_wire(),
                dst as u32,
                src as u32,
                gnitz_wire::pack_operand_pair(
                    start_reg as u32,
                    len_reg.map_or(gnitz_wire::STR_SUBSTR_NO_LEN, u32::from),
                ),
            ],
            // TRIM and LIKE pack their second operand into **a1**, beside the
            // source register — the mirror of SELECT and SUBSTR above.
            L::StrTrim { dst, a, mode, set_idx } => [
                ExprOp::StrTrim.as_wire(),
                dst as u32,
                gnitz_wire::pack_operand_pair(a as u32, mode.as_wire()),
                set_idx,
            ],
            L::StrLike {
                dst,
                src,
                escape,
                pat_idx,
                ci,
            } => [
                if ci { ExprOp::StrIlike } else { ExprOp::StrLike }.as_wire(),
                dst as u32,
                gnitz_wire::pack_operand_pair(src as u32, escape.unwrap_or(0) as u32),
                pat_idx,
            ],
            L::StrConcat { dst, a, b, skip_null } => bin(
                if skip_null {
                    ExprOp::StrConcatNn
                } else {
                    ExprOp::StrConcat
                },
                dst,
                a,
                b,
            ),
            L::IntToStr { dst, a } => un(ExprOp::IntToStr, dst, a),
            L::FloatToStr { dst, a } => un(ExprOp::FloatToStr, dst, a),
            L::StrToInt { dst, a, fi } => [ExprOp::StrToInt.as_wire(), dst as u32, a as u32, fi.type_code() as u32],
            L::StrToFloat { dst, a } => un(ExprOp::StrToFloat, dst, a),
            // The two output opcodes write no register, so the `dst` word is 0
            // and both operands ride a1/a2.
            L::CopyCol { src_col, out } => [ExprOp::CopyCol.as_wire(), 0, src_col, out],
            L::Emit { src, out } => [ExprOp::Emit.as_wire(), 0, src as u32, out],
        }
    }
}

// ---------------------------------------------------------------------------
// LogicalProgram — pre-resolve container
// ---------------------------------------------------------------------------

/// What a resolved program is for. The one classification resolution needs that
/// the instruction stream does not carry: it fixes `result_reg`'s eligibility
/// for the bit_only path, and whether `result_reg` means anything at all.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    /// A predicate driven through [`crate::Evaluator::filter`], which reads
    /// `result_reg` back out of `bool_bits`.
    Filter,
    /// A projection whose outputs are `copies`/`emits`. Its `result_reg` is
    /// force-zeroed and never read.
    Map,
    /// A row-at-a-time scalar (a DML SET right-hand side), read back through
    /// `eval_row` / `eval_row_str`.
    Scalar,
}

pub struct LogicalProgram {
    instrs: Vec<LogicalInstr>,
    num_regs: u32,
    result_reg: u32,
    const_strings: Vec<Vec<u8>>,
    /// Bit `r` set iff register `r` holds a string rather than a scalar, as
    /// [`Self::from_instrs`] finished it.
    ///
    /// **Position-independent**, which is what lets every later pass read the
    /// finished mask instead of rebuilding one in step: read-before-write plus
    /// single assignment put a register's one writer ahead of all its readers.
    str_class: u64,
}

impl LogicalProgram {
    /// Build from typed instructions. The compiler and test builders trust their
    /// own construction, so a structural failure here is a compiler bug, not
    /// client input — [`Self::from_instrs`] panics rather than returns.
    pub fn new(instrs: Vec<LogicalInstr>, num_regs: u32, result_reg: u32, const_strings: Vec<Vec<u8>>) -> Self {
        Self::from_instrs(instrs, num_regs, result_reg, const_strings)
            .unwrap_or_else(|e| panic!("compiler-built LogicalProgram is invalid: {e:?}"))
    }

    /// Assemble from typed instructions, holding every one to the rules a schema
    /// is not needed for. Every constructor routes through here and the type is
    /// immutable, so the walk below establishes the register-valid / alias-free
    /// invariant `regs_split`'s raw split borrows depend on, and
    /// [`Self::str_class`], for every `LogicalProgram` in every profile.
    ///
    /// The fallible entry point: [`Self::new`] unwraps it (a failure is a
    /// compiler bug), `from_wire` propagates it (bad client input), and
    /// [`ExprBuilder::build_logical`](crate::ExprBuilder::build_logical) hands
    /// its instructions straight here rather than encoding them to wire words
    /// for `from_wire` to decode back.
    pub(crate) fn from_instrs(
        instrs: Vec<LogicalInstr>,
        num_regs: u32,
        result_reg: u32,
        const_strings: Vec<Vec<u8>>,
    ) -> Result<Self, ExprValidateErr> {
        use ExprValidateErr as E;
        // Bit per register already written, and per register holding a string.
        // Single-writer keeps the class bitmask set-only: a register's class is
        // fixed by its one writer, so a bit is never cleared.
        let mut written_regs = 0u64;
        let mut str_class = 0u64;
        if num_regs as usize > MAX_REGS {
            return Err(E::TooManyRegs(num_regs));
        }
        // `max(1)` is the register-free `copy_cols` map, whose `result_reg = 0`
        // names nothing. Bounding every role here is what makes
        // `result_reg < MAX_REGS` a property of the type rather than of a rule
        // in another function.
        if result_reg >= num_regs.max(1) {
            return Err(E::ResultRegOutOfRange { result_reg, num_regs });
        }
        for instr in &instrs {
            // Bound every register operand off the one per-opcode operand table,
            // so no arm below restates them.
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
            // The const-pool index an opcode carries rather than names as an
            // operand — the table states that too.
            match ops.extra {
                Extra::ConstIdx(const_idx) => check_const_idx(const_idx, const_strings.len())?,
                // A truncating pool entry is a clean rejection rather than a
                // silent `chunks_exact` tail-drop.
                Extra::IntSet(set_idx) => {
                    check_const_idx(set_idx, const_strings.len())?;
                    let len = const_strings[set_idx as usize].len();
                    if !int_set_len_ok(len) {
                        return Err(E::IntSetNotAligned { set_idx, len });
                    }
                }
                Extra::None => {}
            }
            for &(reg, read) in ops.reads.iter().flatten() {
                // A register must be written before it is read: an unwritten
                // lane holds whatever the previous morsel left there — another
                // row's value, or for a string lane another row's bytes. It is
                // also half of what makes `str_class` position-independent.
                if (written_regs >> reg) & 1 == 0 {
                    return Err(E::RegReadBeforeWrite { reg });
                }
                // An operand's class must be the one the opcode reads: no string
                // opcode reading a scalar register, no scalar opcode reading a
                // string one. `OutputClass` (`Emit`'s source) has no class to be
                // held to — its class *selects* the destination-column rule.
                if let Some(want_str) = read.wants_str() {
                    if ((str_class >> reg) & 1 != 0) != want_str {
                        return Err(E::RegClassMismatch { reg });
                    }
                }
            }
            // Single assignment: one writer is what makes a register's class
            // well-defined for its whole life. Last, so the aliasing check above
            // keeps the more specific diagnosis — `LoadColInt dst=0` then
            // `Select dst=0, cond=0` is `RegisterAliasing`, not `RegRewrite`.
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
        Ok(LogicalProgram {
            instrs,
            num_regs,
            result_reg,
            const_strings,
            str_class,
        })
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

    /// A predicate or scalar blob: framing decoded, then lowered. `label` names
    /// the call site in a `CorruptBlob` — the engine's own wrappers cover the
    /// *invalid program* path, not corrupt framing.
    pub fn from_blob(blob: &[u8], label: &'static str) -> Result<Self, ExprValidateErr> {
        let b = gnitz_wire::decode_expr_blob(blob).ok_or(ExprValidateErr::CorruptBlob(label))?;
        Self::from_wire(&b.code, b.num_regs, b.result_reg, b.const_strings)
    }

    /// The same for a **map** blob, which has no result register: a map writes
    /// output slots, so its `result_reg` is forced to 0 here rather than at each
    /// call site.
    pub fn from_map_blob(blob: &[u8], label: &'static str) -> Result<Self, ExprValidateErr> {
        let b = gnitz_wire::decode_expr_blob(blob).ok_or(ExprValidateErr::CorruptBlob(label))?;
        Self::from_wire(&b.code, b.num_regs, 0, b.const_strings)
    }

    /// Lower a wire expr blob (flat u32 quads `[op, dst, a1, a2]`) into the
    /// typed logical form. Client-controlled throughout: an unknown opcode or a
    /// forged operand word is rejected by [`Self::decode_quad`], everything
    /// structural by [`Self::from_instrs`], and neither panics.
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
            instrs.push(Self::decode_quad(q)?);
        }
        Self::from_instrs(instrs, num_regs, result_reg, const_strings)
    }

    /// Decode one wire quad `[op, dst, a1, a2]`. The exact inverse of
    /// [`LogicalInstr::to_wire`], and with it the only statement of the operand
    /// word layout; an unknown opcode is rejected rather than panicked, since the
    /// words are client-controlled.
    ///
    /// The match over [`ExprOp`] is exhaustive and has **no `_` arm**: a new
    /// opcode in gnitz-wire fails to compile here until it gets a decode arm,
    /// which is the drift the two tables would otherwise have to be tested for.
    /// A cast target, TRIM's mode and LIKE's escape are narrowed to their types
    /// here too, so no later pass carries a raw word.
    pub(crate) fn decode_quad(q: &[u32]) -> Result<LogicalInstr, ExprValidateErr> {
        let op = ExprOp::from_wire(q[0]).ok_or(ExprValidateErr::UnknownOpcode(q[0]))?;
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
        // `ExprOp::StrTrim` shape; `ci` lives in the opcode, so nothing
        // downstream has to re-derive it.
        let str_like = |ci| -> Result<LogicalInstr, ExprValidateErr> {
            let (src, escape) = gnitz_wire::unpack_operand_pair(q[2]);
            Ok(LogicalInstr::StrLike {
                dst,
                src,
                escape: like_escape(escape as u32)?,
                pat_idx: q[3],
                ci,
            })
        };
        Ok(match op {
            ExprOp::LoadColInt => LogicalInstr::LoadColInt { dst, col: q[2] },
            ExprOp::LoadColFloat => LogicalInstr::LoadColFloat { dst, col: q[2] },
            ExprOp::LoadConst => LogicalInstr::LoadConst {
                dst,
                val: gnitz_wire::decode_load_const(q[2], q[3]),
            },
            ExprOp::IntAdd => LogicalInstr::IntAdd { dst, a, b },
            ExprOp::IntSub => LogicalInstr::IntSub { dst, a, b },
            ExprOp::IntMul => LogicalInstr::IntMul { dst, a, b },
            ExprOp::IntDiv => LogicalInstr::IntDiv { dst, a, b },
            ExprOp::IntMod => LogicalInstr::IntMod { dst, a, b },
            ExprOp::IntNeg => iu(IntUnaryOp::Neg),
            ExprOp::FloatAdd => LogicalInstr::FloatAdd { dst, a, b },
            ExprOp::FloatSub => LogicalInstr::FloatSub { dst, a, b },
            ExprOp::FloatMul => LogicalInstr::FloatMul { dst, a, b },
            ExprOp::FloatDiv => LogicalInstr::FloatDiv { dst, a, b },
            ExprOp::FloatNeg => fu(FloatUnaryOp::Neg),
            ExprOp::CmpEq => cmp(CmpOp::Eq),
            ExprOp::CmpNe => cmp(CmpOp::Ne),
            ExprOp::CmpGt => cmp(CmpOp::Gt),
            ExprOp::CmpGe => cmp(CmpOp::Ge),
            ExprOp::CmpLt => cmp(CmpOp::Lt),
            ExprOp::CmpLe => cmp(CmpOp::Le),
            ExprOp::FcmpEq => fcmp(CmpOp::Eq),
            ExprOp::FcmpNe => fcmp(CmpOp::Ne),
            ExprOp::FcmpGt => fcmp(CmpOp::Gt),
            ExprOp::FcmpGe => fcmp(CmpOp::Ge),
            ExprOp::FcmpLt => fcmp(CmpOp::Lt),
            ExprOp::FcmpLe => fcmp(CmpOp::Le),
            ExprOp::BoolAnd => LogicalInstr::BoolAnd { dst, a, b },
            ExprOp::BoolOr => LogicalInstr::BoolOr { dst, a, b },
            ExprOp::BoolNot => LogicalInstr::BoolNot { dst, a },
            ExprOp::IsNull => LogicalInstr::IsNull {
                dst,
                col: q[2],
                invert: false,
            },
            ExprOp::IsNotNull => LogicalInstr::IsNull {
                dst,
                col: q[2],
                invert: true,
            },
            ExprOp::Emit => LogicalInstr::Emit { src: a, out: q[3] },
            ExprOp::IntToFloat => LogicalInstr::IntToFloat { dst, a },
            ExprOp::IntAbs => iu(IntUnaryOp::Abs),
            ExprOp::FloatAbs => fu(FloatUnaryOp::Abs),
            ExprOp::FloatFloor => fu(FloatUnaryOp::Floor),
            ExprOp::FloatCeil => fu(FloatUnaryOp::Ceil),
            ExprOp::FloatRound => fu(FloatUnaryOp::Round),
            ExprOp::FloatTrunc => fu(FloatUnaryOp::Trunc),
            ExprOp::FloatToF32 => LogicalInstr::FloatToF32 { dst, a },
            // `cast_target` sees the full u32: a forged high-bit word must be
            // rejected, not silently truncated into a valid type code.
            ExprOp::FloatToInt => LogicalInstr::FloatToInt {
                dst,
                a,
                fi: cast_target(q[3])?,
            },
            ExprOp::IntCast => LogicalInstr::IntCast {
                dst,
                a,
                fi: cast_target(q[3])?,
            },
            ExprOp::IntMax2 => LogicalInstr::IntMinMax2 {
                dst,
                a,
                b,
                is_max: true,
            },
            ExprOp::IntMin2 => LogicalInstr::IntMinMax2 {
                dst,
                a,
                b,
                is_max: false,
            },
            ExprOp::FloatMax2 => LogicalInstr::FloatMinMax2 {
                dst,
                a,
                b,
                is_max: true,
            },
            ExprOp::FloatMin2 => LogicalInstr::FloatMinMax2 {
                dst,
                a,
                b,
                is_max: false,
            },
            ExprOp::Select => {
                let (sa, sb) = gnitz_wire::unpack_operand_pair(q[3]);
                LogicalInstr::Select {
                    dst,
                    cond: q[2] as u16,
                    a: sa,
                    b: sb,
                }
            }
            ExprOp::LoadNull => LogicalInstr::LoadNull { dst },
            ExprOp::CopyCol => LogicalInstr::CopyCol {
                src_col: q[2],
                out: q[3],
            },
            ExprOp::StrColEqConst => LogicalInstr::StrColConst {
                op: StrOp::Eq,
                dst,
                col: q[2],
                const_idx: q[3],
            },
            ExprOp::StrColLtConst => LogicalInstr::StrColConst {
                op: StrOp::Lt,
                dst,
                col: q[2],
                const_idx: q[3],
            },
            ExprOp::StrColLeConst => LogicalInstr::StrColConst {
                op: StrOp::Le,
                dst,
                col: q[2],
                const_idx: q[3],
            },
            ExprOp::StrColEqCol => LogicalInstr::StrColCol {
                op: StrOp::Eq,
                dst,
                col_a: q[2],
                col_b: q[3],
            },
            ExprOp::StrColLtCol => LogicalInstr::StrColCol {
                op: StrOp::Lt,
                dst,
                col_a: q[2],
                col_b: q[3],
            },
            ExprOp::StrColLeCol => LogicalInstr::StrColCol {
                op: StrOp::Le,
                dst,
                col_a: q[2],
                col_b: q[3],
            },
            // `value_reg` rides the `a` slot (`q[2] as u16`); `set_idx` takes
            // the full `q[3]` u32 const index, never truncated to u16.
            ExprOp::IntInSet => LogicalInstr::IntInSet {
                dst,
                value_reg: a,
                set_idx: q[3],
            },
            ExprOp::LoadColStr => LogicalInstr::LoadColStr { dst, col: q[2] },
            ExprOp::LoadConstStr => LogicalInstr::LoadConstStr { dst, const_idx: q[2] },
            ExprOp::LoadNullStr => LogicalInstr::LoadNullStr { dst },
            ExprOp::StrSelect => {
                let (sa, sb) = gnitz_wire::unpack_operand_pair(q[3]);
                LogicalInstr::StrSelect {
                    dst,
                    cond: q[2] as u16,
                    a: sa,
                    b: sb,
                }
            }
            ExprOp::StrCmpEq => str_cmp(StrOp::Eq),
            ExprOp::StrCmpLt => str_cmp(StrOp::Lt),
            ExprOp::StrCmpLe => str_cmp(StrOp::Le),
            ExprOp::StrLenBytes => LogicalInstr::StrLen { dst, a, chars: false },
            ExprOp::StrLenChars => LogicalInstr::StrLen { dst, a, chars: true },
            ExprOp::StrUpper => LogicalInstr::StrCase { dst, a, upper: true },
            ExprOp::StrLower => LogicalInstr::StrCase { dst, a, upper: false },
            ExprOp::StrSubstr => {
                let (start_reg, len_word) = gnitz_wire::unpack_operand_pair(q[3]);
                LogicalInstr::StrSubstr {
                    dst,
                    src: a,
                    start_reg,
                    len_reg: (len_word as u32 != gnitz_wire::STR_SUBSTR_NO_LEN).then_some(len_word),
                }
            }
            ExprOp::StrTrim => {
                // The mode rides the `a1` word beside the source register.
                let (src, mode) = gnitz_wire::unpack_operand_pair(q[2]);
                LogicalInstr::StrTrim {
                    dst,
                    a: src,
                    mode: trim_mode(mode as u32)?,
                    set_idx: q[3],
                }
            }
            ExprOp::StrLike => str_like(false)?,
            ExprOp::StrIlike => str_like(true)?,
            ExprOp::StrConcat => LogicalInstr::StrConcat {
                dst,
                a,
                b,
                skip_null: false,
            },
            ExprOp::StrConcatNn => LogicalInstr::StrConcat {
                dst,
                a,
                b,
                skip_null: true,
            },
            ExprOp::IntToStr => LogicalInstr::IntToStr { dst, a },
            ExprOp::FloatToStr => LogicalInstr::FloatToStr { dst, a },
            ExprOp::StrToInt => LogicalInstr::StrToInt {
                dst,
                a,
                fi: cast_target(q[3])?,
            },
            ExprOp::StrToFloat => LogicalInstr::StrToFloat { dst, a },
        })
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
    /// Sequential sources AND dense destinations: without the second check a
    /// permuted destination would answer `Some`, and a real permutation would be
    /// executed as a block copy.
    pub fn sequential_copy_base(&self) -> Option<usize> {
        let srcs = self.payload_copy_srcs()?;
        let &base = srcs.first()?;
        let ok = srcs.iter().enumerate().all(|(i, &s)| s == base + i as u32);
        ok.then_some(base as usize)
    }

    /// Lower to the resolved form, then run the two one-shot analyses over the
    /// resolved instruction stream — nullability and register roles — for the
    /// given [`Role`] ([`Role::Filter`] keeps `result_reg` eligible for
    /// bit_only). Consuming: a `Vec<LogicalInstr>` cannot be mutated in place into
    /// a `Vec<Instr>`. Preserves the per-register U64 signed→unsigned tracking.
    ///
    /// `out_schema` is the map's output schema, `None` for the roles that write
    /// no output slots. It fixes each copy's destination width here, where
    /// `check_copy_types` has just approved the widening — so the legality of a
    /// widening and the width it writes at are one fact.
    ///
    /// Every "decoded once" below — LIKE matchers, trim sets, const cells, the
    /// `INT_IN_SET` pools — means **once per compile**, not once per program run
    /// and never per row.
    pub(crate) fn resolve_program(
        self,
        schema: &dyn SchemaFacts,
        out_schema: Option<&dyn SchemaFacts>,
        role: Role,
    ) -> ResolvedProgram {
        use gnitz_wire::type_code;
        use Instr as I;
        use LogicalInstr as L;

        // Read out before the `self.instrs` partial move below.
        let str_class = self.str_class;

        // `check_col`'s `ColKind::payload_only` rule rejects a PK column for
        // every opcode resolved through here, and the constructors run the
        // schema pass before resolving.
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
        let mut copies: Vec<(ColumnLocator, u32, u8)> = Vec::new();
        let mut emits: Vec<(u16, u32)> = Vec::new();
        // Filled by `batch::str_col_slot`, in buffer-slot order.
        let mut str_cols: Vec<u8> = Vec::new();
        // Resolution is 1:1 on every computing opcode and carries each register
        // operand through by name, so masks taken off the logical stream apply
        // unchanged to the resolved one; the two output sinks leave the stream
        // entirely, and name no register a mask covers.
        let ProgramFacts {
            bit_only,
            bool_input,
            no_nulls,
            reg_u64,
        } = analyze(&self.instrs, schema, self.result_reg, role == Role::Filter);
        // Answered once per register by `analyze`, off each opcode's `U64Rule`,
        // so no arm below restates the rule.
        let is_u64 = |r: u16| (reg_u64 >> r) & 1 != 0;
        // Off the schema alone, so a column-reading opcode gets the NOT NULL
        // collapse without wiring anything of its own. A PK column has no
        // payload slot and so contributes no bit, which is the same rule that
        // makes `LoadPk`'s destination unconditionally non-null.
        let nullable_slots = schema.nullable_payload_slots();
        for li in self.instrs {
            // The two output sinks never reach the kernel dispatch: they name an
            // output slot, not a computation, and the map materializes them
            // columnar-side off `copies`/`emits`. Diverting them here is inert —
            // `validate`'s output-slot coverage is a popcount, so it does not
            // depend on their position in the stream.
            match li {
                L::CopyCol { src_col, out } => {
                    // The same destination type code `check_copy_types` approved
                    // the widening against; its `wire_stride` is the copy width.
                    let stride = out_schema.map_or(0, |os| {
                        gnitz_wire::wire_stride(os.col_type_code(os.payload_col_idx(out as usize))) as u8
                    });
                    copies.push((schema.locate(src_col as usize), out, stride));
                    continue;
                }
                L::Emit { src, out } => {
                    emits.push((src, out));
                    continue;
                }
                _ => {}
            }
            // One logical instruction, one resolved instruction.
            instrs.push(match li {
                L::LoadColInt { dst, col } => {
                    let loc = schema.locate(col as usize);
                    // Total on a validated program: `validate` runs
                    // `check_col(.., ColKind::FixedIntCol)` on every `LoadColInt`,
                    // and that predicate is `gnitz_wire::is_fixed_int` — the same
                    // eight codes `from_type_code` answers `Some` for. It covers
                    // the PK arm too (`ColKind::FixedIntCol` is not payload-only, so
                    // a U128/UUID PK is rejected as `ColKindMismatch`), which is
                    // what makes the kernel's wide-column wildcard unnecessary.
                    let fi = FixedInt::from_type_code(TypeCode::from_validated_u8(loc.type_code()))
                        .expect("validated LoadColInt names a fixed-int column");
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
                L::IntAdd { dst, a, b } => I::IntAdd { dst, a, b },
                L::IntSub { dst, a, b } => I::IntSub { dst, a, b },
                L::IntMul { dst, a, b } => I::IntMul { dst, a, b },
                L::IntDiv { dst, a, b } => I::IntDiv {
                    dst,
                    a,
                    b,
                    signed: !is_u64(dst),
                },
                L::IntMod { dst, a, b } => I::IntMod {
                    dst,
                    a,
                    b,
                    signed: !is_u64(dst),
                },
                L::FloatAdd { dst, a, b } => I::FloatAdd { dst, a, b },
                L::FloatSub { dst, a, b } => I::FloatSub { dst, a, b },
                L::FloatMul { dst, a, b } => I::FloatMul { dst, a, b },
                L::FloatDiv { dst, a, b } => I::FloatDiv { dst, a, b },
                L::Cmp { op, dst, a, b } => {
                    // EQ/NE are bit-identical signed/unsigned; ordered compares
                    // pick the unsigned form when either operand is U64.
                    let signed = matches!(op, CmpOp::Eq | CmpOp::Ne) || !(is_u64(a) || is_u64(b));
                    I::Cmp { op, dst, a, b, signed }
                }
                L::FCmp { op, dst, a, b } => I::FCmp { op, dst, a, b },
                L::FloatUnary { op, dst, a } => I::FloatUnary { op, dst, a },
                L::IntUnary { op, dst, a } => I::IntUnary { op, dst, a },
                L::FloatToF32 { dst, a } => I::FloatToF32 { dst, a },
                // Total on a validated program, the `LoadColInt` shape above:
                L::FloatToInt { dst, a, fi } => I::FloatToInt { dst, a, fi },
                L::IntCast { dst, a, fi } => I::IntCast {
                    dst,
                    a,
                    fi,
                    src_signed: !is_u64(a),
                },
                L::IntMinMax2 { dst, a, b, is_max } => I::IntMinMax2 {
                    dst,
                    a,
                    b,
                    is_max,
                    signed: !is_u64(dst),
                },
                L::FloatMinMax2 { dst, a, b, is_max } => I::FloatMinMax2 { dst, a, b, is_max },
                L::IntToFloat { dst, a } => I::IntToFloat {
                    dst,
                    a,
                    signed: !is_u64(a),
                },
                L::Select { dst, cond, a, b } => I::Select { dst, cond, a, b },
                L::LoadNull { dst } => I::LoadNull { dst },
                L::BoolAnd { dst, a, b } => I::BoolAnd { dst, a, b },
                L::BoolOr { dst, a, b } => I::BoolOr { dst, a, b },
                L::BoolNot { dst, a } => I::BoolNot { dst, a },
                L::IsNull { dst, col, invert } => I::IsNull {
                    dst,
                    pi: payload_slot(col as usize),
                    invert,
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
                    // Decoded once per compile, never per row — and by an
                    // immutable read (not `mem::take`), because nothing enforces
                    // one-const-index-per-opcode: a forged blob that
                    // shares an index between two opcodes must stay inert, not
                    // corrupt the other's slot. Unlike `StrColConst`/`StrTrim`/
                    // `LoadConstStr`, which keep a slot table so a shared index
                    // decodes once, this needs none: `add_const_bytes` never
                    // dedups, so no program the tree emits can share a `set_idx`,
                    // and decoding a forged one twice is harmless.
                    let mut set = decode_int_set(&self.const_strings[set_idx as usize]);
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
                L::LoadColStr { dst, col } => {
                    let pi = payload_slot(col as usize);
                    I::LoadColStr {
                        dst,
                        pi,
                        buf: crate::batch::str_col_slot(&mut str_cols, pi),
                    }
                }
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
                    start_signed: !is_u64(start_reg),
                    len_signed: len_reg.is_none_or(|l| !is_u64(l)),
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
                        mode,
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
                    like_matchers.push(LikeMatcher::compile(pattern, escape, ci));
                    I::StrLike { dst, src, matcher_idx }
                }
                L::StrConcat { dst, a, b, skip_null } => I::StrConcat { dst, a, b, skip_null },
                L::IntToStr { dst, a } => I::IntToStr {
                    dst,
                    a,
                    signed: !is_u64(a),
                },
                L::FloatToStr { dst, a } => I::FloatToStr { dst, a },
                L::StrToInt { dst, a, fi } => I::StrToInt { dst, a, fi },
                L::StrToFloat { dst, a } => I::StrToFloat { dst, a },
                // Diverted above; every other variant lowers to an instruction.
                L::CopyCol { .. } | L::Emit { .. } => unreachable!(),
            });
        }
        // Stable-partition by register class, so each writer reads a slice
        // instead of re-testing per entry. Safe because `validate` requires one
        // writer per output slot, so emit order carries no meaning.
        let (mut emits, str_emits): (Vec<_>, Vec<_>) =
            emits.into_iter().partition(|&(src, _)| (str_class >> src) & 1 == 0);
        let str_emit_start = emits.len();
        emits.extend(str_emits);
        ResolvedProgram {
            copies,
            emits,
            str_emit_start,
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
            str_class,
            str_cols,
            role,
        }
    }

    /// Validate as a filter predicate: the schema pass plus the two rules only a
    /// filter has.
    ///
    /// A filter reads `result_reg` out of `regs`/`bool_bits`, so it must own one
    /// ([`Self::validate_result_reg`]) and that one must not be a string
    /// register — a string `result_reg` would filter rows on recycled scratch.
    /// The string rule is the filter's alone: `resolve_scalar` *wants* a
    /// string-valued result, and reads it through `eval_row_str`.
    pub(crate) fn validate_predicate(&self, schema: &dyn SchemaFacts) -> Result<(), ExprValidateErr> {
        self.validate_result_reg(schema)?;
        if (self.str_class >> self.result_reg) & 1 != 0 {
            return Err(ExprValidateErr::RegClassMismatch {
                reg: self.result_reg as u16,
            });
        }
        Ok(())
    }

    /// [`Self::validate`] plus the rule the filter and scalar roles share: a
    /// program whose result is read back out of a register must own one.
    /// `validate` cannot apply it — a register-free program is exactly the shape
    /// every `copy_cols` map has, and a map writes output slots instead.
    pub(crate) fn validate_result_reg(&self, schema: &dyn SchemaFacts) -> Result<(), ExprValidateErr> {
        self.validate(schema, None)?;
        if self.num_regs == 0 {
            return Err(ExprValidateErr::ResultRegRequired);
        }
        Ok(())
    }

    /// Hold every column and output operand to the schema the program will run
    /// against, over the same per-opcode operand table [`Self::from_instrs`]
    /// walks — so a new opcode cannot silently bypass a bound here either.
    ///
    /// `out_schema` is `None` for the roles that write no output slots — a
    /// filter and a scalar, whose output opcodes are eval no-ops. With one, this
    /// also decides **output coverage**: every declared payload slot must be
    /// written exactly once, since a map's output batch is provisioned
    /// uninitialized and an unwritten slot ships recycled bytes.
    pub(crate) fn validate(
        &self,
        in_schema: &dyn SchemaFacts,
        out_schema: Option<&dyn SchemaFacts>,
    ) -> Result<(), ExprValidateErr> {
        // Bit per written output payload slot. `check_out_slot` bounds every
        // `out` below `num_payload_cols() <= 64`, so no shift can overflow.
        let mut written = 0u64;
        for instr in &self.instrs {
            let ops = operands(instr);
            // Ahead of the output arm: `check_copy_types` indexes the schema
            // with `src_col`, so the bound must already hold there.
            for &(col, kind) in ops.cols.iter().flatten() {
                check_col(in_schema, col, kind)?;
            }
            if let (Some(os), Some((out, rule))) = (out_schema, ops.out) {
                written |= check_out_slot(in_schema, os, out, rule, self.str_class)?;
            }
        }
        // Popcount rather than length, so a duplicate destination is caught too:
        // it leaves another slot's bit clear.
        if let Some(os) = out_schema {
            if written.count_ones() as usize != os.num_payload_cols() {
                return Err(ExprValidateErr::OutputSlotUnwritten {
                    written,
                    num_payload_cols: os.num_payload_cols(),
                });
            }
        }
        Ok(())
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
    /// A scalar value, together with how the register inherits U64-ness.
    Value(U64Rule),
    /// A scalar whose i64 image is exactly its truth bit, 0 or 1. A reader may
    /// therefore take the packed `bool_bits` bit instead, and the kernel may
    /// skip writing `regs` when the register is `bit_only`. A mask-style `-1`
    /// for true is the violation this rules out.
    Bool,
    /// A German-string view.
    Str,
}

/// How a `Value` destination inherits **U64-ness** — whether the register's i64
/// image is to be read as a `u64`. It selects the unsigned variant of every div,
/// mod, ordered compare, min/max, int→float and int→text below, because a `u64`
/// at or above 2^63 has a negative i64 bit pattern.
///
/// Carried by [`WriteAs::Value`] rather than sitting beside it as an optional
/// field, so a new arithmetic opcode cannot leave it unstated — omission would
/// silently mean signed, a wrong answer rather than a compile error.
#[derive(Clone, Copy, PartialEq, Eq)]
enum U64Rule {
    /// The verdict the opcode already knows: `false` for a float image or an
    /// integer the opcode itself bounds below 2^63 (a length, a parse of a
    /// signed target), and for a cast, whether its target names U64.
    Fixed(bool),
    /// U64 iff any [`ReadAs::Value`] operand is. `ReadAs::Bool` operands are
    /// excluded — SELECT's condition is a truth bit, not part of its result.
    FromOperands,
    /// U64 iff the column operand's declared type code is U64.
    FromColType,
}

impl ReadAs {
    /// Whether this operand's register must hold a string, or `None` for the one
    /// read the construction pass does not hold to a class.
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
/// The **one** per-opcode table, read by the walks over the instruction stream
/// — `from_instrs`, `validate` and `analyze` — so a new opcode is classified
/// once: its operands, its NULL production and its destination's U64-ness alike.
/// Every arm destructures every field of every variant it matches, with `_` for
/// fields it ignores and **no `..`** — so an opcode that gains an operand is a
/// compile error here rather than an unbounded operand reaching the kernels.
struct Operands {
    /// `None` for the two output opcodes, which write no register.
    dst: Option<(u16, WriteAs)>,
    reads: [Option<(u16, ReadAs)>; MAX_READS],
    cols: [Option<(u32, ColKind)>; MAX_COL_OPERANDS],
    /// The output payload slot this opcode writes, and which type rule its
    /// destination is held to. `None` for everything but the two output opcodes.
    out: Option<(u32, OutRule)>,
    /// A value the opcode carries that is neither a register nor a column, but
    /// still reaches a panicking / OOB / truncating site if forged.
    extra: Extra,
    /// True iff the kernel can turn non-NULL input into a NULL result — a zero
    /// divisor, an out-of-range cast, an unparsable text→number, a CONCAT past
    /// `u32::MAX`, `LoadNull*` on every row. A column operand's own nullability
    /// is `cols`, not this.
    makes_null: bool,
}

/// Which type rule an output opcode's destination payload slot is held to.
#[derive(Clone, Copy)]
enum OutRule {
    /// A verbatim column copy: the slot must hold `src_col` unchanged.
    Copy { src_col: u32 },
    /// A register store: the slot must match `src`'s register class and width.
    Emit { src: u16 },
}

/// A per-opcode const-pool index — client-controlled, and bounded against the
/// pool by [`LogicalProgram::from_instrs`] before a kernel reads it. Stated in
/// the operand table beside the opcode's registers and columns, so classifying a
/// new opcode is one edit rather than two.
#[derive(Clone, Copy)]
enum Extra {
    None,
    /// A const-pool index.
    ConstIdx(u32),
    /// An `INT_IN_SET` value pool: a const index whose entry must be whole i64s.
    IntSet(u32),
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
        out: None,
        extra: Extra::None,
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

    /// Record the output payload slot and the rule its destination is held to.
    fn writing_out(mut self, out: u32, rule: OutRule) -> Self {
        self.out = Some((out, rule));
        self
    }

    /// Record a client-controlled const-pool index the construction pass must
    /// bound.
    fn with_extra(mut self, extra: Extra) -> Self {
        self.extra = extra;
        self
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
    use LogicalInstr as L;
    use ReadAs::{Bool as RBool, OutputClass, Str as RStr, Value as RVal};
    use U64Rule::{Fixed, FromColType, FromOperands};
    use WriteAs::{Bool as WBool, Str as WStr, Value as WVal};
    match *li {
        // --- Two scalar registers in, one scalar value out ---
        // Split int from float on the U64 rule alone: a pure int transform keeps
        // its operands' width and signedness, an f64 image has none to keep.
        L::IntAdd { dst, a, b }
        | L::IntSub { dst, a, b }
        | L::IntMul { dst, a, b }
        | L::IntMinMax2 { dst, a, b, is_max: _ } => writes(dst, WVal(FromOperands)).reading(a, RVal).reading(b, RVal),
        L::FloatAdd { dst, a, b }
        | L::FloatSub { dst, a, b }
        | L::FloatMul { dst, a, b }
        | L::FloatMinMax2 { dst, a, b, is_max: _ } => writes(dst, WVal(Fixed(false))).reading(a, RVal).reading(b, RVal),
        // A zero divisor yields NULL.
        L::IntDiv { dst, a, b } | L::IntMod { dst, a, b } => writes(dst, WVal(FromOperands))
            .reading(a, RVal)
            .reading(b, RVal)
            .may_null(),
        L::FloatDiv { dst, a, b } => writes(dst, WVal(Fixed(false)))
            .reading(a, RVal)
            .reading(b, RVal)
            .may_null(),
        L::Cmp { op: _, dst, a, b } | L::FCmp { op: _, dst, a, b } => {
            writes(dst, WBool).reading(a, RVal).reading(b, RVal)
        }
        L::BoolAnd { dst, a, b } | L::BoolOr { dst, a, b } => writes(dst, WBool).reading(a, RBool).reading(b, RBool),
        L::BoolNot { dst, a } => writes(dst, WBool).reading(a, RBool),
        L::IntUnary { op: _, dst, a } => writes(dst, WVal(FromOperands)).reading(a, RVal),
        L::FloatUnary { op: _, dst, a } | L::IntToFloat { dst, a } => writes(dst, WVal(Fixed(false))).reading(a, RVal),
        // The three narrowing casts yield NULL on an out-of-range value.
        L::FloatToF32 { dst, a } => writes(dst, WVal(Fixed(false))).reading(a, RVal).may_null(),
        L::FloatToInt { dst, a, fi } | L::IntCast { dst, a, fi } => writes(dst, WVal(Fixed(fi == FixedInt::U64)))
            .reading(a, RVal)
            .may_null(),
        // `cond` is a truth bit; the branches are values. `dst` is a value
        // write, not a boolean one — see `WriteAs::Bool`.
        L::Select { dst, cond, a, b } => writes(dst, WVal(FromOperands))
            .reading(cond, RBool)
            .reading(a, RVal)
            .reading(b, RVal),
        L::IntInSet {
            dst,
            value_reg,
            set_idx,
        } => writes(dst, WBool)
            .reading(value_reg, RVal)
            .with_extra(Extra::IntSet(set_idx)),
        L::LoadConst { dst, val: _ } => writes(dst, WVal(Fixed(false))),
        // A NULL on every row.
        L::LoadNull { dst } => writes(dst, WVal(Fixed(false))).may_null(),

        // --- Column operands ---
        // The integer load kernels decode only the eight fixed-width integer
        // codes. `ColKind::FixedIntCol` is not payload-only, so this is the one
        // column operand that may name a PK column.
        L::LoadColInt { dst, col } => writes(dst, WVal(FromColType)).on_col(col, ColKind::FixedIntCol),
        // The float load kernel branches on width alone, so a 1- or 2-byte
        // integer column would make it slice an 8-byte stride from a narrower
        // region.
        L::LoadColFloat { dst, col } => writes(dst, WVal(Fixed(false))).on_col(col, ColKind::FloatPayload),
        // The null-bitmap readers decode no value, so `AnyPayload` admits U128 and
        // STRING, and their boolean is definite over a NULL row.
        L::IsNull { dst, col, invert: _ } => writes(dst, WBool).on_col(col, ColKind::AnyPayload),
        // The German-string compares read 16-byte cells; a narrower column
        // would make `col_data(pi, 16)` over-read its region. The verdict is a
        // bool, but a NULL operand makes it NULL, so the null bit flows.
        L::StrColConst {
            op: _,
            dst,
            col,
            const_idx,
        } => writes(dst, WBool)
            .on_col(col, ColKind::StringPayload)
            .with_extra(Extra::ConstIdx(const_idx)),
        L::StrColCol {
            op: _,
            dst,
            col_a,
            col_b,
        } => writes(dst, WBool)
            .on_col(col_a, ColKind::StringPayload)
            .on_col(col_b, ColKind::StringPayload),
        // The string-register column load reads the same 16-byte cells the
        // `ExprOp::StrCol*` compares do, so it carries the same requirement.
        L::LoadColStr { dst, col } => writes(dst, WStr).on_col(col, ColKind::StringPayload),

        // --- String registers ---
        L::LoadConstStr { dst, const_idx } => writes(dst, WStr).with_extra(Extra::ConstIdx(const_idx)),
        // A NULL on every row.
        L::LoadNullStr { dst } => writes(dst, WStr).may_null(),
        L::IntToStr { dst, a } | L::FloatToStr { dst, a } => writes(dst, WStr).reading(a, RVal),
        L::StrLen { dst, a, chars: _ } => writes(dst, WVal(Fixed(false))).reading(a, RStr),
        // Both text→number parses yield NULL on an unparsable or out-of-range
        // value.
        L::StrToFloat { dst, a } => writes(dst, WVal(Fixed(false))).reading(a, RStr).may_null(),
        L::StrToInt { dst, a, fi } => writes(dst, WVal(Fixed(fi == FixedInt::U64)))
            .reading(a, RStr)
            .may_null(),
        L::StrCmp { op: _, dst, a, b } => writes(dst, WBool).reading(a, RStr).reading(b, RStr),
        // One string operand plus compile-time pattern data. The verdict is a
        // definite 0/1 — LIKE introduces no NULL of its own, so the operand's
        // null bit is the only one.
        L::StrLike {
            dst,
            src,
            escape: _,
            pat_idx,
            ci: _,
        } => writes(dst, WBool)
            .reading(src, RStr)
            .with_extra(Extra::ConstIdx(pat_idx)),
        L::StrCase { dst, a, upper: _ } => writes(dst, WStr).reading(a, RStr),
        L::StrTrim {
            dst,
            a,
            mode: _,
            set_idx,
        } => writes(dst, WStr).reading(a, RStr).with_extra(Extra::ConstIdx(set_idx)),
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
        L::CopyCol { src_col, out } => nothing()
            .on_col(src_col, ColKind::AnyCol)
            .writing_out(out, OutRule::Copy { src_col }),
        L::Emit { src, out } => nothing()
            .reading(src, OutputClass)
            .writing_out(out, OutRule::Emit { src }),
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

/// The cast opcodes' target word as the fixed-int it names. A forged code must
/// not reach eval, where it would index a bounds table that has no arm for it.
fn cast_target(tc: u32) -> Result<FixedInt, ExprValidateErr> {
    u8::try_from(tc)
        .ok()
        .and_then(TypeCode::try_from_u8)
        .and_then(FixedInt::from_type_code)
        .ok_or(ExprValidateErr::BadCastTarget { tc })
}

/// `StrTrim`'s mode word as the mode it names.
fn trim_mode(mode: u32) -> Result<TrimMode, ExprValidateErr> {
    TrimMode::from_wire(mode).ok_or(ExprValidateErr::BadTrimMode { mode })
}

/// `StrLike`'s escape word as the escape character it names, `None` for the 0
/// that disables escaping. The escape shares a word with the source register, so
/// the half above a byte must be clear.
fn like_escape(escape: u32) -> Result<Option<u8>, ExprValidateErr> {
    if escape > u8::MAX as u32 {
        return Err(ExprValidateErr::BadLikeEscape { escape });
    }
    Ok((escape != 0).then_some(escape as u8))
}

/// The one column-operand check: range, then payload-ness, then the type class
/// the opcode's kernel can decode — in that order, so a stronger requirement can
/// never be tested against an unbounded index. The last two keep separate
/// diagnostics because they are reachable on disjoint inputs: no PK-eligible
/// type is a float or a German string.
///
/// The bound is `num_columns()`, not `MAX_COLUMNS`: the `[num_columns, 65)` zone
/// reads a zeroed schema slot. The kernels dispatch on a column's type without
/// re-checking it, so this is the only place a client blob is held to the
/// contract.
fn check_col(s: &dyn SchemaFacts, col: u32, need: ColKind) -> Result<(), ExprValidateErr> {
    if col as usize >= s.num_columns() {
        return Err(ExprValidateErr::ColOutOfRange {
            col,
            num_columns: s.num_columns(),
        });
    }
    if need.payload_only() && s.is_pk_col(col as usize) {
        return Err(ExprValidateErr::ColNotPayload { col });
    }
    if let Some((accepts, want)) = need.type_test() {
        let type_code = s.col_type_code(col as usize);
        if !accepts(type_code) {
            return Err(ExprValidateErr::ColKindMismatch { col, type_code, want });
        }
    }
    Ok(())
}

/// One output opcode's destination payload slot: bounded against the output
/// width, then held to the type rule its opcode selected. Returns the coverage
/// bit it claims, so the caller accumulates a value instead of sharing a mutable
/// one with a helper that may or may not write it.
fn check_out_slot(
    in_schema: &dyn SchemaFacts,
    out_schema: &dyn SchemaFacts,
    out: u32,
    rule: OutRule,
    str_class: u64,
) -> Result<u64, ExprValidateErr> {
    if out as usize >= out_schema.num_payload_cols() {
        return Err(ExprValidateErr::OutputIdxOutOfRange {
            out,
            num_payload_cols: out_schema.num_payload_cols(),
        });
    }
    match rule {
        OutRule::Copy { src_col } => check_copy_types(in_schema, out_schema, src_col, out)?,
        // EMIT's source class picks which slot rule applies.
        OutRule::Emit { src } => check_emit_slot(out_schema, out, (str_class >> src) & 1 != 0)?,
    }
    Ok(1u64 << out)
}

/// A COPY_COL destination slot must hold its source verbatim. `copy_column`
/// byte-copies at equal width and otherwise `widen_native_le`s a narrower integer
/// into a wider slot — there is no narrowing and no representation change.
fn check_copy_types(
    in_schema: &dyn SchemaFacts,
    out_schema: &dyn SchemaFacts,
    src_col: u32,
    out: u32,
) -> Result<(), ExprValidateErr> {
    let src_tc = in_schema.col_type_code(src_col as usize);
    let out_tc = out_schema.col_type_code(out_schema.payload_col_idx(out as usize));
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
fn check_emit_slot(out_schema: &dyn SchemaFacts, out: u32, is_str: bool) -> Result<(), ExprValidateErr> {
    // `col_type_code`, like its two sibling checks — not `locate`, whose extra
    // work (a release-active bound assert, plus an O(pk_count) OPK-offset walk
    // for a PK column) buys nothing here: `size()` IS `wire_stride(type_code)`.
    let type_code = out_schema.col_type_code(out_schema.payload_col_idx(out as usize));
    if is_str != gnitz_wire::is_german_string(type_code) {
        return Err(ExprValidateErr::EmitClassMismatch { out, type_code });
    }
    if !is_str && (!gnitz_wire::is_valid_type_code(type_code) || gnitz_wire::wire_stride(type_code) != 8) {
        return Err(ExprValidateErr::EmitSlotNotEightBytes { out, type_code });
    }
    Ok(())
}

/// The `ExprOp::IntInSet` const-pool layout, `N × 8-byte LE`, stated once for
/// [`LogicalProgram::from_instrs`] and the decoder below.
/// `gnitz_wire::ExprOp::IntInSet` owns the wire contract; the emitter writes it
/// with `gnitz_wire::as_le_bytes`.
fn int_set_len_ok(len: usize) -> bool {
    len.is_multiple_of(8)
}

fn decode_int_set(bytes: &[u8]) -> Vec<i64> {
    debug_assert!(int_set_len_ok(bytes.len()), "construction rejects a misaligned pool");
    bytes
        .chunks_exact(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

// ---------------------------------------------------------------------------
// ResolvedProgram — the evaluable form
// ---------------------------------------------------------------------------

pub(crate) struct ResolvedProgram {
    pub(crate) instrs: Vec<Instr>,
    /// A map's verbatim column moves, as `(source locator, output payload slot,
    /// destination write width)`. Off the instruction stream, not in it: they
    /// name a destination rather than a computation, so keeping them in the
    /// stream would give the per-morsel dispatch up to one no-op arm per
    /// projected column.
    ///
    /// The width is the *output* column's, wider than the source's only for a
    /// promoted integer column — the widening `check_copy_types` approved. `0`
    /// for a role resolved without an output schema, which never reads these.
    pub(crate) copies: Vec<(ColumnLocator, u32, u8)>,
    /// A map's computed columns, as `(source register, output payload slot)`,
    /// with the scalar-register entries first and the string-register ones from
    /// [`Self::str_emit_start`] on. The class a consumer needs is not stored
    /// beside them: the partition point is read off [`Self::str_class`], the one
    /// record of a register's class.
    pub(crate) emits: Vec<(u16, u32)>,
    /// Where [`Self::emits`]' string half starts.
    str_emit_start: usize,
    pub(crate) num_regs: u32,
    /// The register holding the filter verdict, or the scalar result. A map's is
    /// meaningless and never read; [`LogicalProgram::from_map_blob`] is where
    /// that is stated once, rather than at each map's construction site.
    pub(crate) result_reg: u32,
    /// The 16-byte German-string cells, indexed by the resolved `cell_idx`,
    /// with any heap half in `const_arena`. Only the constants a `StrColConst`
    /// names get one — encoded once at resolve, compared by
    /// `gnitz_wire::compare_german_strings`. `cell_idx` is in range because
    /// `resolve` hands it back from the same push, not because anything bounds
    /// it: it is not a const-pool index and no validating pass sees it.
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
    /// The payload slots of the string columns this program holds views into, in
    /// the buffer-slot order `batch::str_col_slot` assigned. A drive resolves one
    /// column region per entry.
    pub(crate) str_cols: Vec<u8>,
    /// [`LogicalProgram::str_class`], carried over rather than re-derived: it is
    /// the one record behind [`Self::str_lanes`], [`Self::result_is_str`] and
    /// where [`Self::emits`] splits.
    str_class: u64,
    /// What this program is for. Read only to decide whether `result_reg` means
    /// anything — a map's is force-zeroed.
    role: Role,
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

    /// How many string register lanes the scratch must hold: one past the
    /// highest string register, or 0 for a program with none. Lanes are
    /// register-major and addressed as `str_views[reg * MORSEL + row]`, so
    /// nothing above the highest string register is ever touched — sizing by
    /// `num_regs` would reserve 4 KiB per register for lanes that cannot be read.
    pub(crate) fn str_lanes(&self) -> u32 {
        MAX_REGS as u32 - self.str_class.leading_zeros()
    }

    /// True iff `result_reg` holds a string — i.e. the result must be read
    /// through [`crate::Evaluator::eval_row_str`], not `eval_row`. Only a scalar
    /// can answer true: a map's `result_reg` is force-zeroed and names no
    /// result, and `validate_predicate` rejects a string-valued filter.
    pub(crate) fn result_is_str(&self) -> bool {
        self.role != Role::Map && (self.str_class >> self.result_reg) & 1 != 0
    }

    /// The emits whose source register holds a scalar.
    pub(crate) fn scalar_emits(&self) -> &[(u16, u32)] {
        &self.emits[..self.str_emit_start]
    }

    /// The emits whose source register holds a string.
    pub(crate) fn str_emits(&self) -> &[(u16, u32)] {
        &self.emits[self.str_emit_start..]
    }
}

/// What [`analyze`] derives in its one pass over the instruction stream.
struct ProgramFacts {
    /// Bit `r` set iff `r` is only consumed by boolean ops, so its producer can
    /// skip the i64 unpack into `regs[r]`.
    bit_only: u64,
    /// Bit `r` set iff a boolean consumer reads `r`, so its producer must
    /// populate `bool_bits[r]`.
    bool_input: u64,
    /// True iff no instruction can produce a NULL against the schema, so the
    /// evaluator skips null-bit tracking entirely.
    no_nulls: bool,
    /// Bit `r` set iff register `r`'s i64 image is to be read as a `u64`, per
    /// each opcode's [`U64Rule`]. Final rather than running, and safe to read at
    /// any point for the reason [`LogicalProgram::str_class`] gives.
    reg_u64: u64,
}

/// Derive every per-register and per-program fact `resolve_program` needs, in one
/// pass over the one operand table:
///
/// * **Register roles.** A filter's `result_reg` is forced to be a bool input:
///   the filter's nullable arm consumes the result as packed bits, so its
///   producer must populate `bool_bits` whatever opcode it is. A map's result
///   register needs no such force — `Emit` already marks its source non-bool —
///   and a bare scalar's may legitimately stay `bit_only`.
/// * **Strict non-nullability**, against the schema the program is about to be
///   resolved against, the only schema for which the answer means anything. A PK
///   column operand never contributes: the null bitmap is payload-indexed, so the
///   PK region carries no null bit for a load to inherit.
///
/// Every `1u64 << reg` below is in range: `LogicalProgram::new` asserts
/// `num_regs <= MAX_REGS` and bounds every register operand by `num_regs`.
fn analyze(instrs: &[LogicalInstr], schema: &dyn SchemaFacts, result_reg: u32, is_filter: bool) -> ProgramFacts {
    let (mut bool_produced, mut non_bool_read, mut bool_input) = (0u64, 0u64, 0u64);
    let mut no_nulls = true;
    let mut reg_u64 = 0u64;
    for li in instrs {
        let ops = operands(li);
        match ops.dst {
            Some((dst, WriteAs::Bool)) => bool_produced |= 1u64 << dst,
            // In stream order, which is what `U64Rule::FromOperands` needs: a
            // destination's U64-ness is a function of registers earlier
            // instructions wrote.
            Some((dst, WriteAs::Value(rule))) => reg_u64 |= (u64_verdict(rule, &ops, schema, reg_u64) as u64) << dst,
            Some((_, WriteAs::Str)) | None => {}
        }
        for &(reg, read) in ops.reads.iter().flatten() {
            let bit = 1u64 << reg;
            if read == ReadAs::Bool {
                bool_input |= bit;
            } else {
                non_bool_read |= bit;
            }
        }
        no_nulls &= !ops.makes_null
            && !ops.cols.iter().flatten().any(|&(col, kind)| {
                kind.carries_null_to_register() && !schema.is_pk_col(col as usize) && schema.col_nullable(col as usize)
            });
    }
    if is_filter {
        bool_input |= 1u64 << result_reg as usize;
    }
    ProgramFacts {
        bit_only: bool_produced & !non_bool_read,
        bool_input,
        no_nulls,
        reg_u64,
    }
}

/// Whether one opcode's `Value` destination holds a `u64`, per its [`U64Rule`].
/// `so_far` is the mask over the registers already written, which is every
/// register this opcode can read.
fn u64_verdict(rule: U64Rule, ops: &Operands, schema: &dyn SchemaFacts, so_far: u64) -> bool {
    match rule {
        U64Rule::Fixed(v) => v,
        U64Rule::FromOperands => ops
            .reads
            .iter()
            .flatten()
            .any(|&(reg, read)| read == ReadAs::Value && (so_far >> reg) & 1 != 0),
        // `col_type_code` and `locate(..).type_code()` are held equal by
        // `assert_schema_facts_matrix`, so this is the locator's own code.
        U64Rule::FromColType => ops
            .cols
            .iter()
            .flatten()
            .any(|&(col, _)| schema.col_type_code(col as usize) == gnitz_wire::type_code::U64),
    }
}

#[cfg(test)]
#[path = "tests/program.rs"]
mod tests;
