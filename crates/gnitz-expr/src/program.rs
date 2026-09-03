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
use std::num::NonZeroU8;

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
    OutputSlotCountMismatch {
        sinks: usize,
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
            Self::AnyCol => None,
            Self::FixedIntCol => Some((gnitz_wire::is_fixed_int, "a fixed-width integer column")),
            Self::FloatPayload => Some((gnitz_wire::is_float, "a floating-point column")),
            Self::StringPayload => Some((gnitz_wire::is_german_string, "a string or blob column")),
        }
    }

    /// True iff a PK column is unusable here.
    fn payload_only(self) -> bool {
        matches!(self, Self::FloatPayload | Self::StringPayload)
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
    Sqrt,
    Ln,
    Log10,
    Exp,
    /// -1.0 / 0.0 / 1.0, and NaN for NaN.
    Sign,
}

/// Pure integer unary transform: same width in, same width out. `Neg` and `Abs`
/// are `wrapping_*`, so `-i64::MIN` and `ABS(i64::MIN)` are `i64::MIN`, and
/// the operand's U64 tracking carries to their result; `Sign` is -1 / 0 / 1,
/// always signed, and reads an unsigned operand as never negative.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntUnaryOp {
    Neg,
    Abs,
    Sign,
}

/// Integer arithmetic operator. Parallel to [`FloatArithOp`] rather than shared
/// with it: `Mod` has no float form, and a shared enum admitting float-`Mod`
/// would force an unreachable arm into `to_wire`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntArithOp {
    Add,
    Sub,
    Mul,
    /// A zero divisor yields NULL.
    Div,
    /// A zero divisor yields NULL.
    Mod,
}

/// IEEE-754 arithmetic operator — [`IntArithOp`]'s float twin, minus `Mod`,
/// plus `Pow`, which has no integer form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FloatArithOp {
    Add,
    Sub,
    Mul,
    /// A zero divisor yields NULL.
    Div,
    /// `powf`: the IEEE result, never NULL.
    Pow,
}

/// [`IntArithOp`] as resolution leaves it. `signed` sits on the two operators
/// that read it rather than on the enclosing instruction, where Add/Sub/Mul
/// would have to be given a value it means nothing for.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ResolvedIntOp {
    Add,
    Sub,
    Mul,
    /// A zero divisor yields NULL.
    Div {
        signed: bool,
    },
    /// A zero divisor yields NULL.
    Mod {
        signed: bool,
    },
}

// ---------------------------------------------------------------------------
// LogicalInstr — the wire-mirroring form (logical column indices)
// ---------------------------------------------------------------------------

/// A register, named by the index of the instruction that writes it. Single
/// assignment and read-before-write already forced the two numbers to be equal,
/// so only one of them is stored and a forged register file is unrepresentable.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Reg(pub u16);

impl Reg {
    /// True iff instruction `i` may read this register: its one writer is the
    /// instruction at its own index, so only an earlier index has been written.
    /// The one comparison that covers range, self-reference and forward
    /// reference alike.
    fn written_before(self, i: usize) -> bool {
        (self.0 as usize) < i
    }
}

/// An index into a program's const pool.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ConstIdx(pub u32);

/// One output payload slot of a map, in slot order: `sinks[i]` writes slot `i`.
/// Position *is* the destination, so an unwritten or twice-written slot cannot
/// be expressed — where an unwritten one used to ship the uninitialized output
/// batch's recycled bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Sink {
    /// Copy an input column verbatim. No type operand: the engine resolves the
    /// source locator (PK byte window or dense payload slot) and both widths
    /// from the schemas it validates the program against, so a restated type
    /// code could only ever disagree with them.
    Col(u32),
    /// Store a register. Its class picks the destination-column rule, and is
    /// read off `str_class` rather than restated here.
    Reg(Reg),
}

impl Sink {
    /// The register this sink stores — the sink half of [`operands`], so every
    /// walk over a program reads a sink's operand off one table rather than
    /// spelling the match again.
    fn reg(self) -> Option<Reg> {
        match self {
            Sink::Col(_) => None,
            Sink::Reg(r) => Some(r),
        }
    }

    /// The input column this sink copies verbatim, with what the copy requires
    /// of it. The column half of [`Self::reg`].
    fn on_col(self) -> Option<(u32, ColKind)> {
        match self {
            // The copy is columnar and bypasses the register file, so any source
            // column will do and no register takes its null bit.
            Sink::Col(src_col) => Some((src_col, ColKind::AnyCol)),
            Sink::Reg(_) => None,
        }
    }
}

/// One instruction with logical (schema) column indices, mirroring the wire
/// opcodes the client emits. `LogicalProgram::resolve` lowers each into `Instr`.
///
/// Every variant writes the register named by its own position, so none carries
/// a destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalInstr {
    LoadColInt {
        col: u32,
    },
    LoadColFloat {
        col: u32,
    },
    LoadConst {
        val: i64,
    },
    IntArith {
        op: IntArithOp,
        a: Reg,
        b: Reg,
    },
    FloatArith {
        op: FloatArithOp,
        a: Reg,
        b: Reg,
    },
    Cmp {
        op: CmpOp,
        a: Reg,
        b: Reg,
    },
    FCmp {
        op: CmpOp,
        a: Reg,
        b: Reg,
    },
    IntToFloat {
        a: Reg,
    },
    FloatUnary {
        op: FloatUnaryOp,
        a: Reg,
    },
    IntUnary {
        op: IntUnaryOp,
        a: Reg,
    },
    FloatToInt {
        a: Reg,
        fi: FixedInt,
    },
    IntCast {
        a: Reg,
        fi: FixedInt,
    },
    FloatToF32 {
        a: Reg,
    },
    IntMinMax2 {
        a: Reg,
        b: Reg,
        is_max: bool,
    },
    FloatMinMax2 {
        a: Reg,
        b: Reg,
        is_max: bool,
    },
    /// SQL CASE blend: takes `a`'s value + null bit where `cond` is non-NULL and
    /// truthy, else `b`'s. Carries a value, never a boolean.
    Select {
        cond: Reg,
        a: Reg,
        b: Reg,
    },
    /// An always-NULL value (0, null bit set): CASE without ELSE, NULLIF's match
    /// branch.
    LoadNull,
    /// Three-valued AND, or OR when `is_or` — one kernel, the operator carried
    /// as data.
    BoolBinary {
        a: Reg,
        b: Reg,
        is_or: bool,
    },
    BoolNot {
        a: Reg,
    },
    /// `IS NULL`, or `IS NOT NULL` when `invert`. The two read the same
    /// null-bitmap bit and differ only in its polarity, so they share one
    /// variant and one kernel — the `StrLen { chars }` / `StrCase { upper }`
    /// shape.
    IsNull {
        col: u32,
        invert: bool,
    },
    /// `IS NULL` (`IS NOT NULL` when `invert`) over a register's null lane. The
    /// operand may be of either class — only its null bit is read — which is
    /// what lets a computed value of any type be null-tested.
    IsNullReg {
        a: Reg,
        invert: bool,
    },
    StrColConst {
        op: CmpOp,
        col: u32,
        const_idx: ConstIdx,
    },
    StrColCol {
        op: CmpOp,
        col_a: u32,
        col_b: u32,
    },
    /// Integer set membership: `value_reg ∈ set[set_idx]`, over the packed
    /// sorted-i64 pool decoded once at `resolve`.
    IntInSet {
        value_reg: Reg,
        set_idx: ConstIdx,
    },
    LoadColStr {
        col: u32,
    },
    LoadConstStr {
        const_idx: ConstIdx,
    },
    /// An always-NULL string — the string-class twin of
    /// [`LogicalInstr::LoadNull`], which a string context must emit instead so
    /// its consumers' operand class matches.
    LoadNullStr,
    /// String CASE blend. `cond` is a scalar register; `a`/`b` and the result
    /// are string.
    StrSelect {
        cond: Reg,
        a: Reg,
        b: Reg,
    },
    /// String compare writing a boolean into a *scalar* register.
    StrCmp {
        op: CmpOp,
        a: Reg,
        b: Reg,
    },
    /// Length into a *scalar* register: bytes, or `chars` — the count of
    /// non-continuation bytes, which on valid UTF-8 is the codepoint count.
    StrLen {
        a: Reg,
        chars: bool,
    },
    /// ASCII-only case fold (`a-z`/`A-Z`); every other byte passes through, so a
    /// multibyte UTF-8 sequence is unchanged.
    StrCase {
        a: Reg,
        upper: bool,
    },
    /// Half-open character window `[start, start + len)`, 1-based, intersected
    /// with the string. `len_reg = None` runs to the end; a negative length is
    /// NULL.
    StrSubstr {
        src: Reg,
        start_reg: Reg,
        len_reg: Option<Reg>,
    },
    StrTrim {
        a: Reg,
        mode: TrimMode,
        set_idx: ConstIdx,
    },
    /// SQL LIKE writing a boolean into a *scalar* register. `pat_idx` is a
    /// const-pool index naming the raw pattern bytes and `escape` the escape
    /// character, `None` where escaping is disabled. `ci` is ILIKE's ASCII-only
    /// case folding, carried from the opcode.
    StrLike {
        src: Reg,
        escape: Option<NonZeroU8>,
        pat_idx: ConstIdx,
        ci: bool,
    },
    /// `skip_null` is CONCAT's asymmetric null rule: a NULL `b` contributes the
    /// empty string, a NULL `a` (the fold accumulator) still propagates. `false`
    /// is SQL `||` — NULL in either operand, NULL out.
    StrConcat {
        a: Reg,
        b: Reg,
        skip_null: bool,
    },
    IntToStr {
        a: Reg,
    },
    FloatToStr {
        a: Reg,
    },
    StrToInt {
        a: Reg,
        fi: FixedInt,
    },
    StrToFloat {
        a: Reg,
    },
    /// LEFT (`left`) / RIGHT: `n_reg` characters from one end, a negative count
    /// dropping that many from the other. A sub-view; never NULL of its own.
    StrSide {
        src: Reg,
        n_reg: Reg,
        left: bool,
    },
    /// 1-based character index of `needle` in `hay` into a *scalar* register:
    /// 0 when absent, 1 for an empty needle.
    StrPos {
        hay: Reg,
        needle: Reg,
    },
    /// The characters in reverse order, a fresh arena copy.
    StrReverse {
        a: Reg,
    },
    /// Every non-overlapping `from` in `s` replaced by `to`; an empty `from`
    /// leaves `s` unchanged. NULL past `u32::MAX` bytes, CONCAT's rule.
    StrReplace {
        s: Reg,
        from: Reg,
        to: Reg,
    },
    /// LPAD (`left`) / RPAD to `n_reg` characters with `fill` repeated; a longer
    /// `s` truncates, `n <= 0` is empty, an empty `fill` pads nothing. NULL past
    /// `u32::MAX` bytes.
    StrPad {
        s: Reg,
        n_reg: Reg,
        fill: Reg,
        left: bool,
    },
    /// The `n_reg`-th field of `s` split on `delim` (from the right when
    /// negative), empty past the last field; `n = 0` is NULL. A sub-view.
    StrSplitPart {
        s: Reg,
        delim: Reg,
        n_reg: Reg,
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
    /// [`IntArithOp`] with the resolve-time signedness verdict folded into the
    /// two operators that read one.
    IntArith {
        op: ResolvedIntOp,
        dst: u16,
        a: u16,
        b: u16,
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
    FloatArith {
        op: FloatArithOp,
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
    /// `signed` is the resolve-time U64 tracking of `a`; only `Sign` reads it.
    IntUnary {
        op: IntUnaryOp,
        dst: u16,
        a: u16,
        signed: bool,
    },
    /// `fi` is the fixed-int target `decode_triple` narrowed the wire type code
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
    /// Three-valued AND, or OR when `is_or` — one kernel, the operator carried
    /// as data, as [`LogicalInstr::BoolBinary`] carries it.
    BoolBinary {
        dst: u16,
        a: u16,
        b: u16,
        is_or: bool,
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
    IsNullReg {
        dst: u16,
        a: u16,
        invert: bool,
    },
    /// A German-string column against a constant. The constant is encoded at
    /// `resolve` into `ResolvedProgram.const_cells`; `cell_idx` indexes that
    /// vector, not the const pool.
    StrColConst {
        op: CmpOp,
        dst: u16,
        pi: u8,
        cell_idx: u32,
    },
    StrColCol {
        op: CmpOp,
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
        op: CmpOp,
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
    StrSubstr {
        dst: u16,
        src: u16,
        start: IntReg,
        len: Option<IntReg>,
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
    /// `fi` is the fixed-int target `decode_triple` narrowed the wire type code
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
    StrSide {
        dst: u16,
        src: u16,
        n: IntReg,
        left: bool,
    },
    StrPos {
        dst: u16,
        hay: u16,
        needle: u16,
    },
    StrReverse {
        dst: u16,
        a: u16,
    },
    StrReplace {
        dst: u16,
        s: u16,
        from: u16,
        to: u16,
    },
    StrPad {
        dst: u16,
        s: u16,
        n: IntReg,
        fill: u16,
        left: bool,
    },
    StrSplitPart {
        dst: u16,
        s: u16,
        delim: u16,
        n: IntReg,
    },
}

/// A scalar register read as a count or a bound. It widens to `i128` per the
/// register's resolve-time U64 tracking, so a large unsigned value never reads
/// as negative.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IntReg {
    pub(crate) reg: u16,
    pub(crate) signed: bool,
}

impl LogicalInstr {
    /// Serialise to the wire triple `[op, a1, a2]` — the exact inverse of
    /// [`LogicalProgram::from_wire`]'s decode, and the **one** place the operand
    /// word layout is written: which word carries a packed pair, which opcode a
    /// flag selects, where a cast target rides. Stated here once and read back
    /// there once; the round-trip test is what holds the two together.
    ///
    /// Unused words are 0, matching what the decoder ignores.
    pub fn to_wire(self) -> [u32; 3] {
        use LogicalInstr as L;
        // Every arm is `[op, a1, a2]`; these shorten the common shapes.
        let bin = |op: ExprOp, a: Reg, b: Reg| [op.as_wire(), a.0 as u32, b.0 as u32];
        let un = |op: ExprOp, a: Reg| [op.as_wire(), a.0 as u32, 0];
        let col = |op: ExprOp, c: u32| [op.as_wire(), c, 0];
        match self {
            L::LoadColInt { col: c } => col(ExprOp::LoadColInt, c),
            L::LoadColFloat { col: c } => col(ExprOp::LoadColFloat, c),
            L::LoadConst { val } => {
                let (a1, a2) = gnitz_wire::encode_load_const(val);
                [ExprOp::LoadConst.as_wire(), a1, a2]
            }
            L::IntArith { op, a, b } => bin(
                match op {
                    IntArithOp::Add => ExprOp::IntAdd,
                    IntArithOp::Sub => ExprOp::IntSub,
                    IntArithOp::Mul => ExprOp::IntMul,
                    IntArithOp::Div => ExprOp::IntDiv,
                    IntArithOp::Mod => ExprOp::IntMod,
                },
                a,
                b,
            ),
            L::FloatArith { op, a, b } => bin(
                match op {
                    FloatArithOp::Add => ExprOp::FloatAdd,
                    FloatArithOp::Sub => ExprOp::FloatSub,
                    FloatArithOp::Mul => ExprOp::FloatMul,
                    FloatArithOp::Div => ExprOp::FloatDiv,
                    FloatArithOp::Pow => ExprOp::FloatPow,
                },
                a,
                b,
            ),
            L::Cmp { op, a, b } => bin(
                match op {
                    CmpOp::Eq => ExprOp::CmpEq,
                    CmpOp::Ne => ExprOp::CmpNe,
                    CmpOp::Gt => ExprOp::CmpGt,
                    CmpOp::Ge => ExprOp::CmpGe,
                    CmpOp::Lt => ExprOp::CmpLt,
                    CmpOp::Le => ExprOp::CmpLe,
                },
                a,
                b,
            ),
            L::FCmp { op, a, b } => bin(
                match op {
                    CmpOp::Eq => ExprOp::FcmpEq,
                    CmpOp::Ne => ExprOp::FcmpNe,
                    CmpOp::Gt => ExprOp::FcmpGt,
                    CmpOp::Ge => ExprOp::FcmpGe,
                    CmpOp::Lt => ExprOp::FcmpLt,
                    CmpOp::Le => ExprOp::FcmpLe,
                },
                a,
                b,
            ),
            L::IntToFloat { a } => un(ExprOp::IntToFloat, a),
            L::FloatUnary { op, a } => un(
                match op {
                    FloatUnaryOp::Neg => ExprOp::FloatNeg,
                    FloatUnaryOp::Abs => ExprOp::FloatAbs,
                    FloatUnaryOp::Floor => ExprOp::FloatFloor,
                    FloatUnaryOp::Ceil => ExprOp::FloatCeil,
                    FloatUnaryOp::Round => ExprOp::FloatRound,
                    FloatUnaryOp::Trunc => ExprOp::FloatTrunc,
                    FloatUnaryOp::Sqrt => ExprOp::FloatSqrt,
                    FloatUnaryOp::Ln => ExprOp::FloatLn,
                    FloatUnaryOp::Log10 => ExprOp::FloatLog10,
                    FloatUnaryOp::Exp => ExprOp::FloatExp,
                    FloatUnaryOp::Sign => ExprOp::FloatSign,
                },
                a,
            ),
            L::IntUnary { op, a } => un(
                match op {
                    IntUnaryOp::Neg => ExprOp::IntNeg,
                    IntUnaryOp::Abs => ExprOp::IntAbs,
                    IntUnaryOp::Sign => ExprOp::IntSign,
                },
                a,
            ),
            L::FloatToInt { a, fi } => [ExprOp::FloatToInt.as_wire(), a.0 as u32, fi.type_code() as u32],
            L::IntCast { a, fi } => [ExprOp::IntCast.as_wire(), a.0 as u32, fi.type_code() as u32],
            L::FloatToF32 { a } => un(ExprOp::FloatToF32, a),
            L::IntMinMax2 { a, b, is_max } => bin(if is_max { ExprOp::IntMax2 } else { ExprOp::IntMin2 }, a, b),
            L::FloatMinMax2 { a, b, is_max } => bin(if is_max { ExprOp::FloatMax2 } else { ExprOp::FloatMin2 }, a, b),
            // SELECT and STR_SELECT pack `(a, b)` into the a2 word.
            L::Select { cond, a, b } => [
                ExprOp::Select.as_wire(),
                cond.0 as u32,
                gnitz_wire::pack_operand_pair(a.0 as u32, b.0 as u32),
            ],
            L::StrSelect { cond, a, b } => [
                ExprOp::StrSelect.as_wire(),
                cond.0 as u32,
                gnitz_wire::pack_operand_pair(a.0 as u32, b.0 as u32),
            ],
            L::LoadNull => [ExprOp::LoadNull.as_wire(), 0, 0],
            L::LoadNullStr => [ExprOp::LoadNullStr.as_wire(), 0, 0],
            L::BoolBinary { a, b, is_or } => bin(if is_or { ExprOp::BoolOr } else { ExprOp::BoolAnd }, a, b),
            L::BoolNot { a } => un(ExprOp::BoolNot, a),
            L::IsNull { col: c, invert } => col(if invert { ExprOp::IsNotNull } else { ExprOp::IsNull }, c),
            L::IsNullReg { a, invert } => un(
                if invert {
                    ExprOp::IsNotNullReg
                } else {
                    ExprOp::IsNullReg
                },
                a,
            ),
            L::StrColConst { op, col: c, const_idx } => [
                match op {
                    CmpOp::Eq => ExprOp::StrColEqConst,
                    CmpOp::Ne => ExprOp::StrColNeConst,
                    CmpOp::Gt => ExprOp::StrColGtConst,
                    CmpOp::Ge => ExprOp::StrColGeConst,
                    CmpOp::Lt => ExprOp::StrColLtConst,
                    CmpOp::Le => ExprOp::StrColLeConst,
                }
                .as_wire(),
                c,
                const_idx.0,
            ],
            L::StrColCol { op, col_a, col_b } => [
                match op {
                    CmpOp::Eq => ExprOp::StrColEqCol,
                    CmpOp::Ne => ExprOp::StrColNeCol,
                    CmpOp::Gt => ExprOp::StrColGtCol,
                    CmpOp::Ge => ExprOp::StrColGeCol,
                    CmpOp::Lt => ExprOp::StrColLtCol,
                    CmpOp::Le => ExprOp::StrColLeCol,
                }
                .as_wire(),
                col_a,
                col_b,
            ],
            L::IntInSet { value_reg, set_idx } => [ExprOp::IntInSet.as_wire(), value_reg.0 as u32, set_idx.0],
            L::LoadColStr { col: c } => col(ExprOp::LoadColStr, c),
            L::LoadConstStr { const_idx } => col(ExprOp::LoadConstStr, const_idx.0),
            L::StrCmp { op, a, b } => bin(
                match op {
                    CmpOp::Eq => ExprOp::StrCmpEq,
                    CmpOp::Ne => ExprOp::StrCmpNe,
                    CmpOp::Gt => ExprOp::StrCmpGt,
                    CmpOp::Ge => ExprOp::StrCmpGe,
                    CmpOp::Lt => ExprOp::StrCmpLt,
                    CmpOp::Le => ExprOp::StrCmpLe,
                },
                a,
                b,
            ),
            L::StrLen { a, chars } => un(
                if chars {
                    ExprOp::StrLenChars
                } else {
                    ExprOp::StrLenBytes
                },
                a,
            ),
            L::StrCase { a, upper } => un(if upper { ExprOp::StrUpper } else { ExprOp::StrLower }, a),
            // SUBSTR packs `(start, len)` into a2; the no-FOR form rides the
            // `STR_SUBSTR_NO_LEN` sentinel.
            L::StrSubstr {
                src,
                start_reg,
                len_reg,
            } => [
                ExprOp::StrSubstr.as_wire(),
                src.0 as u32,
                gnitz_wire::pack_operand_pair(
                    start_reg.0 as u32,
                    len_reg.map_or(gnitz_wire::STR_SUBSTR_NO_LEN, |r| r.0 as u32),
                ),
            ],
            // TRIM and LIKE pack their second operand into **a1**, beside the
            // source register — the mirror of SELECT and SUBSTR above.
            L::StrTrim { a, mode, set_idx } => [
                ExprOp::StrTrim.as_wire(),
                gnitz_wire::pack_operand_pair(a.0 as u32, mode.as_wire()),
                set_idx.0,
            ],
            L::StrLike {
                src,
                escape,
                pat_idx,
                ci,
            } => [
                if ci { ExprOp::StrIlike } else { ExprOp::StrLike }.as_wire(),
                gnitz_wire::pack_operand_pair(src.0 as u32, escape.map_or(0, NonZeroU8::get) as u32),
                pat_idx.0,
            ],
            L::StrConcat { a, b, skip_null } => bin(
                if skip_null {
                    ExprOp::StrConcatNn
                } else {
                    ExprOp::StrConcat
                },
                a,
                b,
            ),
            L::IntToStr { a } => un(ExprOp::IntToStr, a),
            L::FloatToStr { a } => un(ExprOp::FloatToStr, a),
            L::StrToInt { a, fi } => [ExprOp::StrToInt.as_wire(), a.0 as u32, fi.type_code() as u32],
            L::StrToFloat { a } => un(ExprOp::StrToFloat, a),
            L::StrSide { src, n_reg, left } => bin(if left { ExprOp::StrLeft } else { ExprOp::StrRight }, src, n_reg),
            L::StrPos { hay, needle } => bin(ExprOp::StrPos, hay, needle),
            L::StrReverse { a } => un(ExprOp::StrReverse, a),
            // The three-register producers pack their trailing pair into a2, the
            // SELECT / SUBSTR shape.
            L::StrReplace { s, from, to } => [
                ExprOp::StrReplace.as_wire(),
                s.0 as u32,
                gnitz_wire::pack_operand_pair(from.0 as u32, to.0 as u32),
            ],
            L::StrPad { s, n_reg, fill, left } => [
                if left { ExprOp::StrLpad } else { ExprOp::StrRpad }.as_wire(),
                s.0 as u32,
                gnitz_wire::pack_operand_pair(n_reg.0 as u32, fill.0 as u32),
            ],
            L::StrSplitPart { s, delim, n_reg } => [
                ExprOp::StrSplitPart.as_wire(),
                s.0 as u32,
                gnitz_wire::pack_operand_pair(delim.0 as u32, n_reg.0 as u32),
            ],
        }
    }
}

impl Sink {
    /// Serialise to the wire pair `[kind, value]`, the inverse of
    /// [`LogicalProgram::decode_sink`]. Sinks ride their own blob region, so
    /// this space is disjoint from [`ExprOp`]'s.
    fn to_wire(self) -> [u32; 2] {
        match self {
            Sink::Col(src_col) => [SINK_COL, src_col],
            Sink::Reg(r) => [SINK_REG, r.0 as u32],
        }
    }
}

/// [`Sink`]'s two wire kinds.
const SINK_COL: u32 = 0;
const SINK_REG: u32 = 1;

// ---------------------------------------------------------------------------
// LogicalProgram — pre-resolve container
// ---------------------------------------------------------------------------

/// Which resolver a program came through — one variant per entry point in
/// `eval.rs`, with the output schema on the one role that writes output slots.
///
/// Resolution reads one further bit of it: only a **filter** forces `result_reg`
/// into `bool_input`, because
/// [`Evaluator::filter_ranges`](crate::Evaluator::filter_ranges)'s nullable arm
/// consumes the verdict as a packed bit whatever opcode produced it.
#[derive(Clone, Copy)]
pub(crate) enum Role<'a> {
    Filter,
    Map(&'a dyn SchemaFacts),
    Scalar,
}

impl<'a> Role<'a> {
    /// The schema a map writes its sinks into — `None` for the two roles that
    /// write none, and so resolve no copy destination widths.
    fn out_schema(self) -> Option<&'a dyn SchemaFacts> {
        match self {
            Role::Map(os) => Some(os),
            Role::Filter | Role::Scalar => None,
        }
    }
}

#[derive(Debug)]
pub struct LogicalProgram {
    /// The compute instructions. Instruction `i` writes register `i`, so this is
    /// the register file too, and its length is the register count.
    instrs: Vec<LogicalInstr>,
    /// The output payload slots, in slot order — empty for a filter or scalar,
    /// which write none.
    sinks: Vec<Sink>,
    /// The register a filter or scalar result is read out of. `None` for a map,
    /// which writes [`Self::sinks`] instead and has no result.
    result_reg: Option<Reg>,
    const_strings: Vec<Vec<u8>>,
    /// Bit `r` set iff register `r` holds a string rather than a scalar, as
    /// [`Self::from_instrs`] finished it.
    ///
    /// **Position-independent**, which is what lets every later pass read the
    /// finished mask instead of rebuilding one in step: a register's one writer
    /// is the instruction at its own index, ahead of every reader.
    str_class: u64,
}

impl LogicalProgram {
    /// Build from typed instructions. The compiler and test builders trust their
    /// own construction, so a structural failure here is a compiler bug, not
    /// client input — [`Self::from_instrs`] panics rather than returns.
    pub fn new(
        instrs: Vec<LogicalInstr>,
        sinks: Vec<Sink>,
        result_reg: Option<Reg>,
        const_strings: Vec<Vec<u8>>,
    ) -> Self {
        Self::from_instrs(instrs, sinks, result_reg, const_strings)
            .unwrap_or_else(|e| panic!("compiler-built LogicalProgram is invalid: {e:?}"))
    }

    /// Assemble from typed instructions, holding every one to the rules a schema
    /// is not needed for. Every constructor routes through here and the type is
    /// immutable, so the walk below establishes the register-valid invariant
    /// `regs_split`'s raw split borrows depend on, and [`Self::str_class`], for
    /// every `LogicalProgram` in every profile.
    ///
    /// The fallible entry point: [`Self::new`] unwraps it (a failure is a
    /// compiler bug), `from_wire` propagates it (bad client input), and
    /// [`ExprBuilder::build`](crate::ExprBuilder::build) hands its instructions
    /// straight here rather than encoding them to wire words for `from_wire` to
    /// decode back.
    pub(crate) fn from_instrs(
        instrs: Vec<LogicalInstr>,
        sinks: Vec<Sink>,
        result_reg: Option<Reg>,
        const_strings: Vec<Vec<u8>>,
    ) -> Result<Self, ExprValidateErr> {
        use ExprValidateErr as E;
        // Bit per register holding a string. Instruction `i` is register `i`'s
        // one writer, so the class bitmask is set-only: a bit is never cleared.
        let mut str_class = 0u64;
        // Every per-register mask below is a `u64`, so a 65th instruction would
        // shift out of range.
        if instrs.len() > MAX_REGS {
            return Err(E::TooManyRegs(instrs.len() as u32));
        }
        if let Some(r) = result_reg {
            if r.0 as usize >= instrs.len() {
                return Err(E::ResultRegOutOfRange {
                    result_reg: r.0 as u32,
                    num_regs: instrs.len() as u32,
                });
            }
        }
        for (i, instr) in instrs.iter().enumerate() {
            // Bound every operand off the one per-opcode operand table, so no
            // arm below restates them.
            let ops = operands(instr);
            // The const-pool index an opcode carries rather than names as an
            // operand — the table states that too.
            check_extra(ops.extra, &const_strings)?;
            for &(reg, read) in ops.reads.iter().flatten() {
                // Reading a not-yet-written lane would take whatever the
                // previous morsel left there — another row's value, or for a
                // string lane another row's bytes.
                if !reg.written_before(i) {
                    return Err(E::RegReadBeforeWrite { reg: reg.0 });
                }
                // An operand's class must be the one the opcode reads: no string
                // opcode reading a scalar register, no scalar opcode reading a
                // string one.
                if read != ReadAs::NullBit && ((str_class >> reg.0) & 1 != 0) != read.wants_str() {
                    return Err(E::RegClassMismatch { reg: reg.0 });
                }
            }
            if ops.write == WriteAs::Str {
                str_class |= 1u64 << i;
            }
        }
        // A sink's source register is bounded but not ordered: sinks run after
        // every instruction, so any of them is readable.
        for reg in sinks.iter().filter_map(|s| s.reg()) {
            if !reg.written_before(instrs.len()) {
                return Err(E::RegOutOfRange {
                    reg: reg.0,
                    num_regs: instrs.len() as u32,
                });
            }
        }
        Ok(LogicalProgram {
            instrs,
            sinks,
            result_reg,
            const_strings,
            str_class,
        })
    }

    /// A pure projection: `copies[i] = src_col` copies logical input column
    /// `src_col` into output payload slot `i`. The source type is derived in
    /// `resolve` from the schema. The instruction-free shape a map consumer
    /// turns into verbatim column moves and nothing else. The one wire-free
    /// constructor a *production* caller uses — the circuit compiler builds
    /// projections with it; [`LogicalProgram::new`] is reached only from
    /// hand-written test programs.
    pub fn copy_cols(copies: &[u32]) -> Self {
        let sinks = copies.iter().map(|&src_col| Sink::Col(src_col)).collect();
        LogicalProgram::new(Vec::new(), sinks, None, Vec::new())
    }

    /// The typed instructions, in emission order — instruction `i` writing
    /// register `i`.
    pub fn instrs(&self) -> &[LogicalInstr] {
        &self.instrs
    }

    /// The byte-transparent const pool the instructions index into.
    pub fn const_strings(&self) -> &[Vec<u8>] {
        &self.const_strings
    }

    /// Serialise to the self-contained wire blob (magic "EXPR"), for a program
    /// shipped to the engine — the inverse of [`Self::from_blob`]. The one
    /// encode: `ExprBuilder` never produces wire words, so a blob can only come
    /// from a program that has already been validated here.
    pub fn to_blob_bytes(&self) -> Vec<u8> {
        let code: Vec<u32> = self.instrs.iter().copied().flat_map(LogicalInstr::to_wire).collect();
        let sinks: Vec<u32> = self.sinks.iter().copied().flat_map(Sink::to_wire).collect();
        gnitz_wire::encode_expr_blob(
            self.result_reg.map_or(0, |r| r.0 as u32),
            &code,
            &sinks,
            &self.const_strings,
        )
    }

    /// A predicate or scalar blob: framing decoded, then lowered. `label` names
    /// the call site in a `CorruptBlob` — the engine's own wrappers cover the
    /// *invalid program* path, not corrupt framing.
    pub fn from_blob(blob: &[u8], label: &'static str) -> Result<Self, ExprValidateErr> {
        let b = gnitz_wire::decode_expr_blob(blob).ok_or(ExprValidateErr::CorruptBlob(label))?;
        Self::from_wire(&b.code, &b.sinks, Some(Reg(b.result_reg as u16)), b.const_strings)
    }

    /// The same for a **map** blob, which has no result register: a map writes
    /// output slots, so its result is `None` here rather than at each call site.
    pub fn from_map_blob(blob: &[u8], label: &'static str) -> Result<Self, ExprValidateErr> {
        let b = gnitz_wire::decode_expr_blob(blob).ok_or(ExprValidateErr::CorruptBlob(label))?;
        Self::from_wire(&b.code, &b.sinks, None, b.const_strings)
    }

    /// Lower a wire expr blob — flat u32 triples `[op, a1, a2]` plus the sink
    /// region's `[kind, value]` pairs — into the typed logical form.
    /// Client-controlled throughout: an unknown opcode or a forged operand word
    /// is rejected by [`Self::decode_triple`], everything structural by
    /// [`Self::from_instrs`], and neither panics.
    pub fn from_wire(
        code: &[u32],
        sinks: &[u32],
        result_reg: Option<Reg>,
        const_strings: Vec<Vec<u8>>,
    ) -> Result<Self, ExprValidateErr> {
        if !code.len().is_multiple_of(3) || !sinks.len().is_multiple_of(2) {
            return Err(ExprValidateErr::CorruptBlob("expr region length"));
        }
        let instrs = code
            .chunks_exact(3)
            .map(Self::decode_triple)
            .collect::<Result<Vec<_>, _>>()?;
        let sinks = sinks
            .chunks_exact(2)
            .map(Self::decode_sink)
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_instrs(instrs, sinks, result_reg, const_strings)
    }

    /// Decode one sink pair `[kind, value]` — the inverse of [`Sink::to_wire`].
    fn decode_sink(p: &[u32]) -> Result<Sink, ExprValidateErr> {
        match p[0] {
            SINK_COL => Ok(Sink::Col(p[1])),
            SINK_REG => Ok(Sink::Reg(Reg(p[1] as u16))),
            other => Err(ExprValidateErr::UnknownOpcode(other)),
        }
    }

    /// Decode one wire triple `[op, a1, a2]`. The exact inverse of
    /// [`LogicalInstr::to_wire`], and with it the only statement of the operand
    /// word layout; an unknown opcode is rejected rather than panicked, since the
    /// words are client-controlled.
    ///
    /// The match over [`ExprOp`] is exhaustive and has **no `_` arm**: a new
    /// opcode in gnitz-wire fails to compile here until it gets a decode arm,
    /// which is the drift the two tables would otherwise have to be tested for.
    /// A cast target, TRIM's mode and LIKE's escape are narrowed to their types
    /// here too, so no later pass carries a raw word.
    pub(crate) fn decode_triple(t: &[u32]) -> Result<LogicalInstr, ExprValidateErr> {
        use LogicalInstr as L;
        let op = ExprOp::from_wire(t[0]).ok_or(ExprValidateErr::UnknownOpcode(t[0]))?;
        let a = Reg(t[1] as u16);
        let b = Reg(t[2] as u16);
        // Each closure captures this instruction's operands so the per-opcode
        // arms below stay one-liners.
        let cmp = |op| L::Cmp { op, a, b };
        let fcmp = |op| L::FCmp { op, a, b };
        let fu = |op| L::FloatUnary { op, a };
        let iu = |op| L::IntUnary { op, a };
        let ia = |op| L::IntArith { op, a, b };
        let fa = |op| L::FloatArith { op, a, b };
        let str_cmp = |op| L::StrCmp { op, a, b };
        let str_col_const = |op| L::StrColConst {
            op,
            col: t[1],
            const_idx: ConstIdx(t[2]),
        };
        let str_col_col = |op| L::StrColCol {
            op,
            col_a: t[1],
            col_b: t[2],
        };
        // The escape rides the `a1` word beside the source register, the
        // `ExprOp::StrTrim` shape; `ci` lives in the opcode, so nothing
        // downstream has to re-derive it.
        let str_like = |ci| -> Result<L, ExprValidateErr> {
            let (src, escape) = gnitz_wire::unpack_operand_pair(t[1]);
            Ok(L::StrLike {
                src: Reg(src),
                escape: like_escape(escape as u32)?,
                pat_idx: ConstIdx(t[2]),
                ci,
            })
        };
        Ok(match op {
            ExprOp::LoadColInt => L::LoadColInt { col: t[1] },
            ExprOp::LoadColFloat => L::LoadColFloat { col: t[1] },
            ExprOp::LoadConst => L::LoadConst {
                val: gnitz_wire::decode_load_const(t[1], t[2]),
            },
            ExprOp::IntAdd => ia(IntArithOp::Add),
            ExprOp::IntSub => ia(IntArithOp::Sub),
            ExprOp::IntMul => ia(IntArithOp::Mul),
            ExprOp::IntDiv => ia(IntArithOp::Div),
            ExprOp::IntMod => ia(IntArithOp::Mod),
            ExprOp::IntNeg => iu(IntUnaryOp::Neg),
            ExprOp::FloatAdd => fa(FloatArithOp::Add),
            ExprOp::FloatSub => fa(FloatArithOp::Sub),
            ExprOp::FloatMul => fa(FloatArithOp::Mul),
            ExprOp::FloatDiv => fa(FloatArithOp::Div),
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
            ExprOp::BoolAnd => L::BoolBinary { a, b, is_or: false },
            ExprOp::BoolOr => L::BoolBinary { a, b, is_or: true },
            ExprOp::BoolNot => L::BoolNot { a },
            ExprOp::IsNull => L::IsNull {
                col: t[1],
                invert: false,
            },
            ExprOp::IsNotNull => L::IsNull {
                col: t[1],
                invert: true,
            },
            ExprOp::IsNullReg => L::IsNullReg { a, invert: false },
            ExprOp::IsNotNullReg => L::IsNullReg { a, invert: true },
            ExprOp::IntToFloat => L::IntToFloat { a },
            ExprOp::IntAbs => iu(IntUnaryOp::Abs),
            ExprOp::FloatAbs => fu(FloatUnaryOp::Abs),
            ExprOp::FloatFloor => fu(FloatUnaryOp::Floor),
            ExprOp::FloatCeil => fu(FloatUnaryOp::Ceil),
            ExprOp::FloatRound => fu(FloatUnaryOp::Round),
            ExprOp::FloatTrunc => fu(FloatUnaryOp::Trunc),
            ExprOp::FloatSqrt => fu(FloatUnaryOp::Sqrt),
            ExprOp::FloatLn => fu(FloatUnaryOp::Ln),
            ExprOp::FloatLog10 => fu(FloatUnaryOp::Log10),
            ExprOp::FloatExp => fu(FloatUnaryOp::Exp),
            ExprOp::FloatSign => fu(FloatUnaryOp::Sign),
            ExprOp::IntSign => iu(IntUnaryOp::Sign),
            ExprOp::FloatPow => fa(FloatArithOp::Pow),
            ExprOp::FloatToF32 => L::FloatToF32 { a },
            // `cast_target` sees the full u32: a forged high-bit word must be
            // rejected, not silently truncated into a valid type code.
            ExprOp::FloatToInt => L::FloatToInt {
                a,
                fi: cast_target(t[2])?,
            },
            ExprOp::IntCast => L::IntCast {
                a,
                fi: cast_target(t[2])?,
            },
            ExprOp::IntMax2 => L::IntMinMax2 { a, b, is_max: true },
            ExprOp::IntMin2 => L::IntMinMax2 { a, b, is_max: false },
            ExprOp::FloatMax2 => L::FloatMinMax2 { a, b, is_max: true },
            ExprOp::FloatMin2 => L::FloatMinMax2 { a, b, is_max: false },
            ExprOp::Select => {
                let (sa, sb) = gnitz_wire::unpack_operand_pair(t[2]);
                L::Select {
                    cond: a,
                    a: Reg(sa),
                    b: Reg(sb),
                }
            }
            ExprOp::LoadNull => L::LoadNull,
            ExprOp::StrColEqConst => str_col_const(CmpOp::Eq),
            ExprOp::StrColNeConst => str_col_const(CmpOp::Ne),
            ExprOp::StrColGtConst => str_col_const(CmpOp::Gt),
            ExprOp::StrColGeConst => str_col_const(CmpOp::Ge),
            ExprOp::StrColLtConst => str_col_const(CmpOp::Lt),
            ExprOp::StrColLeConst => str_col_const(CmpOp::Le),
            ExprOp::StrColEqCol => str_col_col(CmpOp::Eq),
            ExprOp::StrColNeCol => str_col_col(CmpOp::Ne),
            ExprOp::StrColGtCol => str_col_col(CmpOp::Gt),
            ExprOp::StrColGeCol => str_col_col(CmpOp::Ge),
            ExprOp::StrColLtCol => str_col_col(CmpOp::Lt),
            ExprOp::StrColLeCol => str_col_col(CmpOp::Le),
            // `set_idx` takes the full `a2` u32 const index, never truncated to
            // a register's u16.
            ExprOp::IntInSet => L::IntInSet {
                value_reg: a,
                set_idx: ConstIdx(t[2]),
            },
            ExprOp::LoadColStr => L::LoadColStr { col: t[1] },
            ExprOp::LoadConstStr => L::LoadConstStr {
                const_idx: ConstIdx(t[1]),
            },
            ExprOp::LoadNullStr => L::LoadNullStr,
            ExprOp::StrSelect => {
                let (sa, sb) = gnitz_wire::unpack_operand_pair(t[2]);
                L::StrSelect {
                    cond: a,
                    a: Reg(sa),
                    b: Reg(sb),
                }
            }
            ExprOp::StrCmpEq => str_cmp(CmpOp::Eq),
            ExprOp::StrCmpNe => str_cmp(CmpOp::Ne),
            ExprOp::StrCmpGt => str_cmp(CmpOp::Gt),
            ExprOp::StrCmpGe => str_cmp(CmpOp::Ge),
            ExprOp::StrCmpLt => str_cmp(CmpOp::Lt),
            ExprOp::StrCmpLe => str_cmp(CmpOp::Le),
            ExprOp::StrLenBytes => L::StrLen { a, chars: false },
            ExprOp::StrLenChars => L::StrLen { a, chars: true },
            ExprOp::StrUpper => L::StrCase { a, upper: true },
            ExprOp::StrLower => L::StrCase { a, upper: false },
            ExprOp::StrSubstr => {
                let (start_reg, len_word) = gnitz_wire::unpack_operand_pair(t[2]);
                L::StrSubstr {
                    src: a,
                    start_reg: Reg(start_reg),
                    len_reg: (len_word as u32 != gnitz_wire::STR_SUBSTR_NO_LEN).then_some(Reg(len_word)),
                }
            }
            ExprOp::StrTrim => {
                // The mode rides the `a1` word beside the source register.
                let (src, mode) = gnitz_wire::unpack_operand_pair(t[1]);
                L::StrTrim {
                    a: Reg(src),
                    mode: trim_mode(mode as u32)?,
                    set_idx: ConstIdx(t[2]),
                }
            }
            ExprOp::StrLike => str_like(false)?,
            ExprOp::StrIlike => str_like(true)?,
            ExprOp::StrConcat => L::StrConcat { a, b, skip_null: false },
            ExprOp::StrConcatNn => L::StrConcat { a, b, skip_null: true },
            ExprOp::IntToStr => L::IntToStr { a },
            ExprOp::FloatToStr => L::FloatToStr { a },
            ExprOp::StrToInt => L::StrToInt {
                a,
                fi: cast_target(t[2])?,
            },
            ExprOp::StrToFloat => L::StrToFloat { a },
            ExprOp::StrLeft => L::StrSide {
                src: a,
                n_reg: b,
                left: true,
            },
            ExprOp::StrRight => L::StrSide {
                src: a,
                n_reg: b,
                left: false,
            },
            ExprOp::StrPos => L::StrPos { hay: a, needle: b },
            ExprOp::StrReverse => L::StrReverse { a },
            ExprOp::StrReplace => {
                let (from, to) = gnitz_wire::unpack_operand_pair(t[2]);
                L::StrReplace {
                    s: a,
                    from: Reg(from),
                    to: Reg(to),
                }
            }
            ExprOp::StrLpad | ExprOp::StrRpad => {
                let (n_reg, fill) = gnitz_wire::unpack_operand_pair(t[2]);
                L::StrPad {
                    s: a,
                    n_reg: Reg(n_reg),
                    fill: Reg(fill),
                    left: op == ExprOp::StrLpad,
                }
            }
            ExprOp::StrSplitPart => {
                let (delim, n_reg) = gnitz_wire::unpack_operand_pair(t[2]);
                L::StrSplitPart {
                    s: a,
                    delim: Reg(delim),
                    n_reg: Reg(n_reg),
                }
            }
        })
    }

    /// If the program computes nothing and its sinks are one contiguous block of
    /// column copies `src = [base, base+1, …]`, return `Some(base)`: the leading
    /// columns the program skips (the PK region a finalize / identity MAP
    /// inherits verbatim rather than copying). Otherwise `None`.
    pub fn sequential_copy_base(&self) -> Option<usize> {
        if !self.instrs.is_empty() {
            return None;
        }
        let Sink::Col(base) = *self.sinks.first()? else {
            return None;
        };
        self.sinks
            .iter()
            .enumerate()
            .all(|(i, s)| matches!(*s, Sink::Col(c) if c == base + i as u32))
            .then_some(base as usize)
    }

    /// Lower to the resolved form under one [`Role`], running the one-shot
    /// analysis (nullability, register roles, U64 tracking) over the instruction
    /// stream. [`Role::Map`]'s output schema fixes each copy's destination width
    /// here, where `check_copy_types` has just approved the widening.
    ///
    /// Every "decoded once" below means **once per compile**, never per row.
    pub(crate) fn resolve_program(self, schema: &dyn SchemaFacts, role: Role<'_>) -> ResolvedProgram {
        let out_schema = role.out_schema();
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
        // Off the instruction stream for the reason `copies` is: a constant
        // register names no computation, and each stream entry would cost the
        // per-morsel dispatch a no-op arm.
        let mut const_regs: Vec<(u16, i64)> = Vec::new();
        let mut const_str_regs: Vec<(u16, u32, u32)> = Vec::new();
        // Filled by `batch::str_col_slot`, in buffer-slot order.
        let mut str_cols: Vec<u8> = Vec::new();
        // Resolution is 1:1 on every instruction and carries each register
        // operand through by name, so masks taken off the logical stream apply
        // unchanged to the resolved one; the sinks are a separate list, and name
        // no register a mask covers.
        let ProgramFacts {
            bit_only,
            bool_pack,
            no_nulls,
            reg_u64,
        } = analyze(
            &self.instrs,
            &self.sinks,
            schema,
            self.result_reg,
            matches!(role, Role::Filter),
        );
        // Answered once per register by `analyze`, off each opcode's `U64Rule`,
        // so no arm below restates the rule.
        let is_u64 = |r: u16| (reg_u64 >> r) & 1 != 0;
        let int_reg = |r: Reg| IntReg {
            reg: r.0,
            signed: !is_u64(r.0),
        };
        // Off the schema alone, so a column-reading opcode gets the NOT NULL
        // collapse without wiring anything of its own. A PK column has no
        // payload slot and so contributes no bit, which is the same rule that
        // makes `LoadPk`'s destination unconditionally non-null.
        let nullable_slots = schema.nullable_payload_slots();
        // Read out before `self.instrs` is consumed below: a register file is
        // exactly one entry per instruction.
        let num_regs = self.instrs.len() as u32;
        // Sinks name an output slot, not a computation, and the map materializes
        // them columnar-side off `copies`/`emits`, so they never reach the
        // kernel dispatch. Slot order is their position.
        for (out, sink) in self.sinks.iter().enumerate() {
            let out = out as u32;
            match *sink {
                Sink::Col(src_col) => {
                    // The same destination type code `check_copy_types` approved
                    // the widening against; its `wire_stride` is the copy width.
                    let stride = out_schema.map_or(0, |os| {
                        gnitz_wire::wire_stride(os.col_type_code(os.payload_col_idx(out as usize))) as u8
                    });
                    copies.push((schema.locate(src_col as usize), out, stride));
                }
                Sink::Reg(r) => emits.push((r.0, out)),
            }
        }
        for (i, li) in self.instrs.into_iter().enumerate() {
            // A register is the index of the instruction that writes it.
            let dst = i as u16;
            // One logical instruction, one resolved instruction — bar the
            // constants (the two loads, and a lift or PK null test over one),
            // which leave the stream for their own tables.
            let resolved = match li {
                L::LoadColInt { col } => {
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
                L::LoadColFloat { col } => {
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
                L::LoadConst { val } => {
                    const_regs.push((dst, val));
                    continue;
                }
                // The one thing resolution adds to the operator: which of the
                // two dividing kernels runs unsigned, off the per-register U64
                // tracking.
                L::IntArith {
                    op,
                    a: Reg(a),
                    b: Reg(b),
                } => I::IntArith {
                    op: match op {
                        IntArithOp::Add => ResolvedIntOp::Add,
                        IntArithOp::Sub => ResolvedIntOp::Sub,
                        IntArithOp::Mul => ResolvedIntOp::Mul,
                        IntArithOp::Div => ResolvedIntOp::Div { signed: !is_u64(dst) },
                        IntArithOp::Mod => ResolvedIntOp::Mod { signed: !is_u64(dst) },
                    },
                    dst,
                    a,
                    b,
                },
                L::FloatArith {
                    op,
                    a: Reg(a),
                    b: Reg(b),
                } => I::FloatArith { op, dst, a, b },
                L::Cmp {
                    op,
                    a: Reg(a),
                    b: Reg(b),
                } => {
                    // EQ/NE are bit-identical signed/unsigned; ordered compares
                    // pick the unsigned form when either operand is U64.
                    let signed = matches!(op, CmpOp::Eq | CmpOp::Ne) || !(is_u64(a) || is_u64(b));
                    I::Cmp { op, dst, a, b, signed }
                }
                L::FCmp {
                    op,
                    a: Reg(a),
                    b: Reg(b),
                } => I::FCmp { op, dst, a, b },
                L::FloatUnary { op, a: Reg(a) } => I::FloatUnary { op, dst, a },
                L::IntUnary { op, a: Reg(a) } => I::IntUnary {
                    op,
                    dst,
                    a,
                    signed: !is_u64(a),
                },
                L::FloatToF32 { a: Reg(a) } => I::FloatToF32 { dst, a },
                // Total on a validated program, the `LoadColInt` shape above:
                L::FloatToInt { a: Reg(a), fi } => I::FloatToInt { dst, a, fi },
                L::IntCast { a: Reg(a), fi } => I::IntCast {
                    dst,
                    a,
                    fi,
                    src_signed: !is_u64(a),
                },
                L::IntMinMax2 {
                    a: Reg(a),
                    b: Reg(b),
                    is_max,
                } => I::IntMinMax2 {
                    dst,
                    a,
                    b,
                    is_max,
                    signed: !is_u64(dst),
                },
                L::FloatMinMax2 {
                    a: Reg(a),
                    b: Reg(b),
                    is_max,
                } => I::FloatMinMax2 { dst, a, b, is_max },
                // A constant lifted to f64 is another constant: `LoadConst` is
                // never U64, so the signed conversion is the one the kernel
                // would run, and a constant lane carries no null bit to copy.
                L::IntToFloat { a: Reg(a) } => match const_regs.iter().find(|(r, _)| *r == a) {
                    Some(&(_, val)) => {
                        const_regs.push((dst, crate::batch::encode_f64(val as f64)));
                        continue;
                    }
                    None => I::IntToFloat {
                        dst,
                        a,
                        signed: !is_u64(a),
                    },
                },
                L::Select {
                    cond: Reg(cond),
                    a: Reg(a),
                    b: Reg(b),
                } => I::Select { dst, cond, a, b },
                L::LoadNull => I::LoadNull { dst },
                L::BoolBinary {
                    a: Reg(a),
                    b: Reg(b),
                    is_or,
                } => I::BoolBinary { dst, a, b, is_or },
                L::BoolNot { a: Reg(a) } => I::BoolNot { dst, a },
                // A PK column is never NULL, so its test is the constant the
                // kernel would fill for a NOT NULL payload slot.
                L::IsNull { col, invert } => match schema.locate(col as usize) {
                    ColumnLocator::Pk { .. } => {
                        const_regs.push((dst, invert as i64));
                        continue;
                    }
                    ColumnLocator::Payload { slot, .. } => I::IsNull { dst, pi: slot, invert },
                },
                L::IsNullReg { a: Reg(a), invert } => I::IsNullReg { dst, a, invert },
                L::StrColConst {
                    op,
                    col,
                    const_idx: ConstIdx(const_idx),
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
                L::StrColCol { op, col_a, col_b } => I::StrColCol {
                    op,
                    dst,
                    pi_a: payload_slot(col_a as usize),
                    pi_b: payload_slot(col_b as usize),
                },
                L::IntInSet {
                    value_reg: Reg(value_reg),
                    set_idx: ConstIdx(set_idx),
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
                L::LoadColStr { col } => {
                    let pi = payload_slot(col as usize);
                    I::LoadColStr {
                        dst,
                        pi,
                        buf: crate::batch::str_col_slot(&mut str_cols, pi),
                    }
                }
                L::LoadConstStr {
                    const_idx: ConstIdx(const_idx),
                } => {
                    let ci = const_idx as usize;
                    // Appended on first reference, not once per instruction: two
                    // opcodes may share a const index.
                    let (off, len) = *const_spans[ci].get_or_insert_with(|| {
                        let bytes = &self.const_strings[ci];
                        let span = (const_arena.len() as u32, bytes.len() as u32);
                        const_arena.extend_from_slice(bytes);
                        span
                    });
                    const_str_regs.push((dst, off, len));
                    continue;
                }
                L::LoadNullStr => I::LoadNullStr { dst },
                L::StrSelect {
                    cond: Reg(cond),
                    a: Reg(a),
                    b: Reg(b),
                } => I::StrSelect { dst, cond, a, b },
                L::StrCmp {
                    op,
                    a: Reg(a),
                    b: Reg(b),
                } => I::StrCmp { op, dst, a, b },
                L::StrLen { a: Reg(a), chars } => I::StrLen { dst, a, chars },
                L::StrCase { a: Reg(a), upper } => I::StrCase { dst, a, upper },
                L::StrSubstr {
                    src: Reg(src),
                    start_reg,
                    len_reg,
                } => I::StrSubstr {
                    dst,
                    src,
                    start: int_reg(start_reg),
                    len: len_reg.map(int_reg),
                },
                L::StrTrim {
                    a: Reg(a),
                    mode,
                    set_idx: ConstIdx(set_idx),
                } => {
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
                    src: Reg(src),
                    escape,
                    pat_idx: ConstIdx(pat_idx),
                    ci,
                } => {
                    let pattern = &self.const_strings[pat_idx as usize];
                    let matcher_idx = like_matchers.len() as u32;
                    like_matchers.push(LikeMatcher::compile(pattern, escape.map(NonZeroU8::get), ci));
                    I::StrLike { dst, src, matcher_idx }
                }
                L::StrConcat {
                    a: Reg(a),
                    b: Reg(b),
                    skip_null,
                } => I::StrConcat { dst, a, b, skip_null },
                L::IntToStr { a: Reg(a) } => I::IntToStr {
                    dst,
                    a,
                    signed: !is_u64(a),
                },
                L::FloatToStr { a: Reg(a) } => I::FloatToStr { dst, a },
                L::StrToInt { a: Reg(a), fi } => I::StrToInt { dst, a, fi },
                L::StrToFloat { a: Reg(a) } => I::StrToFloat { dst, a },
                L::StrSide {
                    src: Reg(src),
                    n_reg,
                    left,
                } => I::StrSide {
                    dst,
                    src,
                    n: int_reg(n_reg),
                    left,
                },
                L::StrPos {
                    hay: Reg(hay),
                    needle: Reg(needle),
                } => I::StrPos { dst, hay, needle },
                L::StrReverse { a: Reg(a) } => I::StrReverse { dst, a },
                L::StrReplace {
                    s: Reg(s),
                    from: Reg(from),
                    to: Reg(to),
                } => I::StrReplace { dst, s, from, to },
                L::StrPad {
                    s: Reg(s),
                    n_reg,
                    fill: Reg(fill),
                    left,
                } => I::StrPad {
                    dst,
                    s,
                    n: int_reg(n_reg),
                    fill,
                    left,
                },
                L::StrSplitPart {
                    s: Reg(s),
                    delim: Reg(delim),
                    n_reg,
                } => I::StrSplitPart {
                    dst,
                    s,
                    delim,
                    n: int_reg(n_reg),
                },
            };
            instrs.push(resolved);
        }
        // Stable-partition by register class, so each writer reads a slice
        // instead of re-testing per entry. Safe because each sink names its own
        // output slot, so emit order carries no meaning.
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
            bool_pack_mask: bool_pack,
            instrs,
            const_regs,
            const_str_regs,
            num_regs,
            result_reg: self.result_reg.map_or(0, |r| r.0 as u32),
            const_cells,
            int_sets,
            trim_sets,
            like_matchers,
            const_arena,
            // One past the highest string register, or 0 for a program with
            // none: lanes are register-major, so nothing above it is addressed.
            str_lanes: MAX_REGS as u32 - str_class.leading_zeros(),
            str_cols,
            result_is_str: self.result_reg.is_some_and(|r| (str_class >> r.0) & 1 != 0),
        }
    }

    /// [`Self::validate`] plus the extra rules the [`Role`] fixes: a filter and a
    /// scalar read their result back out of a register and must own one, and a
    /// filter's must not be a string register — it consumes the result as a
    /// packed truth bit, where `resolve_scalar` *wants* a string and reads it
    /// through `eval_row_str`.
    pub(crate) fn validate_for(&self, schema: &dyn SchemaFacts, role: Role<'_>) -> Result<(), ExprValidateErr> {
        self.validate(schema, role.out_schema())?;
        match role {
            Role::Map(_) => Ok(()),
            Role::Filter | Role::Scalar => {
                let Some(r) = self.result_reg else {
                    return Err(ExprValidateErr::ResultRegRequired);
                };
                if matches!(role, Role::Filter) && (self.str_class >> r.0) & 1 != 0 {
                    return Err(ExprValidateErr::RegClassMismatch { reg: r.0 });
                }
                Ok(())
            }
        }
    }

    /// Hold every column and sink to the schema the program will run against,
    /// over the same per-opcode operand table [`Self::from_instrs`] walks — so a
    /// new opcode cannot silently bypass a bound here either.
    ///
    /// `out_schema` is `None` for the roles that write no output slots — a
    /// filter and a scalar, whose sink list is empty. With one, this also
    /// decides **output coverage**: sink `i` writes slot `i`, so covering every
    /// declared payload slot is a count.
    pub(crate) fn validate(
        &self,
        in_schema: &dyn SchemaFacts,
        out_schema: Option<&dyn SchemaFacts>,
    ) -> Result<(), ExprValidateErr> {
        for instr in &self.instrs {
            for &(col, kind) in operands(instr).cols.iter().flatten() {
                check_col(in_schema, col, kind)?;
            }
        }
        // Ahead of the per-sink rules, which address `out_schema` by sink
        // position: covering the output exactly is what keeps every position in
        // range.
        if let Some(os) = out_schema {
            if self.sinks.len() != os.num_payload_cols() {
                return Err(ExprValidateErr::OutputSlotCountMismatch {
                    sinks: self.sinks.len(),
                    num_payload_cols: os.num_payload_cols(),
                });
            }
        }
        for (out, sink) in self.sinks.iter().enumerate() {
            let out = out as u32;
            // Ahead of the output rules, which index the schema with `src_col`.
            if let Some((src_col, kind)) = sink.on_col() {
                check_col(in_schema, src_col, kind)?;
            }
            let Some(os) = out_schema else { continue };
            match *sink {
                Sink::Col(src_col) => check_copy_types(in_schema, os, src_col, out)?,
                // The source register's class picks which slot rule applies.
                Sink::Reg(r) => check_emit_slot(os, out, (self.str_class >> r.0) & 1 != 0)?,
            }
        }
        Ok(())
    }
}

/// Bound the const-pool index an opcode carries rather than names as an
/// operand, and hold an `INT_IN_SET` pool to whole i64s — a truncating entry is
/// a clean rejection rather than a silent `chunks_exact` tail-drop.
fn check_extra(extra: Extra, const_strings: &[Vec<u8>]) -> Result<(), ExprValidateErr> {
    match extra {
        Extra::ConstIdx(const_idx) => check_const_idx(const_idx.0, const_strings.len()),
        Extra::IntSet(set_idx) => {
            check_const_idx(set_idx.0, const_strings.len())?;
            let len = const_strings[set_idx.0 as usize].len();
            match int_set_len_ok(len) {
                true => Ok(()),
                false => Err(ExprValidateErr::IntSetNotAligned {
                    set_idx: set_idx.0,
                    len,
                }),
            }
        }
        Extra::None => Ok(()),
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
    /// The register's null lane alone, out of `null_bits`: neither its value
    /// nor its truth bit, so the operand's class is not constrained and its
    /// producer need not unpack.
    NullBit,
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
    /// Whether this operand's register must hold a string.
    fn wants_str(self) -> bool {
        matches!(self, ReadAs::Str)
    }
}

/// Every operand of one instruction — the registers it reads, the columns it
/// addresses — each with what its kernel requires of it, plus what it writes
/// into its own register and whether the kernel can manufacture a NULL.
///
/// The **one** per-opcode table, read by the walks over the instruction stream
/// — `from_instrs`, `validate` and `analyze` — so a new opcode is classified
/// once: its operands, its NULL production and its destination's U64-ness alike.
/// Every arm destructures every field of every variant it matches, with `_` for
/// fields it ignores and **no `..`** — so an opcode that gains an operand is a
/// compile error here rather than an unbounded operand reaching the kernels.
struct Operands {
    /// What the instruction writes into the register it names by position.
    write: WriteAs,
    reads: [Option<(Reg, ReadAs)>; MAX_READS],
    cols: [Option<(u32, ColKind)>; MAX_COL_OPERANDS],
    /// A value the opcode carries that is neither a register nor a column, but
    /// still reaches a panicking / OOB / truncating site if forged.
    extra: Extra,
    /// True iff the kernel can turn non-NULL input into a NULL result — a zero
    /// divisor, an out-of-range cast, an unparsable text→number, a CONCAT past
    /// `u32::MAX`, `LoadNull*` on every row. A column operand's own nullability
    /// is `cols`, not this.
    makes_null: bool,
}

/// A per-opcode const-pool index — client-controlled, and bounded against the
/// pool by [`LogicalProgram::from_instrs`] before a kernel reads it. Stated in
/// the operand table beside the opcode's registers and columns, so classifying a
/// new opcode is one edit rather than two.
#[derive(Clone, Copy)]
enum Extra {
    None,
    /// A const-pool index.
    ConstIdx(ConstIdx),
    /// An `INT_IN_SET` value pool: a const index whose entry must be whole i64s.
    IntSet(ConstIdx),
}

/// SELECT and SUBSTRING, the widest opcodes, read three registers.
const MAX_READS: usize = 3;
/// `StrColCol` compares two columns; no opcode addresses more.
const MAX_COL_OPERANDS: usize = 2;

fn writes(write: WriteAs) -> Operands {
    Operands {
        write,
        reads: [None; MAX_READS],
        cols: [None; MAX_COL_OPERANDS],
        extra: Extra::None,
        makes_null: false,
    }
}

impl Operands {
    /// Mark the kernel a NULL producer; see [`Operands::makes_null`].
    fn may_null(mut self) -> Self {
        self.makes_null = true;
        self
    }

    fn reading(mut self, reg: Reg, read: ReadAs) -> Self {
        let slot = self
            .reads
            .iter()
            .position(Option::is_none)
            .expect("an opcode reads at most MAX_READS registers");
        self.reads[slot] = Some((reg, read));
        self
    }

    fn reading_opt(self, reg: Option<Reg>, read: ReadAs) -> Self {
        match reg {
            Some(r) => self.reading(r, read),
            None => self,
        }
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
    use ReadAs::{Bool as RBool, Str as RStr, Value as RVal};
    use U64Rule::{Fixed, FromColType, FromOperands};
    use WriteAs::{Bool as WBool, Str as WStr, Value as WVal};
    match *li {
        // --- Two scalar registers in, one scalar value out ---
        // Split int from float on the U64 rule alone: a pure int transform keeps
        // its operands' width and signedness, an f64 image has none to keep.
        // A zero divisor yields NULL; the other operators produce none of their
        // own.
        L::IntArith { op, a, b } => {
            let ops = writes(WVal(FromOperands)).reading(a, RVal).reading(b, RVal);
            // Spelled out rather than left to a `_` arm: a new operator that
            // *can* produce NULL would otherwise default to `makes_null: false`,
            // `analyze` would set `no_nulls`, and the evaluator would skip null
            // tracking — a wrong answer in release with no compile error.
            match op {
                IntArithOp::Div | IntArithOp::Mod => ops.may_null(),
                IntArithOp::Add | IntArithOp::Sub | IntArithOp::Mul => ops,
            }
        }
        L::FloatArith { op, a, b } => {
            let ops = writes(WVal(Fixed(false))).reading(a, RVal).reading(b, RVal);
            match op {
                FloatArithOp::Div => ops.may_null(),
                FloatArithOp::Add | FloatArithOp::Sub | FloatArithOp::Mul | FloatArithOp::Pow => ops,
            }
        }
        // Null-*skipping*, unlike the arithmetic above: a NULL operand yields
        // the other operand rather than propagating. That is why neither shares
        // an arm with it, however alike the operand shapes look.
        L::IntMinMax2 { a, b, is_max: _ } => writes(WVal(FromOperands)).reading(a, RVal).reading(b, RVal),
        L::FloatMinMax2 { a, b, is_max: _ } => writes(WVal(Fixed(false))).reading(a, RVal).reading(b, RVal),
        L::Cmp { op: _, a, b } | L::FCmp { op: _, a, b } => writes(WBool).reading(a, RVal).reading(b, RVal),
        L::BoolBinary { a, b, is_or: _ } => writes(WBool).reading(a, RBool).reading(b, RBool),
        L::BoolNot { a } => writes(WBool).reading(a, RBool),
        // SIGN's -1/0/1 is signed whatever its operand was; the wrapping pair
        // keeps the operand's width and tracking.
        L::IntUnary { op, a } => match op {
            IntUnaryOp::Sign => writes(WVal(Fixed(false))).reading(a, RVal),
            IntUnaryOp::Neg | IntUnaryOp::Abs => writes(WVal(FromOperands)).reading(a, RVal),
        },
        L::FloatUnary { op: _, a } | L::IntToFloat { a } => writes(WVal(Fixed(false))).reading(a, RVal),
        // The three narrowing casts yield NULL on an out-of-range value.
        L::FloatToF32 { a } => writes(WVal(Fixed(false))).reading(a, RVal).may_null(),
        L::FloatToInt { a, fi } | L::IntCast { a, fi } => {
            writes(WVal(Fixed(fi == FixedInt::U64))).reading(a, RVal).may_null()
        }
        // `cond` is a truth bit; the branches are values. The destination is a
        // value write, not a boolean one — see `WriteAs::Bool`.
        L::Select { cond, a, b } => writes(WVal(FromOperands))
            .reading(cond, RBool)
            .reading(a, RVal)
            .reading(b, RVal),
        L::IntInSet { value_reg, set_idx } => writes(WBool)
            .reading(value_reg, RVal)
            .with_extra(Extra::IntSet(set_idx)),
        L::LoadConst { val: _ } => writes(WVal(Fixed(false))),
        // A NULL on every row.
        L::LoadNull => writes(WVal(Fixed(false))).may_null(),

        // --- Column operands ---
        // The integer load kernels decode only the eight fixed-width integer
        // codes. `ColKind::FixedIntCol` is not payload-only, so this is the one
        // column operand that may name a PK column.
        L::LoadColInt { col } => writes(WVal(FromColType)).on_col(col, ColKind::FixedIntCol),
        // The float load kernel branches on width alone, so a 1- or 2-byte
        // integer column would make it slice an 8-byte stride from a narrower
        // region.
        L::LoadColFloat { col } => writes(WVal(Fixed(false))).on_col(col, ColKind::FloatPayload),
        // The null-bitmap reader decodes no value, so any column will do — U128
        // and STRING included — and its boolean is definite over a NULL row.
        L::IsNull { col, invert: _ } => writes(WBool).on_col(col, ColKind::AnyCol),
        L::IsNullReg { a, invert: _ } => writes(WBool).reading(a, ReadAs::NullBit),
        // The German-string compares read 16-byte cells; a narrower column
        // would make `col_data(pi, 16)` over-read its region. The verdict is a
        // bool, but a NULL operand makes it NULL, so the null bit flows.
        L::StrColConst { op: _, col, const_idx } => writes(WBool)
            .on_col(col, ColKind::StringPayload)
            .with_extra(Extra::ConstIdx(const_idx)),
        L::StrColCol { op: _, col_a, col_b } => writes(WBool)
            .on_col(col_a, ColKind::StringPayload)
            .on_col(col_b, ColKind::StringPayload),
        // The string-register column load reads the same 16-byte cells the
        // `ExprOp::StrCol*` compares do, so it carries the same requirement.
        L::LoadColStr { col } => writes(WStr).on_col(col, ColKind::StringPayload),

        // --- String registers ---
        L::LoadConstStr { const_idx } => writes(WStr).with_extra(Extra::ConstIdx(const_idx)),
        // A NULL on every row.
        L::LoadNullStr => writes(WStr).may_null(),
        L::IntToStr { a } | L::FloatToStr { a } => writes(WStr).reading(a, RVal),
        L::StrLen { a, chars: _ } => writes(WVal(Fixed(false))).reading(a, RStr),
        // Both text→number parses yield NULL on an unparsable or out-of-range
        // value.
        L::StrToFloat { a } => writes(WVal(Fixed(false))).reading(a, RStr).may_null(),
        L::StrToInt { a, fi } => writes(WVal(Fixed(fi == FixedInt::U64))).reading(a, RStr).may_null(),
        L::StrCmp { op: _, a, b } => writes(WBool).reading(a, RStr).reading(b, RStr),
        // One string operand plus compile-time pattern data. The verdict is a
        // definite 0/1 — LIKE introduces no NULL of its own, so the operand's
        // null bit is the only one.
        L::StrLike {
            src,
            escape: _,
            pat_idx,
            ci: _,
        } => writes(WBool).reading(src, RStr).with_extra(Extra::ConstIdx(pat_idx)),
        L::StrCase { a, upper: _ } => writes(WStr).reading(a, RStr),
        L::StrTrim { a, mode: _, set_idx } => writes(WStr).reading(a, RStr).with_extra(Extra::ConstIdx(set_idx)),
        // A combined length above `u32::MAX` yields NULL, which is easy to miss
        // because CONCAT otherwise looks like a pure transform.
        L::StrConcat { a, b, skip_null: _ } => writes(WStr).reading(a, RStr).reading(b, RStr).may_null(),
        // A scalar condition blending two string branches.
        L::StrSelect { cond, a, b } => writes(WStr).reading(cond, RBool).reading(a, RStr).reading(b, RStr),
        // A string source with integer window bounds. SUBSTR's only NULL is a
        // negative length, so the no-FOR form makes none — the window itself
        // clamps every out-of-range endpoint.
        L::StrSubstr {
            src,
            start_reg,
            len_reg,
        } => {
            let ops = writes(WStr)
                .reading(src, RStr)
                .reading(start_reg, RVal)
                .reading_opt(len_reg, RVal);
            if len_reg.is_some() {
                ops.may_null()
            } else {
                ops
            }
        }
        // The window producers are total: every count clamps into the string.
        L::StrSide { src, n_reg, left: _ } => writes(WStr).reading(src, RStr).reading(n_reg, RVal),
        L::StrReverse { a } => writes(WStr).reading(a, RStr),
        // A character index is bounded by the byte length, so never U64.
        L::StrPos { hay, needle } => writes(WVal(Fixed(false))).reading(hay, RStr).reading(needle, RStr),
        // The two arena producers share CONCAT's overflow NULL; SPLIT_PART's is
        // the zero field index.
        L::StrReplace { s, from, to } => writes(WStr)
            .reading(s, RStr)
            .reading(from, RStr)
            .reading(to, RStr)
            .may_null(),
        L::StrPad {
            s,
            n_reg,
            fill,
            left: _,
        } => writes(WStr)
            .reading(s, RStr)
            .reading(n_reg, RVal)
            .reading(fill, RStr)
            .may_null(),
        L::StrSplitPart { s, delim, n_reg } => writes(WStr)
            .reading(s, RStr)
            .reading(delim, RStr)
            .reading(n_reg, RVal)
            .may_null(),
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
/// the half above a byte must be clear. `NonZeroU8` because 0 *is* the absence:
/// a `Some(0)` would encode back as `None`, so the round trip is only the
/// identity while the type cannot hold one.
fn like_escape(escape: u32) -> Result<Option<NonZeroU8>, ExprValidateErr> {
    if escape > u8::MAX as u32 {
        return Err(ExprValidateErr::BadLikeEscape { escape });
    }
    Ok(NonZeroU8::new(escape as u8))
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

/// A column sink's destination slot must hold its source verbatim. `copy_column`
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

/// A register sink's destination slot must hold what the source register's class stores: a
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
    /// beside them: it is the partition, whose point is [`Self::str_emit_start`].
    pub(crate) emits: Vec<(u16, u32)>,
    /// Where [`Self::emits`]' string half starts.
    str_emit_start: usize,
    /// The scalar constant registers, as `(destination, value)`. Off the
    /// instruction stream for the reason [`Self::copies`] is, and installed once
    /// per evaluator: a register's one writer is the instruction at its own
    /// index, so a constant lane is written before any morsel runs.
    pub(crate) const_regs: Vec<(u16, i64)>,
    /// The string constant registers, as `(destination, `[`Self::const_arena`]
    /// `offset, length)` — the same treatment, over the lane array.
    pub(crate) const_str_regs: Vec<(u16, u32, u32)>,
    pub(crate) num_regs: u32,
    /// The register holding the filter verdict, or the scalar result. A map has
    /// none and this reads 0, which nothing consults: a map's result leaves
    /// through [`Self::emits`].
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
    /// How many string register lanes the scratch must hold: one past the
    /// highest string register, 0 for a program with none. Lanes are
    /// register-major, so sizing by [`Self::num_regs`] would reserve 4 KiB per
    /// register for lanes nothing can read.
    pub(crate) str_lanes: u32,
    /// True iff [`Self::result_reg`] holds a string. Decided at resolve, where
    /// the logical program still says whether there *is* a result register: a
    /// map has none, so this is false for one without a second test of what the
    /// program is for.
    result_is_str: bool,
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

    /// True iff the result must be read through
    /// [`crate::Evaluator::eval_row_str`], not `eval_row`. Only a scalar can
    /// answer true: a map has no result register, and [`Role::Filter`]'s extra
    /// rule in `validate_for` rejects a string-valued filter.
    pub(crate) fn result_is_str(&self) -> bool {
        self.result_is_str
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
    /// Bit `r` set iff `r`'s producer must write `bool_bits[r]` — a boolean
    /// consumer reads it, or it is bit_only and the filter reads the packed bit
    /// directly. The union, because that is the only form read back.
    bool_pack: u64,
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
/// * **Register roles** — which registers are produced as booleans, and which
///   are read as something other than a truth bit. A map has no result register
///   to force; a bare scalar's may legitimately stay `bit_only`.
/// * **Strict non-nullability**, against the schema the program is about to be
///   resolved against, the only schema for which the answer means anything. A PK
///   column operand never contributes: the null bitmap is payload-indexed, so the
///   PK region carries no null bit for a load to inherit.
///
/// Every `1u64 << reg` below is in range: `from_instrs` caps the instruction
/// count at `MAX_REGS` and bounds every register operand by it.
fn analyze(
    instrs: &[LogicalInstr],
    sinks: &[Sink],
    schema: &dyn SchemaFacts,
    result_reg: Option<Reg>,
    is_filter: bool,
) -> ProgramFacts {
    let (mut bool_produced, mut non_bool_read, mut bool_input) = (0u64, 0u64, 0u64);
    let mut no_nulls = true;
    let mut reg_u64 = 0u64;
    for (i, li) in instrs.iter().enumerate() {
        let ops = operands(li);
        match ops.write {
            WriteAs::Bool => bool_produced |= 1u64 << i,
            // In stream order, which is what `U64Rule::FromOperands` needs: a
            // destination's U64-ness is a function of registers earlier
            // instructions wrote. The read classification below needs no order,
            // which is what lets both live in one walk.
            WriteAs::Value(rule) => reg_u64 |= (u64_verdict(rule, &ops, schema, reg_u64) as u64) << i,
            WriteAs::Str => {}
        }
        no_nulls &= !ops.makes_null
            && !ops.cols.iter().flatten().any(|&(col, kind)| {
                kind.carries_null_to_register() && !schema.is_pk_col(col as usize) && schema.col_nullable(col as usize)
            });
        for &(reg, read) in ops.reads.iter().flatten() {
            let bit = 1u64 << reg.0;
            match read {
                ReadAs::Bool => bool_input |= bit,
                // A null-lane read needs neither the value nor the truth bit.
                ReadAs::NullBit => {}
                ReadAs::Value | ReadAs::Str => non_bool_read |= bit,
            }
        }
    }
    // A sink stores a register's **value**, never its truth bit: that is what
    // keeps an emitted boolean out of `bit_only`, where its producer would skip
    // the unpack and the map would ship the previous morsel's lane.
    for reg in sinks.iter().filter_map(|s| s.reg()) {
        non_bool_read |= 1u64 << reg.0;
    }
    // A filter's `result_reg` is forced to be a bool input: the filter's
    // nullable arm consumes the result as packed bits, so its producer must
    // populate `bool_bits` whatever opcode it is. A scalar's is read as a value
    // (`reg_values`), so a boolean producer there must unpack into the lane.
    if let Some(r) = result_reg {
        if is_filter {
            bool_input |= 1u64 << r.0;
        } else {
            non_bool_read |= 1u64 << r.0;
        }
    }
    let bit_only = bool_produced & !non_bool_read;
    ProgramFacts {
        bit_only,
        bool_pack: bit_only | bool_input,
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
            .any(|&(reg, read)| read == ReadAs::Value && (so_far >> reg.0) & 1 != 0),
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
