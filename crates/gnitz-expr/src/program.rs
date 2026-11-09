//! Compiled scalar-expression programs, in two typed forms: [`LogicalProgram`]
//! (`LogicalInstr` over logical column indices, the form a blob decodes into)
//! and `ResolvedProgram` (`Instr` over payload slots and PK offsets, the form the
//! kernels run). `LogicalProgram::resolve_program` checks the former against a
//! schema and lowers it into the latter. Each opcode carries its operands as
//! typed fields, so an opcode routed to the wrong operands fails to compile.
//!
//! The selectors an instruction carries, `CmpOp`, the arithmetic and unary
//! operators and `TrimMode`, are declared here; `CalendarOp` is declared in
//! `calendar.rs`, beside the arithmetic it selects.

use crate::calendar::CalendarOp;
use crate::like::{LikeMatcher, LikePattern};
use crate::{ColumnLocator, SchemaDescriptor};
use gnitz_wire::{encode_german_string, FixedInt, ScalarKind, TypeCode};
use std::fmt;

/// The register file is capped at 64: the `BoolBinary` 3VL paths, the
/// null-bit propagation, and every register-indexed mask address registers by
/// bit in a `u64`.
pub(crate) const MAX_REGS: usize = u64::BITS as usize;

/// Payload slots are capped at 64 by the null bitmap's one `u64` word per row:
/// the length of `ResolvedProgram::read_widths` and of the evaluator's region table.
pub(crate) const PAYLOAD_SLOTS: usize = u64::BITS as usize;

/// Why a client-supplied expression program, or the range walk a read carries
/// beside it, was rejected. A variant's payload is there for its `Display` to
/// name the offending operand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprValidateErr {
    UnknownOpcode(u32),
    TooManyRegs(u32),
    /// A sink's register is not below `num_regs`.
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
    /// A const-pool entry that is not what its opcode reads it as.
    PoolEntryMalformed {
        const_idx: u32,
        want: PoolEntry,
    },
    ColOutOfRange {
        col: u32,
        num_columns: usize,
    },
    ColKindMismatch {
        col: u32,
        type_code: TypeCode,
        want: ColKind,
    },
    CopyTypeMismatch {
        col: u32,
        src_tc: TypeCode,
        out: u32,
        out_tc: TypeCode,
    },
    EmitSlotWidth {
        out: u32,
        type_code: TypeCode,
    },
    EmitClassMismatch {
        out: u32,
        type_code: TypeCode,
    },
    OutputSlotCountMismatch {
        sinks: usize,
        num_payload_cols: usize,
    },
    /// A filter or scalar whose output is not exactly one register sink.
    OutputRoleMismatch,
    /// Framing or region bytes that describe no program — the decoder's own
    /// message, prefixed with the call site that read them.
    CorruptBlob(String),
    /// An instruction's selector word names no member of its opcode's family —
    /// the one statement covering every family, since that is the only thing a
    /// selector can be wrong about.
    BadSelector {
        op: u32,
        selector: u32,
    },
    BadSinkKind(u32),
    /// A range walk naming a column out of range or of a type with no key order.
    BadWalk(String),
}

/// The client-facing rendering.
impl fmt::Display for ExprValidateErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExprValidateErr::TooManyRegs(n) => {
                write!(
                    f,
                    "expression needs {n} registers; the limit is {MAX_REGS} — \
                     split the predicate, or project fewer computed columns"
                )
            }
            ExprValidateErr::ColKindMismatch { col, type_code, want } => {
                write!(
                    f,
                    "column {col} (type code {type_code}) cannot be used here; this operator needs {want}"
                )
            }
            // Already a sentence naming the format and the fault; `Debug` would
            // quote and escape it inside `CorruptBlob("…")`.
            ExprValidateErr::CorruptBlob(msg) | ExprValidateErr::BadWalk(msg) => write!(f, "{msg}"),
            other => write!(f, "{other:?}"),
        }
    }
}

/// What an opcode's kernel requires of a column operand, by type. No PK column
/// is a string or a float, so a kind admitting only those reads payload columns
/// alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColKind {
    /// PK or payload, any type: the reader decodes no value.
    AnyCol,
    /// A column a scalar register loads: a fixed-width integer or a float.
    ScalarCol,
    /// The 16-byte German-string layout, STRING or BLOB.
    StringCol,
}

/// The phrase a `ColKindMismatch` renders for the kind it asks for.
impl fmt::Display for ColKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AnyCol => "any column",
            Self::ScalarCol => "a fixed-width integer or floating-point column",
            Self::StringCol => "a string or blob column",
        })
    }
}

impl ColKind {
    /// Whether a column of type `tc` may be this operand.
    fn admits(self, tc: TypeCode) -> bool {
        match self {
            Self::AnyCol => true,
            Self::ScalarCol => ScalarKind::from_type_code(tc).is_some(),
            Self::StringCol => tc.is_german_string(),
        }
    }

    /// Whether the kernel decodes the column's value, and so carries its null bit.
    fn decodes(self) -> bool {
        self != Self::AnyCol
    }
}

// ---------------------------------------------------------------------------
// Typed instruction operands
// ---------------------------------------------------------------------------

// Each of the six is a wire instruction's **selector**: its word 1, naming
// which member of the opcode's family this instruction is. Wire enums, so
// that word's encode and decode are one call each rather than a match per
// direction.

gnitz_wire::wire_enum! {
    /// Comparison operator, shared by integer (`Cmp`) and float (`FCmp`) compares.
    pub enum CmpOp: u32 {
        Eq = 0,
        Ne = 1,
        Gt = 2,
        Ge = 3,
        Lt = 4,
        Le = 5,
    }
}

impl CmpOp {
    /// `x OP y` ⟺ `y OP.converse() x`.
    pub(crate) fn converse(self) -> CmpOp {
        match self {
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Ge => CmpOp::Le,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Eq | CmpOp::Ne => self,
        }
    }
}

gnitz_wire::wire_enum! {
    /// Pure float unary transform: its operand's IEEE result, propagating the
    /// operand's null bit and producing no NULL of its own — `SQRT(-1)` is NaN
    /// and `LN(0)` is `-inf` rather than either being NULL.
    pub enum FloatUnaryOp: u32 {
        Neg = 0,
        Abs = 1,
        Floor = 2,
        Ceil = 3,
        /// `round_ties_even`, not `round`.
        Round = 4,
        Trunc = 5,
        Sqrt = 6,
        Ln = 7,
        Log10 = 8,
        Exp = 9,
        /// -1.0 / 0.0 / 1.0, and NaN for NaN.
        Sign = 10,
    }
}

gnitz_wire::wire_enum! {
    /// Pure integer unary transform: same width in, same width out. `Neg` and
    /// `Abs` are `wrapping_*`, so `-i64::MIN` and `ABS(i64::MIN)` are `i64::MIN`,
    /// and the operand's U64 tracking carries to their result; `Sign` is
    /// -1 / 0 / 1, always signed, and reads an unsigned operand as never negative.
    pub enum IntUnaryOp: u32 {
        Neg = 0,
        Abs = 1,
        Sign = 2,
    }
}

gnitz_wire::wire_enum! {
    /// Integer arithmetic operator.
    pub enum IntArithOp: u32 {
        Add = 0,
        Sub = 1,
        Mul = 2,
        /// A zero divisor yields NULL.
        Div = 3,
        /// A zero divisor yields NULL.
        Mod = 4,
    }
}

gnitz_wire::wire_enum! {
    /// IEEE-754 arithmetic operator — [`IntArithOp`]'s float twin, minus `Mod`,
    /// plus `Pow`, which has no integer form.
    pub enum FloatArithOp: u32 {
        Add = 0,
        Sub = 1,
        Mul = 2,
        /// A zero divisor yields NULL.
        Div = 3,
        /// `powf`: the IEEE result, never NULL.
        Pow = 4,
    }
}

gnitz_wire::wire_enum! {
    /// Which end(s) a `StrTrim` strips: its wire selector.
    pub enum TrimMode: u32 {
        Both = 0,
        Leading = 1,
        Trailing = 2,
    }
}

impl TrimMode {
    #[inline]
    pub(crate) fn trims_start(self) -> bool {
        matches!(self, TrimMode::Both | TrimMode::Leading)
    }

    #[inline]
    pub(crate) fn trims_end(self) -> bool {
        matches!(self, TrimMode::Both | TrimMode::Trailing)
    }
}

// ---------------------------------------------------------------------------
// LogicalInstr — the wire-mirroring form (logical column indices)
// ---------------------------------------------------------------------------

/// A register, named by the index of the one instruction that writes it;
/// instructions therefore carry no destination, and a forged register file is
/// unrepresentable.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
/// be expressed, and no output slot is left holding the output batch's recycled
/// bytes.
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

    /// The input column this sink copies, the other half of [`Self::reg`].
    fn col(self) -> Option<u32> {
        match self {
            Sink::Col(c) => Some(c),
            Sink::Reg(_) => None,
        }
    }
}

/// One instruction with logical (schema) column indices, mirroring the wire
/// opcodes the client emits. `LogicalProgram::resolve_program` lowers each into
/// `Instr`.
///
/// Every variant writes the register named by its own position, so none carries
/// a destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalInstr {
    /// A fixed-width integer or float column into a scalar register; the kernel
    /// is picked from the column's type.
    LoadCol {
        col: u32,
    },
    /// `unsigned`: `val`'s bits are a `u64`, and the register is U64-tracked.
    LoadConst {
        val: i64,
        unsigned: bool,
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
    /// A calendar transform of a temporal register, read as microseconds when
    /// `micros` and as days otherwise. The two conversion ops may yield NULL.
    Calendar {
        op: CalendarOp,
        a: Reg,
        micros: bool,
    },
    /// Truncate-toward-zero float→int cast with a range check against `fi`.
    /// NaN, ±∞ and an out-of-range truncated value all produce NULL.
    FloatToInt {
        a: Reg,
        fi: FixedInt,
    },
    /// Integer domain cast: the register bits pass through unchanged when the
    /// value is in `fi`'s domain, and the row is NULLed otherwise. The source is
    /// read signed or unsigned per the resolve-time U64 tracking of `a`.
    IntCast {
        a: Reg,
        fi: FixedInt,
    },
    /// [`gnitz_wire::narrow_f32`], NULL where that has no value: NaN and ±∞ pass
    /// through, as does underflow to ±0.
    FloatToF32 {
        a: Reg,
    },
    /// Null-*skipping* 2-ary extremum: a NULL operand yields the other operand,
    /// and the result is NULL only when both are. Compares signed or unsigned
    /// per the U64 tracking of either operand.
    IntMinMax2 {
        a: Reg,
        b: Reg,
        is_max: bool,
    },
    /// [`LogicalInstr::IntMinMax2`]'s float twin, comparing by `f64::total_cmp`
    /// — never `==` or `partial_cmp`.
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
    /// shape. Reads the batch bitmap without a load, which is what lets it alone
    /// serve a column no load opcode admits (U128, BLOB).
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
    /// `col LIKE 'lit%'`: whether German-string column `col` starts with the
    /// text at `const_idx`.
    StrColPrefix {
        col: u32,
        const_idx: ConstIdx,
    },
    /// Integer set membership: `value_reg ∈ set[set_idx]`, over the strictly
    /// ascending pool of packed `N × 8-byte LE` values at const index `set_idx`,
    /// decoded once at `resolve_program`. NULL input propagates to NULL.
    IntInSet {
        value_reg: Reg,
        set_idx: ConstIdx,
    },
    /// A German-string column into a string register; nulls come from the batch
    /// bitmap.
    LoadColStr {
        col: u32,
    },
    /// A const-pool entry's raw bytes into a string register.
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
    /// Byte-lexicographic (`[u8]::cmp` over the content bytes), the same order
    /// `compare_german_strings` imposes on canonical cells.
    StrCmp {
        op: CmpOp,
        a: Reg,
        b: Reg,
    },
    /// Length into a *scalar* register: bytes, or `chars` — the codepoint count.
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
    /// Strip the byte set at const index `set_idx` from the end(s) `mode` names.
    StrTrim {
        a: Reg,
        mode: TrimMode,
        set_idx: ConstIdx,
    },
    /// SQL LIKE into a scalar register, a definite 0/1. `pat_idx` names a
    /// [`crate::LikePattern`]'s bytes; `ci` is ILIKE.
    StrLike {
        src: Reg,
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
    /// Integer register to decimal text, read signed or unsigned per the
    /// resolve-time U64 tracking of `a`.
    IntToStr {
        a: Reg,
    },
    /// Float register to text, as [`crate::FloatText`] spells it. `f32`: `a`
    /// holds a widened f32, spelled as that f32's shortest form.
    FloatToStr {
        a: Reg,
        f32: bool,
    },
    /// Parse ASCII decimal — surrounding whitespace and an optional sign
    /// allowed, nothing else — into the scalar register. An unparsable string or
    /// a value outside `fi`'s domain is NULL, never a wrap.
    StrToInt {
        a: Reg,
        fi: FixedInt,
    },
    /// Parse into an f64 scalar register; any failure, non-UTF-8 bytes included,
    /// is NULL.
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

/// How a `Cmp` reads its two registers' bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IntOrder {
    Signed,
    Unsigned,
    /// `a` is unsigned and `b` signed; resolve swaps a signed/unsigned pair into
    /// this orientation.
    UnsignedSigned,
}

/// One resolved, evaluable instruction. A `signed: false` flag reads an i64
/// register as u64, from the per-register U64 tracking: the destination's for
/// `IntArith` (only `Div` and `Mod` read it) and `IntMinMax2`; an `IntReg`
/// operand carries its own register's. `Cmp` carries the pair's [`IntOrder`].
///
/// Operands and null behaviour are classified on [`LogicalInstr`], by
/// [`operands`]. A variant here with no 1:1 logical counterpart therefore
/// inherits both from whichever opcode lowers into it, and must match it in
/// each.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Instr {
    /// Payload integer load. `fi` *is* the eight-arm decode the kernel dispatches
    /// on, established once at resolve time, so the row loop carries no wildcard
    /// arm.
    LoadPayloadInt {
        pi: u8,
        fi: FixedInt,
    },
    /// Payload F32 load, widened to the register's f64 image. An F64 column
    /// needs no kernel of its own — it lowers to `LoadPayloadInt` with `I64`.
    LoadPayloadF32 {
        pi: u8,
    },
    /// PK-region integer load: the OPK column at byte `off`.
    LoadPk {
        off: u8,
        fi: FixedInt,
    },
    /// `signed` is the resolve-time U64 tracking of the destination; only `Div`
    /// and `Mod` read it.
    IntArith {
        op: IntArithOp,
        a: u16,
        b: u16,
        signed: bool,
    },
    Cmp {
        op: CmpOp,
        a: u16,
        b: u16,
        order: IntOrder,
    },
    FCmp {
        op: CmpOp,
        a: u16,
        b: u16,
    },
    FloatArith {
        op: FloatArithOp,
        a: u16,
        b: u16,
    },
    IntToFloat {
        a: IntReg,
    },
    FloatUnary {
        op: FloatUnaryOp,
        a: u16,
    },
    /// Only `Sign` reads `a`'s signedness.
    IntUnary {
        op: IntUnaryOp,
        a: IntReg,
    },
    Calendar {
        op: CalendarOp,
        a: u16,
        micros: bool,
    },
    FloatToInt {
        a: u16,
        fi: FixedInt,
    },
    IntCast {
        a: IntReg,
        fi: FixedInt,
    },
    FloatToF32 {
        a: u16,
    },
    IntMinMax2 {
        a: u16,
        b: u16,
        is_max: bool,
        signed: bool,
    },
    FloatMinMax2 {
        a: u16,
        b: u16,
        is_max: bool,
    },
    /// SQL CASE blend over raw i64 bit patterns.
    Select {
        cond: u16,
        a: u16,
        b: u16,
    },
    /// Three-valued AND, or OR when `is_or` — one kernel, the operator carried
    /// as data, as [`LogicalInstr::BoolBinary`] carries it.
    BoolBinary {
        a: u16,
        b: u16,
        is_or: bool,
    },
    BoolNot {
        a: u16,
    },
    IsNull {
        pi: u8,
        invert: bool,
    },
    IsNullReg {
        a: u16,
        invert: bool,
    },
    /// A German-string column against `ResolvedProgram::const_cells[const_idx]`.
    StrColConst {
        op: CmpOp,
        pi: u8,
        const_idx: u32,
    },
    StrColCol {
        op: CmpOp,
        pi_a: u8,
        pi_b: u8,
    },
    /// A German-string column against the prefix spanning `len` bytes at `off`
    /// of `ResolvedProgram::const_arena`.
    StrColPrefix {
        pi: u8,
        off: u32,
        len: u32,
    },
    /// Integer set membership: `value_reg ∈ ResolvedProgram::int_sets[set_idx]`.
    IntInSet {
        value_reg: u16,
        set_idx: u32,
    },
    /// A German-string column into a string register. `pi` is the payload slot,
    /// resolved from the logical column index, and indexes the evaluator's
    /// region table.
    LoadColStr {
        pi: u8,
    },
    StrSelect {
        cond: u16,
        a: u16,
        b: u16,
    },
    StrCmp {
        op: CmpOp,
        a: u16,
        b: u16,
    },
    StrLen {
        a: u16,
        chars: bool,
    },
    StrCase {
        a: u16,
        upper: bool,
    },
    StrSubstr {
        src: u16,
        start: IntReg,
        len: Option<IntReg>,
    },
    /// Strips the bytes of `ResolvedProgram::trim_sets[set_idx]`.
    StrTrim {
        a: u16,
        mode: TrimMode,
        set_idx: u32,
    },
    /// `matcher_idx` indexes `ResolvedProgram::like_matchers`, not the const
    /// pool.
    StrLike {
        src: u16,
        matcher_idx: u32,
    },
    StrConcat {
        a: u16,
        b: u16,
        skip_null: bool,
    },
    IntToStr {
        a: IntReg,
    },
    FloatToStr {
        a: u16,
        f32: bool,
    },
    StrToInt {
        a: u16,
        fi: FixedInt,
    },
    StrToFloat {
        a: u16,
    },
    StrSide {
        src: u16,
        n: IntReg,
        left: bool,
    },
    StrPos {
        hay: u16,
        needle: u16,
    },
    StrReverse {
        a: u16,
    },
    StrReplace {
        s: u16,
        from: u16,
        to: u16,
    },
    StrPad {
        s: u16,
        n: IntReg,
        fill: u16,
        left: bool,
    },
    StrSplitPart {
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
    /// The type this instruction range-checks its result into, for the casts
    /// that do — a value they produce is whole in that type's low bytes, which is
    /// what lets [`crate::map::EmitStore::for_slot`] admit a slot narrower than the register.
    /// `None` for every other instruction: a register is otherwise the full
    /// 8-byte image and narrowing it would truncate.
    pub(crate) fn range_check(&self) -> Option<FixedInt> {
        match *self {
            LogicalInstr::IntCast { fi, .. }
            | LogicalInstr::FloatToInt { fi, .. }
            | LogicalInstr::StrToInt { fi, .. } => Some(fi),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// LogicalProgram — pre-resolve container
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct LogicalProgram {
    /// The compute instructions. Instruction `i` writes register `i`, so this is
    /// the register file too, and its length is the register count.
    instrs: Vec<LogicalInstr>,
    /// The output: one `Sink::Reg` for a filter or scalar, one sink per output
    /// payload slot for a map.
    sinks: Vec<Sink>,
    const_strings: Vec<Vec<u8>>,
    /// Bit `r` set iff register `r` holds a string rather than a scalar, as
    /// [`Self::new`] finished it.
    ///
    /// **Position-independent**, which is what lets every later pass read the
    /// finished mask instead of rebuilding one in step: a register's one writer
    /// is the instruction at its own index, ahead of every reader.
    str_class: u64,
}

impl LogicalProgram {
    /// Assemble from typed instructions, holding every one to the rules a schema
    /// is not needed for. Every constructor routes through here and no field is
    /// reachable to mutate after it, so the walk below establishes, for every
    /// `LogicalProgram`, that each operand register precedes its reader, which
    /// `split_regs` relies on, and the `str_class` mask.
    pub fn new(
        instrs: Vec<LogicalInstr>,
        sinks: Vec<Sink>,
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
        for (i, instr) in instrs.iter().enumerate() {
            // Bound every operand off the one per-opcode operand table, so no
            // arm below restates them.
            let ops = operands(instr);
            // The const-pool index an opcode carries rather than names as an
            // operand — the table states that too.
            if let Some((idx, want)) = ops.pool {
                check_pool_entry(idx, want, &const_strings)?;
            }
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
                if read != ReadAs::NullBit && ((str_class >> reg.0) & 1 != 0) != (read == ReadAs::Str) {
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
        Ok(LogicalProgram { instrs, sinks, const_strings, str_class })
    }

    /// The typed instructions, in emission order — instruction `i` writing
    /// register `i`.
    pub fn instrs(&self) -> &[LogicalInstr] {
        &self.instrs
    }

    /// Every input column the program reads: an instruction's column operand or
    /// a sink's copied column.
    pub fn cols_read(&self) -> impl Iterator<Item = u32> + '_ {
        let ops = self
            .instrs
            .iter()
            .flat_map(|i| operands(i).cols.into_iter().flatten().map(|(c, _)| c));
        ops.chain(self.sinks.iter().filter_map(|s| s.col()))
    }

    /// The payload columns of `schema`, as payload bits, the program `blob`
    /// reads: every one for a blob that does not decode.
    pub fn payload_read(blob: &[u8], schema: &SchemaDescriptor) -> u64 {
        Self::from_blob(blob).map_or(schema.payload_slots(), |p| schema.payload_bits(p.cols_read()))
    }

    /// The output sinks.
    pub(crate) fn sinks(&self) -> &[Sink] {
        &self.sinks
    }

    /// The const pool the instructions index into.
    pub fn const_strings(&self) -> &[Vec<u8>] {
        &self.const_strings
    }

    /// Check the program against `schema` and lower it to the resolved form, with
    /// `sink_read` how the consumer reads each sink's register.
    pub(crate) fn resolve_program(
        &self,
        schema: &SchemaDescriptor,
        sink_read: ReadAs,
    ) -> Result<ResolvedProgram, ExprValidateErr> {
        use Instr as I;
        use LogicalInstr as L;

        // The per-register masks, built in stream order off the operand table:
        // every register an instruction reads is a lower one, so each bit the
        // lowering asks for is already final.
        let (mut bool_produced, mut non_bool_read, mut bool_input) = (0u64, 0u64, 0u64);
        let mut read_as = |reg: Reg, read: ReadAs| match read {
            ReadAs::Bool => bool_input |= 1u64 << reg.0,
            // A null-lane read needs neither the value nor the truth bit.
            ReadAs::NullBit => {}
            ReadAs::Value | ReadAs::Str => non_bool_read |= 1u64 << reg.0,
        };
        let (mut null_regs, mut reg_u64) = (0u64, 0u64);
        let mut instrs = Vec::with_capacity(self.instrs.len());
        // By pool index, each built when an opcode first names it.
        let n_pool = self.const_strings.len();
        let mut int_sets: Vec<Option<Vec<i64>>> = vec![None; n_pool];
        let mut trim_sets: Vec<Option<[u64; 4]>> = vec![None; n_pool];
        let mut const_cells: Vec<Option<[u8; 16]>> = vec![None; n_pool];
        let mut const_spans: Vec<Option<(u32, u32)>> = vec![None; n_pool];
        let mut like_matchers: Vec<LikeMatcher> = Vec::new();
        let mut const_arena: Vec<u8> = Vec::new();
        // Off the instruction stream: a constant register names no computation,
        // and each stream entry would cost the per-morsel dispatch a no-op arm.
        let mut const_regs: Vec<(u16, i64)> = Vec::new();
        let mut const_str_regs: Vec<(u16, u32, u32)> = Vec::new();
        let mut const_nulls: u64 = 0;
        let nullable_slots = schema.nullable_payload_slots();
        // By payload slot, the cell width an instruction reads it at; 0 if none.
        let mut read_width = [0u8; PAYLOAD_SLOTS];
        // Pool entry `ci`'s span of `const_arena`, copied in on first reference.
        fn const_span(spans: &mut [Option<(u32, u32)>], arena: &mut Vec<u8>, pool: &[Vec<u8>], ci: u32) -> (u32, u32) {
            *spans[ci as usize].get_or_insert_with(|| {
                let bytes = &pool[ci as usize];
                let span = (arena.len() as u32, bytes.len() as u32);
                arena.extend_from_slice(bytes);
                span
            })
        }
        // A string column's payload slot; the operand pass admitted it as one.
        let str_slot = |col: u32| {
            schema
                .payload_slot(col as usize)
                .expect("a string column is a payload column") as u8
        };
        for (i, li) in self.instrs.iter().copied().enumerate() {
            // A register is the index of the instruction that writes it.
            let dst = i as u16;
            let ops = operands(&li);
            // Each column operand is held to `schema` here, so the lowering
            // below locates its columns infallibly.
            let (mut col_is_u64, mut may_null) = (false, ops.makes_null);
            for &(col, kind) in ops.cols.iter().flatten() {
                let loc = locate_col(schema, col, kind)?;
                col_is_u64 |= loc.type_code() == TypeCode::U64;
                // A decoding kernel reads a payload column's cells at their
                // width, and carries its NULL.
                if kind.decodes() {
                    if let Some(slot) = loc.payload_slot() {
                        read_width[slot] = loc.size() as u8;
                    }
                    may_null |= loc.is_null_word(nullable_slots);
                }
            }
            match ops.write {
                WriteAs::Bool => bool_produced |= 1u64 << i,
                WriteAs::Value(rule) => {
                    let is_u64 = match rule {
                        U64Rule::Fixed(v) => v,
                        // In stream order: `reg_u64` holds every register this one reads.
                        U64Rule::FromOperands => (ops.reads.iter().flatten())
                            .any(|&(reg, read)| read == ReadAs::Value && (reg_u64 >> reg.0) & 1 != 0),
                        U64Rule::FromColType => col_is_u64,
                    };
                    reg_u64 |= (is_u64 as u64) << i;
                }
                WriteAs::Str => {}
            }
            for &(reg, read) in ops.reads.iter().flatten() {
                // A null test reads the NULL away; every other read carries it.
                may_null |= read != ReadAs::NullBit && (null_regs >> reg.0) & 1 != 0;
                read_as(reg, read);
            }
            null_regs |= (may_null as u64) << i;
            // Off each opcode's `U64Rule`, so no arm below restates the rule;
            // `dst` and every register it reads are set by now.
            let is_u64 = |r: u16| (reg_u64 >> r) & 1 != 0;
            let int_reg = |r: Reg| IntReg { reg: r.0, signed: !is_u64(r.0) };
            // A constant leaves the stream for its own table.
            let resolved = match li {
                L::LoadCol { col } => {
                    let loc = schema.locate(col as usize);
                    let kind = ScalarKind::from_type_code(loc.type_code()).expect("locate_col admitted a ScalarCol");
                    match (kind, loc) {
                        (ScalarKind::Int(fi), ColumnLocator::Pk { byte_off, .. }) => I::LoadPk { off: byte_off, fi },
                        (ScalarKind::Int(fi), ColumnLocator::Payload { slot, .. }) => {
                            I::LoadPayloadInt { pi: slot, fi }
                        }
                        // An F64 column's bytes are already the register's image.
                        (ScalarKind::F64, ColumnLocator::Payload { slot, .. }) => {
                            I::LoadPayloadInt { pi: slot, fi: FixedInt::I64 }
                        }
                        (ScalarKind::F32, ColumnLocator::Payload { slot, .. }) => I::LoadPayloadF32 { pi: slot },
                        (ScalarKind::F32 | ScalarKind::F64, ColumnLocator::Pk { .. }) => {
                            unreachable!("a PK column is an integer")
                        }
                    }
                }
                L::LoadConst { val, .. } => {
                    const_regs.push((dst, val));
                    continue;
                }
                // The one thing resolution adds to the operator: which of the
                // two dividing kernels runs unsigned, off the per-register U64
                // tracking.
                L::IntArith { op, a: Reg(a), b: Reg(b) } => I::IntArith { op, a, b, signed: !is_u64(dst) },
                L::FloatArith { op, a: Reg(a), b: Reg(b) } => I::FloatArith { op, a, b },
                // Each operand is read with its own signedness, so a U64 value past
                // 2^63 never equals or orders against a negative one.
                L::Cmp { op, a: Reg(a), b: Reg(b) } => {
                    let (op, a, b, order) = match (is_u64(a), is_u64(b)) {
                        (false, false) => (op, a, b, IntOrder::Signed),
                        (true, true) => (op, a, b, IntOrder::Unsigned),
                        (true, false) => (op, a, b, IntOrder::UnsignedSigned),
                        (false, true) => (op.converse(), b, a, IntOrder::UnsignedSigned),
                    };
                    I::Cmp { op, a, b, order }
                }
                L::FCmp { op, a: Reg(a), b: Reg(b) } => I::FCmp { op, a, b },
                L::FloatUnary { op, a: Reg(a) } => I::FloatUnary { op, a },
                L::IntUnary { op, a } => I::IntUnary { op, a: int_reg(a) },
                L::Calendar { op, a: Reg(a), micros } => I::Calendar { op, a, micros },
                L::FloatToF32 { a: Reg(a) } => I::FloatToF32 { a },
                L::FloatToInt { a: Reg(a), fi } => I::FloatToInt { a, fi },
                L::IntCast { a, fi } => I::IntCast { a: int_reg(a), fi },
                L::IntMinMax2 { a: Reg(a), b: Reg(b), is_max } => I::IntMinMax2 { a, b, is_max, signed: !is_u64(dst) },
                L::FloatMinMax2 { a: Reg(a), b: Reg(b), is_max } => I::FloatMinMax2 { a, b, is_max },
                L::IntToFloat { a } => I::IntToFloat { a: int_reg(a) },
                L::Select { cond: Reg(cond), a: Reg(a), b: Reg(b) } => I::Select { cond, a, b },
                L::LoadNull | L::LoadNullStr => {
                    const_nulls |= 1u64 << dst;
                    continue;
                }
                L::BoolBinary { a: Reg(a), b: Reg(b), is_or } => I::BoolBinary { a, b, is_or },
                L::BoolNot { a: Reg(a) } => I::BoolNot { a },
                // A column that cannot be NULL — a PK column, or a `NOT NULL`
                // payload one — is a constant.
                L::IsNull { col, invert } => match schema.locate(col as usize) {
                    ColumnLocator::Payload { slot, .. } if gnitz_wire::null_word_get(nullable_slots, slot as usize) => {
                        I::IsNull { pi: slot, invert }
                    }
                    _ => {
                        const_regs.push((dst, invert as i64));
                        continue;
                    }
                },
                L::IsNullReg { a: Reg(a), invert } => I::IsNullReg { a, invert },
                L::StrColConst { op, col, const_idx: ConstIdx(const_idx) } => {
                    let ci = const_idx as usize;
                    const_cells[ci]
                        .get_or_insert_with(|| encode_german_string(&self.const_strings[ci], &mut const_arena));
                    I::StrColConst { op, pi: str_slot(col), const_idx }
                }
                L::StrColCol { op, col_a, col_b } => I::StrColCol {
                    op,
                    pi_a: str_slot(col_a),
                    pi_b: str_slot(col_b),
                },
                L::StrColPrefix { col, const_idx: ConstIdx(const_idx) } => {
                    let (off, len) = const_span(&mut const_spans, &mut const_arena, &self.const_strings, const_idx);
                    I::StrColPrefix { pi: str_slot(col), off, len }
                }
                L::IntInSet {
                    value_reg: Reg(value_reg),
                    set_idx: ConstIdx(set_idx),
                } => {
                    let si = set_idx as usize;
                    int_sets[si].get_or_insert_with(|| decode_int_set(&self.const_strings[si]));
                    I::IntInSet { value_reg, set_idx }
                }
                L::LoadColStr { col } => I::LoadColStr { pi: str_slot(col) },
                L::LoadConstStr { const_idx: ConstIdx(const_idx) } => {
                    let (off, len) = const_span(&mut const_spans, &mut const_arena, &self.const_strings, const_idx);
                    const_str_regs.push((dst, off, len));
                    continue;
                }
                L::StrSelect { cond: Reg(cond), a: Reg(a), b: Reg(b) } => I::StrSelect { cond, a, b },
                L::StrCmp { op, a: Reg(a), b: Reg(b) } => I::StrCmp { op, a, b },
                L::StrLen { a: Reg(a), chars } => I::StrLen { a, chars },
                L::StrCase { a: Reg(a), upper } => I::StrCase { a, upper },
                L::StrSubstr { src: Reg(src), start_reg, len_reg } => I::StrSubstr {
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
                    trim_sets[si].get_or_insert_with(|| {
                        let mut table = [0u64; 4];
                        for &byte in &self.const_strings[si] {
                            table[(byte >> 6) as usize] |= 1u64 << (byte & 63);
                        }
                        table
                    });
                    I::StrTrim { a, mode, set_idx }
                }
                L::StrLike {
                    src: Reg(src),
                    pat_idx: ConstIdx(pat_idx),
                    ci,
                } => {
                    let pattern = &self.const_strings[pat_idx as usize];
                    let matcher_idx = like_matchers.len() as u32;
                    like_matchers.push(LikeMatcher::compile(pattern, ci));
                    I::StrLike { src, matcher_idx }
                }
                L::StrConcat { a: Reg(a), b: Reg(b), skip_null } => I::StrConcat { a, b, skip_null },
                L::IntToStr { a } => I::IntToStr { a: int_reg(a) },
                L::FloatToStr { a: Reg(a), f32 } => I::FloatToStr { a, f32 },
                L::StrToInt { a: Reg(a), fi } => I::StrToInt { a, fi },
                L::StrToFloat { a: Reg(a) } => I::StrToFloat { a },
                L::StrSide { src: Reg(src), n_reg, left } => I::StrSide { src, n: int_reg(n_reg), left },
                L::StrPos { hay: Reg(hay), needle: Reg(needle) } => I::StrPos { hay, needle },
                L::StrReverse { a: Reg(a) } => I::StrReverse { a },
                L::StrReplace { s: Reg(s), from: Reg(from), to: Reg(to) } => I::StrReplace { s, from, to },
                L::StrPad { s: Reg(s), n_reg, fill: Reg(fill), left } => I::StrPad { s, n: int_reg(n_reg), fill, left },
                L::StrSplitPart { s: Reg(s), delim: Reg(delim), n_reg } => {
                    I::StrSplitPart { s, delim, n: int_reg(n_reg) }
                }
            };
            instrs.push((dst, resolved));
        }
        // A sink is one more read of its register, as `sink_read`.
        for reg in self.sinks.iter().filter_map(|s| s.reg()) {
            read_as(reg, sink_read);
        }
        Ok(ResolvedProgram {
            bit_only: bool_produced & !non_bool_read,
            // `bool_input` alone: a register in `bit_only \ bool_input` has
            // `IsNullReg` as its only possible reader, which reads the null
            // words and never the packed bit.
            bool_pack: bool_input,
            null_regs,
            reg_u64,
            nullable_slots,
            read_mask: (read_width.iter().enumerate())
                .filter(|&(_, &w)| w != 0)
                .fold(0, |m, (slot, _)| m | 1 << slot),
            read_widths: read_width,
            instrs,
            const_regs,
            const_str_regs,
            const_nulls,
            const_cells: dense(const_cells),
            int_sets: dense(int_sets),
            trim_sets: dense(trim_sets),
            like_matchers,
            const_arena,
            // A register file is exactly one entry per instruction.
            num_regs: self.instrs.len() as u32,
            str_lanes: MAX_REGS as u32 - self.str_class.leading_zeros(),
        })
    }

    /// Whether register `r` holds a string rather than a scalar.
    pub(crate) fn is_str(&self, r: Reg) -> bool {
        (self.str_class >> r.0) & 1 != 0
    }
}

/// Bound the const-pool index an opcode carries rather than names as an
/// operand, and hold the entry to what the opcode reads it as.
fn check_pool_entry(idx: ConstIdx, want: PoolEntry, const_strings: &[Vec<u8>]) -> Result<(), ExprValidateErr> {
    let bytes = const_strings
        .get(idx.0 as usize)
        .ok_or(ExprValidateErr::ConstIdxOutOfRange { const_idx: idx.0, n: const_strings.len() })?;
    let ok = match want {
        PoolEntry::Bytes => true,
        PoolEntry::Text => std::str::from_utf8(bytes).is_ok(),
        PoolEntry::LikePattern => LikePattern::is_encoding(bytes),
        PoolEntry::TrimSet => bytes.is_ascii(),
        PoolEntry::IntSet => int_set_is_canonical(bytes),
    };
    match ok {
        true => Ok(()),
        false => Err(ExprValidateErr::PoolEntryMalformed { const_idx: idx.0, want }),
    }
}

/// How a kernel consumes a register operand.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadAs {
    /// The register's i64 / f64 image out of its lanes.
    Value,
    /// A German-string view out of `str_views`.
    Str,
    /// A packed truth bit out of the truth words, never the i64 image — which
    /// is what lets a producer whose every reader is a `Bool` skip the unpack
    /// (`bit_only`).
    Bool,
    /// The register's null words alone: neither its value nor its truth bit,
    /// so the operand's class is not constrained and its producer need not
    /// unpack.
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
    /// therefore take the packed truth bit instead, and the kernel may skip
    /// writing the lanes when the register is `bit_only`. A mask-style `-1`
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
    /// signed target); for a cast, whether its target names U64; for a
    /// constant, its own flag.
    Fixed(bool),
    /// U64 iff any [`ReadAs::Value`] operand is. `ReadAs::Bool` operands are
    /// excluded — SELECT's condition is a truth bit, not part of its result.
    FromOperands,
    /// U64 iff the column operand's declared type code is U64.
    FromColType,
}

/// Every operand of one instruction — the registers it reads, the columns it
/// addresses — each with what its kernel requires of it, plus what it writes
/// into its own register and whether the kernel can manufacture a NULL.
///
/// The **one** per-opcode table, read by the walks over the instruction stream
/// — [`LogicalProgram::new`] and `resolve_program` — so a new opcode is classified
/// once: its operands, its NULL production and its destination's U64-ness alike.
/// Every arm destructures every field of every variant it matches, with `_` for
/// fields it ignores and **no `..`** — so an opcode that gains an operand is a
/// compile error here rather than an unbounded operand reaching the kernels.
struct Operands {
    /// What the instruction writes into the register it names by position.
    write: WriteAs,
    reads: [Option<(Reg, ReadAs)>; MAX_READS],
    cols: [Option<(u32, ColKind)>; MAX_COL_OPERANDS],
    /// The const-pool entry the opcode carries rather than names as an operand,
    /// and what it reads that entry as.
    pool: Option<(ConstIdx, PoolEntry)>,
    /// True iff the kernel can turn non-NULL input into a NULL result — a zero
    /// divisor, an out-of-range cast, an unparsable text→number, a CONCAT past
    /// `u32::MAX`, `LoadNull*` on every row. A column operand's own nullability
    /// is `cols`, not this.
    makes_null: bool,
}

/// What an opcode reads its const-pool entry as. The pool is client-controlled,
/// so [`LogicalProgram::new`] holds each named entry to its kind before a
/// kernel reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoolEntry {
    /// Any bytes: a German-string compare, which also serves BLOB columns.
    Bytes,
    /// A STRING value, so UTF-8.
    Text,
    /// A LIKE pattern: UTF-8 text around the wildcard bytes.
    LikePattern,
    /// TRIM's byte set. ASCII, so stripping bytes never splits a character.
    TrimSet,
    /// An `IntInSet` pool in canonical form.
    IntSet,
}

/// The most registers one opcode reads.
const MAX_READS: usize = 3;
/// `StrColCol` compares two columns; no opcode addresses more.
const MAX_COL_OPERANDS: usize = 2;

fn writes(write: WriteAs) -> Operands {
    Operands {
        write,
        reads: [None; MAX_READS],
        cols: [None; MAX_COL_OPERANDS],
        pool: None,
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

    /// Record the const-pool entry the construction pass must bound and check.
    fn with_pool(mut self, idx: ConstIdx, want: PoolEntry) -> Self {
        self.pool = Some((idx, want));
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
            // No `_` arm: a new operator must say whether it makes NULL.
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
        // Null-skipping: a NULL operand yields the other.
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
        L::Calendar { op, a, micros: _ } => {
            let ops = writes(WVal(Fixed(false))).reading(a, RVal);
            if op.may_null() {
                ops.may_null()
            } else {
                ops
            }
        }
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
            .with_pool(set_idx, PoolEntry::IntSet),
        L::LoadConst { val: _, unsigned } => writes(WVal(Fixed(unsigned))),
        // A NULL on every row.
        L::LoadNull => writes(WVal(Fixed(false))).may_null(),

        // --- Column operands ---
        L::LoadCol { col } => writes(WVal(FromColType)).on_col(col, ColKind::ScalarCol),
        // The null-bitmap reader decodes no value, so any column will do — U128
        // and STRING included — and its boolean is definite over a NULL row.
        L::IsNull { col, invert: _ } => writes(WBool).on_col(col, ColKind::AnyCol),
        L::IsNullReg { a, invert: _ } => writes(WBool).reading(a, ReadAs::NullBit),
        // The German-string compares read 16-byte cells; a narrower column
        // would make `col_data(pi, 16)` over-read its region. The verdict is a
        // bool, but a NULL operand makes it NULL, so the null bit flows.
        L::StrColConst { op: _, col, const_idx } => writes(WBool)
            .on_col(col, ColKind::StringCol)
            .with_pool(const_idx, PoolEntry::Bytes),
        L::StrColCol { op: _, col_a, col_b } => writes(WBool)
            .on_col(col_a, ColKind::StringCol)
            .on_col(col_b, ColKind::StringCol),
        L::StrColPrefix { col, const_idx } => writes(WBool)
            .on_col(col, ColKind::StringCol)
            .with_pool(const_idx, PoolEntry::Text),
        // The string-register column load reads the same 16-byte cells the
        // `LogicalInstr::StrCol*` compares do, so it carries the same requirement.
        L::LoadColStr { col } => writes(WStr).on_col(col, ColKind::StringCol),

        // --- String registers ---
        L::LoadConstStr { const_idx } => writes(WStr).with_pool(const_idx, PoolEntry::Text),
        // A NULL on every row.
        L::LoadNullStr => writes(WStr).may_null(),
        L::IntToStr { a } | L::FloatToStr { a, f32: _ } => writes(WStr).reading(a, RVal),
        L::StrLen { a, chars: _ } => writes(WVal(Fixed(false))).reading(a, RStr),
        // Both text→number parses yield NULL on an unparsable or out-of-range
        // value.
        L::StrToFloat { a } => writes(WVal(Fixed(false))).reading(a, RStr).may_null(),
        L::StrToInt { a, fi } => writes(WVal(Fixed(fi == FixedInt::U64))).reading(a, RStr).may_null(),
        L::StrCmp { op: _, a, b } => writes(WBool).reading(a, RStr).reading(b, RStr),
        L::StrLike { src, pat_idx, ci: _ } => writes(WBool)
            .reading(src, RStr)
            .with_pool(pat_idx, PoolEntry::LikePattern),
        L::StrCase { a, upper: _ } => writes(WStr).reading(a, RStr),
        L::StrTrim { a, mode: _, set_idx } => writes(WStr).reading(a, RStr).with_pool(set_idx, PoolEntry::TrimSet),
        // A combined length above `u32::MAX` yields NULL, which is easy to miss
        // because CONCAT otherwise looks like a pure transform.
        L::StrConcat { a, b, skip_null: _ } => writes(WStr).reading(a, RStr).reading(b, RStr).may_null(),
        // A scalar condition blending two string branches.
        L::StrSelect { cond, a, b } => writes(WStr).reading(cond, RBool).reading(a, RStr).reading(b, RStr),
        // A string source with integer window bounds. SUBSTR's only NULL is a
        // negative length, so the no-FOR form makes none — the window itself
        // clamps every out-of-range endpoint.
        L::StrSubstr { src, start_reg, len_reg } => {
            let ops = writes(WStr).reading(src, RStr).reading(start_reg, RVal);
            match len_reg {
                Some(len) => ops.reading(len, RVal).may_null(),
                None => ops,
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
        L::StrPad { s, n_reg, fill, left: _ } => writes(WStr)
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

/// Column operand `col` located in `s`, held to what `need`'s kernel can read:
/// in range and of a type its kernel decodes.
pub(crate) fn locate_col(s: &SchemaDescriptor, col: u32, need: ColKind) -> Result<ColumnLocator, ExprValidateErr> {
    let Some(loc) = s.try_locate(col as usize) else {
        return Err(ExprValidateErr::ColOutOfRange { col, num_columns: s.num_columns() });
    };
    let type_code = loc.type_code();
    if !need.admits(type_code) {
        return Err(ExprValidateErr::ColKindMismatch { col, type_code, want: need });
    }
    Ok(loc)
}

/// One slot per entry, the entries nothing built left at their default.
fn dense<T: Default>(slots: Vec<Option<T>>) -> Vec<T> {
    slots.into_iter().map(Option::unwrap_or_default).collect()
}

/// The `LogicalInstr::IntInSet` const-pool form: `N × 8-byte LE` i64s, strictly
/// ascending.
fn int_set_is_canonical(bytes: &[u8]) -> bool {
    bytes.len().is_multiple_of(8)
        && bytes
            .as_chunks::<8>()
            .0
            .windows(2)
            .all(|w| i64::from_le_bytes(w[0]) < i64::from_le_bytes(w[1]))
}

fn decode_int_set(bytes: &[u8]) -> Vec<i64> {
    debug_assert!(int_set_is_canonical(bytes), "construction rejects a non-canonical pool");
    gnitz_wire::le_scalars(bytes).expect("construction rejects a non-canonical pool")
}

// ---------------------------------------------------------------------------
// ResolvedProgram — the evaluable form
// ---------------------------------------------------------------------------

pub(crate) struct ResolvedProgram {
    /// The computing instructions, as `(destination, instruction)`: the constant
    /// loads are lifted out, so a position is not a register.
    pub(crate) instrs: Vec<(u16, Instr)>,
    /// The scalar constant registers, as `(destination, value)`: written once per
    /// evaluator, not per morsel.
    pub(crate) const_regs: Vec<(u16, i64)>,
    /// The string constant registers, as `(destination, `[`Self::const_arena`]
    /// `offset, length)` — the same treatment, over the lane array.
    pub(crate) const_str_regs: Vec<(u16, u32, u32)>,
    /// Bit `r` set iff register `r` is a NULL constant of either class: its
    /// null words are set once per evaluator, beside the other constants.
    pub(crate) const_nulls: u64,
    /// `StrColConst`'s constants as German-string cells, by pool index; a long
    /// one's heap half is in `const_arena`.
    pub(crate) const_cells: Vec<[u8; 16]>,
    /// `IntInSet`'s value pools, strictly ascending, by pool index.
    pub(crate) int_sets: Vec<Vec<i64>>,
    /// `StrTrim`'s byte sets as 256-bit membership tables, by pool index.
    pub(crate) trim_sets: Vec<[u64; 4]>,
    /// One per `StrLike` instruction, at its `matcher_idx`: `ci` is compiled
    /// in, so two instructions over one pattern get one each.
    pub(crate) like_matchers: Vec<LikeMatcher>,
    /// Every constant byte the program needs at run time: the spans a
    /// `LoadConstStr` bakes in and a `StrColPrefix` compares, and the heap half of each long `const_cells`
    /// entry. Holds only the constants some opcode names, so an unreferenced
    /// pool entry is not carried for the plan's life. Also the string arena's
    /// non-cleared prefix: the per-morsel reset truncates back to
    /// `const_arena.len()`, so a `LoadConstStr` span stays valid for the
    /// evaluator's life.
    pub(crate) const_arena: Vec<u8>,
    /// Registers of either class: a string register keeps its null and truth
    /// words in the scalar file too.
    num_regs: u32,
    /// One past the highest string register; 0 for a program with none.
    pub(crate) str_lanes: u32,
    // The per-register masks below are meaningful only for the schema this
    // program was resolved against, since the payload indices in `instrs` were
    // assigned from it.
    /// Bit `r` set iff register `r` is only consumed by boolean ops, so nothing
    /// reads its lanes. A permission, not an obligation: the integer and float
    /// compares, the IN scan over a small set, the column string compares
    /// (`StrColConst`, `StrColCol`, `StrColPrefix`), `BoolBinary`, `BoolNot` and
    /// the null tests skip the lanes and write the packed truth bits natively.
    bit_only: u64,
    /// Bit `r` set iff `r`'s producer must write its packed truth bits: some
    /// downstream consumer reads it as a truth bit. A filter's result register
    /// is covered because resolution forces it into that set, whatever opcode
    /// writes it.
    bool_pack: u64,
    /// Bit `r` set iff register `r` may hold a NULL: its kernel can make one, it
    /// decodes a nullable column, or it reads such a register other than by a
    /// null test. A kernel writes no null word for a register outside it, whose
    /// operands are all outside it too.
    pub(crate) null_regs: u64,
    /// Bit `r` set iff register `r`'s i64 image is to be read as a `u64`, per
    /// each opcode's [`U64Rule`].
    reg_u64: u64,
    /// Bit `N` set iff payload slot `N`'s column admits NULL in the schema this
    /// program was resolved against. A column read from a slot outside this mask
    /// contributes no null bit, so its destination register's null words are
    /// left zero instead of gathered per row.
    ///
    /// Carries the same schema caveat as the masks above, and rests on the
    /// same assumption as [`Self::null_regs`]: a `NOT NULL` column's
    /// declared flag is believed over the bit the batch carries for it.
    pub(crate) nullable_slots: u64,
    /// Bit `N` set iff an instruction reads payload slot `N`: the regions an
    /// evaluation call fetches, once each, before its first morsel.
    pub(crate) read_mask: u64,
    /// The cell width each slot in [`Self::read_mask`] is read at; zero for
    /// every slot outside it.
    pub(crate) read_widths: [u8; PAYLOAD_SLOTS],
}

impl ResolvedProgram {
    pub(crate) fn num_regs(&self) -> usize {
        self.num_regs as usize
    }

    /// Per instruction per morsel: whether `reg`'s kernel tracks null words.
    pub(crate) fn may_null(&self, reg: u16) -> bool {
        (self.null_regs >> reg) & 1 != 0
    }

    /// Per instruction per morsel, from the BOOL arms of `eval_morsel`.
    pub(crate) fn is_bit_only(&self, reg: u16) -> bool {
        (self.bit_only >> reg) & 1 != 0
    }

    /// True iff `reg`'s producer must write its packed truth bits. Per
    /// instruction per morsel, from `maybe_pack_bool_bits`.
    pub(crate) fn needs_bool_pack(&self, reg: u16) -> bool {
        (self.bool_pack >> reg) & 1 != 0
    }

    /// Whether `reg`'s i64 image is to be read as a `u64`.
    pub(crate) fn is_u64(&self, reg: u16) -> bool {
        (self.reg_u64 >> reg) & 1 != 0
    }
}

#[cfg(test)]
#[path = "tests/program.rs"]
mod tests;

#[cfg(test)]
#[path = "fuzz/program.rs"]
mod fuzz;
