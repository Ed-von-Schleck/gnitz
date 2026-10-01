//! Compiled scalar-expression programs.
//!
//! Two typed forms: `LogicalProgram` (`LogicalInstr`, logical column indices —
//! the shape the wire blob lowers into) and `ResolvedProgram` (`Instr`, resolved
//! payload/PK indices — the evaluable form). `LogicalProgram::resolve_program`
//! lowers the former into the latter. Instruction meaning is carried by the
//! type: a missing or mis-routed opcode is a compile error, not a silent
//! miscompute. The wire vocabulary both forms are named in — [`ExprOp`], the
//! selector families, [`SinkKind`] — is declared here too, beside the
//! encode/decode pair that is its only reader.

use crate::calendar::CalendarOp;
use crate::like::{LikeMatcher, LikePattern};
use crate::{ColumnLocator, SchemaFacts};
use gnitz_wire::{decode_all, encode_german_string, FixedInt, ScalarKind, TypeCode, Writer};
use std::fmt;

/// The register file is capped at 64: the `BoolBinary` 3VL paths, the
/// null-bit propagation, and every register-indexed mask address registers by
/// bit in a `u64`.
pub(crate) const MAX_REGS: usize = u64::BITS as usize;

/// The const pool's entry cap: an instruction names at most one pool entry, and
/// `from_instrs` caps instructions at [`MAX_REGS`] — so an entry past this is one
/// no instruction can index.
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

/// Why a client-authored expr program was rejected at compile. A variant's payload
/// is there for its `Display` to name the offending operand.
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
    ColNotPayload {
        col: u32,
    },
    ColKindMismatch {
        col: u32,
        type_code: TypeCode,
        want: &'static str,
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
    /// A range walk that names no walkable key: a column out of range or of a
    /// type with no key order, or an equality prefix covering every column.
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
            ExprValidateErr::ColNotPayload { col } => {
                write!(
                    f,
                    "column {col} is part of the primary key; this operator needs a payload column"
                )
            }
            // Already a sentence naming the format and the fault; `Debug` would
            // quote and escape it inside `CorruptBlob("…")`.
            ExprValidateErr::CorruptBlob(msg) | ExprValidateErr::BadWalk(msg) => write!(f, "{msg}"),
            other => write!(f, "{other:?}"),
        }
    }
}

/// What a `ColKindMismatch` asks for in place of a column no scalar register loads.
const SCALAR_COL: &str = "a fixed-width integer or floating-point column";

/// A column-type predicate paired with the phrase a `ColKindMismatch` renders
/// for it — the two halves of one requirement, so neither can be widened without
/// the other.
type ColTypeTest = (fn(TypeCode) -> bool, &'static str);

/// What an opcode's kernel requires of a column operand. A `Col` name admits a
/// PK column; a `Payload` name does not — the kernels behind those address a
/// column through a dense payload index, which a PK column has none of.
#[derive(Clone, Copy)]
enum ColKind {
    /// PK or payload, any type: the reader decodes no value.
    AnyCol,
    /// A column a scalar register loads: a fixed-width integer, PK or payload,
    /// or a float.
    ScalarCol,
    /// Payload only, the 16-byte German-string layout.
    StringPayload,
}

impl ColKind {
    /// The type predicate this kind imposes, with the phrase a `ColKindMismatch`
    /// renders for it; `None` for a kind that imposes none.
    fn type_test(self) -> Option<ColTypeTest> {
        match self {
            Self::AnyCol => None,
            Self::ScalarCol => Some((|t| ScalarKind::from_type_code(t).is_some(), SCALAR_COL)),
            Self::StringPayload => Some((TypeCode::is_german_string, "a string or blob column")),
        }
    }

    /// True iff a PK column is unusable here.
    fn payload_only(self) -> bool {
        matches!(self, Self::StringPayload)
    }
}

// ---------------------------------------------------------------------------
// The wire instruction vocabulary
// ---------------------------------------------------------------------------

gnitz_wire::wire_enum! {
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
    /// `encode_load_const` / `decode_load_const`.
    ///
    /// A wire enum so [`LogicalProgram::decode_instr`] matches exhaustively: a
    /// new opcode is a compile error there until it gets a decode arm — which
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

// ---------------------------------------------------------------------------
// Typed instruction operands
// ---------------------------------------------------------------------------

// Each of the six is an [`ExprOp`] **selector**: the wire instruction's word 1,
// naming which member of the opcode's family this instruction is. Wire enums, so
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
    pub fn converse(self) -> CmpOp {
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
    /// Integer arithmetic operator. Parallel to [`FloatArithOp`] rather than
    /// shared with it: `Mod` has no float form, and a shared enum admitting
    /// float-`Mod` would force an unreachable arm into `to_wire`.
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
    /// Which end(s) [`ExprOp::StrTrim`] strips — its selector. The one
    /// definition of that word both the planner and the engine encode against.
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

    /// A wire register word, saturating past `u16` to a register the file never
    /// holds rather than wrapping into one it does.
    fn from_wire(w: u32) -> Self {
        const { assert!(MAX_REGS < u16::MAX as usize) };
        Reg(u16::try_from(w).unwrap_or(u16::MAX))
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
    /// Round through f32 precision. NULL iff the source is finite and the
    /// rounded result is not; NaN and ±∞ pass through (both representable), as
    /// does underflow to ±0.
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
    /// Float register to text: the shortest round-trip decimal, switched to
    /// scientific notation outside `[1e-4, 1e15)` so the output stays bounded
    /// (Rust's positional `Display` renders `1e300` as 301 digits). The
    /// non-finite values spell `Infinity` / `-Infinity` / `NaN`, as PostgreSQL
    /// does.
    FloatToStr {
        a: Reg,
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

/// One resolved, evaluable instruction. `signed` flags carry the result of the
/// per-register U64 type tracking: `signed: false` selects the unsigned path on
/// `IntArith`'s `Div` and `Mod` and on `IntToFloat`, reinterpreting the i64
/// register as u64; `Cmp` carries the pair's [`IntOrder`].
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
    /// PK-region integer load: the addressed OPK column at byte `off`. `off`
    /// cannot come from `fi` — it is the column's offset within the OPK region,
    /// not its width.
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
        a: u16,
        signed: bool,
    },
    FloatUnary {
        op: FloatUnaryOp,
        a: u16,
    },
    /// `signed` is the resolve-time U64 tracking of `a`; only `Sign` reads it.
    IntUnary {
        op: IntUnaryOp,
        a: u16,
        signed: bool,
    },
    Calendar {
        op: CalendarOp,
        a: u16,
        micros: bool,
    },
    /// `fi` is the fixed-int target `decode_instr` narrowed the wire selector
    /// to, so the kernel's bounds lookup is total.
    FloatToInt {
        a: u16,
        fi: FixedInt,
    },
    IntCast {
        a: u16,
        fi: FixedInt,
        src_signed: bool,
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
    /// SQL CASE blend (resolved): identical to the logical form — blends raw i64
    /// bit patterns; the lowerer lifts integer branches to f64.
    Select {
        cond: u16,
        a: u16,
        b: u16,
    },
    LoadNull,
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
    /// Integer set membership: `value_reg ∈ ResolvedProgram::int_sets[set_idx]`.
    IntInSet {
        value_reg: u16,
        set_idx: u32,
    },
    /// A German-string column into a string register. `pi` is the payload slot,
    /// resolved from the logical column index, and is also the kernel's buffer
    /// index into [`ResolvedProgram::str_cols`]' table.
    LoadColStr {
        pi: u8,
    },
    LoadNullStr,
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
        a: u16,
        signed: bool,
    },
    FloatToStr {
        a: u16,
    },
    /// `fi` is the fixed-int target `decode_instr` narrowed the wire selector
    /// to, so the kernel's range lookup is total.
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
    /// what lets [`EmitWidth::for_slot`] admit a slot narrower than the register.
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

    /// Serialise to the wire instruction `[op, selector, a1, a2, a3]`, the exact
    /// inverse of `LogicalProgram::decode_instr` — the two are the only
    /// statements of the word layout, held together by the round-trip test.
    /// Unused words are 0, matching what the decoder ignores.
    pub(crate) fn to_wire(self) -> [u32; INSTR_WORDS] {
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
            L::FloatToStr { a } => un(ExprOp::FloatToStr, 0, a),
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
}

impl Sink {
    /// Serialise to the wire pair `[kind, value]`, the inverse of
    /// [`LogicalProgram::decode_sink`].
    fn to_wire(self) -> [u32; SINK_WORDS] {
        match self {
            Sink::Col(src_col) => [SinkKind::Col.as_wire(), src_col],
            Sink::Reg(r) => [SinkKind::Reg.as_wire(), r.0 as u32],
        }
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
pub(crate) fn encode_expr_blob(
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
    pub(crate) sinks: Vec<Sink>,
    const_strings: Vec<Vec<u8>>,
    /// Bit `r` set iff register `r` holds a string rather than a scalar, as
    /// [`Self::from_instrs`] finished it.
    ///
    /// **Position-independent**, which is what lets every later pass read the
    /// finished mask instead of rebuilding one in step: a register's one writer
    /// is the instruction at its own index, ahead of every reader.
    pub(crate) str_class: u64,
}

impl LogicalProgram {
    /// [`Self::from_instrs`], panicking on a structural failure.
    pub fn new(instrs: Vec<LogicalInstr>, sinks: Vec<Sink>, const_strings: Vec<Vec<u8>>) -> Self {
        Self::from_instrs(instrs, sinks, const_strings)
            .unwrap_or_else(|e| panic!("compiler-built LogicalProgram is invalid: {e:?}"))
    }

    /// Assemble from typed instructions, holding every one to the rules a schema
    /// is not needed for. Every constructor routes through here and the type is
    /// immutable, so the walk below establishes the register-valid invariant
    /// `regs_split`'s raw split borrows depend on, and [`Self::str_class`], for
    /// every `LogicalProgram`.
    pub(crate) fn from_instrs(
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
        Ok(LogicalProgram { instrs, sinks, const_strings, str_class })
    }

    /// A pure projection: `copies[i] = src_col` copies logical input column
    /// `src_col` into output payload slot `i`. The source type is derived in
    /// `map_sinks` from the schema. The instruction-free shape a map consumer
    /// turns into verbatim column moves and nothing else.
    pub fn copy_cols(copies: &[u32]) -> Self {
        let sinks = copies.iter().map(|&src_col| Sink::Col(src_col)).collect();
        LogicalProgram::new(Vec::new(), sinks, Vec::new())
    }

    /// The typed instructions, in emission order — instruction `i` writing
    /// register `i`.
    pub fn instrs(&self) -> &[LogicalInstr] {
        &self.instrs
    }

    /// The const pool the instructions index into.
    pub fn const_strings(&self) -> &[Vec<u8>] {
        &self.const_strings
    }

    /// Serialise to the wire blob — the inverse of [`Self::from_blob`], and the
    /// one encode.
    pub fn to_blob_bytes(&self) -> Vec<u8> {
        encode_expr_blob(
            self.instrs.iter().copied().map(LogicalInstr::to_wire),
            self.sinks.iter().copied().map(Sink::to_wire),
            &self.const_strings,
        )
    }

    /// A program blob: the header walked, then the regions lowered into the
    /// typed logical form — the inverse of [`encode_expr_blob`].
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
            .map(Self::decode_instr)
            .collect::<Result<Vec<_>, _>>()?;
        let sinks = sinks
            .as_chunks::<SINK_BYTES>()
            .0
            .iter()
            .map(Self::decode_sink)
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_instrs(instrs, sinks, const_strings)
    }

    /// Decode one sink pair `[kind, value]` — the inverse of [`Sink::to_wire`].
    /// The sink region is its own space, so a bad kind is its own error rather
    /// than an [`ExprOp`] that does not exist.
    fn decode_sink(p: &[u8; SINK_BYTES]) -> Result<Sink, ExprValidateErr> {
        let (kind, value) = (gnitz_wire::read_u32_le(p, 0), gnitz_wire::read_u32_le(p, 4));
        match SinkKind::from_wire(kind).ok_or(ExprValidateErr::BadSinkKind(kind))? {
            SinkKind::Col => Ok(Sink::Col(value)),
            SinkKind::Reg => Ok(Sink::Reg(Reg::from_wire(value))),
        }
    }

    /// Decode one wire instruction, the exact inverse of
    /// [`LogicalInstr::to_wire`]. Client-controlled words, so a bad one is
    /// rejected rather than panicked.
    ///
    /// The match over [`ExprOp`] is exhaustive with **no `_` arm**, and every
    /// opcode narrows its selector — through its family's `from_wire`, or
    /// through `no_sel`. Together that makes the accepted `(op, selector)` set
    /// computable from the enums instead of from a hand-synced table.
    ///
    /// Not a bijection: unused operand words are ignored.
    pub(crate) fn decode_instr(t: &[u8; INSTR_BYTES]) -> Result<LogicalInstr, ExprValidateErr> {
        use LogicalInstr as L;
        let w = |i: usize| gnitz_wire::read_u32_le(t, i * 4);
        let op = ExprOp::from_wire(w(0)).ok_or(ExprValidateErr::UnknownOpcode(w(0)))?;
        let (opw, sel) = (w(0), w(1));
        let (a, b, c) = (Reg::from_wire(w(2)), Reg::from_wire(w(3)), Reg::from_wire(w(4)));
        // One statement of the one thing a selector can be wrong about: it names
        // no member of this opcode's family.
        let bad_sel = || ExprValidateErr::BadSelector { op: opw, selector: sel };
        // An opcode that names no family carries selector 0 and nothing else.
        let no_sel = |i: L| if sel == 0 { Ok(i) } else { Err(bad_sel()) };
        // Each closure captures this instruction's operands so the per-opcode
        // arms below stay one-liners.
        let cmp_op = || CmpOp::from_wire(sel).ok_or_else(bad_sel);
        Ok(match op {
            ExprOp::LoadCol => no_sel(L::LoadCol { col: w(2) })?,
            ExprOp::LoadConst => L::LoadConst {
                val: decode_load_const(w(2), w(3)),
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
                micros: flag(opw, w(3))?,
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
            ExprOp::IsNull => L::IsNull { col: w(2), invert: flag(opw, sel)? },
            ExprOp::IsNullReg => L::IsNullReg { a, invert: flag(opw, sel)? },
            ExprOp::StrColConst => L::StrColConst {
                op: cmp_op()?,
                col: w(2),
                const_idx: ConstIdx(w(3)),
            },
            ExprOp::StrColCol => L::StrColCol { op: cmp_op()?, col_a: w(2), col_b: w(3) },
            ExprOp::IntInSet => no_sel(L::IntInSet { value_reg: a, set_idx: ConstIdx(w(3)) })?,
            ExprOp::LoadColStr => no_sel(L::LoadColStr { col: w(2) })?,
            ExprOp::LoadConstStr => no_sel(L::LoadConstStr { const_idx: ConstIdx(w(2)) })?,
            ExprOp::LoadNullStr => no_sel(L::LoadNullStr)?,
            ExprOp::StrSelect => no_sel(L::StrSelect { cond: a, a: b, b: c })?,
            ExprOp::StrCmp => L::StrCmp { op: cmp_op()?, a, b },
            ExprOp::StrLen => L::StrLen { a, chars: flag(opw, sel)? },
            ExprOp::StrCase => L::StrCase { a, upper: flag(opw, sel)? },
            ExprOp::StrSubstr => no_sel(L::StrSubstr {
                src: a,
                start_reg: b,
                len_reg: (w(4) != u32::MAX).then_some(c),
            })?,
            ExprOp::StrTrim => L::StrTrim {
                a,
                mode: TrimMode::from_wire(sel).ok_or_else(bad_sel)?,
                set_idx: ConstIdx(w(3)),
            },
            // `ci` lives in the selector, so nothing downstream re-derives it.
            ExprOp::StrLike => L::StrLike {
                src: a,
                pat_idx: ConstIdx(w(3)),
                ci: flag(opw, sel)?,
            },
            ExprOp::StrConcat => L::StrConcat { a, b, skip_null: flag(opw, sel)? },
            ExprOp::IntToStr => no_sel(L::IntToStr { a })?,
            ExprOp::FloatToStr => no_sel(L::FloatToStr { a })?,
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

    /// Whether this program, run as a map from `in_schema` to `out_schema` with the input PK carried
    /// through, reproduces its input: one layout on both sides, and the program copies each payload
    /// column into its own slot and computes nothing.
    pub(crate) fn is_identity_map(&self, in_schema: &dyn SchemaFacts, out_schema: &dyn SchemaFacts) -> bool {
        in_schema.same_layout(out_schema)
            && self.instrs.is_empty()
            && self
                .sinks
                .iter()
                .copied()
                .eq((0..in_schema.num_payload_cols()).map(|pi| Sink::Col(in_schema.payload_col_idx(pi) as u32)))
    }

    /// Check the program against `schema` and lower it to the resolved form, with
    /// `sink_read` how the consumer reads each sink's register.
    pub(crate) fn resolve_program(
        &self,
        schema: &dyn SchemaFacts,
        sink_read: ReadAs,
    ) -> Result<ResolvedProgram, ExprValidateErr> {
        use Instr as I;
        use LogicalInstr as L;

        let ProgramFacts { bit_only, bool_pack, no_nulls, reg_u64 } = self.analyze(schema, sink_read)?;
        let str_class = self.str_class;

        let payload_slot = |col: u32| match schema.payload_slot(col as usize) {
            Some(slot) => Ok(slot as u8),
            None => Err(ExprValidateErr::ColNotPayload { col }),
        };
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
        // Bit `pi` per string column some `LoadColStr` loads; `with_str_bufs` resolves
        // one column region per set bit.
        let mut str_cols: u64 = 0;
        // Answered once per register by `analyze`, off each opcode's `U64Rule`,
        // so no arm below restates the rule.
        let is_u64 = |r: u16| (reg_u64 >> r) & 1 != 0;
        let int_reg = |r: Reg| IntReg { reg: r.0, signed: !is_u64(r.0) };
        let nullable_slots = schema.nullable_payload_slots();
        // A register file is exactly one entry per instruction.
        let num_regs = self.instrs.len() as u32;
        for (i, li) in self.instrs.iter().copied().enumerate() {
            // A register is the index of the instruction that writes it.
            let dst = i as u16;
            // One logical instruction, one resolved instruction — bar the
            // constants (the two loads, and a null test over a column that
            // cannot be NULL), which leave the stream for their own tables.
            let resolved = match li {
                L::LoadCol { col } => {
                    let loc = locate_col(schema, col, ColKind::ScalarCol)?;
                    match (ScalarKind::from_type_code(loc.type_code()), loc) {
                        (Some(ScalarKind::Int(fi)), ColumnLocator::Pk { byte_off, .. }) => {
                            I::LoadPk { off: byte_off, fi }
                        }
                        (Some(ScalarKind::Int(fi)), ColumnLocator::Payload { slot, .. }) => {
                            I::LoadPayloadInt { pi: slot, fi }
                        }
                        // An F64 column's bytes are already the register's image.
                        (Some(ScalarKind::F64), ColumnLocator::Payload { slot, .. }) => {
                            I::LoadPayloadInt { pi: slot, fi: FixedInt::I64 }
                        }
                        (Some(ScalarKind::F32), ColumnLocator::Payload { slot, .. }) => I::LoadPayloadF32 { pi: slot },
                        // No kernel reads a float out of the PK region.
                        (Some(ScalarKind::F32 | ScalarKind::F64), ColumnLocator::Pk { .. }) => {
                            return Err(ExprValidateErr::ColNotPayload { col })
                        }
                        (None, _) => {
                            return Err(ExprValidateErr::ColKindMismatch {
                                col,
                                type_code: loc.type_code(),
                                want: SCALAR_COL,
                            })
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
                L::IntUnary { op, a: Reg(a) } => I::IntUnary { op, a, signed: !is_u64(a) },
                L::Calendar { op, a: Reg(a), micros } => I::Calendar { op, a, micros },
                L::FloatToF32 { a: Reg(a) } => I::FloatToF32 { a },
                L::FloatToInt { a: Reg(a), fi } => I::FloatToInt { a, fi },
                L::IntCast { a: Reg(a), fi } => I::IntCast { a, fi, src_signed: !is_u64(a) },
                L::IntMinMax2 { a: Reg(a), b: Reg(b), is_max } => I::IntMinMax2 { a, b, is_max, signed: !is_u64(dst) },
                L::FloatMinMax2 { a: Reg(a), b: Reg(b), is_max } => I::FloatMinMax2 { a, b, is_max },
                L::IntToFloat { a: Reg(a) } => I::IntToFloat { a, signed: !is_u64(a) },
                L::Select { cond: Reg(cond), a: Reg(a), b: Reg(b) } => I::Select { cond, a, b },
                L::LoadNull => I::LoadNull,
                L::BoolBinary { a: Reg(a), b: Reg(b), is_or } => I::BoolBinary { a, b, is_or },
                L::BoolNot { a: Reg(a) } => I::BoolNot { a },
                // A column that cannot be NULL — a PK column, or a `NOT NULL`
                // payload one — is a constant.
                L::IsNull { col, invert } => match locate_col(schema, col, ColKind::AnyCol)? {
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
                    I::StrColConst { op, pi: payload_slot(col)?, const_idx }
                }
                L::StrColCol { op, col_a, col_b } => I::StrColCol {
                    op,
                    pi_a: payload_slot(col_a)?,
                    pi_b: payload_slot(col_b)?,
                },
                L::IntInSet {
                    value_reg: Reg(value_reg),
                    set_idx: ConstIdx(set_idx),
                } => {
                    let si = set_idx as usize;
                    int_sets[si].get_or_insert_with(|| decode_int_set(&self.const_strings[si]));
                    I::IntInSet { value_reg, set_idx }
                }
                L::LoadColStr { col } => {
                    let pi = payload_slot(col)?;
                    str_cols |= 1u64 << pi;
                    I::LoadColStr { pi }
                }
                L::LoadConstStr { const_idx: ConstIdx(const_idx) } => {
                    let ci = const_idx as usize;
                    let (off, len) = *const_spans[ci].get_or_insert_with(|| {
                        let bytes = &self.const_strings[ci];
                        let span = (const_arena.len() as u32, bytes.len() as u32);
                        const_arena.extend_from_slice(bytes);
                        span
                    });
                    const_str_regs.push((dst, off, len));
                    continue;
                }
                L::LoadNullStr => I::LoadNullStr,
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
                L::IntToStr { a: Reg(a) } => I::IntToStr { a, signed: !is_u64(a) },
                L::FloatToStr { a: Reg(a) } => I::FloatToStr { a },
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
        Ok(ResolvedProgram {
            no_nulls,
            nullable_slots,
            bit_only_mask: bit_only,
            bool_pack_mask: bool_pack,
            instrs,
            const_regs,
            const_str_regs,
            const_cells: dense(const_cells),
            int_sets: dense(int_sets),
            trim_sets: dense(trim_sets),
            like_matchers,
            const_arena,
            str_lanes: MAX_REGS as u32 - str_class.leading_zeros(),
            scalar_lanes: (0..num_regs)
                .rev()
                .find(|&r| (str_class >> r) & 1 == 0)
                .map_or(0, |r| r + 1),
            str_cols,
            reg_u64,
        })
    }

    /// Hold every column operand to `schema` and derive [`ProgramFacts`], in one
    /// pass over the operand table. A sink is one more read of its register, as
    /// `sink_read`.
    fn analyze(&self, schema: &dyn SchemaFacts, sink_read: ReadAs) -> Result<ProgramFacts, ExprValidateErr> {
        let (mut bool_produced, mut non_bool_read, mut bool_input) = (0u64, 0u64, 0u64);
        let mut read_as = |reg: Reg, read: ReadAs| match read {
            ReadAs::Bool => bool_input |= 1u64 << reg.0,
            // A null-lane read needs neither the value nor the truth bit.
            ReadAs::NullBit => {}
            ReadAs::Value | ReadAs::Str => non_bool_read |= 1u64 << reg.0,
        };
        let nullable_slots = schema.nullable_payload_slots();
        let mut no_nulls = true;
        let mut reg_u64 = 0u64;
        for (i, li) in self.instrs.iter().enumerate() {
            let ops = operands(li);
            let mut col_is_u64 = false;
            for &(col, kind) in ops.cols.iter().flatten() {
                let loc = locate_col(schema, col, kind)?;
                col_is_u64 |= loc.type_code() == TypeCode::U64;
                // A kind names a type exactly when its kernel decodes the column
                // into a register, which is when the null bit flows.
                no_nulls &= kind.type_test().is_none() || !loc.is_null_word(nullable_slots);
            }
            match ops.write {
                WriteAs::Bool => bool_produced |= 1u64 << i,
                // In stream order: `reg_u64` holds every register this one reads.
                WriteAs::Value(rule) => reg_u64 |= (u64_verdict(rule, &ops, col_is_u64, reg_u64) as u64) << i,
                WriteAs::Str => {}
            }
            no_nulls &= !ops.makes_null;
            for &(reg, read) in ops.reads.iter().flatten() {
                read_as(reg, read);
            }
        }
        for reg in self.sinks.iter().filter_map(|s| s.reg()) {
            read_as(reg, sink_read);
        }
        Ok(ProgramFacts {
            bit_only: bool_produced & !non_bool_read,
            // `bool_input` alone: a register in `bit_only \ bool_input` has
            // `IsNullReg` as its only possible reader, which reads `null_bits`
            // and never the packed bit.
            bool_pack: bool_input,
            no_nulls,
            reg_u64,
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
    check_const_idx(idx.0, const_strings.len())?;
    let bytes = &const_strings[idx.0 as usize];
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
pub(crate) enum ReadAs {
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
    /// signed target); for a cast, whether its target names U64; for a
    /// constant, its own flag.
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
/// — `from_instrs` and `analyze` — so a new opcode is classified
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
/// so [`LogicalProgram::from_instrs`] holds each named entry to its kind before a
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

    fn reading_opt(self, reg: Option<Reg>, read: ReadAs) -> Self {
        match reg {
            Some(r) => self.reading(r, read),
            None => self,
        }
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
            .on_col(col, ColKind::StringPayload)
            .with_pool(const_idx, PoolEntry::Bytes),
        L::StrColCol { op: _, col_a, col_b } => writes(WBool)
            .on_col(col_a, ColKind::StringPayload)
            .on_col(col_b, ColKind::StringPayload),
        // The string-register column load reads the same 16-byte cells the
        // `ExprOp::StrCol*` compares do, so it carries the same requirement.
        L::LoadColStr { col } => writes(WStr).on_col(col, ColKind::StringPayload),

        // --- String registers ---
        L::LoadConstStr { const_idx } => writes(WStr).with_pool(const_idx, PoolEntry::Text),
        // A NULL on every row.
        L::LoadNullStr => writes(WStr).may_null(),
        L::IntToStr { a } | L::FloatToStr { a } => writes(WStr).reading(a, RVal),
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

/// Column operand `col` located in `s`, held to what `need`'s kernel can read:
/// in range, a payload column where it must be one, and of a type it decodes.
fn locate_col(s: &dyn SchemaFacts, col: u32, need: ColKind) -> Result<ColumnLocator, ExprValidateErr> {
    let Some(loc) = s.try_locate(col as usize) else {
        return Err(ExprValidateErr::ColOutOfRange { col, num_columns: s.num_columns() });
    };
    if need.payload_only() && matches!(loc, ColumnLocator::Pk { .. }) {
        return Err(ExprValidateErr::ColNotPayload { col });
    }
    let type_code = loc.type_code();
    match need.type_test() {
        Some((accepts, want)) if !accepts(type_code) => Err(ExprValidateErr::ColKindMismatch { col, type_code, want }),
        _ => Ok(loc),
    }
}

/// One slot per entry, the entries nothing built left at their default.
fn dense<T: Default>(slots: Vec<Option<T>>) -> Vec<T> {
    slots.into_iter().map(Option::unwrap_or_default).collect()
}

/// The `ExprOp::IntInSet` const-pool form: `N × 8-byte LE` i64s, strictly
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
    let mut v = Vec::new();
    gnitz_wire::extend_from_le_bytes(&mut v, bytes);
    v
}

// ---------------------------------------------------------------------------
// NullPerm — columnar null bitmap permutation
// ---------------------------------------------------------------------------

/// How column copies derive each output row's null word from its input row's:
/// one move per distance a copied nullable column's bit travels.
pub struct NullPerm {
    /// The nearest move — for most copy lists the only one, held inline so a
    /// per-row [`Self::apply`] reads no heap. Its mask is zero when no nullable
    /// column is copied.
    first: NullMove,
    rest: Vec<NullMove>,
}

/// The source bits that all travel one distance: `up` slots toward the high end
/// or `down` toward the low end, one of the two being zero.
#[derive(Clone, Copy)]
struct NullMove {
    mask: u64,
    up: u8,
    down: u8,
}

impl NullMove {
    #[inline(always)]
    fn apply(self, word: u64) -> u64 {
        ((word & self.mask) << self.up) >> self.down
    }
}

impl NullPerm {
    /// Build from the column moves; `nullable` is the input's payload slots that
    /// admit NULL.
    pub fn new(copies: &[ColCopy], nullable: u64) -> Self {
        let mut moves: Vec<NullMove> = Vec::new();
        for c in copies {
            let ColumnLocator::Payload { slot: src, .. } = c.src else {
                continue;
            };
            if !gnitz_wire::null_word_get(nullable, src as usize) {
                continue;
            }
            let dst = c.slot as u8;
            let (up, down) = (dst.saturating_sub(src), src.saturating_sub(dst));
            match moves.iter_mut().find(|m| (m.up, m.down) == (up, down)) {
                Some(m) => m.mask |= 1u64 << src,
                None => moves.push(NullMove { mask: 1u64 << src, up, down }),
            }
        }
        // Nearest first: bits that stay put take the first pass, which then
        // needs no shift.
        moves.sort_by_key(|m| m.up.max(m.down));
        let mut moves = moves.into_iter();
        let first = moves.next().unwrap_or(NullMove { mask: 0, up: 0, down: 0 });
        NullPerm { first, rest: moves.collect() }
    }

    /// One input row's null word as its output row's.
    #[inline(always)]
    pub fn apply(&self, word: u64) -> u64 {
        self.rest
            .iter()
            .fold(self.first.apply(word), |bits, m| bits | m.apply(word))
    }

    /// Derive the null words of `out` rows `[dst_base, dst_base + n)` from
    /// source rows `[src_start, src_start + n)`, one u64 per row. The whole
    /// window is written: the destination may be an uninitialized tail.
    pub(crate) fn write_rows(&self, in_null_bmp: &[u8], src_start: usize, out: &mut [u8], dst_base: usize, n: usize) {
        let dst = &mut out[dst_base * 8..(dst_base + n) * 8];
        let src = in_null_bmp[src_start * 8..(src_start + n) * 8].as_chunks::<8>().0;
        let word = |w: &[u8; 8]| u64::from_le_bytes(*w);
        let (first, rest) = (self.first, &self.rest[..]);
        if first.mask == 0 {
            return dst.fill(0);
        }
        // A move at a time, each pass one uniform shift; several moves go block by
        // block so the words they revisit stay in L1.
        let block = if rest.is_empty() { n.max(1) } else { 256 };
        for (d, s) in dst.as_chunks_mut::<8>().0.chunks_mut(block).zip(src.chunks(block)) {
            if first.up == 0 && first.down == 0 {
                for (d, s) in d.iter_mut().zip(s) {
                    *d = (word(s) & first.mask).to_le_bytes();
                }
            } else {
                for (d, s) in d.iter_mut().zip(s) {
                    *d = first.apply(word(s)).to_le_bytes();
                }
            }
            for &mv in rest {
                for (d, s) in d.iter_mut().zip(s) {
                    *d = (word(d) | mv.apply(word(s))).to_le_bytes();
                }
            }
        }
    }
}

/// A column copied verbatim into a map's output.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ColCopy {
    pub src: ColumnLocator,
    /// The output payload slot.
    pub slot: usize,
    /// The output slot's width: wider than the source only for a promoted integer.
    pub width: usize,
}

/// A computed output slot written from a scalar register.
#[derive(Debug, PartialEq)]
pub(crate) struct ScalarEmit {
    pub(crate) reg: usize,
    pub(crate) slot: usize,
    pub(crate) width: EmitWidth,
}

/// The byte width of a scalar emit's output slot.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum EmitWidth {
    W1,
    W2,
    W4,
    W8,
}

impl EmitWidth {
    /// The width a scalar register stores into a slot of `type_code`: its whole
    /// 8-byte image, or the low bytes for a fixed-int slot whose source
    /// range-checked the value to exactly that width.
    fn for_slot(type_code: TypeCode, src: &LogicalInstr) -> Option<Self> {
        match type_code.wire_stride() {
            8 => Some(EmitWidth::W8),
            w if type_code.is_fixed_int() && src.range_check().map(FixedInt::width) == Some(w) => match w {
                1 => Some(EmitWidth::W1),
                2 => Some(EmitWidth::W2),
                4 => Some(EmitWidth::W4),
                _ => None,
            },
            _ => None,
        }
    }

    pub(crate) fn bytes(self) -> usize {
        match self {
            EmitWidth::W1 => 1,
            EmitWidth::W2 => 2,
            EmitWidth::W4 => 4,
            EmitWidth::W8 => 8,
        }
    }
}

/// A computed output slot written from a string register.
#[derive(Debug, PartialEq)]
pub(crate) struct StrEmit {
    pub(crate) reg: usize,
    pub(crate) slot: usize,
}

/// Where each of a map's output slots comes from.
pub(crate) struct MapSinks {
    pub(crate) copies: Vec<ColCopy>,
    /// How the copies move the input's null bits.
    pub(crate) null_perm: NullPerm,
    pub(crate) scalar_emits: Vec<ScalarEmit>,
    pub(crate) str_emits: Vec<StrEmit>,
}

impl LogicalProgram {
    /// Hold this map's sinks to its two schemas and say where each output slot
    /// comes from. Sink `i` writes slot `i`, so covering every declared payload
    /// slot is a count.
    pub(crate) fn map_sinks(
        &self,
        in_schema: &dyn SchemaFacts,
        out_schema: &dyn SchemaFacts,
    ) -> Result<MapSinks, ExprValidateErr> {
        use ExprValidateErr as E;
        let out_slots = out_schema.payload_locators();
        if self.sinks.len() != out_slots.len() {
            return Err(E::OutputSlotCountMismatch {
                sinks: self.sinks.len(),
                num_payload_cols: out_slots.len(),
            });
        }
        let (mut copies, mut scalar_emits, mut str_emits) = (Vec::new(), Vec::new(), Vec::new());
        for (slot, (sink, out_loc)) in self.sinks.iter().zip(out_slots).enumerate() {
            let (out, type_code) = (slot as u32, out_loc.type_code());
            match *sink {
                Sink::Col(col) => {
                    let src = locate_col(in_schema, col, ColKind::AnyCol)?;
                    let src_tc = src.type_code();
                    // Verbatim, or a narrower integer widened.
                    if src_tc != type_code && !src_tc.is_widening_promotion(type_code) {
                        return Err(E::CopyTypeMismatch { col, src_tc, out, out_tc: type_code });
                    }
                    copies.push(ColCopy { src, slot, width: out_loc.size() });
                }
                Sink::Reg(r) if self.is_str(r) != type_code.is_german_string() => {
                    return Err(E::EmitClassMismatch { out, type_code })
                }
                Sink::Reg(r) if self.is_str(r) => str_emits.push(StrEmit { reg: r.0 as usize, slot }),
                Sink::Reg(r) => {
                    let width = EmitWidth::for_slot(type_code, &self.instrs[r.0 as usize])
                        .ok_or(E::EmitSlotWidth { out, type_code })?;
                    scalar_emits.push(ScalarEmit { reg: r.0 as usize, slot, width });
                }
            }
        }
        let null_perm = NullPerm::new(&copies, in_schema.nullable_payload_slots());
        Ok(MapSinks {
            copies,
            null_perm,
            scalar_emits,
            str_emits,
        })
    }
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
    /// `LoadConstStr` bakes in, and the heap half of each long `const_cells`
    /// entry. Holds only the constants some opcode names, so an unreferenced
    /// pool entry is not carried for the plan's life. Also the string arena's
    /// non-cleared prefix: the per-morsel reset truncates back to
    /// `const_arena.len()`, so a `LoadConstStr` span stays valid for the
    /// evaluator's life.
    pub(crate) const_arena: Vec<u8>,
    /// Bit `pi` set iff some `LoadColStr` holds views into payload slot `pi`'s
    /// string column. A drive resolves one column region per set bit, into the
    /// slot the bit names — so a string register's `src` is a payload slot, not
    /// an allocation order, and no column can be left without one.
    pub(crate) str_cols: u64,
    /// One past the highest string register; 0 for a program with none.
    pub(crate) str_lanes: u32,
    /// One past the highest scalar register. A string register has no i64 lane.
    pub(crate) scalar_lanes: u32,
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
    /// Bit `r` set iff register `r` is only consumed by boolean ops, so nothing
    /// reads `regs[r]`. A permission, not an obligation: `BoolBinary`, `BoolNot`
    /// and `IsNullReg` skip the lane and write `bool_bits` natively.
    bit_only_mask: u64,
    /// Bit `r` set iff `r`'s producer must write `bool_bits[r]`: some downstream
    /// consumer reads it as a truth bit. A filter's result register is covered
    /// because `analyze` forces it into that set, whatever opcode writes it.
    bool_pack_mask: u64,
    /// Bit `r` set iff register `r`'s i64 image is to be read as a `u64`.
    pub(crate) reg_u64: u64,
}

impl ResolvedProgram {
    /// One past the highest register of either class.
    pub(crate) fn num_regs(&self) -> usize {
        self.str_lanes.max(self.scalar_lanes) as usize
    }

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

/// What [`analyze`] derives in its one pass over the instruction stream.
struct ProgramFacts {
    /// Bit `r` set iff `r` is only consumed by boolean ops, so nothing reads
    /// `regs[r]`. [`ResolvedProgram::bit_only_mask`] states what it permits.
    bit_only: u64,
    /// Bit `r` set iff `r`'s producer must write `bool_bits[r]`: some downstream
    /// consumer reads it as a truth bit, a filter's result register included.
    bool_pack: u64,
    /// True iff no instruction can produce a NULL against the schema, so the
    /// evaluator skips null-bit tracking entirely.
    no_nulls: bool,
    /// Bit `r` set iff register `r`'s i64 image is to be read as a `u64`, per
    /// each opcode's [`U64Rule`]. Final rather than running, and safe to read at
    /// any point for the reason [`LogicalProgram::str_class`] gives.
    reg_u64: u64,
}

/// Whether one opcode's `Value` destination holds a `u64`, per its [`U64Rule`].
/// `col_is_u64` is whether a column operand is U64; `so_far` is the mask over
/// the registers already written, which is every register this opcode can read.
fn u64_verdict(rule: U64Rule, ops: &Operands, col_is_u64: bool, so_far: u64) -> bool {
    match rule {
        U64Rule::Fixed(v) => v,
        U64Rule::FromOperands => ops
            .reads
            .iter()
            .flatten()
            .any(|&(reg, read)| read == ReadAs::Value && (so_far >> reg.0) & 1 != 0),
        U64Rule::FromColType => col_is_u64,
    }
}

#[cfg(test)]
#[path = "tests/program.rs"]
mod tests;
