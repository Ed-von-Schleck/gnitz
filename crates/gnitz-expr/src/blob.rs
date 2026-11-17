//! The expression blob: the wire layout `LogicalInstr` declares through
//! [`expr_instrs!`], which derives `ExprOp` and both instruction tables from it;
//! the wire id [`SinkKind`]; and the framing. Only this module reads the wire
//! form.

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
pub(crate) const INSTR_WORDS: usize = 5;
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

/// The instruction words a field can sit at, as `expr_instrs!` names them.
pub(crate) const SEL: usize = 1;
pub(crate) const A1: usize = 2;
pub(crate) const A2: usize = 3;
pub(crate) const A3: usize = 4;

/// Declares `LogicalInstr` with each variant's opcode and each field's word,
/// `Variant = opcode { field @ WORD: Type, .. }`, and derives from that one
/// table `ExprOp`, `LogicalInstr::to_wire` and `LogicalInstr::from_wire`.
///
/// The decoder ignores unused operand words, so it is not a bijection, but it
/// refuses a non-zero selector on a variant with no field at [`SEL`]. A field
/// word outside its type's domain is a `BadSelector` naming that word.
macro_rules! expr_instrs {
    (
        $(#[$m:meta])*
        pub enum LogicalInstr {
            $($(#[$vm:meta])* $V:ident = $code:literal $({ $($f:ident @ $p:ident : $t:ty),* $(,)? })?),* $(,)?
        }
    ) => {
        $(#[$m])*
        pub enum LogicalInstr {
            $($(#[$vm])* $V $({ $($f: $t),* })?),*
        }

        ::gnitz_wire::wire_enum! {
            /// Each `LogicalInstr` variant's opcode: wire identity only, every
            /// rule about what an instruction means living on the variant.
            ///
            /// An instruction is `[opcode, selector, a1, a2, a3]`. The selector
            /// holds the variant's operator, cast target, TRIM mode or 0/1 flag,
            /// and is **0** for a variant with none, which the decoder enforces.
            /// Every other field is one whole operand word, except an `i64`,
            /// whose low half rides its word and high half the next; Calendar's
            /// `micros` rides a2 because the selector holds its `CalendarOp`.
            /// Unused words are 0 and ignored.
            pub(crate) enum ExprOp: u32 {
                $($V = $code),*
            }
        }

        impl LogicalInstr {
            /// Serialise to the wire instruction `[op, selector, a1, a2, a3]`,
            /// unused words 0.
            pub(crate) fn to_wire(self) -> [u32; $crate::blob::INSTR_WORDS] {
                use $crate::blob::Field;
                let mut w = [0; $crate::blob::INSTR_WORDS];
                match self {
                    $(Self::$V $({ $($f),* })? => {
                        w[0] = ExprOp::$V.as_wire();
                        $($(Field::put($f, &mut w, $crate::blob::$p);)*)?
                    })*
                }
                w
            }

            /// Decode one wire instruction, the inverse of [`Self::to_wire`] up
            /// to ignored words. Client-controlled words, so a bad one is
            /// rejected rather than panicked.
            pub(crate) fn from_wire(w: [u32; $crate::blob::INSTR_WORDS]) -> Result<Self, $crate::ExprValidateErr> {
                use $crate::blob::{Field, SEL};
                let bad = |at: usize| $crate::ExprValidateErr::BadSelector { op: w[0], selector: w[at] };
                let op = ExprOp::from_wire(w[0]).ok_or($crate::ExprValidateErr::UnknownOpcode(w[0]))?;
                Ok(match op {
                    $(ExprOp::$V => {
                        if w[SEL] != 0 && !<[usize]>::contains(&[$($($crate::blob::$p),*)?], &SEL) {
                            return Err(bad(SEL));
                        }
                        Self::$V $({ $($f: <$t as Field>::get(&w, $crate::blob::$p).ok_or_else(|| bad($crate::blob::$p))?),* })?
                    })*
                })
            }

            /// The register operands among the fields, in declaration order.
            #[cfg(test)]
            pub(crate) fn regs(&self) -> Vec<Reg> {
                use $crate::blob::Field;
                match *self {
                    $(Self::$V $({ $($f),* })? => {
                        #[allow(unused_mut)]
                        let mut regs = Vec::new();
                        $($(regs.extend(Field::reg(&$f));)*)?
                        regs
                    })*
                }
            }
        }
    };
}
pub(crate) use expr_instrs;

/// One `LogicalInstr` field's place in the instruction words: `put` writes it
/// at word `at`, and `get` reads it back, `None` for words outside its domain.
pub(crate) trait Field: Sized {
    fn put(self, w: &mut [u32; INSTR_WORDS], at: usize);
    fn get(w: &[u32; INSTR_WORDS], at: usize) -> Option<Self>;

    /// The register this field names, if it is a register operand.
    #[cfg(test)]
    fn reg(&self) -> Option<Reg> {
        None
    }
}

impl Field for Reg {
    fn put(self, w: &mut [u32; INSTR_WORDS], at: usize) {
        w[at] = self.0 as u32;
    }
    fn get(w: &[u32; INSTR_WORDS], at: usize) -> Option<Self> {
        Some(Reg::from_wire(w[at]))
    }
    #[cfg(test)]
    fn reg(&self) -> Option<Reg> {
        Some(*self)
    }
}

/// An optional register, absent as `u32::MAX`, which no register index can be.
impl Field for Option<Reg> {
    fn put(self, w: &mut [u32; INSTR_WORDS], at: usize) {
        w[at] = self.map_or(u32::MAX, |r| r.0 as u32);
    }
    fn get(w: &[u32; INSTR_WORDS], at: usize) -> Option<Self> {
        Some((w[at] != u32::MAX).then(|| Reg::from_wire(w[at])))
    }
    #[cfg(test)]
    fn reg(&self) -> Option<Reg> {
        *self
    }
}

impl Field for u32 {
    fn put(self, w: &mut [u32; INSTR_WORDS], at: usize) {
        w[at] = self;
    }
    fn get(w: &[u32; INSTR_WORDS], at: usize) -> Option<Self> {
        Some(w[at])
    }
}

impl Field for ConstIdx {
    fn put(self, w: &mut [u32; INSTR_WORDS], at: usize) {
        w[at] = self.0;
    }
    fn get(w: &[u32; INSTR_WORDS], at: usize) -> Option<Self> {
        Some(ConstIdx(w[at]))
    }
}

/// A byte-wide operand; a word past 255 is outside its domain.
impl Field for u8 {
    fn put(self, w: &mut [u32; INSTR_WORDS], at: usize) {
        w[at] = self.into();
    }
    fn get(w: &[u32; INSTR_WORDS], at: usize) -> Option<Self> {
        u8::try_from(w[at]).ok()
    }
}

/// A two-member family's word, 0 or 1.
impl Field for bool {
    fn put(self, w: &mut [u32; INSTR_WORDS], at: usize) {
        w[at] = self as u32;
    }
    fn get(w: &[u32; INSTR_WORDS], at: usize) -> Option<Self> {
        match w[at] {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }
}

/// The one field wider than a word: low 32 bits at `at`, high 32 at `at + 1`.
impl Field for i64 {
    fn put(self, w: &mut [u32; INSTR_WORDS], at: usize) {
        w[at] = self as u32;
        w[at + 1] = (self >> 32) as u32;
    }
    fn get(w: &[u32; INSTR_WORDS], at: usize) -> Option<Self> {
        Some(((w[at + 1] as i64) << 32) | (w[at] as i64 & 0xFFFF_FFFF))
    }
}

/// A cast's fixed-int target as its storage type code. A forged code must not
/// reach eval, where it would index a bounds table that has no arm for it, so
/// the full word is narrowed: a word past 255 cannot truncate into a valid code.
impl Field for FixedInt {
    fn put(self, w: &mut [u32; INSTR_WORDS], at: usize) {
        w[at] = self.type_code().as_wire() as u32;
    }
    fn get(w: &[u32; INSTR_WORDS], at: usize) -> Option<Self> {
        u8::try_from(w[at])
            .ok()
            .and_then(TypeCode::from_wire)
            // A temporal code names no cast target of its own: the encoder
            // spells the storage type, so a decoded word must be one too.
            .and_then(FixedInt::exact)
    }
}

/// A selector family, through its wire enum.
macro_rules! family_fields {
    ($($t:ty),*) => {$(
        impl Field for $t {
            fn put(self, w: &mut [u32; INSTR_WORDS], at: usize) {
                w[at] = self.as_wire();
            }
            fn get(w: &[u32; INSTR_WORDS], at: usize) -> Option<Self> {
                Self::from_wire(w[at])
            }
        }
    )*};
}
family_fields!(
    CmpOp,
    IntArithOp,
    FloatArithOp,
    FloatUnaryOp,
    IntUnaryOp,
    CalendarOp,
    TrimMode
);

gnitz_wire::wire_enum! {
    /// What one sink pair `[kind, value]` names. Sinks ride the blob's own
    /// region, so this space is disjoint from `ExprOp`'s.
    pub(crate) enum SinkKind: u32 {
        /// Copy input column `value` verbatim.
        Col = 0,
        /// Store register `value`.
        Reg = 1,
    }
}

impl Reg {
    /// A wire register word, saturating past `u16` to a register the file never
    /// holds rather than wrapping into one it does.
    fn from_wire(w: u32) -> Self {
        const { assert!(MAX_REGS < u16::MAX as usize) };
        Reg(u16::try_from(w).unwrap_or(u16::MAX))
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
    /// than an `ExprOp` that does not exist.
    fn from_wire([kind, value]: [u32; SINK_WORDS]) -> Result<Sink, ExprValidateErr> {
        match SinkKind::from_wire(kind).ok_or(ExprValidateErr::BadSinkKind(kind))? {
            SinkKind::Col => Ok(Sink::Col(value)),
            SinkKind::Reg => Ok(Sink::Reg(Reg::from_wire(value))),
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
