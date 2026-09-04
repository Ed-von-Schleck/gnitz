//! [`ExprBuilder`]: the emitter that turns a client-side expression into a
//! [`LogicalProgram`].
//!
//! It holds the three things a program is made of — the instruction list, the
//! const pool, and the output sinks — and hands them to
//! [`LogicalProgram::from_instrs`] in [`ExprBuilder::build`], its one exit. The
//! structural rules (register limit, operand order, const-pool bounds) are
//! decided there, so a caller emits first and is told what is unsupported once.

use crate::{ConstIdx, ExprValidateErr, LogicalInstr, LogicalProgram, Reg, Sink, MAX_REGS};

/// Accumulates the instructions, sinks and constants of one expression program.
#[derive(Default)]
pub struct ExprBuilder {
    instrs: Vec<LogicalInstr>,
    sinks: Vec<Sink>,
    const_strings: Vec<Vec<u8>>,
}

impl ExprBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// The register holding `instr`'s value: the one an identical instruction
    /// already writes, else a fresh one — its own index. Every instruction is a
    /// pure function of its operands, so identical ones share a register.
    pub fn emit(&mut self, instr: LogicalInstr) -> Reg {
        // A lift of a constant is another constant — algebraic and
        // schema-independent, so it folds here rather than at resolve. The
        // integer constant it reads stays: a register *is* its instruction's
        // index, so `emit` can only append or reuse, never delete.
        let instr = match instr {
            // `get`, not an index: an out-of-range operand is `build`'s to reject.
            LogicalInstr::IntToFloat { a } => match self.instrs.get(a.0 as usize) {
                Some(&LogicalInstr::LoadConst { val }) => LogicalInstr::LoadConst {
                    val: crate::batch::encode_f64(val as f64),
                },
                _ => instr,
            },
            _ => instr,
        };
        // The fold scan stops paying once the program is already past the
        // register cap — `build` rejects it whatever follows — so lowering a
        // huge expression stays linear instead of quadratic in its size.
        if self.instrs.len() <= MAX_REGS {
            if let Some(i) = self.instrs.iter().position(|e| *e == instr) {
                return Reg(i as u16);
            }
        }
        let reg = Reg(self.instrs.len() as u16);
        self.instrs.push(instr);
        reg
    }

    /// Append an output payload slot. Slots are filled in call order, so the
    /// n-th call writes output payload column n.
    pub fn sink(&mut self, sink: Sink) {
        self.sinks.push(sink);
    }

    /// The const-pool index of `bytes`, shared with an earlier equal entry. The
    /// pool is byte-transparent — german-string cells and packed i64 sets share
    /// it, each read by the opcode that indexes it.
    fn add_const_bytes(&mut self, bytes: Vec<u8>) -> ConstIdx {
        if let Some(i) = self.const_strings.iter().position(|c| *c == bytes) {
            return ConstIdx(i as u32);
        }
        let idx = ConstIdx(self.const_strings.len() as u32);
        self.const_strings.push(bytes);
        idx
    }

    pub fn add_const_string(&mut self, s: String) -> ConstIdx {
        self.add_const_bytes(s.into_bytes())
    }

    /// The register holding `v`'s f64 image. A float register *is* an i64 slot
    /// carrying `to_bits`, and the kernels read it back through that same codec,
    /// so a caller must never write `v as i64` — which compiles and is silently
    /// a different number.
    pub fn const_f64(&mut self, v: f64) -> Reg {
        self.emit(LogicalInstr::LoadConst { val: crate::batch::encode_f64(v) })
    }

    /// Push an i64 value pool for `IntInSet`, packed as `N × 8-byte LE`, and
    /// return its const index. Sorting is not a wire contract: the engine's
    /// `resolve` sorts the decoded pool before binary-searching it, because set
    /// membership does not depend on order and trusting the client here would
    /// turn a skewed pool into a wrong answer. Callers still sort (and dedup) to
    /// keep the pool small.
    pub fn add_const_int_set(&mut self, values: &[i64]) -> ConstIdx {
        self.add_const_bytes(gnitz_wire::as_le_bytes(values).to_vec())
    }

    /// The builder's one exit: the typed program, held to every structural rule
    /// a schema is not needed for. A caller resolving in-process goes straight
    /// on to `resolve_filter` / `resolve_scalar`; one shipping the program to
    /// the engine calls [`LogicalProgram::to_blob_bytes`] on it. Neither pays a
    /// wire round trip to be validated.
    ///
    /// `result_reg` is `None` for a map, which reads its output off the sinks.
    pub fn build(self, result_reg: Option<Reg>) -> Result<LogicalProgram, ExprValidateErr> {
        LogicalProgram::from_instrs(self.instrs, self.sinks, result_reg, self.const_strings)
    }
}

#[cfg(test)]
#[path = "tests/builder.rs"]
mod tests;
