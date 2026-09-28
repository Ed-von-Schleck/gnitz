//! [`ExprBuilder`]: the emitter that turns a client-side expression into a
//! [`LogicalProgram`]. A caller emits freely; [`ExprBuilder::build`] is where it
//! is told, once, what is unsupported.

use crate::{CalendarOp, ConstIdx, ExprValidateErr, LogicalInstr, LogicalProgram, Reg, Sink, MAX_REGS};

/// Accumulates the instructions and constants of one expression program.
#[derive(Default)]
pub struct ExprBuilder {
    instrs: Vec<LogicalInstr>,
    const_strings: Vec<Vec<u8>>,
}

impl ExprBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// False once the program is past the register cap, where `build` rejects it
    /// whatever follows — so both dedup scans below stop paying there, and
    /// lowering a huge expression stays linear rather than quadratic in its size.
    fn within_reg_cap(&self) -> bool {
        self.instrs.len() <= MAX_REGS
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
                Some(&LogicalInstr::LoadConst { val, unsigned }) => {
                    return self.const_f64(if unsigned { val as u64 as f64 } else { val as f64 })
                }
                _ => instr,
            },
            // An overflowing day count stays the kernel, which makes it NULL.
            LogicalInstr::Calendar {
                op: CalendarOp::ToMicros,
                a,
                micros: false,
            } => match self.instrs.get(a.0 as usize) {
                Some(&LogicalInstr::LoadConst { val, unsigned: false }) => match crate::calendar::days_to_micros(val) {
                    (val, false) => LogicalInstr::LoadConst { val, unsigned: false },
                    (_, true) => instr,
                },
                _ => instr,
            },
            // A range check of a value already range-checked into the same type is
            // that value: it is whole in the target and carries the same U64 tracking.
            LogicalInstr::IntCast { a, fi }
                if self.instrs.get(a.0 as usize).and_then(LogicalInstr::range_check) == Some(fi) =>
            {
                return a
            }
            _ => instr,
        };
        if self.within_reg_cap() {
            if let Some(i) = self.instrs.iter().position(|e| *e == instr) {
                return Reg(i as u16);
            }
        }
        let reg = Reg(self.instrs.len() as u16);
        self.instrs.push(instr);
        reg
    }

    /// The const-pool index of `bytes`, shared with an earlier equal entry. The
    /// pool is byte-transparent — german-string cells and packed i64 sets share
    /// it, each read by the opcode that indexes it. Only a miss allocates.
    pub fn add_const_bytes(&mut self, bytes: &[u8]) -> ConstIdx {
        if self.within_reg_cap() {
            if let Some(i) = self.const_strings.iter().position(|c| c.as_slice() == bytes) {
                return ConstIdx(i as u32);
            }
        }
        let idx = ConstIdx(self.const_strings.len() as u32);
        self.const_strings.push(bytes.to_vec());
        idx
    }

    /// The register holding `v`'s f64 image. A float register *is* an i64 slot
    /// carrying `to_bits`, and the kernels read it back through that same codec,
    /// so a caller must never write `v as i64` — which compiles and is silently
    /// a different number.
    pub fn const_f64(&mut self, v: f64) -> Reg {
        self.emit(LogicalInstr::LoadConst {
            val: crate::batch::encode_f64(v),
            unsigned: false,
        })
    }

    /// The const index of `values` as an `IntInSet` pool: strictly ascending,
    /// packed `N × 8-byte LE`.
    pub fn add_const_int_set(&mut self, mut values: Vec<i64>) -> ConstIdx {
        values.sort_unstable();
        values.dedup();
        self.add_const_bytes(gnitz_wire::as_le_bytes(&values))
    }

    /// The typed program, held to every structural rule a schema is not needed
    /// for. `sinks` is its output — one `Sink::Reg` for a filter or scalar, one
    /// sink per output payload slot for a map.
    pub fn build(self, sinks: Vec<Sink>) -> Result<LogicalProgram, ExprValidateErr> {
        LogicalProgram::from_instrs(self.instrs, sinks, self.const_strings)
    }
}

#[cfg(test)]
#[path = "tests/builder.rs"]
mod tests;
