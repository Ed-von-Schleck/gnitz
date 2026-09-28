//! DBSP VM: executes compiled circuit programs entirely in Rust.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use gnitz_store::expr::MapPlan;
use gnitz_store::ops;
use gnitz_store::relation::{CircuitState, StateIdx};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::schema::Slot;
use gnitz_store::storage::Batch;

mod builder;
mod exec;

#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(in crate::query) mod fixtures;

pub(in crate::query) use builder::build;
pub(in crate::query) use exec::{execute_epoch_multi, replay_chunk};

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// A register whose batch an instruction reads or writes — the one index space
/// a `Program` has, a trace being named by the [`StateIdx`] of its store.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::query) struct DeltaReg(pub u16);

impl DeltaReg {
    #[inline]
    pub(in crate::query) fn at(self) -> usize {
        self.0 as usize
    }
}

/// One VM instruction: the delta it reads, the delta it writes, and the
/// operator between them with all operator-specific data pre-resolved.
pub(in crate::query) struct Instr {
    in_reg: DeltaReg,
    out_reg: DeltaReg,
    op: Op,
    /// [`OpFacts::inert_on_empty`], baked.
    inert_on_empty: bool,
}

/// The operators, each boxing whatever the emitter baked for it.
pub(in crate::query) enum Op {
    Filter(Box<gnitz_expr::RowFilter>),
    Map(Box<MapPlan>),
    Negate,
    /// The one operator with a second delta operand.
    Union {
        in_b: DeltaReg,
    },
    /// Shared instruction for `distinct` and `positive_part`: per consolidated
    /// (PK, payload), emit `clamp(w_new) − clamp(w_old)` at `preset`'s bounds.
    WeightClamp {
        /// The history trace `I(in)`, which a `Program::integrates` entry
        /// accumulates.
        hist: StateIdx,
        preset: ops::ClampPreset,
    },
    /// The delta-trace inner join, equi, range and cross alike: the probe is
    /// baked by the compiler from the wire's `JoinKind` and side flag, so neither
    /// spelling reaches the instruction set.
    JoinDT {
        trace: StateIdx,
        probe: ops::JoinProbe,
    },
    WorkerFilter {
        slot: Slot,
    },
    /// Widen every row with NULL-filled payload columns — the LEFT JOIN
    /// null-fill's unmatched preserved rows — on the side `nulls_first` names.
    /// The column count is the difference between the two registers' schemas.
    NullExtend {
        nulls_first: bool,
    },
    Reduce {
        out_trace: StateIdx,
        plan: Box<BakedReduce>,
    },
    /// Per-group top-N: the ordered index of every input row is populated with
    /// the delta before the walk.
    TopN {
        out_trace: StateIdx,
        plan: Box<BakedTopN>,
    },
}

/// One `Op::Reduce`'s baked operator data: the plan and the table its combined
/// value index lives in.
pub(in crate::query) struct BakedReduce {
    pub(in crate::query) plan: ops::ReducePlan,
    avi_table: Option<StateIdx>,
}

impl BakedReduce {
    pub(in crate::query) fn new(plan: ops::ReducePlan, avi_table: Option<StateIdx>) -> BakedReduce {
        assert_eq!(
            plan.avi.is_some(),
            avi_table.is_some(),
            "a reduce's value-index bake and its table are set together",
        );
        BakedReduce { plan, avi_table }
    }

    /// The combined value index: its table and the bake that projects into it.
    fn avi(&self) -> Option<(StateIdx, &ops::AviBake)> {
        self.avi_table.zip(self.plan.avi.as_ref())
    }
}

/// One `Op::TopN`'s baked operator data: the plan and the table its ordered
/// index lives in.
pub(in crate::query) struct BakedTopN {
    pub(in crate::query) plan: gnitz_store::ops::TopNPlan,
    pub(in crate::query) index_table: StateIdx,
}

/// Everything the VM's passes need to know about one operator, classified in one
/// place.
struct OpFacts {
    /// It reads its input at net weights, so the VM folds that register when it
    /// is written.
    consolidates_in: bool,
    /// It writes operator state — a value index, an ordered index. Not the
    /// output trace, which a `Program::integrates` entry writes.
    writes_state: bool,
    /// With every delta operand empty its kernel returns
    /// `empty_with_schema(out_reg)` and touches no trace.
    inert_on_empty: bool,
}

fn facts(op: &Op) -> OpFacts {
    let linear = OpFacts {
        consolidates_in: false,
        writes_state: false,
        inert_on_empty: true,
    };
    match op {
        Op::Filter(_)
        | Op::Map(_)
        | Op::Negate
        | Op::Union { .. }
        | Op::WorkerFilter { .. }
        | Op::NullExtend { .. } => linear,
        Op::WeightClamp { .. } | Op::JoinDT { .. } => OpFacts { consolidates_in: true, ..linear },
        Op::Reduce { plan, .. } => OpFacts {
            consolidates_in: !plan.plan.is_exact_linear(),
            writes_state: true,
            // A global-ground reduce mints V₀ from an empty delta.
            inert_on_empty: !plan.plan.seeds_ground,
        },
        Op::TopN { .. } => OpFacts { writes_state: true, ..linear },
    }
}

impl Instr {
    pub(in crate::query) fn new(in_reg: DeltaReg, out_reg: DeltaReg, op: Op) -> Instr {
        let inert_on_empty = facts(&op).inert_on_empty;
        Instr { in_reg, out_reg, op, inert_on_empty }
    }

    /// The registers whose batch this instruction reads, in operand order. No
    /// `..` in the match: an opcode that gains a delta operand must fail to
    /// compile here.
    #[inline]
    fn reads(&self) -> [Option<DeltaReg>; 2] {
        let second = match &self.op {
            Op::Union { in_b } => Some(*in_b),
            Op::Filter(_)
            | Op::Map(_)
            | Op::Negate
            | Op::WeightClamp { hist: _, preset: _ }
            | Op::JoinDT { trace: _, probe: _ }
            | Op::WorkerFilter { slot: _ }
            | Op::NullExtend { nulls_first: _ }
            | Op::Reduce { out_trace: _, plan: _ }
            | Op::TopN { out_trace: _, plan: _ } => None,
        };
        [Some(self.in_reg), second]
    }
}

/// A compiled program with the registers it runs over.
pub(in crate::query) struct Vm {
    pub(in crate::query) program: Program,
    /// One batch per delta register.
    batches: Box<[Batch]>,
    /// No epoch has been dispatched yet and some instruction is not inert on an
    /// empty delta (a global-ground `Reduce` this worker owns) — the one reason
    /// an all-empty epoch is worth dispatching.
    pub(in crate::query) pending_ground_row: bool,
}

impl Vm {
    /// Free every register's buffer. For the end of a backfill, whose last chunk
    /// would otherwise stay resident for the cached plan's lifetime.
    pub(super) fn release(&mut self) {
        for batch in self.batches.iter_mut() {
            batch.release_buffers();
        }
    }
}

// ---------------------------------------------------------------------------
// Program
// ---------------------------------------------------------------------------

/// A compiled DBSP program, owning every resource its instructions name except
/// the child stores ([`CircuitState`]).
pub(in crate::query) struct Program {
    instructions: Box<[Instr]>,
    /// Each tick's accumulation of a register into its trace. Run after the
    /// whole instruction range — and not at all by a replay, which is what makes
    /// a replay read-only.
    integrates: Box<[(DeltaReg, StateIdx)]>,
    regs: Box<[Reg]>,
    /// The register the epoch's output is extracted from.
    out_reg: DeltaReg,
}

/// Who last reads a register: an instruction, an integrate — which run after
/// every instruction — or nobody: unread, or the sink, which the epoch extracts
/// after both.
#[derive(Clone, Copy, PartialEq, Debug)]
enum LastRead {
    Nobody,
    Instr(usize),
    Integrate(usize),
}

/// What `build` derived about one register.
struct Reg {
    schema: SchemaDescriptor,
    /// Some instruction reads it at net weights, so it is folded when written.
    fold: bool,
    /// Its last reader, which may take its batch.
    last_read: LastRead,
}

/// Where a read-only replay enters a program: the register it seeds and the
/// first instruction reading it.
#[derive(Clone, Copy)]
pub(in crate::query) struct ReplayEntry {
    reg: DeltaReg,
    pc: usize,
}

impl ReplayEntry {
    pub(in crate::query) fn reg(self) -> DeltaReg {
        self.reg
    }
}

impl Program {
    /// The schema register `reg` is labelled with. By reference: the dispatch
    /// loop hands it straight to kernels, and a `SchemaDescriptor` is not cheap
    /// to copy.
    pub(in crate::query) fn schema_of(&self, reg: DeltaReg) -> &SchemaDescriptor {
        &self.regs[reg.at()].schema
    }

    /// The schema of the register this program's output leaves in.
    pub(in crate::query) fn out_schema(&self) -> &SchemaDescriptor {
        self.schema_of(self.out_reg)
    }

    /// The register the epoch's output is extracted from.
    #[cfg(test)]
    pub(in crate::query) fn out_reg(&self) -> DeltaReg {
        self.out_reg
    }

    /// The operators, in program order.
    #[cfg(test)]
    pub(in crate::query) fn ops(&self) -> impl Iterator<Item = &Op> + '_ {
        self.instructions.iter().map(|i| &i.op)
    }

    /// Every instruction with, per operand, whether it is that operand's last
    /// reader — the take verdict the dispatch acts on.
    #[cfg(test)]
    pub(in crate::query) fn take_verdicts(&self) -> impl Iterator<Item = (&Op, [Option<bool>; 2])> + '_ {
        self.instructions.iter().enumerate().map(|(pc, instr)| {
            let takes = instr
                .reads()
                .map(|r| r.map(|r| self.regs[r.at()].last_read == LastRead::Instr(pc)));
            (&instr.op, takes)
        })
    }

    /// True iff some instruction trims the delta to this worker's own rows, so
    /// the result is a slice rather than a copy of what every worker computes.
    pub(in crate::query) fn trims_per_worker(&self) -> bool {
        self.instructions
            .iter()
            .any(|i| matches!(i.op, Op::WorkerFilter { .. }))
    }

    /// Enter a read-only replay at `reg`'s first reader.
    pub(in crate::query) fn replay_entry(&self, reg: DeltaReg) -> Result<ReplayEntry, String> {
        let pc = self
            .instructions
            .iter()
            .position(|i| i.reads().into_iter().flatten().any(|r| r == reg))
            .unwrap_or(self.instructions.len());
        let rest = &self.instructions[pc..];
        if rest.iter().any(|i| facts(&i.op).writes_state) {
            return Err("replay: the program writes operator state past its entry".into());
        }
        Ok(ReplayEntry { reg, pc })
    }
}
