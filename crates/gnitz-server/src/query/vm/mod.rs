//! DBSP VM: executes compiled circuit programs entirely in Rust.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use gnitz_store::relation::{CircuitState, StateIdx};
use gnitz_wire::ClampKind;
use gnitz_zset::algebra::MapPlan;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;
use gnitz_zset::schema::Slot;
use gnitz_zset::stream;

mod builder;
mod exec;

#[cfg(test)]
#[path = "tests/fixtures.rs"]
mod fixtures;

pub(in crate::query) use builder::ProgramBuilder;
pub(in crate::query) use exec::{execute_epoch_multi, replay_chunk, SourceReads};

// ---------------------------------------------------------------------------
// Instruction set
// ---------------------------------------------------------------------------

/// A register whose batch an instruction reads or writes — the one index space
/// a `Program` has, a trace being named by its [`Integral`]. Minted only by
/// [`ProgramBuilder`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::query) struct DeltaReg(u16);

impl DeltaReg {
    #[inline]
    pub(in crate::query) fn at(self) -> usize {
        self.0 as usize
    }
}

/// One VM instruction: the delta it reads, the delta it writes, and the
/// operator between them with all operator-specific data pre-resolved.
struct Instr {
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
    /// (PK, payload), emit `clamp(w_new) − clamp(w_old)` at `kind`'s bounds.
    WeightClamp {
        /// The history trace `I(in)`, which `finish` integrates.
        hist: StateIdx,
        kind: ClampKind,
    },
    /// The delta-trace inner join, equi, range and cross alike: the probe is
    /// baked by the compiler from the wire's `JoinKind` and side flag, so neither
    /// spelling reaches the instruction set.
    JoinDT {
        trace: Integral,
        probe: stream::JoinProbe,
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

/// The integral of a delta, as its readers find it: a join's probe, and a
/// bounded view's replay, which seeds from it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::query) enum Integral {
    /// A child store the circuit integrates a register into.
    Own(StateIdx),
    /// The store of the relation the delta is scanned from, read as it stood
    /// when the view last absorbed that relation: nothing is integrated, because
    /// the relation's own ingest already holds every row a trace would.
    Source(u64),
}

/// One `Op::Reduce`'s baked operator data: the plan and the table its combined
/// value index lives in.
pub(in crate::query) struct BakedReduce {
    plan: stream::ReducePlan,
    avi_table: Option<StateIdx>,
}

impl BakedReduce {
    pub(in crate::query) fn new(plan: stream::ReducePlan, avi_table: Option<StateIdx>) -> BakedReduce {
        assert_eq!(
            plan.index_schema().is_some(),
            avi_table.is_some(),
            "a reduce's value index and its table are set together",
        );
        BakedReduce { plan, avi_table }
    }
}

/// One `Op::TopN`'s baked operator data: the plan and the table its ordered
/// index lives in.
pub(in crate::query) struct BakedTopN {
    pub(in crate::query) plan: gnitz_zset::stream::TopNPlan,
    pub(in crate::query) index_table: StateIdx,
}

/// Everything the VM's passes need to know about one operator, classified in one
/// place.
struct OpFacts {
    /// It reads its input at net weights, so the VM folds that register when it
    /// is written.
    consolidates_in: bool,
    /// Its output depends on state it owns — its input's history, a value index,
    /// an ordered index — so a replay past it would read that state against a
    /// seed it never saw.
    stateful: bool,
    /// With every delta operand empty its kernel returns
    /// `empty_with_schema(out_reg)` and touches no trace.
    inert_on_empty: bool,
}

fn facts(op: &Op) -> OpFacts {
    let linear = OpFacts {
        consolidates_in: false,
        stateful: false,
        inert_on_empty: true,
    };
    match op {
        Op::Filter(_)
        | Op::Map(_)
        | Op::Negate
        | Op::Union { .. }
        | Op::WorkerFilter { .. }
        | Op::NullExtend { .. } => linear,
        Op::WeightClamp { .. } => OpFacts {
            consolidates_in: true,
            stateful: true,
            ..linear
        },
        Op::JoinDT { .. } => OpFacts { consolidates_in: true, ..linear },
        Op::Reduce { plan, .. } => OpFacts {
            consolidates_in: !plan.plan.is_exact_linear(),
            stateful: true,
            // A global-ground reduce mints V₀ from an empty delta.
            inert_on_empty: !plan.plan.seeds_ground,
        },
        Op::TopN { .. } => OpFacts { stateful: true, ..linear },
    }
}

impl Instr {
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
            | Op::WeightClamp { hist: _, kind: _ }
            | Op::JoinDT { trace: _, probe: _ }
            | Op::WorkerFilter { slot: _ }
            | Op::NullExtend { nulls_first: _ }
            | Op::Reduce { out_trace: _, plan: _ }
            | Op::TopN { out_trace: _, plan: _ } => None,
        };
        [Some(self.in_reg), second]
    }

    /// The trace this instruction's op owns as the integral of one of its own
    /// registers — the clamp's input history, a reduce's or top-N's output — which
    /// `finish` schedules as an integrate. No `_` arm, as in [`Self::reads`]: a new
    /// opcode must say whether it owns one.
    fn own_integral(&self) -> Option<(DeltaReg, StateIdx)> {
        match &self.op {
            Op::WeightClamp { hist, .. } => Some((self.in_reg, *hist)),
            Op::Reduce { out_trace, .. } | Op::TopN { out_trace, .. } => Some((self.out_reg, *out_trace)),
            Op::Filter(_)
            | Op::Map(_)
            | Op::Negate
            | Op::Union { .. }
            | Op::JoinDT { .. }
            | Op::WorkerFilter { .. }
            | Op::NullExtend { .. } => None,
        }
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
/// the stores: the view's own children ([`CircuitState`]) and the relations an
/// [`Integral::Source`] reads.
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

/// What [`ProgramBuilder::finish`] derived about one register.
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

    /// A batch written to `reg` is folded.
    pub(in crate::query) fn folds(&self, reg: DeltaReg) -> bool {
        self.regs[reg.at()].fold
    }

    /// True iff some instruction trims the delta to this worker's own rows, so
    /// the result is a slice rather than a copy of what every worker computes.
    pub(in crate::query) fn trims_per_worker(&self) -> bool {
        self.instructions
            .iter()
            .any(|i| matches!(i.op, Op::WorkerFilter { .. }))
    }

    /// Enter a read-only replay at `reg`'s first reader. Refused where the seed
    /// would reach a stateful operator, or where one runs without it: every
    /// other register is empty in a replay, so an operator the seed does not
    /// reach is skipped exactly when it is inert on an empty delta.
    pub(in crate::query) fn replay_entry(&self, reg: DeltaReg) -> Result<ReplayEntry, String> {
        let pc = self
            .instructions
            .iter()
            .position(|i| i.reads().into_iter().flatten().any(|r| r == reg))
            .unwrap_or(self.instructions.len());
        let mut seeded = vec![false; self.regs.len()];
        seeded[reg.at()] = true;
        for instr in &self.instructions[pc..] {
            let reads_seed = instr.reads().into_iter().flatten().any(|r| seeded[r.at()]);
            if facts(&instr.op).stateful && (reads_seed || !instr.inert_on_empty) {
                return Err("replay: the seed reaches a stateful operator".into());
            }
            seeded[instr.out_reg.at()] |= reads_seed;
        }
        Ok(ReplayEntry { reg, pc })
    }
}
