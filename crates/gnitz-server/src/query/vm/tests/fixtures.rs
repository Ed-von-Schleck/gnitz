//! VM test fixtures: a plan assembled by hand, and the child stores its
//! operators read and write. A child of `vm`, so `exec`'s tests reach it.

use super::*;
use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use gnitz_wire::ViewBudget;
use gnitz_zset::algebra::Slot;

/// The view a test program's children are opened under.
const VIEW_ID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;

/// Push `plan`'s reduce over `in_reg`, declaring its output trace and, for a
/// MIN/MAX, its value index, and integrating its output — as the emitter does.
/// Returns the output register and trace.
pub(super) fn reduce(p: &mut ProgramBuilder, in_reg: DeltaReg, plan: stream::ReducePlan) -> (DeltaReg, StateIdx) {
    let out_schema = *plan.output_schema();
    let out_trace = p.declare("reduce".into(), out_schema);
    let index = plan.index_schema().map(|schema| p.declare("avidx".into(), *schema));
    let op = Op::Reduce {
        out_trace: Integral::Own(out_trace),
        index,
        plan: Box::new(plan),
    };
    let out = p.push(in_reg, out_schema, op);
    p.integrate(out, out_trace);
    (out, out_trace)
}

/// A built program, the operator state it runs over, and what that state lives
/// in.
pub(super) struct TestVm {
    pub(super) vm: Vm,
    pub(super) state: CircuitState,
    pub(super) registry: RelationRegistry,
    /// The sources the program has been fed no row of.
    pub(super) unfed: Vec<u64>,
    _dir: tempfile::TempDir,
}

impl TestVm {
    /// Finish `p` with output `out`, opening every child store it declares in a
    /// fresh directory as the DAG opens a compiled view's.
    pub(super) fn open(p: ProgramBuilder, out: DeltaReg) -> TestVm {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = RelationRegistry::new(dir.path().to_str().unwrap(), Slot::SOLO, StoreConfig::default());
        let schema = crate::test_support::make_schema_u128_i64();
        registry
            .register(RelationSpec {
                id: VIEW_ID,
                kind: RelationKind::View(ViewBudget::Plain),
                schema,
                placement: gnitz_zset::algebra::Placement::full_pk(&schema),
                pk_repeats: false,
                compress: false,
            })
            .unwrap();
        let vm = p.finish(out);
        TestVm {
            state: CircuitState::open(&registry, VIEW_ID, vm.layout()).unwrap(),
            vm,
            registry,
            unfed: Vec::new(),
            _dir: dir,
        }
    }

    /// The program, beside the stores an epoch of it runs over.
    fn parts(&mut self) -> (&mut Vm, Stores<'_>) {
        let stores = Stores {
            own: &mut self.state,
            registry: &self.registry,
            unfed: &self.unfed,
        };
        (&mut self.vm, stores)
    }

    /// The ground rows the program's global aggregates owe, each minted in an
    /// epoch of its own.
    pub(super) fn ground(&mut self) -> Batch {
        let (vm, mut stores) = self.parts();
        mint_ground_rows(vm, &mut stores).unwrap()
    }

    /// One tick's epoch over `seed`, taken, of a program that runs no round.
    pub(super) fn epoch(&mut self, reg: DeltaReg, mut seed: Batch) -> Batch {
        let (vm, mut stores) = self.parts();
        let mut epoch = Epoch::tick(reg, &mut seed, true);
        match run(vm, &mut stores, &mut epoch, None).unwrap() {
            Ran::Done(out) => out,
            Ran::Round { .. } => panic!("the program stands at a round"),
        }
    }

    /// One tick's epoch over the lent `seed`, each round it stands at answered by
    /// `gather`, which is handed the round's rows and whether they were lent.
    pub(super) fn epoch_with(
        &mut self,
        reg: DeltaReg,
        seed: &mut Batch,
        take: bool,
        mut gather: impl FnMut(&Batch, bool) -> Batch,
    ) -> Batch {
        let (vm, mut stores) = self.parts();
        let mut epoch = Epoch::tick(reg, seed, take);
        let mut gathered = None;
        loop {
            match run(vm, &mut stores, &mut epoch, gathered.take()).unwrap() {
                Ran::Done(out) => return out,
                Ran::Round { batch, .. } => {
                    let lent = matches!(batch, std::borrow::Cow::Borrowed(_));
                    gathered = Some(gather(&batch, lent));
                }
            }
        }
    }

    pub(super) fn replay(&mut self, reg: DeltaReg, seed: Batch) -> Batch {
        let entry = self.vm.replay_entry(reg).unwrap();
        let (vm, mut stores) = self.parts();
        replay_chunk(vm, &mut stores, entry, seed).unwrap()
    }

    /// The rows one child store holds, at their net weights.
    pub(super) fn held(&self, idx: StateIdx) -> std::rc::Rc<Batch> {
        self.state.cursor_between(idx, &[], &[]).materialize()
    }

    /// The net contents of one child store, as a Z-set — how a test sees what an
    /// epoch left in a trace.
    pub(super) fn trace(&self, idx: StateIdx) -> std::collections::HashMap<crate::test_support::RowKey, i64> {
        let b = self.held(idx);
        crate::test_support::zset_of(&b, b.schema())
    }
}

impl std::ops::Deref for TestVm {
    type Target = Vm;

    fn deref(&self) -> &Vm {
        &self.vm
    }
}

/// `Filter(col > lit)` over `schema`'s integer column `col`.
pub(super) fn filter_gt(schema: &SchemaDescriptor, col: u32, lit: i64) -> Op {
    let program = crate::test_support::cmp_all(&[(gnitz_expr::CmpOp::Gt, col, lit)]);
    Op::Filter(Box::new(program.resolve_filter(schema).unwrap()))
}

/// `out` holds exactly `rows` — `(pk, weight, payload)` over a
/// `make_schema_u128_i64`-shaped schema — in that order.
#[track_caller]
pub(super) fn assert_rows(out: &Batch, rows: &[(u128, i64, i64)]) {
    use crate::test_support::{make_batch_u128_raw, weighted_rows};
    assert_eq!(
        weighted_rows(out),
        weighted_rows(&make_batch_u128_raw(out.schema(), rows))
    );
}
