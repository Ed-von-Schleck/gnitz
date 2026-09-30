//! VM test fixtures: a plan assembled by hand, and the child stores its
//! operators read and write. A child of `vm`, so both `builder`'s and `exec`'s
//! tests reach it.

use super::*;
use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StateLayout, StoreConfig};
use gnitz_wire::ViewProps;

/// What the emitter assembles, assembled by a test: the program, and the child
/// stores its operators declare.
#[derive(Default)]
pub(super) struct TestPlan {
    prog: ProgramBuilder,
    layout: StateLayout,
}

impl std::ops::Deref for TestPlan {
    type Target = ProgramBuilder;

    fn deref(&self) -> &ProgramBuilder {
        &self.prog
    }
}

impl std::ops::DerefMut for TestPlan {
    fn deref_mut(&mut self) -> &mut ProgramBuilder {
        &mut self.prog
    }
}

impl TestPlan {
    /// One child store, declared the way a compile declares one.
    pub(super) fn table(&mut self, name: &str, schema: SchemaDescriptor) -> StateIdx {
        self.layout.declare(name.to_string(), schema)
    }

    /// Push `plan`'s reduce over `in_reg`, declaring its output trace and, for a
    /// MIN/MAX, its value index — as the emitter does. Returns the output
    /// register and trace.
    pub(super) fn reduce(&mut self, in_reg: DeltaReg, plan: ops::ReducePlan) -> (DeltaReg, StateIdx) {
        let out_schema = plan.shape.output_schema;
        let out_trace = self.table("reduce", out_schema);
        let avi_table = plan.avi.as_ref().map(|bake| self.table("avidx", bake.schema));
        let plan = Box::new(BakedReduce::new(plan, avi_table));
        (
            self.prog.push(in_reg, out_schema, Op::Reduce { out_trace, plan }),
            out_trace,
        )
    }

    /// Finish the program with output `out`, opening every declared child store
    /// in a fresh directory as the DAG opens a compiled view's.
    pub(super) fn open(self, out: DeltaReg) -> TestVm {
        const VIEW_ID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;
        let dir = tempfile::tempdir().unwrap();
        let mut registry = RelationRegistry::new(dir.path().to_str().unwrap(), Slot::SOLO, StoreConfig::default());
        registry
            .register(RelationSpec {
                id: VIEW_ID,
                kind: RelationKind::View(ViewProps::Plain),
                schema: crate::test_support::make_schema_u128_i64(),
            })
            .unwrap();
        TestVm {
            state: CircuitState::open(&registry, VIEW_ID, self.layout).unwrap(),
            vm: self.prog.finish(out),
            _registry: registry,
            _dir: dir,
        }
    }
}

/// A built program, the operator state it runs over, and what that state lives
/// in.
pub(super) struct TestVm {
    pub(super) vm: Vm,
    pub(super) state: CircuitState,
    _registry: RelationRegistry,
    _dir: tempfile::TempDir,
}

impl TestVm {
    pub(super) fn epoch<const N: usize>(&mut self, inputs: [(DeltaReg, Batch); N]) -> Batch {
        execute_epoch_multi(&mut self.vm, &mut self.state, inputs).unwrap()
    }

    pub(super) fn replay(&mut self, reg: DeltaReg, seed: Batch) -> Batch {
        let entry = self.vm.program.replay_entry(reg).unwrap();
        replay_chunk(&mut self.vm, &mut self.state, entry, seed).unwrap()
    }

    /// The net contents of one child store, as a Z-set — how a test sees what an
    /// epoch left in a trace.
    pub(super) fn trace(&self, idx: StateIdx) -> std::collections::HashMap<crate::test_support::RowKey, i64> {
        let b = self.state.cursor(idx).materialize();
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
    use gnitz_expr::LogicalInstr::{Cmp, LoadColInt, LoadConst};
    let mut eb = gnitz_expr::ExprBuilder::new();
    let (a, b) = (
        eb.emit(LoadColInt { col }),
        eb.emit(LoadConst { val: lit, unsigned: false }),
    );
    let r = eb.emit(Cmp { op: gnitz_expr::CmpOp::Gt, a, b });
    let program = eb.build(vec![gnitz_expr::Sink::Reg(r)]).unwrap();
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
