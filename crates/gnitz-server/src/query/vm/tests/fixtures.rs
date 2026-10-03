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

/// The view a [`TestPlan`]'s children are opened under.
const VIEW_ID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;

impl TestPlan {
    /// One child store, declared the way a compile declares one.
    pub(super) fn table(&mut self, name: &str, schema: SchemaDescriptor) -> StateIdx {
        self.layout.declare(name.to_string(), schema)
    }

    /// Push `plan`'s reduce over `in_reg`, declaring its output trace and, for a
    /// MIN/MAX, its value index — as the emitter does. Returns the output
    /// register and trace.
    pub(super) fn reduce(&mut self, in_reg: DeltaReg, plan: stream::ReducePlan) -> (DeltaReg, StateIdx) {
        let out_schema = *plan.output_schema();
        let out_trace = self.table("reduce", out_schema);
        let index = plan.index_schema().map(|schema| self.table("avidx", *schema));
        let op = Op::Reduce {
            out_trace: Some(out_trace),
            index,
            plan: Box::new(plan),
        };
        (self.prog.push(in_reg, out_schema, op), out_trace)
    }

    /// Finish the program with output `out`, opening every declared child store
    /// in a fresh directory as the DAG opens a compiled view's.
    pub(super) fn open(self, out: DeltaReg) -> TestVm {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = RelationRegistry::new(dir.path().to_str().unwrap(), Slot::SOLO, StoreConfig::default());
        let schema = crate::test_support::make_schema_u128_i64();
        registry
            .register(RelationSpec {
                id: VIEW_ID,
                kind: RelationKind::View(ViewProps::Plain),
                schema,
                placement: gnitz_zset::schema::Placement::full_pk(&schema),
            })
            .unwrap();
        TestVm {
            state: CircuitState::open(&registry, VIEW_ID, self.layout).unwrap(),
            vm: self.prog.finish(out),
            registry,
            unfed: Vec::new(),
            _dir: dir,
        }
    }
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
    /// The program, beside the stores an epoch of it runs over.
    fn parts(&mut self) -> (&mut Vm, Stores<'_>) {
        let stores = Stores {
            own: &mut self.state,
            registry: &self.registry,
            view: VIEW_ID,
            unfed: &self.unfed,
        };
        (&mut self.vm, stores)
    }

    pub(super) fn epoch<const N: usize>(&mut self, inputs: [(DeltaReg, Batch); N]) -> Batch {
        let (vm, mut stores) = self.parts();
        execute_epoch(vm, &mut stores, inputs).unwrap()
    }

    pub(super) fn replay(&mut self, reg: DeltaReg, seed: Batch) -> Batch {
        let entry = self.vm.replay_entry(reg).unwrap();
        let (vm, mut stores) = self.parts();
        replay_chunk(vm, &mut stores, entry, seed).unwrap()
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
    let program = crate::test_support::cmp_const(gnitz_expr::CmpOp::Gt, col, lit);
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
