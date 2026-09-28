//! VM test fixtures: the registry and child stores a hand-built plan's
//! operators read and write. A child of `vm`, so both `builder`'s and `exec`'s
//! tests reach it.

use super::*;
use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StateLayout, StoreConfig};
use gnitz_store::schema::Slot;
use gnitz_store::storage::StoreError;
use gnitz_wire::ViewProps;

/// The view id every plan below is compiled for.
pub(in crate::query) const VIEW_ID: i64 = gnitz_wire::FIRST_USER_TABLE_ID as i64;

/// A registry over the base directory `dir` holding one view, whose output store
/// `CircuitState::open` reads its recovery policy from.
pub(in crate::query) fn vm_registry(dir: &std::path::Path) -> RelationRegistry {
    let mut registry = RelationRegistry::new(dir.to_str().unwrap(), Slot::SOLO, StoreConfig::default());
    registry
        .register(RelationSpec {
            id: VIEW_ID,
            kind: RelationKind::View(ViewProps::Plain),
            schema: crate::test_support::make_schema_u128_i64(),
        })
        .unwrap();
    registry
}

/// What the emitter hands `build`, assembled by a test instead.
#[derive(Default)]
pub(in crate::query) struct TestPlan {
    layout: StateLayout,
    pub(in crate::query) instructions: Vec<Instr>,
    pub(in crate::query) integrates: Vec<(DeltaReg, StateIdx)>,
}

impl TestPlan {
    pub(in crate::query) fn push(&mut self, in_reg: u16, out_reg: u16, op: Op) {
        self.instructions
            .push(Instr::new(DeltaReg(in_reg), DeltaReg(out_reg), op));
    }

    pub(in crate::query) fn integrate(&mut self, in_reg: u16, trace: StateIdx) {
        self.integrates.push((DeltaReg(in_reg), trace));
    }

    /// One child store, declared the way a compile declares one.
    pub(in crate::query) fn table(&mut self, name: &str, schema: SchemaDescriptor) -> StateIdx {
        self.layout.declare(name.to_string(), schema)
    }

    /// Build a plan that declared no child store.
    pub(in crate::query) fn build(self, schemas: Vec<SchemaDescriptor>, out: u16) -> TestVm {
        TestVm {
            vm: super::build(self.instructions, self.integrates, schemas, DeltaReg(out)),
            state: CircuitState::default(),
        }
    }

    /// Build, opening every declared child store under `registry`'s view, as the
    /// DAG opens a compiled view's.
    pub(in crate::query) fn build_in(
        self,
        registry: &RelationRegistry,
        schemas: Vec<SchemaDescriptor>,
        out: u16,
    ) -> TestVm {
        TestVm {
            state: CircuitState::open(registry, VIEW_ID, self.layout).unwrap(),
            vm: super::build(self.instructions, self.integrates, schemas, DeltaReg(out)),
        }
    }
}

/// A built program and the operator state it runs over.
pub(in crate::query) struct TestVm {
    pub(in crate::query) vm: Vm,
    pub(in crate::query) state: CircuitState,
}

impl TestVm {
    pub(in crate::query) fn epoch<const N: usize>(
        &mut self,
        inputs: [(DeltaReg, Batch); N],
    ) -> Result<Batch, StoreError> {
        execute_epoch_multi(&mut self.vm, &mut self.state, inputs)
    }

    pub(in crate::query) fn replay(&mut self, seed: (DeltaReg, Batch)) -> Result<Batch, StoreError> {
        let entry = self.vm.program.replay_entry(seed.0).unwrap();
        replay_chunk(&mut self.vm, &mut self.state, entry, seed.1)
    }
}

impl std::ops::Deref for TestVm {
    type Target = Vm;

    fn deref(&self) -> &Vm {
        &self.vm
    }
}

impl std::ops::DerefMut for TestVm {
    fn deref_mut(&mut self) -> &mut Vm {
        &mut self.vm
    }
}

/// The net contents of one child store, as a Z-set — how a test sees what an
/// epoch left in a trace.
pub(in crate::query) fn trace_zset(
    vm: &TestVm,
    idx: StateIdx,
    schema: &SchemaDescriptor,
) -> std::collections::HashMap<crate::test_support::RowKey, i64> {
    crate::test_support::zset_of(&vm.state.cursor(idx).materialize(), schema)
}
