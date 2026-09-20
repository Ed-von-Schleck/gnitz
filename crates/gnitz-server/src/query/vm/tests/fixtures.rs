//! VM test fixtures: the registry and child stores a hand-built plan's
//! operators read and write. A child of `vm`, so both `builder`'s and `exec`'s
//! tests reach it.

use super::*;
use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use gnitz_store::storage::Slot;
use gnitz_wire::ViewProps;

/// The view id every plan below is compiled for.
pub(in crate::query) const VIEW_ID: i64 = gnitz_wire::FIRST_USER_TABLE_ID as i64;

/// A registry holding one view homed under `dir` — the shape the VM actually
/// runs, and what `CircuitState::open_child` reads its recovery policy from.
pub(in crate::query) fn vm_registry(dir: &std::path::Path) -> RelationRegistry {
    let mut registry = RelationRegistry::new(Slot::SOLO, StoreConfig::default());
    registry
        .register(RelationSpec {
            id: VIEW_ID,
            kind: RelationKind::View,
            schema: crate::test_support::make_schema_u128_i64(),
            directory: dir.to_str().unwrap().to_string(),
            props: ViewProps::default(),
        })
        .unwrap();
    registry
}

/// What the emitter hands `build`, assembled by a test instead.
#[derive(Default)]
pub(in crate::query) struct TestPlan {
    pub(in crate::query) state: CircuitState,
    pub(in crate::query) instructions: Vec<Instr>,
    pub(in crate::query) integrates: Vec<(DeltaReg, StateIdx)>,
}

impl TestPlan {
    pub(in crate::query) fn push(&mut self, in_reg: u16, out_reg: u16, op: Op) {
        self.instructions.push(Instr {
            in_reg: DeltaReg(in_reg),
            out_reg: DeltaReg(out_reg),
            op,
        });
    }

    pub(in crate::query) fn integrate(&mut self, in_reg: u16, trace: StateIdx) {
        self.integrates.push((DeltaReg(in_reg), trace));
    }

    /// One child store, opened the way a compile opens one.
    pub(in crate::query) fn table(
        &mut self,
        registry: &RelationRegistry,
        dir: &std::path::Path,
        name: &str,
        schema: SchemaDescriptor,
    ) -> StateIdx {
        self.state
            .open_child(registry, VIEW_ID, dir.to_str().unwrap(), name, schema)
            .unwrap()
    }

    pub(in crate::query) fn build(self, schemas: Vec<SchemaDescriptor>, out: u16) -> Box<VmHandle> {
        super::build(self.instructions, self.integrates, schemas, self.state, DeltaReg(out))
    }
}

/// The net contents of one child store, as a Z-set — how a test sees what an
/// epoch left in a trace.
pub(in crate::query) fn trace_zset(
    vm: &VmHandle,
    idx: StateIdx,
    schema: &SchemaDescriptor,
) -> std::collections::HashMap<crate::test_support::RowKey, i64> {
    crate::test_support::zset_of(&vm.state.cursor(idx).materialize(), schema)
}
