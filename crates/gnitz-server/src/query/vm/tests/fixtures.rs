//! VM test fixtures: the registry and child stores a hand-built program's trace
//! registers are backed by. A child of `vm`, so both `builder`'s and `exec`'s
//! tests reach it.

use super::*;
use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StateIdx, StoreConfig};
use gnitz_store::schema::SchemaDescriptor;
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

/// One child store backing a trace register, opened the way a compile opens one.
pub(in crate::query) fn owned_table(
    builder: &mut ProgramBuilder,
    registry: &RelationRegistry,
    dir: &std::path::Path,
    name: &str,
    schema: SchemaDescriptor,
) -> StateIdx {
    builder
        .state
        .open_child(registry, VIEW_ID, dir.to_str().unwrap(), name, schema)
        .unwrap()
}
