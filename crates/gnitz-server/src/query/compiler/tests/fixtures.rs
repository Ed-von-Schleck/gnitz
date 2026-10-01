//! Compiler test fixtures: the relation registry a circuit compiles against and
//! the few helpers every test file shares. A child of `compiler`, so it reaches
//! `LoadedCircuit`'s private items.

use super::*;
use gnitz_store::relation::{RelationKind, RelationSpec, StoreConfig};
use gnitz_store::schema::Slot;
use gnitz_wire::Circuit;

pub(super) fn loaded(circuit: Circuit) -> LoadedCircuit {
    LoadedCircuit::new(circuit).expect("test circuit within the node limit")
}

/// One table row's circuit, built into an empty [`Circuit`].
pub(super) type Build = fn(&mut Circuit);

/// An unbounded delta scan of `source`.
pub(super) fn scan(circuit: &mut Circuit, source: u64) -> NodeId {
    circuit.input_delta(source, gnitz_wire::ReadBound::None)
}

/// A decodable expr-program blob for a `Filter` or computed `Map` that must
/// exist and is never executed.
pub(super) fn dummy_expr_blob() -> Vec<u8> {
    gnitz_expr::LogicalProgram::copy_cols(&[]).to_blob_bytes()
}

/// The guard that rejected a build. Naming it is what makes a guard test
/// attributable: a bare `is_err()` also passes when an unrelated guard fires.
pub(super) fn rejection<T>(r: Result<T, String>) -> String {
    r.map(|_| "a plan").expect_err("expected a rejection")
}

/// A registry for worker `slot` holding only `rows`, each registered as a stream —
/// a storeless kind, so a compile finds its schemas without opening a store.
pub(super) fn sources_at(slot: Slot, rows: impl IntoIterator<Item = (u64, SchemaDescriptor)>) -> RelationRegistry {
    let mut registry = RelationRegistry::new("", slot, StoreConfig::default());
    for (id, schema) in rows {
        let spec = RelationSpec { id, kind: RelationKind::Stream, schema };
        registry.register(spec).expect("a stream registers without a store");
    }
    registry
}

/// [`sources_at`] the one worker of a single-worker process.
pub(super) fn sources(rows: impl IntoIterator<Item = (u64, SchemaDescriptor)>) -> RelationRegistry {
    sources_at(Slot::SOLO, rows)
}

/// A [`Relay`] without its key, `Stays` for none: the routing decision a test
/// compares.
#[derive(Debug, PartialEq)]
pub(super) enum Route {
    Stays,
    Broadcast,
    Round,
    Share,
}

pub(super) fn route(relay: Option<&Relay>) -> Route {
    match relay {
        None => Route::Stays,
        Some(Relay::Broadcast) => Route::Broadcast,
        Some(Relay::Round(_)) => Route::Round,
        Some(Relay::Share(_)) => Route::Share,
    }
}
