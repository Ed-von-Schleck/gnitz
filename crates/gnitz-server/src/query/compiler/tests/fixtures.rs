//! Compiler test fixtures: the relation registry a circuit compiles against and
//! the few helpers every test file shares. A child of `compiler`, so it reaches
//! `LoadedCircuit`'s private items.

use super::*;
use gnitz_store::relation::{RelationKind, RelationSpec, StoreConfig};
use gnitz_wire::Circuit;
use gnitz_zset::algebra::{Placement, Slot};

pub(super) fn loaded(circuit: Circuit) -> LoadedCircuit {
    assert!(!circuit.nodes().is_empty(), "a decoded circuit has an output");
    LoadedCircuit(circuit)
}

/// `loaded`'s routing. A view's schema decides only which key its placement
/// names, which no route reads.
pub(super) fn routing(loaded: &LoadedCircuit, registry: &RelationRegistry) -> Result<ViewMeta, String> {
    let view = crate::test_support::make_schema_u64_i64();
    ViewMeta::derive(loaded, registry, &view).map(|(meta, _)| meta)
}

/// The id the compiled circuit's view goes by.
pub(super) const VIEW: u64 = 99;

/// `circuit` compiled into a view of schema `view`, under the routing derived
/// from it.
pub(super) fn compile(
    circuit: Circuit,
    registry: &RelationRegistry,
    view: &SchemaDescriptor,
    bounded: bool,
) -> Result<(CompileOutput, StateLayout), String> {
    let loaded = loaded(circuit);
    let (meta, _) = ViewMeta::derive(&loaded, registry, view)?;
    compile_view(&loaded, registry, VIEW, view, &meta, bounded)
}

/// `loaded` emitted whole; `store`: its output node's rows are the view store's.
pub(super) fn whole<'a>(
    loaded: &'a LoadedCircuit,
    registry: &'a RelationRegistry,
    meta: &'a ViewMeta,
    store: bool,
) -> Result<EmitCtx<'a>, String> {
    let mut ctx = EmitCtx::new(loaded, registry, meta, store.then_some(VIEW));
    for (nid, op) in loaded.ops() {
        let reg = emit_node(&mut ctx, nid, op)?;
        ctx.regs.push(reg);
    }
    Ok(ctx)
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

/// A source relation to register: its schema, and where its rows live. From a
/// bare schema, keyed by its whole PK.
#[derive(Clone, Copy)]
pub(super) struct Source {
    pub(super) schema: SchemaDescriptor,
    pub(super) placement: Placement,
}

impl From<SchemaDescriptor> for Source {
    fn from(schema: SchemaDescriptor) -> Self {
        Source {
            schema,
            placement: Placement::full_pk(&schema),
        }
    }
}

impl Source {
    /// This source's schema, placed by `placement`.
    pub(super) fn placed(self, placement: Placement) -> Source {
        Source { placement, ..self }
    }
}

/// A registry for worker `slot` holding only `rows`, each registered as a stream —
/// a storeless kind, so a compile finds its schemas without opening a store.
pub(super) fn sources_at<S: Into<Source>>(slot: Slot, rows: impl IntoIterator<Item = (u64, S)>) -> RelationRegistry {
    let mut registry = RelationRegistry::new("", slot, StoreConfig::default());
    for (id, source) in rows {
        let Source { schema, placement } = source.into();
        let spec = RelationSpec {
            id,
            kind: RelationKind::Stream,
            schema,
            placement,
            pk_repeats: false,
        };
        registry.register(spec).expect("a stream registers without a store");
    }
    registry
}

/// [`sources_at`] the one worker of a single-worker process.
pub(super) fn sources<S: Into<Source>>(rows: impl IntoIterator<Item = (u64, S)>) -> RelationRegistry {
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
        Some(Relay::Round(plan)) if plan.is_broadcast() => Route::Broadcast,
        Some(Relay::Round(_)) => Route::Round,
        Some(Relay::Share(_)) => Route::Share,
    }
}
