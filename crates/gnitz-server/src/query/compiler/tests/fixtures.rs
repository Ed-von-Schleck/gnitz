//! Compiler test fixtures: the relation registry a circuit compiles against and
//! the few helpers every test file shares. A child of `compiler`, so it reaches
//! its private items.

use super::*;
use gnitz_store::relation::{RelationKind, RelationSpec, StoreConfig};
use gnitz_wire::{AggDescriptor, Circuit, GroupAggs};
use gnitz_zset::algebra::{Placement, Slot};

/// `circuit`'s routing.
pub(super) fn routing<'a>(circuit: &Circuit, registry: &'a RelationRegistry) -> Result<ViewMeta<'a>, String> {
    ViewMeta::derive(circuit, registry)
}

/// The id the compiled circuit's view goes by.
pub(super) const VIEW: u64 = 99;

/// `circuit` compiled into a view of the schema it outputs.
pub(super) fn compile(circuit: &Circuit, registry: &RelationRegistry, bounded: bool) -> Result<CompileOutput, String> {
    let meta = routing(circuit, registry)?;
    let ctx = EmitCtx::emit(circuit, &meta, VIEW)?;
    let view = ctx.prog.schema_of(ctx.regs[circuit.out()]);
    compile_view(circuit, registry, VIEW, &view, bounded).map(|(out, ..)| out)
}

/// The placement a view of the schema `circuit` outputs registers under.
pub(super) fn placed(circuit: &Circuit, registry: &RelationRegistry) -> Result<Placement, String> {
    let view = *compile(circuit, registry, false)?.vm.out_schema();
    compile_view(circuit, registry, VIEW, &view, false).map(|(_, placement, _)| placement)
}

/// One table row's circuit, built into an empty [`Circuit`].
pub(super) type Build = fn(&mut Circuit);

/// One table row's operators over an input node: answers the last of them.
pub(super) type Stage = fn(&mut Circuit, NodeId) -> NodeId;

/// An unbounded delta scan of `source`.
pub(super) fn scan(circuit: &mut Circuit, source: u64) -> NodeId {
    circuit.input_delta(source, gnitz_wire::ReadBound::None)
}

/// A filter program comparing column `col` with a constant.
pub(super) fn predicate(col: u32) -> Vec<u8> {
    crate::test_support::cmp_all(&[(gnitz_expr::CmpOp::Lt, col, 100)]).to_blob_bytes()
}

/// COUNT(*) grouped by `group_cols`.
pub(super) fn count_by(group_cols: &[u32]) -> GroupAggs {
    GroupAggs {
        group_cols: group_cols.to_vec(),
        aggs: vec![AggDescriptor::COUNT_STAR],
    }
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

/// A registry for worker `slot` over `dir` holding only `rows`, each of its kind.
fn registry_of<S: Into<Source>>(
    dir: &str,
    slot: Slot,
    rows: impl IntoIterator<Item = (u64, RelationKind, S)>,
) -> RelationRegistry {
    let mut registry = RelationRegistry::new(dir, slot, StoreConfig::default());
    for (id, kind, source) in rows {
        let Source { schema, placement } = source.into();
        let spec = RelationSpec {
            id,
            kind,
            placement,
            pk_repeats: false,
            compress: false,
        };
        registry.register(spec, schema).expect("a relation registers");
    }
    registry
}

/// A registry for worker `slot` holding only `rows`, each registered as a stream —
/// a storeless kind, so a compile finds its schemas without opening a store.
pub(super) fn sources_at<S: Into<Source>>(slot: Slot, rows: impl IntoIterator<Item = (u64, S)>) -> RelationRegistry {
    registry_of("", slot, rows.into_iter().map(|(id, s)| (id, RelationKind::Stream, s)))
}

/// A registry for worker `slot` under a fresh directory holding only `rows`, each
/// of its kind, with a store where the kind has one.
pub(super) fn tables_at<S: Into<Source>>(
    slot: Slot,
    rows: impl IntoIterator<Item = (u64, RelationKind, S)>,
) -> (RelationRegistry, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry_of(dir.path().to_str().unwrap(), slot, rows);
    (registry, dir)
}

/// [`sources_at`] the one worker of a single-worker process.
pub(super) fn sources<S: Into<Source>>(rows: impl IntoIterator<Item = (u64, S)>) -> RelationRegistry {
    sources_at(Slot::SOLO, rows)
}
