//! The test helpers only this crate uses: they name `Table`, the registry and the
//! store's own files.

use std::num::NonZeroU64;
use std::path::Path;

use crate::relation::{IndexClaim, RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use crate::storage::{RecoverySource, StoreBudgets, Table, DEFAULT_RAM_TIER_BYTES};
use gnitz_expr::LogicalProgram;
use gnitz_wire::{ComputeMap, OrderKey, ReadBound, ReadSink, ReadSpec, RowsCut, SinkKind};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{Placement, SchemaDescriptor, Slot};

/// A table under `dir` at a RAM tier of `ram_tier_bytes`, its shards unbounded.
pub(crate) fn new_table(
    dir: impl AsRef<Path>,
    schema: SchemaDescriptor,
    recovery: RecoverySource,
    ram_tier_bytes: usize,
) -> Table {
    Table::new(
        dir.as_ref().to_str().unwrap(),
        schema,
        recovery,
        StoreBudgets::new(ram_tier_bytes),
    )
    .unwrap()
}

/// A rederived table under `dir` at the default budgets — nothing a test puts
/// here spills, since that needs the whole RAM tier. For a test that just
/// needs somewhere to put rows.
pub(crate) fn scratch_table(dir: impl AsRef<Path>, schema: SchemaDescriptor) -> Table {
    new_table(
        dir,
        schema,
        RecoverySource::Rederive { resume_at: None },
        DEFAULT_RAM_TIER_BYTES,
    )
}

/// Flip the low bit of `path`'s last byte with a `pwrite`, which a live mapping
/// of the file sees; a truncating rewrite would fault that mapping instead.
pub(crate) fn flip_last_byte_in_place(path: impl AsRef<Path>) {
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    let last = file.metadata().unwrap().len() - 1;
    let mut byte = [0u8; 1];
    file.read_exact_at(&mut byte, last).unwrap();
    file.write_all_at(&[byte[0] ^ 0x01], last).unwrap();
}

/// The relation a [`relation_fixture`] holds.
pub(crate) const TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;

/// A registry over a directory of its own, removed when the fixture drops.
pub(crate) struct RelationFixture {
    registry: RelationRegistry,
    _dir: tempfile::TempDir,
}

impl std::ops::Deref for RelationFixture {
    type Target = RelationRegistry;
    fn deref(&self) -> &RelationRegistry {
        &self.registry
    }
}

impl std::ops::DerefMut for RelationFixture {
    fn deref_mut(&mut self) -> &mut RelationRegistry {
        &mut self.registry
    }
}

/// A registry holding [`TID`] as a `kind` relation over `schema`, with an index
/// on each column of `indexed`, holding `rounds`, one ingest each.
pub(crate) fn relation_fixture(
    kind: RelationKind,
    schema: SchemaDescriptor,
    indexed: &[u32],
    rounds: impl IntoIterator<Item = Batch>,
) -> RelationFixture {
    let dir = tempfile::tempdir().unwrap();
    let mut registry = RelationRegistry::new(dir.path().to_str().unwrap(), Slot::SOLO, StoreConfig::default());
    registry
        .register(RelationSpec {
            id: TID,
            kind,
            schema,
            placement: Placement::full_pk(&schema),
        })
        .unwrap();
    for (id, &col) in (TID + 1..).zip(indexed) {
        registry
            .add_index(
                TID,
                IndexClaim::Index { id, unique: false },
                gnitz_wire::PkColList::from_slice(&[col]),
            )
            .unwrap();
    }
    for rows in rounds {
        registry.ingest(TID, rows).unwrap();
    }
    RelationFixture { registry, _dir: dir }
}

/// `v`'s key image in an I64 column.
pub(crate) fn img(v: i64) -> u128 {
    gnitz_wire::key_image(gnitz_wire::TypeCode::I64, v as u64 as u128)
}

/// A rows sink's cut at `k > 0`.
pub(crate) fn cut(k: u64, order: Vec<OrderKey>) -> Option<RowsCut> {
    Some(RowsCut { k: NonZeroU64::new(k).unwrap(), order })
}

/// A rows spec with no bound and no predicate.
pub(crate) fn rows_spec(map: Option<ComputeMap>, cut: Option<RowsCut>) -> ReadSpec {
    ReadSpec {
        bound: ReadBound::None,
        predicate: Vec::new(),
        sink: ReadSink { map, kind: SinkKind::Rows { cut } },
    }
}

/// `program` as a sink map declaring `reply`'s payload columns.
pub(crate) fn map_of(program: LogicalProgram, reply: &SchemaDescriptor) -> Option<ComputeMap> {
    let out_cols = reply
        .payload_columns()
        .map(|(_, c)| (c.type_code, c.nullable))
        .collect();
    Some(ComputeMap {
        program: program.to_blob_bytes(),
        out_cols,
    })
}
