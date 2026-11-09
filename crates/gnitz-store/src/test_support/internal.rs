//! The test helpers only this crate uses: they name `Table`, the registry and the
//! store's own files.

use std::num::NonZeroU64;
use std::path::Path;
use std::rc::Rc;

use crate::relation::{IndexClaim, RelationKind, RelationRegistry, RelationSpec, SecondaryIndex, StoreConfig};
use crate::storage::{Cut, RecoverySource, Table, DEFAULT_RAM_TIER_BYTES};
use gnitz_expr::LogicalProgram;
use gnitz_wire::{
    ComputeMap, KeyRange, OrderKey, PkColList, ReadBound, ReadSink, ReadSpec, RowsCut, SinkKind, ViewBudget,
};
use gnitz_zset::algebra::{Placement, Slot};
use gnitz_zset::repr::{Batch, BatchBuilder, MappedShard, Run};
use gnitz_zset::schema::SchemaDescriptor;

/// Every shard `t` holds, the pending ones included, in read order.
pub(crate) fn table_shards(t: &Table) -> Vec<Rc<MappedShard>> {
    t.runs(Cut::Now)
        .filter_map(|r| match r {
            Run::Shard(s) => Some(s),
            Run::Mem(_) => None,
        })
        .collect()
}

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
        ram_tier_bytes,
        None,
        false,
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

/// Relation `id`, a `kind` over `schema`, placed by its full PK.
pub(crate) fn spec(id: u64, kind: RelationKind, schema: SchemaDescriptor) -> RelationSpec {
    RelationSpec {
        id,
        kind,
        placement: Placement::full_pk(&schema),
        pk_repeats: false,
        compress: false,
    }
}

/// A one-worker registry over `base`.
pub(crate) fn solo(base: impl AsRef<Path>) -> RelationRegistry {
    RelationRegistry::new(base.as_ref().to_str().unwrap(), Slot::SOLO, StoreConfig::default())
}

/// A store config whose RAM tier spills after a few rows, so a handful of
/// rounds puts rows in several shards.
pub(crate) fn spilling() -> StoreConfig {
    StoreConfig {
        ram_tier_bytes: 256,
        ..StoreConfig::default()
    }
}

/// A fed view, its feed retaining `delta_bytes`.
pub(crate) fn fed_view(delta_bytes: u64) -> RelationKind {
    RelationKind::View(ViewBudget::Fed {
        delta_bytes: NonZeroU64::new(delta_bytes).unwrap(),
    })
}

/// A registry holding [`TID`] as a `kind` relation over `schema`, with an index
/// on each column list of `indexed`, holding `rounds`, one ingest each.
pub(crate) fn relation_fixture(
    kind: RelationKind,
    schema: SchemaDescriptor,
    indexed: &[&[u32]],
    rounds: impl IntoIterator<Item = Batch>,
) -> RelationFixture {
    relation_fixture_with(
        StoreConfig::default(),
        Slot::SOLO,
        spec(TID, kind, schema),
        schema,
        indexed,
        rounds,
    )
}

/// [`relation_fixture`] under `config`, at `slot`, registered as `spec`.
pub(crate) fn relation_fixture_with(
    config: StoreConfig,
    slot: Slot,
    spec: RelationSpec,
    schema: SchemaDescriptor,
    indexed: &[&[u32]],
    rounds: impl IntoIterator<Item = Batch>,
) -> RelationFixture {
    let dir = tempfile::tempdir().unwrap();
    let mut registry = RelationRegistry::new(dir.path().to_str().unwrap(), slot, config);
    let indexes: Vec<_> = (TID + 1..)
        .zip(indexed)
        .map(|(id, &cols)| (IndexClaim::Index { id, unique: false }, PkColList::from_slice(cols)))
        .collect();
    registry.register_at(spec, schema, &indexes, None).unwrap();
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

/// `lo <= col < hi`, as a wire predicate.
pub(crate) fn between(col: u32, lo: i64, hi: i64) -> Vec<u8> {
    use gnitz_expr::CmpOp::{Ge, Lt};
    crate::test_support::cmp_all(&[(Ge, col, lo), (Lt, col, hi)]).to_blob_bytes()
}

/// The bound `first..=last` over the one column `col`'s key images.
pub(crate) fn col_range(col: u32, first: u128, last: u128) -> ReadBound {
    ReadBound::Range(KeyRange::new(PkColList::from_slice(&[col]), &[], first, last))
}

/// One cut key: column `col`, NULLs last.
pub(crate) fn order_by(col: u16, desc: bool) -> Vec<OrderKey> {
    vec![OrderKey { col, desc, nulls_first: false }]
}

/// A batch over `schema`, whose PK is one U64 column, of a row per `(id, weight)`,
/// `put_row` writing row `id`'s payload.
pub(crate) fn id_rows(
    schema: &SchemaDescriptor,
    rows: impl IntoIterator<Item = (u64, i64)>,
    mut put_row: impl FnMut(&mut BatchBuilder, u64),
) -> Batch {
    let mut bb = BatchBuilder::new(schema);
    for (id, w) in rows {
        bb.begin_row(id as u128, w);
        put_row(&mut bb, id);
        bb.end_row();
    }
    bb.finish()
}

/// Assert `ix` holds exactly the entries `rows` project to, weights included.
pub(crate) fn assert_index_holds(ix: &SecondaryIndex, rows: &Batch, what: impl std::fmt::Display) {
    let got = ix.cursor().materialize();
    let schema = *got.schema();
    let want = gnitz_zset::algebra::index_entries(rows, ix.key_spec(), &schema);
    assert_eq!(
        gnitz_zset_testkit::zset_of(&got, &schema),
        gnitz_zset_testkit::zset_of(&want, &schema),
        "{what}"
    );
}

/// `program` as a sink map declaring `reply`'s payload columns.
pub(crate) fn map_of(program: LogicalProgram, reply: &SchemaDescriptor) -> Option<ComputeMap> {
    Some(
        program.to_map(
            reply
                .payload_columns()
                .map(|(_, c)| (c.type_code, c.nullable))
                .collect(),
        ),
    )
}

/// `id | c0 = id % 512 | c1 = id % 2 | four I64 | two STRING`, the out-of-line
/// strings on every other row, as a `kind` relation whose ids `0..rows` sit in
/// `shards` shards, interleaved, and none in the RAM tier.
pub(crate) fn wide_on_disk(kind: RelationKind, rows: u64, shards: usize) -> RelationFixture {
    use gnitz_wire::TypeCode::{String as S, I64};
    use gnitz_zset::schema::SchemaColumn;
    let cols = [I64, I64, I64, I64, I64, I64, S, S].map(|tc| SchemaColumn::new(tc, false));
    let schema = gnitz_zset_testkit::u64_pk_schema(&cols);
    let mut r = relation_fixture(kind, schema, &[], []);
    for at in 0..shards as u64 {
        let batch = id_rows(&schema, (at..rows).step_by(shards).map(|id| (id, 1)), |bb, id| {
            bb.put_u64(id % 512);
            bb.put_u64(id % 2);
            for k in 0..4 {
                bb.put_u64(id.wrapping_mul(2_654_435_761 + k));
            }
            for k in 0..2 {
                match id % 2 == 0 {
                    true => bb.put_string(&format!("an out-of-line string value {k} of row {id}")),
                    false => bb.put_string("short"),
                }
            }
        });
        r.ingest(TID, batch).unwrap();
        match kind.is_base_table() {
            true => r.checkpoint_base().unwrap(),
            false => r.checkpoint_ephemeral([], at + 1, |_| true).unwrap(),
        }
    }
    let table = r.relation(TID).unwrap().table();
    assert_eq!(
        (table_shards(table).len(), table.runs(Cut::Now).count()),
        (shards, shards)
    );
    r
}
