mod alter_tests;
mod atomicity_tests;
mod bundle_precheck_tests;
mod compound_pk_smoke;
mod ddl_fixture;
use ddl_fixture::make_secondary_index_name;
mod ddl_tests;
mod dir_deletion_tests;
mod engine_tests;
mod fk_tests;
#[path = "benches/hydrate.rs"]
mod hydrate_bench;
mod index_tests;
#[path = "benches/ingest_unticked.rs"]
mod ingest_unticked_bench;
mod reopen_rebuild_tests;
mod schema_codec;
mod source_cursor_tests;
mod stream_tests;
mod sys_retraction_tests;
mod uuid_tests;
mod view_preflight_tests;
#[path = "benches/view_tick.rs"]
mod view_tick_bench;
mod wide_pk_validation;

use super::sys_tables::*;
use super::*;
use gnitz_store::relation::{relation_dir, relations_dir, ChildAddr, ChildKind, SecondaryIndex};
use gnitz_wire::{PkColList, TypeCode, PK_LIST_PACKED_FLAG};
use gnitz_zset::schema::Slot;

use std::fs;

/// Every live row of `opk`'s PK group, read as a one-key `PkSet`.
fn pk_group(engine: &mut CatalogEngine, tid: u64, opk: &[u8]) -> std::rc::Rc<gnitz_zset::repr::Batch> {
    let keys = gnitz_wire::PkKeys::from_keys(opk.len(), [opk]);
    read_rows(engine, tid, gnitz_wire::ReadBound::PkSet(keys))
}

/// [`pk_group`] by a narrow native key.
fn pk_group_native(engine: &mut CatalogEngine, tid: u64, key: u128) -> std::rc::Rc<gnitz_zset::repr::Batch> {
    let schema = engine
        .registry
        .relation(tid)
        .map(gnitz_store::relation::Relation::schema)
        .expect("a registered relation");
    pk_group(engine, tid, &opk_pk(&schema, &[key]))
}

use crate::test_support::{
    cmp_const, col_def, col_tab_batch, cols_of, distinct_circuit, equi_join_circuit, fk_def, idx_tab_batch,
    negate_chain, net_weight, nullable_def, opk_pk, pk_payload_schema, push_sys_row, push_view_tab_row, read_rows,
    register_identity_view, scan_all, schema_tab_batch, scratch_dir, seek_by_index, seek_by_index_range, sum_weights,
    table_tab_batch, try_register_identity_view, try_register_view, two_term_join_circuit, uuid_def, write_circuit,
    write_identity_circuit, LocalDrive,
};

/// Live rows carrying a net NEGATIVE weight — §1 positivity says a base table
/// (system families included) must hold none. A `-1` that retracts a row nothing
/// ever inserted never cancels, so it sits here forever.
fn count_negative_records(mut c: ReadCursor) -> usize {
    let mut count = 0;
    while c.valid {
        if c.current_weight < 0 {
            count += 1;
        }
        c.advance();
    }
    count
}

/// Every stored weight under `idx_id` in IDX_TAB: empty once a `(+1, -1)` pair
/// has cancelled, `[1]` for a live index, `[-1]` for a durable ghost — which
/// [`count_records`], gating on `current_weight > 0`, cannot see at all.
fn idx_weights_for(engine: &CatalogEngine, idx_id: u64) -> Vec<i64> {
    let mut c = engine.sys_relation(SysFamily::Index).cursor();
    let mut v = Vec::new();
    while c.valid {
        if c.current_key_narrow() as u64 == idx_id {
            v.push(c.current_weight);
        }
        c.advance();
    }
    v
}

/// Live rows of `family` whose leading key column is `leading` — for a table's
/// TABLE_TAB row, 1 after a clean rename and 2+ for a persistent ghost.
fn rows_under(engine: &CatalogEngine, family: SysFamily, leading: u64) -> usize {
    let mut count = 0;
    engine.for_each_row_under(family, leading, |_| count += 1);
    count
}

fn count_records(mut c: ReadCursor) -> usize {
    let mut count = 0;
    while c.valid {
        if c.current_weight > 0 {
            count += 1;
        }
        c.advance();
    }
    count
}

/// A single-row unbounded-VIEW_TAB batch for tests that register a view via the
/// raw system-table path.
fn build_view_tab_row(vid: u64, view_name: &str) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, 1, vid, view_name, 0, 0, 0);
    bb.finish()
}

/// A `DDL_TXN` bundle of `blocks`, one per family.
fn bundle(blocks: impl IntoIterator<Item = (SysFamily, Batch)>) -> [Option<Batch>; SysFamily::COUNT] {
    let mut families: [Option<Batch>; SysFamily::COUNT] = std::array::from_fn(|_| None);
    for (family, batch) in blocks {
        assert!(families[family.index()].replace(batch).is_none());
    }
    families
}

/// `(live rows, net-negative rows)` of every system family.
fn sys_row_counts(engine: &CatalogEngine) -> Vec<(usize, usize)> {
    SysFamily::ALL
        .iter()
        .map(|&f| {
            (
                count_records(engine.sys_relation(f).cursor()),
                count_negative_records(engine.sys_relation(f).cursor()),
            )
        })
        .collect()
}

fn temp_dir(name: &str) -> String {
    scratch_dir("catalog", name)
}

// An arbitrary fixed 128-bit value for UUID columns; the bit pattern is
// irrelevant, only that it round-trips through a U128/UUID column.
const UUID_A: u128 = 0x0000_0000_0000_AAAA_0000_0000_0000_BBBB;

/// A non-nullable U64 schema column — the building block of the compound-PK
/// fixtures below.
fn u64c() -> gnitz_zset::schema::SchemaColumn {
    gnitz_zset::schema::SchemaColumn::new(TypeCode::U64, false)
}

/// The OPK image of a three-column U64 compound PK (`pk_stride` = 24, wide).
/// Encoded through the production encoder, so a broken encoder fails the test
/// rather than agreeing with a second spelling of the rule here.
fn pk24(a: u64, b: u64, c: u64) -> [u8; 24] {
    let schema = pk_payload_schema(&[TypeCode::U64; 3]);
    opk_pk(&schema, &[a as u128, b as u128, c as u128])
        .try_into()
        .expect("three U64 PK columns encode to 24 bytes")
}

/// An empty `public.t` with `cols` (PK = column 0) in a fresh temp dir. Returns
/// the dir too, so a test that inspects or removes it does not re-derive it.
fn table_fixture(name: &str, cols: &[CatalogColumn]) -> (CatalogEngine, u64, String) {
    let dir = temp_dir(name);
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let tid = engine.create_table("public.t", cols, &[0]).unwrap();
    (engine, tid, dir)
}

/// Backfill `view` from each of `sources` in turn, each ending with the pad
/// chunk a drained worker drives.
fn backfill(engine: &mut CatalogEngine, view: u64, sources: &[u64]) {
    let chunk_rows = engine.registry.scan_chunk_rows();
    for &source in sources {
        let mut cursor = engine.open_source_cursor(view, source).unwrap();
        loop {
            let chunk = cursor.drain_chunk(chunk_rows);
            let drained = chunk.is_none();
            let what = crate::query::Drive::Backfill { view, source };
            crate::query::drive(&mut LocalDrive(engine), what, chunk).unwrap();
            if drained {
                break;
            }
        }
        engine.dag.finish_backfill(&mut engine.registry, view, source).unwrap();
    }
}

/// `ids` as rows of `tid` at `weight`, in the order given; `cells(id)` is one
/// row's payload columns.
fn rows<const N: usize>(
    engine: &CatalogEngine,
    tid: u64,
    weight: i64,
    ids: impl IntoIterator<Item = u64>,
    cells: impl Fn(u64) -> [u64; N],
) -> Batch {
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
    let mut bb = BatchBuilder::new(&schema);
    for id in ids {
        bb.begin_row(id as u128, weight);
        cells(id).into_iter().for_each(|c| bb.put_u64(c));
        bb.end_row();
    }
    bb.finish()
}

/// [`table_fixture`] holding ids `0..n`, every row at weight 1, ingested as one
/// batch.
fn ingest_fixture<const N: usize>(
    name: &str,
    cols: &[CatalogColumn],
    n: u64,
    cells: impl Fn(u64) -> [u64; N],
) -> (CatalogEngine, u64) {
    let (mut engine, tid, _dir) = table_fixture(name, cols);
    engine.registry.ingest(tid, rows(&engine, tid, 1, 0..n, cells)).unwrap();
    (engine, tid)
}

/// A value spread over the whole `u64` range, a bijection of `id`.
fn scramble(id: u64) -> u64 {
    id.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Drop `engine` unflushed and remove its data directory.
fn discard(engine: CatalogEngine) {
    let dir = engine.registry.base_dir().to_owned();
    drop(engine);
    let _ = fs::remove_dir_all(dir);
}

/// A one-row `public` TABLE_TAB `+1` under explicit packed `pk_col_idx` and
/// `flags` words — the shapes `create_table` cannot make.
fn table_tab_row_words(tid: u64, table_name: &str, pk_col_idx: u64, flags: u64) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    let sink: &mut dyn gnitz_wire::sys_rows::SysRowSink = &mut bb;
    sink.begin_row(&[tid as u128], 1);
    sink.put_u64(PUBLIC_SCHEMA_ID);
    sink.put_string(table_name);
    sink.put_u64(pk_col_idx);
    sink.put_u64(flags);
    sink.end_row();
    bb.finish()
}

/// Register a base table through the DDL hooks with an explicit packed `flags`
/// word — the routing shapes `create_table` cannot make (it always builds a
/// full-PK-distributed, non-replicated table): REPLICATED and
/// CLUSTER BY (a distribution prefix shorter than the PK).
fn create_flagged_table(
    engine: &mut CatalogEngine,
    table_name: &str,
    cols: &[CatalogColumn],
    pk_cols: &[u32],
    flags: u64,
) -> u64 {
    let tid = engine.allocate_ids(1).unwrap();
    engine.write_column_records(tid, cols).unwrap();
    let batch = table_tab_row_words(tid, table_name, PkColList::from_slice(pk_cols).pack(), flags);
    engine.ingest_to_family(gnitz_wire::TABLE_TAB, &batch).unwrap();
    tid
}

/// The three non-default `TABLE_TAB.flags` words the fixtures use, so a test reads
/// as the property under test rather than as a bit pattern.
fn replicated_flags() -> u64 {
    gnitz_wire::TableProps {
        distribution: gnitz_wire::TableDistribution::Replicated,
        ..Default::default()
    }
    .pack()
}

/// CLUSTER BY the PK's leading `k` columns.
fn clustered_flags(k: usize) -> u64 {
    gnitz_wire::TableProps {
        distribution: gnitz_wire::TableDistribution::Keyed { prefix_len: k as u8 },
        ..Default::default()
    }
    .pack()
}

fn stream_flags() -> u64 {
    gnitz_wire::TableProps { stream: true, ..Default::default() }.pack()
}

/// A COL_TAB rewrite pair on column `col_idx` of `owner_id`: `mutate` produces
/// the `+1` row from a clone of `old`, while the `-1` reproduces `old`
/// byte-for-byte — so only the guard under test can reject it.
fn col_alter_pair(owner_id: u64, col_idx: i64, old: &CatalogColumn, mutate: impl FnOnce(&mut CatalogColumn)) -> Batch {
    let mut altered = old.clone();
    mutate(&mut altered);
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    old.write_col_tab_row(&mut bb, owner_id, col_idx as usize, -1);
    altered.write_col_tab_row(&mut bb, owner_id, col_idx as usize, 1);
    bb.finish()
}
