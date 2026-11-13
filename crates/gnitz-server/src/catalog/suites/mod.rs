mod alter_tests;
mod atomicity_tests;
mod bundle_precheck_tests;
mod catalog_view_tests;
mod ddl_fixture;
use ddl_fixture::make_secondary_index_name;
mod ddl_tests;
mod dir_deletion_tests;
mod engine_tests;
mod fk_tests;
#[path = "benches/hydrate.rs"]
mod hydrate_bench;
mod index_tests;
mod reopen_rebuild_tests;
mod schema_codec;
mod source_cursor_tests;
mod stream_tests;
mod sys_retraction_tests;
mod uuid_tests;
mod view_compile_tests;
#[path = "benches/view_tick.rs"]
mod view_tick_bench;
mod wide_pk_validation;

use std::fs;

use gnitz_store::relation::{relation_dir, ChildKind, IndexClaim, Relation, RelationKind, SecondaryIndex, StoreConfig};
use gnitz_wire::{
    payload_u64, CatalogColumn, Circuit, ColTabRow, NullKeys, PkColList, SysRow, TableTabRow, TypeCode, ViewTabRow,
    PK_LIST_PACKED_FLAG,
};
use gnitz_zset::repr::{Batch, BatchBuilder, ReadCursor};

use super::sys_tables::*;
use super::*;

/// Every live row of `opk`'s PK group, read as a one-key `PkSet`.
fn pk_group(engine: &mut CatalogEngine, tid: u64, opk: &[u8]) -> gnitz_store::read::ReadRows {
    let keys = gnitz_wire::PkKeys::from_keys(opk.len(), [opk]);
    read_rows(engine, tid, gnitz_wire::ReadBound::PkSet(keys))
}

/// [`pk_group`] by a narrow native key.
fn pk_group_native(engine: &mut CatalogEngine, tid: u64, key: u128) -> gnitz_store::read::ReadRows {
    let schema = engine
        .registry
        .relation(tid)
        .map(gnitz_store::relation::Relation::schema)
        .expect("a registered relation");
    pk_group(engine, tid, &opk_pk(&schema, &[key]))
}

use crate::test_support::{
    apply_ddl, bundle, circuit_batch, cmp_all, col_def, col_tab_batch, cols_of, distinct_circuit, empty_catalog,
    equi_join_circuit, fk_child_cols, fk_def, fk_def_on_delete, fk_parent, idx_tab_batch, left_join_engine, mix,
    negate_chain, net_weight, nullable_def, opk_pk, pk_payload_schema, push_and_tick, push_sys_row, read_rows, refusal,
    register_full_pk, register_identity_view, scan_all, schema_tab_batch, sum_weights, sys_batch, table_engine,
    table_tab_batch, table_tab_row, try_register_identity_view, try_register_view, two_term_join_circuit, uuid_def,
    view_blocks, view_tab_row, weights,
};

/// Live rows carrying a net NEGATIVE weight — §1 positivity says a base table
/// (system families included) must hold none. A `-1` that retracts a row nothing
/// ever inserted never cancels, so it sits here forever.
fn count_negative_records(c: ReadCursor) -> usize {
    weights(c).iter().filter(|&&w| w < 0).count()
}

/// Every stored weight under `idx_id` in IDX_TAB: empty once a `(+1, -1)` pair
/// has cancelled, `[1]` for a live index, `[-1]` for a durable ghost — which
/// [`count_records`], gating on `current_weight() > 0`, cannot see at all.
fn idx_weights_for(engine: &CatalogEngine, idx_id: u64) -> Vec<i64> {
    let mut c = engine.sys_relation(SysFamily::Index).cursor();
    let mut v = Vec::new();
    while c.is_valid() {
        if gnitz_wire::sys_leading_id(c.current_pk_bytes()) == idx_id {
            v.push(c.current_weight());
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

fn count_records(c: ReadCursor) -> usize {
    weights(c).iter().filter(|&&w| w > 0).count()
}

/// Reserve `count` SERIAL ids of `seq`: their base and the `sequences` delta it
/// applied, or the refusal's text.
fn reserve(engine: &mut CatalogEngine, seq: u64, count: u64) -> Result<(u64, Batch), String> {
    let mut laid = Vec::new();
    let base = engine
        .reserve_user_sequence(seq, count, &mut |_, b| {
            laid.push(b.clone());
            Ok(())
        })
        .map_err(refusal)?;
    assert_eq!(laid.len(), 1, "a reservation lays out one group");
    Ok((base, laid.remove(0)))
}

/// The base of a fresh database's first allocation of `count` ids, and the
/// `sequences` group that allocation lays out, from a scratch engine: a log
/// tail that creates ids carries it ahead of the creates.
fn logged_allocation(count: u64) -> (u64, Batch) {
    let (_dir, mut engine) = empty_catalog(1);
    let mut laid = Vec::new();
    let base = engine
        .reserve_catalog_ids(count, &mut |family, b| {
            assert_eq!(family, SysFamily::Sequence);
            laid.push(b.clone());
            Ok(())
        })
        .map_err(refusal)
        .unwrap();
    assert_eq!(laid.len(), 1, "an allocation lays out one group");
    (base, laid.remove(0))
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

// An arbitrary fixed 128-bit value for UUID columns; the bit pattern is
// irrelevant, only that it round-trips through a U128/UUID column.
const UUID_A: u128 = 0x0000_0000_0000_AAAA_0000_0000_0000_BBBB;

/// The OPK image of a three-column U64 compound PK (`pk_stride` = 24, wide).
/// Encoded through the production encoder, so a broken encoder fails the test
/// rather than agreeing with a second spelling of the rule here.
fn pk24(a: u64, b: u64, c: u64) -> [u8; 24] {
    let schema = pk_payload_schema(&[TypeCode::U64; 3]);
    opk_pk(&schema, &[a as u128, b as u128, c as u128])
        .try_into()
        .expect("three U64 PK columns encode to 24 bytes")
}

/// Backfill `view`, as a worker's `Backfill` does.
fn backfill(engine: &mut CatalogEngine, view: u64) {
    crate::query::backfill(engine, view).unwrap();
}

/// The child directories of relation `id` holding a manifest.
fn published_children(dir: &str, id: u64) -> Vec<String> {
    let rel = relation_dir(dir, id);
    let mut names: Vec<String> = fs::read_dir(&rel)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|name| std::path::Path::new(&format!("{rel}/{name}/manifest.bin")).exists())
        .collect();
    names.sort();
    names
}

/// A `DISTINCT` over `source` re-keyed onto its column 0: an output store and
/// one trace.
fn traced_circuit(source: u64, cols: &[CatalogColumn]) -> Circuit {
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(source, gnitz_wire::ReadBound::None);
    let keep: Vec<u32> = (1..cols.len() as u32).collect();
    let key = [(0, cols[0].def.ty.tc)];
    let rekeyed = circuit.map_reindex(scan, &key, &keep, NullKeys::Keep);
    circuit.distinct(rekeyed);
    circuit
}

/// A backfilled [`traced_circuit`] view.
fn traced_view(engine: &mut CatalogEngine, source: u64, name: &str, cols: &[CatalogColumn]) -> u64 {
    let vid = try_register_view(engine, traced_circuit(source, cols), name, cols, 0, 0).unwrap();
    backfill(engine, vid);
    vid
}

/// A stream `name` with `cols`, PK = column 0.
fn create_stream(engine: &mut CatalogEngine, name: &str, cols: &[CatalogColumn]) -> u64 {
    engine
        .create_table_with(
            name,
            cols,
            &[0],
            gnitz_wire::TableProps { stream: true, ..Default::default() },
        )
        .unwrap()
}

/// `source` holds rows no SAL replay brings back, so a view reaching it must
/// await a rebuild at reopen, and the verdict cascades to a view over that one.
///
/// Every view here gets an output manifest at the committed generation,
/// published by an ephemeral round that skips the filter
/// `flush_ephemeral_round` applies, so the `source` clause alone decides the
/// verdict. A view over the base table `public.t` is the control: it resumes.
fn assert_unreplayed_source_rebuilds_at_reopen(mut engine: CatalogEngine, source: u64, source_cols: &[CatalogColumn]) {
    let dir = &engine.registry.base_dir().to_owned();
    let cols = crate::test_support::u64_i64_cols();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let direct = register_identity_view(&mut engine, source, "v_direct", source_cols);
    let downstream = register_identity_view(&mut engine, direct, "v_downstream", source_cols);
    let over_table = register_identity_view(&mut engine, tid, "v_table", &cols);

    // The generation advanced the way a boot advances it, then every view's
    // output store published through an ephemeral round at
    // that generation — a completed checkpoint.
    let g = engine.advance_durable_generation().unwrap();
    engine.registry.checkpoint_ephemeral([], g, |_| true).unwrap();
    engine.close();

    let engine = CatalogEngine::open(dir, 1).unwrap();
    assert!(
        engine.boot.rebuild.contains(&direct),
        "a direct unreplayed source invalidates"
    );
    assert!(engine.boot.rebuild.contains(&downstream), "and the verdict cascades");
    assert!(
        !engine.boot.rebuild.contains(&over_table),
        "a checkpointed view over a base table resumes, so the rejection is about the source"
    );
}

/// An ephemeral round publishes nothing of a view `source` reaches — neither
/// its output store nor a trace — and everything of a view over the base table
/// `public.t`; the boot then rebuilds exactly what the round left out.
fn assert_round_publishes_nothing_reaching(mut engine: CatalogEngine, source: u64, source_cols: &[CatalogColumn]) {
    let dir = &engine.registry.base_dir().to_owned();
    let cols = crate::test_support::u64_i64_cols();
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();

    let direct = traced_view(&mut engine, source, "v_direct", source_cols);
    let downstream = traced_view(&mut engine, direct, "v_downstream", source_cols);
    let over_table = traced_view(&mut engine, tid, "v_table", &cols);

    let g = engine.advance_durable_generation().unwrap();
    engine.flush_ephemeral_round(g).unwrap();

    assert_eq!(published_children(dir, direct), Vec::<String>::new());
    assert_eq!(published_children(dir, downstream), Vec::<String>::new());
    let table_children = published_children(dir, over_table);
    assert!(table_children.contains(&"w0of1".to_string()), "got {table_children:?}");
    assert!(
        table_children.iter().any(|c| c.starts_with("scratch_")),
        "the trace of a view over a table is published: {table_children:?}"
    );
    engine.close();

    let engine = CatalogEngine::open(dir, 1).unwrap();
    assert!(engine.boot.rebuild.contains(&direct));
    assert!(engine.boot.rebuild.contains(&downstream));
    assert!(!engine.boot.rebuild.contains(&over_table));
}

/// `(rows, summed weight)` of relation `id`: equal iff every row stands at weight 1.
fn held(engine: &CatalogEngine, id: u64) -> (usize, i64) {
    let rel = engine.registry.relation(id).expect("a registered relation");
    (count_records(rel.cursor()), sum_weights(rel.cursor()))
}

/// Net weight of `tid`'s single index circuit.
fn index_weight(engine: &CatalogEngine, tid: u64) -> i64 {
    let entry = engine.registry.relation_or_err(tid).unwrap();
    assert_eq!(entry.indexes().len(), 1, "one index circuit");
    sum_weights(entry.indexes()[0].cursor())
}

/// Seal `source` and tick what the seal answers, as a worker's `Tick` does.
fn seal_and_tick(engine: &mut CatalogEngine, source: u64) {
    crate::query::tick(engine, source, 1).unwrap();
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

/// [`table_engine`] holding ids `0..n`, every row at weight 1, ingested as one
/// batch.
fn ingest_fixture<const N: usize>(
    cols: &[CatalogColumn],
    n: u64,
    cells: impl Fn(u64) -> [u64; N],
) -> (tempfile::TempDir, CatalogEngine, u64) {
    let (dir, mut engine, tid) = table_engine(1, cols);
    engine.registry.ingest(tid, rows(&engine, tid, 1, 0..n, cells)).unwrap();
    (dir, engine, tid)
}

/// A one-row `public` TABLE_TAB `+1` under an explicit packed `pk_col_idx`
/// word and `flags` — the shapes `create_table` cannot make.
fn table_tab_row_words(tid: u64, table_name: &str, pk_col_idx: u64, flags: gnitz_wire::TableProps) -> Batch {
    let row = TableTabRow { flags, ..table_tab_row(tid, table_name) };
    crate::test_support::forged_sys_batch(row, gnitz_wire::TableTabSlot::pk_col_idx as usize, pk_col_idx)
}

/// A COL_TAB rewrite pair on column `col_idx` of `owner_id`: `mutate` produces
/// the `+1` row from a clone of `old`, while the `-1` reproduces `old`
/// byte-for-byte — so only the guard under test can reject it.
fn col_alter_pair(owner_id: u64, col_idx: i64, old: &CatalogColumn, mutate: impl FnOnce(&mut CatalogColumn)) -> Batch {
    let mut altered = old.clone();
    mutate(&mut altered);
    sys_batch([
        (ColTabRow::of(owner_id, col_idx as u64, &old.def, old.fk), -1),
        (ColTabRow::of(owner_id, col_idx as u64, &altered.def, altered.fk), 1),
    ])
}
