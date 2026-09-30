mod alter_tests;
mod atomicity_tests;
mod compound_pk_smoke;
mod ddl_fixture;
use ddl_fixture::make_secondary_index_name;
use gnitz_expr::SchemaFacts;
mod ddl_tests;
mod dir_deletion_tests;
mod echo_fold_bench;
mod engine_tests;
mod fk_tests;
mod hydrate_bench;
mod index_tests;
mod ingest_unticked_bench;
mod reopen_rebuild_tests;
mod scan_spec_bench;
mod schema_codec;
mod source_cursor_tests;
mod stream_tests;
mod sys_retraction_tests;
mod uuid_tests;
mod view_preflight_tests;
mod wide_pk_validation;

use super::sys_tables::*;
use super::*;
use gnitz_store::relation::SecondaryIndex;
use gnitz_store::schema::Slot;
use gnitz_wire::{pack_pk_cols, TypeCode, PK_LIST_PACKED_FLAG};

use std::fs;

/// Every row of `tid` under `bound`, in the relation's own layout.
fn read_rows(
    engine: &mut CatalogEngine,
    tid: u64,
    bound: gnitz_wire::ReadBound,
) -> std::rc::Rc<gnitz_store::storage::Batch> {
    let schema = engine
        .registry
        .relation(tid)
        .map(gnitz_store::relation::Relation::schema)
        .expect("a registered relation");
    engine
        .scan_spec(tid, gnitz_wire::ReadSpec::all_rows(bound), schema.layout_digest())
        .expect("a read")
}

/// Every row of `tid`.
fn scan_all(engine: &mut CatalogEngine, tid: u64) -> std::rc::Rc<gnitz_store::storage::Batch> {
    read_rows(engine, tid, gnitz_wire::ReadBound::None)
}

/// Every live row of `opk`'s PK group, read as a one-key `PkSet`.
fn pk_group(engine: &mut CatalogEngine, tid: u64, opk: &[u8]) -> std::rc::Rc<gnitz_store::storage::Batch> {
    let keys = gnitz_wire::PkKeys::from_keys(opk.len(), [opk]);
    read_rows(engine, tid, gnitz_wire::ReadBound::PkSet(keys))
}

/// [`pk_group`] by a narrow native key.
fn pk_group_native(engine: &mut CatalogEngine, tid: u64, key: u128) -> std::rc::Rc<gnitz_store::storage::Batch> {
    let schema = engine
        .registry
        .relation(tid)
        .map(gnitz_store::relation::Relation::schema)
        .expect("a registered relation");
    let opk = schema.opk_key(&key.to_le_bytes());
    pk_group(engine, tid, opk.pk_bytes())
}

use crate::test_support::{
    col_def, col_tab_batch, equi_join_circuit, fk_def, idx_tab_batch, negate_chain, nullable_def, opk_pk,
    pk_payload_schema, push_table_tab_row, push_view_tab_row, register_identity_view, schema_tab_batch, scratch_dir,
    seek_by_index, seek_by_index_range, sum_weights, try_register_identity_view, try_register_view, uuid_def,
    write_circuit, write_identity_circuit, LocalDrive,
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
    let mut bb = BatchBuilder::new(*SysFamily::View.schema());
    push_view_tab_row(&mut bb, 1, vid, view_name, 0, 0, 0);
    bb.finish()
}

fn temp_dir(name: &str) -> String {
    scratch_dir("catalog", name)
}

// An arbitrary fixed 128-bit value for UUID columns; the bit pattern is
// irrelevant, only that it round-trips through a U128/UUID column.
const UUID_A: u128 = 0x0000_0000_0000_AAAA_0000_0000_0000_BBBB;

/// A non-nullable U64 schema column — the building block of the compound-PK
/// fixtures below.
fn u64c() -> gnitz_store::schema::SchemaColumn {
    gnitz_store::schema::SchemaColumn::new(TypeCode::U64, false)
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

/// A `col <op> lit` predicate blob over a fixed-int column — the program shape a
/// `WHERE` conjunct compiles to. Built through the client's own `ExprBuilder`,
/// so the test blobs are byte-identical to what the planner ships.
fn pred_cmp_blob(op: gnitz_expr::CmpOp, col: usize, lit: i64) -> Vec<u8> {
    let mut eb = gnitz_expr::ExprBuilder::new();
    let (a, b) = (
        eb.emit(gnitz_expr::LogicalInstr::LoadColInt { col: col as u32 }),
        eb.emit(gnitz_expr::LogicalInstr::LoadConst { val: lit, unsigned: false }),
    );
    let r = eb.emit(gnitz_expr::LogicalInstr::Cmp { op, a, b });
    eb.build(vec![gnitz_expr::Sink::Reg(r)])
        .expect("a well-formed program")
        .to_blob_bytes()
}

/// A pure-gather projection program: `srcs[i]` copied into output payload slot
/// `i`, and nothing else.
fn proj_blob(srcs: &[u32]) -> Vec<u8> {
    gnitz_expr::LogicalProgram::copy_cols(srcs).to_blob_bytes()
}

/// `program` as a sink map declaring `reply`'s payload columns — the slots a
/// mapped rows reply carries after the PK it inherits.
fn map_of(program: Vec<u8>, reply: &gnitz_store::schema::SchemaDescriptor) -> Option<gnitz_wire::ComputeMap> {
    let out_cols = reply
        .payload_columns()
        .map(|(_, c)| (c.type_code, c.nullable))
        .collect();
    Some(gnitz_wire::ComputeMap { program, out_cols })
}

/// A rows-sink spec over the whole table.
fn rows_spec(
    predicate: Vec<u8>,
    map: Option<gnitz_wire::ComputeMap>,
    order: Vec<gnitz_wire::OrderKey>,
    limit_k: u64,
) -> gnitz_wire::ReadSpec {
    gnitz_wire::ReadSpec {
        bound: gnitz_wire::ReadBound::None,
        predicate,
        sink: gnitz_wire::ReadSink {
            map,
            kind: gnitz_wire::SinkKind::Rows { order, limit_k },
        },
    }
}

/// An empty `public.t` with `cols` (PK = column 0) in a fresh temp dir. Returns
/// the dir too, so a test that inspects or removes it does not re-derive it.
fn table_fixture(name: &str, cols: &[CatalogColumn]) -> (CatalogEngine, u64, String) {
    let dir = temp_dir(name);
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let tid = engine.create_table("public.t", cols, &[0]).unwrap();
    (engine, tid, dir)
}

/// Compile `view` and backfill it from each of `sources` in turn.
fn backfill(engine: &mut CatalogEngine, view: u64, sources: &[u64]) {
    engine.dag.open_plan(&engine.registry, view).unwrap();
    let chunk_rows = engine.registry.scan_chunk_rows();
    for &source in sources {
        let mut cursor = engine.open_source_cursor(view, source).unwrap();
        while let Some(chunk) = cursor.drain_chunk(chunk_rows) {
            let what = crate::query::Drive::Backfill { view, source };
            crate::query::drive(&mut LocalDrive(engine), what, chunk).unwrap();
        }
    }
    engine.dag.finish_backfill(&mut engine.registry, view).unwrap();
}

/// [`table_fixture`] ingested in `rounds` PK-interleaved passes over ids `0..n`,
/// every row at weight 1. `put_row` writes one row's payload columns.
///
/// One pass leaves a single sorted run; more makes the read cursor a genuine
/// N-way merge over that many runs, which is what the bench needs and a
/// bulk-drain would skip entirely.
fn ingest_fixture(
    name: &str,
    cols: &[CatalogColumn],
    n: u64,
    rounds: u64,
    mut put_row: impl FnMut(&mut BatchBuilder, u64),
) -> (CatalogEngine, u64) {
    let (mut engine, tid, _dir) = table_fixture(name, cols);
    let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
    for round in 0..rounds {
        let mut bb = BatchBuilder::new(schema);
        let mut id = round;
        while id < n {
            bb.begin_row(id as u128, 1);
            put_row(&mut bb, id);
            bb.end_row();
            id += rounds;
        }
        engine.ingest_to_family(tid, &bb.finish()).unwrap();
    }
    (engine, tid)
}

/// Build a raw TABLE_TAB row for tests that drive the catalog applier or
/// hook layer directly (bypassing `create_table`).
fn build_table_tab_row(tid: u64, raw_pk_cols: u64, table_name: &str) -> Batch {
    build_table_tab_row_flags(tid, raw_pk_cols, table_name, 0)
}

/// Like `build_table_tab_row` but with an explicit packed `flags` word — the
/// routing shapes `create_table` cannot make. Writes through the production row
/// builder, so a fixture cannot drift from the layout the engine registers
/// tables with.
fn build_table_tab_row_flags(tid: u64, raw_pk_cols: u64, table_name: &str, flags: u64) -> Batch {
    let mut bb = BatchBuilder::new(*SysFamily::Table.schema());
    push_table_tab_row(&mut bb, tid, PUBLIC_SCHEMA_ID, table_name, raw_pk_cols, flags, 1);
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
    let batch = build_table_tab_row_flags(tid, pack_pk_cols(pk_cols), table_name, flags);
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
    let mut bb = BatchBuilder::new(*SysFamily::Column.schema());
    old.write_col_tab_row(&mut bb, owner_id, col_idx as usize, -1);
    altered.write_col_tab_row(&mut bb, owner_id, col_idx as usize, 1);
    bb.finish()
}
