mod alter_tests;
mod atomicity_tests;
mod compound_pk_smoke;
mod ddl_tests;
mod dir_deletion_tests;
mod engine_tests;
mod fk_tests;
mod index_tests;
mod reopen_rebuild_tests;
mod scan_spec_bench;
mod scan_spec_tests;
mod source_cursor_tests;
mod sys_retraction_tests;
mod uuid_tests;
mod view_preflight_tests;
mod wide_pk_validation;

use super::sys_tables::*;
use super::*;
use crate::schema::type_code;

use std::fs;

use crate::test_support::{col_def, fk_def, nullable_def, scratch_dir, uuid_def};

fn temp_dir(name: &str) -> String {
    scratch_dir("catalog", name)
}

// Arbitrary fixed 128-bit values for UUID columns; distinct from each other,
// the actual bit pattern is irrelevant. `UUID_A` is the value a parent stores
// and a valid child references, `UUID_B` a value no parent holds.
const UUID_A: u128 = 0x0000_0000_0000_AAAA_0000_0000_0000_BBBB;
const UUID_B: u128 = 0x0000_0000_0000_BEEF_0000_0000_0000_DEAD;

/// A non-nullable U64 schema column — the building block of the compound-PK
/// fixtures below.
fn u64c() -> crate::schema::SchemaColumn {
    crate::schema::SchemaColumn::new(type_code::U64, 0)
}

/// The OPK image of a three-column U64 compound PK (`pk_stride` = 24, wide).
/// Unsigned columns encode big-endian per column, which is what the wide-PK
/// write path stores and what the FK RESTRICT check decodes.
fn pk24(a: u64, b: u64, c: u64) -> [u8; 24] {
    let mut p = [0u8; 24];
    p[0..8].copy_from_slice(&a.to_be_bytes());
    p[8..16].copy_from_slice(&b.to_be_bytes());
    p[16..24].copy_from_slice(&c.to_be_bytes());
    p
}

fn count_records(table: &mut Table) -> usize {
    let mut count = 0;
    let mut c = table.open_cursor();
    while c.valid {
        if c.current_weight > 0 {
            count += 1;
        }
        c.advance();
    }
    count
}

/// A `col < lit` predicate blob over a fixed-int column — the program shape a
/// `WHERE` conjunct compiles to. Built through the client's own `ExprBuilder`,
/// so the test blobs are byte-identical to what the planner ships.
fn pred_lt_blob(col: usize, lit: i64) -> Vec<u8> {
    let mut eb = gnitz_expr::ExprBuilder::new();
    let (a, b) = (eb.load_col_int(col), eb.load_const(lit));
    let r = eb.cmp_lt(a, b);
    eb.build(r).encode()
}

/// A pure-gather projection blob: `(src_col, out_payload_slot)` CopyCols and
/// nothing else.
fn proj_blob(copies: &[(u32, u32)]) -> Vec<u8> {
    let mut eb = gnitz_expr::ExprBuilder::new();
    for &(src, out) in copies {
        eb.copy_col(src, out);
    }
    eb.build(0).encode()
}

/// A `ReadSink::Rows` spec over the whole table. Shared by the `scan_spec` tests
/// and bench; the bound-walk tests take the `identity_spec` alias.
fn rows_spec(
    predicate: Vec<u8>,
    projection: Vec<u8>,
    order: Vec<gnitz_wire::OrderKey>,
    limit_k: u64,
) -> gnitz_wire::ReadSpec {
    gnitz_wire::ReadSpec {
        bound: gnitz_wire::ReadBound::None,
        predicate,
        sink: gnitz_wire::ReadSink::Rows {
            projection,
            order,
            limit_k,
        },
    }
}

/// An empty `public.t` with `cols` (PK = column 0) in a fresh temp dir. Returns
/// the dir too, so a test that inspects or removes it does not re-derive it.
fn table_fixture(name: &str, cols: &[ColumnDef]) -> (CatalogEngine, i64, String) {
    let dir = temp_dir(name);
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let tid = engine.create_table("public.t", cols, &[0]).unwrap();
    (engine, tid, dir)
}

/// [`table_fixture`] ingested in `rounds` PK-interleaved passes over ids `0..n`,
/// every row at weight 1. `put_row` writes one row's payload columns.
///
/// One pass leaves a single sorted run; more makes the read cursor a genuine
/// N-way merge over that many runs, which is what the bench needs and a
/// bulk-drain would skip entirely.
fn ingest_fixture(
    name: &str,
    cols: &[ColumnDef],
    n: u64,
    rounds: u64,
    mut put_row: impl FnMut(&mut BatchBuilder, u64),
) -> (CatalogEngine, i64) {
    let (mut engine, tid, _dir) = table_fixture(name, cols);
    let schema = engine.get_schema_desc(tid).unwrap();
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
fn build_table_tab_row(tid: i64, raw_pk_cols: u64, table_name: &str) -> Batch {
    build_table_tab_row_flags(tid, raw_pk_cols, table_name, 0)
}

/// Like `build_table_tab_row` but with an explicit packed `flags` word (so a test
/// can build a row that passes precheck yet fails `hook_table_register`, e.g. a
/// REPLICATED table with a non-default distribution prefix). Writes through the
/// production row builder, so a fixture cannot drift from the layout the engine
/// registers tables with.
fn build_table_tab_row_flags(tid: i64, raw_pk_cols: u64, table_name: &str, flags: u64) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
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
    cols: &[ColumnDef],
    pk_cols: &[u32],
    flags: u64,
) -> i64 {
    let tid = engine.allocate_table_id();
    engine.write_column_records(tid, OWNER_KIND_TABLE, cols).unwrap();
    let batch = build_table_tab_row_flags(tid, pack_pk_cols(pk_cols), table_name, flags);
    engine.ingest_to_family(TABLE_TAB_ID, &batch).unwrap();
    tid
}

/// One circuit node for `write_circuit_chain`: opcode, source table, and the
/// optional expr/param blob.
type CircuitNode<'a> = (u64, Option<i64>, Option<&'a [u8]>);

/// Write `vid`'s circuit through the applied-delta path: one node per entry of
/// `nodes`, chained `i → i+1` on `PORT_IN`. The payload column layout follows
/// `gnitz_wire::CIRCUIT_NODES_COLS` / `CIRCUIT_EDGES_COLS`; the compound PK
/// `(view_id, sub)` is packed by `pack_view_pk`.
fn write_circuit_chain(engine: &mut CatalogEngine, vid: i64, nodes: &[CircuitNode<'_>]) {
    let mut bb = BatchBuilder::new(sys_tab_schema(CIRCUIT_NODES_TAB_ID));
    for (i, &(opcode, source, blob)) in nodes.iter().enumerate() {
        bb.begin_row(pack_view_pk(vid, i as u64), 1);
        bb.put_u64(i as u64); // node_id
        bb.put_u64(opcode);
        match source {
            Some(t) => bb.put_u64(t as u64),
            None => bb.put_null(),
        }
        match blob {
            Some(b) => bb.put_blob(b),
            None => bb.put_null(),
        }
        bb.end_row();
    }
    engine.ingest_to_family(CIRCUIT_NODES_TAB_ID, &bb.finish()).unwrap();

    let mut bb = BatchBuilder::new(sys_tab_schema(CIRCUIT_EDGES_TAB_ID));
    for src in 0..nodes.len().saturating_sub(1) as u64 {
        bb.begin_row(pack_view_pk(vid, src), 1);
        bb.put_u64(src + 1); // dst_node
        bb.put_u64(gnitz_wire::PORT_IN); // dst_port
        bb.put_u64(src); // src_node
        bb.end_row();
    }
    engine.ingest_to_family(CIRCUIT_EDGES_TAB_ID, &bb.finish()).unwrap();
}

/// The minimal identity circuit `ScanDelta(base) → Integrate`. `scan_blob` is
/// the scan node's optional expr/param blob — a bounded-scan fixture ships its
/// `RangeDescriptor` there.
fn write_identity_circuit(engine: &mut CatalogEngine, vid: i64, base_tid: i64, scan_blob: Option<&[u8]>) {
    write_circuit_chain(
        engine,
        vid,
        &[
            (gnitz_wire::OPCODE_SCAN_DELTA, Some(base_tid), scan_blob),
            (gnitz_wire::OPCODE_INTEGRATE, None, None),
        ],
    );
}

/// Append one raw VIEW_TAB row at `weight`. `sql` is stored verbatim. The bare
/// `0` pk_col_idx decodes back to a single-column PK `[0]`. A `-1` reproduces
/// exactly what a `+1` wrote, which is what the §3.3 retraction CAS compares.
fn push_view_tab_row(bb: &mut BatchBuilder, weight: i64, vid: i64, view_name: &str, sql: &str) {
    bb.begin_row(vid as u128, weight);
    bb.put_u64(PUBLIC_SCHEMA_ID as u64);
    bb.put_string(view_name);
    bb.put_string(sql);
    bb.put_u64(0); // pk_col_idx
    bb.end_row();
}

/// A single-row VIEW_TAB batch for tests that register a view via the raw
/// system-table path.
fn build_view_tab_row(vid: i64, view_name: &str, sql: &str) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, 1, vid, view_name, sql);
    bb.finish()
}

/// Register an identity view over `base_tid` through the raw system-table path,
/// returning its vid. The circuit and column records precede the VIEW_TAB row —
/// the order `hook_view_register` needs to resolve the view's sources and
/// schema.
fn register_identity_view(engine: &mut CatalogEngine, base_tid: i64, name: &str, cols: &[ColumnDef]) -> i64 {
    let vid = engine.allocate_table_id();
    write_identity_circuit(engine, vid, base_tid, None);
    engine.write_column_records(vid, OWNER_KIND_VIEW, cols).unwrap();
    let batch = build_view_tab_row(vid, name, "");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();
    vid
}

/// A COL_TAB rewrite pair on column `col_idx` of `owner_id`: `mutate` produces
/// the `+1` row from a clone of `old`, while the `-1` reproduces `old`
/// byte-for-byte — so only the guard under test can reject it.
fn col_alter_pair(
    owner_id: i64,
    owner_kind: i64,
    col_idx: i64,
    old: &ColumnDef,
    mutate: impl FnOnce(&mut ColumnDef),
) -> Batch {
    let mut altered = old.clone();
    mutate(&mut altered);
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    push_col_tab_row(&mut bb, owner_id, owner_kind, col_idx, old, -1);
    push_col_tab_row(&mut bb, owner_id, owner_kind, col_idx, &altered, 1);
    bb.finish()
}
