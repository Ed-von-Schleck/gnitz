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
mod uuid_tests;
mod wide_pk_validation;

use super::sys_tables::*;
use super::*;
use crate::schema::type_code;

use std::fs;

fn temp_dir(name: &str) -> String {
    crate::foundation::posix_io::raise_fd_limit_for_tests();
    // Namespace the fixed path by user: /tmp is shared and sticky, so a dir
    // left by a DIFFERENT user (mode 0755, owner-only-writable) would occupy
    // the bare `gnitz_catalog_test_{name}` path forever — the start-of-test
    // `remove_dir_all` cannot delete it (sticky bit) and `CatalogEngine::open`
    // then fails EACCES creating subdirs under it. Per-user namespacing keeps
    // each user on a path they own (so the remove-at-start self-cleans the
    // previous same-user run — no turd accumulation), while never colliding
    // across users. Falls back to the pid if $USER is unset.
    let owner = std::env::var("USER").unwrap_or_else(|_| std::process::id().to_string());
    let path = std::env::temp_dir()
        .join(format!("gnitz_catalog_test_{owner}_{name}"))
        .to_str()
        .unwrap()
        .to_owned();
    let _ = fs::remove_dir_all(&path);
    path
}

/// A plain non-nullable, non-FK, non-hidden column of the given type.
fn col_def(name: &str, type_code: u8) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code,
        is_nullable: false,
        fk_table_id: 0,
        fk_col_idx: 0,
        is_hidden: false,
    }
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
    let mut eb = gnitz_core::ExprBuilder::new();
    let (a, b) = (eb.load_col_int(col), eb.load_const(lit));
    let r = eb.cmp_lt(a, b);
    eb.build(r).encode()
}

/// A pure-gather projection blob: `(src_col, out_payload_slot)` CopyCols and
/// nothing else.
fn proj_blob(copies: &[(u32, u32)]) -> Vec<u8> {
    let mut eb = gnitz_core::ExprBuilder::new();
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
    let mut engine = CatalogEngine::open(&dir).unwrap();
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
    let schema = engine.get_schema(tid).unwrap();
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
/// hook layer directly (bypassing `create_table`). `dir` only feeds the
/// stored directory string — the path is never actually created on disk.
fn build_table_tab_row(dir: &str, tid: i64, raw_pk_cols: u64, table_name: &str) -> Batch {
    build_table_tab_row_flags(dir, tid, raw_pk_cols, table_name, 0)
}

/// Like `build_table_tab_row` but with an explicit packed `flags` word (so a test
/// can build a row that passes precheck yet fails `hook_table_register`, e.g. a
/// REPLICATED table with a non-default distribution prefix).
fn build_table_tab_row_flags(dir: &str, tid: i64, raw_pk_cols: u64, table_name: &str, flags: u64) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    bb.begin_row(tid as u128, 1);
    bb.put_u64(PUBLIC_SCHEMA_ID as u64);
    bb.put_string(table_name);
    bb.put_string(&format!("{dir}/public/{table_name}"));
    bb.put_u64(raw_pk_cols);
    bb.put_u64(0); // created_lsn
    bb.put_u64(flags);
    bb.end_row();
    bb.finish()
}

/// Register a base table through the DDL hooks with an explicit packed `flags`
/// word — the routing shapes `create_table` cannot make (it always builds a
/// full-PK-hashed, non-replicated table): REPLICATED (an unhashed store) and
/// CLUSTER BY (a distribution prefix shorter than the PK).
fn create_flagged_table(
    engine: &mut CatalogEngine,
    dir: &str,
    table_name: &str,
    cols: &[ColumnDef],
    pk_cols: &[u32],
    flags: u64,
) -> i64 {
    let tid = engine.allocate_table_id();
    engine.write_column_records(tid, OWNER_KIND_TABLE, cols).unwrap();
    let batch = build_table_tab_row_flags(dir, tid, pack_pk_cols(pk_cols), table_name, flags);
    engine.ingest_to_family(TABLE_TAB_ID, &batch).unwrap();
    tid
}

/// Build the minimal identity circuit `ScanDelta(base) → Integrate` for
/// `vid` and write its rows through the applied-delta path. The payload
/// column layout follows `gnitz_wire::CIRCUIT_NODES_COLS` /
/// `CIRCUIT_EDGES_COLS`; the compound PK `(view_id, sub)` is packed by
/// `pack_view_pk`. `scan_blob` is the scan node's optional expr/param blob —
/// a bounded-scan fixture ships its `RangeDescriptor` there.
fn write_identity_circuit(engine: &mut CatalogEngine, vid: i64, base_tid: i64, scan_blob: Option<&[u8]>) {
    let nodes_schema = sys_tab_schema(CIRCUIT_NODES_TAB_ID);
    let mut bb = BatchBuilder::new(nodes_schema);
    // node 0: ScanDelta(base_tid)
    bb.begin_row(pack_view_pk(vid, 0), 1);
    bb.put_u64(0);
    bb.put_u64(gnitz_wire::OPCODE_SCAN_DELTA);
    bb.put_u64(base_tid as u64); // source_table
    match scan_blob {
        Some(b) => bb.put_blob(b),
        None => bb.put_null(),
    }
    bb.end_row();
    // node 1: Integrate (terminal sink — moves the delta into the view store)
    bb.begin_row(pack_view_pk(vid, 1), 1);
    bb.put_u64(1);
    bb.put_u64(gnitz_wire::OPCODE_INTEGRATE);
    bb.put_null(); // source_table
    bb.put_null(); // expr_program
    bb.end_row();
    engine.ingest_to_family(CIRCUIT_NODES_TAB_ID, &bb.finish()).unwrap();

    let edges_schema = sys_tab_schema(CIRCUIT_EDGES_TAB_ID);
    let mut bb = BatchBuilder::new(edges_schema);
    bb.begin_row(pack_view_pk(vid, 0), 1);
    bb.put_u64(1); // dst_node
    bb.put_u64(gnitz_wire::PORT_IN); // dst_port
    bb.put_u64(0); // src_node
    bb.end_row();
    engine.ingest_to_family(CIRCUIT_EDGES_TAB_ID, &bb.finish()).unwrap();
}

/// Append one raw VIEW_TAB row. `sql` is stored verbatim; cache_directory is
/// left empty (the register hook computes the real view directory itself and
/// neither column is read back by the appliers). The bare `0` pk_col_idx decodes
/// back to a single-column PK `[0]`.
fn push_view_tab_row(bb: &mut BatchBuilder, vid: i64, view_name: &str, sql: &str) {
    bb.begin_row(vid as u128, 1);
    bb.put_u64(PUBLIC_SCHEMA_ID as u64);
    bb.put_string(view_name);
    bb.put_string(sql);
    bb.put_string(""); // cache_directory
    bb.put_u64(0); // created_lsn
    bb.put_u64(0); // pk_col_idx
    bb.end_row();
}

/// A single-row VIEW_TAB batch for tests that register a view via the raw
/// system-table path.
fn build_view_tab_row(vid: i64, view_name: &str, sql: &str) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, vid, view_name, sql);
    bb.finish()
}
