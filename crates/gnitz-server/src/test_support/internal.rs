//! The catalog-level test helpers — the batch-level ones are
//! `gnitz-zset-testkit`'s.
//!
//! This file is compiled once, and only inside this crate, so it names
//! crate-internals as `crate::` and widens no API — nothing links this crate.

use crate::catalog::{CatalogColumn, CatalogEngine, SysFamily, PUBLIC_SCHEMA_ID};
use gnitz_expr::{ColumnTable, SchemaFacts};
use gnitz_wire::sys_rows::{
    write_circuit_row, write_idx_tab_row, write_schema_tab_row, write_table_tab_row, FkRef, IdxTabRow, SchemaTabRow,
    SysRowSink, TableTabRow,
};
use gnitz_wire::Circuit;
use gnitz_wire::{ColumnDef, TypeCode};
use gnitz_zset::repr::{Batch, BatchBuilder, ReadCursor};

/// A scratch directory for a test that needs a real on-disk tree, wiped at the
/// start of the run so the previous same-user run self-cleans. `scope` names the
/// subsystem, `name` the test.
///
/// The path is namespaced by `$USER` (pid if unset): `/tmp` is shared and
/// sticky, so a directory left by a *different* user occupies the bare path
/// forever — the start-of-test `remove_dir_all` cannot delete it (sticky bit)
/// and `CatalogEngine::open` then fails EACCES creating subdirs under it.
pub fn scratch_dir(scope: &str, name: &str) -> String {
    let owner = std::env::var("USER").unwrap_or_else(|_| std::process::id().to_string());
    let path = std::env::temp_dir()
        .join(format!("gnitz_{scope}_test_{owner}_{name}"))
        .to_str()
        .unwrap()
        .to_owned();
    let _ = std::fs::remove_dir_all(&path);
    path
}

// ── Catalog column fixtures ─────────────────────────────────────────────────

/// A plain non-nullable, non-FK, non-hidden column of the given type.
pub fn col_def(name: &str, type_code: TypeCode) -> CatalogColumn {
    CatalogColumn {
        def: ColumnDef::new(name, type_code, false),
        fk: None,
    }
}

/// A plain non-nullable UUID column.
pub fn uuid_def(name: &str) -> CatalogColumn {
    col_def(name, TypeCode::UUID)
}

/// A nullable column of the given type.
pub fn nullable_def(name: &str, type_code: TypeCode) -> CatalogColumn {
    CatalogColumn {
        def: ColumnDef::new(name, type_code, true),
        fk: None,
    }
}

/// A column of `type_code` carrying an FK onto `(parent_tid, parent_col)`.
pub fn fk_def(name: &str, type_code: TypeCode, parent_tid: u64, parent_col: u32) -> CatalogColumn {
    CatalogColumn {
        fk: Some(FkRef { table_id: parent_tid, col: parent_col }),
        ..col_def(name, type_code)
    }
}

/// Every row of `tid` under `bound`, in the relation's own layout.
pub fn read_rows(engine: &mut CatalogEngine, tid: u64, bound: gnitz_wire::ReadBound) -> std::rc::Rc<Batch> {
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
pub fn scan_all(engine: &mut CatalogEngine, tid: u64) -> std::rc::Rc<Batch> {
    read_rows(engine, tid, gnitz_wire::ReadBound::None)
}

/// Net weight summed over every (PK, payload) a store's cursor yields — what a
/// Z-set store actually holds. A row *count* would hide a double-drive, which
/// leaves the row set identical and only doubles the weights.
pub fn sum_weights(mut c: ReadCursor) -> i64 {
    let mut sum = 0;
    while c.valid {
        sum += c.current_weight;
        c.advance();
    }
    sum
}

// ---------------------------------------------------------------------------
// Circuit + view fixtures
// ---------------------------------------------------------------------------

/// `circuit` as `vid`'s CIRCUIT_TAB batch, through the row writer the client
/// commits with.
pub fn circuit_batch(vid: u64, circuit: &Circuit) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Circuit.schema());
    write_circuit_row(&mut bb, vid, circuit);
    bb.finish()
}

/// `vid`'s CIRCUIT_TAB row holding `cell` as given, whatever it decodes to.
pub fn circuit_cell_batch(vid: u64, cell: &[u8]) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Circuit.schema());
    bb.begin_row(vid as u128, 1);
    bb.put_blob(cell);
    bb.end_row();
    bb.finish()
}

/// Write `vid`'s circuit through the applied-delta path.
pub fn write_circuit(engine: &mut CatalogEngine, vid: u64, circuit: Circuit) {
    engine.submit(SysFamily::Circuit, circuit_batch(vid, &circuit)).unwrap();
}

/// The minimal identity circuit `ScanDelta(source, bound) → Integrate`.
pub fn identity_circuit(source_tid: u64, bound: gnitz_wire::ReadBound) -> Circuit {
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(source_tid, bound);
    circuit.sink(scan);
    circuit
}

/// One unbounded `ScanDelta` per source, the first sunk: the dependency-map
/// shape of a view over `sources`.
pub fn scanning_circuit(sources: &[u64]) -> Circuit {
    let mut circuit = Circuit::default();
    let scans: Vec<_> = sources
        .iter()
        .map(|&s| circuit.input_delta(s, gnitz_wire::ReadBound::None))
        .collect();
    circuit.sink(scans[0]);
    circuit
}

/// `ScanDelta(source)` then `n - 1` `Negate`s: an `n`-node circuit.
pub fn negate_chain(source: u64, n: usize) -> Circuit {
    let mut circuit = Circuit::default();
    let mut tip = circuit.input_delta(source, gnitz_wire::ReadBound::None);
    for _ in 1..n {
        tip = circuit.negate(tip);
    }
    circuit
}

/// `source` scanned and reindexed on `key`, keeping its column 0, with `key`
/// stated as its scatter key — what a spine that moves no column produces.
pub fn scan_keyed(circuit: &mut Circuit, source: u64, key: &[gnitz_wire::ReindexSlot]) -> gnitz_wire::NodeId {
    let scan = circuit.input_delta(source, gnitz_wire::ReadBound::None);
    let role = gnitz_wire::ReindexRole::ScatterKey { source_key: key.to_vec() };
    circuit.map_reindex(scan, key, &[0], role, gnitz_wire::NullKeys::Keep)
}

/// [`scan_keyed`] on `source`'s column 1, of type `tc`.
pub fn reindexed_on_col1(circuit: &mut Circuit, source: u64, tc: gnitz_wire::TypeCode) -> gnitz_wire::NodeId {
    scan_keyed(circuit, source, &[(1, tc.reindex_output_type())])
}

/// An equi-join of `a` and `b` on their column 1, of type `tc`, each side
/// stating its scatter key where `keyed` says so.
pub fn equi_join_circuit(a: u64, b: u64, tc: gnitz_wire::TypeCode, keyed: [bool; 2]) -> Circuit {
    let mut circuit = Circuit::default();
    let [ka, kb] = [(a, keyed[0]), (b, keyed[1])].map(|(source, keyed)| match keyed {
        true => reindexed_on_col1(&mut circuit, source, tc),
        false => circuit.input_delta(source, gnitz_wire::ReadBound::None),
    });
    let joined = circuit.join(ka, kb, gnitz_wire::JoinKind::Equi, false);
    circuit.sink(joined);
    circuit
}

/// A two-term inner equi-join of `a` and `b` on their column 1, of type `tc`,
/// output `[key, a.0, b.0]`.
pub fn two_term_join_circuit(a: u64, b: u64, tc: gnitz_wire::TypeCode) -> Circuit {
    let mut circuit = Circuit::default();
    let deltas = [a, b].map(|source| reindexed_on_col1(&mut circuit, source, tc));
    let joined = circuit.join_terms(deltas, deltas, gnitz_wire::JoinKind::Equi);
    circuit.sink(joined);
    circuit
}

/// `a (c0, c1, c2) LEFT JOIN b (c0, c1) ON a.<key> = b.c0` over U64 keys, as
/// `[key, a.c0, a.c1, a.c2, b.c1]`. Answers the circuit and `a`'s join re-key.
pub fn left_join_circuit(a: u64, b: u64, key: u32) -> (Circuit, gnitz_wire::NodeId) {
    use gnitz_wire::{JoinKind, NullKeys, ReindexRole};
    let mut c = Circuit::default();
    let a_key = [(key, TypeCode::U64)];
    let role = || ReindexRole::ScatterKey { source_key: a_key.to_vec() };
    let sa = c.input_delta(a, gnitz_wire::ReadBound::None);
    let sb = c.input_delta(b, gnitz_wire::ReadBound::None);
    let b_key = [(0, TypeCode::U64)];
    let b_role = ReindexRole::ScatterKey { source_key: b_key.to_vec() };
    let rb = c.map_reindex(sb, &b_key, &[1], b_role, NullKeys::Drop);
    let ra = c.map_reindex(sa, &a_key, &[0, 1, 2], role(), NullKeys::Drop);
    let all = c.map_reindex(sa, &a_key, &[0, 1, 2], role(), NullKeys::Keep);
    let inner = c.join_terms([ra, rb], [ra, rb], JoinKind::Equi);
    // `a`'s rows some row of `b` matches: its re-key against `b`'s key set.
    let keys = c.map(rb, &[]);
    let set = c.distinct(keys);
    let matched = c.join_terms([ra, set], [ra, set], JoinKind::Equi);
    let nu = c.difference(all, matched);
    let filled = c.null_extend(nu, &[TypeCode::U64], false);
    let out = c.union(filled, inner);
    c.sink(out);
    (c, ra)
}

/// A drive host for circuits that exchange nothing.
pub struct LocalDrive<'a>(pub &'a mut CatalogEngine);

impl crate::query::DriveHost for LocalDrive<'_> {
    fn parts(
        &mut self,
    ) -> (
        &mut crate::query::DagEngine,
        &mut gnitz_store::relation::RelationRegistry,
    ) {
        (&mut self.0.dag, &mut self.0.registry)
    }

    fn exchange(
        &mut self,
        view_id: u64,
        _batch: std::borrow::Cow<'_, Batch>,
        _plan: &gnitz_zset::algebra::ScatterPlan,
        _fold: bool,
    ) -> Batch {
        panic!("view {view_id} relayed: this host serves exchange-free circuits only");
    }
}

/// [`identity_circuit`] written as `vid`'s circuit.
pub fn write_identity_circuit(engine: &mut CatalogEngine, vid: u64, source_tid: u64, bound: gnitz_wire::ReadBound) {
    write_circuit(engine, vid, identity_circuit(source_tid, bound));
}

// ── System-row fixtures ──────────────────────────────────────────────────

/// Append one `family` row keyed `key` — the leading id, then the member a
/// pair-keyed family adds — at `weight`. Payload column `pi` holds `cell(pi)`: a
/// U64 as it is, a STRING or BLOB as a text of it too long to sit inline, so a
/// comparison of two such cells reads both blob heaps.
pub fn push_sys_row(bb: &mut BatchBuilder, family: SysFamily, key: [u64; 2], weight: i64, cell: impl Fn(usize) -> u64) {
    let schema = family.schema();
    bb.begin_row_opk(&key.map(u128::from)[..schema.pk_cols().len()], weight);
    for (pi, col) in schema.payload_columns() {
        match col.type_code {
            TypeCode::U64 => bb.put_u64(cell(pi)),
            _ => bb.put_string(&format!("payload_cell_{}", cell(pi))),
        }
    }
    bb.end_row();
}

/// A TABLE_TAB batch of `(table_id, name, weight)` rows: `public` tables keyed
/// on column 0, under default props.
pub fn table_tab_batch(rows: &[(u64, &str, i64)]) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    for &(table_id, name, weight) in rows {
        let row = TableTabRow {
            table_id,
            schema_id: PUBLIC_SCHEMA_ID,
            name,
            pk: gnitz_wire::PkColList::from_slice(&[0]),
            props: gnitz_wire::TableProps::default(),
        };
        write_table_tab_row(&mut bb, &row, weight);
    }
    bb.finish()
}

/// A SCHEMA_TAB batch of `(schema_id, name, weight)` rows.
pub fn schema_tab_batch(rows: &[(u64, &str, i64)]) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Schema.schema());
    for &(schema_id, name, weight) in rows {
        write_schema_tab_row(&mut bb, &SchemaTabRow { schema_id, name }, weight);
    }
    bb.finish()
}

/// `defs` as `owner_id`'s COL_TAB batch at `weight`, numbered by position.
pub fn col_tab_batch(owner_id: u64, defs: &[CatalogColumn], weight: i64) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    crate::catalog::write_col_tab_rows(&mut bb, owner_id, defs, weight);
    bb.finish()
}

/// The one-row IDX_TAB batch of an index over `cols` at `weight`.
pub fn idx_tab_batch(index_id: u64, owner_id: u64, cols: &[u32], name: &str, is_unique: bool, weight: i64) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Index.schema());
    write_idx_tab_row(
        &mut bb,
        &IdxTabRow {
            index_id,
            owner_id,
            cols: gnitz_wire::PkColList::from_slice(cols),
            name,
            is_unique,
        },
        weight,
    );
    bb.finish()
}

/// Append one raw VIEW_TAB row with PK list `[0]`, word by word, so a test can forge
/// budget words no `ViewProps` holds (`0` = off; `owner_view_id` `0` = a user view).
pub fn push_view_tab_row(
    bb: &mut BatchBuilder,
    weight: i64,
    vid: u64,
    view_name: &str,
    capacity_bytes: u64,
    delta_bytes: u64,
    owner_view_id: u64,
) {
    let sink: &mut dyn SysRowSink = bb;
    sink.begin_row(&[vid as u128], weight);
    sink.put_u64(PUBLIC_SCHEMA_ID);
    sink.put_string(view_name);
    sink.put_u64(gnitz_wire::PkColList::from_slice(&[0]).pack());
    sink.put_u64(capacity_bytes);
    sink.put_u64(delta_bytes);
    sink.put_u64(owner_view_id);
    sink.put_u64(0); // pk_repeats
    sink.end_row();
}

/// Register a view running `circuit` through the raw system-table path,
/// returning its vid or the registration's own error. Budgets in bytes,
/// `0` = off.
pub fn try_register_view(
    engine: &mut CatalogEngine,
    circuit: Circuit,
    name: &str,
    cols: &[CatalogColumn],
    capacity_bytes: u64,
    delta_bytes: u64,
) -> Result<u64, String> {
    let vid = engine.allocate_ids(1).unwrap();
    write_circuit(engine, vid, circuit);
    engine.write_column_records(vid, cols).unwrap();
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, 1, vid, name, capacity_bytes, delta_bytes, 0);
    engine.submit(SysFamily::View, bb.finish())?;
    Ok(vid)
}

/// [`try_register_view`] for an identity view over `source_tid`.
pub fn try_register_identity_view(
    engine: &mut CatalogEngine,
    source_tid: u64,
    name: &str,
    cols: &[CatalogColumn],
    capacity_bytes: u64,
    delta_bytes: u64,
) -> Result<u64, String> {
    let circuit = identity_circuit(source_tid, gnitz_wire::ReadBound::None);
    try_register_view(engine, circuit, name, cols, capacity_bytes, delta_bytes)
}

/// [`try_register_identity_view`] for an unbounded view that must succeed.
pub fn register_identity_view(engine: &mut CatalogEngine, source_tid: u64, name: &str, cols: &[CatalogColumn]) -> u64 {
    try_register_identity_view(engine, source_tid, name, cols, 0, 0).unwrap()
}

/// The rows a range read over `cols` returns, or `None` when none match.
pub fn seek_by_index_range(
    engine: &mut CatalogEngine,
    tid: u64,
    cols: &[u32],
    eq: &[u128],
    start: gnitz_wire::Cut,
    end: gnitz_wire::Cut,
) -> Result<(Option<std::rc::Rc<Batch>>, gnitz_zset::schema::SchemaDescriptor), gnitz_wire::WireFault> {
    let schema = engine.registry.relation_or_err(tid)?.schema();
    let range = gnitz_wire::KeyRange::new(gnitz_wire::PkColList::from_slice(cols), eq, start, end);
    let spec = gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::Range(range));
    let rows = engine.scan_spec(tid, spec, schema.layout_digest())?;
    Ok(((!rows.is_empty()).then_some(rows), schema))
}

/// [`seek_by_index_range`] at the point `natives` names, fewer than `cols` for a
/// prefix seek.
pub fn seek_by_index(
    engine: &mut CatalogEngine,
    tid: u64,
    cols: &[u32],
    natives: &[u128],
) -> Result<(Option<std::rc::Rc<Batch>>, gnitz_zset::schema::SchemaDescriptor), gnitz_wire::WireFault> {
    let schema = engine.registry.relation_or_err(tid)?.schema();
    let images: Vec<u128> = cols
        .iter()
        .zip(natives)
        .map(|(&c, &v)| gnitz_wire::key_image(schema.columns[c as usize].type_code, v))
        .collect();
    let (&last, eq) = images.split_last().expect("at least one key value");
    seek_by_index_range(
        engine,
        tid,
        cols,
        eq,
        gnitz_wire::Cut::before(last),
        gnitz_wire::Cut::after(last),
    )
}
