//! The catalog-level test helpers — the batch-level ones are
//! `gnitz-zset-testkit`'s.
//!
//! This file is compiled once, and only inside this crate, so it names
//! crate-internals as `crate::` and widens no API — nothing links this crate.

use crate::catalog::{CatalogColumn, CatalogEngine, SysFamily, ZoneError, PUBLIC_SCHEMA_ID};
use gnitz_wire::sys_rows::{CircuitRow, FkAction, FkRef, IdxTabRow, SchemaTabRow, SysRow, TableTabRow, ViewTabRow};
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

/// A column of `type_code` carrying a restricting FK onto `(parent_tid, parent_col)`.
pub fn fk_def(name: &str, type_code: TypeCode, parent_tid: u64, parent_col: u32) -> CatalogColumn {
    fk_def_on_delete(name, type_code, parent_tid, parent_col, FkAction::Restrict)
}

/// [`fk_def`] under an explicit `ON DELETE` action.
pub fn fk_def_on_delete(
    name: &str,
    type_code: TypeCode,
    parent_tid: u64,
    parent_col: u32,
    on_delete: FkAction,
) -> CatalogColumn {
    CatalogColumn {
        fk: Some(FkRef {
            table_id: parent_tid,
            col: parent_col,
            on_delete,
        }),
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

/// The summed weight of relation `id`'s rows.
pub fn net_weight(engine: &CatalogEngine, id: u64) -> i64 {
    sum_weights(engine.registry.relation(id).expect("a registered relation").cursor())
}

/// Column records for a view whose output is `schema`.
pub fn cols_of(schema: &gnitz_zset::schema::SchemaDescriptor) -> Vec<CatalogColumn> {
    schema
        .columns()
        .iter()
        .enumerate()
        .map(|(ci, c)| CatalogColumn {
            def: ColumnDef::new(format!("c{ci}"), c.type_code, c.nullable),
            fk: None,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Circuit + view fixtures
// ---------------------------------------------------------------------------

/// `circuit` as `vid`'s CIRCUIT_TAB batch, through the row writer the client
/// commits with.
pub fn circuit_batch(vid: u64, circuit: &Circuit) -> Batch {
    let circuit = circuit.encode();
    sys_batch([(
        CircuitRow {
            view_id: vid,
            circuit: circuit.as_slice().into(),
        },
        1,
    )])
}

/// `vid`'s CIRCUIT_TAB row holding `cell` as given, whatever it decodes to.
pub fn circuit_cell_batch(vid: u64, cell: &[u8]) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Circuit.schema());
    bb.begin_row(vid as u128, 1);
    bb.put_blob(cell);
    bb.end_row();
    bb.finish()
}

/// The minimal identity circuit: `ScanDelta(source, bound)`, its own output.
pub fn identity_circuit(source_tid: u64, bound: gnitz_wire::ReadBound) -> Circuit {
    let mut circuit = Circuit::default();
    circuit.input_delta(source_tid, bound);
    circuit
}

/// `ScanDelta(source) → Distinct`.
pub fn distinct_circuit(source: u64) -> Circuit {
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(source, gnitz_wire::ReadBound::None);
    circuit.distinct(scan);
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

/// `source` scanned and reindexed on `key`, keeping its column 0, with the
/// leading `routed` slots of `key` stated as its scatter key — what a spine that
/// moves no column produces.
pub fn scan_routed(
    circuit: &mut Circuit,
    source: u64,
    key: &[gnitz_wire::ReindexSlot],
    routed: usize,
) -> gnitz_wire::NodeId {
    let scan = circuit.input_delta(source, gnitz_wire::ReadBound::None);
    let role = gnitz_wire::ReindexRole::ScatterKey {
        source_cols: key[..routed].iter().map(|slot| slot.0).collect(),
    };
    circuit.map_reindex(scan, key, &[0], role, gnitz_wire::NullKeys::Keep)
}

/// [`scan_routed`] by the whole of `key`.
pub fn scan_keyed(circuit: &mut Circuit, source: u64, key: &[gnitz_wire::ReindexSlot]) -> gnitz_wire::NodeId {
    scan_routed(circuit, source, key, key.len())
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
    circuit.join([ka, kb], [ka, kb], gnitz_wire::JoinKind::Equi);
    circuit
}

/// A two-term inner equi-join of `a` and `b` on their column 1, of type `tc`,
/// output `[key, a.0, b.0]`.
pub fn two_term_join_circuit(a: u64, b: u64, tc: gnitz_wire::TypeCode) -> Circuit {
    let mut circuit = Circuit::default();
    let deltas = [a, b].map(|source| reindexed_on_col1(&mut circuit, source, tc));
    circuit.join(deltas, deltas, gnitz_wire::JoinKind::Equi);
    circuit
}

/// `a (c0, c1, c2) LEFT JOIN b (c0, c1, c2) ON a.<key> = b.c1` over U64 keys, as
/// `[key, a.c0, a.c1, a.c2, b.c2]`: `b` is not unique on its key, so `a`'s
/// matched rows are its re-key against `b`'s key set. Of `a`'s columns `c2`
/// alone is nullable, so only a join on it re-keys `a` a second time, keeping
/// its NULL-keyed rows. Answers the circuit and `a`'s join re-key.
pub fn left_join_circuit(a: u64, b: u64, key: u32) -> (Circuit, gnitz_wire::NodeId) {
    use gnitz_wire::{JoinKind, NullKeys, ReindexRole};
    let mut c = Circuit::default();
    let a_key = [(key, TypeCode::U64)];
    let role = || ReindexRole::ScatterKey { source_cols: vec![key] };
    // `b` first: a backfill feeds the sources in scan order, and `a`'s rows are
    // then null-filled against a `b` that is already there.
    let sb = c.input_delta(b, gnitz_wire::ReadBound::None);
    let sa = c.input_delta(a, gnitz_wire::ReadBound::None);
    let b_key = [(1, TypeCode::U64)];
    let b_role = ReindexRole::ScatterKey { source_cols: vec![1] };
    let rb = c.map_reindex(sb, &b_key, &[2], b_role, NullKeys::Drop);
    let ra = c.map_reindex(sa, &a_key, &[0, 1, 2], role(), NullKeys::Drop);
    let all = match key == 2 {
        true => c.map_reindex(sa, &a_key, &[0, 1, 2], role(), NullKeys::Keep),
        false => ra,
    };
    let inner = c.join([ra, rb], [ra, rb], JoinKind::Equi);
    let keys = c.map(rb, &[]);
    let set = c.distinct(keys);
    let matched = c.join([ra, set], [ra, set], JoinKind::Equi);
    let nu = c.difference(all, matched);
    let filled = c.null_extend(nu, &[TypeCode::U64], false);
    c.union(filled, inner);
    (c, ra)
}

/// Tables `a (id | nn, nullable)` and `b (id | k, w)`, every column a U64, in a
/// fresh catalog at `dir`, under the view of [`left_join_circuit`] on `a`'s
/// column `key`: `[key | id, nn, nullable, w]`.
pub fn left_join_engine(dir: &str, key: u32) -> (CatalogEngine, [u64; 2], u64) {
    use TypeCode::U64;
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let a_cols = [col_def("id", U64), col_def("nn", U64), nullable_def("nullable", U64)];
    let a = engine.create_table("public.a", &a_cols, &[0]).unwrap();
    let b_cols = [col_def("id", U64), col_def("k", U64), col_def("w", U64)];
    let b = engine.create_table("public.b", &b_cols, &[0]).unwrap();
    let view_cols = [
        col_def("key", U64),
        col_def("id", U64),
        col_def("nn", U64),
        nullable_def("nullable", U64),
        nullable_def("w", U64),
    ];
    let (circuit, _) = left_join_circuit(a, b, key);
    let view = try_register_view(&mut engine, circuit, "v", &view_cols, 0, 0).unwrap();
    (engine, [a, b], view)
}

/// An engine drives itself as one worker whose exchange hands it its own rows
/// back: every round of a drive runs, as at one worker.
impl crate::query::DriveHost for CatalogEngine {
    fn parts(
        &mut self,
    ) -> (
        &mut crate::query::DagEngine,
        &mut gnitz_store::relation::RelationRegistry,
    ) {
        (&mut self.dag, &mut self.registry)
    }

    fn exchange(
        &mut self,
        _view_id: u64,
        batch: std::borrow::Cow<'_, Batch>,
        _plan: &gnitz_zset::algebra::ScatterPlan,
        _fold: bool,
        drained: bool,
    ) -> (Batch, bool) {
        (batch.into_owned(), drained)
    }
}

/// Push `rows` into `tid` and tick it, as a worker applies a push.
pub fn push_and_tick(engine: &mut CatalogEngine, tid: u64, rows: Batch) {
    engine.ingest_unticked(tid, rows).unwrap();
    crate::query::tick(engine, tid, 1).unwrap();
}

/// A [`CatalogEngine`] driven as itself, counting the rounds it exchanged.
pub struct EchoDrive<'a> {
    pub cat: &'a mut CatalogEngine,
    pub rounds: u64,
}

impl<'a> EchoDrive<'a> {
    pub fn new(cat: &'a mut CatalogEngine) -> Self {
        EchoDrive { cat, rounds: 0 }
    }
}

impl crate::query::DriveHost for EchoDrive<'_> {
    fn parts(
        &mut self,
    ) -> (
        &mut crate::query::DagEngine,
        &mut gnitz_store::relation::RelationRegistry,
    ) {
        crate::query::DriveHost::parts(self.cat)
    }

    fn exchange(
        &mut self,
        view_id: u64,
        batch: std::borrow::Cow<'_, Batch>,
        plan: &gnitz_zset::algebra::ScatterPlan,
        fold: bool,
        drained: bool,
    ) -> (Batch, bool) {
        self.rounds += 1;
        crate::query::DriveHost::exchange(self.cat, view_id, batch, plan, fold, drained)
    }
}

// ── DDL entry ────────────────────────────────────────────────────────────

/// A `DDL_TXN` bundle of `blocks`, one per family.
pub fn bundle(blocks: impl IntoIterator<Item = (SysFamily, Batch)>) -> [Option<Batch>; SysFamily::COUNT] {
    let mut families: [Option<Batch>; SysFamily::COUNT] = std::array::from_fn(|_| None);
    for (family, batch) in blocks {
        assert!(families[family.index()].replace(batch).is_none());
    }
    families
}

/// Apply `blocks` as one DDL bundle, as the DDL driver does; either refusal as its text.
pub fn apply_ddl(
    engine: &mut CatalogEngine,
    blocks: impl IntoIterator<Item = (SysFamily, Batch)>,
) -> Result<(), String> {
    engine
        .apply_bundle(bundle(blocks), &mut |_, _| Ok(()))
        .map(drop)
        .map_err(refusal)
}

/// Either refusal of a catalog write, as its text.
pub fn refusal(e: ZoneError) -> String {
    match e {
        ZoneError::Refused(f) => f.text,
        ZoneError::Diverged(e) => e,
    }
}

/// The blocks of a bundle creating `views`, each `(vid, circuit, columns)`:
/// their CIRCUIT_TAB and COL_TAB rows, and `rows` as the VIEW_TAB block.
pub fn view_blocks<'a>(
    views: impl IntoIterator<Item = (u64, &'a Circuit, &'a [CatalogColumn])>,
    rows: Batch,
) -> [(SysFamily, Batch); 3] {
    let mut circuits = Batch::empty_with_schema(SysFamily::Circuit.schema());
    let mut columns = Batch::empty_with_schema(SysFamily::Column.schema());
    for (vid, circuit, cols) in views {
        circuits.append_batch(&circuit_batch(vid, circuit));
        columns.append_batch(&col_tab_batch(vid, cols, 1));
    }
    [
        (SysFamily::Circuit, circuits),
        (SysFamily::Column, columns),
        (SysFamily::View, rows),
    ]
}

// ── System-row fixtures ──────────────────────────────────────────────────

/// `rows` as one batch of their family, each at its weight.
pub fn sys_batch<R: SysRow>(rows: impl IntoIterator<Item = (R, i64)>) -> Batch {
    let family = SysFamily::from_id(R::FAMILY).expect("a SysRow names a system family");
    let mut bb = BatchBuilder::new(family.schema());
    for (row, weight) in rows {
        row.write(&mut bb, weight);
    }
    bb.finish()
}

/// Append one `family` row keyed `key` — the leading id, then the member a
/// pair-keyed family adds — at `weight`. Payload column `pi` holds `cell(pi)`: a
/// U64 as it is, a STRING or BLOB as a text of it too long to sit inline, so a
/// comparison of two such cells reads both blob heaps.
pub fn push_sys_row(bb: &mut BatchBuilder, family: SysFamily, key: [u64; 2], weight: i64, cell: impl Fn(usize) -> u64) {
    let schema = family.schema();
    bb.begin_row_natives(&key.map(u128::from)[..schema.pk_cols().len()], weight);
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
    sys_batch(rows.iter().map(|&(table_id, name, weight)| {
        let row = TableTabRow {
            table_id,
            schema_id: PUBLIC_SCHEMA_ID,
            name: name.into(),
            pk_col_idx: gnitz_wire::PkColList::from_slice(&[0]).pack(),
            flags: gnitz_wire::TableProps::default().pack(),
        };
        (row, weight)
    }))
}

/// A SCHEMA_TAB batch of `(schema_id, name, weight)` rows.
pub fn schema_tab_batch(rows: &[(u64, &str, i64)]) -> Batch {
    sys_batch(
        rows.iter()
            .map(|&(schema_id, name, weight)| (SchemaTabRow { schema_id, name: name.into() }, weight)),
    )
}

/// `defs` as `owner_id`'s COL_TAB batch at `weight`, numbered by position.
pub fn col_tab_batch(owner_id: u64, defs: &[CatalogColumn], weight: i64) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    crate::catalog::write_col_tab_rows(&mut bb, owner_id, defs, weight);
    bb.finish()
}

/// The one-row IDX_TAB batch of an index over `cols` at `weight`.
pub fn idx_tab_batch(index_id: u64, owner_id: u64, cols: &[u32], name: &str, is_unique: bool, weight: i64) -> Batch {
    let row = IdxTabRow {
        index_id,
        owner_id,
        source_col_idx: gnitz_wire::PkColList::from_slice(cols).pack(),
        name: name.into(),
        is_unique: is_unique as u64,
    };
    sys_batch([(row, weight)])
}

/// Append one VIEW_TAB row with PK list `[0]`. Every budget field is a raw
/// word, so a test can forge one no `ViewBudget` holds (`0` = off;
/// `owner_view_id` `0` = a user view).
pub fn push_view_tab_row(
    bb: &mut BatchBuilder,
    weight: i64,
    vid: u64,
    view_name: &str,
    capacity_bytes: u64,
    delta_bytes: u64,
    owner_view_id: u64,
) {
    let row = ViewTabRow {
        view_id: vid,
        schema_id: PUBLIC_SCHEMA_ID,
        name: view_name.into(),
        pk_col_idx: gnitz_wire::PkColList::from_slice(&[0]).pack(),
        capacity_bytes,
        delta_bytes,
        owner_view_id,
        pk_repeats: 0,
        compress: 0,
        definition: Default::default(),
    };
    row.write(bb, weight);
}

/// Register a view running `circuit` as one DDL bundle, returning its vid or
/// the bundle's refusal. Budgets in bytes,
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
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, 1, vid, name, capacity_bytes, delta_bytes, 0);
    apply_ddl(engine, view_blocks([(vid, &circuit, cols)], bb.finish()))?;
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
