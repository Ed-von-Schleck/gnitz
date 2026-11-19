//! The catalog-level test helpers — the batch-level ones are
//! `gnitz-zset-testkit`'s.
//!
//! This file is compiled once, and only inside this crate, so it names
//! crate-internals as `crate::` and widens no API — nothing links this crate.

use crate::catalog::{CatalogEngine, DdlBundle, PUBLIC_SCHEMA_ID, SysFamily, SysFamilyExt, ZoneError};
use gnitz_store::relation::PkRule;
use gnitz_store::relation::{RelationKind, RelationRegistry, RelationSpec, StoreConfig};
use gnitz_trace::runs::ReadCursor;
use gnitz_wire::{
    CatalogColumn, Circuit, CircuitRow, ColTabRow, ColumnDef, FkAction, FkRef, IdxTabRow, PkColList, SchemaTabRow,
    SysRow, TableProps, TableTabRow, TypeCode, ViewTabRow,
};
use gnitz_zset::algebra::Placement;
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::schema::SchemaDescriptor;

/// Relation `id`, a `kind` over `schema` placed by its full PK.
pub fn full_pk_spec(id: u64, kind: RelationKind, schema: &SchemaDescriptor) -> RelationSpec {
    RelationSpec {
        id,
        kind,
        placement: Placement::full_pk(schema),
        pk_repeats: false,
        compress: false,
    }
}

/// Register [`full_pk_spec`].
pub fn register_full_pk(registry: &mut RelationRegistry, id: u64, kind: RelationKind, schema: SchemaDescriptor) {
    registry.register(full_pk_spec(id, kind, &schema), schema).unwrap();
}

// ── Catalog column fixtures ─────────────────────────────────────────────────

/// Create `public.parent(pid U64)` on PK [0]; its id.
pub fn fk_parent(engine: &mut CatalogEngine) -> u64 {
    engine
        .create_table("public.parent", &[col_def("pid", TypeCode::U64)], &[0])
        .unwrap()
}

/// A child's columns `(cid U64, fk U64)`, `fk` referencing `parent`'s column 0.
pub fn fk_child_cols(parent: u64) -> [CatalogColumn; 2] {
    [col_def("cid", TypeCode::U64), fk_def("fk", TypeCode::U64, parent, 0)]
}

/// A plain non-nullable, non-FK, non-hidden column of the given type.
pub fn col_def(name: &str, type_code: TypeCode) -> CatalogColumn {
    CatalogColumn {
        def: ColumnDef::new(name, type_code, false),
        fk: None,
    }
}

/// The columns of `make_schema_u64_i64`: a U64 `id` key and an I64 `v`.
pub fn u64_i64_cols() -> [CatalogColumn; 2] {
    [col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)]
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
pub fn read_rows(engine: &mut CatalogEngine, tid: u64, bound: gnitz_wire::ReadBound) -> gnitz_store::read::ReadRows {
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
pub fn scan_all(engine: &mut CatalogEngine, tid: u64) -> gnitz_store::read::ReadRows {
    read_rows(engine, tid, gnitz_wire::ReadBound::None)
}

/// Net weight summed over every (PK, payload) a store's cursor yields — what a
/// Z-set store actually holds. A row *count* would hide a double-drive, which
/// leaves the row set identical and only doubles the weights.
pub fn sum_weights(c: ReadCursor) -> i64 {
    weights(c).iter().sum()
}

/// The weight of every row a cursor yields, in cursor order.
pub fn weights(mut c: ReadCursor) -> Vec<i64> {
    let mut out = Vec::new();
    while c.is_valid() {
        out.push(c.current_weight());
        c.advance();
    }
    out
}

/// The summed weight of relation `id`'s rows.
pub fn net_weight(engine: &CatalogEngine, id: u64) -> i64 {
    sum_weights(engine.registry.relation(id).expect("a registered relation").cursor())
}

/// Column records for a view whose output is `schema`.
pub fn cols_of(schema: &SchemaDescriptor) -> Vec<CatalogColumn> {
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

/// A copy of `cols`, each a source column and its type, into non-null payload
/// slots under `input`'s PK.
pub fn map_copy(
    circuit: &mut Circuit,
    input: gnitz_wire::NodeId,
    cols: &[gnitz_wire::ReindexSlot],
) -> gnitz_wire::NodeId {
    let sources: Vec<u32> = cols.iter().map(|&(c, _)| c).collect();
    let slots = cols.iter().map(|&(_, tc)| (tc, false)).collect();
    circuit.map_expr(input, gnitz_expr::LogicalProgram::copy_cols(&sources).to_map(slots))
}

/// `source` scanned under the leading `routed` slots of `key` as its scatter
/// key, and reindexed on `key`, keeping its column 0 — what a spine that moves
/// no column produces.
pub fn scan_routed(
    circuit: &mut Circuit,
    source: u64,
    key: &[gnitz_wire::ReindexSlot],
    routed: usize,
) -> gnitz_wire::NodeId {
    let scan = circuit
        .input_scattered(source, gnitz_wire::ReadBound::None, Some(key[..routed].to_vec()))
        .unwrap();
    circuit.map_reindex(scan, key, &[0], gnitz_wire::NullKeys::Keep)
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
    use gnitz_wire::{JoinKind, NullKeys, ReadBound};
    let mut c = Circuit::default();
    let a_key = [(key, TypeCode::U64)];
    let b_key = [(1, TypeCode::U64)];
    // `b` first: a backfill feeds the sources in scan order, and `a`'s rows are
    // then null-filled against a `b` that is already there.
    let sb = c.input_scattered(b, ReadBound::None, Some(b_key.to_vec())).unwrap();
    let sa = c.input_scattered(a, ReadBound::None, Some(a_key.to_vec())).unwrap();
    let rb = c.map_reindex(sb, &b_key, &[2], NullKeys::Drop);
    let ra = c.map_reindex(sa, &a_key, &[0, 1, 2], NullKeys::Drop);
    let all = match key == 2 {
        true => c.map_reindex(sa, &a_key, &[0, 1, 2], NullKeys::Keep),
        false => ra,
    };
    let inner = c.join([ra, rb], [ra, rb], JoinKind::Equi);
    let keys = map_copy(&mut c, rb, &[]);
    let set = c.distinct(keys);
    let matched = c.join([ra, set], [ra, set], JoinKind::Equi);
    let nu = c.difference(all, matched);
    let filled = c.null_extend(nu, &[TypeCode::U64], false);
    c.union(filled, inner);
    (c, ra)
}

/// The columns of `a (id | nn, nullable)`, `b (id | k, w)` and the view of
/// [`left_join_circuit`] `[key | id, nn, nullable, w]`, every one a U64.
pub fn left_join_cols() -> ([CatalogColumn; 3], [CatalogColumn; 3], [CatalogColumn; 5]) {
    use TypeCode::U64;
    (
        [col_def("id", U64), col_def("nn", U64), nullable_def("nullable", U64)],
        [col_def("id", U64), col_def("k", U64), col_def("w", U64)],
        [
            col_def("key", U64),
            col_def("id", U64),
            col_def("nn", U64),
            nullable_def("nullable", U64),
            nullable_def("w", U64),
        ],
    )
}

/// Tables `a` and `b` of [`left_join_cols`] in a fresh catalog at `dir`, under
/// the view of [`left_join_circuit`] on `a`'s column `key`.
pub fn left_join_engine(dir: &str, key: u32) -> (CatalogEngine, [u64; 2], u64) {
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let (a_cols, b_cols, view_cols) = left_join_cols();
    let a = engine.create_table("public.a", &a_cols, &[0]).unwrap();
    let b = engine.create_table("public.b", &b_cols, &[0]).unwrap();
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

    fn exchange<'b>(
        &mut self,
        lanes: impl ExactSizeIterator<Item = crate::query::Lane<'b>>,
        drained: bool,
        gathered: &mut Vec<Batch>,
    ) -> bool {
        gathered.extend(lanes.map(|lane| lane.batch.into_owned()));
        drained
    }
}

/// Push `rows` into `tid` and tick it, as a worker applies a push.
pub fn push_and_tick(engine: &mut CatalogEngine, tid: u64, rows: Batch) {
    engine.ingest_unticked(tid, rows, PkRule::Enforce).unwrap();
    crate::query::tick(engine, tid, 1).unwrap();
}

/// A [`CatalogEngine`] driven as itself, counting the rounds it exchanged and
/// the waits they shared.
pub struct EchoDrive<'a> {
    pub cat: &'a mut CatalogEngine,
    pub rounds: u64,
    pub waits: u64,
}

impl<'a> EchoDrive<'a> {
    pub fn new(cat: &'a mut CatalogEngine) -> Self {
        EchoDrive { cat, rounds: 0, waits: 0 }
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

    fn exchange<'b>(
        &mut self,
        lanes: impl ExactSizeIterator<Item = crate::query::Lane<'b>>,
        drained: bool,
        gathered: &mut Vec<Batch>,
    ) -> bool {
        self.rounds += lanes.len() as u64;
        self.waits += 1;
        crate::query::DriveHost::exchange(self.cat, lanes, drained, gathered)
    }
}

// ── DDL entry ────────────────────────────────────────────────────────────

/// A `DDL_TXN` bundle of `blocks`, one per family.
pub fn bundle(blocks: impl IntoIterator<Item = (SysFamily, Batch)>) -> DdlBundle {
    let mut families = DdlBundle::default();
    for (family, batch) in blocks {
        families.insert(family, batch).unwrap();
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

// ── Engine fixtures ──────────────────────────────────────────────────────

/// A fresh engine at `workers` on its own temporary directory, which lives as
/// long as the returned guard.
pub fn empty_catalog(workers: u32) -> (tempfile::TempDir, CatalogEngine) {
    empty_catalog_with(workers, StoreConfig::from_env("GNITZ_"))
}

/// [`empty_catalog`] under `config`.
pub fn empty_catalog_with(workers: u32, config: StoreConfig) -> (tempfile::TempDir, CatalogEngine) {
    let dir = tempfile::tempdir().unwrap();
    let engine = CatalogEngine::open_with(dir.path().to_str().unwrap(), workers, config).unwrap();
    (dir, engine)
}

/// [`empty_catalog`] holding an empty `public.t` over `cols` on PK column 0;
/// and its id.
pub fn table_engine(workers: u32, cols: &[CatalogColumn]) -> (tempfile::TempDir, CatalogEngine, u64) {
    table_engine_with(workers, cols, StoreConfig::from_env("GNITZ_"))
}

/// [`table_engine`] under `config`.
pub fn table_engine_with(
    workers: u32,
    cols: &[CatalogColumn],
    config: StoreConfig,
) -> (tempfile::TempDir, CatalogEngine, u64) {
    let (dir, mut engine) = empty_catalog_with(workers, config);
    let tid = engine.create_table("public.t", cols, &[0]).unwrap();
    (dir, engine, tid)
}

/// The environment's [`StoreConfig`], scanning `scan_chunk_rows` rows at a time.
pub fn chunked(scan_chunk_rows: usize) -> StoreConfig {
    StoreConfig {
        scan_chunk_rows,
        ..StoreConfig::from_env("GNITZ_")
    }
}

/// Split `schema.name`, defaulting the schema half. Only the in-process DDL
/// entry points take qualified-name strings; the wire path ships schema and
/// entity ids separately.
pub fn parse_qualified_name<'a>(name: &'a str, default_schema: &'a str) -> (&'a str, &'a str) {
    match name.find('.') {
        Some(dot) => (&name[..dot], &name[dot + 1..]),
        None => (default_schema, name),
    }
}

/// The COL_TAB records and TABLE_TAB `+1` that register table `tid` named `name`
/// in schema `sid`, in apply order.
pub fn table_blocks(
    tid: u64,
    sid: u64,
    name: &str,
    cols: &[CatalogColumn],
    pk: &[u32],
    props: TableProps,
) -> [(SysFamily, Batch); 2] {
    let row = TableTabRow::of(tid, sid, name, PkColList::from_slice(pk), props);
    [
        (SysFamily::Column, col_tab_batch(tid, cols, 1)),
        (SysFamily::Table, sys_batch([(row, 1)])),
    ]
}

impl CatalogEngine {
    /// [`Self::open_with`] under the environment's [`StoreConfig`].
    pub(crate) fn open(base_dir: &str, num_workers: u32) -> Result<Self, String> {
        Self::open_with(base_dir, num_workers, StoreConfig::from_env("GNITZ_"))
    }

    /// [`Self::open_master`] over an empty log, then the rest of a store-owning
    /// boot, as a standalone host at rank 0.
    pub(crate) fn open_with(base_dir: &str, num_workers: u32, config: StoreConfig) -> Result<Self, String> {
        let mut engine = Self::open_master(base_dir, num_workers, config, [])?;
        engine.open_stores(0, gnitz_store::relation::Residency::Origin)?;
        Ok(engine)
    }

    /// [`Self::reserve_catalog_ids`] with no log to lay the advance out in.
    pub(crate) fn allocate_ids(&mut self, count: u64) -> Result<u64, String> {
        self.reserve_catalog_ids(count, &mut |_, _| Ok(())).map_err(refusal)
    }

    /// Flush every base and system store, then drop the engine. Dropping one
    /// without `close` is a crash.
    pub(crate) fn close(mut self) {
        let _ = self.registry.checkpoint_base();
        let _ = self.flush_all_system_tables();
    }

    /// Build a table directly in the catalog: allocate the id, then apply its
    /// COL_TAB records and TABLE_TAB `+1` as one bundle, as a client's
    /// `CREATE TABLE` does over the wire.
    pub(crate) fn create_table(
        &mut self,
        qualified_name: &str,
        col_defs: &[CatalogColumn],
        pk_cols: &[u32],
    ) -> Result<u64, String> {
        self.create_table_with(qualified_name, col_defs, pk_cols, TableProps::default())
    }

    /// A SERIAL table `(id U64 PRIMARY KEY)`.
    pub(crate) fn create_serial_table(&mut self, qualified_name: &str) -> Result<u64, String> {
        let serial = TableProps { serial: true, ..Default::default() };
        self.create_table_with(qualified_name, &[col_def("id", TypeCode::U64)], &[0], serial)
    }

    /// [`Self::create_table`] under `props`.
    pub(crate) fn create_table_with(
        &mut self,
        qualified_name: &str,
        col_defs: &[CatalogColumn],
        pk_cols: &[u32],
        props: TableProps,
    ) -> Result<u64, String> {
        let (schema_name, table_name) = parse_qualified_name(qualified_name, "public");

        // Only what the bundle cannot derive for itself: the schema id, and an id
        // for the new table. Every rule this shape must satisfy is the
        // production precheck's — restating one here would let a test asserting
        // that rejection pass against this copy while the production arm was
        // broken.
        let sid = self
            .schema_id(schema_name)
            .ok_or_else(|| format!("Schema does not exist: {schema_name}"))?;
        let tid = self.allocate_ids(1).unwrap();

        apply_ddl(self, table_blocks(tid, sid, table_name, col_defs, pk_cols, props))?;
        Ok(tid)
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
    let mut bb = BatchBuilder::new(R::FAMILY.schema());
    for (row, weight) in rows {
        row.write(&mut bb, weight);
    }
    bb.finish()
}

/// `row` as a one-row `+1` batch of its family, with payload slot `slot` holding
/// the raw U64 word `word` in place of the value `row` states: a word no writer
/// emits, for a test of the decode that refuses it.
pub fn forged_sys_batch<R: SysRow>(row: R, slot: usize, word: u64) -> Batch {
    struct Forge {
        bb: BatchBuilder,
        col: usize,
        slot: usize,
        word: u64,
    }
    impl gnitz_wire::SysRowSink for Forge {
        fn begin_row(&mut self, pk: &[u128], weight: i64) {
            self.col = 0;
            self.bb.begin_row_natives(pk, weight);
        }
        fn put_u64(&mut self, v: u64) {
            self.bb.put_u64(if self.col == self.slot { self.word } else { v });
            self.col += 1;
        }
        fn put_string(&mut self, s: &str) {
            self.bb.put_string(s);
            self.col += 1;
        }
        fn put_bytes(&mut self, b: &[u8]) {
            self.bb.put_blob(b);
            self.col += 1;
        }
        fn end_row(&mut self) {
            self.bb.end_row();
        }
    }
    let family = R::FAMILY;
    let mut forge = Forge {
        bb: BatchBuilder::new(family.schema()),
        col: 0,
        slot,
        word,
    };
    row.write(&mut forge, 1);
    forge.bb.finish()
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
            TypeCode::U64 => {
                bb.put_u64(cell(pi));
            }
            _ => {
                bb.put_string(&format!("payload_cell_{}", cell(pi)));
            }
        }
    }
    bb.end_row();
}

/// A `public` TABLE_TAB row keyed on column 0, under default props.
pub fn table_tab_row(table_id: u64, name: &str) -> TableTabRow<'_> {
    TableTabRow::of(
        table_id,
        PUBLIC_SCHEMA_ID,
        name,
        gnitz_wire::PkColList::from_slice(&[0]),
        gnitz_wire::TableProps::default(),
    )
}

/// A TABLE_TAB batch of `(table_id, name, weight)` rows, each a [`table_tab_row`].
pub fn table_tab_batch(rows: &[(u64, &str, i64)]) -> Batch {
    sys_batch(rows.iter().map(|&(id, name, weight)| (table_tab_row(id, name), weight)))
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
    sys_batch(
        defs.iter()
            .enumerate()
            .map(|(i, cd)| (ColTabRow::of(owner_id, i as u64, &cd.def, cd.fk), weight)),
    )
}

/// The one-row IDX_TAB batch of an index over `cols` at `weight`.
pub fn idx_tab_batch(index_id: u64, owner_id: u64, cols: &[u32], name: &str, is_unique: bool, weight: i64) -> Batch {
    let row = IdxTabRow {
        index_id,
        owner_id,
        source_col_idx: gnitz_wire::PkColList::from_slice(cols),
        name: name.into(),
        is_unique,
    };
    sys_batch([(row, weight)])
}

/// A `public` user VIEW_TAB row with PK list `[0]` and every budget off. Every
/// budget field is a raw word, so a test can forge one no `ViewBudget` holds by
/// struct update (`0` = off; `owner_view_id` `0` = a user view).
pub fn view_tab_row(view_id: u64, name: &str) -> ViewTabRow<'_> {
    ViewTabRow {
        view_id,
        schema_id: PUBLIC_SCHEMA_ID,
        name: name.into(),
        pk_col_idx: gnitz_wire::PkColList::from_slice(&[0]),
        capacity_bytes: 0,
        delta_bytes: 0,
        owner_view_id: 0,
        pk_repeats: false,
        compress: false,
        definition: Default::default(),
    }
}

/// A chain created as one DDL bundle: a segment running `segment` over
/// `segment_cols`, and its owner, the next id, running `owner(segment id)` over
/// `owner_cols` and holding `capacity_bytes` (`0` = off). Answers
/// `(segment, owner)` or the bundle's refusal.
pub fn try_register_chain(
    engine: &mut CatalogEngine,
    segment: &Circuit,
    segment_cols: &[CatalogColumn],
    owner: impl FnOnce(u64) -> Circuit,
    owner_cols: &[CatalogColumn],
    capacity_bytes: u64,
) -> Result<(u64, u64), String> {
    let seg = engine.allocate_ids(2).unwrap();
    let top = seg + 1;
    let owner = owner(seg);
    let names = [format!("_seg{seg}"), format!("top{top}")];
    let segment_row = ViewTabRow {
        owner_view_id: top,
        ..view_tab_row(seg, &names[0])
    };
    let owner_row = ViewTabRow {
        capacity_bytes,
        ..view_tab_row(top, &names[1])
    };
    let rows = sys_batch([(segment_row, 1), (owner_row, 1)]);
    let created = [(seg, segment, segment_cols), (top, &owner, owner_cols)];
    apply_ddl(engine, view_blocks(created, rows))?;
    Ok((seg, top))
}

/// [`try_register_chain`] of an unbounded chain that must succeed.
pub fn register_chain(
    engine: &mut CatalogEngine,
    segment: &Circuit,
    segment_cols: &[CatalogColumn],
    owner: impl FnOnce(u64) -> Circuit,
    owner_cols: &[CatalogColumn],
) -> (u64, u64) {
    try_register_chain(engine, segment, segment_cols, owner, owner_cols, 0).unwrap()
}

/// `t JOIN u ON t.k = u.k`, both `(id, k)` U64, as a segment `(k, t_id, u_id)`,
/// under `SELECT k, SUM(t_id), COUNT(*) … GROUP BY k`, a reduce local to the
/// join's key. Answers `(segment, owner)`.
pub fn join_chain_under_a_reduce(engine: &mut CatalogEngine, t: u64, u: u64) -> (u64, u64) {
    use gnitz_wire::{AggDescriptor, AggFunc, GroupAggs, ReadBound};
    let joined = ["k", "t_id", "u_id"].map(|name| col_def(name, TypeCode::U64));
    let joined_schema =
        crate::test_support::u64_pk_schema(&[gnitz_zset::schema::SchemaColumn::new(TypeCode::U64, false); 2]);
    let aggs = [
        AggDescriptor { agg_op: AggFunc::Sum, col_idx: 1 },
        AggDescriptor::COUNT_STAR,
    ];
    let reduced = *gnitz_trace::stream::ReducePlan::from_wire(&joined_schema, &[0], &aggs, false)
        .unwrap()
        .output_schema();
    let reduce = |segment| {
        let mut circuit = Circuit::default();
        let scan = circuit.input_delta(segment, ReadBound::None);
        circuit.reduce_local(scan, GroupAggs { group_cols: vec![0], aggs: aggs.to_vec() });
        circuit
    };
    let join = two_term_join_circuit(t, u, TypeCode::U64);
    register_chain(engine, &join, &joined, reduce, &cols_of(&reduced))
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
    let row = ViewTabRow {
        capacity_bytes,
        delta_bytes,
        ..view_tab_row(vid, name)
    };
    apply_ddl(engine, view_blocks([(vid, &circuit, cols)], sys_batch([(row, 1)])))?;
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

/// `COUNT(*)` over `source`, a relation of `schema`, grouped by `group_cols`,
/// registered as `name`; and its plan.
pub fn register_count_of(
    engine: &mut CatalogEngine,
    source: u64,
    schema: &SchemaDescriptor,
    group_cols: &[u32],
    name: &str,
) -> (u64, gnitz_trace::stream::ReducePlan) {
    let aggs = [gnitz_wire::AggDescriptor::COUNT_STAR];
    // Only a global reduce owes a ground row; `ReducePlan` refuses one on a grouped reduce.
    let plan = gnitz_trace::stream::ReducePlan::from_wire(schema, group_cols, &aggs, group_cols.is_empty()).unwrap();
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(source, gnitz_wire::ReadBound::None);
    circuit.reduce(
        scan,
        gnitz_wire::GroupAggs {
            group_cols: group_cols.to_vec(),
            aggs: aggs.to_vec(),
        },
    );
    let view = try_register_view(engine, circuit, name, &cols_of(plan.output_schema()), 0, 0).unwrap();
    (view, plan)
}
