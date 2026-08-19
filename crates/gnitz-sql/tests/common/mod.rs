#![allow(dead_code)]

//! Shared setup/assert helpers for the gnitz-sql integration tests.
//!
//! Every integration test forks a private `gnitz-server` via
//! `ServerHandle::start()`, so each test runs against an isolated, empty
//! server: schema names only need to be unique within one server. A single
//! shared `make_planner` therefore serves every test file.

use gnitz_core::{ColData, GnitzClient, Schema, ZSetBatch};
use gnitz_sql::{GnitzSqlError, SqlPlanner, SqlResult};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::{CIRCUIT_NODES_TAB, OPCODE_FILTER};
use std::collections::{BTreeMap, HashMap};

/// Returns (client, schema_name) with a unique schema already created.
/// The schema name is unique per call so parallel tests don't collide.
pub fn make_planner(srv: &ServerHandle) -> (GnitzClient, String) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let sn = format!("s{}", SEQ.fetch_add(1, Ordering::Relaxed));
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    client.create_schema(&sn).unwrap();
    (client, sn)
}

/// Plan and execute a statement, asserting it succeeds.
pub fn exec(client: &mut GnitzClient, sn: &str, sql: &str) {
    let mut p = SqlPlanner::new(client, sn);
    p.execute(sql).unwrap();
}

/// Plan and execute a statement, returning the planner result so a test can
/// assert it errors (`.is_err()`) or inspect the variant.
pub fn try_exec(client: &mut GnitzClient, sn: &str, sql: &str) -> Result<Vec<SqlResult>, GnitzSqlError> {
    let mut p = SqlPlanner::new(client, sn);
    p.execute(sql)
}

/// Assert `sql` fails with `want_variant` (`Unsupported`/`Bind`/`Plan`) whose
/// inner message contains `want_msg` — pins both the guard identity and the
/// message a migration must reproduce.
pub fn assert_rejects_variant(client: &mut GnitzClient, sn: &str, sql: &str, want_variant: &str, want_msg: &str) {
    let e = try_exec(client, sn, sql).unwrap_err();
    let (variant, msg) = match &e {
        GnitzSqlError::Unsupported(m) => ("Unsupported", m.clone()),
        GnitzSqlError::Bind(m) => ("Bind", m.clone()),
        GnitzSqlError::Plan(m) => ("Plan", m.clone()),
        other => ("other", format!("{other:?}")),
    };
    assert_eq!(variant, want_variant, "variant mismatch for `{sql}`: {e:?}");
    assert!(
        msg.contains(want_msg),
        "for `{sql}`\n  expected substring: {want_msg:?}\n  got: {msg:?}"
    );
}

/// Execute a single statement expected to return `RowsAffected`, returning the
/// reported row count.
pub fn affected(client: &mut GnitzClient, sn: &str, sql: &str) -> usize {
    let mut p = SqlPlanner::new(client, sn);
    match p.execute(sql).unwrap().pop().unwrap() {
        SqlResult::RowsAffected { count } => count,
        _ => panic!("expected RowsAffected"),
    }
}

/// Execute a single row-returning statement and return its (schema, batch).
pub fn read_sql(client: &mut GnitzClient, sn: &str, sql: &str) -> (Schema, ZSetBatch) {
    let mut p = SqlPlanner::new(client, sn);
    let mut res = p.execute(sql).unwrap();
    match res.pop().unwrap() {
        SqlResult::Rows { schema, batch } => (schema, batch),
        _ => panic!("expected Rows from `{sql}`"),
    }
}

/// Read `SELECT * FROM view` and return its (schema, batch).
pub fn read_view(client: &mut GnitzClient, sn: &str, view: &str) -> (Schema, ZSetBatch) {
    read_sql(client, sn, &format!("SELECT * FROM {}", view))
}

/// User-visible column names, lowercased — the read path hidden-prepends the
/// source PK (and grouped shapes a synthetic key), so a result's client-facing
/// schema is the visible subset.
pub fn visible_names(s: &Schema) -> Vec<String> {
    s.visible_columns().map(|(_, c)| c.name.to_lowercase()).collect()
}

pub fn col_idx(schema: &Schema, name: &str) -> usize {
    schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| {
            panic!(
                "column '{}' not in view schema {:?}",
                name,
                schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
            )
        })
}

pub fn i64_at(batch: &ZSetBatch, col: usize, row: usize) -> i64 {
    match &batch.columns[col] {
        ColData::Fixed(b) => i64::from_le_bytes(b[row * 8..row * 8 + 8].try_into().unwrap()),
        other => panic!("expected Fixed col, got {:?}", std::mem::discriminant(other)),
    }
}

/// Net weight of `value` in integer column `col` of `view`: sum the weights of
/// every row whose `col` equals `value`. Set-op tests read the multiplicity of a
/// single projected value this way.
pub fn view_value_weight(client: &mut GnitzClient, sn: &str, view: &str, col: &str, value: i64) -> i64 {
    let (schema, batch) = read_view(client, sn, view);
    let ci = col_idx(&schema, col);
    (0..batch.len())
        .filter(|&r| i64_at(&batch, ci, r) == value)
        .map(|r| batch.weights[r])
        .sum()
}

/// Read a PK-region integer column's value (as i64) at `row`. The client decodes
/// the OPK PK region back to native little-endian on receive, so a fixed-width
/// integer PK column reads exactly like a payload column. `ci` must be a PK
/// column index. The PK region is native-LE, tightly packed in pk-column order,
/// at every arity.
pub fn pk_i64_at(schema: &Schema, batch: &ZSetBatch, ci: usize, row: usize) -> i64 {
    let tc = schema.columns[ci].type_code;
    let bytes = batch.pks.col_window(row, schema.pk_byte_offset(ci), tc.wire_stride());
    if gnitz_wire::is_signed_int(tc as u8) {
        gnitz_wire::read_signed_exact(bytes)
    } else {
        gnitz_wire::read_unsigned_exact(bytes) as i64
    }
}

/// Read integer column `ci` of `row`, whichever region it lives in: a
/// PK-region column (a base table's own PK, a shape's natural PK — e.g. a
/// GROUP BY over the source PK — or a synthetic key like `_join_pk`) is
/// decoded from `batch.pks`; a payload column from `batch.columns`.
pub fn cell_i64(schema: &Schema, batch: &ZSetBatch, ci: usize, row: usize) -> i64 {
    if schema.is_pk_col(ci) {
        pk_i64_at(schema, batch, ci, row)
    } else {
        i64_at(batch, ci, row)
    }
}

/// Project `cols` out of a result, one sorted tuple per row, optionally with the
/// row's Z-set weight appended as a trailing element.
fn project_sorted(schema: &Schema, batch: &ZSetBatch, cols: &[&str], with_weight: bool) -> Vec<Vec<i64>> {
    let idxs: Vec<usize> = cols.iter().map(|c| col_idx(schema, c)).collect();
    let mut rows: Vec<Vec<i64>> = (0..batch.len())
        .map(|r| {
            let mut row: Vec<i64> = idxs.iter().map(|&ci| cell_i64(schema, batch, ci, r)).collect();
            if with_weight {
                row.push(batch.weights[r]);
            }
            row
        })
        .collect();
    rows.sort();
    rows
}

/// Read a view's named (integer) columns into sorted row tuples, so a test can
/// compare incremental view contents against an expected full recompute without
/// decoding the OPK PK region by hand.
pub fn payload_rows(client: &mut GnitzClient, sn: &str, view: &str, cols: &[&str]) -> Vec<Vec<i64>> {
    let (schema, batch) = read_view(client, sn, view);
    project_sorted(&schema, &batch, cols, false)
}

/// [`payload_rows`] for an arbitrary row-returning statement — a point read, a
/// filtered read, or a recompute over a base table — with each row's Z-set
/// weight appended as a trailing element. Row presence is the weaker property: a
/// placement bug that loses rows and a weight bug that duplicates them both read
/// as "the right values are there".
pub fn query_rows_weighted(client: &mut GnitzClient, sn: &str, sql: &str, cols: &[&str]) -> Vec<Vec<i64>> {
    let (schema, batch) = read_sql(client, sn, sql);
    project_sorted(&schema, &batch, cols, true)
}

/// The expected side of a [`query_rows_weighted`] compare when every row is
/// present exactly once: `rows` with a trailing `1`.
pub fn at_weight_one(rows: &[Vec<i64>]) -> Vec<Vec<i64>> {
    let mut out: Vec<Vec<i64>> = rows.iter().map(|r| r.iter().copied().chain([1]).collect()).collect();
    out.sort();
    out
}

/// `INSERT INTO table (cols) VALUES …` for integer row tuples.
pub fn insert_rows(client: &mut GnitzClient, sn: &str, table: &str, cols: &[&str], rows: &[Vec<i64>]) {
    let vals: Vec<String> = rows
        .iter()
        .map(|r| {
            let cells: Vec<String> = r.iter().map(|v| v.to_string()).collect();
            format!("({})", cells.join(", "))
        })
        .collect();
    exec(
        client,
        sn,
        &format!("INSERT INTO {table} ({}) VALUES {}", cols.join(", "), vals.join(", ")),
    );
}

pub fn f64_at(batch: &ZSetBatch, col: usize, row: usize) -> f64 {
    match &batch.columns[col] {
        ColData::Fixed(b) => f64::from_le_bytes(b[row * 8..row * 8 + 8].try_into().unwrap()),
        other => panic!("expected Fixed col, got {:?}", std::mem::discriminant(other)),
    }
}

/// Is the payload column at schema index `col` NULL in `row`? The null word
/// is indexed by payload position (schema index minus the single PK column).
pub fn is_null_at(batch: &ZSetBatch, payload_idx: usize, row: usize) -> bool {
    gnitz_core::null_word_get(batch.nulls[row], payload_idx)
}

/// Scan the circuit `nodes` system table once, for reuse across several
/// `opcode_node_count` calls in one test (the table is invariant after the
/// circuits are built, so one scan + round-trip serves every count).
pub fn scan_circuit_nodes(client: &mut GnitzClient) -> Option<ZSetBatch> {
    client.scan(CIRCUIT_NODES_TAB).unwrap().1
}

/// Count rows belonging to view `vid` whose column-3 u64 equals `op`, in a
/// circuit-table batch — `nodes` (from `scan_circuit_nodes`, column 3 = opcode)
/// or `node_columns` (column 3 = kind). Both tables share the compound PK
/// (view_id, sub) packed LE into 16 bytes (view_id in the low 8); a scan returns
/// the full schema order, so the discriminator is column index 3 (Fixed u64-LE,
/// non-PK).
pub fn opcode_node_count(batch: Option<&ZSetBatch>, vid: u64, op: u64) -> usize {
    let Some(batch) = batch else { return 0 };
    (0..batch.len())
        .filter(|&i| circuit_row_vid(batch, i) == vid && circuit_u64(batch, 3, i) == op)
        .count()
}

/// Count Filter nodes for `vid` (scans the nodes table). Thin wrapper over
/// `opcode_node_count` for the NULL-join-key plan-shape tests.
pub fn filter_node_count(client: &mut GnitzClient, vid: u64) -> usize {
    opcode_node_count(scan_circuit_nodes(client).as_ref(), vid, OPCODE_FILTER)
}

/// Count `vid`'s `NODE_COL_KIND_SCAN_BOUND` param rows — one per indexed column
/// of a pushed-down backfill-scan bound, so `0` means the view's `ScanDelta`
/// carries no bound. The `node_columns` table shares the circuit-table shape
/// `opcode_node_count` matches on: (view_id, sub) PK, u64 discriminator (here
/// `kind`) at column index 3.
pub fn scan_bound_col_count(client: &mut GnitzClient, vid: u64) -> usize {
    opcode_node_count(
        client.scan(gnitz_wire::CIRCUIT_NODE_COLUMNS_TAB).unwrap().1.as_ref(),
        vid,
        gnitz_wire::NODE_COL_KIND_SCAN_BOUND,
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Circuit-shape canonicalizer — the pinning-suite infrastructure.
//
// A deterministic, emission-order-independent textual dump of a view's whole
// circuit chain (final view + every hidden segment it transitively scans). Raw
// `NodeId`s are construction-order-dependent (`CircuitBuilder::alloc_node`, ids
// from 1) — *not* topological — so any id-keyed form churns on a refactor that
// re-emits the same graph in a different order. This dump depends only on
// **structure**: nodes are labelled by their DFS-from-sink position (`#N`), base
// scan sources render by NAME, and hidden-segment sources render by structural
// segment index. It reads only the three circuit system tables via `client.scan`
// — no production change.
// ─────────────────────────────────────────────────────────────────────────────

/// One decoded circuit (one `view_id`), grouped for rendering.
#[derive(Default)]
struct ViewCircuit {
    /// `node_id -> (opcode, source_table)`. `source_table` is `Some` only on the
    /// childless `Scan*` leaves (opcodes 11 / 31), per `encode_op_node`.
    nodes: BTreeMap<u64, (u64, Option<u64>)>,
    /// `node_id -> sorted [(kind, position, value1, value2)]`.
    params: BTreeMap<u64, Vec<(u64, u64, u64, u64)>>,
    /// `dst_node -> [(port, src_node)]`, sorted by port.
    inputs: BTreeMap<u64, Vec<(u64, u64)>>,
}

impl ViewCircuit {
    /// Sort params and inputs into their canonical (order-independent) form.
    fn normalize(&mut self) {
        for p in self.params.values_mut() {
            p.sort_unstable();
        }
        for e in self.inputs.values_mut() {
            e.sort_by_key(|(port, _)| *port);
        }
    }
}

/// A `nodes`/`node_columns`/`edges` circuit-table column read as `u64` (every
/// discriminator/param column is a non-null `Fixed` U64).
fn circuit_u64(batch: &ZSetBatch, col: usize, row: usize) -> u64 {
    match &batch.columns[col] {
        ColData::Fixed(b) => u64::from_le_bytes(b[row * 8..row * 8 + 8].try_into().unwrap()),
        other => panic!("circuit column {col} not Fixed: {other:?}"),
    }
}

/// `view_id` of circuit-table row `i` — the low 8 bytes of the (view_id, sub)
/// compound PK, native-LE (the client decodes the OPK PK region on receive).
fn circuit_row_vid(batch: &ZSetBatch, i: usize) -> u64 {
    u64::from_le_bytes(batch.pks.get_bytes(i)[0..8].try_into().unwrap())
}

/// Fixed id → name for the circuit opcodes (mirrors `OPCODE_*`, `wire/circuit.rs`).
/// Opcode 7 is the sink; 25 is the join-trace integral — kept distinct in the dump.
fn opcode_name(op: u64) -> &'static str {
    match op {
        gnitz_wire::OPCODE_FILTER => "FILTER",
        gnitz_wire::OPCODE_NEGATE => "NEGATE",
        gnitz_wire::OPCODE_UNION => "UNION",
        gnitz_wire::OPCODE_JOIN_DELTA_TRACE => "JOIN_DELTA_TRACE",
        gnitz_wire::OPCODE_INTEGRATE => "INTEGRATE_SINK",
        gnitz_wire::OPCODE_REDUCE => "REDUCE",
        gnitz_wire::OPCODE_DISTINCT => "DISTINCT",
        gnitz_wire::OPCODE_SCAN_DELTA => "SCAN_DELTA",
        gnitz_wire::OPCODE_EXCHANGE_SHARD => "EXCHANGE_SHARD",
        gnitz_wire::OPCODE_NULL_EXTEND => "NULL_EXTEND",
        gnitz_wire::OPCODE_INTEGRATE_TRACE => "INTEGRATE_TRACE",
        gnitz_wire::OPCODE_MAP_PROJ => "MAP_PROJ",
        gnitz_wire::OPCODE_MAP_EXPR => "MAP_EXPR",
        gnitz_wire::OPCODE_MAP_HASH_ROW => "MAP_HASH_ROW",
        gnitz_wire::OPCODE_JOIN_DELTA_TRACE_RANGE => "JOIN_DELTA_TRACE_RANGE",
        gnitz_wire::OPCODE_WORKER_FILTER => "WORKER_FILTER",
        gnitz_wire::OPCODE_POSITIVE_PART => "POSITIVE_PART",
        other => panic!("canonical_circuit_dump: unknown opcode {other}"),
    }
}

/// Fixed id → name for the node-column kinds (mirrors `NODE_COL_KIND_*`).
fn kind_name(kind: u64) -> &'static str {
    match kind {
        gnitz_wire::NODE_COL_KIND_GROUP => "GROUP",
        gnitz_wire::NODE_COL_KIND_SHARD => "SHARD",
        gnitz_wire::NODE_COL_KIND_PROJ => "PROJ",
        gnitz_wire::NODE_COL_KIND_NULL_EXT => "NULL_EXT",
        gnitz_wire::NODE_COL_KIND_AGG_SPEC => "AGG_SPEC",
        gnitz_wire::NODE_COL_KIND_BRANCH_ID => "BRANCH_ID",
        gnitz_wire::NODE_COL_KIND_REINDEX => "REINDEX",
        gnitz_wire::NODE_COL_KIND_RANGE_JOIN => "RANGE_JOIN",
        gnitz_wire::NODE_COL_KIND_GLOBAL_GROUND => "GLOBAL_GROUND",
        gnitz_wire::NODE_COL_KIND_REDUCE_OUT_KEY => "REDUCE_OUT_KEY",
        gnitz_wire::NODE_COL_KIND_SCAN_BOUND => "SCAN_BOUND",
        gnitz_wire::NODE_COL_KIND_ROUTE_KEY => "ROUTE_KEY",
        other => panic!("canonical_circuit_dump: unknown node-column kind {other}"),
    }
}

/// DFS pre-order from `start`, recursing children in **port order**, assigning
/// each first-visited node its `#N` index. A revisited node (DAG sharing) keeps
/// its first index. Cycle-free by construction; the visited guard makes DAG
/// sharing terminate.
fn dfs_index(start: u64, vc: &ViewCircuit, index_of: &mut HashMap<u64, usize>, order: &mut Vec<u64>) {
    if index_of.contains_key(&start) {
        return;
    }
    index_of.insert(start, order.len());
    order.push(start);
    if let Some(children) = vc.inputs.get(&start) {
        for &(_port, src) in children {
            dfs_index(src, vc, index_of, order);
        }
    }
}

/// Render one circuit into `out`, discovering any hidden segments its `Scan*`
/// leaves reference (appended to `seg_order`/`seg_index` in first-reach `#N`
/// order). Base sources render `base:<name>`; segment sources `seg:<idx>`.
fn render_circuit(
    vc: &ViewCircuit,
    views: &BTreeMap<u64, ViewCircuit>,
    base_by_id: &HashMap<u64, &str>,
    seg_index: &mut HashMap<u64, usize>,
    seg_order: &mut Vec<u64>,
    out: &mut String,
) {
    // The sink is the unique IntegrateSink (opcode 7); IntegrateTrace is 25.
    let sinks: Vec<u64> = vc
        .nodes
        .iter()
        .filter(|(_, (op, _))| *op == gnitz_wire::OPCODE_INTEGRATE)
        .map(|(nid, _)| *nid)
        .collect();
    assert_eq!(
        sinks.len(),
        1,
        "canonical_circuit_dump: expected exactly one IntegrateSink (opcode 7), got {}",
        sinks.len()
    );

    let mut index_of: HashMap<u64, usize> = HashMap::new();
    let mut order: Vec<u64> = Vec::new();
    dfs_index(sinks[0], vc, &mut index_of, &mut order);
    assert_eq!(
        index_of.len(),
        vc.nodes.len(),
        "canonical_circuit_dump: {} node(s) unreachable from the sink — off-cone nodes are undefined",
        vc.nodes.len() - index_of.len()
    );

    for (n, nid) in order.iter().enumerate() {
        let (opcode, src) = vc.nodes[nid];
        let mut line = format!("#{n} {}", opcode_name(opcode));
        if let Some(params) = vc.params.get(nid) {
            if !params.is_empty() {
                let ps: Vec<String> = params
                    .iter()
                    .map(|(k, p, v1, v2)| format!("({},{},{},{})", kind_name(*k), p, v1, v2))
                    .collect();
                line.push_str(&format!(" params:[{}]", ps.join(";")));
            }
        }
        if let Some(id) = src {
            let rendered = if let Some(name) = base_by_id.get(&id) {
                format!("base:{name}")
            } else if views.contains_key(&id) {
                let idx = if let Some(&idx) = seg_index.get(&id) {
                    idx
                } else {
                    let idx = seg_order.len();
                    seg_index.insert(id, idx);
                    seg_order.push(id);
                    idx
                };
                format!("seg:{idx}")
            } else {
                panic!(
                    "canonical_circuit_dump: source_table {id} is neither a listed base table nor a \
                     hidden segment — add it to base_tables"
                );
            };
            line.push_str(&format!(" src:{rendered}"));
        }
        if let Some(children) = vc.inputs.get(nid) {
            let cs: Vec<String> = children
                .iter()
                .map(|(port, src)| format!("#{}@{}", index_of[src], port))
                .collect();
            line.push_str(&format!(" <- ({})", cs.join(",")));
        }
        line.push('\n');
        out.push_str(&line);
    }
}

/// Render `final_vid`'s circuit followed by every transitively-scanned hidden
/// segment, in structural-index order (`final_vid` = seg 0).
fn render_chain(views: &BTreeMap<u64, ViewCircuit>, final_vid: u64, base_tables: &[(&str, u64)]) -> String {
    let base_by_id: HashMap<u64, &str> = base_tables.iter().map(|(n, id)| (*id, *n)).collect();
    let mut seg_index: HashMap<u64, usize> = HashMap::new();
    let mut seg_order: Vec<u64> = vec![final_vid];
    seg_index.insert(final_vid, 0);

    let mut out = String::new();
    let mut i = 0;
    while i < seg_order.len() {
        let vid = seg_order[i];
        let vc = views
            .get(&vid)
            .unwrap_or_else(|| panic!("canonical_circuit_dump: view {vid} has no rows in the circuit tables"));
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&format!("seg {i}:\n"));
        render_circuit(vc, views, &base_by_id, &mut seg_index, &mut seg_order, &mut out);
        i += 1;
    }
    out
}

fn maybe_eprint_dump(dump: &str) {
    if std::env::var_os("GNITZ_DUMP_CIRCUIT").is_some() {
        eprintln!("---8<--- canonical_circuit_dump ---8<---\n{dump}---8<--- end ---8<---");
    }
}

/// Deterministic, emission-order-independent textual dump of a view's whole
/// circuit chain (final view + every hidden segment it transitively scans).
/// `base_tables` maps the test's known base-table names → ids so base scan
/// sources render by NAME; segment sources render by structural segment index.
/// Reads only the three circuit system tables via `client.scan`.
///
/// Set `GNITZ_DUMP_CIRCUIT=1` to `eprintln!` the dump while authoring a sentinel
/// (capture it, paste it as `EXPECTED`); a normal run only returns the string.
pub fn canonical_circuit_dump(client: &mut GnitzClient, final_vid: u64, base_tables: &[(&str, u64)]) -> String {
    let mut views: BTreeMap<u64, ViewCircuit> = BTreeMap::new();

    if let Some(b) = scan_circuit_nodes(client) {
        for i in 0..b.len() {
            let vid = circuit_row_vid(&b, i);
            let nid = circuit_u64(&b, 2, i);
            let opcode = circuit_u64(&b, 3, i);
            // Only the `ScanDelta` leaf (opcode 11) carries a source_table
            // (`encode_op_node`); reading col 4 solely for it sidesteps the
            // nullable-column read entirely.
            let src = if opcode == gnitz_wire::OPCODE_SCAN_DELTA {
                Some(circuit_u64(&b, 4, i))
            } else {
                None
            };
            views.entry(vid).or_default().nodes.insert(nid, (opcode, src));
        }
    }
    if let Some(b) = client.scan(gnitz_wire::CIRCUIT_NODE_COLUMNS_TAB).unwrap().1 {
        for i in 0..b.len() {
            let vid = circuit_row_vid(&b, i);
            let nid = circuit_u64(&b, 2, i);
            let kind = circuit_u64(&b, 3, i);
            let pos = circuit_u64(&b, 4, i);
            let v1 = circuit_u64(&b, 5, i);
            let v2 = circuit_u64(&b, 6, i);
            views
                .entry(vid)
                .or_default()
                .params
                .entry(nid)
                .or_default()
                .push((kind, pos, v1, v2));
        }
    }
    if let Some(b) = client.scan(gnitz_wire::CIRCUIT_EDGES_TAB).unwrap().1 {
        for i in 0..b.len() {
            let vid = circuit_row_vid(&b, i);
            let dst = circuit_u64(&b, 2, i);
            let port = circuit_u64(&b, 3, i);
            let src = circuit_u64(&b, 4, i);
            views
                .entry(vid)
                .or_default()
                .inputs
                .entry(dst)
                .or_default()
                .push((port, src));
        }
    }
    for vc in views.values_mut() {
        vc.normalize();
    }

    let dump = render_chain(&views, final_vid, base_tables);
    maybe_eprint_dump(&dump);
    dump
}

/// Render a single circuit straight from its `Circuit::into_rows()` bundle,
/// with no server round-trip — the order-independence unit test's oracle. Shares
/// the exact rendering core (`render_chain`) with [`canonical_circuit_dump`], so
/// a byte-identical result across two node-creation orders proves the dump
/// depends only on structure.
pub fn canonical_circuit_dump_from_rows(
    view_id: u64,
    rows: &gnitz_core::CircuitRows,
    base_tables: &[(&str, u64)],
) -> String {
    let mut vc = ViewCircuit::default();
    for (nid, opcode, src, _blob) in &rows.nodes {
        vc.nodes.insert(*nid, (*opcode, *src));
    }
    for (nid, kind, pos, v1, v2) in &rows.node_columns {
        vc.params.entry(*nid).or_default().push((*kind, *pos as u64, *v1, *v2));
    }
    for (dst, port, src) in &rows.edges {
        vc.inputs.entry(*dst).or_default().push((*port as u64, *src));
    }
    vc.normalize();
    let mut views = BTreeMap::new();
    views.insert(view_id, vc);
    render_chain(&views, view_id, base_tables)
}
