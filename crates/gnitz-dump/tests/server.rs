#![cfg(feature = "integration")]

//! A dump of a real server restored into another: every table's rows, every
//! view's, the statements that recreate them, and the SERIAL positions.

use std::path::Path;
use std::sync::Arc;

use gnitz_core::{block_on, BatchAppender, Catalog, GnitzClient, RelName, RelProps, ScanReply, ZSetBatch};
use gnitz_sql::SqlResult;
use gnitz_test_harness::ServerHandle;
use gnitz_wire::{Circuit, OpNode, ReadBound, WireConflictMode};
use parquet::column::writer::ColumnWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;

/// Every kind of thing a dump carries, and the renames it has to survive.
const SCRIPT: &str = "
    CREATE SCHEMA sales;
    CREATE TABLE sales.customers (
        id BIGINT PRIMARY KEY, name TEXT NOT NULL, code BIGINT,
        CONSTRAINT customers_code UNIQUE (code));
    CREATE TABLE sales.orders (
        id BIGSERIAL PRIMARY KEY,
        customer BIGINT NOT NULL REFERENCES sales.customers (id) ON DELETE CASCADE,
        amount DECIMAL(12, 2), placed DATE, at TIMESTAMP, flag BOOLEAN);
    CREATE INDEX orders_by_customer ON sales.orders (customer);
    CREATE TABLE kinds (
        a INT, b SMALLINT UNSIGNED, u UUID, h HUGEINT, uh UHUGEINT, f FLOAT, d DOUBLE,
        bl BLOB, t TINYINT, PRIMARY KEY (a, b)) WITH (compress = true) CLUSTER BY a;
    CREATE TABLE tree (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES tree (id), label TEXT);
    CREATE TABLE reps (id BIGINT PRIMARY KEY, v BIGINT) WITH (replicated = true);
    CREATE TABLE events (id BIGINT PRIMARY KEY, kind BIGINT, amount BIGINT) WITH (stream = true);
    CREATE TABLE alt (id BIGINT PRIMARY KEY, gone BIGINT, keep TEXT);

    CREATE VIEW sales.totals AS
        SELECT c.name, SUM(o.amount) AS total, COUNT(*) AS n
        FROM sales.orders o JOIN sales.customers c ON o.customer = c.id GROUP BY c.name;
    CREATE VIEW hot WITH (delta = '1 MB') AS SELECT kind, SUM(amount) AS s FROM events GROUP BY kind;
    CREATE VIEW deep AS
        WITH r AS (SELECT id, parent FROM tree WHERE parent IS NOT NULL)
        SELECT r.id, r.parent, t.label FROM r JOIN tree t ON r.parent = t.id;
    CREATE VIEW big_kinds (aa, bb) AS SELECT a, b FROM kinds WHERE a > 0;
    CREATE VIEW capped WITH (capacity = '4 MB') AS SELECT id, label FROM tree;
    CREATE VIEW orphans AS SELECT id FROM tree WHERE NOT EXISTS (SELECT 1 FROM reps WHERE reps.id = tree.id);
    CREATE VIEW children AS SELECT id, parent FROM tree WHERE parent IS NOT NULL;
    CREATE INDEX children_parent ON children (parent);

    INSERT INTO sales.customers VALUES (1, 'ann', 11), (2, 'bob', NULL), (3, 'cy', 33);
    INSERT INTO sales.orders (customer, amount, placed, at, flag) VALUES
        (1, 12.50, '2026-01-02', '2026-01-02 03:04:05.678901', true),
        (1, -0.01, NULL, NULL, false), (2, 99999.99, '1970-01-01', '1969-12-31 23:59:59', NULL),
        (3, NULL, '2999-12-31', '2026-10-16 00:00:00', true);
    INSERT INTO kinds (a, b, u, h, uh, f, d, t) VALUES
        (-2147483648, 0, '01234567-89ab-cdef-0123-456789abcdef', -170141183460469231731687303715884105728,
         340282366920938463463374607431768211455, -0.0, 1.5e300, -128),
        (2147483647, 65535, NULL, NULL, NULL, NULL, NULL, NULL),
        (7, 7, '00000000-0000-0000-0000-000000000000', 1, 1, 3.25, -2.5, 127);
    CREATE TABLE files (id BIGINT PRIMARY KEY, body BLOB, note BYTEA NOT NULL);
    INSERT INTO tree VALUES (100, NULL, 'root'), (50, 100, 'mid'), (10, 50, 'leaf'), (60, 100, 'other'), (5, 5, 'self');
    INSERT INTO reps VALUES (5, 1), (60, 2), (1000, 3);
    INSERT INTO events VALUES (1, 1, 10), (2, 1, 20), (3, 2, 5);
    INSERT INTO alt VALUES (1, 11, 'one'), (2, 22, NULL);
    ALTER TABLE alt DROP COLUMN gone;
    ALTER TABLE alt ADD COLUMN extra BIGINT;
    INSERT INTO alt VALUES (3, 'three', 33);

    ALTER TABLE tree RENAME TO forest;
    ALTER TABLE forest RENAME COLUMN label TO title;
    ALTER TABLE sales.customers RENAME COLUMN name TO full_name;
";

/// `files` rows SQL cannot write: blobs, pushed as the binary protocol carries them.
fn push_files(client: &mut GnitzClient) {
    let desc = block_on(client.resolve_relation(&RelName::new("public", "files").unwrap())).unwrap();
    let mut batch = ZSetBatch::new(&desc.schema);
    let mut app = BatchAppender::new(&mut batch);
    app.add_row(1, 1).bytes_val(&[0, 255, 7]).bytes_val(b"");
    app.add_row(2, 1).null().bytes_val(&[0xab; 40]);
    block_on(client.push(&*desc, &desc.schema, batch, WireConflictMode::Error)).unwrap();
}

fn exec(client: &mut GnitzClient, sql: &str) -> Vec<SqlResult> {
    block_on(gnitz_sql::execute(client, "public", sql)).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The Z-set `sql` answers: each row's cells as bytes and its weight, sorted.
fn rows(client: &mut GnitzClient, sql: &str) -> Vec<(Vec<Option<Vec<u8>>>, i64)> {
    let Some(SqlResult::Rows(reply)) = exec(client, sql).pop() else {
        panic!("{sql} answered no rows")
    };
    decode(&reply)
}

fn decode(reply: &ScanReply) -> Vec<(Vec<Option<Vec<u8>>>, i64)> {
    let schema = &reply.schema;
    let batch = &reply.batch;
    let mut out: Vec<_> = (0..batch.len())
        .map(|i| {
            let cells = schema
                .visible_columns()
                .map(|(ci, c)| {
                    let loc = schema.layout().locate(ci);
                    (!loc.is_null(batch, i)).then(|| match c.ty.tc.is_german_string() {
                        true => loc.content(batch, i).into_owned(),
                        false => {
                            let mut scratch = [0u8; 16];
                            loc.native_le_bytes(batch, i, &mut scratch).to_vec()
                        }
                    })
                })
                .collect();
            (cells, batch.weights[i])
        })
        .collect();
    out.sort();
    out
}

/// Every user relation's `schema.name`, and the statement that creates it.
fn statements(catalog: &Catalog) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = catalog
        .relations
        .iter()
        .filter(|r| r.schema != "_system")
        .map(|r| {
            let stmt = match r.props {
                RelProps::Table(_) => gnitz_sql::create_table_statement(catalog, r),
                RelProps::View(_) => gnitz_sql::create_view_statement(catalog, r),
            };
            (r.qualified(), stmt.unwrap())
        })
        .collect();
    out.sort();
    out
}

fn dump(client: &mut GnitzClient, dir: &Path) -> gnitz_dump::Summary {
    block_on(gnitz_dump::dump(client, dir)).unwrap()
}

#[test]
fn a_restored_database_is_the_dumped_one() {
    let src = ServerHandle::start_n(4);
    let mut a = GnitzClient::connect(src.sock_path()).unwrap();
    exec(&mut a, SCRIPT);
    push_files(&mut a);
    let dir = tempfile::tempdir().unwrap();
    let dumped = dump(&mut a, dir.path());
    assert_eq!(
        (dumped.schemas, dumped.tables, dumped.views),
        (1, 8, 7),
        "schemas, tables and streams, views"
    );

    let dst = ServerHandle::start_n(2);
    let mut b = GnitzClient::connect(dst.sock_path()).unwrap();
    let restored = block_on(gnitz_dump::restore(&mut b, dir.path())).unwrap();
    assert_eq!(restored, dumped);

    let (ca, cb) = (block_on(a.catalog()).unwrap(), block_on(b.catalog()).unwrap());
    assert_eq!(statements(&ca), statements(&cb));
    for rel in [
        "sales.customers",
        "sales.orders",
        "kinds",
        "forest",
        "reps",
        "alt",
        "files",
    ] {
        let q = format!("SELECT * FROM {rel}");
        assert_eq!(rows(&mut a, &q), rows(&mut b, &q), "{rel}");
    }
    for view in ["sales.totals", "deep", "big_kinds", "capped", "orphans", "children"] {
        let q = format!("SELECT * FROM {view}");
        let got = rows(&mut b, &q);
        assert_eq!(rows(&mut a, &q), got, "{view}");
        assert!(!got.is_empty(), "{view} restored empty");
    }
    // A stream holds no rows to dump: a view over one starts as it would after
    // a reboot of the dumped server.
    assert!(!rows(&mut a, "SELECT * FROM hot").is_empty());
    assert!(rows(&mut b, "SELECT * FROM hot").is_empty());
    let children = cb.get(&RelName::new("public", "children").unwrap()).unwrap();
    assert_eq!(
        children.indexes.iter().map(|i| i.name.as_str()).collect::<Vec<_>>(),
        ["children_parent"]
    );

    // The sequence stands where the dump left it, so a new row takes a fresh id.
    let before = rows(&mut b, "SELECT id FROM sales.orders");
    exec(&mut b, "INSERT INTO sales.orders (customer) VALUES (2)");
    let after = rows(&mut b, "SELECT id FROM sales.orders");
    assert_eq!(after.len(), before.len() + 1);
    // Writes and cascades work as they did.
    exec(&mut b, "DELETE FROM sales.customers WHERE id = 1");
    assert!(rows(&mut b, "SELECT id FROM sales.orders WHERE customer = 1").is_empty());
}

#[test]
fn a_restore_refuses_a_database_holding_its_names() {
    let src = ServerHandle::start_n(2);
    let mut a = GnitzClient::connect(src.sock_path()).unwrap();
    exec(
        &mut a,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT); CREATE INDEX t_v ON t (v); INSERT INTO t VALUES (1, 2)",
    );
    let dir = tempfile::tempdir().unwrap();
    dump(&mut a, dir.path());
    let err = block_on(gnitz_dump::restore(&mut a, dir.path())).unwrap_err();
    assert!(err.0.contains("already holds public.t"), "{err}");
    // A non-empty directory is no place for a dump.
    let err = block_on(gnitz_dump::dump(&mut a, dir.path())).unwrap_err();
    assert!(err.0.contains("not empty"), "{err}");

    // An index name is the database's, whatever relation holds it.
    let dst = ServerHandle::start_n(2);
    let mut b = GnitzClient::connect(dst.sock_path()).unwrap();
    exec(
        &mut b,
        "CREATE TABLE other (id BIGINT PRIMARY KEY, w BIGINT); CREATE INDEX t_v ON other (w)",
    );
    let before = statements(&block_on(b.catalog()).unwrap());
    let err = block_on(gnitz_dump::restore(&mut b, dir.path())).unwrap_err();
    assert!(err.0.contains("already holds index t_v"), "{err}");
    assert_eq!(statements(&block_on(b.catalog()).unwrap()), before);
}

/// Whether the circuit of view `name` reads its source through a bound.
fn scans_bounded(client: &mut GnitzClient, name: &str) -> bool {
    let catalog = block_on(client.catalog()).unwrap();
    let id = catalog.get(&RelName::new("public", name).unwrap()).unwrap().id;
    let found = rows(
        client,
        &format!("SELECT circuit FROM _system.circuits WHERE view_id = {id}"),
    );
    let [(cells, 1)] = &found[..] else {
        panic!("{name}: {} circuit rows", found.len())
    };
    let circuit = Circuit::decode(cells[0].as_deref().unwrap()).unwrap();
    let mut bounds = circuit.nodes().iter().filter_map(|n| match &n.op {
        OpNode::ScanDelta { bound, .. } => Some(bound),
        _ => None,
    });
    let bound = bounds.next().expect("a scan");
    assert!(bounds.next().is_none(), "{name}: one scan");
    *bound != ReadBound::None
}

/// The indexes of the views' sources are created ahead of the views, so a view
/// is planned with the same index bound it was planned with first.
#[test]
fn a_restored_view_keeps_its_index_bound() {
    let src = ServerHandle::start_n(2);
    let mut a = GnitzClient::connect(src.sock_path()).unwrap();
    exec(
        &mut a,
        "CREATE TABLE s (id BIGINT PRIMARY KEY, k BIGINT); CREATE INDEX s_k ON s (k);
         CREATE VIEW sv AS SELECT id FROM s WHERE k = 7",
    );
    assert!(scans_bounded(&mut a, "sv"));
    let dir = tempfile::tempdir().unwrap();
    dump(&mut a, dir.path());
    let dst = ServerHandle::start_n(2);
    let mut b = GnitzClient::connect(dst.sock_path()).unwrap();
    block_on(gnitz_dump::restore(&mut b, dir.path())).unwrap();
    assert!(scans_bounded(&mut b, "sv"));
}

/// A restore pushes without a duplicate-key check, so a file holding a key
/// twice, which no dump writes, is caught by the rows it leaves.
#[test]
fn a_file_holding_a_key_twice_is_refused() {
    let src = ServerHandle::start_n(2);
    let mut a = GnitzClient::connect(src.sock_path()).unwrap();
    exec(
        &mut a,
        "CREATE TABLE d (id BIGINT PRIMARY KEY, v BIGINT NOT NULL); INSERT INTO d VALUES (1, 10), (2, 20)",
    );
    let dir = tempfile::tempdir().unwrap();
    dump(&mut a, dir.path());

    let manifest_path = dir.path().join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
    let entry = &mut manifest["tables"][0];
    assert_eq!(entry["name"], "d");
    let file = dir.path().join(entry["file"].as_str().unwrap());
    entry["rows"] = 3.into();
    std::fs::write(&manifest_path, manifest.to_string()).unwrap();

    let reader = SerializedFileReader::new(std::fs::File::open(&file).unwrap()).unwrap();
    let meta = reader.metadata().file_metadata();
    let root = Arc::new(meta.schema().clone());
    let props = WriterProperties::builder()
        .set_key_value_metadata(meta.key_value_metadata().cloned())
        .build();
    drop(reader);
    let mut w = SerializedFileWriter::new(std::fs::File::create(&file).unwrap(), root, Arc::new(props)).unwrap();
    let mut group = w.next_row_group().unwrap();
    for values in [[1i64, 2, 1], [10, 20, 30]] {
        let mut col = group.next_column().unwrap().unwrap();
        let ColumnWriter::Int64ColumnWriter(c) = col.untyped() else {
            panic!("two BIGINT columns")
        };
        c.write_batch(&values, None, None).unwrap();
        col.close().unwrap();
    }
    group.close().unwrap();
    w.close().unwrap();

    let dst = ServerHandle::start_n(2);
    let mut b = GnitzClient::connect(dst.sock_path()).unwrap();
    let err = block_on(gnitz_dump::restore(&mut b, dir.path())).unwrap_err();
    assert!(err.0.contains("3 rows, of which 2 have distinct keys"), "{err}");
}

/// Rows of `(id, parent, body)`, each about 250 bytes in a push, for `ids`.
fn tree_rows(
    desc: &gnitz_core::RelDescriptor,
    ids: impl Iterator<Item = i64>,
    parent: impl Fn(i64) -> Option<i64>,
) -> ZSetBatch {
    let mut batch = ZSetBatch::new(&desc.schema);
    let mut app = BatchAppender::new(&mut batch);
    for id in ids {
        let r = app.add_row(id as u128, 1);
        match parent(id) {
            Some(p) => r.int_val(i128::from(p)),
            None => r.null(),
        };
        r.str_val(&format!("{id:0>200}"));
    }
    batch
}

/// A table that references itself and takes more than one push is restored in
/// an order where every row's parents come first, and the rows of a reference
/// cycle in one push.
#[test]
fn a_self_referencing_table_larger_than_a_push_restores() {
    const ROWS: i64 = 84_000;
    let src = ServerHandle::start_n(4);
    let mut a = GnitzClient::connect(src.sock_path()).unwrap();
    exec(
        &mut a,
        "CREATE TABLE chain (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES chain (id), body TEXT);
         CREATE TABLE ring (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES ring (id), body TEXT)",
    );
    // Each row's parent has the next larger id, so key order puts children
    // before their parents; pushed parents first.
    let chain = block_on(a.resolve_relation(&RelName::new("public", "chain").unwrap())).unwrap();
    for top in (0..ROWS).rev().step_by(20_000) {
        let batch = tree_rows(&chain, ((top - 19_999).max(0)..=top).rev(), |id| {
            (id + 1 < ROWS).then_some(id + 1)
        });
        block_on(a.push(&*chain, &chain.schema, batch, WireConflictMode::Error)).unwrap();
    }
    // One cycle through every row, which only one push can carry.
    let ring = block_on(a.resolve_relation(&RelName::new("public", "ring").unwrap())).unwrap();
    let batch = tree_rows(&ring, 0..ROWS, |id| Some((id + 1) % ROWS));
    block_on(a.push(&*ring, &ring.schema, batch, WireConflictMode::Error)).unwrap();

    let dir = tempfile::tempdir().unwrap();
    assert_eq!(dump(&mut a, dir.path()).rows, 2 * ROWS as u64);
    let dst = ServerHandle::start_n(3);
    let mut b = GnitzClient::connect(dst.sock_path()).unwrap();
    block_on(gnitz_dump::restore(&mut b, dir.path())).unwrap();
    for table in ["chain", "ring"] {
        let q = format!("SELECT * FROM {table}");
        assert_eq!(rows(&mut a, &q), rows(&mut b, &q), "{table}");
    }
}
