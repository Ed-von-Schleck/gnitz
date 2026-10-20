#![cfg(feature = "integration")]

//! A dump of a real server restored into another: every table's rows, every
//! view's, the statements that recreate them, and the SERIAL positions.

use std::path::Path;
use std::sync::Arc;

use gnitz_core::{block_on, BatchAppender, Catalog, GnitzClient, RelName, ZSetBatch};
use gnitz_sql::SqlResult;
use gnitz_test_harness::ServerHandle;
use gnitz_wire::{Circuit, OpNode, ReadBound, RelClass, WireConflictMode};
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
        bl BLOB, t TINYINT, tu TINYINT UNSIGNED, s SMALLINT, iu INT UNSIGNED, bu BIGINT UNSIGNED,
        PRIMARY KEY (a, b)) WITH (compress = true) CLUSTER BY a;
    CREATE TABLE tree (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES tree (id), label TEXT);
    CREATE TABLE reps (id BIGINT PRIMARY KEY, v BIGINT) WITH (replicated = true);
    CREATE TABLE events (id BIGINT PRIMARY KEY, kind BIGINT, amount BIGINT) WITH (stream = true);
    CREATE TABLE alt (id BIGINT PRIMARY KEY, gone BIGINT, keep TEXT);
    CREATE TABLE nothing (id BIGINT PRIMARY KEY, v TEXT);

    CREATE VIEW sales.totals AS
        SELECT c.name, SUM(o.amount) AS total, COUNT(*) AS n
        FROM sales.orders o JOIN sales.customers c ON o.customer = c.id GROUP BY c.name;
    CREATE VIEW sales.by_one AS SELECT id FROM sales.orders WHERE customer = 1;
    CREATE VIEW hot WITH (delta = '1 MB') AS SELECT kind, SUM(amount) AS s FROM events GROUP BY kind;
    CREATE VIEW deep AS
        WITH r AS (SELECT id, parent FROM tree WHERE parent IS NOT NULL)
        SELECT r.id, r.parent, t.label FROM r JOIN tree t ON r.parent = t.id;
    CREATE VIEW big_kinds (aa, bb) AS SELECT a, b FROM kinds WHERE a > 0;
    CREATE VIEW capped WITH (capacity = '4 MB') AS SELECT id, label FROM tree;
    CREATE VIEW orphans AS SELECT id FROM tree WHERE NOT EXISTS (SELECT 1 FROM reps WHERE reps.id = tree.id);
    CREATE VIEW parents AS SELECT parent FROM tree;
    CREATE VIEW children AS SELECT id, parent FROM tree WHERE parent IS NOT NULL;
    CREATE INDEX children_parent ON children (parent);
    CREATE VIEW grandchildren AS SELECT id FROM children WHERE parent > 10;

    INSERT INTO sales.customers VALUES (1, 'ann', 11), (2, 'bob', NULL), (3, 'cy', 33);
    INSERT INTO sales.orders (customer, amount, placed, at, flag) VALUES
        (1, 12.50, '2026-01-02', '2026-01-02 03:04:05.678901', true),
        (1, -0.01, NULL, NULL, false), (2, 99999.99, '1970-01-01', '1969-12-31 23:59:59', NULL),
        (3, NULL, '2999-12-31', '2026-10-16 00:00:00', true);
    INSERT INTO kinds (a, b, u, h, uh, f, d, t, tu, s, iu, bu) VALUES
        (-2147483648, 0, '01234567-89ab-cdef-0123-456789abcdef', -170141183460469231731687303715884105728,
         340282366920938463463374607431768211455, -0.0, 1.5e300, -128,
         255, -32768, 4294967295, 18446744073709551615),
        (2147483647, 65535, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
        (7, 7, '00000000-0000-0000-0000-000000000000', 1, 1, 3.25, -2.5, 127, 0, 32767, 0, 0);
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

/// The Z-set `sql` answers: each row's visible cells as bytes and its weight, sorted.
fn rows(client: &mut GnitzClient, sql: &str) -> Vec<(Vec<Option<Vec<u8>>>, i64)> {
    let Some(SqlResult::Rows(reply)) = exec(client, sql).pop() else {
        panic!("{sql} answered no rows")
    };
    let (schema, batch) = (&reply.schema, &reply.batch);
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

/// Every user relation's `schema.name` and the properties its row states.
fn props(catalog: &Catalog) -> Vec<(String, gnitz_core::RelProps)> {
    let mut out: Vec<_> = catalog
        .relations
        .iter()
        .filter(|r| r.class() != RelClass::Catalog)
        .map(|r| (r.qualified(), r.props))
        .collect();
    out.sort_by(|x, y| x.0.cmp(&y.0));
    out
}

fn dump(client: &mut GnitzClient, dir: &Path) {
    block_on(gnitz_dump::dump(client, dir)).unwrap();
}

fn restore_err(client: &mut GnitzClient, dir: &Path) -> String {
    block_on(gnitz_dump::restore(client, dir)).unwrap_err().0
}

/// Whether the circuit of view `view` reads its source through a bound.
fn scans_bounded(client: &mut GnitzClient, view: &RelName) -> bool {
    let catalog = block_on(client.catalog()).unwrap();
    let id = catalog.get(view).unwrap().id;
    let found = rows(
        client,
        &format!("SELECT circuit FROM _system.circuits WHERE view_id = {id}"),
    );
    let [(cells, 1)] = &found[..] else {
        panic!("{view}: {} circuit rows", found.len())
    };
    let circuit = Circuit::decode(cells[0].as_deref().unwrap()).unwrap();
    let mut bounds = circuit.nodes().iter().filter_map(|n| match &n.op {
        OpNode::ScanDelta { bound, .. } => Some(bound),
        _ => None,
    });
    let bound = bounds.next().expect("a scan");
    assert!(bounds.next().is_none(), "{view}: one scan");
    *bound != ReadBound::None
}

/// A restore recreates every relation, index, property and SERIAL position,
/// so the restored database dumps to the same text, and every table and view
/// holds the dumped one's rows at their weights. It refuses a database holding
/// any name the dump creates.
#[test]
fn a_restored_database_is_the_dumped_one() {
    let src = ServerHandle::start_n(4);
    let mut a = GnitzClient::connect(src.sock_path()).unwrap();
    exec(&mut a, SCRIPT);
    push_files(&mut a);
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    dump(&mut a, dir);
    assert!(restore_err(&mut a, dir).contains("already holds schema sales"));
    let err = block_on(gnitz_dump::dump(&mut a, dir)).unwrap_err();
    assert!(err.0.contains("not empty"), "{err}");

    let dst = ServerHandle::start_n(2);
    let mut b = GnitzClient::connect(dst.sock_path()).unwrap();
    // An index name is the database's, whatever relation holds it. A refused
    // restore creates nothing, or the one after it would be refused too.
    for (conflict, drop, refusal) in [
        (
            "CREATE TABLE forest (id BIGINT PRIMARY KEY)",
            "DROP TABLE forest",
            "already holds public.forest",
        ),
        (
            "CREATE TABLE other (id BIGINT PRIMARY KEY, w BIGINT); CREATE INDEX orders_by_customer ON other (w)",
            "DROP TABLE other",
            "already holds index orders_by_customer",
        ),
    ] {
        exec(&mut b, conflict);
        let err = restore_err(&mut b, dir);
        assert!(err.contains(refusal), "{err}");
        exec(&mut b, drop);
    }
    block_on(gnitz_dump::restore(&mut b, dir)).unwrap();

    let again = tempfile::tempdir().unwrap();
    dump(&mut b, again.path());
    for file in ["schema.sql", "post-data.sql", "manifest.json"] {
        let read = |d: &Path| std::fs::read_to_string(d.join(file)).unwrap();
        assert_eq!(read(dir), read(again.path()), "{file}");
    }
    let (ca, cb) = (block_on(a.catalog()).unwrap(), block_on(b.catalog()).unwrap());
    assert_eq!(props(&ca), props(&cb));
    for r in ca.relations.iter() {
        let q = format!("SELECT * FROM {}", gnitz_sql::qualified_name(r));
        let reads_stream = r
            .reads
            .iter()
            .any(|&id| ca.relation(id).is_some_and(|s| s.class() == RelClass::Stream));
        match r.class() {
            RelClass::Catalog | RelClass::Stream => {}
            RelClass::Table => assert_eq!(rows(&mut a, &q), rows(&mut b, &q), "{q}"),
            // A stream holds no rows to dump: a view over one starts as it would
            // after a reboot of the dumped server.
            _ if reads_stream => {
                assert!(!rows(&mut a, &q).is_empty(), "{q}");
                assert!(rows(&mut b, &q).is_empty(), "{q}");
            }
            _ => {
                let got = rows(&mut b, &q);
                assert_eq!(rows(&mut a, &q), got, "{q}");
                assert!(!got.is_empty(), "{q} restored empty");
            }
        }
    }
    // The tables' indexes are created ahead of the views, so a view is planned
    // with the index bound it was planned with first.
    let by_one = RelName::new("sales", "by_one").unwrap();
    assert!(scans_bounded(&mut a, &by_one) && scans_bounded(&mut b, &by_one));
    // Writes and cascades work as they did.
    exec(&mut b, "INSERT INTO sales.orders (customer) VALUES (2)");
    exec(&mut b, "DELETE FROM sales.customers WHERE id = 1");
    assert!(rows(&mut b, "SELECT id FROM sales.orders WHERE customer = 1").is_empty());
}

/// A restore pushes without a duplicate-key check, so a file holding a key
/// twice, which no dump writes, is caught by the rows it leaves.
#[test]
fn a_file_holding_a_key_twice_is_refused() {
    let server = ServerHandle::start_n(2);
    let mut a = GnitzClient::connect(server.sock_path()).unwrap();
    exec(
        &mut a,
        "CREATE TABLE d (id BIGINT PRIMARY KEY, v BIGINT NOT NULL); INSERT INTO d VALUES (1, 10), (2, 20)",
    );
    let dir = tempfile::tempdir().unwrap();
    dump(&mut a, dir.path());
    exec(&mut a, "DROP TABLE d");

    let file = dir.path().join("data/0.parquet");
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

    let err = restore_err(&mut a, dir.path());
    assert!(err.contains("3 rows, of which 2 have distinct keys"), "{err}");
}

/// A row's parent, given its id.
type Parent = fn(i64) -> Option<i64>;

/// Tables of `(id, parent, body)` rows that together take more than one restore
/// push each, a row's `parent` the given function of its id: one whose rows
/// reference larger ids, so key order puts children before their parents; one
/// cycle through every row, which only one push can carry; and one referencing
/// nothing.
const TABLES: [(&str, &str, Parent); 3] = [
    ("chain", "REFERENCES chain (id)", |id| (id + 1 < ROWS).then_some(id + 1)),
    ("ring", "REFERENCES ring (id)", |id| Some((id + 1) % ROWS)),
    ("plain", "", Some),
];

/// Rows per table, each about 1 KB in a push, and more than one dump page.
const ROWS: i64 = 20_000;

/// A table that references itself and takes more than one push is restored in
/// an order where every row's parents come first, and the rows of a reference
/// cycle in one push; one that references nothing in pushes of whole pages.
#[test]
fn tables_larger_than_a_push_restore() {
    let src = ServerHandle::start_n(4);
    let mut a = GnitzClient::connect(src.sock_path()).unwrap();
    for (name, refs, parent) in TABLES {
        exec(
            &mut a,
            &format!("CREATE TABLE {name} (id BIGINT PRIMARY KEY, parent BIGINT {refs}, body TEXT)"),
        );
        let desc = block_on(a.resolve_relation(&RelName::new("public", name).unwrap())).unwrap();
        let mut batch = ZSetBatch::new(&desc.schema);
        let mut app = BatchAppender::new(&mut batch);
        for id in 0..ROWS {
            let r = app.add_row(id as u128, 1);
            match parent(id) {
                Some(p) => r.int_val(i128::from(p)),
                None => r.null(),
            };
            r.str_val(&format!("{id:0>1000}"));
        }
        block_on(a.push(&*desc, &desc.schema, batch, WireConflictMode::Error)).unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    dump(&mut a, dir.path());
    let dst = ServerHandle::start_n(3);
    let mut b = GnitzClient::connect(dst.sock_path()).unwrap();
    block_on(gnitz_dump::restore(&mut b, dir.path())).unwrap();
    for (name, ..) in TABLES {
        let q = format!("SELECT * FROM {name}");
        assert_eq!(rows(&mut a, &q), rows(&mut b, &q), "{name}");
    }
}
