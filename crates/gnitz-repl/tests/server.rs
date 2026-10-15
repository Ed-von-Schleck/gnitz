#![cfg(feature = "integration")]

//! The shell against a real server: scripts, the catalog snapshot completion
//! and `\d` read, and `\watch` following a view's delta feed.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gnitz_repl::render::{Format, Settings};
use gnitz_repl::shell::{Flow, Options, Shell};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::{RelClass, ViewBudget};

const WORKERS: usize = 4;
const PATIENCE: Duration = Duration::from_secs(30);

fn shell(sock: &str, interrupt: Option<Arc<AtomicBool>>) -> Shell {
    Shell::connect(Options {
        target: sock.to_string(),
        schema: "public".to_string(),
        settings: Settings {
            format: Format::Csv,
            ..Settings::default()
        },
        interactive: false,
        stop_on_error: true,
        interrupt,
    })
    .expect("connect")
}

/// Run `script`, its results written to `out`, and read them back.
fn results(shell: &mut Shell, out: &Path, script: &str) -> String {
    shell.output_to_file(out.to_str().unwrap()).unwrap();
    shell.run_script(script).expect("script runs");
    shell.output_to_stdout();
    std::fs::read_to_string(out).unwrap()
}

/// Wait until `path` holds `needle`, or fail.
fn wait_for(path: &Path, needle: &str) -> String {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        if text.contains(needle) {
            return text;
        }
        assert!(Instant::now() < deadline, "no {needle:?} in:\n{text}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_script_runs_statements_and_meta_commands_in_order() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.csv");
    let mut sh = shell(server.sock_path(), None);
    let text = results(
        &mut sh,
        &out,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, price DECIMAL(10, 2), day DATE);\n\
         INSERT INTO t VALUES (1, 'a', 1.50, '2024-01-02'), (2, NULL, 2.00, '1969-12-31');\n\
         \\pset null '-'\n\
         SELECT * FROM t ORDER BY id;\n\
         SELECT name FROM t WHERE id = 3",
    );
    assert_eq!(
        text,
        "id,name,price,day\n1,a,1.50,2024-01-02\n2,,2.00,1969-12-31\nname\n"
    );
}

#[test]
fn a_bag_result_repeats_each_row_by_its_weight() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.csv");
    let mut sh = shell(server.sock_path(), None);
    let text = results(
        &mut sh,
        &out,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, kind BIGINT);\n\
         INSERT INTO t VALUES (1, 7), (2, 7), (3, 8);\n\
         SELECT kind FROM t;",
    );
    let mut lines: Vec<&str> = text.lines().collect();
    lines.sort();
    assert_eq!(lines, ["7", "7", "8", "kind"]);
}

#[test]
fn a_failed_statement_stops_the_script() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.csv");
    let mut sh = shell(server.sock_path(), None);
    sh.output_to_file(out.to_str().unwrap()).unwrap();
    assert!(sh.run_script("SELECT * FROM missing; SELECT 1 AS one;").is_err());
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "");
}

#[test]
fn the_catalog_snapshot_sees_relations_columns_and_indexes() {
    let server = ServerHandle::start_n(WORKERS);
    let mut sh = shell(server.sock_path(), None);
    sh.run_script(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, kind BIGINT NOT NULL, note TEXT);\n\
         CREATE INDEX t_kind ON t (kind);\n\
         CREATE TABLE ev (id BIGINT PRIMARY KEY, kind BIGINT) WITH (stream = true);\n\
         CREATE VIEW v WITH (delta = '1 MB') AS SELECT kind, COUNT(*) AS n FROM ev GROUP BY kind;",
    )
    .unwrap();
    sh.refresh_catalog();
    sh.with_catalog(|c| {
        let t = c.find("public", "t").expect("t");
        assert_eq!(t.class, RelClass::Table);
        let cols: Vec<(&str, String, bool)> = t
            .columns
            .iter()
            .map(|c| (c.name.as_str(), gnitz_sql::sql_type_name(c.ty), c.is_nullable))
            .collect();
        assert_eq!(
            cols,
            [
                ("id", "BIGINT".to_string(), false),
                ("kind", "BIGINT".to_string(), false),
                ("note", "TEXT".to_string(), true)
            ]
        );
        assert_eq!(t.pk, ["id"]);
        assert_eq!(t.indexes.len(), 1);
        assert_eq!(
            (t.indexes[0].name.as_str(), t.indexes[0].columns.as_slice()),
            ("t_kind", &["kind".to_string()][..])
        );
        assert_eq!(c.find("public", "ev").expect("ev").class, RelClass::Stream);
        let v = c.find("public", "v").expect("v");
        assert_eq!(v.class, RelClass::FedView);
        assert_eq!(v.budget, ViewBudget::from_row(0, 1 << 20).unwrap());
        assert_eq!(c.find("PUBLIC", "V").map(|r| r.id), Some(v.id));
        assert!(c.schemas.contains(&"_system".to_string()));
        assert_eq!(
            c.find("_system", "tables").expect("_system.tables").class,
            RelClass::Catalog
        );
    });
}

#[test]
fn watch_prints_the_view_then_its_deltas_until_interrupted() {
    let server = ServerHandle::start_n(WORKERS);
    let sock = server.sock_path().to_string();
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("watch.csv");

    let mut writer = shell(&sock, None);
    writer
        .run_script(
            "CREATE TABLE e (id BIGINT PRIMARY KEY, kind BIGINT, amount BIGINT);\n\
             CREATE VIEW hot WITH (delta = '1 MB') AS SELECT kind, SUM(amount) AS total FROM e GROUP BY kind;\n\
             INSERT INTO e VALUES (1, 1, 10), (2, 2, 7);",
        )
        .unwrap();

    let flag = Arc::new(AtomicBool::new(false));
    let mut watcher = shell(&sock, Some(Arc::clone(&flag)));
    watcher.output_to_file(out.to_str().unwrap()).unwrap();
    let watching = std::thread::spawn(move || watcher.meta("\\watch hot"));

    let snapshot = wait_for(&out, "+1,2,7");
    assert!(
        snapshot.starts_with("weight,kind,total\n") && snapshot.contains("+1,1,10"),
        "{snapshot}"
    );

    writer.run_script("INSERT INTO e VALUES (3, 2, 100);").unwrap();
    let text = wait_for(&out, "+1,2,107");
    assert!(text.contains("-1,2,7"), "the old aggregate is retracted:\n{text}");
    assert_eq!(text.matches("weight,").count(), 1, "one CSV header:\n{text}");
    assert_eq!(
        text.matches("1,10").count(),
        1,
        "an unchanged group emits nothing:\n{text}"
    );

    flag.store(true, Ordering::SeqCst);
    let flow = watching.join().expect("watch thread").expect("watch ends cleanly");
    assert_eq!(flow, Flow::Continue);
}

#[test]
fn watch_refuses_a_view_without_a_delta_feed() {
    let server = ServerHandle::start_n(WORKERS);
    let mut sh = shell(server.sock_path(), None);
    sh.run_script(
        "CREATE TABLE e (id BIGINT PRIMARY KEY, kind BIGINT);\n\
         CREATE VIEW plain AS SELECT kind FROM e;",
    )
    .unwrap();
    assert!(sh.meta("\\watch plain").is_err());
}
