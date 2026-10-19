#![cfg(feature = "integration")]

//! The shell against a real server: scripts, the catalog snapshot completion
//! and `\d` read, and `\watch` following a view's delta feed.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gnitz_cli::render::{Format, Settings};
use gnitz_cli::shell::{Flow, Options, Shell};
use gnitz_core::{RelName, RelProps};
use gnitz_test_harness::ServerHandle;
use gnitz_wire::{RelClass, ViewBudget, ViewProps};

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
    let c = sh.catalog().unwrap();
    let t = c.get(&RelName::new("public", "t").unwrap()).expect("t");
    assert_eq!(t.class(), RelClass::Table);
    let cols: Vec<(&str, String, bool)> = t
        .columns
        .iter()
        .map(|c| {
            (
                c.def.name.as_str(),
                gnitz_sql::sql_type_name(c.def.ty),
                c.def.is_nullable,
            )
        })
        .collect();
    assert_eq!(
        cols,
        [
            ("id", "BIGINT".to_string(), false),
            ("kind", "BIGINT".to_string(), false),
            ("note", "TEXT".to_string(), true)
        ]
    );
    assert_eq!(t.column_names(&t.pk), ["id"]);
    let [ix] = &t.indexes[..] else {
        panic!("{:?}", t.indexes)
    };
    assert_eq!((ix.name.as_str(), t.column_names(&ix.cols)), ("t_kind", vec!["kind"]));
    let ev = c.get(&RelName::new("public", "ev").unwrap()).expect("ev");
    assert_eq!(ev.class(), RelClass::Stream);
    let v = c.get(&RelName::new("public", "v").unwrap()).expect("v");
    assert_eq!(v.class(), RelClass::FedView);
    assert_eq!(
        v.props,
        RelProps::View(ViewProps {
            budget: ViewBudget::from_row(0, 1 << 20).unwrap(),
            compress: false
        })
    );
    assert_eq!(v.reads, [ev.id]);
    assert_eq!(c.get(&RelName::new("PUBLIC", "V").unwrap()).map(|r| r.id), Some(v.id));
    assert!(c.schemas.contains(&"_system".to_string()));
    assert_eq!(
        c.get(&RelName::new("_system", "tables").unwrap())
            .expect("_system.tables")
            .class(),
        RelClass::Catalog
    );
}

/// `\d` reads the catalog as it stands, whichever session changed it: its
/// foreign keys both ways, and what a view reads through its chain.
#[test]
fn describe_shows_keys_and_what_reads_what() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.txt");
    let mut sh = shell(server.sock_path(), None);
    sh.run_script("CREATE TABLE parent (id BIGINT PRIMARY KEY);").unwrap();

    shell(server.sock_path(), None)
        .run_script(
            "CREATE TABLE child (id BIGINT PRIMARY KEY, pid BIGINT REFERENCES parent(id) ON DELETE CASCADE);\n\
             CREATE VIEW joined AS SELECT c.id, p.id AS pid FROM child c JOIN parent p ON c.pid = p.id;",
        )
        .unwrap();

    let parent = results(&mut sh, &out, "\\d parent");
    assert!(parent.contains("Referenced by:\n    public.child\n"), "{parent}");
    assert!(parent.contains("Read by:\n    public.joined\n"), "{parent}");
    let child = results(&mut sh, &out, "\\d child");
    assert!(
        child.contains("Foreign keys:\n    (pid) REFERENCES public.parent(id) ON DELETE CASCADE\n"),
        "{child}"
    );
    let joined = results(&mut sh, &out, "\\d joined");
    assert!(
        joined.contains("Reads:\n    public.parent\n    public.child\n"),
        "{joined}"
    );
}

#[test]
fn listings_are_results_in_the_output_format() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.csv");
    let mut sh = shell(server.sock_path(), None);
    let text = results(
        &mut sh,
        &out,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, kind BIGINT, note TEXT);\n\\dt",
    );
    assert_eq!(text, "Schema,Name,Type,Columns\npublic,t,table,3\n");
}

/// A name a meta-command takes is read as SQL reads one: quoted, qualified,
/// and a pattern over the qualified name.
#[test]
fn meta_command_names_read_as_sql_reads_them() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.csv");
    let mut sh = shell(server.sock_path(), None);
    sh.run_script("CREATE TABLE t (id BIGINT PRIMARY KEY, kind BIGINT);\nCREATE INDEX t_kind ON t (kind);")
        .unwrap();
    for name in ["\"t\"", "public.\"t\""] {
        let text = results(&mut sh, &out, &format!("\\d {name}"));
        assert!(text.contains("Table \"public.t\""), "{name}: {text}");
    }
    assert_eq!(
        results(&mut sh, &out, "\\dt \"t\""),
        "Schema,Name,Type,Columns\npublic,t,table,2\n"
    );
    assert_eq!(
        results(&mut sh, &out, "\\di public.*"),
        "Schema,Name,Table,Columns,Unique\npublic,t_kind,t,kind,false\n"
    );
}

#[test]
fn describe_lists_the_with_options_a_view_was_created_with() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.txt");
    let mut sh = shell(server.sock_path(), None);
    let text = results(
        &mut sh,
        &out,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, kind BIGINT);\n\
         CREATE VIEW b WITH (capacity = '4 MB', compress = true) AS SELECT id, kind FROM t WHERE kind > 0;\n\
         \\d b",
    );
    assert!(
        text.contains("With:\n    capacity = '4 MB'\n    compress = true\n"),
        "{text}"
    );
}

#[test]
fn watch_with_no_argument_fails_before_planning() {
    let server = ServerHandle::start_n(WORKERS);
    let mut sh = shell(server.sock_path(), None);
    assert!(sh.meta("\\watch").is_err());
    assert!(sh.meta("\\watch   ").is_err());
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

const APP: &str = "
    CREATE SCHEMA app;
    CREATE TABLE app.users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, manager BIGINT REFERENCES app.users (id));
    CREATE TABLE app.scores (user_id BIGINT NOT NULL REFERENCES app.users (id) ON DELETE CASCADE, game INT,
        points DECIMAL(10, 2), PRIMARY KEY (user_id, game)) CLUSTER BY user_id;
    CREATE INDEX scores_game ON app.scores (game);
    INSERT INTO app.users (name, manager) VALUES ('boss', NULL);
    INSERT INTO app.users (name, manager) VALUES ('ann', 1), ('bob', 1);
    INSERT INTO app.scores VALUES (1, 1, 10.5), (2, 1, 3.25), (3, 2, 7);
    CREATE VIEW app.leaders AS
        SELECT u.name, SUM(s.points) AS total FROM app.scores AS s JOIN app.users u ON s.user_id = u.id
        GROUP BY u.name;
    ALTER TABLE app.users RENAME COLUMN name TO display_name;
    ALTER TABLE app.scores RENAME TO results;
";

/// `\d+` writes a view's definition under the names its sources carry now.
#[test]
fn describe_verbose_shows_a_views_definition() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.txt");
    let mut sh = shell(server.sock_path(), None);
    sh.run_script(APP).unwrap();
    let text = results(&mut sh, &out, "\\d+ app.leaders");
    assert!(
        text.contains(
            "Definition:\n    CREATE VIEW app.leaders AS\n    SELECT u.name, SUM(s.points) AS total \
             FROM app.results AS s JOIN app.users u(id, name, manager) ON s.user_id = u.id\n"
        ),
        "{text}"
    );
}

/// `\dump` into a directory and `\restore` from it on another server give the
/// same answers.
#[test]
fn dump_and_restore_carry_a_database_between_servers() {
    let source = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let dump = dir.path().join("dump");
    let out = dir.path().join("out.csv");
    let mut sh = shell(source.sock_path(), None);
    sh.run_script(APP).unwrap();
    sh.run_script(&format!("\\dump {}", dump.display())).unwrap();
    let schema = std::fs::read_to_string(dump.join("schema.sql")).unwrap();
    assert!(schema.contains("CREATE TABLE app.results (\n"), "{schema}");
    let post = std::fs::read_to_string(dump.join("post-data.sql")).unwrap();
    assert!(
        post.contains("CREATE INDEX scores_game ON app.results (game);"),
        "{post}"
    );
    let queries =
        "SELECT * FROM app.leaders ORDER BY name;\nSELECT id, display_name, manager FROM app.users ORDER BY id;";
    let before = results(&mut sh, &out, queries);

    let target = ServerHandle::start_n(2);
    let mut sh = shell(target.sock_path(), None);
    sh.run_script(&format!("\\restore {}", dump.display())).unwrap();
    assert_eq!(results(&mut sh, &out, queries), before);
    assert!(sh.run_script(&format!("\\restore {}", dump.display())).is_err());
}

#[test]
fn backup_writes_an_entry_of_the_backup_directory() {
    let server = ServerHandle::start_with_backups(WORKERS);
    let mut sh = shell(server.sock_path(), None);
    sh.run_script("CREATE TABLE t (id BIGINT PRIMARY KEY); INSERT INTO t VALUES (1);\n\\backup nightly")
        .unwrap();
    assert!(server.backups().join("nightly").join("_relations").is_dir());
    assert!(sh.meta("\\backup nightly").is_err());
    assert!(sh.meta("\\backup").is_err());
}

/// The shell binary with `args`, given no target: the server it starts is the
/// suites' one, its scratch directories go under `tmp`, and its SAL is small
/// even on a kept data directory, as the harness's servers' are.
fn gnitz(tmp: &Path, args: &[&str]) -> std::process::Command {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_gnitz"));
    cmd.args(args)
        .env("GNITZ_SERVER_BIN", gnitz_test_harness::server_bin())
        .env("GNITZ_SAL_BYTES", "67108864")
        .env("TMPDIR", tmp)
        .env_remove("GNITZ_TARGET");
    cmd
}

/// What the run of `cmd` printed to stdout, once it has succeeded.
fn stdout_of(mut cmd: std::process::Command) -> String {
    let out = cmd.stdin(std::process::Stdio::null()).output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{}: {stderr}", out.status);
    String::from_utf8(out.stdout).unwrap()
}

fn is_empty_dir(dir: &Path) -> bool {
    std::fs::read_dir(dir).unwrap().next().is_none()
}

#[test]
fn with_no_target_a_session_runs_a_server_of_its_own_and_removes_it() {
    let tmp = tempfile::tempdir().unwrap();
    let out = stdout_of(gnitz(
        tmp.path(),
        &[
            "-F",
            "csv",
            "-c",
            "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)",
            "-c",
            "INSERT INTO t VALUES (1, 10), (2, 20)",
            "-c",
            "SELECT * FROM t ORDER BY id",
        ],
    ));
    assert!(out.ends_with("id,v\n1,10\n2,20\n"), "{out}");
    assert!(
        is_empty_dir(tmp.path()),
        "the session left its scratch directory behind"
    );
}

#[test]
fn a_session_given_a_data_directory_leaves_its_rows_to_the_next() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("db");
    let data = data.to_str().unwrap();
    stdout_of(gnitz(
        tmp.path(),
        &[
            "--data",
            data,
            "-c",
            "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)",
            "-c",
            "INSERT INTO t VALUES (7, 70)",
        ],
    ));
    let out = stdout_of(gnitz(
        tmp.path(),
        &["--data", data, "-F", "csv", "-c", "SELECT * FROM t"],
    ));
    assert!(out.ends_with("id,v\n7,70\n"), "{out}");
}

#[test]
fn a_session_with_no_server_to_start_says_so_and_starts_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("no-such-server");
    let out = gnitz(tmp.path(), &["-c", "SELECT 1"])
        .env("GNITZ_SERVER_BIN", &missing)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("GNITZ_SERVER_BIN names"), "{stderr}");
    assert!(is_empty_dir(tmp.path()));
}

#[test]
fn an_interrupted_script_stops_its_server_and_removes_its_data() {
    use std::io::{BufRead, BufReader, Write};
    use std::process::Stdio;

    let tmp = tempfile::tempdir().unwrap();
    let mut child = gnitz(tmp.path(), &["-F", "csv"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // One answered statement: the session is connected, so its server is held.
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "SELECT 1 AS one;").unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    while line.trim() != "1" {
        line.clear();
        assert_ne!(stdout.read_line(&mut line).unwrap(), 0, "the script ended unanswered");
    }
    // SAFETY: `kill` takes no pointer; the child is not yet reaped.
    unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) };
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(128 + libc::SIGINT));
    drop(stdin);
    assert!(
        is_empty_dir(tmp.path()),
        "the interrupted session left its scratch directory behind"
    );
}
