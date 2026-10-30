#![cfg(feature = "integration")]

//! The shell against a real server: scripts, the meta-commands that read the
//! catalog, and `\watch` following a view's delta feed.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

use gnitz_cli::{Flow, Format, Options, Settings, Shell};
use gnitz_test_harness::ServerHandle;

const WORKERS: usize = 4;
const PATIENCE: Duration = Duration::from_secs(30);

fn shell(sock: &str, interrupt: Option<UnixStream>) -> Shell {
    Shell::connect(Options {
        target: sock.to_string(),
        schema: "public".to_string(),
        settings: Settings {
            format: Format::Csv,
            ..Settings::default()
        },
        color: false,
        interactive: false,
        stop_on_error: true,
        interrupt,
    })
    .expect("connect")
}

/// Send results to `out`; `\o` reads its path as one word.
fn output_to(shell: &mut Shell, out: &Path) {
    shell.run_script(&format!("\\o {}", out.display())).expect("\\o");
}

/// Run `script`, its results written to `out`, and read them back. Each is a
/// script of its own, so SQL `script` leaves without a `;` runs at its end.
fn results(shell: &mut Shell, out: &Path, script: &str) -> String {
    output_to(shell, out);
    shell.run_script(script).expect("script runs");
    shell.run_script("\\o").expect("\\o");
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

/// Each type a SQL literal can write, in the text the shell prints, and
/// `\\echo`'s text as typed past its first word.
#[test]
fn each_column_type_prints_its_canonical_text() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.csv");
    let mut sh = shell(server.sock_path(), None);
    let text = results(
        &mut sh,
        &out,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, ok BOOLEAN, u BIGINT UNSIGNED, h UHUGEINT, \
         f DOUBLE, ts TIMESTAMP, g UUID);\n\
         INSERT INTO t VALUES (1, true, 18446744073709551615, 340282366920938463463374607431768211455, \
         1.5, '2024-01-02 03:04:05', '00112233-4455-6677-8899-aabbccddeeff');\n\
         SELECT ok, u, h, f, ts, g FROM t;\n\
         \\echo  done  here",
    );
    assert_eq!(
        text,
        "ok,u,h,f,ts,g\n\
         true,18446744073709551615,340282366920938463463374607431768211455,1.5,2024-01-02 03:04:05,\
         00112233-4455-6677-8899-aabbccddeeff\n\
         done  here\n"
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
    output_to(&mut sh, &out);
    assert!(sh.run_script("SELECT * FROM missing; SELECT 1 AS one;").is_err());
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "");
}

/// Each listing keeps its own command's classes, and `\d` shows a table's
/// keys and a view's options.
#[test]
fn listings_keep_their_command_s_classes() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.csv");
    let mut sh = shell(server.sock_path(), None);
    sh.run_script(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, kind BIGINT NOT NULL, note TEXT);\n\
         CREATE INDEX t_kind ON t (kind);\n\
         CREATE TABLE ev (id BIGINT PRIMARY KEY, kind BIGINT) WITH (stream = true);\n\
         CREATE VIEW v WITH (delta = '1 MB') AS SELECT kind, COUNT(*) AS n FROM ev GROUP BY kind;",
    )
    .unwrap();
    const HEADER: &str = "Schema,Name,Type,Columns\n";
    assert_eq!(results(&mut sh, &out, "\\dt"), format!("{HEADER}public,t,table,3\n"));
    assert_eq!(results(&mut sh, &out, "\\ds"), format!("{HEADER}public,ev,stream,2\n"));
    assert_eq!(
        results(&mut sh, &out, "\\dv"),
        format!("{HEADER}public,v,view with a delta feed,2\n")
    );
    assert!(results(&mut sh, &out, "\\dS tables").contains("_system,tables,"));
    assert!(!results(&mut sh, &out, "\\d").contains("_system"));
    let schemas = results(&mut sh, &out, "\\dn");
    assert!(schemas.contains("_system") && schemas.contains("public"), "{schemas}");
    let t = results(&mut sh, &out, "\\d t");
    assert!(t.contains("not null") && t.contains("PK"), "{t}");
    let v = results(&mut sh, &out, "\\d v");
    assert!(v.contains("With:\n    delta = '1 MB'\n"), "{v}");
}

/// `\d` reads the catalog as it stands, whichever session changed it: its
/// foreign keys both ways, what a view reads through its chain, and the
/// `WITH` options a view was created with.
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
    let bounded = results(
        &mut sh,
        &out,
        "CREATE VIEW b WITH (capacity = '4 MB', compress = true) AS SELECT id FROM parent;\n\\d b",
    );
    assert!(
        bounded.contains("With:\n    capacity = '4 MB'\n    compress = true\n"),
        "{bounded}"
    );
}

/// A name a meta-command takes is read as SQL reads one: quoted, qualified,
/// and a pattern over the qualified name; a listing is a result in the output
/// format.
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
    sh.run_script("CREATE SCHEMA \"table\";").unwrap();
    sh.run_script("\\use \"table\"").expect("a quoted schema name");
    sh.run_script("CREATE TABLE u (id BIGINT PRIMARY KEY);").unwrap();
    assert!(
        sh.run_script("\\d u").is_ok(),
        "an unqualified name resolves in the schema used"
    );
    sh.run_script("\\use PUBLIC").expect("a schema name in any case");
    assert!(sh.run_script("\\d u").is_err(), "and in no other");
    assert!(sh.run_script("\\use nope").is_err());
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

    let (cancel, mut interrupt) = UnixStream::pair().unwrap();
    let mut watcher = shell(&sock, Some(cancel));
    output_to(&mut watcher, &out);
    let watch = |mut watcher: Shell| {
        std::thread::spawn(move || {
            let flow = watcher.run_script("\\watch hot");
            (watcher, flow)
        })
    };
    let watching = watch(watcher);

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

    interrupt.write_all(b"\x02").unwrap();
    let (mut watcher, flow) = watching.join().expect("watch thread");
    assert_eq!(flow.expect("watch ends cleanly"), Flow::Continue);

    // An aligned watch prints each answer as a table with no row count.
    watcher.run_script("\\pset format aligned").unwrap();
    output_to(&mut watcher, &out);
    let watching = watch(watcher);
    let text = wait_for(&out, "107");
    assert!(text.contains("snapshot") && !text.contains("rows)"), "{text}");
    interrupt.write_all(b"\x02").unwrap();
    let (_, flow) = watching.join().expect("watch thread");
    assert_eq!(flow.expect("watch ends cleanly"), Flow::Continue);
}

#[test]
fn watch_refuses_no_argument_and_a_view_without_a_delta_feed() {
    let server = ServerHandle::start_n(WORKERS);
    let mut sh = shell(server.sock_path(), None);
    assert!(sh.run_script("\\watch").is_err());
    sh.run_script(
        "CREATE TABLE e (id BIGINT PRIMARY KEY, kind BIGINT);\n\
         CREATE VIEW plain AS SELECT kind FROM e;",
    )
    .unwrap();
    assert!(sh.run_script("\\watch plain").is_err());
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

/// `\dump` writes a dump, and `\restore` reads one back: into the dumped
/// database, which holds every name it creates, it is refused.
#[test]
fn dump_and_restore_run_from_the_shell() {
    let server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let mut sh = shell(server.sock_path(), None);
    sh.run_script(APP).unwrap();
    sh.run_script(&format!("\\dump {}", dir.path().display())).unwrap();
    assert!(dir.path().join("manifest.json").is_file());
    assert!(sh.run_script(&format!("\\restore {}", dir.path().display())).is_err());
}

#[test]
fn backup_writes_an_entry_of_the_backup_directory() {
    let server = ServerHandle::start_with_backups(WORKERS, &[]);
    let mut sh = shell(server.sock_path(), None);
    sh.run_script("CREATE TABLE t (id BIGINT PRIMARY KEY); INSERT INTO t VALUES (1);\n\\backup nightly")
        .unwrap();
    assert!(server.backups().join("nightly").join("_relations").is_dir());
    assert!(sh.run_script("\\backup nightly").is_err());
    assert!(sh.run_script("\\backup").is_err());
}

/// The shell binary with `args`. Where they name no server, the one it starts
/// is the suites' one, its scratch directories go under `tmp`, and its SAL is
/// small even on a kept data directory, as the harness's servers' are.
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

/// The run of the shell binary on the server at `sock` with `args`, CSV
/// output and `stdin` as its input.
fn run_at(tmp: &Path, sock: &str, args: &[&str], stdin: &str) -> std::process::Output {
    let mut child = gnitz(tmp, &[&[sock, "-F", "csv"], args].concat())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin.as_bytes()).unwrap();
    child.wait_with_output().unwrap()
}

/// A script stops at its first failed statement, and with
/// `--continue-on-error` runs every other; either way it exits 1, whatever
/// follows the failure. `-c` and `-f` run in the order given, and a `\i -`
/// in a script read from stdin reads on from it.
#[test]
fn a_script_runs_its_inputs_in_order_and_its_exit_status_says_whether_one_failed() {
    let server = ServerHandle::start_n(WORKERS);
    let sock = server.sock_path();
    let dir = tempfile::tempdir().unwrap();
    let stdout = |out: &std::process::Output| String::from_utf8_lossy(&out.stdout).into_owned();

    let failing = "SELECT * FROM missing; SELECT 1 AS one;";
    let out = run_at(dir.path(), sock, &["-c", failing], "");
    assert_eq!((out.status.code(), stdout(&out).as_str()), (Some(1), ""));
    assert!(String::from_utf8_lossy(&out.stderr).contains("ERROR:"));
    let out = run_at(dir.path(), sock, &["--continue-on-error", "-c", failing], "");
    assert_eq!((out.status.code(), stdout(&out).as_str()), (Some(1), "one\n1\n"));

    let quit_after = dir.path().join("quit.sql");
    std::fs::write(&quit_after, "SELECT * FROM missing;\n\\q\nSELECT 2 AS two;\n").unwrap();
    let out = run_at(
        dir.path(),
        sock,
        &["--continue-on-error", "-f", quit_after.to_str().unwrap()],
        "",
    );
    assert_eq!((out.status.code(), stdout(&out).as_str()), (Some(1), ""));

    let setup = dir.path().join("setup.sql");
    std::fs::write(
        &setup,
        "CREATE TABLE t (id BIGINT PRIMARY KEY);\nINSERT INTO t VALUES (5);\n",
    )
    .unwrap();
    let out = run_at(
        dir.path(),
        sock,
        &["-f", setup.to_str().unwrap(), "-c", "SELECT * FROM t"],
        "",
    );
    assert_eq!((out.status.code(), stdout(&out).as_str()), (Some(0), "id\n5\n"));

    let out = run_at(dir.path(), sock, &[], "SELECT 1 AS a;\n\\i -\nSELECT 2 AS b;\n");
    assert_eq!((out.status.code(), stdout(&out).as_str()), (Some(0), "a\n1\nb\n2\n"));
}

/// A statement after the server restarts finds the connection closed; the
/// shell dials again, so the next one runs.
#[test]
fn a_statement_after_the_server_restarts_runs_on_a_new_connection() {
    let mut server = ServerHandle::start_n(WORKERS);
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.csv");
    let mut sh = shell(server.sock_path(), None);
    sh.run_script("CREATE TABLE t (id BIGINT PRIMARY KEY); INSERT INTO t VALUES (1);")
        .unwrap();
    server.restart().unwrap();
    assert!(sh.run_script("SELECT * FROM t;").is_err());
    assert_eq!(results(&mut sh, &out, "SELECT * FROM t;"), "id\n1\n");
    assert_eq!(results(&mut sh, &out, "\\c\nSELECT * FROM t;"), "id\n1\n");
}

fn is_empty_dir(dir: &Path) -> bool {
    std::fs::read_dir(dir).unwrap().next().is_none()
}

#[test]
fn with_no_target_a_session_runs_a_server_of_its_own_and_removes_it() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cmd = gnitz(
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
    );
    // An empty GNITZ_TARGET names no server.
    cmd.env("GNITZ_TARGET", "");
    let out = stdout_of(cmd);
    assert_eq!(out, "id,v\n1,10\n2,20\n");
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
    // `--data` asks for the session's own server, whatever the environment
    // names.
    let mut cmd = gnitz(tmp.path(), &["--data", data, "-F", "csv", "-c", "SELECT * FROM t"]);
    cmd.env("GNITZ_TARGET", tmp.path().join("no-server.sock"));
    let out = stdout_of(cmd);
    assert_eq!(out, "id,v\n7,70\n");
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

    // A target the environment names but that cannot be read starts nothing.
    use std::os::unix::ffi::OsStrExt;
    let out = gnitz(tmp.path(), &["-c", "SELECT 1"])
        .env("GNITZ_TARGET", std::ffi::OsStr::from_bytes(b"/tmp/\xff.sock"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("GNITZ_TARGET is not UTF-8"), "{stderr}");
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
