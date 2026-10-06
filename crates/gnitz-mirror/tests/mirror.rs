//! The acceptance suite for a client that mirrors: one `GnitzClient` with a
//! store attached, read through the SQL layer against a live server.
//!
//! A Rust integration test rather than a pytest: it reaches a real server through
//! `gnitz-test-harness` and builds its `ReadSpec`s through `gnitz-sql`, both
//! **dev**-dependencies, so neither enters the crate's dependency graph.
//!
//! `tests/store.rs` beside it covers the store alone, with no server and no
//! client.
//!
//! **No test runs at one worker.** At W=1 there is one slice, so a mirror that
//! mishandled the concatenation of W slices would still pass, and the
//! replicated-vs-keyed routing split collapses to the same thing. Four is the
//! default; the shape cases also run at two, which moves that split rather than
//! removing it.

mod support;

#[path = "benches/mirror.rs"]
mod bench;

use gnitz_core::block_on;
use gnitz_core::{ClientError, GnitzClient, MirrorError, PollOutcome, PollResult, RelName, Schema};
use gnitz_mirror::{Mirror, MirrorConfig};
use gnitz_sql::GnitzSqlError;
use gnitz_test_harness::ServerHandle;
use gnitz_wire::WireStatus;
use gnitz_zset_testkit::{assert_child_ok, run_test_in_child};
use std::time::Duration;
use support::common::{block_copy, has_copy, has_manifest, manifest_path, unblock_copy};
use support::{canonical, canonical_rows, cost, differential, query, sql, Answer, Reply};

/// `schema.name`.
fn rel(schema: &str, name: &str) -> RelName {
    RelName::new(schema, name).unwrap()
}

/// Four workers, because that is the only count that exercises the fan-out.
const WORKERS: usize = 4;
/// Feed budget: large enough that nothing in this suite is swept out from under
/// a cursor except where a test means it to be.
const FEED: &str = "8 MB";

/// A test's server plus the two clients it needs: one with a store attached, one
/// that reads the same relations directly for the differential.
struct Fixture {
    server: ServerHandle,
    direct: GnitzClient,
    /// `Option` so a test can close or drop it, releasing the directory.
    mirror: Option<GnitzClient>,
    dir: tempfile::TempDir,
}

/// The store on `dir`, which must be free.
fn open_store(dir: &str) -> Mirror {
    Mirror::open(dir, MirrorConfig::default()).expect("a store opens")
}

/// A client on `target` with a store on `dir` attached.
fn mirroring_client(target: &str, dir: &str) -> GnitzClient {
    let mut client = GnitzClient::connect(target).unwrap();
    client
        .attach_mirror(open_store(dir))
        .expect("a fresh client attaches it");
    client
}

/// Create `schema.name` as a view over `body` that keeps a delta feed — the only
/// kind a client can mirror.
fn fed_view(client: &mut GnitzClient, schema: &str, name: &str, body: &str) {
    sql(
        client,
        schema,
        &format!("CREATE VIEW {name} WITH (delta = '{FEED}') AS {body}"),
    );
}

impl Fixture {
    fn start() -> Fixture {
        Fixture::start_with(WORKERS, &[])
    }

    /// At a chosen worker count, with server-side environment of its own — the
    /// harness sets it on the server process alone.
    fn start_with(workers: usize, env: &[(&str, &str)]) -> Fixture {
        let server = ServerHandle::start_with_env(workers, env);
        let dir = tempfile::tempdir().unwrap();
        let mut direct = GnitzClient::connect(server.sock_path()).unwrap();
        seed(&mut direct);
        let mirror = mirroring_client(server.sock_path(), dir.path().to_str().unwrap());
        Fixture {
            server,
            direct,
            mirror: Some(mirror),
            dir,
        }
    }

    fn mirror(&mut self) -> &mut GnitzClient {
        self.mirror.as_mut().unwrap()
    }

    fn base_dir(&self) -> String {
        self.dir.path().to_str().unwrap().to_string()
    }

    /// Replace the connection, keeping the copies — what a host does after the
    /// server it was talking to went away.
    fn reconnect_mirror(&mut self) {
        let target = self.server.sock_path().to_string();
        block_on(self.mirror().reconnect(&target)).expect("reconnect");
    }

    /// Restart the server, and give the direct client a live connection.
    fn restart_server(&mut self) {
        self.server.restart();
        self.direct = GnitzClient::connect(self.server.sock_path()).unwrap();
    }

    /// Close the client's store — the exit checkpoint — and release the
    /// directory.
    fn close(&mut self) {
        let mut client = self.mirror.take().expect("a client to close");
        block_on(client.close_mirror()).expect("exit checkpoint");
    }

    /// [`Fixture::close`], then a fresh client on the same directory.
    fn reopen(&mut self) {
        self.close();
        self.open();
    }

    /// Open a fresh client on the directory, which must be free: a test models
    /// a crash by dropping the client without closing it, then calls this.
    fn open(&mut self) {
        assert!(self.mirror.is_none(), "the directory is still held");
        let dir = self.base_dir();
        self.mirror = Some(mirroring_client(self.server.sock_path(), &dir));
    }

    /// Make the named views of `schema` emit the rounds the pushes so far
    /// produced: a read against the server drains its pending ticks. `COUNT(*)`
    /// because only the drain is wanted: it folds server-side and replies with
    /// one row per worker where `SELECT *` ships the view.
    fn tick(&mut self, schema: &str, views: &[impl AsRef<str>]) {
        for v in views {
            let name = v.as_ref();
            let _ = query(&mut self.direct, schema, &format!("SELECT COUNT(*) AS n FROM {name}"));
        }
    }

    /// Carry every push so far into the copies: a poll drains the ticks they
    /// left pending, as a read against the server does.
    fn drain(&mut self) -> Vec<PollOutcome> {
        block_on(self.mirror().poll_mirror(Duration::ZERO)).expect("poll")
    }

    /// `m` mirrored delta-fed views of `t`, named `<prefix>0..m`, each selecting
    /// `cols` where `v >= i` — so they differ only in a `WHERE` and a
    /// misdirected reply lands rows that are individually plausible. Each first
    /// registration bootstraps; drained, so every copy holds a round.
    fn many_views(&mut self, prefix: &str, m: usize, cols: &str) -> Vec<(String, u64)> {
        let names: Vec<String> = (0..m).map(|i| format!("{prefix}{i}")).collect();
        for (i, name) in names.iter().enumerate() {
            fed_view(
                &mut self.direct,
                "s",
                name,
                &format!("SELECT {cols} FROM t WHERE v >= {i}"),
            );
        }
        let views: Vec<(String, u64)> = names
            .into_iter()
            .map(|name| {
                let out = block_on(self.mirror().mirror_view(&rel("s", &name))).expect("mirror");
                assert!(out.result.reseeded(), "{name}: a first registration bootstraps");
                (name, out.view_id)
            })
            .collect();
        self.drain();
        views
    }

    /// Mirror both views of the shared schema, and return their ids. Nearly
    /// every test wants both: the keyed one carries the compound PK, the
    /// replicated one the worker-0 feed.
    fn mirror_both(&mut self) -> (u64, u64) {
        let keyed = block_on(self.mirror().mirror_view(&rel("s", "v_keyed"))).expect("mirror v_keyed");
        let repl = block_on(self.mirror().mirror_view(&rel("s", "v_repl"))).expect("mirror v_repl");
        (keyed.view_id, repl.view_id)
    }

    /// Run one `SELECT` through the mirroring client.
    fn local(&mut self, sql_text: &str) -> Reply {
        query(self.mirror(), "s", sql_text)
    }

    /// [`differential`] for a relation the mirroring client holds a valid copy of.
    fn differential(&mut self, schema: &str, sql_text: &str) -> usize {
        differential(
            self.mirror.as_mut().unwrap(),
            &mut self.direct,
            schema,
            sql_text,
            Answer::Local,
        )
    }

    /// [`differential`] for a relation the mirroring client must delegate.
    fn delegated(&mut self, schema: &str, sql_text: &str) -> usize {
        differential(
            self.mirror.as_mut().unwrap(),
            &mut self.direct,
            schema,
            sql_text,
            Answer::Upstream,
        )
    }
}

/// The schema every test shares.
///
/// `v_keyed` has a **two-column PK**: a single-column `BIGINT` PK makes a wrong
/// PK stride look right. `v_repl` is replicated, so its feed lives on worker 0
/// alone.
fn seed(client: &mut GnitzClient) {
    block_on(client.create_schema("s")).unwrap();
    sql(
        client,
        "s",
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, \
                         f DOUBLE PRECISION NOT NULL, body TEXT NOT NULL, PRIMARY KEY (a, b))",
    );
    sql(
        client,
        "s",
        "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) WITH (replicated = true)",
    );
    fed_view(client, "s", "v_keyed", "SELECT a, b, v, f, body FROM t WHERE v >= 0");
    fed_view(client, "s", "v_repl", "SELECT id, v FROM r WHERE v > 3");
}

/// Inserts, then an UPDATE and a DELETE over part of the range — so a round
/// carries fresh keys, the retract/insert pair a PK-conflicting insert produces,
/// and pure retractions. Without the last two, the retraction half goes
/// untested: a capture that shipped `+1` with no `-1` passes a row-set check.
fn churn(client: &mut GnitzClient, lo: i64, hi: i64) {
    let rows: Vec<String> = (lo..=hi)
        .map(|i| format!("({}, {}, {}, {}.5, 'body-{i:0>20}')", i, i % 7, i * 3, i))
        .collect();
    sql(client, "s", &format!("INSERT INTO t VALUES {}", rows.join(",")));
    let rrows: Vec<String> = (lo..=hi).map(|i| format!("({i}, {})", i * 2)).collect();
    sql(client, "s", &format!("INSERT INTO r VALUES {}", rrows.join(",")));
    let span = hi - lo + 1;
    sql(
        client,
        "s",
        &format!("UPDATE t SET v = v + 1 WHERE a >= {lo} AND a < {}", lo + span / 3),
    );
    sql(client, "s", &format!("DELETE FROM t WHERE a > {}", hi - span / 4));
    sql(
        client,
        "s",
        &format!("UPDATE r SET v = v + 5 WHERE id < {}", lo + span / 3),
    );
}

/// A stream in the shared schema.
const STREAM: &str = "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, \
                      amount BIGINT NOT NULL) WITH (stream = true)";

// ---------------------------------------------------------------------------
// The differential
// ---------------------------------------------------------------------------

/// Every sink a direct SELECT can land on — the whole-relation read, the
/// bounded/projected rows sink and the aggregate fold — and ordered windows,
/// including `LIMIT … OFFSET` over a tie-heavy sort key where a different
/// tie-break selects different rows.
const MENU: &[&str] = &[
    "SELECT * FROM v_keyed",
    "SELECT a, b, v FROM v_keyed WHERE a = 42",
    "SELECT a, v FROM v_keyed WHERE a > 100 AND a < 140",
    "SELECT b, COUNT(*) AS n, SUM(v) AS total, MIN(a) AS lo, MAX(a) AS hi FROM v_keyed GROUP BY b",
    "SELECT COUNT(*) AS n, SUM(v) AS total FROM v_keyed",
    "SELECT DISTINCT b FROM v_keyed",
    "SELECT b, SUM(v) AS total FROM v_keyed GROUP BY b HAVING SUM(v) > 1000",
    "SELECT * FROM v_repl",
    "SELECT COUNT(*) AS n, SUM(v) AS total FROM v_repl",
    "SELECT a, b, v FROM v_keyed ORDER BY a DESC, b ASC",
    "SELECT a, b, body FROM v_keyed WHERE v > 100 ORDER BY a ASC, b DESC LIMIT 23",
    // `b` is `a % 7`, so the sort key ties in blocks of ~40 rows: which rows the
    // window selects depends on the whole tie-break, not just on `b`.
    "SELECT a, b FROM v_keyed ORDER BY b ASC, a ASC LIMIT 19 OFFSET 31",
    "SELECT a, b FROM v_keyed ORDER BY b DESC, a DESC LIMIT 19 OFFSET 31",
    "SELECT id, v FROM v_repl ORDER BY v DESC, id ASC LIMIT 15 OFFSET 7",
];

/// A mirrored read equals the same read against the server, over the whole
/// [`MENU`], after the bootstrap and after each of two polls that each span
/// several rounds hitting keys the copy already holds: one round's reply is
/// sorted and distinct, a multi-round reply is neither.
///
/// The comparison runs after the ordinary client-side finishing on both sides,
/// which is the point: the local reply is a single-worker reply, and the
/// finishers take it unchanged.
#[test]
fn a_mirrored_read_equals_the_server_read() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 200);
    fx.mirror_both();
    fx.drain();

    for round in 0..3 {
        if round > 0 {
            for step in 1..=3 {
                let lo = round * 40;
                sql(
                    &mut fx.direct,
                    "s",
                    &format!("UPDATE t SET v = v + {step} WHERE a >= {lo} AND a < {}", lo + 25),
                );
            }
            let lo = 161 + round * 40;
            churn(&mut fx.direct, lo, lo + 39);
            fx.drain();
        }
        let compared: usize = MENU.iter().map(|q| fx.differential("s", q)).sum();
        assert!(
            compared > 300,
            "round {round}: the differential compared only {compared} rows"
        );
        assert_floats_close(&mut fx, "SELECT b, SUM(f) AS tf, AVG(f) AS af FROM v_keyed GROUP BY b");
    }
}

/// Assert a float aggregate reads the same groups at the same weights locally and
/// on the server, with each value within reassociation tolerance.
///
/// Float SUM/AVG is order-dependent — addition is non-associative — and the
/// mirror is one partition where the server is W. This is the same divergence
/// the server already has between two worker counts, not a mirror defect.
fn assert_floats_close(fx: &mut Fixture, q: &str) {
    let local = canonical(&fx.local(q));
    let remote = canonical(&query(&mut fx.direct, "s", q));
    assert_eq!(local.len(), remote.len(), "{q}: the same groups");
    for ((lk, lw), (rk, rw)) in local.iter().zip(&remote) {
        assert_eq!((&lk.0, lw), (&rk.0, rw), "{q}: groups line up by key, at exact weights");
        for (lc, rc) in lk.1.iter().zip(&rk.1) {
            let (Some(lb), Some(rb)) = (lc, rc) else {
                assert_eq!(lc, rc, "{q}: a null on one side only");
                continue;
            };
            let (lv, rv) = (f64_of(lb), f64_of(rb));
            assert!(
                (lv - rv).abs() <= 1e-6 * rv.abs().max(1.0),
                "{q}: diverged beyond reassociation: {lv} vs {rv}",
            );
        }
    }
}

fn f64_of(b: &[u8]) -> f64 {
    f64::from_le_bytes(b.try_into().expect("an eight-byte cell"))
}

/// Many views of one table: registration is idempotent, a poll with nothing
/// pushed moves nothing, and one batched poll advances every view weight-exact.
///
/// They differ only in a `WHERE`, so a misdirected position lands rows that are
/// individually plausible: only the weights say otherwise.
#[test]
fn many_views_of_one_table_advance_in_one_poll() {
    const M: usize = 6;
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 120);
    let views = fx.many_views("m", M, "a, b, v");
    let cursors = |fx: &mut Fixture| {
        views
            .iter()
            .map(|(_, id)| fx.mirror().cursor_of(*id))
            .collect::<Vec<_>>()
    };
    let settled = cursors(&mut fx);

    for (name, id) in &views {
        let out = block_on(fx.mirror().mirror_view(&rel("s", name))).expect("re-register");
        assert_eq!(out.view_id, *id, "{name}: the same relation keeps its id");
        assert!(!out.result.reseeded(), "{name}: a re-registration resumes");
    }
    let report = block_on(fx.mirror().poll_mirror(Duration::ZERO)).expect("an empty poll");
    assert_eq!(report.len(), M, "one entry per view: {report:?}");
    assert!(
        !report.iter().any(|o| o.result.reseeded()),
        "an empty poll reseeds nothing"
    );
    assert_eq!(
        cursors(&mut fx),
        settled,
        "neither a re-registration nor an empty poll moves a cursor",
    );

    // A round with retractions in it.
    churn(&mut fx.direct, 121, 240);
    let names: Vec<&str> = views.iter().map(|(n, _)| n.as_str()).collect();
    let report = fx.drain();
    assert_eq!(report.len(), M, "one entry per view: {report:?}");
    assert!(
        report.iter().all(|o| matches!(o.result, PollResult::Advanced)),
        "every view advances: {report:?}",
    );
    let compared: usize = names
        .iter()
        .map(|name| fx.differential("s", &format!("SELECT * FROM {name}")))
        .sum();
    assert!(compared > M * 100, "the comparison covered {compared} rows");
}

// ---------------------------------------------------------------------------
// The round trip
// ---------------------------------------------------------------------------

/// What each mirror call costs in requests — counts, not clocks.
///
/// Mirroring a view is one RESOLVE and one bootstrap read, first sighting of its
/// schema or not: the store mints no ids, so no registration reaches upstream
/// for one. A mirrored SELECT issues none, and neither does its `EXPLAIN` —
/// routed to the connection it would fail with the server down, and describe a
/// plan against a relation the statement will not read. A delegated read
/// resolves its relation once, and a poll of both views is one request.
#[test]
fn each_mirror_call_costs_what_it_must() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    let m = fx.mirror();

    for view in ["v_keyed", "v_repl"] {
        let (_, sent) = cost(m, |m| block_on(m.mirror_view(&rel("s", view))).expect("mirror"));
        assert_eq!(sent, 2, "{view}: one RESOLVE and one bootstrap read");
    }
    let (_, sent) = cost(m, |m| query(m, "s", "SELECT a, b, v FROM v_keyed WHERE a = 7"));
    assert_eq!(sent, 0, "a mirrored SELECT issues no request at all");

    let (plan, sent) = cost(m, |m| query(m, "s", "EXPLAIN SELECT a, b, v FROM v_keyed WHERE a = 7"));
    assert_eq!(sent, 0, "nor does its EXPLAIN");
    let lines: Vec<String> = canonical_rows(&plan)
        .into_iter()
        .filter_map(|((_, cells), _)| Some(String::from_utf8_lossy(cells.first()?.as_ref()?).into_owned()))
        .collect();
    assert!(
        lines.iter().any(|l| l == "read view v_keyed (local copy)"),
        "the plan names the local copy: {lines:?}",
    );

    let (_, sent) = cost(m, |m| query(m, "s", "SELECT a, b, v FROM t WHERE a = 7"));
    assert_eq!(sent, 2, "a delegated read costs one RESOLVE and one read");

    let (_, sent) = cost(m, |m| block_on(m.poll_mirror(Duration::ZERO)).expect("poll"));
    assert_eq!(sent, 1, "a poll of both views is one request");
}

/// A `CREATE VIEW` over a mirrored view binds the server's id, not the local
/// registration's. The registration is only as fresh as the last poll, and this
/// statement writes a durable catalog row against it. The view is recreated
/// upstream under a fresh id here; the new view must read that one.
#[test]
fn a_view_created_over_a_mirrored_view_binds_the_servers_id() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 10);
    fx.mirror_both();
    fx.drain();

    // Recreate the mirrored view upstream: same name, fresh id. Nothing tells the
    // mirror, so its registration still binds the retired one.
    sql(&mut fx.direct, "s", "DROP VIEW v_keyed");
    fed_view(
        &mut fx.direct,
        "s",
        "v_keyed",
        "SELECT a, b, v, f, body FROM t WHERE v >= 0",
    );

    // The same statement through both clients. Bound to the retired id, the
    // mirroring client's view would backfill from a relation that no longer
    // exists and come back empty.
    sql(fx.mirror(), "s", "CREATE VIEW over_mirror AS SELECT a, b FROM v_keyed");
    sql(
        &mut fx.direct,
        "s",
        "CREATE VIEW over_direct AS SELECT a, b FROM v_keyed",
    );
    let [got, want] =
        ["over_mirror", "over_direct"].map(|v| canonical(&query(&mut fx.direct, "s", &format!("SELECT * FROM {v}"))));
    assert!(!want.is_empty(), "the comparison needs rows to differ over");
    assert_eq!(got, want, "the new view reads the live v_keyed, not the retired one");
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

/// Only a view with a delta feed can be mirrored, only by a client with a store
/// attached, and a store is attached once. Every refusal says why.
#[test]
fn every_refusal_says_why() {
    let mut fx = Fixture::start();
    sql(&mut fx.direct, "s", STREAM);
    sql(&mut fx.direct, "s", "CREATE VIEW plain AS SELECT a, b, v FROM t");
    sql(
        &mut fx.direct,
        "s",
        "CREATE VIEW bounded WITH (capacity = '4 MB') AS SELECT a, b, v FROM t WHERE v > 0",
    );
    for (name, why) in [
        ("t", "is a table; only a view can be mirrored"),
        ("ev", "is a stream; only a view can be mirrored"),
        ("plain", "keeps no delta feed"),
        ("bounded", "is capacity-bounded"),
    ] {
        let e = block_on(fx.mirror().mirror_view(&rel("s", name)))
            .unwrap_err()
            .to_string();
        assert!(e.contains(why), "{name}: {e}");
    }
    assert!(fx.mirror().mirrored_ids().is_empty(), "a refusal registers nothing");

    let mut plain = GnitzClient::connect(fx.server.sock_path()).unwrap();
    let e = block_on(plain.mirror_view(&rel("s", "v_keyed"))).unwrap_err();
    assert!(
        matches!(&e, ClientError::Refused(f) if f.text.contains("mirrors nothing")),
        "a client with no store says so: {e}"
    );

    let held = fx.base_dir();
    let second_dir = tempfile::tempdir().unwrap();
    let second_path = second_dir.path().to_str().unwrap();
    let e = fx
        .mirror()
        .attach_mirror(open_store(second_path))
        .unwrap_err()
        .to_string();
    assert!(e.contains(&held), "the refusal must name the path already held: {e}");
    // The refused store was dropped with the call, which released its lock.
    Mirror::open(second_path, MirrorConfig::default()).expect("a refused attach must release the store it was passed");
}

/// A storage fault reaches the host as an error and degrades one copy; the
/// process and the store both live.
///
/// This is the acceptance test for making the engine safe to link, and it drives
/// the `Err` channel — the one that holds in both build profiles. The seam
/// substitutes an `Err` at the engine's own ingest site, so the path runs
/// without a real disk fault.
///
/// In a child process, because a fault seam is read once per process.
#[test]
#[cfg_attr(not(debug_assertions), ignore = "the seam folds away in a release build")]
fn a_storage_fault_does_not_kill_the_host() {
    run_child(
        "storage_fault_child",
        &[("GNITZ_INJECT_INGEST_APPLY_ERROR", "store")],
        "the host must survive a storage fault and see an error",
    );
}

/// Runs only in the child the test above spawns. The armed fault fails every
/// store ingest in this process, so no sibling copy survives here;
/// `tests/store.rs` covers that.
#[test]
fn storage_fault_child() {
    let Some((sock, dir)) = child_target() else { return };
    let mut direct = GnitzClient::connect(&sock).unwrap();
    let mut mirror = mirroring_client(&sock, &dir);
    let err =
        block_on(mirror.mirror_view(&rel("s", "v_keyed"))).expect_err("the armed seam must fail the bootstrap ingest");
    assert!(
        !matches!(err, ClientError::Mirror(MirrorError::Poisoned(_))),
        "an ingest fault damages one copy, so it must not poison the store: {err}",
    );
    assert!(mirror.mirror_poisoned().is_none(), "the handle stays usable");

    // The registration outlives the failed bootstrap; the copy behind it does
    // not, so the read is delegated rather than answered off an erased copy.
    let [tid] = mirror.mirrored_ids()[..] else {
        panic!("the failed ingest must leave exactly its own registration behind")
    };
    assert!(!mirror.mirrors(tid), "an erased copy is not answered locally");
    differential(&mut mirror, &mut direct, "s", "SELECT * FROM v_keyed", Answer::Upstream);

    // The store is intact, so it still publishes.
    block_on(mirror.checkpoint_mirror()).expect("a store with one erased copy is still safe to publish");
    // A poll succeeds at the call level, with that view's failure inside.
    let report = block_on(mirror.poll_mirror(Duration::ZERO)).expect("a per-view fault is not the call's");
    assert!(
        matches!(&report[..], [o] if o.view_id == tid && matches!(o.result, PollResult::Failed(_))),
        "the armed seam refuses every re-bootstrap too: {report:?}",
    );
}

// ---------------------------------------------------------------------------
// Persistence and recovery
// ---------------------------------------------------------------------------

/// A reopen resumes each copy from its checkpoint rather than reseeding; two
/// stores on two directories in one process each keep their own; and a close
/// that changes nothing writes nothing — a publish renames a fresh inode in, so
/// an unchanged inode is a skipped publish.
#[test]
fn a_reopen_resumes_every_copy_and_a_quiet_close_writes_nothing() {
    let mut fx = Fixture::start();
    let second_dir = tempfile::tempdir().unwrap();
    let second_path = second_dir.path().to_str().unwrap().to_string();
    let mut second = mirroring_client(fx.server.sock_path(), &second_path);

    churn(&mut fx.direct, 1, 80);
    let keyed = block_on(fx.mirror().mirror_view(&rel("s", "v_keyed")))
        .expect("mirror v_keyed")
        .view_id;
    let repl = block_on(second.mirror_view(&rel("s", "v_repl")))
        .expect("mirror v_repl")
        .view_id;
    churn(&mut fx.direct, 81, 120);
    fx.drain();
    block_on(second.poll_mirror(Duration::ZERO)).expect("poll the second store");
    block_on(fx.mirror().checkpoint_mirror()).expect("checkpoint");
    block_on(second.checkpoint_mirror()).expect("checkpoint the second store");
    let inodes = |base_dir: &str| [manifest_inode(base_dir, keyed), manifest_inode(&second_path, repl)];
    let published = inodes(&fx.base_dir());

    // Both close — the exit checkpoint — with nothing moved since the last one.
    fx.reopen();
    block_on(second.close_mirror()).expect("close the second store");
    assert_eq!(
        inodes(&fx.base_dir()),
        published,
        "closing a store whose copies did not move must write nothing",
    );
    second
        .attach_mirror(open_store(&second_path))
        .expect("a later attach on the same connection is legal");

    for (client, view, id) in [(fx.mirror(), "v_keyed", keyed), (&mut second, "v_repl", repl)] {
        let out = block_on(client.mirror_view(&rel("s", view))).expect("re-mirror");
        assert_eq!(out.view_id, id, "{view}: the reopen lands on the same id");
        assert!(
            !out.result.reseeded(),
            "{view} was checkpointed and must resume, not bootstrap"
        );
    }
    fx.differential("s", "SELECT * FROM v_keyed");
    differential(&mut second, &mut fx.direct, "s", "SELECT * FROM v_repl", Answer::Local);
}

/// The inode of the mirrored copy of `view_id`'s manifest.
fn manifest_inode(base_dir: &str, view_id: u64) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(manifest_path(base_dir, view_id))
        .expect("a published manifest")
        .ino()
}

/// A client dropped without a close forfeits the rounds since its last
/// checkpoint and nothing else: the reopened copies resume at that checkpoint's
/// cursor rather than bootstrapping, and the feed delivers the forfeited rounds
/// once — a second application leaves the row set identical and doubles them.
#[test]
fn a_dropped_client_resumes_at_its_last_checkpoint() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let (keyed, _) = fx.mirror_both();
    fx.drain();
    block_on(fx.mirror().checkpoint_mirror()).expect("checkpoint");
    let published = fx.mirror().cursor_of(keyed).expect("a checkpointed round");
    churn(&mut fx.direct, 61, 120);
    fx.drain();
    assert!(
        fx.mirror().cursor_of(keyed).is_some_and(|c| c.tick > published.tick),
        "rounds past the checkpoint, for the drop to forfeit",
    );

    fx.mirror = None;
    fx.open();
    for view in ["v_keyed", "v_repl"] {
        let out = block_on(fx.mirror().mirror_view(&rel("s", view))).expect("re-mirror");
        assert!(
            !out.result.reseeded(),
            "{view}: the feed still covers the forfeited rounds"
        );
        fx.differential("s", &format!("SELECT * FROM {view}"));
    }
}

/// A failed exit checkpoint is `close_mirror`'s own result, and the store is
/// released all the same.
#[test]
fn a_failed_exit_checkpoint_is_reported_and_releases_the_store() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    let (keyed, _) = fx.mirror_both();
    let dir = fx.base_dir();

    block_copy(&dir, keyed);
    let err = block_on(fx.mirror().close_mirror()).expect_err("a checkpoint into a blocked copy");
    assert!(matches!(err, ClientError::Mirror(MirrorError::Engine(_))), "{err}");
    unblock_copy(&dir, keyed);
    fx.mirror()
        .attach_mirror(open_store(&dir))
        .expect("the failed close released the directory and the client's slot");
}

/// A copy answers at its last polled round, so a write through the mirroring
/// client itself is not read back until a poll carries it.
#[test]
fn a_mirrored_read_does_not_see_this_clients_own_write_before_a_poll() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    fx.mirror_both();
    fx.drain();
    let q = "SELECT * FROM v_keyed";
    let before = canonical(&fx.local(q));

    sql(fx.mirror(), "s", "INSERT INTO t VALUES (1000, 1, 5, 0.5, 'late')");
    let (unmoved, sent) = cost(fx.mirror(), |m| query(m, "s", q));
    assert_eq!(sent, 0, "the read is still answered off the copy");
    assert_eq!(canonical(&unmoved), before, "at the round the copy was last polled to");

    fx.drain();
    assert_eq!(
        canonical(&fx.local(q)).len(),
        before.len() + 1,
        "the poll carries the write"
    );
    fx.differential("s", q);
}

// ---------------------------------------------------------------------------
// A poll that drains, and a poll that waits
// ---------------------------------------------------------------------------

/// Run `statement` against the server from a connection of its own, `after`
/// from now.
fn later(fx: &Fixture, after: Duration, statement: &'static str) -> std::thread::JoinHandle<()> {
    let target = fx.server.sock_path().to_string();
    std::thread::spawn(move || {
        let mut client = GnitzClient::connect(&target).unwrap();
        std::thread::sleep(after);
        sql(&mut client, "s", statement);
    })
}

/// The wait a poll asks for, and the bound on one released early — far enough
/// apart that a loaded machine cannot blur them.
const HELD: Duration = Duration::from_secs(60);
const RELEASED: Duration = Duration::from_secs(20);

/// A push no read has ticked is in the copy after one poll.
#[test]
fn a_poll_carries_a_push_no_read_has_ticked() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    fx.mirror_both();
    fx.drain();
    let q = "SELECT * FROM v_keyed";
    let before = canonical(&fx.local(q)).len();

    sql(&mut fx.direct, "s", "INSERT INTO t VALUES (1000, 1, 5, 0.5, 'late')");
    let (report, sent) = cost(fx.mirror(), |m| block_on(m.poll_mirror(Duration::ZERO)).expect("poll"));
    assert_eq!(sent, 1, "one request, and none of them a read of the view");
    assert!(
        report.iter().all(|o| matches!(o.result, PollResult::Advanced)),
        "{report:?}"
    );
    assert_eq!(canonical(&fx.local(q)).len(), before + 1, "the poll carries the push");
    fx.differential("s", q);
}

/// A waiting poll with nothing to report is held for its wait, and a commit
/// ends it at once.
#[test]
fn a_waiting_poll_is_held_until_a_commit_reaches_a_view() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    fx.mirror_both();
    fx.drain();

    let wait = Duration::from_millis(300);
    let t0 = std::time::Instant::now();
    let report = block_on(fx.mirror().poll_mirror(wait)).expect("a quiet poll");
    assert!(t0.elapsed() >= wait, "nothing changed, so the reply was held");
    assert!(
        report.iter().all(|o| matches!(o.result, PollResult::Advanced)),
        "{report:?}"
    );

    let writer = later(
        &fx,
        Duration::from_millis(200),
        "UPDATE t SET v = v + 100 WHERE a <= 10; INSERT INTO t VALUES (1000, 1, 5, 0.5, 'late')",
    );
    let t0 = std::time::Instant::now();
    let _ = block_on(fx.mirror().poll_mirror(HELD)).expect("a poll the commit ends");
    assert!(t0.elapsed() < RELEASED, "the commit released it, not the wait");
    writer.join().unwrap();
    // The writer's second commit may have missed the poll its first one ended.
    fx.drain();
    assert!(fx.differential("s", "SELECT * FROM v_keyed") > 30);
}

/// A commit to a relation a view reads ends the wait even when the view lets
/// none of it through: the reply is empty and moves the cursor alone.
#[test]
fn a_commit_the_view_filters_out_ends_the_wait_with_an_empty_delta() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    let (keyed, _) = fx.mirror_both();
    fx.drain();
    let before = canonical(&fx.local("SELECT * FROM v_keyed"));
    let settled = fx.mirror().cursor_of(keyed).expect("a settled copy");

    // `v_keyed` keeps `v >= 0`.
    let writer = later(
        &fx,
        Duration::from_millis(200),
        "INSERT INTO t VALUES (2000, 1, -5, 0.5, 'filtered')",
    );
    let t0 = std::time::Instant::now();
    block_on(fx.mirror().poll_mirror(HELD)).expect("a poll the commit ends");
    assert!(t0.elapsed() < RELEASED, "the commit released it");
    writer.join().unwrap();
    assert_eq!(
        canonical(&fx.local("SELECT * FROM v_keyed")),
        before,
        "no row reached the view"
    );
    let now = fx.mirror().cursor_of(keyed).expect("still a copy");
    assert!(
        now.tick > settled.tick,
        "and the round it did not change is behind the cursor"
    );
}

/// A subscriber looping on a waiting poll under back-to-back commits is
/// answered no more often than the server's drain gap allows.
#[test]
fn a_waiting_loop_under_back_to_back_commits_is_paced() {
    const COMMITS: usize = 60;
    /// The server's gap between two drains waiting polls ask for.
    const GAP_MS: u128 = 10;

    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    fx.mirror_both();
    fx.drain();
    let q = "SELECT * FROM v_keyed";
    let before = canonical(&fx.local(q)).len();

    let target = fx.server.sock_path().to_string();
    let writer = std::thread::spawn(move || {
        let mut client = GnitzClient::connect(&target).unwrap();
        for i in 0..COMMITS {
            sql(
                &mut client,
                "s",
                &format!("INSERT INTO t VALUES ({}, 1, 5, 0.5, 'burst')", 5000 + i),
            );
        }
    });
    let t0 = std::time::Instant::now();
    let mut replies = 0;
    while canonical(&fx.local(q)).len() < before + COMMITS {
        block_on(fx.mirror().poll_mirror(HELD)).expect("poll");
        replies += 1;
    }
    let elapsed = t0.elapsed().as_millis();
    writer.join().unwrap();
    assert!(
        replies <= elapsed / GAP_MS + 2,
        "{replies} replies in {elapsed} ms: closer together than the gap"
    );
    fx.differential("s", q);
}

/// A view dropped under a waiting poll ends the wait with that view's own
/// failure.
#[test]
fn a_view_dropped_under_a_waiting_poll_ends_the_wait() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    fx.mirror_both();
    fed_view(&mut fx.direct, "s", "v_doomed", "SELECT a, b, v FROM t WHERE v >= 1");
    let doomed = block_on(fx.mirror().mirror_view(&rel("s", "v_doomed")))
        .expect("mirror")
        .view_id;
    fx.drain();

    let dropper = later(&fx, Duration::from_millis(200), "DROP VIEW v_doomed");
    let t0 = std::time::Instant::now();
    let report = block_on(fx.mirror().poll_mirror(HELD)).expect("a per-view failure is not the call's");
    assert!(t0.elapsed() < RELEASED, "the drop released it");
    dropper.join().unwrap();
    for o in &report {
        assert_eq!(
            matches!(o.result, PollResult::Failed(_)),
            o.view_id == doomed,
            "only the dropped view fails: {report:?}"
        );
    }
}

/// More views than the server reads at one cut are still one request and one
/// wait.
#[test]
fn a_wait_covers_more_views_than_one_cut_reads() {
    const M: usize = 70;
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let views = fx.many_views("w", M, "a, b, v");

    // View `i` keeps `v >= i`, so this row reaches every one of them.
    let writer = later(
        &fx,
        Duration::from_millis(200),
        "INSERT INTO t VALUES (3000, 1, 69, 0.5, 'wide')",
    );
    let t0 = std::time::Instant::now();
    let (report, sent) = cost(fx.mirror(), |m| block_on(m.poll_mirror(HELD)).expect("poll"));
    assert!(t0.elapsed() < RELEASED, "the commit released it");
    writer.join().unwrap();
    assert_eq!(sent, 1, "{M} views ride one request");
    assert_eq!(report.len(), M, "one entry per view");
    assert!(
        report.iter().all(|o| matches!(o.result, PollResult::Advanced)),
        "{report:?}"
    );
    for (name, _) in [&views[0], &views[M - 1]] {
        assert!(fx.differential("s", &format!("SELECT * FROM {name}")) > 20, "{name}");
    }
}

/// The next request a connection sends ends its waiting poll, and both are
/// answered in order; a request ahead of one is not held with it.
#[test]
fn a_waiting_poll_is_answered_when_its_connection_sends_the_next_request() {
    use gnitz_wire::txn_frame::{encode_delta_poll, DeltaPollItem};
    use gnitz_zset::schema::SchemaFacts;
    use std::io::{Read, Write};

    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    let (keyed, _) = fx.mirror_both();
    fx.drain();
    let item = DeltaPollItem {
        view_id: keyed,
        after_tick: fx.mirror().cursor_of(keyed).expect("a settled copy").tick.get(),
        reply_layout: block_on(fx.mirror().resolve_relation(&rel("s", "v_keyed")))
            .expect("resolve")
            .schema
            .layout_digest(),
    };

    let mut raw = std::os::unix::net::UnixStream::connect(fx.server.sock_path()).unwrap();
    let send = |raw: &mut std::os::unix::net::UnixStream, payload: &[u8]| {
        raw.write_all(&gnitz_wire::frame_len_prefix(payload.len())).unwrap();
        raw.write_all(payload).unwrap();
    };
    let recv = |raw: &mut std::os::unix::net::UnixStream| {
        let mut len = [0u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES];
        raw.read_exact(&mut len).unwrap();
        let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
        raw.read_exact(&mut payload).unwrap();
        payload
    };
    send(&mut raw, &gnitz_wire::HELLO);
    assert_eq!(recv(&mut raw), gnitz_wire::HELLO);

    let t0 = std::time::Instant::now();
    send(&mut raw, &encode_delta_poll(&[item], HELD.as_millis() as u64));
    send(&mut raw, &encode_delta_poll(&[item], 0));
    for which in ["the waiting poll", "the request behind it"] {
        let reply = recv(&mut raw);
        let ctrl = gnitz_wire::control::peek_control_block(&reply).expect("a control header");
        assert!(ctrl.fault(&reply).is_none(), "{which} is answered, not refused");
        assert_eq!(ctrl.hdr.target_id, keyed, "{which}");
    }
    assert!(t0.elapsed() < RELEASED, "the second request released the first");

    // A request ahead of a waiting poll, read in with it, is answered before
    // the poll is held.
    let mut both = Vec::new();
    for wait in [0, HELD.as_millis() as u64] {
        let payload = encode_delta_poll(&[item], wait);
        both.extend_from_slice(&gnitz_wire::frame_len_prefix(payload.len()));
        both.extend_from_slice(&payload);
    }
    let t0 = std::time::Instant::now();
    raw.write_all(&both).unwrap();
    recv(&mut raw);
    assert!(
        t0.elapsed() < RELEASED,
        "the reply ahead of the poll did not wait with it"
    );
    send(&mut raw, &encode_delta_poll(&[item], 0));
    recv(&mut raw);
    recv(&mut raw);
}

/// A server restart ends every feed, so a cursor checkpointed before it carries a
/// tag the new boot does not continue: re-mirroring reseeds each copy under its
/// id, inside the call.
///
/// A view over a stream comes back at the value it would have if the stream had
/// never received a row, and the copy must follow it *there*: one that resumed
/// across the restart would hold rows the server no longer has.
///
/// The store is closed and reopened, not reconnected: a reconnect drops every
/// cursor, and the cursor-less poll that follows never meets the foreign tag.
#[test]
fn a_cursor_from_before_a_server_restart_reseeds() {
    let mut fx = Fixture::start();
    sql(&mut fx.direct, "s", STREAM);
    fed_view(
        &mut fx.direct,
        "s",
        "v_stream",
        "SELECT kind, SUM(amount) AS total FROM ev GROUP BY kind",
    );
    let events: Vec<String> = (0..160).map(|i| format!("({i}, {}, {})", i % 5, i + 1)).collect();
    sql(
        &mut fx.direct,
        "s",
        &format!("INSERT INTO ev VALUES {}", events.join(",")),
    );
    churn(&mut fx.direct, 1, 60);
    let (keyed, repl) = fx.mirror_both();
    let stream = block_on(fx.mirror().mirror_view(&rel("s", "v_stream")))
        .expect("mirror v_stream")
        .view_id;
    let views = [("v_keyed", keyed), ("v_repl", repl), ("v_stream", stream)];
    fx.drain();
    fx.differential("s", "SELECT * FROM v_stream");

    fx.close();
    fx.restart_server();
    churn(&mut fx.direct, 61, 120);
    fx.open();
    for (name, id) in views {
        let out = block_on(fx.mirror().mirror_view(&rel("s", name))).expect(name);
        assert_eq!(out.view_id, id, "{name}: a restart keeps the id");
        assert!(out.result.reseeded(), "{name}: a foreign tag must reseed, not advance");
    }
    fx.differential("s", "SELECT * FROM v_keyed");
    fx.differential("s", "SELECT * FROM v_repl");
    // Spelled out rather than run through the differential: both sides are empty,
    // which it refuses as a pass that agrees about nothing.
    assert!(fx.mirror().mirrors(stream), "the stream view's copy answers locally");
    assert!(
        canonical(&fx.local("SELECT * FROM v_stream")).is_empty(),
        "a view over a stream comes back with zero rows, and the copy must follow it there",
    );
}

// ---------------------------------------------------------------------------
// Store shapes
// ---------------------------------------------------------------------------

/// The schema the shape cases seed for themselves.
///
/// `t` and `u` are two base tables where `u.tid` covers only the lower part of
/// `t.id`, so an outer join has unmatched rows and an `EXCEPT` is non-empty. `n`
/// has a nullable payload of every width, `k` a four-column PK carrying negative
/// and mixed-sign values — the OPK sign flip — and `blb` a `BLOB` payload, which
/// only the binary `create_table` API admits: no SQL type maps to it.
///
/// Beside the shared `seed` rather than folded into it: every other test would
/// otherwise carry these relations through every churn.
const SH: &str = "sh";

fn seed_shapes(client: &mut GnitzClient) {
    block_on(client.create_schema(SH)).unwrap();
    for table in [
        "t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, body TEXT NOT NULL)",
        "u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)",
        "n (id BIGINT NOT NULL PRIMARY KEY, i BIGINT, s TEXT, d DOUBLE PRECISION, r REAL)",
        "k (a BIGINT NOT NULL, b INTEGER NOT NULL, c SMALLINT NOT NULL, d BIGINT NOT NULL, \
            v BIGINT NOT NULL, PRIMARY KEY (a, b, c, d))",
    ] {
        sql(client, SH, &format!("CREATE TABLE {table}"));
    }
    let cols = vec![
        gnitz_wire::ColumnDef::new("id", gnitz_wire::TypeCode::I64, false),
        gnitz_wire::ColumnDef::new("b", gnitz_wire::TypeCode::Blob, true),
    ];
    block_on(client.create_table(
        &rel(SH, "blb"),
        &Schema { columns: cols, pk_cols: vec![0] },
        &[],
        gnitz_wire::TableProps::default(),
        &[],
    ))
    .expect("a BLOB column is admissible through the binary API");
}

/// Fresh keys in every table: `u` reaching only halfway up `t`'s range, every
/// nullable column of `n` null on one row in three, `k`'s key centred on zero.
fn churn_shapes(client: &mut GnitzClient, lo: i64, hi: i64) {
    let mid = lo + (hi - lo) / 2;
    let insert = |client: &mut GnitzClient, table: &str, rows: Vec<String>| {
        sql(client, SH, &format!("INSERT INTO {table} VALUES {}", rows.join(",")));
    };
    let t = (lo..=hi).map(|i| format!("({i}, {}, 'body-{i:0>20}')", i * 3));
    insert(client, "t", t.collect());
    let u = (lo..=mid).map(|i| format!("({i}, {i}, {})", i * 7));
    insert(client, "u", u.collect());
    let n = (lo..=hi).map(|i| match i % 3 {
        0 => format!("({i}, NULL, NULL, NULL, NULL)"),
        _ => format!("({i}, {}, 'text-{i:0>12}', {i}.25, {i}.5)", i * 3),
    });
    insert(client, "n", n.collect());
    let k = (lo - mid..=hi - mid).map(|i| format!("({i}, {}, {}, {}, {})", -i, i % 7, i * 2, i * 11));
    insert(client, "k", k.collect());

    let rel = block_on(client.resolve_relation(&rel(SH, "blb"))).unwrap();
    let mut batch = gnitz_core::ZSetBatch::new(&rel.schema);
    {
        let mut app = gnitz_core::BatchAppender::new(&mut batch);
        for i in lo..=hi {
            app.add_row(i as u128, 1);
            if i % 4 == 0 {
                app.null();
            } else {
                app.bytes_val(&vec![(i % 251) as u8; 1 + (i as usize % 17)]);
            }
        }
    }
    block_on(client.push(rel.tid, &rel.schema, &batch, gnitz_wire::WireConflictMode::Update)).expect("push blobs");
}

/// A round that touches only keys the copies already hold — the retract/insert
/// pair a PK-conflicting update produces, present values turned into nulls, and
/// pure retractions. A poll that covered only fresh keys would never fold a
/// weight onto an existing element.
fn rechurn_shapes(client: &mut GnitzClient, lo: i64, hi: i64) {
    for statement in [
        format!("UPDATE t SET v = v + 7 WHERE id >= {lo} AND id <= {hi}"),
        format!("UPDATE u SET w = w + 11 WHERE id >= {lo} AND id <= {hi}"),
        format!("UPDATE n SET i = NULL, s = NULL WHERE id >= {lo} AND id <= {hi}"),
        format!("UPDATE n SET d = NULL, r = NULL WHERE id > {hi}"),
        "UPDATE k SET v = v - 1 WHERE a < 0".to_string(),
        format!("DELETE FROM t WHERE id = {lo}"),
        format!("DELETE FROM u WHERE id = {}", lo + 1),
        format!("DELETE FROM n WHERE id = {lo}"),
        format!("DELETE FROM k WHERE a = {lo}"),
        format!("DELETE FROM blb WHERE id = {lo}"),
    ] {
        sql(client, SH, &statement);
    }
}

/// The view bodies, each with the reads that reach past its `SELECT *`.
const SHAPE_VIEWS: &[(&str, &str, &[&str])] = &[
    (
        "v_join",
        "SELECT t.id, t.body, u.w FROM t JOIN u ON t.id = u.tid",
        &["SELECT id, w FROM v_join WHERE w > 200"],
    ),
    (
        "v_left",
        "SELECT t.id, t.v, u.w FROM t LEFT JOIN u ON t.id = u.tid",
        &["SELECT id, v, w FROM v_left WHERE w IS NULL"],
    ),
    (
        "v_group",
        "SELECT tid, COUNT(*) AS n, SUM(w) AS total FROM u GROUP BY tid",
        &["SELECT tid, total FROM v_group WHERE total > 200"],
    ),
    (
        // `w` is null-filled by the outer join, so this keys by `_group_pk`.
        "v_group_null",
        "SELECT w, COUNT(*) AS n FROM v_left GROUP BY w",
        &["SELECT w, n FROM v_group_null WHERE n > 1"],
    ),
    (
        "v_except",
        "SELECT id FROM t EXCEPT SELECT tid FROM u",
        &["SELECT id FROM v_except WHERE id > 30"],
    ),
    (
        "v_distinct",
        "SELECT DISTINCT v FROM t",
        &["SELECT v FROM v_distinct WHERE v > 100"],
    ),
    (
        // An id `u` covers appears in both arms, so its element carries weight
        // 2: a copy with set semantics would still hold every row.
        "v_bag",
        "SELECT id FROM t UNION ALL SELECT tid FROM u",
        &["SELECT id FROM v_bag WHERE id > 20"],
    ),
    (
        // Over `v_group`, so the copy is fed by a view whose own source is a
        // maintained view rather than a base table.
        "v_chain",
        "SELECT tid, total FROM v_group WHERE total > 100",
        &["SELECT tid FROM v_chain WHERE tid > 20"],
    ),
    (
        "v_null",
        "SELECT id, i, s, d, r FROM n",
        &[
            "SELECT id, i FROM v_null WHERE i IS NULL",
            "SELECT id, s FROM v_null WHERE s IS NULL",
            "SELECT id, d, r FROM v_null WHERE d IS NOT NULL",
        ],
    ),
    (
        "v_blob",
        "SELECT id, b FROM blb",
        &["SELECT id, b FROM v_blob WHERE b IS NULL"],
    ),
    (
        // A point lookup and a range, both across the sign boundary.
        "v_pk",
        "SELECT a, b, c, d, v FROM k",
        &[
            "SELECT a, b, c, d, v FROM v_pk WHERE a = -17 AND b = 17 AND c = -3 AND d = -34",
            "SELECT a, v FROM v_pk WHERE a > -12 AND a < 12",
        ],
    ),
];

/// Every store shape a view body produces, mirrored in one handle, against a
/// server at four workers and at two.
///
/// Each puts a hidden synthetic key (`_join_pk`, `_group_pk`, `_set_pk`), a
/// compound signed key or a nullable payload through local registration,
/// `pk_stride` and the null bitmap, where a wrong shape is silently wrong rather
/// than an error. Two
/// workers is the other split where a mishandled concatenation of per-worker
/// bootstrap trains is real.
#[test]
fn every_view_body_shape_mirrors() {
    for workers in [WORKERS, 2] {
        let mut fx = Fixture::start_with(workers, &[]);
        seed_shapes(&mut fx.direct);
        churn_shapes(&mut fx.direct, 1, 80);
        for (name, body, _) in SHAPE_VIEWS {
            fed_view(&mut fx.direct, SH, name, body);
        }

        let names: Vec<&str> = SHAPE_VIEWS.iter().map(|(n, _, _)| *n).collect();
        for name in &names {
            let out = block_on(fx.mirror().mirror_view(&rel(SH, name))).expect(name);
            assert!(out.result.reseeded(), "{name}: a first registration bootstraps");
        }

        let mut compared = 0;
        for round in 0..3 {
            if round > 0 {
                rechurn_shapes(&mut fx.direct, 2 + round * 10, 40 + round * 10);
            }
            fx.drain();
            for (name, _, reads) in SHAPE_VIEWS {
                compared += fx.differential(SH, &format!("SELECT * FROM {name}"));
                for read in *reads {
                    compared += fx.differential(SH, read);
                }
            }
        }
        assert!(
            compared > 1500,
            "W={workers}: the shape differential compared only {compared} rows"
        );
    }
}

/// A bootstrap and a poll that each span many reply frames, over an
/// all-fixed-width view — every frame's data block a pure region copy, with no
/// heap to relocate — and over long TEXT, where every row points into the
/// batch's string heap and each frame ships a heap compacted to its own rows. A
/// loop that stopped at the first block loses rows silently.
///
/// The views are created over already-populated tables, so this also covers a
/// first bootstrap whose rows all arrived through a `CREATE VIEW` backfill —
/// rows the delta feed never carries.
#[test]
fn a_bootstrap_and_a_poll_span_many_frames() {
    let mut fx = Fixture::start_with(WORKERS, &[("GNITZ_REPLY_FRAME_BUDGET", "16384")]);
    block_on(fx.direct.create_schema("fr")).unwrap();
    sql(
        &mut fx.direct,
        "fr",
        "CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL)",
    );
    sql(
        &mut fx.direct,
        "fr",
        "CREATE TABLE d (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, body TEXT NOT NULL)",
    );
    // Rows per round. 200 bytes of heap per TEXT row, so a round of them is
    // ~1 MB across four workers — ~16× the frame budget on each of them.
    const WIDE: i64 = 20_000;
    const TEXT: i64 = 5_000;
    let fill = |client: &mut GnitzClient, round: i64| {
        for lo in (round * WIDE + 1..=(round + 1) * WIDE).step_by(1_000) {
            let rows: Vec<String> = (lo..lo + 1_000)
                .map(|i| format!("({i}, {}, {}, {})", i * 2, i * 3, i * 5))
                .collect();
            sql(client, "fr", &format!("INSERT INTO w VALUES {}", rows.join(",")));
        }
        for lo in (round * TEXT + 1..=(round + 1) * TEXT).step_by(500) {
            let rows: Vec<String> = (lo..lo + 500)
                .map(|i| format!("({i}, {}, 'row-{i}-{}')", i % 7, "z".repeat(200)))
                .collect();
            sql(client, "fr", &format!("INSERT INTO d VALUES {}", rows.join(",")));
        }
    };
    fill(&mut fx.direct, 0);
    fed_view(&mut fx.direct, "fr", "v_wide", "SELECT id, a, b, c FROM w WHERE a >= 0");
    fed_view(&mut fx.direct, "fr", "v_text", "SELECT id, k, body FROM d WHERE k >= 0");

    let views = [("v_wide", WIDE), ("v_text", TEXT)];
    for (view, _) in views {
        block_on(fx.mirror().mirror_view(&rel("fr", view))).expect(view);
    }
    for round in 1..=2 {
        if round == 2 {
            fill(&mut fx.direct, 1);
        }
        fx.drain();
        for (view, per_round) in views {
            let n = fx.differential("fr", &format!("SELECT * FROM {view}"));
            assert!(
                n as i64 >= per_round * round,
                "{view}: round {round} carried only {n} rows"
            );
        }
    }
}

/// Two schemas in one handle, and forgetting a view out of one.
///
/// A record carries its own schema name and the store enters no schema of its
/// own, so a retraction takes down exactly one view and leaves every other view
/// of that schema readable. Mirrored again — same server id, same layout — the
/// forgotten view bootstraps a fresh copy: the retraction removed the record and
/// the directory, and nothing weaker catches a mistake there, since a view
/// recreated upstream takes a *new* id.
#[test]
fn two_schemas_share_one_handle() {
    let mut fx = Fixture::start();
    seed_shapes(&mut fx.direct);
    churn_shapes(&mut fx.direct, 1, 60);
    churn(&mut fx.direct, 1, 60);
    fed_view(&mut fx.direct, SH, "v_lin", "SELECT id, v, body FROM t WHERE v > 10");
    fed_view(
        &mut fx.direct,
        SH,
        "v_grp",
        "SELECT tid, SUM(w) AS total FROM u GROUP BY tid",
    );

    let (keyed, _) = fx.mirror_both();
    let lin = block_on(fx.mirror().mirror_view(&rel(SH, "v_lin")))
        .expect("mirror v_lin")
        .view_id;
    block_on(fx.mirror().mirror_view(&rel(SH, "v_grp"))).expect("mirror v_grp");
    fx.drain();
    fx.differential("s", "SELECT * FROM v_keyed");
    fx.differential(SH, "SELECT * FROM v_lin");
    fx.differential(SH, "SELECT * FROM v_grp");

    block_on(fx.mirror().forget_view(lin)).expect("forget v_lin");
    assert!(
        fx.mirror().mirrors(keyed),
        "forgetting one view must not disturb another schema's copy"
    );
    fx.drain();
    fx.differential(SH, "SELECT * FROM v_grp");
    fx.differential("s", "SELECT * FROM v_keyed");
    fx.delegated(SH, "SELECT * FROM v_lin");

    let again = block_on(fx.mirror().mirror_view(&rel(SH, "v_lin"))).expect("re-mirror v_lin");
    assert_eq!(again.view_id, lin, "the same relation comes back under the same id");
    assert!(again.result.reseeded(), "a forgotten copy bootstraps afresh");
    fx.differential(SH, "SELECT * FROM v_lin");
}

/// A view dropped upstream fails its own poll forever, and nothing else stops.
///
/// Both halves come off the returned report — a `Failed` entry at the dropped
/// view's id and an `Advanced` one at the survivor's, with the round the
/// survivor now answers at beside it. The dropped view's copy **keeps answering
/// locally** until it is forgotten: the copy answers as of its last poll and that
/// round is real.
#[test]
fn a_view_dropped_upstream_names_itself_and_stops_nothing_else() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let (keyed, repl_id) = fx.mirror_both();
    fx.drain();
    let before_repl = fx.local("SELECT * FROM v_repl");
    let before_keyed = fx.mirror().cursor_of(keyed).expect("a cursor");

    sql(&mut fx.direct, "s", "DROP VIEW v_repl");
    churn(&mut fx.direct, 61, 120);
    let report = fx.drain();
    let dead = report
        .iter()
        .find(|o| o.view_id == repl_id)
        .expect("the dropped view is reported under its own id");
    assert!(
        matches!(&dead.result, PollResult::Failed(ClientError::Refused(f)) if f.status == WireStatus::NotFound),
        "the dropped view fails as not found: {:?}",
        dead.result,
    );

    let alive = report
        .iter()
        .find(|o| o.view_id == keyed)
        .expect("every view is attempted");
    assert!(matches!(alive.result, PollResult::Advanced));
    assert!(
        alive.cursor.is_some_and(|c| c.tick > before_keyed.tick),
        "the survivor advanced, and the report says to which round",
    );
    assert_eq!(
        alive.cursor,
        fx.mirror().cursor_of(keyed),
        "the reported round is the one the copy answers at",
    );
    fx.differential("s", "SELECT * FROM v_keyed");

    let still = fx.local("SELECT * FROM v_repl");
    assert_eq!(
        canonical(&before_repl),
        canonical(&still),
        "the dropped view's copy answers its last round until it is forgotten",
    );

    block_on(fx.mirror().forget_view(repl_id)).expect("forget the dead registration");
    let report =
        block_on(fx.mirror().poll_mirror(Duration::ZERO)).expect("the poll recovers once the dead view is gone");
    assert!(
        matches!(&report[..], [o] if o.view_id == keyed && matches!(o.result, PollResult::Advanced)),
        "only the survivor is reported, and it advances: {report:?}",
    );
}

/// A view recreated upstream under the same name takes a fresh id, and the poll
/// follows it there — re-resolving, retracting the old registration, registering
/// and reseeding the new one — without the host re-registering. A recovery that
/// bootstrapped in place would read a relation that no longer exists.
///
/// Both arms that reach it: a poll whose cursor names an id the server no longer
/// knows, and a cursor-less poll after a reconnect. Each recreation changes the
/// column set too — the reachable form of a schema change under a live mirror,
/// since `ALTER TABLE` is refused while a dependent view exists.
#[test]
fn a_recreated_view_is_followed_to_its_new_id() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let (mut old, repl) = fx.mirror_both();
    fx.drain();

    let rounds: [(i64, &[&str], bool); 2] = [(1, &["a", "b", "v"], false), (2, &["a", "b", "v", "f", "body"], true)];
    for (round, cols, reconnect) in rounds {
        sql(&mut fx.direct, "s", "DROP VIEW v_keyed");
        fed_view(
            &mut fx.direct,
            "s",
            "v_keyed",
            &format!("SELECT {} FROM t WHERE v >= 0", cols.join(", ")),
        );
        churn(&mut fx.direct, 1 + round * 60, 60 + round * 60);
        if reconnect {
            fx.reconnect_mirror();
        }

        let report = block_on(fx.mirror().poll_mirror(Duration::ZERO)).expect("the poll follows the view");
        let new = report
            .iter()
            .find(|o| o.view_id != old && o.view_id != repl)
            .unwrap_or_else(|| panic!("round {round}: the recreated view is reported at a fresh id: {report:?}"));
        assert!(new.result.reseeded(), "round {round}: and it reseeded");
        let new = new.view_id;
        assert!(
            !fx.mirror().mirrored_ids().contains(&old),
            "round {round}: the stale id is retracted, not kept beside the new one",
        );
        assert!(fx.mirror().mirrors(new));
        assert_eq!(
            fx.local("SELECT * FROM v_keyed").0.columns.len(),
            cols.len(),
            "round {round}: the copy reads under the new column set",
        );
        fx.drain();
        fx.differential("s", "SELECT * FROM v_keyed");
        fx.differential("s", "SELECT a, v FROM v_keyed WHERE a > 20 AND a < 60");
        old = new;
    }
}

/// A reconnect keeps every registration, is refused inside a transaction, and
/// **closes the read gate** until the next poll, which reseeds every view.
#[test]
fn a_reconnect_keeps_the_registrations_and_closes_the_read_gate() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let (keyed, repl) = fx.mirror_both();
    fx.drain();

    let target = fx.server.sock_path().to_string();
    fx.mirror().txn_begin().expect("begin");
    let refused = block_on(fx.mirror().reconnect(&target)).unwrap_err().to_string();
    assert!(refused.contains("inside a transaction"), "{refused}");
    fx.mirror().txn_rollback().expect("rollback");

    fx.reconnect_mirror();
    let ids: std::collections::BTreeSet<u64> = fx.mirror().mirrored_ids().into_iter().collect();
    assert_eq!(ids, [keyed, repl].into(), "the poll's work list survives a reconnect");
    for tid in [keyed, repl] {
        assert!(
            !fx.mirror().mirrors(tid),
            "every cursor is dropped, so the gate is shut"
        );
    }
    fx.delegated("s", "SELECT * FROM v_keyed");

    let report = block_on(fx.mirror().poll_mirror(Duration::ZERO)).expect("the poll after a reconnect");
    assert_eq!(report.len(), 2);
    assert!(
        report.iter().all(|o| o.result.reseeded()),
        "every view reseeds, and the report says so",
    );
    fx.drain();
    fx.differential("s", "SELECT * FROM v_keyed");
    fx.differential("s", "SELECT * FROM v_repl");
}

/// An expired cursor — one the retention sweep has rolled past — is recovered
/// inside the poll.
///
/// The server refuses it with its own status; the handle discards the copy and
/// re-reads it whole, and the host sees a `Reseeded` rather than an error whose
/// advice it has no way to act on.
#[test]
fn an_expired_cursor_reseeds_inside_the_poll() {
    // A delta store spills once its RAM tier crosses this ceiling; shrinking it
    // is how the capacity sweep is reached on modest data.
    let mut fx = Fixture::start_with(
        WORKERS,
        &[("GNITZ_RAM_TIER_BYTES", "1024"), ("GNITZ_CHECKPOINT_BYTES", "32768")],
    );
    sql(
        &mut fx.direct,
        "s",
        "CREATE VIEW v_tiny WITH (delta = '1 KB') AS SELECT a, b, v, body FROM t WHERE v >= 0",
    );
    churn(&mut fx.direct, 1, 60);
    let tid = block_on(fx.mirror().mirror_view(&rel("s", "v_tiny")))
        .expect("mirror v_tiny")
        .view_id;
    fx.drain();

    // Push the workers' retention floor past the copy's cursor, without polling.
    // Wide rows and many rounds, so what the sweep drops is measured in hundreds
    // of kilobytes against a one-kilobyte budget.
    // Each body distinct: a fold shares one heap span among equal strings, and
    // rows that collapsed that way would never fill a memtable.
    for k in 0..20 {
        let lo = 1_000 + k * 200;
        let rows: Vec<String> = (lo..lo + 200)
            .map(|i| format!("({i}, {}, {i}, 0.5, '{i:x>200}')", i % 7))
            .collect();
        sql(&mut fx.direct, "s", &format!("INSERT INTO t VALUES {}", rows.join(",")));
    }

    let report = fx.drain();
    assert!(
        report.iter().any(|o| o.view_id == tid && o.result.reseeded()),
        "an expired cursor is recovered by reseeding: {report:?}",
    );
    fx.drain();
    fx.differential("s", "SELECT * FROM v_tiny");
    fx.differential("s", "SELECT a, v FROM v_tiny WHERE a > 4000");
}

// ---------------------------------------------------------------------------
// Upstream DDL
// ---------------------------------------------------------------------------

/// Every DDL bundle **through the mirroring client** that retires a view retires
/// its copy, whatever verb built it — the VIEW_TAB batch the statement pushed is
/// what says so, so no verb carries a teardown of its own.
///
/// The copy and the DDL path share one object, so without it a `SELECT` after a
/// `DROP VIEW` on this same client answers rows off a view it just dropped.
#[test]
fn a_ddl_that_retires_a_view_retires_its_copy() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    fed_view(&mut fx.direct, "s", "v_drop", "SELECT a, b, v FROM t WHERE v >= 0");
    fed_view(&mut fx.direct, "s", "v_rep", "SELECT a, b, v FROM t WHERE b = 1");
    block_on(fx.direct.create_schema("s2")).expect("a second schema");
    sql(
        &mut fx.direct,
        "s2",
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    sql(&mut fx.direct, "s2", "INSERT INTO u VALUES (1, 10), (2, 20), (3, 30)");
    fed_view(&mut fx.direct, "s2", "v_u", "SELECT id, v FROM u WHERE v > 0");

    let replace = format!("CREATE OR REPLACE VIEW v_rep WITH (delta = '{FEED}') AS SELECT a, b, v FROM t WHERE b = 2");
    /// `(schema, view, the DDL that retires it)`.
    type Case<'a> = (&'a str, &'a str, &'a dyn Fn(&mut GnitzClient));
    let cases: [Case; 3] = [
        ("s", "v_drop", &|m| sql(m, "s", "DROP VIEW v_drop")),
        // The retired view's lone `-1` rides the same batch as the replacement's
        // `+1`s, which take a fresh id.
        ("s", "v_rep", &|m| sql(m, "s", &replace)),
        // One `-1` per member view, all in one bundle.
        ("s2", "v_u", &|m| {
            block_on(m.drop_schema("s2")).expect("drop the schema")
        }),
    ];
    for (schema, view, retire) in cases {
        let tid = block_on(fx.mirror().mirror_view(&rel(schema, view)))
            .expect(view)
            .view_id;
        fx.drain();
        assert!(fx.mirror().mirrors(tid), "{view}: the copy answers before the DDL");
        retire(fx.mirror());
        assert!(!fx.mirror().mirrors(tid), "{view}: the copy stops answering");
        assert!(
            !fx.mirror().mirrored_ids().contains(&tid),
            "{view}: and the registration goes with it",
        );
    }
    fx.delegated("s", "SELECT * FROM v_rep");
}

/// A rename **through the mirroring client** renames the copy rather than
/// destroying it.
///
/// A rename keeps the id, the layout and the rows, so the store's registration
/// takes its in-place arm — the same one a rename by *another* client takes
/// (`a_view_renamed_upstream_survives_a_new_view_under_its_old_name`). The same
/// upstream event must not cost a full re-bootstrap merely because this
/// connection is the one that issued it.
#[test]
fn a_rename_through_the_mirroring_client_keeps_the_copy() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    let tid = block_on(fx.mirror().mirror_view(&rel("s", "v_keyed")))
        .expect("mirror")
        .view_id;
    fx.drain();
    let before = fx.mirror().cursor_of(tid).expect("a round to answer at");

    sql(fx.mirror(), "s", "ALTER TABLE v_keyed RENAME TO v_moved");

    assert_eq!(
        fx.mirror().cursor_of(tid),
        Some(before),
        "the copy survives a rename this client issued, at the feed position it had",
    );
    fx.differential("s", "SELECT * FROM v_moved");
    // The old name is gone rather than answering off the copy.
    assert!(
        block_on(gnitz_sql::execute(fx.mirror(), "s", "SELECT * FROM v_keyed")).is_err(),
        "a renamed view must not keep answering under its old name",
    );

    // The feed continues under the new name — the rename moved the binding, not
    // just the label.
    churn(&mut fx.direct, 41, 80);
    fx.drain();
    assert!(
        fx.mirror().cursor_of(tid).is_some_and(|c| c.tick > before.tick),
        "the copy advances after the rename",
    );
    fx.differential("s", "SELECT * FROM v_moved");
}

/// A rename through a client that has **not** claimed the copy retracts the
/// record instead of binding it.
///
/// After a reopen the store holds every copy a previous session left and this
/// session has registered none of them. Binding one here would mirror a view the
/// host never asked for; leaving it would leave a stale name in the store's
/// record, which a later registration's name scan would match against a
/// different view created under it.
#[test]
fn a_rename_by_a_client_that_never_claimed_the_copy_retracts_the_record() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    let tid = block_on(fx.mirror().mirror_view(&rel("s", "v_keyed")))
        .expect("mirror")
        .view_id;
    fx.drain();

    fx.reopen();
    assert!(
        !fx.mirror().mirrored_ids().contains(&tid),
        "the new session claimed nothing",
    );
    assert!(
        has_copy(&fx.base_dir(), tid),
        "but the store still holds the replayed copy"
    );

    sql(fx.mirror(), "s", "ALTER TABLE v_keyed RENAME TO v_moved");
    assert!(
        !has_copy(&fx.base_dir(), tid),
        "an unclaimed record is retracted with its directory, not left naming the old name",
    );

    // The freed name is now genuinely free: a new view under it mirrors clean.
    fed_view(&mut fx.direct, "s", "v_keyed", "SELECT a, b, v FROM t WHERE b = 3");
    let fresh = block_on(fx.mirror().mirror_view(&rel("s", "v_keyed")))
        .expect("mirror the new view")
        .view_id;
    assert_ne!(fresh, tid);
    fx.drain();
    fx.differential("s", "SELECT * FROM v_keyed");
}

/// `DROP VIEW v_0; ALTER VIEW v_1 RENAME TO v_0` upstream, with both mirrored.
///
/// Phase one holds `B`'s reply for `(prev_B, T]` while `A`'s poll is refused.
/// Recovering `A` inside that loop would re-point its registration at `B` —
/// whose copy and cursor are live — and re-fetch an interval `B`'s reply already
/// carries; applying both leaves the **row set identical** and every weight in
/// the overlap doubled, which is why the check here is a weight-exact
/// differential rather than a shape assertion.
#[test]
fn a_drop_and_a_rename_into_the_freed_name_report_once_each() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let [(_, a), (_, b)] = fx.many_views("v_", 2, "a, b, v")[..] else {
        unreachable!()
    };

    // Rounds both copies are behind on, so phase one really does hold `B`'s
    // reply when `A`'s poll comes back refused.
    churn(&mut fx.direct, 61, 120);
    sql(&mut fx.direct, "s", "DROP VIEW v_0");
    sql(&mut fx.direct, "s", "ALTER TABLE v_1 RENAME TO v_0");
    let report = fx.drain();
    assert!(
        matches!(&report[..], [o] if o.view_id == b && matches!(o.result, PollResult::Advanced)),
        "one entry per mirrored view, at the id it ended up under, and the \
         survivor advanced rather than being reseeded by the other's recovery: {report:?}",
    );
    assert!(
        !fx.mirror().mirrored_ids().contains(&a),
        "the dropped view's registration moved rather than staying beside it",
    );
    fx.drain();
    fx.differential("s", "SELECT * FROM v_0");
}

/// A view another client renamed keeps its copy when this client re-mirrors it
/// under the new name and then mirrors a new view created under the old one:
/// the record's name is refreshed, so the new view does not match it.
#[test]
fn a_view_renamed_upstream_survives_a_new_view_under_its_old_name() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let renamed = block_on(fx.mirror().mirror_view(&rel("s", "v_keyed")))
        .expect("mirror v_keyed")
        .view_id;
    fx.drain();

    sql(&mut fx.direct, "s", "ALTER TABLE v_keyed RENAME TO v_moved");
    fed_view(&mut fx.direct, "s", "v_keyed", "SELECT a, b, v FROM t WHERE b = 3");

    assert_eq!(
        block_on(fx.mirror().mirror_view(&rel("s", "v_moved")))
            .expect("re-mirror under the new name")
            .view_id,
        renamed,
        "a rename keeps the id",
    );
    let fresh = block_on(fx.mirror().mirror_view(&rel("s", "v_keyed")))
        .expect("mirror the new view")
        .view_id;
    assert_ne!(fresh, renamed);
    assert!(
        fx.mirror().mirrors(renamed),
        "the renamed view's copy must survive a new view under its old name",
    );
    assert!(fx.mirror().mirrors(fresh));
    fx.drain();
    fx.differential("s", "SELECT * FROM v_moved");
    fx.differential("s", "SELECT * FROM v_keyed");
}

/// Two views cross-renamed upstream: `v_0 → v_c`, then `v_1 → v_0`. Mirroring
/// the reused name retracts the copy that lost it. Both views share a layout, so
/// the registration at `v_1`'s id takes the rename-in-place arm.
#[test]
fn a_cross_rename_retracts_the_copy_that_lost_its_name() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let [(_, a), (_, b)] = fx.many_views("v_", 2, "a, b, v")[..] else {
        unreachable!()
    };
    assert!(has_copy(&fx.base_dir(), a), "v_0's copy is on disk");

    sql(&mut fx.direct, "s", "ALTER TABLE v_0 RENAME TO v_c");
    sql(&mut fx.direct, "s", "ALTER TABLE v_1 RENAME TO v_0");

    let out = block_on(fx.mirror().mirror_view(&rel("s", "v_0"))).expect("re-mirror");
    assert_eq!(out.view_id, b, "the reused name now resolves to the other view");
    // The arm under test: `v_1`'s record stood, so its copy kept its cursor and
    // advanced. A reseed here would mean the registration fell through to the
    // arm that always retracted, and the assertions below would prove nothing.
    assert!(
        !out.result.reseeded(),
        "the record at this id stands, so the rename is in place: {:?}",
        out.result
    );
    assert!(
        !fx.mirror().mirrored_ids().contains(&a),
        "the client drops the displaced copy's binding",
    );
    assert!(
        !has_copy(&fx.base_dir(), a),
        "and the displaced copy's directory is gone, not left behind",
    );
    fx.drain();
    fx.differential("s", "SELECT * FROM v_0");
}

// ---------------------------------------------------------------------------
// Poison
// ---------------------------------------------------------------------------

/// A poisoned store refuses a mirrored read rather than answering it, an unheld
/// relation still reads through the same client, and `close_mirror` is the way
/// out.
///
/// In a child because the seam is read once per process. It is the one test of
/// the panic guard: nothing but a panic reaches its arm.
#[test]
#[cfg_attr(not(debug_assertions), ignore = "the seam folds away in a release build")]
fn a_poisoned_store_refuses_the_reads_it_would_answer() {
    run_child(
        "poisoned_read_child",
        &[("GNITZ_INJECT_MIRROR_INGEST_PANIC", "1")],
        "a poisoned copy must refuse its own reads and leave every other one alone",
    );
}

/// Runs only in the child the test above spawns.
#[test]
fn poisoned_read_child() {
    let Some((sock, dir)) = child_target() else { return };
    let mut direct = GnitzClient::connect(&sock).unwrap();
    let mut mirror = mirroring_client(&sock, &dir);
    let tid = block_on(mirror.mirror_view(&rel("s", "v_keyed")))
        .expect("the bootstrap succeeds")
        .view_id;
    assert!(mirror.mirrors(tid), "the copy is valid before the poll that panics");

    churn(&mut direct, 41, 80);
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(mirror.poll_mirror(Duration::ZERO))
    }));
    assert!(unwound.is_err(), "the armed seam must panic inside the guarded region");
    assert!(
        mirror.mirror_poisoned().is_some(),
        "the guard poisons before it lets the unwind continue",
    );

    // The copy is still gated in — cursor and registration both stand — so this
    // is a read the store would otherwise have answered.
    assert!(mirror.mirrors(tid));
    let err = block_on(gnitz_sql::execute(&mut mirror, "s", "SELECT * FROM v_keyed"))
        .expect_err("a poisoned copy must refuse the read it would answer");
    assert!(
        matches!(
            err,
            GnitzSqlError::Client(ClientError::Mirror(MirrorError::Poisoned(_)))
        ),
        "{err}"
    );

    // Neither call-level verb spends a round trip finding out.
    let (refused, sent) = cost(&mut mirror, |m| {
        [
            block_on(m.poll_mirror(Duration::ZERO)).err(),
            block_on(m.mirror_view(&rel("s", "v_repl"))).err(),
        ]
    });
    assert!(
        refused
            .iter()
            .all(|e| matches!(e, Some(ClientError::Mirror(MirrorError::Poisoned(_))))),
        "{refused:?}"
    );
    assert_eq!(sent, 0);

    // A relation the copy does not hold is unaffected: poison must not take
    // every read on the connection down with it.
    differential(&mut mirror, &mut direct, "s", "SELECT * FROM t", Answer::Upstream);

    // A DDL that retires the mirrored view must not fail because the store
    // refuses the teardown: the client-side gate is what stops the copy
    // answering, and everything past it is reclamation.
    block_on(gnitz_sql::execute(&mut mirror, "s", "DROP VIEW v_keyed"))
        .expect("a poisoned store must not fail a DDL that already committed");
    assert!(!mirror.mirrors(tid), "and the copy stops answering all the same");

    // `close_mirror` publishes no checkpoint, releases the directory, and leaves
    // the connection usable — the only recovery a poisoned copy has.
    block_on(mirror.close_mirror()).expect("closing a poisoned store is not an error");
    assert!(
        !has_manifest(&dir, tid),
        "a poisoned store must publish nothing on the way out",
    );
    assert!(mirror.mirror_poisoned().is_none(), "the poison went with the store");
    mirror
        .attach_mirror(open_store(&dir))
        .expect("a later attach on the same connection is legal");
    let recovered = block_on(mirror.mirror_view(&rel("s", "v_repl"))).expect("and it bootstraps again");
    assert!(recovered.result.reseeded());
}

// ---------------------------------------------------------------------------
// Child-process plumbing
// ---------------------------------------------------------------------------

/// Start a child on `name` against a seeded, churned server and an empty mirror
/// directory, with `envs` on top, and assert it ran and passed.
fn run_child(name: &str, envs: &[(&str, &str)], what: &str) {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    // The directory is the child's to open.
    fx.mirror = None;
    let dir = fx.base_dir();
    let mut all = vec![
        ("GNITZ_MIRROR_SOCK", fx.server.sock_path()),
        ("GNITZ_MIRROR_DIR", dir.as_str()),
    ];
    all.extend_from_slice(envs);
    let out = run_test_in_child(module_path!(), name, &all);
    assert_child_ok(&out, what);
}

/// What a parent handed this child, or `None` in the parent's own run of the
/// same test — every child test is also collected by the outer `cargo test`.
fn child_target() -> Option<(String, String)> {
    Some((
        std::env::var("GNITZ_MIRROR_SOCK").ok()?,
        std::env::var("GNITZ_MIRROR_DIR").ok()?,
    ))
}

/// A name whose copy answers reads is planned from the copy's descriptor, kept
/// descriptor or not: the read and its EXPLAIN agree, and neither asks the
/// server.
#[test]
fn a_mirrored_name_is_read_off_its_copy_whatever_is_kept() {
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let old = block_on(fx.mirror().mirror_view(&rel("s", "v_keyed")))
        .expect("mirror v_keyed")
        .view_id;
    fx.drain();

    sql(&mut fx.direct, "s", "DROP VIEW v_keyed");
    fed_view(&mut fx.direct, "s", "v_keyed", "SELECT a, b, v FROM t WHERE b = 3");
    let new = block_on(fx.mirror().resolve_relation(&rel("s", "v_keyed")))
        .expect("resolve")
        .tid;
    assert_ne!(new, old, "the kept descriptor names the recreated view");

    let (_, requests) = cost(fx.mirror(), |c| query(c, "s", "SELECT * FROM v_keyed"));
    assert_eq!(requests, 0, "the copy answers the read");
    let (_, plan) = query(fx.mirror(), "s", "EXPLAIN SELECT * FROM v_keyed");
    assert!(String::from_utf8_lossy(&plan.blob).contains("local copy"));
}
