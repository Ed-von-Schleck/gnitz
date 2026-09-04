//! The acceptance suite for a client that mirrors: one `GnitzClient` with a
//! store attached, read through the SQL layer against a live server.
//!
//! A Rust integration test rather than a pytest: it reaches a real server through
//! `gnitz-test-harness` and builds its `ReadSpec`s through `gnitz-sql`, both
//! **dev**-dependencies — neither is in the crate's dependency graph, and
//! neither closes a cycle, since nothing depends on `gnitz-mirror`.
//!
//! `tests/store.rs` beside it covers the store alone, with no server and no
//! client.
//!
//! **No test runs at one worker.** At W=1 there is one slice, so a mirror that
//! mishandled the concatenation of W slices would still pass, and the
//! replicated-vs-keyed routing split collapses to the same thing. Four is the
//! default; one case runs at two, which moves that split rather than removing
//! it.

mod support;

use gnitz_core::{ClientError, GnitzClient, MirrorError, PollResult, Schema, ZSetBatch};
use gnitz_mirror::Mirror;
use gnitz_sql::SqlPlanner;
use gnitz_store::relation::{relation_dir, RelationKind};
use gnitz_store::storage::{ChildAddr, Slot};
use gnitz_store_testkit::{assert_child_ok, run_test_in_child, CHILD_OK};
use gnitz_test_harness::ServerHandle;
use support::{assert_same_sequence, assert_same_zset, canonical, query, serial, sql, EnvVar};

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

/// A client on `target` with a store on `dir` attached.
fn mirroring_client(target: &str, dir: &str) -> GnitzClient {
    let mut client = GnitzClient::connect(target).unwrap();
    client
        .attach_mirror(Mirror::open(dir).expect("a store opens"))
        .expect("a fresh client attaches it");
    client
}

/// A four-worker server, or a panic naming what is missing. The harness answers
/// `None` when the binary is absent, and an early `return` on that would report
/// green having run nothing — with no CI, that silence has no beneficiary.
fn start_server() -> ServerHandle {
    start_server_with(WORKERS, &[])
}

/// The same, at a chosen worker count and with server-side environment of its
/// own — the harness sets it on the server process alone, so a seam one test
/// needs cannot race a sibling through the process env.
fn start_server_with(workers: usize, env: &[(&str, &str)]) -> ServerHandle {
    ServerHandle::start_with_env(workers, env)
}

impl Fixture {
    fn start() -> Fixture {
        Fixture::start_with(WORKERS, &[])
    }

    fn start_with(workers: usize, env: &[(&str, &str)]) -> Fixture {
        let server = start_server_with(workers, env);
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
        self.mirror().reconnect(&target).expect("reconnect");
    }

    /// Close the client's store — the exit checkpoint — and open a fresh one on
    /// the same directory.
    fn reopen(&mut self) {
        let mut client = self.mirror.take().expect("a client to close");
        client.close_mirror().expect("exit checkpoint");
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
    /// produced, then carry them into the copies.
    ///
    /// The order is not cosmetic and it is the whole freshness contract: a poll
    /// does **not** drive a tick and a read against the server does, so a push
    /// ACKed but not yet ticked is in the server's answer and not in the copy.
    /// Reading first drains, so the rounds exist before the poll that collects
    /// them. `COUNT(*)` because only the drain is wanted: it folds server-side
    /// and replies with one row per worker where `SELECT *` ships the view.
    fn drain(&mut self, schema: &str, views: &[&str]) {
        for v in views {
            let _ = query(&mut self.direct, schema, &format!("SELECT COUNT(*) AS n FROM {v}"));
        }
        self.mirror().poll_mirror().expect("poll");
    }

    /// [`Fixture::drain`] over the two views of the shared schema.
    fn quiesce(&mut self) {
        self.drain("s", &["v_keyed", "v_repl"]);
    }

    /// Mirror both views of the shared schema, and return their ids. Nearly
    /// every test wants both: the keyed one carries the compound PK, the
    /// replicated one the worker-0 feed.
    fn mirror_both(&mut self) -> (u64, u64) {
        let keyed = self.mirror().mirror_view("s", "v_keyed").expect("mirror v_keyed");
        let repl = self.mirror().mirror_view("s", "v_repl").expect("mirror v_repl");
        (keyed.view_id, repl.view_id)
    }

    /// Run one `SELECT` through the mirroring client.
    fn local(&mut self, sql_text: &str) -> (Schema, ZSetBatch) {
        query(self.mirror.as_mut().unwrap(), "s", sql_text)
    }

    /// Assert a query answers identically locally and against the server, and
    /// return how many rows that agreement covered. The vacuous pass is refused
    /// inside `assert_same_zset`; the count is here so a caller comparing several
    /// queries can hold their sum to a floor of its own.
    fn differential(&mut self, schema: &str, sql_text: &str) -> usize {
        let local = query(self.mirror.as_mut().unwrap(), schema, sql_text);
        let remote = query(&mut self.direct, schema, sql_text);
        assert_same_zset(sql_text, (&local.0, &local.1), (&remote.0, &remote.1))
    }

    /// The same, comparing the reply as a **sequence** — for an `ORDER BY` query,
    /// where the multiset comparison would accept the right rows in the wrong
    /// order.
    fn ordered_differential(&mut self, schema: &str, sql_text: &str) -> usize {
        let local = query(self.mirror.as_mut().unwrap(), schema, sql_text);
        let remote = query(&mut self.direct, schema, sql_text);
        assert_same_sequence(sql_text, (&local.0, &local.1), (&remote.0, &remote.1))
    }
}

/// The schema every test shares.
///
/// `v_keyed` has a **two-column PK** on purpose: a single-column `BIGINT` PK
/// makes a wrong PK stride look right, so the round-stamp strip would go
/// untested at exactly the point it can be silently wrong. `v_repl` is
/// replicated, which routes its feed to worker 0 alone rather than broadcasting.
fn seed(client: &mut GnitzClient) {
    client.create_schema("s").unwrap();
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
    sql(
        client,
        "s",
        &format!("CREATE VIEW v_keyed WITH (delta = '{FEED}') AS SELECT a, b, v, f, body FROM t WHERE v >= 0"),
    );
    sql(
        client,
        "s",
        &format!("CREATE VIEW v_repl WITH (delta = '{FEED}') AS SELECT id, v FROM r WHERE v > 3"),
    );
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

// ---------------------------------------------------------------------------
// The differential
// ---------------------------------------------------------------------------

/// A mirrored read equals the same read against the server, over every sink a
/// direct SELECT can route to: the plain scan, the bounded/projected rows sink,
/// and the aggregate fold.
///
/// The comparison runs after the ordinary client-side finishing on both sides,
/// which is the point: the local reply is a single-worker reply, and the
/// finishers take it unchanged.
#[test]
fn a_mirrored_read_equals_the_server_read() {
    let _g = serial();
    let mut fx = Fixture::start();

    churn(&mut fx.direct, 1, 200);
    fx.mirror_both();
    fx.quiesce();

    let mut compared = 0;
    for q in [
        "SELECT * FROM v_keyed",
        "SELECT a, b, v FROM v_keyed WHERE a = 42",
        "SELECT a, v FROM v_keyed WHERE a > 100 AND a < 140",
        "SELECT b, COUNT(*) AS n, SUM(v) AS total, MIN(a) AS lo, MAX(a) AS hi FROM v_keyed GROUP BY b",
        "SELECT COUNT(*) AS n, SUM(v) AS total FROM v_keyed",
        "SELECT DISTINCT b FROM v_keyed",
        "SELECT b, SUM(v) AS total FROM v_keyed GROUP BY b HAVING SUM(v) > 1000",
        "SELECT * FROM v_repl",
        "SELECT COUNT(*) AS n, SUM(v) AS total FROM v_repl",
    ] {
        compared += fx.differential("s", q);
    }
    assert!(compared > 300, "the differential compared only {compared} rows");
}

/// The poll arm, across rounds that touch the same keys.
///
/// Three distinct bugs in the round-stamp strip hide from a weaker test: a
/// single-column `BIGINT` PK makes a wrong stride look right, a bootstrap-only
/// test never runs the poll arm at all, and a poll that covers one round at a
/// time never exposes an inherited `Consolidated` layout claim — a single
/// round's capture really is sorted and distinct, so the claim would be true. So
/// this polls only after several ticks have accumulated, over a compound-PK view
/// whose keys are hit again in every batch.
#[test]
fn a_multi_round_poll_over_repeated_keys_stays_weight_exact() {
    let _g = serial();
    let mut fx = Fixture::start();

    churn(&mut fx.direct, 1, 60);
    fx.mirror_both();
    fx.quiesce();
    fx.differential("s", "SELECT * FROM v_keyed");

    for round in 0..3 {
        // Three ticks per poll, all hitting keys the copy already holds: the
        // strip must leave the batch claiming a raw layout, so the ingest's own
        // sort-and-fold sums the repeats onto the right element.
        for step in 0..3 {
            let lo = 1 + round * 20;
            sql(
                &mut fx.direct,
                "s",
                &format!("UPDATE t SET v = v + {} WHERE a >= {lo} AND a < {}", step + 1, lo + 25),
            );
        }
        churn(&mut fx.direct, 61 + round * 40, 100 + round * 40);
        fx.quiesce();
        fx.differential("s", "SELECT * FROM v_keyed");
        fx.differential("s", "SELECT * FROM v_repl");
    }
}

/// Integer aggregates match exactly; a float aggregate is compared within
/// tolerance.
///
/// Float SUM/AVG is order-dependent — addition is non-associative — and the
/// mirror is one partition where the server is W. This is the same divergence
/// the server already has between two worker counts, not a mirror defect.
#[test]
fn integer_aggregates_are_exact_and_floats_are_close() {
    let _g = serial();
    let mut fx = Fixture::start();

    churn(&mut fx.direct, 1, 300);
    fx.mirror_both();
    fx.quiesce();

    fx.differential("s", "SELECT b, COUNT(*) AS n, SUM(v) AS total FROM v_keyed GROUP BY b");

    let q = "SELECT b, SUM(f) AS tf, AVG(f) AS af FROM v_keyed GROUP BY b";
    let local = fx.local(q);
    let remote = query(&mut fx.direct, "s", q);
    let (la, ra) = (canonical(&local.0, &local.1), canonical(&remote.0, &remote.1));
    assert_eq!(la.len(), ra.len(), "float aggregate must produce the same groups");
    for ((lk, lw), (rk, rw)) in la.iter().zip(ra.iter()) {
        assert_eq!(lk.0, rk.0, "float aggregate groups must line up by key");
        assert_eq!(lw, rw, "an aggregate row's weight is exact whatever its value");
        for (lc, rc) in lk.1.iter().zip(rk.1.iter()) {
            let (Some(lb), Some(rb)) = (lc, rc) else { continue };
            let (lv, rv) = (f64_of(lb), f64_of(rb));
            assert!(
                (lv - rv).abs() <= 1e-6 * rv.abs().max(1.0),
                "float aggregate diverged beyond reassociation: {lv} vs {rv}",
            );
        }
    }
}

fn f64_of(b: &[u8]) -> f64 {
    f64::from_le_bytes(b.try_into().expect("a DOUBLE cell is eight bytes"))
}

// ---------------------------------------------------------------------------
// The round trip
// ---------------------------------------------------------------------------

/// A registered view is described out of the local registration; an unregistered
/// name delegates. The planner asks for exactly one descriptor per relation, so
/// this call is what keeps a mirrored SELECT round-trip-free.
#[test]
fn a_registered_view_is_described_locally() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 10);
    let (keyed, _) = fx.mirror_both();
    fx.quiesce();

    let m = fx.mirror.as_mut().unwrap();
    let before = m.requests_sent();
    let desc = m
        .resolve_local_first("s", "v_keyed")
        .expect("a registered view describes")
        .expect("and is present");
    assert_eq!(
        m.requests_sent(),
        before,
        "describing a registered view must issue no request"
    );
    assert_eq!(desc.tid, keyed, "the copy is held under the server's id");
    assert!(desc.class.is_view());
    assert!(desc.delta, "only a fed view can be mirrored");
    assert!(!desc.replicated, "a resolve reports replication for a base table alone");
    assert!(desc.indexes.is_empty(), "only a base table may own an index");

    // An unregistered relation goes upstream: one RESOLVE, answered by the server.
    let before = m.requests_sent();
    let delegated = m
        .resolve_local_first("s", "t")
        .expect("a delegated describe")
        .expect("and the table is present");
    assert_eq!(m.requests_sent() - before, 1, "a delegated describe costs one RESOLVE");
    assert!(!delegated.class.is_view());
}

/// A `CREATE VIEW` over a mirrored view binds the server's id, not the local
/// registration's. The registration is only as fresh as the last poll, and this
/// statement writes a durable catalog row against it. The view is recreated
/// upstream under a fresh id here; the new view must read that one.
#[test]
fn a_view_created_over_a_mirrored_view_binds_the_servers_id() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 10);
    fx.mirror_both();
    fx.quiesce();

    let live = query(&mut fx.direct, "s", "SELECT a, b FROM v_keyed").1.len();
    assert!(live > 0, "the differential needs rows to differ over");

    // Recreate the mirrored view upstream: same name, fresh id. Nothing tells the
    // mirror, so its registration still binds the retired one.
    sql(&mut fx.direct, "s", "DROP VIEW v_keyed");
    sql(
        &mut fx.direct,
        "s",
        &format!("CREATE VIEW v_keyed WITH (delta = '{FEED}') AS SELECT a, b, v, f, body FROM t WHERE v >= 0"),
    );

    {
        let m = fx.mirror.as_mut().unwrap();
        let before = m.requests_sent();
        let mut planner = gnitz_sql::SqlPlanner::new(m, "s");
        planner
            .execute("CREATE VIEW over_mirror AS SELECT a, b FROM v_keyed")
            .expect("a view over the recreated relation");
        drop(planner);
        assert!(m.requests_sent() > before, "a DDL statement must resolve upstream");
    }

    // Bound to the retired id the new view would backfill from a relation that no
    // longer exists and come back empty.
    let got = query(&mut fx.direct, "s", "SELECT a, b FROM over_mirror").1.len();
    assert_eq!(got, live, "the new view reads the live v_keyed, not the retired one");
}

/// A mirrored SELECT issues no request; a delegated one issues exactly one.
///
/// The first covers the local `describe_relation`: a delegated resolve would show
/// up as one request per statement and nothing else in this suite would notice.
/// The second pins the statement bracket — without it the descriptor-by-id lookup
/// misses the scope and the index probe issues a second RESOLVE, so the count
/// reads two.
///
/// Both are counts, not clocks.
#[test]
fn a_mirrored_select_issues_no_request() {
    let _g = serial();
    let mut fx = Fixture::start();

    churn(&mut fx.direct, 1, 40);
    fx.mirror_both();
    fx.quiesce();

    let m = fx.mirror.as_mut().unwrap();
    let before = m.requests_sent();
    let _ = query(m, "s", "SELECT a, b, v FROM v_keyed WHERE a = 7");
    let after = m.requests_sent();
    assert_eq!(after, before, "a mirrored SELECT must issue no request at all");

    // The `EXPLAIN` of one is served by the copy as well. Routed to the
    // connection it would cost a round trip, fail with the server down, and
    // describe a plan against a relation the statement will not read.
    let before = m.requests_sent();
    let (_, plan) = query(m, "s", "EXPLAIN SELECT a, b, v FROM v_keyed WHERE a = 7");
    assert!(!plan.weights.is_empty(), "EXPLAIN must describe a plan");
    assert_eq!(
        m.requests_sent(),
        before,
        "EXPLAIN of a mirrored SELECT must issue no request either",
    );

    // An unmirrored relation read through the same call: one RESOLVE plus the
    // read itself is two frames, so assert the delta rather than a bare zero —
    // what this pins is that the bracket kept the resolve to *one*.
    let before = m.requests_sent();
    let _ = query(m, "s", "SELECT a, b, v FROM t WHERE a = 7");
    let delegated = m.requests_sent() - before;
    assert_eq!(
        delegated, 2,
        "a delegated read costs one RESOLVE and one read; a missing statement bracket makes it three",
    );
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

/// A second store on a *held directory* is refused, in this process and across
/// processes alike — the data-directory `flock` is the only thing that refuses
/// one, and it does not care which process the second opener is in. The refusal
/// lands on the open, before any client attaches.
#[test]
fn a_second_handle_on_one_directory_is_refused() {
    let _g = serial();
    let mut fx = Fixture::start();

    // In-process. A second open takes a fresh open file description, which
    // `flock` treats as a conflict — unlike a forked worker's inherited one.
    assert!(
        Mirror::open(&fx.base_dir()).is_err(),
        "a second store on a directory this process holds must be refused",
    );

    // Cross-process, with the first client dropped so only the flock can refuse.
    let dir = fx.base_dir();
    fx.mirror = None;
    let held = Mirror::open(&dir).expect("the directory is free again");
    let out = run_test_in_child(
        module_path!(),
        "second_process_open_child",
        &[("GNITZ_MIRROR_DIR", &dir)],
    );
    assert_child_ok(&out, "the second-process child must run to the end");
    let printed = String::from_utf8_lossy(&out.stdout);
    assert!(
        printed.contains("is already held"),
        "a second process on a held directory must be refused: {printed}",
    );
    drop(held);
}

/// Two mirroring clients on two directories, in one process, each resuming its
/// own copy.
///
/// Nothing process-global decides what a store resumes from, so the two are
/// independent: each reopens at the generation its own checkpoint published,
/// advances rather than reseeds, and answers what the server answers.
#[test]
fn two_handles_on_two_directories_keep_their_own_state() {
    let _g = serial();
    let mut fx = Fixture::start();
    let second_dir = tempfile::tempdir().unwrap();
    let mut second = mirroring_client(fx.server.sock_path(), second_dir.path().to_str().unwrap());

    churn(&mut fx.direct, 1, 80);
    fx.mirror().mirror_view("s", "v_keyed").expect("mirror v_keyed");
    second.mirror_view("s", "v_repl").expect("mirror v_repl");
    fx.quiesce();
    second.poll_mirror().expect("poll the second handle");

    // Put the two directories at *different* generations, so a shared one would
    // leave at least one of them looking for a generation its manifests never
    // carried. The closes below checkpoint each again, first to 2, second to 1.
    fx.mirror().checkpoint_mirror().expect("checkpoint the first copy");
    churn(&mut fx.direct, 81, 100);
    fx.quiesce();

    // Both checkpoint on the way out, then both come back.
    second.close_mirror().expect("close the second handle");
    fx.reopen();
    let mut second = mirroring_client(fx.server.sock_path(), second_dir.path().to_str().unwrap());

    let keyed = fx.mirror().mirror_view("s", "v_keyed").expect("re-mirror v_keyed");
    let repl = second.mirror_view("s", "v_repl").expect("re-mirror v_repl");
    for (view, outcome) in [("v_keyed", keyed), ("v_repl", repl)] {
        assert!(
            !outcome.result.reseeded(),
            "{view} was checkpointed and must resume, not bootstrap",
        );
    }

    fx.differential("s", "SELECT * FROM v_keyed");
    let sql_text = "SELECT * FROM v_repl";
    let local = query(&mut second, "s", sql_text);
    let remote = query(&mut fx.direct, "s", sql_text);
    assert_same_zset(sql_text, (&local.0, &local.1), (&remote.0, &remote.1));
}

/// A cursor naming a copy that is not there must not be honoured.
///
/// `forget_view` retracts the registration into the system memtable and removes
/// the store directory at once, so an exit before the next checkpoint leaves the
/// view replayed as registered over a store that never came back — with its
/// cursor still in a file whose generation matches. Advancing on that cursor
/// applies `(c, now]` onto nothing: reads answer short, and `reseeded` says
/// `false`, so no subscriber is told.
#[test]
fn a_cursor_naming_an_absent_copy_bootstraps() {
    let _g = serial();
    let (server, dir) = child_fixture();

    run_child(
        "forgotten_view_leaves_its_cursor_child",
        &server,
        &dir,
        &[],
        "the child must checkpoint, forget the view, and exit without checkpointing again",
    );

    let mut mirror = mirroring_client(server.sock_path(), dir.path().to_str().unwrap());
    let outcome = mirror.mirror_view("s", "v_keyed").expect("re-mirror v_keyed");
    assert!(
        outcome.result.reseeded(),
        "a cursor over a copy that never came back must bootstrap, not advance",
    );
    let mut direct = GnitzClient::connect(server.sock_path()).unwrap();
    let sql_text = "SELECT * FROM v_keyed";
    let local = query(&mut mirror, "s", sql_text);
    let remote = query(&mut direct, "s", sql_text);
    assert_same_zset(sql_text, (&local.0, &local.1), (&remote.0, &remote.1));
}

/// Runs only in the child `a_cursor_naming_an_absent_copy_bootstraps` spawns.
///
/// What a crash or a `SIGKILL` leaves behind — a durable cursor file and a
/// durable registration, both from the checkpoint, and no record of the forget.
#[test]
fn forgotten_view_leaves_its_cursor_child() {
    let Some((sock, dir)) = child_target() else { return };
    let mut mirror = mirroring_client(&sock, &dir);
    let tid = mirror.mirror_view("s", "v_keyed").expect("mirror v_keyed").view_id;
    mirror.checkpoint_mirror().expect("checkpoint the copy and its cursor");
    mirror.forget_view(tid).expect("forget v_keyed");
    println!("{CHILD_OK}");
}

/// Forgetting a view and mirroring it again — same server id, same layout —
/// leaves a working copy.
///
/// The retraction removes the record and the directory; the re-registration
/// enters a fresh one under the same id. Nothing weaker catches a mistake
/// there: a view recreated upstream takes a *new* id.
#[test]
fn a_forgotten_view_can_be_mirrored_again() {
    let _g = serial();
    let mut fx = Fixture::start();

    churn(&mut fx.direct, 1, 60);
    let tid = fx.mirror().mirror_view("s", "v_keyed").expect("mirror v_keyed").view_id;
    fx.quiesce();
    let before = fx.differential("s", "SELECT * FROM v_keyed");

    fx.mirror().forget_view(tid).expect("forget");
    assert!(
        !fx.mirror().mirrors(tid),
        "a forgotten view is no longer answered locally"
    );

    assert_eq!(
        fx.mirror().mirror_view("s", "v_keyed").expect("re-mirror").view_id,
        tid,
        "the same relation comes back under the same server id",
    );
    fx.quiesce();
    assert_eq!(
        fx.differential("s", "SELECT * FROM v_keyed"),
        before,
        "the rebuilt copy must hold what it held before",
    );
}

/// A relation with no feed cannot be mirrored, and neither can a base table.
#[test]
fn only_a_fed_view_can_be_mirrored() {
    let _g = serial();
    let mut fx = Fixture::start();
    sql(&mut fx.direct, "s", "CREATE VIEW plain AS SELECT a, b, v FROM t");

    let m = fx.mirror.as_mut().unwrap();
    let e = m.mirror_view("s", "t").unwrap_err().to_string();
    assert!(e.contains("only a view can be mirrored"), "{e}");
    let e = m.mirror_view("s", "plain").unwrap_err().to_string();
    assert!(e.contains("keeps no delta feed"), "{e}");
}

/// A storage fault reaches the host as an error and poisons the handle, rather
/// than ending the process.
///
/// This is the acceptance test for making the engine safe to link, and it drives
/// the `Err` channel — the one that holds in both build profiles. The seam
/// substitutes an `Err` at exactly the ingest site the engine used to `_exit`
/// from, and it exists precisely so the fail-stop path can be driven without a
/// real disk fault: no tmpfs and no privileges.
///
/// It runs in a child process because a fault seam is read once per process into
/// a latch, so setting the variable in this one would race every sibling test.
#[test]
fn a_storage_fault_does_not_kill_the_host() {
    let _g = serial();
    if !cfg!(debug_assertions) {
        return; // the seam folds away in a release build
    }
    let (server, dir) = child_fixture();

    run_child(
        "poisons_on_a_storage_fault_child",
        &server,
        &dir,
        &[("GNITZ_INJECT_INGEST_APPLY_ERROR", "store")],
        "the host must survive a storage fault and see an error; exit 134 is the abort this replaced",
    );
}

/// Runs only in the child the test above spawns.
#[test]
fn poisons_on_a_storage_fault_child() {
    let Some((sock, dir)) = child_target() else { return };
    let mut mirror = mirroring_client(&sock, &dir);
    let err = mirror
        .mirror_view("s", "v_keyed")
        .expect_err("the armed seam must fail the bootstrap ingest");
    assert!(
        matches!(err, ClientError::Mirror(MirrorError::Poisoned(_))),
        "an ingest fault must poison the store: {err}",
    );
    assert!(mirror.mirror_poisoned().is_some());
    // Every subsequent call is refused rather than answering off a copy with a
    // hole in it.
    assert!(mirror.poll_mirror().is_err());
    assert!(mirror.checkpoint_mirror().is_err());
    // And the drop checkpoints nothing.
    drop(mirror);
    println!("{CHILD_OK}");
}

/// A registration whose bootstrap could not finish answers no read locally.
///
/// A read gated on the registration alone answers the erased copy with zero
/// rows, no request and no error — and keeps doing it, since the refusal behind
/// it is repeatable. Seam-driven: the server's only refusal here is an oversized
/// reply.
#[test]
fn a_failed_bootstrap_answers_no_read_locally() {
    let _g = serial();
    if !cfg!(debug_assertions) {
        return; // the seam folds away in a release build
    }
    let (server, dir) = child_fixture();

    run_child(
        "failed_bootstrap_child",
        &server,
        &dir,
        &[("GNITZ_INJECT_MIRROR_BOOTSTRAP_ERROR", "1")],
        "a read over a copy that never arrived must delegate rather than answer empty",
    );
}

/// Runs only in the child the test above spawns.
#[test]
fn failed_bootstrap_child() {
    let Some((sock, dir)) = child_target() else { return };
    let mut direct = GnitzClient::connect(&sock).unwrap();
    let mut mirror = mirroring_client(&sock, &dir);

    let err = mirror
        .mirror_view("s", "v_keyed")
        .expect_err("the armed seam must fail the bootstrap read");
    assert!(
        !matches!(err, ClientError::Mirror(MirrorError::Poisoned(_))),
        "a failed bootstrap is not a poisoning: {err}",
    );
    assert!(mirror.mirror_poisoned().is_none(), "the client stays usable");

    // The registration outlives the failed bootstrap, which is what makes the
    // window reachable at all; the copy behind it does not.
    let [tid] = mirror.mirrored_ids()[..] else {
        panic!("the failed bootstrap must leave exactly its own registration behind")
    };
    assert!(
        !mirror.mirrors(tid),
        "a registration with no valid copy is not answered locally"
    );

    let local = query(&mut mirror, "s", "SELECT * FROM v_keyed");
    let remote = query(&mut direct, "s", "SELECT * FROM v_keyed");
    assert_same_zset(
        "the read must be delegated, not answered off the erased copy",
        (&local.0, &local.1),
        (&remote.0, &remote.1),
    );

    // The seam is one-shot, so the next poll is a real bootstrap: the handle
    // recovers rather than delegating forever.
    mirror.poll_mirror().expect("the next poll bootstraps for real");
    assert!(
        mirror.mirrors(tid),
        "a completed bootstrap makes the copy answerable again"
    );
    let local = query(&mut mirror, "s", "SELECT * FROM v_keyed");
    assert_same_zset(
        "the recovered copy answers what the server does",
        (&local.0, &local.1),
        (&remote.0, &remote.1),
    );
    println!("{CHILD_OK}");
}

/// The whole durability path runs with `io_uring` unavailable: open, checkpoint,
/// and a reopen that resumes.
#[test]
fn the_blocking_fsync_fallback_carries_durability() {
    let _g = serial();
    let (server, dir) = child_fixture();

    run_child(
        "blocking_fsync_fallback_child",
        &server,
        &dir,
        &[("GNITZ_DISABLE_IO_URING", "1")],
        "the blocking fallback must carry the full path",
    );
}

/// Runs only in the child the test above spawns.
#[test]
fn blocking_fsync_fallback_child() {
    let Some((sock, dir)) = child_target() else { return };
    let open = |dir: &str| mirroring_client(&sock, dir);
    let mut mirror = open(&dir);
    let tid = mirror.mirror_view("s", "v_keyed").expect("mirror").view_id;
    mirror.poll_mirror().expect("poll");
    mirror
        .checkpoint_mirror()
        .expect("checkpoint under the blocking fallback");
    let before = query(&mut mirror, "s", "SELECT * FROM v_keyed");
    let cursor = mirror.cursor_of(tid).expect("a cursor");
    drop(mirror);

    let mut mirror = open(&dir);
    assert_eq!(
        mirror.mirror_view("s", "v_keyed").expect("re-mirror").view_id,
        tid,
        "the reopen must land on the same id",
    );
    assert_eq!(
        mirror.cursor_of(tid).map(|c| c.tick >= cursor.tick),
        Some(true),
        "the reopen must continue the persisted cursor rather than restart it",
    );
    let after = query(&mut mirror, "s", "SELECT * FROM v_keyed");
    assert_eq!(
        canonical(&before.0, &before.1),
        canonical(&after.0, &after.1),
        "the resumed copy must hold what the checkpoint made durable",
    );
    println!("{CHILD_OK}");
}

/// The round trip, measured as instructions retired: a narrowly-bounded read
/// against the mirror versus the same read against the server.
///
/// `#[ignore]`d and run with `--nocapture`, like the engine's other timing
/// tests: it prints a measurement rather than asserting a threshold, and needs a
/// `perf_event_open` the sandbox may refuse.
///
/// The counter's scope is **the calling thread**, kernel time included where the
/// kernel allows it. That is the right scope for what is being claimed: what a
/// mirror removes from a caller is the caller's own syscall, socket and wakeup
/// work. The W workers' work behind a served read is outside it by construction,
/// and the falsifiable form of "no round trip at all" is the request count that
/// `a_mirrored_select_issues_no_request` pins, not this.
///
/// **The read must be one the bound narrows.** A full scan measures the wrong
/// thing: the mirror does on one thread what the server splits W ways, so
/// instructions retired would come out level while wall-clock moved *against*
/// the mirror. The full-scan number is recorded too, as the statement of that
/// trade rather than as a target.
#[test]
#[ignore]
fn round_trip_cost_bench() {
    let _g = serial();
    let mut fx = Fixture::start();
    let Some(counter) = support::perf::Instructions::open() else {
        println!("perf_event_open refused; skipping the instruction count");
        return;
    };
    if !counter.counts_kernel {
        println!("note: perf_event_paranoid forbids kernel-mode counting, so a served read's syscall and");
        println!("      wakeup path are invisible here and its figure is an under-count.");
    }

    churn(&mut fx.direct, 1, 2_000);
    fx.mirror_both();
    fx.quiesce();

    for (label, q) in [
        ("point", "SELECT a, b, v FROM v_keyed WHERE a = 977"),
        ("narrow", "SELECT a, b, v FROM v_keyed WHERE a > 900 AND a < 940"),
        ("full", "SELECT a, b, v FROM v_keyed"),
    ] {
        // One warm pass each: the first read of a relation pays cache fills on
        // both sides, and neither is what this measures.
        let _ = fx.local(q);
        let _ = query(&mut fx.direct, "s", q);

        let (_, local) = counter.measure(|| fx.local(q));
        let (_, remote) = counter.measure(|| query(&mut fx.direct, "s", q));
        println!("{label:>7}: mirror {local:>12} instr, server {remote:>12} instr (client side)");
    }
}

// ---------------------------------------------------------------------------
// Persistence and recovery
// ---------------------------------------------------------------------------

/// A restart resumes rather than reseeds: the store comes back from its manifest
/// and the first poll continues from the persisted cursor.
///
/// Observed as the poll's returned rows, not by inspecting handle state — a
/// bootstrap would erase and re-read, so a copy that came back complete *and*
/// whose cursor moved forward rather than restarting is the observable
/// difference.
#[test]
fn a_restart_resumes_rather_than_reseeds() {
    let _g = serial();
    let mut fx = Fixture::start();

    churn(&mut fx.direct, 1, 120);
    let (tid, _) = fx.mirror_both();
    fx.quiesce();
    fx.mirror().checkpoint_mirror().expect("checkpoint");
    let before = fx.local("SELECT * FROM v_keyed");
    let cursor = fx.mirror().cursor_of(tid).expect("a cursor");

    fx.reopen();
    assert_eq!(
        fx.mirror().mirror_view("s", "v_keyed").expect("re-mirror").view_id,
        tid,
        "the reopen must land on the same id",
    );
    assert!(
        fx.mirror().cursor_of(tid).is_some_and(|c| c.tick >= cursor.tick),
        "the first poll must continue the persisted cursor, not restart from a bootstrap",
    );
    assert!(
        has_manifest(&fx.base_dir(), tid),
        "a resumed copy keeps the manifest its checkpoint published; a reseed would have unlinked it",
    );
    let after = fx.local("SELECT * FROM v_keyed");
    assert_eq!(
        canonical(&before.0, &before.1),
        canonical(&after.0, &after.1),
        "a resumed copy must hold exactly what it held before the restart",
    );
    fx.mirror().mirror_view("s", "v_repl").expect("re-mirror v_repl");
    fx.differential("s", "SELECT * FROM v_keyed");
}

/// Both torn-checkpoint crash points reseed rather than corrupt.
///
/// Driven directly, against a **live** server whose tag still continues — which
/// is the whole point: the continuing tag is what makes both failures silent
/// without the generation header, so a test that restarted the server first
/// would test nothing.
///
/// * A cursor file left at the previous checkpoint's generation beside copies
///   published at the new one must not double any weight.
/// * A durable generation advanced past every output manifest must not lose any
///   row.
#[test]
fn a_torn_checkpoint_reseeds_rather_than_corrupts() {
    let _g = serial();
    let mut fx = Fixture::start();

    churn(&mut fx.direct, 1, 90);
    fx.mirror_both();
    fx.quiesce();
    fx.mirror().checkpoint_mirror().expect("checkpoint");

    // Case 1 — the copies moved on and were published; the cursor file did not.
    // Snapshot the file, apply more rounds, checkpoint again, restore the old
    // file: that is exactly a crash between the ephemeral round and the write.
    let state_path = state_file(&fx.base_dir());
    let stale = std::fs::read(&state_path).expect("a checkpoint writes the record file");
    churn(&mut fx.direct, 91, 180);
    fx.quiesce();
    fx.mirror().checkpoint_mirror().expect("second checkpoint");
    fx.mirror = None;
    std::fs::write(&state_path, &stale).unwrap();

    fx.open();
    fx.mirror_both();
    fx.quiesce();
    fx.differential("s", "SELECT * FROM v_keyed");

    // Case 2 — the generation the record file names is ahead of every output
    // manifest, so each copy erases at open. That is the checkpoint's own
    // generation bump with the flush round left out, which is the crash.
    fx.mirror()
        .checkpoint_mirror()
        .expect("checkpoint before the torn bump");
    fx.mirror = None;
    patch_state_header(&fx.base_dir(), STATE_OFF_GENERATION, |g| g + 1);

    fx.open();
    fx.mirror_both();
    fx.quiesce();
    fx.differential("s", "SELECT * FROM v_keyed");
    fx.differential("s", "SELECT * FROM v_repl");
}

/// An apply past the byte threshold checkpoints on its own — the 64 MB default
/// puts it out of every other test's reach. The observable is the manifest, which
/// only a checkpoint publishes, and this test calls none.
#[test]
fn an_applied_delta_drives_its_own_checkpoint() {
    let _g = serial();
    // Declared before the fixture so it is removed after the handle it governs
    // is dropped.
    let _threshold = EnvVar::set("GNITZ_MIRROR_CHECKPOINT_BYTES", "1");
    let mut fx = Fixture::start();

    churn(&mut fx.direct, 1, 40);
    let (tid, _) = fx.mirror_both();
    fx.quiesce();

    assert!(
        has_manifest(&fx.base_dir(), tid),
        "an apply past the threshold must publish the copy's manifest with no explicit checkpoint",
    );
    assert!(
        std::path::Path::new(&state_file(&fx.base_dir())).exists(),
        "the same checkpoint must write the record file",
    );
    fx.differential("s", "SELECT * FROM v_keyed");
}

/// A checkpoint by a handle that re-registered only some of its views keeps every
/// cursor.
///
/// The flush round republishes every copy the store holds, so a cursor set
/// gathered from the registrations alone strands the unclaimed one — published
/// at the new generation with no cursor, and bootstrapped next session though it
/// was intact.
/// Checked by the manifest, before any further checkpoint republishes one.
#[test]
fn a_partially_registered_reopen_keeps_every_cursor() {
    let _g = serial();
    let mut fx = Fixture::start();

    churn(&mut fx.direct, 1, 60);
    fx.mirror().mirror_view("s", "v_keyed").expect("mirror v_keyed");
    let repl = fx.mirror().mirror_view("s", "v_repl").expect("mirror v_repl").view_id;
    fx.quiesce();
    fx.mirror().checkpoint_mirror().expect("checkpoint");

    // Reopen holding both copies, claim only one, and checkpoint again.
    fx.reopen();
    fx.mirror().mirror_view("s", "v_keyed").expect("re-mirror v_keyed");
    fx.mirror()
        .checkpoint_mirror()
        .expect("checkpoint with v_repl unclaimed");

    fx.reopen();
    fx.mirror().mirror_view("s", "v_repl").expect("re-mirror v_repl");
    assert!(
        has_manifest(&fx.base_dir(), repl),
        "the unclaimed view's cursor must survive the checkpoint that republished its copy",
    );
    fx.mirror().mirror_view("s", "v_keyed").expect("re-mirror v_keyed");
    fx.quiesce();
    fx.differential("s", "SELECT * FROM v_repl");
    fx.differential("s", "SELECT * FROM v_keyed");
}

/// The record file the crate writes beside the copies.
fn state_file(base_dir: &str) -> String {
    format!("{base_dir}/mirror_state")
}

/// Whether the mirrored copy of `view_id` currently has a published manifest —
/// the on-disk difference between a resumed store and an erased one.
///
/// Built through the engine's own path grammar rather than by searching for a
/// file of that name: it then names the copy's output store and nothing else
/// under the tree, and it follows a change to the layout instead of quietly
/// answering `false` forever. A copy is laid out for one worker, at the solo
/// slot every mirror opens under.
fn has_manifest(base_dir: &str, view_id: u64) -> bool {
    let rel_dir = relation_dir(base_dir, "_copies", RelationKind::View, view_id as i64);
    std::path::Path::new(&ChildAddr::worker(Slot::SOLO).manifest(&rel_dir)).exists()
}

/// A server restart erases the copy: the stored cursor tag no longer matches the
/// one the next reply carries, and the recovery reseeds under the same id.
#[test]
fn a_server_restart_reseeds_the_copy() {
    let _g = serial();
    let mut fx = Fixture::start();

    churn(&mut fx.direct, 1, 60);
    let (tid, _) = fx.mirror_both();
    fx.quiesce();

    fx.server.restart();
    fx.direct = GnitzClient::connect(fx.server.sock_path()).unwrap();
    fx.reconnect_mirror();
    churn(&mut fx.direct, 61, 120);
    let _ = query(&mut fx.direct, "s", "SELECT COUNT(*) AS n FROM v_keyed");
    fx.mirror()
        .poll_mirror()
        .expect("the poll must recover from a foreign tag by itself");
    assert!(fx.mirror().mirrors(tid), "the id is unchanged across a server restart");
    fx.quiesce();
    fx.differential("s", "SELECT * FROM v_keyed");
    fx.differential("s", "SELECT * FROM v_repl");
}

/// Byte offset of the `mirror_state` generation word the test above fabricates.
/// The crate owns the layout; this is what a torn checkpoint is *expressible
/// as*, so a layout change that moved it would fail that test rather than
/// silently stop testing anything.
const STATE_OFF_GENERATION: usize = 0;

/// Rewrite one `u64` of the mirror's record-file header, to fabricate a durable
/// state the handle would never write.
fn patch_state_header(base_dir: &str, offset: usize, f: impl FnOnce(u64) -> u64) {
    let path = state_file(base_dir);
    let mut bytes = std::fs::read(&path).expect("a checkpoint wrote the record file");
    let old = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
    bytes[offset..offset + 8].copy_from_slice(&f(old).to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();
}

// ---------------------------------------------------------------------------
// Store shapes
// ---------------------------------------------------------------------------

/// The schema the shape cases seed for themselves: two base tables where
/// `u.tid` covers only the lower part of `t.id`, so an outer join has unmatched
/// rows and an `EXCEPT` is non-empty.
///
/// Beside the shared `seed` rather than folded into it: every other test would
/// otherwise carry two more relations through every churn.
const SH: &str = "sh";

fn seed_shapes(client: &mut GnitzClient) {
    client.create_schema(SH).unwrap();
    sql(
        client,
        SH,
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, body TEXT NOT NULL)",
    );
    sql(
        client,
        SH,
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)",
    );
}

/// Fresh keys in both tables, `u` reaching only halfway up `t`'s range.
fn churn_shapes(client: &mut GnitzClient, lo: i64, hi: i64) {
    let t: Vec<String> = (lo..=hi)
        .map(|i| format!("({i}, {}, 'body-{i:0>20}')", i * 3))
        .collect();
    sql(client, SH, &format!("INSERT INTO t VALUES {}", t.join(",")));
    let mid = lo + (hi - lo) / 2;
    let u: Vec<String> = (lo..=mid).map(|i| format!("({i}, {i}, {})", i * 7)).collect();
    sql(client, SH, &format!("INSERT INTO u VALUES {}", u.join(",")));
}

/// A round that touches only keys the copies already hold — the retract/insert
/// pair a PK-conflicting update produces, plus a pure retraction. A poll that
/// covered only fresh keys would never fold a weight onto an existing element.
fn rechurn_shapes(client: &mut GnitzClient, lo: i64, hi: i64) {
    sql(
        client,
        SH,
        &format!("UPDATE t SET v = v + 7 WHERE id >= {lo} AND id <= {hi}"),
    );
    sql(
        client,
        SH,
        &format!("UPDATE u SET w = w + 11 WHERE id >= {lo} AND id <= {hi}"),
    );
    sql(client, SH, &format!("DELETE FROM t WHERE id = {lo}"));
    sql(client, SH, &format!("DELETE FROM u WHERE id = {}", lo + 1));
}

/// The six view bodies of S1, each with the read that reaches past its `SELECT *`.
const SHAPE_VIEWS: &[(&str, &str, &str)] = &[
    (
        "v_join",
        "SELECT t.id, t.body, u.w FROM t JOIN u ON t.id = u.tid",
        "SELECT id, w FROM v_join WHERE w > 200",
    ),
    (
        "v_left",
        "SELECT t.id, t.v, u.w FROM t LEFT JOIN u ON t.id = u.tid",
        "SELECT id, v, w FROM v_left WHERE w IS NULL",
    ),
    (
        "v_group",
        "SELECT tid, COUNT(*) AS n, SUM(w) AS total FROM u GROUP BY tid",
        "SELECT tid, total FROM v_group WHERE total > 200",
    ),
    (
        "v_except",
        "SELECT id FROM t EXCEPT SELECT tid FROM u",
        "SELECT id FROM v_except WHERE id > 30",
    ),
    (
        "v_distinct",
        "SELECT DISTINCT v FROM t",
        "SELECT v FROM v_distinct WHERE v > 100",
    ),
    (
        // Over `v_group`, so the copy is fed by a view whose own source is a
        // maintained view rather than a base table.
        "v_chain",
        "SELECT tid, total FROM v_group WHERE total > 100",
        "SELECT tid FROM v_chain WHERE tid > 20",
    ),
];

/// Every store shape a view body produces, mirrored in one handle.
///
/// Each puts a hidden synthetic key (`_join_pk`, `_group_pk`, `_set_pk`) or a
/// nullable payload through local registration, `pk_stride`, the round-stamp
/// strip and the null bitmap — the four places a wrong shape is silently wrong
/// rather than an error.
#[test]
fn every_view_body_shape_mirrors() {
    let _g = serial();
    let mut fx = Fixture::start();
    seed_shapes(&mut fx.direct);
    churn_shapes(&mut fx.direct, 1, 80);
    for (name, body, _) in SHAPE_VIEWS {
        sql(
            &mut fx.direct,
            SH,
            &format!("CREATE VIEW {name} WITH (delta = '{FEED}') AS {body}"),
        );
    }

    let names: Vec<&str> = SHAPE_VIEWS.iter().map(|(n, _, _)| *n).collect();
    for name in &names {
        let out = fx.mirror().mirror_view(SH, name).expect(name);
        assert!(out.result.reseeded(), "{name}: a first registration bootstraps");
        assert!(fx.mirror().mirrors(out.view_id), "{name}: the copy must answer locally");
    }

    let mut compared = 0;
    for round in 0..3 {
        if round > 0 {
            rechurn_shapes(&mut fx.direct, 2 + round as i64 * 10, 40 + round as i64 * 10);
        }
        fx.drain(SH, &names);
        for (name, _, extra) in SHAPE_VIEWS {
            compared += fx.differential(SH, &format!("SELECT * FROM {name}"));
            compared += fx.differential(SH, extra);
        }
    }
    assert!(compared > 500, "the shape differential compared only {compared} rows");
}

/// Nullable payloads of every width, and the reads that single them out.
///
/// A `BLOB` payload joins them only through the binary `create_table` API:
/// `sql_type_to_typecode` produces `TypeCode::Blob` for no SQL type, so
/// `b BLOB` in a `CREATE TABLE` is rejected outright.
#[test]
fn nullable_payloads_of_every_width_survive_the_copy() {
    let _g = serial();
    let mut fx = Fixture::start();
    fx.direct.create_schema("nl").unwrap();
    sql(
        &mut fx.direct,
        "nl",
        "CREATE TABLE n (id BIGINT NOT NULL PRIMARY KEY, i BIGINT, s TEXT, d DOUBLE PRECISION, r REAL)",
    );
    // Every column null on one row, none on the next.
    let rows: Vec<String> = (1..=120)
        .map(|i| {
            if i % 3 == 0 {
                format!("({i}, NULL, NULL, NULL, NULL)")
            } else {
                format!("({i}, {}, 'text-{i:0>12}', {i}.25, {i}.5)", i * 3)
            }
        })
        .collect();
    sql(
        &mut fx.direct,
        "nl",
        &format!("INSERT INTO n VALUES {}", rows.join(",")),
    );

    // The BLOB table: created and seeded through the binary API, then read and
    // retracted through SQL like any other.
    let cols = vec![
        gnitz_core::ColumnDef::new("id", gnitz_core::TypeCode::I64, false),
        gnitz_core::ColumnDef::new("b", gnitz_core::TypeCode::Blob, true),
    ];
    fx.direct
        .create_table("nl", "blb", &cols, &[0], gnitz_core::TableProps::default(), &[])
        .expect("a BLOB column is admissible through the binary API");
    let (blb_tid, blb_schema) = fx.direct.resolve_table_id("nl", "blb").unwrap();
    let mut batch = gnitz_core::ZSetBatch::new(&blb_schema);
    {
        let mut app = gnitz_core::BatchAppender::new(&mut batch, &blb_schema);
        for i in 1i64..=120 {
            app.add_row(i as u128, 1);
            if i % 4 == 0 {
                app.null();
            } else {
                app.bytes_val(&vec![(i % 251) as u8; 1 + (i as usize % 17)]);
            }
        }
    }
    fx.direct.push(blb_tid, &blb_schema, &batch).expect("push blobs");

    sql(
        &mut fx.direct,
        "nl",
        &format!("CREATE VIEW v_null WITH (delta = '{FEED}') AS SELECT id, i, s, d, r FROM n"),
    );
    sql(
        &mut fx.direct,
        "nl",
        &format!("CREATE VIEW v_blob WITH (delta = '{FEED}') AS SELECT id, b FROM blb"),
    );
    fx.mirror().mirror_view("nl", "v_null").expect("mirror v_null");
    fx.mirror().mirror_view("nl", "v_blob").expect("mirror v_blob");

    let views = ["v_null", "v_blob"];
    let reads = [
        "SELECT * FROM v_null",
        "SELECT id, i FROM v_null WHERE i IS NULL",
        "SELECT id, s FROM v_null WHERE s IS NULL",
        "SELECT id, d, r FROM v_null WHERE d IS NOT NULL",
        "SELECT * FROM v_blob",
        "SELECT id, b FROM v_blob WHERE b IS NULL",
    ];
    fx.drain("nl", &views);
    let mut compared = 0;
    for q in reads {
        compared += fx.differential("nl", q);
    }

    // A round that turns present values into nulls, and one that retracts rows.
    sql(&mut fx.direct, "nl", "UPDATE n SET i = NULL, s = NULL WHERE id < 40");
    sql(
        &mut fx.direct,
        "nl",
        "UPDATE n SET d = NULL, r = NULL WHERE id >= 40 AND id < 70",
    );
    sql(&mut fx.direct, "nl", "DELETE FROM n WHERE id > 100");
    sql(&mut fx.direct, "nl", "DELETE FROM blb WHERE id > 100");
    fx.drain("nl", &views);
    for q in reads {
        compared += fx.differential("nl", q);
    }
    assert!(
        compared > 200,
        "the nullable differential compared only {compared} rows"
    );
}

/// A four-column PK carrying negative and mixed-sign values — the OPK sign flip
/// — read as a full scan, a point lookup and a range.
#[test]
fn a_compound_signed_pk_reads_the_same_locally() {
    let _g = serial();
    let mut fx = Fixture::start();
    fx.direct.create_schema("pk").unwrap();
    sql(
        &mut fx.direct,
        "pk",
        "CREATE TABLE k (a BIGINT NOT NULL, b INTEGER NOT NULL, c SMALLINT NOT NULL, \
                         d BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (a, b, c, d))",
    );
    let rows: Vec<String> = (-40i64..=40)
        .map(|i| format!("({i}, {}, {}, {}, {})", -i, i % 7, i * 2, i * 11))
        .collect();
    sql(
        &mut fx.direct,
        "pk",
        &format!("INSERT INTO k VALUES {}", rows.join(",")),
    );
    sql(
        &mut fx.direct,
        "pk",
        &format!("CREATE VIEW v_pk WITH (delta = '{FEED}') AS SELECT a, b, c, d, v FROM k"),
    );
    fx.mirror().mirror_view("pk", "v_pk").expect("mirror v_pk");
    fx.drain("pk", &["v_pk"]);

    let mut compared = 0;
    for q in [
        "SELECT * FROM v_pk",
        "SELECT a, b, c, d, v FROM v_pk WHERE a = -17 AND b = 17 AND c = -3 AND d = -34",
        "SELECT a, v FROM v_pk WHERE a > -12 AND a < 12",
    ] {
        compared += fx.differential("pk", q);
    }

    sql(&mut fx.direct, "pk", "UPDATE k SET v = v - 1 WHERE a < 0");
    sql(&mut fx.direct, "pk", "DELETE FROM k WHERE a > 30");
    fx.drain("pk", &["v_pk"]);
    compared += fx.differential("pk", "SELECT * FROM v_pk");
    assert!(
        compared > 100,
        "the compound-PK differential compared only {compared} rows"
    );
}

/// Ordered reads come back in order, including a `LIMIT` and a `LIMIT … OFFSET`
/// over a tie-heavy sort key where a different tie-break selects different rows.
///
/// Unordered queries stay multiset comparisons throughout this suite, and that
/// is deliberate: their row order is unspecified, and the mirror is one
/// partition where the server is W, so the sequences do differ.
#[test]
fn ordered_reads_are_ordered() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 300);
    fx.mirror_both();
    fx.quiesce();

    let mut compared = 0;
    for q in [
        "SELECT a, b, v FROM v_keyed ORDER BY a DESC, b ASC",
        "SELECT a, b, body FROM v_keyed WHERE v > 100 ORDER BY a ASC, b DESC LIMIT 23",
        // `b` is `a % 7`, so the sort key ties in blocks of ~40 rows: which rows
        // the window selects depends on the whole tie-break, not just on `b`.
        "SELECT a, b FROM v_keyed ORDER BY b ASC, a ASC LIMIT 19 OFFSET 31",
        "SELECT a, b FROM v_keyed ORDER BY b DESC, a DESC LIMIT 19 OFFSET 31",
        "SELECT id, v FROM v_repl ORDER BY v DESC, id ASC LIMIT 15 OFFSET 7",
    ] {
        compared += fx.ordered_differential("s", q);
    }
    assert!(compared > 100, "the ordered differential compared only {compared} rows");
}

/// The join body against a server at **two** workers.
///
/// The other split where a mishandled concatenation of per-worker bootstrap
/// trains is real. Not one worker: at one slice the server's partitioning
/// matches the mirror's, so a copy that assumed the two agree passes.
#[test]
fn a_second_worker_count_mirrors_the_same() {
    let _g = serial();
    let mut fx = Fixture::start_with(2, &[]);
    seed_shapes(&mut fx.direct);
    churn_shapes(&mut fx.direct, 1, 120);
    sql(
        &mut fx.direct,
        SH,
        &format!(
            "CREATE VIEW v_join WITH (delta = '{FEED}') AS \
             SELECT t.id, t.body, u.w FROM t JOIN u ON t.id = u.tid"
        ),
    );
    fx.mirror().mirror_view(SH, "v_join").expect("mirror v_join");
    fx.drain(SH, &["v_join"]);
    let mut compared = fx.differential(SH, "SELECT * FROM v_join");

    rechurn_shapes(&mut fx.direct, 2, 60);
    fx.drain(SH, &["v_join"]);
    compared += fx.differential(SH, "SELECT * FROM v_join");
    compared += fx.differential(SH, "SELECT COUNT(*) AS n, SUM(w) AS total FROM v_join");
    assert!(
        compared > 100,
        "the two-worker differential compared only {compared} rows"
    );
}

/// A bootstrap and a poll that each span many reply frames, over an
/// all-fixed-width view: every frame's data block is a pure region copy, with no
/// heap to relocate. A loop that stopped at the first block loses rows silently.
///
/// The view is created over an already-populated table, so this also covers the
/// first bootstrap of a view whose rows all arrived through a `CREATE VIEW`
/// backfill — rows the delta feed never carries.
#[test]
fn a_bootstrap_and_a_poll_span_many_frames() {
    let _g = serial();
    let mut fx = Fixture::start_with(WORKERS, &[("GNITZ_REPLY_FRAME_BUDGET", "16384")]);
    fx.direct.create_schema("fr").unwrap();
    sql(
        &mut fx.direct,
        "fr",
        "CREATE TABLE w (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL)",
    );
    let fill = |client: &mut GnitzClient, lo: i64, hi: i64| {
        for chunk in (lo..=hi).step_by(1000) {
            let end = (chunk + 999).min(hi);
            let rows: Vec<String> = (chunk..=end)
                .map(|i| format!("({i}, {}, {}, {})", i * 2, i * 3, i * 5))
                .collect();
            sql(client, "fr", &format!("INSERT INTO w VALUES {}", rows.join(",")));
        }
    };
    fill(&mut fx.direct, 1, 20_000);
    sql(
        &mut fx.direct,
        "fr",
        &format!("CREATE VIEW v_wide WITH (delta = '{FEED}') AS SELECT id, a, b, c FROM w WHERE a >= 0"),
    );

    fx.mirror().mirror_view("fr", "v_wide").expect("mirror v_wide");
    fx.drain("fr", &["v_wide"]);
    let bootstrapped = fx.differential("fr", "SELECT * FROM v_wide");
    assert!(bootstrapped >= 20_000, "the bootstrap carried only {bootstrapped} rows");

    fill(&mut fx.direct, 20_001, 40_000);
    fx.drain("fr", &["v_wide"]);
    let polled = fx.differential("fr", "SELECT * FROM v_wide");
    assert!(polled >= 40_000, "the poll carried only {polled} rows");
}

/// The same, over long TEXT: every row points into the batch's string heap, so
/// each frame ships a heap compacted to its own rows. A bootstrap reads the view
/// whole and cannot narrow, so this is the shape that used to be unmirrorable.
#[test]
fn a_long_text_view_mirrors_across_many_frames() {
    let _g = serial();
    let mut fx = Fixture::start_with(WORKERS, &[("GNITZ_REPLY_FRAME_BUDGET", "16384")]);
    fx.direct.create_schema("tx").unwrap();
    sql(
        &mut fx.direct,
        "tx",
        "CREATE TABLE d (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, body TEXT NOT NULL)",
    );
    // 200 bytes per row of heap, so 5 000 rows is ~1 MB across four workers —
    // ~16× the frame budget on each of them.
    let fill = |client: &mut GnitzClient, lo: i64, hi: i64| {
        for chunk in (lo..=hi).step_by(500) {
            let end = (chunk + 499).min(hi);
            let rows: Vec<String> = (chunk..=end)
                .map(|i| format!("({i}, {}, '{}')", i % 7, format_args!("row-{i}-{}", "z".repeat(200))))
                .collect();
            sql(client, "tx", &format!("INSERT INTO d VALUES {}", rows.join(",")));
        }
    };
    fill(&mut fx.direct, 1, 5_000);
    sql(
        &mut fx.direct,
        "tx",
        &format!("CREATE VIEW v_text WITH (delta = '{FEED}') AS SELECT id, k, body FROM d WHERE k >= 0"),
    );

    fx.mirror().mirror_view("tx", "v_text").expect("mirror v_text");
    fx.drain("tx", &["v_text"]);
    let bootstrapped = fx.differential("tx", "SELECT * FROM v_text");
    assert!(bootstrapped >= 5_000, "the bootstrap carried only {bootstrapped} rows");

    fill(&mut fx.direct, 5_001, 10_000);
    fx.drain("tx", &["v_text"]);
    let polled = fx.differential("tx", "SELECT * FROM v_text");
    assert!(polled >= 10_000, "the poll carried only {polled} rows");
    // The strings themselves, not just the row count.
    fx.ordered_differential("tx", "SELECT id, body FROM v_text WHERE k = 3 ORDER BY id");
}

/// Two schemas in one handle, and forgetting a view out of one.
///
/// A record carries its own schema name and the store enters no schema of its
/// own, so a retraction takes down exactly one view and leaves every other view
/// of that schema readable.
#[test]
fn two_schemas_share_one_handle() {
    let _g = serial();
    let mut fx = Fixture::start();
    seed_shapes(&mut fx.direct);
    churn_shapes(&mut fx.direct, 1, 60);
    churn(&mut fx.direct, 1, 60);
    sql(
        &mut fx.direct,
        SH,
        &format!("CREATE VIEW v_lin WITH (delta = '{FEED}') AS SELECT id, v, body FROM t WHERE v > 10"),
    );
    sql(
        &mut fx.direct,
        SH,
        &format!("CREATE VIEW v_grp WITH (delta = '{FEED}') AS SELECT tid, SUM(w) AS total FROM u GROUP BY tid"),
    );

    let (keyed, _) = fx.mirror_both();
    let lin = fx.mirror().mirror_view(SH, "v_lin").expect("mirror v_lin").view_id;
    fx.mirror().mirror_view(SH, "v_grp").expect("mirror v_grp");
    fx.quiesce();
    fx.drain(SH, &["v_lin", "v_grp"]);
    fx.differential("s", "SELECT * FROM v_keyed");
    fx.differential(SH, "SELECT * FROM v_lin");
    fx.differential(SH, "SELECT * FROM v_grp");

    fx.mirror().forget_view(lin).expect("forget v_lin");
    assert!(!fx.mirror().mirrors(lin));
    assert!(
        fx.mirror().mirrors(keyed),
        "forgetting one view must not disturb another schema's copy"
    );
    fx.drain(SH, &["v_grp"]);
    fx.differential(SH, "SELECT * FROM v_grp");
    fx.differential("s", "SELECT * FROM v_keyed");
    // Delegated now, and still correct.
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
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let (keyed, repl_id) = fx.mirror_both();
    fx.quiesce();
    let before_repl = fx.local("SELECT * FROM v_repl");
    let before_keyed = fx.mirror().cursor_of(keyed).expect("a cursor");

    sql(&mut fx.direct, "s", "DROP VIEW v_repl");
    churn(&mut fx.direct, 61, 120);
    let _ = query(&mut fx.direct, "s", "SELECT COUNT(*) AS n FROM v_keyed");

    let report = fx
        .mirror()
        .poll_mirror()
        .expect("a per-view failure is not a failure of the call");
    let dead = report
        .iter()
        .find(|o| o.view_id == repl_id)
        .expect("the dropped view is reported under its own id");
    let PollResult::Failed(e) = &dead.result else {
        panic!("the dropped view must fail: {:?}", dead.result)
    };
    // The id carries the identification now; the message need not.
    assert!(!e.to_string().is_empty(), "a failed view reports why");

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
        canonical(&before_repl.0, &before_repl.1),
        canonical(&still.0, &still.1),
        "the dropped view's copy answers its last round until it is forgotten",
    );

    fx.mirror().forget_view(repl_id).expect("forget the dead registration");
    let report = fx
        .mirror()
        .poll_mirror()
        .expect("the poll recovers once the dead view is gone");
    assert!(
        report.iter().all(|o| !matches!(o.result, PollResult::Failed(_))),
        "nothing fails once the dead registration is gone",
    );
    assert!(
        report.iter().any(|o| o.view_id == keyed),
        "the survivor is reported again"
    );
}

/// A view recreated under the same name with a **different column set**.
///
/// The poll re-resolves, finds the descriptor no longer matches, retracts the
/// old registration, registers the new one, reseeds, and reads correctly under
/// the new shape — all inside the poll, without the host re-registering. This is
/// the reachable form of a schema change under a live mirror: `ALTER TABLE` is
/// refused while a dependent view exists, so a mirrored view's base can never
/// change shape beneath it.
///
/// It also covers the recreated-view *id* move on its own: the tag stops
/// continuing for the reason a server restart does not, so a recovery that
/// bootstrapped in place would read a relation that no longer exists.
#[test]
fn a_view_recreated_with_a_different_column_set_reseeds_under_the_new_shape() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let (old, _) = fx.mirror_both();
    fx.quiesce();
    assert_eq!(
        fx.local("SELECT * FROM v_keyed").0.columns.len(),
        5,
        "the original view projects five columns",
    );

    sql(&mut fx.direct, "s", "DROP VIEW v_keyed");
    sql(
        &mut fx.direct,
        "s",
        &format!("CREATE VIEW v_keyed WITH (delta = '{FEED}') AS SELECT a, b, v FROM t WHERE v >= 0"),
    );
    churn(&mut fx.direct, 61, 120);
    let _ = query(&mut fx.direct, "s", "SELECT COUNT(*) AS n FROM v_keyed");

    fx.mirror()
        .poll_mirror()
        .expect("the poll re-registers under the new shape");
    let new = fx
        .mirror()
        .mirrored_ids()
        .into_iter()
        .find(|&t| t != old)
        .expect("the recreated view must be mirrored under a fresh id");
    assert!(!fx.mirror().mirrors(old), "the old id must be gone");
    assert!(fx.mirror().mirrors(new));
    assert_eq!(
        fx.local("SELECT * FROM v_keyed").0.columns.len(),
        3,
        "the copy must read under the new column set",
    );
    fx.quiesce();
    fx.differential("s", "SELECT * FROM v_keyed");
    fx.differential("s", "SELECT a, v FROM v_keyed WHERE a > 20 AND a < 60");
}

/// Registration is idempotent, and a poll with nothing pushed is a no-op.
#[test]
fn registration_is_idempotent_and_an_empty_poll_moves_nothing() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    for i in 0..10 {
        sql(
            &mut fx.direct,
            "s",
            &format!("CREATE VIEW v{i} WITH (delta = '{FEED}') AS SELECT a, b, v FROM t WHERE v > {i}"),
        );
    }
    let names: Vec<String> = (0..10).map(|i| format!("v{i}")).collect();
    let mut ids = Vec::new();
    for name in &names {
        let out = fx.mirror().mirror_view("s", name).expect(name);
        assert!(out.result.reseeded(), "{name}: a first registration bootstraps");
        ids.push(out.view_id);
    }

    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    fx.drain("s", &refs);
    let mut compared = 0;
    for name in &names {
        compared += fx.differential("s", &format!("SELECT * FROM {name}"));
    }
    assert!(compared > 100, "ten views compared only {compared} rows");

    // A second registration: same id, no reseed, cursor unmoved.
    let before: Vec<_> = ids.iter().map(|&t| fx.mirror().cursor_of(t).unwrap()).collect();
    for (i, name) in names.iter().enumerate() {
        let out = fx.mirror().mirror_view("s", name).expect("re-register");
        assert_eq!(out.view_id, ids[i], "{name}: the same relation keeps its id");
        assert!(
            !out.result.reseeded(),
            "{name}: a re-registration resumes rather than reseeds"
        );
    }
    let after: Vec<_> = ids.iter().map(|&t| fx.mirror().cursor_of(t).unwrap()).collect();
    assert_eq!(before, after, "a re-registration with nothing pushed moves no cursor");

    let report = fx.mirror().poll_mirror().expect("an empty poll");
    assert_eq!(report.len(), ids.len(), "one poll reports every registration");
    assert!(
        !report.iter().any(|o| o.result.reseeded()),
        "an empty poll reseeds nothing"
    );
    let again: Vec<_> = ids.iter().map(|&t| fx.mirror().cursor_of(t).unwrap()).collect();
    assert_eq!(after, again, "an empty poll moves no cursor");
}

/// A view over a stream, across a restart.
///
/// The view returns to the value it would have if the stream had never received
/// a row, and the copy must follow it *there*: the tag stops continuing and the
/// reseed reads the reset view. A mirror that resumed its copy across that
/// restart holds rows the server no longer has.
///
/// The restart kills the socket, so the handle is given a fresh connection
/// first — a poll on the dead one reports a transport failure rather than the
/// foreign tag this is about.
#[test]
fn a_view_over_a_stream_follows_its_reset_across_a_restart() {
    let _g = serial();
    let mut fx = Fixture::start();
    sql(
        &mut fx.direct,
        "s",
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, amount BIGINT NOT NULL) \
         WITH (stream = true)",
    );
    sql(
        &mut fx.direct,
        "s",
        &format!(
            "CREATE VIEW v_stream WITH (delta = '{FEED}') AS SELECT kind, SUM(amount) AS total FROM ev GROUP BY kind"
        ),
    );
    for round in 0..4 {
        let rows: Vec<String> = (0..40)
            .map(|i| format!("({}, {}, {})", round * 40 + i, i % 5, i + 1))
            .collect();
        sql(
            &mut fx.direct,
            "s",
            &format!("INSERT INTO ev VALUES {}", rows.join(",")),
        );
    }
    let tid = fx
        .mirror()
        .mirror_view("s", "v_stream")
        .expect("mirror v_stream")
        .view_id;
    fx.drain("s", &["v_stream"]);
    let before = fx.local("SELECT * FROM v_stream");
    assert!(
        !canonical(&before.0, &before.1).is_empty(),
        "the copy must hold the stream's aggregate before the restart",
    );

    fx.server.restart();
    fx.direct = GnitzClient::connect(fx.server.sock_path()).unwrap();
    fx.reconnect_mirror();
    let _ = query(&mut fx.direct, "s", "SELECT COUNT(*) AS n FROM v_stream");
    let report = fx
        .mirror()
        .poll_mirror()
        .expect("the poll recovers from the foreign tag");
    assert!(
        report.iter().any(|o| o.view_id == tid && o.result.reseeded()),
        "a tag that stopped continuing must be reported as a reseed",
    );

    // Spelled out rather than run through the differential: both sides are empty
    // afterwards, and `assert_same_zset` refuses that as a pass that agrees
    // about nothing.
    let after = fx.local("SELECT * FROM v_stream");
    assert!(
        canonical(&after.0, &after.1).is_empty(),
        "a view over a stream comes back with zero rows, and the copy must follow it there",
    );
}

/// A relation the handle does **not** hold still reads correctly through it.
///
/// The mirror is a client too, and a read it delegates must lose nothing. The
/// gate is one boolean per relation, so a delegated read reaches the connection
/// by the same path whatever its shape — a few shapes stand for all of them,
/// and the mirrored side of the same menu is covered by
/// `a_mirrored_read_equals_the_server_read`.
#[test]
fn delegation_is_correct_for_every_read_shape() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 200);
    fx.mirror_both();
    fx.quiesce();

    // `t` and `r` are base tables: mirrored by nothing, so every read is delegated.
    let mut compared = 0;
    for q in [
        "SELECT * FROM t",
        "SELECT b, COUNT(*) AS n, SUM(v) AS total, MIN(a) AS lo, MAX(a) AS hi FROM t GROUP BY b",
        "SELECT * FROM r",
    ] {
        compared += fx.differential("s", q);
    }
    compared += fx.ordered_differential("s", "SELECT a, v FROM t ORDER BY v DESC, a ASC LIMIT 9 OFFSET 5");
    assert!(
        compared > 300,
        "the delegated differential compared only {compared} rows"
    );
}

/// A failing checkpoint is reported, and the handle survives it.
#[test]
fn a_failing_checkpoint_leaves_the_handle_usable() {
    let _g = serial();
    if !cfg!(debug_assertions) {
        return; // the seam folds away in a release build
    }
    let (server, dir) = child_fixture();
    run_child(
        "failing_checkpoint_child",
        &server,
        &dir,
        &[("GNITZ_INJECT_MIRROR_CHECKPOINT_ERROR", "1")],
        "a failed checkpoint must be reported rather than poison the handle",
    );
}

/// Runs only in the child the test above spawns.
#[test]
fn failing_checkpoint_child() {
    let Some((sock, dir)) = child_target() else { return };
    let mut direct = GnitzClient::connect(&sock).unwrap();
    let mut mirror = mirroring_client(&sock, &dir);
    let tid = mirror.mirror_view("s", "v_keyed").expect("mirror").view_id;

    let err = mirror
        .checkpoint_mirror()
        .expect_err("the armed seam must fail the checkpoint");
    assert!(
        !matches!(err, ClientError::Mirror(MirrorError::Poisoned(_))),
        "a failed checkpoint is not a poisoning: {err}",
    );
    assert!(mirror.mirror_poisoned().is_none(), "the client stays usable");
    assert!(
        !has_manifest(&dir, tid),
        "the failed checkpoint must have published nothing",
    );

    // The copy is intact in the RAM tier, so reads are unaffected.
    let local = query(&mut mirror, "s", "SELECT * FROM v_keyed");
    let remote = query(&mut direct, "s", "SELECT * FROM v_keyed");
    assert_same_zset(
        "a failed checkpoint must not disturb the copy",
        (&local.0, &local.1),
        (&remote.0, &remote.1),
    );

    // The seam is one-shot, so the retry is the real thing.
    mirror
        .checkpoint_mirror()
        .expect("the retry after a one-shot fault succeeds");
    assert!(has_manifest(&dir, tid), "the successful retry must publish a manifest",);
    println!("{CHILD_OK}");
}

/// The copy is a store, not a resident Z-set.
///
/// `#[ignore]`d, run with `--nocapture`, and in a child process, because the RAM
/// ceiling is a read-once-per-process latch. **It is not a gate**: a checkpoint
/// publishes shard files whatever the RAM ceiling is, so their presence proves
/// nothing, and an RSS threshold is not sound enough to fail a build on. The
/// number is the statement of the claim.
#[test]
#[ignore]
fn resident_footprint_bench() {
    let _g = serial();
    let (server, dir) = child_fixture();
    print!(
        "{}",
        run_child(
            "resident_footprint_child",
            &server,
            &dir,
            &[("GNITZ_MIRROR_RAM_TIER_BYTES", "262144")],
            "the footprint child must run to the end",
        )
    );
}

/// Runs only in the child the test above spawns.
#[test]
fn resident_footprint_child() {
    let Some((sock, dir)) = child_target() else { return };
    let mut direct = GnitzClient::connect(&sock).unwrap();
    // In slices, so no one statement carries tens of thousands of value tuples.
    for lo in (41..40_000).step_by(2_000) {
        churn(&mut direct, lo, lo + 1_999);
    }
    let _ = query(&mut direct, "s", "SELECT * FROM v_keyed");

    let base = rss_bytes();
    let mut mirror = mirroring_client(&sock, &dir);
    let tid = mirror.mirror_view("s", "v_keyed").expect("mirror").view_id;
    mirror.checkpoint_mirror().expect("checkpoint");
    let rows = query(&mut mirror, "s", "SELECT a, b, v FROM v_keyed WHERE a = 2100")
        .1
        .weights
        .len();
    let held = rss_bytes();
    let on_disk = dir_bytes(std::path::Path::new(&dir));
    println!(
        "copy of view {tid}: {on_disk} bytes on disk, host RSS {base} -> {held} \
         (+{}), point read returned {rows} row(s)",
        held.saturating_sub(base),
    );
    println!("{CHILD_OK}");
}

/// This process's resident set, in bytes. `0` where `/proc` does not answer.
fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("VmRSS:"))
                .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())
        })
        .map_or(0, |kb| kb * 1024)
}

/// Every byte under `dir`, recursively.
fn dir_bytes(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                dir_bytes(&p)
            } else {
                std::fs::metadata(&p).map_or(0, |m| m.len())
            }
        })
        .sum()
}

// ---------------------------------------------------------------------------
// One client, one copy
// ---------------------------------------------------------------------------

/// A store is attached once, and a mirror verb without one is refused.
#[test]
fn attaching_a_store_is_once_and_a_mirror_verb_needs_one() {
    let _g = serial();
    let mut fx = Fixture::start();

    let mut plain = GnitzClient::connect(fx.server.sock_path()).unwrap();
    let e = plain.mirror_view("s", "v_keyed").unwrap_err();
    assert!(
        matches!(e, ClientError::NoMirrorStore),
        "a client with no store says so: {e}"
    );

    let held = fx.base_dir();
    let second_dir = tempfile::tempdir().unwrap();
    let second_path = second_dir.path().to_str().unwrap().to_string();
    let store = Mirror::open(&second_path).expect("a second directory opens");
    let e = fx.mirror().attach_mirror(store).unwrap_err().to_string();
    assert!(e.contains(&held), "the refusal must name the path already held: {e}");
    // The refused store was dropped with the call, which released its lock.
    Mirror::open(&second_path).expect("a refused attach must release the store it was passed");
}

/// `DROP VIEW` and a rename **through the mirroring client** stop that view
/// answering locally, and `ALTER VIEW … AS` cannot reach one at all.
///
/// The copy and the DDL path share one object now, so without the hook a
/// `SELECT` after a `DROP VIEW` on this same client answers rows off a view it
/// just dropped, and a rename leaves the old name reading the copy while the new
/// name goes upstream. The third case is closed one layer up: retargeting a view
/// that carries a delta feed is refused, and a feed is what makes a view
/// mirrorable at all.
#[test]
fn ddl_through_the_mirroring_client_retires_its_own_copy() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 40);
    for name in ["v_drop", "v_rename"] {
        sql(
            &mut fx.direct,
            "s",
            &format!("CREATE VIEW {name} WITH (delta = '{FEED}') AS SELECT a, b, v FROM t WHERE v >= 0"),
        );
    }
    let mut ids = Vec::new();
    for name in ["v_drop", "v_rename"] {
        ids.push(fx.mirror().mirror_view("s", name).expect(name).view_id);
    }
    fx.drain("s", &["v_drop", "v_rename"]);
    for &tid in &ids {
        assert!(fx.mirror().mirrors(tid), "each copy answers before the DDL");
    }

    sql(fx.mirror(), "s", "DROP VIEW v_drop");
    fx.mirror()
        .alter_rename_relation("s", "v_rename", "v_renamed")
        .expect("rename");

    for (name, &tid) in ["v_drop", "v_rename"].iter().zip(ids.iter()) {
        assert!(
            !fx.mirror().mirrors(tid),
            "{name}: the copy must stop answering once this client retired it",
        );
    }
    // The one that still exists upstream reads correctly — delegated now.
    fx.differential("s", "SELECT * FROM v_renamed");
    // And the old name is gone rather than answering off the copy.
    assert!(
        SqlPlanner::new(fx.mirror(), "s")
            .execute("SELECT * FROM v_rename")
            .is_err(),
        "a renamed view must not keep answering under its old name",
    );
    // A mirrored view cannot be retargeted, so no copy is ever left behind one.
    let e = SqlPlanner::new(fx.mirror(), "s")
        .execute("ALTER VIEW v_renamed AS SELECT a, b, v FROM t WHERE v > 1")
        .expect_err("a fed view cannot be retargeted");
    assert!(e.to_string().contains("delta feed"), "{e}");
}

/// A view another client renamed keeps its copy when this client re-mirrors it
/// under the new name and then mirrors a new view created under the old one:
/// the record's name is refreshed, so the new view does not match it.
#[test]
fn a_view_renamed_upstream_survives_a_new_view_under_its_old_name() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let renamed = fx.mirror().mirror_view("s", "v_keyed").expect("mirror v_keyed").view_id;
    fx.quiesce();

    fx.direct
        .alter_rename_relation("s", "v_keyed", "v_moved")
        .expect("rename through the other client");
    sql(
        &mut fx.direct,
        "s",
        &format!("CREATE VIEW v_keyed WITH (delta = '{FEED}') AS SELECT a, b, v FROM t WHERE b = 3"),
    );
    let _ = query(&mut fx.direct, "s", "SELECT COUNT(*) AS n FROM v_keyed");

    assert_eq!(
        fx.mirror()
            .mirror_view("s", "v_moved")
            .expect("re-mirror under the new name")
            .view_id,
        renamed,
        "a rename keeps the id",
    );
    let fresh = fx
        .mirror()
        .mirror_view("s", "v_keyed")
        .expect("mirror the new view")
        .view_id;
    assert_ne!(fresh, renamed);
    assert!(
        fx.mirror().mirrored_ids().contains(&renamed),
        "the renamed view's registration must survive a new view under its old name",
    );
    assert!(fx.mirror().mirrors(renamed), "and its copy still answers");
    assert!(fx.mirror().mirrors(fresh));
    fx.quiesce();
    fx.differential("s", "SELECT * FROM v_moved");
    fx.differential("s", "SELECT * FROM v_keyed");
}

/// A reconnect keeps every registration, is refused inside a transaction, and
/// **closes the read gate** until the next poll.
///
/// The park hook rides along too — that it does is asserted where it can be
/// asserted without a signal race, over the session move `reconnect` performs
/// (`gnitz-core`'s `a_taken_park_hook_moves_to_the_session_that_replaces_it`).
#[test]
fn a_reconnect_keeps_the_registrations_and_closes_the_read_gate() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let (keyed, repl) = fx.mirror_both();
    fx.quiesce();

    let target = fx.server.sock_path().to_string();
    fx.mirror().txn_begin().expect("begin");
    assert!(
        fx.mirror().reconnect(&target).is_err(),
        "a reconnect inside a transaction is refused",
    );
    fx.mirror().txn_rollback().expect("rollback");

    fx.reconnect_mirror();

    let mut ids = fx.mirror().mirrored_ids();
    ids.sort_unstable();
    let mut want = vec![keyed, repl];
    want.sort_unstable();
    assert_eq!(ids, want, "the poll's work list survives a reconnect");
    for tid in [keyed, repl] {
        assert!(
            !fx.mirror().mirrors(tid),
            "every cursor is dropped, so the gate is shut"
        );
        assert!(fx.mirror().cursor_of(tid).is_none());
    }

    let before_reqs = fx.mirror().requests_sent();
    fx.differential("s", "SELECT * FROM v_keyed");
    assert!(
        fx.mirror().requests_sent() > before_reqs,
        "a read between the reconnect and the next poll goes upstream",
    );
    let report = fx.mirror().poll_mirror().expect("the poll after a reconnect");
    assert_eq!(report.len(), 2);
    assert!(
        report.iter().all(|o| o.result.reseeded()),
        "every view reseeds, and the report says so",
    );
    fx.quiesce();
    fx.differential("s", "SELECT * FROM v_keyed");
    fx.differential("s", "SELECT * FROM v_repl");
}

/// A poll whose id no longer resolves upstream re-resolves by name rather than
/// bootstrapping at the stale id.
///
/// The reachable form of the cursor-less arm: a reconnect drops every cursor,
/// and the view was recreated upstream under a fresh id meanwhile. A bootstrap
/// in place would read a relation that no longer exists.
#[test]
fn a_cursor_less_poll_re_resolves_rather_than_bootstrapping_in_place() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 60);
    let (old, _) = fx.mirror_both();
    fx.quiesce();

    sql(&mut fx.direct, "s", "DROP VIEW v_keyed");
    sql(
        &mut fx.direct,
        "s",
        &format!("CREATE VIEW v_keyed WITH (delta = '{FEED}') AS SELECT a, b, v, f, body FROM t WHERE v >= 0"),
    );
    let _ = query(&mut fx.direct, "s", "SELECT COUNT(*) AS n FROM v_keyed");

    fx.reconnect_mirror();
    let report = fx.mirror().poll_mirror().expect("the poll re-resolves by name");
    let moved = report
        .iter()
        .find(|o| o.view_id != old && o.result.reseeded())
        .expect("the recreated view must be mirrored under its fresh id");
    assert!(
        !fx.mirror().mirrored_ids().contains(&old),
        "the stale id must be retracted, not kept beside the new one",
    );
    assert!(fx.mirror().mirrors(moved.view_id));
    fx.differential("s", "SELECT * FROM v_keyed");
}

/// Mirroring a view costs one RESOLVE and one bootstrap read, and nothing else
/// — no `SCHEMA_TAB` scan, first sighting of a schema or not.
///
/// The store mints no ids, so it needs no schema id, so no registration reaches
/// upstream for one. The assertion is on both views, not just the second: a
/// first-sighting scan would show up on the first alone.
#[test]
fn mirroring_a_view_costs_one_resolve_and_one_bootstrap() {
    let _g = serial();
    let mut fx = Fixture::start();
    churn(&mut fx.direct, 1, 20);
    let _ = query(&mut fx.direct, "s", "SELECT COUNT(*) AS n FROM v_keyed");
    let _ = query(&mut fx.direct, "s", "SELECT COUNT(*) AS n FROM v_repl");

    let before = fx.mirror().requests_sent();
    fx.mirror().mirror_view("s", "v_keyed").expect("the first view");
    let first = fx.mirror().requests_sent() - before;
    fx.mirror().mirror_view("s", "v_repl").expect("the second view");
    let second = fx.mirror().requests_sent() - before - first;

    assert_eq!(second, 2, "one RESOLVE and one bootstrap read");
    assert_eq!(
        first, 2,
        "and the first sighting of a schema costs no more than the second"
    );
}

/// A poisoned store refuses a mirrored read rather than answering it, an unheld
/// relation still reads through the same client, and `close_mirror` is the way
/// out.
///
/// In a child because the seam is a per-process latch. It is also the first test
/// the panic guard has: nothing but a panic reaches its arm.
#[test]
fn a_poisoned_store_refuses_the_reads_it_would_answer() {
    let _g = serial();
    if !cfg!(debug_assertions) {
        return; // the seam folds away in a release build
    }
    let (server, dir) = child_fixture();
    run_child(
        "poisoned_read_child",
        &server,
        &dir,
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
    let tid = mirror
        .mirror_view("s", "v_keyed")
        .expect("the bootstrap succeeds")
        .view_id;
    assert!(mirror.mirrors(tid), "the copy is valid before the poll that panics");

    churn(&mut direct, 41, 80);
    let _ = query(&mut direct, "s", "SELECT COUNT(*) AS n FROM v_keyed");
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| mirror.poll_mirror()));
    assert!(unwound.is_err(), "the armed seam must panic inside the guarded region");
    assert!(
        mirror.mirror_poisoned().is_some(),
        "the guard poisons before it lets the unwind continue",
    );

    // The copy is still gated in — cursor and registration both stand — so this
    // is a read the store would otherwise have answered.
    assert!(mirror.mirrors(tid));
    let err = SqlPlanner::new(&mut mirror, "s")
        .execute("SELECT * FROM v_keyed")
        .expect_err("a poisoned copy must refuse the read it would answer");
    assert!(err.to_string().contains("poisoned"), "the refusal must say why: {err}",);

    // A relation the copy does not hold is unaffected: poison must not take
    // every read on the connection down with it.
    let local = query(&mut mirror, "s", "SELECT * FROM t");
    let remote = query(&mut direct, "s", "SELECT * FROM t");
    assert_same_zset(
        "an unheld relation still reads through a poisoned client",
        (&local.0, &local.1),
        (&remote.0, &remote.1),
    );

    // `close_mirror` publishes no checkpoint, releases the directory, and leaves
    // the connection usable — the only recovery a poisoned copy has.
    mirror.close_mirror().expect("closing a poisoned store is not an error");
    assert!(
        !has_manifest(&dir, tid),
        "a poisoned store must publish nothing on the way out",
    );
    assert!(mirror.mirror_poisoned().is_none(), "the poison went with the store");
    let second = mirroring_client(&sock, &dir);
    drop(second);
    mirror
        .attach_mirror(Mirror::open(&dir).expect("the directory came back"))
        .expect("a later attach on the same connection is legal");
    let recovered = mirror.mirror_view("s", "v_keyed").expect("and it bootstraps again");
    assert!(recovered.result.reseeded());
    println!("{CHILD_OK}");
}

// ---------------------------------------------------------------------------
// Child-process plumbing
// ---------------------------------------------------------------------------

/// A seeded, churned server and an empty mirror directory — the setup every
/// parent of a child test needs before it can hand the pair over.
fn child_fixture() -> (ServerHandle, tempfile::TempDir) {
    let server = start_server();
    let dir = tempfile::tempdir().unwrap();
    let mut direct = GnitzClient::connect(server.sock_path()).unwrap();
    seed(&mut direct);
    churn(&mut direct, 1, 40);
    // Drains the pending ticks, so the rounds a bootstrap reads already exist.
    let _ = query(&mut direct, "s", "SELECT COUNT(*) AS n FROM v_keyed");
    (server, dir)
}

/// Start a child on `name` against `server`/`dir`, with `envs` on top, and
/// assert it ran to its sentinel. Returns the child's stdout, which the
/// footprint bench prints.
fn run_child(name: &str, server: &ServerHandle, dir: &tempfile::TempDir, envs: &[(&str, &str)], what: &str) -> String {
    let mut all = vec![
        ("GNITZ_MIRROR_SOCK", server.sock_path()),
        ("GNITZ_MIRROR_DIR", dir.path().to_str().unwrap()),
    ];
    all.extend_from_slice(envs);
    let out = run_test_in_child(module_path!(), name, &all);
    assert_child_ok(&out, what);
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// What a parent handed this child, or `None` in the parent's own run of the
/// same test — every child test is also collected by the outer `cargo test`.
fn child_target() -> Option<(String, String)> {
    Some((
        std::env::var("GNITZ_MIRROR_SOCK").ok()?,
        std::env::var("GNITZ_MIRROR_DIR").ok()?,
    ))
}

/// Runs only in the child `a_second_handle_on_one_directory_is_refused` spawns.
/// A bare `Mirror::open` — it takes the same `flock` the parent holds, and needs
/// no server, so it takes only the directory a parent hands over.
#[test]
fn second_process_open_child() {
    let Ok(dir) = std::env::var("GNITZ_MIRROR_DIR") else {
        return;
    };
    match Mirror::open(&dir) {
        Ok(_) => println!("second open unexpectedly succeeded"),
        Err(e) => println!("{e}"),
    }
    println!("{CHILD_OK}");
}
