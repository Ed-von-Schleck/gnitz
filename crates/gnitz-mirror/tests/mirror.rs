//! The mirror's acceptance suite.
//!
//! A Rust integration test rather than a pytest: the Python suite drives
//! `gnitz-py`, which links no mirror. It reaches a real server through
//! `gnitz-test-harness` and builds its `ReadSpec`s through `gnitz-sql`, both
//! **dev**-dependencies — neither is in the crate's dependency graph, and
//! neither closes a cycle, since nothing depends on `gnitz-mirror`.
//!
//! **Every test runs at four workers.** W=1 exercises none of what the design
//! rests on: at W=1 there is one slice, so a mirror that mishandled the
//! concatenation of W slices would still pass, and the replicated-vs-keyed
//! routing split collapses to the same thing.

mod support;

use gnitz_core::{GnitzClient, Schema, ZSetBatch};
use gnitz_mirror::Mirror;
use gnitz_test_harness::ServerHandle;
use support::{assert_same_zset, canonical, query, serial, sql};

/// Four workers, because that is the only count that exercises the fan-out.
const WORKERS: usize = 4;
/// Feed budget: large enough that nothing in this suite is swept out from under
/// a cursor except where a test means it to be.
const FEED: &str = "8 MB";

/// A test's server plus the two connections it needs: one the mirror owns, one
/// that reads the same relations directly for the differential.
struct Fixture {
    server: ServerHandle,
    direct: GnitzClient,
    mirror: Option<Mirror>,
    dir: tempfile::TempDir,
}

impl Fixture {
    fn start() -> Option<Fixture> {
        let server = ServerHandle::start_n(WORKERS)?;
        let dir = tempfile::tempdir().unwrap();
        let mut direct = GnitzClient::connect(server.sock_path()).unwrap();
        seed(&mut direct);
        let mirror = Mirror::open(
            dir.path().to_str().unwrap(),
            GnitzClient::connect(server.sock_path()).unwrap(),
        )
        .expect("a fresh mirror opens");
        Some(Fixture {
            server,
            direct,
            mirror: Some(mirror),
            dir,
        })
    }

    fn mirror(&mut self) -> &mut Mirror {
        self.mirror.as_mut().unwrap()
    }

    fn base_dir(&self) -> String {
        self.dir.path().to_str().unwrap().to_string()
    }

    /// Replace the mirror's upstream connection, keeping its local state — what
    /// a host does after the server it was talking to went away.
    fn reconnect_mirror(&mut self) {
        let client = GnitzClient::connect(self.server.sock_path()).unwrap();
        *self.mirror().client_mut() = client;
    }

    /// Drop the handle (which checkpoints) and open a fresh one on the same
    /// directory.
    fn reopen(&mut self) {
        self.mirror = None;
        let client = GnitzClient::connect(self.server.sock_path()).unwrap();
        self.mirror = Some(Mirror::open(&self.base_dir(), client).expect("a checkpointed mirror reopens"));
    }

    /// Make the server emit the rounds the pushes so far produced, then carry
    /// them into the copy.
    ///
    /// The order is not cosmetic and it is the whole freshness contract: a poll
    /// does **not** drive a tick and a read against the server does, so a push
    /// ACKed but not yet ticked is in the server's answer and not in the copy.
    /// Reading first drains, so the rounds exist before the poll that collects
    /// them.
    fn quiesce(&mut self) {
        let _ = query(&mut self.direct, "s", "SELECT * FROM v_keyed");
        let _ = query(&mut self.direct, "s", "SELECT * FROM v_repl");
        self.mirror().poll().expect("poll");
    }

    /// Mirror both views of the shared schema, and return `v_keyed`'s id — the
    /// one a test names when it needs an id. Nearly every test wants both: the
    /// keyed one carries the compound PK, the replicated one the worker-0 feed.
    fn mirror_both(&mut self) -> u64 {
        let tid = self.mirror().mirror_view("s", "v_keyed").expect("mirror v_keyed");
        self.mirror().mirror_view("s", "v_repl").expect("mirror v_repl");
        tid
    }

    /// Run one `SELECT` against the local copy.
    fn local(&mut self, sql_text: &str) -> (Schema, ZSetBatch) {
        query(self.mirror.as_mut().unwrap(), "s", sql_text)
    }

    /// Assert a query answers identically locally and against the server, and
    /// return how many rows that agreement covered — two empty replies agree
    /// about nothing, so every caller sums this and refuses a vacuous pass.
    fn assert_differential(&mut self, sql_text: &str) -> usize {
        let local = self.local(sql_text);
        let remote = query(&mut self.direct, "s", sql_text);
        assert_same_zset(sql_text, (&local.0, &local.1), (&remote.0, &remote.1))
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
    let Some(mut fx) = Fixture::start() else { return };

    churn(&mut fx.direct, 1, 200);
    fx.mirror_both();
    fx.quiesce();

    let mut compared = 0;
    for q in [
        "SELECT * FROM v_keyed",
        "SELECT a, b, v FROM v_keyed WHERE a = 42",
        "SELECT a, v FROM v_keyed WHERE a > 100 AND a < 140",
        "SELECT a, b, body FROM v_keyed WHERE v > 300 ORDER BY a DESC, b ASC LIMIT 17",
        "SELECT a, v FROM v_keyed ORDER BY v LIMIT 9 OFFSET 5",
        "SELECT b, COUNT(*) AS n, SUM(v) AS total, MIN(a) AS lo, MAX(a) AS hi FROM v_keyed GROUP BY b",
        "SELECT COUNT(*) AS n, SUM(v) AS total FROM v_keyed",
        "SELECT DISTINCT b FROM v_keyed",
        "SELECT b, SUM(v) AS total FROM v_keyed GROUP BY b HAVING SUM(v) > 1000",
        "SELECT * FROM v_repl",
        "SELECT id, v FROM v_repl WHERE id > 50 ORDER BY id LIMIT 11",
        "SELECT COUNT(*) AS n, SUM(v) AS total FROM v_repl",
    ] {
        compared += fx.assert_differential(q);
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
    let Some(mut fx) = Fixture::start() else { return };

    churn(&mut fx.direct, 1, 60);
    fx.mirror_both();
    fx.quiesce();
    fx.assert_differential("SELECT * FROM v_keyed");

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
        fx.assert_differential("SELECT * FROM v_keyed");
        fx.assert_differential("SELECT * FROM v_repl");
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
    let Some(mut fx) = Fixture::start() else { return };

    churn(&mut fx.direct, 1, 300);
    fx.mirror_both();
    fx.quiesce();

    fx.assert_differential("SELECT b, COUNT(*) AS n, SUM(v) AS total FROM v_keyed GROUP BY b");

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

/// A mirrored SELECT issues no request; a delegated one issues exactly one.
///
/// The first is the falsifiable form of the local `resolve_relation` and
/// `table_indexes`: a delegated resolve would show up as one request per
/// statement and nothing else in this suite would notice. The second pins the
/// statement bracket — without it the descriptor-by-id lookup misses the scope
/// and the index probe issues a second RESOLVE, so the count reads two.
///
/// Both are counts, not clocks.
#[test]
fn a_mirrored_select_issues_no_request() {
    let _g = serial();
    let Some(mut fx) = Fixture::start() else { return };

    churn(&mut fx.direct, 1, 40);
    fx.mirror_both();
    fx.quiesce();

    let m = fx.mirror.as_mut().unwrap();
    let before = m.client_mut().requests_sent();
    let _ = query(m, "s", "SELECT a, b, v FROM v_keyed WHERE a = 7");
    let after = m.client_mut().requests_sent();
    assert_eq!(after, before, "a mirrored SELECT must issue no request at all");

    // An unmirrored relation read through the same call: one RESOLVE plus the
    // read itself is two frames, so assert the delta rather than a bare zero —
    // what this pins is that the bracket kept the resolve to *one*.
    let before = m.client_mut().requests_sent();
    let _ = query(m, "s", "SELECT a, b, v FROM t WHERE a = 7");
    let delegated = m.client_mut().requests_sent() - before;
    assert_eq!(
        delegated, 2,
        "a delegated read costs one RESOLVE and one read; a missing statement bracket makes it three",
    );
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

/// A second handle is refused, in the process and across processes alike.
#[test]
fn a_second_handle_is_refused() {
    let _g = serial();
    let Some(mut fx) = Fixture::start() else { return };

    // In-process: any directory, because what it protects is process-global.
    let other = tempfile::tempdir().unwrap();
    let client = GnitzClient::connect(fx.server.sock_path()).unwrap();
    assert!(
        Mirror::open(other.path().to_str().unwrap(), client).is_err(),
        "a second live handle in one process must be refused",
    );

    // Cross-process: the same directory, with the first handle dropped so only
    // the flock can refuse it.
    let dir = fx.base_dir();
    fx.mirror = None;
    let client = GnitzClient::connect(fx.server.sock_path()).unwrap();
    let held = Mirror::open(&dir, client).expect("the directory is free again");
    let out = child_test("second_process_open_child", &[("GNITZ_MIRROR_LOCK_DIR", &dir)]);
    let err = String::from_utf8_lossy(&out.stdout);
    assert!(
        err.contains("locked by another live process"),
        "a second process on a held directory must be refused: {err}",
    );
    drop(held);
}

/// Forgetting a view and mirroring it again — same server id, same layout —
/// leaves a working copy.
///
/// The retraction and the re-registration meet in the local COL_TAB. Retracting
/// the VIEW_TAB row cascades a retraction of the relation's columns, so a
/// retraction that also negated them itself would leave each at net `-1`; the
/// re-registration's `+1` would then sum to zero and the relation would register
/// with no columns at all. Nothing weaker catches it: a view recreated upstream
/// takes a *new* id, where the stranded rows are invisible.
#[test]
fn a_forgotten_view_can_be_mirrored_again() {
    let _g = serial();
    let Some(mut fx) = Fixture::start() else { return };

    churn(&mut fx.direct, 1, 60);
    let tid = fx.mirror().mirror_view("s", "v_keyed").expect("mirror v_keyed");
    fx.quiesce();
    let before = fx.assert_differential("SELECT * FROM v_keyed");
    assert!(before > 0, "the differential must cover some rows");

    fx.mirror().forget_view(tid).expect("forget");
    assert!(
        !fx.mirror().mirrors(tid),
        "a forgotten view is no longer answered locally"
    );

    assert_eq!(
        fx.mirror().mirror_view("s", "v_keyed").expect("re-mirror"),
        tid,
        "the same relation comes back under the same server id",
    );
    fx.quiesce();
    assert_eq!(
        fx.assert_differential("SELECT * FROM v_keyed"),
        before,
        "the rebuilt copy must hold what it held before",
    );
}

/// A relation with no feed cannot be mirrored, and neither can a base table.
#[test]
fn only_a_fed_view_can_be_mirrored() {
    let _g = serial();
    let Some(mut fx) = Fixture::start() else { return };
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
    let Some(server) = ServerHandle::start_n(WORKERS) else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let mut direct = GnitzClient::connect(server.sock_path()).unwrap();
    seed(&mut direct);
    churn(&mut direct, 1, 40);
    let _ = query(&mut direct, "s", "SELECT * FROM v_keyed");

    let out = child_test(
        "poisons_on_a_storage_fault_child",
        &[
            ("GNITZ_MIRROR_SOCK", server.sock_path()),
            ("GNITZ_MIRROR_DIR", dir.path().to_str().unwrap()),
            ("GNITZ_INJECT_INGEST_APPLY_ERROR", "store"),
        ],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "the host must survive a storage fault and see an error; exit 134 is the abort this replaced",
    );
}

/// Runs only in the child the test above spawns.
#[test]
fn poisons_on_a_storage_fault_child() {
    let (Ok(sock), Ok(dir)) = (std::env::var("GNITZ_MIRROR_SOCK"), std::env::var("GNITZ_MIRROR_DIR")) else {
        return;
    };
    let client = GnitzClient::connect(&sock).unwrap();
    let mut mirror = Mirror::open(&dir, client).expect("open");
    let err = mirror
        .mirror_view("s", "v_keyed")
        .expect_err("the armed seam must fail the bootstrap ingest");
    assert!(
        matches!(err, gnitz_mirror::MirrorError::Poisoned(_)),
        "an ingest fault must poison the handle: {err}",
    );
    assert!(mirror.poisoned().is_some());
    // Every subsequent call is refused rather than answering off a copy with a
    // hole in it.
    assert!(mirror.poll().is_err());
    assert!(mirror.checkpoint().is_err());
    // And the drop must not checkpoint a possibly-torn store.
    drop(mirror);
}

/// The whole durability path runs with `io_uring` unavailable: open, checkpoint,
/// and a reopen that resumes.
#[test]
fn the_blocking_fsync_fallback_carries_durability() {
    let _g = serial();
    let Some(server) = ServerHandle::start_n(WORKERS) else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let mut direct = GnitzClient::connect(server.sock_path()).unwrap();
    seed(&mut direct);
    churn(&mut direct, 1, 40);
    let _ = query(&mut direct, "s", "SELECT * FROM v_keyed");

    let out = child_test(
        "blocking_fsync_fallback_child",
        &[
            ("GNITZ_MIRROR_SOCK", server.sock_path()),
            ("GNITZ_MIRROR_DIR", dir.path().to_str().unwrap()),
            ("GNITZ_DISABLE_IO_URING", "1"),
        ],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "the blocking fallback must carry the full path"
    );
}

/// Runs only in the child the test above spawns.
#[test]
fn blocking_fsync_fallback_child() {
    let (Ok(sock), Ok(dir)) = (std::env::var("GNITZ_MIRROR_SOCK"), std::env::var("GNITZ_MIRROR_DIR")) else {
        return;
    };
    let open = |dir: &str| Mirror::open(dir, GnitzClient::connect(&sock).unwrap()).expect("open");
    let mut mirror = open(&dir);
    let tid = mirror.mirror_view("s", "v_keyed").expect("mirror");
    mirror.poll().expect("poll");
    mirror.checkpoint().expect("checkpoint under the blocking fallback");
    let before = query(&mut mirror, "s", "SELECT * FROM v_keyed");
    let cursor = mirror.cursor_of(tid).expect("a cursor");
    drop(mirror);

    let mut mirror = open(&dir);
    assert_eq!(
        mirror.mirror_view("s", "v_keyed").expect("re-mirror"),
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
    let Some(mut fx) = Fixture::start() else { return };
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
    let Some(mut fx) = Fixture::start() else { return };

    churn(&mut fx.direct, 1, 120);
    let tid = fx.mirror_both();
    fx.quiesce();
    fx.mirror().checkpoint().expect("checkpoint");
    let before = fx.local("SELECT * FROM v_keyed");
    let cursor = fx.mirror().cursor_of(tid).expect("a cursor");

    fx.reopen();
    assert_eq!(
        fx.mirror().mirror_view("s", "v_keyed").expect("re-mirror"),
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
    fx.assert_differential("SELECT * FROM v_keyed");
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
    let Some(mut fx) = Fixture::start() else { return };

    churn(&mut fx.direct, 1, 90);
    fx.mirror_both();
    fx.quiesce();
    fx.mirror().checkpoint().expect("checkpoint");

    // Case 1 — the copies moved on and were published; the cursor file did not.
    // Snapshot the file, apply more rounds, checkpoint again, restore the old
    // file: that is exactly a crash between the ephemeral round and the write.
    let cursor_path = format!("{}/mirror_cursors", fx.base_dir());
    let stale = std::fs::read(&cursor_path).expect("a checkpoint writes the cursor file");
    churn(&mut fx.direct, 91, 180);
    fx.quiesce();
    fx.mirror().checkpoint().expect("second checkpoint");
    fx.mirror = None;
    std::fs::write(&cursor_path, &stale).unwrap();

    fx.reopen();
    fx.mirror_both();
    fx.quiesce();
    fx.assert_differential("SELECT * FROM v_keyed");

    // Case 2 — the generation is durably ahead of every output manifest, so each
    // copy erases at open. The bump is the same one the checkpoint's step 0
    // makes; leaving out the flush round is the crash.
    fx.mirror().checkpoint().expect("checkpoint before the torn bump");
    fx.mirror = None;
    with_engine(&fx.base_dir(), |e| {
        e.bump_checkpoint_generation().expect("durably advance the generation");
    });

    fx.reopen();
    fx.mirror_both();
    fx.quiesce();
    fx.assert_differential("SELECT * FROM v_keyed");
    fx.assert_differential("SELECT * FROM v_repl");
}

/// A foreign topology word reseeds, even though the generation still matches.
///
/// Without this gate the copies resume on the generation alone, because a view's
/// own recovery source never consults topology — so a state-format bump would
/// let a mirror resume shards written in the previous layout. Driven through the
/// engine's public topology record rather than by poking the field: the mirror
/// always opens at one worker, so a recorded `2` mismatches for the same reason
/// a state-format bump would.
///
/// The observable is the copy's **manifest**, which is what "erased" means on
/// disk: a checkpoint publishes one, a resume loads it, and an erase-and-reseed
/// unlinks it and writes nothing until the next checkpoint. A cursor round would
/// not do: with nothing pushed in between, a bootstrap and a poll report the same
/// round, so the two paths are indistinguishable by it.
#[test]
fn a_foreign_topology_word_reseeds() {
    let _g = serial();
    let Some(mut fx) = Fixture::start() else { return };

    churn(&mut fx.direct, 1, 60);
    let tid = fx.mirror_both();
    fx.quiesce();
    fx.mirror().checkpoint().expect("checkpoint");
    assert!(
        has_manifest(&fx.base_dir(), tid),
        "a checkpoint must publish the copy's manifest",
    );
    fx.mirror = None;
    with_engine(&fx.base_dir(), |e| {
        e.record_topology(2).expect("record a foreign topology");
        e.flush_all_system_tables().expect("make it durable");
    });

    fx.reopen();
    fx.mirror().mirror_view("s", "v_keyed").expect("re-mirror");
    assert!(
        !has_manifest(&fx.base_dir(), tid),
        "a foreign topology word must erase the copy and bootstrap it, not resume it",
    );
    fx.mirror().mirror_view("s", "v_repl").expect("re-mirror v_repl");
    fx.quiesce();
    fx.assert_differential("SELECT * FROM v_keyed");
    fx.assert_differential("SELECT * FROM v_repl");
}

/// Whether the mirrored copy of `view_id` currently has a published manifest —
/// the on-disk difference between a resumed store and an erased one.
fn has_manifest(base_dir: &str, view_id: u64) -> bool {
    let root = std::path::Path::new(base_dir).join("s").join(format!("v_{view_id}"));
    fn search(dir: &std::path::Path) -> bool {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return false;
        };
        entries.flatten().any(|e| {
            let p = e.path();
            if p.is_dir() {
                search(&p)
            } else {
                p.file_name().is_some_and(|n| n == "manifest.bin")
            }
        })
    }
    search(&root)
}

/// A server restart erases the copy: the stored cursor tag no longer matches the
/// one the next reply carries, and the recovery reseeds under the same id.
#[test]
fn a_server_restart_reseeds_the_copy() {
    let _g = serial();
    let Some(mut fx) = Fixture::start() else { return };

    churn(&mut fx.direct, 1, 60);
    let tid = fx.mirror_both();
    fx.quiesce();

    fx.server.restart();
    fx.direct = GnitzClient::connect(fx.server.sock_path()).unwrap();
    fx.reconnect_mirror();
    churn(&mut fx.direct, 61, 120);
    let _ = query(&mut fx.direct, "s", "SELECT * FROM v_keyed");
    fx.mirror()
        .poll()
        .expect("the poll must recover from a foreign tag by itself");
    assert!(fx.mirror().mirrors(tid), "the id is unchanged across a server restart");
    fx.quiesce();
    fx.assert_differential("SELECT * FROM v_keyed");
    fx.assert_differential("SELECT * FROM v_repl");
}

/// A view recreated under the same name reseeds under its **new** id.
///
/// The tag stops continuing for the other of its two reasons, and the recovery
/// must re-resolve the name before it bootstraps. A test that only restarts the
/// server exercises the reason that leaves the id alone, so it would pass against
/// a recovery that bootstraps in place — and that recovery reads a relation that
/// no longer exists.
#[test]
fn a_view_recreated_under_the_same_name_reseeds_under_its_new_id() {
    let _g = serial();
    let Some(mut fx) = Fixture::start() else { return };

    churn(&mut fx.direct, 1, 60);
    let old = fx.mirror_both();
    fx.quiesce();

    sql(&mut fx.direct, "s", "DROP VIEW v_keyed");
    sql(
        &mut fx.direct,
        "s",
        &format!("CREATE VIEW v_keyed WITH (delta = '{FEED}') AS SELECT a, b, v, f, body FROM t WHERE v >= 0"),
    );
    churn(&mut fx.direct, 61, 120);
    let _ = query(&mut fx.direct, "s", "SELECT * FROM v_keyed");

    fx.mirror().poll().expect("the poll must re-resolve before it reseeds");
    let new = fx
        .mirror()
        .mirrored_ids()
        .into_iter()
        .find(|&t| t != old)
        .expect("the recreated view must be mirrored under a fresh id");
    assert!(!fx.mirror().mirrors(old), "the old id must be gone");
    assert_ne!(old, new);
    fx.quiesce();
    fx.assert_differential("SELECT * FROM v_keyed");
}

/// Reach into the mirror's own data directory through a bare engine, to
/// fabricate a durable state the handle would never write.
///
/// The caller must have dropped the handle first: the directory takes one
/// writer, which is exactly what the two tests above rely on the lock for.
fn with_engine(dir: &str, f: impl FnOnce(&mut gnitz_engine::catalog::CatalogEngine)) {
    let mut engine = gnitz_engine::catalog::CatalogEngine::open(dir, 1).expect("the directory is free");
    f(&mut engine);
    engine.close();
}

// ---------------------------------------------------------------------------
// Child-process plumbing
// ---------------------------------------------------------------------------

/// Re-run this test binary filtered to `name` with `envs` set, and hand back the
/// child's exit status and stdout.
///
/// A fault seam and the `io_uring` verdict are each read once per process into a
/// latch, so a test that needs one set has to be its own process — setting the
/// variable here would race every sibling under the shared test runner.
fn child_test(name: &str, envs: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.arg("--exact").arg(name).arg("--nocapture");
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.output().unwrap()
}

/// Runs only in the child `a_second_handle_is_refused` spawns. It opens the
/// engine rather than a `Mirror` because that is where the lock is taken, and
/// because it needs no server to reach it.
#[test]
fn second_process_open_child() {
    let Ok(dir) = std::env::var("GNITZ_MIRROR_LOCK_DIR") else {
        return;
    };
    match gnitz_engine::catalog::CatalogEngine::open(&dir, 1) {
        Ok(_) => println!("second open unexpectedly succeeded"),
        Err(e) => println!("{e}"),
    }
}
