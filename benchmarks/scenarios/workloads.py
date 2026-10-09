"""Scenarios that vary how the server is used: statements through the parser,
read verbs against views, transactions, contended writers, readers beside
writers, replicated placement, and views pushed out to subscribers."""

from __future__ import annotations

import asyncio
import random
import tempfile
import time
from dataclasses import dataclass

import gnitz
from gnitz import aio

from harness.run import time_calls, time_pushes

from .base import INGEST_BATCH, LOAD_BATCH, Fact, Scenario, Shape, Zipf, scatter
from .relational import FLOAT, NATIONS, SHIPMODES


def ops_for(run, per=1000, least=200):
    """How many single operations a phase runs."""
    return max(run.rows // per, least)


# ---------------------------------------------------------------------------
# SQL statements
# ---------------------------------------------------------------------------

@dataclass
class Statements(Scenario):
    """Single statements through `execute_sql`: what a client that speaks only
    SQL pays, statement by statement. No view is involved; a phase's rows are
    the rows its statements wrote or returned."""

    def run(self, run):
        n, ops = run.rows, ops_for(run)
        run.ddl("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL, cat BIGINT NOT NULL)",
                "CREATE TABLE inv (pk BIGINT NOT NULL PRIMARY KEY, qty BIGINT NOT NULL)",
                "CREATE TABLE srl (id BIGSERIAL PRIMARY KEY, v BIGINT NOT NULL)",
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                "CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES parent(id))")
        rng = random.Random(7)
        span = 1_000_000_000

        def row(k):
            # `val` is all but unique, so a probe on it finds one row.
            return dict(pk=k + 1, val=scatter(k, 40) % span, cat=rng.randrange(20))
        load = (run.ops("t", (row(k) for k in range(n)), LOAD_BATCH)
                + run.ops("inv", (dict(pk=k + 1, qty=1000) for k in range(ops)), LOAD_BATCH)
                + run.ops("parent", (dict(id=k + 1) for k in range(ops * 50)), LOAD_BATCH))
        with run.phase("load") as ph:
            ph.replay(load)

        at = [n]

        def insert(batch):
            def stmt(_):
                lo = at[0]
                at[0] += batch
                return ("INSERT INTO t (pk, val, cat) VALUES "
                        + ", ".join(f"({k + 1}, {scatter(k, 40) % span}, {k % 20})" for k in range(lo, lo + batch)))
            return stmt

        def each(name, stmt, count=ops, rows=1, counted=False):
            """`count` statements; `counted` takes a statement's rows from what it reports."""
            stmts = [stmt(i) for i in range(count)]
            with run.phase(name) as ph:
                for s in stmts:
                    out = ph.sql("statement", s, rows=0 if counted else rows)
                    if counted:
                        ph.rows += sum(r["count"] for r in out if r["type"] == "RowsAffected")

        each("insert_1", insert(1))
        each("insert_10", insert(10), rows=10)
        each("insert_100", insert(100), rows=100)
        each("select_all", lambda i: "SELECT * FROM t", 20, rows=at[0])
        each("select_payload", lambda i: "SELECT val, cat FROM t", 20, rows=at[0])
        each("select_key_val", lambda i: "SELECT pk, val FROM t", 20, rows=at[0])
        each("select_pk", lambda i: f"SELECT * FROM t WHERE pk = {scatter(i, 40) % n + 1}")
        each("select_limit", lambda i: "SELECT * FROM t LIMIT 100", rows=100)
        each("select_ordered", lambda i: "SELECT * FROM t ORDER BY pk LIMIT 100", rows=100)
        each("select_keyset", lambda i: f"SELECT * FROM t WHERE pk > {i * 7919 % (n - 100)} ORDER BY pk LIMIT 100",
             rows=100)
        each("update_pk", lambda i: f"UPDATE t SET cat = {i} WHERE pk = {scatter(i, 40) % n + 1}")
        each("update_scan", lambda i: f"UPDATE t SET cat = cat + 1 WHERE val > {span - span // 1000 * (i + 1)}", 10,
             counted=True)
        each("delete_pk", lambda i: f"DELETE FROM t WHERE pk = {i * 3 + 1}")
        each("delete_scan", lambda i: f"DELETE FROM t WHERE val > {span // 2 + i * (span // 1000)} AND val < {span // 2 + (i + 1) * (span // 1000)}",
             10, counted=True)
        each("create_index", lambda i: "CREATE INDEX ON t(val)", 1, rows=at[0])
        probes = [scatter(k, 40) % span for k in range(n // 2, n // 2 + ops)]
        each("select_index", lambda i: f"SELECT * FROM t WHERE val = {probes[i]}")
        each("update_index", lambda i: f"UPDATE t SET cat = {i} WHERE val = {probes[i]}")
        each("insert_100_indexed", insert(100), rows=100)
        each("upsert", lambda i: f"INSERT INTO inv (pk, qty) VALUES ({i * 2 + 1}, {i + 2}) "
                                 "ON CONFLICT (pk) DO UPDATE SET qty = EXCLUDED.qty")
        each("update_arith", lambda i: f"UPDATE inv SET qty = qty - 3 WHERE pk = {i % ops + 1}")
        each("insert_returning", lambda i: f"INSERT INTO srl (v) VALUES ({i * 10}) RETURNING id")
        each("insert_fk", lambda i: "INSERT INTO child VALUES "
                                    + ", ".join(f"({i * 50 + j + 1}, {i * 50 + j + 1})" for j in range(50)), rows=50)


# ---------------------------------------------------------------------------
# Read verbs against views
# ---------------------------------------------------------------------------

@dataclass
class Reads(Scenario):
    """The binary read verbs against views, which are what the product serves.
    Each seek follows a one-row push of its own, so it is a read of a
    just-acknowledged write and the tick that write owes runs inside it."""

    def run(self, run):
        n, ops = run.rows, ops_for(run)
        groups = 1000
        run.ddl("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust BIGINT UNSIGNED NOT NULL, g BIGINT NOT NULL, "
                "v BIGINT NOT NULL, ind BIGINT NOT NULL)",
                "CREATE INDEX ON t(ind)",
                "CREATE VIEW v_group AS SELECT g, SUM(v) AS s FROM t GROUP BY g",
                "CREATE VIEW v_cust AS SELECT cust, SUM(v) AS s FROM t GROUP BY cust",
                "CREATE VIEW v_pass AS SELECT * FROM t WHERE v >= 0",
                *(f"CREATE VIEW v_m{i} AS SELECT g, SUM(v) AS s FROM t WHERE v >= {i} GROUP BY g" for i in range(8)))
        rng = random.Random(7)

        def row(k):
            return dict(pk=k + 1, cust=k % groups + 1, g=k % groups, v=rng.randrange(1000), ind=k % 1001)
        load = run.ops("t", (row(k) for k in range(n)), LOAD_BATCH)
        with run.phase("load") as ph:
            ph.replay(load)

        c, t = run.conn, run.tables["t"]
        view = {v: c.resolve_table(v) for v in run.views}
        many = [view[f"v_m{i}"] for i in range(8)]
        with run.phase("scan_view") as ph:
            for _ in range(ops):
                ph.timed("scan", c.scan, *view["v_group"], rows=groups)
        singles = run.ops("t", (row(k) for k in range(n, n + 2 * ops)), 1)
        with run.phase("seek_group") as ph:
            for i, op in enumerate(singles[:ops]):
                ph.push(op)
                got = ph.timed("seek", c.seek, *view["v_cust"], (n + i) % groups + 1)
                assert len(got) == 1
        with run.phase("seek_passthrough") as ph:
            for i, op in enumerate(singles[ops:]):
                ph.push(op)
                got = ph.timed("seek", c.seek, *view["v_pass"], n + ops + i + 1)
                assert len(got) == 1, "a seek follows the write it reads"
        with run.phase("seek_index") as ph:
            for i in range(ops):
                got = ph.timed("seek", c.seek_by_index, t.tid, t.schema, [4], [i % 1001])
                ph.rows += len(got)
        with run.phase("adhoc_group") as ph:
            for i in range(50):
                ph.sql("select", f"SELECT g, SUM(v) AS s FROM t WHERE ind = {i} GROUP BY g", rows=n // 1001)
        with run.phase("scan_two_views") as ph:
            for _ in range(ops):
                ph.timed("scan_many", c.scan_many, many[:2], rows=2 * groups)
        with run.phase("scan_eight_views") as ph:
            for _ in range(ops):
                ph.timed("scan_many", c.scan_many, many, rows=8 * groups)


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

def _atomic_writer(conn, widx, ops, lines):
    o_tid, o_sch = conn.resolve_table("orders")
    l_tid, l_sch = conn.resolve_table("lineitem")
    conflicts, ms = 0, []
    for i in range(ops):
        okey = (widx * ops + i + 1) * 1000
        order = gnitz.ZSetBatch(o_sch).extend([dict(o_key=okey, o_cust=okey % 97 + 1, o_amt=100)])
        items = gnitz.ZSetBatch(l_sch).extend(dict(l_order=okey, l_line=ln, l_qty=ln) for ln in range(1, lines + 1))
        t = time.perf_counter()
        try:
            with conn.transaction() as txn:
                txn.push(o_tid, order)
                txn.push(l_tid, items)
        except gnitz.GnitzConflictError:
            conflicts += 1
        ms.append((time.perf_counter() - t) * 1e3)
    return {"calls": {"commit": (ms, (ops - conflicts) * (lines + 1))}, "conflicts": conflicts}


@dataclass
class Transactions(Scenario):
    """What a transaction costs over the same writes without one: atomic
    multi-table commits from four writers, a transaction of compounding
    UPDATEs against its autocommit twin, and the bare round trips."""

    def run(self, run):
        ops, lines = ops_for(run, 400, 500), 8
        run.ddl("CREATE TABLE orders (o_key BIGINT NOT NULL PRIMARY KEY, o_cust BIGINT NOT NULL, o_amt BIGINT NOT NULL)",
                "CREATE TABLE lineitem (l_order BIGINT NOT NULL, l_line BIGINT NOT NULL, l_qty BIGINT NOT NULL, "
                "PRIMARY KEY (l_order, l_line))",
                "CREATE TABLE acc (pk BIGINT NOT NULL PRIMARY KEY, bal BIGINT NOT NULL)",
                "CREATE TABLE kv (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                "CREATE VIEW v_order_lines AS SELECT l_order, COUNT(*) AS n, SUM(l_qty) AS qty FROM lineitem "
                "GROUP BY l_order")
        c = run.conn
        c.execute_sql("INSERT INTO acc VALUES (1, 1000000000)")
        with run.phase("atomic_commits") as ph:
            got = ph.clients([(_atomic_writer, w, ops, lines) for w in range(4)])
            ph.extra["conflicts"] = got["conflicts"]
        assert len(c.scan(*c.resolve_table("orders"))) == 4 * ops - got["conflicts"]

        update = "UPDATE acc SET bal = bal - 1 WHERE pk = 1"
        per_txn = 16

        def block(_):
            c.execute_sql("BEGIN")
            for _ in range(per_txn):
                c.execute_sql(update)
            c.execute_sql("COMMIT")
        with run.phase("txn_updates") as ph:
            ph.add("transaction", time_calls(block, ops // 5), rows=per_txn * (ops // 5))
        with run.phase("autocommit_updates") as ph:
            ph.add("statement", time_calls(lambda _: c.execute_sql(update), per_txn * (ops // 5)),
                   rows=per_txn * (ops // 5))

        def empty(_):
            c.execute_sql("BEGIN")
            c.execute_sql("COMMIT")
        with run.phase("empty_txn") as ph:
            ph.add("transaction", time_calls(empty, ops), rows=ops)
        kv = run.tables["kv"]
        rows = [gnitz.ZSetBatch(kv.schema).extend([dict(pk=i + 1, v=i)]) for i in range(2 * ops)]

        def one_write(i):
            with c.transaction() as txn:
                txn.push(kv.tid, rows[i])
        with run.phase("one_write_txn") as ph:
            ph.add("transaction", time_calls(one_write, ops), rows=ops)
        with run.phase("autocommit_push") as ph:
            ph.add("push", time_calls(lambda i: c.push(kv.tid, rows[ops + i]), ops), rows=ops)


def _rmw(conn, sql, ops):
    conflicts, ms = 0, []
    for _ in range(ops):
        t = time.perf_counter()
        while True:
            try:
                conn.execute_sql(sql)
                break
            except gnitz.GnitzConflictError:
                # Autocommit already retried, so a conflict that surfaces is sustained contention.
                conflicts += 1
        ms.append((time.perf_counter() - t) * 1e3)
    return {"calls": {"update": (ms, ops)}, "conflicts": conflicts}


@dataclass
class Contention(Scenario):
    """Optimistic concurrency under load: `clients` writers each increment the
    same `hot` rows in one statement, retrying until it commits. Every row must
    end at exactly the number of committed statements."""

    def run(self, run):
        ops = ops_for(run, 1000, 200)
        run.ddl("CREATE TABLE ctr (pk BIGINT NOT NULL PRIMARY KEY, n BIGINT NOT NULL)")
        c, ctr = run.conn, run.tables["ctr"]
        base = 0
        for hot in (1, 8, 64):
            for clients in (1, 2, 4):
                keys = range(base + 1, base + hot + 1)
                base += hot
                c.execute_sql("INSERT INTO ctr VALUES " + ", ".join(f"({k}, 0)" for k in keys))
                sql = f"UPDATE ctr SET n = n + 1 WHERE pk IN ({', '.join(map(str, keys))})"
                with run.phase(f"hot{hot}_x{clients}") as ph:
                    ph.extra["conflicts"] = ph.clients([(_rmw, sql, ops)] * clients)["conflicts"]
                got = {r.pk: r.n for r in c.scan(ctr.tid, ctr.schema) if r._weight > 0 and r.pk in keys}
                assert got == dict.fromkeys(keys, clients * ops), f"a lost update at hot={hot}, clients={clients}"


# ---------------------------------------------------------------------------
# Readers beside writers
# ---------------------------------------------------------------------------

HTAP_LINES, HTAP_CUSTOMERS, WRITER_BASE = 8, 200, 10_000_000


def _htap_writer(conn, widx, ops, products):
    o_tid, o_sch = conn.resolve_table("orders")
    l_tid, l_sch = conn.resolve_table("lineitem")
    rng, hot = random.Random(widx), Zipf(products)
    conflicts, ms = 0, []
    for i in range(ops):
        okey = WRITER_BASE * (widx + 1) + i
        order = gnitz.ZSetBatch(o_sch).extend([dict(
            o_key=okey, o_cust=okey % HTAP_CUSTOMERS + 1, o_status="OFP"[okey % 3], o_price=rng.randrange(100, 50_000))])
        items = gnitz.ZSetBatch(l_sch).extend(dict(l_order=okey, l_line=ln, l_qty=ln) for ln in range(1, HTAP_LINES + 1))
        product = hot(rng)
        t = time.perf_counter()
        with conn.transaction() as txn:
            txn.push(o_tid, order)
            txn.push(l_tid, items)
        while True:
            try:
                conn.execute_sql(f"UPDATE inventory SET qty = qty - 1 WHERE pk = {product}")
                break
            except gnitz.GnitzConflictError:
                conflicts += 1
        ms.append((time.perf_counter() - t) * 1e3)
    return {"calls": {"order": (ms, ops * (HTAP_LINES + 2))}, "conflicts": conflicts}


def _htap_reader(conn, reads):
    rels = [conn.resolve_table(name) for name in ("orders", "lineitem", "v_rev_by_nation", "v_orders_by_status")]
    torn, ms = 0, []
    for _ in range(reads):
        t = time.perf_counter()
        orders, items, _rev, by_status = conn.scan_many(rels)
        ms.append((time.perf_counter() - t) * 1e3)
        lines = {}
        for r in items:
            lines[r.l_order] = lines.get(r.l_order, 0) + r._weight
        n_orders = 0
        for r in orders:
            n_orders += r._weight
            # An order and its lines are one commit: a snapshot holds both or neither.
            torn += r.o_key >= WRITER_BASE and lines.get(r.o_key, 0) != HTAP_LINES
        # A view never leads its source at the same cut.
        torn += sum(r.cnt * r._weight for r in by_status) > n_orders
    return {"calls": {"scan_many": (ms, reads)}, "torn": torn}


@dataclass
class Htap(Scenario):
    """Transactional writers beside dashboard readers: each writer commits an
    order with its lines atomically and decrements a hot inventory row under
    optimistic concurrency; each reader takes one snapshot of two tables and
    two views over them. No snapshot may show an order without all its lines,
    or a view ahead of its table."""

    def run(self, run):
        ops, products = ops_for(run, 500, 400), 5_000
        base_orders = run.rows // 10
        run.ddl("CREATE TABLE customer (c_key BIGINT NOT NULL PRIMARY KEY, c_nation BIGINT NOT NULL)",
                "CREATE TABLE orders (o_key BIGINT NOT NULL PRIMARY KEY, o_cust BIGINT NOT NULL, "
                "o_status TEXT NOT NULL, o_price BIGINT NOT NULL)",
                "CREATE TABLE lineitem (l_order BIGINT NOT NULL, l_line BIGINT NOT NULL, l_qty BIGINT NOT NULL, "
                "PRIMARY KEY (l_order, l_line))",
                "CREATE TABLE inventory (pk BIGINT NOT NULL PRIMARY KEY, qty BIGINT NOT NULL)",
                "CREATE VIEW v_customer_orders AS SELECT customer.c_nation AS c_nation, orders.o_price AS o_price "
                "FROM customer JOIN orders ON orders.o_cust = customer.c_key",
                "CREATE VIEW v_rev_by_nation AS SELECT c_nation, SUM(o_price) AS rev FROM v_customer_orders "
                "GROUP BY c_nation",
                "CREATE VIEW v_orders_by_status AS SELECT o_status, COUNT(*) AS cnt FROM orders GROUP BY o_status")
        rng = random.Random(7)
        load = (run.ops("customer", (dict(c_key=k, c_nation=k % 25) for k in range(1, HTAP_CUSTOMERS + 1)), LOAD_BATCH)
                + run.ops("orders", (dict(o_key=k, o_cust=k % HTAP_CUSTOMERS + 1, o_status="OFP"[k % 3],
                                          o_price=rng.randrange(100, 50_000)) for k in range(1, base_orders + 1)),
                          LOAD_BATCH)
                + run.ops("lineitem", (dict(l_order=k, l_line=1, l_qty=k % 20 + 1) for k in range(1, base_orders + 1)),
                          LOAD_BATCH)
                + run.ops("inventory", (dict(pk=p, qty=1_000_000) for p in range(1, products + 1)), LOAD_BATCH))
        with run.phase("load") as ph:
            ph.replay(load)
        with run.phase("mixed") as ph:
            got = ph.clients([(_htap_writer, w, ops, products) for w in range(2)]
                             + [(_htap_reader, max(ops // 20, 10))] * 2)
            ph.extra.update(conflicts=got["conflicts"], torn_snapshots=got["torn"])
        assert got["torn"] == 0, f"{got['torn']} snapshots were torn or showed a view ahead of its table"


READER_GROUP_BASE, READER_PK_BASE = 10_000_000, 100_000_000


def _background_writer(conn, widx, pushes, groups):
    tid, schema = conn.resolve_table("o")
    rng = random.Random(widx)
    rows = [gnitz.ZSetBatch(schema).extend([dict(pk=READER_PK_BASE * (100 + widx) + i, cust=rng.randrange(groups) + 1, amt=1)])
            for i in range(pushes)]
    return {"calls": {"background_push": (time_calls(lambda i: conn.push(tid, rows[i]), pushes), pushes)}}


def _own_writes_reader(conn, ridx, ops, view):
    tid, schema = conn.resolve_table("o")
    vid, v_schema = conn.resolve_table(view)
    group, first = READER_GROUP_BASE + ridx, READER_PK_BASE * (ridx + 1)
    rows = [gnitz.ZSetBatch(schema).extend([dict(pk=first + i, cust=group, amt=1)]) for i in range(ops)]
    stale, push_ms, seek_ms = 0, [], []
    for i in range(ops):
        t = time.perf_counter()
        conn.push(tid, rows[i])
        mid = time.perf_counter()
        got = [r for r in conn.seek(vid, v_schema, group if view == "v_rev" else first + i) if r._weight > 0]
        end = time.perf_counter()
        push_ms.append((mid - t) * 1e3)
        seek_ms.append((end - mid) * 1e3)
        stale += len(got) != 1 or (got[0].s != i + 1 if view == "v_rev" else got[0].pk != first + i)
    return {"calls": {"push": (push_ms, ops), "seek": (seek_ms, 0)}, "stale": stale}


@dataclass
class Serving(Scenario):
    """Point reads of a view under write load: three clients each push a row to
    a key of their own and seek it back at once, while a fourth streams writes
    to everyone else's. Every seek must show the write just before it."""

    def run(self, run):
        groups, ops = run.rows // 2, ops_for(run, 200, 1000)
        run.ddl("CREATE TABLE o (pk BIGINT NOT NULL PRIMARY KEY, cust BIGINT UNSIGNED NOT NULL, amt BIGINT NOT NULL)",
                "CREATE VIEW v_rev AS SELECT cust, SUM(amt) AS s FROM o GROUP BY cust",
                "CREATE VIEW v_passthru AS SELECT * FROM o WHERE amt >= 0")
        load = run.ops("o", (dict(pk=k, cust=k, amt=k % 100 + 1) for k in range(1, groups + 1)), LOAD_BATCH)
        with run.phase("load") as ph:
            ph.replay(load)
        for i, (name, view) in enumerate((("grouped", "v_rev"), ("passthrough", "v_passthru"))):
            with run.phase(name) as ph:
                got = ph.clients([(_own_writes_reader, 10 * i + r, ops, view) for r in range(3)]
                                 + [(_background_writer, i, 3 * ops, groups)])
                ph.extra["stale_seeks"] = got["stale"]
            assert got["stale"] == 0, f"{got['stale']} seeks of {view} missed the write before them"


# ---------------------------------------------------------------------------
# Placement
# ---------------------------------------------------------------------------

@dataclass
class Replicated(Scenario):
    """A replicated table against its partitioned twin, phase for phase. A
    replicated table keeps a full copy on every worker: a join against it
    needs no exchange, and every write, scan and backfill of it costs a factor
    of the worker count."""

    def run(self, run):
        n = run.rows
        for p, opt in (("rep", " WITH (replicated = true)"), ("part", "")):
            run.ddl(f"CREATE TABLE dim_{p} (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL){opt}")
        run.ddl("CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, val BIGINT NOT NULL)")
        c = run.conn

        def dim(lo, hi):
            return (dict(pk=k + 1, val=k % 1000) for k in range(lo, hi))
        fact = run.ops("fact", (dict(pk=k + 1, fk=k % 1000 + 1, val=k) for k in range(n)), LOAD_BATCH)
        with run.phase("load_fact") as ph:
            ph.replay(fact)
        for p in ("rep", "part"):
            ops = run.ops(f"dim_{p}", dim(0, n), INGEST_BATCH)
            with run.phase(f"write_{p}") as ph:
                ph.replay(ops)
            t = run.tables[f"dim_{p}"]
            with run.phase(f"scan_{p}") as ph:
                for _ in range(5):
                    ph.timed("scan", c.scan, t.tid, t.schema, rows=n)
        for p in ("rep", "part"):
            with run.phase(f"backfill_{p}") as ph:
                run.ddl_in(ph, f"CREATE VIEW v_all_{p} AS SELECT pk, val FROM dim_{p} UNION ALL SELECT pk, val FROM fact",
                           rows=2 * n)
        for p in ("rep", "part"):
            ops = run.ops(f"dim_{p}", dim(n, n + n // 4), INGEST_BATCH)
            with run.phase(f"maintain_{p}") as ph:
                ph.replay(ops)


# ---------------------------------------------------------------------------
# Views pushed out
# ---------------------------------------------------------------------------

@dataclass
class Feed(Scenario):
    """A view's deltas pushed out to clients: subscribers that each keep one
    tenant's rows of a filter view, and a mirror holding an aggregate view and
    a filtered alias as a local store its host reads without a round trip."""

    def run(self, run):
        n, tenants, subscribers = run.rows, 50, 8
        run.ddl("CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, tenant BIGINT NOT NULL, kind BIGINT NOT NULL, "
                "body TEXT NOT NULL)",
                "CREATE VIEW v_feed WITH (delta = '64 MB') AS SELECT id, tenant, kind, body FROM ev WHERE kind < 8",
                "CREATE VIEW v_tenants WITH (delta = '16 MB') AS SELECT tenant, COUNT(*) AS n FROM ev GROUP BY tenant")
        rng = random.Random(7)

        def row(k):
            return dict(id=k + 1, tenant=rng.randrange(tenants), kind=rng.randrange(10),
                        body=f"event {k} of some length: {rng.randrange(10**12):012d}")
        load = run.ops("ev", (row(k) for k in range(n)), LOAD_BATCH)
        with run.phase("load") as ph:
            ph.replay(load)

        subs = []
        with run.phase("bootstrap") as ph:
            for t in range(subscribers):
                conn = gnitz.connect(run.server.sock_path)
                vid, schema, spec = conn.subscription(f"SELECT id, body FROM v_feed WHERE tenant = {t}")
                rows, cursor = ph.timed("bootstrap", conn.delta_bootstrap, vid, schema, spec)
                ph.rows += len(rows)
                conn.subscribe(vid, schema, cursor, spec)
                subs.append(conn)

        def delivered(synced):
            return sum(len(p.rows) for p in synced.pushed if p.rows is not None)
        at = n
        fresh = [row(k) for k in range(at, at + n // 4)]
        at += n // 4
        ops = run.ops("ev", fresh, INGEST_BATCH)
        got = 0
        with run.phase("fanout") as ph:
            for op in ops:
                ph.push(op)
                for conn in subs:
                    got += delivered(ph.timed("sync", conn.sync))
            ph.extra["delivered_rows"] = got
        for conn in subs:
            conn.close()
        want = sum(r["tenant"] < subscribers and r["kind"] < 8 for r in fresh)
        assert got == want, f"the subscribers were sent {got} rows of the {want} their filters keep"

        mirror = gnitz.connect(run.server.sock_path)
        store = tempfile.mkdtemp(dir=run.tmp, prefix="mirror_")
        mirror.mirror_at(store)
        with run.phase("mirror_seed") as ph:
            ph.timed("mirror", mirror.mirror_view, "v_tenants", rows=tenants)
            ph.timed("mirror", mirror.mirror_subscription, "t7", "SELECT id, kind FROM v_feed WHERE tenant = 7",
                     rows=n // tenants)
        ops = run.ops("ev", (row(k) for k in range(at, at + n // 4)), INGEST_BATCH)
        with run.phase("mirror_follow") as ph:
            for op in ops:
                ph.push(op)
                ph.timed("sync", mirror.sync)
                ph.timed("local_read", mirror.execute_sql, "SELECT tenant, n FROM v_tenants")
                ph.timed("local_read", mirror.execute_sql, "SELECT id, kind FROM _local.t7 LIMIT 100")
        local = {(r.tenant, r.n) for r in mirror.execute_sql("SELECT tenant, n FROM v_tenants")[0]["rows"]}
        upstream = {(r.tenant, r.n) for r in run.conn.scan(*run.conn.resolve_table("v_tenants")) if r._weight > 0}
        assert local == upstream, "the mirror's copy is the view"
        mirror.close_mirror()
        mirror.close()


async def _write_followed(target, ops, rows, store, beside):
    """`ops` pushed on one connection while a held sync follows the mirrored
    `v_kinds`, on that connection or with `beside` on one of its own: each
    push's milliseconds and the syncs answered, once the copy counts `rows`."""
    async with aio.connect(target) as conn:
        follower = aio.connect(target) if beside else conn
        await follower.mirror_at(store)
        vid = (await follower.mirror_view("v_kinds")).view_id
        _, copy = await follower.resolve_table("v_kinds")
        syncs = 0

        async def follow():
            nonlocal syncs
            while True:
                await follower.sync(30.0)
                syncs += 1
        following = asyncio.ensure_future(follow())
        ms = await time_pushes(conn, ops)

        async def caught_up():
            while sum(r.n for r in await follower.scan(vid, copy)) < rows:
                await asyncio.sleep(0.002)
        await asyncio.wait_for(caught_up(), 60)
        following.cancel()
        await asyncio.gather(following, return_exceptions=True)
        await follower.close_mirror()
        if beside:
            await follower.aclose()
    return ms, syncs


@dataclass
class Follow(Scenario):
    """A writer that follows a view: awaited one-row pushes on a connection
    that keeps a mirror of an aggregate view current with a held sync, and the
    same pushes with the follower on a connection of its own."""

    def run(self, run):
        n = max(run.rows // 100, 1000)
        run.ddl("CREATE TABLE w (pk BIGINT NOT NULL PRIMARY KEY, kind BIGINT NOT NULL)",
                "CREATE VIEW v_kinds WITH (delta = '16 MB') AS SELECT kind, COUNT(*) AS n FROM w GROUP BY kind")
        lo = 0
        for name, beside in (("on_the_writing_connection", False), ("on_its_own_connection", True)):
            store = tempfile.mkdtemp(dir=run.tmp, prefix="mirror_")
            ops = run.ops("w", (dict(pk=k, kind=k % 16) for k in range(lo, lo + n)), 1)
            with run.phase(name) as ph:
                ms, syncs = asyncio.run(_write_followed(run.server.sock_path, ops, lo + n, store, beside))
                ph.add("push", ms, rows=n)
                ph.extra["syncs_answered"] = syncs
                ph.extra["unpaced"] = True      # a follower's ticks keep to the clock
            lo += n


# ---------------------------------------------------------------------------
# TPC-H as views
# ---------------------------------------------------------------------------

def tpch(n, rng):
    """`n` line items over three tenths as many orders and a tenth of those in
    customers; hot customers and hot orders take most of the rows."""
    orders, customers = max(n * 3 // 10, 1), max(n * 3 // 100, 1)
    hot_customer, hot_order = Zipf(customers), Zipf(orders)
    dims = [("region", (dict(r_key=i, r_name=name) for i, name in enumerate(
                ["AMERICA", "ASIA", "EUROPE", "AFRICA", "MIDDLE EAST"]))),
            ("nation", (dict(n_key=i, n_name=name, n_region=i % 5) for i, name in enumerate(NATIONS)))]
    return dims, {
        "customer": Fact(lambda k: dict(c_key=k + 1, c_name=f"Customer#{k + 1:09d}", c_nation=rng.randrange(25),
                                        c_acctbal=rng.randrange(-99_999, 1_000_000)), share=3 / 100),
        "orders": Fact(lambda k: dict(o_key=k + 1, o_cust=hot_customer.at(scatter(k, 53) / 2**53),
                                      o_status=rng.choice("FOP"), o_date=rng.randrange(1, 2001),
                                      o_price=rng.randrange(1000, 500_001)), share=3 / 10),
        "lineitem": Fact(lambda k: dict(
            l_order=hot_order.at(scatter(k, 53) / 2**53), l_line=k + 1, l_qty=rng.randrange(1, 51),
            l_price=rng.randrange(100, 100_001), l_disc=rng.randrange(11) / 100, l_tax=rng.randrange(9) / 100,
            l_ship=rng.randrange(1, 1001), l_rflag=rng.choice("ANR"), l_lstatus=rng.choice("OF"),
            l_mode=rng.choice(SHIPMODES))),
    }


TPCH = Shape(
    "tpch", "analytics",
    "TPC-H's schema under five of its queries as views — pricing summary, shipping priority, revenue forecast, "
    "shipping modes, order priority — with Zipfian customers and orders",
    ("l0", "compacted"),
    ["CREATE TABLE region (r_key BIGINT NOT NULL PRIMARY KEY, r_name TEXT NOT NULL)",
     "CREATE TABLE nation (n_key BIGINT NOT NULL PRIMARY KEY, n_name TEXT NOT NULL, n_region BIGINT NOT NULL)",
     "CREATE TABLE customer (c_key BIGINT NOT NULL PRIMARY KEY, c_name TEXT NOT NULL, c_nation BIGINT NOT NULL, "
     "c_acctbal BIGINT NOT NULL)",
     "CREATE TABLE orders (o_key BIGINT NOT NULL PRIMARY KEY, o_cust BIGINT NOT NULL, o_status TEXT NOT NULL, "
     "o_date BIGINT NOT NULL, o_price BIGINT NOT NULL)",
     "CREATE TABLE lineitem (l_order BIGINT NOT NULL, l_line BIGINT NOT NULL, l_qty BIGINT NOT NULL, "
     "l_price BIGINT NOT NULL, l_disc DOUBLE NOT NULL, l_tax DOUBLE NOT NULL, l_ship BIGINT NOT NULL, "
     "l_rflag TEXT NOT NULL, l_lstatus TEXT NOT NULL, l_mode TEXT NOT NULL, PRIMARY KEY (l_order, l_line))",
     "CREATE VIEW v_q1_base AS SELECT l_rflag, l_lstatus, l_qty, l_price, l_price*(1-l_disc) AS disc_price, "
     "l_price*(1-l_disc)*(1+l_tax) AS charge, l_disc FROM lineitem WHERE l_ship <= 900",
     "CREATE VIEW v_q1 AS SELECT l_rflag, l_lstatus, SUM(l_qty) AS sum_qty, SUM(l_price) AS sum_base, "
     "SUM(disc_price) AS sum_disc, SUM(charge) AS sum_charge, AVG(l_qty) AS avg_qty, AVG(l_disc) AS avg_disc, "
     "COUNT(*) AS cnt FROM v_q1_base GROUP BY l_rflag, l_lstatus",
     "CREATE VIEW v_q3_join AS WITH co AS (SELECT customer.c_key AS c_key, orders.o_key AS o_key, "
     "orders.o_date AS o_date FROM customer JOIN orders ON orders.o_cust = customer.c_key "
     "WHERE orders.o_status = 'O') SELECT co.o_key AS o_key, co.o_date AS o_date, lineitem.l_price AS l_price, "
     "lineitem.l_disc AS l_disc FROM co JOIN lineitem ON lineitem.l_order = co.o_key",
     "CREATE VIEW v_q3_rev AS SELECT o_key, o_date, l_price*(1-l_disc) AS rev FROM v_q3_join",
     "CREATE VIEW v_q3 AS SELECT o_key, o_date, SUM(rev) AS revenue FROM v_q3_rev GROUP BY o_key, o_date",
     "CREATE VIEW v_q6_base AS SELECT l_price*l_disc AS rev FROM lineitem "
     "WHERE l_ship >= 100 AND l_ship < 400 AND l_disc BETWEEN 0.05 AND 0.07 AND l_qty < 24",
     "CREATE VIEW v_q6 AS SELECT SUM(rev) AS revenue FROM v_q6_base",
     "CREATE VIEW v_q12_join AS SELECT lineitem.l_mode AS l_mode, orders.o_status AS o_status "
     "FROM orders JOIN lineitem ON lineitem.l_order = orders.o_key WHERE lineitem.l_mode IN ('MAIL','SHIP')",
     "CREATE VIEW v_q12_flags AS SELECT l_mode, CASE WHEN o_status='O' THEN 1 ELSE 0 END AS hi, "
     "CASE WHEN o_status='O' THEN 0 ELSE 1 END AS lo FROM v_q12_join",
     "CREATE VIEW v_q12 AS SELECT l_mode, SUM(hi) AS high_line, SUM(lo) AS low_line FROM v_q12_flags GROUP BY l_mode",
     "CREATE VIEW v_q4_orders AS SELECT orders.o_key AS o_key, orders.o_status AS o_status FROM orders "
     "WHERE EXISTS (SELECT 1 FROM lineitem WHERE lineitem.l_order = orders.o_key)",
     "CREATE VIEW v_q4 AS SELECT o_status, COUNT(*) AS order_count FROM v_q4_orders GROUP BY o_status"],
    tpch, read="v_q1", unverified={v: FLOAT for v in ("v_q1", "v_q3", "v_q6")})


SCENARIOS = [
    TPCH,
    Statements("statements", "sql", Statements.__doc__.split(":")[0]),
    Reads("reads", "sql", "the binary read verbs against views: scan, seek by a group key and by a base key, "
                          "an index seek, an ad-hoc aggregate, and one snapshot of two and of eight views"),
    Transactions("transactions", "concurrency", "atomic multi-table commits from four writers, a transaction of "
                                                "compounding UPDATEs against its autocommit twin, and the bare round trips"),
    Contention("contention", "concurrency", "one, two and four writers incrementing the same 1, 8 and 64 rows "
                                            "under optimistic concurrency, with no update lost"),
    Htap("htap", "concurrency", "two transactional writers beside two readers that snapshot two tables and two "
                                "views at one cut, with no snapshot torn"),
    Serving("serving", "concurrency", "three clients that each seek a view for their own last write, beside a "
                                      "writer to everyone else's keys, with no seek stale"),
    Replicated("replicated", "placement", "a replicated table against its partitioned twin: write, scan, "
                                          "backfill and maintenance of a UNION ALL view over each"),
    Feed("feed", "distribution", "eight subscribers that each keep one tenant's rows of a filter view, and a "
                                 "mirror that follows an aggregate view and a filtered alias as a local store"),
    Follow("follow", "distribution", "awaited one-row pushes on a connection that follows a mirrored view with a "
                                     "held sync, and the same with the follower on a connection of its own"),
]
