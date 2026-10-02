"""Disk footprint of tables and the views over them, by scenario.

Each scenario loads one shape of data under the views that stress it, shuts the
server down so its final checkpoint puts every store on disk, and reports the
data directory: `gnitz-server --disk-usage` for the bytes by relation, store and
LSM level, and the same shards summed by region class — key, weight, null
bitmap, payload by encoding, string heap, PK filter, and the header, directory
and alignment padding around them.

A scenario runs under one or both regimes. `l0` leaves the server's own RAM
tier, which a store of this size never fills, so every store is one L0 shard per
worker at the checkpoint. `compacted` shrinks the tier so the same rows spill,
fold and compact into the deeper levels.

"Value bytes" is what the live rows hold: each number at its column's width,
each string at its length, a NULL at nothing. "Pushed" is the same measure over
every row a scenario sent, the rewritten and the deleted ones included.

What each store wrote — the shards it superseded on the way included — comes
from an `strace` attached to the workers; without one, or without the right to
attach it, the written columns are empty.

Run through `make bench-disk`, which builds the server and the extension first.
"""

from __future__ import annotations

import argparse
import compression.zstd as zstd
import functools
import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "crates/gnitz-py/tests"))
sys.path.insert(0, str(REPO_ROOT / "benchmarks"))

import gnitz  # noqa: E402
from _serverproc import ServerProc, disk_usage  # noqa: E402
from helpers.shards import read_shards, region_class, store_of  # noqa: E402

WIDTHS = {"U8": 1, "I8": 1, "U16": 2, "I16": 2, "U32": 4, "I32": 4, "F32": 4, "U64": 8, "I64": 8,
          "F64": 8, "U128": 16, "UUID": 16, "I128": 16, "DATE": 4, "TIMESTAMP": 8, "DECIMAL": 16}
COMPACTED_RAM_TIER = 1 << 20
WORDS = [f"{a}{b}{c}" for a in ("in", "con", "re", "de", "trans", "per", "sub", "ex")
         for b in ("struct", "form", "port", "duc", "scrib", "mit", "vers", "clud")
         for c in ("", "ion", "ed", "ing", "or", "ive", "able", "ure")]


class Loader:
    """Pushes rows through the binary API and prices what it pushed."""

    def __init__(self, conn, batch_rows):
        self.conn, self.batch_rows = conn, batch_rows
        self.pushed = 0
        self._tables = {}

    def _table(self, name):
        if name not in self._tables:
            tid, schema = self.conn.resolve_table(name)
            widths = {c.name: WIDTHS.get(gnitz.TypeCode(c.type_code).name) for c in schema.columns}
            self._tables[name] = (tid, schema, widths)
        return self._tables[name]

    def price(self, table, row):
        """The value bytes of one row."""
        widths = self._table(table)[2]
        return sum(0 if v is None else widths[k] or len(v.encode()) for k, v in row.items())

    def push(self, table, rows, weight=1):
        """Push `rows` (dicts); returns their value bytes."""
        tid, schema, _ = self._table(table)
        total, batch, held = 0, gnitz.ZSetBatch(schema), 0
        for row in rows:
            total += self.price(table, row)
            batch.append(_weight=weight, **row)
            held += 1
            if held == self.batch_rows:
                self.conn.push(tid, batch)
                batch, held = gnitz.ZSetBatch(schema), 0
        if held:
            self.conn.push(tid, batch)
        self.pushed += total
        return total


@dataclass
class Scenario:
    name: str
    doc: str
    ddl: list[str]
    load: Callable[[Loader, int, random.Random], int]   # returns the live rows' value bytes

    @property
    def relations(self) -> list[str]:
        """The tables and views `ddl` creates, in its order."""
        return [m[1] for stmt in self.ddl if (m := re.match(r"CREATE (?:TABLE|VIEW) (\w+)", stmt))]


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def load_strings_repeated(ld, rows, rng):
    tenants = [f"tenant-{i:03d}" for i in range(50)]                    # inline: 12 bytes or fewer
    statuses = ["ok", "ok", "ok", "client_error", "server_error_upstream_timeout"]
    urls = [f"https://app.example.com/api/v2/resources/{rng.randrange(10**9):09d}/items/{i:05d}"
            for i in range(2_000)]
    agents = [f"Mozilla/5.0 (X11; Linux x86_64; rv:{100 + i}.0) Gecko/20100101 Firefox/{100 + i}.0 "
              f"build-{i:04d}" for i in range(30)]
    return ld.push("events", (
        dict(id=k + 1, tenant=rng.choice(tenants), url=rng.choice(urls), agent=rng.choice(agents),
             status=rng.choice(statuses), amount=rng.randrange(1000))
        for k in range(rows)))


def load_strings_unique(ld, rows, rng):
    def row(k):
        words = " ".join(rng.choice(WORDS) for _ in range(rng.randrange(3, 9)))
        return dict(
            id=k + 1,
            kind=rng.randrange(4),
            token=str(uuid.UUID(int=rng.getrandbits(128))),
            path=f"/srv/data/projects/{rng.randrange(400):04d}/modules/{rng.randrange(60):03d}/src/file_{k:08d}.rs",
            title=words.capitalize(),
            tag=f"t{k:x}",                                               # inline
        )
    return ld.push("docs", (row(k) for k in range(rows)))


def load_ints(ld, rows, rng):
    return ld.push("metrics", (
        dict(id=k + 1, ts=1_700_000_000_000_000 + k * 1000 + rng.randrange(1000),
             device=rng.randrange(10_000), kind=rng.randrange(8), value=rng.randrange(1000),
             big=rng.getrandbits(60), ratio=rng.randrange(100) / 100, flag=rng.randrange(2))
        for k in range(rows)))


def load_compound_pk(ld, rows, rng):
    per_device = max(rows // (20 * 500), 1)

    def gen():
        k = 0
        while k < rows:
            tenant, device = rng.randrange(20), rng.randrange(500)
            base = 1_700_000_000 + rng.randrange(10**6) * 1000
            for i in range(per_device):
                if k == rows:
                    return
                k += 1
                yield dict(tenant=tenant, device=device, ts=base * 1000 + i * 60_000 + k % 60_000,
                           v=rng.randrange(100_000))
    return ld.push("readings", gen())


def load_sparse_nulls(ld, rows, rng):
    def row(k):
        r = dict(id=k + 1)
        for i in range(8):
            r[f"c{i}"] = rng.randrange(10**6) if rng.random() < 0.1 else None
        for i in range(4):
            r[f"s{i}"] = f"note {rng.randrange(10**7)} for attribute {i}" if rng.random() < 0.1 else None
        return r
    return ld.push("wide", (row(k) for k in range(rows)))


def load_join(ld, rows, rng, key_range=1.2):
    """An order's customer is drawn from `key_range` times the customers there are: of 1.2, a sixth match
    nothing."""
    customers = max(rows // 20, 1)
    regions = ["emea-north", "emea-south", "apac", "americas-east", "americas-west"]
    total = ld.push("customers", (
        dict(id=c + 1, name=f"Customer {rng.choice(WORDS)} {rng.choice(WORDS)} {c:06d} GmbH",
             region=rng.choice(regions), tier=rng.randrange(5))
        for c in range(customers)))
    statuses = ["open", "paid", "shipped", "returned_by_customer"]
    return total + ld.push("orders", (
        dict(id=k + 1, customer_id=rng.randrange(int(customers * key_range)) + 1,
             amount=rng.randrange(100_000), status=rng.choice(statuses))
        for k in range(rows)))


def load_churn(ld, rows, rng, rounds=3):
    owners = [f"owner-{i:05d}" for i in range(5_000)]
    live = {}

    def row(k):
        r = dict(id=k, owner=rng.choice(owners), balance=rng.randrange(1000),
                 note=f"adjusted in round {rng.randrange(10**6):06d} by the nightly reconciliation job")
        live[k] = r
        return r
    ld.push("accounts", (row(k + 1) for k in range(rows)))
    for _ in range(rounds):                                             # each round rewrites half the rows
        ld.push("accounts", (row(k) for k in rng.sample(range(1, rows + 1), rows // 2)))
    gone = rng.sample(range(1, rows + 1), rows // 5)
    ld.push("accounts", (live.pop(k) for k in gone), weight=-1)
    return sum(ld.price("accounts", r) for r in live.values())


def load_uuid_pk(ld, rows, rng):
    users = [uuid.UUID(int=rng.getrandbits(128)) for _ in range(max(rows // 50, 1))]
    return ld.push("sessions", (
        dict(sid=uuid.UUID(int=rng.getrandbits(128)), user_ref=rng.choice(users),
             started=1_700_000_000 + k * 3 + rng.randrange(3), hits=rng.randrange(200))
        for k in range(rows)))


def load_operators(ld, rows, rng):
    users = max(rows // 40, 1)
    total = ld.push("banned", (dict(user_id=u) for u in rng.sample(range(users), users // 10)))
    return total + ld.push("visits", (
        dict(id=k + 1, user_id=rng.randrange(users), page=rng.randrange(200), day=19_000 + k * 365 // rows,
             ms=rng.randrange(60_000))
        for k in range(rows)))


CHURN_DDL = [
    "CREATE TABLE accounts (id BIGINT NOT NULL PRIMARY KEY, owner TEXT NOT NULL, balance BIGINT NOT NULL, "
    "note TEXT NOT NULL)",
    "CREATE VIEW v_rich AS SELECT id, owner, balance FROM accounts WHERE balance > 500",
    "CREATE VIEW v_owner AS SELECT owner, COUNT(*) AS n, SUM(balance) AS total FROM accounts GROUP BY owner",
]

CUSTOMERS = ("CREATE TABLE customers (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, region TEXT NOT NULL, "
             "tier INT NOT NULL)")
ORDERS = ("CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, customer_id BIGINT NOT NULL, "
          "amount BIGINT NOT NULL, status TEXT NOT NULL)")
FANOUT_VIEWS = [
    "CREATE VIEW v_named AS SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
    "CREATE VIEW v_region AS SELECT o.id, o.status, c.region FROM orders o JOIN customers c ON o.customer_id = c.id",
    "CREATE VIEW v_tier AS SELECT o.id, o.amount, c.tier FROM orders o LEFT JOIN customers c ON o.customer_id = c.id",
    "CREATE VIEW v_region_total AS SELECT c.region, COUNT(*) AS n, SUM(o.amount) AS total FROM orders o "
    "JOIN customers c ON o.customer_id = c.id GROUP BY c.region",
    "CREATE VIEW v_cust_ext AS SELECT customer_id, MIN(amount) AS lo, MAX(amount) AS hi FROM orders "
    "GROUP BY customer_id",
    "CREATE VIEW v_cust_sum AS SELECT customer_id, COUNT(*) AS n, SUM(amount) AS total FROM orders "
    "GROUP BY customer_id",
]

SCENARIOS = [
    Scenario(
        "strings_repeated",
        "four TEXT columns drawn from small value sets, a filter view copying them and a grouped view "
        "keyed on two",
        ["CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, tenant TEXT NOT NULL, url TEXT NOT NULL, "
         "agent TEXT NOT NULL, status TEXT NOT NULL, amount BIGINT NOT NULL)",
         "CREATE VIEW v_errors AS SELECT id, tenant, url, agent, status FROM events WHERE status <> 'ok'",
         "CREATE VIEW v_by_url AS SELECT tenant, url, COUNT(*) AS n, SUM(amount) AS total "
         "FROM events GROUP BY tenant, url"],
        load_strings_repeated),
    Scenario(
        "strings_unique",
        "TEXT columns that never repeat — random tokens, paths sharing long prefixes, word titles, "
        "short inline tags — under a filter view",
        ["CREATE TABLE docs (id BIGINT NOT NULL PRIMARY KEY, kind INT NOT NULL, token TEXT NOT NULL, "
         "path TEXT NOT NULL, title TEXT NOT NULL, tag TEXT NOT NULL)",
         "CREATE VIEW v_kind0 AS SELECT id, token, path, title FROM docs WHERE kind = 0"],
        load_strings_unique),
    Scenario(
        "ints",
        "integer and float columns of every range — a near-monotone timestamp, small domains, 60 random "
        "bits — under a filter view, linear and MIN/MAX aggregates, and a secondary index",
        ["CREATE TABLE metrics (id BIGINT NOT NULL PRIMARY KEY, ts BIGINT NOT NULL, device INT NOT NULL, "
         "kind SMALLINT NOT NULL, value BIGINT NOT NULL, big BIGINT NOT NULL, ratio DOUBLE NOT NULL, "
         "flag BIGINT NOT NULL)",
         "CREATE INDEX ON metrics(device)",
         "CREATE VIEW v_kind3 AS SELECT id, ts, device, value FROM metrics WHERE kind = 3",
         "CREATE VIEW v_dev AS SELECT device, COUNT(*) AS n, SUM(value) AS total FROM metrics GROUP BY device",
         "CREATE VIEW v_ext AS SELECT device, MIN(value) AS lo, MAX(ts) AS last FROM metrics GROUP BY device"],
        load_ints),
    Scenario(
        "compound_pk",
        "a three-column key whose leading columns repeat for long runs, under a grouped view",
        ["CREATE TABLE readings (tenant INT NOT NULL, device INT NOT NULL, ts BIGINT NOT NULL, "
         "v BIGINT NOT NULL, PRIMARY KEY (tenant, device, ts))",
         "CREATE VIEW v_device AS SELECT tenant, device, COUNT(*) AS n, SUM(v) AS total "
         "FROM readings GROUP BY tenant, device"],
        load_compound_pk),
    Scenario(
        "sparse_nulls",
        "twelve nullable columns, each NULL in nine rows of ten, under a filter view",
        ["CREATE TABLE wide (id BIGINT NOT NULL PRIMARY KEY, "
         + ", ".join(f"c{i} BIGINT" for i in range(8)) + ", "
         + ", ".join(f"s{i} TEXT" for i in range(4)) + ")",
         "CREATE VIEW v_c0 AS SELECT id, c0, c1, s0 FROM wide WHERE c0 IS NOT NULL"],
        load_sparse_nulls),
    Scenario(
        "join",
        "an inner and a left join of an order table to a customer table a twentieth its size",
        [CUSTOMERS,
         ORDERS,
         "CREATE VIEW v_inner AS SELECT o.id, o.amount, o.status, c.name, c.region "
         "FROM orders o JOIN customers c ON o.customer_id = c.id",
         "CREATE VIEW v_left AS SELECT o.id, o.amount, c.name "
         "FROM orders o LEFT JOIN customers c ON o.customer_id = c.id"],
        load_join),
    Scenario(
        "fanout",
        "six views and an index that each read an order table by its customer: three joins, a join under an "
        "aggregate, a MIN/MAX and a SUM per customer",
        [CUSTOMERS, ORDERS, "CREATE INDEX ON orders(customer_id)", *FANOUT_VIEWS],
        functools.partial(load_join, key_range=1)),
    Scenario(
        "fanout_clustered",
        "the fanout scenario over an order table keyed and clustered by its customer, whose own store is "
        "then the order the views read it in",
        [CUSTOMERS,
         "CREATE TABLE orders (customer_id BIGINT NOT NULL, id BIGINT NOT NULL, amount BIGINT NOT NULL, "
         "status TEXT NOT NULL, PRIMARY KEY (customer_id, id)) CLUSTER BY customer_id",
         *FANOUT_VIEWS],
        functools.partial(load_join, key_range=1)),
    Scenario(
        "churn",
        "every row rewritten one and a half times on average and a fifth deleted, under a filter view "
        "and a grouped one",
        CHURN_DDL, load_churn),
    Scenario(
        "churn_long",
        "the churn scenario with every row rewritten six times on average",
        CHURN_DDL, functools.partial(load_churn, rounds=12)),
    Scenario(
        "uuid_pk",
        "a random 16-byte key and a 16-byte reference column, under a view grouped on the reference",
        ["CREATE TABLE sessions (sid UUID NOT NULL PRIMARY KEY, user_ref UUID NOT NULL, started BIGINT NOT NULL, "
         "hits INT NOT NULL)",
         "CREATE VIEW v_user AS SELECT user_ref, COUNT(*) AS n, SUM(hits) AS total FROM sessions GROUP BY user_ref"],
        load_uuid_pk),
    Scenario(
        "operators",
        "the operator state no other scenario holds: a DISTINCT and an EXCEPT, whose leaves are keyed on "
        "a hash of the row, and a per-group top-N",
        ["CREATE TABLE visits (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT NOT NULL, page INT NOT NULL, "
         "day INT NOT NULL, ms INT NOT NULL)",
         "CREATE TABLE banned (user_id BIGINT NOT NULL PRIMARY KEY)",
         "CREATE VIEW v_pairs AS SELECT DISTINCT user_id, page FROM visits",
         "CREATE VIEW v_clean AS SELECT user_id FROM visits EXCEPT SELECT user_id FROM banned",
         "CREATE VIEW v_latest AS SELECT id, user_id, page, day FROM visits "
         "QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY day DESC) <= 3"],
        load_operators),
]


# ---------------------------------------------------------------------------
# Running and reporting
# ---------------------------------------------------------------------------

class WriteTrace:
    """Every `pwrite64` of the traced processes, summed by the store whose shard
    it lands in."""

    _CALL = re.compile(r"^pwrite64\(\d+<([^>]*)>.*\) = (\d+)$")

    def __init__(self, pids, prefix):
        self.prefix = Path(prefix)
        self.proc = None
        if not shutil.which("strace"):
            return
        args = ["strace", "-ff", "-y", "-e", "trace=pwrite64", "-o", str(prefix)]
        self.proc = subprocess.Popen(args + [f"--attach={pid}" for pid in pids],
                                     stderr=subprocess.PIPE, text=True)
        # A row pushed before the last attach would be written unseen.
        for _ in pids:
            if "attached" not in self.proc.stderr.readline():
                self.proc.kill()
                self.proc = None
                return

    def by_store(self):
        """`{(relation, store): bytes}` once every traced process has exited, or
        None when nothing was traced."""
        if self.proc is None:
            return None
        self.proc.wait()
        written = defaultdict(int)
        for log in self.prefix.parent.glob(self.prefix.name + ".*"):
            for line in log.read_text(errors="replace").splitlines():
                m = self._CALL.match(line)
                store = m and store_of(m.group(1))
                if store:
                    written[store] += int(m.group(2))
            log.unlink()
        return written


def zstd_bytes(blobs):
    """What a general-purpose compressor leaves of `blobs`, each on its own."""
    return sum(len(zstd.compress(b, level=3)) for b in blobs if b)


def run(scenario, regime, args):
    """Load `scenario` under `regime`; returns its result record."""
    ram_tier = None if regime == "l0" else args.ram_tier_bytes or COMPACTED_RAM_TIER
    (REPO_ROOT / "tmp").mkdir(exist_ok=True)
    tmp = Path(tempfile.mkdtemp(dir=REPO_ROOT / "tmp", prefix=f"bench_disk_{scenario.name}_{regime}_"))
    data_dir = tmp / "data"
    server = ServerProc(str(data_dir), str(tmp / "gnitz.sock"))
    # The SAL is fallocated whole at boot and is not what this measures.
    server.extra_env["GNITZ_SAL_BYTES"] = os.environ.get("GNITZ_SAL_BYTES", str(256 << 20))
    if ram_tier is not None:
        server.extra_env["GNITZ_RAM_TIER_BYTES"] = str(ram_tier)
    server.extra_env.update(kv.split("=", 1) for kv in args.env)
    server.start(workers=args.workers, timeout=20.0)
    try:
        with gnitz.connect(server.sock_path) as conn:
            # Views first: a view maintains only the deltas pushed after it exists.
            for stmt in scenario.ddl:
                conn.execute_sql(stmt)
            trace = WriteTrace(list(server.worker_pids().values()), tmp / "writes")
            loader = Loader(conn, args.batch_rows)
            value_bytes = scenario.load(loader, args.rows, random.Random(7))
            names = {}
            for name in scenario.relations:
                tid, schema = conn.resolve_table(name)
                names[tid] = (name, len(conn.scan(tid, schema)))
        # Its final checkpoint puts every store on disk.
        server.stop_graceful(timeout=600)
        written = trace.by_store()
    finally:
        server.stop()

    report, stores = disk_usage(data_dir)
    system = {s["line"] for s in stores if s["relation"] < gnitz.FIRST_USER_TABLE_ID}
    shards = read_shards(data_dir, gnitz.FIRST_USER_TABLE_ID)
    shard_bytes = sum(s.size for s in shards)
    assert shard_bytes == sum(s["bytes"] for s in stores if s["line"] not in system), \
        "the shards read here are the ones the server reports"

    # (relation, store) -> class -> bytes, and what zstd leaves of each store.
    by_store = defaultdict(lambda: defaultdict(int))
    blobs = defaultdict(list)
    for s in shards:
        key = (s.relation, s.store)
        by_store[key]["overhead"] += s.overhead
        by_store[key]["rows"] += s.rows
        by_store[key]["retractions"] += s.retractions
        for r in s.regions:
            by_store[key][region_class(r)] += len(r.data)
            blobs[key].append(r.data)
    store_records = []
    for (rel, store), classes in sorted(by_store.items()):
        rows, retractions = classes.pop("rows"), classes.pop("retractions")
        store_records.append({
            "relation": rel, "name": names.get(rel, (f"relation {rel}", 0))[0], "store": store, "rows": rows,
            "retractions": retractions, "written": None if written is None else written[(rel, store)],
            "bytes": sum(classes.values()), "zstd3": zstd_bytes(blobs[(rel, store)]),
            "classes": dict(sorted(classes.items())),
        })
    identical = sum(int(line.split()[4]) for line in report.splitlines() if line.startswith("identical shards:"))
    record = {
        "scenario": scenario.name, "regime": regime, "rows": args.rows, "workers": args.workers,
        "ram_tier_bytes": ram_tier, "value_bytes": value_bytes, "pushed_bytes": loader.pushed,
        "shard_bytes": shard_bytes, "identical_bytes": identical,
        "written_bytes": None if written is None else sum(
            n for (rel, _), n in written.items() if rel >= gnitz.FIRST_USER_TABLE_ID),
        "relations": {name: live for name, live in names.values()},
        "stores": store_records,
        "report": "\n".join(line for line in report.splitlines() if line not in system),
    }
    if args.keep:
        # The shards are what a kept directory is read for; the SAL is its fallocated size.
        (data_dir / "wal.sal").unlink(missing_ok=True)
        (tmp / "result.json").write_text(json.dumps(record, indent=1))
        record["kept"] = str(tmp)
    else:
        shutil.rmtree(tmp, ignore_errors=True)
    return record


def print_record(rec):
    print(f"\n=== {rec['scenario']} / {rec['regime']}: {rec['rows']} rows of {rec['value_bytes']} value bytes, "
          f"{rec['workers']} workers, RAM tier {rec['ram_tier_bytes'] or 'default'}")
    for name, live in rec["relations"].items():
        print(f"  {name}: {live} rows")
    print(rec["report"])
    classes = sorted({c for s in rec["stores"] for c in s["classes"]})
    # `retract` is the rows of negative weight; `live` the rows a scan of the relation
    # returns; `written` every shard byte the store wrote to hold `bytes`.
    head = (f"{'store':<28} {'rows':>9} {'retract':>8} {'live':>8} {'bytes':>11} {'B/row':>6} {'zstd-3':>6} "
            f"{'written':>11}  " + " ".join(f"{c:>16}" for c in classes))
    print(head)
    for s in rec["stores"]:
        label = f"{s['name']} {s['store']}"
        live = rec["relations"].get(s["name"], "") if s["store"] == "rows" else ""
        cells = " ".join(f"{s['classes'].get(c, 0):>16}" for c in classes)
        print(f"{label:<28} {s['rows']:>9} {s['retractions']:>8} {live:>8} {s['bytes']:>11} "
              f"{s['bytes'] / max(s['rows'], 1):>6.1f} {s['zstd3'] / max(s['bytes'], 1):>6.2f} "
              f"{'' if s['written'] is None else s['written']:>11}  {cells}")
    print(f"shard bytes per value byte: {rec['shard_bytes'] / rec['value_bytes']:.2f}"
          + ("" if rec["written_bytes"] is None else
             f"; the workers wrote {rec['written_bytes'] / rec['pushed_bytes']:.2f} bytes per value byte pushed")
          + (f"; {rec['identical_bytes']} bytes are a second copy of an identical shard" if rec["identical_bytes"] else "")
          + (f"; kept: {rec['kept']}" if "kept" in rec else ""))


def print_summary(records):
    print(f"\n{'scenario':<18} {'regime':<10} {'value bytes':>12} {'shard bytes':>12} {'per value':>9} "
          f"{'base':>12} {'views':>12} {'traces+idx':>12} {'zstd-3':>6} {'dead rows':>9} {'pushed':>12} "
          f"{'written':>12} {'per pushed':>10}")
    for rec in records:
        # Every scenario names its views `v_…`; a relation it did not name is one
        # the planner put under a view.
        tables = [s for s in rec["stores"] if s["store"] == "rows"]
        views = sum(s["bytes"] for s in tables if s["name"].startswith(("v_", "relation ")))
        base = sum(s["bytes"] for s in tables) - views
        other = sum(s["bytes"] for s in rec["stores"] if s["store"] != "rows")
        z = sum(s["zstd3"] for s in rec["stores"]) / max(rec["shard_bytes"], 1)
        # A retraction and the row it cancels are both dead.
        dead = 2 * sum(s["retractions"] for s in rec["stores"]) / max(sum(s["rows"] for s in rec["stores"]), 1)
        print(f"{rec['scenario']:<18} {rec['regime']:<10} {rec['value_bytes']:>12} {rec['shard_bytes']:>12} "
              f"{rec['shard_bytes'] / rec['value_bytes']:>9.2f} {base:>12} {views:>12} {other:>12} {z:>6.2f} "
              f"{dead:>8.0%} {rec['pushed_bytes']:>12} "
              + ("" if rec["written_bytes"] is None else
                 f"{rec['written_bytes']:>12} {rec['written_bytes'] / rec['pushed_bytes']:>10.2f}"))


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--rows", type=int, default=400_000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch-rows", type=int, default=20_000)
    ap.add_argument("--scenario", default="", help="comma-separated scenario names (default: all)")
    ap.add_argument("--regime", default="both", choices=["l0", "compacted", "both"])
    ap.add_argument("--ram-tier-bytes", type=int, default=None,
                    help=f"GNITZ_RAM_TIER_BYTES of the compacted regime (default: {COMPACTED_RAM_TIER})")
    ap.add_argument("--env", action="append", default=[], metavar="NAME=VALUE",
                    help="an environment variable of the server; repeatable")
    ap.add_argument("--tag", default="", help="a label in the results file's name")
    ap.add_argument("--keep", action="store_true", help="keep each data directory and print its path")
    ap.add_argument("--list", action="store_true", help="list the scenarios and exit")
    args = ap.parse_args()
    if args.list:
        for s in SCENARIOS:
            print(f"{s.name:<18} {s.doc}")
        return

    wanted = [n for n in args.scenario.split(",") if n]
    unknown = set(wanted) - {s.name for s in SCENARIOS}
    if unknown:
        ap.error(f"unknown scenario: {', '.join(sorted(unknown))}")
    regimes = ["l0", "compacted"] if args.regime == "both" else [args.regime]
    records = []
    for scenario in SCENARIOS:
        if wanted and scenario.name not in wanted:
            continue
        for regime in regimes:
            rec = run(scenario, regime, args)
            print_record(rec)
            records.append(rec)
    print_summary(records)
    out = REPO_ROOT / "benchmarks/results/disk"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{time.strftime('%Y%m%d-%H%M%S')}{'-' + args.tag if args.tag else ''}.json"
    path.write_text(json.dumps(records, indent=1))
    print(f"\nresults: {path}")


if __name__ == "__main__":
    main()
