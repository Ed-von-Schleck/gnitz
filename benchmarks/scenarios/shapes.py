"""Scenarios that vary the shape of the data: column types, value ranges, key
layout, NULL density, mutation rate, and what a relation's `WITH (...)` asks
of its store."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal

from .base import BYTES, WORDS, Fact, Shape, scatter


def strings_repeated(n, rng):
    tenants = [f"tenant-{i:03d}" for i in range(50)]                    # inline: 12 bytes or fewer
    statuses = ["ok", "ok", "ok", "client_error", "server_error_upstream_timeout"]
    urls = [f"https://app.example.com/api/v2/resources/{rng.randrange(10**9):09d}/items/{i:05d}"
            for i in range(2_000)]
    agents = [f"Mozilla/5.0 (X11; Linux x86_64; rv:{100 + i}.0) Gecko/20100101 Firefox/{100 + i}.0 "
              f"build-{i:04d}" for i in range(30)]
    return [], {"events": Fact(lambda k: dict(
        id=k + 1, tenant=rng.choice(tenants), url=rng.choice(urls), agent=rng.choice(agents),
        status=rng.choice(statuses), amount=rng.randrange(1000)))}


def strings_unique(n, rng):
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
    return [], {"docs": Fact(row)}


def ints(n, rng):
    return [], {"metrics": Fact(lambda k: dict(
        id=k + 1, ts=1_700_000_000_000_000 + k * 1000 + rng.randrange(1000),
        device=rng.randrange(10_000), kind=rng.randrange(8), value=rng.randrange(1000),
        big=rng.getrandbits(60), ratio=rng.randrange(100) / 100, flag=rng.randrange(2)))}


def compound_pk(n, rng):
    """Rows come in runs that share a tenant and a device."""
    run_len = max(n // (20 * 500), 1)

    def row(k):
        run = k // run_len
        base = 1_700_000_000_000 + scatter(run, 20) * 1_000_000
        return dict(tenant=scatter(run, 32) % 20, device=scatter(run, 48) % 500,
                    ts=base + (k % run_len) * 60_000, v=rng.randrange(100_000))
    return [], {"readings": Fact(row)}


def sparse_nulls(n, rng):
    def row(k):
        r = dict(id=k + 1)
        for i in range(8):
            r[f"c{i}"] = rng.randrange(10**6) if rng.random() < 0.1 else None
        for i in range(4):
            r[f"s{i}"] = f"note {rng.randrange(10**7)} for attribute {i}" if rng.random() < 0.1 else None
        return r
    return [], {"wide": Fact(row)}


def uuid_pk(n, rng):
    users = [uuid.UUID(int=rng.getrandbits(128)) for _ in range(max(n // 50, 1))]
    return [], {"sessions": Fact(lambda k: dict(
        sid=uuid.UUID(int=scatter(k, 128)), user_ref=rng.choice(users),
        started=1_700_000_000 + k * 3 + rng.randrange(3), hits=rng.randrange(200)))}


def money(n, rng):
    accounts = [uuid.UUID(int=rng.getrandbits(128)) for _ in range(max(n // 100, 1))]
    currencies = ["EUR", "EUR", "EUR", "USD", "GBP", "CHF"]
    return [], {"ledger": Fact(lambda k: dict(
        id=k + 1, account=rng.choice(accounts),
        booked=datetime(2024, 1, 1) + timedelta(seconds=k * 7 + rng.randrange(7)),
        value_date=date(2024, 1, 1) + timedelta(days=k * 365 // n),
        amount=Decimal(rng.randrange(-500_000, 500_000)).scaleb(-2),
        fee=Decimal(rng.choice([0, 0, 0, 25, 150])).scaleb(-2), currency=rng.choice(currencies)))}


SMALL_TABLES, SMALL_TABLE_ROWS = 100, 200


def small_tables(n, rng):
    names = [f"item-{i:03d}" for i in range(20)]
    return [], {f"t_{t:03d}": Fact(lambda k: dict(id=k + 1, name=rng.choice(names), qty=rng.randrange(100),
                                                  ts=1_700_000_000 + k))
                for t in range(SMALL_TABLES)}


def accounts(n, rng):
    owners = [f"owner-{i:05d}" for i in range(5_000)]
    return [], {"accounts": Fact(lambda k: dict(
        id=k + 1, owner=rng.choice(owners), balance=rng.randrange(1000),
        note=f"adjusted in round {rng.randrange(10**6):06d} by the nightly reconciliation job"))}


CHURN_DDL = [
    "CREATE TABLE accounts (id BIGINT NOT NULL PRIMARY KEY, owner TEXT NOT NULL, balance BIGINT NOT NULL, "
    "note TEXT NOT NULL)",
    "CREATE VIEW v_rich AS SELECT id, owner, balance FROM accounts WHERE balance > 500",
    "CREATE VIEW v_owner AS SELECT owner, COUNT(*) AS n, SUM(balance) AS total FROM accounts GROUP BY owner",
]


def clicks(n, rng):
    users = max(n // 40, 1)
    countries = ["de", "fr", "us", "gb", "jp", "br", "in", "pl"]
    dims = [("users", (dict(id=u, name=f"user-{u:07d}", country=rng.choice(countries)) for u in range(users)))]
    return dims, {"clicks": Fact(lambda k: dict(
        id=k + 1, user_id=rng.randrange(users), page=rng.randrange(200), ms=rng.randrange(60_000)))}


def click_twins(n, rng):
    users = max(n // 40, 1)

    def row(k):
        return dict(id=k + 1, user_id=scatter(k, 32) % users, page=scatter(k, 40) % 200, ms=scatter(k, 48) % 60_000)
    return [], {"clicks": Fact(row), "visits": Fact(row)}


def messages(n, rng):
    return [], {"messages": Fact(lambda k: dict(
        id=k + 1, kind=rng.randrange(4),
        body=f"message {rng.randrange(10**9):09d} " + " ".join(rng.choice(WORDS) for _ in range(6))))}


SCENARIOS = [
    Shape(
        "strings_repeated", "shape",
        "four TEXT columns drawn from small value sets, a filter view copying them and a grouped view "
        "keyed on two",
        BYTES,
        ["CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, tenant TEXT NOT NULL, url TEXT NOT NULL, "
         "agent TEXT NOT NULL, status TEXT NOT NULL, amount BIGINT NOT NULL)",
         "CREATE VIEW v_errors AS SELECT id, tenant, url, agent, status FROM events WHERE status <> 'ok'",
         "CREATE VIEW v_by_url AS SELECT tenant, url, COUNT(*) AS n, SUM(amount) AS total "
         "FROM events GROUP BY tenant, url"],
        strings_repeated),
    Shape(
        "strings_unique", "shape",
        "TEXT columns that never repeat — random tokens, paths sharing long prefixes, word titles, "
        "short inline tags — under a filter view",
        BYTES,
        ["CREATE TABLE docs (id BIGINT NOT NULL PRIMARY KEY, kind INT NOT NULL, token TEXT NOT NULL, "
         "path TEXT NOT NULL, title TEXT NOT NULL, tag TEXT NOT NULL)",
         "CREATE VIEW v_kind0 AS SELECT id, token, path, title FROM docs WHERE kind = 0"],
        strings_unique),
    Shape(
        "ints", "shape",
        "integer and float columns of every range — a near-monotone timestamp, small domains, 60 random "
        "bits — under a filter view, linear and MIN/MAX aggregates, and a secondary index",
        BYTES,
        ["CREATE TABLE metrics (id BIGINT NOT NULL PRIMARY KEY, ts BIGINT NOT NULL, device INT NOT NULL, "
         "kind SMALLINT NOT NULL, value BIGINT NOT NULL, big BIGINT NOT NULL, ratio DOUBLE NOT NULL, "
         "flag BIGINT NOT NULL)",
         "CREATE INDEX ON metrics(device)",
         "CREATE VIEW v_kind3 AS SELECT id, ts, device, value FROM metrics WHERE kind = 3",
         "CREATE VIEW v_dev AS SELECT device, COUNT(*) AS n, SUM(value) AS total FROM metrics GROUP BY device",
         "CREATE VIEW v_ext AS SELECT device, MIN(value) AS lo, MAX(ts) AS last FROM metrics GROUP BY device"],
        ints),
    Shape(
        "compound_pk", "shape",
        "a three-column key whose leading columns repeat for long runs, under a grouped view",
        BYTES,
        ["CREATE TABLE readings (tenant INT NOT NULL, device INT NOT NULL, ts BIGINT NOT NULL, "
         "v BIGINT NOT NULL, PRIMARY KEY (tenant, device, ts))",
         "CREATE VIEW v_device AS SELECT tenant, device, COUNT(*) AS n, SUM(v) AS total "
         "FROM readings GROUP BY tenant, device"],
        compound_pk),
    Shape(
        "sparse_nulls", "shape",
        "twelve nullable columns, each NULL in nine rows of ten, under a filter view",
        BYTES,
        ["CREATE TABLE wide (id BIGINT NOT NULL PRIMARY KEY, "
         + ", ".join(f"c{i} BIGINT" for i in range(8)) + ", "
         + ", ".join(f"s{i} TEXT" for i in range(4)) + ")",
         "CREATE VIEW v_c0 AS SELECT id, c0, c1, s0 FROM wide WHERE c0 IS NOT NULL"],
        sparse_nulls),
    Shape(
        "uuid_pk", "shape",
        "a random 16-byte key and a 16-byte reference column, under a view grouped on the reference",
        BYTES,
        ["CREATE TABLE sessions (sid UUID NOT NULL PRIMARY KEY, user_ref UUID NOT NULL, started BIGINT NOT NULL, "
         "hits INT NOT NULL)",
         "CREATE VIEW v_user AS SELECT user_ref, COUNT(*) AS n, SUM(hits) AS total FROM sessions GROUP BY user_ref"],
        uuid_pk),
    Shape(
        "money", "shape",
        "the named types over a stored integer — DECIMAL amounts of a few digits, a near-monotone TIMESTAMP, "
        "a DATE — and a UUID reference drawn from a small set, under a filter view and a sum per reference",
        BYTES,
        ["CREATE TABLE ledger (id BIGINT NOT NULL PRIMARY KEY, account UUID NOT NULL, booked TIMESTAMP NOT NULL, "
         "value_date DATE NOT NULL, amount DECIMAL(18, 2) NOT NULL, fee DECIMAL(18, 2) NOT NULL, "
         "currency TEXT NOT NULL)",
         "CREATE VIEW v_large AS SELECT id, account, booked, amount FROM ledger WHERE amount > 4000",
         "CREATE VIEW v_balance AS SELECT account, COUNT(*) AS n, SUM(amount) AS balance, SUM(fee) AS fees "
         "FROM ledger GROUP BY account"],
        money),
    Shape(
        "small_tables", "shape",
        f"{SMALL_TABLES} tables of {SMALL_TABLE_ROWS} rows, each under a grouped view: what a relation costs "
        "before its first row",
        BYTES,
        [stmt for t in range(SMALL_TABLES) for stmt in (
            f"CREATE TABLE t_{t:03d} (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, qty BIGINT NOT NULL, "
            "ts BIGINT NOT NULL)",
            f"CREATE VIEW v_{t:03d} AS SELECT name, COUNT(*) AS n, SUM(qty) AS total FROM t_{t:03d} GROUP BY name")],
        small_tables, fixed_rows=SMALL_TABLE_ROWS),
    Shape(
        "churn", "mutation",
        "every row rewritten one and a half times on average and a fifth deleted, under a filter view "
        "and a grouped one",
        BYTES, CHURN_DDL, accounts, rewrites=1.5, deletes=0.2),
    Shape(
        "churn_long", "mutation",
        "the churn scenario with every row rewritten six times on average",
        BYTES, CHURN_DDL, accounts, rewrites=6.0, deletes=0.2),
    Shape(
        "stream", "policy",
        "a stream, which holds no row of its own, under two aggregates and a join to a table: every view "
        "is rebuilt at boot, so nothing but the table is published",
        BYTES,
        ["CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, country TEXT NOT NULL)",
         "CREATE TABLE clicks (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT NOT NULL, page INT NOT NULL, "
         "ms INT NOT NULL) WITH (stream = true)",
         "CREATE VIEW v_page AS SELECT page, COUNT(*) AS n, SUM(ms) AS total FROM clicks GROUP BY page",
         "CREATE VIEW v_slowest AS SELECT user_id, MAX(ms) AS slowest FROM clicks GROUP BY user_id",
         "CREATE VIEW v_country AS SELECT u.country, COUNT(*) AS n FROM clicks c JOIN users u "
         "ON c.user_id = u.id GROUP BY u.country"],
        clicks),
    Shape(
        "stream_twin", "policy",
        "the same rows pushed to a stream and to a table, each under the same two aggregates: the table's "
        "views are published, the stream's are not",
        BYTES,
        ["CREATE TABLE clicks (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT NOT NULL, page INT NOT NULL, "
         "ms INT NOT NULL) WITH (stream = true)",
         "CREATE TABLE visits (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT NOT NULL, page INT NOT NULL, "
         "ms INT NOT NULL)",
         *(f"CREATE VIEW v_{side}_{name} AS SELECT {body} FROM {source} GROUP BY {key}"
           for side, source in (("stream", "clicks"), ("table", "visits"))
           for name, key, body in (("page", "page", "page, COUNT(*) AS n, SUM(ms) AS total"),
                                   ("slowest", "user_id", "user_id, MAX(ms) AS slowest")))],
        click_twins, rewrites=0, deletes=0),
    Shape(
        "bounded", "policy",
        "a filter view held under a capacity a sixth of what it would take, beside its unbounded twin",
        BYTES,
        ["CREATE TABLE messages (id BIGINT NOT NULL PRIMARY KEY, kind INT NOT NULL, body TEXT NOT NULL)",
         "CREATE VIEW v_bounded WITH (capacity = '1 MB') AS SELECT id, body FROM messages WHERE kind < 3",
         "CREATE VIEW v_twin AS SELECT id, body FROM messages WHERE kind < 3"],
        messages),
]
