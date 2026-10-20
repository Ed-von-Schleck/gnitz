"""Compressed stores: `CREATE TABLE … WITH (compress = true)` and the same on a
view.

The claim under test is that compression is invisible to every read. Each test
runs a compressed relation beside a plain twin over the same rows and requires
the two to agree, weights and all, through a scan, a seek, the ad-hoc `SELECT`
path — a string predicate, a string ORDER BY, a GROUP BY and MIN/MAX over the
string — and views over it, a join's among them, across a crash and a graceful
restart, while the
compressed relation's shards hold their heaps coded and the twin's do not.

The stores reach the disk regime on a few thousand rows through `SWEEP_ENV`.

A stream refuses `WITH (compress = true)`: `admissibility/test_stream_rejections.py`.
"""

import gnitz
from _read import bag, rows
from _serverproc import disk_usage
from _sql import insert

_WORDS = ["the", "river", "under", "morning", "light", "carried", "every", "lantern", "toward", "harbour"]


def _body(i):
    """Row `i`'s text: long enough to live on the heap, words a symbol table
    codes, and unique to the row."""
    return f"{i} " + " ".join(_WORDS[(i * 7 + k * 3) % len(_WORDS)] for k in range(12 + i % 9))


def _tables(c):
    for name, opts in (("t", " WITH (compress = true)"), ("pt", "")):
        c.execute_sql(f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, body TEXT NOT NULL){opts}")
    c.execute_sql("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)")


def _churn(c, lo, hi, chunk=100):
    """The same inserts, UPDATE and DELETE into `t` and its twin, and `u`'s rows."""
    for a in range(lo, hi + 1, chunk):
        b = min(a + chunk - 1, hi)
        for name in ("t", "pt"):
            insert(c, name, [(i, i * 3, _body(i)) for i in range(a, b + 1)])
        insert(c, "u", [(i, i, i * 7) for i in range(a, b + 1)])
    span = hi - lo + 1
    for name in ("t", "pt"):
        c.execute_sql(f"UPDATE {name} SET body = body || ' updated' WHERE id >= {lo} AND id < {lo + span // 3}")
        c.execute_sql(f"DELETE FROM {name} WHERE id > {hi - span // 5}")


_READS = [
    "SELECT * FROM {t}",
    "SELECT id, body FROM {t} WHERE body LIKE '1%under%'",
    "SELECT id, body FROM {t} ORDER BY body LIMIT 40",
    "SELECT body, COUNT(*) AS n FROM {t} GROUP BY body",
    "SELECT MIN(body) AS lo, MAX(body) AS hi FROM {t}",
]


def _agree(c, views):
    """The compressed relations read as their twins through every verb."""
    for tmpl in _READS:
        want = bag(rows(c, tmpl.format(t="pt")))
        assert want, tmpl
        assert bag(rows(c, tmpl.format(t="t"))) == want, tmpl
    tid, pid = c.resolve_table("t"), c.resolve_table("pt")
    live = bag(c.scan(*pid))
    assert bag(c.scan(*tid)) == live
    keys = sorted(k[0] for k in live)
    for pk in keys[:3] + keys[len(keys) // 2 : len(keys) // 2 + 3] + keys[-3:]:
        assert bag(c.seek(*tid, pk)) == bag(c.seek(*pid, pk)), f"seek {pk}"
    for name in views:
        assert bag(rows(c, f"SELECT * FROM {name}")) == bag(rows(c, f"SELECT * FROM p_{name}")), name


def test_a_compressed_relation_reads_as_its_plain_twin(sweeping):
    views = {
        "lin": "SELECT id, v, body FROM {t} WHERE v > 10",
        "joined": "SELECT {t}.id, {t}.body, u.w FROM {t} JOIN u ON {t}.id = u.tid",
    }
    with gnitz.connect(sweeping.target) as c:
        _tables(c)
        for name, body in views.items():
            c.execute_sql(f"CREATE VIEW {name} WITH (compress = true) AS {body.format(t='t')}")
            c.execute_sql(f"CREATE VIEW p_{name} AS {body.format(t='pt')}")
        _churn(c, 1, 1500)
        _agree(c, views)
        compressed = {c.resolve_table(name)[0] for name in ["t", *views]}

    sweeping.restart()
    with gnitz.connect(sweeping.target) as c:
        _agree(c, views)
        _churn(c, 1501, 2500)
        _agree(c, views)

    sweeping.restart(graceful=True)
    with gnitz.connect(sweeping.target) as c:
        _agree(c, views)
    # Every relation of the server, its twins and the catalog's included: the
    # compressed ones hold their heaps coded, and no other does.
    report, stores = disk_usage(sweeping.data_dir)
    assert {s["relation"] for s in stores if s["store"] == "rows" and "blob symbols" in s["regions"]} == compressed, report
