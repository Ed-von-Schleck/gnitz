"""Compressed stores: `CREATE TABLE … WITH (compress = true)` and the same on a
view.

The claim under test is that compression is invisible to every read. Each test
runs a compressed relation beside a plain twin over the same rows and requires
the two to agree, weights and all, through a scan, a seek, the ad-hoc `SELECT`
path — a string predicate, a string ORDER BY, a GROUP BY and MIN/MAX over the
string — and views over it, a join's among them, across a crash and a graceful
restart, while the compressed relations' shards hold their heaps coded and the
twins' do not.

The stores reach the disk regime on a few thousand rows through `SWEEP_ENV`.

A stream refuses `WITH (compress = true)`: the planner's, in
`crates/gnitz-sql/src/suites/plan_create_table.rs`.
"""

import gnitz
from _read import reads_alike
from _serverproc import disk_usage
from _sql import insert

_WORDS = ["the", "river", "under", "morning", "light", "carried", "every", "lantern", "toward", "harbour"]


def _body(i):
    """Row `i`'s text: long enough to live on the heap, words a symbol table
    codes, and unique to the row."""
    return f"{i} " + " ".join(_WORDS[(i * 7 + k * 3) % len(_WORDS)] for k in range(12 + i % 9))


# Each compressed relation `{name}` and its plain twin `p_{name}`; `{t}` is the
# table of the same pair.
_VIEWS = {
    "lin": "SELECT id, v, body FROM {t} WHERE v > 10",
    "joined": "SELECT {t}.id, {t}.body, u.w FROM {t} JOIN u ON {t}.id = u.tid",
}
_NAMES = ["t", *_VIEWS]

_READS = [
    "SELECT * FROM {v}",
    "SELECT id, body FROM {v} WHERE body LIKE '1%under%'",
    "SELECT id, body FROM {v} ORDER BY body LIMIT 40",
    "SELECT body, COUNT(*) AS n FROM {v} GROUP BY body",
    "SELECT MIN(body) AS lo, MAX(body) AS hi FROM {v}",
]


def _churn(c, lo, hi, chunk=100):
    """The same inserts, UPDATE and DELETE into `t` and its twin, and `u`'s rows."""
    for a in range(lo, hi + 1, chunk):
        b = min(a + chunk - 1, hi)
        for name in ("t", "p_t"):
            insert(c, name, [(i, i * 3, _body(i)) for i in range(a, b + 1)])
        insert(c, "u", [(i, i, i * 7) for i in range(a, b + 1)])
    span = hi - lo + 1
    for name in ("t", "p_t"):
        c.execute_sql(f"UPDATE {name} SET body = body || ' updated' WHERE id >= {lo} AND id < {lo + span // 3}")
        c.execute_sql(f"DELETE FROM {name} WHERE id > {hi - span // 5}")


def _agree(c):
    for name in _NAMES:
        reads_alike(c, name, f"p_{name}", _READS)


def test_a_compressed_relation_reads_as_its_plain_twin(sweeping):
    with gnitz.connect(sweeping.target) as c:
        for name, opts in (("t", " WITH (compress = true)"), ("p_t", "")):
            c.execute_sql(f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, body TEXT NOT NULL){opts}")
        c.execute_sql("CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)")
        for name, body in _VIEWS.items():
            c.execute_sql(f"CREATE VIEW {name} WITH (compress = true) AS {body.format(t='t')}")
            c.execute_sql(f"CREATE VIEW p_{name} AS {body.format(t='p_t')}")
        _churn(c, 1, 1500)
        _agree(c)
        compressed = {c.resolve_table(name)[0] for name in _NAMES}

    sweeping.restart()
    with gnitz.connect(sweeping.target) as c:
        _agree(c)
        _churn(c, 1501, 2500)
        _agree(c)

    sweeping.restart(graceful=True)
    with gnitz.connect(sweeping.target) as c:
        _agree(c)
    # Every relation of the server, its twins and the catalog's included: the
    # compressed ones hold their heaps coded, and no other does.
    report, stores = disk_usage(sweeping.data_dir)
    assert {s["relation"] for s in stores if s["store"] == "rows" and "blob symbols" in s["regions"]} == compressed, report
