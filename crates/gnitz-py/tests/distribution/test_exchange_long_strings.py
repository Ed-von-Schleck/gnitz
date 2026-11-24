"""Exchange rounds whose rows carry a string heap, on a server whose exchange
outboxes are tiny: the views' exchanged rows carry `t.body`, a string past the
inline threshold, so every partition a worker publishes and every slice it
gathers carries heap bytes, and every round spans many parts. Half the rows
reach the views through their backfill and half through the tick, and a delete
then retracts heap-carrying rows across the same rounds.
"""

from collections import Counter

import gnitz
from _read import bag, rows
from _serverproc import MULTI
from _shapes import views_ddl
from _sql import insert

# (pk, k, body): four rows per body, two in each half, so a group routed by a
# wrong key splits.
_T = [(i, i % 400, f"row-{i % 200}-" + "z" * 200) for i in range(800)]
_U = [(i, i * 3) for i in range(400)]   # (id, b)

_VIEWS = {
    "jv": ("SELECT t.pk, t.body, u.b FROM t JOIN u ON t.k = u.id",),
    "gv": ("SELECT body, COUNT(*) AS n FROM t GROUP BY body",),
}


def _check(client, t, u, what):
    want = {"jv": {(pk, body, u[k]): 1 for pk, k, body in t if k in u},
            "gv": dict.fromkeys(Counter(body for *_, body in t).items(), 1)}
    for name in _VIEWS:
        assert bag(rows(client, f"SELECT * FROM {name}")) == want[name], (what, name)


def test_long_string_rounds_span_many_parts(own_server):
    own_server.start(workers=MULTI, extra_env={"GNITZ_MESH_OUTBOX_PAGES": "2"})
    with gnitz.connect(own_server.target) as client:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, body TEXT NOT NULL); "
            "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)")
        insert(client, "t", _T[:400])
        insert(client, "u", _U[::2])
        client.execute_sql(views_ddl(_VIEWS))
        _check(client, _T[:400], dict(_U[::2]), "backfill")
        insert(client, "t", _T[400:])
        insert(client, "u", _U[1::2])
        _check(client, _T, dict(_U), "tick")
        # Two of each body's four rows leave: `gv` moves by retraction alone.
        client.execute_sql("DELETE FROM t WHERE k < 100")
        _check(client, [r for r in _T if r[1] >= 100], dict(_U), "delete")
