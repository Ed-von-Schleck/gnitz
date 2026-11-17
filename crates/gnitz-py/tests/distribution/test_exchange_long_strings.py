"""Exchange rounds whose rows carry a string heap, on a server whose exchange
outboxes are tiny: the views' exchanged rows carry `t.body`, a string past the
inline threshold, so every partition a worker publishes and every slice it
gathers carries heap bytes, and every round spans many parts. Half the rows
reach the views through their backfill and half through the tick.
"""

import gnitz
from _read import bag, rows
from _serverproc import MULTI
from _sql import insert

# (pk, k, body): four rows per body, two in each half, so a group routed by a
# wrong key splits.
_T = [(i, i % 400, f"row-{i % 200}-" + "z" * 200) for i in range(800)]
_U = [(i, i * 3) for i in range(400)]   # (id, b)

# view -> (body, expected bag)
_VIEWS = {
    "jv": ("SELECT t.pk, t.body, u.b FROM t JOIN u ON t.k = u.id", {(pk, body, k * 3): 1 for pk, k, body in _T}),
    "gv": ("SELECT body, COUNT(*) AS n FROM t GROUP BY body", {(body, 4): 1 for _, _, body in _T}),
}


def test_long_string_rounds_span_many_parts(own_server):
    own_server.start(workers=MULTI, extra_env={"GNITZ_MESH_OUTBOX_PAGES": "2"})
    with gnitz.connect(own_server.target) as client:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, body TEXT NOT NULL); "
            "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)")
        insert(client, "t", _T[:400])
        insert(client, "u", _U[::2])
        client.execute_sql("; ".join(f"CREATE VIEW {name} AS {body}" for name, (body, _) in _VIEWS.items()))
        insert(client, "t", _T[400:])
        insert(client, "u", _U[1::2])
        for name, (_, want) in _VIEWS.items():
            assert bag(rows(client, f"SELECT * FROM {name}")) == want, name
