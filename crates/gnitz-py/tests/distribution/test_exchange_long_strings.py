"""Exchange rounds whose rows carry a string heap: the views' exchanged rows
carry `t.body`, a string past the inline threshold, so every partition a worker
publishes and every slice it gathers carries heap bytes. Views created before
the insert run the tick path, after it the backfill path, pad rounds included.

The same cases run again on a server with tiny exchange outboxes, where every
round spans many parts.
"""

import gnitz
import pytest
from _read import bag, rows
from _serverproc import NEEDS_MULTI
from _sql import insert

pytestmark = NEEDS_MULTI

_T = [(i, i % 400, f"row-{i}-" + "z" * 200) for i in range(800)]   # (pk, k, body)
_U = [(i, i * 3) for i in range(400)]                               # (id, b)

_VIEWS = {
    "jv": "SELECT t.pk, t.body, u.b FROM t JOIN u ON t.k = u.id",
    "gv": "SELECT body, COUNT(*) AS n FROM t GROUP BY body",
}
_EXPECTED = {
    "jv": {(pk, body, k * 3): 1 for pk, k, body in _T},
    "gv": {(body, 1): 1 for _, _, body in _T},
}


def _exchange(client, views_first):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, body TEXT NOT NULL); "
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)")
    views = "; ".join(f"CREATE VIEW {name} AS {body}" for name, body in _VIEWS.items())
    if views_first:
        client.execute_sql(views)
    insert(client, "t", _T)
    insert(client, "u", _U)
    if not views_first:
        client.execute_sql(views)
    for name, expected in _EXPECTED.items():
        assert bag(rows(client, f"SELECT * FROM {name}")) == expected, name


_EACH_PATH = pytest.mark.parametrize("views_first", [True, False], ids=["tick", "backfill"])


@_EACH_PATH
def test_a_round_exchanges_long_string_rows(client, views_first):
    _exchange(client, views_first)


@_EACH_PATH
def test_long_string_rounds_span_many_parts(own_server, views_first):
    own_server.start(extra_env={"GNITZ_MESH_OUTBOX_BYTES": "8192"})
    _exchange(gnitz.connect(own_server.target), views_first)
