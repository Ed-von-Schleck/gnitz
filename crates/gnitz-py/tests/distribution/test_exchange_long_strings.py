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

_SMALL_OUTBOX_ENV = {"GNITZ_MESH_OUTBOX_BYTES": "8192"}

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


def _create_tables(client, sn):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, body TEXT NOT NULL); "
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)",
        schema_name=sn)


def _create_views(client, sn):
    for name, body in _VIEWS.items():
        client.execute_sql(f"CREATE VIEW {name} AS {body}", schema_name=sn)


def _fill(client, sn):
    insert(client, sn, "t", _T)
    insert(client, sn, "u", _U)


def _assert_views(client, sn):
    for name, expected in _EXPECTED.items():
        assert bag(rows(client, sn, f"SELECT * FROM {name}")) == expected, name


def test_a_tick_exchanges_long_string_rows(client, schema_name):
    sn = schema_name
    _create_tables(client, sn)
    _create_views(client, sn)
    _fill(client, sn)
    _assert_views(client, sn)


def test_a_backfill_exchanges_long_string_rows(client, schema_name):
    sn = schema_name
    _create_tables(client, sn)
    _fill(client, sn)
    _create_views(client, sn)
    _assert_views(client, sn)


@pytest.mark.parametrize("views_first", [True, False], ids=["tick", "backfill"])
def test_long_string_rounds_span_many_parts(own_server, views_first):
    own_server.start(extra_env=_SMALL_OUTBOX_ENV)
    with gnitz.connect(own_server.sock_path) as client:
        sn = "parts"
        client.create_schema(sn)
        _create_tables(client, sn)
        if views_first:
            _create_views(client, sn)
            _fill(client, sn)
        else:
            _fill(client, sn)
            _create_views(client, sn)
        _assert_views(client, sn)
