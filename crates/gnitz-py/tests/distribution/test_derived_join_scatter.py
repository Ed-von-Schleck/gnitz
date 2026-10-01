"""A join whose input is a derived table: where the master scatters its delta.

The planner states each source's scatter key in that source's own columns; the
reindex node carries the same key in its own input layout. A `Map` between the
scan and the reindex moves one and not the other, and a projection that neither
keeps the join key's position nor makes it the source PK is exactly where the
two part.

Asserted by WEIGHT: the failure this shape admits leaves one side unscattered,
so its rows and their partner's land on different workers and matches simply
vanish. At one worker every row is already co-located, so the whole module
needs W > 1.
"""

import pytest
from _read import bag, rows
from _serverproc import NEEDS_MULTI
from _sql import insert

pytestmark = NEEDS_MULTI

# `t`'s join key `k` is neither its PK nor equal to it, so a delta placed by the
# PK lands on a different worker from one scattered by the key.
_T = [(i, 1000 + i, i * 10) for i in range(1, 25)]     # (id, k, a)
_U = [(1000 + i, i * 100 + 1) for i in range(1, 25)]   # (id, b)


@pytest.fixture
def joined(client):
    """`t` and `u` filled, ready for a view over a derived table of `t`."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL); "
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)")
    insert(client, "t", _T)
    insert(client, "u", _U)


_DERIVED = {
    # `s` is computed, so the projection emits a `Map` and the PK-front rule
    # moves `k` behind `t`'s own PK. `k` is still a source column — column 1 —
    # which is where `t`'s delta has to scatter from.
    "a computed projection over the key": (
        "SELECT x.k AS k, x.s AS s, u.b AS b FROM (SELECT a + 1 AS s, k FROM t) x JOIN u ON x.k = u.id",
        {(k, a + 1, i * 100 + 1): 1 for i, k, a in _T}),
    # The same displacement through a pass-through `Map` rather than a computed
    # one: two output columns off one source column cannot be a rename in place,
    # so the projection emits a node and `k2` lands a slot away from the column
    # it copies.
    "a duplicating projection over the key": (
        "SELECT x.k AS k, x.k2 AS k2, u.b AS b FROM (SELECT k, k AS k2 FROM t) x JOIN u ON x.k2 = u.id",
        {(k, k, i * 100 + 1): 1 for i, k, _ in _T}),
    # `x.s` is no column of `t` at all, so no key over `t` can state where its
    # delta scatters and the side is cut to a hidden segment instead.
    # `a * 10 + 1` is `u.b` on every row.
    "a computed join key": (
        "SELECT x.id AS id, u.b AS b FROM (SELECT id, a * 10 + 1 AS s FROM t) x JOIN u ON x.s = u.b",
        {(i, a * 10 + 1): 1 for i, _, a in _T}),
    # The control: a projection of bare, distinct columns renames the frame's
    # slots in place and emits no `Map`, so the node's key and the source's have
    # always agreed.
    "a projection that moves nothing": (
        "SELECT x.k AS k, x.a AS a, u.b AS b FROM (SELECT a, k FROM t) x JOIN u ON x.k = u.id",
        {(k, a, i * 100 + 1): 1 for i, k, a in _T}),
}


@pytest.mark.parametrize("body,want", _DERIVED.values(), ids=_DERIVED.keys())
def test_a_derived_side_joins_every_matching_pair(client, joined, body, want):
    client.execute_sql(f"CREATE VIEW v AS {body}")
    assert bag(rows(client, "SELECT * FROM v")) == want


def test_a_delta_after_the_view_reaches_both_sides(client, joined):
    """The scatter runs per delta, so the routing has to hold for rows inserted
    after the view exists as well as for its backfill."""
    client.execute_sql(
        "CREATE VIEW v AS SELECT x.k AS k, x.s AS s, u.b AS b "
        "FROM (SELECT a + 1 AS s, k FROM t) x JOIN u ON x.k = u.id")
    insert(client, "t", [(100, 2000, 5000)])
    insert(client, "u", [(2000, 77)])
    client.execute_sql("DELETE FROM u WHERE id = 1003")

    want = {(k, a + 1, i * 100 + 1): 1 for i, k, a in _T if k != 1003}
    want[(2000, 5001, 77)] = 1
    assert bag(rows(client, "SELECT * FROM v")) == want
