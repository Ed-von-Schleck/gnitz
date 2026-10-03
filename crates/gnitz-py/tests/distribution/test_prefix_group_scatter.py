"""A reduce and a top-N grouped by leading PK columns: the group's own PK bytes
are its key, so its rows scatter by them, and where the table is distributed by
exactly those columns they scatter nowhere at all.

Asserted by WEIGHT against a Python recomputation: a group routed by one key
and stamped by another splits across workers into two rows under one group,
which a row-set comparison of the groups reads as correct. At one worker every
row is already co-located, so the whole module needs W > 1.
"""

import pytest
from _read import bag, rows
from _serverproc import NEEDS_MULTI
from _sql import insert

pytestmark = NEEDS_MULTI

# (a, b, c, v): twelve `(a, b)` groups of five rows, every `v` distinct.
_ROWS = [(i % 3, i % 4, i, (i * 37) % 101) for i in range(60)]

_FOLD = "SELECT a, b, COUNT(*) AS n, SUM(v) AS s, MIN(v) AS lo FROM t GROUP BY {group}"
_TOP = "SELECT a, b, c, v FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY {group} ORDER BY v DESC) <= 2"


def _groups(state):
    groups = {}
    for a, b, _, v in state:
        groups.setdefault((a, b), []).append(v)
    return groups


def _fold(state):
    return {(a, b, len(vs), sum(vs), min(vs)): 1 for (a, b), vs in _groups(state).items()}


def _top(state):
    top = {(a, b): sorted(vs, reverse=True)[:2] for (a, b), vs in _groups(state).items()}
    return {r: 1 for r in state if r[3] in top[r[0], r[1]]}


def _check(client, state, group):
    assert bag(rows(client, "SELECT * FROM g")) == _fold(state)
    assert bag(rows(client, _FOLD.format(group=group))) == _fold(state)
    assert bag(rows(client, "SELECT * FROM top")) == _top(state)


@pytest.mark.parametrize("group", ["a, b", "b, a"])
@pytest.mark.parametrize("cluster", ["", " CLUSTER BY a, b", " CLUSTER BY a"])
def test_a_leading_pk_group_holds_one_row_per_group(client, cluster, group):
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, "
        f"v BIGINT NOT NULL, PRIMARY KEY (a, b, c)){cluster}")
    insert(client, "t", _ROWS[:30])
    client.execute_sql(
        f"CREATE VIEW g AS {_FOLD.format(group=group)}; "
        f"CREATE VIEW top AS {_TOP.format(group=group)}")
    insert(client, "t", _ROWS[30:])
    state = list(_ROWS)
    _check(client, state, group)

    # The minimum of group (0, 0) leaves, so its MIN is read back from history.
    low = min((r for r in state if r[:2] == (0, 0)), key=lambda r: r[3])
    client.execute_sql(f"DELETE FROM t WHERE a = 0 AND b = 0 AND c = {low[2]}")
    state.remove(low)
    # A whole group leaves.
    client.execute_sql("DELETE FROM t WHERE a = 1 AND b = 2")
    state = [r for r in state if r[:2] != (1, 2)]
    # A row rises to the top of its group.
    moved = min((r for r in state if r[:2] == (2, 3)), key=lambda r: r[3])
    client.execute_sql(f"UPDATE t SET v = 1000 WHERE a = 2 AND b = 3 AND c = {moved[2]}")
    state[state.index(moved)] = (*moved[:3], 1000)
    _check(client, state, group)
