"""A reduce and a top-N grouped by payload columns whose values fit the hidden
group key: the key holds the group's own packed bytes, NULLs marked, and the
exchange routes each row by them.

Asserted by WEIGHT against a Python recomputation: a group routed by one key
and stamped by another splits across workers into two rows under one group, and
a packing that aliased NULL with zero would merge two groups into one — neither
shows in a comparison of which groups exist. At one worker every row is already
co-located, so the whole module needs W > 1.
"""

import pytest
from _read import bag, rows
from _serverproc import NEEDS_MULTI
from _sql import insert

pytestmark = NEEDS_MULTI

_I32 = [-2**31, 0, 2**31 - 1]
_I64 = [-2**63, 0, 2**63 - 1]
_N = [None, 0, -2**63, 2**63 - 1]

# (pk, g, h, n, v): every `v` distinct, each group column at both ends of its
# range, and NULL beside zero in `n`.
_ROWS = [(i, _I32[i % 3], _I64[i // 3 % 3], _N[i % 4], (i * 37) % 101) for i in range(72)]

_COL = {"g": 1, "h": 2, "n": 3}

# view -> its GROUP BY list.
_FOLDS = {"by_gh": "g, h", "by_n": "n", "by_gn": "g, n", "by_ng": "n, g"}


def _body(group):
    return f"SELECT {group}, COUNT(*) AS c, SUM(v) AS s, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY {group}"


_TOP = "SELECT pk, g, h, v FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY g, h ORDER BY v DESC) <= 2"


def _groups(state, group):
    cols = [_COL[c.strip()] for c in group.split(",")]
    groups = {}
    for r in state.values():
        groups.setdefault(tuple(r[c] for c in cols), []).append(r[4])
    return groups


def _fold(state, group):
    return {(*k, len(vs), sum(vs), min(vs), max(vs)): 1 for k, vs in _groups(state, group).items()}


def _top(state):
    top = {k: sorted(vs, reverse=True)[:2] for k, vs in _groups(state, "g, h").items()}
    return {(pk, g, h, v): 1 for pk, g, h, _, v in state.values() if v in top[g, h]}


def _check(client, state):
    for view, group in _FOLDS.items():
        assert bag(rows(client, f"SELECT * FROM {view}")) == _fold(state, group), view
        assert bag(rows(client, _body(group))) == _fold(state, group), f"ad-hoc {group}"
    assert bag(rows(client, "SELECT * FROM top")) == _top(state)


@pytest.mark.parametrize("cluster", ["", " CLUSTER BY pk"])
def test_a_packed_group_holds_one_row_per_group(client, cluster):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g INT NOT NULL, h BIGINT NOT NULL, "
        f"n BIGINT, v BIGINT NOT NULL){cluster}")
    insert(client, "t", _ROWS[:36])
    client.execute_sql(
        "; ".join(f"CREATE VIEW {view} AS {_body(group)}" for view, group in _FOLDS.items())
        + f"; CREATE VIEW top AS {_TOP}")
    insert(client, "t", _ROWS[36:])
    state = {r[0]: r for r in _ROWS}
    _check(client, state)

    def set_n(pk, n):
        client.execute_sql(f"UPDATE t SET n = {'NULL' if n is None else n} WHERE pk = {pk}")
        state[pk] = (*state[pk][:3], n, state[pk][4])

    # A row moves from the zero group to the NULL group, and one the other way.
    set_n(next(pk for pk, r in state.items() if r[3] == 0), None)
    set_n(next(pk for pk, r in state.items() if r[3] is None and r[1] == 0), 0)
    # The minimum of a `(g, h)` group leaves, and so does every NULL of one `g`.
    low = min((r for r in state.values() if r[1:3] == (_I32[0], _I64[0])), key=lambda r: r[4])
    client.execute_sql(f"DELETE FROM t WHERE pk = {low[0]}")
    del state[low[0]]
    client.execute_sql(f"DELETE FROM t WHERE n IS NULL AND g = {_I32[2]}")
    state = {pk: r for pk, r in state.items() if not (r[3] is None and r[1] == _I32[2])}
    _check(client, state)
