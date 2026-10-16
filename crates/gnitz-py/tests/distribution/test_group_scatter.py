"""A reduce and a top-N whose group is the table's leading PK columns, or
payload columns whose values fit the hidden group key packed, NULLs marked.
Either way each row scatters by its group's bytes — and a group of exactly the
table's distribution columns scatters nowhere at all.

Asserted by WEIGHT against a Python recomputation after every write: a group
routed by one key and stamped by another splits across workers into two rows
under one group, and a packing that aliased NULL with zero would merge two
groups into one — neither shows in a comparison of which groups exist. At one
worker every row is already co-located, so the whole module needs W > 1.
"""

import pytest
from _read import bag, rows
from _serverproc import NEEDS_MULTI
from _shapes import groups
from _sql import churn, insert, values

pytestmark = NEEDS_MULTI

_I32 = [-2**31, 0, 2**31 - 1]
_I64 = [-2**63, 0, 2**63 - 1]
_N = [None, 0, -2**63, 2**63 - 1]

# (a, b, c, g, h, n, v), `c` the row's index: twelve `(a, b)` groups of six,
# each payload group column at both ends of its range, NULL beside zero in `n`,
# and every `v` distinct.
_ROWS = [(i % 3, i % 4, i, _I32[i // 3 % 3], _I64[i // 9 % 3], _N[i // 2 % 4], (i * 37) % 101) for i in range(72)]
_COL = {c: i for i, c in enumerate("abcghnv")}

# view -> its GROUP BY list.
_FOLDS = {"by_ab": "a, b", "by_gh": "g, h", "by_gn": "g, n", "by_ng": "n, g"}
_TOPS = {"top_ab": "a, b", "top_gh": "g, h"}


def _fold(group):
    return f"SELECT {group}, COUNT(*) AS cnt, SUM(v) AS s, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY {group}"


def _top(group):
    return f"SELECT c, {group}, v FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY {group} ORDER BY v DESC) <= 2"


def _set(c, **cols):
    return tuple(cols.get(name, v) for name, v in zip(_COL, _ROWS[c]))


# Rows move between the zero and the NULL group both ways; the minimum of `(0,
# 0)` leaves, as does all of `(1, 2)`; a row rises to the top of `(2, 3)`; and
# every NULL of one `g` leaves.
_CHURN = [
    (f"INSERT INTO t VALUES {values(_ROWS[36:])}", {r[2]: r for r in _ROWS[36:]}),
    ("UPDATE t SET n = NULL WHERE c = 2", {2: _set(2, n=None)}),
    ("UPDATE t SET n = 0 WHERE c = 32", {32: _set(32, n=0)}),
    ("DELETE FROM t WHERE c = 0", {0: None}),
    ("DELETE FROM t WHERE a = 1 AND b = 2", {r[2]: None for r in _ROWS if r[:2] == (1, 2)}),
    ("UPDATE t SET v = 1000 WHERE c = 71", {71: _set(71, v=1000)}),
    (f"DELETE FROM t WHERE n IS NULL AND g = {_I32[2]}",
     {r[2]: None for r in _ROWS if r[5] is None and r[3] == _I32[2]}),
]


def _groups(state, group):
    cols = [_COL[c.strip()] for c in group.split(",")]
    return groups(state.values(), lambda r: tuple(r[c] for c in cols))


def _want_fold(state, group):
    return {(*k, len(rs), sum(r[6] for r in rs), min(r[6] for r in rs), max(r[6] for r in rs)): 1
            for k, rs in _groups(state, group).items()}


def _want_top(state, group):
    return {(r[2], *k, r[6]): 1 for k, rs in _groups(state, group).items()
            for r in sorted(rs, key=lambda r: -r[6])[:2]}


@pytest.mark.parametrize("cluster", ["", " CLUSTER BY a, b", " CLUSTER BY a"])
def test_a_scattered_group_holds_one_row_per_group(client, cluster):
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, g INT NOT NULL, "
        f"h BIGINT NOT NULL, n BIGINT, v BIGINT NOT NULL, PRIMARY KEY (a, b, c)){cluster}")
    # Half the rows before the views (backfill) and half after (the tick).
    insert(client, "t", _ROWS[:36])
    client.execute_sql("; ".join(
        [f"CREATE VIEW {view} AS {_fold(group)}" for view, group in _FOLDS.items()]
        + [f"CREATE VIEW {view} AS {_top(group)}" for view, group in _TOPS.items()]))
    state = {r[2]: r for r in _ROWS[:36]}
    for sql in churn(client, state, _CHURN):
        for view, group in _FOLDS.items():
            assert bag(rows(client, f"SELECT * FROM {view}")) == _want_fold(state, group), (sql, view)
            assert bag(rows(client, _fold(group))) == _want_fold(state, group), (sql, f"ad-hoc {group}")
        for view, group in _TOPS.items():
            assert bag(rows(client, f"SELECT * FROM {view}")) == _want_top(state, group), (sql, view)
