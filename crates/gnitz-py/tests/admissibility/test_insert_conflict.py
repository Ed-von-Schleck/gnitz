"""What `INSERT` refuses on a primary key.

A plain `INSERT` ships conflict mode `Error`: a PK already present — in committed
state or earlier in the same VALUES list — rejects the whole statement, and
nothing is applied. What `ON CONFLICT` admits instead is in
mutation_pattern/test_dml.py; the conflict targets and actions it does not
support are pinned in the planner's own tests.
"""

import re

import pytest
import gnitz
from _read import bag, scanned
from _serverproc import NEEDS_MULTI
from _sql import insert

_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"

_TWICE = "Batch contains multiple rows with key (pk)=(1)"

# `(ddl, committed rows, {statement refused: the clash it names})`: the
# committed rows are all that is left.
_REJECTED = {
    "against committed": (_T, [(1, 10)], {"INSERT INTO t VALUES (1, 20)": "Key (pk)=(1) already exists"}),
    "intra-batch pair": (_T, [], {"INSERT INTO t VALUES (1, 10), (1, 20)": _TWICE}),
    "intra-batch late": (_T, [], {"INSERT INTO t VALUES (1, 10), (2, 20), (1, 30)": _TWICE}),
    # Two copies of one row fold to one element at weight 2.
    "intra-batch identical": (_T, [], {"INSERT INTO t VALUES (1, 10), (1, 10)": _TWICE}),
    "mid-batch committed": (
        _T, [(2, 200)], {"INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)": "Key (pk)=(2) already exists"}),
    # The probe is scattered by the key, so it must partition at the table's own
    # router width: `CLUSTER BY a` hashes a proper prefix of the PK, and a probe
    # routed at the full-PK width lands on a worker that does not store the key
    # and lets the duplicate commit. Several keys, so no lucky co-location
    # passes it.
    "clustered": (
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, "
        "PRIMARY KEY (a, b)) CLUSTER BY a",
        [(a, b, a * 100 + b) for a in range(8) for b in range(4)],
        {f"INSERT INTO t VALUES ({a}, {b}, -1)": f"Key (a, b)=({a}, {b}) already exists"
         for a, b in [(0, 0), (3, 2), (7, 3)]}),
    # A replicated table is whole on every worker, so the probe single-sources
    # one copy — and the survivor still reads back once, not W times.
    "replicated": pytest.param(
        _T + " WITH (replicated = true)", [(1, 100)],
        {"INSERT INTO t VALUES (1, 999)": "Key (pk)=(1) already exists"}, marks=NEEDS_MULTI),
}


@pytest.mark.parametrize("ddl,committed,refused", _REJECTED.values(), ids=_REJECTED.keys())
def test_duplicate_pk_rejects_the_whole_statement(client, ddl, committed, refused):
    client.execute_sql(ddl)
    if committed:
        insert(client, "t", committed)
    for stmt, clash in refused.items():
        with pytest.raises(gnitz.GnitzIntegrityError, match=f"^duplicate key .*: {re.escape(clash)}$"):
            client.execute_sql(stmt)
    assert bag(scanned(client, "t")) == dict.fromkeys(committed, 1)
