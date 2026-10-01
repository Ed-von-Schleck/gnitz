"""A band join's relay key against the table's own distribution prefix.

A band join (`ON a.k = b.k AND a.t < b.t`) relays each delta by its equality
prefix alone. Where that prefix IS the table's distribution key — `CLUSTER BY k`
over `PRIMARY KEY (k, b)` — the rows already sit where the join needs them and
the relay is skipped; distributed by the whole compound PK instead, the prefix
is narrower than the placement and the relay must run. The view is the same
either way.

Asserted by WEIGHT: a relay wrongly skipped leaves the two sides on different
workers and matches simply vanish. At one worker every row is co-located, so
this needs W > 1.
"""

from collections import Counter

import pytest
from _read import bag, rows
from _serverproc import NEEDS_MULTI
from _sql import insert

pytestmark = NEEDS_MULTI

# `t` varies within a `k` group on both sides, so each group holds matches and
# non-matches rather than the whole product.
_P = [(k, b, k * 10 + b) for k in range(1, 7) for b in range(1, 4)]
_Q = [(k, b, k * 10 + 2 * b) for k in range(1, 7) for b in range(1, 4)]


@pytest.mark.parametrize("cluster_by_k", [True, False], ids=["cluster_by_k", "cluster_by_the_whole_pk"])
def test_a_band_join_matches_whatever_the_distribution_prefix_is(client, cluster_by_k):
    clause = " CLUSTER BY k" if cluster_by_k else ""
    for name in ("p", "q"):
        client.execute_sql(
            f"CREATE TABLE {name} (k BIGINT NOT NULL, b BIGINT NOT NULL, t BIGINT NOT NULL, "
            f"PRIMARY KEY (k, b)){clause}")
    # Half the rows before the view (backfill) and half after (the relay under
    # test), so one run covers both paths into the join.
    insert(client, "p", _P[::2])
    insert(client, "q", _Q[::2])
    client.execute_sql(
        "CREATE VIEW v AS SELECT p.k AS k, p.b AS pb, q.b AS qb "
        "FROM p JOIN q ON p.k = q.k AND p.t < q.t")
    insert(client, "p", _P[1::2])
    insert(client, "q", _Q[1::2])

    want = Counter((pk, pb, qb) for pk, pb, pt in _P for qk, qb, qt in _Q if pk == qk and pt < qt)
    assert bag(rows(client, "SELECT * FROM v")) == dict(want)
