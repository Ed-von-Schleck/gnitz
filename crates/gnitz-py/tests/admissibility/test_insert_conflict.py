"""What `INSERT` refuses on a primary key, and what `ON CONFLICT` admits instead.

A plain `INSERT` ships conflict mode `Error`: a PK already present — in committed
state or earlier in the same VALUES list — rejects the whole statement, and
nothing is applied. `ON CONFLICT (pk)` replaces that verdict with DO NOTHING or
DO UPDATE. The conflict targets and actions it does not support are pinned in
the planner's own tests.
"""

import pytest
import gnitz
from _read import bag, scanned
from _serverproc import NEEDS_MULTI
from _sql import insert, values

_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"

# `(ddl, committed rows, statements each refused)`: the committed rows are all
# that is left.
_REJECTED = {
    "against committed": (_T, [(1, 10)], ["INSERT INTO t VALUES (1, 20)"]),
    "intra-batch pair": (_T, [], ["INSERT INTO t VALUES (1, 10), (1, 20)"]),
    "intra-batch late": (_T, [], ["INSERT INTO t VALUES (1, 10), (2, 20), (1, 30)"]),
    "mid-batch committed": (_T, [(2, 200)], ["INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)"]),
    # The probe is scattered by the key, so it must partition at the table's own
    # router width: `CLUSTER BY a` hashes a proper prefix of the PK, and a probe
    # routed at the full-PK width lands on a worker that does not store the key
    # and lets the duplicate commit. Several keys, so no lucky co-location
    # passes it.
    "clustered": (
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, "
        "PRIMARY KEY (a, b)) CLUSTER BY a",
        [(a, b, a * 100 + b) for a in range(8) for b in range(4)],
        [f"INSERT INTO t VALUES ({a}, {b}, -1)" for a, b in [(0, 0), (3, 2), (7, 3)]]),
    # A replicated table is whole on every worker, so the probe single-sources
    # one copy — and the survivor still reads back once, not W times.
    "replicated": pytest.param(
        _T + " WITH (replicated = true)", [(1, 100)], ["INSERT INTO t VALUES (1, 999)"],
        marks=NEEDS_MULTI),
}


@pytest.mark.parametrize("ddl,committed,refused", _REJECTED.values(), ids=_REJECTED.keys())
def test_duplicate_pk_rejects_the_whole_statement(client, schema_name, ddl, committed, refused):
    client.execute_sql(ddl, schema_name=schema_name)
    if committed:
        insert(client, schema_name, "t", committed)
    for stmt in refused:
        with pytest.raises(gnitz.GnitzError, match="(?i)duplicate key"):
            client.execute_sql(stmt, schema_name=schema_name)
    assert bag(scanned(client, schema_name, "t")) == dict.fromkeys(committed, 1)


_ON_CONFLICT = [
    ("do nothing skips the conflict",
     "INSERT INTO t VALUES (1, 10)",
     "INSERT INTO t VALUES (1, 20), (2, 30) ON CONFLICT (pk) DO NOTHING",
     [(1, 10), (2, 30)]),
    ("do nothing with no target",
     "INSERT INTO t VALUES (1, 10)",
     "INSERT INTO t VALUES (1, 20), (2, 30) ON CONFLICT DO NOTHING",
     [(1, 10), (2, 30)]),
    ("do nothing, every row conflicts",
     "INSERT INTO t VALUES (1, 10), (2, 20)",
     "INSERT INTO t VALUES (1, 999), (2, 999), (3, 30) ON CONFLICT (pk) DO NOTHING",
     [(1, 10), (2, 20), (3, 30)]),
    ("do update from EXCLUDED",
     "INSERT INTO t VALUES (1, 10)",
     "INSERT INTO t VALUES (1, 100), (2, 200) ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
     [(1, 100), (2, 200)]),
    ("do update from a literal",
     "INSERT INTO t VALUES (1, 10)",
     "INSERT INTO t VALUES (1, 20) ON CONFLICT (pk) DO UPDATE SET val = 42",
     [(1, 42)]),
]


@pytest.mark.parametrize("seed,stmt,final", [c[1:] for c in _ON_CONFLICT],
                         ids=[c[0] for c in _ON_CONFLICT])
def test_on_conflict_resolves_instead_of_rejecting(client, schema_name, seed, stmt, final):
    client.execute_sql(_T, schema_name=schema_name)
    client.execute_sql(seed, schema_name=schema_name)
    client.execute_sql(stmt, schema_name=schema_name)
    # A conflict resolved on a worker other than the key's owner leaves the
    # original row beside its replacement.
    assert bag(scanned(client, schema_name, "t")) == dict.fromkeys(final, 1)


def test_insert_partial_column_list(client, schema_name):
    """A column list may name a subset in any order; every column it omits is
    written NULL (gnitz has no column DEFAULTs), so omitting a NOT NULL one is
    the NOT NULL violation — raised before anything is written."""
    sn = schema_name
    client.execute_sql("CREATE TABLE p (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b TEXT); " + _T,
                       schema_name=sn)
    # Reordered as well as partial: the slot map, not the position, decides
    # which column each VALUES element lands in.
    client.execute_sql("INSERT INTO p (b, pk) VALUES ('x', 1)", schema_name=sn)
    client.execute_sql("INSERT INTO p (pk, a, b) VALUES (2, 20, 'y')", schema_name=sn)
    assert bag(scanned(client, sn, "p")) == {(1, None, "x"): 1, (2, 20, "y"): 1}

    with pytest.raises(gnitz.GnitzError, match="(?i)not null"):
        client.execute_sql("INSERT INTO t (pk) VALUES (1)", schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {}


def test_do_update_over_a_string_column_and_a_rejected_null(client, schema_name):
    """`SET s = EXCLUDED.s` takes the string classifier arm, which the integer
    tables above never reach. A NULL under a NOT NULL column has to be refused
    before the merge reads it: the merged batch is the only thing this plan
    pushes, so an unvalidated NULL would be read back as a real 0 and committed
    silently."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE s (pk BIGINT NOT NULL PRIMARY KEY, txt TEXT NOT NULL, "
        "v BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("INSERT INTO s VALUES (1, 'old', 10)", schema_name=sn)

    client.execute_sql(
        "INSERT INTO s VALUES (1, 'new', 11), (2, 'fresh', 22) "
        "ON CONFLICT (pk) DO UPDATE SET txt = EXCLUDED.txt", schema_name=sn)
    want = {(1, "new", 10): 1, (2, "fresh", 22): 1}
    assert bag(scanned(client, sn, "s")) == want

    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(
            "INSERT INTO s VALUES (1, NULL, 0) ON CONFLICT (pk) DO UPDATE SET txt = EXCLUDED.txt",
            schema_name=sn)
    assert bag(scanned(client, sn, "s")) == want, "no silent write survived the refusal"


@pytest.mark.parametrize("arity", [2, 3])
def test_a_conflict_target_may_name_a_whole_compound_key(client, schema_name, arity):
    """The target is the primary key whatever its arity, so the probe resolving
    which rows conflict has to compare the whole packed key. A probe that
    compared a prefix would report the new key as a conflict and update the wrong
    row; one that compared nothing would insert a duplicate."""
    sn, keys = schema_name, ", ".join("abc"[:arity])

    def row(k, v):
        return (k,) * arity + (v,)

    client.execute_sql(
        "CREATE TABLE c (" + ", ".join(f"{k} BIGINT UNSIGNED NOT NULL" for k in "abc"[:arity])
        + f", v BIGINT NOT NULL, PRIMARY KEY ({keys}))", schema_name=sn)
    insert(client, sn, "c", [row(1, 10), row(2, 20)])
    client.execute_sql(
        f"INSERT INTO c VALUES {values([row(1, 111), row(3, 30)])} "
        f"ON CONFLICT ({keys}) DO UPDATE SET v = EXCLUDED.v", schema_name=sn)
    assert bag(scanned(client, sn, "c")) == dict.fromkeys([row(1, 111), row(2, 20), row(3, 30)], 1)
