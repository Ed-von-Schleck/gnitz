"""What `INSERT` refuses on a primary key, and what `ON CONFLICT` admits instead.

A plain `INSERT` ships conflict mode `Error`: a PK already present — in committed
state or earlier in the same VALUES list — rejects the whole statement, and
nothing is applied. `ON CONFLICT (pk)` replaces that verdict with DO NOTHING or
DO UPDATE. Everything else about the target is unsupported in v1 and says so.
"""

import pytest
import gnitz
from _serverproc import NEEDS_MULTI

_DDL = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"


@pytest.fixture
def t(client, schema_name):
    """`t(pk, val)` in a fresh schema; yields its tid."""
    client.execute_sql(_DDL, schema_name=schema_name)
    return client.resolve_table(schema_name, "t")[0]


def _rows(client, tid):
    return sorted((row.pk, row.val) for row in client.scan(tid))


# A rejected INSERT applies nothing — the batch is atomic whether the conflict is
# against committed state or against an earlier row of the same VALUES list.
_REJECTED = [
    ("against committed",  "INSERT INTO t VALUES (1, 10)", "INSERT INTO t VALUES (1, 20)",              [(1, 10)]),
    ("intra-batch pair",   None,                           "INSERT INTO t VALUES (1, 10), (1, 20)",     []),
    ("intra-batch late",   None,                           "INSERT INTO t VALUES (1,10),(2,20),(1,30)", []),
    ("mid-batch committed", "INSERT INTO t VALUES (2, 200)", "INSERT INTO t VALUES (1,10),(2,20),(3,30)", [(2, 200)]),
]


@pytest.mark.parametrize("seed,stmt,final", [c[1:] for c in _REJECTED],
                         ids=[c[0] for c in _REJECTED])
def test_duplicate_pk_rejects_the_whole_statement(client, schema_name, t, seed, stmt, final):
    if seed:
        client.execute_sql(seed, schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="(?i)duplicate key"):
        client.execute_sql(stmt, schema_name=schema_name)
    assert _rows(client, t) == final


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
def test_on_conflict_resolves_instead_of_rejecting(client, schema_name, t, seed, stmt, final):
    client.execute_sql(seed, schema_name=schema_name)
    client.execute_sql(stmt, schema_name=schema_name)
    assert _rows(client, t) == final


_UNSUPPORTED = [
    # A conflict target must name exactly the primary key — `(val)` names
    # something else, `(pk, val)` names more than it. Both fail the same check.
    ("INSERT INTO t VALUES (1, 10) ON CONFLICT (val) DO NOTHING", "primary key"),
    ("INSERT INTO t VALUES (1, 10) ON CONFLICT (pk, val) DO NOTHING", "primary key"),
    ("INSERT INTO t VALUES (1, 20) ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val WHERE val > 5",
     "DO UPDATE WHERE"),
    # PG rejects a row that would be affected a second time by the same command.
    ("INSERT INTO t VALUES (1, 10), (1, 20) ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
     "second time"),
]


@pytest.mark.parametrize("stmt,message", _UNSUPPORTED)
def test_unsupported_on_conflict_variant_names_its_rule(client, schema_name, t, stmt, message):
    with pytest.raises(gnitz.GnitzError, match=message):
        client.execute_sql(stmt, schema_name=schema_name)


def test_insert_partial_column_list(client, schema_name):
    """A column list may name a subset in any order; every column it omits is
    written NULL (gnitz has no column DEFAULTs), so omitting a NOT NULL one is
    the NOT NULL violation — raised before anything is written."""
    client.execute_sql(
        "CREATE TABLE p (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b TEXT)",
        schema_name=schema_name,
    )
    # Reordered as well as partial: the slot map, not the position, decides
    # which column each VALUES element lands in.
    client.execute_sql("INSERT INTO p (b, pk) VALUES ('x', 1)", schema_name=schema_name)
    client.execute_sql("INSERT INTO p (pk, a, b) VALUES (2, 20, 'y')", schema_name=schema_name)
    tid, _ = client.resolve_table(schema_name, "p")
    assert sorted((r.pk, r.a, r.b) for r in client.scan(tid)) == [(1, None, "x"), (2, 20, "y")]

    client.execute_sql(_DDL, schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="(?i)not null"):
        client.execute_sql("INSERT INTO t (pk) VALUES (1)", schema_name=schema_name)
    assert not list(client.scan(client.resolve_table(schema_name, "t")[0]))


def test_duplicate_on_a_clustered_table_is_rejected(client, schema_name):
    """The Error-mode PK probe is SCATTERED by the key, so it must partition at
    the table's own router width. `CLUSTER BY a` hashes a proper prefix of the
    PK; a probe routed at the default full-PK width lands on a worker that does
    not store the key, comes back empty, and the duplicate commits silently —
    with no error anywhere, since the probe schema's equality ignores placement.
    """
    client.execute_sql(
        "CREATE TABLE c (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, "
        "PRIMARY KEY (a, b)) CLUSTER BY a",
        schema_name=schema_name)
    # Several distinct `a` values, so the prefix hash spreads over the cluster
    # and a full-PK hash of the same keys lands elsewhere.
    rows = ", ".join(f"({a}, {b}, {a * 100 + b})" for a in range(8) for b in range(4))
    client.execute_sql(f"INSERT INTO c VALUES {rows}", schema_name=schema_name)
    tid, _ = client.resolve_table(schema_name, "c")
    assert len(client.scan(tid)) == 32

    for a, b in [(0, 0), (3, 2), (7, 3)]:
        with pytest.raises(gnitz.GnitzError, match="(?i)duplicate key"):
            client.execute_sql(f"INSERT INTO c VALUES ({a}, {b}, -1)", schema_name=schema_name)
    assert len(client.scan(tid)) == 32


@NEEDS_MULTI
def test_duplicate_on_a_replicated_table_is_rejected(client, schema_name):
    """The other placement the Error-mode probe must get right. A replicated
    table is not scattered at all: every worker holds the whole thing, so the
    probe single-sources one worker's full copy. Fanning it out instead would
    answer W times over and reject nothing differently — but the read of the
    survivor must still show one copy, not W."""
    client.execute_sql(
        "CREATE TABLE r (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL) "
        "WITH (replicated = true)", schema_name=schema_name)
    client.execute_sql("INSERT INTO r VALUES (1, 100)", schema_name=schema_name)

    with pytest.raises(gnitz.GnitzError, match="(?i)duplicate key"):
        client.execute_sql("INSERT INTO r VALUES (1, 999)", schema_name=schema_name)

    tid, _ = client.resolve_table(schema_name, "r")
    assert sorted((r.pk, r.val) for r in client.scan(tid)) == [(1, 100)]


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
    tid = client.resolve_table(sn, "s")[0]
    assert sorted((r.pk, r.txt) for r in client.scan(tid)) == [(1, "new"), (2, "fresh")]

    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(
            "INSERT INTO s VALUES (1, NULL, 0) ON CONFLICT (pk) DO UPDATE SET txt = EXCLUDED.txt",
            schema_name=sn)
    assert sorted((r.pk, r.txt) for r in client.scan(tid)) == [(1, "new"), (2, "fresh")], \
        "no silent write survived the refusal"


@pytest.mark.parametrize("pk_ddl,pk_list,target,seed,upsert,final", [
    pytest.param(
        "a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL", "(a, b)", "(a, b)",
        "(1, 1, 10), (2, 2, 20)",
        "(1, 1, 111), (3, 3, 30)", [(1, 1, 111), (2, 2, 20), (3, 3, 30)], id="arity-2"),
    pytest.param(
        "a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
        "c BIGINT UNSIGNED NOT NULL", "(a, b, c)", "(a, b, c)",
        "(1, 1, 1, 10), (2, 2, 2, 20)",
        "(1, 1, 1, 111), (3, 3, 3, 30)",
        [(1, 1, 1, 111), (2, 2, 2, 20), (3, 3, 3, 30)], id="arity-3"),
])
def test_a_conflict_target_may_name_a_whole_compound_key(
        client, schema_name, pk_ddl, pk_list, target, seed, upsert, final):
    """The target is the primary key whatever its arity, so the probe resolving
    which rows conflict has to compare the whole packed key. A probe that
    compared a prefix would report the new key as a conflict and update the wrong
    row; one that compared nothing would insert a duplicate."""
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE c ({pk_ddl}, v BIGINT NOT NULL, PRIMARY KEY {pk_list})",
        schema_name=sn)
    client.execute_sql(f"INSERT INTO c VALUES {seed}", schema_name=sn)
    client.execute_sql(
        f"INSERT INTO c VALUES {upsert} ON CONFLICT {target} DO UPDATE SET v = EXCLUDED.v",
        schema_name=sn)
    tid, _ = client.resolve_table(sn, "c")
    assert sorted(tuple(r)[:len(final[0])] for r in client.scan(tid)) == sorted(final)


def test_a_conflict_target_naming_part_of_a_compound_key_is_refused(client, schema_name):
    """`(a)` is a proper prefix of the key, not the key. Accepting it would make
    the upsert resolve against a set of rows rather than one, with no rule for
    which to update."""
    client.execute_sql(
        "CREATE TABLE c (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
        "v BIGINT NOT NULL, PRIMARY KEY (a, b))", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="primary key"):
        client.execute_sql(
            "INSERT INTO c VALUES (1, 1, 10) ON CONFLICT (a) DO NOTHING",
            schema_name=schema_name)
