"""SQL writes: what an INSERT's VALUES clause spells, and how UPDATE and DELETE
find their rows, committed and buffered alike.

UPDATE and DELETE resolve their targets through one access-path ladder — PK
equality, a PK set, a PK range or compound prefix, a unique or non-unique
index, a predicate scan — and the bound together with the server-side
predicate is the whole WHERE. Inside a transaction a statement must also see
the transaction's own buffered rows, which no server-side walk ever saw: a row
born in it, a committed row it overrode, one it deleted. A key the expression
VM cannot hold (a 64-bit extreme, a UUID, a 128-bit decimal) has to be applied
by the walk alone, even over a buffer.

So every scenario runs twice — statement by statement in autocommit, and as
one BEGIN … COMMIT — and both must report the given per-statement counts and
leave the given Z-set.
"""

import pytest
from _read import bag, scanned

# `ReadSpec`'s wire cap on a `pk IN (…)` gather; DML chunks past it.
MAX_PK_SET_KEYS = 65_536

_U64_MAX = (1 << 64) - 1
_I64_MIN = -(1 << 63)
_UUID_A = "550e8400-e29b-41d4-a716-446655440000"
_UUID_B = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"


def test_every_values_spelling_writes_its_rows(client, schema_name):
    """`VALUES`, `ROW(…)`, the singular `VALUE`, a unary plus and a parenthesised
    literal are spellings of one decoder, in a PK slot and a payload slot alike.
    A signed NULL is the NULL it spells — not a zero with the null bit clear."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)", schema_name=sn)
    res = client.execute_sql(
        "INSERT INTO t VALUES (1, 10), (2, 20); "
        "INSERT INTO t VALUES ROW(3, 30); "
        "INSERT INTO t VALUE (4, 40); "
        "INSERT INTO t VALUES (+5, (50)), ((6), +60); "
        "INSERT INTO t VALUES (7, +NULL), (8, -NULL), (9, NULL)", schema_name=sn)
    assert [r["count"] for r in res] == [2, 1, 1, 2, 3]
    assert bag(scanned(client, sn, "t")) == {
        (1, 10): 1, (2, 20): 1, (3, 30): 1, (4, 40): 1, (5, 50): 1, (6, 60): 1,
        (7, None): 1, (8, None): 1, (9, None): 1}


_LADDER = ("CREATE TABLE {t} (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL, "
           "cat BIGINT NOT NULL, u BIGINT NOT NULL, s TEXT); "
           "CREATE INDEX ON {t} (cat); CREATE UNIQUE INDEX ON {t} (u)")
_LADDER_SEED = "(1, 100, 10, 1, 'a'), (2, 200, 10, 2, 'past-the-inline-limit'), (3, 300, 30, 3, NULL), (4, 400, 40, 4, '')"
# (statement, rows affected). Comments name what the statement has to see.
_LADDER_STEPS = [
    ("INSERT INTO {t} VALUES (5, 250, 10, 5, 'born')", 1),
    ("UPDATE {t} SET val = val + 1 WHERE pk = 1", 1),
    ("UPDATE {t} SET val = val + 1 WHERE pk = 1", 1),                  # compounds on the first
    ("UPDATE {t} SET val = 350, cat = 99 WHERE pk = 3", 1),
    ("UPDATE {t} SET val = 0 WHERE cat = 30", 0),                      # the override moved it out
    ("UPDATE {t} SET val = val + 1 WHERE cat = 99", 1),                # a value only the buffer holds
    ("UPDATE {t} SET val = val * 2 WHERE cat = 10", 3),                # a key group, born row included
    ("DELETE FROM {t} WHERE pk = 4", 1),                               # born 5 is not swept in
    ("UPDATE {t} SET val = 9 WHERE pk = 4", 0),                        # no resurrection
    ("DELETE FROM {t} WHERE pk = 4", 0),
    ("INSERT INTO {t} VALUES (4, 55, 40, 4, 'again')", 1),             # the replace idiom
    ("UPDATE {t} SET val = val + 1 WHERE val = 55", 1),                # the re-inserted payload
    ("UPDATE {t} SET s = 'set' WHERE pk IN (1, 5, 5, 999)", 2),        # repeat once, absent nothing
    ("UPDATE {t} SET val = 7 WHERE pk IN (2, 3) AND val > 360", 1),    # the set, post-filtered
    ("UPDATE {t} SET val = val + 1 WHERE pk NOT IN (2)", 4),
    ("DELETE FROM {t} WHERE pk > 2 AND cat = 10", 1),                  # overridden 1 is below the cut
    ("UPDATE {t} SET val = 1 WHERE u = 2", 1),
    ("DELETE FROM {t} WHERE val IN (1, 57, 501)", 2),                  # tombstoned 5 carried 501
    ("INSERT INTO {t} VALUES (1, 0, 0, 1, 'x'), (6, 60, 60, 6, 'y') "
     "ON CONFLICT (pk) DO UPDATE SET val = val + 100", 2),
    ("INSERT INTO {t} VALUES (6, 0, 0, 6, 'x'), (7, 70, 70, 7, 'z') ON CONFLICT (pk) DO NOTHING", 1),
    ("DELETE FROM {t} WHERE pk = 6", 1),
    ("INSERT INTO {t} VALUES (6, 7, 60, 6, 'w') "
     "ON CONFLICT (pk) DO UPDATE SET val = val + 100", 1),             # a deleted key is no conflict
    ("UPDATE {t} SET val = val * 2", 4),
    ("DELETE FROM {t} WHERE pk = 9999", 0),
]
_LADDER_FINAL = [(1, 610, 10, 1, "set"), (3, 704, 99, 3, None), (6, 14, 60, 6, "w"), (7, 140, 70, 7, "z")]


def _wide_key(pk_sql, key, other, other_py):
    """A key the expression VM has no register for, beside an ordinary one."""
    return (f"CREATE TABLE {{t}} (id {pk_sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            f"({key}, 1), ({other}, 2)",
            [(f"UPDATE {{t}} SET v = 7 WHERE id = {other}", 1),
             (f"UPDATE {{t}} SET v = 9 WHERE id = {key}", 1),
             (f"UPDATE {{t}} SET v = 8 WHERE id IN ({other}, {other})", 1),
             (f"DELETE FROM {{t}} WHERE id IN ({key})", 1)],
            [(other_py, 8)])


# name -> (CREATE for table {t}, seed VALUES, steps, final rows)
_SCENARIOS = {
    "ladder": (_LADDER, _LADDER_SEED, _LADDER_STEPS, _LADDER_FINAL),
    "ladder-indexed-val": (_LADDER + "; CREATE INDEX ON {t} (val)", _LADDER_SEED, _LADDER_STEPS, _LADDER_FINAL),
    # A bare prefix names a key group, not a key, so it restricts no buffered
    # candidate to one key; the predicate DELETE reads back both key columns.
    "compound-pk": (
        "CREATE TABLE {t} (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, "
        "s TEXT NOT NULL, PRIMARY KEY (a, b))",
        "(1, 1, 0, 'p11'), (1, 9, 0, 'p19'), (2, 9, 0, 'p29'), (3, 3, 22, 'p33')",
        [("INSERT INTO {t} VALUES (1, 7, 0, 'born')", 1),
         ("DELETE FROM {t} WHERE a = 1 AND b > 5", 2),
         ("UPDATE {t} SET v = v + 5 WHERE a = 1 AND b = 1", 1),
         ("INSERT INTO {t} VALUES (1, 4, 0, 'born'), (5, 5, 0, 'born')", 2),
         ("UPDATE {t} SET v = v + 1 WHERE a = 1", 2),
         ("DELETE FROM {t} WHERE a > 2", 2),
         ("DELETE FROM {t} WHERE v = 0", 1)],
        [(1, 1, 6, "p11"), (1, 4, 1, "born")]),
    "u64-max-key": _wide_key("BIGINT UNSIGNED", _U64_MAX, 1, 1),
    "i64-min-key": _wide_key("BIGINT", _I64_MIN, -1, -1),
    "uuid-key": _wide_key("UUID", f"'{_UUID_A}'", f"'{_UUID_B}'", _UUID_B),
    # A 128-bit PK range pins no key, so a buffered row would face the whole
    # WHERE; a transaction holding only a tombstone has none to face.
    "u128-range": (
        "CREATE TABLE {t} (id DECIMAL(38,0) NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        ", ".join(f"({i}, {i})" for i in range(1, 11)),
        [("DELETE FROM {t} WHERE id = 1", 1), ("DELETE FROM {t} WHERE id > 5", 5)],
        [(i, i) for i in range(2, 6)]),
}


@pytest.mark.parametrize("create,seed,steps,final", _SCENARIOS.values(), ids=_SCENARIOS.keys())
def test_a_transaction_resolves_each_statement_as_autocommit_does(
        client, schema_name, create, seed, steps, final):
    sn = schema_name
    for t in ("ac", "tx"):
        client.execute_sql(f"{create}; INSERT INTO {{t}} VALUES {seed}".format(t=t), schema_name=sn)
    sqls, counts = [s for s, _ in steps], [n for _, n in steps]

    ac = [client.execute_sql(s.format(t="ac"), schema_name=sn)[0]["count"] for s in sqls]
    res = client.execute_sql("; ".join(["BEGIN", *sqls, "COMMIT"]).format(t="tx"), schema_name=sn)
    assert ac == [r["count"] for r in res[1:-1]] == counts
    assert bag(scanned(client, sn, "ac")) == bag(scanned(client, sn, "tx")) == dict.fromkeys(final, 1)


def test_a_pk_set_past_the_wire_cap_is_gathered_in_chunks(client, schema_name):
    """Longer than one `ReadSpec` can carry: DML chunks the gather across
    requests rather than declining to a full scan. Absent keys contribute
    nothing, so the count is the rows actually touched."""
    sn = schema_name
    present = [1, 2, 3, MAX_PK_SET_KEYS, MAX_PK_SET_KEYS + 1]
    outside = MAX_PK_SET_KEYS + 2
    client.execute_sql(
        "CREATE TABLE t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        "INSERT INTO t VALUES " + ", ".join(f"({i}, {i})" for i in present + [outside]),
        schema_name=sn)

    keys = ", ".join(str(i) for i in range(1, MAX_PK_SET_KEYS + 2))
    res = client.execute_sql(f"DELETE FROM t WHERE id IN ({keys})", schema_name=sn)
    assert res[0]["count"] == len(present)
    assert bag(scanned(client, sn, "t")) == {(outside, outside): 1}


def test_an_update_over_a_pk_set_reads_back_every_committed_row(client, schema_name):
    """An UPDATE bounded by a PK set reads its committed rows back through the
    PkSet gather, over keys spanning every worker, so a key the gather drops is
    a row that silently keeps its old value."""
    sn, n = schema_name, 400
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        "INSERT INTO t VALUES " + ", ".join(f"({i}, {i * 10})" for i in range(n)),
        schema_name=sn)
    wanted = set(range(0, n, 7))
    in_list = ", ".join(str(i) for i in sorted(wanted))
    client.execute_sql(f"UPDATE t SET v = v + 1 WHERE id IN ({in_list})", schema_name=sn)
    assert bag(scanned(client, sn, "t")) == \
        {(i, i * 10 + (i in wanted)): 1 for i in range(n)}
