"""E2E tests for the DML WHERE pushdown: UPDATE and DELETE resolve their target
rows through the shared access-path ladder and a server-side predicate, and
DELETE reads back nothing but the primary keys it will retract.

Two properties are under test throughout.

**The read is bounded and projected.** The bound and the predicate together are
the whole WHERE, so a committed reply row is final — no client-side re-filtering,
whatever the bound shape. That is what makes a wide (128-bit) PK range or an
indexed 128-bit equality servable: the walk applies those conjuncts byte-exactly
and the expression VM, which has no 16-byte register, never sees them.

**Only the transaction's own buffered rows are re-filtered client-side**, because
no server-side walk ever saw them. A bound naming an exact key set restricts the
buffered candidates to those keys and re-imposes only its residual; any looser
bound restricts nothing and must re-impose the full WHERE.

Run with GNITZ_WORKERS=4: the projected read is a `ReadSpec` fanned out across
workers, and a single-worker run skips the concatenation entirely.

    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/mutation_pattern/test_dml_where_pushdown.py -v
"""


import pytest
import gnitz
from _read import rows


# `ReadSpec`'s wire cap on a `pk IN (…)` gather; DML chunks past it.
MAX_PK_SET_KEYS = 65_536

U64_MAX = (1 << 64) - 1
I64_MIN = -(1 << 63)
UUID_A = "550e8400-e29b-41d4-a716-446655440000"
UUID_B = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"




def _sql(client, schema_name, *statements):
    return client.execute_sql("; ".join(statements), schema_name=schema_name)


def _count(client, schema_name, table="t"):
    return rows(client, schema_name, f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]


# ---------------------------------------------------------------------------
# The projected DELETE read: the payload never crosses the wire, and the rows
# it does not touch come back byte-identical.
# ---------------------------------------------------------------------------


def test_delete_by_predicate_leaves_a_text_payload_untouched(client, schema_name):
    """DELETE reads keys only, so the TEXT column is never fetched — and every
    surviving row must still carry it verbatim."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, v BIGINT NOT NULL, s TEXT NOT NULL)",
        schema_name=schema_name,
    )
    want = {i: ("kept" if i % 3 else "doomed") + "-" * i for i in range(1, 40)}
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({i}, {i % 3}, '{v}')" for i, v in want.items()),
        schema_name=schema_name)

    res = client.execute_sql("DELETE FROM t WHERE v = 0", schema_name=schema_name)
    assert res[0]["count"] == len([i for i in want if i % 3 == 0])

    survivors = {r["id"]: r["s"] for r in rows(client, schema_name, "SELECT id, s FROM t")}
    assert survivors == {i: s for i, s in want.items() if i % 3}


def test_delete_by_predicate_on_a_compound_pk_table(client, schema_name):
    """The PK-only reply carries both key columns, at their own byte offsets."""
    client.execute_sql(
        "CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
        "v BIGINT NOT NULL, s TEXT NOT NULL, PRIMARY KEY (a, b))",
        schema_name=schema_name,
    )
    seed = [(a, b, a * 10 + b) for a in range(1, 4) for b in range(1, 4)]
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({a}, {b}, {v}, 'p{a}{b}')" for a, b, v in seed),
        schema_name=schema_name)

    res = client.execute_sql("DELETE FROM t WHERE v = 22", schema_name=schema_name)
    assert res[0]["count"] == 1
    got = sorted((r["a"], r["b"]) for r in rows(client, schema_name, "SELECT a, b FROM t"))
    assert got == sorted((a, b) for a, b, v in seed if v != 22)


def test_delete_by_predicate_after_drop_column(client, schema_name):
    """A dropped column leaves a hidden physical slot. The key reply names only
    the PK columns, so the hidden slot cannot leak into it or shift it."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, gone BIGINT NOT NULL, v BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({i}, {i * 100}, {i % 2})" for i in range(1, 7)),
        schema_name=schema_name)
    client.execute_sql("ALTER TABLE t DROP COLUMN gone", schema_name=schema_name)

    res = client.execute_sql("DELETE FROM t WHERE v = 1", schema_name=schema_name)
    assert res[0]["count"] == 3
    got = sorted((r["id"], r["v"]) for r in rows(client, schema_name, "SELECT * FROM t"))
    assert got == [(2, 0), (4, 0), (6, 0)]


# ---------------------------------------------------------------------------
# Conjuncts the access recognizers consume but the expression VM cannot compile.
# Each runs inside a transaction that has ALREADY written the table, which is
# where a client-side re-filter would be forced to compile them.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pk_sql,key,other",
    [
        ("BIGINT UNSIGNED", str(U64_MAX), "1"),
        ("BIGINT", str(I64_MIN), "-1"),
        ("UUID", f"'{UUID_A}'", f"'{UUID_B}'"),
    ],
)
def test_wide_key_equality_in_a_written_transaction(client, schema_name, pk_sql, key, other):
    """A key literal no compiled predicate can carry: the bound applies it
    byte-exactly, and the buffered candidates are restricted to that key, so
    nothing reaches the VM's wide-literal gate."""
    client.execute_sql(
        f"CREATE TABLE t (id {pk_sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(f"INSERT INTO t VALUES ({key}, 1), ({other}, 2)", schema_name=schema_name)

    # The transaction writes the table first, so the net map is non-empty for
    # every statement that follows.
    res = _sql(
        client,
        schema_name,
        "BEGIN",
        f"UPDATE t SET v = 7 WHERE id = {other}",
        f"UPDATE t SET v = 9 WHERE id = {key}",
        "COMMIT",
    )
    assert res[2]["count"] == 1, "the wide-key UPDATE must find its row"
    assert sorted(r["v"] for r in rows(client, schema_name, "SELECT v FROM t")) == [7, 9]

    res = _sql(
        client,
        schema_name,
        "BEGIN",
        f"UPDATE t SET v = 8 WHERE id = {other}",
        f"DELETE FROM t WHERE id = {key}",
        "COMMIT",
    )
    assert res[2]["count"] == 1, "the wide-key DELETE must find its row"
    assert _count(client, schema_name) == 1


def test_uuid_in_list_in_a_written_transaction(client, schema_name):
    """The gather form of the same shape: a UUID `IN (…)` list is consumed into
    an exact key set, and its residual is empty."""
    client.execute_sql(
        "CREATE TABLE t (id UUID NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(f"INSERT INTO t VALUES ('{UUID_A}', 1), ('{UUID_B}', 2)", schema_name=schema_name)

    res = _sql(
        client,
        schema_name,
        "BEGIN",
        f"UPDATE t SET v = 5 WHERE id IN ('{UUID_B}')",
        f"DELETE FROM t WHERE id IN ('{UUID_A}')",
        "COMMIT",
    )
    assert res[2]["count"] == 1
    assert [r["v"] for r in rows(client, schema_name, "SELECT v FROM t")] == [5]


# ---------------------------------------------------------------------------
# A wide PK range: servable only because the walk applies it, never the VM.
# ---------------------------------------------------------------------------


def _u128_table(client, schema_name):
    client.execute_sql(
        "CREATE TABLE t (id DECIMAL(38,0) NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql("INSERT INTO t VALUES " + ", ".join(f"({i}, {i})" for i in range(1, 11)),
                       schema_name=schema_name)


def test_wide_pk_range_delete_in_autocommit(client, schema_name):
    """`WHERE u128_pk > 5` is a byte-exact PK walk with an empty residual, so in
    autocommit nothing is compiled client-side at all."""
    _u128_table(client, schema_name)
    res = client.execute_sql("DELETE FROM t WHERE id > 5", schema_name=schema_name)
    assert res[0]["count"] == 5
    assert sorted(int(r["id"]) for r in rows(client, schema_name, "SELECT id FROM t")) == [1, 2, 3, 4, 5]


def test_wide_pk_range_delete_over_a_tombstone_only_transaction(client, schema_name):
    """The range pins no single key, so a buffered row would have to face the
    FULL WHERE — which the VM cannot compile for a 128-bit column. A transaction
    holding only a tombstone has no such row, and must not pay for one."""
    _u128_table(client, schema_name)
    res = _sql(
        client,
        schema_name,
        "BEGIN",
        "DELETE FROM t WHERE id = 1",  # a tombstone, no Present row
        "DELETE FROM t WHERE id > 5",
        "COMMIT",
    )
    assert res[2]["count"] == 5
    assert sorted(int(r["id"]) for r in rows(client, schema_name, "SELECT id FROM t")) == [2, 3, 4, 5]


def test_wide_pk_range_still_rejects_over_a_buffered_row(client, schema_name):
    """The exposure shrinks; it does not vanish. A buffered `Present` row under an
    unpinned wide-PK bound genuinely needs the whole WHERE compiled, and that is
    the shape the expression VM refuses."""
    _u128_table(client, schema_name)
    with pytest.raises(gnitz.GnitzError) as e:
        _sql(
            client,
            schema_name,
            "BEGIN",
            "INSERT INTO t VALUES (99, 99)",
            "DELETE FROM t WHERE id > 5",
            "COMMIT",
        )
    assert "128" in str(e.value), str(e.value)
    # The BEGIN opened in this same call, so the refusal auto-rolled it back:
    # nothing was committed and no buffer is left stranded.
    assert sorted(int(r["id"]) for r in rows(client, schema_name, "SELECT id FROM t")) == \
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("ROLLBACK", schema_name=schema_name)


# ---------------------------------------------------------------------------
# Buffered rows against each bound shape.
# ---------------------------------------------------------------------------


def test_pk_range_with_buffered_rows_straddling_the_cut(client, schema_name):
    """`id > 5 AND flag = 1`: the walk applies the PK cut and the predicate the
    flag, but a buffered row saw neither — so both conjuncts are re-imposed on
    it, and the one below the cut must not be swept in."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, flag BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql("INSERT INTO t VALUES (1, 1), (7, 1), (8, 0)", schema_name=schema_name)

    res = _sql(
        client,
        schema_name,
        "BEGIN",
        "INSERT INTO t VALUES (4, 1), (9, 1)",  # one below the cut, one above
        "DELETE FROM t WHERE id > 5 AND flag = 1",
        "COMMIT",
    )
    assert res[2]["count"] == 2, "committed id=7 and buffered id=9; NOT buffered id=4"
    got = sorted((r["id"], r["flag"]) for r in rows(client, schema_name, "SELECT * FROM t"))
    assert got == [(1, 1), (4, 1), (8, 0)]


def test_compound_pk_prefix_and_range_bounds(client, schema_name):
    """A bare compound-PK prefix names a key *group*, not a key: it bounds the
    walk but must not restrict the buffered candidates to one key."""
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (a, b))",
        schema_name=schema_name,
    )
    client.execute_sql("INSERT INTO t VALUES (1, 1, 0), (1, 9, 0), (2, 9, 0)", schema_name=schema_name)

    # `a = 1 AND b > 5` — an equality prefix plus a range on the next PK column.
    res = _sql(
        client,
        schema_name,
        "BEGIN",
        "INSERT INTO t VALUES (1, 7, 0)",
        "DELETE FROM t WHERE a = 1 AND b > 5",
        "COMMIT",
    )
    assert res[2]["count"] == 2, "committed (1,9) and buffered (1,7)"
    assert sorted((r["a"], r["b"]) for r in rows(client, schema_name, "SELECT a, b FROM t")) == [(1, 1), (2, 9)]

    # `a > 1` — a range on the leading PK column, no equality at all.
    res = _sql(
        client,
        schema_name,
        "BEGIN",
        "INSERT INTO t VALUES (5, 5, 0)",
        "DELETE FROM t WHERE a > 1",
        "COMMIT",
    )
    assert res[2]["count"] == 2, "committed (2,9) and buffered (5,5)"
    assert sorted((r["a"], r["b"]) for r in rows(client, schema_name, "SELECT a, b FROM t")) == [(1, 1)]


def test_an_empty_residual_does_not_sweep_an_unrelated_buffered_row(client, schema_name):
    """`WHERE id = 5` pins one key and leaves nothing residual. The key
    restriction — not the (empty) residual — is what keeps an unrelated
    transaction-born row out of the retraction."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql("INSERT INTO t VALUES (5, 50)", schema_name=schema_name)

    res = _sql(
        client,
        schema_name,
        "BEGIN",
        "INSERT INTO t VALUES (11, 110)",
        "DELETE FROM t WHERE id = 5",
        "COMMIT",
    )
    assert res[2]["count"] == 1
    assert sorted((r["id"], r["v"]) for r in rows(client, schema_name, "SELECT * FROM t")) == [(11, 110)]


def test_transaction_mixed_buffered_and_committed_delete(client, schema_name):
    """One DELETE over all three kinds of row: a buffered override, a
    buffered-then-deleted row, and an untouched committed one."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql("INSERT INTO t VALUES (1, 7), (2, 7), (3, 0)", schema_name=schema_name)

    res = _sql(
        client,
        schema_name,
        "BEGIN",
        "INSERT INTO t VALUES (4, 7)",       # transaction-born, matches
        "UPDATE t SET v = 7 WHERE id = 3",   # buffered into the match set
        "DELETE FROM t WHERE id = 2",        # buffered tombstone; also matches v = 7
        "DELETE FROM t WHERE v = 7",         # 1 (committed), 3 (buffered), 4 (born)
        "COMMIT",
    )
    assert res[4]["count"] == 3, "the tombstoned row is already gone, not re-retracted"
    assert _count(client, schema_name) == 0


def test_update_read_your_own_writes_is_unchanged(client, schema_name):
    """The buffered payload, not the committed one, decides what an UPDATE
    matches and what it carries through."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, w BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql("INSERT INTO t VALUES (1, 10, 100)", schema_name=schema_name)

    res = _sql(
        client,
        schema_name,
        "BEGIN",
        "UPDATE t SET v = 5 WHERE id = 1",
        "UPDATE t SET w = 1 WHERE v = 10",  # the committed value: matches nothing
        "UPDATE t SET w = 2 WHERE v = 5",   # the buffered value: matches
        "COMMIT",
    )
    assert (res[2]["count"], res[3]["count"]) == (0, 1)
    row = rows(client, schema_name, "SELECT * FROM t")[0]
    assert (row["id"], row["v"], row["w"]) == (1, 5, 2)


# ---------------------------------------------------------------------------
# A `pk IN (…)` list past the wire's gather cap.
# ---------------------------------------------------------------------------


def test_delete_pk_in_past_the_wire_key_cap(client, schema_name):
    """Longer than one `ReadSpec` can carry: DML chunks the gather across
    requests rather than declining to a full scan. Absent keys contribute
    nothing, so the count is the rows actually touched."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    present = [1, 2, 3, MAX_PK_SET_KEYS, MAX_PK_SET_KEYS + 1]
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({i}, {i})" for i in present),
        schema_name=schema_name,
    )

    keys = ", ".join(str(i) for i in range(1, MAX_PK_SET_KEYS + 2))
    res = client.execute_sql(f"DELETE FROM t WHERE id IN ({keys})", schema_name=schema_name)
    assert res[0]["count"] == len(present)
    assert _count(client, schema_name) == 0
