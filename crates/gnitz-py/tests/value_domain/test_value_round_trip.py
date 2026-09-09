"""A value survives the write, and the two write paths write the same bytes.

Every column type is reached by two independent encoders: `batch.append(v=x)`
packs it client-side, and `INSERT INTO t VALUES (.., x)` packs it in the
planner. Nothing forces them to agree, so each type is stated once at its range
extremes and asserted **both** against the literal it was written as and against
the other encoder's bytes. A width-generic or sign-blind pack on either side
shows up here and nowhere else.

The extremes are the values that separate those two implementations: a signed
minimum has its high bit set, an unsigned maximum has every bit set, and `-1` is
the value a zero-extension and a sign-extension disagree about. A midpoint
proves nothing either encoder could get wrong.

Asserted on weights, because both failures this file can have — a row written
twice by one path, a row silently truncated into another row's identity — read
as correct from a row set.
"""

import pytest
import gnitz
from _read import bag, rows, scanned

TC = gnitz.TypeCode

U8_MAX, U16_MAX, U32_MAX = 255, 65535, 4294967295
U64_MAX = 18446744073709551615
U128_MAX = (1 << 128) - 1
I8_MIN, I8_MAX = -128, 127
I16_MIN, I16_MAX = -32768, 32767
I32_MIN, I32_MAX = -2147483648, 2147483647
I64_MIN, I64_MAX = -9223372036854775808, 9223372036854775807

# (SQL spelling, TypeCode, values at the extremes of the range, one value just
# outside it). The float values are exactly representable in F32, so the two
# encoders can be compared for equality rather than for nearness.
_TYPES = [
    ("TINYINT", TC.I8, [I8_MIN, -1, 0, I8_MAX], I8_MAX + 1),
    ("TINYINT UNSIGNED", TC.U8, [0, 1, U8_MAX], -1),
    ("SMALLINT", TC.I16, [I16_MIN, -1, 0, I16_MAX], I16_MIN - 1),
    ("SMALLINT UNSIGNED", TC.U16, [0, 1, U16_MAX], U16_MAX + 1),
    ("INT", TC.I32, [I32_MIN, -1, 0, I32_MAX], I32_MAX + 1),
    ("INT UNSIGNED", TC.U32, [0, 1, U32_MAX], U32_MAX + 1),
    ("BIGINT", TC.I64, [I64_MIN, -1, 0, I64_MAX], I64_MIN - 1),
    ("BIGINT UNSIGNED", TC.U64, [0, 1, U64_MAX], U64_MAX + 1),
    ("FLOAT", TC.F32, [0.0, -1.5, 1.5], None),
    ("DOUBLE", TC.F64, [0.0, -1.5, 1.5], None),
]

_IDS = [t[0].replace(" ", "_") for t in _TYPES]


@pytest.mark.parametrize("col_sql,tc,values,_bad", _TYPES, ids=_IDS)
def test_a_payload_value_reads_back_as_written_by_either_encoder(
        client, schema_name, col_sql, tc, values, _bad):
    """The literal is what comes back, and the binary path produces the same
    rows as the SQL one. Comparing the two encoders alone would pass if both
    were wrong the same way, so the literal comparison rides alongside it."""
    sn = schema_name
    ddl = f"CREATE TABLE {{}} (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, v {col_sql} NOT NULL)"
    client.execute_sql(ddl.format("ta"), schema_name=sn)
    client.execute_sql(ddl.format("tb"), schema_name=sn)
    tid_a, schema = client.resolve_table(sn, "ta")
    assert schema.columns[1].type_code == tc

    batch = gnitz.ZSetBatch(schema)
    for pk, v in enumerate(values, start=1):
        batch.append(pk=pk, v=v)
    client.push(tid_a, batch)
    client.execute_sql(
        "INSERT INTO tb VALUES " + ", ".join(f"({pk}, {v!r})" for pk, v in
                                             enumerate(values, start=1)),
        schema_name=sn)

    want = {(pk, v): 1 for pk, v in enumerate(values, start=1)}
    assert bag(scanned(client, sn, "ta"), "pk", "v") == want
    assert bag(scanned(client, sn, "tb"), "pk", "v") == want


@pytest.mark.parametrize("col_sql,tc,values,_bad", _TYPES, ids=_IDS)
def test_a_key_value_reads_back_as_written_by_either_encoder(
        client, schema_name, col_sql, tc, values, _bad):
    """The same agreement on a *key* column, which the payload case cannot
    reach: the order-preserving encoding sign-flips, and the flip lives in the
    PK encoder alone. A key written by one path and probed by the other that
    disagreed on the flip would seek a key no row carries — the failure mode
    that drops a retraction silently rather than raising.

    Float types have no key form, so they only assert the refusal.
    """
    sn = schema_name
    ddl = f"CREATE TABLE {{}} (k {col_sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"
    if tc in (TC.F32, TC.F64):
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(ddl.format("ta"), schema_name=sn)
        return

    client.execute_sql(ddl.format("ta"), schema_name=sn)
    client.execute_sql(ddl.format("tb"), schema_name=sn)
    tid_a, schema = client.resolve_table(sn, "ta")

    batch = gnitz.ZSetBatch(schema)
    for i, k in enumerate(values):
        batch.append(k=k, v=i)
    client.push(tid_a, batch)
    client.execute_sql(
        "INSERT INTO tb VALUES " + ", ".join(f"({k!r}, {i})" for i, k in enumerate(values)),
        schema_name=sn)

    want = {(k, i): 1 for i, k in enumerate(values)}
    assert bag(scanned(client, sn, "ta"), "k", "v") == want
    assert bag(scanned(client, sn, "tb"), "k", "v") == want

    # Each key addresses its own row through the seek path, so the encoder the
    # planner uses for a WHERE literal agrees with the one that wrote the key.
    for i, k in enumerate(values):
        assert bag(rows(client, sn, f"SELECT v FROM ta WHERE k = {k!r}")) == {(i,): 1}


# The float rows carry no boundary: every finite literal is representable, so
# there is nothing for the range check to refuse.
_BOUNDED = [(sql, values, bad) for sql, _tc, values, bad in _TYPES if bad is not None]


@pytest.mark.parametrize("col_sql,values,bad", _BOUNDED,
                         ids=[t[0].replace(" ", "_") for t in _BOUNDED])
def test_a_value_past_the_range_is_refused_not_truncated(
        client, schema_name, col_sql, values, bad):
    """One past the boundary has to be rejected: truncating it would land it on
    another row's identity, and a wrapped value is indistinguishable from one
    that was written deliberately."""
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, v {col_sql} NOT NULL)",
        schema_name=sn)
    client.execute_sql(f"INSERT INTO t VALUES (1, {values[-1]})", schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(f"INSERT INTO t VALUES (2, {bad})", schema_name=sn)
    # The in-range row is untouched: the refusal is not a partial apply.
    assert bag(scanned(client, sn, "t"), "pk", "v") == {(1, values[-1]): 1}


# ---------------------------------------------------------------------------
# The wide and variable-width types, whose extremes are not integers
# ---------------------------------------------------------------------------


def test_a_wide_key_round_trips_at_both_sides_of_the_u64_boundary(
        client, schema_name):
    """A U128 key is the only one whose value crosses a *word* boundary, so the
    values that matter are the ones where the high word turns non-zero. Each is
    also probed, because a key that stores correctly but hashes on its low word
    alone would route the last three to the same worker as the first."""
    sn = schema_name
    keys = [0, 1, U64_MAX, 1 << 64, U128_MAX]
    client.execute_sql(
        "CREATE TABLE t (k DECIMAL(38,0) NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({k}, {i})" for i, k in enumerate(keys)),
        schema_name=sn)

    assert bag(scanned(client, sn, "t"), "k", "v") == {(k, i): 1 for i, k in enumerate(keys)}
    for i, k in enumerate(keys):
        assert bag(rows(client, sn, f"SELECT v FROM t WHERE k = {k}")) == {(i,): 1}
    # A key between two stored ones misses rather than matching its neighbour.
    assert bag(rows(client, sn, f"SELECT v FROM t WHERE k = {(1 << 64) + 1}")) == {}


def test_a_uuid_round_trips_as_its_canonical_string(client, schema_name):
    """A UUID is 16 bytes in and a canonical string out, at both key and payload
    position — including the all-zero and all-`f` values, whose byte images are
    the ones a length- or endianness-slip renders identically."""
    sn = schema_name
    lo = "00000000-0000-0000-0000-000000000000"
    hi = "ffffffff-ffff-ffff-ffff-ffffffffffff"
    mid = "550e8400-e29b-41d4-a716-446655440000"
    client.execute_sql(
        "CREATE TABLE t (k UUID NOT NULL PRIMARY KEY, u UUID NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        f"INSERT INTO t VALUES ('{lo}', '{hi}'), ('{mid}', '{lo}'), ('{hi}', '{mid}')",
        schema_name=sn)

    assert bag(scanned(client, sn, "t"), "k", "u") == {
        (lo, hi): 1, (mid, lo): 1, (hi, mid): 1}
    # A decimal literal spells a UUID too, and pads to the same 16 bytes.
    client.execute_sql(
        "CREATE TABLE d (pk BIGINT NOT NULL PRIMARY KEY, u UUID NOT NULL)",
        schema_name=sn)
    client.execute_sql("INSERT INTO d VALUES (1, 42)", schema_name=sn)
    assert bag(scanned(client, sn, "d"), "u") == {
        ("00000000-0000-0000-0000-00000000002a",): 1}
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("INSERT INTO d VALUES (2, 'not-a-uuid')", schema_name=sn)


def test_a_string_round_trips_at_each_length_class(client, schema_name):
    """A German string stores its content inline up to 12 bytes and on the heap
    beyond, so the length classes are three different reads. The empty string is
    the one a NULL would be confused with if the null bitmap were not consulted.
    """
    sn = schema_name
    vals = ["", "hello", "this_is_a_longer_string"]
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({i}, '{v}')" for i, v in enumerate(vals)),
        schema_name=sn)
    assert bag(scanned(client, sn, "t"), "pk", "s") == {
        (i, v): 1 for i, v in enumerate(vals)}


@pytest.mark.parametrize("col_sql", [
    "TINYINT", "BIGINT", "DOUBLE", "TEXT", "DECIMAL(38,0)", "UUID", "DATE", "TIMESTAMP",
], ids=["i8", "i64", "f64", "string", "u128", "uuid", "date", "timestamp"])
def test_null_survives_beside_a_written_value_in_every_column_family(
        client, schema_name, col_sql):
    """The null bitmap is the sole truth for a NULL, and a NULL payload cell is
    zero-filled — so a read that ignored the bitmap would return the type's zero
    rather than None. One written row per case is what makes that distinguishable:
    the zero row and the NULL row must stay two rows.
    """
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v {col_sql})",
        schema_name=sn)
    zero = {"TEXT": "''", "UUID": "'00000000-0000-0000-0000-000000000000'",
            "DATE": "'1970-01-01'", "TIMESTAMP": "'1970-01-01 00:00:00'"}.get(col_sql, "0")
    client.execute_sql(f"INSERT INTO t VALUES (1, NULL), (2, {zero})", schema_name=sn)

    got = bag(scanned(client, sn, "t"), "pk", "v")
    assert len(got) == 2 and got[(1, None)] == 1
    assert (2, None) not in got, "a written zero must not read back as NULL"
