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

One table carries every type at once, which is what the cross-encoder claim is
about: one push and one INSERT covering the whole set. A key column is
per-table, so the key arms keep one table each.

Asserted on weights, because both failures this file can have — a row written
twice by one path, a row silently truncated into another row's identity — read
as correct from a row set.
"""

from datetime import date, datetime

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

_COLS = [f"v{i}" for i in range(len(_TYPES))]
_PAYLOAD = ", ".join(f"{c} {sql} NOT NULL" for c, (sql, *_) in zip(_COLS, _TYPES))

# One row per position of the longest extreme list; a shorter list repeats, so
# every type still writes each of its own extremes.
_ROWS = [tuple(vals[i % len(vals)] for _sql, _tc, vals, _bad in _TYPES)
         for i in range(max(len(v) for _sql, _tc, v, _bad in _TYPES))]

# The value just past each type's range, where it has one.
_BOUNDED = [(j, bad) for j, (_sql, _tc, _v, bad) in enumerate(_TYPES) if bad is not None]


def _values(rows, first=0):
    """`(pk, v0, …), …` for an INSERT, keys numbered from `first`."""
    return ", ".join("(" + ", ".join(repr(v) for v in (pk,) + tuple(row)) + ")"
                     for pk, row in enumerate(rows, start=first))


def test_a_payload_value_reads_back_as_written_by_either_encoder(client, schema_name):
    """The literal is what comes back, and the binary path produces the same
    rows as the SQL one. Comparing the two encoders alone would pass if both
    were wrong the same way, so the literal comparison rides alongside it."""
    sn = schema_name
    ddl = f"CREATE TABLE {{}} (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, {_PAYLOAD})"
    client.execute_sql(ddl.format("ta"), schema_name=sn)
    client.execute_sql(ddl.format("tb"), schema_name=sn)
    tid_a, schema = client.resolve_table(sn, "ta")
    assert [c.type_code for c in schema.columns[1:]] == [tc for _s, tc, *_ in _TYPES]

    batch = gnitz.ZSetBatch(schema)
    for pk, row in enumerate(_ROWS):
        batch.append(pk=pk, **dict(zip(_COLS, row)))
    client.push(tid_a, batch)
    client.execute_sql("INSERT INTO tb VALUES " + _values(_ROWS), schema_name=sn)

    want = {(pk,) + row: 1 for pk, row in enumerate(_ROWS)}
    assert bag(scanned(client, sn, "ta"), "pk", *_COLS) == want
    assert bag(scanned(client, sn, "tb"), "pk", *_COLS) == want


def test_a_payload_retracts_against_its_own_declared_width(client, schema_name):
    """A retraction is matched on (key, payload), so the payload comparison reads
    each column at its declared width. A 1- or 2-byte column read as 8 takes the
    neighbouring bytes with it and the retraction then matches nothing, leaving
    the old row at weight 1 — which a row set reads as correct."""
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, {_PAYLOAD})",
        schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES " + _values(_ROWS), schema_name=sn)
    assert bag(scanned(client, sn, "v"), "pk", *_COLS) == \
        {(pk,) + row: 1 for pk, row in enumerate(_ROWS)}

    client.execute_sql("DELETE FROM t WHERE pk = 0", schema_name=sn)
    # An UPDATE retracts the old payload and inserts the new, so it runs the same
    # comparison; `v0` is the narrowest column, where a too-wide read starts.
    client.execute_sql(f"UPDATE t SET v0 = {_ROWS[0][0]!r} WHERE pk = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "pk", *_COLS) == \
        {(1, _ROWS[0][0]) + _ROWS[1][1:]: 1} | \
        {(pk,) + row: 1 for pk, row in enumerate(_ROWS) if pk > 1}


def test_a_value_past_the_range_is_refused_not_truncated(client, schema_name):
    """One past the boundary has to be rejected: truncating it would land it on
    another row's identity, and a wrapped value is indistinguishable from one
    that was written deliberately. The in-range row survives every refusal, so
    none of them is a partial apply."""
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, {_PAYLOAD})",
        schema_name=sn)
    good = _ROWS[0]
    client.execute_sql("INSERT INTO t VALUES " + _values([good], first=1), schema_name=sn)

    for j, bad in _BOUNDED:
        row = good[:j] + (bad,) + good[j + 1:]
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO t VALUES " + _values([row], first=2),
                               schema_name=sn)

    assert bag(scanned(client, sn, "t"), "pk", *_COLS) == {(1,) + good: 1}


# A float has no key form; the refusal is `test_type_catalog.py`'s.
_KEYABLE = [(sql, values) for sql, tc, values, _bad in _TYPES
            if tc not in (TC.F32, TC.F64)]


@pytest.mark.parametrize("col_sql,values", _KEYABLE,
                         ids=[t[0].replace(" ", "_") for t in _KEYABLE])
def test_a_key_value_reads_back_as_written_by_either_encoder(
        client, schema_name, col_sql, values):
    """The same agreement on a *key* column, which the payload case cannot
    reach: the order-preserving encoding sign-flips, and the flip lives in the
    PK encoder alone. A key written by one path and probed by the other that
    disagreed on the flip would seek a key no row carries — the failure mode
    that drops a retraction silently rather than raising.
    """
    sn = schema_name
    ddl = f"CREATE TABLE {{}} (k {col_sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"
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


# (SQL spelling, the type's own zero as a literal, that zero in Python). The
# integer widths are `test_null_semantics.py`'s; these are the families whose
# zero is not the integer 0.
_FAMILIES = [
    ("DOUBLE", "0.0", 0.0),
    ("TEXT", "''", ""),
    ("DECIMAL(38,0)", "0", 0),
    ("UUID", "'00000000-0000-0000-0000-000000000000'",
     "00000000-0000-0000-0000-000000000000"),
    ("DATE", "'1970-01-01'", date(1970, 1, 1)),
    ("TIMESTAMP", "'1970-01-01 00:00:00'", datetime(1970, 1, 1)),
]


def test_null_survives_beside_a_written_value_in_every_column_family(
        client, schema_name):
    """The null bitmap is the sole truth for a NULL, and a NULL payload cell is
    zero-filled, so the type's own zero is the value a read that ignored the
    bitmap would return. Both rows are asserted by value, not by non-nullness."""
    sn = schema_name
    cols = [f"v{i}" for i in range(len(_FAMILIES))]
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, " +
        ", ".join(f"{c} {sql}" for c, (sql, _, _) in zip(cols, _FAMILIES)) + ")",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, " + ", ".join(["NULL"] * len(_FAMILIES)) + "), "
        "(2, " + ", ".join(lit for _, lit, _ in _FAMILIES) + ")", schema_name=sn)

    assert bag(scanned(client, sn, "t"), "pk", *cols) == {
        (1,) + (None,) * len(_FAMILIES): 1,
        (2,) + tuple(zero for _, _, zero in _FAMILIES): 1}


def test_a_string_round_trips_at_each_length_class(client, schema_name):
    """A German string stores its content inline up to 12 bytes and on the heap
    beyond, so the values straddle that boundary exactly. The empty string is
    the one a NULL would be confused with if the null bitmap were not consulted.
    """
    sn = schema_name
    vals = ["", "exactly12chr", "thirteen-char"]
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({i}, '{v}')" for i, v in enumerate(vals)),
        schema_name=sn)
    assert bag(scanned(client, sn, "t"), "pk", "s") == {
        (i, v): 1 for i, v in enumerate(vals)}


# (SQL type, four values in ascending order). The integer widths straddle the
# sign and the byte boundaries; FLOAT widens to an F64 extremum, exactly since
# the values are dyadic; TEXT shares a prefix past the inline length; UUID and
# DECIMAL(38,0) order on all 16 bytes, the UUIDs differing only past byte 8.
_ORDERED = [
    ("TINYINT", [I8_MIN, -5, 0, I8_MAX]),
    ("SMALLINT", [I16_MIN, -1, 256, I16_MAX]),
    ("INT UNSIGNED", [0, 100, 2_147_483_648, U32_MAX]),
    ("BIGINT", [I64_MIN, -1, 0, I64_MAX]),
    ("FLOAT", [-2.25, 0.5, 1.5, 4.0]),
    ("TEXT", ["", "shared/prefix/a", "shared/prefix/ab", "shared/prefix/abd"]),
    ("UUID", ["550e8400-e29b-41d4-a716-446655430000", "550e8400-e29b-41d4-a716-446655440000",
              "550e8400-e29b-41d4-a716-446655440001", "ffffffff-ffff-ffff-ffff-ffffffffffff"]),
    ("DECIMAL(38,0)", [0, 5, (1 << 64) + 1, 10 ** 38 - 1]),
]
# Signed twins, so a group key compared without its sign reads a neighbour's.
_GROUPS = [(-5, -7), (5, 7), (-5, 7), (5, -7)]
# The PK column's values, straddling the sign and the high bytes.
_KEY_IDS = [-5, 1, 256, 65536]


def test_min_max_select_and_recede_by_each_familys_own_order(client, schema_name):
    """MIN/MAX select by the column's own order and decode the selected value
    back out of the value index. Each group holds a different three of the four
    positions, so an extreme leaking across groups reads another group's value.
    `id` is a PRIMARY KEY column, stored only as order-preserving bytes, so its
    extremes must decode before they compare. Retracting both of a group's
    extreme carriers recedes it to the survivor, and emptying the table empties
    every view."""
    sn = schema_name
    cols = [f"c{j}" for j in range(len(_ORDERED))] + ["id"]
    families = [vals for _, vals in _ORDERED] + [_KEY_IDS]
    client.execute_sql(
        "CREATE TABLE t (ka INT NOT NULL, kb BIGINT NOT NULL, id BIGINT NOT NULL, "
        + "".join(f"c{j} {sql} NOT NULL, " for j, (sql, _) in enumerate(_ORDERED))
        + "PRIMARY KEY (ka, kb, id)); "
        + "; ".join(f"CREATE VIEW m_{c} AS SELECT ka, kb, MIN({c}) AS lo, MAX({c}) AS hi "
                    "FROM t GROUP BY ka, kb" for c in cols), schema_name=sn)
    # One statement per position, holding it in every group but its own.
    for pos in range(4):
        client.execute_sql("INSERT INTO t VALUES " + ", ".join(
            "(" + ", ".join(map(repr, (*_GROUPS[g], _KEY_IDS[pos], *(v[pos] for _, v in _ORDERED)))) + ")"
            for g in range(4) if g != pos), schema_name=sn)

    def expect(held):
        for c, vals in zip(cols, families):
            assert bag(scanned(client, sn, f"m_{c}"), "ka", "kb", "lo", "hi") == {
                (*_GROUPS[g], vals[min(ps)], vals[max(ps)]): 1 for g, ps in held.items()}, c

    held = {g: [p for p in range(4) if p != g] for g in range(4)}
    expect(held)
    ka, kb = _GROUPS[1]
    client.execute_sql(
        f"DELETE FROM t WHERE ka = {ka} AND kb = {kb} AND id IN ({_KEY_IDS[0]}, {_KEY_IDS[3]})",
        schema_name=sn)
    held[1] = [2]
    expect(held)
    client.execute_sql("DELETE FROM t", schema_name=sn)
    expect({})
