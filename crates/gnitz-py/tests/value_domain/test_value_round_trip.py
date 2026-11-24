"""A value survives the write, the two write paths write the same bytes, and a
NULL is none of a type's values.

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
twice by one path, a retraction that matches no stored row — read as correct
from a row set.
"""

from datetime import date, datetime
from decimal import Decimal

import pytest
import gnitz
from _read import bag, rows, scanned
from _sql import insert, lit

TC = gnitz.TypeCode

U8_MAX, U16_MAX, U32_MAX = 255, 65535, 4294967295
U64_MAX = 18446744073709551615
U128_MAX = (1 << 128) - 1
I8_MIN, I8_MAX = -128, 127
I16_MIN, I16_MAX = -32768, 32767
I32_MIN, I32_MAX = -2147483648, 2147483647
I64_MIN, I64_MAX = -9223372036854775808, 9223372036854775807

# (SQL spelling, TypeCode, values ascending, the type's zero among them). The
# integer widths straddle the sign and their high bit; the float values are
# exactly representable in F32, so the two encoders can be compared for equality
# rather than for nearness; the strings share a prefix with an inline value past
# the 12-byte inline limit; the 16-byte types cross the word boundary and the
# UUIDs differ only in their last byte; the temporal values span the epoch, the
# sign of the integer they are stored as.
_TYPES = [
    ("TINYINT", TC.I8, [I8_MIN, -1, 0, I8_MAX]),
    ("TINYINT UNSIGNED", TC.U8, [0, 1, 128, U8_MAX]),
    ("SMALLINT", TC.I16, [I16_MIN, -1, 0, 256, I16_MAX]),
    ("SMALLINT UNSIGNED", TC.U16, [0, 1, 32768, U16_MAX]),
    ("INT", TC.I32, [I32_MIN, -1, 0, I32_MAX]),
    ("INT UNSIGNED", TC.U32, [0, 1, 2**31, U32_MAX]),
    ("BIGINT", TC.I64, [I64_MIN, -1, 0, I64_MAX]),
    ("BIGINT UNSIGNED", TC.U64, [0, 1, 2**63, U64_MAX]),
    ("UINT128", TC.U128, [0, 1, U64_MAX, 1 << 64, U128_MAX]),
    ("INT128", TC.I128, [-(1 << 127), -1, 0, (1 << 127) - 1]),
    ("FLOAT", TC.F32, [-2.25, 0.0, 0.5, 4.0]),
    ("DOUBLE", TC.F64, [-2.25, 0.0, 0.5, 4.0]),
    ("TEXT", TC.STRING, ["", "exactly12chr", "exactly12chr-a", "exactly12chr-ab"]),
    ("BLOB", TC.BLOB, [b"", b"exactly12chr", b"exactly12chr-a", b"exactly12chr-ab"]),
    ("UUID", TC.UUID, ["00000000-0000-0000-0000-000000000000",
                       "550e8400-e29b-41d4-a716-446655440000",
                       "550e8400-e29b-41d4-a716-446655440001",
                       "ffffffff-ffff-ffff-ffff-ffffffffffff"]),
    ("DECIMAL(18, 2)", TC.DECIMAL, [Decimal("-92233720368547758.08"), Decimal("-0.01"),
                                    Decimal("0.00"), Decimal("92233720368547758.07")]),
    ("DATE", TC.DATE, [date(1, 1, 1), date(1969, 12, 31), date(1970, 1, 1), date(9999, 12, 31)]),
    ("TIMESTAMP", TC.TIMESTAMP, [datetime(1, 1, 1), datetime(1969, 12, 31, 23, 59, 59, 999999),
                                 datetime(1970, 1, 1), datetime(9999, 12, 31, 23, 59, 59, 999999)]),
    ("BOOLEAN", TC.BOOLEAN, [False, True]),
]

_COLS = [f"v{i}" for i in range(len(_TYPES))]
_PAYLOAD = ", ".join(f"{c} {sql} NOT NULL" for c, (sql, _, _) in zip(_COLS, _TYPES))

# `(pk, v0, …)`, one row per position of the longest value list; a shorter list
# repeats, so every type still writes each of its own values.
_ROWS = [(pk, *(vals[pk % len(vals)] for _, _, vals in _TYPES))
         for pk in range(max(len(vals) for _, _, vals in _TYPES))]


def test_a_payload_value_reads_back_as_written_by_either_encoder(client):
    """The literal is what comes back, and the binary path writes the same bytes
    as the SQL one: an EXCEPT of the two tables, in both directions, keys each
    row by every column a set op can key — all but the floats — and so is empty
    only if the encoders agree bit for bit on them.
    Comparing the two encoders alone would pass if both were wrong the same way,
    so the literal comparison rides alongside it. `1 == True`,
    `Decimal("1.5") == 1.5` and a `datetime` is a `date`, so the Python type each
    value reads back as is asserted beside its value.

    A retraction is then matched on (key, payload), so the payload comparison
    reads each column at its declared width. A 1- or 2-byte column read as 8
    takes the neighbouring bytes with it and the retraction then matches
    nothing, leaving the old row at weight 1 — which a row set reads as
    correct."""
    assert {tc for _, tc, _ in _TYPES} == set(TC), "a column type is missing"
    keyed = [j for j, (_, tc, _) in enumerate(_TYPES) if tc not in (TC.F32, TC.F64)]
    cols = ", ".join(_COLS[j] for j in keyed)
    ddl = f"CREATE TABLE {{}} (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, {_PAYLOAD})"
    client.execute_sql(
        f"{ddl.format('ta')}; {ddl.format('tb')}; "
        f"CREATE VIEW xab AS SELECT {cols} FROM ta EXCEPT SELECT {cols} FROM tb; "
        f"CREATE VIEW xba AS SELECT {cols} FROM tb EXCEPT SELECT {cols} FROM ta")
    tid_a, schema = client.resolve_table("ta")
    assert [c.type_code for c in schema.columns[1:]] == [tc for _, tc, _ in _TYPES]

    client.push(tid_a, gnitz.ZSetBatch(schema).extend(dict(zip(("pk", *_COLS), r)) for r in _ROWS))
    insert(client, "tb", _ROWS)

    for t in ("ta", "tb"):
        got = scanned(client, t)
        assert bag(got) == dict.fromkeys(_ROWS, 1), t
        assert {tuple(map(type, r)) for r in got} == {tuple(map(type, _ROWS[0]))}, t
    assert bag(scanned(client, "xab")) == bag(scanned(client, "xba")) == {}

    client.execute_sql("DELETE FROM tb WHERE pk = 0")
    # An UPDATE retracts the old payload and inserts the new, so it runs the same
    # comparison; `v0` is the narrowest column, where a too-wide read starts.
    client.execute_sql(f"UPDATE tb SET v0 = {lit(_ROWS[0][1])} WHERE pk = 1")
    moved = (1, _ROWS[0][1], *_ROWS[1][2:])
    assert bag(scanned(client, "tb")) == {moved: 1} | dict.fromkeys(_ROWS[2:], 1)
    def proj(r):
        return tuple(r[1 + j] for j in keyed)

    assert bag(scanned(client, "xab")) == {proj(_ROWS[0]): 1, proj(_ROWS[1]): 1}
    assert bag(scanned(client, "xba")) == {proj(moved): 1}


# What a read that ignored the null bitmap would return for a NULL, per type.
_ZEROS = (0, "", b"", "00000000-0000-0000-0000-000000000000", date(1970, 1, 1), datetime(1970, 1, 1))


def test_a_null_is_distinct_from_its_types_zero_in_every_column_type(client):
    """A NULL payload cell is zero-filled and the null bitmap is the sole truth,
    so each type's NULL is held beside its own zero: `v = zero` must admit the
    written zero and exclude the NULL, and a read hands the NULL back as `None`.
    `IS NULL` and `IS NOT NULL` are asserted as exact complements, and `v > zero`
    rides alongside because the ordering load is the other per-width part of the
    read. A 128-bit column takes no value expression, so it gets the two null
    tests alone, which read the bitmap without loading the value."""
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, {_PAYLOAD.replace(' NOT NULL', '')})")
    seed = _ROWS + [(len(_ROWS), *[None] * len(_TYPES))]
    insert(client, "t", seed)
    assert bag(scanned(client, "t")) == dict.fromkeys(seed, 1)

    for j, (sql, tc, vals) in enumerate(_TYPES, start=1):
        zero = next(v for v in vals if v in _ZEROS)
        preds = [("IS NULL", lambda v: v is None), ("IS NOT NULL", lambda v: v is not None)]
        if tc not in (TC.U128, TC.I128, TC.UUID):
            preds += [(f"= {lit(zero)}", lambda v: v is not None and v == zero),
                      (f"> {lit(zero)}", lambda v: v is not None and v > zero)]
        for pred, keep in preds:
            want = {(r[0],): 1 for r in seed if keep(r[j])}
            assert bag(rows(client, f"SELECT pk FROM t WHERE {_COLS[j - 1]} {pred}")) == want, \
                f"{sql} {pred}"


# Neither a float, a string nor a BLOB has a key form.
_KEYABLE = [(sql, vals) for sql, tc, vals in _TYPES if tc not in (TC.F32, TC.F64, TC.STRING, TC.BLOB)]


@pytest.mark.parametrize("col_sql,vals", _KEYABLE,
                         ids=[sql.split("(")[0].replace(" ", "_") for sql, _ in _KEYABLE])
def test_a_key_value_reads_back_as_written_by_either_encoder(client, col_sql, vals):
    """The same agreement on a *key* column, which the payload case cannot
    reach: the order-preserving encoding sign-flips, and the flip lives in the
    PK encoder alone. A key written by one path and probed by another that
    disagreed on the flip would seek a key no row carries — the failure mode
    that drops a retraction silently rather than raising. So every key is
    probed by the client's seek and by a WHERE literal, and all of them are
    retracted by the client's delete.

    Order is asserted through a range cut at every stored key, never through a
    sorted read: a range on a key column is lowered to a walk over the encoded
    bytes, so the rows it returns state the byte order the encoder produced,
    and a cut at each key separates both a missing flip and a spurious one.
    """
    ddl = f"CREATE TABLE {{}} (k {col_sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)"
    client.execute_sql(f"{ddl.format('ta')}; {ddl.format('tb')}")
    tid, schema = client.resolve_table("ta")
    client.push(tid, gnitz.ZSetBatch(schema).extend({"k": k, "v": i} for i, k in enumerate(vals)))
    insert(client, "tb", [(k, i) for i, k in enumerate(vals)])

    for t in ("ta", "tb"):
        got = scanned(client, t)
        assert bag(got) == {(k, i): 1 for i, k in enumerate(vals)}, t
        assert {type(r.k) for r in got} == {type(vals[0])}, t

    for i, k in enumerate(vals):
        assert bag(client.seek(tid, schema, k)) == {(k, i): 1}
        assert bag(rows(client, f"SELECT v FROM ta WHERE k = {lit(k)}")) == {(i,): 1}
        assert bag(rows(client, f"SELECT v FROM ta WHERE k < {lit(k)}")) == \
            {(j,): 1 for j in range(i)}

    client.delete(tid, schema, vals)
    assert bag(scanned(client, "ta")) == {}


# Signed twins, so a group key compared without its sign reads a neighbour's.
_GROUPS = [(-5, -7), (5, 7), (-5, 7), (5, -7)]
# The PK column's value per row of `_ROWS`, straddling the sign and the high bytes.
_KEY_IDS = [-5, 1, 256, 65536, I64_MIN]


def test_min_max_select_and_recede_by_each_types_own_order(client):
    """MIN/MAX select by the column's own order and decode the selected value
    back out of the value index, for every column type. Each group holds all but
    one of the rows, a different one each, so an extreme leaking across groups
    reads another group's value. `id` is a PRIMARY KEY column, stored only as
    order-preserving bytes, so its extremes must decode before they compare.
    Retracting two of a group's rows recedes it to the survivors, and emptying
    the table empties every view."""
    cols = ["id", *_COLS]
    client.execute_sql(
        f"CREATE TABLE t (ka INT NOT NULL, kb BIGINT NOT NULL, id BIGINT NOT NULL, {_PAYLOAD}, "
        "PRIMARY KEY (ka, kb, id)); "
        + "; ".join(f"CREATE VIEW m_{c} AS SELECT ka, kb, MIN({c}) AS lo, MAX({c}) AS hi "
                    "FROM t GROUP BY ka, kb" for c in cols))
    held = {g: [(_KEY_IDS[pk], *vals) for pk, *vals in _ROWS if pk != g] for g in range(len(_GROUPS))}
    # One statement per row, holding it in every group but its own.
    for pk, *vals in _ROWS:
        insert(client, "t", [(*_GROUPS[g], _KEY_IDS[pk], *vals) for g in held if g != pk])

    def expect(held):
        for j, c in enumerate(cols):
            assert bag(scanned(client, f"m_{c}")) == {
                (*_GROUPS[g], min(r[j] for r in rs), max(r[j] for r in rs)): 1
                for g, rs in held.items()}, c

    expect(held)
    (ka, kb), gone = _GROUPS[1], (_KEY_IDS[0], _KEY_IDS[3])
    client.execute_sql(f"DELETE FROM t WHERE ka = {ka} AND kb = {kb} AND id IN {gone}")
    held[1] = [r for r in held[1] if r[0] not in gone]
    expect(held)
    client.execute_sql("DELETE FROM t")
    expect({})
