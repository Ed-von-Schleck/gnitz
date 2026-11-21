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

# (SQL spelling, TypeCode, values at the extremes of the range, ascending). The
# float values are exactly representable in F32, so the two encoders can be
# compared for equality rather than for nearness; the strings straddle the
# 12-byte inline limit; the 16-byte types cross the word boundary; the temporal
# values span the epoch, the sign of the integer they are stored as.
_TYPES = [
    ("TINYINT", TC.I8, [I8_MIN, -1, 0, I8_MAX]),
    ("TINYINT UNSIGNED", TC.U8, [0, 1, U8_MAX]),
    ("SMALLINT", TC.I16, [I16_MIN, -1, 0, I16_MAX]),
    ("SMALLINT UNSIGNED", TC.U16, [0, 1, U16_MAX]),
    ("INT", TC.I32, [I32_MIN, -1, 0, I32_MAX]),
    ("INT UNSIGNED", TC.U32, [0, 1, U32_MAX]),
    ("BIGINT", TC.I64, [I64_MIN, -1, 0, I64_MAX]),
    ("BIGINT UNSIGNED", TC.U64, [0, 1, U64_MAX]),
    ("UINT128", TC.U128, [0, 1, U64_MAX, 1 << 64, U128_MAX]),
    ("INT128", TC.I128, [-(1 << 127), -1, 0, (1 << 127) - 1]),
    ("FLOAT", TC.F32, [-1.5, 0.0, 1.5]),
    ("DOUBLE", TC.F64, [-1.5, 0.0, 1.5]),
    ("TEXT", TC.STRING, ["", "exactly12chr", "thirteen-char"]),
    ("BLOB", TC.BLOB, [b"", b"exactly12chr", b"thirteen-char"]),
    ("UUID", TC.UUID, ["00000000-0000-0000-0000-000000000000",
                       "550e8400-e29b-41d4-a716-446655440000",
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

# `(pk, v0, …)`, one row per position of the longest extreme list; a shorter
# list repeats, so every type still writes each of its own extremes.
_ROWS = [(pk, *(vals[pk % len(vals)] for _, _, vals in _TYPES))
         for pk in range(max(len(vals) for _, _, vals in _TYPES))]


def test_a_payload_value_reads_back_as_written_by_either_encoder(client):
    """The literal is what comes back, and the binary path produces the same
    rows as the SQL one. Comparing the two encoders alone would pass if both
    were wrong the same way, so the literal comparison rides alongside it.

    `1 == True`, `Decimal("1.5") == 1.5` and a `datetime` is a `date`, so the
    Python type each value reads back as is asserted beside its value."""
    assert {tc for _, tc, _ in _TYPES} == set(TC), "a column type is missing"
    ddl = f"CREATE TABLE {{}} (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, {_PAYLOAD})"
    client.execute_sql(f"{ddl.format('ta')}; {ddl.format('tb')}")
    tid_a, schema = client.resolve_table("ta")
    assert [c.type_code for c in schema.columns[1:]] == [tc for _, tc, _ in _TYPES]

    client.push(tid_a, gnitz.ZSetBatch(schema).extend(dict(zip(("pk", *_COLS), r)) for r in _ROWS))
    insert(client, "tb", _ROWS)

    for t in ("ta", "tb"):
        got = scanned(client, t)
        assert bag(got, "pk", *_COLS) == dict.fromkeys(_ROWS, 1), t
        assert {tuple(map(type, r)) for r in got} == {tuple(map(type, _ROWS[0]))}, t


def test_a_payload_retracts_against_its_own_declared_width(client):
    """A retraction is matched on (key, payload), so the payload comparison reads
    each column at its declared width. A 1- or 2-byte column read as 8 takes the
    neighbouring bytes with it and the retraction then matches nothing, leaving
    the old row at weight 1 — which a row set reads as correct."""
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, {_PAYLOAD}); "
        "CREATE VIEW v AS SELECT * FROM t")
    insert(client, "t", _ROWS)
    assert bag(scanned(client, "v"), "pk", *_COLS) == dict.fromkeys(_ROWS, 1)

    client.execute_sql("DELETE FROM t WHERE pk = 0")
    # An UPDATE retracts the old payload and inserts the new, so it runs the same
    # comparison; `v0` is the narrowest column, where a too-wide read starts.
    client.execute_sql(f"UPDATE t SET v0 = {lit(_ROWS[0][1])} WHERE pk = 1")
    assert bag(scanned(client, "v"), "pk", *_COLS) == \
        {(1, _ROWS[0][1], *_ROWS[1][2:]): 1} | dict.fromkeys(_ROWS[2:], 1)


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

    want = {(k, i): 1 for i, k in enumerate(vals)}
    got = scanned(client, "ta")
    assert bag(got) == want
    assert {type(r.k) for r in got} == {type(vals[0])}
    assert bag(scanned(client, "tb")) == want

    for i, k in enumerate(vals):
        assert bag(client.seek(tid, schema, k)) == {(k, i): 1}
        assert bag(rows(client, f"SELECT v FROM ta WHERE k = {lit(k)}")) == {(i,): 1}
        assert bag(rows(client, f"SELECT v FROM ta WHERE k < {lit(k)}")) == \
            {(j,): 1 for j in range(i)}

    client.delete(tid, schema, vals)
    assert bag(scanned(client, "ta")) == {}


# (SQL type, four values in ascending order). The integer widths straddle the
# sign and the byte boundaries; TEXT shares a prefix past the inline length;
# UUID, UINT128 and INT128 order on all 16 bytes, the UUIDs differing only past
# byte 8 and INT128 straddling its sign.
_ORDERED = [
    ("TINYINT", [I8_MIN, -5, 0, I8_MAX]),
    ("SMALLINT", [I16_MIN, -1, 256, I16_MAX]),
    ("INT UNSIGNED", [0, 100, 2_147_483_648, U32_MAX]),
    ("BIGINT", [I64_MIN, -1, 0, I64_MAX]),
    ("FLOAT", [-2.25, 0.5, 1.5, 4.0]),
    ("TEXT", ["", "shared/prefix/a", "shared/prefix/ab", "shared/prefix/abd"]),
    ("UUID", ["550e8400-e29b-41d4-a716-446655430000", "550e8400-e29b-41d4-a716-446655440000",
              "550e8400-e29b-41d4-a716-446655440001", "ffffffff-ffff-ffff-ffff-ffffffffffff"]),
    ("UINT128", [0, 5, (1 << 64) + 1, 10 ** 38 - 1]),
    ("INT128", [-(1 << 127), -5, 0, (1 << 127) - 1]),
]
# Signed twins, so a group key compared without its sign reads a neighbour's.
_GROUPS = [(-5, -7), (5, 7), (-5, 7), (5, -7)]
# The PK column's values, straddling the sign and the high bytes.
_KEY_IDS = [-5, 1, 256, 65536]


def test_min_max_select_and_recede_by_each_familys_own_order(client):
    """MIN/MAX select by the column's own order and decode the selected value
    back out of the value index. Each group holds a different three of the four
    positions, so an extreme leaking across groups reads another group's value.
    `id` is a PRIMARY KEY column, stored only as order-preserving bytes, so its
    extremes must decode before they compare. Retracting both of a group's
    extreme carriers recedes it to the survivor, and emptying the table empties
    every view."""
    cols = [f"c{j}" for j in range(len(_ORDERED))] + ["id"]
    families = [vals for _, vals in _ORDERED] + [_KEY_IDS]
    client.execute_sql(
        "CREATE TABLE t (ka INT NOT NULL, kb BIGINT NOT NULL, id BIGINT NOT NULL, "
        + "".join(f"c{j} {sql} NOT NULL, " for j, (sql, _) in enumerate(_ORDERED))
        + "PRIMARY KEY (ka, kb, id)); "
        + "; ".join(f"CREATE VIEW m_{c} AS SELECT ka, kb, MIN({c}) AS lo, MAX({c}) AS hi "
                    "FROM t GROUP BY ka, kb" for c in cols))
    # One statement per position, holding it in every group but its own.
    for pos in range(4):
        insert(client, "t", [(*_GROUPS[g], _KEY_IDS[pos], *(v[pos] for _, v in _ORDERED))
                             for g in range(4) if g != pos])

    def expect(held):
        for c, vals in zip(cols, families):
            assert bag(scanned(client, f"m_{c}"), "ka", "kb", "lo", "hi") == {
                (*_GROUPS[g], vals[min(ps)], vals[max(ps)]): 1 for g, ps in held.items()}, c

    held = {g: [p for p in range(4) if p != g] for g in range(4)}
    expect(held)
    ka, kb = _GROUPS[1]
    client.execute_sql(
        f"DELETE FROM t WHERE ka = {ka} AND kb = {kb} AND id IN ({_KEY_IDS[0]}, {_KEY_IDS[3]})")
    held[1] = [2]
    expect(held)
    client.execute_sql("DELETE FROM t")
    expect({})
