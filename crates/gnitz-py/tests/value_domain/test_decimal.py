"""DECIMAL(p, s): a fixed-point value stored as a scaled integer.

The scale is the whole type. It is fixed at DDL, travels in the catalog, comes
back on the wire, and decides what a literal rounds to — so a column is only a
DECIMAL if the scale survives every one of those hops. A value read back as a
float would compare equal to the right number (`Decimal("12.50") == 12.5`) while
having lost exactly the property the type exists for, which is why the rendered
*type* is asserted alongside the value.

A DECIMAL is always a scaled `I64`.

The per-row arithmetic over these columns is a program rather than a type, and
lives in `scalar_expression/test_fixed_point.py`.
"""

from decimal import Decimal

import pytest
import gnitz
from _read import bag, rows, scanned

DEC = gnitz.TypeCode.DECIMAL


def test_the_declared_scale_reaches_the_catalog_and_the_wire(client, priced):
    """Every literal form lands at the column's scale, rounding half away from
    zero when it carries more fraction than the scale holds. `NUMERIC` is the
    same type under another spelling, and a column with no scale reads as 0."""
    tid, schema = client.resolve_table("t")
    assert schema.columns[2].type_code == DEC and schema.columns[2].scale == 2
    assert schema.columns[3].scale == 3
    assert schema.columns[1].scale == 0, "a BIGINT carries scale 0, not a null scale"

    rs = client.scan(tid, schema)
    assert bag(rs, "id", "price", "qty") == {
        (1, Decimal("12.50"), Decimal("3.000")): 1,
        (2, Decimal("0.10"), Decimal("3.500")): 1,
        # '7.125' at scale 2 and 1.005 at scale 2 both round half away from zero.
        (3, Decimal("7.13"), Decimal("0.001")): 1,
        (4, Decimal("-0.50"), None): 1,
        (5, Decimal("1.01"), Decimal("2.000")): 1,
    }
    # `Decimal("12.50") == 12.5` is True, so the comparison above would pass on a
    # float too: the rendered type is its own assertion.
    row = next(r for r in rs if r.id == 1)
    assert isinstance(row.price, Decimal) and isinstance(row.qty, Decimal)


@pytest.mark.parametrize("value,message", [
    ("'abc'", "invalid DECIMAL"),
    ("99999999999999999999", "out of range"),
], ids=["not-a-number", "past-the-scaled-i64"])
def test_a_literal_the_scale_cannot_hold_is_refused(client, priced, value, message):
    """A value with no fixed-point image is refused rather than truncated: the
    scaled integer behind the scale is what bounds it, not the declared
    precision."""
    with pytest.raises(gnitz.GnitzRefusedError, match=message):
        client.execute_sql(f"INSERT INTO t VALUES (9, 1, {value}, 1)")


def test_a_decimal_key_routes_and_seeks_on_its_stored_integer(client):
    """A DECIMAL PK is a signed 8-byte key like any other, so it routes, seeks
    and ranges on the scaled integer. A literal at a finer scale than the column
    names no key at all and must miss rather than round onto a neighbour."""
    client.execute_sql(
        "CREATE TABLE p (amt DECIMAL(6, 2) NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
    _, ps = client.resolve_table("p")
    assert ps.columns[0].scale == 2
    # The amounts span zero, so the stored integer's sign flip is under test
    # alongside the seek: without it every negative amount would sort above
    # every positive one and the ranges below would answer the wrong halves.
    amts = [Decimal(i - 6) / 4 for i in range(12)]
    client.execute_sql(
        "INSERT INTO p VALUES " + ", ".join(f"({a}, {i})" for i, a in enumerate(amts)))

    assert bag(rows(client, "SELECT amt, v FROM p WHERE amt = 1.25")) == \
        {(Decimal("1.25"), 11): 1}
    assert bag(rows(client, "SELECT v FROM p WHERE amt = 1.255")) == {}
    assert bag(rows(client, "SELECT v FROM p WHERE amt < 0")) == \
        {(i,): 1 for i, a in enumerate(amts) if a < 0}
    assert bag(rows(client, "SELECT v FROM p WHERE amt >= 0")) == \
        {(i,): 1 for i, a in enumerate(amts) if a >= 0}
    assert bag(rows(client, "SELECT v FROM p WHERE amt >= -0.5 AND amt < 0.6")) == \
        {(i,): 1 for i, a in enumerate(amts) if Decimal("-0.5") <= a < Decimal("0.6")}
    assert bag(rows(client, "SELECT v FROM p WHERE amt IN (-1.5, 0.25, 9)")) == \
        {(i,): 1 for i, a in enumerate(amts) if a in (Decimal("-1.5"), Decimal("0.25"))}

    client.execute_sql("DELETE FROM p WHERE amt = -0.75")
    client.execute_sql("UPDATE p SET v = v + 100 WHERE amt > 1.0")
    assert bag(scanned(client, "p"), "amt", "v") == {
        (a.quantize(Decimal("0.01")), i + (100 if a > 1 else 0)): 1
        for i, a in enumerate(amts) if i != 3}

    with pytest.raises(gnitz.GnitzRefusedError, match="invalid DECIMAL"):
        client.execute_sql("INSERT INTO p VALUES ('x', 1)")


def test_a_grouped_or_joined_decimal_key_keeps_its_scale(client, priced):
    """A DECIMAL used as a group key or a join key carries its scale into the
    output schema, so the key a downstream reader sees is the same fixed-point
    value rather than a bare integer."""
    client.execute_sql(
        "CREATE VIEW byp AS SELECT price, COUNT(*) AS n FROM t GROUP BY price")
    _, bs = client.resolve_table("byp")
    key = next(c for c in bs.columns if c.name == "price")
    assert (key.type_code, key.scale) == (DEC, 2)
    assert bag(scanned(client, "byp"), "price", "n") == {
        (Decimal("0.10"), 1): 1, (Decimal("7.13"), 1): 1,
        (Decimal("-0.50"), 1): 1, (Decimal("1.01"), 1): 1,
        (Decimal("12.50"), 1): 1}

    client.execute_sql(
        "CREATE TABLE tier (id BIGINT NOT NULL PRIMARY KEY, price DECIMAL(10, 2) NOT NULL, "
        "label BIGINT NOT NULL)")
    client.execute_sql("INSERT INTO tier VALUES (1, 12.5, 90), (2, 0.1, 91)")
    client.execute_sql(
        "CREATE VIEW j AS SELECT t.id AS id, tier.label AS label FROM t "
        "JOIN tier ON t.price = tier.price")
    assert bag(scanned(client, "j"), "id", "label") == {(1, 90): 1, (2, 91): 1}


def test_a_literal_is_the_decimal_it_spells(client):
    """A numeric literal carries every digit it was written with, not a float's
    shortest print: a cell keeps digits past 17 and rounds from all of them, and
    a comparison against a literal finer than the column is decided exactly —
    no stored value equals it, and no scale-up of the column wraps."""
    client.execute_sql(
        "CREATE TABLE e (id BIGINT NOT NULL PRIMARY KEY, price DECIMAL(18, 2) NOT NULL); "
        "CREATE TABLE f (id BIGINT NOT NULL PRIMARY KEY, price DECIMAL(18, 2) NOT NULL)")
    client.execute_sql(
        "INSERT INTO e VALUES (1, 1234567890123456.78), (2, 0.1234567890123456789); "
        "INSERT INTO f VALUES (1, 1.00), (2, 10.00)")
    assert bag(scanned(client, "e"), "id", "price") == {
        (1, Decimal("1234567890123456.78")): 1, (2, Decimal("0.12")): 1}
    assert bag(rows(client, "SELECT id FROM f WHERE price = 1.000000000000000001")) == {}
    assert bag(rows(client, "SELECT price FROM f WHERE price < 1.000000000000000001")) == {
        (Decimal("1.00"),): 1}
