"""DECIMAL(p, s): a fixed-point value stored as a scaled integer.

The scale is the whole type. It is fixed at DDL, travels in the catalog, comes
back on the wire, and decides what a literal rounds to — so a column is only a
DECIMAL if the scale survives every one of those hops. A value read back as a
float would compare equal to the right number (`Decimal("12.50") == 12.5`) while
having lost exactly the property the type exists for, which is why the rendered
*type* is asserted alongside the value.

Two precisions mean two different storage types: up to 18 digits the column is a
scaled `I64` and carries a scale; `DECIMAL(38,0)` and `(39,0)` are the SQL
spelling of a 128-bit unsigned integer and carry none. Only the first is a
DECIMAL in the sense of this file.

The per-row arithmetic over these columns is a program rather than a type, and
lives in `scalar_expression/test_fixed_point.py`.
"""

from decimal import Decimal

import pytest
import gnitz
from _read import bag, rows, scanned

DEC = gnitz.TypeCode.DECIMAL


@pytest.fixture
def priced(client, schema_name):
    """`t(id, cat, price DECIMAL(10,2), qty NUMERIC(8,3))` holding one row of
    each spelling a literal can take: a plain decimal, a string, a negative, an
    integer widened to the scale, and one whose fraction is longer than the
    scale so it must round."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, "
        "price DECIMAL(10, 2) NOT NULL, qty NUMERIC(8, 3))", schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 12.50, 3), (2, 1, 0.10, 3.5), "
        "(3, 2, '7.125', '0.001'), (4, 2, -0.5, NULL), (5, 3, 1.005, 2)",
        schema_name=schema_name)
    return schema_name


def test_the_declared_scale_reaches_the_catalog_and_the_wire(client, priced):
    """Every literal form lands at the column's scale, rounding half away from
    zero when it carries more fraction than the scale holds. `NUMERIC` is the
    same type under another spelling, and a column with no scale reads as 0."""
    sn = priced
    tid, schema = client.resolve_table(sn, "t")
    assert schema.columns[2].type_code == DEC and schema.columns[2].scale == 2
    assert schema.columns[3].scale == 3
    assert schema.columns[1].scale == 0, "a BIGINT carries scale 0, not a null scale"

    rs = client.scan(tid)
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
    ("'abc'", "invalid DECIMAL literal"),
    ("99999999999999999999", "out of range"),
], ids=["not-a-number", "past-the-scaled-i64"])
def test_a_literal_the_scale_cannot_hold_is_refused(client, priced, value, message):
    """A value with no fixed-point image is refused rather than truncated: the
    scaled integer behind the scale is what bounds it, not the declared
    precision."""
    with pytest.raises(gnitz.GnitzError, match=message):
        client.execute_sql(f"INSERT INTO t VALUES (9, 1, {value}, 1)", schema_name=priced)


def test_a_decimal_key_routes_and_seeks_on_its_stored_integer(client, schema_name):
    """A DECIMAL PK is a signed 8-byte key like any other, so it routes, seeks
    and ranges on the scaled integer. A literal at a finer scale than the column
    names no key at all and must miss rather than round onto a neighbour."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE p (amt DECIMAL(6, 2) NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn)
    _, ps = client.resolve_table(sn, "p")
    assert ps.columns[0].scale == 2
    # The amounts span zero, so the stored integer's sign flip is under test
    # alongside the seek: without it every negative amount would sort above
    # every positive one and the ranges below would answer the wrong halves.
    amts = [Decimal(i - 6) / 4 for i in range(12)]
    client.execute_sql(
        "INSERT INTO p VALUES " + ", ".join(f"({a}, {i})" for i, a in enumerate(amts)),
        schema_name=sn)

    assert bag(rows(client, sn, "SELECT amt, v FROM p WHERE amt = 1.25")) == \
        {(Decimal("1.25"), 11): 1}
    assert bag(rows(client, sn, "SELECT v FROM p WHERE amt = 1.255")) == {}
    assert bag(rows(client, sn, "SELECT v FROM p WHERE amt < 0")) == \
        {(i,): 1 for i, a in enumerate(amts) if a < 0}
    assert bag(rows(client, sn, "SELECT v FROM p WHERE amt >= 0")) == \
        {(i,): 1 for i, a in enumerate(amts) if a >= 0}
    assert bag(rows(client, sn, "SELECT v FROM p WHERE amt >= -0.5 AND amt < 0.6")) == \
        {(i,): 1 for i, a in enumerate(amts) if Decimal("-0.5") <= a < Decimal("0.6")}
    assert bag(rows(client, sn, "SELECT v FROM p WHERE amt IN (-1.5, 0.25, 9)")) == \
        {(i,): 1 for i, a in enumerate(amts) if a in (Decimal("-1.5"), Decimal("0.25"))}

    client.execute_sql("DELETE FROM p WHERE amt = -0.75", schema_name=sn)
    client.execute_sql("UPDATE p SET v = v + 100 WHERE amt > 1.0", schema_name=sn)
    assert bag(scanned(client, sn, "p"), "amt", "v") == {
        (a.quantize(Decimal("0.01")), i + (100 if a > 1 else 0)): 1
        for i, a in enumerate(amts) if i != 3}

    with pytest.raises(gnitz.GnitzError, match="not a valid DECIMAL"):
        client.execute_sql("INSERT INTO p VALUES ('x', 1)", schema_name=sn)


def test_a_grouped_or_joined_decimal_key_keeps_its_scale(client, priced):
    """A DECIMAL used as a group key or a join key carries its scale into the
    output schema, so the key a downstream reader sees is the same fixed-point
    value rather than a bare integer."""
    sn = priced
    client.execute_sql(
        "CREATE VIEW byp AS SELECT price, COUNT(*) AS n FROM t GROUP BY price",
        schema_name=sn)
    _, bs = client.resolve_table(sn, "byp")
    key = next(c for c in bs.columns if c.name == "price")
    assert (key.type_code, key.scale) == (DEC, 2)
    assert bag(scanned(client, sn, "byp"), "price", "n") == {
        (Decimal("0.10"), 1): 1, (Decimal("7.13"), 1): 1,
        (Decimal("-0.50"), 1): 1, (Decimal("1.01"), 1): 1,
        (Decimal("12.50"), 1): 1}

    client.execute_sql(
        "CREATE TABLE tier (id BIGINT NOT NULL PRIMARY KEY, price DECIMAL(10, 2) NOT NULL, "
        "label BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("INSERT INTO tier VALUES (1, 12.5, 90), (2, 0.1, 91)",
                       schema_name=sn)
    client.execute_sql(
        "CREATE VIEW j AS SELECT t.id AS id, tier.label AS label FROM t "
        "JOIN tier ON t.price = tier.price", schema_name=sn)
    assert bag(scanned(client, sn, "j"), "id", "label") == {(1, 90): 1, (2, 91): 1}


def test_two_decimals_meet_only_at_one_scale(client, priced):
    """A set operation and an equijoin both compare stored integers, and two
    scales make that comparison meaningless — 1.20 at scale 2 is 120, at scale 3
    it is 1200. Each pair is refused at plan time rather than compared wrongly,
    and an integer is refused for the same reason: it carries no scale at all."""
    client.execute_sql(
        "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, qty NUMERIC(8, 3) NOT NULL)",
        schema_name=priced)
    for body, message in (
        ("SELECT price FROM t UNION SELECT qty FROM t", "type mismatch"),
        ("SELECT a.id FROM t a JOIN t2 ON a.price = t2.qty", "same scale"),
        ("SELECT a.id FROM t a JOIN t2 ON a.price = t2.id", "same scale"),
    ):
        with pytest.raises(gnitz.GnitzError, match=message):
            client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=priced)
