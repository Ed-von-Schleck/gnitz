"""DECIMAL(p, s): a fixed-point value stored as a scaled integer.

The scale is the whole type. It is fixed at DDL, travels in the catalog, comes
back on the wire, and decides what a literal rounds to — so a column is only a
DECIMAL if the scale survives every one of those hops. A value read back as a
float would compare equal to the right number (`Decimal("12.50") == 12.5`) while
having lost exactly the property the type exists for.

The per-row arithmetic over these columns is a program rather than a type, and
lives in `scalar_expression/test_fixed_point.py`.
"""

from decimal import Decimal

import gnitz
from _read import bag, scanned

DEC = gnitz.TypeCode.DECIMAL


def test_the_declared_scale_reaches_the_catalog_and_the_wire(client, priced):
    """Every literal form lands at the column's scale, rounding half away from
    zero when it carries more fraction than the scale holds. `NUMERIC` is the
    same type under another spelling."""
    tid, schema = client.resolve_table("t")
    # `Decimal("3.000") == 3`, so the rows below cannot see a scale.
    assert [(c.type_code, c.scale) for c in schema.columns[2:]] == [(DEC, 2), (DEC, 3)]
    assert bag(client.scan(tid, schema), "id", "price", "qty") == {
        (1, Decimal("12.50"), Decimal("3.000")): 1,
        (2, Decimal("0.10"), Decimal("3.500")): 1,
        # '7.125' at scale 2 and 1.005 at scale 2 both round half away from zero.
        (3, Decimal("7.13"), Decimal("0.001")): 1,
        (4, Decimal("-0.50"), None): 1,
        (5, Decimal("1.01"), Decimal("2.000")): 1,
    }


def test_a_grouped_or_joined_decimal_key_keeps_its_scale(client, priced):
    """A DECIMAL used as a group key or a join key carries its scale into the
    output schema, so the key a downstream reader sees, hidden or not, is the
    same fixed-point value rather than a bare integer."""
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
    assert bag(scanned(client, "j", hidden=True), "_join_pk", "id", "label") == {
        (Decimal("12.50"), 1, 90): 1, (Decimal("0.10"), 2, 91): 1}
