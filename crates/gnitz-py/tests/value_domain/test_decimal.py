"""DECIMAL(p, s): a fixed-point value stored as a scaled integer.

The scale is the whole type. It is fixed at DDL, travels in the catalog, comes
back on the wire, and decides what a literal rounds to — so a column is only a
DECIMAL if the scale survives every one of those hops. A value read back as a
float, or at another scale, would compare equal to the right number
(`Decimal("12.50") == 12.5 == Decimal("12.5")`) while having lost exactly the
property the type exists for, so a value is asserted by its spelling.

A DECIMAL as a join or group key is one row of the key tables in
`test_key_promotion.py`. The per-row arithmetic over these columns is a program
rather than a type, and lives in `scalar_expression/test_fixed_point.py`.
"""

from collections import Counter

import gnitz
from _exprs import declared
from _read import scanned

DEC = gnitz.TypeCode.DECIMAL


def test_the_declared_scale_reaches_the_catalog_and_the_wire(client, priced):
    """Every literal form lands at the column's scale, rounding half away from
    zero when it carries more fraction than the scale holds. `NUMERIC` is the
    same type under another spelling."""
    client.execute_sql(priced)
    types = declared(client, "t")
    assert (types["price"], types["qty"]) == ((DEC, 2), (DEC, 3))
    spelled = Counter()
    for r in scanned(client, "t"):
        spelled[r.id, str(r.price), None if r.qty is None else str(r.qty)] += r._weight
    assert spelled == {
        (1, "12.50", "3.000"): 1,
        (2, "0.10", "3.500"): 1,
        # '7.125' at scale 2 and 1.005 at scale 2 both round half away from zero.
        (3, "7.13", "0.001"): 1,
        (4, "-0.50", None): 1,
        (5, "1.01", "2.000"): 1,
    }
