"""DATE and TIMESTAMP: two named types over a stored integer.

A DATE is an `I32` of days from the epoch and a TIMESTAMP an `I64` of
microseconds, which makes both **signed** and therefore key-eligible on exactly
the same terms as any other signed integer — they sign-flip, they route, they
range. Pre-epoch values are what prove that: they are the negative half of the
stored integer, and without the flip they would sort above every date since 1970.

DATE is also the only 4-byte key the system reaches through a named type, so it
is the one end-to-end exercise of that width in a key position.

The calendar functions over these columns — EXTRACT, DATE_PART, DATE_TRUNC, date
arithmetic — are a per-row program rather than a property of the type, and live
in `scalar_expression/test_calendar.py`.
"""

from datetime import date, datetime, timedelta

import pytest
import gnitz
from _read import bag, rows, scanned

D = date(2024, 2, 29)
TS = datetime(2024, 2, 29, 13, 45, 7, 250_000)


def test_both_literal_grammars_reach_the_same_stored_value(client, schema_name):
    """A typed literal, a bare string and a CAST are three spellings of one
    value, and a NULL in either column stays a NULL rather than becoming the
    epoch. The pre-epoch row is the one a `u32`/`u64` read would render as a
    date far in the future."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, d DATE, ts TIMESTAMP)",
        schema_name=sn)
    _, schema = client.resolve_table(sn, "t")
    assert schema.columns[1].type_code == gnitz.TypeCode.DATE
    assert schema.columns[2].type_code == gnitz.TypeCode.TIMESTAMP

    client.execute_sql(
        "INSERT INTO t VALUES "
        "(1, DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:07.25'), "
        "(2, '1969-12-31', '1969-12-31T23:59:59'), "
        "(3, CAST('2000-01-01' AS DATE), CAST('2000-01-01 00:00:00' AS TIMESTAMP)), "
        "(4, NULL, NULL)", schema_name=sn)

    assert bag(scanned(client, sn, "t"), "id", "d", "ts") == {
        (1, D, TS): 1,
        (2, date(1969, 12, 31), datetime(1969, 12, 31, 23, 59, 59)): 1,
        (3, date(2000, 1, 1), datetime(2000, 1, 1)): 1,
        (4, None, None): 1,
    }


@pytest.mark.parametrize("col_sql,tc,keys", [
    ("DATE", gnitz.TypeCode.DATE,
     [date(1900, 1, 1), date(1969, 12, 31), date(1970, 1, 1), date(2024, 2, 29)]),
    ("TIMESTAMP", gnitz.TypeCode.TIMESTAMP,
     [datetime(1900, 1, 1), datetime(1969, 12, 31, 23, 59, 59), datetime(1970, 1, 1),
      datetime(2024, 2, 29, 13, 45, 7)]),
], ids=["date", "timestamp"])
def test_a_temporal_key_orders_across_the_epoch(client, schema_name, col_sql, tc, keys):
    """The stored integer is signed, so the epoch is the sign boundary and a
    range spanning it must be contiguous. Without the flip the two pre-epoch keys
    would answer a `>= epoch` range and the post-epoch ones would not.

    DATE is 4 bytes and TIMESTAMP 8, so the pair also covers the two key widths a
    named temporal type reaches.
    """
    sn = schema_name
    epoch = keys[2]
    client.execute_sql(
        f"CREATE TABLE t (k {col_sql} NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn)
    _, schema = client.resolve_table(sn, "t")
    assert schema.columns[schema.pk_indices[0]].type_code == tc

    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"('{k.isoformat()}', {i})"
                                            for i, k in enumerate(keys)),
        schema_name=sn)

    assert bag(scanned(client, sn, "t"), "k", "v") == {(k, i): 1 for i, k in enumerate(keys)}
    assert bag(rows(client, sn, f"SELECT v FROM t WHERE k < '{epoch.isoformat()}'")) == \
        {(0,): 1, (1,): 1}
    assert bag(rows(client, sn, f"SELECT v FROM t WHERE k >= '{epoch.isoformat()}'")) == \
        {(2,): 1, (3,): 1}
    # Each key addresses its own row through the point-seek path.
    for i, k in enumerate(keys):
        assert bag(rows(client, sn, f"SELECT v FROM t WHERE k = '{k.isoformat()}'")) == \
            {(i,): 1}


def test_a_date_key_serves_the_read_and_write_verbs_alike(client, schema_name):
    """The key the client seeks by, the key a WHERE literal names and the key an
    UPDATE's range walks are one encoding, so all three must address the same
    rows. A `date` object and its string spelling are the same key."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (d DATE NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn)
    tid, _ = client.resolve_table(sn, "t")
    days = [D + timedelta(days=i) for i in range(12)]
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"('{x.isoformat()}', {i})"
                                            for i, x in enumerate(days)),
        schema_name=sn)

    # The binding seeks by a `date` object; the planner by a literal.
    assert bag(client.seek(tid, pk=D + timedelta(days=5)), "d", "v") == \
        {(D + timedelta(days=5), 5): 1}
    assert list(client.seek(tid, pk=date(1999, 1, 1))) == []

    client.execute_sql("DELETE FROM t WHERE d = DATE '2024-03-05'", schema_name=sn)
    client.execute_sql("DELETE FROM t WHERE d = '2024-03-06'", schema_name=sn)
    client.execute_sql("UPDATE t SET v = v + 100 WHERE d >= DATE '2024-03-09'",
                       schema_name=sn)

    gone = (date(2024, 3, 5), date(2024, 3, 6))
    assert bag(scanned(client, sn, "t"), "d", "v") == {
        (x, i + (100 if x >= date(2024, 3, 9) else 0)): 1
        for i, x in enumerate(days) if x not in gone}
