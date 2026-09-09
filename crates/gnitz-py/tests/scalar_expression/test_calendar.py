"""The calendar program: EXTRACT, DATE_PART, DATE_TRUNC, the temporal casts and
date arithmetic.

A DATE is a day count and a TIMESTAMP a microsecond count, so every function
here is arithmetic on an integer that has to be re-interpreted as a calendar.
The cases that matter are the ones where the calendar and the integer disagree:
a leap day, a month end, an ISO week that belongs to the previous year, and a
pre-epoch instant where the count is negative and truncation has to floor rather
than round toward zero.

Each function's **result type** is asserted beside its value. DATE_TRUNC returns
the input type, EXTRACT returns an integer, and a date difference is an integer
rather than a date — a function that returned the right number at the wrong type
would render correctly here and mis-key a downstream join.

That DATE and TIMESTAMP are types — their literals, their storage integer, their
key behaviour — is `value_domain/test_temporal.py`.
"""

from datetime import date, datetime

import pytest
import gnitz
from _read import bag, rows, scanned


@pytest.fixture
def cal(client, schema_name):
    """Four rows chosen so every calendar edge is present at once: a leap day, a
    month end at the last microsecond, a Sunday in an ISO week belonging to the
    previous year paired with a pre-epoch timestamp, and an all-NULL row."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, d DATE, ts TIMESTAMP, "
        "amt BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES "
        "(1, DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:07', 10), "
        "(2, DATE '2024-03-31', TIMESTAMP '2024-03-31 23:59:59.999999', 20), "
        "(3, DATE '2021-01-03', TIMESTAMP '1969-12-31 23:00:00', 30), "
        "(4, NULL, NULL, 40)", schema_name=schema_name)
    return schema_name


def test_each_calendar_field_and_truncation_keeps_its_own_result_type(client, cal):
    """One view carrying every field and every truncation, so the result types
    are stated as one table rather than one per function."""
    sn = cal
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, "
        "EXTRACT(YEAR FROM d) AS y, EXTRACT(MONTH FROM d) AS m, EXTRACT(DAY FROM d) AS dd, "
        "EXTRACT(DOW FROM d) AS dow, EXTRACT(ISODOW FROM d) AS isodow, "
        "EXTRACT(DOY FROM d) AS doy, EXTRACT(WEEK FROM d) AS wk, "
        "EXTRACT(QUARTER FROM d) AS q, EXTRACT(HOUR FROM ts) AS h, "
        "DATE_PART('minute', ts) AS mi, EXTRACT(SECOND FROM ts) AS s, "
        "EXTRACT(EPOCH FROM ts) AS ep, "
        "DATE_TRUNC('month', d) AS dm, DATE_TRUNC('hour', ts) AS th, "
        "DATE_TRUNC('week', d) AS dw, CAST(ts AS DATE) AS tsd, "
        "CAST(d AS TIMESTAMP) AS dts, d + 1 AS tomorrow, "
        "d - DATE '2024-01-01' AS since_ny, ts - 1000000 AS ts_minus_1s "
        "FROM t", schema_name=sn)

    _, vs = client.resolve_table(sn, "v")
    types = {c.name: c.type_code for c in vs.columns}
    TC = gnitz.TypeCode
    # A truncation returns its input's type; a field and a date difference are
    # integers; a cast returns what it names.
    assert [types[n] for n in ("y", "since_ny")] == [TC.I64, TC.I64]
    assert [types[n] for n in ("dm", "dw", "tsd", "tomorrow")] == [TC.DATE] * 4
    assert [types[n] for n in ("th", "dts", "ts_minus_1s")] == [TC.TIMESTAMP] * 3

    r = {x["id"]: x for x in scanned(client, sn, "v")}
    got = r[1]
    assert (got["y"], got["m"], got["dd"], got["dow"], got["isodow"], got["doy"],
            got["wk"], got["q"]) == (2024, 2, 29, 4, 4, 60, 9, 1)
    assert (got["h"], got["mi"], got["s"]) == (13, 45, 7)
    assert got["ep"] == int((datetime(2024, 2, 29, 13, 45, 7) - datetime(1970, 1, 1))
                            .total_seconds())
    assert (got["dm"], got["th"], got["dw"]) == \
        (date(2024, 2, 1), datetime(2024, 2, 29, 13), date(2024, 2, 26))
    assert (got["tsd"], got["dts"]) == (date(2024, 2, 29), datetime(2024, 2, 29))
    assert (got["tomorrow"], got["since_ny"], got["ts_minus_1s"]) == \
        (date(2024, 3, 1), 59, datetime(2024, 2, 29, 13, 45, 6))

    # A month end truncates to the month's first day, not its last.
    got = r[2]
    assert (got["dm"], got["th"], got["tsd"]) == \
        (date(2024, 3, 1), datetime(2024, 3, 31, 23), date(2024, 3, 31))

    # 2021-01-03 is a Sunday in ISO week 53 of 2020, so the week truncation
    # crosses a year boundary; the pre-epoch timestamp floors rather than
    # rounding toward zero, which is what makes its epoch exactly -3600.
    got = r[3]
    assert (got["dow"], got["isodow"], got["wk"], got["dw"]) == \
        (0, 7, 53, date(2020, 12, 28))
    assert (got["h"], got["tsd"], got["th"]) == \
        (23, date(1969, 12, 31), datetime(1969, 12, 31, 23))
    assert got["ep"] == -3600

    # Every function propagates a NULL input rather than substituting the epoch.
    assert all(r[4][k] is None for k in
               ("y", "h", "dm", "th", "tsd", "dts", "tomorrow", "since_ny"))


def test_a_truncation_used_as_a_group_key_maintains_its_buckets(client, cal):
    """DATE_TRUNC in a GROUP BY makes the bucket a computed key, so the reduce
    must re-derive it on every delta. The NULL row forms its own bucket — a
    grouping that dropped it would lose a row rather than mis-place one."""
    sn = cal
    client.execute_sql(
        "CREATE VIEW bym AS SELECT DATE_TRUNC('month', d) AS month, SUM(amt) AS total, "
        "MIN(ts) AS first_ts, MAX(d) AS last_d, COUNT(*) AS n "
        "FROM t GROUP BY DATE_TRUNC('month', d)", schema_name=sn)
    _, vs = client.resolve_table(sn, "bym")
    types = {c.name: c.type_code for c in vs.columns}
    assert (types["month"], types["first_ts"], types["last_d"]) == \
        (gnitz.TypeCode.DATE, gnitz.TypeCode.TIMESTAMP, gnitz.TypeCode.DATE)

    client.execute_sql(
        "INSERT INTO t VALUES (5, DATE '2024-02-01', TIMESTAMP '2024-02-01 00:00:00', 5)",
        schema_name=sn)

    cols = ("month", "total", "first_ts", "last_d", "n")
    assert bag(scanned(client, sn, "bym"), *cols) == {
        (date(2024, 2, 1), 15, datetime(2024, 2, 1), date(2024, 2, 29), 2): 1,
        (date(2024, 3, 1), 20, datetime(2024, 3, 31, 23, 59, 59, 999_999),
         date(2024, 3, 31), 1): 1,
        (date(2021, 1, 1), 30, datetime(1969, 12, 31, 23), date(2021, 1, 3), 1): 1,
        (None, 40, None, None, 1): 1,
    }

    # Retracting the February extremum re-derives that bucket from what is left.
    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "bym"), *cols)[
        (date(2024, 2, 1), 5, datetime(2024, 2, 1), date(2024, 2, 1), 1)] == 1


def test_a_temporal_predicate_compares_on_the_stored_integer(client, cal):
    """A comparison against a literal is an integer compare after the literal is
    parsed at the column's type, so a bare string and a typed literal must select
    the same rows — including a BETWEEN, whose two bounds are parsed separately.
    """
    sn = cal
    client.execute_sql(
        "CREATE VIEW recent AS SELECT id FROM t "
        "WHERE ts >= TIMESTAMP '2024-03-01 00:00:00' OR d < '2022-01-01'",
        schema_name=sn)
    assert bag(scanned(client, sn, "recent"), "id") == {(2,): 1, (3,): 1}

    assert bag(rows(client, sn,
                    "SELECT id, EXTRACT(YEAR FROM ts) AS y FROM t "
                    "WHERE d = DATE '2024-02-29'")) == {(1, 2024): 1}
    assert bag(rows(client, sn, "SELECT COUNT(*) AS n FROM t "
                                "WHERE d BETWEEN '2024-01-01' AND '2024-12-31'")) == \
        {(2,): 1}


@pytest.mark.parametrize("body,message", [
    # A view is recomputed from deltas, so a clock read would make its contents
    # depend on when a tick ran rather than on what the input said.
    ("SELECT id, NOW() AS n FROM t", "non-deterministic"),
    ("SELECT id, CURRENT_DATE AS n FROM t", "non-deterministic"),
    ("SELECT id, EXTRACT(YEAR FROM amt) AS y FROM t", "DATE or TIMESTAMP"),
    ("SELECT id, DATE_TRUNC('fortnight', d) AS y FROM t", "not supported"),
    # Summing day counts yields a number that is not a date and not meaningful.
    ("SELECT SUM(d) AS s FROM t", "not supported"),
    ("SELECT id, DATE '2024-02-30' AS x FROM t", "invalid DATE literal"),
], ids=["now", "current-date", "extract-from-integer", "unknown-unit",
        "sum-of-dates", "impossible-literal"])
def test_a_calendar_expression_a_view_cannot_hold_is_refused(client, cal, body, message):
    """Each refusal names what is wrong with the expression, not the node that
    rejected it: a clock read, a non-temporal operand, an unknown unit, an
    aggregate with no meaning over dates, and a literal that denotes no day."""
    with pytest.raises(gnitz.GnitzError, match=message):
        client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=cal)
