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

DATE, TS, I64 = gnitz.TypeCode.DATE, gnitz.TypeCode.TIMESTAMP, gnitz.TypeCode.I64


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


# Each function against its result type and its value on ids 1..4. Every one
# propagates a NULL input rather than substituting the epoch.
_FUNCTIONS = {
    "EXTRACT(YEAR FROM d)": (I64, [2024, 2024, 2021, None]),
    "EXTRACT(MONTH FROM d)": (I64, [2, 3, 1, None]),
    "EXTRACT(DAY FROM d)": (I64, [29, 31, 3, None]),
    "EXTRACT(DOW FROM d)": (I64, [4, 0, 0, None]),
    "EXTRACT(ISODOW FROM d)": (I64, [4, 7, 7, None]),
    "EXTRACT(DOY FROM d)": (I64, [60, 91, 3, None]),
    # 2021-01-03 is a Sunday in ISO week 53 of 2020.
    "EXTRACT(WEEK FROM d)": (I64, [9, 13, 53, None]),
    "EXTRACT(QUARTER FROM d)": (I64, [1, 1, 1, None]),
    "EXTRACT(HOUR FROM ts)": (I64, [13, 23, 23, None]),
    "DATE_PART('minute', ts)": (I64, [45, 59, 0, None]),
    "EXTRACT(SECOND FROM ts)": (I64, [7, 59, 0, None]),
    # The pre-epoch timestamp floors rather than rounding toward zero, which is
    # what makes its epoch exactly -3600.
    "EXTRACT(EPOCH FROM ts)": (I64, [1709214307, 1711929599, -3600, None]),
    # A month end truncates to the month's first day, not its last; the week
    # truncation of 2021-01-03 crosses a year boundary.
    "DATE_TRUNC('month', d)": (DATE, [date(2024, 2, 1), date(2024, 3, 1), date(2021, 1, 1),
                                      None]),
    "DATE_TRUNC('week', d)": (DATE, [date(2024, 2, 26), date(2024, 3, 25), date(2020, 12, 28),
                                     None]),
    "DATE_TRUNC('hour', ts)": (TS, [datetime(2024, 2, 29, 13), datetime(2024, 3, 31, 23),
                                    datetime(1969, 12, 31, 23), None]),
    "CAST(ts AS DATE)": (DATE, [date(2024, 2, 29), date(2024, 3, 31), date(1969, 12, 31), None]),
    "CAST(d AS TIMESTAMP)": (TS, [datetime(2024, 2, 29), datetime(2024, 3, 31),
                                  datetime(2021, 1, 3), None]),
    "d + 1": (DATE, [date(2024, 3, 1), date(2024, 4, 1), date(2021, 1, 4), None]),
    "d - DATE '2024-01-01'": (I64, [59, 90, -1093, None]),
    "ts - 1000000": (TS, [datetime(2024, 2, 29, 13, 45, 6),
                          datetime(2024, 3, 31, 23, 59, 58, 999_999),
                          datetime(1969, 12, 31, 22, 59, 59), None]),
}


def test_each_calendar_function_keeps_its_own_result_type(client, cal):
    """One projection per function, so the result types are stated as one table
    — served alike by a maintained view and an ad-hoc read. A predicate parses
    its literal at the column's type, so a bare string and a typed literal
    select the same rows, including a BETWEEN, whose bounds parse separately."""
    sn = cal
    select = "SELECT id, " + ", ".join(
        f"{e} AS c{n}" for n, e in enumerate(_FUNCTIONS)) + " FROM t"
    client.execute_sql(f"CREATE VIEW v AS {select}", schema_name=sn)

    declared = [c.type_code for c in client.resolve_table(sn, "v")[1].columns[1:]]
    assert declared == [tc for tc, _ in _FUNCTIONS.values()]

    expected = {r: 1 for r in zip([1, 2, 3, 4], *(vs for _, vs in _FUNCTIONS.values()))}
    assert bag(scanned(client, sn, "v")) == expected
    assert bag(rows(client, sn, select)) == expected

    client.execute_sql(
        "CREATE VIEW recent AS SELECT id FROM t "
        "WHERE ts >= TIMESTAMP '2024-03-01 00:00:00' OR d < '2022-01-01'", schema_name=sn)
    assert bag(scanned(client, sn, "recent")) == {(2,): 1, (3,): 1}
    assert bag(rows(client, sn, "SELECT id FROM t WHERE d = DATE '2024-02-29'")) == {(1,): 1}
    assert bag(rows(client, sn, "SELECT COUNT(*) AS n FROM t "
                                "WHERE d BETWEEN '2024-01-01' AND '2024-12-31'")) == {(2,): 1}


def test_a_truncation_used_as_a_group_key_maintains_its_buckets(client, cal):
    """DATE_TRUNC in a GROUP BY makes the bucket a computed key, so the reduce
    must re-derive it on every delta. The NULL row forms its own bucket — a
    grouping that dropped it would lose a row rather than mis-place one."""
    sn = cal
    client.execute_sql(
        "CREATE VIEW bym AS SELECT DATE_TRUNC('month', d) AS month, SUM(amt) AS total, "
        "MIN(ts) AS first_ts, MAX(d) AS last_d, COUNT(*) AS n "
        "FROM t GROUP BY DATE_TRUNC('month', d)", schema_name=sn)
    types = {c.name: c.type_code for c in client.resolve_table(sn, "bym")[1].columns}
    assert (types["month"], types["first_ts"], types["last_d"]) == (DATE, TS, DATE)

    client.execute_sql(
        "INSERT INTO t VALUES (5, DATE '2024-02-01', TIMESTAMP '2024-02-01 00:00:00', 5)",
        schema_name=sn)
    cols = ("month", "total", "first_ts", "last_d", "n")
    unchanged = {
        (date(2024, 3, 1), 20, datetime(2024, 3, 31, 23, 59, 59, 999_999),
         date(2024, 3, 31), 1): 1,
        (date(2021, 1, 1), 30, datetime(1969, 12, 31, 23), date(2021, 1, 3), 1): 1,
        (None, 40, None, None, 1): 1,
    }
    assert bag(scanned(client, sn, "bym"), *cols) == {
        (date(2024, 2, 1), 15, datetime(2024, 2, 1), date(2024, 2, 29), 2): 1, **unchanged}

    # Retracting the February extremum re-derives that bucket from what is left.
    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "bym"), *cols) == {
        (date(2024, 2, 1), 5, datetime(2024, 2, 1), date(2024, 2, 1), 1): 1, **unchanged}
