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
"""

from datetime import date, datetime

import gnitz
from _exprs import declared, projected
from _read import bag, scanned

TC = gnitz.TypeCode
DATE, TS, I64, BOOL = (TC.DATE, 0), (TC.TIMESTAMP, 0), (TC.I64, 0), (TC.BOOLEAN, 0)

_T = "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, d DATE, ts TIMESTAMP, amt BIGINT NOT NULL)"
# Every calendar edge at once: a leap day, a month end at the last microsecond,
# a Sunday in an ISO week belonging to the previous year beside a pre-epoch
# instant half a second past a whole hour, and an all-NULL row.
_SEED = ("INSERT INTO t VALUES "
         "(1, DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:07', 10), "
         "(2, DATE '2024-03-31', TIMESTAMP '2024-03-31 23:59:59.999999', 20), "
         "(3, DATE '2021-01-03', TIMESTAMP '1969-12-31 23:00:00.5', 30), "
         "(4, NULL, NULL, 40)")

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
    # The pre-epoch instant is -3599.5 s: flooring gives -3600, rounding toward
    # zero would give -3599 — and would truncate it into 1970 below.
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
                          datetime(1969, 12, 31, 22, 59, 59, 500_000), None]),
    # A comparison parses its literal at the column's type, so a bare string and
    # a typed literal name the same value — a BETWEEN's two bounds included.
    "ts >= TIMESTAMP '2024-03-01 00:00:00' OR d < '2022-01-01'": (BOOL, [False, True, True, None]),
    "d = DATE '2024-02-29'": (BOOL, [True, False, False, None]),
    "d BETWEEN '2024-01-01' AND '2024-12-31'": (BOOL, [True, True, False, None]),
}


def test_each_calendar_function_keeps_its_own_result_type(client):
    client.execute_sql(_T)
    projected(client, _SEED, _FUNCTIONS)


def test_a_truncation_used_as_a_group_key_maintains_its_buckets(client):
    """DATE_TRUNC in a GROUP BY makes the bucket a computed key, so the reduce
    must re-derive it on every delta, and an extremum over a temporal column
    keeps its type. The NULL row forms its own bucket — a grouping that dropped
    it would lose a row rather than mis-place one."""
    client.execute_sql(_T)
    client.execute_sql(
        "CREATE VIEW bym AS SELECT DATE_TRUNC('month', d) AS month, SUM(amt) AS total, "
        "MIN(ts) AS first_ts, MAX(d) AS last_d, COUNT(*) AS n "
        "FROM t GROUP BY DATE_TRUNC('month', d)")
    types = declared(client, "bym")
    assert (types["month"], types["first_ts"], types["last_d"]) == (DATE, TS, DATE)

    client.execute_sql(_SEED)
    client.execute_sql(
        "INSERT INTO t VALUES (5, DATE '2024-02-01', TIMESTAMP '2024-02-01 00:00:00', 5)")
    cols = ("month", "total", "first_ts", "last_d", "n")
    unchanged = {
        (date(2024, 3, 1), 20, datetime(2024, 3, 31, 23, 59, 59, 999_999),
         date(2024, 3, 31), 1): 1,
        (date(2021, 1, 1), 30, datetime(1969, 12, 31, 23, 0, 0, 500_000), date(2021, 1, 3), 1): 1,
        (None, 40, None, None, 1): 1,
    }
    assert bag(scanned(client, "bym"), *cols) == {
        (date(2024, 2, 1), 15, datetime(2024, 2, 1), date(2024, 2, 29), 2): 1, **unchanged}

    # Retracting the February extremum re-derives that bucket from what is left.
    client.execute_sql("DELETE FROM t WHERE id = 1")
    assert bag(scanned(client, "bym"), *cols) == {
        (date(2024, 2, 1), 5, datetime(2024, 2, 1), date(2024, 2, 1), 1): 1, **unchanged}
