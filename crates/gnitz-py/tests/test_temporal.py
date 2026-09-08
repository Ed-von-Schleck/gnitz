"""E2E tests: DATE / TIMESTAMP columns and the calendar functions.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_temporal.py -v --tb=short
"""
from datetime import date, datetime, timedelta

import pytest
import gnitz
from _uid import uid as _uid

D = date(2024, 2, 29)
TS = datetime(2024, 2, 29, 13, 45, 7, 250_000)
EPOCH_DAYS = (D - date(1970, 1, 1)).days


def _cleanup(client, sn, *names):
    for name in names:
        for kind in ("VIEW", "TABLE"):
            try:
                client.execute_sql(f"DROP {kind} {name}", schema_name=sn)
            except Exception:
                pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _rows(client, sn, q):
    res = client.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", res
    return list(res["rows"])


def _view(client, sn, name):
    return client.scan(client.resolve_table(sn, name)[0]).mappings()


class TestTemporalColumns:
    def test_ddl_literals_and_rendering(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, d DATE, ts TIMESTAMP)",
                schema_name=sn,
            )
            tid, schema = client.resolve_table(sn, "t")
            assert schema.columns[1].type_code == gnitz.TypeCode.DATE
            assert schema.columns[2].type_code == gnitz.TypeCode.TIMESTAMP
            client.execute_sql(
                "INSERT INTO t VALUES "
                "(1, DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:07.25'), "
                "(2, '1969-12-31', '1969-12-31T23:59:59'), "
                "(3, CAST('2000-01-01' AS DATE), CAST('2000-01-01 00:00:00' AS TIMESTAMP)), "
                "(4, NULL, NULL)",
                schema_name=sn,
            )
            rows = {r["id"]: (r["d"], r["ts"]) for r in client.scan(tid).mappings()}
            assert rows == {
                1: (D, TS),
                2: (date(1969, 12, 31), datetime(1969, 12, 31, 23, 59, 59)),
                3: (date(2000, 1, 1), datetime(2000, 1, 1)),
                4: (None, None),
            }
        finally:
            _cleanup(client, sn, "t")

    def test_append_accepts_objects_and_ints(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, d DATE NOT NULL, ts TIMESTAMP NOT NULL)",
                schema_name=sn,
            )
            tid, schema = client.resolve_table(sn, "t")
            batch = gnitz.ZSetBatch(schema)
            batch.append(id=1, d=D, ts=TS)
            batch.append(id=2, d=EPOCH_DAYS, ts=EPOCH_DAYS * 86_400_000_000)
            batch.append(id=3, d=TS, ts=D)
            client.push(tid, batch)
            rows = {r["id"]: (r["d"], r["ts"]) for r in client.scan(tid).mappings()}
            assert rows == {1: (D, TS), 2: (D, datetime(2024, 2, 29)), 3: (D, datetime(2024, 2, 29))}
            with pytest.raises(ValueError, match="naive"):
                gnitz.ZSetBatch(schema).append(id=9, d=D, ts=datetime.now().astimezone())
            # A string is not a DATE on the append path: the SQL literal forms
            # are the planner's, not the batch builder's.
            with pytest.raises(TypeError):
                gnitz.ZSetBatch(schema).append(id=9, d="2024-02-29", ts=TS)
        finally:
            _cleanup(client, sn, "t")

    def test_date_primary_key_routes_and_seeks(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (d DATE NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            tid, schema = client.resolve_table(sn, "t")
            assert schema.columns[0].type_code == gnitz.TypeCode.DATE
            days = [D + timedelta(days=i) for i in range(12)]
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(f"('{x.isoformat()}', {i})" for i, x in enumerate(days)),
                schema_name=sn,
            )
            hit = client.seek(tid, pk=D + timedelta(days=5)).mappings()
            assert [(r["d"], r["v"]) for r in hit] == [(D + timedelta(days=5), 5)]
            assert client.seek(tid, pk=date(1999, 1, 1)).mappings() == []
            client.execute_sql("DELETE FROM t WHERE d = DATE '2024-03-05'", schema_name=sn)
            client.execute_sql("DELETE FROM t WHERE d = '2024-03-06'", schema_name=sn)
            client.execute_sql("UPDATE t SET v = v + 100 WHERE d >= DATE '2024-03-09'", schema_name=sn)
            rows = sorted((r["d"], r["v"]) for r in client.scan(tid).mappings())
            want = [(x, i + (100 if x >= date(2024, 3, 9) else 0)) for i, x in enumerate(days) if x not in (date(2024, 3, 5), date(2024, 3, 6))]
            assert rows == want
            with pytest.raises(Exception, match="not a valid date"):
                client.execute_sql("INSERT INTO t VALUES ('2024-02-30', 1)", schema_name=sn)
        finally:
            _cleanup(client, sn, "t")


class TestCalendarFunctions:
    def _seed(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, d DATE, ts TIMESTAMP, amt BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES "
            "(1, DATE '2024-02-29', TIMESTAMP '2024-02-29 13:45:07', 10), "
            "(2, DATE '2024-03-31', TIMESTAMP '2024-03-31 23:59:59.999999', 20), "
            "(3, DATE '2021-01-03', TIMESTAMP '1969-12-31 23:00:00', 30), "
            "(4, NULL, NULL, 40)",
            schema_name=sn,
        )

    def test_extract_and_trunc_in_a_view(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._seed(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, "
                "EXTRACT(YEAR FROM d) AS y, EXTRACT(MONTH FROM d) AS m, EXTRACT(DAY FROM d) AS dd, "
                "EXTRACT(DOW FROM d) AS dow, EXTRACT(ISODOW FROM d) AS isodow, EXTRACT(DOY FROM d) AS doy, "
                "EXTRACT(WEEK FROM d) AS wk, EXTRACT(QUARTER FROM d) AS q, "
                "EXTRACT(HOUR FROM ts) AS h, DATE_PART('minute', ts) AS mi, EXTRACT(SECOND FROM ts) AS s, "
                "EXTRACT(EPOCH FROM ts) AS ep, "
                "DATE_TRUNC('month', d) AS dm, DATE_TRUNC('hour', ts) AS th, DATE_TRUNC('week', d) AS dw, "
                "CAST(ts AS DATE) AS tsd, CAST(d AS TIMESTAMP) AS dts, "
                "d + 1 AS tomorrow, d - DATE '2024-01-01' AS since_ny, ts - 1000000 AS ts_minus_1s "
                "FROM t",
                schema_name=sn,
            )
            _, vs = client.resolve_table(sn, "v")
            types = {c.name: c.type_code for c in vs.columns}
            assert types["y"] == gnitz.TypeCode.I64
            assert types["dm"] == gnitz.TypeCode.DATE
            assert types["dw"] == gnitz.TypeCode.DATE
            assert types["th"] == gnitz.TypeCode.TIMESTAMP
            assert types["tsd"] == gnitz.TypeCode.DATE
            assert types["dts"] == gnitz.TypeCode.TIMESTAMP
            assert types["tomorrow"] == gnitz.TypeCode.DATE
            assert types["since_ny"] == gnitz.TypeCode.I64
            assert types["ts_minus_1s"] == gnitz.TypeCode.TIMESTAMP
            rows = {r["id"]: r for r in _view(client, sn, "v")}
            r = rows[1]
            assert (r["y"], r["m"], r["dd"], r["dow"], r["isodow"], r["doy"], r["wk"], r["q"]) == (2024, 2, 29, 4, 4, 60, 9, 1)
            assert (r["h"], r["mi"], r["s"]) == (13, 45, 7)
            assert r["ep"] == int((datetime(2024, 2, 29, 13, 45, 7) - datetime(1970, 1, 1)).total_seconds())
            assert (r["dm"], r["th"], r["dw"]) == (date(2024, 2, 1), datetime(2024, 2, 29, 13), date(2024, 2, 26))
            assert (r["tsd"], r["dts"]) == (date(2024, 2, 29), datetime(2024, 2, 29))
            assert (r["tomorrow"], r["since_ny"], r["ts_minus_1s"]) == (date(2024, 3, 1), 59, datetime(2024, 2, 29, 13, 45, 6))
            r = rows[2]
            assert (r["dm"], r["th"], r["tsd"]) == (date(2024, 3, 1), datetime(2024, 3, 31, 23), date(2024, 3, 31))
            r = rows[3]
            # 2021-01-03 is a Sunday in ISO week 53 of 2020; a pre-epoch timestamp floors.
            assert (r["dow"], r["isodow"], r["wk"], r["dw"]) == (0, 7, 53, date(2020, 12, 28))
            assert (r["h"], r["tsd"], r["th"]) == (23, date(1969, 12, 31), datetime(1969, 12, 31, 23))
            assert r["ep"] == -3600
            r = rows[4]
            assert all(r[k] is None for k in ("y", "h", "dm", "th", "tsd", "dts", "tomorrow", "since_ny"))
        finally:
            _cleanup(client, sn, "v", "t")

    def test_month_bucketing_and_extrema_maintain(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._seed(client, sn)
            client.execute_sql(
                "CREATE VIEW bym AS SELECT DATE_TRUNC('month', d) AS month, SUM(amt) AS total, "
                "MIN(ts) AS first_ts, MAX(d) AS last_d, COUNT(*) AS n FROM t GROUP BY DATE_TRUNC('month', d)",
                schema_name=sn,
            )
            _, vs = client.resolve_table(sn, "bym")
            types = {c.name: c.type_code for c in vs.columns}
            assert (types["month"], types["first_ts"], types["last_d"]) == (
                gnitz.TypeCode.DATE, gnitz.TypeCode.TIMESTAMP, gnitz.TypeCode.DATE)
            client.execute_sql("INSERT INTO t VALUES (5, DATE '2024-02-01', TIMESTAMP '2024-02-01 00:00:00', 5)", schema_name=sn)

            def snapshot():
                return {r["month"]: (r["total"], r["first_ts"], r["last_d"], r["n"]) for r in _view(client, sn, "bym")}

            assert snapshot() == {
                date(2024, 2, 1): (15, datetime(2024, 2, 1), date(2024, 2, 29), 2),
                date(2024, 3, 1): (20, datetime(2024, 3, 31, 23, 59, 59, 999_999), date(2024, 3, 31), 1),
                date(2021, 1, 1): (30, datetime(1969, 12, 31, 23), date(2021, 1, 3), 1),
                None: (40, None, None, 1),
            }
            client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
            assert snapshot()[date(2024, 2, 1)] == (5, datetime(2024, 2, 1), date(2024, 2, 1), 1)
        finally:
            _cleanup(client, sn, "bym", "t")

    def test_filters_and_adhoc(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._seed(client, sn)
            client.execute_sql(
                "CREATE VIEW recent AS SELECT id, d FROM t WHERE ts >= TIMESTAMP '2024-03-01 00:00:00' OR d < '2022-01-01'",
                schema_name=sn,
            )
            assert sorted(r["id"] for r in _view(client, sn, "recent")) == [2, 3]
            rows = _rows(client, sn, "SELECT id, d, EXTRACT(YEAR FROM ts) AS y FROM t WHERE d = DATE '2024-02-29'")
            assert [(r.id, r.d, r.y) for r in rows] == [(1, D, 2024)]
            rows = _rows(client, sn, "SELECT COUNT(*) AS n FROM t WHERE d BETWEEN '2024-01-01' AND '2024-12-31'")
            assert rows[0].n == 2
        finally:
            _cleanup(client, sn, "recent", "t")

    @pytest.mark.parametrize(
        "stmt, needle",
        [
            ("CREATE VIEW v AS SELECT id, NOW() AS n FROM t", "non-deterministic"),
            ("CREATE VIEW v AS SELECT id, CURRENT_DATE AS n FROM t", "non-deterministic"),
            ("CREATE VIEW v AS SELECT id, EXTRACT(YEAR FROM amt) AS y FROM t", "DATE or TIMESTAMP"),
            ("CREATE VIEW v AS SELECT id, DATE_TRUNC('fortnight', d) AS y FROM t", "not supported"),
            ("CREATE VIEW v AS SELECT SUM(d) AS s FROM t", "not supported"),
            ("CREATE VIEW v AS SELECT id, DATE '2024-02-30' AS x FROM t", "invalid DATE literal"),
            ("CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, z TIMESTAMP WITH TIME ZONE)", "time zone"),
        ],
    )
    def test_rejections(self, client, stmt, needle):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._seed(client, sn)
            with pytest.raises(Exception, match=needle):
                client.execute_sql(stmt, schema_name=sn)
        finally:
            _cleanup(client, sn, "v", "t2", "t")
