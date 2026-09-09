"""An index seek is as fresh as a scan: a seek issued right after a push must see
the pushed row.
"""

from uuid import uuid4



def _sn():
    """Unique schema name for test isolation."""
    return "idx" + uuid4().hex[:8]


def _drop_all(client, sn, tables=(), views=(), indices=()):
    """Drop tables, views, and indices before dropping schema."""
    for idx in indices:
        try:
            client.execute_sql(f"DROP INDEX {idx}", schema_name=sn)
        except Exception:
            pass
    for v in views:
        try:
            client.execute_sql(f"DROP VIEW {v}", schema_name=sn)
        except Exception:
            pass
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    client.drop_schema(sn)













class TestIndexReadBarrier:
    """Validates that index seeks after pushes see fresh data.

    The server fires pending ticks before index seeks to ensure derived
    index views are up-to-date.
    """

    def test_index_seek_immediately_after_push(self, client):
        """Push a row, then immediately seek by index — must find it."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY,"
                " val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 42)", schema_name=sn)
            results = client.execute_sql(
                "SELECT * FROM t WHERE val = 42", schema_name=sn)
            assert results[0]["type"] == "Rows"
            rows = results[0]["rows"]
            assert len(rows.pks) == 1
            assert rows.pks[0] == 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_index_seek_multiple_pushes(self, client):
        """Push several rows, seek by index for each — all found."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY,"
                " val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
            for i in range(5):
                client.execute_sql(
                    f"INSERT INTO t VALUES ({i + 1}, {(i + 1) * 100})",
                    schema_name=sn,
                )
            for i in range(5):
                results = client.execute_sql(
                    f"SELECT * FROM t WHERE val = {(i + 1) * 100}",
                    schema_name=sn,
                )
                assert results[0]["type"] == "Rows"
                rows = results[0]["rows"]
                assert len(rows.pks) == 1
                assert rows.pks[0] == i + 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_index_seek_anticorrelated_multi_source(self, client):
        """Index seek must hold when the indexed value and source PK are
        ANTI-correlated across many pushes.

        A secondary index has a compound PK ``(indexed_value, src_pk)``. The
        read-cursor's N-way merge must order it the same way storage sorts
        each run — column order ``(value, src_pk)`` — not by the raw u128
        view of the PK bytes (which would order by ``(src_pk, value)`` because
        src_pk lands in the high 64 bits). Correlated data (value rising with
        pk) hides the difference; anti-correlated data (value falling as pk
        rises) makes the two orders disagree, so a wrong merge order seeks the
        wrong run's head and the lookup misses.
        """
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY,"
                " val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
            n = 6
            # Each INSERT is a separate push -> a separate index run, so the
            # lookup below merges N anti-correlated sources.
            for i in range(n):
                pk = i + 1
                val = (n - i) * 100  # pk=1->600, pk=2->500, ... pk=6->100
                client.execute_sql(
                    f"INSERT INTO t VALUES ({pk}, {val})", schema_name=sn)
            for i in range(n):
                pk = i + 1
                val = (n - i) * 100
                results = client.execute_sql(
                    f"SELECT * FROM t WHERE val = {val}", schema_name=sn)
                assert results[0]["type"] == "Rows"
                rows = results[0]["rows"]
                assert len(rows.pks) == 1, \
                    f"val={val} (pk={pk}) not found — index merge order bug"
                assert rows.pks[0] == pk, \
                    f"val={val} resolved to pk={rows.pks[0]}, expected {pk}"
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])
