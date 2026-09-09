"""A view whose body is one relation under a WHERE.
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













class TestFilterView:

    def test_create_view_with_filter(self, client):
        """CREATE VIEW with a WHERE clause succeeds."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            results = client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 10",
                schema_name=sn,
            )
            assert results[0]["type"] == "ViewCreated"
        finally:
            _drop_all(client, sn, views=["v"], tables=["t"])

    def test_scan_view(self, client):
        """Rows inserted into a source table appear in an associated view."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 10",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=sn)

            vid, _ = client.resolve_table(sn, "v")
            result = client.scan(vid)
            assert result.schema is not None
            assert len(result.pks) == 1
        finally:
            _drop_all(client, sn, views=["v"], tables=["t"])
