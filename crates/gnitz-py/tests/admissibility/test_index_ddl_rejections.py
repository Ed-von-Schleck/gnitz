"""`CREATE INDEX` / `DROP INDEX` naming something that cannot be indexed or does
not exist.
"""

import pytest
import gnitz
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













class TestIndexDdlRejections:

    def test_create_index_nonexistent_col(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE INDEX ON t(ghost)", schema_name=sn)
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_drop_nonexistent_index(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "DROP INDEX nonexistent__t__idx_col", schema_name=sn
                )
        finally:
            client.drop_schema(sn)

    def test_create_index_limits(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, s VARCHAR NOT NULL)",
                schema_name=sn,
            )
            # Index on a STRING column is rejected.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE INDEX ON t(s)", schema_name=sn)
            # Duplicate column in the index list is rejected.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE INDEX ON t(a, a)", schema_name=sn)
        finally:
            _drop_all(client, sn, tables=["t"])
