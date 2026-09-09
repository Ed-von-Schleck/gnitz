"""`CREATE INDEX` / `DROP INDEX` naming something that cannot be indexed or does
not exist.
"""

import pytest
import gnitz


@pytest.mark.parametrize("sql", [
    "CREATE INDEX ON t(ghost)",     # no such column
    "CREATE INDEX ON t(s)",         # STRING is not an indexable type
    "CREATE INDEX ON t(a, a)",      # the same column twice
    "DROP INDEX nonexistent__t__idx_col",
])
def test_index_ddl_rejected(client, schema_name, sql):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
        "a BIGINT NOT NULL, s TEXT NOT NULL)",
        schema_name=schema_name,
    )
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(sql, schema_name=schema_name)
