"""`CREATE INDEX` / `DROP INDEX` naming something that cannot be indexed or does
not exist.
"""

import pytest
import gnitz


@pytest.mark.parametrize("sql", [
    "CREATE INDEX ON t(ghost)",     # no such column
    # An index key is an order-preserving byte string: IEEE-754 breaks the
    # byte-equal key contract and a string has no fixed width, so none of these
    # can carry one.
    "CREATE INDEX ON t(s)",
    "CREATE INDEX ON t(vc)",
    "CREATE INDEX ON t(f)",
    "CREATE INDEX ON t(d)",
    "CREATE INDEX ON t(a, a)",      # the same column twice
    "DROP INDEX nonexistent__t__idx_col",
])
def test_index_ddl_rejected(client, schema_name, sql):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "s TEXT NOT NULL, vc VARCHAR NOT NULL, f FLOAT NOT NULL, d DOUBLE NOT NULL)",
        schema_name=schema_name,
    )
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(sql, schema_name=schema_name)
