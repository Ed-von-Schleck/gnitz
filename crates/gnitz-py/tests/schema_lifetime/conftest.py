import pytest


@pytest.fixture
def base(client, schema_name):
    """`schema_name`, holding `t(id, a, b)` with rows `(1, 10, 100)` and
    `(2, 20, 200)`."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)", schema_name=schema_name)
    return schema_name
