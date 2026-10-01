import pytest


@pytest.fixture
def base(client):
    """`t(id, a, b)` holding `(1, 10, 100)` and `(2, 20, 200)`."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)")
