"""`_system.clock` is the server's: no client write or DDL reaches it. What a
view body may do with NOW() is pinned in the planner's own tests."""

import pytest
import gnitz


@pytest.mark.parametrize(
    "sql, match",
    [
        ("INSERT INTO _system.clock VALUES (0, TIMESTAMP '2000-01-01 00:00:00')", "is a system table"),
        ("DELETE FROM _system.clock WHERE id = 0", "is a system table"),
        ("UPDATE _system.clock SET now = TIMESTAMP '2000-01-01 00:00:00'", "is a system table"),
        ("CREATE INDEX clock_now ON _system.clock(now)", "is a system table"),
        ("DROP TABLE _system.clock", "system"),
    ],
)
def test_the_clock_takes_no_client_write(client, sql, match):
    with pytest.raises(gnitz.GnitzError, match=match):
        client.execute_sql(sql)


def test_a_push_to_the_clock_is_refused(client):
    tid, schema = client.resolve_table("_system.clock")
    with pytest.raises(gnitz.GnitzRefusedError, match="written by the server alone"):
        client.push(tid, gnitz.ZSetBatch(schema).extend([{"id": 0, "now": 0}]))
