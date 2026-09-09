"""A view over two base tables ticks from either source, before and after a crash
restart.
"""

import gnitz


def test_two_source_view_maintained_from_both_sources_across_restart(own_server):
    """A view over two base tables ticks from either source, and still does
    after a crash restart — the rebuild and the tick fan-out read the same
    graph."""
    sock_path = own_server.sock_path
    own_server.start()
    conn = gnitz.connect(sock_path)

    conn.create_schema("s")
    conn.execute_sql(
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name="s")
    conn.execute_sql(
        "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name="s")
    conn.execute_sql(
        "CREATE VIEW ab AS SELECT pk, val FROM a UNION ALL SELECT pk, val FROM b",
        schema_name="s")

    def view_vals(c):
        vid = c.resolve_table("s", "ab")[0]
        return sorted(r["val"] for r in c.scan(vid))

    conn.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name="s")
    conn.execute_sql("INSERT INTO b VALUES (2, 20)", schema_name="s")
    assert view_vals(conn) == [10, 20], "the view must tick from both sources"

    conn.close()
    own_server.restart()
    conn = gnitz.connect(sock_path)

    assert view_vals(conn) == [10, 20], "the recovered view keeps both sources' rows"
    conn.execute_sql("INSERT INTO a VALUES (3, 30)", schema_name="s")
    conn.execute_sql("INSERT INTO b VALUES (4, 40)", schema_name="s")
    assert view_vals(conn) == [10, 20, 30, 40], (
        "after a restart the view must still tick from both sources")

    conn.close()
