"""E2E tests for maintained top-N views: `CREATE VIEW … ORDER BY … LIMIT n
[OFFSET m]` (the global window) and `QUALIFY ROW_NUMBER() OVER (PARTITION BY
… ORDER BY …) <= n` (one window per partition).

The operator keeps an ordered index of every input row, so retracting the
leader promotes the next row from the index — the assertions below are over
the maintained *state* after inserts, deletes and updates, weights included.
Run with GNITZ_WORKERS=4: the global window is two-phase (a local window per
worker, exchanged and cut once), and the determinism guarantee is that the
selected rows are a function of the data alone.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/relational_shape/test_top_n_views.py -v --tb=short
"""

import pytest
import gnitz
from _uid import uid as _uid


def _cleanup(client, sn):
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _rows(client, sn, view, keys):
    """The view's rows as key tuples with their weights, sorted."""
    vid = client.resolve_table(sn, view)[0]
    out = []
    for r in client.scan(vid):
        d = r._asdict()
        out.append(tuple(d[k] for k in keys) + (r.weight,))
    return sorted(out, key=lambda t: tuple((x is None, x) for x in t))


def _scores(client, sn, score="BIGINT"):
    client.execute_sql(
        "CREATE TABLE scores (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
        f"score {score}, name VARCHAR(30) NOT NULL)",
        schema_name=sn,
    )


class TestGlobalTopN:
    def test_top_n_desc_promotes_on_delete_and_update(self, client):
        sn = "tn" + _uid()
        client.create_schema(sn)
        try:
            _scores(client, sn)
            client.execute_sql(
                "CREATE VIEW top2 AS SELECT id, score FROM scores ORDER BY score DESC LIMIT 2",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO scores VALUES (1, 0, 10, 'a'), (2, 0, 30, 'b'), (3, 0, 20, 'c'), (4, 0, 5, 'd')",
                schema_name=sn,
            )
            assert _rows(client, sn, "top2", ["id", "score"]) == [(2, 30, 1), (3, 20, 1)]
            # Delete the leader: id 1 is promoted from the index.
            client.execute_sql("DELETE FROM scores WHERE id = 2", schema_name=sn)
            assert _rows(client, sn, "top2", ["id", "score"]) == [(1, 10, 1), (3, 20, 1)]
            # A row below the window rises into it.
            client.execute_sql("UPDATE scores SET score = 25 WHERE id = 4", schema_name=sn)
            assert _rows(client, sn, "top2", ["id", "score"]) == [(3, 20, 1), (4, 25, 1)]
            # A row in the window falls out of it.
            client.execute_sql("UPDATE scores SET score = 1 WHERE id = 3", schema_name=sn)
            assert _rows(client, sn, "top2", ["id", "score"]) == [(1, 10, 1), (4, 25, 1)]
            # Empty the table: the window empties.
            client.execute_sql("DELETE FROM scores", schema_name=sn)
            assert _rows(client, sn, "top2", ["id", "score"]) == []
        finally:
            _cleanup(client, sn)

    def test_offset_multi_key_and_null_placement(self, client):
        sn = "tn" + _uid()
        client.create_schema(sn)
        try:
            _scores(client, sn)
            # ASC default is NULLS LAST; the second key orders ties.
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, score, name FROM scores "
                "ORDER BY score ASC, name DESC LIMIT 2 OFFSET 1",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO scores VALUES (1, 0, 5, 'x'), (2, 0, 5, 'y'), (3, 0, NULL, 'n'), "
                "(4, 0, 7, 'z'), (5, 0, 1, 'w')",
                schema_name=sn,
            )
            # Order: 5(w,1), 2(y,5), 1(x,5), 4(z,7), 3(NULL) → skip 5, keep 2 and 1.
            assert _rows(client, sn, "v", ["id"]) == [(1, 1), (2, 1)]
            # Delete the skipped row: the offset now skips id 2.
            client.execute_sql("DELETE FROM scores WHERE id = 5", schema_name=sn)
            assert _rows(client, sn, "v", ["id"]) == [(1, 1), (4, 1)]

            client.execute_sql(
                "CREATE VIEW nf AS SELECT id FROM scores ORDER BY score ASC NULLS FIRST LIMIT 1",
                schema_name=sn,
            )
            assert _rows(client, sn, "nf", ["id"]) == [(3, 1)]
            client.execute_sql(
                "CREATE VIEW nd AS SELECT id FROM scores ORDER BY score DESC LIMIT 1",
                schema_name=sn,
            )
            # DESC default is NULLS FIRST.
            assert _rows(client, sn, "nd", ["id"]) == [(3, 1)]
        finally:
            _cleanup(client, sn)

    def test_hidden_key_string_key_and_position(self, client):
        sn = "tn" + _uid()
        client.create_schema(sn)
        try:
            _scores(client, sn)
            # A key that is not projected rides as a hidden column; a positional
            # key names a projected one.
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, name FROM scores ORDER BY score * 2 DESC, 2 LIMIT 2",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO scores VALUES (1, 0, 10, 'b'), (2, 0, 10, 'a'), (3, 0, 20, 'c')",
                schema_name=sn,
            )
            assert _rows(client, sn, "v", ["id", "name"]) == [(2, "a", 1), (3, "c", 1)]
            vid = client.resolve_table(sn, "v")[0]
            assert sorted(next(iter(client.scan(vid)))._asdict().keys()) == ["id", "name"]
            client.execute_sql(
                "CREATE VIEW s AS SELECT id FROM scores ORDER BY name DESC LIMIT 1",
                schema_name=sn,
            )
            assert _rows(client, sn, "s", ["id"]) == [(3, 1)]
        finally:
            _cleanup(client, sn)

    def test_weights_fill_slots(self, client):
        """A stream's PK is not unique, so an event pushed twice is one element
        at weight 2 — which fills two slots of the window."""
        sn = "tn" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, score BIGINT NOT NULL) WITH (stream = true)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, score FROM ev ORDER BY score DESC LIMIT 3",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO ev VALUES (1, 10), (2, 30), (3, 20), (2, 30)", schema_name=sn)
            # Slots: 30, 30, 20, 10 → id 2 at weight 2, id 3 at weight 1.
            assert _rows(client, sn, "v", ["id"]) == [(2, 2), (3, 1)]
            client.execute_sql("INSERT INTO ev VALUES (4, 40)", schema_name=sn)
            # Slots: 40, 30, 30, 20 → the boundary cuts id 2 to weight 2 still, id 3 out.
            assert _rows(client, sn, "v", ["id"]) == [(2, 2), (4, 1)]
        finally:
            _cleanup(client, sn)

    def test_over_grouped_and_set_op_bodies(self, client):
        sn = "tn" + _uid()
        client.create_schema(sn)
        try:
            _scores(client, sn)
            client.execute_sql(
                "CREATE VIEW leaders AS SELECT grp, SUM(score) AS total FROM scores "
                "GROUP BY grp ORDER BY total DESC LIMIT 2",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO scores VALUES (1, 1, 10, 'a'), (2, 1, 12, 'b'), (3, 2, 15, 'c'), "
                "(4, 3, 30, 'd'), (5, 4, 1, 'e')",
                schema_name=sn,
            )
            assert _rows(client, sn, "leaders", ["grp", "total"]) == [(1, 22, 1), (3, 30, 1)]
            client.execute_sql("INSERT INTO scores VALUES (6, 4, 100, 'f')", schema_name=sn)
            assert _rows(client, sn, "leaders", ["grp", "total"]) == [(3, 30, 1), (4, 101, 1)]

            client.execute_sql(
                "CREATE VIEW u AS SELECT id, score FROM scores WHERE grp = 1 "
                "UNION SELECT id, score FROM scores WHERE grp = 4 ORDER BY score LIMIT 2",
                schema_name=sn,
            )
            assert _rows(client, sn, "u", ["id"]) == [(1, 1), (5, 1)]
        finally:
            _cleanup(client, sn)

    def test_rejections(self, client):
        sn = "tn" + _uid()
        client.create_schema(sn)
        try:
            _scores(client, sn)
            for sql, needle in [
                ("CREATE VIEW v AS SELECT id FROM scores ORDER BY score", "ORDER BY without LIMIT"),
                ("CREATE VIEW v AS SELECT id FROM scores LIMIT 3", "LIMIT without ORDER BY"),
                ("CREATE VIEW v AS SELECT id FROM scores ORDER BY score LIMIT 0", "LIMIT 0"),
                # The view body honours the ORDER BY / LIMIT pair, so a leftover
                # OFFSET must still be refused rather than silently dropped.
                ("CREATE VIEW v AS SELECT id FROM scores OFFSET 5", "OFFSET without"),
                ("CREATE VIEW v AS SELECT DISTINCT grp FROM scores ORDER BY score LIMIT 1", "selected column"),
                (
                    "CREATE VIEW v WITH (capacity = '4 MB') AS SELECT id FROM scores ORDER BY score LIMIT 1",
                    "capacity",
                ),
                (
                    "CREATE VIEW v AS SELECT id FROM scores UNION SELECT id FROM scores ORDER BY score LIMIT 1",
                    "output column or position",
                ),
            ]:
                with pytest.raises(Exception) as ei:
                    client.execute_sql(sql, schema_name=sn)
                assert needle in str(ei.value), (sql, str(ei.value))
        finally:
            _cleanup(client, sn)


class TestPartitionedTopN:
    def test_partition_by_a_permuted_compound_pk(self, client):
        """PARTITION BY naming the whole PK in a different order is one row per
        PK. The output is keyed by the PK in *pk-list* order, so the operator
        has to shard by that order too — otherwise the rows land on a worker the
        multi-worker gather does not look for them on."""
        sn = "tn" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE pairs (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, "
                "PRIMARY KEY (a, b))",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW p AS SELECT a, b, v FROM pairs "
                "QUALIFY ROW_NUMBER() OVER (PARTITION BY b, a ORDER BY v DESC) <= 1",
                schema_name=sn,
            )
            rows = ", ".join(f"({a}, {b}, {a * 10 + b})" for a in range(4) for b in range(4))
            client.execute_sql(f"INSERT INTO pairs VALUES {rows}", schema_name=sn)
            # Every (a, b) is its own partition, so every row survives.
            assert _rows(client, sn, "p", ["a", "b"]) == sorted(
                (a, b, 1) for a in range(4) for b in range(4)
            )
            client.execute_sql("DELETE FROM pairs WHERE a = 2", schema_name=sn)
            assert _rows(client, sn, "p", ["a", "b"]) == sorted(
                (a, b, 1) for a in range(4) if a != 2 for b in range(4)
            )
        finally:
            _cleanup(client, sn)


    def test_qualify_row_number_is_a_window_per_partition(self, client):
        sn = "tn" + _uid()
        client.create_schema(sn)
        try:
            # A window key must be NOT NULL: the rule is the desugar's, and it is
            # checked before the top-N rewrite is chosen.
            _scores(client, sn, score="BIGINT NOT NULL")
            client.execute_sql(
                "CREATE VIEW top1 AS SELECT grp, id, score FROM scores "
                "QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) <= 1",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO scores VALUES (1, 1, 10, 'a'), (2, 1, 30, 'b'), (3, 2, 20, 'c'), (4, 2, 25, 'd')",
                schema_name=sn,
            )
            assert _rows(client, sn, "top1", ["grp", "id"]) == [(1, 2, 1), (2, 4, 1)]
            client.execute_sql("DELETE FROM scores WHERE id = 2", schema_name=sn)
            assert _rows(client, sn, "top1", ["grp", "id"]) == [(1, 1, 1), (2, 4, 1)]
            # A partition emptied loses its row; a new partition gains one.
            client.execute_sql("DELETE FROM scores WHERE id = 1", schema_name=sn)
            client.execute_sql("INSERT INTO scores VALUES (5, 3, 1, 'e')", schema_name=sn)
            assert _rows(client, sn, "top1", ["grp", "id"]) == [(2, 4, 1), (3, 5, 1)]

            # `< n` and the literal on the left mean the same window.
            client.execute_sql(
                "CREATE VIEW top2 AS SELECT grp, id FROM scores "
                "QUALIFY 3 > ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO scores VALUES (6, 2, 5, 'f'), (7, 2, 50, 'g')",
                schema_name=sn,
            )
            assert _rows(client, sn, "top2", ["grp", "id"]) == [(2, 3, 1), (2, 6, 1), (3, 5, 1)]
        finally:
            _cleanup(client, sn)

    def test_projected_row_number_keeps_the_desugar(self, client):
        """`rn` in the SELECT list is a value the top-N cannot supply, so the
        band-join desugar answers, and both spellings agree on the rows."""
        sn = "tn" + _uid()
        client.create_schema(sn)
        try:
            _scores(client, sn, score="BIGINT NOT NULL")
            client.execute_sql(
                "CREATE VIEW ranked AS SELECT grp, id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) AS rn "
                "FROM scores QUALIFY rn <= 2",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW cut AS SELECT grp, id FROM scores "
                "QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) <= 2",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO scores VALUES (1, 1, 10, 'a'), (2, 1, 30, 'b'), (3, 1, 20, 'c'), (4, 2, 25, 'd')",
                schema_name=sn,
            )
            assert _rows(client, sn, "ranked", ["grp", "id"]) == _rows(client, sn, "cut", ["grp", "id"])
            assert _rows(client, sn, "ranked", ["grp", "id", "rn"]) == [(1, 2, 1, 1), (1, 3, 2, 1), (2, 4, 1, 1)]
        finally:
            _cleanup(client, sn)


def test_top_n_view_survives_restart(own_server):
    """The ordered index is checkpointed with the view: after a restart the
    window keeps moving under new writes, promotion included."""
    sock_path = own_server.sock_path
    own_server.start()
    conn = gnitz.connect(sock_path)
    conn.create_schema("tn")
    _scores(conn, "tn")
    conn.execute_sql(
        "CREATE VIEW top2 AS SELECT id, score FROM scores ORDER BY score DESC LIMIT 2",
        schema_name="tn",
    )
    conn.execute_sql(
        "INSERT INTO scores VALUES (1, 0, 10, 'a'), (2, 0, 30, 'b'), (3, 0, 20, 'c')",
        schema_name="tn",
    )
    assert _rows(conn, "tn", "top2", ["id"]) == [(2, 1), (3, 1)]
    conn.close()

    own_server.restart()
    conn = gnitz.connect(sock_path)
    assert _rows(conn, "tn", "top2", ["id"]) == [(2, 1), (3, 1)]
    conn.execute_sql("DELETE FROM scores WHERE id = 2", schema_name="tn")
    assert _rows(conn, "tn", "top2", ["id"]) == [(1, 1), (3, 1)]
    conn.close()
