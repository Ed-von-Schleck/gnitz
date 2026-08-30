"""HIR commit 6 — new combine-class compositions the routing flip enables:
nested/mixed set-op chains, computed DISTINCT / set-op-identity columns
(approver-enabled), a grouped set-op side, a join-FROM set-op side, and
same-relation INTERSECT/EXCEPT (the uniform collision wrapper). Maintained
incrementally under inserts *and* deletes, and under the data-before-view
(backfill) order — asserted on **weights**, not row presence."""

from _uid import uid as _uid


def _rows(client, sn, view, keys):
    vid = client.resolve_table(sn, view)[0]
    rows = [tuple(r._asdict()[k] for k in keys) for r in client.scan(vid)]
    return sorted(rows)


def _weights(client, sn, view, keys):
    """Net weight per row — the Z-set observable. A row present at the wrong
    multiplicity, or a ghost at weight 0, is invisible to `_rows` but not here."""
    vid = client.resolve_table(sn, view)[0]
    acc = {}
    for r in client.scan(vid):
        k = tuple(r._asdict()[c] for c in keys)
        acc[k] = acc.get(k, 0) + r.weight
    return {k: w for k, w in sorted(acc.items()) if w != 0}


def _tu(client, sn):
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)", schema_name=sn)


def test_nested_setop_chain(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        # (A UNION B) UNION C — inner set-op cut to a segment, outer re-hashes.
        client.execute_sql(
            "CREATE VIEW v AS SELECT a FROM t UNION SELECT a FROM u UNION SELECT b FROM t", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 1, 2), (2, 3, 4)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (1, 5, 6)", schema_name=sn)
        # {1,3} ∪ {5} ∪ {2,4} = {1,2,3,4,5}
        assert _rows(client, sn, "v", ["a"]) == [(1,), (2,), (3,), (4,), (5,)]
        client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)  # drops a=3, b=4
        assert _rows(client, sn, "v", ["a"]) == [(1,), (2,), (5,)]
    finally:
        client.drop_schema(sn)


def test_mixed_nested_setop(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS (SELECT a FROM t UNION ALL SELECT a FROM u) INTERSECT SELECT b FROM t", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 7, 7), (2, 3, 9)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (1, 9, 0)", schema_name=sn)
        # left (UNION ALL) values = {7,3,9}; right (t.b) = {7,9}; INTERSECT = {7,9}
        assert _rows(client, sn, "v", ["a"]) == [(7,), (9,)]
    finally:
        client.drop_schema(sn)


def test_computed_distinct(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        # Computed DISTINCT identity column (materialize-then-hash).
        client.execute_sql("CREATE VIEW v AS SELECT DISTINCT a + 1 AS a1, b FROM t", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 1, 2), (2, 1, 2), (3, 3, 2)", schema_name=sn)
        # distinct (a+1, b) = {(2,2), (4,2)}
        assert _rows(client, sn, "v", ["a1", "b"]) == [(2, 2), (4, 2)]
        client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)  # a duplicate (2,2) remains via id=2
        assert _rows(client, sn, "v", ["a1", "b"]) == [(2, 2), (4, 2)]
    finally:
        client.drop_schema(sn)


def test_computed_setop_side(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        # Computed set-op-identity column on the EXCEPT left side.
        client.execute_sql("CREATE VIEW v AS SELECT a * 2 AS x FROM t EXCEPT SELECT b FROM u", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 3, 0), (2, 5, 0)", schema_name=sn)  # x = {6, 10}
        client.execute_sql("INSERT INTO u VALUES (1, 0, 6)", schema_name=sn)             # b = {6}
        # {6,10} EXCEPT {6} = {10}
        assert _rows(client, sn, "v", ["x"]) == [(10,)]
        client.execute_sql("DELETE FROM u WHERE id = 1", schema_name=sn)                 # right = {}
        assert _rows(client, sn, "v", ["x"]) == [(6,), (10,)]
    finally:
        client.drop_schema(sn)


def test_grouped_setop_side(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        # A grouped side under UNION ALL — the reduce is cut to a segment the set op re-hashes.
        client.execute_sql(
            "CREATE VIEW v AS SELECT a AS k, SUM(b) AS s FROM t GROUP BY a UNION ALL SELECT id, a FROM u",
            schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 2), (2, 10, 3), (3, 20, 5)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (7, 99, 0)", schema_name=sn)
        # grouped: (10, 2+3=5), (20, 5); UNION ALL right: (7, 99)
        assert _rows(client, sn, "v", ["k", "s"]) == [(7, 99), (10, 5), (20, 5)]
        client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)  # group 10 → sum 2
        assert _rows(client, sn, "v", ["k", "s"]) == [(7, 99), (10, 2), (20, 5)]
    finally:
        client.drop_schema(sn)


def test_same_relation_except(client):
    """`t EXCEPT t` / `t INTERSECT t` — the uniform source-collision wrapper. Both
    delta inputs would otherwise be the same source id, which the clamp algebra
    cannot drive in one epoch; the wrapper turns one push into two cascade epochs.
    `t EXCEPT t` is empty and `t INTERSECT t` is `t`, at every stage."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql("CREATE VIEW v_ex AS SELECT a FROM t EXCEPT SELECT a FROM t", schema_name=sn)
        client.execute_sql("CREATE VIEW v_in AS SELECT a FROM t INTERSECT SELECT a FROM t", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 5, 0), (2, 6, 0)", schema_name=sn)
        assert _weights(client, sn, "v_ex", ["a"]) == {}
        assert _weights(client, sn, "v_in", ["a"]) == {(5,): 1, (6,): 1}
        client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
        assert _weights(client, sn, "v_ex", ["a"]) == {}
        assert _weights(client, sn, "v_in", ["a"]) == {(6,): 1}
    finally:
        client.drop_schema(sn)


def test_same_relation_setop_backfill(client):
    """Same-relation INTERSECT/EXCEPT over data that already exists — the wrapper
    segment must seed the distributed backfill, else it reads a still-empty
    sibling and the view silently loses every pre-existing row."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql("INSERT INTO t VALUES (1, 5, 0), (2, 6, 0)", schema_name=sn)
        client.execute_sql("CREATE VIEW v_ex AS SELECT a FROM t EXCEPT SELECT a FROM t", schema_name=sn)
        client.execute_sql("CREATE VIEW v_in AS SELECT a FROM t INTERSECT SELECT a FROM t", schema_name=sn)
        assert _weights(client, sn, "v_ex", ["a"]) == {}
        assert _weights(client, sn, "v_in", ["a"]) == {(5,): 1, (6,): 1}
    finally:
        client.drop_schema(sn)


def test_nested_setop_chain_backfill(client):
    """The nested-chain shape over pre-existing data: the inner set-op segment
    seeds, so the outer must backfill in dependency order."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql("INSERT INTO t VALUES (1, 1, 2), (2, 3, 4)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (1, 5, 6)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT a FROM t UNION SELECT a FROM u UNION SELECT b FROM t", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(1,): 1, (2,): 1, (3,): 1, (4,): 1, (5,): 1}
    finally:
        client.drop_schema(sn)


def test_join_setop_side(client):
    """A set-op side whose FROM is an inline join — the join is cut to a hidden
    segment the set op then re-hashes (§2.7 item 2)."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT t.a FROM t JOIN u ON t.b = u.id UNION SELECT b FROM u", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (7, 40, 50), (8, 41, 51)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 11, 7), (2, 12, 8)", schema_name=sn)
        # join side: t.b=7→a=11, t.b=8→a=12; right side: u.b = {50, 51}
        assert _weights(client, sn, "v", ["a"]) == {(11,): 1, (12,): 1, (50,): 1, (51,): 1}
        client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(12,): 1, (50,): 1, (51,): 1}
    finally:
        client.drop_schema(sn)


def test_union_all_weights(client):
    """UNION ALL is Z-set addition: an identical row from both sides must carry
    weight 2, not be deduped. Row-presence assertions cannot see this."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql("CREATE VIEW v AS SELECT a FROM t UNION ALL SELECT a FROM u", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 9, 0)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (1, 9, 0)", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(9,): 2}
        client.execute_sql("DELETE FROM u WHERE id = 1", schema_name=sn)
        assert _weights(client, sn, "v", ["a"]) == {(9,): 1}
    finally:
        client.drop_schema(sn)


def test_grouped_setop_side_backfill(client):
    """A grouped side under UNION ALL over pre-existing data — the reduce segment
    seeds, so the set op must backfill after it."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        _tu(client, sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10, 2), (2, 10, 3), (3, 20, 5)", schema_name=sn)
        client.execute_sql("INSERT INTO u VALUES (7, 99, 0)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT a AS k, SUM(b) AS s FROM t GROUP BY a UNION ALL SELECT id, a FROM u",
            schema_name=sn)
        assert _weights(client, sn, "v", ["k", "s"]) == {(7, 99): 1, (10, 5): 1, (20, 5): 1}
    finally:
        client.drop_schema(sn)
