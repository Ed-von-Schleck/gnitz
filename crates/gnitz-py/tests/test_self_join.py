"""E2E tests: self-joins (a base relation referenced twice in one join).

`SELECT e.nm, m.nm FROM emp e JOIN emp m ON e.mgr = m.id` — the two inputs are the
same base table, which would drop the bilinear cross-term under single-source-per-epoch.
The planner wraps the *repeated* occurrence in an auto-generated pass-through hidden view
(`SELECT * FROM emp` under a fresh id), so the join sees two distinct sources; the shared
base reaches them in separate epochs and the result stays correct and incremental.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_self_join.py -v --tb=short
"""
import random
import pytest


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn):
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _rows(client, sn, view, keys):
    vid = client.resolve_table(sn, view)[0]
    return sorted(tuple(r._asdict()[k] for k in keys) for r in client.scan(vid) if r.weight > 0)


class TestSelfJoin:
    def test_employee_manager(self, client):
        """The classic self-join: each employee joined to its manager row."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, nm BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT e.nm AS emp, m.nm AS boss FROM emp e JOIN emp m ON e.mgr = m.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 1, 300)", schema_name=sn)
            # e2, e3 report to 1 (nm 100); e1's mgr 0 has no row.
            assert _rows(client, sn, "v", ["emp", "boss"]) == [(200, 100), (300, 100)]

            # New report to employee 2.
            client.execute_sql("INSERT INTO emp VALUES (4, 2, 400)", schema_name=sn)
            assert _rows(client, sn, "v", ["emp", "boss"]) == [(200, 100), (300, 100), (400, 200)]

            # Retract manager 1 — both of its reports lose their boss row.
            client.execute_sql("DELETE FROM emp WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "v", ["emp", "boss"]) == [(400, 200)]
        finally:
            _cleanup(client, sn)

    def test_self_join_residual_where(self, client):
        """Employees earning more than their manager — a residual over both occurrences."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, sal BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT e.id AS eid FROM emp e JOIN emp m ON e.mgr = m.id WHERE e.sal > m.sal",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO emp VALUES (1, 0, 500), (2, 1, 600), (3, 1, 400)", schema_name=sn)
            # e2 (600) > mgr1 (500) kept; e3 (400) < 500 dropped.
            assert _rows(client, sn, "v", ["eid"]) == [(2,)]

            # A raise pushes e3 above its manager.
            client.execute_sql("UPDATE emp SET sal = 700 WHERE id = 3", schema_name=sn)
            assert _rows(client, sn, "v", ["eid"]) == [(2,), (3,)]
        finally:
            _cleanup(client, sn)

    def test_self_join_plus_third_table(self, client):
        """A self-referencing pair plus a distinct third table (repeat + multi-way)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, dept BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE dept (id BIGINT NOT NULL PRIMARY KEY, budget BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT e.id AS eid, m.id AS mid, d.budget AS bud "
                "FROM emp e JOIN emp m ON e.mgr = m.id JOIN dept d ON e.dept = d.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO dept VALUES (10, 999)", schema_name=sn)
            client.execute_sql("INSERT INTO emp VALUES (1, 0, 10), (2, 1, 10)", schema_name=sn)
            assert _rows(client, sn, "v", ["eid", "mid", "bud"]) == [(2, 1, 999)]
        finally:
            _cleanup(client, sn)

    def test_three_occurrences_grandmanager(self, client):
        """The same table three times: employee, manager, grand-manager (two pass-throughs)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, nm BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT e.nm AS emp, g.nm AS grand "
                "FROM emp e JOIN emp m ON e.mgr = m.id JOIN emp g ON m.mgr = g.id",
                schema_name=sn,
            )
            # 1 is top (mgr 0); 2 reports to 1; 3 reports to 2. e=3 -> m=2 -> g=1.
            client.execute_sql("INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 2, 300)", schema_name=sn)
            assert _rows(client, sn, "v", ["emp", "grand"]) == [(300, 100)]

            # New chain member deeper.
            client.execute_sql("INSERT INTO emp VALUES (4, 3, 400)", schema_name=sn)  # e=4 -> m=3 -> g=2
            assert _rows(client, sn, "v", ["emp", "grand"]) == [(300, 100), (400, 200)]
        finally:
            _cleanup(client, sn)


class TestSelfJoinLifecycle:
    def test_backfill_view_after_data(self, client):
        """Data exists before the self-join view — the pass-through + join backfill."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, nm BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 1, 300)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT e.nm AS emp, m.nm AS boss FROM emp e JOIN emp m ON e.mgr = m.id",
                schema_name=sn,
            )
            assert _rows(client, sn, "v", ["emp", "boss"]) == [(200, 100), (300, 100)]
            client.execute_sql("INSERT INTO emp VALUES (4, 2, 400)", schema_name=sn)
            assert _rows(client, sn, "v", ["emp", "boss"]) == [(200, 100), (300, 100), (400, 200)]
        finally:
            _cleanup(client, sn)

    def test_drop_cascades_passthrough(self, client):
        """DROP VIEW retires the auto-generated pass-through hidden view, freeing the base."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, nm BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT e.nm AS emp, m.nm AS boss FROM emp e JOIN emp m ON e.mgr = m.id",
                schema_name=sn,
            )
            client.execute_sql("DROP VIEW v", schema_name=sn)
            # If the pass-through were orphaned it would still depend on emp and RESTRICT.
            client.execute_sql("DROP TABLE emp", schema_name=sn)
        finally:
            _cleanup(client, sn)

    def test_drop_base_under_self_join_restricts(self, client):
        """The base of a live self-join cannot be dropped (both occurrences depend on it)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, nm BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT e.nm AS emp, m.nm AS boss FROM emp e JOIN emp m ON e.mgr = m.id",
                schema_name=sn,
            )
            with pytest.raises(Exception):
                client.execute_sql("DROP TABLE emp", schema_name=sn)
        finally:
            _cleanup(client, sn)


class TestSelfJoinChainPruning:
    """A relation referenced three-plus times (a self-join chain via the pass-through
    wrapper) with a narrow final projection — pruning keeps only the live
    columns of each occurrence."""

    def test_three_level_self_join_chain(self, client):
        """emp e -> its manager m -> the manager's manager g, projecting only e.nm and
        g.nm. Intermediate segments carry just the join keys + those two names."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, mgr BIGINT NOT NULL, nm BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT e.nm AS emp, g.nm AS grandboss "
                "FROM emp e JOIN emp m ON e.mgr = m.id JOIN emp g ON m.mgr = g.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO emp VALUES (1, 0, 100), (2, 1, 200), (3, 2, 300)", schema_name=sn)
            # e3(mgr=2) -> m2(id=2, mgr=1) -> g1(id=1, nm=100): (300, 100).
            # e2(mgr=1) -> m1(id=1, mgr=0) -> g0 absent: dropped.
            assert _rows(client, sn, "v", ["emp", "grandboss"]) == [(300, 100)]

            # New employee 4 -> m3 -> g2(nm=200).
            client.execute_sql("INSERT INTO emp VALUES (4, 3, 400)", schema_name=sn)
            assert _rows(client, sn, "v", ["emp", "grandboss"]) == [(300, 100), (400, 200)]

            # Retract emp 1 (the grandmanager on e3's chain) -> (300,100) drops.
            client.execute_sql("DELETE FROM emp WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "v", ["emp", "grandboss"]) == [(400, 200)]
        finally:
            _cleanup(client, sn)
