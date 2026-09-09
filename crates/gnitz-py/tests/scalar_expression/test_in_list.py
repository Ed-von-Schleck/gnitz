"""E2E tests: large integer ``IN`` / ``NOT IN`` lists compile to one ``IntInSet``
opcode (removing the old ~16-item OR-chain register cliff) and maintain
correctly across inserts and deletes.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/scalar_expression/test_in_list.py -v --tb=short
"""
import gnitz
from _uid import uid as _uid




def _cleanup(client, sn, *names):
    for name in names or ("v_not", "v", "vh", "t", "g"):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _live_vals(client, vid):
    """Set of `val` values in the live (positive-weight) view state."""
    return {r[1] for r in client.scan(vid)}


# A 500-element set spanning negatives and positives — far past the old
# ~16-item OR-chain limit that hard-failed with TooManyRegs.
SET = list(range(-100, 400))
SET_SQL = ", ".join(str(v) for v in SET)


class TestLargeInList:
    def test_large_in_list_view_maintains(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            # Previously TooManyRegs at CREATE VIEW; now one ``IntInSet``.
            client.execute_sql(
                f"CREATE VIEW v AS SELECT * FROM t WHERE val IN ({SET_SQL})",
                schema_name=sn,
            )
            client.execute_sql(
                f"CREATE VIEW v_not AS SELECT * FROM t WHERE val NOT IN ({SET_SQL})",
                schema_name=sn,
            )
            tid, t_schema = client.resolve_table(sn, "t")
            vid, _ = client.resolve_table(sn, "v")
            vid_not, _ = client.resolve_table(sn, "v_not")

            # Members (in SET) and non-members (outside), incl. the boundaries.
            rows = [
                (1, -100),   # min of SET → member
                (2, 399),    # max of SET → member
                (3, 0),      # member
                (4, -101),   # just below → non-member
                (5, 400),    # just above → non-member
                (6, 5000),   # non-member
            ]
            values = ", ".join(f"({pk}, {val})" for pk, val in rows)
            client.execute_sql(f"INSERT INTO t VALUES {values}", schema_name=sn)

            assert _live_vals(client, vid) == {-100, 399, 0}
            assert _live_vals(client, vid_not) == {-101, 400, 5000}

            # Delete a member: it leaves IN, appears nowhere (NOT IN never had it).
            batch = gnitz.ZSetBatch(t_schema)
            batch.append(pk=3, val=0, _weight=-1)
            client.push(tid, batch)
            assert _live_vals(client, vid) == {-100, 399}
            assert _live_vals(client, vid_not) == {-101, 400, 5000}

            # Delete a non-member: it leaves NOT IN, IN is unaffected.
            batch = gnitz.ZSetBatch(t_schema)
            batch.append(pk=6, val=5000, _weight=-1)
            client.push(tid, batch)
            assert _live_vals(client, vid) == {-100, 399}
            assert _live_vals(client, vid_not) == {-101, 400}
        finally:
            _cleanup(client, sn)

    def test_having_large_in_list(self, client):
        # HAVING is lowered against the reduce-output schema, so a large IN there
        # compiled to the same register-capped OR-chain and hit TooManyRegs at
        # ~17 items. It now takes the ``IntInSet`` fast path.
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE g (pk BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            # 25 keep-values — well past the old ~17-item cap.
            keep = list(range(0, 50, 2))  # 25 even numbers 0..48
            keep_sql = ", ".join(str(v) for v in keep)
            client.execute_sql(
                f"CREATE VIEW vh AS SELECT grp, SUM(val) AS s FROM g GROUP BY grp "
                f"HAVING SUM(val) IN ({keep_sql})",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "g")  # table id (unused beyond insert)
            vh, _ = client.resolve_table(sn, "vh")

            # One row per group: grp = i, val = i, so SUM(val) per group = i.
            rows = ", ".join(f"({i}, {i}, {i})" for i in range(50))
            client.execute_sql(f"INSERT INTO g VALUES {rows}", schema_name=sn)

            # Only groups whose sum (= grp) is an even number in [0, 48] survive.
            got = {(r[0], r[1]) for r in client.scan(vh)}
            assert got == {(i, i) for i in keep}
        finally:
            _cleanup(client, sn)


# From the SQL front-end suite: an IN list as a maintained view's WHERE.

class TestInListViewFilter:
    def test_view_where_in_list(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val IN (10, 30)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # A NULL val makes every Eq NULL → the OR-chain NULL → row excluded.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, NULL)",
                schema_name=sn,
            )
            rows = list(client.scan(vid))
            assert sorted(r.pk for r in rows) == [1, 3]
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_view_where_not_in_excludes_null(self, client):
        """NOT IN over a NULL operand is NOT(NULL) = NULL → excluded (SQL 3VL)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val NOT IN (10, 30)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, NULL)",
                schema_name=sn,
            )
            rows = list(client.scan(vid))
            assert sorted(r.pk for r in rows) == [2]
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)

    def test_view_where_string_in_list(self, client):
        """String elements route through the ``StrColConst`` lowering."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, tag TEXT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE tag IN ('red', 'blue')",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 'red'), (2, 'green'), (3, 'blue'), (4, NULL)",
                schema_name=sn,
            )
            rows = list(client.scan(vid))
            assert sorted(r.pk for r in rows) == [1, 3]
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)
