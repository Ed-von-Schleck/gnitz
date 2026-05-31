"""E2E tests for GROUP BY with COUNT/SUM/MIN/MAX/AVG, HAVING.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_aggregates.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


class TestGroupBy:
    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE orders ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  category BIGINT NOT NULL,"
            "  amount BIGINT NOT NULL,"
            "  price BIGINT NOT NULL"
            ")",
            schema_name=sn,
        )

    def test_count_star(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 20, 300, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 2
            # Find rows by category
            by_cat = {}
            for r in rows:
                cat = r["category"]
                by_cat[cat] = r
            assert by_cat[10]["cnt"] == 2
            assert by_cat[20]["cnt"] == 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_sum(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, SUM(amount) AS total FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 20, 300, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            by_cat = {}
            for r in rows:
                by_cat[r["category"]] = r
            assert by_cat[10]["total"] == 300  # 100+200
            assert by_cat[20]["total"] == 300

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_multi_agg(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt, SUM(amount) AS total "
                "FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 20, 300, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            by_cat = {}
            for r in rows:
                by_cat[r["category"]] = r
            assert by_cat[10]["cnt"] == 2
            assert by_cat[10]["total"] == 300
            assert by_cat[20]["cnt"] == 1
            assert by_cat[20]["total"] == 300

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_min_max(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, MIN(amount) AS lo, MAX(amount) AS hi "
                "FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 10, 50, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 1
            row = next(iter(rows))
            assert row["lo"] == 50
            assert row["hi"] == 200

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_multi_col_group_by_min_distinct_groups(self, client):
        """Single MIN over a two-column GROUP BY routes through the byte-form
        AVI (group key = a ++ b ++ encoded value). Each distinct (a, b) must
        resolve its own minimum — the index carries the full group key, so two
        groups can never share a bucket."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  a INT NOT NULL,"
                "  b INT NOT NULL,"
                "  val BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a, b, MIN(val) AS lo "
                "FROM t GROUP BY a, b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # 9 distinct (a, b) groups, each with several rows. The expected
            # MIN per group is a*100 + b (the smallest val inserted for it).
            pk = 0
            expected = {}
            rows_sql = []
            for a in range(3):
                for b in range(3):
                    base = a * 100 + b
                    expected[(a, b)] = base
                    for k in range(3):
                        pk += 1
                        rows_sql.append(f"({pk}, {a}, {b}, {base + k * 10})")
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(rows_sql),
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == len(expected)
            for r in rows:
                key = (r["a"], r["b"])
                assert r["lo"] == expected[key], (
                    f"group {key}: expected MIN {expected[key]}, got {r['lo']}"
                )

            # Incremental update: push a value below group (1,1)'s current MIN.
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk + 1}, 1, 1, -5)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            by_key = {(r["a"], r["b"]): r["lo"] for r in rows}
            assert by_key[(1, 1)] == -5, "pushing a smaller value must lower the MIN"
            # Other groups unchanged.
            assert by_key[(2, 2)] == expected[(2, 2)]

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_wide_multi_col_group_by_min_max(self, client):
        """A two-column GROUP BY over two BIGINT columns gives a composite AVI
        key of a(8) ++ b(8) ++ encoded value(8) = 24 bytes, past the 16-byte
        narrow cap — so this exercises the wide byte-form AVI seek. Inserting in
        several batches forces memtable flushes (multi-source + on-disk shard
        reads), and MIN/MAX must stay correct per group across incremental
        inserts and a retraction."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  a BIGINT NOT NULL,"
                "  b BIGINT NOT NULL,"
                "  val BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a, b, MIN(val) AS lo, MAX(val) AS hi "
                "FROM t GROUP BY a, b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # 6 distinct (a, b) groups; values chosen so the per-group MIN/MAX
            # are unambiguous. Use large group coordinates to span >32 bits.
            groups = [
                (1_000_000_001, 2_000_000_001),
                (1_000_000_001, 2_000_000_002),  # shares `a` with the first
                (1_000_000_002, 2_000_000_001),
                (5, 7),
                (5, 8),
                (9_999_999_999, 9_999_999_998),
            ]
            expected_lo = {}
            expected_hi = {}
            pk = 0
            # Insert in several batches so the source table flushes between
            # them — drives multi-source consolidation of the wide AVI key.
            for batch_no in range(3):
                rows_sql = []
                for gi, (a, b) in enumerate(groups):
                    for k in range(2):
                        pk += 1
                        v = (gi + 1) * 1000 + batch_no * 100 + k
                        rows_sql.append(f"({pk}, {a}, {b}, {v})")
                        expected_lo[(a, b)] = min(expected_lo.get((a, b), v), v)
                        expected_hi[(a, b)] = max(expected_hi.get((a, b), v), v)
                client.execute_sql(
                    "INSERT INTO t VALUES " + ", ".join(rows_sql),
                    schema_name=sn,
                )

            rows = client.scan(vid)
            assert len(rows) == len(groups)
            for r in rows:
                key = (r["a"], r["b"])
                assert r["lo"] == expected_lo[key], (
                    f"group {key}: MIN expected {expected_lo[key]}, got {r['lo']}"
                )
                assert r["hi"] == expected_hi[key], (
                    f"group {key}: MAX expected {expected_hi[key]}, got {r['hi']}"
                )

            # Incremental insert below the current MIN of group (5, 7).
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk + 1}, 5, 7, -42)",
                schema_name=sn,
            )
            by_lo = {(r["a"], r["b"]): r["lo"] for r in client.scan(vid)}
            assert by_lo[(5, 7)] == -42, "smaller value must lower the group MIN"

            # Retract the new extremum: MIN of (5, 7) must recover its prior value.
            client.execute_sql(
                f"DELETE FROM t WHERE pk = {pk + 1}",
                schema_name=sn,
            )
            by_lo = {(r["a"], r["b"]): r["lo"] for r in client.scan(vid)}
            assert by_lo[(5, 7)] == expected_lo[(5, 7)], (
                "retracting the extremum must restore the next-best MIN"
            )

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_signed_group_key_negative_values_byte_form(self, client):
        """A single MIN over a two-column GROUP BY whose group columns are
        SIGNED and hold NEGATIVE values. The byte-form AVI key concatenates each
        group column's raw two's-complement bytes (a negative INT has its high
        bit set, e.g. -5 -> 0xFFFFFFFB), so the prefix seek must still isolate
        the exact group and keep a negative group distinct from its positive
        twin. The existing byte-form AVI suites only use non-negative group
        coordinates, so this guards the signed gather + exact-prefix lookup."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  a INT NOT NULL,"
                "  b INT NOT NULL,"
                "  val BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a, b, MIN(val) AS lo "
                "FROM t GROUP BY a, b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Sign twins: (-5,-7) vs (5,7) vs (-5,7) vs (5,-7) must never share a
            # bucket. (0,0) anchors the all-zero key. Each group gets several
            # vals; the expected MIN is the smallest inserted.
            groups = [(-5, -7), (5, 7), (-5, 7), (5, -7), (0, 0)]
            expected = {}
            pk = 0
            rows_sql = []
            for gi, (a, b) in enumerate(groups):
                base = (gi + 1) * 1000
                for k in range(3):
                    pk += 1
                    v = base + (2 - k) * 10  # smallest is base (k=2)
                    rows_sql.append(f"({pk}, {a}, {b}, {v})")
                expected[(a, b)] = base
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(rows_sql),
                schema_name=sn,
            )

            rows = client.scan(vid)
            by_key = {(r["a"], r["b"]): r["lo"] for r in rows if r.weight > 0}
            assert len(by_key) == len(groups), f"expected {len(groups)} groups, got {by_key}"
            for key, lo in expected.items():
                assert by_key[key] == lo, f"group {key}: expected MIN {lo}, got {by_key[key]}"

            # Push a value below the current MIN of a negative group, then retract
            # it: the byte-form AVI must lower the MIN and then recover the prior
            # extremum from the remaining entries of that exact (negative) group.
            client.execute_sql(f"INSERT INTO t VALUES ({pk + 1}, -5, -7, -999)", schema_name=sn)
            by_key = {(r["a"], r["b"]): r["lo"] for r in client.scan(vid) if r.weight > 0}
            assert by_key[(-5, -7)] == -999, "smaller value must lower the negative group's MIN"
            assert by_key[(5, 7)] == expected[(5, 7)], "positive twin must be unaffected"

            client.execute_sql(f"DELETE FROM t WHERE pk = {pk + 1}", schema_name=sn)
            by_key = {(r["a"], r["b"]): r["lo"] for r in client.scan(vid) if r.weight > 0}
            assert by_key[(-5, -7)] == expected[(-5, -7)], (
                "retracting the extremum must restore the negative group's next-best MIN"
            )

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_wide_gi_group_by_min_max_u128_pk(self, client):
        """GROUP BY MIN/MAX over a table with a 16-byte PRIMARY KEY
        (DECIMAL(38,0) = U128). The GroupIndex key is `8-byte group code` ++
        `16-byte source PK` = 24 bytes — the *wide* (>16) consolidation path,
        which base-table compound PKs (stride capped at 16) can't reach. The
        consolidation sort orders the wide GI key via an order-preserving
        prefix + compare_pk_bytes tiebreak; deleting the row holding a group's
        MIN/MAX forces an incremental recompute that re-seeks the source trace
        through that wide key. Big ids (> u64::MAX) make the source PK occupy
        all 16 bytes, so the OPK prefix straddles its high column."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  id DECIMAL(38,0) NOT NULL PRIMARY KEY,"
                "  g BIGINT NOT NULL,"
                "  amount BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, MIN(amount) AS lo, MAX(amount) AS hi "
                "FROM t GROUP BY g",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # 38-nine id is ~1e38 (well beyond u64::MAX ~1.8e19): the U128 source
            # PK uses its upper 8 bytes, exercising the wide OPK prefix.
            big = 99999999999999999999999999999999999999
            client.execute_sql(
                f"INSERT INTO t VALUES "
                f"(1, 10, 100), (2, 10, 50), ({big}, 10, 200), "
                f"(3, 20, 70), (4, 20, 30)",
                schema_name=sn,
            )
            by_g = {r["g"]: r for r in client.scan(vid) if r.weight > 0}
            assert by_g[10]["lo"] == 50 and by_g[10]["hi"] == 200, by_g
            assert by_g[20]["lo"] == 30 and by_g[20]["hi"] == 70, by_g

            # Delete group 10's MIN holder (id=2) → MIN recomputes to 100 via a
            # wide-GI re-seek of the remaining group-10 source rows.
            client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
            by_g = {r["g"]: r for r in client.scan(vid) if r.weight > 0}
            assert by_g[10]["lo"] == 100 and by_g[10]["hi"] == 200, by_g

            # Delete group 10's MAX holder (the huge id) → MAX recomputes to 100.
            client.execute_sql(f"DELETE FROM t WHERE id = {big}", schema_name=sn)
            by_g = {r["g"]: r for r in client.scan(vid) if r.weight > 0}
            assert by_g[10]["lo"] == 100 and by_g[10]["hi"] == 100, by_g
            assert by_g[20]["lo"] == 30 and by_g[20]["hi"] == 70, by_g

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt "
                "FROM orders GROUP BY category HAVING COUNT(*) > 1",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 20, 300, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            # Only category 10 has count > 1
            assert len(rows) == 1
            row = next(iter(rows))
            assert row["category"] == 10
            assert row["cnt"] == 2

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_avg(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, AVG(amount) AS avg_amt "
                "FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            assert len(rows) == 1
            row = next(iter(rows))
            # AVG(100, 200) = 150.0
            assert abs(row["avg_amt"] - 150.0) < 0.001

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_avg_delete_to_all_null_emits_null(self, client):
        """A group that loses its only non-NULL value (via DELETE) is re-emitted
        with AVG = NULL (COUNT_NON_NULL drops to 0), and the group stays present
        because it still has the NULL row. The AVG column must therefore be
        nullable and expose the value as Python None."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  category BIGINT NOT NULL,"
                "  amount BIGINT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, AVG(amount) AS avg_amt "
                "FROM t GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # category 10: one non-NULL (5) and one NULL → AVG ignores NULL → 5.0
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 5), (2, 10, NULL)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1
            assert abs(rows[0]["avg_amt"] - 5.0) < 0.001

            # Delete the only non-NULL contributor; the NULL row keeps the group.
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1, f"group must persist, got {rows}"
            assert rows[0]["category"] == 10
            assert rows[0]["avg_amt"] is None, (
                f"AVG of an all-NULL group must be NULL, got {rows[0]['avg_amt']}")

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_group_by_with_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt "
                "FROM orders WHERE amount > 100 GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 50, 50), (2, 10, 200, 60), (3, 10, 300, 70)",
                schema_name=sn,
            )

            rows = client.scan(vid)
            # Only 2 rows have amount > 100
            assert len(rows) == 1
            row = next(iter(rows))
            assert row["cnt"] == 2

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_count_retraction(self, client):
        """Deleting a row decrements the group count."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 10, 300, 70)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 1
            assert next(iter(rows))["cnt"] == 3

            client.execute_sql("DELETE FROM orders WHERE pk = 1", schema_name=sn)
            rows = client.scan(vid)
            assert len(rows) == 1
            assert next(iter(rows))["cnt"] == 2, f"expected cnt=2 after delete, got {rows}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_sum_retraction(self, client):
        """Deleting a row decrements the group sum."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, SUM(amount) AS total FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert next(iter(rows))["total"] == 300

            client.execute_sql("DELETE FROM orders WHERE pk = 1", schema_name=sn)
            rows = client.scan(vid)
            assert len(rows) == 1
            assert next(iter(rows))["total"] == 200, f"expected total=200, got {rows}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_incremental_group_by(self, client):
        """Insert in two batches; verify the view updates incrementally."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt, SUM(amount) AS total "
                "FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Batch 1
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 1
            row = next(iter(rows))
            assert row["cnt"] == 1
            assert row["total"] == 100

            # Batch 2
            client.execute_sql(
                "INSERT INTO orders VALUES (2, 10, 200, 60)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 1
            row = next(iter(rows))
            assert row["cnt"] == 2
            assert row["total"] == 300

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_group_elimination(self, client):
        """Insert rows in 2 groups, delete all rows from group A,
        verify group A vanishes while group B remains."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50), (2, 10, 200, 60), (3, 20, 300, 70)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert len(rows) == 2

            # Delete all rows from category 10
            client.execute_sql("DELETE FROM orders WHERE pk = 1", schema_name=sn)
            client.execute_sql("DELETE FROM orders WHERE pk = 2", schema_name=sn)

            by_cat = {r["category"]: r["cnt"] for r in client.scan(vid) if r.weight > 0}
            # Category 10 should have cnt=0 or be absent; category 20 should have cnt=1
            assert by_cat.get(10, 0) == 0
            assert by_cat[20] == 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_reinsertion_after_deletion(self, client):
        """Insert row, delete it, re-insert it, verify aggregate restores."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, SUM(amount) AS total FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50)",
                schema_name=sn,
            )
            rows = client.scan(vid)
            assert next(iter(rows))["total"] == 100

            client.execute_sql("DELETE FROM orders WHERE pk = 1", schema_name=sn)
            totals = {r["category"]: r["total"] for r in client.scan(vid) if r.weight > 0}
            assert totals.get(10, 0) == 0

            # Re-insert and verify aggregate restores
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100, 50)",
                schema_name=sn,
            )
            totals = {r["category"]: r["total"] for r in client.scan(vid) if r.weight > 0}
            assert totals[10] == 100

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)


class TestGroupByPkAndNullable:
    """`GROUP BY` containing the PK column or a nullable column."""

    def test_group_by_pk_and_other_col(self, client):
        """`GROUP BY pk, other_col` must produce one group per PK (since PKs
        are unique)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE orders ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  category BIGINT NOT NULL,"
                "  amount BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS "
                "SELECT pk, category, COUNT(*) AS cnt "
                "FROM orders GROUP BY pk, category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 3
            by_pk = {r["pk"]: r for r in rows}
            assert by_pk[1]["category"] == 10 and by_pk[1]["cnt"] == 1
            assert by_pk[2]["category"] == 10 and by_pk[2]["cnt"] == 1
            assert by_pk[3]["category"] == 20 and by_pk[3]["cnt"] == 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_group_by_pk_and_other_col_uuid_pk(self, client):
        """`GROUP BY pk, other_col` with a 16-byte UUID PK projection."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE items ("
                "  pk UUID NOT NULL PRIMARY KEY,"
                "  category BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS "
                "SELECT pk, category, COUNT(*) AS cnt "
                "FROM items GROUP BY pk, category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            uuid_a = '550e8400-e29b-41d4-a716-446655440000'
            uuid_b = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            client.execute_sql(
                f"INSERT INTO items VALUES ('{uuid_a}', 10), ('{uuid_b}', 20)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 2
            by_pk = {r["pk"]: r for r in rows}
            assert by_pk[uuid_a]["category"] == 10 and by_pk[uuid_a]["cnt"] == 1
            assert by_pk[uuid_b]["category"] == 20 and by_pk[uuid_b]["cnt"] == 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE items", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_group_by_nullable_int_distinguishes_null_from_zero(self, client):
        """`GROUP BY nullable_int` must form a distinct group for NULL,
        not merge it with the integer-zero group (NULL stores as zero bytes)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  grp BIGINT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Two rows with grp=NULL, one with grp=0, one with grp=7.
            client.execute_sql(
                "INSERT INTO t VALUES (1, NULL), (2, 0), (3, NULL), (4, 7)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            # Three distinct groups: NULL, 0, 7
            assert len(rows) == 3, f"expected 3 groups, got {len(rows)}: {rows}"
            counts = {r["grp"]: r["cnt"] for r in rows}
            assert counts.get(None) == 2, f"NULL group must have 2 rows; got {counts}"
            assert counts.get(0) == 1, f"0-group must have 1 row; got {counts}"
            assert counts.get(7) == 1, f"7-group must have 1 row; got {counts}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)


class TestReducePathRegressions:
    """Reduce-path defects: U128 numeric-agg reject, signed-PK projection,
    F32 MIN/MAX, nullable-group MIN/MAX, and HAVING over the grouped relation."""

    def test_sum_avg_over_u128_rejected(self, client):
        """SUM/AVG accumulate into a 64-bit slot; a U128 source overflows it and
        the engine marks the decode unreachable, so the planner must reject it."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  category BIGINT NOT NULL,"
                "  big DECIMAL(38,0) NOT NULL"  # U128
                ")",
                schema_name=sn,
            )
            for fn in ("SUM", "AVG"):
                with pytest.raises(gnitz.GnitzError):
                    client.execute_sql(
                        f"CREATE VIEW v AS SELECT category, {fn}(big) AS x "
                        "FROM t GROUP BY category",
                        schema_name=sn,
                    )
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    @pytest.mark.parametrize("col_type", [
        "TINYINT", "SMALLINT", "INT", "BIGINT",
    ])
    def test_group_by_signed_pk_projection_roundtrips(self, client, col_type):
        """Projecting a signed PK group column must emit the native value, not
        the order-preserving (sign-flipped) OPK image. Includes negatives."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                f"CREATE TABLE t (pk {col_type} NOT NULL PRIMARY KEY, category BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT pk, category, COUNT(*) AS cnt "
                "FROM t GROUP BY pk, category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (-5, 10), (-1, 20), (0, 30), (7, 40)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            by_pk = {r["pk"]: r for r in rows}
            assert set(by_pk) == {-5, -1, 0, 7}, f"signed PKs must round-trip: {sorted(by_pk)}"
            assert by_pk[-5]["category"] == 10
            assert by_pk[-1]["category"] == 20
            assert by_pk[0]["category"] == 30
            assert by_pk[7]["category"] == 40
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_min_max_f32_not_corrupted(self, client):
        """MIN/MAX over an F32 column (AVI-eligible with an int group key) must
        emit the true extremal, not an F32 bit pattern read as an F64 denormal."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  grp BIGINT NOT NULL,"
                "  v REAL NOT NULL"  # F32
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT grp, MIN(v) AS lo, MAX(v) AS hi "
                "FROM t GROUP BY grp",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 9, 1.5), (2, 9, -2.25), (3, 9, 4.0)",
                schema_name=sn,
            )
            row = next(r for r in client.scan(vid) if r.weight > 0)
            assert abs(row["lo"] - (-2.25)) < 1e-6, f"MIN f32 corrupted: {row['lo']}"
            assert abs(row["hi"] - 4.0) < 1e-6, f"MAX f32 corrupted: {row['hi']}"
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_min_max_group_by_nullable_int_null_vs_zero(self, client):
        """MIN/MAX GROUP BY a nullable int takes the GI path; the GI must be
        disabled for a nullable group column so the NULL group does not collide
        with group 0 (which would fetch 0's history or drop NULL's state)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  grp BIGINT NULL,"
                "  v BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT grp, MIN(v) AS lo, MAX(v) AS hi "
                "FROM t GROUP BY grp",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # NULL group: {100, 50}; group 0: {7, 9}; distinct.
            client.execute_sql(
                "INSERT INTO t VALUES (1, NULL, 100), (2, 0, 7), (3, NULL, 50), (4, 0, 9)",
                schema_name=sn,
            )
            by_grp = {r["grp"]: r for r in client.scan(vid) if r.weight > 0}
            assert set(by_grp) == {None, 0}, f"NULL and 0 groups must stay distinct: {by_grp}"
            assert (by_grp[None]["lo"], by_grp[None]["hi"]) == (50, 100)
            assert (by_grp[0]["lo"], by_grp[0]["hi"]) == (7, 9)

            # Incremental update: lower the NULL group's min, raise 0's max.
            client.execute_sql("INSERT INTO t VALUES (5, NULL, 10), (6, 0, 999)", schema_name=sn)
            by_grp = {r["grp"]: r for r in client.scan(vid) if r.weight > 0}
            assert (by_grp[None]["lo"], by_grp[None]["hi"]) == (10, 100)
            assert (by_grp[0]["lo"], by_grp[0]["hi"]) == (7, 999)
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_unprojected_group_col(self, client):
        """HAVING on a GROUP BY column omitted from SELECT (valid standard SQL):
        binds against the grouped relation, before projection."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a, COUNT(*) AS cnt FROM t GROUP BY a, b HAVING b > 5",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # (a,b) groups: (1,3) excluded, (1,9) kept, (2,7) kept.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 3), (2, 1, 9), (3, 2, 7)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            groups = sorted(r["a"] for r in rows)
            assert groups == [1, 2], f"only groups with b>5 survive: {groups}"
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_aliased_group_col_by_original_name(self, client):
        """HAVING references a group column by its source name even when SELECT
        aliases it: HAVING binds against the grouped relation (source names)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a AS x, COUNT(*) AS cnt FROM t GROUP BY a HAVING a > 5",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 3), (2, 9), (3, 7)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            xs = sorted(r["x"] for r in rows)
            assert xs == [7, 9], f"only a>5 survive (aliased to x): {xs}"
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_aggregate_not_projected(self, client):
        """HAVING on an aggregate absent from SELECT: it must still be
        materialised in the reduce and resolvable in HAVING."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # a=10 appears twice (kept), a=20 once (excluded).
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 10), (3, 20)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            groups = sorted(r["a"] for r in rows)
            assert groups == [10], f"only groups with COUNT(*)>1 survive: {groups}"
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)
