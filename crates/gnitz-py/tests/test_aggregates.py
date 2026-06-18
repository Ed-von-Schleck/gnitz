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

    def test_having_sum_times_two(self, client):
        """HAVING SUM(amount) * 2 > 10 — the `Mul` operator the old HAVING binder
        lacked now binds via the unified structural core. SUM appears only in
        HAVING (materialised by collect_having_aggs)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category FROM orders "
                "GROUP BY category HAVING SUM(amount) * 2 > 10",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # cat 10: SUM=8 → *2=16 > 10 PASS (SUM=8 alone would fail > 10, so the
            #         `* 2` is load-bearing); cat 20: SUM=2 → *2=4 < 10 FAIL.
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 10, 4, 0), (2, 10, 4, 0), (3, 20, 2, 0)",
                schema_name=sn,
            )
            cats = sorted(r["category"] for r in client.scan(vid))
            assert cats == [10], f"only category 10 (SUM*2=16>10) passes, got {cats}"
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_not_count(self, client):
        """HAVING NOT (COUNT(*) = 1) — the `UnaryOp` (NOT) the old HAVING binder
        lacked now binds via the unified core."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category, COUNT(*) AS cnt FROM orders "
                "GROUP BY category HAVING NOT (COUNT(*) = 1)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # cat 10: 2 rows → NOT(2=1)=true PASS; cat 20: 1 row → NOT(1=1)=false
            #         FAIL; cat 30: 3 rows → PASS.
            client.execute_sql(
                "INSERT INTO orders VALUES "
                "(1, 10, 1, 0), (2, 10, 1, 0), (3, 20, 1, 0), "
                "(4, 30, 1, 0), (5, 30, 1, 0), (6, 30, 1, 0)",
                schema_name=sn,
            )
            cats = sorted(r["category"] for r in client.scan(vid))
            assert cats == [10, 30], f"categories with COUNT != 1, got {cats}"
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE orders", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_sum_between(self, client):
        """HAVING SUM(amount) BETWEEN 5 AND 20 — the `BETWEEN` desugar the old
        HAVING binder lacked, AND the collect_having_aggs lockstep: the aggregate
        inside BETWEEN must be materialised or binding fails to resolve it."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT category FROM orders "
                "GROUP BY category HAVING SUM(amount) BETWEEN 5 AND 20",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # cat 10: SUM=8 in [5,20] PASS; cat 20: SUM=2 < 5 FAIL;
            # cat 30: SUM=100 > 20 FAIL.
            client.execute_sql(
                "INSERT INTO orders VALUES "
                "(1, 10, 4, 0), (2, 10, 4, 0), (3, 20, 2, 0), (4, 30, 100, 0)",
                schema_name=sn,
            )
            cats = sorted(r["category"] for r in client.scan(vid))
            assert cats == [10], f"only category 10 (SUM=8 in [5,20]) passes, got {cats}"
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

    def test_sum_delete_to_all_null_emits_null(self, client):
        """SUM over a nullable column must become NULL when a retraction removes
        the last non-NULL contributor from a still-surviving group — the SUM
        analog of test_avg_delete_to_all_null_emits_null. On the linear fold the
        accumulator tracked presence as a saturating has_value bool, so a SUM that
        netted back to 0 emitted a concrete 0 where SQL wants NULL. The fix adds a
        hidden COUNT_NON_NULL companion and null-gates the SUM on it in the reduce
        finalize. Runs under GNITZ_WORKERS=1 and =4: GROUP BY shards each group
        onto one worker, so the 4-worker run confirms the fix on the sharded path.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  k BIGINT NOT NULL,"
                "  v BIGINT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT k, SUM(v) AS sm, COUNT(*) AS c "
                "FROM t GROUP BY k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Group k=10: one non-NULL (v=5) and one NULL contributor.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 5), (2, 10, NULL)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1
            assert rows[0]["k"] == 10
            assert rows[0]["sm"] == 5, f"SUM(5, NULL) = 5, got {rows[0]['sm']}"
            assert rows[0]["c"] == 2

            # Retract the last non-NULL contributor. The NULL row keeps the group
            # alive (c=1), so SUM over {NULL} must be NULL, not 0.
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1, f"group must persist via its NULL row, got {rows}"
            assert rows[0]["k"] == 10
            assert rows[0]["c"] == 1
            assert rows[0]["sm"] is None, (
                f"SUM of a group with no non-NULL values must be NULL, got {rows[0]['sm']}")

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_sum_all_null_group_emits_null(self, client):
        """The stays-NULL direction: a group that is all-NULL from the start has
        SUM = NULL while still reporting its true COUNT(*). A non-NULL group
        alongside it confirms only the all-NULL group is NULL (and is unaffected
        by sharding onto a different worker)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  k BIGINT NOT NULL,"
                "  v BIGINT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT k, SUM(v) AS sm, COUNT(*) AS c "
                "FROM t GROUP BY k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # k=10: two NULL rows (SUM NULL, c=2). k=20: a non-NULL row (SUM=7).
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, NULL), (2, 10, NULL), (3, 20, 7)",
                schema_name=sn,
            )
            rows = {r["k"]: r for r in client.scan(vid) if r.weight > 0}
            assert rows[10]["c"] == 2
            assert rows[10]["sm"] is None, (
                f"SUM of an all-NULL group must be NULL, got {rows[10]['sm']}")
            assert rows[20]["c"] == 1
            assert rows[20]["sm"] == 7

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_min_retraction_to_and_from_null(self, client):
        """A nullable MIN that is NULL (every contributor NULL) is a live state, kept
        alive by COUNT(*). MIN is a Direct passthrough, so its raw null bit is
        user-visible — a retraction must reproduce that NULL exactly. The old code
        re-emitted the retracted MIN as a concrete 0, which does not cancel the prior
        NULL row (compare_rows ranks null < non-null) and leaks a stale ghost. Insert
        an all-NULL group, add a non-NULL row (the prior MIN=NULL row must retract as
        NULL → exactly one clean view row), then retract the non-NULL row (MIN returns
        to NULL, no ghost). No existing E2E retracts a nullable MIN to NULL. Runs under
        GNITZ_WORKERS=1 and =4: GROUP BY shards each group whole onto one worker, so
        the 4-worker run confirms the per-worker fix on the sharded path."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  k BIGINT NOT NULL,"
                "  v BIGINT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT k, COUNT(*) AS c, MIN(v) AS mn "
                "FROM t GROUP BY k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Tick 1: an all-NULL group. COUNT(*) keeps it alive; MIN = NULL.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, NULL), (2, 10, NULL)",
                schema_name=sn,
            )
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1
            assert rows[0]["k"] == 10
            assert rows[0]["c"] == 2
            assert rows[0]["mn"] is None, (
                f"all-NULL group MIN must be NULL, got {rows[0]['mn']}")

            # Tick 2: a non-NULL row → MIN = 5. The prior (MIN=NULL) row must be
            # retracted *as NULL* to cancel tick 1 byte-for-byte; a bad retraction
            # (MIN re-emitted as 0) leaves the stale NULL row live → two rows here.
            client.execute_sql("INSERT INTO t VALUES (3, 10, 5)", schema_name=sn)
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1, f"stale MIN=NULL ghost not cancelled: {rows}"
            assert rows[0]["c"] == 3
            assert rows[0]["mn"] == 5, f"MIN must be 5, got {rows[0]['mn']}"
            assert rows[0].weight == 1, f"live row must be weight 1, got {rows[0].weight}"

            # Tick 3: retract the only non-NULL row → MIN returns to NULL, c=2.
            client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1, f"group must persist via its NULL rows: {rows}"
            assert rows[0]["c"] == 2
            assert rows[0]["mn"] is None, (
                f"MIN must return to NULL once the non-NULL row is gone, got {rows[0]['mn']}")
            assert rows[0].weight == 1, f"no ghost: live row must be weight 1, got {rows[0].weight}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_sum_is_null(self, client):
        """HAVING SUM(v) IS NULL admits exactly the groups with no non-NULL
        contributor, agreeing with the companion-gated projection rather than the
        raw SUM column's saturating has_value bit. SUM(v) appears only in HAVING
        (not the SELECT list), so the planner must materialise it — and its hidden
        COUNT_NON_NULL companion — from the predicate alone. Membership updates
        incrementally as the last non-NULL value is retracted."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  k BIGINT NOT NULL,"
                "  v BIGINT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT k, COUNT(*) AS c "
                "FROM t GROUP BY k HAVING SUM(v) IS NULL",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # k=10 has a non-NULL value (SUM=5) → excluded.
            # k=20 is all-NULL (SUM=NULL) → admitted.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 5), (2, 10, NULL), (3, 20, NULL)",
                schema_name=sn,
            )
            rows = {r["k"]: r for r in client.scan(vid) if r.weight > 0}
            assert set(rows) == {20}, f"only the all-NULL group passes HAVING, got {set(rows)}"
            assert rows[20]["c"] == 1

            # Retract k=10's last non-NULL value → its SUM becomes NULL → admitted.
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            rows = {r["k"]: r for r in client.scan(vid) if r.weight > 0}
            assert set(rows) == {10, 20}, (
                f"k=10 enters HAVING once its SUM becomes NULL, got {set(rows)}")
            assert rows[10]["c"] == 1

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_sum_value_gates_on_null(self, client):
        """HAVING SUM(v) = 0 — SUM in *value* position, not IS NULL — must admit a
        genuine-zero group {5, -5} yet exclude a group whose SUM became NULL when
        its last non-NULL contributor was retracted. The value binds through the
        same COUNT_NON_NULL companion gate the SELECT projection uses; binding the
        raw SUM column instead would read the saturated 0 and wrongly admit the
        all-NULL group (NULL = 0 is UNKNOWN, so it must not pass)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  k BIGINT NOT NULL,"
                "  v BIGINT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT k, COUNT(*) AS c "
                "FROM t GROUP BY k HAVING SUM(v) = 0",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # k=10: {5, -5} → SUM = 0, a genuine zero → admitted.
            # k=20: {5, NULL} → SUM = 5 → excluded for now.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 5), (2, 10, -5), (3, 20, 5), (4, 20, NULL)",
                schema_name=sn,
            )
            rows = {r["k"]: r for r in client.scan(vid) if r.weight > 0}
            assert set(rows) == {10}, f"only the genuine-zero group passes, got {set(rows)}"
            assert rows[10]["c"] == 2

            # Retract k=20's last non-NULL value. Its raw SUM nets to 0, but with no
            # non-NULL contributor the SUM is NULL → SUM(v) = 0 is UNKNOWN → still
            # excluded. A raw (un-gated) SUM column would read 0 and wrongly admit it.
            client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
            rows = {r["k"]: r for r in client.scan(vid) if r.weight > 0}
            assert set(rows) == {10}, (
                f"a group whose SUM became NULL must not satisfy SUM(v) = 0, got {set(rows)}")
            assert rows[10]["c"] == 2

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


class TestGroupByKeyCorrectness:
    """GROUP BY key correctness: float keys rejected (Fix A), the group identity
    is a true 128-bit hash (Fix B), and BLOB grouping keys work end-to-end (Fix
    C). Run with GNITZ_WORKERS=4 so the exchange scatter (which routes by the
    same group identity) is exercised."""

    def test_float_group_by_key_rejected(self, client):
        """Fix A1: a float grouping key splits -0.0/+0.0 and distinct-NaN bit
        patterns into separate groups (and routes them to distinct workers), so
        the DDL must be rejected with a clear error."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  f DOUBLE NOT NULL,"
                "  v BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT f, COUNT(*) AS n FROM t GROUP BY f",
                    schema_name=sn,
                )
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_string_group_by_many_keys_retraction(self, client):
        """Fix B: a single-STRING GROUP BY takes the synthetic _group_pk path,
        whose routing/stored identity is now a full 128-bit Xxh3 fold (was a
        64-bit hash widened to u128, a ~2^32 birthday bound that could merge two
        distinct groups → silently wrong aggregation). Many distinct string keys
        must aggregate correctly across workers, and a DELETE must update the
        affected group only."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  s TEXT NOT NULL,"
                "  val BIGINT NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT s, COUNT(*) AS n, SUM(val) AS total "
                "FROM t GROUP BY s",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # 50 distinct string groups, 2 rows each.
            pk = 0
            exp_count = {}
            exp_sum = {}
            rows_sql = []
            for g in range(50):
                key = f"group-key-{g:04d}"
                for k in range(2):
                    pk += 1
                    val = g * 10 + k
                    rows_sql.append(f"({pk}, '{key}', {val})")
                    exp_count[key] = exp_count.get(key, 0) + 1
                    exp_sum[key] = exp_sum.get(key, 0) + val
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(rows_sql), schema_name=sn
            )

            rows = [r for r in client.scan(vid) if r.weight > 0]
            by_key = {r["s"]: (r["n"], r["total"]) for r in rows}
            assert len(by_key) == 50, (
                f"every distinct string key must be its own group (no hash-collision "
                f"merge): got {len(by_key)} groups"
            )
            for key in exp_count:
                assert by_key[key] == (exp_count[key], exp_sum[key]), (
                    f"group {key!r}: expected (count, sum)="
                    f"{(exp_count[key], exp_sum[key])}, got {by_key[key]}"
                )

            # Retraction: DELETE one contributing row of group-key-0001 (pk=4,
            # val=11). That group's count 2→1 and sum 21→10.
            client.execute_sql("DELETE FROM t WHERE pk = 4", schema_name=sn)
            rows = [r for r in client.scan(vid) if r.weight > 0]
            by_key = {r["s"]: (r["n"], r["total"]) for r in rows}
            assert by_key["group-key-0001"] == (1, 10), (
                f"after deleting one row, group must update: got {by_key['group-key-0001']}"
            )
            # An untouched group is unchanged.
            assert by_key["group-key-0002"] == (2, exp_sum["group-key-0002"])

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_multi_col_group_by_sum_retraction(self, client):
        """Fix B: a multi-column GROUP BY routes through the 128-bit fold for its
        exchange scatter and stored _group_pk. Many distinct (a, b) keys must
        aggregate correctly across workers, with a DELETE updating one group."""
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
                "CREATE VIEW v AS SELECT a, b, COUNT(*) AS n, SUM(val) AS total "
                "FROM t GROUP BY a, b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            pk = 0
            exp_sum = {}
            rows_sql = []
            for a in range(8):
                for b in range(8):
                    for k in range(2):
                        pk += 1
                        val = a * 1000 + b * 10 + k
                        rows_sql.append(f"({pk}, {a}, {b}, {val})")
                        exp_sum[(a, b)] = exp_sum.get((a, b), 0) + val
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(rows_sql), schema_name=sn
            )

            rows = [r for r in client.scan(vid) if r.weight > 0]
            by_key = {(r["a"], r["b"]): (r["n"], r["total"]) for r in rows}
            assert len(by_key) == 64, (
                f"every distinct (a,b) must be its own group: got {len(by_key)}"
            )
            for key, s in exp_sum.items():
                assert by_key[key] == (2, s), (
                    f"group {key}: expected (2, {s}), got {by_key[key]}"
                )

            # Delete pk=1 → group (0,0) row val=0. count 2→1, sum drops by 0's val.
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            rows = [r for r in client.scan(vid) if r.weight > 0]
            by_key = {(r["a"], r["b"]): (r["n"], r["total"]) for r in rows}
            assert by_key[(0, 0)] == (1, exp_sum[(0, 0)] - 0), (
                f"after delete, group (0,0) must update: got {by_key[(0, 0)]}"
            )
            assert by_key[(7, 7)] == (2, exp_sum[(7, 7)]), "untouched group unchanged"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_blob_group_by_max_retraction(self, client):
        """Fix C: a BLOB grouping key works end-to-end. BLOB shares the 16-byte
        German-string layout with STRING; the two group-membership compares
        (cursor_matches_group, compare_by_group_cols) now dispatch BLOB by
        content instead of `unreachable!`-ing. SQL DDL rejects BLOB, so build the
        table/circuit via the binding layer (like test_distinct_blob_payload).
        Long (>12-byte) blobs sharing a prefix force the heap-tail compare; MAX
        is non-linear so the trace replay runs cursor_matches_group on BLOB."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [
            gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("grp", gnitz.TypeCode.BLOB),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64),
        ]
        schema = gnitz.Schema(cols)
        tname = "t_" + _uid()
        tid = client.create_table(sn, tname, cols, unique_pk=False)

        cb = client.circuit_builder(source_table_id=tid)
        # group_by col 1 (BLOB), MAX (agg_func_id=4) on col 2 (val).
        red = cb.reduce(cb.input_delta(), group_by_cols=[1], agg_func_id=4, agg_col_idx=2)
        cb.sink(red)
        out_cols = [
            gnitz.ColumnDef("gpk", gnitz.TypeCode.U128, primary_key=True),
            gnitz.ColumnDef("grp", gnitz.TypeCode.BLOB),
            gnitz.ColumnDef("mx", gnitz.TypeCode.I64),
        ]
        vid = client.create_view_with_circuit(sn, "vb", cb.build(), out_cols)

        try:
            # 20 distinct long blobs sharing the "blob-group-payload-" prefix;
            # 2 rows each with vals {g*10, g*10+5} → MAX = g*10+5.
            def blob_for(g):
                return f"blob-group-payload-{g:04d}".encode()

            pk = 0
            b = gnitz.ZSetBatch(schema)
            for g in range(20):
                for k in range(2):
                    pk += 1
                    b.append(pk=pk, grp=blob_for(g), val=g * 10 + k * 5, weight=1)
            client.push(tid, b)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            by_blob = {bytes(r["grp"]): r["mx"] for r in rows}
            assert len(by_blob) == 20, (
                f"every distinct blob must be its own group (full-content compare, "
                f"not 4-byte-prefix): got {len(by_blob)}"
            )
            for g in range(20):
                assert by_blob[blob_for(g)] == g * 10 + 5, (
                    f"blob {g}: MAX must be {g * 10 + 5}, got {by_blob[blob_for(g)]}"
                )

            # Retraction: remove the val=(g*10+5) row from blob 3 (pk=8) → MAX of
            # that group drops from 35 to 30. The replay scans the trace via
            # cursor_matches_group on the BLOB column.
            rb = gnitz.ZSetBatch(schema)
            rb.append(pk=8, grp=blob_for(3), val=3 * 10 + 5, weight=-1)
            client.push(tid, rb)

            rows = [r for r in client.scan(vid) if r.weight > 0]
            by_blob = {bytes(r["grp"]): r["mx"] for r in rows}
            assert by_blob[blob_for(3)] == 30, (
                f"after retracting the max row, blob 3's MAX must drop to 30, got "
                f"{by_blob[blob_for(3)]}"
            )
            # An untouched blob is unchanged.
            assert by_blob[blob_for(7)] == 7 * 10 + 5

            client.drop_view(sn, "vb")
            client.drop_table(sn, tname)
        finally:
            client.drop_schema(sn)


class TestAdaptiveMinMaxOutputType:
    """MIN/MAX preserve the source column's type instead of widening to BIGINT.

    `MIN(INT)` is `INT`, `MAX(SMALLINT UNSIGNED)` is `SMALLINT UNSIGNED`, etc. —
    the extremum is one of the input rows, so it is always representable in the
    source type. The reduce emits the value at the source-column width and the
    trace read-backs reconstruct the 8-byte accumulator from that width
    (width-gated, so an F32 MIN/MAX that widens to F64 is still read verbatim).
    Run with GNITZ_WORKERS>=2 (conftest default 4) so a group's rows spread over
    workers and the gather-reduce combine and its retraction read a narrow agg
    column, with >=2 groups per partial batch to catch the per-row offset bug.
    """

    @staticmethod
    def _col_type(schema, name):
        for c in schema.columns:
            if c.name == name:
                return c.type_code
        raise KeyError(
            f"column {name!r} not in view schema {[c.name for c in schema.columns]}")

    # (label, SQL type, expected view TypeCode, {group key: [source values]}).
    # Each value set straddles its type's interesting boundary: negatives down to
    # the type minimum for the signed widths, and values above i32::MAX for the
    # unsigned zero-extension path (a sign-extended read would turn those
    # negative and break MAX).
    _NARROW_CASES = [
        ("tinyint", "TINYINT", gnitz.TypeCode.I8, {
            10: [-128, -5, 60, 127],
            20: [-1, 0, 1, 9],
            30: [100, -120, 33, 7],
        }),
        ("smallint", "SMALLINT", gnitz.TypeCode.I16, {
            10: [-32768, -5, 30000, 32767],
            20: [-1, 0, 1000, 9],
            30: [12345, -12345, 33, 7],
        }),
        ("int", "INT", gnitz.TypeCode.I32, {
            10: [-2_000_000_000, -5, 2_000_000_000, 7],
            20: [-1, 0, 123456, 9],
            30: [42, -42, 100000, -99999],
        }),
        ("int_unsigned", "INT UNSIGNED", gnitz.TypeCode.U32, {
            10: [0, 4_000_000_000, 2_147_483_648, 100],
            20: [2_147_483_647, 2_147_483_649, 1, 4_294_967_295],
            30: [10, 20, 30, 40],
        }),
    ]

    @pytest.mark.parametrize(
        "label,sql_type,py_tc,groups", _NARROW_CASES,
        ids=[c[0] for c in _NARROW_CASES],
    )
    def test_narrow_minmax_preserves_source_type(
        self, client, label, sql_type, py_tc, groups,
    ):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  k BIGINT NOT NULL,"
                f"  v {sql_type} NOT NULL"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT k, MIN(v) AS lo, MAX(v) AS hi "
                "FROM t GROUP BY k",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table_id(sn, "v")

            # The view's MIN/MAX columns must carry the SOURCE type, not BIGINT.
            assert self._col_type(vschema, "lo") == py_tc, (
                f"{label}: MIN column type {self._col_type(vschema, 'lo')} != {py_tc}")
            assert self._col_type(vschema, "hi") == py_tc, (
                f"{label}: MAX column type {self._col_type(vschema, 'hi')} != {py_tc}")

            # Replicate each group's values across distinct PKs so a group's rows
            # spread over the workers (drives the gather-reduce combine on a
            # narrow column); >=2 groups land in one partial batch.
            pk = 0
            rows_sql = []
            expected = {}
            for g, vals in groups.items():
                for _rep in range(4):
                    for val in vals:
                        pk += 1
                        rows_sql.append(f"({pk}, {g}, {val})")
                expected[g] = (min(vals), max(vals))
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(rows_sql), schema_name=sn)

            got = {r["k"]: (r["lo"], r["hi"]) for r in client.scan(vid) if r.weight > 0}
            assert got == expected, (
                f"{label}: per-group (MIN, MAX) wrong: {got} != {expected}")

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_f32_minmax_stays_f64_across_workers(self, client):
        """MIN/MAX over an F32 (FLOAT) column widen to F64: the output column is
        8 bytes holding `f64::to_bits`, so the width-gated read-back reads it
        verbatim instead of mis-decoding it as F32. Many groups plus an
        incremental update exercise the gather combine and its retraction on
        that 8-byte-but-float-source column."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  k BIGINT NOT NULL,"
                "  v FLOAT NOT NULL"  # F32
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT k, MIN(v) AS lo, MAX(v) AS hi "
                "FROM t GROUP BY k",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table_id(sn, "v")
            assert self._col_type(vschema, "lo") == gnitz.TypeCode.F64, (
                f"F32 MIN must widen to F64, got {self._col_type(vschema, 'lo')}")
            assert self._col_type(vschema, "hi") == gnitz.TypeCode.F64

            # F32-exact values (dyadic, small) so the F32->F64 widening is exact.
            pk = 0
            rows_sql = []
            expected = {}
            for g in range(8):
                vals = [-2.25 + g, 1.5 + g, 4.0 + g, 0.5 + g]
                for _rep in range(3):
                    for val in vals:
                        pk += 1
                        rows_sql.append(f"({pk}, {g}, {val})")
                expected[g] = (min(vals), max(vals))
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(rows_sql), schema_name=sn)

            got = {r["k"]: (r["lo"], r["hi"]) for r in client.scan(vid) if r.weight > 0}
            for g, (lo, hi) in expected.items():
                assert abs(got[g][0] - lo) < 1e-6, f"group {g} MIN: {got[g][0]} != {lo}"
                assert abs(got[g][1] - hi) < 1e-6, f"group {g} MAX: {got[g][1]} != {hi}"

            # Incremental: push a new global MAX into group 0 — triggers the
            # gather retraction read-back of the prior F64-widened MAX.
            client.execute_sql(
                f"INSERT INTO t VALUES ({pk + 1}, 0, 99.5)", schema_name=sn)
            got = {r["k"]: (r["lo"], r["hi"]) for r in client.scan(vid) if r.weight > 0}
            assert abs(got[0][1] - 99.5) < 1e-6, f"new MAX must be 99.5, got {got[0][1]}"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    @pytest.mark.parametrize(
        "sql_type,py_tc,vals,drop_value,expect_after,is_max", [
            # MIN held by a negative row; deleting it recovers the next-smallest.
            ("INT", gnitz.TypeCode.I32, [-1000, -5, 50, 200], -1000, -5, False),
            # MAX above i32::MAX; the retraction must read the old MAX
            # zero-extended (a sign-extended read turns 4e9 negative and breaks it).
            ("INT UNSIGNED", gnitz.TypeCode.U32,
             [100, 2_000_000_000, 4_000_000_000], 4_000_000_000, 2_000_000_000, True),
        ], ids=["int_min", "u32_max_above_i32max"],
    )
    def test_narrow_minmax_retraction(
        self, client, sql_type, py_tc, vals, drop_value, expect_after, is_max,
    ):
        """Deleting the row holding a group's extremum retracts the old narrow
        value (read from trace_out at the source width) and recomputes the next
        extremum. A wrong-width / sign-extended read corrupts the retraction."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                f"v {sql_type} NOT NULL)",
                schema_name=sn,
            )
            agg = "MAX" if is_max else "MIN"
            client.execute_sql(
                f"CREATE VIEW v AS SELECT k, {agg}(v) AS m FROM t GROUP BY k",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table_id(sn, "v")
            assert self._col_type(vschema, "m") == py_tc

            # Group 1 is retracted from; group 2 is an untouched control (so a
            # partial batch carries >1 group). Separate INSERTs drive incremental
            # extremum updates, each reading the prior narrow value back.
            pk = 0
            drop_pk = None
            for val in vals:
                pk += 1
                if val == drop_value:
                    drop_pk = pk
                client.execute_sql(
                    f"INSERT INTO t VALUES ({pk}, 1, {val})", schema_name=sn)
            pk += 1
            client.execute_sql(f"INSERT INTO t VALUES ({pk}, 2, 7)", schema_name=sn)

            extremum = max(vals) if is_max else min(vals)
            got = {r["k"]: r["m"] for r in client.scan(vid) if r.weight > 0}
            assert got[1] == extremum, f"initial {agg}: {got[1]} != {extremum}"

            client.execute_sql(f"DELETE FROM t WHERE pk = {drop_pk}", schema_name=sn)
            got = {r["k"]: r["m"] for r in client.scan(vid) if r.weight > 0}
            assert got[1] == expect_after, (
                f"after retracting the {agg} holder, {agg} must recover "
                f"{expect_after}, got {got[1]}")
            assert got[2] == 7, "untouched group must be unchanged"

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_narrow_minmax_having_and_projection(self, client):
        """HAVING and the projected column both read the agg at the source width
        (the planner's `reduce_schema` mirror). A SMALLINT MAX must filter and
        project correctly as SMALLINT — if the mirror still said BIGINT, the
        finalize copy and the HAVING bind would use the wrong width."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "v SMALLINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT k, MAX(v) AS m FROM t GROUP BY k "
                "HAVING MAX(v) > 1000",
                schema_name=sn,
            )
            vid, vschema = client.resolve_table_id(sn, "v")
            assert self._col_type(vschema, "m") == gnitz.TypeCode.I16, (
                f"projected MAX must be SMALLINT/I16, got {self._col_type(vschema, 'm')}")

            # Groups straddling the threshold 1000 (values within I16 range):
            #   k=10 MAX=900   excluded     k=20 MAX=1500  kept
            #   k=30 MAX=30000 kept         k=40 MAX=1000  excluded (not > 1000)
            client.execute_sql(
                "INSERT INTO t VALUES "
                "(1,10,900),(2,10,500),(3,10,-100),"
                "(4,20,1500),(5,20,200),(6,20,-5),"
                "(7,30,30000),(8,30,123),(9,30,-32768),"
                "(10,40,1000),(11,40,0),(12,40,7)",
                schema_name=sn,
            )
            got = {r["k"]: r["m"] for r in client.scan(vid) if r.weight > 0}
            assert got == {20: 1500, 30: 30000}, (
                f"HAVING MAX(v) > 1000 wrong: {got}")

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)
