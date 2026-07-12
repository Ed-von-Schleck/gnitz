"""E2E tests for GROUP BY with COUNT/SUM/MIN/MAX/AVG, HAVING.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_aggregates.py -v --tb=short
"""
import random
import pytest
import gnitz
import _oracle as oracle


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

    def test_wide_group_by_min_max_u128_pk_source(self, client):
        """GROUP BY MIN/MAX over a table with a 16-byte PRIMARY KEY
        (DECIMAL(38,0) = U128). The two MIN/MAX aggregates over a non-PK group key
        resolve through the combined value index (keyed `g ‖ ordinal ‖ av`), which
        never re-reads the source trace by PK — so the wide U128 source PK (big ids
        > u64::MAX occupying all 16 bytes) is irrelevant to the read path. Deleting
        the row holding a group's MIN/MAX forces an incremental recompute that
        seeks the index's post-delta extreme directly."""
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

            # Delete group 10's MIN holder (id=2) → MIN recomputes to 100 from the
            # combined value index's post-delta extreme for group 10.
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

    def test_pk_source_min_group_by_full_pk(self, client):
        """The aggregate source is a PRIMARY KEY column: `SELECT a, b, MIN(b) ...
        GROUP BY a, b` over PK (a, b). Each (a, b) is its own group so MIN(b) is
        just b — but the value-index population must OPK-decode b's at-rest bytes
        before order-encoding, or the round-tripped MIN(b) is byte-swapped (256 ->
        1). b straddles the high byte and the sign."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b))",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a, b, MIN(b) AS mb FROM t GROUP BY a, b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # b straddles the high byte (1=0x0001 and 256=0x0100 are byte-swap
            # twins) and the sign (-5); MIN(b) must round-trip to b for every row.
            rows = [(1, 1), (1, 256), (1, 100), (2, 65536), (2, -5), (2, 300)]
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(f"({a}, {b})" for a, b in rows),
                schema_name=sn,
            )
            got = {(r["a"], r["b"]): r["mb"] for r in client.scan(vid) if r.weight > 0}
            assert len(got) == len(rows), f"expected {len(rows)} groups, got {got}"
            for a, b in rows:
                assert got[(a, b)] == b, f"MIN(b) for ({a},{b}) must round-trip to {b}, got {got[(a, b)]}"

            # Delete the row at a byte-swap-sensitive value: its group vanishes,
            # the others are untouched.
            client.execute_sql("DELETE FROM t WHERE a = 1 AND b = 256", schema_name=sn)
            got = {(r["a"], r["b"]): r["mb"] for r in client.scan(vid) if r.weight > 0}
            assert (1, 256) not in got, "deleted group must vanish"
            assert got[(1, 1)] == 1 and got[(2, 65536)] == 65536, got

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_pk_source_min_max_group_by_partial_pk(self, client):
        """MIN/MAX over a PRIMARY KEY column with multi-row groups: `SELECT a,
        MIN(b), MAX(b) ... GROUP BY a` over PK (a, b). A group holds several `b`
        that straddle the high byte, so the value index must OPK-decode `b` to keep
        its order — else the extreme walk (and its retract-at-extreme recovery)
        selects a byte-swapped value."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b))",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a, MIN(b) AS lo, MAX(b) AS hi FROM t GROUP BY a",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            # Group 100: b straddles the high byte {1, 256, 100, 65536} (MIN=1,
            # MAX=65536). Group 300: negatives {-5, 4, 10} (MIN=-5, MAX=10).
            groups = {100: [1, 256, 100, 65536], 300: [-5, 4, 10]}
            rows = [(a, b) for a, bs in groups.items() for b in bs]
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(f"({a}, {b})" for a, b in rows),
                schema_name=sn,
            )
            by_a = {r["a"]: r for r in client.scan(vid) if r.weight > 0}
            assert by_a[100]["lo"] == 1 and by_a[100]["hi"] == 65536, by_a
            assert by_a[300]["lo"] == -5 and by_a[300]["hi"] == 10, by_a

            # Delete group 100's MAX holder (b=65536) → MAX falls to 256 (the
            # byte-swap twin of 1: order must survive the decode).
            client.execute_sql("DELETE FROM t WHERE a = 100 AND b = 65536", schema_name=sn)
            by_a = {r["a"]: r for r in client.scan(vid) if r.weight > 0}
            assert by_a[100]["lo"] == 1 and by_a[100]["hi"] == 256, by_a

            # Delete group 100's MIN holder (b=1) → MIN rises to 100.
            client.execute_sql("DELETE FROM t WHERE a = 100 AND b = 1", schema_name=sn)
            by_a = {r["a"]: r for r in client.scan(vid) if r.weight > 0}
            assert by_a[100]["lo"] == 100 and by_a[100]["hi"] == 256, by_a

            # Delete group 300's MIN holder (b=-5) → MIN rises to 4.
            client.execute_sql("DELETE FROM t WHERE a = 300 AND b = -5", schema_name=sn)
            by_a = {r["a"]: r for r in client.scan(vid) if r.weight > 0}
            assert by_a[300]["lo"] == 4 and by_a[300]["hi"] == 10, by_a

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
            # Category 10 is emptied → the group must VANISH (cardinality 0), not
            # survive as a cnt=0 zombie. Category 20 stays at cnt=1.
            assert 10 not in by_cat, f"emptied group must be absent, got {by_cat}"
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
            # Emptied SUM-only group must VANISH, not survive as a total=0 zombie.
            assert 10 not in totals, f"emptied group must be absent, got {totals}"

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
        """MIN/MAX GROUP BY a nullable int. A nullable group column is not
        byte-form-eligible for the combined value index (the key prefix has no
        null bit), so the reduce takes the single-scan trace fallback, which
        distinguishes a NULL group from group 0 — they must not collide (a
        collision would fetch 0's history or drop NULL's state)."""
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


class TestHavingIsNullCompleteness:
    """HAVING IS [NOT] NULL over the grouped relation for direct aggregates
    (MIN/MAX/COUNT/non-nullable SUM) and group columns. Direct aggregates read the
    value column's raw null bit; group columns const-fold when non-nullable (every
    PK-region key) or read their payload null bit on the synthetic path. Complements
    the companion-gated nullable-SUM/AVG HAVING tests in TestGroupBy."""

    def test_having_min_is_null_direct_aggregate(self, client):
        """HAVING MIN(v) IS [NOT] NULL reads the value column's raw null bit: an
        all-NULL group renders MIN=NULL. Membership updates incrementally as a
        group flips to all-NULL by retraction — weights verified."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_null AS SELECT k, MIN(v) AS mn FROM t GROUP BY k HAVING MIN(v) IS NULL",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_notnull AS SELECT k, MIN(v) AS mn FROM t GROUP BY k HAVING MIN(v) IS NOT NULL",
                schema_name=sn,
            )
            vid_null = client.resolve_table(sn, "v_null")[0]
            vid_notnull = client.resolve_table(sn, "v_notnull")[0]

            def groups(vid):
                return {r["k"]: r.weight for r in client.scan(vid) if r.weight > 0}

            # k=10: {5, NULL} → MIN=5; k=20: {NULL} → MIN=NULL.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 5), (2, 10, NULL), (3, 20, NULL)",
                schema_name=sn,
            )
            assert groups(vid_null) == {20: 1}, "IS NULL admits only the all-NULL group k=20"
            assert groups(vid_notnull) == {10: 1}, "IS NOT NULL admits only k=10 (MIN=5)"

            # Retract k=10's only non-NULL contributor → MIN(k=10) becomes NULL.
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            assert groups(vid_null) == {10: 1, 20: 1}, "k=10 enters IS NULL once its MIN → NULL"
            assert groups(vid_notnull) == {}, "no group has a non-NULL MIN now"

            client.execute_sql("DROP VIEW v_null", schema_name=sn)
            client.execute_sql("DROP VIEW v_notnull", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_count_star_is_not_null_const_folds(self, client):
        """COUNT(*) never renders NULL, so HAVING COUNT(*) IS NOT NULL const-folds
        to true (every surviving group passes) and IS NULL to false (none)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_pass AS SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING COUNT(*) IS NOT NULL",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_none AS SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING COUNT(*) IS NULL",
                schema_name=sn,
            )
            vid_pass = client.resolve_table(sn, "v_pass")[0]
            vid_none = client.resolve_table(sn, "v_none")[0]
            client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 20)", schema_name=sn)
            assert {r["k"]: r.weight for r in client.scan(vid_pass) if r.weight > 0} == {10: 1, 20: 1}
            assert [r for r in client.scan(vid_none) if r.weight > 0] == [], "IS NULL admits no group"
            client.execute_sql("DROP VIEW v_pass", schema_name=sn)
            client.execute_sql("DROP VIEW v_none", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_nullable_group_col_is_null(self, client):
        """HAVING on a nullable GROUP BY column (synthetic-PK path) reads the payload
        null bit: IS NULL admits the single NULL-key group, IS NOT NULL admits the
        rest (including the distinct integer-zero group)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_null AS SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING g IS NULL",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_notnull AS SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING g IS NOT NULL",
                schema_name=sn,
            )
            vid_null = client.resolve_table(sn, "v_null")[0]
            vid_notnull = client.resolve_table(sn, "v_notnull")[0]
            # Two NULL-key rows, one g=0, one g=7.
            client.execute_sql("INSERT INTO t VALUES (1, NULL), (2, NULL), (3, 0), (4, 7)", schema_name=sn)
            null_rows = [r for r in client.scan(vid_null) if r.weight > 0]
            assert len(null_rows) == 1, "IS NULL admits exactly the NULL-key group"
            assert null_rows[0]["g"] is None and null_rows[0]["c"] == 2 and null_rows[0].weight == 1
            nn = {r["g"]: (r["c"], r.weight) for r in client.scan(vid_notnull) if r.weight > 0}
            assert nn == {0: (1, 1), 7: (1, 1)}, "IS NOT NULL admits g=0 and g=7 (NULL != 0)"
            client.execute_sql("DROP VIEW v_null", schema_name=sn)
            client.execute_sql("DROP VIEW v_notnull", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_natural_pk_group_col_const_folds(self, client):
        """HAVING on a natural-PK group column const-folds (PK columns are
        non-nullable), for both a single-column PK (group_set_eq_pk) and a compound
        PK. Emitting EXPR_IS_NULL against the PK region would trip eval_is_null's
        debug assertion — running against the debug server exercises that guard.
        IS NOT NULL passes every group; IS NULL passes none."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            # Single-column PK: GROUP BY pk hits group_set_eq_pk.
            client.execute_sql(
                "CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW s_pass AS SELECT pk, COUNT(*) AS c FROM t1 GROUP BY pk HAVING pk IS NOT NULL",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW s_none AS SELECT pk, COUNT(*) AS c FROM t1 GROUP BY pk HAVING pk IS NULL",
                schema_name=sn,
            )
            # Compound PK: GROUP BY a, b hits the compound group_set_eq_pk path.
            client.execute_sql(
                "CREATE TABLE t2 (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b))",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW c_pass AS SELECT a, b, COUNT(*) AS c FROM t2 GROUP BY a, b HAVING a IS NOT NULL",
                schema_name=sn,
            )
            s_pass = client.resolve_table(sn, "s_pass")[0]
            s_none = client.resolve_table(sn, "s_none")[0]
            c_pass = client.resolve_table(sn, "c_pass")[0]

            client.execute_sql("INSERT INTO t1 VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO t2 VALUES (5, 6), (5, 7)", schema_name=sn)

            assert {r["pk"]: r.weight for r in client.scan(s_pass) if r.weight > 0} == {1: 1, 2: 1}
            assert [r for r in client.scan(s_none) if r.weight > 0] == [], "IS NULL on a PK admits nothing"
            got = {(r["a"], r["b"]): r.weight for r in client.scan(c_pass) if r.weight > 0}
            assert got == {(5, 6): 1, (5, 7): 1}, "compound-PK group col IS NOT NULL passes all"

            for v in ("s_pass", "s_none", "c_pass"):
                client.execute_sql(f"DROP VIEW {v}", schema_name=sn)
            client.execute_sql("DROP TABLE t1", schema_name=sn)
            client.execute_sql("DROP TABLE t2", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_qualified_aggregate_column(self, client):
        """HAVING MIN(t.v) — a qualified aggregate argument — binds the same column
        as SELECT MIN(t.v), in both the IS [NOT] NULL and the value positions."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_notnull AS SELECT k FROM t GROUP BY k HAVING MIN(t.v) IS NOT NULL",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v_gt AS SELECT k FROM t GROUP BY k HAVING MIN(t.v) > 3",
                schema_name=sn,
            )
            vid_notnull = client.resolve_table(sn, "v_notnull")[0]
            vid_gt = client.resolve_table(sn, "v_gt")[0]
            # k=10: MIN=5 (not null, >3); k=20: MIN=NULL; k=30: MIN=2 (not null, not >3).
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 5), (2, 20, NULL), (3, 30, 2)",
                schema_name=sn,
            )
            assert {r["k"] for r in client.scan(vid_notnull) if r.weight > 0} == {10, 30}
            assert {r["k"] for r in client.scan(vid_gt) if r.weight > 0} == {10}
            client.execute_sql("DROP VIEW v_notnull", schema_name=sn)
            client.execute_sql("DROP VIEW v_gt", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_null_test_on_expression_rejected(self, client):
        """IS [NOT] NULL in HAVING is only for an aggregate or a group column; an
        arithmetic expression is rejected (not silently treated as a column)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError, match="aggregate or a group column"):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT a FROM t GROUP BY a HAVING (a + b) IS NULL",
                    schema_name=sn,
                )
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
            vid, vschema = client.resolve_table(sn, "v")

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
            vid, vschema = client.resolve_table(sn, "v")
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
            vid, vschema = client.resolve_table(sn, "v")
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
            vid, vschema = client.resolve_table(sn, "v")
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


class TestAggregateDifferential:
    """GROUP BY aggregates checked against the from-scratch differential oracle
    after every epoch, including a nested aggregate-over-DISTINCT view. The
    expected value is recomputed in pure Python and never touches the engine, so
    a shared-operator bug cannot hide behind a hand-written expectation the way it
    can in the value-pinned tests above. The source is a unique-PK base table, so
    these stay clear of the foreign-group reduce-gather defect (a separate plan
    owns the non-unique-PK multi-aggregate shape). All columns are integer, so
    every column — integer AVG included — is compared by value: SUM/COUNT are
    exact integers and AVG is one deterministic f64 division of them."""

    def test_all_aggs_integer_churn(self, client):
        """COUNT/SUM/AVG/MIN/MAX over one integer column driven through insert →
        UPDATE → delete-the-extremum (forces a MIN/MAX retraction recompute) →
        empty-a-group → re-insert. AVG values are non-even (7/3, 3/2) so the f64
        division is actually exercised, not just whole-number results."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
                "a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, COUNT(*) AS cnt, SUM(a) AS total, "
                "AVG(a) AS av, MIN(a) AS lo, MAX(a) AS hi FROM t GROUP BY g",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["g", "cnt", "total", "av", "lo", "hi"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["g", "a"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["g", "a"], ["g"],
                    [("cnt", "COUNT", None), ("total", "SUM", "a"), ("av", "AVG", "a"),
                     ("lo", "MIN", "a"), ("hi", "MAX", "a")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            # g=10: {1,2,4} → AVG 7/3; g=20: {5} → AVG 5.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 1), (2, 10, 2), (3, 10, 4), (4, 20, 5)",
                schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 10, "a": 1}, {"pk": 2, "g": 10, "a": 2},
                {"pk": 3, "g": 10, "a": 4}, {"pk": 4, "g": 20, "a": 5}])
            check("after-insert")

            # Raise g=10's max to 9 → SUM/AVG/MAX shift.
            client.execute_sql("UPDATE t SET a = 9 WHERE pk = 3", schema_name=sn)
            oracle.apply_update(t_state, "pk", 3, {"a": 9})
            check("after-update-max")

            # Delete g=10's current MAX holder → MAX recomputes from history to 2.
            client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [3])
            check("after-delete-extremum")

            # Empty g=20 entirely → the group vanishes from the view.
            client.execute_sql("DELETE FROM t WHERE pk = 4", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [4])
            check("after-group-empty")

            # Re-create g=20 with a fresh value.
            client.execute_sql("INSERT INTO t VALUES (5, 20, 8)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 5, "g": 20, "a": 8}])
            check("after-reinsert")
        finally:
            client.drop_schema(sn)

    def test_nullable_aggs_null_edge_churn(self, client):
        """A nullable aggregate column driven to all-NULL and back. SUM/AVG/MIN/MAX
        must skip NULL inputs and read back NULL once a group's last non-NULL value
        is retracted (the group surviving via COUNT(*)), then recover — exactly the
        null-gate the oracle models. Independent recompute across the whole churn,
        not a single hand-checked transition."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "v BIGINT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT k, COUNT(*) AS c, SUM(v) AS sm, AVG(v) AS av, "
                "MIN(v) AS mn, MAX(v) AS hi FROM t GROUP BY k",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["k", "c", "sm", "av", "mn", "hi"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["k", "v"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["k", "v"], ["k"],
                    [("c", "COUNT", None), ("sm", "SUM", "v"), ("av", "AVG", "v"),
                     ("mn", "MIN", "v"), ("hi", "MAX", "v")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            # k=10: one non-NULL (5) + one NULL → aggs over {5}, c=2.
            client.execute_sql("INSERT INTO t VALUES (1, 10, 5), (2, 10, NULL)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "k": 10, "v": 5}, {"pk": 2, "k": 10, "v": None}])
            check("after-insert-mixed")

            # Add a second non-NULL → aggs over {5,15}, AVG 10.
            client.execute_sql("INSERT INTO t VALUES (3, 10, 15)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 3, "k": 10, "v": 15}])
            check("after-second-nonnull")

            # Retract one non-NULL → aggs over {15}.
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1])
            check("after-delete-one-nonnull")

            # Retract the last non-NULL → group all-NULL: SUM/AVG/MIN/MAX NULL, c=1.
            client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [3])
            check("after-all-null")

            # Re-introduce a non-NULL → aggregates recover.
            client.execute_sql("INSERT INTO t VALUES (4, 10, 7)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 4, "k": 10, "v": 7}])
            check("after-recover")
        finally:
            client.drop_schema(sn)

    def test_nested_count_distinct_over_distinct_view_churn(self, client):
        """A nested view — outer GROUP BY COUNT(*) over an inner SELECT DISTINCT —
        is count-distinct-per-group, checked differentially across churn. It
        composes incremental DISTINCT boundary-crossing with a cross-view delta
        into a downstream aggregate, a path no single-view test reaches. The oracle
        chains oracle_distinct into oracle_groupby_aggregate, mirroring the view
        stack. Both groups stay non-empty throughout (a fully-emptied COUNT-only
        group is a separate, independently tracked reduce defect), so the churn
        exercises distinct-pair boundary crossings, duplicate carriers, and a
        cross-group key UPDATE — never a group's elimination."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
                "k BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql("CREATE VIEW d AS SELECT DISTINCT g, k FROM t", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM d GROUP BY g", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["g", "c"]
            t_state = {}

            def check(ctx):
                d = oracle.oracle_distinct(
                    oracle.oracle_filter_project(t_state, None, ["g", "k"]))
                exp, cols = oracle.oracle_groupby_aggregate(
                    d, ["g", "k"], ["g"], [("c", "COUNT", None)])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            # g=1: k in {7,7,8} → distinct {7,8} → c=2; g=2: k in {9,10} → c=2.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 7), (2, 1, 7), (3, 1, 8), (4, 2, 9), (5, 2, 10)",
                schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 1, "k": 7}, {"pk": 2, "g": 1, "k": 7},
                {"pk": 3, "g": 1, "k": 8}, {"pk": 4, "g": 2, "k": 9},
                {"pk": 5, "g": 2, "k": 10}])
            check("after-insert")

            # Drop one carrier of (1,7) → pair survives → g=1 c=2 unchanged.
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1])
            check("after-delete-one-carrier")

            # Drop the last carrier of (1,7) → its pair exits → g=1 distinct {8} c=1
            # (the group stays alive via k=8 — no outer-group elimination).
            client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [2])
            check("after-delete-last-carrier-of-pair")

            # Insert a duplicate carrier of (2,9) → distinct set unchanged → c=2.
            client.execute_sql("INSERT INTO t VALUES (6, 2, 9)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 6, "g": 2, "k": 9}])
            check("after-duplicate-carrier")

            # Move (2,10)'s only carrier onto k=9 → pair (2,10) exits → g=2 c=1
            # (still alive via the (2,9) carriers).
            client.execute_sql("UPDATE t SET k = 9 WHERE pk = 5", schema_name=sn)
            oracle.apply_update(t_state, "pk", 5, {"k": 9})
            check("after-update-onto-existing-pair")

            # Cross-group key UPDATE: move pk=4 from g=2 to g=1 (k=9). g=2 keeps its
            # (2,9) pair via pk5/pk6 (c=1), g=1 gains a new distinct (1,9) → c=2.
            # Retracts one DISTINCT pair and inserts another in one epoch, both
            # groups remaining non-empty.
            client.execute_sql("UPDATE t SET g = 1 WHERE pk = 4", schema_name=sn)
            oracle.apply_update(t_state, "pk", 4, {"g": 1})
            check("after-cross-group-move")
        finally:
            client.drop_schema(sn)


class TestAggregateQualifierRejection:
    """Aggregate-call qualifiers the binder does not implement (DISTINCT, FILTER,
    OVER, …) must be rejected loudly, never silently dropped to the plain
    aggregate — the durable-wrong-result class. Both binding sites are guarded:
    the GROUP BY SELECT list and HAVING."""

    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t ("
            "  pk BIGINT NOT NULL PRIMARY KEY,"
            "  g BIGINT NOT NULL,"
            "  x BIGINT NOT NULL"
            ")",
            schema_name=sn,
        )

    @pytest.mark.parametrize("fn", ["COUNT", "SUM", "AVG", "MIN", "MAX"])
    def test_select_list_distinct_rejected(self, client, fn):
        """SELECT-list `agg(DISTINCT x)` binds to the plain aggregate today; the
        guard must reject it so the view is never created. MIN/MAX(DISTINCT x) is
        a no-op distinct but is rejected by the blanket rule for a uniform surface."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    f"CREATE VIEW v AS SELECT g, {fn}(DISTINCT x) AS a FROM t GROUP BY g",
                    schema_name=sn,
                )
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_having_distinct_rejected(self, client):
        """The second binding site: `HAVING COUNT(DISTINCT x) > 1` routes through
        having_agg_func, not bind_function — a SELECT-list-only fix would miss it
        and silently evaluate COUNT(x) > 1."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT g, SUM(x) AS s FROM t GROUP BY g "
                    "HAVING COUNT(DISTINCT x) > 1",
                    schema_name=sn,
                )
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_filter_clause_rejected(self, client):
        """`SUM(x) FILTER (WHERE x > 0)` is the same silent-drop class as DISTINCT
        — the FILTER would be dropped and a plain SUM(x) computed."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT g, SUM(x) FILTER (WHERE x > 0) AS s "
                    "FROM t GROUP BY g",
                    schema_name=sn,
                )
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_over_window_rejected(self, client):
        """`SUM(x) OVER (PARTITION BY g)` is a window function silently computed as
        a grouped aggregate — window semantics lost. Must be rejected."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT g, SUM(x) OVER (PARTITION BY g) AS s "
                    "FROM t GROUP BY g",
                    schema_name=sn,
                )
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_plain_aggregates_and_all_still_compute(self, client):
        """Regression: the guard must leave plain aggregates and the one accepted
        qualifier — ALL (`COUNT(ALL x)` ≡ `COUNT(x)`) — binding and computing
        unchanged."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, "
                "COUNT(*) AS c_star, COUNT(x) AS c_x, COUNT(ALL x) AS c_all, "
                "SUM(x) AS s, AVG(x) AS a, MIN(x) AS mn, MAX(x) AS mx "
                "FROM t GROUP BY g",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 2), (2, 10, 4), (3, 20, 7)",
                schema_name=sn,
            )
            by_g = {r["g"]: r for r in client.scan(vid) if r.weight > 0}
            assert by_g[10]["c_star"] == 2
            assert by_g[10]["c_x"] == 2
            assert by_g[10]["c_all"] == 2            # COUNT(ALL x) ≡ COUNT(x)
            assert by_g[10]["s"] == 6                # 2 + 4
            assert abs(by_g[10]["a"] - 3.0) < 0.001  # AVG = 6/2
            assert by_g[10]["mn"] == 2
            assert by_g[10]["mx"] == 4
            assert by_g[20]["c_all"] == 1
            assert by_g[20]["s"] == 7
            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)


class TestLinearReduceGroupExistence:
    """An all-linear GROUP BY view (COUNT/SUM/AVG, no MIN/MAX) carrying NO user
    COUNT(*) must still decide group existence on cardinality: an emptied group
    vanishes (no sum=0 / NULL / count=0 zombie) and a brand-new group whose
    aggregated column is all-NULL appears (SUM/AVG = NULL). These are invisible to
    the COUNT(*)-carrying differential tests — COUNT(*)'s accumulator is touched
    on every row and masks the gate. Checked against the from-scratch oracle,
    which models the SQL semantics exactly (an emptied group is absent; an all-NULL
    group is present with SUM/AVG = NULL)."""

    def test_sum_only_nonnull_empty_and_recreate(self, client):
        """SELECT g, SUM(a) over non-nullable a, no COUNT(*): emptying a group
        drops it (no sum=0 zombie); re-inserting brings it back."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
                "a BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, SUM(a) AS s FROM t GROUP BY g",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["g", "s"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["g", "a"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["g", "a"], ["g"], [("s", "SUM", "a")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 3), (2, 20, 5)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 10, "a": 3}, {"pk": 2, "g": 20, "a": 5}])
            check("after-insert")

            # Empty g=10 → the group must vanish, not survive as (g=10, s=0).
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1])
            check("after-empty-g10")

            # Re-create g=10 → it returns.
            client.execute_sql("INSERT INTO t VALUES (3, 10, 7)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 3, "g": 10, "a": 7}])
            check("after-recreate-g10")
        finally:
            client.drop_schema(sn)

    def test_sum_only_nullable_all_null_group_and_empty(self, client):
        """SELECT g, SUM(a) over nullable a, no COUNT(*): a brand-new group whose
        only row is all-NULL appears with SUM = NULL (not dropped); emptying a
        group still removes it."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
                "a BIGINT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, SUM(a) AS s FROM t GROUP BY g",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["g", "s"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["g", "a"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["g", "a"], ["g"], [("s", "SUM", "a")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 4), (2, 20, 6)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 10, "a": 4}, {"pk": 2, "g": 20, "a": 6}])
            check("after-insert")

            # New group g=7 whose only row is all-NULL → present with SUM = NULL.
            client.execute_sql("INSERT INTO t VALUES (3, 7, NULL)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 3, "g": 7, "a": None}])
            check("after-new-all-null")

            # Empty g=10 → vanishes.
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1])
            check("after-empty-g10")
        finally:
            client.drop_schema(sn)

    def test_avg_only_nullable_all_null_group_and_empty(self, client):
        """SELECT g, AVG(a) over nullable a, no COUNT(*): same lifecycle as SUM —
        a new all-NULL group appears with AVG = NULL; an emptied group vanishes."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
                "a BIGINT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, AVG(a) AS av FROM t GROUP BY g",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["g", "av"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["g", "a"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["g", "a"], ["g"], [("av", "AVG", "a")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 4), (2, 20, 6)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 10, "a": 4}, {"pk": 2, "g": 20, "a": 6}])
            check("after-insert")

            # New all-NULL group g=7 → present with AVG = NULL.
            client.execute_sql("INSERT INTO t VALUES (3, 7, NULL)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 3, "g": 7, "a": None}])
            check("after-new-all-null")

            # Empty g=10 → vanishes.
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1])
            check("after-empty-g10")
        finally:
            client.drop_schema(sn)

    def test_count_col_only_empty_recreate_and_all_null_present(self, client):
        """SELECT g, COUNT(a) over nullable a, no COUNT(*): emptying a group drops
        it, re-inserting restores it. A new all-NULL group becomes PRESENT with
        COUNT(a) = 0 (SQL guarantees COUNT is never NULL), asserted through the
        Python client's null-bit-gated decode via a direct scan."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
                "a BIGINT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, COUNT(a) AS c FROM t GROUP BY g",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["g", "c"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["g", "a"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["g", "a"], ["g"], [("c", "COUNT", "a")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 4), (2, 20, 6)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 10, "a": 4}, {"pk": 2, "g": 20, "a": 6}])
            check("after-insert")

            # Empty g=10 → vanishes; re-insert → returns (COUNT(a) = 1).
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1])
            check("after-empty-g10")
            client.execute_sql("INSERT INTO t VALUES (3, 10, 7)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 3, "g": 10, "a": 7}])
            check("after-recreate-g10")

            # New all-NULL group g=7: the row APPEARS and COUNT(a) renders as a
            # concrete 0 (SQL guarantees COUNT is never NULL), surfaced here through
            # the client's null-bit-gated decode.
            client.execute_sql("INSERT INTO t VALUES (4, 7, NULL)", schema_name=sn)
            g7 = [r for r in client.scan(vid) if r.weight > 0 and r["g"] == 7]
            assert len(g7) == 1, f"new all-NULL COUNT(a) group must appear, got {g7}"
            assert g7[0]["c"] == 0, f"COUNT(a) of all-NULL group must be 0, not NULL, got {g7[0]['c']}"
        finally:
            client.drop_schema(sn)

    def test_count_star_and_col_all_null_and_mixed_groups_churn(self, client):
        """SELECT g, COUNT(*), COUNT(x): the differential oracle drives the VALUE
        (not just existence) of COUNT(x) over an all-NULL group (= 0) and a mixed
        group (some NULL, some non-NULL x), across inserts and deletes. COUNT(*)
        counts every row; COUNT(x) counts only non-NULL x and is never NULL — so
        emptying a group's non-NULL rows leaves COUNT(x) = 0 while COUNT(*) keeps
        the row alive."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
                "x BIGINT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, COUNT(*) AS ca, COUNT(x) AS cx "
                "FROM t GROUP BY g", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["g", "ca", "cx"]
            aggs = [("ca", "COUNT", None), ("cx", "COUNT", "x")]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["g", "x"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["g", "x"], ["g"], aggs)
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            # g=7 all-NULL x (ca=2, cx=0); g=8 mixed (ca=3, cx=2); g=9 all
            # non-NULL control (ca=1, cx=1).
            client.execute_sql(
                "INSERT INTO t VALUES (1, 7, NULL), (2, 7, NULL), (3, 8, 5), "
                "(4, 8, NULL), (5, 8, 6), (6, 9, 9)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 7, "x": None}, {"pk": 2, "g": 7, "x": None},
                {"pk": 3, "g": 8, "x": 5}, {"pk": 4, "g": 8, "x": None},
                {"pk": 5, "g": 8, "x": 6}, {"pk": 6, "g": 9, "x": 9}])
            check("after-insert")

            # Delete one non-NULL of g=8 → cx drops to 1, ca to 2.
            client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [3])
            check("after-delete-one-nonnull-g8")

            # Delete g=8's last non-NULL → g=8 all-NULL: ca=1, cx=0 (still alive).
            client.execute_sql("DELETE FROM t WHERE pk = 5", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [5])
            check("after-delete-to-all-null-g8")

            # Re-insert a non-NULL x into the now-all-NULL g=8 → cx back to 1.
            client.execute_sql("INSERT INTO t VALUES (7, 8, 11)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 7, "g": 8, "x": 11}])
            check("after-reinsert-nonnull-g8")
        finally:
            client.drop_schema(sn)

    def test_nested_sum_inner_empty_no_phantom(self, client):
        """The nested cascade: an outer COUNT(*) GROUP BY over an inner SUM GROUP
        BY. When an inner group empties, the inner row must vanish — otherwise its
        sum=0 zombie feeds the outer as a phantom (s=0, c=1) group and leaves a
        stale (s=old, c=0). Checked by chaining the oracle through both views."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
                "a BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW iv AS SELECT g, SUM(a) AS s FROM t GROUP BY g",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW ov AS SELECT s, COUNT(*) AS c FROM iv GROUP BY s",
                schema_name=sn)
            ov_id = client.resolve_table(sn, "ov")[0]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["g", "a"])
                inner, inner_cols = oracle.oracle_groupby_aggregate(
                    base, ["g", "a"], ["g"], [("s", "SUM", "a")])
                outer, outer_cols = oracle.oracle_groupby_aggregate(
                    inner, inner_cols, ["s"], [("c", "COUNT", None)])
                assert outer_cols == ["s", "c"]
                oracle.assert_view_matches(client, ov_id, ["s", "c"], outer, ctx=ctx)

            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 5), (2, 2, 8)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 1, "a": 5}, {"pk": 2, "g": 2, "a": 8}])
            check("after-insert")

            # Empty inner group g=1 → its row must disappear from iv, so ov shows
            # neither a phantom (s=0, c=1) nor a stale (s=5, c=0).
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1])
            check("after-inner-empty")
        finally:
            client.drop_schema(sn)


class TestGlobalAggregate:
    """Ungrouped (global) aggregate views — `SELECT MIN(x) FROM t` with no GROUP
    BY. One logical group at the synthetic constant PK V0; SQL scalar-aggregate
    semantics require exactly one output row even over an empty/fully-retracted
    source (COUNT(*)=0, SUM/MIN/MAX/AVG=NULL). Run under GNITZ_WORKERS=1 and =4;
    at =4 every source row funnels through partition 220's owner."""

    @staticmethod
    def _one_row(client, vid):
        """The single positive-weight row of a global aggregate (asserts exactly one)."""
        rows = [r for r in client.scan(vid) if r.weight > 0]
        assert len(rows) == 1, f"expected exactly one global row, got {len(rows)}: {rows}"
        return rows[0]

    def test_count_sum_avg_min_max_populated(self, client):
        """All five aggregates over a populated source, one row, correct values."""
        sn = "ga_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total, "
                "AVG(a) AS av, MIN(a) AS lo, MAX(a) AS hi FROM t",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 2), (2, 4), (3, 9)", schema_name=sn)
            r = self._one_row(client, vid)
            assert r["cnt"] == 3
            assert r["total"] == 15
            assert abs(r["av"] - 5.0) < 1e-9
            assert r["lo"] == 2
            assert r["hi"] == 9
        finally:
            client.drop_schema(sn)

    def test_min_max_retract_to_next_best(self, client):
        """Deleting the current MIN/MAX advances the global extremum to next-best."""
        sn = "gar_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW vlo AS SELECT MIN(a) AS m FROM t", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW vhi AS SELECT MAX(a) AS m FROM t", schema_name=sn)
            vlo = client.resolve_table(sn, "vlo")[0]
            vhi = client.resolve_table(sn, "vhi")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)
            assert self._one_row(client, vlo)["m"] == 10
            assert self._one_row(client, vhi)["m"] == 30
            # Delete both extrema; MIN -> 20, MAX -> 20.
            client.execute_sql("DELETE FROM t WHERE pk IN (1, 3)", schema_name=sn)
            assert self._one_row(client, vlo)["m"] == 20, "MIN must advance to next-best"
            assert self._one_row(client, vhi)["m"] == 20, "MAX must advance to next-best"
        finally:
            client.drop_schema(sn)

    def test_where_filter(self, client):
        """A WHERE filter on a global aggregate is honored."""
        sn = "gaw_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total "
                "FROM t WHERE a > 5", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 2), (2, 7), (3, 9)", schema_name=sn)
            r = self._one_row(client, vid)
            assert r["cnt"] == 2 and r["total"] == 16
        finally:
            client.drop_schema(sn)

    def test_having_filter_and_empty_source(self, client):
        """HAVING is a post-reduce filter. Over an empty source the ground row is
        in trace_out@V0 but `HAVING COUNT(*) > 0` filters it out of the view."""
        sn = "gah_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(*) AS cnt FROM t HAVING COUNT(*) > 2",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            # Empty source: 0 > 2 filters the ground row → zero rows.
            assert [r for r in client.scan(vid) if r.weight > 0] == []
            client.execute_sql("INSERT INTO t VALUES (1, 1), (2, 1)", schema_name=sn)
            assert [r for r in client.scan(vid) if r.weight > 0] == [], "2 not > 2"
            client.execute_sql("INSERT INTO t VALUES (3, 1)", schema_name=sn)
            assert self._one_row(client, vid)["cnt"] == 3
        finally:
            client.drop_schema(sn)

    def test_having_count_gt_zero_empty_source_zero_rows(self, client):
        """`HAVING COUNT(*) > 0` over an empty source yields zero rows (the ground
        row exists in trace_out@V0 but is filtered)."""
        sn = "gah0_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(*) AS cnt FROM t HAVING COUNT(*) > 0",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            assert [r for r in client.scan(vid) if r.weight > 0] == []
        finally:
            client.drop_schema(sn)

    def test_empty_source_one_row_at_creation_and_after_delete_all(self, client):
        """Over a never-populated source the view shows one ground row (COUNT(*)=0,
        SUM=NULL); after delete-all it returns to that ground; a refill restores
        the computed values."""
        sn = "gae_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            # View created over an EMPTY table — the seed must fire.
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total FROM t",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            r = self._one_row(client, vid)
            assert r["cnt"] == 0, "COUNT(*) over empty source is 0"
            assert r["total"] is None, "SUM over empty source is NULL"

            client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 7)", schema_name=sn)
            r = self._one_row(client, vid)
            assert r["cnt"] == 2 and r["total"] == 12

            client.execute_sql("DELETE FROM t", schema_name=sn)
            r = self._one_row(client, vid)
            assert r["cnt"] == 0 and r["total"] is None, "delete-all returns to ground"

            client.execute_sql("INSERT INTO t VALUES (3, 4)", schema_name=sn)
            r = self._one_row(client, vid)
            assert r["cnt"] == 1 and r["total"] == 4, "value returns on refill"
        finally:
            client.drop_schema(sn)

    def test_empty_source_count_col_is_zero(self, client):
        """The empty-source ground renders COUNT(col)=0 (where SUM/MIN/MAX would be
        NULL). A non-empty all-NULL source renders COUNT(col)=0 too — the same emit
        predicate — and is covered by the grouped/all-NULL tests; this one pins the
        empty-source ground specifically."""
        sn = "gec_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(a) AS c FROM t", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            assert self._one_row(client, vid)["c"] == 0, "COUNT(col) over empty source is 0"
        finally:
            client.drop_schema(sn)

    def test_nonnullable_sum_empties_to_one_null_row(self, client):
        """A non-nullable lone SUM (AggShape::Direct, no companion) driven empty:
        the cardinality gate sheds the computed row and the ground branch supplies
        one NULL row — not a concrete 0."""
        sn = "gns_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT SUM(a) AS total FROM t", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, 5)", schema_name=sn)
            assert self._one_row(client, vid)["total"] == 5
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            assert self._one_row(client, vid)["total"] is None, "emptied SUM is NULL, not 0"
        finally:
            client.drop_schema(sn)

    def test_lone_min_all_null_then_value(self, client):
        """A lone MIN over an all-NULL (non-empty) source is one NULL row; inserting
        a value makes it appear."""
        sn = "gmn_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT MIN(a) AS m FROM t", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, NULL), (2, NULL)", schema_name=sn)
            assert self._one_row(client, vid)["m"] is None, "all-NULL MIN is NULL"
            client.execute_sql("INSERT INTO t VALUES (3, 7)", schema_name=sn)
            assert self._one_row(client, vid)["m"] == 7, "MIN appears once a value arrives"
        finally:
            client.drop_schema(sn)

    def test_mixed_all_null_count_min_routes_to_ground(self, client):
        """`SELECT COUNT(x), MIN(x)` over a non-empty all-NULL source: all
        accumulators untouched → should_emit false → the ground supplies
        COUNT(x)=0, MIN=NULL (never a COUNT=-N zombie)."""
        sn = "gmm_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(a) AS c, MIN(a) AS m FROM t",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, NULL), (2, NULL)", schema_name=sn)
            r = self._one_row(client, vid)
            assert r["c"] == 0, "all-NULL mixed COUNT(x) routes to ground → 0"
            assert r["m"] is None, "all-NULL MIN is NULL"
        finally:
            client.drop_schema(sn)

    def test_select_star_hides_group_pk(self, client):
        """SELECT * over a global aggregate hides the synthetic `_group_pk` U128
        constant and ships only the aggregate column."""
        sn = "gss_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT MIN(a) AS lo FROM t", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO t VALUES (1, 3)", schema_name=sn)
            r = self._one_row(client, vid)
            cols = set(r._asdict().keys())
            assert "_group_pk" not in cols, f"_group_pk must be hidden, got {cols}"
            assert cols == {"lo"} and r["lo"] == 3
        finally:
            client.drop_schema(sn)

    def test_strict_validator_rejections(self, client):
        """Computed-over-aggregate and literal projections are rejected at plan
        time (the strict grouped-projection validator), pinning the invariant the
        ground derivation depends on."""
        sn = "gsv_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            for bad in [
                "SELECT COUNT(*) + 1 AS c FROM t",
                "SELECT COUNT(*) AS c, 'x' AS lit FROM t",
                "SELECT SUM(a * 2) AS s FROM t",
            ]:
                with pytest.raises(Exception):
                    client.execute_sql(f"CREATE VIEW bad AS {bad}", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_global_aggregate_differential(self, client):
        """Global aggregate checked against the recompute oracle across an
        empty → insert → update → delete-all → reinsert churn. The empty-source
        one-row result (the entire reason the ground machinery exists) is covered
        by the oracle, not only hand assertions."""
        sn = "gad_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total, "
                "AVG(a) AS av, MIN(a) AS lo, MAX(a) AS hi FROM t",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["cnt", "total", "av", "lo", "hi"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["a"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["a"], [],
                    [("cnt", "COUNT", None), ("total", "SUM", "a"), ("av", "AVG", "a"),
                     ("lo", "MIN", "a"), ("hi", "MAX", "a")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            check("empty-at-creation")
            client.execute_sql(
                "INSERT INTO t VALUES (1, 2), (2, 4), (3, 9)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "a": 2}, {"pk": 2, "a": 4}, {"pk": 3, "a": 9}])
            check("after-insert")
            client.execute_sql("UPDATE t SET a = 1 WHERE pk = 3", schema_name=sn)
            oracle.apply_update(t_state, "pk", 3, {"a": 1})
            check("after-update-min")
            client.execute_sql("DELETE FROM t", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1, 2, 3])
            check("after-delete-all")
            client.execute_sql("INSERT INTO t VALUES (4, 8)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 4, "a": 8}])
            check("after-reinsert")
        finally:
            client.drop_schema(sn)

    def test_replicated_source_empty_and_populated(self, client):
        """A global aggregate over a WITH (replicated=true) source: the empty-source
        ground row (the `i_am_owner` replicated disjunct + backfill empty-epoch fix)
        must be exactly one row, weight 1 — not zero (disjunct dropped) and not N
        (N-fold per-worker). Also checks a populated value."""
        sn = "grep_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL) "
                "WITH (replicated = true)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total FROM t",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1, f"replicated empty-source ground must be ONE row, got {len(rows)}"
            assert rows[0].weight == 1, f"ground weight must be 1, got {rows[0].weight}"
            assert rows[0]["cnt"] == 0 and rows[0]["total"] is None
            client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 7)", schema_name=sn)
            r = self._one_row(client, vid)
            assert r["cnt"] == 2 and r["total"] == 12, "replicated source not N-fold multiplied"
        finally:
            client.drop_schema(sn)

    def test_replicated_nonaggregate_regression(self, client):
        """A replicated non-aggregate (filter/map) view over an empty source still
        backfills correctly with the new empty-epoch — no spurious row."""
        sn = "grn_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL) "
                "WITH (replicated = true)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT pk, a * 2 AS d FROM t WHERE a > 0",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            assert [r for r in client.scan(vid) if r.weight > 0] == [], "no spurious row over empty source"
            client.execute_sql("INSERT INTO t VALUES (1, 3)", schema_name=sn)
            rows = {r["pk"]: r["d"] for r in client.scan(vid) if r.weight > 0}
            assert rows == {1: 6}
        finally:
            client.drop_schema(sn)

    def test_min_max_together_under_small_churn(self, client):
        """`SELECT MIN(x), MAX(x)` in one view is NOT AVI-eligible (two aggregates)
        and not all-linear, so it takes the whole-table replay path. Correctness
        only — a small churn (insert → delete an extremum → empty → refill),
        checked against the recompute oracle, exercises that O(table)/tick path."""
        sn = "gmx_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT MIN(a) AS lo, MAX(a) AS hi FROM t",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["lo", "hi"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["a"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["a"], [], [("lo", "MIN", "a"), ("hi", "MAX", "a")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            check("empty")
            client.execute_sql(
                "INSERT INTO t VALUES (1, 5), (2, 9), (3, 2)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "a": 5}, {"pk": 2, "a": 9}, {"pk": 3, "a": 2}])
            check("after-insert")  # lo=2, hi=9
            client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [2])
            check("after-delete-max")  # hi recomputes to 5
            client.execute_sql("DELETE FROM t", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1, 3])
            check("after-empty")  # one NULL/NULL ground row
            client.execute_sql("INSERT INTO t VALUES (4, 7)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 4, "a": 7}])
            check("after-refill")  # lo=hi=7
        finally:
            client.drop_schema(sn)

    # ── Two-phase (distributable) all-linear global aggregate ──────────────────

    @staticmethod
    def _count_reduce_nodes(client, vid):
        """REDUCE (opcode 9) circuit nodes for view `vid`: 2 for the two-phase
        shape (reduce_local + reduce_combine), 1 for the single funnel reduce."""
        OPCODE_REDUCE, CIRCUIT_NODES_TAB = 9, 11
        return sum(
            1 for r in client.scan(CIRCUIT_NODES_TAB)
            if r.weight > 0 and r["view_id"] == vid and r["opcode"] == OPCODE_REDUCE
        )

    def test_two_phase_all_linear_distributed(self, client):
        """All-linear global aggregate (COUNT*/SUM/AVG/COUNT(col)) over a
        partitioned table takes the two-phase path: a per-worker local partial,
        then ≤N partials combined on V0's owner. Checked against the recompute
        oracle across insert/update/delete churn spread over all partitions.
        Integer arithmetic is bit-exact, so two-phase == funnel == oracle with no
        tolerance. Run under GNITZ_WORKERS=1 and =4 — passing at both pins the
        weight-exact partial split (the funnel==two_phase identity)."""
        sn = "g2p_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total, "
                "AVG(a) AS av, COUNT(b) AS nb FROM t",
                schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            # Eligible → two-phase (2 REDUCE nodes), even at W=1.
            assert self._count_reduce_nodes(client, vid) == 2, "all-linear integer global agg must be two-phase"
            project = ["cnt", "total", "av", "nb"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["a", "b"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["a", "b"], [],
                    [("cnt", "COUNT", None), ("total", "SUM", "a"),
                     ("av", "AVG", "a"), ("nb", "COUNT", "b")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            check("empty")
            # 40 rows with varied PKs (spread across partitions); b NULL on a third.
            rows = [{"pk": i, "a": (i * 7) % 50, "b": (None if i % 3 == 0 else i)} for i in range(1, 41)]
            client.execute_sql(
                "INSERT INTO t VALUES " + ", ".join(
                    f"({r['pk']}, {r['a']}, {'NULL' if r['b'] is None else r['b']})" for r in rows),
                schema_name=sn)
            oracle.apply_insert(t_state, "pk", rows)
            check("after-insert")
            client.execute_sql("UPDATE t SET a = 100 WHERE pk = 5", schema_name=sn)
            oracle.apply_update(t_state, "pk", 5, {"a": 100})
            # Un-null a previously-NULL b (pk 9, 9 % 3 == 0) → COUNT(b) rises.
            client.execute_sql("UPDATE t SET b = 9 WHERE pk = 9", schema_name=sn)
            oracle.apply_update(t_state, "pk", 9, {"b": 9})
            check("after-update")
            del_pks = list(range(1, 41, 2))  # odd PKs, across partitions
            client.execute_sql(
                f"DELETE FROM t WHERE pk IN ({', '.join(map(str, del_pks))})", schema_name=sn)
            oracle.apply_delete(t_state, "pk", del_pks)
            check("after-delete")
            remaining = [r["pk"] for r in rows if r["pk"] not in del_pks]
            client.execute_sql("DELETE FROM t", schema_name=sn)
            oracle.apply_delete(t_state, "pk", remaining)
            check("after-delete-all")  # back to the ground row
            client.execute_sql("INSERT INTO t VALUES (99, 3, 4)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 99, "a": 3, "b": 4}])
            check("after-refill")
        finally:
            client.drop_schema(sn)

    def test_two_phase_fresh_all_null_count_col_is_zero(self, client):
        """Two-phase COUNT(col) over a non-empty source whose column was NEVER
        non-null renders 0: each worker's partial COUNT_NON_NULL is untouched and
        the combine SumZero of them grounds to 0. This is SQL-correct and now agrees
        with the funnel/grouped reduce, which also render an untouched COUNT(col) as
        0 (the empty_renders_zero family). The column MUST be fresh all-NULL —
        inserting non-null rows then nulling them leaves a concrete 0 on every path
        and tests nothing."""
        sn = "g2n_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NULL)",
                schema_name=sn)
            client.execute_sql("CREATE VIEW v AS SELECT COUNT(a) AS c FROM t", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            assert self._count_reduce_nodes(client, vid) == 2, "COUNT(col) global agg must be two-phase"
            client.execute_sql(
                "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, NULL), (4, NULL)", schema_name=sn)
            assert self._one_row(client, vid)["c"] == 0, "fresh all-NULL COUNT(col) is 0 under two-phase"
        finally:
            client.drop_schema(sn)

    def test_two_phase_eligibility_integer_vs_float(self, client):
        """Eligibility predicate: integer SUM/AVG global aggregates compile to the
        two-phase shape (2 REDUCE nodes); a float SUM — and AVG over a float, whose
        SUM component is float — are excluded (non-associative IEEE addition would
        make the result worker-count-dependent) and keep the single funnel reduce
        (1 REDUCE node)."""
        sn = "g2f_" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, ai BIGINT NOT NULL, af DOUBLE NOT NULL)",
                schema_name=sn)
            client.execute_sql("CREATE VIEW vsi AS SELECT SUM(ai) AS s FROM t", schema_name=sn)
            client.execute_sql("CREATE VIEW vai AS SELECT AVG(ai) AS s FROM t", schema_name=sn)
            client.execute_sql("CREATE VIEW vsf AS SELECT SUM(af) AS s FROM t", schema_name=sn)
            client.execute_sql("CREATE VIEW vaf AS SELECT AVG(af) AS s FROM t", schema_name=sn)
            vsi = client.resolve_table(sn, "vsi")[0]
            vai = client.resolve_table(sn, "vai")[0]
            vsf = client.resolve_table(sn, "vsf")[0]
            vaf = client.resolve_table(sn, "vaf")[0]
            assert self._count_reduce_nodes(client, vsi) == 2, "integer SUM is two-phase"
            assert self._count_reduce_nodes(client, vai) == 2, "AVG over integer is two-phase"
            assert self._count_reduce_nodes(client, vsf) == 1, "float SUM keeps the funnel"
            assert self._count_reduce_nodes(client, vaf) == 1, "AVG over float keeps the funnel"
        finally:
            client.drop_schema(sn)


class TestCombinedValueIndex:
    """The combined AggValueIndex serves every MIN/MAX of a reduce (grouped or
    global) from one table keyed `group ‖ ordinal ‖ av`, replacing the GroupIndex
    whose PK-keyed trace re-read leaked foreign groups' rows into a group's
    extremes. These exercise the planner→compiler→engine path end to end."""

    def test_foreign_group_fanout_join(self, client):
        """A fan-out join feeds `GROUP BY g` over a NON-unique-PK reduce input: one
        `fact` row joins several `dim` rows carrying different `g`, so the same
        join-output PK spans multiple groups — the exact shape the removed GI
        over-read corrupted. The combined index isolates each group by its key
        prefix, so each group's MIN/MAX excludes the others'. Run incrementally
        (insert after the view exists) so the per-tick recompute path is taken."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE fact (fid BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE dim (did BIGINT NOT NULL PRIMARY KEY, fkey BIGINT NOT NULL, "
                "g INT NOT NULL, x BIGINT NOT NULL, y BIGINT NOT NULL)",
                schema_name=sn)
            # j: one fact row fans out to many dim rows with different g, so the
            # join-output PK (fact.fid) repeats across groups.
            client.execute_sql(
                "CREATE VIEW j AS SELECT fact.fid AS fid, dim.g AS g, dim.x AS x, dim.y AS y "
                "FROM fact JOIN dim ON fact.fid = dim.fkey",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW agg AS SELECT g, MIN(x) AS lo, MAX(y) AS hi, COUNT(*) AS c "
                "FROM j GROUP BY g",
                schema_name=sn)
            vid = client.resolve_table(sn, "agg")[0]

            client.execute_sql("INSERT INTO fact VALUES (1), (2)", schema_name=sn)
            # fact 1 → g=10(x=5,y=100), g=20(x=7,y=200); fact 2 → g=10(x=3,y=50), g=20(x=9,y=300).
            client.execute_sql(
                "INSERT INTO dim VALUES "
                "(1, 1, 10, 5, 100), (2, 1, 20, 7, 200), "
                "(3, 2, 10, 3, 50), (4, 2, 20, 9, 300)",
                schema_name=sn)

            by_g = {r["g"]: r for r in client.scan(vid) if r.weight > 0}
            assert (by_g[10]["lo"], by_g[10]["hi"], by_g[10]["c"]) == (3, 100, 2), (
                f"group 10 extremes must exclude group 20's rows; got {by_g[10]}")
            assert (by_g[20]["lo"], by_g[20]["hi"], by_g[20]["c"]) == (7, 300, 2), (
                f"group 20 extremes must exclude group 10's rows; got {by_g[20]}")

            # Incremental: delete fact 2's group-10 contributor (dim did=3, x=3).
            # Group 10's MIN must recompute to 5 from the surviving group-10 row —
            # never pulling group 20's smaller-keyed entries.
            client.execute_sql("DELETE FROM dim WHERE did = 3", schema_name=sn)
            by_g = {r["g"]: r for r in client.scan(vid) if r.weight > 0}
            assert (by_g[10]["lo"], by_g[10]["hi"], by_g[10]["c"]) == (5, 100, 1), by_g[10]
            assert (by_g[20]["lo"], by_g[20]["hi"], by_g[20]["c"]) == (7, 300, 2), by_g[20]
        finally:
            client.drop_schema(sn)

    def test_foreign_group_fanout_join_backfill(self, client):
        """Same fan-out foreign-group shape, but the grouped view is CREATEd over a
        PRE-POPULATED join (combined-index backfill / from-scratch derivation),
        not an incremental tick."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE fact (fid BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE dim (did BIGINT NOT NULL PRIMARY KEY, fkey BIGINT NOT NULL, "
                "g INT NOT NULL, x BIGINT NOT NULL, y BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql("INSERT INTO fact VALUES (1), (2)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO dim VALUES "
                "(1, 1, 10, 5, 100), (2, 1, 20, 7, 200), "
                "(3, 2, 10, 3, 50), (4, 2, 20, 9, 300)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW j AS SELECT fact.fid AS fid, dim.g AS g, dim.x AS x, dim.y AS y "
                "FROM fact JOIN dim ON fact.fid = dim.fkey",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW agg AS SELECT g, MIN(x) AS lo, MAX(y) AS hi, COUNT(*) AS c "
                "FROM j GROUP BY g",
                schema_name=sn)
            vid = client.resolve_table(sn, "agg")[0]
            by_g = {r["g"]: r for r in client.scan(vid) if r.weight > 0}
            assert (by_g[10]["lo"], by_g[10]["hi"], by_g[10]["c"]) == (3, 100, 2), by_g[10]
            assert (by_g[20]["lo"], by_g[20]["hi"], by_g[20]["c"]) == (7, 300, 2), by_g[20]
        finally:
            client.drop_schema(sn)

    def test_lone_nullable_min_all_null_group(self, client):
        """A lone `SELECT g, MIN(a) GROUP BY g` over a NULLABLE `a` — no user
        COUNT(*). The planner appends a hidden cardinality COUNT and the combined
        index carries the nullable MIN, so an all-NULL group is PRESENT with
        MIN=NULL (matching the differential oracle), not silently dropped. Checked
        from-scratch after each epoch: a new all-NULL group appears; a mixed
        group's last non-NULL retraction leaves it surviving as NULL; retracting
        the remaining NULL row removes it."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, a BIGINT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, MIN(a) AS lo FROM t GROUP BY g", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["g", "lo"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["g", "a"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["g", "a"], ["g"], [("lo", "MIN", "a")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            # g=10 mixed {a=5, a=NULL}; g=20 entirely all-NULL {a=NULL}.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 5), (2, 10, NULL), (3, 20, NULL)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 10, "a": 5}, {"pk": 2, "g": 10, "a": None},
                {"pk": 3, "g": 20, "a": None}])
            check("insert: all-NULL group 20 must appear as (20, NULL)")

            # Retract g=10's only non-NULL → survives via the NULL row as (10, NULL).
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [1])
            check("retract last non-NULL: group 10 survives as (10, NULL)")

            # Retract g=10's remaining NULL row → group 10 disappears.
            client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [2])
            check("retract remaining NULL: group 10 gone")
        finally:
            client.drop_schema(sn)

    def test_lone_min_nonnull_combined_index(self, client):
        """Non-nullable lone `MIN(a) GROUP BY g` over the combined-index common
        path: insert, retract the current MIN (forces a post-state index re-seek),
        re-insert below. Differential oracle each epoch. Runs at the suite's worker
        count (W=4 under `make e2e`), pinning the path byte-for-byte correct."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, a BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT g, MIN(a) AS lo FROM t GROUP BY g", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            project = ["g", "lo"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["g", "a"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["g", "a"], ["g"], [("lo", "MIN", "a")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            client.execute_sql(
                "INSERT INTO t VALUES (1, 10, 5), (2, 10, 3), (3, 10, 9), (4, 20, 7)",
                schema_name=sn)
            oracle.apply_insert(t_state, "pk", [
                {"pk": 1, "g": 10, "a": 5}, {"pk": 2, "g": 10, "a": 3},
                {"pk": 3, "g": 10, "a": 9}, {"pk": 4, "g": 20, "a": 7}])
            check("insert")

            # Retract g=10's current MIN (a=3) → recompute to 5 from the index.
            client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [2])
            check("retract-min")

            # Insert below the current MIN → drops to 1.
            client.execute_sql("INSERT INTO t VALUES (5, 10, 1)", schema_name=sn)
            oracle.apply_insert(t_state, "pk", [{"pk": 5, "g": 10, "a": 1}])
            check("reinsert-lower")
        finally:
            client.drop_schema(sn)


class TestFinalizeMapShapes:
    """The post-reduce finalize (AVG, nullable-SUM) is a columnar MAP over the raw
    reduce output. Two of its column shapes are exercised nowhere else:

      * a **German-string group column** copied through the finalize MAP — short
        (inline) and long (heap-backed) values must survive the blob relocate /
        passthrough, not just the raw reduce emit;
      * a **compound synthetic `_group_pk`** (multi-column GROUP BY) under a
        finalize, whose PK region the MAP inherits verbatim.

    Both are checked against the from-scratch oracle across insert, retraction,
    and group-emptying ticks."""

    def test_string_group_by_avg_and_nullable_sum(self, client):
        """GROUP BY a TEXT column with AVG + a SUM over a NULLABLE column. The
        finalize MAP copies the string group column (relocating its blob heap) and
        computes AVG / nullfill-SUM in the same pass. Mixes short inline strings
        with strings past the 12-byte German-string inline limit so both the inline
        and heap-backed cells cross the finalize."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t ("
                "  pk BIGINT NOT NULL PRIMARY KEY,"
                "  s TEXT NOT NULL,"
                "  x BIGINT NOT NULL,"
                "  y BIGINT"          # nullable → NullfillSum finalize
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT s, AVG(x) AS avg_x, SUM(y) AS sum_y "
                "FROM t GROUP BY s",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            project = ["s", "avg_x", "sum_y"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["s", "x", "y"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["s", "x", "y"], ["s"],
                    [("avg_x", "AVG", "x"), ("sum_y", "SUM", "y")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            # "ab" is inline (≤12 bytes); the other two exceed it and live on the
            # blob heap. Group "long-heap-backed-key-2" is all-NULL in y → SUM NULL.
            short, long1, long2 = "ab", "long-heap-backed-key-1", "long-heap-backed-key-2"
            rows = [
                {"pk": 1, "s": short, "x": 2, "y": 10},
                {"pk": 2, "s": short, "x": 5, "y": None},
                {"pk": 3, "s": long1, "x": 7, "y": 3},
                {"pk": 4, "s": long1, "x": 8, "y": 4},
                {"pk": 5, "s": long2, "x": 9, "y": None},
            ]
            client.execute_sql(
                "INSERT INTO t VALUES "
                + ", ".join(
                    f"({r['pk']}, '{r['s']}', {r['x']}, "
                    f"{'NULL' if r['y'] is None else r['y']})"
                    for r in rows
                ),
                schema_name=sn,
            )
            oracle.apply_insert(t_state, "pk", rows)
            check("after-insert")

            # Retract one row of the inline group: AVG(x) 3.5 → 2.0, SUM(y) stays 10.
            client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [2])
            check("after-retract-inline-group")

            # Retract the last non-NULL y of long1 → its SUM(y) must become NULL,
            # while AVG(x) recomputes over the survivor.
            client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [3])
            check("after-retract-last-nonnull-y")

            # Empty the all-NULL heap-backed group entirely → it must vanish.
            client.execute_sql("DELETE FROM t WHERE pk = 5", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [5])
            check("after-empty-long2")

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_multi_col_group_by_avg(self, client):
        """GROUP BY a, b with AVG: the compound group key folds into a synthetic
        `_group_pk` the finalize MAP inherits verbatim, and both group-exemplar
        columns are copied through it into payload slots."""
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
                "CREATE VIEW v AS SELECT a, b, COUNT(*) AS n, AVG(val) AS av "
                "FROM t GROUP BY a, b",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            project = ["a", "b", "n", "av"]
            t_state = {}

            def check(ctx):
                base = oracle.oracle_filter_project(t_state, None, ["a", "b", "val"])
                exp, cols = oracle.oracle_groupby_aggregate(
                    base, ["a", "b", "val"], ["a", "b"],
                    [("n", "COUNT", None), ("av", "AVG", "val")])
                assert cols == project
                oracle.assert_view_matches(client, vid, project, exp, ctx=ctx)

            # 12 distinct (a, b) keys, 2 rows each → AVG is an exact .0 or .5.
            rows = []
            pk = 0
            for a in range(3):
                for b in range(4):
                    for k in range(2):
                        pk += 1
                        rows.append({"pk": pk, "a": a, "b": b, "val": a * 10 + b + k})
            client.execute_sql(
                "INSERT INTO t VALUES "
                + ", ".join(f"({r['pk']}, {r['a']}, {r['b']}, {r['val']})" for r in rows),
                schema_name=sn,
            )
            oracle.apply_insert(t_state, "pk", rows)
            check("after-insert")

            # Retract one row of (a=1, b=2): its AVG shifts, others unchanged.
            target = next(r for r in rows if r["a"] == 1 and r["b"] == 2)
            client.execute_sql(
                f"DELETE FROM t WHERE pk = {target['pk']}", schema_name=sn)
            oracle.apply_delete(t_state, "pk", [target["pk"]])
            check("after-retract-one")

            # Empty (a=0, b=0) entirely → that group must vanish.
            gone = [r["pk"] for r in rows if r["a"] == 0 and r["b"] == 0]
            client.execute_sql(
                f"DELETE FROM t WHERE pk IN ({', '.join(str(p) for p in gone)})",
                schema_name=sn,
            )
            oracle.apply_delete(t_state, "pk", gone)
            check("after-empty-group")

            client.execute_sql("DROP VIEW v", schema_name=sn)
            client.execute_sql("DROP TABLE t", schema_name=sn)
        finally:
            client.drop_schema(sn)
