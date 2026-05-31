"""E2E tests: a NULL equi-join key must match nothing (SQL three-valued logic).

A NULL join key must never produce a match — not against a real 0 / "" key on
the other side, and not against another NULL key. Before the fix, map_reindex
promoted a NULL integer key to synthetic PK 0 and a NULL string to the
empty-content hash 0, so NULL keys collided with real 0 / "" keys and with each
other.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_null_join_key.py -v --tb=short
"""
import os
import random
import gnitz

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, tables=None, views=None):
    for name in (views or []):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in (tables or []):
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _scan_dicts(client, tid):
    return [r._asdict() for r in client.scan(tid) if r.weight > 0]


class TestNullJoinKey:
    def test_inner_join_null_key_no_match(self, client):
        """INNER JOIN: a NULL key matches neither a real 0 nor another NULL.

        Reproduction from the bug report — must yield exactly {(2,'zero')}.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT l.id, r.name FROM l JOIN r ON l.fk = r.k",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO r VALUES (0, 'zero')", schema_name=sn)
            client.execute_sql("INSERT INTO l VALUES (1, NULL), (2, 0)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            # Only id=2 (fk=0) matches r.k=0; id=1 (fk=NULL) must not match.
            assert len(rows) == 1, f"expected 1 row, got {rows}"
            assert rows[0]["id"] == 2
            assert rows[0]["name"] == "zero"
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_inner_join_null_keys_both_sides_no_cross_match(self, client):
        """INNER JOIN: NULL keys on both sides must not match each other."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, rk BIGINT NULL, name VARCHAR(50) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT l.id, r.name FROM l JOIN r ON l.fk = r.rk",
                schema_name=sn,
            )
            # Two NULL-key rows on the left, two NULL-key rows on the right, plus
            # one matching real-key pair (7).
            client.execute_sql(
                "INSERT INTO r VALUES (100, NULL, 'rnull1'), (101, NULL, 'rnull2'), (102, 7, 'seven')",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO l VALUES (1, NULL), (2, NULL), (3, 7)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = _scan_dicts(client, vid)
            # Only (3, 'seven') matches; no NULL pairs cross-match.
            assert len(rows) == 1, f"expected 1 row, got {rows}"
            assert rows[0]["id"] == 3
            assert rows[0]["name"] == "seven"
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_left_join_null_left_key_emits_null_right(self, client):
        """LEFT JOIN: a NULL preserved-side key is emitted once with NULL right,
        and does not match a right 0 key."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT l.id, r.name FROM l LEFT JOIN r ON l.fk = r.k",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO r VALUES (0, 'zero')", schema_name=sn)
            client.execute_sql("INSERT INTO l VALUES (1, NULL), (2, 0)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = sorted(_scan_dicts(client, vid), key=lambda r: r["id"])
            assert len(rows) == 2, f"expected 2 rows, got {rows}"
            # id=1 (fk=NULL): preserved with NULL right name, NOT matched to 0.
            assert rows[0]["id"] == 1
            assert rows[0]["name"] is None
            # id=2 (fk=0): matches the real 0 key.
            assert rows[1]["id"] == 2
            assert rows[1]["name"] == "zero"
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_left_join_null_and_zero_collision_consolidation(self, client):
        """LEFT JOIN with both a NULL-key and a real-0-key left row sharing
        synthetic _join_pk = 0: they must not merge or cancel, the 0 row matches,
        and the NULL row emits (id, NULL) exactly once."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT l.id, r.name FROM l LEFT JOIN r ON l.fk = r.k",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO r VALUES (0, 'zero')", schema_name=sn)
            # id=1 has NULL fk, id=2 has real 0 fk — both reindex to _join_pk = 0.
            client.execute_sql("INSERT INTO l VALUES (1, NULL), (2, 0)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = sorted(_scan_dicts(client, vid), key=lambda r: r["id"])
            # Exactly two distinct output rows — no merge despite shared join PK.
            assert len(rows) == 2, f"expected 2 rows, got {rows}"
            assert rows[0]["id"] == 1 and rows[0]["name"] is None
            assert rows[1]["id"] == 2 and rows[1]["name"] == "zero"
            # Each must appear with weight exactly 1 (no double-emit, no cancel).
            full = [r for r in client.scan(vid) if r.weight != 0]
            weights = {r._asdict()["id"]: r.weight for r in full}
            assert weights == {1: 1, 2: 1}, f"unexpected weights: {weights}"
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_inner_join_nullable_string_key(self, client):
        """INNER JOIN on a nullable string key: NULL must not match the empty
        string '' on the other side."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, sk VARCHAR(50) NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, rk VARCHAR(50) NULL, name VARCHAR(50) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT l.id, r.name FROM l JOIN r ON l.sk = r.rk",
                schema_name=sn,
            )
            # Right side has an empty-string key.
            client.execute_sql(
                "INSERT INTO r VALUES (1, '', 'empty'), (2, 'x', 'ex')",
                schema_name=sn,
            )
            # Left: id=1 NULL key, id=2 empty-string key, id=3 'x'.
            client.execute_sql(
                "INSERT INTO l VALUES (1, NULL), (2, ''), (3, 'x')",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            rows = sorted(_scan_dicts(client, vid), key=lambda r: r["id"])
            # id=1 (NULL) must NOT match '' ; id=2 ('') matches 'empty';
            # id=3 ('x') matches 'ex'.
            assert len(rows) == 2, f"expected 2 rows, got {rows}"
            assert rows[0]["id"] == 2 and rows[0]["name"] == "empty"
            assert rows[1]["id"] == 3 and rows[1]["name"] == "ex"
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_inner_join_null_key_incremental_transitions(self, client):
        """INSERT → UPDATE (NULL → real) → re-NULL → DELETE: the IS NULL and
        IS NOT NULL branches are exact complements at every tick.

        Going back to NULL uses DELETE + INSERT because `UPDATE ... SET col = NULL`
        is not supported by the assignment expression compiler — orthogonal to
        the equi-join NULL-key path under test here.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT l.id, r.name FROM l JOIN r ON l.fk = r.k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]

            client.execute_sql("INSERT INTO r VALUES (5, 'five')", schema_name=sn)

            # Tick 1: insert with NULL key → no match.
            client.execute_sql("INSERT INTO l VALUES (1, NULL)", schema_name=sn)
            rows = _scan_dicts(client, vid)
            assert len(rows) == 0, f"NULL key should not match: {rows}"

            # Tick 2: update NULL → 5 → now matches.
            client.execute_sql("UPDATE l SET fk = 5 WHERE id = 1", schema_name=sn)
            rows = _scan_dicts(client, vid)
            assert len(rows) == 1, f"expected match after NULL→5: {rows}"
            assert rows[0]["name"] == "five"

            # Tick 3: real → NULL (via delete+insert) → match retracts cleanly.
            client.execute_sql("DELETE FROM l WHERE id = 1", schema_name=sn)
            client.execute_sql("INSERT INTO l VALUES (1, NULL)", schema_name=sn)
            rows = _scan_dicts(client, vid)
            assert len(rows) == 0, f"expected no match after real→NULL: {rows}"

            # Tick 4: delete the NULL-key row → still empty, no stray weights.
            client.execute_sql("DELETE FROM l WHERE id = 1", schema_name=sn)
            full = [r for r in client.scan(vid) if r.weight != 0]
            assert len(full) == 0, f"expected no rows after delete: {full}"
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])

    def test_left_join_null_left_key_incremental_transitions(self, client):
        """LEFT JOIN: toggling the preserved-side key NULL↔real keeps the
        bypass branch and the match branch exact complements (no double-emit)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, name VARCHAR(50) NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT l.id, r.name FROM l LEFT JOIN r ON l.fk = r.k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO r VALUES (5, 'five')", schema_name=sn)

            # Insert NULL-key left row → emitted once with NULL right.
            client.execute_sql("INSERT INTO l VALUES (1, NULL)", schema_name=sn)
            rows = [r._asdict() for r in client.scan(vid) if r.weight != 0]
            assert len(rows) == 1 and rows[0]["name"] is None, rows

            # NULL → 5: bypass row retracts, match row appears.
            client.execute_sql("UPDATE l SET fk = 5 WHERE id = 1", schema_name=sn)
            rows = [r._asdict() for r in client.scan(vid) if r.weight != 0]
            assert len(rows) == 1 and rows[0]["name"] == "five", rows

            # 5 → NULL (via delete+insert): match retracts, bypass row reappears once.
            client.execute_sql("DELETE FROM l WHERE id = 1", schema_name=sn)
            client.execute_sql("INSERT INTO l VALUES (1, NULL)", schema_name=sn)
            rows = [r._asdict() for r in client.scan(vid) if r.weight != 0]
            assert len(rows) == 1 and rows[0]["name"] is None, rows
        finally:
            _cleanup(client, sn, tables=["l", "r"], views=["v"])
