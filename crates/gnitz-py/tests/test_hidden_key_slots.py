"""E2E: hidden key slots — a view's visible schema is what the SELECT projects.

Every SQL view emitter that fabricates a synthetic key (`_join_pk`, `_pair_pk`,
`_set_pk`, `_distinct_pk`, `_group_pk`, `_src_pk`) marks that column hidden, and
a simple view's auto-prepended unprojected source PK is hidden too. Hidden
columns are physical (they still key/route/sort/consolidate the view) but are
excluded from `SELECT *`, name resolution, duplicate-name checks, and client
rows. The `include_hidden=True` scan surfaces them for debugging.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_hidden_key_slots.py -v --tb=short
"""
import random

import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _field_names(row):
    """The row's *visible* field names (hidden key slots excluded)."""
    return list(row._asdict().keys())


def _live(client, vid, include_hidden=False):
    return [r for r in client.scan(vid, include_hidden=include_hidden) if r.weight > 0]


def _assert_no_synthetic(rows):
    """No presented column may be a hidden key slot (leading `_`)."""
    for r in rows:
        for name in _field_names(r):
            assert not name.startswith("_"), f"synthetic key leaked into row: {name} in {_field_names(r)}"


# ---------------------------------------------------------------------------
# `SELECT *` over each synthetic-key shape omits the key column
# ---------------------------------------------------------------------------

class TestSelectStarHidesKey:
    def _ab(self, client, sn):
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL)",
            schema_name=sn,
        )

    def test_equi_join_hides_join_pk(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._ab(client, sn)
            client.execute_sql(
                "CREATE VIEW jv AS SELECT a.av AS av, b.bv AS bv "
                "FROM a JOIN b ON a.k = b.k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "jv")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 7, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 7, 200)", schema_name=sn)
            rows = _live(client, vid)
            assert len(rows) == 1, rows
            assert set(_field_names(rows[0])) == {"av", "bv"}
            _assert_no_synthetic(rows)
            assert (rows[0]["av"], rows[0]["bv"]) == (100, 200)
        finally:
            client.drop_schema(sn)

    def test_range_join_hides_pair_pk(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._ab(client, sn)
            client.execute_sql(
                "CREATE VIEW rv AS SELECT a.av AS av, b.bv AS bv "
                "FROM a JOIN b ON a.k < b.k",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "rv")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 5, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 9, 200)", schema_name=sn)
            rows = _live(client, vid)
            assert len(rows) == 1, rows
            assert set(_field_names(rows[0])) == {"av", "bv"}
            _assert_no_synthetic(rows)
        finally:
            client.drop_schema(sn)

    def test_exists_hides_join_pk(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._ab(client, sn)
            client.execute_sql(
                "CREATE VIEW ev AS SELECT av FROM a "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "ev")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 7, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 7, 200)", schema_name=sn)
            rows = _live(client, vid)
            assert len(rows) == 1, rows
            assert set(_field_names(rows[0])) == {"av"}
            _assert_no_synthetic(rows)
        finally:
            client.drop_schema(sn)

    def test_union_hides_set_pk(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW uv AS SELECT val FROM a UNION SELECT val FROM b", schema_name=sn)
            vid = client.resolve_table(sn, "uv")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 20)", schema_name=sn)
            rows = _live(client, vid)
            assert set(_field_names(rows[0])) == {"val"}
            _assert_no_synthetic(rows)
        finally:
            client.drop_schema(sn)

    def test_distinct_hides_distinct_pk(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW dv AS SELECT DISTINCT val FROM a", schema_name=sn)
            vid = client.resolve_table(sn, "dv")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 10), (3, 20)", schema_name=sn)
            rows = _live(client, vid)
            assert set(_field_names(rows[0])) == {"val"}
            _assert_no_synthetic(rows)
            assert sorted(r["val"] for r in rows) == [10, 20]
        finally:
            client.drop_schema(sn)

    def test_group_by_synthetic_hides_group_pk(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            # A STRING group key forces the synthetic `_group_pk` path (a natural
            # PK-eligible key would stay visible, which is correct and not the
            # case under test here).
            client.execute_sql(
                "CREATE TABLE orders (pk BIGINT NOT NULL PRIMARY KEY, category TEXT NOT NULL, amount BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW gv AS SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "gv")[0]
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 'x', 10), (2, 'x', 20), (3, 'y', 30)", schema_name=sn)
            rows = _live(client, vid)
            _assert_no_synthetic(rows)
            names = set(_field_names(rows[0]))
            assert names == {"category", "cnt"}, names
            got = {r["category"]: r["cnt"] for r in rows}
            assert got == {"x": 2, "y": 1}, got
        finally:
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Simple view: unprojected source PK is hidden and not name-resolvable
# ---------------------------------------------------------------------------

def test_simple_view_omits_unprojected_pk_and_downstream_cannot_name_it(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
        # `SELECT val` omits the PK `id`; it rides the view hidden.
        client.execute_sql("CREATE VIEW sv AS SELECT val FROM t", schema_name=sn)
        vid = client.resolve_table(sn, "sv")[0]
        client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=sn)
        rows = _live(client, vid)
        assert set(_field_names(rows[0])) == {"val"}
        _assert_no_synthetic(rows)

        # A downstream view naming the hidden PK by name fails cleanly.
        with pytest.raises(Exception) as ei:
            client.execute_sql("CREATE VIEW ds AS SELECT id FROM sv", schema_name=sn)
        assert "id" in str(ei.value).lower() or "not found" in str(ei.value).lower()
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Set-op over two join views dedups on projected content, not on `_join_pk`
# ---------------------------------------------------------------------------

def test_setop_over_two_join_views_dedups_on_content(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL)",
            schema_name=sn)
        for v in ("jv1", "jv2"):
            client.execute_sql(
                f"CREATE VIEW {v} AS SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k",
                schema_name=sn)
        # UNION ALL keeps both copies (weight 2); the upstream `_join_pk` must NOT
        # keep the two structurally-identical rows distinct.
        client.execute_sql(
            "CREATE VIEW ua AS SELECT * FROM jv1 UNION ALL SELECT * FROM jv2", schema_name=sn)
        # UNION (distinct) collapses them to one.
        client.execute_sql(
            "CREATE VIEW ud AS SELECT * FROM jv1 UNION SELECT * FROM jv2", schema_name=sn)
        ua = client.resolve_table(sn, "ua")[0]
        ud = client.resolve_table(sn, "ud")[0]
        client.execute_sql("INSERT INTO a VALUES (1, 7, 100)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (1, 7, 200)", schema_name=sn)

        all_rows = _live(client, ua)
        _assert_no_synthetic(all_rows)
        assert sum(r.weight for r in all_rows if (r["av"], r["bv"]) == (100, 200)) == 2

        dist_rows = _live(client, ud)
        _assert_no_synthetic(dist_rows)
        got = [(r["av"], r["bv"], r.weight) for r in dist_rows]
        assert got == [(100, 200, 1)], got
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# include_hidden=True surfaces the key slots with correctly-decoded values
# ---------------------------------------------------------------------------

def test_include_hidden_surfaces_join_pk(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE VIEW jv AS SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k",
            schema_name=sn)
        vid = client.resolve_table(sn, "jv")[0]
        client.execute_sql("INSERT INTO a VALUES (1, 7, 100)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (1, 7, 200)", schema_name=sn)

        # Default: hidden.
        vis = _live(client, vid)
        assert "_join_pk" not in _field_names(vis[0])

        # include_hidden: the synthetic key surfaces at its physical slot with the
        # decoded join-key value (7).
        raw = _live(client, vid, include_hidden=True)
        assert len(raw) == 1, raw
        names = _field_names(raw[0])
        assert names[0] == "_join_pk", names
        assert raw[0]["_join_pk"] == 7
        assert (raw[0]["av"], raw[0]["bv"]) == (100, 200)
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# View-over-view cascade where every layer's key is hidden
# ---------------------------------------------------------------------------

def test_view_over_view_cascade_hidden_keys(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE VIEW l1 AS SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k",
            schema_name=sn)
        # `SELECT *` over the hidden-keyed join view must not re-admit `_join_pk`.
        client.execute_sql("CREATE VIEW l2 AS SELECT * FROM l1", schema_name=sn)
        client.execute_sql("CREATE VIEW l3 AS SELECT * FROM l2", schema_name=sn)
        vid = client.resolve_table(sn, "l3")[0]
        client.execute_sql("INSERT INTO a VALUES (1, 7, 100)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (1, 7, 200)", schema_name=sn)
        rows = _live(client, vid)
        assert len(rows) == 1, rows
        assert set(_field_names(rows[0])) == {"av", "bv"}
        _assert_no_synthetic(rows)
        assert (rows[0]["av"], rows[0]["bv"]) == (100, 200)
    finally:
        client.drop_schema(sn)
