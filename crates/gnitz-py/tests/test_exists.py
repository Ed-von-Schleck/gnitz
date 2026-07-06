"""E2E tests for [NOT] EXISTS / x [NOT] IN (SELECT ...) views.

Every scenario asserts weights, not just row presence: the semi-join must emit
w_A * [S>0] and the anti-join w_A * [S=0] — a second inner match must never
double an outer row's weight.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_exists.py -v --tb=short
"""
import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, *names):
    """Drop each name as a view or table (whichever it is), then the schema."""
    for name in names:
        for stmt in (f"DROP VIEW {name}", f"DROP TABLE {name}"):
            try:
                client.execute_sql(stmt, schema_name=sn)
                break
            except Exception:
                pass
    client.drop_schema(sn)


def _weights(client, vid, col):
    """{col_value: net_weight} over the view's positive-weight rows."""
    out = {}
    for row in client.scan(vid):
        if row.weight == 0:
            continue
        out[getattr(row, col)] = out.get(getattr(row, col), 0) + row.weight
    return {k: w for k, w in out.items() if w != 0}


_CREATE_A = "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)"
_CREATE_B = "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)"


class TestEquiExists:
    def _setup(self, client, sn):
        client.execute_sql(_CREATE_A, schema_name=sn)
        client.execute_sql(_CREATE_B, schema_name=sn)

    def test_exists_incremental_membership_flips(self, client):
        """Insert first inner match -> outer row appears in EXISTS and leaves
        NOT EXISTS; a second match never doubles the weight; retracting down to
        zero matches flips back."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW semi AS SELECT v FROM a "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW anti AS SELECT v FROM a "
                "WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            semi = client.resolve_table(sn, "semi")[0]
            anti = client.resolve_table(sn, "anti")[0]

            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            assert _weights(client, semi, "v") == {}
            assert _weights(client, anti, "v") == {100: 1, 200: 1}

            # First match for k=10.
            client.execute_sql("INSERT INTO b VALUES (1, 10, 7)", schema_name=sn)
            assert _weights(client, semi, "v") == {100: 1}
            assert _weights(client, anti, "v") == {200: 1}

            # Second match for the same key: weight must STAY 1 (w_A * [S>0]).
            client.execute_sql("INSERT INTO b VALUES (2, 10, 8)", schema_name=sn)
            assert _weights(client, semi, "v") == {100: 1}
            assert _weights(client, anti, "v") == {200: 1}

            # Retract one of two matches: still matched.
            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            assert _weights(client, semi, "v") == {100: 1}

            # Retract the last match: flips back.
            client.execute_sql("DELETE FROM b WHERE id = 2", schema_name=sn)
            assert _weights(client, semi, "v") == {}
            assert _weights(client, anti, "v") == {100: 1, 200: 1}
        finally:
            _cleanup(client, sn, "semi", "anti", "a", "b")

    def test_in_and_not_in_subquery(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW vin AS SELECT v FROM a WHERE k IN (SELECT k FROM b)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW vnotin AS SELECT v FROM a WHERE k NOT IN (SELECT k FROM b)",
                schema_name=sn,
            )
            vin = client.resolve_table(sn, "vin")[0]
            vnotin = client.resolve_table(sn, "vnotin")[0]

            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 20, 5)", schema_name=sn)
            assert _weights(client, vin, "v") == {200: 1}
            assert _weights(client, vnotin, "v") == {100: 1}

            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            assert _weights(client, vin, "v") == {}
            assert _weights(client, vnotin, "v") == {100: 1, 200: 1}
        finally:
            _cleanup(client, sn, "vin", "vnotin", "a", "b")

    def test_exists_backfill_over_populated_tables(self, client):
        """CREATE VIEW after the data exists: backfill must produce the same
        result as the incremental order."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)", schema_name=sn
            )
            client.execute_sql("INSERT INTO b VALUES (1, 10, 5), (2, 10, 6), (3, 30, 7)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW semi AS SELECT v FROM a "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW anti AS SELECT v FROM a "
                "WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            semi = client.resolve_table(sn, "semi")[0]
            anti = client.resolve_table(sn, "anti")[0]
            assert _weights(client, semi, "v") == {100: 1, 300: 1}
            assert _weights(client, anti, "v") == {200: 1}
        finally:
            _cleanup(client, sn, "semi", "anti", "a", "b")

    def test_weight_exact_over_bag_valued_outer(self, client):
        """A weight-2 outer row (binary push) stays weight-2 in the semi output
        even with several inner matches, and weight-2 in the anti output when
        unmatched."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            cols = [
                gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("k", gnitz.TypeCode.I64),
                gnitz.ColumnDef("v", gnitz.TypeCode.I64),
            ]
            schema = gnitz.Schema(cols)
            # unique_pk=False: a true bag table — upsert semantics would clamp
            # the weight-2 row this test is about.
            tid = client.create_table(sn, "bag", cols, unique_pk=False)
            client.execute_sql(_CREATE_B, schema_name=sn)
            client.execute_sql(
                "CREATE VIEW semi AS SELECT v FROM bag "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.k = bag.k)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW anti AS SELECT v FROM bag "
                "WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = bag.k)",
                schema_name=sn,
            )
            semi = client.resolve_table(sn, "semi")[0]
            anti = client.resolve_table(sn, "anti")[0]

            batch = gnitz.ZSetBatch(schema)
            batch.append(id=1, k=10, v=100, weight=2)
            client.push(tid, batch)
            assert _weights(client, anti, "v") == {100: 2}
            assert _weights(client, semi, "v") == {}

            # Two matches: semi carries the row at its true multiplicity 2 — not
            # 4 (weight-exactness of A - positive_part(A - pi_A(inner))).
            client.execute_sql("INSERT INTO b VALUES (1, 10, 5), (2, 10, 6)", schema_name=sn)
            assert _weights(client, semi, "v") == {100: 2}
            assert _weights(client, anti, "v") == {}

            client.execute_sql("DELETE FROM b WHERE id IN (1, 2)", schema_name=sn)
            assert _weights(client, semi, "v") == {}
            assert _weights(client, anti, "v") == {100: 2}
        finally:
            _cleanup(client, sn, "semi", "anti", "bag", "b")

    def test_null_correlation_keys_asymmetry(self, client):
        """A NULL outer key matches nothing: excluded from EXISTS, included in
        NOT EXISTS. A NULL inner key never matches anything."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, w BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW semi AS SELECT v FROM a "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW anti AS SELECT v FROM a "
                "WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            semi = client.resolve_table(sn, "semi")[0]
            anti = client.resolve_table(sn, "anti")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 100), (2, NULL, 200)", schema_name=sn
            )
            # A NULL-keyed inner row matches nothing — not even a NULL outer key.
            client.execute_sql("INSERT INTO b VALUES (1, NULL, 5)", schema_name=sn)
            assert _weights(client, semi, "v") == {}
            assert _weights(client, anti, "v") == {100: 1, 200: 1}

            client.execute_sql("INSERT INTO b VALUES (2, 10, 6)", schema_name=sn)
            assert _weights(client, semi, "v") == {100: 1}
            assert _weights(client, anti, "v") == {200: 1}
        finally:
            _cleanup(client, sn, "semi", "anti", "a", "b")

    def test_local_conjuncts_both_sides(self, client):
        """Outer local conjuncts filter A before everything; inner-only
        conjuncts pre-filter B so failing rows are not matches."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT v FROM a "
                "WHERE v > 100 AND EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w > 5)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)", schema_name=sn
            )
            # w = 5 fails the pre-filter: no match yet.
            client.execute_sql("INSERT INTO b VALUES (1, 10, 5)", schema_name=sn)
            assert _weights(client, vid, "v") == {}
            # w = 6 passes: only a-rows with v > 100 and k = 10 appear.
            client.execute_sql("INSERT INTO b VALUES (2, 10, 6)", schema_name=sn)
            assert _weights(client, vid, "v") == {200: 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_compound_and_cross_width_keys(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k1 INT NOT NULL, "
                "k2 BIGINT NOT NULL, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k1 BIGINT NOT NULL, "
                "k2 BIGINT NOT NULL)",
                schema_name=sn,
            )
            # k1 is INT vs BIGINT (cross-width promotion); two-column key.
            client.execute_sql(
                "CREATE VIEW v AS SELECT v FROM a WHERE EXISTS "
                "(SELECT 1 FROM b WHERE b.k1 = a.k1 AND b.k2 = a.k2)",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql(
                "INSERT INTO a VALUES (1, 7, 70, 100), (2, 7, 71, 200)", schema_name=sn
            )
            client.execute_sql("INSERT INTO b VALUES (1, 7, 70)", schema_name=sn)
            # Only the full (k1, k2) match counts.
            assert _weights(client, vid, "v") == {100: 1}
        finally:
            _cleanup(client, sn, "v", "a", "b")

    def test_select_star_identity_projection(self, client):
        """SELECT * takes the identity-projection skip (no output map): the
        view exposes the synthetic PK plus every outer column, for both the
        equi (no exchange) and band (output exchange) shapes."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW star_eq AS SELECT * FROM a "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW star_bd AS SELECT * FROM a "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w < a.v)",
                schema_name=sn,
            )
            eq = client.resolve_table(sn, "star_eq")[0]
            bd = client.resolve_table(sn, "star_bd")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 10, 50)", schema_name=sn)
            rows = [r for r in client.scan(eq) if r.weight > 0]
            assert [(r.id, r.k, r.v, r.weight) for r in rows] == [(1, 10, 100, 1)]
            rows = [r for r in client.scan(bd) if r.weight > 0]
            assert [(r.id, r.k, r.v, r.weight) for r in rows] == [(1, 10, 100, 1)]
        finally:
            _cleanup(client, sn, "star_eq", "star_bd", "a", "b")

    def test_not_in_nullable_rejected(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT)", schema_name=sn
            )
            client.execute_sql(_CREATE_B, schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="NOT NULL"):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT * FROM a WHERE k NOT IN (SELECT k FROM b)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "a", "b")

    def test_views_as_sources_and_chaining(self, client):
        """Outer and inner are themselves views; an EXISTS view chains under a
        further view; DROP + re-CREATE works."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE VIEW av AS SELECT * FROM a WHERE v > 0", schema_name=sn)
            client.execute_sql("CREATE VIEW bv AS SELECT * FROM b WHERE w > 0", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW semi AS SELECT v FROM av "
                "WHERE EXISTS (SELECT 1 FROM bv WHERE bv.k = av.k)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW over AS SELECT v FROM semi WHERE v >= 200", schema_name=sn
            )
            semi = client.resolve_table(sn, "semi")[0]
            over = client.resolve_table(sn, "over")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)", schema_name=sn
            )
            client.execute_sql("INSERT INTO b VALUES (1, 20, 5), (2, 30, 6)", schema_name=sn)
            assert _weights(client, semi, "v") == {200: 1, 300: 1}
            assert _weights(client, over, "v") == {200: 1, 300: 1}

            # DROP and re-CREATE with the opposite polarity.
            client.execute_sql("DROP VIEW over", schema_name=sn)
            client.execute_sql("DROP VIEW semi", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW semi AS SELECT v FROM av "
                "WHERE NOT EXISTS (SELECT 1 FROM bv WHERE bv.k = av.k)",
                schema_name=sn,
            )
            semi2 = client.resolve_table(sn, "semi")[0]
            assert _weights(client, semi2, "v") == {100: 1}
        finally:
            _cleanup(client, sn, "over", "semi", "av", "bv", "a", "b")


class TestBandExists:
    def test_band_exists_and_not_exists(self, client):
        """b.k = a.k AND b.t < a.t — matches flip as thresholds cross, weights
        stay exact with several matches."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "t BIGINT NOT NULL, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "t BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW semi AS SELECT v FROM a "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.t < a.t)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW anti AS SELECT v FROM a "
                "WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.t < a.t)",
                schema_name=sn,
            )
            semi = client.resolve_table(sn, "semi")[0]
            anti = client.resolve_table(sn, "anti")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 50, 100), (2, 10, 5, 200), (3, 20, 50, 300)",
                schema_name=sn,
            )
            assert _weights(client, anti, "v") == {100: 1, 200: 1, 300: 1}

            # b (k=10, t=40): below a1's t=50, not below a2's t=5, wrong k for a3.
            client.execute_sql("INSERT INTO b VALUES (1, 10, 40)", schema_name=sn)
            assert _weights(client, semi, "v") == {100: 1}
            assert _weights(client, anti, "v") == {200: 1, 300: 1}

            # Second match for a1 (t=45): weight must stay 1.
            client.execute_sql("INSERT INTO b VALUES (2, 10, 45)", schema_name=sn)
            assert _weights(client, semi, "v") == {100: 1}

            client.execute_sql("DELETE FROM b WHERE id IN (1, 2)", schema_name=sn)
            assert _weights(client, semi, "v") == {}
            assert _weights(client, anti, "v") == {100: 1, 200: 1, 300: 1}
        finally:
            _cleanup(client, sn, "semi", "anti", "a", "b")


class TestPureRangeExists:
    def _setup(self, client, sn, x_nullable=False):
        x_null = "" if x_nullable else " NOT NULL"
        client.execute_sql(
            f"CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT{x_null}, "
            "v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT)",
            schema_name=sn,
        )

    def test_pure_range_exists_threshold(self, client):
        """EXISTS(b.y < a.x) collapses to x > MIN(b.y); empty inner side means
        no matches; retracting the extremum re-derives the threshold."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW semi AS SELECT v FROM a "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.y < a.x)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW anti AS SELECT v FROM a "
                "WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y < a.x)",
                schema_name=sn,
            )
            semi = client.resolve_table(sn, "semi")[0]
            anti = client.resolve_table(sn, "anti")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn
            )
            # Empty inner side: every row is unmatched (A - empty = A).
            assert _weights(client, semi, "v") == {}
            assert _weights(client, anti, "v") == {100: 1, 200: 1}

            # y = 15: only x = 20 has a y below it.
            client.execute_sql("INSERT INTO b VALUES (1, 15)", schema_name=sn)
            assert _weights(client, semi, "v") == {200: 1}
            assert _weights(client, anti, "v") == {100: 1}

            # y = 5: now both match; weights stay 1 despite two candidate rows.
            client.execute_sql("INSERT INTO b VALUES (2, 5)", schema_name=sn)
            assert _weights(client, semi, "v") == {100: 1, 200: 1}
            assert _weights(client, anti, "v") == {}

            # Retract the minimum: threshold recomputes to 15.
            client.execute_sql("DELETE FROM b WHERE id = 2", schema_name=sn)
            assert _weights(client, semi, "v") == {200: 1}
            assert _weights(client, anti, "v") == {100: 1}

            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            assert _weights(client, semi, "v") == {}
            assert _weights(client, anti, "v") == {100: 1, 200: 1}
        finally:
            _cleanup(client, sn, "semi", "anti", "a", "b")

    def test_pure_range_all_null_inner_and_null_outer(self, client):
        """All-NULL inner range values behave like an empty inner side; a NULL
        outer range key is never matched (anti includes it, semi never does)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn, x_nullable=True)
            client.execute_sql(
                "CREATE VIEW semi AS SELECT v FROM a "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.y < a.x)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW anti AS SELECT v FROM a "
                "WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y < a.x)",
                schema_name=sn,
            )
            semi = client.resolve_table(sn, "semi")[0]
            anti = client.resolve_table(sn, "anti")[0]

            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 100), (2, NULL, 200)", schema_name=sn
            )
            # Inner side holds only a NULL range value: like empty.
            client.execute_sql("INSERT INTO b VALUES (1, NULL)", schema_name=sn)
            assert _weights(client, semi, "v") == {}
            assert _weights(client, anti, "v") == {100: 1, 200: 1}

            # A real y = 5 matches x = 10 but never the NULL x.
            client.execute_sql("INSERT INTO b VALUES (2, 5)", schema_name=sn)
            assert _weights(client, semi, "v") == {100: 1}
            assert _weights(client, anti, "v") == {200: 1}
        finally:
            _cleanup(client, sn, "semi", "anti", "a", "b")


def _flag_map(client, vid):
    """{id: flag} over positive-weight rows; asserts no id appears twice (a
    missing old-mark-row retraction would leave both flag=0 and flag=1 present)."""
    seen = {}
    for row in client.scan(vid):
        if row.weight <= 0:
            continue
        assert row.id not in seen, f"id {row.id} present twice — missing mark-flip retraction"
        seen[row.id] = row.flag
    return seen


def _idset(client, vid):
    """Set of ids in positive-weight rows."""
    return sorted(r.id for r in client.scan(vid) if r.weight > 0)


class TestMarkSubquery:
    """Exactly one EXISTS/IN subquery in an arbitrary boolean position, rewritten
    to a 0/1 mark that ordinary expression evaluation consumes."""

    def _setup(self, client, sn):
        client.execute_sql(_CREATE_A, schema_name=sn)  # a(id pk, k, v)
        client.execute_sql(_CREATE_B, schema_name=sn)  # b(id pk, k, w)

    def test_exists_under_or_flips_incrementally(self, client):
        """WHERE EXISTS(b.k=a.k) OR v = 100 — a row is kept if it matches OR v=100."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW mk AS SELECT id FROM a "
                "WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k) OR v = 100",
                schema_name=sn,
            )
            mk = client.resolve_table(sn, "mk")[0]
            # id=1: v=100 → kept via the OR even with no match. id=2: v=200, no match → excluded.
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            assert _idset(client, mk) == [1]
            # Add a match for a.k=20 (id=2) → now kept via EXISTS.
            client.execute_sql("INSERT INTO b VALUES (1, 20, 7)", schema_name=sn)
            assert _idset(client, mk) == [1, 2]
            # Retract the match → id=2 leaves again (v=200 != 100).
            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            assert _idset(client, mk) == [1]
        finally:
            _cleanup(client, sn, "mk", "a", "b")

    def test_select_star_mark_identity(self, client):
        """SELECT * under a mark WHERE — the identity projection over the outer
        columns (no _mark column materialized; the mark is consumed by the WHERE)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW mk AS SELECT * FROM a "
                "WHERE v = 100 OR EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
                schema_name=sn,
            )
            mk = client.resolve_table(sn, "mk")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            assert _idset(client, mk) == [1]
            client.execute_sql("INSERT INTO b VALUES (1, 20, 7)", schema_name=sn)
            assert _idset(client, mk) == [1, 2]
        finally:
            _cleanup(client, sn, "mk", "a", "b")

    def test_projected_exists_flag_flips(self, client):
        """SELECT id, EXISTS(...) AS flag — flag flips 0↔1 with no duplicate row
        (the old-mark row must be retracted when the new one is emitted)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW mk AS SELECT id, EXISTS (SELECT 1 FROM b WHERE b.k = a.k) AS flag FROM a",
                schema_name=sn,
            )
            mk = client.resolve_table(sn, "mk")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            assert _flag_map(client, mk) == {1: 0, 2: 0}
            # Match for a.k=10 → id=1 flag flips to 1; the flag=0 row must be gone.
            client.execute_sql("INSERT INTO b VALUES (1, 10, 7)", schema_name=sn)
            assert _flag_map(client, mk) == {1: 1, 2: 0}
            # Retract → flips back to 0.
            client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
            assert _flag_map(client, mk) == {1: 0, 2: 0}
        finally:
            _cleanup(client, sn, "mk", "a", "b")

    def test_case_over_exists(self, client):
        """CASE WHEN EXISTS(...) THEN v ELSE 0 END — computed over the mark."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW mk AS SELECT id, "
                "CASE WHEN EXISTS (SELECT 1 FROM b WHERE b.k = a.k) THEN v ELSE 0 END AS c FROM a",
                schema_name=sn,
            )
            mk = client.resolve_table(sn, "mk")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            got = {r.id: r.c for r in client.scan(mk) if r.weight > 0}
            assert got == {1: 0, 2: 0}
            client.execute_sql("INSERT INTO b VALUES (1, 10, 7)", schema_name=sn)
            got = {r.id: r.c for r in client.scan(mk) if r.weight > 0}
            assert got == {1: 100, 2: 0}  # id=1 matched → v; id=2 unmatched → 0
        finally:
            _cleanup(client, sn, "mk", "a", "b")

    def test_not_exists_and_under_not(self, client):
        """WHERE NOT (EXISTS(...) AND v > 150): keep if no match OR v<=150."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW mk AS SELECT id FROM a "
                "WHERE NOT (EXISTS (SELECT 1 FROM b WHERE b.k = a.k) AND v > 150)",
                schema_name=sn,
            )
            mk = client.resolve_table(sn, "mk")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            # No matches yet → NOT(0 AND ...) = TRUE → both kept.
            assert _idset(client, mk) == [1, 2]
            # Match both. id=1: v=100<=150 kept. id=2: v=200>150 AND match → NOT(TRUE)=excluded.
            client.execute_sql("INSERT INTO b VALUES (1, 10, 7), (2, 20, 8)", schema_name=sn)
            assert _idset(client, mk) == [1]
        finally:
            _cleanup(client, sn, "mk", "a", "b")

    def test_not_in_under_or_nonnullable(self, client):
        """WHERE v = 100 OR k NOT IN (SELECT k FROM b), all columns NOT NULL."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW mk AS SELECT id FROM a WHERE v = 100 OR k NOT IN (SELECT k FROM b)",
                schema_name=sn,
            )
            mk = client.resolve_table(sn, "mk")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (1, 20, 5)", schema_name=sn)
            # id=1: v=100 → kept. id=2: v=200, k=20 IN b → NOT IN false → excluded.
            assert _idset(client, mk) == [1]
        finally:
            _cleanup(client, sn, "mk", "a", "b")

    def test_band_correlation_mark(self, client):
        """Band correlation (eq prefix + range) in a mark position."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW mk AS SELECT id FROM a "
                "WHERE v = 100 OR EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w < a.v)",
                schema_name=sn,
            )
            mk = client.resolve_table(sn, "mk")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            # id=1: v=100 → kept via OR. id=2: no b yet → excluded.
            assert _idset(client, mk) == [1]
            # b with k=20, w=50 < a.v=200 → id=2 matches the band.
            client.execute_sql("INSERT INTO b VALUES (1, 20, 50)", schema_name=sn)
            assert _idset(client, mk) == [1, 2]
        finally:
            _cleanup(client, sn, "mk", "a", "b")

    def test_pure_range_correlation_mark(self, client):
        """Pure-range correlation (only a range conjunct) in a mark position."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW mk AS SELECT id FROM a "
                "WHERE v = 100 OR EXISTS (SELECT 1 FROM b WHERE b.w < a.v)",
                schema_name=sn,
            )
            mk = client.resolve_table(sn, "mk")[0]
            client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
            assert _idset(client, mk) == [1]  # only v=100 kept, no b rows
            # b.w=50 < both a.v (100, 200) → both match the range → both kept.
            client.execute_sql("INSERT INTO b VALUES (1, 99, 50)", schema_name=sn)
            assert _idset(client, mk) == [1, 2]
        finally:
            _cleanup(client, sn, "mk", "a", "b")

    def test_nullable_in_mark_position_rejected(self, client):
        """A nullable IN in a mark position is rejected regardless of polarity,
        while the positive top-level AND conjunct form keeps working."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, v BIGINT NOT NULL)",
                schema_name=sn,
            )  # a.k nullable
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
                schema_name=sn,
            )
            # Positive IN under OR — rejected (mark position, nullable operand).
            for stmt in (
                "CREATE VIEW m1 AS SELECT id FROM a WHERE v = 1 OR k IN (SELECT k FROM b)",
                "CREATE VIEW m2 AS SELECT id FROM a WHERE NOT (k IN (SELECT k FROM b))",
                "CREATE VIEW m3 AS SELECT id, k IN (SELECT k FROM b) AS f FROM a",
            ):
                with pytest.raises(Exception):
                    client.execute_sql(stmt, schema_name=sn)
            # The positive top-level AND conjunct form is still accepted (nullable k).
            client.execute_sql(
                "CREATE VIEW ok AS SELECT id FROM a WHERE k IN (SELECT k FROM b)",
                schema_name=sn,
            )
            assert client.resolve_table(sn, "ok")[0] is not None
        finally:
            _cleanup(client, sn, "m1", "m2", "m3", "ok", "a", "b")

    def test_two_subqueries_rejected(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            with pytest.raises(Exception) as ei:
                client.execute_sql(
                    "CREATE VIEW mk AS SELECT id FROM a WHERE "
                    "EXISTS (SELECT 1 FROM b WHERE b.k = a.k) OR "
                    "EXISTS (SELECT 1 FROM b WHERE b.w = a.v)",
                    schema_name=sn,
                )
            assert "at most one" in str(ei.value).lower() or "compose" in str(ei.value).lower()
        finally:
            _cleanup(client, sn, "mk", "a", "b")
