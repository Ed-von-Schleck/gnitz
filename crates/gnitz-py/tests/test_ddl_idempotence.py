"""E2E tests for the DDL clauses that decide what happens when the name is (or is
not) already taken: `CREATE OR REPLACE VIEW`, and `IF NOT EXISTS` / `IF EXISTS`
across tables, views and indexes.

Every one of these is a NAME test — no dialect compares the existing definition,
and gnitz could not: a view's catalog rows are its compiled circuit, never its
text.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_ddl_idempotence.py -v --tb=short
"""
from collections import Counter

import pytest
import gnitz
import _oracle as oracle
from _uid import uid as _uid


def _base(client, sn):
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)


class TestOrReplaceView:
    def test_replace_swaps_the_definition_and_backfills(self, client):
        """The replacement serves the NEW body's rows over the data already there,
        at weight 1 each — not the old body's, and not both."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=sn)
            first = client.resolve_table(sn, "v")[0]
            oracle.assert_view_matches(
                client, first, ["id", "a"], Counter({(1, 10): 1, (2, 20): 1}), "before replace"
            )

            client.execute_sql("CREATE OR REPLACE VIEW v AS SELECT id, b FROM t", schema_name=sn)
            second = client.resolve_table(sn, "v")[0]
            assert second != first, "a replacement takes a fresh view id"
            oracle.assert_view_matches(
                client, second, ["id", "b"], Counter({(1, 100): 1, (2, 200): 1}), "after replace"
            )

            # Still maintained: a push after the replace reaches the NEW body.
            client.execute_sql("INSERT INTO t VALUES (3, 30, 300)", schema_name=sn)
            oracle.assert_view_matches(
                client,
                second,
                ["id", "b"],
                Counter({(1, 100): 1, (2, 200): 1, (3, 300): 1}),
                "after replace + insert",
            )
        finally:
            client.drop_schema(sn)

    def test_replace_over_a_free_name_is_a_plain_create(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql("CREATE OR REPLACE VIEW fresh AS SELECT id, a FROM t", schema_name=sn)
            vid = client.resolve_table(sn, "fresh")[0]
            oracle.assert_view_matches(client, vid, ["id", "a"], Counter({(1, 10): 1, (2, 20): 1}))
        finally:
            client.drop_schema(sn)

    def test_replace_refuses_a_table_and_a_dependent_view(self, client):
        """Two refusals with different reasons: a table is the wrong kind, and a
        view with dependents is RESTRICT — the replacement takes a fresh id, so
        retiring the old one would strand every circuit that scans it."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            with pytest.raises(gnitz.GnitzError, match="is a table; CREATE OR REPLACE VIEW requires a view"):
                client.execute_sql("CREATE OR REPLACE VIEW t AS SELECT id FROM t", schema_name=sn)

            client.execute_sql("CREATE VIEW base AS SELECT id, a FROM t", schema_name=sn)
            client.execute_sql("CREATE VIEW dep AS SELECT id FROM base", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="View dependency"):
                client.execute_sql("CREATE OR REPLACE VIEW base AS SELECT id, b FROM t", schema_name=sn)
            # Nothing was torn down: the old definition still serves its rows.
            oracle.assert_view_matches(
                client,
                client.resolve_table(sn, "base")[0],
                ["id", "a"],
                Counter({(1, 10): 1, (2, 20): 1}),
                "after the refused replace",
            )
        finally:
            client.drop_schema(sn)

    def test_replace_refuses_a_body_that_reads_the_view_itself(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="referencing the view itself"):
                client.execute_sql("CREATE OR REPLACE VIEW v AS SELECT id FROM v", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_replace_carries_the_with_clause_where_alter_view_cannot(self, client):
        """`ALTER VIEW … AS` refuses a bounded or fed view because its grammar has
        nowhere to restate the budget. `CREATE OR REPLACE VIEW` writes it out, so
        the same retarget is exactly what the statement asked for."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql(
                "CREATE VIEW bounded WITH (capacity = '4 MB') AS SELECT id, a FROM t", schema_name=sn
            )
            client.execute_sql("CREATE VIEW fed WITH (delta = '4 MB') AS SELECT id, a FROM t", schema_name=sn)

            with pytest.raises(gnitz.GnitzError, match="cannot retarget a capacity-bounded view"):
                client.execute_sql("ALTER VIEW bounded AS SELECT id, b FROM t", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="cannot retarget a view with a delta feed"):
                client.execute_sql("ALTER VIEW fed AS SELECT id, b FROM t", schema_name=sn)

            client.execute_sql(
                "CREATE OR REPLACE VIEW bounded WITH (capacity = '4 MB') AS SELECT id, b FROM t",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE OR REPLACE VIEW fed WITH (delta = '4 MB') AS SELECT id, b FROM t", schema_name=sn
            )
            for name in ("bounded", "fed"):
                oracle.assert_view_matches(
                    client,
                    client.resolve_table(sn, name)[0],
                    ["id", "b"],
                    Counter({(1, 100): 1, (2, 200): 1}),
                    name,
                )

            # Rows alone cannot tell a bounded view under its cap from an
            # unbounded one, nor a fed view from an unfed one — so check the two
            # properties directly. A bounded view is a leaf (nothing may be
            # created over it); a fed view is the only kind a delta bootstrap
            # answers.
            with pytest.raises(gnitz.GnitzError, match="capacity-bounded view; views cannot be created over it"):
                client.execute_sql("CREATE VIEW over_it AS SELECT id FROM bounded", schema_name=sn)
            fed_id, fed_schema = client.resolve_table(sn, "fed")
            client.delta_bootstrap(fed_id, fed_schema)
        finally:
            client.drop_schema(sn)

    def test_replace_honours_an_omitted_with_clause_as_written(self, client):
        """`CREATE OR REPLACE VIEW` restates the whole definition, so omitting
        `WITH (…)` asks for an unbounded, unfed view — it does not silently carry
        the old view's budgets forward. This is the decision that separates it
        from `ALTER VIEW … AS`, which refuses the retarget precisely because it
        has no way to say either thing."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql(
                "CREATE VIEW bounded WITH (capacity = '4 MB') AS SELECT id, a FROM t", schema_name=sn
            )
            client.execute_sql("CREATE VIEW fed WITH (delta = '4 MB') AS SELECT id, a FROM t", schema_name=sn)

            client.execute_sql("CREATE OR REPLACE VIEW bounded AS SELECT id, b FROM t", schema_name=sn)
            client.execute_sql("CREATE OR REPLACE VIEW fed AS SELECT id, b FROM t", schema_name=sn)

            # No longer a leaf: a view over it is now legal.
            client.execute_sql("CREATE VIEW over_it AS SELECT id FROM bounded", schema_name=sn)
            # No longer fed: a bootstrap has nothing to answer from.
            fed_id, fed_schema = client.resolve_table(sn, "fed")
            with pytest.raises(gnitz.GnitzError):
                client.delta_bootstrap(fed_id, fed_schema)
        finally:
            client.drop_schema(sn)

    def test_or_replace_with_if_not_exists_is_refused(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            with pytest.raises(gnitz.GnitzError, match="ask for opposite outcomes"):
                client.execute_sql(
                    "CREATE OR REPLACE VIEW IF NOT EXISTS v AS SELECT id FROM t", schema_name=sn
                )
        finally:
            client.drop_schema(sn)

    def test_or_replace_on_a_table_is_refused_by_name(self, client):
        """`CREATE OR REPLACE TABLE` parses under GenericDialect and means
        "discard this table's rows"; DROP TABLE already spells that out loud."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            with pytest.raises(gnitz.GnitzError, match=r"OR REPLACE \(it would discard"):
                client.execute_sql("CREATE OR REPLACE TABLE t (id BIGINT PRIMARY KEY)", schema_name=sn)
        finally:
            client.drop_schema(sn)


class TestIfNotExists:
    def test_view_skip_leaves_the_standing_definition(self, client):
        """The clause is a name test, so a *different* body under a taken name is
        skipped, not compiled — and the standing view keeps serving its own rows."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("CREATE VIEW IF NOT EXISTS v AS SELECT id, b FROM t", schema_name=sn)
            assert client.resolve_table(sn, "v")[0] == vid, "the skip creates nothing"
            oracle.assert_view_matches(client, vid, ["id", "a"], Counter({(1, 10): 1, (2, 20): 1}))
        finally:
            client.drop_schema(sn)

    def test_view_creates_when_the_name_is_free(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql("CREATE VIEW IF NOT EXISTS v AS SELECT id, a FROM t", schema_name=sn)
            oracle.assert_view_matches(
                client, client.resolve_table(sn, "v")[0], ["id", "a"], Counter({(1, 10): 1, (2, 20): 1})
            )
        finally:
            client.drop_schema(sn)

    def test_table_skip_keeps_the_rows(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            tid = client.resolve_table(sn, "t")[0]
            client.execute_sql(
                "CREATE TABLE IF NOT EXISTS t (id BIGINT NOT NULL PRIMARY KEY, z BIGINT NOT NULL)",
                schema_name=sn,
            )
            assert client.resolve_table(sn, "t")[0] == tid
            oracle.assert_view_matches(
                client, tid, ["id", "a", "b"], Counter({(1, 10, 100): 1, (2, 20, 200): 1})
            )
        finally:
            client.drop_schema(sn)

    def test_index_skip_answers_with_the_standing_index(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            first = client.execute_sql("CREATE INDEX ix_a ON t (a)", schema_name=sn)
            again = client.execute_sql("CREATE INDEX IF NOT EXISTS ix_a ON t (a)", schema_name=sn)
            assert first == again, "the skip answers with the standing index"
            with pytest.raises(gnitz.GnitzError, match="Index already exists"):
                client.execute_sql("CREATE INDEX ix_a ON t (a)", schema_name=sn)
            # A free name under the clause still creates.
            client.execute_sql("CREATE INDEX IF NOT EXISTS ix_b ON t (b)", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_index_clause_requires_a_name(self, client):
        """`IF NOT EXISTS` tests a name, and the grammar will not let it be
        omitted — so the auto-named form never carries the clause."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            with pytest.raises(gnitz.GnitzError, match="parse error"):
                client.execute_sql("CREATE INDEX IF NOT EXISTS ON t (b)", schema_name=sn)
        finally:
            client.drop_schema(sn)


class TestIfExists:
    def test_drop_missing_is_a_no_op_for_every_kind(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql("DROP TABLE IF EXISTS nosuch", schema_name=sn)
            client.execute_sql("DROP VIEW IF EXISTS nosuch", schema_name=sn)
            client.execute_sql("DROP INDEX IF EXISTS nosuch", schema_name=sn)
            # The table it did not name is untouched.
            oracle.assert_view_matches(
                client,
                client.resolve_table(sn, "t")[0],
                ["id", "a", "b"],
                Counter({(1, 10, 100): 1, (2, 20, 200): 1}),
            )
        finally:
            client.drop_schema(sn)

    def test_drop_present_still_drops(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql("CREATE VIEW v AS SELECT id FROM t", schema_name=sn)
            client.execute_sql("CREATE INDEX ix_a ON t (a)", schema_name=sn)
            client.execute_sql("DROP VIEW IF EXISTS v", schema_name=sn)
            client.execute_sql("DROP INDEX IF EXISTS ix_a", schema_name=sn)
            client.execute_sql("DROP TABLE IF EXISTS t", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.resolve_table(sn, "t")
        finally:
            client.drop_schema(sn)

    def test_if_exists_does_not_soften_any_other_refusal(self, client):
        """The clause answers "no such object" only. A dependent view still blocks
        the drop, with the same error the bare form raises."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql("CREATE VIEW base AS SELECT id, a FROM t", schema_name=sn)
            client.execute_sql("CREATE VIEW dep AS SELECT id FROM base", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="View dependency"):
                client.execute_sql("DROP VIEW IF EXISTS base", schema_name=sn)
        finally:
            client.drop_schema(sn)


class TestIdempotenceClausesAcrossKinds:
    """The clauses test a NAME, and a name can be held by the wrong kind of
    relation. These are the decisions that choice implies, pinned so they cannot
    drift silently."""

    def test_create_view_if_not_exists_yields_to_a_table(self, client):
        """No dialect compares the standing definition, and gnitz could not — a
        view's catalog rows are its compiled circuit, never its text. So any
        relation under the name ends the statement, whatever its kind."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            tid = client.resolve_table(sn, "t")[0]
            client.execute_sql("CREATE VIEW IF NOT EXISTS t AS SELECT id FROM t", schema_name=sn)
            assert client.resolve_table(sn, "t")[0] == tid, "the table is untouched"
            # Without the clause the collision is the ordinary error.
            with pytest.raises(gnitz.GnitzError, match="already exists"):
                client.execute_sql("CREATE VIEW t AS SELECT id FROM t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_create_table_if_not_exists_creates_a_free_name(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql(
                "CREATE TABLE IF NOT EXISTS fresh (id BIGINT NOT NULL PRIMARY KEY)", schema_name=sn
            )
            client.execute_sql("INSERT INTO fresh VALUES (7)", schema_name=sn)
            oracle.assert_view_matches(
                client, client.resolve_table(sn, "fresh")[0], ["id"], Counter({(7,): 1})
            )
        finally:
            client.drop_schema(sn)

    def test_drop_if_exists_does_not_cross_kinds(self, client):
        """`IF EXISTS` answers "no such object", not "some other kind of object" —
        `DROP TABLE IF EXISTS v` where `v` is a view must still refuse, or a
        teardown script would silently leave the view standing."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql("CREATE VIEW v AS SELECT id FROM t", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("DROP TABLE IF EXISTS v", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("DROP VIEW IF EXISTS t", schema_name=sn)
            # Both are still there.
            client.resolve_table(sn, "v")
            client.resolve_table(sn, "t")
        finally:
            client.drop_schema(sn)

    def test_drop_if_exists_is_per_name_in_a_list(self, client):
        """One statement, several names: the clause no-ops the missing ones and
        still drops the present ones."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _base(client, sn)
            client.execute_sql("CREATE VIEW v1 AS SELECT id FROM t", schema_name=sn)
            client.execute_sql("CREATE VIEW v2 AS SELECT a FROM t", schema_name=sn)
            client.execute_sql("DROP VIEW IF EXISTS v1, nosuch, v2", schema_name=sn)
            for name in ("v1", "v2"):
                with pytest.raises(gnitz.GnitzError):
                    client.resolve_table(sn, name)
        finally:
            client.drop_schema(sn)

    def test_or_replace_view_refuses_a_stream(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL) WITH (stream = true)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError, match="is a stream; CREATE OR REPLACE VIEW requires a view"):
                client.execute_sql("CREATE OR REPLACE VIEW ev AS SELECT k FROM ev", schema_name=sn)
        finally:
            client.drop_schema(sn)
