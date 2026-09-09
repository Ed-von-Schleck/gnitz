"""What SQL refuses, and with which message.

A query that derives a new relation (JOIN, set-op, EXISTS/IN, scalar subquery,
derived table, grouped CTE) is rejected from the AST alone with one template
pointing at CREATE VIEW. A single-relation read using a feature the direct
path cannot express is a feature-named error instead — never the derivation
template. The rest are clauses no statement family supports: each was once
parsed and silently dropped, which is why the rejection has to name the clause.
"""

import pytest
import gnitz

_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"
_ROWS = "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)"


@pytest.fixture(scope="module")
def rejected(server):
    """`(conn, schema)` holding `p(k)`, `t(pk, val)` with five rows, and
    `u(pk, k)` with two — for the rejection cases only.

    Module-scoped, and its own connection off the session server rather than the
    function-scoped `client`: every test sharing it asserts a *refusal*, so none
    of them can leave a mark on it. The alternative is rebuilding two tables
    forty-odd times to run forty-odd statements that change nothing.
    """
    with gnitz.connect(server) as conn:
        sn = "sqlrej"
        conn.create_schema(sn)
        conn.execute_sql("CREATE TABLE p (k BIGINT PRIMARY KEY)", schema_name=sn)
        conn.execute_sql(_T, schema_name=sn)
        conn.execute_sql(_ROWS, schema_name=sn)
        conn.execute_sql(
            "CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
            schema_name=sn)
        conn.execute_sql("INSERT INTO u VALUES (1, 10), (2, 20)", schema_name=sn)
        yield conn, sn
        conn.drop_schema(sn)


_DERIVATIONS = [
    ("SELECT t.pk FROM t JOIN u ON t.val = u.k", "JOIN"),
    ("SELECT val FROM t UNION SELECT k FROM u", "set operation"),
    ("SELECT val FROM t INTERSECT SELECT k FROM u", "set operation"),
    ("SELECT val FROM t EXCEPT SELECT k FROM u", "set operation"),
    ("SELECT pk FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.k = t.val)", "EXISTS/IN subquery"),
    ("SELECT pk FROM t WHERE val IN (SELECT k FROM u)", "EXISTS/IN subquery"),
    ("SELECT pk FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.k = t.val)", "EXISTS/IN subquery"),
    ("SELECT pk, (SELECT MAX(k) FROM u) FROM t", "scalar subquery"),
    ("SELECT x FROM (SELECT val AS x FROM t) d", "derived table in FROM"),
    ("WITH c AS (SELECT val, COUNT(*) AS n FROM t GROUP BY val) SELECT val FROM c", "grouped CTE"),
    ("SELECT t.pk FROM t, u WHERE t.val = u.k", "comma-join FROM"),
]


@pytest.mark.parametrize("sql,construct", _DERIVATIONS)
def test_a_derivation_is_rejected_with_create_view_advice(rejected, sql, construct):
    """An ad-hoc SELECT reads one relation; a query that derives a new one is
    rejected from the AST alone with one template naming the construct."""
    conn, sn = rejected
    with pytest.raises(gnitz.GnitzError) as ei:
        conn.execute_sql(sql, schema_name=sn)
    msg = str(ei.value)
    assert "this query derives a new one" in msg, f"{sql!r}: not the derivation template: {msg}"
    assert f"({construct})" in msg, f"{sql!r}: must name '{construct}', got: {msg}"
    assert "CREATE VIEW" in msg, f"{sql!r}: must point at CREATE VIEW, got: {msg}"


def test_the_advice_is_literally_what_the_user_should_do(client, schema_name):
    """The comma-join's advice is the template's, verbatim: a view body may be
    written that way, so "CREATE VIEW AS <your query>" is the real next step."""
    client.execute_sql(_T, schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW comma_ok AS SELECT t.pk FROM t, u WHERE t.val = u.k",
        schema_name=schema_name)


def test_a_direct_path_feature_limit_is_not_a_derivation_error(client, schema_name):
    """A single-relation read using a feature the direct path cannot express (a
    LIKE whose pattern is not a literal) is a feature-named error — never the
    derivation template. A string HAVING is not one of them: it compiles through
    the same expression compiler a grouped view's post-reduce FILTER uses, so it
    is served — and so are a literal-pattern LIKE and an ORDER BY expression."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, s TEXT)",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b')", schema_name=schema_name)

    with pytest.raises(gnitz.GnitzError) as ei:
        client.execute_sql("SELECT pk FROM t WHERE s LIKE s", schema_name=schema_name)
    assert "this query derives a new one" not in str(ei.value), (
        f"a non-literal LIKE pattern is a feature limit, not a derivation: {ei.value}")

    res = client.execute_sql("SELECT pk FROM t ORDER BY 0 - pk", schema_name=schema_name)
    assert [r.pk for r in res[0]["rows"]] == [2, 1], res[0]
    res = client.execute_sql(
        "SELECT s, COUNT(*) AS c FROM t GROUP BY s HAVING s = 'a'", schema_name=schema_name)
    assert sorted((r.s, r.c) for r in res[0]["rows"]) == [("a", 1)], res[0]
    # A literal-pattern LIKE contributes no access path, but is served as a
    # residual filter.
    res = client.execute_sql("SELECT pk FROM t WHERE s LIKE 'a%'", schema_name=schema_name)
    assert [r.pk for r in res[0]["rows"]] == [1], res[0]


# Clauses `GenericDialect` parses and the planner once dropped on the floor.
# Each names itself, so the user learns which clause was refused rather than
# silently getting a different query's answer.
_UNHONOURED = [
    ("SELECT DISTINCT ON (pk) pk FROM t", "DISTINCT ON is not supported"),
    # ORDER BY and OFFSET are honoured by the client-side sink; only the
    # ClickHouse per-group `LIMIT ... BY` stays rejected on the LIMIT envelope.
    ("SELECT * FROM t LIMIT 2 BY val", r"LIMIT \.\.\. BY is not supported"),
    ("SELECT * FROM t FETCH FIRST 2 ROWS ONLY", "FETCH is not supported"),
    ("SELECT pk FROM t PREWHERE val > 5", "PREWHERE is not supported"),
    ("SELECT TOP 2 pk FROM t", "TOP is not supported"),
    ("SELECT pk FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY pk) = 1", "QUALIFY is not supported"),
    ("SELECT pk FROM t FOR UPDATE", "FOR UPDATE/SHARE is not supported"),
    ("SELECT pk FROM t SETTINGS max_threads = 1", "SETTINGS is not supported"),
    ("SELECT pk FROM t FORMAT JSON", "FORMAT is not supported"),
    # An INSERT's source Query carries the same envelope, once dropped whole.
    ("INSERT INTO t VALUES (100, 200) LIMIT 1", "LIMIT/OFFSET is not supported"),
    ("INSERT INTO t VALUES (101, 201) FOR UPDATE", "FOR UPDATE/SHARE is not supported"),
    # INSERT ... RETURNING is supported (mutation_pattern/test_serial.py);
    # UPDATE and DELETE RETURNING are not.
    ("UPDATE t SET val = 9 WHERE pk = 1 RETURNING pk", "UPDATE: RETURNING is not supported"),
    ("DELETE FROM t WHERE pk = 1 RETURNING pk", "DELETE: RETURNING is not supported"),
    ("DELETE FROM t LIMIT 1", "DELETE: LIMIT is not supported"),
    ("DELETE FROM t ORDER BY pk", "DELETE: ORDER BY is not supported"),
    # The FROM/USING rejection fires before any table resolution, so `other`
    # need not exist.
    ("UPDATE t SET val = u.val FROM other u WHERE t.pk = u.pk",
     "UPDATE: FROM .join-update. is not supported"),
    ("DELETE FROM t USING other u WHERE t.pk = u.pk",
     "DELETE: USING .join-delete. is not supported"),
    ("INSERT IGNORE INTO t VALUES (6, 60)", "INSERT: IGNORE is not supported"),
    ("REPLACE INTO t VALUES (6, 60)", "INSERT: REPLACE INTO is not supported"),
    ("CREATE TABLE t2 (pk BIGINT PRIMARY KEY) AS SELECT pk FROM t",
     "AS SELECT .CTAS. is not supported"),
    ("CREATE TEMPORARY TABLE tmp (pk BIGINT PRIMARY KEY)",
     "CREATE TABLE: TEMPORARY is not supported"),
    ("CREATE TEMPORARY VIEW vt AS SELECT pk FROM t", "CREATE VIEW: TEMPORARY is not supported"),
    ("CREATE VIEW vbad (a, b) AS SELECT pk FROM t", "defines 2 column aliases but body returns 1"),
    ("CREATE INDEX ix ON t (val) WHERE val > 0",
     "CREATE INDEX: WHERE .partial index. is not supported"),
    # A dropped CHECK or DEFAULT is a constraint that is never enforced.
    ("CREATE TABLE c1 (pk BIGINT PRIMARY KEY, x BIGINT CHECK (x > 0))",
     "column definition: CHECK is not supported"),
    ("CREATE TABLE c2 (pk BIGINT PRIMARY KEY, x BIGINT, CHECK (x > 0))",
     "table constraint: CHECK constraint is not supported"),
    ("CREATE TABLE d1 (pk BIGINT PRIMARY KEY, x BIGINT DEFAULT 5)",
     "column definition: DEFAULT is not supported"),
    ("CREATE TABLE f (pk BIGINT PRIMARY KEY, c BIGINT REFERENCES p(k) ON DELETE CASCADE)",
     "FOREIGN KEY ON DELETE/ON UPDATE action"),
    # A wildcard modifier that would answer a different query than the one
    # written: gnitz honours the column-dropping and renaming ones (EXCEPT,
    # EXCLUDE, RENAME) but neither a computed substitution nor a name-pattern
    # filter, so expanding `*` in their place has to be refused rather than
    # silently ignored. Every surface that expands a wildcard shares one guard.
    ("SELECT * REPLACE (val + 1 AS val) FROM t", "SELECT \\* REPLACE is not supported"),
    ("SELECT * ILIKE 'v%' FROM t", "SELECT \\* ILIKE is not supported"),
    ("CREATE VIEW vrep AS SELECT * REPLACE (val + 1 AS val) FROM t",
     "SELECT \\* REPLACE is not supported"),
    ("CREATE VIEW vilk AS SELECT * ILIKE 'v%' FROM t", "SELECT \\* ILIKE is not supported"),
    ("INSERT INTO t VALUES (200, 300) RETURNING * REPLACE (val + 1 AS val)",
     "SELECT \\* REPLACE is not supported"),
]


@pytest.mark.parametrize("sql,message", _UNHONOURED)
def test_an_unhonoured_clause_names_itself(rejected, sql, message):
    conn, sn = rejected
    with pytest.raises(gnitz.GnitzError, match=message):
        conn.execute_sql(sql, schema_name=sn)


def test_the_neighbouring_spellings_are_unaffected(client, schema_name):
    """Every clause above sits beside one that is served; rejecting the envelope
    must not take the ordinary statement with it."""
    client.execute_sql("CREATE TABLE p (k BIGINT PRIMARY KEY)", schema_name=schema_name)
    client.execute_sql(_T, schema_name=schema_name)
    client.execute_sql(_ROWS, schema_name=schema_name)

    assert len(client.execute_sql("SELECT pk FROM t", schema_name=schema_name)[0]["rows"]) == 5
    assert len(client.execute_sql("SELECT * FROM t LIMIT 2", schema_name=schema_name)[0]["rows"]) == 2
    res = client.execute_sql("SELECT * FROM t ORDER BY val DESC LIMIT 2", schema_name=schema_name)
    assert [r.pk for r in res[0]["rows"]] == [5, 4]
    res = client.execute_sql("SELECT * FROM t ORDER BY val LIMIT 2 OFFSET 1", schema_name=schema_name)
    assert [r.pk for r in res[0]["rows"]] == [2, 3]

    client.execute_sql("INSERT INTO t VALUES (6, 60)", schema_name=schema_name)
    client.execute_sql("UPDATE t SET val = 99 WHERE pk = 6", schema_name=schema_name)
    client.execute_sql("DELETE FROM t WHERE pk = 6", schema_name=schema_name)
    # MATERIALIZED is accepted — gnitz views are incrementally materialized.
    client.execute_sql("CREATE MATERIALIZED VIEW vm AS SELECT pk FROM t", schema_name=schema_name)
    client.execute_sql("CREATE VIEW v2 AS SELECT pk FROM t", schema_name=schema_name)
    # USING BTREE is the accepted default.
    client.execute_sql("CREATE INDEX ix2 ON t (val)", schema_name=schema_name)
    client.execute_sql("CREATE INDEX ixb ON t USING BTREE (val)", schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE f2 (pk BIGINT PRIMARY KEY, c BIGINT REFERENCES p(k))", schema_name=schema_name)


def test_replicated_and_cluster_by_are_mutually_exclusive(client, schema_name):
    """The two placements are alternatives, not layers: `CLUSTER BY` hashes a PK
    prefix to choose a worker, and a replicated table chooses none because every
    worker holds all of it. Accepting both would leave the router with two
    answers, so the planner refuses the pair by name.

    `WITH` parses before `CLUSTER BY`, and `CLUSTER BY` takes bare columns — this
    is the spelling that reaches the check rather than failing in the parser.
    """
    with pytest.raises(gnitz.GnitzError, match="(?is)replicated.*cluster by"):
        client.execute_sql(
            "CREATE TABLE bad (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) "
            "WITH (replicated = true) CLUSTER BY id",
            schema_name=schema_name)


def test_a_string_literal_is_refused_by_a_blob_column(client, schema_name):
    """`sql_type_to_typecode` names no BLOB, but the driver creates BLOB columns
    freely and SQL can then INSERT into them — so the INSERT literal path must
    refuse text for a column that takes bytes, rather than writing the string's
    own bytes into a column whose values are not text."""
    client.create_table(schema_name, "t", [
        gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
        gnitz.ColumnDef("b", gnitz.TypeCode.BLOB),
    ])
    with pytest.raises(gnitz.GnitzError, match="string literal for non-string column"):
        client.execute_sql("INSERT INTO t (pk, b) VALUES (1, 'x')", schema_name=schema_name)
