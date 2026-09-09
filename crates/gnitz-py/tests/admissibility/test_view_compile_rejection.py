"""A view the engine refuses to compile must be an error, not a silent empty view.

The circuit is compiled on the master inside the DDL, before it is durable, so a
rejection takes the ordinary ingest failure path and reaches the client with the
view never created. Without that, the view is created, resolvable, and returns no
rows forever — the same observable as a correct view over a source that happens
to match nothing.

Run with GNITZ_WORKERS=4: the pre-flight's premise is that a rank-0 compile
speaks for every worker, and a single-worker run would not exercise that.

The sweep assertion is threshold-free, so it cannot rot when register allocation
or companion-spec injection changes: for each N in a range spanning the limit,
`CREATE VIEW` either errors or returns the correct rows — it never succeeds and
returns zero. Each range below spans the limit measured today, so each sweep
contains both outcomes and neither can pass vacuously. No test pins the limit
itself: what a conjunct costs is a register-allocator detail, not a contract.
"""

import os

import pytest
import gnitz
from _uid import uid as _uid


def _cat_is_one_ladder(n: int) -> str:
    """`n` conjuncts, each true exactly for `cat = 1` and none sharing an
    instruction the builder could fold: `cat * k < k + 1` over distinct odd `k`."""
    return " AND ".join(f"cat * {2 * i + 1} < {2 * i + 2}" for i in range(n))


def _cap_error(exc) -> bool:
    """The rejection names the register limit or the column limit — the two caps
    a view definition can cross. A bare guard name, or a Rust enum leaking
    through as `TooManyRegs(66)`, does not tell the author what to change."""
    msg = str(exc)
    return "registers" in msg or "MAX_COLUMNS" in msg


@pytest.fixture
def caps(client, schema_name):
    """A schema with every base relation the sweeps read, seeded so each
    correct answer is non-empty (a zero-row answer would make the invariant
    unfalsifiable)."""
    sql = client.execute_sql
    sql("CREATE TABLE probe (pk BIGINT NOT NULL PRIMARY KEY)", schema_name=schema_name)
    sql("INSERT INTO probe VALUES (1)", schema_name=schema_name)

    sql("CREATE TABLE hg (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL)",
        schema_name=schema_name)
    sql("INSERT INTO hg VALUES (1, 1), (2, 1), (3, 2)", schema_name=schema_name)

    sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
        schema_name=schema_name)
    sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=schema_name)

    sql("CREATE TABLE ts (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=schema_name)
    sql("INSERT INTO ts VALUES " + ", ".join(f"({i}, 'a{i}')" for i in range(5)),
        schema_name=schema_name)

    # 16 NULLABLE BIGINTs: a nullable SUM finalizes as `sum / (cnt != 0)`,
    # five registers each, so this is the register cap reached by ordinary
    # SQL rather than by a hand-written predicate.
    ncols = ", ".join(f"c{i} BIGINT" for i in range(16))
    sql(f"CREATE TABLE wn (pk BIGINT NOT NULL PRIMARY KEY, {ncols})", schema_name=schema_name)
    vals = ", ".join(str(i) for i in range(16))
    sql(f"INSERT INTO wn VALUES (1, {vals}), (2, {vals})", schema_name=schema_name)

    # 64 NOT NULL BIGINTs + pk = 65 columns: the reduce-output width cap.
    wcols = ", ".join(f"c{i} BIGINT NOT NULL" for i in range(64))
    sql(f"CREATE TABLE w (pk BIGINT NOT NULL PRIMARY KEY, {wcols})", schema_name=schema_name)
    row1 = ", ".join(str(i) for i in range(64))
    row2 = ", ".join(str(i + 100) for i in range(64))
    sql(f"INSERT INTO w VALUES (1, {row1}), (2, {row2})", schema_name=schema_name)
    return schema_name


# `(id, range spanning the limit, body for n, rows the correct answer holds)`.
_SWEEPS = [
    # N equality conjuncts in HAVING; the answer is the single cat=1 group.
    ("having_conjunct", range(10, 16),
     lambda n: "SELECT cat FROM hg GROUP BY cat HAVING " + _cat_is_one_ladder(n), 1),
    # An N-item string IN inside a view's WHERE.
    ("in_list", range(30, 37),
     lambda n: "SELECT pk, s FROM ts WHERE s IN (" + ", ".join(f"'a{i}'" for i in range(n)) + ")", 5),
    # N computed projection items: one shared column load, then a constant and
    # an add apiece.
    ("computed_projection", range(29, 35),
     lambda n: "SELECT pk, " + ", ".join(f"a+{i} AS c{i}" for i in range(n)) + " FROM t", 3),
    # N nullable SUMs grouped by pk — the register cap reached by plain SQL.
    ("nullable_sum", range(10, 17),
     lambda n: "SELECT pk, " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM wn GROUP BY pk", 2),
    # N SUMs over a 65-column NOT NULL table: the reduce output width, which is
    # not an expression program at all, so no amount of expression validation
    # would have caught it.
    ("wide_grouped_aggregate", range(61, 65),
     lambda n: "SELECT pk, " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM w GROUP BY pk", 2),
    # N group-by columns plus one aggregate. The two seeded rows differ in every
    # column, so each N yields two groups.
    ("wide_group_key", range(60, 65),
     lambda n: "SELECT " + ", ".join(f"c{i}" for i in range(n)) + ", SUM(c63) FROM w GROUP BY "
               + ", ".join(f"c{i}" for i in range(n)), 2),
    # N ungrouped SUMs. This breaks one aggregate earlier than the grouped form
    # because the two-phase combine reduce appends a COUNT-of-partials existence
    # gate the declared reduce schema does not model.
    ("ungrouped_wide_aggregate", range(60, 65),
     lambda n: "SELECT " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM w", 1),
]


@pytest.mark.parametrize("ns,sql_for,expected_rows", [s[1:] for s in _SWEEPS],
                         ids=[s[0] for s in _SWEEPS])
def test_a_view_never_compiles_to_silently_empty(client, caps, ns, sql_for, expected_rows):
    """Across a range spanning the limit, `CREATE VIEW` either errors naming a
    cap, or creates a view returning exactly the correct rows. Never a created
    view with zero rows."""
    outcomes = set()
    for n in ns:
        name = "v" + _uid()
        try:
            client.execute_sql(f"CREATE VIEW {name} AS {sql_for(n)}", schema_name=caps)
        except gnitz.GnitzError as e:
            assert _cap_error(e), f"n={n}: rejection must name a limit, got: {e}"
            with pytest.raises(gnitz.GnitzError):
                client.resolve_table(caps, name)
            outcomes.add("error")
            continue
        vid = client.resolve_table(caps, name)[0]
        rows = list(client.scan(vid))
        assert len(rows) == expected_rows, f"n={n}: expected {expected_rows} rows, got {len(rows)}"
        client.drop_view(caps, name)
        outcomes.add("ok")
    assert outcomes == {"ok", "error"}, f"the sweep must span the limit — outcomes were {outcomes}"


def test_adhoc_over_cap_projection_names_the_limit(client, caps):
    """The ad-hoc read path formats the same validator error: the limit itself,
    not the internal enum variant that carries it."""
    sql = "SELECT pk, " + ", ".join(f"a+{i} AS c{i}" for i in range(34)) + " FROM t"
    with pytest.raises(gnitz.GnitzError) as e:
        client.execute_sql(sql, schema_name=caps)
    assert "registers" in str(e.value), f"got: {e.value}"
    assert "TooManyRegs" not in str(e.value), f"internal enum leaked: {e.value}"


def test_uncompilable_hidden_segment_leaves_nothing(client, caps):
    """A chain whose *hidden* segment is the uncompilable one. The bundle is
    atomic, so neither the hidden segment nor the named view survives — and the
    name is free afterwards, which a leaked segment would deny."""
    pred = _cat_is_one_ladder(20)
    with pytest.raises(gnitz.GnitzError) as e:
        client.execute_sql(
            f"CREATE VIEW vh AS WITH g AS (SELECT cat FROM hg GROUP BY cat HAVING {pred}) "
            "SELECT cat FROM g", schema_name=caps)
    assert _cap_error(e.value), f"got: {e.value}"
    with pytest.raises(gnitz.GnitzError):
        client.resolve_table(caps, "vh")

    client.execute_sql(
        "CREATE VIEW vh AS WITH g AS (SELECT cat FROM hg GROUP BY cat HAVING cat = 1) "
        "SELECT cat FROM g", schema_name=caps)
    assert len(list(client.scan(client.resolve_table(caps, "vh")[0]))) == 1


def test_alter_view_rejection_keeps_the_old_view(client, caps):
    """ALTER VIEW is one DDL zone: a rejected new definition must leave the old
    view serving its rows. A drop zone followed by a create zone would not."""
    client.execute_sql("CREATE VIEW va AS SELECT cat FROM hg WHERE cat = 1", schema_name=caps)
    vid = client.resolve_table(caps, "va")[0]
    assert len(list(client.scan(vid))) == 2

    with pytest.raises(gnitz.GnitzError) as e:
        client.execute_sql(
            f"ALTER VIEW va AS SELECT cat FROM hg GROUP BY cat HAVING {_cat_is_one_ladder(20)}",
            schema_name=caps)
    assert _cap_error(e.value), f"got: {e.value}"

    # Untouched: same id, same rows.
    assert client.resolve_table(caps, "va")[0] == vid
    assert len(list(client.scan(vid))) == 2

    # A valid redefinition still replaces it, under the same name.
    client.execute_sql("ALTER VIEW va AS SELECT cat FROM hg WHERE cat = 2", schema_name=caps)
    assert len(list(client.scan(client.resolve_table(caps, "va")[0]))) == 1


def test_rejected_ddl_leaves_no_directory(own_server):
    """The pre-flight compiles into a throwaway `_preflight_<vid>` root and
    removes it on both paths, so a run of rejected statements adds nothing to
    the data dir. A root that survived would leak one directory per rejection."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("resid")
        conn.execute_sql(
            "CREATE TABLE hg (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL)",
            schema_name="resid")
        conn.execute_sql("INSERT INTO hg VALUES (1, 1), (2, 2)", schema_name="resid")
        conn.execute_sql("CREATE VIEW keep AS SELECT cat FROM hg WHERE cat = 1",
                         schema_name="resid")

        schema_dir = os.path.join(own_server.data_dir, "resid")
        before = sorted(os.listdir(schema_dir))

        pred = _cat_is_one_ladder(20)
        for i in range(3):
            for stmt in (f"CREATE VIEW bad{i} AS SELECT cat FROM hg GROUP BY cat HAVING {pred}",
                         f"ALTER VIEW keep AS SELECT cat FROM hg GROUP BY cat HAVING {pred}"):
                with pytest.raises(gnitz.GnitzError):
                    conn.execute_sql(stmt, schema_name="resid")

        assert sorted(os.listdir(schema_dir)) == before, (
            "rejected CREATE/ALTER VIEW must leave no directory behind")
        # And the view they tried to replace still works.
        assert len(list(conn.scan(conn.resolve_table("resid", "keep")[0]))) == 1
