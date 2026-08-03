"""A view the engine refuses to compile must be an error, not a silent empty view.

The circuit is compiled on the master inside the DDL, before it is durable, so a
rejection takes the ordinary ingest failure path and reaches the client with the
view never created. Without that, the view is created, resolvable, and returns no
rows forever — the same observable as a correct view over a source that happens
to match nothing.

Run with GNITZ_WORKERS=4: the pre-flight's premise is that a rank-0 compile
speaks for every worker, and a single-worker run would not exercise that.

The primary assertion is threshold-free, so it cannot rot when register
allocation or companion-spec injection changes: for each N in a range spanning
the limit, `CREATE VIEW` either errors or returns the correct rows — it never
succeeds and returns zero. Each range below spans the limit measured today, so
each sweep contains both outcomes and neither can pass vacuously. No test pins
the limit itself: the HAVING ladder costs 4 registers per conjunct, which is a
register-allocator detail, not a contract.
"""

import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100_000, 999_999))


def _cap_error(exc) -> bool:
    """The rejection names the register limit or the column limit — the two caps
    a view definition can cross. A bare guard name, or a Rust enum leaking
    through as `TooManyRegs(66)`, does not tell the author what to change."""
    msg = str(exc)
    return "registers" in msg or "MAX_COLUMNS" in msg


def _sweep(client, sn, ns, sql_for, expected_rows):
    """Run `CREATE VIEW v AS <sql_for(n)>` for each n, asserting the invariant:
    either a cap error, or exactly `expected_rows` rows. Never a created view
    with zero rows. Returns the set of outcomes so the caller can assert the
    range really spanned the limit."""
    outcomes = set()
    for n in ns:
        name = "v" + _uid()
        try:
            client.execute_sql(f"CREATE VIEW {name} AS {sql_for(n)}", schema_name=sn)
        except Exception as e:
            assert _cap_error(e), f"n={n}: rejection must name a limit, got: {e}"
            with pytest.raises(Exception):
                client.resolve_table(sn, name)
            # The name is free: a valid view under it creates and reads back.
            client.execute_sql(
                f"CREATE VIEW {name} AS SELECT pk FROM probe", schema_name=sn
            )
            vid = client.resolve_table(sn, name)[0]
            assert len(list(client.scan(vid))) == 1, f"n={n}: replacement view must read back"
            client.drop_view(sn, name)
            outcomes.add("error")
            continue
        vid = client.resolve_table(sn, name)[0]
        rows = list(client.scan(vid))
        assert len(rows) == expected_rows, f"n={n}: expected {expected_rows} rows, got {len(rows)}"
        client.drop_view(sn, name)
        outcomes.add("ok")
    assert outcomes == {"ok", "error"}, (
        f"the sweep must span the limit — outcomes were {outcomes}"
    )


@pytest.fixture
def caps(client):
    """A schema with every base relation the sweeps read, seeded so each
    correct answer is non-empty (a zero-row answer would make the invariant
    unfalsifiable)."""
    sn = "vcr" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE probe (pk BIGINT NOT NULL PRIMARY KEY)", schema_name=sn
        )
        client.execute_sql("INSERT INTO probe VALUES (1)", schema_name=sn)

        client.execute_sql(
            "CREATE TABLE hg (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO hg VALUES (1, 1), (2, 1), (3, 2)", schema_name=sn)

        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)

        client.execute_sql(
            "CREATE TABLE ts (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO ts VALUES " + ", ".join(f"({i}, 'a{i}')" for i in range(5)),
            schema_name=sn,
        )

        # 16 NULLABLE BIGINTs: a nullable SUM finalizes as `sum / (cnt != 0)`,
        # five registers each, so this is the register cap reached by ordinary
        # SQL rather than by a hand-written predicate.
        ncols = ", ".join(f"c{i} BIGINT" for i in range(16))
        client.execute_sql(
            f"CREATE TABLE wn (pk BIGINT NOT NULL PRIMARY KEY, {ncols})", schema_name=sn
        )
        vals = ", ".join(str(i) for i in range(16))
        client.execute_sql(f"INSERT INTO wn VALUES (1, {vals}), (2, {vals})", schema_name=sn)

        # 64 NOT NULL BIGINTs + pk = 65 columns: the reduce-output width cap.
        wcols = ", ".join(f"c{i} BIGINT NOT NULL" for i in range(64))
        client.execute_sql(
            f"CREATE TABLE w (pk BIGINT NOT NULL PRIMARY KEY, {wcols})", schema_name=sn
        )
        row1 = ", ".join(str(i) for i in range(64))
        row2 = ", ".join(str(i + 100) for i in range(64))
        client.execute_sql(f"INSERT INTO w VALUES (1, {row1}), (2, {row2})", schema_name=sn)

        yield sn
    finally:
        client.drop_schema(sn)


# ── The sweeps ──────────────────────────────────────────────────────────────


def test_having_conjunct_sweep(client, caps):
    """N equality conjuncts in HAVING. The correct answer is the single cat=1
    group, at every N the planner accepts."""
    _sweep(
        client,
        caps,
        range(14, 21),
        lambda n: "SELECT cat FROM hg GROUP BY cat HAVING "
        + " AND ".join(["cat = 1"] * n),
        expected_rows=1,
    )


def test_in_list_sweep(client, caps):
    """An N-item string IN inside a view's WHERE."""
    _sweep(
        client,
        caps,
        range(30, 37),
        lambda n: "SELECT pk, s FROM ts WHERE s IN ("
        + ", ".join(f"'a{i}'" for i in range(n))
        + ")",
        expected_rows=5,
    )


def test_computed_projection_sweep(client, caps):
    """N computed projection items in a view."""
    _sweep(
        client,
        caps,
        range(19, 25),
        lambda n: "SELECT pk, " + ", ".join(f"a+{i} AS c{i}" for i in range(n)) + " FROM t",
        expected_rows=3,
    )


def test_nullable_sum_sweep(client, caps):
    """N nullable SUMs grouped by pk — the register cap reached by plain SQL."""
    _sweep(
        client,
        caps,
        range(10, 17),
        lambda n: "SELECT pk, " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM wn GROUP BY pk",
        expected_rows=2,
    )


def test_wide_grouped_aggregate_sweep(client, caps):
    """N SUMs over a 65-column NOT NULL table, grouped by pk — the reduce
    output width, which is not an expression program at all, so no amount of
    expression validation would have caught it."""
    _sweep(
        client,
        caps,
        range(61, 65),
        lambda n: "SELECT pk, " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM w GROUP BY pk",
        expected_rows=2,
    )


def test_wide_group_key_sweep(client, caps):
    """N group-by columns plus one aggregate. The two seeded rows differ in
    every column, so each N yields two groups."""
    _sweep(
        client,
        caps,
        range(60, 65),
        lambda n: "SELECT "
        + ", ".join(f"c{i}" for i in range(n))
        + ", SUM(c63) FROM w GROUP BY "
        + ", ".join(f"c{i}" for i in range(n)),
        expected_rows=2,
    )


def test_ungrouped_wide_aggregate_sweep(client, caps):
    """N ungrouped SUMs. This breaks one aggregate earlier than the grouped
    form because the two-phase combine reduce appends a COUNT-of-partials
    existence gate the declared reduce schema does not model."""
    _sweep(
        client,
        caps,
        range(60, 65),
        lambda n: "SELECT " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM w",
        expected_rows=1,
    )


# ── Independent of any threshold ────────────────────────────────────────────


def test_adhoc_over_cap_projection_names_the_limit(client, caps):
    """The ad-hoc read path formats the same validator error: the limit itself,
    not the internal enum variant that carries it."""
    sql = "SELECT pk, " + ", ".join(f"a+{i} AS c{i}" for i in range(22)) + " FROM t"
    with pytest.raises(Exception) as e:
        client.execute_sql(sql, schema_name=caps)
    assert "registers" in str(e.value), f"got: {e.value}"
    assert "TooManyRegs" not in str(e.value), f"internal enum leaked: {e.value}"


def test_uncompilable_hidden_segment_leaves_nothing(client, caps):
    """A chain whose *hidden* segment is the uncompilable one. The bundle is
    atomic, so neither the hidden segment nor the named view survives."""
    pred = " AND ".join(["cat = 1"] * 20)
    with pytest.raises(Exception) as e:
        client.execute_sql(
            f"CREATE VIEW vh AS WITH g AS (SELECT cat FROM hg GROUP BY cat HAVING {pred}) "
            "SELECT cat FROM g",
            schema_name=caps,
        )
    assert _cap_error(e.value), f"got: {e.value}"
    with pytest.raises(Exception):
        client.resolve_table(caps, "vh")
    # The name is free afterwards, and the valid chain of the same shape works.
    client.execute_sql(
        "CREATE VIEW vh AS WITH g AS (SELECT cat FROM hg GROUP BY cat HAVING cat = 1) "
        "SELECT cat FROM g",
        schema_name=caps,
    )
    vid = client.resolve_table(caps, "vh")[0]
    assert len(list(client.scan(vid))) == 1


def test_alter_view_rejection_keeps_the_old_view(client, caps):
    """ALTER VIEW is one DDL zone: a rejected new definition must leave the old
    view serving its rows. A drop zone followed by a create zone would not."""
    client.execute_sql("CREATE VIEW va AS SELECT cat FROM hg WHERE cat = 1", schema_name=caps)
    vid = client.resolve_table(caps, "va")[0]
    assert len(list(client.scan(vid))) == 2

    pred = " AND ".join(["cat = 1"] * 20)
    with pytest.raises(Exception) as e:
        client.execute_sql(
            f"ALTER VIEW va AS SELECT cat FROM hg GROUP BY cat HAVING {pred}",
            schema_name=caps,
        )
    assert _cap_error(e.value), f"got: {e.value}"

    # Untouched: same id, same rows.
    assert client.resolve_table(caps, "va")[0] == vid
    assert len(list(client.scan(vid))) == 2

    # A valid redefinition still replaces it, under the same name.
    client.execute_sql("ALTER VIEW va AS SELECT cat FROM hg WHERE cat = 2", schema_name=caps)
    vid2 = client.resolve_table(caps, "va")[0]
    assert len(list(client.scan(vid2))) == 1
    client.drop_view(caps, "va")


def test_rejected_ddl_leaves_no_directory(own_server):
    """The pre-flight compiles into a throwaway `_preflight_<vid>` root and
    removes it on both paths, so a run of rejected statements adds nothing to
    the data dir. A root that survived would leak one directory per rejection."""
    import os

    own_server.start()
    conn = gnitz.connect(own_server.sock_path)
    try:
        conn.create_schema("resid")
        conn.execute_sql(
            "CREATE TABLE hg (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL)",
            schema_name="resid",
        )
        conn.execute_sql("INSERT INTO hg VALUES (1, 1), (2, 2)", schema_name="resid")
        conn.execute_sql("CREATE VIEW keep AS SELECT cat FROM hg WHERE cat = 1", schema_name="resid")

        schema_dir = os.path.join(own_server.data_dir, "resid")
        before = sorted(os.listdir(schema_dir))

        pred = " AND ".join(["cat = 1"] * 20)
        for i in range(3):
            with pytest.raises(Exception):
                conn.execute_sql(
                    f"CREATE VIEW bad{i} AS SELECT cat FROM hg GROUP BY cat HAVING {pred}",
                    schema_name="resid",
                )
            with pytest.raises(Exception):
                conn.execute_sql(
                    f"ALTER VIEW keep AS SELECT cat FROM hg GROUP BY cat HAVING {pred}",
                    schema_name="resid",
                )

        assert sorted(os.listdir(schema_dir)) == before, (
            "rejected CREATE/ALTER VIEW must leave no directory behind"
        )
        # And the view they tried to replace still works.
        vid = conn.resolve_table("resid", "keep")[0]
        assert len(list(conn.scan(vid))) == 1
    finally:
        conn.close()
