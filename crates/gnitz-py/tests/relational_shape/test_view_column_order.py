"""A view, and an ad-hoc SELECT, lists its columns in SELECT order — whatever
body it compiles to and wherever its key sits among them.

The engine stores a relation's batches as regions (the key, then the payload),
so a view whose SELECT list puts its key second is a relabelling of those
bytes, not a second layout. What is under test is that every surface reading
the view sees the declared order and the declared names on the right values:
`SELECT *`, a `CREATE VIEW v (x, y)` alias list, a view over the view, and a
keyed read of it. Values are compared through `bag`, so weights count too.
"""
import pytest
from _read import access, bag, rows


def _names(client, q):
    """The visible column names of `q`'s reply, in order."""
    res = client.execute_sql(q)[0]
    assert res["type"] == "Rows", res
    return [c.name for c in res["rows"].schema.columns if not c.is_hidden]


def _read(client, q):
    return _names(client, q), bag(rows(client, q))


_T = [(i, i % 3, i * 10) for i in range(1, 10)]


@pytest.fixture
def base(client):
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)"
    )
    client.execute_sql("INSERT INTO t VALUES " + ",".join(f"({i}, {a}, {b})" for i, a, b in _T))
    return client


@pytest.mark.parametrize(
    "body,names,want",
    [
        # Linear, the key between two payload columns.
        ("SELECT b, id, a FROM t", ["b", "id", "a"], {(b, i, a): 1 for i, a, b in _T}),
        # The natural group key second.
        (
            "SELECT COUNT(*) AS n, a FROM t GROUP BY a",
            ["n", "a"],
            {(3, a): 1 for a in range(3)},
        ),
        # QUALIFY: one row per `a`, the smallest `id`.
        (
            "SELECT a, id FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY id) = 1",
            ["a", "id"],
            {(1, 1): 1, (2, 2): 1, (0, 3): 1},
        ),
        # ORDER BY … LIMIT: the top-N carries its input's columns, relabelled back.
        (
            "SELECT b, id FROM t ORDER BY b DESC LIMIT 2",
            ["b", "id"],
            {(90, 9): 1, (80, 8): 1},
        ),
    ],
    ids=["linear", "groupby", "qualify", "top_n"],
)
def test_select_star_over_a_view_is_its_select_order(base, body, names, want):
    base.execute_sql(f"CREATE VIEW v AS {body}")
    assert _read(base, "SELECT * FROM v") == (names, want)
    # The same body read ad hoc agrees with the maintained view; QUALIFY is a
    # view-only clause.
    if "QUALIFY" not in body:
        assert _read(base, body) == (names, want)


def test_column_aliases_name_the_select_list_in_order(base):
    """`y` names `id`, the key, though the key is stored first."""
    base.execute_sql("CREATE VIEW v (x, y) AS SELECT a, id FROM t")
    assert _read(base, "SELECT * FROM v") == (["x", "y"], {(a, i): 1 for i, a, _ in _T})
    assert bag(rows(base, "SELECT x FROM v")) == {(a,): 3 for a in range(3)}
    assert bag(rows(base, "SELECT y FROM v WHERE y = 4")) == {(4,): 1}


def test_a_view_over_a_reordered_view_reads_it_by_name(base):
    base.execute_sql("CREATE VIEW v AS SELECT b, id, a FROM t")
    base.execute_sql("CREATE VIEW w AS SELECT * FROM v WHERE a > 0")
    base.execute_sql("CREATE VIEW g AS SELECT a, SUM(b) AS s FROM v GROUP BY a")
    assert _read(base, "SELECT * FROM w") == (
        ["b", "id", "a"],
        {(b, i, a): 1 for i, a, b in _T if a > 0},
    )
    sums = {}
    for _, a, b in _T:
        sums[a] = sums.get(a, 0) + b
    assert bag(rows(base, "SELECT * FROM g")) == {(a, s): 1 for a, s in sums.items()}
    base.execute_sql("INSERT INTO t VALUES (10, 1, 100)")
    assert bag(rows(base, "SELECT b, id FROM w WHERE id = 10")) == {(100, 10): 1}


def test_a_keyed_read_of_a_reordered_view_finds_its_key(base):
    base.execute_sql("CREATE VIEW v AS SELECT b, id, a FROM t")
    q = "SELECT b, id, a FROM v WHERE id = 4"
    assert access(base, q) == "pk point lookup"
    assert _read(base, q) == (["b", "id", "a"], {(40, 4, 1): 1})
    q = "SELECT id FROM v WHERE id > 6"
    assert access(base, q) == "pk range walk"
    assert bag(rows(base, q)) == {(7,): 1, (8,): 1, (9,): 1}
    base.execute_sql("DELETE FROM t WHERE id = 4")
    assert bag(rows(base, "SELECT b, id, a FROM v WHERE id = 4")) == {}


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, w BIGINT NOT NULL)",
        "CREATE TABLE r (v BIGINT NOT NULL, id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL)",
    ],
    ids=["pk_first", "pk_middle"],
)
def test_an_adhoc_reply_is_its_select_order(client, ddl):
    client.execute_sql(ddl)
    cols = "id, v, w" if "PRIMARY KEY, v" in ddl else "v, id, w"
    rs = [(i, i * 7 % 10, i * 100) for i in range(1, 9)]
    vals = [(v, i, w) if cols.startswith("v") else (i, v, w) for i, v, w in rs]
    client.execute_sql(f"INSERT INTO r ({cols}) VALUES " + ",".join(str(t) for t in vals))
    got = rows(client, "SELECT v, id FROM r ORDER BY id LIMIT 3")
    assert [tuple(r) for r in got] == [(v, i) for i, v, _ in rs[:3]]
    assert _names(client, "SELECT v, id FROM r ORDER BY id LIMIT 3") == ["v", "id"]
    assert _read(client, "SELECT id, v FROM r") == (["id", "v"], {(i, v): 1 for i, v, _ in rs})
    assert _read(client, "SELECT * FROM r") == (
        cols.split(", "),
        {tuple({"id": i, "v": v, "w": w}[c] for c in cols.split(", ")): 1 for i, v, w in rs},
    )
