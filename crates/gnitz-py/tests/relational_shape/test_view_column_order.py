"""A view, and an ad-hoc SELECT, lists its columns in SELECT order — whatever
body it compiles to and wherever its key sits among them.

The engine stores a relation's batches as regions (the key, then the payload),
so a view whose SELECT list puts its key second is a relabelling of those
bytes, not a second layout. What is under test is that every surface reading
the view sees the declared order and the declared names on the right values:
`SELECT *`, a `CREATE VIEW v (x, y)` alias list, a view over the view, and a
keyed read of it. Values are compared through `bag`, so weights count too.
"""
import gnitz
import pytest
from _read import access, bag, ordered, reply, rows
from _shapes import grouped
from _sql import insert


def _read(client, q):
    """The visible column names of `q`'s reply, in order, and its bag."""
    res = reply(client.execute_sql(q))
    return list(res._fields), bag(res)


_T = [(i, i % 3, i * 10) for i in range(1, 10)]


@pytest.fixture
def base(client):
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)"
    )
    insert(client, "t", _T)
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
    """`y` names `id`, the key, though the key is stored first. The list names the
    output positionally on every body and budget and on both spellings: a bounded
    view, a join whose hidden `_join_pk` it skips, and `ALTER VIEW w (x, y)`. The
    names land on the registered schema, so a view over `v` selects the aliases
    and the body's own names no longer resolve."""
    base.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL); INSERT INTO u VALUES (70, 1); "
        "CREATE VIEW v (x, y) AS SELECT a, id FROM t; "
        "CREATE VIEW bv (x, y) WITH (capacity = '4 MB') AS SELECT a, id FROM t; "
        "CREATE VIEW j (x, y) AS SELECT u.id AS uid, t.id FROM t JOIN u ON t.a = u.a; "
        "CREATE VIEW over_v AS SELECT x, y FROM v; "
        "CREATE VIEW w AS SELECT id, b FROM t; ALTER VIEW w (x, y) AS SELECT a, id FROM t")
    want = {(a, i): 1 for i, a, _ in _T}
    for name, w in (("v", want), ("bv", want), ("over_v", want), ("w", want),
                    ("j", {(70, i): 1 for i, a, _ in _T if a == 1})):
        assert _read(base, f"SELECT * FROM {name}") == (["x", "y"], w), name
    with pytest.raises(gnitz.GnitzRefusedError, match="column 'id' not found"):
        base.execute_sql("CREATE VIEW nope AS SELECT id FROM v")
    assert bag(rows(base, "SELECT x FROM v")) == {(a,): 3 for a in range(3)}
    assert bag(rows(base, "SELECT y FROM v WHERE y = 4")) == {(4,): 1}


def test_a_view_over_a_reordered_view_reads_it_by_name_and_key(base):
    """A view over `v`, whose key sits second, resolves its columns by name, and
    a keyed read of `v` finds the key wherever it sits."""
    base.execute_sql(
        "CREATE VIEW v AS SELECT b, id, a FROM t; "
        "CREATE VIEW w AS SELECT * FROM v WHERE a > 0; "
        "CREATE VIEW g AS SELECT a, SUM(b) AS s FROM v GROUP BY a")
    assert _read(base, "SELECT * FROM w") == (
        ["b", "id", "a"],
        {(b, i, a): 1 for i, a, b in _T if a > 0},
    )
    assert bag(rows(base, "SELECT * FROM g")) == grouped(_T, lambda r: r[1], lambda r: r[2])
    q = "SELECT b, id, a FROM v WHERE id = 4"
    assert access(base, q) == "pk point lookup"
    assert _read(base, q) == (["b", "id", "a"], {(40, 4, 1): 1})
    q = "SELECT id FROM v WHERE id > 6"
    assert access(base, q) == "pk range walk"
    assert bag(rows(base, q)) == {(7,): 1, (8,): 1, (9,): 1}
    base.execute_sql("INSERT INTO t VALUES (10, 1, 100); DELETE FROM t WHERE id = 4")
    assert bag(rows(base, "SELECT b, id FROM w WHERE id = 10")) == {(100, 10): 1}
    assert bag(rows(base, "SELECT b, id, a FROM v WHERE id = 4")) == {}


@pytest.mark.parametrize("cols", [("id", "v", "w"), ("v", "id", "w")], ids=["pk_first", "pk_middle"])
def test_an_adhoc_reply_is_its_select_order(client, cols):
    client.execute_sql("CREATE TABLE r (" + ", ".join(
        f"{c} BIGINT NOT NULL" + (" PRIMARY KEY" if c == "id" else "") for c in cols) + ")")
    rs = [dict(id=i, v=i * 7 % 10, w=i * 100) for i in range(1, 9)]
    insert(client, "r", [tuple(r[c] for c in cols) for r in rs])
    assert ordered(rows(client, "SELECT v, id FROM r ORDER BY id LIMIT 3")) == [
        ((("v", r["v"]), ("id", r["id"])), 1) for r in rs[:3]]
    assert _read(client, "SELECT id, v FROM r") == (["id", "v"], {(r["id"], r["v"]): 1 for r in rs})
    assert _read(client, "SELECT * FROM r") == (list(cols), {tuple(r[c] for c in cols): 1 for r in rs})
