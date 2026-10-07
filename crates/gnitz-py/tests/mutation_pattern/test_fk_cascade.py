"""ON DELETE CASCADE: what a delete of a referenced row takes with it, decided
by the write's fold and committed with the write or not at all."""

import contextlib

import pytest
import gnitz
import _fk
from _serverproc import NEEDS_MULTI


def _chain(a="", b="", c="", last="CASCADE"):
    """`a ◄ b ◄ c`, the first edge cascading and the second under `last`, each
    table under its own `WITH` clause."""
    return (
        f"CREATE TABLE a (id BIGINT PRIMARY KEY, val BIGINT NOT NULL){a}; "
        f"CREATE TABLE b (id BIGINT PRIMARY KEY, a_id BIGINT NOT NULL REFERENCES a(id) ON DELETE CASCADE){b}; "
        f"CREATE TABLE c (id BIGINT PRIMARY KEY, label TEXT NOT NULL,"
        f" b_id BIGINT NOT NULL REFERENCES b(id) ON DELETE {last}){c}"
    )


_PER_B = "; CREATE VIEW per_b AS SELECT b_id, COUNT(*) AS n FROM c GROUP BY b_id"
_REPL = " WITH (replicated = true)"
# Four `a` rows, three `b` rows under each and two `c` rows under each of those.
_A = [(i, i * 10) for i in range(1, 5)]
_B = [(a * 10 + j, a) for a, _ in _A for j in range(3)]
_C = [(b * 10 + k, f"c{b}-{k}", b) for b, _ in _B for k in range(2)]
_CHAIN_ROWS = {"a": _A, "b": _B, "c": _C}
_PLACEMENTS = {
    "partitioned": {},
    "replicated-parent": pytest.param({"a": _REPL}, marks=NEEDS_MULTI),
    "replicated-child": pytest.param({"b": _REPL, "c": _REPL}, marks=NEEDS_MULTI),
    "all-replicated": pytest.param({"a": _REPL, "b": _REPL, "c": _REPL}, marks=NEEDS_MULTI),
}


def _chain_after(gone):
    """The chain's rows once the `a` rows `gone` and their descendants left."""
    b = [r for r in _B if r[1] not in gone]
    kept = {r[0] for r in b}
    return {"a": [r for r in _A if r[0] not in gone], "b": b, "c": [r for r in _C if r[2] in kept]}


def _chain_want(gone):
    """The bags of the chain, and of `per_b` over it, once the `a` rows `gone` left."""
    after = _chain_after(gone)
    return {**_fk.want(after), "per_b": dict.fromkeys(((b, 2) for b, _ in after["b"]), 1)}


@pytest.mark.parametrize("placement", _PLACEMENTS.values(), ids=_PLACEMENTS.keys())
def test_a_delete_takes_every_level_of_descendants_and_a_view_follows(client, placement):
    _fk.seed(client, _chain(**placement) + _PER_B, _CHAIN_ROWS)
    want = _chain_want(set())
    assert _fk.held(client, want) == want

    client.execute_sql("DELETE FROM a WHERE id IN (2, 3)")
    want = _chain_want({2, 3})
    assert _fk.held(client, want) == want

    # A row with no descendants left, and one that never existed.
    client.execute_sql("DELETE FROM b WHERE a_id = 1; DELETE FROM a WHERE id IN (1, 99)")
    want = _chain_want({1, 2, 3})
    assert _fk.held(client, want) == want


def test_a_binary_delete_cascades_as_the_statement_does(client):
    _fk.seed(client, _chain(), _CHAIN_ROWS)
    client.delete(*client.resolve_table("a"), [2, 3])
    assert _fk.held(client, _CHAIN_ROWS) == _fk.want(_chain_after({2, 3}))

    # A push whose retraction and insert fold to a surviving row deletes nothing.
    tid, schema = client.resolve_table("a")
    client.push(tid, gnitz.ZSetBatch(schema).extend([{"id": 1, "val": 10, "_weight": -1}, {"id": 1, "val": 11}]))
    after = _chain_after({2, 3})
    after["a"] = [(1, 11), (4, 40)]
    assert _fk.held(client, _CHAIN_ROWS) == _fk.want(after)


def test_a_cascade_outlasts_the_pushs_own_write_of_the_row(client):
    """One push deletes row 1 and rewrites row 2 under it: the cascade's delete
    of row 2 is applied behind the push's own row."""
    _fk.seed(client, _TREE, {"tree": [(1, None), (2, 1), (3, 2), (4, None)]})
    tid, schema = client.resolve_table("tree")
    client.push(tid, gnitz.ZSetBatch(schema).extend(
        [{"id": 1, "parent_id": None, "_weight": -1}, {"id": 2, "parent_id": 1}]))
    assert _fk.held(client, ["tree"]) == _fk.want({"tree": [(4, None)]})


def test_one_restricting_reference_refuses_the_whole_delete(client):
    _fk.seed(client, _chain(last="RESTRICT"), _CHAIN_ROWS)
    with _fk.refused():
        client.execute_sql("DELETE FROM a WHERE id IN (2, 3)")
    assert _fk.held(client, _CHAIN_ROWS) == _fk.want(_CHAIN_ROWS)

    # Once nothing restricts the rows the cascade reaches, it runs.
    client.execute_sql("DELETE FROM c WHERE b_id >= 20 AND b_id < 40; DELETE FROM a WHERE id IN (2, 3)")
    assert _fk.held(client, _CHAIN_ROWS) == _fk.want(_chain_after({2, 3}))


_TREE = ("CREATE TABLE tree (id BIGINT PRIMARY KEY,"
         " parent_id BIGINT REFERENCES tree(id) ON DELETE CASCADE)")
# Two trees of depth six, three children under every inner node of the first.
_TREE_A = [(1, None)] + [(n * 10 + k, n) for n in (1, 11, 111, 1111, 11111) for k in (1, 2, 3)]
_TREE_B = [(2, None)] + [(2 * 10**d + 2, 2 * 10**(d - 1) + (2 if d > 1 else 0)) for d in range(1, 6)]
_TWO_PARENTS = (
    "CREATE TABLE parent (id BIGINT PRIMARY KEY); "
    "CREATE TABLE child (cid BIGINT PRIMARY KEY,"
    " p1 BIGINT REFERENCES parent(id) ON DELETE CASCADE,"
    " p2 BIGINT REFERENCES parent(id) ON DELETE CASCADE); "
    "CREATE VIEW per_p1 AS SELECT p1, COUNT(*) AS n FROM child GROUP BY p1"
)
_PAIR = (
    "CREATE TABLE parent (id BIGINT PRIMARY KEY, val BIGINT NOT NULL); "
    "CREATE TABLE child (cid BIGINT PRIMARY KEY, note BIGINT NOT NULL,"
    " pid BIGINT NOT NULL REFERENCES parent(id) ON DELETE CASCADE)"
)
_PAIR_ROWS = {"parent": [(1, 100), (2, 200)], "child": [(10, 0, 1), (11, 0, 1), (12, 0, 1), (20, 0, 2)]}
_CODE = (
    "CREATE TABLE p (pid BIGINT PRIMARY KEY, code BIGINT); "
    "CREATE UNIQUE INDEX p_code ON p(code); "
    "CREATE TABLE c (cid BIGINT PRIMARY KEY, ref BIGINT REFERENCES p(code) ON DELETE CASCADE)"
)
_CODE_ROWS = {"p": [(1, 100), (2, 200), (3, None)], "c": [(10, 100), (11, 100), (20, 200), (30, None)]}
# The second level's referenced columns: a member of a compound key, read out of
# a cascaded row's key, and a payload column, read from the committed row.
_DEEP = (
    "CREATE TABLE a (id BIGINT PRIMARY KEY); "
    "CREATE TABLE b (x BIGINT, y BIGINT, a_id BIGINT NOT NULL REFERENCES a(id) ON DELETE CASCADE,"
    " code BIGINT, PRIMARY KEY (x, y)); "
    "CREATE UNIQUE INDEX b_y ON b(y); CREATE UNIQUE INDEX b_code ON b(code); "
    "CREATE TABLE by_key (id BIGINT PRIMARY KEY, y BIGINT REFERENCES b(y) ON DELETE CASCADE); "
    "CREATE TABLE by_code (id BIGINT PRIMARY KEY, code BIGINT REFERENCES b(code) ON DELETE CASCADE); "
    "CREATE TABLE leaf (id BIGINT PRIMARY KEY, r BIGINT REFERENCES by_code(id) ON DELETE CASCADE)"
)
_DEEP_ROWS = {
    "a": [(1,), (2,)],
    "b": [(7, 70, 1, 700), (7, 71, 1, None), (8, 80, 2, 800)],
    "by_key": [(1, 70), (2, 71), (3, 80), (4, None)],
    "by_code": [(1, 700), (2, 800), (3, None)],
    "leaf": [(1, 1), (2, 2), (3, 3)],
}
# `c` references `p.code`, and `p` cascades from `a`.
_MOVED = (
    "CREATE TABLE a (id BIGINT PRIMARY KEY); "
    "CREATE TABLE p (pid BIGINT PRIMARY KEY, a_id BIGINT REFERENCES a(id) ON DELETE CASCADE, code BIGINT); "
    "CREATE UNIQUE INDEX p_code ON p(code); "
    "CREATE TABLE c (cid BIGINT PRIMARY KEY, ref BIGINT REFERENCES p(code) ON DELETE CASCADE)"
)
_MOVED_ROWS = {"a": [(1,), (2,)], "p": [(1, 1, 100), (2, None, 200), (3, 2, 300)], "c": [(10, 200), (11, 300)]}

# `(ddl, rows seeded per table, statements, every table's rows after — or None
# where the statements are refused and every table keeps its seed)`.
_STATEMENTS = {
    "a root's whole subtree, and not the other tree": (
        _TREE, {"tree": _TREE_A + _TREE_B}, "DELETE FROM tree WHERE id = 1", {"tree": _TREE_B}),
    "an inner node's subtree": (
        _TREE, {"tree": _TREE_A + _TREE_B}, "DELETE FROM tree WHERE id = 111",
        {"tree": [r for r in _TREE_A if not str(r[0]).startswith("111")] + _TREE_B}),
    # The cascade comes back round to the row the statement deletes.
    "a ring": (
        _TREE, {"tree": [(1, 3), (2, 1), (3, 2), (4, 4), (5, None)]}, "DELETE FROM tree WHERE id = 1",
        {"tree": [(4, 4), (5, None)]}),
    "a row referencing only itself": (
        _TREE, {"tree": [(4, 4), (5, 4)]}, "DELETE FROM tree WHERE id = 4", {"tree": []}),
    # Row 10 is reached through both columns, row 11 through one; a NULL
    # reference is none.
    "a row reached through two columns": (
        _TWO_PARENTS,
        {"parent": [(1,), (2,), (3,)],
         "child": [(10, 1, 2), (11, 1, None), (12, None, 3), (13, 3, 3), (14, None, None), (15, 3, 1)]},
        "DELETE FROM parent WHERE id IN (1, 2)",
        {"parent": [(3,)], "child": [(12, None, 3), (13, 3, 3), (14, None, None)],
         "per_p1": [(None, 2), (3, 1)]}),
    # The fold decides: a child the transaction moves away or deletes itself is
    # the transaction's, and the rest are the cascade's.
    "a child moved off the parent the transaction deletes": (
        _PAIR, _PAIR_ROWS,
        "BEGIN; UPDATE child SET pid = 2 WHERE cid = 10; DELETE FROM child WHERE cid = 11; "
        "DELETE FROM parent WHERE id = 1; COMMIT",
        {"parent": [(2, 200)], "child": [(10, 0, 2), (20, 0, 2)]}),
    "a child updated under the parent the transaction deletes": (
        _PAIR, _PAIR_ROWS,
        "BEGIN; UPDATE child SET note = 7 WHERE cid = 10; DELETE FROM parent WHERE id = 1; COMMIT",
        {"parent": [(2, 200)], "child": [(20, 0, 2)]}),
    "a child inserted under the parent the transaction deletes": (
        _PAIR, _PAIR_ROWS,
        "BEGIN; DELETE FROM parent WHERE id = 1; INSERT INTO child VALUES (13, 0, 1); COMMIT", None),
    # A parent replaced under its key has not left.
    "a parent deleted and re-inserted": (
        _PAIR, _PAIR_ROWS,
        "BEGIN; DELETE FROM parent WHERE id = 1; INSERT INTO parent VALUES (1, 101); COMMIT",
        {**_PAIR_ROWS, "parent": [(1, 101), (2, 200)]}),
    "a parent's payload update": (
        _PAIR, _PAIR_ROWS, "UPDATE parent SET val = 101 WHERE id = 1",
        {**_PAIR_ROWS, "parent": [(1, 101), (2, 200)]}),
    # Through a UNIQUE column that is no key: the delete of its row cascades, a
    # NULL code references nothing, and moving the code off a surviving row is
    # an update, which no action covers.
    "a referenced code's row": (
        _CODE, _CODE_ROWS, "DELETE FROM p WHERE pid = 1",
        {"p": [(2, 200), (3, None)], "c": [(20, 200), (30, None)]}),
    "a row holding a NULL code": (
        _CODE, _CODE_ROWS, "DELETE FROM p WHERE pid = 3", {"p": [(1, 100), (2, 200)], "c": _CODE_ROWS["c"]}),
    "a referenced code moved off its row": (_CODE, _CODE_ROWS, "UPDATE p SET code = 101 WHERE pid = 1", None),
    "a referenced code handed to another row": (
        _CODE, _CODE_ROWS,
        "BEGIN; DELETE FROM p WHERE pid = 1; UPDATE p SET code = 100 WHERE pid = 3; COMMIT",
        {"p": [(2, 200), (3, 100)], "c": _CODE_ROWS["c"]}),
    "second-level references through a key member and a payload column": (
        _DEEP, _DEEP_ROWS, "DELETE FROM a WHERE id = 1",
        {"a": [(2,)], "b": [(8, 80, 2, 800)], "by_key": [(3, 80), (4, None)],
         "by_code": [(2, 800), (3, None)], "leaf": [(2, 2), (3, 3)]}),
    # Code 200 is handed to row 1, which the cascade from `a` then deletes: it
    # leaves with that row, and its references go with it.
    "a code moved onto a row the cascade deletes": (
        _MOVED, _MOVED_ROWS,
        "BEGIN; DELETE FROM p WHERE pid = 2; UPDATE p SET code = 200 WHERE pid = 1; "
        "DELETE FROM a WHERE id = 1; COMMIT",
        {"a": [(2,)], "p": [(3, 2, 300)], "c": [(11, 300)]}),
    # The same hand-over where nothing deletes row 1: the code has not left.
    "a code moved onto a row that stays": (
        _MOVED, _MOVED_ROWS,
        "BEGIN; DELETE FROM p WHERE pid = 2; UPDATE p SET code = 200 WHERE pid = 1; COMMIT",
        {"a": [(1,), (2,)], "p": [(1, 1, 200), (3, 2, 300)], "c": _MOVED_ROWS["c"]}),
}


@pytest.mark.parametrize("ddl,seed,stmts,after", _STATEMENTS.values(), ids=_STATEMENTS.keys())
def test_a_cascade_deletes_what_the_fold_leaves_referencing_a_deleted_row(client, ddl, seed, stmts, after):
    _fk.seed(client, ddl, seed)
    with _fk.refused() if after is None else contextlib.nullcontext():
        client.execute_sql(stmts)
    want = after or seed
    assert _fk.held(client, want) == _fk.want(want)


def test_a_restricting_edge_beside_a_cascading_one_still_refuses(client):
    """Two children of one parent: the cascading one's rows are no reason to
    spare the restricting one's."""
    ddl = ("CREATE TABLE parent (id BIGINT PRIMARY KEY); "
           "CREATE TABLE soft (id BIGINT PRIMARY KEY, pid BIGINT REFERENCES parent(id) ON DELETE CASCADE); "
           "CREATE TABLE hard (id BIGINT PRIMARY KEY, pid BIGINT REFERENCES parent(id) ON DELETE NO ACTION)")
    seed = {"parent": [(1,), (2,)], "soft": [(1, 1), (2, 2)], "hard": [(1, 1)]}
    _fk.seed(client, ddl, seed)
    with pytest.raises(gnitz.GnitzIntegrityError, match="cannot delete from .*parent.*referenced by .*hard"):
        client.execute_sql("DELETE FROM parent WHERE id = 1")
    assert _fk.held(client, seed) == _fk.want(seed)
    client.execute_sql("DELETE FROM parent WHERE id = 2")
    assert _fk.held(client, seed) == _fk.want({"parent": [(1,)], "soft": [(1, 1)], "hard": [(1, 1)]})


_WIDE = ("CREATE TABLE parent (id BIGINT PRIMARY KEY); "
         "CREATE TABLE child (cid BIGINT PRIMARY KEY, pid BIGINT NOT NULL REFERENCES parent(id) ON DELETE CASCADE)")


def test_one_writes_cascade_is_bounded(own_server):
    """A cascade past the budget is refused whole. Rows the write deletes itself
    are not the cascade's, and do not count against it."""
    own_server.start(extra_env={"GNITZ_CASCADE_BYTES": "4096"})
    n = 2000
    seed = {"parent": [(1,), (2,)], "child": [(i, 1) for i in range(n)] + [(n + i, 2) for i in range(20)]}
    with gnitz.connect(own_server.target) as conn:
        _fk.seed(conn, _WIDE, seed)
        with pytest.raises(gnitz.GnitzError, match="cascades to more rows of .*child.* than one write deletes"):
            conn.execute_sql("DELETE FROM parent WHERE id = 1")
        assert _fk.held(conn, seed) == _fk.want(seed)

        conn.execute_sql("DELETE FROM parent WHERE id = 2")
        seed = {"parent": [(1,)], "child": seed["child"][:n]}
        assert _fk.held(conn, seed) == _fk.want(seed)

        conn.execute_sql("BEGIN; DELETE FROM child WHERE cid >= 30; DELETE FROM parent WHERE id = 1; COMMIT")
        assert _fk.held(conn, seed) == {"parent": {}, "child": {}}


def test_a_cascade_survives_a_crash(own_server):
    """A SIGKILL runs no checkpoint, so the cascade comes back through SAL replay,
    at its own weights."""
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        _fk.seed(conn, _chain() + _PER_B, _CHAIN_ROWS)
        conn.execute_sql("DELETE FROM a WHERE id IN (2, 3)")
    want = _chain_want({2, 3})

    own_server.restart()
    with gnitz.connect(own_server.target) as conn:
        assert _fk.held(conn, want) == want
        conn.execute_sql("DELETE FROM a WHERE id = 4")
    want = _chain_want({2, 3, 4})

    own_server.restart()
    with gnitz.connect(own_server.target) as conn:
        assert _fk.held(conn, want) == want
