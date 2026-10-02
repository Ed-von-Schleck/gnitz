"""Foreign keys: which writes the constraint admits, and which it refuses.

A write is checked against the *fold* it produces, not against committed state
row by row: a child the same write removes exempts its parent, and a referenced
value the write re-adds under another row never leaves. That holds on the
statement path (autocommit or one transaction) and the plain path (binary
push/delete) alike, and across workers — the suite runs at GNITZ_WORKERS=4. The
declarations the planner refuses are pinned in its own tests.
"""

import contextlib

import pytest
import gnitz
from _read import bag, scanned
from _serverproc import NEEDS_MULTI
from _sql import insert

_PARENT = "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"
_CHILD = ("CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY,"
          " pid BIGINT NOT NULL REFERENCES parent(id))")
_FK_PAIR = f"{_PARENT}; {_CHILD}"
_TREE = "CREATE TABLE tree (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT REFERENCES tree(id))"
# A non-PK UNIQUE target, whose referenced value is resolved from committed
# storage through the batched gather.
_CODE_PAIR = ("CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, code BIGINT UNSIGNED); "
              "CREATE UNIQUE INDEX p_code ON p(code); "
              "CREATE TABLE c (cid BIGINT PRIMARY KEY, ref BIGINT UNSIGNED REFERENCES p(code))")


def _seed(client, ddl, seed):
    client.execute_sql(ddl)
    for table, rows in seed.items():
        if rows:
            insert(client, table, rows)


def _held(client, tables):
    """Each named table's bag."""
    return {t: bag(scanned(client, t)) for t in tables}


def _want(rows_by_table):
    return {t: dict.fromkeys(rows, 1) for t, rows in rows_by_table.items()}


def _refused():
    return pytest.raises(gnitz.GnitzIntegrityError, match="(?i)foreign key")


# ── A reference lands only on a live target ──────────────────────────────────

# `(ddl, rows seeded per table, a (table, row) referencing nothing)`.
_ENFORCED = {
    "inline-references": (_FK_PAIR, {"parent": [(1, 100)], "child": [(10, 1)]}, ("child", (11, 999))),
    "table-level": (
        _PARENT + "; CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL,"
                  " FOREIGN KEY (pid) REFERENCES parent(id))",
        {"parent": [(1, 100)], "child": [(10, 1)]}, ("child", (11, 999))),
    # The existence check broadcasts, so the parent's placement decides what
    # each worker answers from.
    "replicated-parent": pytest.param(
        _PARENT + " WITH (replicated = true); " + _CHILD,
        {"parent": [(1, 100)], "child": [(10, 1)]}, ("child", (11, 999)), marks=NEEDS_MULTI),
    # A NULL cell references nothing, so it is not checked.
    "null-reference": (
        _PARENT + "; CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT REFERENCES parent(id))",
        {"parent": [], "child": [(10, None)]}, ("child", (11, 999))),
    # The probe key's type and width come from the parent's PK column, not from
    # its first declared column.
    "parent-pk-not-first": (
        "CREATE TABLE parent (label TEXT NOT NULL, id BIGINT NOT NULL PRIMARY KEY); " + _CHILD,
        {"parent": [("alice", 42)], "child": [(1, 42)]}, ("child", (2, 999))),
    # An FK column that is also the PK has no payload slot, so the check reads
    # the value out of the key region.
    "fk-column-is-the-pk": (
        "CREATE TABLE parent (id BIGINT UNSIGNED PRIMARY KEY); "
        "CREATE TABLE child (cid BIGINT UNSIGNED PRIMARY KEY REFERENCES parent(id))",
        {"parent": [(1,)], "child": [(1,)]}, ("child", (99,))),
    # A row of the same batch satisfies a self-reference — its own included —
    # where a committed-state probe would refuse both.
    "self-reference": (_TREE, {"tree": [(1, None), (2, 1), (3, 3)]}, ("tree", (4, 99))),
}


@pytest.mark.parametrize("ddl,seed,dangling", _ENFORCED.values(), ids=_ENFORCED.keys())
def test_a_reference_lands_only_on_a_live_target(client, ddl, seed, dangling):
    _seed(client, ddl, seed)
    assert _held(client, seed) == _want(seed)
    table, row = dangling
    with _refused():
        insert(client, table, [row])
    assert _held(client, seed) == _want(seed)


# ── A write lands only if every reference survives its fold ──────────────────

_PARENT_CHILD = {"parent": [(1, 100), (2, 200)], "child": [(10, 1)]}
_TWO_CHILDREN = _PARENT + "".join(
    f"; CREATE TABLE {n} (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES parent(id))"
    for n in ("child1", "child2"))
_TREE_ROWS = {"tree": [(1, None), (2, 1)]}
_CODES = [(i, 1000 + i) for i in range(1, 21)]
# Children reference the codes of pid 1..10; pid 11..20 are unreferenced.
_CODE_ROWS = {"p": _CODES, "c": _CODES[:10]}
_NULL_CODE = {"p": [(1, None), (2, 500)], "c": [(1, 500)]}
_TWO_COLUMNS = (
    "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, code BIGINT UNSIGNED NOT NULL,"
    " email BIGINT UNSIGNED NOT NULL); "
    "CREATE UNIQUE INDEX ON p(code); CREATE UNIQUE INDEX ON p(email); "
    "CREATE TABLE c1 (cid BIGINT PRIMARY KEY, ref BIGINT UNSIGNED REFERENCES p(code)); "
    "CREATE TABLE c2 (cid BIGINT PRIMARY KEY, eref BIGINT UNSIGNED REFERENCES p(email))")
_TWO_COLUMN_ROWS = {"p": [(1, 1001, 7001), (2, 1002, 7002), (3, 1003, 7003)],
                    "c1": [(1, 1001)], "c2": [(1, 7002)]}
# An FK column inside the child's PK, alone or as a non-leading member.
_PK_CHILD = ("CREATE TABLE parent (id BIGINT UNSIGNED PRIMARY KEY); "
             "CREATE TABLE child (cid BIGINT UNSIGNED PRIMARY KEY REFERENCES parent(id))")
_MEMBER_CHILD = ("CREATE TABLE parent (id BIGINT UNSIGNED PRIMARY KEY); "
                 "CREATE TABLE child (a BIGINT UNSIGNED REFERENCES parent(id), b BIGINT UNSIGNED,"
                 " PRIMARY KEY (b, a))")
_LOOP = "CREATE TABLE loop (id BIGINT PRIMARY KEY REFERENCES loop(id))"

# `(ddl, rows seeded per table, statements, every table's rows after — or None
# where the statements are refused and every table keeps its seed)`.
_STATEMENTS = {
    "a referenced parent's delete": (_FK_PAIR, _PARENT_CHILD, "DELETE FROM parent WHERE id = 1", None),
    "an unreferenced parent's delete": (
        _FK_PAIR, _PARENT_CHILD, "DELETE FROM parent WHERE id = 2",
        {"parent": [(1, 100)], "child": [(10, 1)]}),
    "a leaf, then its parent": (
        _FK_PAIR, _PARENT_CHILD, "DELETE FROM child WHERE cid = 10; DELETE FROM parent WHERE id = 1",
        {"parent": [(2, 200)], "child": []}),
    # An UPDATE of the FK column is checked against the value it writes...
    "a child re-pointed at nothing": (_FK_PAIR, _PARENT_CHILD, "UPDATE child SET pid = 999 WHERE cid = 10", None),
    "a child re-pointed at a live parent": (
        _FK_PAIR, _PARENT_CHILD, "UPDATE child SET pid = 2 WHERE cid = 10",
        {"parent": [(1, 100), (2, 200)], "child": [(10, 2)]}),
    # ...and one of a referenced parent's payload is an upsert, never a removal.
    "a referenced parent's payload": (
        _FK_PAIR, _PARENT_CHILD, "UPDATE parent SET val = 999 WHERE id = 1",
        {"parent": [(1, 999), (2, 200)], "child": [(10, 1)]}),
    # One transaction folds whole, so statement order and a retire-then-restore
    # of a referenced key never matter.
    "a child before its parent": (
        _FK_PAIR, {"parent": [], "child": []},
        "BEGIN; INSERT INTO child VALUES (1, 100); INSERT INTO parent VALUES (100, 0); COMMIT",
        {"parent": [(100, 0)], "child": [(1, 100)]}),
    "a parent re-inserted beside a new child": (
        _FK_PAIR, {"parent": [(100, 0)], "child": []},
        "BEGIN; DELETE FROM parent WHERE id = 100; INSERT INTO parent VALUES (100, 1); "
        "INSERT INTO child VALUES (1, 100); COMMIT",
        {"parent": [(100, 1)], "child": [(1, 100)]}),
    "a child re-pointed off the parent the bundle deletes": (
        _FK_PAIR, {"parent": [(100, 0), (200, 0)], "child": [(1, 100)]},
        "BEGIN; DELETE FROM parent WHERE id = 100; UPDATE child SET pid = 200 WHERE cid = 1; COMMIT",
        {"parent": [(200, 0)], "child": [(1, 200)]}),
    "a child inserted under the parent the bundle deletes": (
        _FK_PAIR, {"parent": [(100, 0)], "child": []},
        "BEGIN; DELETE FROM parent WHERE id = 100; INSERT INTO child VALUES (1, 100); COMMIT", None),
    # Whichever child still holds a reference blocks; one holding none does not.
    "a parent one of two children references": (
        _TWO_CHILDREN, {"parent": [(1, 100)], "child1": [(10, 1)], "child2": []},
        "DELETE FROM parent WHERE id = 1", None),
    "that child retired, then the parent": (
        _TWO_CHILDREN, {"parent": [(1, 100)], "child1": [(10, 1)], "child2": []},
        "DELETE FROM child1 WHERE cid = 10; DELETE FROM parent WHERE id = 1",
        {"parent": [], "child1": [], "child2": []}),
    # RESTRICT finds the children by seeking the child relation, so a child at a
    # negative PK has to be reachable by that seek.
    "a parent of children at negative keys": (
        _FK_PAIR, {"parent": [(1, 0)], "child": [(-5, 1), (-1, 1)]}, "DELETE FROM parent WHERE id = 1", None),
    "a referenced tree row": (_TREE, _TREE_ROWS, "DELETE FROM tree WHERE id = 1", None),
    # A child removed by the same statement does not block its parent.
    "a whole tree": (_TREE, _TREE_ROWS, "DELETE FROM tree", {"tree": []}),
    "a tree, leaf first": (
        _TREE, _TREE_ROWS, "DELETE FROM tree WHERE id = 2; DELETE FROM tree WHERE id = 1", {"tree": []}),
    # A bulk write over many referenced codes, and the same write over the
    # unreferenced half.
    "a bulk delete of referenced codes": (_CODE_PAIR, _CODE_ROWS, "DELETE FROM p WHERE pid <= 10", None),
    "a bulk delete of unreferenced codes": (
        _CODE_PAIR, _CODE_ROWS, "DELETE FROM p WHERE pid > 10", {"p": _CODES[:10], "c": _CODES[:10]}),
    "a bulk renumbering of referenced codes": (
        _CODE_PAIR, _CODE_ROWS, "UPDATE p SET code = code + 500000 WHERE pid <= 10", None),
    "a bulk renumbering of unreferenced codes": (
        _CODE_PAIR, _CODE_ROWS, "UPDATE p SET code = code + 500000 WHERE pid > 10",
        {"p": _CODES[:10] + [(pid, code + 500000) for pid, code in _CODES[10:]], "c": _CODES[:10]}),
    # A NULL code blocks no child, and deleting an absent PK is a no-op.
    "a NULL code and an absent row": (
        _CODE_PAIR, _NULL_CODE, "DELETE FROM p WHERE pid = 1; DELETE FROM p WHERE pid = 999",
        {"p": [(2, 500)], "c": [(1, 500)]}),
    "a referenced code beside a NULL one": (_CODE_PAIR, _NULL_CODE, "DELETE FROM p WHERE pid = 2", None),
    # Two children through two UNIQUE columns: each child's check sees its own
    # referenced value.
    "a row the code child references": (_TWO_COLUMNS, _TWO_COLUMN_ROWS, "DELETE FROM p WHERE pid = 1", None),
    "a row the email child references": (_TWO_COLUMNS, _TWO_COLUMN_ROWS, "DELETE FROM p WHERE pid = 2", None),
    "a row neither child references": (
        _TWO_COLUMNS, _TWO_COLUMN_ROWS, "DELETE FROM p WHERE pid = 3",
        {**_TWO_COLUMN_ROWS, "p": _TWO_COLUMN_ROWS["p"][:2]}),
    "a parent its PK child references": (
        _PK_CHILD, {"parent": [(1,), (2,)], "child": [(1,)]}, "DELETE FROM parent WHERE id = 1", None),
    "a parent no PK child references": (
        _PK_CHILD, {"parent": [(1,), (2,)], "child": [(1,)]}, "DELETE FROM parent WHERE id = 2",
        {"parent": [(1,)], "child": [(1,)]}),
    "a parent a PK member references": (
        _MEMBER_CHILD, {"parent": [(1,), (2,)], "child": [(1, 7)]}, "DELETE FROM parent WHERE id = 1", None),
    "a parent no PK member references": (
        _MEMBER_CHILD, {"parent": [(1,), (2,)], "child": [(1, 7)]}, "DELETE FROM parent WHERE id = 2",
        {"parent": [(1,)], "child": [(1, 7)]}),
    # A row referencing itself exempts itself.
    "a self-referencing PK row": (_LOOP, {"loop": [(1,)]}, "DELETE FROM loop", {"loop": []}),
}


@pytest.mark.parametrize("ddl,seed,stmts,after", _STATEMENTS.values(), ids=_STATEMENTS.keys())
def test_a_statement_lands_only_if_every_reference_survives_its_fold(
        client, ddl, seed, stmts, after):
    _seed(client, ddl, seed)
    with _refused() if after is None else contextlib.nullcontext():
        client.execute_sql(stmts)
    assert _held(client, seed) == _want(after or seed)


# `(ddl, rows seeded per table, (verb, table, rows pushed or pks deleted), every
# table's rows after — or None where the write is refused)`.
_WRITES = {
    # `retract(1)` beside `insert(2, parent=1)`: probing committed state finds 1
    # present and would leave a durable dangling reference.
    "a push removing the parent it references": (
        _TREE, _TREE_ROWS,
        ("push", "tree", [{"id": 1, "parent_id": None, "_weight": -1}, {"id": 2, "parent_id": 1}]), None),
    # `insert(1)` then `retract(1)` nets to zero, yet the apply removes the
    # committed row 1 — which row 2 references.
    "a push netting a referenced row to zero": (
        _TREE, _TREE_ROWS,
        ("push", "tree", [{"id": 1, "parent_id": None}, {"id": 1, "parent_id": None, "_weight": -1}]), None),
    "a push referencing nothing": (
        _TREE, {"tree": []}, ("push", "tree", [{"id": 3, "parent_id": 99}]), None),
    "a delete of a referenced row": (_TREE, _TREE_ROWS, ("delete", "tree", [1]), None),
    # The only row referencing 1 leaves in the same push.
    "a delete of a parent and its child": (_TREE, _TREE_ROWS, ("delete", "tree", [1, 2]), {"tree": []}),
    # No referenced value leaves the parent, so the reference is intact at the
    # end of the write: the engine checks NO ACTION, not per-row RESTRICT.
    "a push swapping two codes": (
        _CODE_PAIR, {"p": [(1, 100), (2, 200)], "c": [(1, 100)]},
        ("push", "p", [{"pid": 1, "code": 200}, {"pid": 2, "code": 100}]),
        {"p": [(1, 200), (2, 100)], "c": [(1, 100)]}),
    # A delete carries filler payload, so the retired code is read from the
    # committed row.
    "a delete retiring a referenced code": (
        _CODE_PAIR, {"p": [(1, 100), (2, 200), (3, 300)], "c": [(1, 200)]}, ("delete", "p", [1, 2]), None),
    "a delete retiring unreferenced codes": (
        _CODE_PAIR, {"p": [(1, 100), (2, 200), (3, 300)], "c": [(1, 200)]}, ("delete", "p", [1, 3]),
        {"p": [(2, 200)], "c": [(1, 200)]}),
}


@pytest.mark.parametrize("ddl,seed,write,after", _WRITES.values(), ids=_WRITES.keys())
def test_a_binary_write_lands_only_if_every_reference_survives_its_fold(
        client, ddl, seed, write, after):
    _seed(client, ddl, seed)
    verb, table, payload = write
    tid, schema = client.resolve_table(table)

    with _refused() if after is None else contextlib.nullcontext():
        if verb == "push":
            client.push(tid, gnitz.ZSetBatch(schema).extend(payload))
        else:
            client.delete(tid, schema, payload)
    assert _held(client, seed) == _want(after or seed)


def test_a_refused_parent_write_names_its_verb(client):
    """Through a non-PK unique column: moving a referenced code is refused as an
    update, removing its row as a delete, and an unreferenced code moves."""
    _seed(client, _CODE_PAIR, _CODE_ROWS)
    refused = lambda verb: pytest.raises(gnitz.GnitzIntegrityError, match=f"cannot {verb} ")
    with refused("update"):
        client.execute_sql("UPDATE p SET code = 5 WHERE pid = 3")
    with refused("delete from"):
        client.execute_sql("DELETE FROM p WHERE pid = 3")
    client.execute_sql("UPDATE p SET code = 5 WHERE pid = 15")
    moved = [(15, 5) if pid == 15 else (pid, code) for pid, code in _CODES]
    assert _held(client, _CODE_ROWS) == _want({"p": moved, "c": _CODES[:10]})


def test_restrict_over_more_values_than_one_write_carries(client):
    """`DELETE FROM child; DELETE FROM parent;` in one transaction over 1500
    referenced values that each still have a committed child.

    The check is bounded by the write's own row count, not by the number of
    values it asks about: every committed holder the probe returns is a row the
    same bundle retires, and the per-value holder cap the worker honours is the
    bundle's child-row count. Leaving a single child behind must still be fatal,
    whichever order the workers answer in.
    """
    n = 1500
    seed = {"parent": [(i, i * 7) for i in range(n)], "child": [(i, i) for i in range(n)]}
    _seed(client, _FK_PAIR, seed)

    with _refused():
        client.execute_sql("BEGIN; DELETE FROM child WHERE cid <> 777; DELETE FROM parent; COMMIT")
    assert _held(client, seed) == _want(seed)

    client.execute_sql("BEGIN; DELETE FROM child; DELETE FROM parent; COMMIT")
    assert _held(client, seed) == {"parent": {}, "child": {}}


# ── What a declaration registers and what it gates ───────────────────────────

def test_only_a_base_table_registers_an_fk_and_a_self_reference_names_itself(client):
    client.execute_sql(_TREE + "; CREATE VIEW tree_v AS SELECT id, parent_id FROM tree")
    tid, _ = client.resolve_table("tree")
    vid, _ = client.resolve_table("tree_v")
    fks = {(r.owner_id, r.col_idx): r.fk_table_id
           for r in client.scan(gnitz.COL_TAB, gnitz.sys_schema(gnitz.COL_TAB)) if r.owner_id in (tid, vid)}
    assert fks == {(tid, 0): 0, (tid, 1): tid, (vid, 0): 0, (vid, 1): 0}


def test_dup_name_fk_create_leaves_no_phantom_child(client):
    """A duplicate-name CREATE TABLE whose column REFERENCES a parent must not
    strand a phantom FK child: the whole CREATE bundle rolls back, so nothing
    blocks dropping the parent afterwards."""
    client.execute_sql(_PARENT + "; CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)")
    with pytest.raises(gnitz.GnitzRefusedError, match="already exists"):
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY,"
            " pref BIGINT NOT NULL REFERENCES parent(id))")
    # The real `t` has no FK to parent, so the ONLY thing that could block this
    # is a stranded phantom child.
    client.execute_sql("DROP TABLE parent")


def test_drop_self_referential_table_with_rows(client):
    """The drop guard skips a blocking child that is itself in the drop set, and
    for a self-FK the child *is* the table being dropped."""
    _seed(client, _TREE, _TREE_ROWS)
    client.execute_sql("DROP TABLE tree")
    with pytest.raises(gnitz.GnitzNotFoundError):
        client.resolve_table("tree")


def test_the_unique_index_an_fk_resolves_through_cannot_be_dropped(client):
    """The index is how the referenced value is resolved, so dropping it would
    leave the constraint with no way to answer — the same gate `DROP TABLE` on
    the parent has, one level down."""
    client.execute_sql(_CODE_PAIR)
    with pytest.raises(gnitz.GnitzRefusedError, match="(?i)integrity"):
        client.execute_sql("DROP INDEX p_code")
    client.execute_sql("DROP TABLE c; DROP INDEX p_code")


@NEEDS_MULTI
def test_restrict_survives_dropping_a_unique_index_on_the_fk_column(client):
    """Dropping a UNIQUE index on an FK column keeps the column's FK circuit on
    every worker."""
    client.execute_sql(
        _PARENT + "; "
        # Moves the object-id counter, which FK circuits must not depend on.
        "CREATE INDEX parent_val ON parent(val); "
        "CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY,"
        " pid BIGINT NOT NULL UNIQUE REFERENCES parent(id))")
    insert(client, "parent", [(1, 100)])
    insert(client, "child", [(10, 1)])
    client.execute_sql(f"DROP INDEX {client.schema}__child__idx_pid")
    with _refused():
        client.execute_sql("DELETE FROM parent WHERE id = 1")
    client.execute_sql("DROP TABLE child; DROP TABLE parent")
