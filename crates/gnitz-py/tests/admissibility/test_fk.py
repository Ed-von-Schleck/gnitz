"""Foreign keys: what a declaration registers, and what the constraint refuses.

RESTRICT is checked against the *fold* a write produces, not against committed
state row by row: a child the same write removes exempts its parent, and a
referenced value the write re-adds under another row never leaves. That holds on
the transaction path (INSERT/UPDATE/DELETE) and the plain path (binary
push/delete) alike, and across workers — the suite runs at GNITZ_WORKERS=4.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/admissibility/test_fk.py -v --tb=short
"""
import threading

import pytest
import gnitz
from _uid import uid as _uid

_PARENT = "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"
_CHILD_INLINE = ("CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY,"
                 " pid BIGINT NOT NULL REFERENCES parent(id))")
_CHILD_TABLE_LEVEL = ("CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY,"
                      " pid BIGINT NOT NULL, FOREIGN KEY (pid) REFERENCES parent(id))")

# `p(pid, code UNIQUE)` with `c(cid, ref REFERENCES p(code))`: an FK whose target
# is a non-PK UNIQUE column, which resolves the referenced value from committed
# storage through the batched gather.
_P_CODE = ("CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY,"
           " code BIGINT UNSIGNED NOT NULL)")
_C_CODE = ("CREATE TABLE {name} (cid BIGINT PRIMARY KEY,"
           " ref BIGINT UNSIGNED REFERENCES p(code))")


def _race(*labelled_writes):
    """Run each `(label, fn)` on its own thread, released together by a barrier,
    and return the `(label, GnitzError)` pairs raised. Writes that contend for
    the same FK lock set serialize, so one of a conflicting pair is rejected."""
    errors = []
    start = threading.Barrier(len(labelled_writes))

    def run(label, fn):
        start.wait()
        try:
            fn()
        except gnitz.GnitzError as e:
            errors.append((label, e))

    threads = [threading.Thread(target=run, args=w) for w in labelled_writes]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return errors


@pytest.fixture
def fk_pair(client, schema_name):
    """`parent(id, val)` and `child(cid, pid -> parent.id)`."""
    client.execute_sql(_PARENT, schema_name=schema_name)
    client.execute_sql(_CHILD_INLINE, schema_name=schema_name)
    return schema_name


@pytest.fixture
def code_pair(client, schema_name):
    """`p(pid, code UNIQUE)` and `c(cid, ref -> p.code)`."""
    client.execute_sql(_P_CODE, schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=schema_name)
    client.execute_sql(_C_CODE.format(name="c"), schema_name=schema_name)
    return schema_name


@pytest.fixture
def tree(client, schema_name):
    """A self-referential `tree(id, parent_id -> tree.id)`; yields `(sn, tid, schema)`."""
    client.execute_sql(
        "CREATE TABLE tree (id BIGINT NOT NULL PRIMARY KEY,"
        " parent_id BIGINT REFERENCES tree(id))",
        schema_name=schema_name)
    tid, schema = client.resolve_table(schema_name, "tree")
    return schema_name, tid, schema


# ── Declaring the constraint ─────────────────────────────────────────────────

@pytest.mark.parametrize("child_ddl", [_CHILD_INLINE, _CHILD_TABLE_LEVEL],
                         ids=["inline-references", "table-level"])
def test_fk_declared_and_enforced(client, schema_name, child_ddl):
    """Both spellings register the same constraint: a child row referencing a
    live parent lands, one referencing nothing is refused."""
    client.execute_sql(_PARENT, schema_name=schema_name)
    assert client.execute_sql(child_ddl, schema_name=schema_name)[0]["type"] == "TableCreated"

    client.execute_sql("INSERT INTO parent VALUES (1, 100)", schema_name=schema_name)
    client.execute_sql("INSERT INTO child VALUES (10, 1)", schema_name=schema_name)
    assert [r.cid for r in client.scan(client.resolve_table(schema_name, "child")[0])] == [10]

    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.execute_sql("INSERT INTO child VALUES (11, 999)", schema_name=schema_name)


def test_fk_declaration_rejections(client, schema_name):
    """A target that cannot carry a reference: an unresolvable relation, and a
    column that is neither the PK nor a UNIQUE index."""
    with pytest.raises(gnitz.GnitzError, match="(?i)does not exist"):
        client.execute_sql(
            "CREATE TABLE c1 (cid BIGINT NOT NULL PRIMARY KEY,"
            " pid BIGINT NOT NULL REFERENCES phantom(id))", schema_name=schema_name)

    client.execute_sql(_PARENT, schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="(?i)primary key"):
        client.execute_sql(
            "CREATE TABLE c2 (cid BIGINT NOT NULL PRIMARY KEY,"
            " pid BIGINT NOT NULL REFERENCES parent(val))", schema_name=schema_name)


def test_fk_nullable_allows_null(client, schema_name):
    """A NULL FK cell references nothing, so it is not checked."""
    client.execute_sql(_PARENT, schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY,"
        " pid BIGINT REFERENCES parent(id))", schema_name=schema_name)
    client.execute_sql("INSERT INTO child VALUES (10, NULL)", schema_name=schema_name)
    tid, _ = client.resolve_table(schema_name, "child")
    assert [(r.cid, r.pid) for r in client.scan(tid)] == [(10, None)]


def test_fk_parent_pk_not_first_column(client, schema_name):
    """Regression: parent PK declared at a NON-leading column position.

    The distributed FK existence check (`build_check_batch`) resolved the probe
    key's type and width from the parent's first declared column instead of its
    PK column. Here the parent leads with a 16-byte STRING and its BIGINT PK is
    the second column, so the probe was encoded at the wrong width and zeroed
    out — a valid child reference was rejected (and a dangling one could be
    silently accepted if an all-zero parent key existed).
    """
    client.execute_sql(
        "CREATE TABLE users (label VARCHAR(255) NOT NULL, id BIGINT NOT NULL PRIMARY KEY)",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE orders (oid BIGINT NOT NULL PRIMARY KEY,"
        " user_id BIGINT NOT NULL REFERENCES users(id))", schema_name=schema_name)
    client.execute_sql("INSERT INTO users VALUES ('alice', 42)", schema_name=schema_name)
    # Valid reference to users(id = 42) must succeed (pre-fix: rejected).
    client.execute_sql("INSERT INTO orders VALUES (1, 42)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.execute_sql("INSERT INTO orders VALUES (2, 999)", schema_name=schema_name)


def test_dup_name_fk_create_leaves_no_phantom_child(client, schema_name):
    """A duplicate-name CREATE TABLE whose column REFERENCES a parent must not
    strand a phantom FK child. Pre-fix, the COL_TAB families committed as a
    separate RPC *before* the TABLE_TAB name check failed, so the running
    master's fk_by_parent[parent] carried a phantom child and DROP TABLE parent
    was blocked in the same session (and every reboot re-materialised it). Under
    atomic CREATE the whole bundle rolls back in master memory.
    """
    client.execute_sql(_PARENT, schema_name=schema_name)
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY,"
            " pref BIGINT NOT NULL REFERENCES parent(id))", schema_name=schema_name)
    # The real `t` has no FK to parent, so the ONLY thing that could block this
    # is a stranded phantom child.
    client.execute_sql("DROP TABLE parent", schema_name=schema_name)


def test_drop_is_gated_by_the_live_constraint(client, fk_pair):
    """DROP TABLE on a referenced parent is refused while the child table
    exists, and succeeds once it is gone — whether or not rows are present."""
    client.execute_sql("INSERT INTO parent VALUES (1, 100)", schema_name=fk_pair)
    client.execute_sql("INSERT INTO child VALUES (10, 1)", schema_name=fk_pair)

    with pytest.raises(gnitz.GnitzError, match="(?i)integrity violation"):
        client.execute_sql("DROP TABLE parent", schema_name=fk_pair)

    client.execute_sql("DROP TABLE child", schema_name=fk_pair)
    client.execute_sql("DROP TABLE parent", schema_name=fk_pair)


# ── RESTRICT on the transaction path ─────────────────────────────────────────

def test_delete_restrict(client, fk_pair):
    """A referenced parent row cannot be deleted; once the child that references
    it is gone it can, and an unreferenced row never blocked."""
    client.execute_sql("INSERT INTO parent VALUES (1, 100), (2, 200)", schema_name=fk_pair)
    client.execute_sql("INSERT INTO child VALUES (10, 1)", schema_name=fk_pair)
    ptid, _ = client.resolve_table(fk_pair, "parent")

    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.execute_sql("DELETE FROM parent WHERE id = 1", schema_name=fk_pair)
    # An unreferenced parent row was never blocked.
    client.execute_sql("DELETE FROM parent WHERE id = 2", schema_name=fk_pair)
    # Retiring the child frees the one that was.
    client.execute_sql("DELETE FROM child WHERE cid = 10", schema_name=fk_pair)
    client.execute_sql("DELETE FROM parent WHERE id = 1", schema_name=fk_pair)
    assert not list(client.scan(ptid))


def test_update_validates_the_new_value_only(client, fk_pair):
    """An UPDATE of the child's FK column is checked against the value it writes;
    an UPDATE of the parent's *payload* is an upsert, not a removal, so it is
    never a RESTRICT event even while the row is referenced."""
    client.execute_sql("INSERT INTO parent VALUES (1, 100), (2, 200)", schema_name=fk_pair)
    client.execute_sql("INSERT INTO child VALUES (10, 1)", schema_name=fk_pair)

    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.execute_sql("UPDATE child SET pid = 999 WHERE cid = 10", schema_name=fk_pair)
    client.execute_sql("UPDATE child SET pid = 2 WHERE cid = 10", schema_name=fk_pair)

    client.execute_sql("UPDATE parent SET val = 999 WHERE id = 2", schema_name=fk_pair)
    ptid, _ = client.resolve_table(fk_pair, "parent")
    assert sorted((r.id, r.val) for r in client.scan(ptid)) == [(1, 100), (2, 999)]


def test_delete_blocked_by_either_of_two_children(client, schema_name):
    """Two children on one parent: whichever still holds a reference blocks."""
    client.execute_sql(_PARENT, schema_name=schema_name)
    for name in ("child1", "child2"):
        client.execute_sql(
            f"CREATE TABLE {name} (cid BIGINT NOT NULL PRIMARY KEY,"
            " pid BIGINT NOT NULL REFERENCES parent(id))", schema_name=schema_name)
    client.execute_sql("INSERT INTO parent VALUES (1, 100)", schema_name=schema_name)
    client.execute_sql("INSERT INTO child1 VALUES (10, 1)", schema_name=schema_name)

    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.execute_sql("DELETE FROM parent WHERE id = 1", schema_name=schema_name)
    # child2 carries the constraint but holds no row, so retiring child1's row
    # is enough.
    client.execute_sql("DELETE FROM child1 WHERE cid = 10", schema_name=schema_name)
    client.execute_sql("DELETE FROM parent WHERE id = 1", schema_name=schema_name)


def test_fk_holds_across_partitions_in_bulk(client, fk_pair):
    """250 parents and 250 children over a PK spread that reaches every worker:
    the check is distributed, and a dangling reference is still refused at the
    far end of it."""
    n = 250
    for lo in range(0, n, 50):
        hi = min(lo + 50, n)
        client.execute_sql(
            "INSERT INTO parent VALUES " + ", ".join(f"({i}, {i * 7})" for i in range(lo + 1, hi + 1)),
            schema_name=fk_pair)
        client.execute_sql(
            "INSERT INTO child VALUES " + ", ".join(f"({10000 + i}, {i})" for i in range(lo + 1, hi + 1)),
            schema_name=fk_pair)
    ctid, _ = client.resolve_table(fk_pair, "child")
    assert len(client.scan(ctid)) == n
    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.execute_sql(f"INSERT INTO child VALUES (99999, {n + 1})", schema_name=fk_pair)


# ── The self-referential constraint ──────────────────────────────────────────

def test_self_referential_fk_registers_a_real_table_id(client, tree):
    """The wire boundary, which is where the constraint used to be lost.

    The planner cannot name the id of the table being created, so it ships a
    marker the COL_TAB writer rewrites to the owner id. A behavioural test alone
    would pass again the moment some path re-introduced a sentinel, because `0`
    is also the engine's encoding for "this column has no FK".
    """
    sn, tid, _ = tree
    assert tid > 0
    cols = {r.col_idx: r.fk_table_id for r in client.scan(gnitz.COL_TAB) if r.owner_id == tid}
    assert cols[0] == 0, "the PK column carries no FK"
    assert cols[1] == tid, f"parent_id must reference tree itself, got {cols[1]}"


def test_view_over_an_fk_table_is_not_an_fk_child(client, tree):
    """A view's columns are clones of the projected source defs, so a projected
    FK column would carry the source's `fk_table_id`. Registering that as a
    constraint makes the view an FK child of the parent — and a view gets no FK
    auto-index, so every parent delete then fails on a missing one. The same
    view over a self-FK table would make the table its own second, spurious
    child.
    """
    sn, tid, _ = tree
    client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
    client.execute_sql("CREATE VIEW tree_v AS SELECT id, parent_id FROM tree", schema_name=sn)

    vid, _ = client.resolve_table(sn, "tree_v")
    view_fks = [r.col_idx for r in client.scan(gnitz.COL_TAB)
                if r.owner_id == vid and r.fk_table_id != 0]
    assert not view_fks, f"view columns {view_fks} registered an FK"

    # The real constraint still holds, and an unreferenced row deletes.
    client.execute_sql("DELETE FROM tree WHERE id = 2", schema_name=sn)
    assert {r.id for r in client.scan(tid)} == {1}
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("INSERT INTO tree VALUES (3, 99)", schema_name=sn)


def test_self_fk_column_referencing_itself_rejected(client, schema_name):
    """A column that references the very column it is: a tautology, and its FK
    auto-index would be skipped as a PK column, leaving parent deletes
    unvalidatable."""
    with pytest.raises(gnitz.GnitzError, match="(?i)referenced column itself"):
        client.execute_sql(
            "CREATE TABLE selfcol (id BIGINT NOT NULL PRIMARY KEY REFERENCES selfcol(id))",
            schema_name=schema_name)


def test_self_fk_accepts_a_parent_the_same_batch_supplies(client, tree):
    """The ordinary way to seed a tree: the row satisfying the reference is in
    the same batch — including a row that is its own parent. A committed-state
    probe would reject both."""
    sn, tid, _ = tree
    client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
    client.execute_sql("INSERT INTO tree VALUES (3, 3)", schema_name=sn)
    assert {(r.id, r.parent_id) for r in client.scan(tid)} == {(1, None), (2, 1), (3, 3)}


def test_self_fk_rejects_an_absent_parent(client, tree):
    """On the transaction path and the plain push path alike."""
    sn, tid, schema = tree
    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.execute_sql("INSERT INTO tree VALUES (3, 99)", schema_name=sn)
    batch = gnitz.ZSetBatch(schema)
    batch.append(id=3, parent_id=99)
    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.push(tid, batch)
    assert not list(client.scan(tid))


def test_self_fk_delete_restrict_on_both_paths(client, tree):
    """Removing a referenced row is refused whether the removal arrives as a
    statement bundle or as a binary delete."""
    sn, tid, schema = tree
    client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)

    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.execute_sql("DELETE FROM tree WHERE id = 1", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.delete(tid, schema, [1])
    assert {r.id for r in client.scan(tid)} == {1, 2}


@pytest.mark.parametrize("rows,expect_error", [
    # `retract(1)` beside `insert(2, parent_id=1)`: probing committed state
    # finds 1 present and would leave a durable dangling reference.
    ([(1, None, -1), (2, 1, 1)], True),
    # `insert(1)` then `retract(1)` nets to weight zero, yet the apply removes
    # the committed row 1 — which row 2 references.
    ([(1, None, 1), (1, None, -1)], True),
], ids=["removes-the-parent-it-references", "net-zero-removal"])
def test_self_fk_push_is_checked_against_the_fold(client, tree, rows, expect_error):
    sn, tid, schema = tree
    client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
    batch = gnitz.ZSetBatch(schema)
    for rid, parent, w in rows:
        batch.append(id=rid, parent_id=parent, _weight=w)
    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.push(tid, batch)
    assert {r.id for r in client.scan(tid)} == {1, 2}, "the write must not have applied"


@pytest.mark.parametrize("teardown", [
    "DELETE FROM tree",                                       # parent and child at once
    "DELETE FROM tree WHERE id = 2; DELETE FROM tree WHERE id = 1",   # leaf first
], ids=["together", "leaf-first"])
def test_self_fk_teardown_accepted(client, tree, teardown):
    """A child removed by the same statement does not block its parent."""
    sn, tid, _ = tree
    client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
    for stmt in teardown.split("; "):
        client.execute_sql(stmt, schema_name=sn)
    assert not list(client.scan(tid))


def test_binary_delete_of_parent_and_child_together(client, tree):
    """One `delete` removing both `(1, NULL)` and `(2, parent=1)`: the only row
    referencing 1 is in the same push. Dropping the child from that push leaves
    2 referencing 1, and is refused."""
    sn, tid, schema = tree
    client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.delete(tid, schema, [1])
    assert {r.id for r in client.scan(tid)} == {1, 2}
    client.delete(tid, schema, [1, 2])
    assert not list(client.scan(tid))


def test_drop_self_referential_table_with_rows(client, tree):
    """The drop guard skips a blocking child that is itself in the drop set, and
    for a self-FK the child *is* the table being dropped."""
    sn, _, _ = tree
    client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
    client.execute_sql("DROP TABLE tree", schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.resolve_table(sn, "tree")


# ── RESTRICT against a non-PK UNIQUE target ──────────────────────────────────

@pytest.mark.parametrize("retire", [
    "DELETE FROM p WHERE pid <= 10",
    "UPDATE p SET code = code + 500000 WHERE pid <= 10",
], ids=["delete", "update"])
def test_bulk_retirement_of_a_referenced_code_is_blocked(client, code_pair, retire):
    """The referenced parent column is resolved from committed storage via the
    batched gather, so the verdict must hold over a bulk write spanning many
    values — and the same write over unreferenced rows must go through."""
    n = 20
    client.execute_sql(
        "INSERT INTO p VALUES " + ", ".join(f"({i}, {1000 + i})" for i in range(1, n + 1)),
        schema_name=code_pair)
    # Children reference the codes of pid 1..10; pid 11..20 are unreferenced.
    client.execute_sql(
        "INSERT INTO c VALUES " + ", ".join(f"({i}, {1000 + i})" for i in range(1, 11)),
        schema_name=code_pair)

    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(retire, schema_name=code_pair)
    # The same write over the unreferenced half goes through.
    client.execute_sql(retire.replace("<= 10", "> 10"), schema_name=code_pair)

    ptid, _ = client.resolve_table(code_pair, "p")
    rows = {r.pid: r.code for r in client.scan(ptid)}
    # The referenced rows are untouched either way; the unreferenced half was
    # deleted or renumbered by the second statement.
    assert all(rows[i] == 1000 + i for i in range(1, 11))
    if retire.startswith("DELETE"):
        assert sorted(rows) == list(range(1, 11))
    else:
        assert all(rows[i] == 1000 + i + 500000 for i in range(11, n + 1))


def test_two_children_two_columns_single_gather(client, schema_name):
    """Two children referencing two distinct non-PK UNIQUE columns of one
    parent: the single hoisted gather must resolve both columns so each child's
    RESTRICT check sees its own referenced value."""
    client.execute_sql(
        "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, code BIGINT UNSIGNED NOT NULL,"
        " email BIGINT UNSIGNED NOT NULL)", schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON p(email)", schema_name=schema_name)
    client.execute_sql(_C_CODE.format(name="c1"), schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE c2 (cid BIGINT PRIMARY KEY, eref BIGINT UNSIGNED REFERENCES p(email))",
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO p VALUES " + ", ".join(f"({i}, {1000 + i}, {7000 + i})" for i in range(1, 13)),
        schema_name=schema_name)
    # c1 references the code of pid=1; c2 the email of pid=2.
    client.execute_sql("INSERT INTO c1 VALUES (1, 1001)", schema_name=schema_name)
    client.execute_sql("INSERT INTO c2 VALUES (1, 7002)", schema_name=schema_name)

    for pid in (1, 2):
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(f"DELETE FROM p WHERE pid = {pid}", schema_name=schema_name)
    client.execute_sql("DELETE FROM p WHERE pid = 3", schema_name=schema_name)

    ptid, _ = client.resolve_table(schema_name, "p")
    seen = sorted(r.pid for r in client.scan(ptid))
    assert 3 not in seen and 1 in seen and 2 in seen


def test_null_and_absent_referenced_value_never_block(client, schema_name):
    """A NULL referenced value blocks no child, and deleting a non-existent PK
    is a harmless no-op — neither must error or block."""
    client.execute_sql(
        "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, code BIGINT UNSIGNED)",
        schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=schema_name)
    client.execute_sql(_C_CODE.format(name="c"), schema_name=schema_name)
    client.execute_sql("INSERT INTO p (pid, code) VALUES (1, NULL), (2, 500)", schema_name=schema_name)
    client.execute_sql("INSERT INTO c VALUES (1, 500)", schema_name=schema_name)

    client.execute_sql("DELETE FROM p WHERE pid = 1", schema_name=schema_name)   # NULL code
    client.execute_sql("DELETE FROM p WHERE pid = 999", schema_name=schema_name)  # absent PK
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("DELETE FROM p WHERE pid = 2", schema_name=schema_name)

    ptid, _ = client.resolve_table(schema_name, "p")
    assert sorted(r.pid for r in client.scan(ptid)) == [2]


def test_referenced_value_swapped_between_parent_rows_accepted(client, code_pair):
    """`p={(1,code=100),(2,code=200)}` with a child on 100; one push swaps the
    two codes. No referenced value leaves the parent, so the reference is intact
    at end of statement — the engine checks NO ACTION, not per-row RESTRICT."""
    client.execute_sql("INSERT INTO p VALUES (1, 100), (2, 200)", schema_name=code_pair)
    client.execute_sql("INSERT INTO c VALUES (1, 100)", schema_name=code_pair)

    ptid, pschema = client.resolve_table(code_pair, "p")
    b = gnitz.ZSetBatch(pschema)
    b.append(pid=1, code=200)
    b.append(pid=2, code=100)
    client.push(ptid, b)
    assert sorted((r.pid, r.code) for r in client.scan(ptid)) == [(1, 200), (2, 100)]


def test_binary_delete_retiring_a_non_pk_referenced_column(client, code_pair):
    """A plain push whose parent delete retires a non-PK referenced column: the
    delete carries filler payload, so the retired `code` comes from the
    committed row. Referenced codes block; unreferenced ones delete."""
    client.execute_sql("INSERT INTO p VALUES (1, 100), (2, 200), (3, 300)", schema_name=code_pair)
    client.execute_sql("INSERT INTO c VALUES (1, 200)", schema_name=code_pair)
    ptid, pschema = client.resolve_table(code_pair, "p")

    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.delete(ptid, pschema, [1, 2])
    assert sorted(r.pid for r in client.scan(ptid)) == [1, 2, 3]

    client.delete(ptid, pschema, [1, 3])
    assert sorted(r.pid for r in client.scan(ptid)) == [2]


def test_index_epoch_invalidates_a_cached_parent_index_list(client, server, schema_name):
    """FK-target validation reads a durable, per-connection cache of the
    parent's secondary-index metadata. A per-table index epoch makes that cache
    invalidate across connections: when another connection's DDL adds a UNIQUE
    index on the referenced column, the cached list must be re-fetched on the
    next FK check rather than served stale. A second reference on the warm,
    unchanged epoch must then reuse the cache without corrupting it.
    """
    with gnitz.connect(server) as b:
        client.execute_sql(_P_CODE, schema_name=schema_name)
        # B caches the parent's (empty) index list at the base epoch: no UNIQUE
        # index on `code`, so an FK against it is rejected.
        with pytest.raises(gnitz.GnitzError):
            b.execute_sql(_C_CODE.format(name="c1"), schema_name=schema_name)
        # A adds the UNIQUE index on a different connection → epoch bump.
        client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=schema_name)
        # B's cached epoch is now stale; the FK check must re-fetch and succeed.
        b.execute_sql(_C_CODE.format(name="c2"), schema_name=schema_name)
        # A third reference resolves against the warm cache at an unchanged
        # epoch: the "unchanged" reply must not drop the cached list.
        b.execute_sql(_C_CODE.format(name="c3"), schema_name=schema_name)

        # The resolved FK is actually enforced at insert time.
        client.execute_sql("INSERT INTO p VALUES (1, 1000)", schema_name=schema_name)
        b.execute_sql("INSERT INTO c2 VALUES (1, 1000)", schema_name=schema_name)
        with pytest.raises(gnitz.GnitzError):
            b.execute_sql("INSERT INTO c2 VALUES (2, 9999)", schema_name=schema_name)


# ── Under concurrency ────────────────────────────────────────────────────────

@pytest.mark.parametrize("self_fk", [False, True], ids=["two-tables", "self-fk"])
def test_fk_enforced_under_concurrent_push_and_delete(server, self_fk):
    """A binary push and a binary delete are both conflict-mode `Update`, so the
    only thing keeping them off the shared table lock — where they would run
    concurrently and each miss the other's uncommitted rows — is the FK terms of
    the validator's predicate.

    Each round races inserting a child against deleting its parent: exactly one
    must be rejected, and no round may leave a child referencing an absent
    parent. With a self-FK both endpoints are one table, so the lock set dedupes
    to a single tid — the writer must still take that one guard exclusively.
    """
    sn = "fkrace" + _uid()
    with gnitz.connect(server) as setup:
        setup.create_schema(sn)
        if self_fk:
            setup.execute_sql(
                "CREATE TABLE tree (id BIGINT NOT NULL PRIMARY KEY,"
                " parent_id BIGINT REFERENCES tree(id))", schema_name=sn)
            ptid, pschema = setup.resolve_table(sn, "tree")
            ctid, cschema = ptid, pschema
        else:
            setup.execute_sql("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                              schema_name=sn)
            setup.execute_sql(_CHILD_INLINE, schema_name=sn)
            ptid, pschema = setup.resolve_table(sn, "parent")
            ctid, cschema = setup.resolve_table(sn, "child")

    def seed_parent(conn):
        b = gnitz.ZSetBatch(pschema)
        if self_fk:
            b.append(id=1, parent_id=None)
        else:
            b.append(id=1)
        conn.push(ptid, b)

    def make_child():
        b = gnitz.ZSetBatch(cschema)
        if self_fk:
            b.append(id=2, parent_id=1)
        else:
            b.append(cid=2, pid=1)
        return b

    def child_pks(conn):
        return [r.id for r in conn.scan(ctid) if r.id != 1] if self_fk \
            else [r.cid for r in conn.scan(ctid)]

    try:
        with gnitz.connect(server) as setup, gnitz.connect(server) as a, gnitz.connect(server) as b:
            for _ in range(20):
                # Reset to {parent present, child absent}, touching only what is
                # actually there — a retraction of an absent row is not a no-op.
                children = child_pks(setup)
                if children:
                    setup.delete(ctid, cschema, children)
                if 1 not in [r.id for r in setup.scan(ptid)]:
                    seed_parent(setup)

                child = make_child()
                errors = _race(("push", lambda: a.push(ctid, child)),
                               ("delete", lambda: b.delete(ptid, pschema, [1])))

                rows = [(r.id, r.parent_id) for r in setup.scan(ptid)] if self_fk \
                    else [(r.cid, r.pid) for r in setup.scan(ctid)]
                present = {r.id for r in setup.scan(ptid)}
                orphans = [k for k, ref in rows if ref is not None and ref not in present]
                assert not orphans, f"rows {orphans} reference an absent parent"
                assert len(errors) == 1, (
                    f"expected exactly one of the two writes to be rejected, got {errors}")
    finally:
        with gnitz.connect(server) as c:
            c.drop_schema(sn)


# ── At scale ─────────────────────────────────────────────────────────────────

def test_restrict_over_more_values_than_one_write_carries(client, fk_pair):
    """`DELETE FROM child; DELETE FROM parent;` in one transaction over 1500
    referenced values that each still have a committed child.

    The check is bounded by the write's own row count, not by the number of
    values it asks about: every committed holder the probe returns is a row the
    same bundle retires, and the per-value holder cap the worker honours is the
    bundle's child-row count. Leaving a single child behind must still be fatal,
    whichever order the workers answer in.
    """
    n = 1500
    for lo in range(0, n, 500):
        hi = min(lo + 500, n)
        client.execute_sql(
            "INSERT INTO parent VALUES " + ", ".join(f"({i}, {i * 7})" for i in range(lo, hi)),
            schema_name=fk_pair)
        client.execute_sql(
            "INSERT INTO child VALUES " + ", ".join(f"({i}, {i})" for i in range(lo, hi)),
            schema_name=fk_pair)
    parent_tid, _ = client.resolve_table(fk_pair, "parent")
    child_tid, _ = client.resolve_table(fk_pair, "child")
    assert len(client.scan(parent_tid)) == n

    # One child survives → the whole bundle is rejected and nothing is applied.
    with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
        client.execute_sql(
            "BEGIN; DELETE FROM child WHERE cid <> 777; DELETE FROM parent; COMMIT",
            schema_name=fk_pair)
    assert len(client.scan(parent_tid)) == n

    # Leaf-first in one atomic statement pair is the natural spelling, and works.
    client.execute_sql("BEGIN; DELETE FROM child; DELETE FROM parent; COMMIT",
                       schema_name=fk_pair)
    assert len(client.scan(child_tid)) == 0
    assert len(client.scan(parent_tid)) == 0
