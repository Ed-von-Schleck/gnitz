"""E2E tests for foreign key constraints via SQL.

Run:
    cd crates/gnitz-py && uv run pytest tests/test_fk.py -v --tb=short
"""
import os
import random
import threading

import pytest
import gnitz

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


def _uid():
    return str(random.randint(100000, 999999))


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


def _cleanup(client, sn, *tables):
    """Drop tables (in order) and schema, ignoring errors."""
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


class TestFkInlineReferences:
    """Inline REFERENCES syntax: col_name TYPE REFERENCES parent(pk)."""

    def test_create_child_with_inline_references(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            results = client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            assert results[0]["type"] == "TableCreated"
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_fk_insert_valid(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            # Insert parent row first
            client.execute_sql(
                "INSERT INTO parent VALUES (1, 100)",
                schema_name=sn,
            )
            # Insert child referencing existing parent -- should succeed
            client.execute_sql(
                "INSERT INTO child VALUES (10, 1)",
                schema_name=sn,
            )
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_fk_insert_violation(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            # Insert child referencing non-existent parent -- must fail
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql(
                    "INSERT INTO child VALUES (10, 999)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_drop_parent_blocked_by_fk(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            # Verify FK enforcement works (sanity check)
            client.execute_sql("INSERT INTO parent VALUES (1)", schema_name=sn)
            client.execute_sql("INSERT INTO child VALUES (10, 1)", schema_name=sn)

            # Dropping parent while child exists must fail
            err = None
            try:
                client.execute_sql("DROP TABLE parent", schema_name=sn)
            except gnitz.GnitzError as e:
                err = e
            assert err is not None, "DROP TABLE parent should have raised"
            assert "referenced" in str(err).lower() or "integrity" in str(err).lower() or "dependency" in str(err).lower(), \
                f"Expected FK-related error, got: {err}"
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_dup_name_fk_create_leaves_no_phantom_child(self, client):
        """Atomic-DDL headline regression (no crash, deterministic red-green).

        A duplicate-name CREATE TABLE whose column REFERENCES a parent must not
        strand a phantom FK child. Pre-fix, the COL_TAB families committed as a
        separate RPC *before* the TABLE_TAB name check failed, so the running
        master's fk_by_parent[parent] carried a phantom child and DROP TABLE
        parent was blocked in the same session (and every reboot re-materialised
        it). Under atomic CREATE the whole bundle rolls back in master memory, so
        no phantom forms and DROP TABLE parent succeeds.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            # Re-CREATE t (duplicate name) with a column REFERENCES parent. The
            # server ingests COL_TAB (whose FK hook populates fk_by_parent[parent])
            # then rejects TABLE_TAB on the duplicate name — one atomic zone,
            # rolled back on the failure.
            err = None
            try:
                client.execute_sql(
                    "CREATE TABLE t ("
                    "  id BIGINT NOT NULL PRIMARY KEY,"
                    "  pref BIGINT NOT NULL REFERENCES parent(id)"
                    ")",
                    schema_name=sn,
                )
            except gnitz.GnitzError as e:
                err = e
            assert err is not None, "duplicate-name CREATE TABLE must error"

            # The real `t` has no FK to parent, so the ONLY thing that could block
            # this is a stranded phantom child. Post-fix it drops cleanly.
            client.execute_sql("DROP TABLE parent", schema_name=sn)
        finally:
            _cleanup(client, sn, "t", "parent")

    def test_fk_parent_pk_not_first_column(self, client):
        """Regression: parent PK declared at a NON-leading column position.

        The distributed FK existence check (`build_check_batch`) resolved the
        probe key's type and width from the parent's first declared column
        instead of its PK column. Here the parent leads with a 16-byte STRING
        and its BIGINT PK is the second column, so the probe was encoded at the
        wrong width and zeroed out — a valid child reference was rejected (and a
        dangling one could be silently accepted if an all-zero parent key
        existed). The probe must encode at the PK column's type/width.
        """
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            # `id` (the PK) is the SECOND declared column; the leading STRING
            # `label` is wider, so columns[0] differs from the PK column.
            client.execute_sql(
                "CREATE TABLE users ("
                "  label VARCHAR(255) NOT NULL,"
                "  id BIGINT NOT NULL PRIMARY KEY"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE orders ("
                "  oid BIGINT NOT NULL PRIMARY KEY,"
                "  user_id BIGINT NOT NULL REFERENCES users(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO users VALUES ('alice', 42)", schema_name=sn,
            )
            # Valid reference to users(id = 42) must succeed (pre-fix: rejected).
            client.execute_sql(
                "INSERT INTO orders VALUES (1, 42)", schema_name=sn,
            )
            # A dangling reference must still be rejected.
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql(
                    "INSERT INTO orders VALUES (2, 999)", schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "orders", "users")


class TestFkTableLevel:
    """Table-level FOREIGN KEY syntax: FOREIGN KEY (col) REFERENCES parent(pk)."""

    def test_table_level_fk(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            results = client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL,"
                "  FOREIGN KEY (pid) REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            assert results[0]["type"] == "TableCreated"

            # Insert valid FK
            client.execute_sql("INSERT INTO parent VALUES (1)", schema_name=sn)
            client.execute_sql("INSERT INTO child VALUES (10, 1)", schema_name=sn)

            # Insert invalid FK
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql(
                    "INSERT INTO child VALUES (11, 999)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "child", "parent")


class TestFkErrorCases:
    """Error cases: nonexistent target, wrong column, etc."""

    def test_fk_references_nonexistent_table(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            with pytest.raises(gnitz.GnitzError, match="(?i)not found"):
                client.execute_sql(
                    "CREATE TABLE child ("
                    "  cid BIGINT NOT NULL PRIMARY KEY,"
                    "  pid BIGINT NOT NULL REFERENCES phantom(id)"
                    ")",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn)

    def test_fk_references_non_pk_column(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError, match="(?i)primary key"):
                client.execute_sql(
                    "CREATE TABLE child ("
                    "  cid BIGINT NOT NULL PRIMARY KEY,"
                    "  pid BIGINT NOT NULL REFERENCES parent(val)"
                    ")",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "parent")

    def test_fk_nullable_allows_null(self, client):
        """Nullable FK columns should accept NULL values (no FK check)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO child VALUES (10, NULL)", schema_name=sn)
        finally:
            _cleanup(client, sn, "child", "parent")


class TestFkDeleteRestrict:
    """DELETE-side referential integrity: RESTRICT prevents parent row deletion."""

    def test_delete_parent_blocked_when_child_exists(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO parent VALUES (1, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO child VALUES (10, 1)", schema_name=sn)

            # DELETE parent row that is still referenced -- must fail
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql("DELETE FROM parent WHERE id = 1", schema_name=sn)
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_delete_parent_succeeds_after_child_deleted(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO parent VALUES (1, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO child VALUES (10, 1)", schema_name=sn)

            # Delete child first
            client.execute_sql("DELETE FROM child WHERE cid = 10", schema_name=sn)
            # Now deleting parent should succeed
            client.execute_sql("DELETE FROM parent WHERE id = 1", schema_name=sn)
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_delete_parent_with_no_children(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO parent VALUES (1, 100)", schema_name=sn)
            # No child rows -- delete should succeed
            client.execute_sql("DELETE FROM parent WHERE id = 1", schema_name=sn)
        finally:
            _cleanup(client, sn, "child", "parent")


class TestFkUpdate:
    """UPDATE should validate FK on the new row value."""

    def test_update_fk_to_invalid_value(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO parent VALUES (1)", schema_name=sn)
            client.execute_sql("INSERT INTO child VALUES (10, 1)", schema_name=sn)

            # UPDATE FK to non-existent parent -- must fail
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql(
                    "UPDATE child SET pid = 999 WHERE cid = 10",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_update_fk_to_valid_value(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO parent VALUES (1)", schema_name=sn)
            client.execute_sql("INSERT INTO parent VALUES (2)", schema_name=sn)
            client.execute_sql("INSERT INTO child VALUES (10, 1)", schema_name=sn)

            # UPDATE FK to another valid parent -- should succeed
            client.execute_sql(
                "UPDATE child SET pid = 2 WHERE cid = 10",
                schema_name=sn,
            )
        finally:
            _cleanup(client, sn, "child", "parent")


class TestFkMultiChild:
    """Multiple child tables referencing the same parent."""

    def test_delete_blocked_by_either_child(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child1 ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child2 ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO parent VALUES (1)", schema_name=sn)
            client.execute_sql("INSERT INTO child1 VALUES (10, 1)", schema_name=sn)

            # child1 references parent -- DELETE blocked
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql("DELETE FROM parent WHERE id = 1", schema_name=sn)

            # Delete child1, but child2 still has FK constraint (no data though)
            client.execute_sql("DELETE FROM child1 WHERE cid = 10", schema_name=sn)

            # Now delete should succeed (child2 has no rows referencing parent)
            client.execute_sql("DELETE FROM parent WHERE id = 1", schema_name=sn)
        finally:
            _cleanup(client, sn, "child2", "child1", "parent")

    def test_drop_child_then_parent_succeeds(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            # DROP child, then DROP parent should succeed (FK metadata cleaned up)
            client.execute_sql("DROP TABLE child", schema_name=sn)
            client.execute_sql("DROP TABLE parent", schema_name=sn)
        finally:
            _cleanup(client, sn)


_COL_TAB = 4


def _make_tree(client, sn):
    """`CREATE SCHEMA sn; CREATE TABLE tree (id PK, parent_id -> tree.id)`.
    Returns `(tid, schema)`."""
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE tree ("
        "  id BIGINT NOT NULL PRIMARY KEY,"
        "  parent_id BIGINT REFERENCES tree(id)"
        ")",
        schema_name=sn,
    )
    return client.resolve_table(sn, "tree")


class TestFkSelfReferenceSQL:
    """A self-referential FK declared with CREATE TABLE is enforced in both
    directions, on the transaction path (UPDATE/DELETE/upsert) and on the plain
    path (blind INSERT, binary push/delete) alike."""

    def test_self_referential_fk_registers_a_real_table_id(self, client):
        """The wire boundary, which is where the constraint used to be lost.

        The planner cannot name the id of the table being created, so it ships a
        marker the COL_TAB writer rewrites to the owner id. A behavioural test
        alone would pass again the moment some path re-introduced a sentinel,
        because `0` is also the engine's encoding for "this column has no FK".
        """
        sn = "s" + _uid()
        try:
            tid, _ = _make_tree(client, sn)
            assert tid > 0
            cols = {
                r.col_idx: r.fk_table_id
                for r in client.scan(_COL_TAB)
                if r.owner_id == tid
            }
            assert cols[0] == 0, "the PK column carries no FK"
            assert cols[1] == tid, f"parent_id must reference tree itself, got {cols[1]}"
        finally:
            _cleanup(client, sn, "tree")

    def test_view_over_an_fk_table_is_not_an_fk_child(self, client):
        """A view's columns are clones of the projected source defs, so a
        projected FK column would carry the source's `fk_table_id`. Registering
        that as a constraint makes the view an FK child of the parent — and a
        view has no `__fk_` index, so every parent delete then fails on a missing
        one. The same view over a self-FK table would make the table its own
        second, spurious child.
        """
        sn = "s" + _uid()
        try:
            tid, _ = _make_tree(client, sn)
            client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
            client.execute_sql("CREATE VIEW tree_v AS SELECT id, parent_id FROM tree", schema_name=sn)

            vid, _ = client.resolve_table(sn, "tree_v")
            view_fks = [
                r.col_idx for r in client.scan(_COL_TAB) if r.owner_id == vid and r.fk_table_id != 0
            ]
            assert not view_fks, f"view columns {view_fks} registered an FK"

            # The real constraint still holds, and an unreferenced row deletes.
            client.execute_sql("DELETE FROM tree WHERE id = 2", schema_name=sn)
            assert {r.id for r in client.scan(tid)} == {1}
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO tree VALUES (3, 99)", schema_name=sn)
        finally:
            try:
                client.execute_sql("DROP VIEW tree_v", schema_name=sn)
            except Exception:
                pass
            _cleanup(client, sn, "tree")

    def test_self_fk_column_referencing_itself_rejected(self, client):
        """A column that references the very column it is: a tautology, and its
        auto `__fk_` index would be skipped as a PK column, leaving parent
        deletes unvalidatable."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            with pytest.raises(gnitz.GnitzError, match="(?i)referenced column itself"):
                client.execute_sql(
                    "CREATE TABLE selfcol (id BIGINT NOT NULL PRIMARY KEY REFERENCES selfcol(id))",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "selfcol")

    def test_insert_absent_parent_rejected(self, client):
        sn = "s" + _uid()
        try:
            _make_tree(client, sn)
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql("INSERT INTO tree VALUES (3, 99)", schema_name=sn)
        finally:
            _cleanup(client, sn, "tree")

    def test_push_absent_parent_rejected(self, client):
        sn = "s" + _uid()
        try:
            tid, schema = _make_tree(client, sn)
            batch = gnitz.ZSetBatch(schema)
            batch.append(id=3, parent_id=99)
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.push(tid, batch)
            assert not list(client.scan(tid))
        finally:
            _cleanup(client, sn, "tree")

    def test_delete_referenced_parent_rejected(self, client):
        sn = "s" + _uid()
        try:
            tid, _ = _make_tree(client, sn)
            client.execute_sql("INSERT INTO tree VALUES (1, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO tree VALUES (2, 1)", schema_name=sn)

            # Transaction path: the bundle removes 1 but not the row that
            # references it.
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql("DELETE FROM tree WHERE id = 1", schema_name=sn)
            # Plain path: the binary delete probes the committed children.
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.delete(tid, client.resolve_table(sn, "tree")[1], [1])

            assert {r.id for r in client.scan(tid)} == {1, 2}
        finally:
            _cleanup(client, sn, "tree")

    def test_intra_batch_parent_accepted(self, client):
        """The ordinary way to seed a tree: the row satisfying the reference is
        in the same batch. A committed-state probe would reject it."""
        sn = "s" + _uid()
        try:
            tid, _ = _make_tree(client, sn)
            client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
            # A row that is its own parent — the reference resolves against the
            # very row supplying it.
            client.execute_sql("INSERT INTO tree VALUES (3, 3)", schema_name=sn)
            assert {(r.id, r.parent_id) for r in client.scan(tid)} == {
                (1, None),
                (2, 1),
                (3, 3),
            }
        finally:
            _cleanup(client, sn, "tree")

    def test_push_removing_the_parent_it_references_rejected(self, client):
        """One push carrying `retract(1)` and `insert(2, parent_id=1)`. Probing
        committed state finds 1 present and lets it through, leaving a durable
        dangling reference — so assert the table, not only the error."""
        sn = "s" + _uid()
        try:
            tid, schema = _make_tree(client, sn)
            client.execute_sql("INSERT INTO tree VALUES (1, NULL)", schema_name=sn)

            batch = gnitz.ZSetBatch(schema)
            batch.append(id=1, parent_id=None, _weight=-1)
            batch.append(id=2, parent_id=1)
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.push(tid, batch)

            rows = {(r.id, r.parent_id) for r in client.scan(tid)}
            assert rows == {(1, None)}, f"the write must not have applied: {rows}"
        finally:
            _cleanup(client, sn, "tree")

    def test_push_net_zero_removal_still_restrict_checked(self, client):
        """`insert(1, x)` then `retract(1)` nets to weight zero, yet the apply
        removes the committed row 1 — which row 2 references."""
        sn = "s" + _uid()
        try:
            tid, schema = _make_tree(client, sn)
            client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)

            batch = gnitz.ZSetBatch(schema)
            batch.append(id=1, parent_id=None)
            batch.append(id=1, parent_id=None, _weight=-1)
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.push(tid, batch)

            assert {r.id for r in client.scan(tid)} == {1, 2}
        finally:
            _cleanup(client, sn, "tree")

    def test_delete_parent_and_child_together_accepted(self, client):
        """The bundle path's exemption: a child removed by the same statement
        does not block its parent's removal."""
        sn = "s" + _uid()
        try:
            tid, _ = _make_tree(client, sn)
            client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
            client.execute_sql("DELETE FROM tree", schema_name=sn)
            assert not list(client.scan(tid))
        finally:
            _cleanup(client, sn, "tree")

    def test_leaf_first_teardown_accepted(self, client):
        sn = "s" + _uid()
        try:
            tid, _ = _make_tree(client, sn)
            client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
            client.execute_sql("DELETE FROM tree WHERE id = 2", schema_name=sn)
            client.execute_sql("DELETE FROM tree WHERE id = 1", schema_name=sn)
            assert not list(client.scan(tid))
        finally:
            _cleanup(client, sn, "tree")

    def test_drop_self_referential_table_with_rows(self, client):
        """The drop guard skips a blocking child that is itself in the drop set,
        and for a self-FK the child *is* the table being dropped."""
        sn = "s" + _uid()
        try:
            _make_tree(client, sn)
            client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
            client.execute_sql("DROP TABLE tree", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.resolve_table(sn, "tree")
        finally:
            _cleanup(client, sn, "tree")


def test_self_fk_enforced_under_concurrent_push_and_delete(server):
    """Both endpoints of the constraint are one table, so its lock set dedupes
    to a single tid — the writer must still take that one guard exclusively.
    Each round races inserting child `(2, parent_id=1)` against deleting `1`;
    exactly one must be rejected, and no round may leave a child referencing an
    absent parent."""
    sn = "s" + _uid()
    with gnitz.connect(server) as setup:
        tid, schema = _make_tree(setup, sn)

    try:
        with gnitz.connect(server) as setup, gnitz.connect(server) as a, gnitz.connect(server) as b:
            for _ in range(20):
                # Reset to {1}, touching only what is actually there — a
                # retraction of an absent row is not a no-op.
                ids = {r.id for r in setup.scan(tid)}
                if 2 in ids:
                    setup.delete(tid, schema, [2])
                if 1 not in ids:
                    seed = gnitz.ZSetBatch(schema)
                    seed.append(id=1, parent_id=None)
                    setup.push(tid, seed)

                child = gnitz.ZSetBatch(schema)
                child.append(id=2, parent_id=1)
                errors = _race(
                    ("push", lambda: a.push(tid, child)),
                    ("delete", lambda: b.delete(tid, schema, [1])),
                )

                rows = {(r.id, r.parent_id) for r in setup.scan(tid)}
                present = {i for i, _ in rows}
                orphans = [i for i, p in rows if p is not None and p not in present]
                assert not orphans, f"rows {orphans} reference an absent parent"
                assert len(errors) == 1, (
                    f"expected exactly one of the two writes to be rejected, got {errors}"
                )
    finally:
        with gnitz.connect(server) as c:
            _cleanup(c, sn, "tree")


# ---------------------------------------------------------------------------
# Multi-worker FK tests (require GNITZ_WORKERS >= 2)
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
class TestFkMultiWorker:
    """FK validation across multiple workers (distributed IPC path)."""

    def test_fk_insert_valid_multiworker(self, client):
        """Insert 10+ parent rows with spread PKs, then child rows referencing each."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            # Insert parents with spread PKs (different partitions)
            for i in range(1, 16):
                client.execute_sql(
                    f"INSERT INTO parent VALUES ({i * 1000}, {i})",
                    schema_name=sn,
                )
            # Insert children referencing each parent
            for i in range(1, 16):
                client.execute_sql(
                    f"INSERT INTO child VALUES ({i}, {i * 1000})",
                    schema_name=sn,
                )
            # Verify all children exist
            child_tid = client.resolve_table(sn, "child")[0]
            rows = list(client.scan(child_tid))
            assert len(rows) == 15
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_fk_insert_violation_multiworker(self, client):
        """Child references non-existent parent across workers."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO parent VALUES (1000)", schema_name=sn)
            # Reference non-existent parent -- must fail
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql(
                    "INSERT INTO child VALUES (1, 9999)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_fk_delete_restrict_multiworker(self, client):
        """Parent + child potentially on different workers, DELETE parent blocked."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            # Use spread PKs to hit different partitions
            client.execute_sql("INSERT INTO parent VALUES (5000, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO child VALUES (1, 5000)", schema_name=sn)

            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.execute_sql("DELETE FROM parent WHERE id = 5000", schema_name=sn)
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_fk_update_parent_not_blocked_multiworker(self, client):
        """UPDATE parent payload (not PK) with child referencing it -- succeeds."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO parent VALUES (3000, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO child VALUES (1, 3000)", schema_name=sn)

            # UPDATE parent payload -- should NOT be blocked (UPSERT, not DELETE)
            client.execute_sql(
                "UPDATE parent SET val = 999 WHERE id = 3000",
                schema_name=sn,
            )
            parent_tid = client.resolve_table(sn, "parent")[0]
            rows = list(client.scan(parent_tid))
            found = [r for r in rows if r["id"] == 3000]
            assert len(found) == 1
            assert found[0]["val"] == 999
        finally:
            _cleanup(client, sn, "child", "parent")

    def test_fk_many_rows_distributed(self, client):
        """200+ parents + 200+ children, full coverage of worker partitions."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE child ("
                "  cid BIGINT NOT NULL PRIMARY KEY,"
                "  pid BIGINT NOT NULL REFERENCES parent(id)"
                ")",
                schema_name=sn,
            )
            # Insert 250 parents in batches
            n = 250
            for start in range(0, n, 50):
                vals = ", ".join(f"({i})" for i in range(start + 1, min(start + 51, n + 1)))
                client.execute_sql(f"INSERT INTO parent VALUES {vals}", schema_name=sn)

            # Insert 250 children referencing parents
            for start in range(0, n, 50):
                vals = ", ".join(
                    f"({10000 + i}, {i})"
                    for i in range(start + 1, min(start + 51, n + 1))
                )
                client.execute_sql(f"INSERT INTO child VALUES {vals}", schema_name=sn)

            child_tid = client.resolve_table(sn, "child")[0]
            rows = list(client.scan(child_tid))
            assert len(rows) == n
        finally:
            _cleanup(client, sn, "child", "parent")


class TestFkNonPkUniqueGather:
    """RESTRICT against a non-PK UNIQUE FK target, which resolves the
    referenced parent column from committed storage via the batched gather.

    These run at whatever GNITZ_WORKERS is set (the e2e suite uses 4), so the
    gather's sort + per-worker partition routing is exercised; at W=1 they
    confirm the single-worker parity of the same path. The referenced values
    span all partitions (parent PKs 1..N spread across workers)."""

    def test_bulk_delete_referenced_blocked_unreferenced_succeeds(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, "
                "code BIGINT UNSIGNED NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE c (cid BIGINT PRIMARY KEY, "
                "ref BIGINT UNSIGNED REFERENCES p(code))",
                schema_name=sn,
            )
            n = 20
            pvals = ", ".join(f"({i}, {1000 + i})" for i in range(1, n + 1))
            client.execute_sql(f"INSERT INTO p VALUES {pvals}", schema_name=sn)
            # Children reference the codes of pid 1..10; pid 11..20 unreferenced.
            cvals = ", ".join(f"({i}, {1000 + i})" for i in range(1, 11))
            client.execute_sql(f"INSERT INTO c VALUES {cvals}", schema_name=sn)

            # Bulk DELETE spanning referenced rows → blocked (RESTRICT).
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("DELETE FROM p WHERE pid <= 10", schema_name=sn)
            # Bulk DELETE of only unreferenced rows → succeeds.
            client.execute_sql("DELETE FROM p WHERE pid > 10", schema_name=sn)

            results = client.execute_sql("SELECT pid FROM p", schema_name=sn)
            rows_result = next(r for r in results if r["type"] == "Rows")
            seen = sorted(row.pid for row in rows_result["rows"])
            assert seen == list(range(1, 11))
        finally:
            _cleanup(client, sn, "c", "p")

    def test_bulk_update_retire_referenced_blocked(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, "
                "code BIGINT UNSIGNED NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE c (cid BIGINT PRIMARY KEY, "
                "ref BIGINT UNSIGNED REFERENCES p(code))",
                schema_name=sn,
            )
            n = 20
            pvals = ", ".join(f"({i}, {1000 + i})" for i in range(1, n + 1))
            client.execute_sql(f"INSERT INTO p VALUES {pvals}", schema_name=sn)
            cvals = ", ".join(f"({i}, {1000 + i})" for i in range(1, 11))
            client.execute_sql(f"INSERT INTO c VALUES {cvals}", schema_name=sn)

            # Bulk UPDATE retiring referenced codes → blocked.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "UPDATE p SET code = code + 500000 WHERE pid <= 10",
                    schema_name=sn,
                )
            # Bulk UPDATE of unreferenced rows → succeeds.
            client.execute_sql(
                "UPDATE p SET code = code + 500000 WHERE pid > 10", schema_name=sn
            )

            results = client.execute_sql("SELECT pid, code FROM p", schema_name=sn)
            rows_result = next(r for r in results if r["type"] == "Rows")
            seen = sorted(
                (row.pid, row.code) for row in rows_result["rows"] if row.pid > 10
            )
            assert seen == [(i, 1000 + i + 500000) for i in range(11, 21)]
        finally:
            _cleanup(client, sn, "c", "p")

    def test_two_children_two_columns_single_gather(self, client):
        """Two children referencing two distinct non-PK UNIQUE columns of one
        parent: the single hoisted gather must resolve both columns so each
        child's RESTRICT check sees its own referenced value."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, "
                "code BIGINT UNSIGNED NOT NULL, email BIGINT UNSIGNED NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON p(email)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE c1 (cid BIGINT PRIMARY KEY, "
                "ref BIGINT UNSIGNED REFERENCES p(code))",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE c2 (cid BIGINT PRIMARY KEY, "
                "eref BIGINT UNSIGNED REFERENCES p(email))",
                schema_name=sn,
            )
            n = 12
            pvals = ", ".join(
                f"({i}, {1000 + i}, {7000 + i})" for i in range(1, n + 1)
            )
            client.execute_sql(f"INSERT INTO p VALUES {pvals}", schema_name=sn)
            # c1 references code of pid=1; c2 references email of pid=2.
            client.execute_sql("INSERT INTO c1 VALUES (1, 1001)", schema_name=sn)
            client.execute_sql("INSERT INTO c2 VALUES (1, 7002)", schema_name=sn)

            # Deleting pid=1 is blocked by c1 (code), pid=2 by c2 (email).
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("DELETE FROM p WHERE pid = 1", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("DELETE FROM p WHERE pid = 2", schema_name=sn)
            # An unreferenced parent row deletes fine.
            client.execute_sql("DELETE FROM p WHERE pid = 3", schema_name=sn)

            results = client.execute_sql("SELECT pid FROM p", schema_name=sn)
            rows_result = next(r for r in results if r["type"] == "Rows")
            seen = sorted(row.pid for row in rows_result["rows"])
            assert 3 not in seen and 1 in seen and 2 in seen
        finally:
            _cleanup(client, sn, "c1", "c2", "p")

    def test_null_and_absent_referenced_value_never_block(self, client):
        """A NULL referenced value blocks no child, and deleting a non-existent
        PK is a harmless no-op — neither must error or block."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, "
                "code BIGINT UNSIGNED)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE c (cid BIGINT PRIMARY KEY, "
                "ref BIGINT UNSIGNED REFERENCES p(code))",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO p (pid, code) VALUES (1, NULL), (2, 500)", schema_name=sn
            )
            client.execute_sql("INSERT INTO c VALUES (1, 500)", schema_name=sn)

            # NULL referenced value → delete not blocked.
            client.execute_sql("DELETE FROM p WHERE pid = 1", schema_name=sn)
            # Absent PK → no-op, no error, no block.
            client.execute_sql("DELETE FROM p WHERE pid = 999", schema_name=sn)
            # Referenced value still blocks.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("DELETE FROM p WHERE pid = 2", schema_name=sn)

            results = client.execute_sql("SELECT pid FROM p", schema_name=sn)
            rows_result = next(r for r in results if r["type"] == "Rows")
            seen = sorted(row.pid for row in rows_result["rows"])
            assert seen == [2]
        finally:
            _cleanup(client, sn, "c", "p")


class TestFkPlainPushFoldedValidation:
    """A plain push (binary `push`/`delete`) is validated as a one-family
    bundle, so its FK verdicts read the same fold the apply writes: a child the
    same push removes exempts its parent, and a referenced value the push
    re-adds under another row never leaves."""

    def test_binary_delete_of_parent_and_child_together_accepted(self, client):
        """One `delete` removing both `(1, NULL)` and `(2, parent=1)` of a
        self-referencing tree: the only row referencing 1 is in the same push."""
        sn = "s" + _uid()
        try:
            tid, schema = _make_tree(client, sn)
            client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
            client.delete(tid, schema, [1, 2])
            assert not list(client.scan(tid))
        finally:
            _cleanup(client, sn, "tree")

    def test_binary_delete_leaving_the_child_behind_rejected(self, client):
        """The same push, minus the child: 2 still references 1."""
        sn = "s" + _uid()
        try:
            tid, schema = _make_tree(client, sn)
            client.execute_sql("INSERT INTO tree VALUES (1, NULL), (2, 1)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.delete(tid, schema, [1])
            assert {r.id for r in client.scan(tid)} == {1, 2}
        finally:
            _cleanup(client, sn, "tree")

    def test_referenced_value_swapped_between_parent_rows_accepted(self, client):
        """`p={(1,code=100),(2,code=200)}` with a child on 100; one push swaps
        the two codes. No referenced value leaves the parent, so the reference
        is intact at end of statement — the engine checks NO ACTION, not
        per-row RESTRICT."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, "
                "code BIGINT UNSIGNED NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE c (cid BIGINT PRIMARY KEY, "
                "ref BIGINT UNSIGNED REFERENCES p(code))",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO p VALUES (1, 100), (2, 200)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (1, 100)", schema_name=sn)

            ptid, pschema = client.resolve_table(sn, "p")
            b = gnitz.ZSetBatch(pschema)
            b.append(pid=1, code=200)
            b.append(pid=2, code=100)
            client.push(ptid, b)
            assert sorted((r.pid, r.code) for r in client.scan(ptid)) == [(1, 200), (2, 100)]
        finally:
            _cleanup(client, sn, "c", "p")

    def test_binary_delete_retiring_a_non_pk_referenced_column(self, client):
        """A plain push whose parent delete retires a non-PK referenced column:
        the delete carries filler payload, so the retired `code` comes from the
        committed row. Referenced codes block; unreferenced ones delete."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, "
                "code BIGINT UNSIGNED NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
            client.execute_sql(
                "CREATE TABLE c (cid BIGINT PRIMARY KEY, "
                "ref BIGINT UNSIGNED REFERENCES p(code))",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO p VALUES (1, 100), (2, 200), (3, 300)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (1, 200)", schema_name=sn)
            ptid, pschema = client.resolve_table(sn, "p")

            with pytest.raises(gnitz.GnitzError, match="(?i)foreign key"):
                client.delete(ptid, pschema, [1, 2])
            assert sorted(r.pid for r in client.scan(ptid)) == [1, 2, 3]

            client.delete(ptid, pschema, [1, 3])
            assert sorted(r.pid for r in client.scan(ptid)) == [2]
        finally:
            _cleanup(client, sn, "c", "p")


class TestFkIndexEpochInvalidation:
    """FK-target validation reads a durable, per-connection cache of the
    parent's secondary-index metadata. A per-table index epoch makes that cache
    correctly invalidate across connections: when another connection's DDL adds
    (or drops) a UNIQUE index on the referenced column, the cached list must be
    re-fetched on the next FK check rather than served stale."""

    def test_unique_index_created_on_other_connection_is_observed(self, server):
        # Connection B warms its index cache for the parent (no unique index on
        # `code` yet → FK rejected). Connection A then adds the UNIQUE index,
        # bumping the parent's index epoch. B's next FK check must see the stale
        # epoch, re-fetch, and accept the FK it would otherwise wrongly reject.
        sn = "s" + _uid()
        with gnitz.connect(server) as a, gnitz.connect(server) as b:
            try:
                a.create_schema(sn)
                a.execute_sql(
                    "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, "
                    "code BIGINT UNSIGNED NOT NULL)",
                    schema_name=sn,
                )
                # B caches the parent's (empty) index list at the base epoch: no
                # UNIQUE index on `code`, so an FK against it is rejected.
                with pytest.raises(gnitz.GnitzError):
                    b.execute_sql(
                        "CREATE TABLE c1 (cid BIGINT PRIMARY KEY, "
                        "ref BIGINT UNSIGNED REFERENCES p(code))",
                        schema_name=sn,
                    )
                # A adds the UNIQUE index on a different connection → epoch bump.
                a.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
                # B's cached epoch is now stale; the FK check must re-fetch and
                # succeed. (Without the epoch, B would serve its stale empty
                # cache and reject c2.)
                b.execute_sql(
                    "CREATE TABLE c2 (cid BIGINT PRIMARY KEY, "
                    "ref BIGINT UNSIGNED REFERENCES p(code))",
                    schema_name=sn,
                )
                # Sanity: the resolved FK is actually enforced at insert time.
                a.execute_sql("INSERT INTO p VALUES (1, 1000)", schema_name=sn)
                b.execute_sql("INSERT INTO c2 VALUES (1, 1000)", schema_name=sn)
                with pytest.raises(gnitz.GnitzError):
                    b.execute_sql("INSERT INTO c2 VALUES (2, 9999)", schema_name=sn)
            finally:
                _cleanup(a, sn, "c2", "c1", "p")

    def test_repeated_fk_checks_reuse_warm_cache(self, server):
        # Two FK references to the same parent column on one connection: the
        # second resolves against the warm index cache (unchanged epoch). Both
        # must succeed identically — the warm "unchanged" reply must not corrupt
        # or drop the cached list.
        sn = "s" + _uid()
        with gnitz.connect(server) as c:
            try:
                c.create_schema(sn)
                c.execute_sql(
                    "CREATE TABLE p (pid BIGINT UNSIGNED PRIMARY KEY, "
                    "code BIGINT UNSIGNED NOT NULL)",
                    schema_name=sn,
                )
                c.execute_sql("CREATE UNIQUE INDEX ON p(code)", schema_name=sn)
                c.execute_sql(
                    "CREATE TABLE c1 (cid BIGINT PRIMARY KEY, "
                    "ref BIGINT UNSIGNED REFERENCES p(code))",
                    schema_name=sn,
                )
                # Second FK reference → warm-cache (unchanged) path on the client.
                c.execute_sql(
                    "CREATE TABLE c2 (cid BIGINT PRIMARY KEY, "
                    "ref BIGINT UNSIGNED REFERENCES p(code))",
                    schema_name=sn,
                )
            finally:
                _cleanup(c, sn, "c2", "c1", "p")


# ---------------------------------------------------------------------------
# FK enforcement under concurrent binary writes
# ---------------------------------------------------------------------------


def test_fk_enforced_under_concurrent_push_and_delete(server):
    """A binary push and a binary delete are both conflict-mode `Update`, so the
    only thing keeping them off the shared table lock — where they would run
    concurrently and each miss the other's uncommitted rows — is the FK terms of
    the validator's predicate. Both tables here carry an FK, so both writes take
    the whole two-table lock set exclusively.

    Each round races a child insert against the deletion of its parent. Exactly
    one must be rejected, and no round may leave a child referencing an absent
    parent.
    """
    sn = "s" + _uid()
    with gnitz.connect(server) as setup:
        setup.create_schema(sn)
        setup.execute_sql("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
        setup.execute_sql(
            "CREATE TABLE child ("
            "  cid BIGINT NOT NULL PRIMARY KEY,"
            "  pid BIGINT NOT NULL REFERENCES parent(id)"
            ")",
            schema_name=sn,
        )
        ptid, pschema = setup.resolve_table(sn, "parent")
        ctid, cschema = setup.resolve_table(sn, "child")

    try:
        with gnitz.connect(server) as setup, gnitz.connect(server) as a, gnitz.connect(server) as b:
            for _ in range(20):
                # Reset to {parent present, child absent}, touching only what is
                # actually there — a retraction of an absent row is not a no-op.
                children = [r.cid for r in setup.scan(ctid)]
                if children:
                    setup.delete(ctid, cschema, children)
                if not [r.id for r in setup.scan(ptid)]:
                    seed = gnitz.ZSetBatch(pschema)
                    seed.append(id=1)
                    setup.push(ptid, seed)

                child = gnitz.ZSetBatch(cschema)
                child.append(cid=2, pid=1)
                errors = _race(
                    ("push", lambda: a.push(ctid, child)),
                    ("delete", lambda: b.delete(ptid, pschema, [1])),
                )

                parents = {r.id for r in setup.scan(ptid)}
                orphans = [r.cid for r in setup.scan(ctid) if r.pid not in parents]
                assert not orphans, f"child rows {orphans} reference an absent parent"
                assert len(errors) == 1, (
                    f"expected exactly one of the two writes to be rejected, got {errors}"
                )
    finally:
        with gnitz.connect(server) as c:
            _cleanup(c, sn, "child", "parent")
