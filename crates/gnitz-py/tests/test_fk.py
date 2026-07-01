"""E2E tests for foreign key constraints via SQL.

Run:
    cd crates/gnitz-py && uv run pytest tests/test_fk.py -v --tb=short
"""
import os
import random
import pytest
import gnitz

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


def _uid():
    return str(random.randint(100000, 999999))


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


class TestFkSelfReferenceSQL:
    """Self-referential FK via SQL is not supported (table doesn't exist yet)."""

    def test_self_referential_fk_supported(self, client):
        # Self-referencing FKs are now resolved against the in-flight column
        # list during CREATE TABLE (the referenced table is the one being
        # created), so the table is created successfully rather than failing
        # with "table not found".
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE tree ("
                "  id BIGINT NOT NULL PRIMARY KEY,"
                "  parent_id BIGINT REFERENCES tree(id)"
                ")",
                schema_name=sn,
            )
            assert client.resolve_table(sn, "tree")[0] > 0
        finally:
            _cleanup(client, sn, "tree")


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
            rows = [r for r in client.scan(child_tid) if r.weight > 0]
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
            rows = [r for r in client.scan(parent_tid) if r.weight > 0]
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
            rows = [r for r in client.scan(child_tid) if r.weight > 0]
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
