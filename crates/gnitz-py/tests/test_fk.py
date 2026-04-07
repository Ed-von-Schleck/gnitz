"""E2E tests for foreign key constraints via SQL.

Run:
    cd crates/gnitz-py && uv run pytest tests/test_fk.py -v --tb=short
"""
import random
import pytest
import gnitz


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

    def test_self_referential_fk_fails(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            with pytest.raises(gnitz.GnitzError, match="(?i)not found"):
                client.execute_sql(
                    "CREATE TABLE tree ("
                    "  id BIGINT NOT NULL PRIMARY KEY,"
                    "  parent_id BIGINT REFERENCES tree(id)"
                    ")",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn)
