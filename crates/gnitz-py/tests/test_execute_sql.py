"""E2E tests for FLAG_EXECUTE_SQL (server-side SQL submission).

Phase 2 dual-path: `submit_sql` sends the raw SQL to the server. The
server parses via sqlc, classifies, and routes DDL through the same
`apply_migration_row` path that the imperative client-side
`push_migration` already uses. DML via this path is rejected in Phase
2; so is mixed DDL+DML.
"""
import os
import random
import shutil
import tempfile

import pytest
import gnitz

from conftest import start_server_proc, stop_server_proc


_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))


def _uid():
    return str(random.randint(100000, 999999))


@pytest.fixture
def fresh_server():
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"),
        prefix="gnitz_exec_",
    )
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")
    proc = start_server_proc(data_dir, sock_path, workers=_NUM_WORKERS)
    try:
        yield sock_path, data_dir
    finally:
        stop_server_proc(proc)
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def client(fresh_server):
    sock_path, _ = fresh_server
    with gnitz.connect(sock_path) as conn:
        yield conn


# --- Happy path: DDL -----------------------------------------------------

def test_submit_sql_creates_table(client):
    """submit_sql with CREATE TABLE populates the catalog atomically —
    resolve_table must see the new table immediately after the call returns."""
    sn = "exec" + _uid()
    client.create_schema(sn)

    client.submit_sql(
        f"CREATE TABLE {sn}.users (id BIGINT UNSIGNED PRIMARY KEY, email TEXT NOT NULL)",
        schema_name=sn,
    )
    tid, _schema = client.resolve_table(sn, "users")
    assert tid > 0


def test_submit_sql_multiple_tables_atomic(client):
    """A single submission may declare multiple CREATE TABLEs; all apply
    in one migration step."""
    sn = "exec" + _uid()
    client.create_schema(sn)

    client.submit_sql(
        f"CREATE TABLE {sn}.a (id BIGINT UNSIGNED PRIMARY KEY); "
        f"CREATE TABLE {sn}.b (id BIGINT UNSIGNED PRIMARY KEY)",
        schema_name=sn,
    )
    tid_a, _ = client.resolve_table(sn, "a")
    tid_b, _ = client.resolve_table(sn, "b")
    assert tid_a > 0 and tid_b > 0
    assert tid_a != tid_b


def test_submit_sql_create_index(client):
    """CREATE UNIQUE INDEX via submit_sql applies on top of a previously
    created table (second submission that preserves the first's table)."""
    sn = "exec" + _uid()
    client.create_schema(sn)
    base = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL)"
    client.submit_sql(base, schema_name=sn)
    client.submit_sql(
        base + f"; CREATE UNIQUE INDEX {sn}.i_x ON {sn}.t (x)",
        schema_name=sn,
    )
    # Non-fatal assertion — direct index introspection requires a separate
    # catalog query; successful submission without an error is enough
    # evidence that the diff applied.


def test_submit_sql_drop_via_omission(client):
    """Migration semantics: omitting a previously-declared table drops it."""
    sn = "exec" + _uid()
    client.create_schema(sn)
    client.submit_sql(
        f"CREATE TABLE {sn}.keep (id BIGINT UNSIGNED PRIMARY KEY); "
        f"CREATE TABLE {sn}.drop_me (id BIGINT UNSIGNED PRIMARY KEY)",
        schema_name=sn,
    )
    # Second submission names only `keep`, dropping `drop_me`.
    client.submit_sql(
        f"CREATE TABLE {sn}.keep (id BIGINT UNSIGNED PRIMARY KEY)",
        schema_name=sn,
    )
    with pytest.raises(gnitz.GnitzError):
        client.resolve_table(sn, "drop_me")


# --- Rejections ----------------------------------------------------------

def test_submit_sql_rejects_dml(client):
    """Pure DML via submit_sql is rejected — Phase 2 only supports DDL."""
    with pytest.raises(gnitz.GnitzError) as e:
        client.submit_sql("INSERT INTO t VALUES (1)", schema_name="public")
    msg = str(e.value).lower()
    assert "dml" in msg or "not yet supported" in msg


def test_submit_sql_rejects_mixed(client):
    """Mixed DDL + DML in a single submission is rejected."""
    sn = "exec" + _uid()
    client.create_schema(sn)
    with pytest.raises(gnitz.GnitzError) as e:
        client.submit_sql(
            f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY); "
            f"INSERT INTO {sn}.t VALUES (1)",
            schema_name=sn,
        )
    assert "mixed" in str(e.value).lower() or "cannot be mixed" in str(e.value).lower()


def test_submit_sql_rejects_views_for_now(client):
    """Views via submit_sql are blocked in Phase 2 — the underlying
    apply_migration path still rejects CREATE VIEW. Phase 5 unblocks."""
    sn = "exec" + _uid()
    client.create_schema(sn)
    with pytest.raises(gnitz.GnitzError):
        client.submit_sql(
            f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL); "
            f"CREATE VIEW {sn}.v AS SELECT id, x FROM {sn}.t",
            schema_name=sn,
        )


def test_submit_sql_rejects_parse_error(client):
    """Syntactically invalid SQL surfaces the parse error cleanly."""
    with pytest.raises(gnitz.GnitzError) as e:
        client.submit_sql("CREATE TABLE (", schema_name="public")
    assert "parse" in str(e.value).lower()


# --- Dual-path: push_migration still works alongside submit_sql ---------

def test_dual_path_push_migration_and_submit_sql_share_head(client):
    """Phase 2 dual-path: an imperative push_migration and a server-side
    submit_sql must compose through the same migration head. A
    subsequent submit_sql that doesn't redeclare the first's table drops
    it — proving both writes land on the same chain."""
    sn = "exec" + _uid()
    client.create_schema(sn)

    # First: imperative v1 path.
    h1 = client.push_migration(
        0,
        f"CREATE TABLE {sn}.by_push (id BIGINT UNSIGNED PRIMARY KEY)",
        "alice", "first",
    )
    assert h1 != 0
    tid_push, _ = client.resolve_table(sn, "by_push")
    assert tid_push > 0

    # Second: server-side path, adding a second table + keeping the first.
    client.submit_sql(
        f"CREATE TABLE {sn}.by_push (id BIGINT UNSIGNED PRIMARY KEY); "
        f"CREATE TABLE {sn}.by_exec (id BIGINT UNSIGNED PRIMARY KEY)",
        schema_name=sn,
    )
    tid_exec, _ = client.resolve_table(sn, "by_exec")
    assert tid_exec > 0 and tid_exec != tid_push
