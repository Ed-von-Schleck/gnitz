"""Schema-migrations E2E tests.

Each test uses its own fresh server (per-test fixture below) because
migrations mutate the global `current_migration_hash` and later tests
would see stale-parent errors if they tried to start from the genesis
commit against a server that already has a head.
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
    """Per-test server. Each migration test needs a fresh catalog so
    `parent_hash=0` refers to an empty chain."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"),
        prefix="gnitz_mig_",
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
def mig_client(fresh_server):
    sock_path, _ = fresh_server
    with gnitz.connect(sock_path) as conn:
        yield conn


# --- Basic happy path -----------------------------------------------------

def test_push_migration_genesis_creates_table(mig_client):
    """Genesis commit (parent_hash=0) containing a CREATE TABLE must:
    (a) return a non-zero hash, (b) make the table visible for resolution."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)
    sql = f"CREATE TABLE {sn}.users (id BIGINT UNSIGNED PRIMARY KEY, email TEXT NOT NULL)"

    h = mig_client.push_migration(0, sql, "alice", "init")
    assert h != 0, "genesis commit must return a non-zero hash"

    # Table is live — resolve_table must find it.
    tid, _ = mig_client.resolve_table(sn, "users")
    assert tid > 0, f"migration should have created {sn}.users, but resolve_table returned {tid}"


def test_push_migration_chain_uses_new_head(mig_client):
    """Chain of two migrations — second must pass the first's hash as parent."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)

    sql1 = f"CREATE TABLE {sn}.a (id BIGINT UNSIGNED PRIMARY KEY)"
    h1 = mig_client.push_migration(0, sql1, "alice", "add a")
    assert h1 != 0

    sql2 = (f"CREATE TABLE {sn}.a (id BIGINT UNSIGNED PRIMARY KEY); "
            f"CREATE TABLE {sn}.b (id BIGINT UNSIGNED PRIMARY KEY)")
    h2 = mig_client.push_migration(h1, sql2, "alice", "add b")
    assert h2 != 0 and h2 != h1


def test_push_migration_drop_table(mig_client):
    """A migration omitting a previously-declared table drops it."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)
    sql1 = f"CREATE TABLE {sn}.to_drop (id BIGINT UNSIGNED PRIMARY KEY)"
    h1 = mig_client.push_migration(0, sql1, "alice", "add")

    # Empty desired state => drop the table.
    h2 = mig_client.push_migration(h1, "", "alice", "drop to_drop")
    assert h2 != 0

    # Table is gone — drop_table now fails.
    with pytest.raises(gnitz.GnitzError):
        mig_client.drop_table(sn, "to_drop")


def test_push_migration_create_drop_index(mig_client):
    """Create an index via migration, then drop the index (keeping the table)."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)
    sql1 = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL)"
    h1 = mig_client.push_migration(0, sql1, "alice", "table")

    sql2 = (sql1 + "; "
            f"CREATE UNIQUE INDEX {sn}.i_x ON {sn}.t (x)")
    h2 = mig_client.push_migration(h1, sql2, "alice", "add idx")
    assert h2 != 0

    # Drop just the index — keeping the table.
    h3 = mig_client.push_migration(h2, sql1, "alice", "drop idx")
    assert h3 != 0 and h3 != h2


# --- Scope guards ---------------------------------------------------------

def test_push_migration_stale_parent_rejected(mig_client):
    """Submitting with an outdated parent_hash is a protocol error."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)
    sql1 = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY)"
    h1 = mig_client.push_migration(0, sql1, "alice", "first")
    assert h1 != 0

    # Second push with parent_hash=0 (correct: h1) is stale.
    sql2 = (sql1 + "; "
            f"CREATE TABLE {sn}.u (id BIGINT UNSIGNED PRIMARY KEY)")
    with pytest.raises(gnitz.GnitzError) as e:
        mig_client.push_migration(0, sql2, "alice", "stale")
    assert "stale" in str(e.value).lower() or "parent" in str(e.value).lower()


def test_push_migration_empty_diff_rejected(mig_client):
    """Same desired state is a no-op; server rejects explicitly."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)
    sql = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY)"
    h1 = mig_client.push_migration(0, sql, "alice", "first")

    with pytest.raises(gnitz.GnitzError) as e:
        mig_client.push_migration(h1, sql, "alice", "noop")
    msg = str(e.value).lower()
    assert "no-op" in msg or "empty" in msg


def test_push_migration_modify_rejected(mig_client):
    """Modifying a table's column set is rejected — name-keyed diff treats
    adding a column as a modification, which is not supported."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)
    sql1 = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY)"
    h1 = mig_client.push_migration(0, sql1, "alice", "first")

    # Add a second column — name-keyed diff treats this as a modification.
    sql2 = (f"CREATE TABLE {sn}.t "
            f"(id BIGINT UNSIGNED PRIMARY KEY, name TEXT NOT NULL)")
    with pytest.raises(gnitz.GnitzError) as e:
        mig_client.push_migration(h1, sql2, "alice", "modify")
    err = str(e.value).lower()
    assert "modification" in err or "v1" in err


def test_push_migration_creates_view(mig_client):
    """V1.5: CREATE VIEW in a follow-up migration compiles the view's
    circuit client-side, embeds it in the canonical AST, and the
    server applies the 8-batch swap atomically."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)
    sql1 = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY, x BIGINT NOT NULL)"
    h1 = mig_client.push_migration(0, sql1, "alice", "tbl")

    sql2 = (sql1 + "; "
            f"CREATE VIEW {sn}.v AS SELECT * FROM {sn}.t WHERE x > 5")
    h2 = mig_client.push_migration(h1, sql2, "alice", "view")
    assert h2 != 0

    # Push rows and verify the view yields only the rows where x > 5.
    tid, _ = mig_client.resolve_table(sn, "t")
    cols = [gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("x", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    batch = gnitz.ZSetBatch(schema)
    batch.append(id=1, x=3)
    batch.append(id=2, x=10)
    mig_client.push(tid, batch)

    vid, _ = mig_client.resolve_table(sn, "v")
    rows = [r for r in mig_client.scan(vid) if r.weight > 0]
    assert len(rows) == 1
    assert rows[0].id == 2


def test_push_migration_drops_view_via_omission(mig_client):
    """Dropping a view by omitting it from the next desired-state
    triggers build_drop_view_batches → circuit + deps + cols + view
    retractions emitted atomically."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)
    # Same-migration view-on-table requires tid pre-allocation (a
    # V1.5 follow-up), so split this into two migrations.
    sql1 = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY)"
    h1 = mig_client.push_migration(0, sql1, "alice", "tbl")
    sql2 = sql1 + f"; CREATE VIEW {sn}.v AS SELECT * FROM {sn}.t"
    h2 = mig_client.push_migration(h1, sql2, "alice", "init")

    # Omit the view — must drop it.
    sql3 = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY)"
    h3 = mig_client.push_migration(h2, sql3, "alice", "drop v")
    assert h3 != 0
    with pytest.raises(gnitz.GnitzError):
        mig_client.resolve_table(sn, "v")


def test_push_migration_invalid_sql(mig_client):
    """Malformed SQL fails at parse time client-side."""
    with pytest.raises(gnitz.GnitzError) as e:
        mig_client.push_migration(0, "CREATE TABLE (this is not valid", "a", "m")
    assert "parse error" in str(e.value).lower()


# --- DML integration ------------------------------------------------------

def test_migration_created_table_supports_dml(mig_client):
    """A table created via migration must be fully usable for DML.
    Exercises the executor's schema cache path for a migration-made tid."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)
    sql = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY, v BIGINT NOT NULL)"
    h = mig_client.push_migration(0, sql, "alice", "create")
    assert h != 0

    tid, _ = mig_client.resolve_table(sn, "t")
    cols = [gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("v", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    batch = gnitz.ZSetBatch(schema)
    for i in range(1, 4):
        batch.append(id=i, v=i * 100)
    mig_client.push(tid, batch)

    rows = mig_client.scan(tid)
    live = [r for r in rows if r.weight > 0]
    assert sorted(r.id for r in live) == [1, 2, 3]
    assert sorted(r.v for r in live) == [100, 200, 300]


def test_migration_dropped_then_recreated_same_name_is_fresh(mig_client):
    """Drop a table via migration then create a table with the same name.
    The new table must be independent — fresh tid, no stale cached schema
    leaking from the dropped table."""
    sn = "mig" + _uid()
    mig_client.create_schema(sn)

    sql1 = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY, v BIGINT NOT NULL)"
    h1 = mig_client.push_migration(0, sql1, "alice", "create v1")
    tid1, _ = mig_client.resolve_table(sn, "t")

    # Populate cached schema by pushing a row.
    cols_v1 = [gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
               gnitz.ColumnDef("v", gnitz.TypeCode.I64)]
    b = gnitz.ZSetBatch(gnitz.Schema(cols_v1))
    b.append(id=1, v=42)
    mig_client.push(tid1, b)

    # Migration-drop the table.
    h2 = mig_client.push_migration(h1, "", "alice", "drop")
    assert h2 != 0

    # Accessing the dropped tid via name must fail cleanly.
    with pytest.raises(gnitz.GnitzError):
        mig_client.resolve_table(sn, "t")

    # Recreate with a different column shape and different name (same schema),
    # then with the same name. Server must allocate a fresh tid; the
    # old tid's cache (if any) must not poison the new operation.
    sql3 = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY, w TEXT NOT NULL)"
    h3 = mig_client.push_migration(h2, sql3, "alice", "create v2")
    assert h3 != 0
    tid2, _ = mig_client.resolve_table(sn, "t")
    assert tid2 != tid1, "recreated table must have a distinct tid"

    cols_v2 = [gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
               gnitz.ColumnDef("w", gnitz.TypeCode.STRING)]
    b = gnitz.ZSetBatch(gnitz.Schema(cols_v2))
    b.append(id=1, w="hello")
    mig_client.push(tid2, b)

    rows = mig_client.scan(tid2)
    live = [r for r in rows if r.weight > 0]
    assert len(live) == 1
    assert live[0].id == 1 and live[0].w == "hello"


# --- Persistence ----------------------------------------------------------

def test_migration_head_survives_full_restart():
    """Full restart test: start server, commit, SIGKILL, restart fresh,
    verify the head was rehydrated from sys_migrations."""
    tmpdir = tempfile.mkdtemp(
        dir=os.path.expanduser("~/git/gnitz/tmp"),
        prefix="gnitz_mig_restart_",
    )
    try:
        data_dir = os.path.join(tmpdir, "data")
        sock_path = os.path.join(tmpdir, "gnitz.sock")

        # First boot: apply migration, record hash.
        proc = start_server_proc(data_dir, sock_path, workers=_NUM_WORKERS)
        try:
            with gnitz.connect(sock_path) as c:
                sn = "mig" + _uid()
                c.create_schema(sn)
                sql = f"CREATE TABLE {sn}.t (id BIGINT UNSIGNED PRIMARY KEY)"
                h1 = c.push_migration(0, sql, "alice", "before restart")
                assert h1 != 0
        finally:
            stop_server_proc(proc)

        # Restart.
        if os.path.exists(sock_path):
            os.unlink(sock_path)
        proc = start_server_proc(data_dir, sock_path, workers=_NUM_WORKERS)
        try:
            with gnitz.connect(sock_path) as c:
                # Head is h1 now. parent_hash=0 must be rejected.
                sn2 = "mig" + _uid()
                c.create_schema(sn2)
                sql2 = f"CREATE TABLE {sn2}.x (id BIGINT UNSIGNED PRIMARY KEY)"
                with pytest.raises(gnitz.GnitzError) as e:
                    c.push_migration(0, sql2, "alice", "stale-after-restart")
                assert "stale" in str(e.value).lower() or "parent" in str(e.value).lower()

                # Chain from h1 succeeds.
                h2 = c.push_migration(h1, sql2, "alice", "after-restart")
                assert h2 != 0 and h2 != h1
        finally:
            stop_server_proc(proc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
