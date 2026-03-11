"""Test multi-worker mode: --workers=4.

Requires a separate server fixture that starts with --workers 4.
These tests are skipped if the server binary is not found.
"""

import os
import subprocess
import tempfile
import time
import shutil
import random

import pytest

from gnitz_client import GnitzClient, GnitzError, TypeCode, ColumnDef
from gnitz_client.batch import ZSetBatch
from gnitz_client.types import Schema


@pytest.fixture(scope="module")
def worker_server():
    """Start gnitz-server-c with --workers=4."""
    tmpdir = tempfile.mkdtemp(prefix="gnitz_workers_")
    data_dir = os.path.join(tmpdir, "data")
    sock_path = os.path.join(tmpdir, "gnitz.sock")

    binary = os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.join(os.path.dirname(__file__), "..", "..", "gnitz-server-c"),
    )
    binary = os.path.abspath(binary)

    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")

    proc = subprocess.Popen(
        [binary, "--workers=4", data_dir, sock_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    for _ in range(100):
        if os.path.exists(sock_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        stdout, stderr = proc.communicate()
        raise RuntimeError(
            f"Server failed to start.\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}"
        )

    yield sock_path

    proc.kill()
    proc.wait()
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def wclient(worker_server):
    """Per-test client connection to worker server."""
    c = GnitzClient(worker_server)
    yield c
    c.close()


def _make_table(client):
    suffix = random.randint(10000, 99999)
    schema_name = f"wk_{suffix}"
    client.create_schema(schema_name)
    columns = [
        ColumnDef("pk", TypeCode.U64),
        ColumnDef("value", TypeCode.I64),
    ]
    tid = client.create_table(schema_name, f"t_{suffix}", columns, pk_col_idx=0)
    tbl_schema = Schema(columns=columns, pk_index=0)
    return tid, tbl_schema


def test_workers_insert_and_scan(wclient):
    """Insert rows via worker fan-out, scan them all back."""
    tid, tbl_schema = _make_table(wclient)

    num_rows = 100
    pks = list(range(1, num_rows + 1))
    vals = [i * 10 for i in pks]

    batch = ZSetBatch(
        schema=tbl_schema,
        pk_lo=pks,
        pk_hi=[0] * num_rows,
        weights=[1] * num_rows,
        nulls=[0] * num_rows,
        columns=[[], vals],
    )
    wclient.push(tid, tbl_schema, batch)

    schema, result = wclient.scan(tid)
    assert result is not None
    assert len(result.pk_lo) == num_rows

    # Verify all PKs present
    result_pks = sorted(result.pk_lo)
    assert result_pks == pks


def test_workers_large_batch(wclient):
    """Push 1000 rows, verify all returned by scan."""
    tid, tbl_schema = _make_table(wclient)

    num_rows = 1000
    pks = list(range(1, num_rows + 1))
    vals = [i * 7 for i in pks]

    batch = ZSetBatch(
        schema=tbl_schema,
        pk_lo=pks,
        pk_hi=[0] * num_rows,
        weights=[1] * num_rows,
        nulls=[0] * num_rows,
        columns=[[], vals],
    )
    wclient.push(tid, tbl_schema, batch)

    schema, result = wclient.scan(tid)
    assert result is not None
    assert len(result.pk_lo) == num_rows


def test_workers_ddl_create_table(wclient):
    """Create table while workers are running, then push and scan."""
    tid, tbl_schema = _make_table(wclient)

    batch = ZSetBatch(
        schema=tbl_schema,
        pk_lo=[1, 2, 3],
        pk_hi=[0, 0, 0],
        weights=[1, 1, 1],
        nulls=[0, 0, 0],
        columns=[[], [10, 20, 30]],
    )
    wclient.push(tid, tbl_schema, batch)

    schema, result = wclient.scan(tid)
    assert len(result.pk_lo) == 3


def test_workers_delete(wclient):
    """Insert rows, delete some, verify correct count."""
    tid, tbl_schema = _make_table(wclient)

    # Insert 5 rows
    batch = ZSetBatch(
        schema=tbl_schema,
        pk_lo=[1, 2, 3, 4, 5],
        pk_hi=[0, 0, 0, 0, 0],
        weights=[1, 1, 1, 1, 1],
        nulls=[0, 0, 0, 0, 0],
        columns=[[], [10, 20, 30, 40, 50]],
    )
    wclient.push(tid, tbl_schema, batch)

    # Delete 2 rows
    del_batch = ZSetBatch(
        schema=tbl_schema,
        pk_lo=[2, 4],
        pk_hi=[0, 0],
        weights=[-1, -1],
        nulls=[0, 0],
        columns=[[], [20, 40]],
    )
    wclient.push(tid, tbl_schema, del_batch)

    schema, result = wclient.scan(tid)
    assert len(result.pk_lo) == 3
    assert sorted(result.pk_lo) == [1, 3, 5]


# -- Helpers for view tests ------------------------------------------------

def _uid():
    return str(random.randint(100000, 999999))


def _make_table_and_schema(client):
    """Create a schema + table with (pk U64, value I64), return (tid, schema_name, tbl_schema)."""
    suffix = _uid()
    schema_name = f"wv_{suffix}"
    client.create_schema(schema_name)
    columns = [
        ColumnDef("pk", TypeCode.U64),
        ColumnDef("value", TypeCode.I64),
    ]
    tid = client.create_table(schema_name, f"t_{suffix}", columns, pk_col_idx=0)
    tbl_schema = Schema(columns=columns, pk_index=0)
    return tid, schema_name, tbl_schema


def _insert_rows(client, tid, tbl_schema, rows):
    """Insert rows as (pk, value) tuples."""
    batch = ZSetBatch(schema=tbl_schema)
    cols = [[], []]
    for pk, val in rows:
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(1)
        batch.nulls.append(0)
        cols[0].append(None)  # pk column (pk_index=0)
        cols[1].append(val)
    batch.columns = cols
    client.push(tid, tbl_schema, batch)


def _delete_rows(client, tid, tbl_schema, rows):
    """Delete rows as (pk, value) tuples."""
    batch = ZSetBatch(schema=tbl_schema)
    cols = [[], []]
    for pk, val in rows:
        batch.pk_lo.append(pk)
        batch.pk_hi.append(0)
        batch.weights.append(-1)
        batch.nulls.append(0)
        cols[0].append(None)
        cols[1].append(val)
    batch.columns = cols
    client.push(tid, tbl_schema, batch)


def _scan_rows(client, tid):
    """Scan and return sorted list of (pk, value) tuples."""
    schema, batch = client.scan(tid)
    if batch is None or len(batch.pk_lo) == 0:
        return []
    rows = []
    for i in range(len(batch.pk_lo)):
        rows.append((batch.pk_lo[i], batch.columns[1][i]))
    rows.sort()
    return rows


# -- View tests (DAG evaluation on workers) --------------------------------

def test_workers_view_passthrough(wclient):
    """Create table + passthrough view, push rows, verify view has all rows."""
    tid, sn, tbl_schema = _make_table_and_schema(wclient)
    cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("value", TypeCode.I64)]
    vid = wclient.create_view(sn, "v" + _uid(), tid, cols)

    _insert_rows(wclient, tid, tbl_schema, [(1, 10), (2, 20), (3, 30)])

    view_rows = _scan_rows(wclient, vid)
    assert len(view_rows) == 3
    assert view_rows == [(1, 10), (2, 20), (3, 30)]


def test_workers_view_deletes(wclient):
    """Push inserts, then deletes; view should reflect correct state."""
    tid, sn, tbl_schema = _make_table_and_schema(wclient)
    cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("value", TypeCode.I64)]
    vid = wclient.create_view(sn, "v" + _uid(), tid, cols)

    _insert_rows(wclient, tid, tbl_schema, [(1, 10), (2, 20), (3, 30)])
    _delete_rows(wclient, tid, tbl_schema, [(2, 20)])

    view_rows = _scan_rows(wclient, vid)
    assert len(view_rows) == 2
    assert view_rows == [(1, 10), (3, 30)]


def test_workers_view_cascade(wclient):
    """Table -> View A -> View B chain; verify View B gets data."""
    tid, sn, tbl_schema = _make_table_and_schema(wclient)
    cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("value", TypeCode.I64)]
    vid_a = wclient.create_view(sn, "va" + _uid(), tid, cols)
    vid_b = wclient.create_view(sn, "vb" + _uid(), vid_a, cols)

    _insert_rows(wclient, tid, tbl_schema, [(1, 100), (2, 200)])

    rows_a = _scan_rows(wclient, vid_a)
    rows_b = _scan_rows(wclient, vid_b)
    assert len(rows_a) == 2
    assert len(rows_b) == 2
    assert rows_a == rows_b


def test_workers_view_ddl_then_push(wclient):
    """Push to table first, then create view, push more; view sees second push."""
    tid, sn, tbl_schema = _make_table_and_schema(wclient)

    # Push before view exists
    _insert_rows(wclient, tid, tbl_schema, [(1, 10), (2, 20)])

    # Now create view
    cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("value", TypeCode.I64)]
    vid = wclient.create_view(sn, "v" + _uid(), tid, cols)

    # Push after view creation
    _insert_rows(wclient, tid, tbl_schema, [(3, 30), (4, 40)])

    view_rows = _scan_rows(wclient, vid)
    # View only sees the second push (passthrough views don't backfill)
    assert len(view_rows) == 2
    assert view_rows == [(3, 30), (4, 40)]


def test_workers_multiple_views_same_table(wclient):
    """Two views on the same table both get data in multi-worker mode."""
    tid, sn, tbl_schema = _make_table_and_schema(wclient)
    cols = [ColumnDef("pk", TypeCode.U64), ColumnDef("value", TypeCode.I64)]
    vid1 = wclient.create_view(sn, "va" + _uid(), tid, cols)
    vid2 = wclient.create_view(sn, "vb" + _uid(), tid, cols)

    _insert_rows(wclient, tid, tbl_schema, [(1, 10), (2, 20)])

    rows1 = _scan_rows(wclient, vid1)
    rows2 = _scan_rows(wclient, vid2)
    assert len(rows1) == 2
    assert len(rows2) == 2
