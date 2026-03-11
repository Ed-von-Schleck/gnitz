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

from gnitz_client import GnitzClient, TypeCode, ColumnDef
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
