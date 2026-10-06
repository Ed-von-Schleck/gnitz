"""A consumer's code, type-checked and never run: what the package's types say
each client class returns. `test_typing` runs mypy over it under `--strict`, so
an `assert_type` that no longer holds fails there, and so does a `type: ignore`
on a line that has stopped being an error.
"""
import asyncio
from typing import Any, assert_type

import gnitz
import gnitz.aio
from gnitz import ColumnDef, PollResult, Row, ScanResult, Schema, TypeCode, ZSetBatch

KV = Schema([ColumnDef("pk", TypeCode.U64), ColumnDef("val", TypeCode.I64, is_nullable=True)], [0])


def batches() -> ZSetBatch:
    batch = ZSetBatch(KV).append(pk=1, val=10).extend([{"pk": 2, "val": None}], _weight=-1)
    assert_type(batch, ZSetBatch)
    assert_type(len(batch), int)
    for row in batch.rows():
        assert_type(row, Row)
        assert_type(row._weight, int)
        assert_type(row._fields, tuple[str, ...])
        assert_type(row._asdict(), dict[str, Any])
        assert_type(row.val, Any)
        assert_type(row["val"], Any)
        assert_type(row[0], Any)
    ColumnDef("pk")  # type: ignore[call-arg]
    return batch


def blocking(client: gnitz.GnitzClient) -> None:
    assert_type(gnitz.connect("sock"), gnitz.GnitzClient)
    tid = client.create_table("t", KV)
    assert_type(tid, int)
    assert_type(client.push(tid, batches()), int)
    assert_type(client.push(tid, batches(), mode="error"), int)
    client.push(tid, batches(), mode="upsert")  # type: ignore[arg-type]
    client.push("t", batches())  # type: ignore[arg-type]
    assert_type(client.delete(tid, KV, [1, 2]), None)
    assert_type(client.scan(tid, KV), ScanResult)
    assert_type(client.scan(tid, KV).lsn, int | None)
    assert_type(client.seek(tid, KV, 1), ScanResult)
    assert_type(client.seek_by_index(tid, KV, [1], [10]), ScanResult)
    assert_type(client.scan_many([(tid, KV)]), list[ScanResult])
    assert_type(client.resolve_table("t"), tuple[int, Schema])
    rows, cursor = client.delta_bootstrap(tid, KV)
    assert_type(client.delta_poll(tid, KV, cursor), tuple[ScanResult, tuple[int, int]])
    assert_type(client.mirror_view("v"), PollResult)
    assert_type(client.poll(), list[PollResult])
    assert_type(client.cursor(tid), tuple[int, int] | None)
    assert_type(client.mirror_poisoned, str | None)
    assert_type(client.requests_sent, int)
    client.frobnicate()  # type: ignore[attr-defined]

    for result in client.execute_sql("SELECT pk FROM t"):
        assert_type(result, gnitz.SqlResult)
        if result["type"] == "Rows":
            assert_type(result["rows"], ScanResult)
        elif result["type"] == "RowsAffected":
            assert_type(result["count"], int)
        elif result["type"] == "TransactionCommitted":
            assert_type(result["lsn"], int)

    with client.transaction() as txn:
        assert_type(txn, gnitz.GnitzClient)
    with client.pipeline() as piped:
        lsn = piped.push(tid, batches())
        assert_type(lsn, gnitz.Pending[int])
        assert_type(piped.scan(tid, KV), gnitz.Pending[ScanResult])
        assert_type(piped.execute_sql("SELECT 1"), gnitz.Pending[list[gnitz.SqlResult]])
    assert_type(lsn.result(), int)
    with client as entered:
        assert_type(entered, gnitz.GnitzClient)
    assert_type(client.close(), None)


async def on_a_loop() -> None:
    async with gnitz.aio.connect("sock") as conn:
        assert_type(conn, gnitz.AsyncGnitzClient)
        tid = await conn.create_table("t", KV)
        assert_type(tid, int)
        pushed = conn.push(tid, batches())
        assert_type(pushed, asyncio.Future[int])
        assert_type(await asyncio.gather(pushed, conn.scan(tid, KV)), tuple[int, ScanResult])
        assert_type(await conn.execute_sql("SELECT 1"), list[gnitz.SqlResult])
        assert_type(await conn.requests_sent, int)
        async with conn.transaction() as txn:
            assert_type(txn, gnitz.AsyncGnitzClient)
        with conn.transaction():  # type: ignore[misc]
            pass
        conn.pipeline()  # type: ignore[attr-defined]
        conn.push(tid, batches()) + 1  # type: ignore[operator]
    assert_type(await gnitz.aio.connect("sock"), gnitz.AsyncGnitzClient)
    assert_type(await conn.aclose(), None)
