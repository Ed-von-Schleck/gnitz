"""Scenarios of the Python binding alone: what its encode and decode paths cost
with no request in them, and what an awaited call costs on an event loop."""

from __future__ import annotations

import asyncio
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from gnitz import ColumnDef, Schema, TypeCode, ZSetBatch, aio

from harness.run import time_pushes

from .base import Scenario

N = 10_000


def _i64(ncols):
    """`c0 … c{ncols-1}`, all I64, keyed on `c0`."""
    return Schema([ColumnDef(f"c{i}", TypeCode.I64) for i in range(ncols)], [0])


def _pk_and(tc, **kw):
    """`(pk I64, x <tc>)`, keyed on `pk`."""
    return Schema([ColumnDef("pk", TypeCode.I64), ColumnDef("x", tc, **kw)], [0])


def _dicts(ncols, weight=None):
    names = [sys.intern(f"c{c}") for c in range(ncols)]
    rows = [dict.fromkeys(names, i) for i in range(N)]
    if weight is not None:
        for r in rows:
            r["_weight"] = weight
    return rows


def _append_1(b):
    for i in range(N):
        b.append(c0=i)


def _append_3(b):
    for i in range(N):
        b.append(c0=i, c1=i, c2=i)


def _append_10(b):
    for i in range(N):
        b.append(c0=i, c1=i, c2=i, c3=i, c4=i, c5=i, c6=i, c7=i, c8=i, c9=i)


def _append_typed(b, x):
    for i in range(N):
        b.append(pk=i, x=x)


def _iterate(result):
    n = 0
    for _ in result:
        n += 1
    assert n == N


def _present(b):
    for _ in range(N):
        b.rows()


def _attr(r, _):
    for _ in range(N):
        r.c0


def _index(r, _):
    for _ in range(N):
        r[1]


def _name(r, _):
    for _ in range(N):
        r["c0"]


def _unpack(r, _):
    for _ in range(N):
        a, b, c = r


def _eq_row(r, r2):
    for _ in range(N):
        r == r2


def _eq_other(r, _):
    for _ in range(N):
        r == 1


APPENDED = {
    "uuid": (_pk_and(TypeCode.UUID), uuid.UUID(int=0x1234_5678_9ABC_DEF0_1234_5678_9ABC_DEF0)),
    "decimal": (_pk_and(TypeCode.DECIMAL, scale=2), Decimal("1.23")),
    "decimal_str": (_pk_and(TypeCode.DECIMAL, scale=2), "1.23"),
    "decimal_float": (_pk_and(TypeCode.DECIMAL, scale=2), 1.23),
    "date": (_pk_and(TypeCode.DATE), date(2024, 2, 29)),
}
ITERATED = {
    "string": (_pk_and(TypeCode.STRING), "a string past the inline prefix"),
    "uuid": (_pk_and(TypeCode.UUID), "12345678-1234-5678-1234-567812345678"),
    "decimal": (_pk_and(TypeCode.DECIMAL, scale=2), Decimal("1.23")),
    "date": (_pk_and(TypeCode.DATE), date(2024, 2, 29)),
    "timestamp": (_pk_and(TypeCode.TIMESTAMP), datetime(2024, 2, 29, 13, 45, 7, 250_000)),
}
ROW_READS = {"attr": _attr, "index": _index, "name": _name, "unpack": _unpack, "eq_row": _eq_row,
             "eq_other": _eq_other}


@dataclass
class Codec(Scenario):
    """Instructions per row, or per call, on the binding's encode and decode
    paths. No phase sends a request: its one number is the client's."""

    def run(self, run):
        def case(name, fn):
            with run.phase(name) as ph:
                ph.count(fn, N)

        for ncols, body in ((1, _append_1), (3, _append_3), (10, _append_10)):
            b = ZSetBatch(_i64(ncols))
            case(f"append_{ncols}", lambda: body(b))
        for name, (schema, x) in APPENDED.items():
            b = ZSetBatch(schema)
            case(f"append_{name}", lambda: _append_typed(b, x))
        for ncols in (3, 10):
            for label, weight in (("", None), ("_weighted", 2)):
                rows, b = _dicts(ncols, weight), ZSetBatch(_i64(ncols))
                case(f"extend_{ncols}{label}", lambda: b.extend(rows))
        for ncols in (1, 3, 16):
            result = ZSetBatch(_i64(ncols)).extend(_dicts(ncols)).rows()
            case(f"iterate_{ncols}", lambda: _iterate(result))
        for name, (schema, x) in ITERATED.items():
            result = ZSetBatch(schema).extend([{"pk": i, "x": x} for i in range(N)]).rows()
            case(f"iterate_{name}", lambda: _iterate(result))
        for ncols in (3, 16):
            b = ZSetBatch(_i64(ncols)).extend([{f"c{c}": c for c in range(ncols)}])
            case(f"present_{ncols}", lambda: _present(b))
        one = ZSetBatch(_i64(3)).extend([dict(c0=0, c1=1, c2=2)])
        (r,), (r2,) = one.rows(), one.rows()
        for name, read in ROW_READS.items():
            case(f"row_{name}", lambda: read(r, r2))


def _spin(stop):
    x = 1
    while not stop.is_set():
        x = (x * 3 + 1) & 0xFFFF


async def _push_loop(target, ops):
    async with aio.connect(target) as conn:
        return await time_pushes(conn, ops)


@dataclass
class Async(Scenario):
    """`gnitz.aio` per call: a loop of awaited one-row pushes, on an idle
    interpreter and beside a thread spinning pure-Python arithmetic — where a
    released GIL costs a switch interval per call."""

    def run(self, run):
        n = max(run.rows // 100, 1000)
        run.ddl("CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, val BIGINT NOT NULL)")
        for name, busy in (("idle", False), ("beside_a_busy_thread", True)):
            stop = threading.Event()
            spinner = threading.Thread(target=_spin, args=(stop,), daemon=True) if busy else None
            ops = run.ops("t", (dict(pk=i, val=i) for i in range(n)), 1)
            with run.phase(name) as ph:
                if spinner:
                    spinner.start()
                try:
                    ph.add("push", asyncio.run(_push_loop(run.server.sock_path, ops)), rows=n)
                finally:
                    stop.set()
                    if spinner:
                        spinner.join()


SCENARIOS = [
    Codec("codec", "client", "the binding's encode and decode paths, in instructions per row with no request sent"),
    Async("async", "client", "awaited one-row pushes on `gnitz.aio`, on an idle interpreter and beside a busy thread"),
]
