"""Instructions retired per row, or per call, on the binding's encode and decode
paths. No case sends a request, and none is timed."""

import sys
import uuid
from datetime import date, datetime
from decimal import Decimal

import pytest

from gnitz import ColumnDef, Schema, TypeCode, ZSetBatch
from gnitz._native import instructions_retired

N = 10_000


def _i64(ncols):
    """`c0 … c{ncols-1}`, all I64, keyed on `c0`."""
    return Schema([ColumnDef(f"c{i}", TypeCode.I64) for i in range(ncols)], [0])


def _pk_and(tc, **kw):
    """`(pk I64, x <tc>)`, keyed on `pk`."""
    return Schema([ColumnDef("pk", TypeCode.I64), ColumnDef("x", tc, **kw)], [0])


def _dicts(ncols):
    """`N` row dicts over `_i64(ncols)`."""
    names = [sys.intern(f"c{c}") for c in range(ncols)]
    return [dict.fromkeys(names, i) for i in range(N)]


def _count(bench_timer, f, per=N):
    n, counts_kernel = instructions_retired(f)
    bench_timer.extra["instructions"] = round(n / per, 1)
    bench_timer.extra["counts_kernel"] = counts_kernel


# ---------------------------------------------------------------------------
# append
# ---------------------------------------------------------------------------


def _append_1(b):
    for i in range(N):
        b.append(c0=i)


def _append_3(b):
    for i in range(N):
        b.append(c0=i, c1=i, c2=i)


def _append_10(b):
    for i in range(N):
        b.append(c0=i, c1=i, c2=i, c3=i, c4=i, c5=i, c6=i, c7=i, c8=i, c9=i)


@pytest.mark.parametrize("ncols,body", [(1, _append_1), (3, _append_3), (10, _append_10)],
                         ids=["1", "3", "10"])
def test_append_i64(bench_timer, ncols, body):
    b = ZSetBatch(_i64(ncols))
    _count(bench_timer, lambda: body(b))
    assert len(b) == N


_APPENDED = {
    "uuid": (_pk_and(TypeCode.UUID), uuid.UUID(int=0x1234_5678_9ABC_DEF0_1234_5678_9ABC_DEF0)),
    "decimal": (_pk_and(TypeCode.DECIMAL, scale=2), Decimal("1.23")),
    "decimal-str": (_pk_and(TypeCode.DECIMAL, scale=2), "1.23"),
    "decimal-float": (_pk_and(TypeCode.DECIMAL, scale=2), 1.23),
    "date": (_pk_and(TypeCode.DATE), date(2024, 2, 29)),
}


@pytest.mark.parametrize("schema,x", _APPENDED.values(), ids=_APPENDED.keys())
def test_append_typed(bench_timer, schema, x):
    b = ZSetBatch(schema)

    def body():
        for i in range(N):
            b.append(pk=i, x=x)

    _count(bench_timer, body)
    assert len(b) == N


# ---------------------------------------------------------------------------
# extend
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ncols", [3, 10])
@pytest.mark.parametrize("weighted", [False, True], ids=["plain", "weight"])
def test_extend(bench_timer, ncols, weighted):
    rows = _dicts(ncols)
    if weighted:
        for r in rows:
            r["_weight"] = 2
    b = ZSetBatch(_i64(ncols))
    _count(bench_timer, lambda: b.extend(rows))
    assert len(b) == N


# ---------------------------------------------------------------------------
# iterating a result
# ---------------------------------------------------------------------------


def _iterate(bench_timer, batch):
    result = batch.rows()
    seen = []

    def body():
        n = 0
        for _ in result:
            n += 1
        seen.append(n)

    _count(bench_timer, body)
    assert seen == [N]


@pytest.mark.parametrize("ncols", [1, 3, 16])
def test_iterate_i64(bench_timer, ncols):
    _iterate(bench_timer, ZSetBatch(_i64(ncols)).extend(_dicts(ncols)))


_ITERATED = {
    "string": (_pk_and(TypeCode.STRING), "a string past the inline prefix"),
    "uuid": (_pk_and(TypeCode.UUID), "12345678-1234-5678-1234-567812345678"),
    "decimal": (_pk_and(TypeCode.DECIMAL, scale=2), Decimal("1.23")),
    "date": (_pk_and(TypeCode.DATE), date(2024, 2, 29)),
    "timestamp": (_pk_and(TypeCode.TIMESTAMP), datetime(2024, 2, 29, 13, 45, 7, 250_000)),
}


@pytest.mark.parametrize("schema,x", _ITERATED.values(), ids=_ITERATED.keys())
def test_iterate_typed(bench_timer, schema, x):
    _iterate(bench_timer, ZSetBatch(schema).extend([{"pk": i, "x": x} for i in range(N)]))


# ---------------------------------------------------------------------------
# presenting a result, and reading a row
# ---------------------------------------------------------------------------


def _one_row(ncols):
    return ZSetBatch(_i64(ncols)).extend([{f"c{c}": c for c in range(ncols)}])


@pytest.mark.parametrize("ncols", [3, 16])
def test_rows(bench_timer, ncols):
    b = _one_row(ncols)
    b.rows()
    last = []

    def body():
        for _ in range(N):
            result = b.rows()
        last.append(result)

    _count(bench_timer, body)
    assert len(last[0]) == 1


def _attr(r, _):
    for _ in range(N):
        v = r.c0
    return v


def _index(r, _):
    for _ in range(N):
        v = r[1]
    return v


def _name(r, _):
    for _ in range(N):
        v = r["c0"]
    return v


def _unpack(r, _):
    for _ in range(N):
        a, b, c = r
    return a, b, c


def _eq_row(r, r2):
    for _ in range(N):
        v = r == r2
    return v


def _eq_other(r, _):
    for _ in range(N):
        v = r == 1
    return v


_ROW_READS = {
    "attr": (_attr, 0),
    "index": (_index, 1),
    "name": (_name, 0),
    "unpack": (_unpack, (0, 1, 2)),
    "eq-row": (_eq_row, True),
    "eq-other": (_eq_other, False),
}


@pytest.mark.parametrize("read,want", _ROW_READS.values(), ids=_ROW_READS.keys())
def test_row(bench_timer, read, want):
    (r,), (r2,) = _one_row(3).rows(), _one_row(3).rows()
    got = []
    _count(bench_timer, lambda: got.append(read(r, r2)))
    assert got == [want]
