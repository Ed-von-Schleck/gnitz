"""Pairwise-ordering smoke test: every ordered (a, b) pair of curated
fragments runs back-to-back against the session-scoped server, so
ordering regressions that only surface after one fragment leaves a
particular state behind are caught. Each fragment is self-contained:
creates its own schema, loads data, asserts, and drops the schema."""

from __future__ import annotations

import itertools
import uuid

import pytest
import gnitz


def _unique_schema(client, prefix: str) -> str:
    sn = f"{prefix}_{uuid.uuid4().hex[:8]}"
    client.create_schema(sn)
    return sn


def _scan_positive(client, tid):
    return [r for r in client.scan(tid) if r.weight > 0]


# ----------------------------------------------------------------------
# Fragments — each creates + drops its own schema.
# ----------------------------------------------------------------------

def _frag_simple_table(client):
    sn = _unique_schema(client, "pw_simple")
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        tid, sch = client.resolve_table(sn, "t")
        batch = gnitz.ZSetBatch(sch)
        for i in range(1, 21):
            batch.append(pk=i, v=i * 2)
        client.push(tid, batch)
        rows = _scan_positive(client, tid)
        assert len(rows) == 20
    finally:
        client.drop_schema(sn)


def _frag_chained_join(client):
    sn = _unique_schema(client, "pw_cj")
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, "
            "av BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, "
            "a_pk BIGINT NOT NULL, bv BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW vab AS SELECT b.pk, a.av, b.bv "
            "FROM b INNER JOIN a ON b.a_pk = a.pk",
            schema_name=sn,
        )
        a_tid, a_schema = client.resolve_table(sn, "a")
        ab = gnitz.ZSetBatch(a_schema)
        for i in range(1, 21):
            ab.append(pk=i, av=i)
        client.push(a_tid, ab)
        b_tid, b_schema = client.resolve_table(sn, "b")
        bb = gnitz.ZSetBatch(b_schema)
        for i in range(1, 21):
            bb.append(pk=i, a_pk=i, bv=i * 3)
        client.push(b_tid, bb)
        vid, _ = client.resolve_table(sn, "vab")
        rows = _scan_positive(client, vid)
        assert len(rows) == 20
    finally:
        client.drop_schema(sn)


def _frag_agg_view(client):
    sn = _unique_schema(client, "pw_agg")
    try:
        client.execute_sql(
            "CREATE TABLE m (pk BIGINT NOT NULL PRIMARY KEY, "
            "g BIGINT NOT NULL, amt BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW vg AS SELECT g, SUM(amt) AS total FROM m GROUP BY g",
            schema_name=sn,
        )
        tid, sch = client.resolve_table(sn, "m")
        b = gnitz.ZSetBatch(sch)
        for i in range(1, 31):
            b.append(pk=i, g=(i % 5), amt=i)
        client.push(tid, b)
        vid, _ = client.resolve_table(sn, "vg")
        rows = _scan_positive(client, vid)
        assert len(rows) == 5
    finally:
        client.drop_schema(sn)


def _frag_nested_view(client):
    sn = _unique_schema(client, "pw_nested")
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v1 AS SELECT pk, v FROM t WHERE v > 5",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v2 AS SELECT pk, v FROM v1 WHERE v < 50",
            schema_name=sn,
        )
        tid, sch = client.resolve_table(sn, "t")
        b = gnitz.ZSetBatch(sch)
        for i in range(1, 61):
            b.append(pk=i, v=i)
        client.push(tid, b)
        vid, _ = client.resolve_table(sn, "v2")
        rows = _scan_positive(client, vid)
        assert len(rows) == 44  # v>5 AND v<50 → 6..49 = 44 rows
    finally:
        client.drop_schema(sn)


def _frag_delete_pk(client):
    sn = _unique_schema(client, "pw_del")
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        tid, sch = client.resolve_table(sn, "t")
        b = gnitz.ZSetBatch(sch)
        for i in range(1, 21):
            b.append(pk=i, v=i)
        client.push(tid, b)
        for i in range(1, 6):
            client.execute_sql(f"DELETE FROM t WHERE pk = {i}", schema_name=sn)
        rows = _scan_positive(client, tid)
        assert len(rows) == 15
    finally:
        client.drop_schema(sn)


def _frag_scan_after_bulk(client):
    sn = _unique_schema(client, "pw_scan")
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        tid, sch = client.resolve_table(sn, "t")
        b = gnitz.ZSetBatch(sch)
        for i in range(1, 101):
            b.append(pk=i, v=i * i)
        client.push(tid, b)
        rows = _scan_positive(client, tid)
        assert len(rows) == 100
    finally:
        client.drop_schema(sn)


CURATED = [
    ("simple_table", _frag_simple_table),
    ("chained_join", _frag_chained_join),
    ("agg_view", _frag_agg_view),
    ("nested_view", _frag_nested_view),
    ("delete_pk", _frag_delete_pk),
    ("scan_after_bulk", _frag_scan_after_bulk),
]


@pytest.mark.parametrize(
    "a,b",
    [(x, y) for x, y in itertools.permutations([n for n, _ in CURATED], 2)],
    ids=lambda p: p,
)
def test_pair(client, a, b):
    """Run fragment `a` then fragment `b` back-to-back."""
    frag_by_name = dict(CURATED)
    frag_by_name[a](client)
    frag_by_name[b](client)
