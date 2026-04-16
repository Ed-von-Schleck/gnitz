"""Regression tests for multi-source exchange routing and tick batching."""

from __future__ import annotations

import random
import gnitz


def _uid() -> str:
    return str(random.randint(100000, 999999))


def _scan_positive(client, tid):
    return [r for r in client.scan(tid) if r.weight > 0]


def test_multi_source_join_tick_consistency(client):
    """A view joining two tables must correctly route exchange relays when
    both sources are pushed in the same tick window."""
    sn = "exo_ms_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, "
            "a_pk BIGINT NOT NULL, label BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v1 AS SELECT b.pk, a.val, b.label "
            "FROM b INNER JOIN a ON b.a_pk = a.pk",
            schema_name=sn,
        )

        a_tid, a_schema = client.resolve_table(sn, "a")
        a_batch = gnitz.ZSetBatch(a_schema)
        for i in range(1, 51):
            a_batch.append(pk=i, val=i * 10)
        client.push(a_tid, a_batch)

        b_tid, b_schema = client.resolve_table(sn, "b")
        b_batch = gnitz.ZSetBatch(b_schema)
        for i in range(1, 51):
            b_batch.append(pk=i, a_pk=i, label=i * 100)
        client.push(b_tid, b_batch)

        v1_tid, _ = client.resolve_table(sn, "v1")
        assert len(_scan_positive(client, v1_tid)) == 50
    finally:
        client.drop_schema(sn)


def test_tick_backlog_flush(client):
    """Three tables accumulate tick rows; pushing to one forces a tick
    that must drain the backlog for all three without mis-routing."""
    sn = "exo_tbf_" + _uid()
    client.create_schema(sn)
    try:
        for t in ("x", "y", "z"):
            client.execute_sql(
                f"CREATE TABLE {t} (pk BIGINT NOT NULL PRIMARY KEY, "
                f"v BIGINT NOT NULL)",
                schema_name=sn,
            )
        for t in ("x", "y", "z"):
            tid, sch = client.resolve_table(sn, t)
            batch = gnitz.ZSetBatch(sch)
            for i in range(1, 11):
                batch.append(pk=i, v=i)
            client.push(tid, batch)
        for t in ("x", "y", "z"):
            tid, _ = client.resolve_table(sn, t)
            assert len(_scan_positive(client, tid)) == 10
    finally:
        client.drop_schema(sn)
