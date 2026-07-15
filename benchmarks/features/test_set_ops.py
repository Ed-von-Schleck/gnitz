"""Incremental-maintenance cost per set operator (push-driven).

Two content-identical tables t1, t2 (pk, val) with overlapping value ranges; one
view per operator. Row identity is the full projected content (pk, val), which
matches the engine's content-hash leaf key. Deltas stream into one side so both
the additive (union) and non-linear clamp (positive_part) paths are exercised.
Set-op SQL is verbatim from the plan's Appendix A.
"""

from __future__ import annotations

import pytest

from helpers.datagen import FEATURE_SIZES, push_stream, seed_stream, stream_and_assert

VMOD = 500  # value domain: t1[1..dim] and t2[1..dim] share (pk, val) → overlap


def _sz(scale_mode):
    return FEATURE_SIZES[scale_mode]


_base_seed, _stream_build = seed_stream(
    lambda batch, pk, w: batch.append(pk=pk, val=pk % VMOD, weight=w))


def _setup(client, sn, view_sql, dim, base):
    client.execute_sql("CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql(view_sql, schema_name=sn)
    # t2 covers [1..dim]; t1 covers [1..base] (overlap on [1..dim], t1-only above).
    t2_tid, t2_schema = client.resolve_table(sn, "t2")
    push_stream(client, t2_tid, t2_schema, _base_seed, dim)
    t1_tid, t1_schema = client.resolve_table(sn, "t1")
    push_stream(client, t1_tid, t1_schema, _base_seed, base)


def _run(client, sn, bench_timer, view_sql, streamed_table, sz, *, allow_empty=False):
    _setup(client, sn, view_sql, sz["dim"], sz["base"])
    tid, schema = client.resolve_table(sn, streamed_table)
    stream_and_assert(client, sn, bench_timer, tid, schema, _stream_build, sz, "v",
                      allow_empty=allow_empty)


@pytest.mark.parametrize("op,keyword,empty", [
    ("union_all", "UNION ALL", False),
    ("union", "UNION", False),
    ("intersect", "INTERSECT", False),
    ("intersect_all", "INTERSECT ALL", False),
    ("except", "EXCEPT", False),
    ("except_all", "EXCEPT ALL", False),
])
def test_setop_stream_t1(client, schema_name, bench_timer, scale_mode, op, keyword, empty):
    """Stream deltas into t1 (the additive / left side of the operator)."""
    view = f"CREATE VIEW v AS SELECT * FROM t1 {keyword} SELECT * FROM t2"
    _run(client, schema_name, bench_timer, view, "t1", _sz(scale_mode), allow_empty=empty)


@pytest.mark.parametrize("op,keyword", [
    ("except", "EXCEPT"),
    ("except_all", "EXCEPT ALL"),
])
def test_setop_stream_t2(client, schema_name, bench_timer, scale_mode, op, keyword):
    """Stream deltas into t2 (the subtrahend side — drives the clamp differently).
    Streamed t2 rows are t2-only, so the EXCEPT view stays non-empty (t1's base)."""
    view = f"CREATE VIEW v AS SELECT * FROM t1 {keyword} SELECT * FROM t2"
    _run(client, schema_name, bench_timer, view, "t2", _sz(scale_mode))
