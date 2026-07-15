"""Incremental-maintenance cost for advanced aggregate shapes (push-driven).

One base table `e` with numeric and TEXT group keys; one view per shape. Deltas
stream via push() (inserts + retractions), so retractions exercise the MIN/MAX
non-linear retract path and group elimination. SQL is verbatim from Appendix A.
"""

from __future__ import annotations

import random

from helpers.datagen import (SHIPMODES, STATUS, feature_sz, push_stream,
                             seed_stream, stream_and_assert)

_E_DDL = ("CREATE TABLE e (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, "
          "g1 BIGINT NOT NULL, g2 BIGINT NOT NULL, flag TEXT NOT NULL, "
          "flag2 TEXT NOT NULL, g BIGINT NOT NULL, price BIGINT NOT NULL, "
          "disc DOUBLE NOT NULL)")


def _e_row(batch, pk, w):
    rng = random.Random(pk)
    batch.append(pk=pk, v=rng.randint(0, 1_000_000), g1=rng.randint(0, 50),
                 g2=rng.randint(0, 50), flag=rng.choice(STATUS),
                 flag2=rng.choice(SHIPMODES), g=rng.randint(0, 100),
                 price=rng.randint(100, 100000), disc=round(rng.uniform(0.0, 0.1), 2),
                 weight=w)


_base_seed, _stream = seed_stream(_e_row)


def _run(client, sn, bench_timer, view_ddls, read_view, sz):
    client.execute_sql(_E_DDL, schema_name=sn)
    for ddl in view_ddls:
        client.execute_sql(ddl, schema_name=sn)
    tid, schema = client.resolve_table(sn, "e")
    push_stream(client, tid, schema, _base_seed, sz["base"])
    stream_and_assert(client, sn, bench_timer, tid, schema, _stream, sz, read_view)


def test_global(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT SUM(v) AS s, COUNT(*) AS n, MIN(v) AS mn, MAX(v) AS mx FROM e",
    ], "v", feature_sz(scale_mode))


def test_multi_group(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT g1, g2, SUM(v) AS s FROM e GROUP BY g1, g2",
    ], "v", feature_sz(scale_mode))


def test_group_string(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT flag, SUM(v) AS s, COUNT(*) AS n FROM e GROUP BY flag",
    ], "v", feature_sz(scale_mode))


def test_group_two_string(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT flag, flag2, SUM(v) AS s FROM e GROUP BY flag, flag2",
    ], "v", feature_sz(scale_mode))


def test_sum_inner_expr(client, schema_name, bench_timer, scale_mode):
    # R4 decomposition: inner projection view computes the expression, outer
    # aggregate sums it.
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW vb AS SELECT pk AS id, g, price * (1 - disc) AS net FROM e",
        "CREATE VIEW vg AS SELECT g, SUM(net) AS rev FROM vb GROUP BY g",
    ], "vg", feature_sz(scale_mode))
