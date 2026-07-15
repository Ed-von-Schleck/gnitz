"""Incremental-maintenance cost for projection-expression views (push-driven).

Single-table views (expressions allowed outside a JOIN projection). Deltas
stream via push(); the measured cost is projection maintenance. SQL follows the
plan's Appendix A (searched/simple CASE, COALESCE/NULLIF, int×float arithmetic).
"""

from __future__ import annotations

import random

from helpers.datagen import feature_sz, push_stream, seed_stream, stream_and_assert

_E_DDL = ("CREATE TABLE e (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
          "b BIGINT, price BIGINT NOT NULL, disc DOUBLE NOT NULL)")


def _e_row(batch, pk, w):
    rng = random.Random(pk)
    b = None if rng.random() < 0.1 else rng.randint(0, 1000)
    batch.append(pk=pk, a=rng.randint(0, 20), b=b, price=rng.randint(100, 100000),
                 disc=round(rng.uniform(0.0, 0.1), 2), weight=w)


_base_seed, _stream = seed_stream(_e_row)


def _run(client, sn, bench_timer, view_ddl, sz):
    client.execute_sql(_E_DDL, schema_name=sn)
    client.execute_sql(view_ddl, schema_name=sn)
    tid, schema = client.resolve_table(sn, "e")
    push_stream(client, tid, schema, _base_seed, sz["base"])
    stream_and_assert(client, sn, bench_timer, tid, schema, _stream, sz, "v")


def test_case(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer,
         "CREATE VIEW v AS SELECT pk AS id, "
         "CASE WHEN a > 10 THEN 1 WHEN a > 5 THEN 2 ELSE 0 END AS hi FROM e",
         feature_sz(scale_mode))


def test_simple_case(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer,
         "CREATE VIEW v AS SELECT pk AS id, "
         "CASE a WHEN 1 THEN 111 WHEN 2 THEN 222 ELSE 999 END AS m FROM e",
         feature_sz(scale_mode))


def test_coalesce_nullif(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer,
         "CREATE VIEW v AS SELECT pk AS id, COALESCE(b, 0) AS bz, NULLIF(a, 0) AS an FROM e",
         feature_sz(scale_mode))


def test_mixed_arith(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer,
         "CREATE VIEW v AS SELECT pk AS id, price * (1 - disc) AS net FROM e",
         feature_sz(scale_mode))
