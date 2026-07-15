"""Incremental-maintenance cost for filter predicates (push-driven).

Single-table filter views over string / float / nullable columns; the streamed
mix both passes and fails each predicate so the filter circuit is genuinely
exercised. SQL follows the plan's Appendix A.
"""

from __future__ import annotations

import random

from helpers.datagen import (FEATURE_SIZES, NATIONS, SHIPMODES, push_stream,
                             seed_stream, stream_and_assert)

_E_DDL = ("CREATE TABLE e (pk BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, "
          "mode TEXT NOT NULL, d BIGINT NOT NULL, disc DOUBLE NOT NULL, "
          "note TEXT, v BIGINT NOT NULL)")


def _sz(scale_mode):
    return FEATURE_SIZES[scale_mode]


def _e_row(batch, pk, w):
    rng = random.Random(pk)
    note = None if rng.random() < 0.3 else rng.choice(NATIONS)
    batch.append(pk=pk, name=rng.choice(NATIONS), mode=rng.choice(SHIPMODES),
                 d=rng.randint(0, 300), disc=round(rng.uniform(0.0, 0.1), 3),
                 note=note, v=rng.randint(0, 1000), weight=w)


_base_seed, _stream = seed_stream(_e_row)


def _run(client, sn, bench_timer, view_ddl, sz):
    client.execute_sql(_E_DDL, schema_name=sn)
    client.execute_sql(view_ddl, schema_name=sn)
    tid, schema = client.resolve_table(sn, "e")
    push_stream(client, tid, schema, _base_seed, sz["base"])
    stream_and_assert(client, sn, bench_timer, tid, schema, _stream, sz, "v")


def test_string_eq(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer,
         "CREATE VIEW v AS SELECT pk AS id, v FROM e WHERE name = 'GERMANY'",
         _sz(scale_mode))


def test_string_in_list(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer,
         "CREATE VIEW v AS SELECT pk AS id, v FROM e WHERE mode IN ('AIR', 'SHIP', 'MAIL')",
         _sz(scale_mode))


def test_between_float(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer,
         "CREATE VIEW v AS SELECT pk AS id FROM e "
         "WHERE d BETWEEN 100 AND 200 AND disc BETWEEN 0.05 AND 0.07",
         _sz(scale_mode))


def test_is_not_null(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer,
         "CREATE VIEW v AS SELECT pk AS id, v FROM e WHERE note IS NOT NULL",
         _sz(scale_mode))
