"""Incremental-maintenance cost as a function of column type (push-driven).

Holds the view shape (GROUP BY SUM) fixed while varying the key/payload type,
exposing the German-string / float / compound-key / narrow / wide-key overheads
the all-BIGINT suite hides. u128_uuid_key uses a passthrough view over a U128 PK
with a UUID payload. Deltas stream via push().
"""

from __future__ import annotations

import random

from helpers.datagen import (NATIONS, feature_sz, push_stream, seed_stream,
                             stream_and_assert)

NGROUP = 200


def _run(client, sn, bench_timer, table, ddl, view_ddl, read_view, row_fn, sz):
    client.execute_sql(ddl, schema_name=sn)
    client.execute_sql(view_ddl, schema_name=sn)
    tid, schema = client.resolve_table(sn, table)
    base_seed, stream = seed_stream(row_fn)
    push_stream(client, tid, schema, base_seed, sz["base"])
    stream_and_assert(client, sn, bench_timer, tid, schema, stream, sz, read_view)


def test_string_key(client, schema_name, bench_timer, scale_mode):
    def row(b, pk, w):
        rng = random.Random(pk)
        b.append(pk=pk, g=NATIONS[pk % len(NATIONS)], v=rng.randint(0, 1000), weight=w)
    _run(client, schema_name, bench_timer, "t",
         "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g TEXT NOT NULL, v BIGINT NOT NULL)",
         "CREATE VIEW v AS SELECT g, SUM(v) AS s, COUNT(*) AS n FROM t GROUP BY g",
         "v", row, feature_sz(scale_mode))


def test_float_payload(client, schema_name, bench_timer, scale_mode):
    def row(b, pk, w):
        rng = random.Random(pk)
        b.append(pk=pk, g=pk % NGROUP, f=round(rng.uniform(0.0, 1000.0), 4), weight=w)
    _run(client, schema_name, bench_timer, "t",
         "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, f DOUBLE NOT NULL)",
         "CREATE VIEW v AS SELECT g, SUM(f) AS s, AVG(f) AS a FROM t GROUP BY g",
         "v", row, feature_sz(scale_mode))


def test_compound_pk_base(client, schema_name, bench_timer, scale_mode):
    def row(b, pk, w):
        rng = random.Random(pk)
        b.append(l_order=pk, l_line=1, l_ship=pk % NGROUP, l_qty=rng.randint(1, 50), weight=w)
    _run(client, schema_name, bench_timer, "li",
         "CREATE TABLE li (l_order BIGINT NOT NULL, l_line BIGINT NOT NULL, "
         "l_ship BIGINT NOT NULL, l_qty BIGINT NOT NULL, PRIMARY KEY (l_order, l_line))",
         "CREATE VIEW v AS SELECT l_ship, SUM(l_qty) AS s, COUNT(*) AS n FROM li GROUP BY l_ship",
         "v", row, feature_sz(scale_mode))


def test_narrow_ints(client, schema_name, bench_timer, scale_mode):
    def row(b, pk, w):
        rng = random.Random(pk)
        b.append(pk=pk, g=pk % NGROUP, a=rng.randint(-100, 100), bb=rng.randint(-1000, 1000),
                 c=rng.randint(0, 2000), weight=w)
    _run(client, schema_name, bench_timer, "t",
         "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
         "a SMALLINT NOT NULL, bb INT NOT NULL, c INT UNSIGNED NOT NULL)",
         "CREATE VIEW v AS SELECT g, SUM(a) AS sa, SUM(bb) AS sb, SUM(c) AS sc FROM t GROUP BY g",
         "v", row, feature_sz(scale_mode))


def test_u128_uuid_key(client, schema_name, bench_timer, scale_mode):
    def row(b, pk, w):
        rng = random.Random(pk)
        b.append(pk=pk, uid=rng.getrandbits(120), v=rng.randint(0, 1000), weight=w)
    _run(client, schema_name, bench_timer, "t",
         "CREATE TABLE t (pk DECIMAL(38,0) NOT NULL PRIMARY KEY, uid UUID NOT NULL, v BIGINT NOT NULL)",
         "CREATE VIEW v AS SELECT pk, uid, v FROM t",
         "v", row, feature_sz(scale_mode))
