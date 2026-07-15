"""Incremental-maintenance cost per JOIN shape (push-driven).

Fact/dim star: the dim is bulk-loaded, the fact is streamed via push() (inserts
+ weight=-1 retractions), so the measured number is the DBSP join circuit, not
the parser. One view per join shape. Join views project only column refs (R1).
Fact join keys are drawn Zipfian so fan-out and the exchange see realistic skew.
SQL is verbatim from the plan's Appendix A.

Each shape exposes a factory returning (base_seed, stream_build): the base seed
fills PKs 1..base for push_stream; the stream build fills PKs above STREAM_BASE
for stream_deltas. Both draw from the same value distribution.
"""

from __future__ import annotations

from helpers.datagen import (feature_sz, push_stream, stream_and_assert,
                             stream_factory, zipf_choice)

SKEW_S = 1.1
RANGE_DIM = 64               # small dim for pure-range joins (bounds fan-out)
# Pure-range joins are quadratic in fan-out (each fact row matches ~RANGE_DIM/2
# dim rows), so cap the fact base/delta to keep the materialized view bounded.
RANGE_BASE_CAP = 30_000
RANGE_DELTA_CAP = 500


def _seed(client, sn, name, build, count):
    tid, schema = client.resolve_table(sn, name)
    push_stream(client, tid, schema, build, count)


def _run(client, bench_timer, sn, fact_name, stream_build, view_name, sz):
    fact_tid, fact_schema = client.resolve_table(sn, fact_name)
    stream_and_assert(client, sn, bench_timer, fact_tid, fact_schema, stream_build, sz, view_name)


# ---------------------------------------------------------------------------
# Equi-key star shapes: a(pk, k, av) fact ⋈ b(pk, k, bv) dim on k
# ---------------------------------------------------------------------------

def _make_ab(client, sn):
    client.execute_sql("CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, "
                       "k BIGINT NOT NULL, av BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, "
                       "k BIGINT NOT NULL, bv BIGINT NOT NULL)", schema_name=sn)


def _load_dim_b(client, sn, dim):
    # dim keyed by k == pk so each fact.k matches exactly one dim row.
    _seed(client, sn, "b", lambda b, i: b.append(pk=i + 1, k=i + 1, bv=(i * 7) % 1000), dim)


def _fact_ab(dim):
    return stream_factory(
        lambda batch, pk, v, w: batch.append(pk=pk, k=v[0], av=v[1], weight=w),
        lambda rng: (zipf_choice(rng, dim, SKEW_S), rng.randint(0, 1000)),
    )


def _equi_shape(client, sn, bench_timer, sz, view_name, view_sql):
    _make_ab(client, sn)
    client.execute_sql(view_sql, schema_name=sn)
    _load_dim_b(client, sn, sz["dim"])
    base, stream = _fact_ab(sz["dim"])
    _seed(client, sn, "a", base, sz["base"])
    _run(client, bench_timer, sn, "a", stream, view_name, sz)


def test_right(client, schema_name, bench_timer, scale_mode):
    _equi_shape(client, schema_name, bench_timer, feature_sz(scale_mode), "v_right",
                "CREATE VIEW v_right AS SELECT a.k AS k, a.av AS av, b.bv AS bv "
                "FROM a RIGHT JOIN b ON a.k = b.k")


def test_full(client, schema_name, bench_timer, scale_mode):
    _equi_shape(client, schema_name, bench_timer, feature_sz(scale_mode), "v_full",
                "CREATE VIEW v_full AS SELECT a.k AS k, a.av AS av, b.bv AS bv "
                "FROM a FULL JOIN b ON a.k = b.k")


def test_residual(client, schema_name, bench_timer, scale_mode):
    _equi_shape(client, schema_name, bench_timer, feature_sz(scale_mode), "v_resid",
                "CREATE VIEW v_resid AS SELECT a.k AS k, a.av AS av, b.bv AS bv "
                "FROM a JOIN b ON a.k = b.k AND a.av <> b.bv")


# ---------------------------------------------------------------------------
# Three-way multiway (join-body CTE): a ⋈ b ⋈ d on k
# ---------------------------------------------------------------------------

def test_multiway3(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, feature_sz(scale_mode)
    _make_ab(client, sn)
    client.execute_sql("CREATE TABLE d (pk BIGINT NOT NULL PRIMARY KEY, "
                       "k BIGINT NOT NULL, dv BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_mw3 AS WITH h0 AS (SELECT a.k AS k, a.av AS av, b.bv AS bv "
        "FROM a JOIN b ON a.k=b.k) SELECT h0.k AS k, h0.av AS av, h0.bv AS bv, "
        "d.dv AS dv FROM h0 JOIN d ON h0.k = d.k", schema_name=sn)
    _load_dim_b(client, sn, sz["dim"])
    _seed(client, sn, "d", lambda b, i: b.append(pk=i + 1, k=i + 1, dv=(i * 3) % 1000), sz["dim"])
    base, stream = _fact_ab(sz["dim"])
    _seed(client, sn, "a", base, sz["base"])
    _run(client, bench_timer, sn, "a", stream, "v_mw3", sz)


# ---------------------------------------------------------------------------
# Self join: emp(id, mgr, sal) e ⋈ emp m ON e.mgr = m.id
# ---------------------------------------------------------------------------

def test_self(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, feature_sz(scale_mode)
    dim = sz["dim"]
    client.execute_sql("CREATE TABLE emp (id BIGINT NOT NULL PRIMARY KEY, "
                       "mgr BIGINT NOT NULL, sal BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE VIEW v_self AS SELECT e.id AS id, e.sal AS esal, "
                       "m.sal AS msal FROM emp e JOIN emp m ON e.mgr = m.id", schema_name=sn)
    # Managers occupy ids 1..dim; every employee's mgr points into that range.
    _seed(client, sn, "emp",
          lambda b, i: b.append(id=i + 1, mgr=(i % dim) + 1, sal=(i * 11) % 100000), dim)
    base, stream = stream_factory(
        lambda batch, pk, v, w: batch.append(id=pk, mgr=v[0], sal=v[1], weight=w),
        lambda rng: (zipf_choice(rng, dim, SKEW_S), rng.randint(0, 100000)),
    )
    _seed(client, sn, "emp", base, sz["base"])
    _run(client, bench_timer, sn, "emp", stream, "v_self", sz)


# ---------------------------------------------------------------------------
# Band join: ba(pk, k, lo) ⋈ bb(pk, k, t) ON ba.k = bb.k AND ba.lo <= bb.t
# ---------------------------------------------------------------------------

def test_band(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, feature_sz(scale_mode)
    dim = sz["dim"]
    client.execute_sql("CREATE TABLE ba (pk BIGINT NOT NULL PRIMARY KEY, "
                       "k BIGINT NOT NULL, lo BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE bb (pk BIGINT NOT NULL PRIMARY KEY, "
                       "k BIGINT NOT NULL, t BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE VIEW v_band AS SELECT ba.k AS k, ba.lo AS lo, bb.t AS t "
                       "FROM ba JOIN bb ON ba.k = bb.k AND ba.lo <= bb.t", schema_name=sn)
    _seed(client, sn, "bb", lambda b, i: b.append(pk=i + 1, k=i + 1, t=500), dim)
    base, stream = stream_factory(
        lambda batch, pk, v, w: batch.append(pk=pk, k=v[0], lo=v[1], weight=w),
        lambda rng: (zipf_choice(rng, dim, SKEW_S), rng.randint(0, 1000)),
    )
    _seed(client, sn, "ba", base, sz["base"])
    _run(client, bench_timer, sn, "ba", stream, "v_band", sz)


# ---------------------------------------------------------------------------
# Pure-range joins: ra(pk, x) ⋈ rb(pk, y) ON ra.x < rb.y  (small dim bounds fan-out)
# ---------------------------------------------------------------------------

def _make_range(client, sn):
    client.execute_sql("CREATE TABLE ra (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE TABLE rb (pk BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)",
                       schema_name=sn)
    _seed(client, sn, "rb", lambda b, i: b.append(pk=i + 1, y=i + 1), RANGE_DIM)


def _range_factory():
    return stream_factory(
        lambda batch, pk, v, w: batch.append(pk=pk, x=v[0], weight=w),
        lambda rng: (rng.randint(0, RANGE_DIM),),
    )


def _range_sz(scale_mode):
    sz = dict(feature_sz(scale_mode))
    sz["base"] = min(sz["base"], RANGE_BASE_CAP)
    sz["delta"] = min(sz["delta"], RANGE_DELTA_CAP)
    return sz


def test_range_inner(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, _range_sz(scale_mode)
    _make_range(client, sn)
    client.execute_sql("CREATE VIEW v_ri AS SELECT ra.x AS x, rb.y AS y "
                       "FROM ra JOIN rb ON ra.x < rb.y", schema_name=sn)
    base, stream = _range_factory()
    _seed(client, sn, "ra", base, sz["base"])
    _run(client, bench_timer, sn, "ra", stream, "v_ri", sz)


def test_range_left(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, _range_sz(scale_mode)
    _make_range(client, sn)
    client.execute_sql("CREATE VIEW v_rl AS SELECT ra.x AS x, rb.y AS y "
                       "FROM ra LEFT JOIN rb ON ra.x < rb.y", schema_name=sn)
    base, stream = _range_factory()
    _seed(client, sn, "ra", base, sz["base"])
    _run(client, bench_timer, sn, "ra", stream, "v_rl", sz)


# ---------------------------------------------------------------------------
# Compound-key equi join: ca(pk, x, y, av) ⋈ cb(pk, x, y, bv) ON x AND y
# ---------------------------------------------------------------------------

def test_compound_key(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, feature_sz(scale_mode)
    dim = sz["dim"]
    half = max(1, dim // 2)
    client.execute_sql("CREATE TABLE ca (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, "
                       "y BIGINT NOT NULL, av BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE cb (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, "
                       "y BIGINT NOT NULL, bv BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE VIEW v_ckey AS SELECT ca.av AS av, cb.bv AS bv "
                       "FROM ca JOIN cb ON ca.x = cb.x AND ca.y = cb.y", schema_name=sn)
    # dim (x, y) grid: y in {0,1}, x in [1, dim/2] → distinct (x, y) pairs.
    _seed(client, sn, "cb",
          lambda b, i: b.append(pk=i + 1, x=(i % half) + 1, y=i // half, bv=(i * 5) % 1000), dim)
    base, stream = stream_factory(
        lambda batch, pk, v, w: batch.append(pk=pk, x=v[0], y=v[1], av=v[2], weight=w),
        lambda rng: (zipf_choice(rng, half, SKEW_S), rng.randint(0, 1), rng.randint(0, 1000)),
    )
    _seed(client, sn, "ca", base, sz["base"])
    _run(client, bench_timer, sn, "ca", stream, "v_ckey", sz)
