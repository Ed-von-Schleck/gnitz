"""Incremental-maintenance cost per subquery shape (push-driven).

Parent/child schema: p(id, region), ch(id, pid, v). One view per subquery form,
all R2-aliased where the subquery reuses the child relation. Deltas stream into
the child (driving the correlated / mark-join recomputation). Child FKs are
Zipfian (hot parents). SQL is verbatim from the plan's Appendix A.
"""

from __future__ import annotations

from helpers.datagen import (FEATURE_SIZES, push_stream, stream_and_assert,
                             stream_factory, zipf_choice)

SKEW_S = 1.1


def _sz(scale_mode):
    return FEATURE_SIZES[scale_mode]


def _p_seed(batch, k):
    batch.append(id=k + 1, region=(k + 1) % 5, weight=1)


def _ch_factory(dim):
    return stream_factory(
        lambda batch, pk, v, w: batch.append(id=pk, pid=v[0], v=pk % 1000, weight=w),
        lambda rng: (zipf_choice(rng, dim, SKEW_S),),
    )


def _run(client, sn, bench_timer, view_ddls, read_view, sz):
    client.execute_sql("CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE TABLE ch (id BIGINT NOT NULL PRIMARY KEY, "
                       "pid BIGINT NOT NULL, v BIGINT NOT NULL)", schema_name=sn)
    for ddl in view_ddls:
        client.execute_sql(ddl, schema_name=sn)
    p_tid, p_schema = client.resolve_table(sn, "p")
    push_stream(client, p_tid, p_schema, _p_seed, sz["dim"])
    ch_tid, ch_schema = client.resolve_table(sn, "ch")
    ch_base, ch_stream = _ch_factory(sz["dim"])
    push_stream(client, ch_tid, ch_schema, ch_base, sz["base"])
    stream_and_assert(client, sn, bench_timer, ch_tid, ch_schema, ch_stream, sz, read_view)


def test_scalar_proj(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT p.id AS id, "
        "(SELECT COUNT(*) FROM ch WHERE ch.pid = p.id) AS cnt FROM p",
    ], "v", _sz(scale_mode))


def test_scalar_where_corr(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT p.id AS id FROM p "
        "WHERE (SELECT COUNT(*) FROM ch WHERE ch.pid = p.id) >= 2",
    ], "v", _sz(scale_mode))


def test_scalar_where_uncorr(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT o.id AS id FROM ch o WHERE o.v < (SELECT MAX(ci.v) FROM ch ci)",
    ], "v", _sz(scale_mode))


def test_exists(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT p.id AS id, p.region AS region FROM p "
        "WHERE EXISTS (SELECT 1 FROM ch WHERE ch.pid = p.id)",
    ], "v", _sz(scale_mode))


def test_exists_group(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW vx AS SELECT p.id AS id, p.region AS region FROM p "
        "WHERE EXISTS (SELECT 1 FROM ch WHERE ch.pid = p.id)",
        "CREATE VIEW vg AS SELECT region, COUNT(*) AS cnt FROM vx GROUP BY region",
    ], "vg", _sz(scale_mode))


def test_in_sub(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT p.id AS id FROM p WHERE p.id IN (SELECT ch.pid FROM ch)",
    ], "v", _sz(scale_mode))


def test_any(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT o.id AS id FROM ch o WHERE o.v < ANY (SELECT ci.v FROM ch ci)",
    ], "v", _sz(scale_mode))


def test_derived(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS SELECT d.id AS id, ch.v AS v FROM "
        "(SELECT id, region FROM p WHERE region > 0) d JOIN ch ON d.id = ch.pid",
    ], "v", _sz(scale_mode))


def test_cte(client, schema_name, bench_timer, scale_mode):
    _run(client, schema_name, bench_timer, [
        "CREATE VIEW v AS WITH big AS (SELECT id, region FROM p WHERE region > 0) "
        "SELECT big.id AS id, ch.v AS v FROM big JOIN ch ON big.id = ch.pid",
    ], "v", _sz(scale_mode))
