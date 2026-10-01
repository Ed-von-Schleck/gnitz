"""Binary read-path latency against VIEWS (the actual serving surface).

Views (not base tables) are what the product serves: view scan, natural-key view
point-seek (R3), passthrough-view seek by base PK, secondary-index seek, and the
consistent multi-view scan_many snapshot. Latency-focused (p50/p90/p99, ops/s).
Each seek pushes one fresh row immediately before it, so the read reflects a
just-ACKed write (RYOW freshness) and its drain lands in the measured tail.
"""

from __future__ import annotations

from helpers.datagen import feature_sz, push_one, push_stream

NGROUP = 1000
READS = {"quick": 500, "full": 5000}


def _reads(scale_mode):
    return READS[scale_mode]


# ---------------------------------------------------------------------------
# view_scan — scan a grouped view of N groups
# ---------------------------------------------------------------------------

def test_view_scan(client, bench_timer, scale_mode):
    sz = feature_sz(scale_mode)
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                       "g BIGINT NOT NULL, v BIGINT NOT NULL)")
    client.execute_sql("CREATE VIEW v AS SELECT g, SUM(v) AS s FROM t GROUP BY g")
    tid, schema = client.resolve_table("t")
    push_stream(client, tid, schema,
                lambda b, k: b.append(pk=k + 1, g=k % NGROUP, v=(k * 7) % 1000), sz["base"])
    vid, v_sch = client.resolve_table("v")
    for _ in range(_reads(scale_mode)):
        bench_timer.measure(client.scan, vid, v_sch, rows_per_call=NGROUP)
    assert len(client.scan(vid, v_sch)) > 0


# ---------------------------------------------------------------------------
# view_seek_natural — seek a U64-keyed grouped view by natural key (R3)
# ---------------------------------------------------------------------------

def test_view_seek_natural(client, bench_timer, scale_mode):
    sz = feature_sz(scale_mode)
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                       "cust BIGINT UNSIGNED NOT NULL, amt BIGINT NOT NULL)")
    # Group column `cust` is single non-nullable U64 → natural PK, seek-addressable.
    client.execute_sql("CREATE VIEW v AS SELECT cust, SUM(amt) AS s FROM t GROUP BY cust")
    tid, schema = client.resolve_table("t")
    push_stream(client, tid, schema,
                lambda b, k: b.append(pk=k + 1, cust=(k % NGROUP) + 1, amt=(k * 3) % 1000),
                sz["base"])
    vid, v_sch = client.resolve_table("v")
    next_pk = sz["base"] + 1
    hot = 1
    for _ in range(_reads(scale_mode)):
        push_one(client, tid, schema, pk=next_pk, cust=hot, amt=1)  # RYOW write
        next_pk += 1
        bench_timer.measure(client.seek, vid, v_sch, hot, rows_per_call=1)
    assert len(client.seek(vid, v_sch, hot)) == 1


# ---------------------------------------------------------------------------
# view_passthrough_seek — seek a filter/passthrough view by base PK
# ---------------------------------------------------------------------------

def test_view_passthrough_seek(client, bench_timer, scale_mode):
    sz = feature_sz(scale_mode)
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t WHERE v >= 0")
    tid, schema = client.resolve_table("t")
    push_stream(client, tid, schema, lambda b, k: b.append(pk=k + 1, v=(k * 5) % 1000), sz["base"])
    vid, v_sch = client.resolve_table("v")
    next_pk = sz["base"] + 1
    for _ in range(_reads(scale_mode)):
        push_one(client, tid, schema, pk=next_pk, v=1)  # RYOW write
        pk = next_pk
        next_pk += 1
        bench_timer.measure(client.seek, vid, v_sch, pk, rows_per_call=1)
    assert len(client.seek(vid, v_sch, sz["base"])) == 1


# ---------------------------------------------------------------------------
# seek_by_index — binary secondary-index point seek on a base table
# ---------------------------------------------------------------------------

def test_seek_by_index(client, bench_timer, scale_mode):
    sz = feature_sz(scale_mode)
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                       "val BIGINT NOT NULL, payload BIGINT NOT NULL)")
    tid, schema = client.resolve_table("t")
    push_stream(client, tid, schema,
                lambda b, k: b.append(pk=k + 1, val=k % NGROUP, payload=k), sz["base"])
    client.execute_sql("CREATE INDEX ON t(val)")
    reads = _reads(scale_mode)
    for i in range(reads):
        key = i % NGROUP
        bench_timer.measure_rows(client.seek_by_index, tid, schema, [1], [key], rows_fn=len)
    assert len(client.seek_by_index(tid, schema, [1], [0])) > 0


# ---------------------------------------------------------------------------
# groupby_indexed_filter — ad-hoc GROUP BY whose WHERE hits a secondary index
# ---------------------------------------------------------------------------

# Coprime to NGROUP, so `ind` and `g` are independent by CRT: `WHERE ind = c`
# selects base/NIND rows spread across all groups.
NIND = 1001

# One ad-hoc GROUP BY per iteration compiles a transient and backfills it
# through the source drive. Sized like the streaming iteration counts rather
# than `_reads`: an unbounded backfill scans the whole base per iteration, so
# `_reads`' 5000 would scan 5e9 rows at full scale.
GROUPBY_ITERS = {"quick": 20, "full": 50}


def test_groupby_indexed_filter(client, bench_timer, scale_mode):
    sz = feature_sz(scale_mode)
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
                       "v BIGINT NOT NULL, ind BIGINT NOT NULL)")
    tid, schema = client.resolve_table("t")
    push_stream(client, tid, schema,
                lambda b, k: b.append(pk=k + 1, g=k % NGROUP, v=(k * 7) % 1000, ind=k % NIND),
                sz["base"])
    client.execute_sql("CREATE INDEX ON t(ind)")
    # 1/1001 selectivity at every scale, an order of magnitude inside the
    # source drive's index-vs-full-scan gate.
    for i in range(GROUPBY_ITERS[scale_mode]):
        bench_timer.measure(client.execute_sql,
                            f"SELECT g, SUM(v) AS s FROM t WHERE ind = {i % NIND} GROUP BY g", rows_per_call=max(1, sz["base"] // NIND))


# ---------------------------------------------------------------------------
# scan_many_2 / scan_many_8 — consistent multi-view snapshot latency
# ---------------------------------------------------------------------------

def _scan_many_n(client, bench_timer, sz, reads, nviews):
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                       "g BIGINT NOT NULL, v BIGINT NOT NULL)")
    for i in range(nviews):
        client.execute_sql(
            f"CREATE VIEW v{i} AS SELECT g, SUM(v) AS s FROM t WHERE v >= {i} GROUP BY g")
    tid, schema = client.resolve_table("t")
    push_stream(client, tid, schema,
                lambda b, k: b.append(pk=k + 1, g=k % NGROUP, v=(k * 7) % 1000), sz["base"])
    views = [client.resolve_table(f"v{i}") for i in range(nviews)]
    for _ in range(reads):
        bench_timer.measure(client.scan_many, views, rows_per_call=NGROUP * nviews)
    assert all(len(r) > 0 for r in client.scan_many(views))


def test_scan_many_2(client, bench_timer, scale_mode):
    _scan_many_n(client, bench_timer, feature_sz(scale_mode), _reads(scale_mode), 2)


def test_scan_many_8(client, bench_timer, scale_mode):
    _scan_many_n(client, bench_timer, feature_sz(scale_mode), _reads(scale_mode), 8)
