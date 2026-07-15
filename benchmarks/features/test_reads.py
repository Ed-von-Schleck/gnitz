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

def test_view_scan(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, feature_sz(scale_mode)
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                       "g BIGINT NOT NULL, v BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT g, SUM(v) AS s FROM t GROUP BY g",
                       schema_name=sn)
    tid, schema = client.resolve_table(sn, "t")
    push_stream(client, tid, schema,
                lambda b, k: b.append(pk=k + 1, g=k % NGROUP, v=(k * 7) % 1000), sz["base"])
    vid, _ = client.resolve_table(sn, "v")
    for _ in range(_reads(scale_mode)):
        res = bench_timer.measure(client.scan, vid, rows_per_call=NGROUP)  # noqa: F841
    assert len(client.scan(vid)) > 0


# ---------------------------------------------------------------------------
# view_seek_natural — seek a U64-keyed grouped view by natural key (R3)
# ---------------------------------------------------------------------------

def test_view_seek_natural(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, feature_sz(scale_mode)
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                       "cust BIGINT UNSIGNED NOT NULL, amt BIGINT NOT NULL)", schema_name=sn)
    # Group column `cust` is single non-nullable U64 → natural PK, seek-addressable.
    client.execute_sql("CREATE VIEW v AS SELECT cust, SUM(amt) AS s FROM t GROUP BY cust",
                       schema_name=sn)
    tid, schema = client.resolve_table(sn, "t")
    push_stream(client, tid, schema,
                lambda b, k: b.append(pk=k + 1, cust=(k % NGROUP) + 1, amt=(k * 3) % 1000),
                sz["base"])
    vid, _ = client.resolve_table(sn, "v")
    next_pk = sz["base"] + 1
    hot = 1
    for _ in range(_reads(scale_mode)):
        push_one(client, tid, schema, pk=next_pk, cust=hot, amt=1)  # RYOW write
        next_pk += 1
        bench_timer.measure(client.seek, vid, hot, rows_per_call=1)
    assert len(client.seek(vid, hot)) == 1


# ---------------------------------------------------------------------------
# view_passthrough_seek — seek a filter/passthrough view by base PK
# ---------------------------------------------------------------------------

def test_view_passthrough_seek(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, feature_sz(scale_mode)
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t WHERE v >= 0", schema_name=sn)
    tid, schema = client.resolve_table(sn, "t")
    push_stream(client, tid, schema, lambda b, k: b.append(pk=k + 1, v=(k * 5) % 1000), sz["base"])
    vid, _ = client.resolve_table(sn, "v")
    next_pk = sz["base"] + 1
    for _ in range(_reads(scale_mode)):
        push_one(client, tid, schema, pk=next_pk, v=1)  # RYOW write
        pk = next_pk
        next_pk += 1
        bench_timer.measure(client.seek, vid, pk, rows_per_call=1)
    assert len(client.seek(vid, sz["base"])) == 1


# ---------------------------------------------------------------------------
# seek_by_index — binary secondary-index point seek on a base table
# ---------------------------------------------------------------------------

def test_seek_by_index(client, schema_name, bench_timer, scale_mode):
    sn, sz = schema_name, feature_sz(scale_mode)
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                       "val BIGINT NOT NULL, payload BIGINT NOT NULL)", schema_name=sn)
    tid, schema = client.resolve_table(sn, "t")
    push_stream(client, tid, schema,
                lambda b, k: b.append(pk=k + 1, val=k % NGROUP, payload=k), sz["base"])
    client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
    reads = _reads(scale_mode)
    for i in range(reads):
        key = i % NGROUP
        bench_timer.measure(client.seek_by_index, tid, [1], [key], rows_per_call=1)
    assert len(client.seek_by_index(tid, [1], [0])) > 0


# ---------------------------------------------------------------------------
# scan_many_2 / scan_many_8 — consistent multi-view snapshot latency
# ---------------------------------------------------------------------------

def _scan_many_n(client, sn, bench_timer, sz, reads, nviews):
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                       "g BIGINT NOT NULL, v BIGINT NOT NULL)", schema_name=sn)
    for i in range(nviews):
        client.execute_sql(
            f"CREATE VIEW v{i} AS SELECT g, SUM(v) AS s FROM t WHERE v >= {i} GROUP BY g",
            schema_name=sn)
    tid, schema = client.resolve_table(sn, "t")
    push_stream(client, tid, schema,
                lambda b, k: b.append(pk=k + 1, g=k % NGROUP, v=(k * 7) % 1000), sz["base"])
    vids = [client.resolve_table(sn, f"v{i}")[0] for i in range(nviews)]
    for _ in range(reads):
        bench_timer.measure(client.scan_many, vids, rows_per_call=NGROUP * nviews)
    assert all(len(r) > 0 for r in client.scan_many(vids))


def test_scan_many_2(client, schema_name, bench_timer, scale_mode):
    _scan_many_n(client, schema_name, bench_timer, feature_sz(scale_mode), _reads(scale_mode), 2)


def test_scan_many_8(client, schema_name, bench_timer, scale_mode):
    _scan_many_n(client, schema_name, bench_timer, feature_sz(scale_mode), _reads(scale_mode), 8)
