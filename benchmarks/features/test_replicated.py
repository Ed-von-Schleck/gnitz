"""Cost of a REPLICATED table as it grows (push-driven).

A replicated table keeps a full copy on every worker. That buys local joins
against a dimension — a partitioned fact joins it with no exchange on either
side — and it costs, on every axis, a factor of the worker count:

  * writes broadcast, so one pushed row lands in all W workers' ingest + SAL;
  * a scan is served by a single worker, so read throughput does not grow with W;
  * a view backfill re-reads the whole copy on every worker.

Replication targets small dimensions and there is deliberately no size cap — the
cost of a large replicated table is the user's to bound. These sweeps are what
makes that growth curve visible rather than asserted: each test runs at three
dimension sizes, and the `partitioned` arm is the control that isolates what
replication itself costs.

Run the placement comparison across worker counts — the W factor is invisible at
`--workers 1`:

    uv run pytest benchmarks/features/test_replicated.py --workers 4
"""

from __future__ import annotations

import time

import gnitz
import pytest

from helpers.datagen import feature_sz, push_stream, seed_stream

# Dimension sizes swept per scale, decades apart: the point is the shape of the
# curve across a wide span, not any single number.
SIZE_TIERS = ["small", "medium", "large"]
DIM_SWEEP = {
    "quick": {"small": 1_000, "medium": 10_000, "large": 100_000},
    "full": {"small": 10_000, "medium": 100_000, "large": 1_000_000},
}

PLACEMENTS = ["replicated", "partitioned"]

_dim_seed, _dim_stream = seed_stream(
    lambda batch, pk, w: batch.append(pk=pk, val=pk % 1000, _weight=w))


def _create_dim(client, sn, placement):
    opt = " WITH (replicated = true)" if placement == "replicated" else ""
    client.execute_sql(
        f"CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL){opt}",
        schema_name=sn)
    return client.resolve_table(sn, "dim")


def _create_fact(client, sn, rows):
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, fk BIGINT NOT NULL, "
        "val BIGINT NOT NULL)", schema_name=sn)
    tid, schema = client.resolve_table(sn, "fact")
    push_stream(client, tid, schema,
                lambda b, k: b.append(pk=k + 1, fk=(k % 1000) + 1, val=k), rows)
    return tid, schema


def _stream_into_dim(client, bench_timer, tid, schema, sz):
    """Stream `iters` delta epochs of `delta` rows into the dimension, measured."""
    for epoch in range(sz["iters"]):
        batch = gnitz.ZSetBatch(schema)
        for j in range(sz["delta"]):
            _dim_stream(batch, epoch, j, 1)
        bench_timer.measure(client.push, tid, batch, rows_per_call=sz["delta"])


@pytest.mark.parametrize("placement", PLACEMENTS)
def test_dim_write_cost(client, schema_name, bench_timer, scale_mode, placement):
    """Per-row write cost into the dimension at each placement.

    A replicated write broadcasts the whole batch into every worker's ingest and
    SAL; a partitioned write scatters it so each worker takes only its own slice.
    The ratio between the arms is replication's write amplification.
    """
    sz = feature_sz(scale_mode)
    tid, schema = _create_dim(client, schema_name, placement)
    push_stream(client, tid, schema, _dim_seed, sz["dim"])
    _stream_into_dim(client, bench_timer, tid, schema, sz)


@pytest.mark.parametrize("placement", PLACEMENTS)
@pytest.mark.parametrize("tier", SIZE_TIERS)
def test_dim_scan_cost(client, schema_name, bench_timer, scale_mode, placement, tier):
    """Full-scan cost of the dimension as it grows.

    A replicated scan is single-sourced from one worker, so it cannot spread over
    W the way a partitioned scan does. The gap between the arms is what "a
    bottleneck if the replicated table is large or read-hot" means numerically.
    """
    dim_rows = DIM_SWEEP[scale_mode][tier]
    tid, schema = _create_dim(client, schema_name, placement)
    push_stream(client, tid, schema, _dim_seed, dim_rows)

    for _ in range(5):
        bench_timer.measure(lambda: sum(1 for _ in client.scan(tid)),
                            rows_per_call=dim_rows)


@pytest.mark.parametrize("placement", PLACEMENTS)
@pytest.mark.parametrize("tier", SIZE_TIERS)
def test_mixed_view_backfill(client, schema_name, bench_timer, scale_mode,
                             placement, tier):
    """CREATE VIEW backfill over a UNION ALL of the dimension and a fact.

    Backfill drains the dimension on every worker. For a replicated dim each
    worker holds — and therefore drains — the whole table, so the work grows with
    W; for a partitioned dim each drains only its own slice.
    """
    dim_rows = DIM_SWEEP[scale_mode][tier]
    sn = schema_name
    tid, schema = _create_dim(client, sn, placement)
    push_stream(client, tid, schema, _dim_seed, dim_rows)
    _create_fact(client, sn, dim_rows)

    t0 = time.perf_counter()
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk, val FROM dim UNION ALL SELECT pk, val FROM fact",
        schema_name=sn)
    vid, _ = client.resolve_table(sn, "v")
    rows = sum(1 for _ in client.scan(vid))
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    # A view backfills exactly once, and `measure` discards its warmup
    # iterations — timing this through it would record nothing at all. Feed the
    # single sample straight in.
    bench_timer.add_latencies([elapsed_ms], rows=rows)
    assert rows == dim_rows * 2, f"backfilled {rows}, expected {dim_rows * 2}"


@pytest.mark.parametrize("placement", PLACEMENTS)
def test_setop_view_maintenance(client, schema_name, bench_timer, scale_mode, placement):
    """Incremental maintenance of a UNION ALL view while the dimension is written.

    A set-op does not elide its exchange the way an equi join against a
    replicated dim does, so this is the shape where a write-hot replicated table
    drives exchange traffic: the relay carries one payload per worker per round.
    """
    sn = schema_name
    sz = feature_sz(scale_mode)
    tid, schema = _create_dim(client, sn, placement)
    push_stream(client, tid, schema, _dim_seed, sz["dim"])
    _create_fact(client, sn, sz["dim"])
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk, val FROM dim UNION ALL SELECT pk, val FROM fact",
        schema_name=sn)

    _stream_into_dim(client, bench_timer, tid, schema, sz)

    vid, _ = client.resolve_table(sn, "v")
    assert sum(1 for _ in client.scan(vid)) > 0, "view empty after streaming"
