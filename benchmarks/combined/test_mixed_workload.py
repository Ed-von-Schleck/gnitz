"""Combined benchmark: concurrent writers + view readers (multi-client)."""

import random
import pytest

import gnitz
from helpers.datagen import DataGen, SCALES
from helpers.timing import run_parallel


def _writer_fn(conn, schema_name, scale_mode, writer_id):
    """Writer process: INSERT batches into fact table."""
    scale = SCALES[scale_mode]
    gen = DataGen(seed=42 + writer_id)
    for i in range(scale["write_iters"]):
        pk_base = writer_id * 1_000_000 + i * 100
        sql = gen.insert_sql("fact", ["pk", "dim_pk", "amount"], 100, pk_base)
        conn.execute_sql(sql, schema_name=schema_name)


def _reader_fn(conn, schema_name, scale_mode, view_name):
    """Reader process: scan view repeatedly."""
    scale = SCALES[scale_mode]
    for _ in range(scale["read_iters"]):
        conn.execute_sql(f"SELECT * FROM {view_name}", schema_name=schema_name)


def test_mixed_workload(client, socket_path, schema_name, bench_timer,
                        scale, scale_mode, num_clients):
    """Concurrent writers INSERT into a table while readers scan a view."""
    if num_clients < 2:
        pytest.skip("mixed workload requires --clients >= 2")

    client.execute_sql(
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
        "label BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
        "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE VIEW v AS SELECT fact.pk, fact.amount, dim.label "
        "FROM fact INNER JOIN dim ON fact.dim_pk = dim.pk",
        schema_name=schema_name,
    )

    # Bulk load dim
    rng = random.Random(42)
    dim_tid, dim_schema = client.resolve_table(schema_name, "dim")
    batch = gnitz.ZSetBatch(dim_schema)
    for i in range(1, 101):
        batch.append(pk=i, label=rng.randint(0, 50))
    client.push(dim_tid, batch)

    # Split clients: half writers, half readers
    n_writers = max(1, num_clients // 2)
    n_readers = num_clients - n_writers

    import multiprocessing
    barrier = multiprocessing.Barrier(n_writers + n_readers)
    queue = multiprocessing.Queue()

    import time

    def _run_writer(sock, sn, sm, wid, bar, q):
        with gnitz.connect(sock) as c:
            bar.wait()
            t0 = time.perf_counter()
            _writer_fn(c, sn, sm, wid)
            q.put(("writer", time.perf_counter() - t0))

    def _run_reader(sock, sn, sm, vn, bar, q):
        with gnitz.connect(sock) as c:
            bar.wait()
            t0 = time.perf_counter()
            _reader_fn(c, sn, sm, vn)
            q.put(("reader", time.perf_counter() - t0))

    procs = []
    for wid in range(n_writers):
        p = multiprocessing.Process(
            target=_run_writer,
            args=(socket_path, schema_name, scale_mode, wid, barrier, queue),
        )
        p.start()
        procs.append(p)
    for _ in range(n_readers):
        p = multiprocessing.Process(
            target=_run_reader,
            args=(socket_path, schema_name, scale_mode, "v", barrier, queue),
        )
        p.start()
        procs.append(p)

    for p in procs:
        p.join(timeout=120)

    latencies = []
    while not queue.empty():
        role, dt = queue.get_nowait()
        latencies.append(dt * 1000.0)

    # Record aggregated result
    bench_timer._latencies.extend(latencies)
    bench_timer._rows = n_writers * scale["write_iters"] * 100
    bench_timer._iterations = len(latencies) + bench_timer._warmup + 1
