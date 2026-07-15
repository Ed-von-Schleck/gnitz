"""SQL client-path latency for ON CONFLICT upserts and RETURNING inserts.

These are parse-plan-bound single statements — a genuine SQL-client cost,
measured as point-op latency (not view-maintenance throughput).
"""


def test_on_conflict_upsert(client, schema_name, bench_timer, scale):
    """INSERT ... ON CONFLICT (pk) DO UPDATE SET qty = EXCLUDED.qty. Half the
    keys already exist (upsert), half are fresh (insert) — both parse paths."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE inv (pk BIGINT NOT NULL PRIMARY KEY, qty BIGINT NOT NULL)",
        schema_name=sn,
    )
    n = scale["write_iters"] * 10
    # Seed the lower half so alternating keys collide.
    seed = ",".join(f"({k},1)" for k in range(1, n // 2 + 1))
    if seed:
        client.execute_sql(f"INSERT INTO inv (pk, qty) VALUES {seed}", schema_name=sn)

    for i in range(scale["write_iters"]):
        pk = (i % n) + 1
        bench_timer.measure(
            client.execute_sql,
            f"INSERT INTO inv (pk, qty) VALUES ({pk}, {i + 2}) "
            "ON CONFLICT (pk) DO UPDATE SET qty = EXCLUDED.qty",
            sn,
            rows_per_call=1,
        )


def test_returning_id(client, schema_name, bench_timer, scale):
    """INSERT ... RETURNING id on a BIGSERIAL PK — server answers the generated
    key. Measures the round-trip parse+plan+return cost."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE srl (id BIGSERIAL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn,
    )
    for i in range(scale["write_iters"]):
        bench_timer.measure(
            client.execute_sql,
            f"INSERT INTO srl (v) VALUES ({i * 10}) RETURNING id",
            sn,
            rows_per_call=1,
        )


def test_update_arith(client, schema_name, bench_timer, scale):
    """UPDATE inv SET qty = qty - 3 WHERE pk = k — an RMW point update (reads
    then writes through the OCC precondition path)."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE inv (pk BIGINT NOT NULL PRIMARY KEY, qty BIGINT NOT NULL)",
        schema_name=sn,
    )
    n = scale["write_iters"] * 10
    seed = ",".join(f"({k},1000)" for k in range(1, n + 1))
    client.execute_sql(f"INSERT INTO inv (pk, qty) VALUES {seed}", schema_name=sn)
    for i in range(scale["write_iters"]):
        pk = (i % n) + 1
        bench_timer.measure(
            client.execute_sql,
            f"UPDATE inv SET qty = qty - 3 WHERE pk = {pk}",
            sn,
            rows_per_call=1,
        )
