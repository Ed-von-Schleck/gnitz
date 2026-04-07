"""Combined benchmark: chained joins via view-on-view (A join B join C)."""

import random
import pytest

import gnitz
from helpers.datagen import DataGen


def test_chained_join(client, schema_name, bench_timer, scale):
    """A join B via v1, then v1 join C via v2."""
    client.execute_sql(
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, "
        "a_pk BIGINT NOT NULL, label BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    client.execute_sql(
        "CREATE TABLE c (pk BIGINT NOT NULL PRIMARY KEY, "
        "b_pk BIGINT NOT NULL, score BIGINT NOT NULL)",
        schema_name=schema_name,
    )
    # v1: A join B
    client.execute_sql(
        "CREATE VIEW v1 AS SELECT b.pk, a.val, b.label "
        "FROM b INNER JOIN a ON b.a_pk = a.pk",
        schema_name=schema_name,
    )
    # v2: v1 join C
    client.execute_sql(
        "CREATE VIEW v2 AS SELECT c.pk, v1.val, v1.label, c.score "
        "FROM c INNER JOIN v1 ON c.b_pk = v1.pk",
        schema_name=schema_name,
    )

    # Bulk load A and B via push
    rng = random.Random(42)
    a_tid, a_schema = client.resolve_table(schema_name, "a")
    a_batch = gnitz.ZSetBatch(a_schema)
    for i in range(1, 101):
        a_batch.append(pk=i, val=rng.randint(0, 1000))
    client.push(a_tid, a_batch)

    b_tid, b_schema = client.resolve_table(schema_name, "b")
    b_batch = gnitz.ZSetBatch(b_schema)
    for i in range(1, 501):
        b_batch.append(pk=i, a_pk=rng.randint(1, 100), label=rng.randint(0, 50))
    client.push(b_tid, b_batch)

    # Measured: INSERT into C triggers v1 join + v2 join
    gen = DataGen()
    fk_range = {"b_pk": (1, 500)}
    for i in range(scale["write_iters"]):
        sql = gen.insert_sql("c", ["pk", "b_pk", "score"], 100, i,
                             col_ranges=fk_range)
        bench_timer.measure(
            client.execute_sql, sql, schema_name,
            rows_per_call=100,
        )
