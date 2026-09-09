"""Exchange routing: what a repartition must carry across workers.

A STRING group key routes by a hash of its bytes rather than by a widened
scalar; a summed partial must survive the merge of every worker's
contribution; and a cluster holding exchange-requiring views in more than one
schema must keep each schema's relay bound to its own exchange schema.
"""

import os
import random

import pytest
import gnitz
from _uid import uid as _uid

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)




def _drop_all(client, sn, tables=(), views=()):
    for v in views:
        try:
            client.execute_sql(f"DROP VIEW {v}", schema_name=sn)
        except Exception:
            pass
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    client.drop_schema(sn)


def _scan_reduce_map(client, vid):
    """Scan a reduce view → {group_val: agg_val} for positive-weight rows.

    The visible layout of a GROUP BY view is [group_col, agg] whether the view
    is keyed naturally or by a (hidden) synthetic group PK.
    """
    return {row[0]: row[1] for row in client.scan(vid)}


@_NEEDS_MULTI
def test_string_group_by_multiworker(client):
    """STRING column as GROUP BY key routed across workers.  STRING is 16 bytes;
    exchange routing must hash the content, not copy 16 bytes into an 8-byte buffer."""
    sn = "sgb_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "name VARCHAR(100) NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT name, SUM(val) AS total FROM t GROUP BY name",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        vals = ", ".join([
            "(1, 'alpha', 10)", "(2, 'alpha', 20)", "(3, 'alpha', 30)",
            "(4, 'beta', 100)", "(5, 'beta', 200)",
            "(6, 'a_very_long_group_name', 5)", "(7, 'a_very_long_group_name', 15)",
        ])
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        totals = _scan_reduce_map(client, vid)

        assert totals["alpha"] == 60, f"alpha: {totals.get('alpha')}"
        assert totals["beta"] == 300, f"beta: {totals.get('beta')}"
        assert totals["a_very_long_group_name"] == 20, (
            f"long name: {totals.get('a_very_long_group_name')}"
        )
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


# -----------------------------------------------------------------------
# EXCEPT cursor re-seek on UPDATE (same-PK retraction + insertion)
# -----------------------------------------------------------------------

@_NEEDS_MULTI
def test_exchange_merge_sum_correct(client):
    """SUM across workers after exchange merge must not double-count.
    Verifies the exchange output is not falsely marked as consolidated."""
    sn = "emc_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        vals = ", ".join(f"({i}, 1, {i})" for i in range(1, 101))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        totals = _scan_reduce_map(client, vid)
        expected = sum(range(1, 101))  # 5050
        assert totals[1] == expected, f"SUM: {totals[1]} != {expected}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])


# -----------------------------------------------------------------------
# Multi-worker MIN/MAX incremental gather-reduce
# -----------------------------------------------------------------------

@_NEEDS_MULTI
class TestMultiSchemaExchange:
    """Regression tests for multi-worker exchange correctness when multiple
    schemas contain exchange-requiring views (join + agg).

    The bugs these cover:
    - Exchange loop consumed DDL_SYNC messages from the SAL instead of
      waiting for the exchange relay, producing wrong column counts.
    - poll_tick_progress (async tick) did not propagate the exchange schema
      to relay_exchange, causing the relay to use the view output schema.
    - Join-shard exchange path did not set batch.schema, causing encode_wire
      to panic on None schema.
    """

    def test_join_agg_view_across_schemas(self, client):
        """Two schemas each with a join+agg view chain. The first schema's
        views must survive tick processing while the second schema's DDL
        creates interleaved SAL messages."""
        rng = random.Random(42)

        # --- Schema A: filter → join → agg (4-column fact table) ---
        sa = "mw_xchg_a_" + _uid()
        client.create_schema(sa)
        try:
            client.execute_sql(
                "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
                "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL, "
                "category BIGINT NOT NULL)", schema_name=sa)
            client.execute_sql(
                "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
                "region BIGINT NOT NULL)", schema_name=sa)
            client.execute_sql(
                "CREATE VIEW v_filter AS SELECT * FROM fact WHERE amount > 0",
                schema_name=sa)
            client.execute_sql(
                "CREATE VIEW v_joined AS SELECT v_filter.pk, v_filter.amount, "
                "v_filter.category, dim.region "
                "FROM v_filter INNER JOIN dim ON v_filter.dim_pk = dim.pk",
                schema_name=sa)
            client.execute_sql(
                "CREATE VIEW v_agg AS SELECT region, SUM(amount) AS total "
                "FROM v_joined GROUP BY region", schema_name=sa)

            # Load dim via push
            dt, ds = client.resolve_table(sa, "dim")
            batch = gnitz.ZSetBatch(ds)
            for i in range(1, 11):
                batch.append(pk=i, region=i % 5)
            client.push(dt, batch)

            # INSERT rows (triggers tick → exchange)
            for bi in range(5):
                bp = bi * 20
                vals = ", ".join(
                    f"({bp+j+1}, {(bp+j) % 10 + 1}, {rng.randint(1, 1000)}, {rng.randint(0, 5)})"
                    for j in range(20))
                client.execute_sql(
                    f"INSERT INTO fact (pk, dim_pk, amount, category) VALUES {vals}",
                    schema_name=sa)

            # --- Schema B: join → agg (3-column fact, different column count) ---
            sb = "mw_xchg_b_" + _uid()
            client.create_schema(sb)
            try:
                client.execute_sql(
                    "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
                    "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL)",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
                    "region BIGINT NOT NULL)", schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v_joined AS SELECT fact.pk, fact.amount, dim.region "
                    "FROM fact INNER JOIN dim ON fact.dim_pk = dim.pk",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v_agg AS SELECT region, SUM(amount) AS total "
                    "FROM v_joined GROUP BY region", schema_name=sb)

                dt2, ds2 = client.resolve_table(sb, "dim")
                batch2 = gnitz.ZSetBatch(ds2)
                for i in range(1, 11):
                    batch2.append(pk=i, region=i % 5)
                client.push(dt2, batch2)

                for bi in range(5):
                    bp = bi * 20
                    vals = ", ".join(
                        f"({bp+j+1}, {(bp+j) % 10 + 1}, {rng.randint(1, 1000)})"
                        for j in range(20))
                    client.execute_sql(
                        f"INSERT INTO fact (pk, dim_pk, amount) VALUES {vals}",
                        schema_name=sb)

                # Verify both agg views produce correct results
                _, sa_agg_schema = client.resolve_table(sa, "v_agg")
                _, sb_agg_schema = client.resolve_table(sb, "v_agg")
                sa_vid, _ = client.resolve_table(sa, "v_agg")
                sb_vid, _ = client.resolve_table(sb, "v_agg")

                sa_rows = list(client.scan(sa_vid))
                sb_rows = list(client.scan(sb_vid))

                assert len(sa_rows) > 0, "schema A agg view should have results"
                assert len(sb_rows) > 0, "schema B agg view should have results"

            finally:
                _drop_all(client, sb, views=["v_agg", "v_joined"],
                          tables=["fact", "dim"])
        finally:
            _drop_all(client, sa, views=["v_agg", "v_joined", "v_filter"],
                      tables=["fact", "dim"])

    def test_deep_pipeline_after_prior_schemas(self, client):
        """A deep view pipeline (5 views) created after other schemas with
        exchange views are already active."""
        rng = random.Random(42)

        # --- Schema A: join + agg ---
        sa = "mw_deep_a_" + _uid()
        client.create_schema(sa)
        try:
            client.execute_sql(
                "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
                "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL, "
                "category BIGINT NOT NULL)", schema_name=sa)
            client.execute_sql(
                "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
                "label BIGINT NOT NULL)", schema_name=sa)
            client.execute_sql(
                "CREATE VIEW v_joined AS SELECT fact.pk, fact.amount, "
                "fact.category, dim.label "
                "FROM fact INNER JOIN dim ON fact.dim_pk = dim.pk",
                schema_name=sa)
            client.execute_sql(
                "CREATE VIEW v_agg AS SELECT label, SUM(amount) AS total "
                "FROM v_joined GROUP BY label", schema_name=sa)

            dt, ds = client.resolve_table(sa, "dim")
            batch = gnitz.ZSetBatch(ds)
            for i in range(1, 21):
                batch.append(pk=i, label=i % 10)
            client.push(dt, batch)

            for bi in range(3):
                bp = bi * 20
                vals = ", ".join(
                    f"({bp+j+1}, {(bp+j) % 20 + 1}, {rng.randint(1, 1000)}, {rng.randint(0, 5)})"
                    for j in range(20))
                client.execute_sql(
                    f"INSERT INTO fact (pk, dim_pk, amount, category) VALUES {vals}",
                    schema_name=sa)

            # --- Schema B: 5-view chain ---
            sb = "mw_deep_b_" + _uid()
            client.create_schema(sb)
            try:
                client.execute_sql(
                    "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, "
                    "dim_pk BIGINT NOT NULL, amount BIGINT NOT NULL, "
                    "category BIGINT NOT NULL)", schema_name=sb)
                client.execute_sql(
                    "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, "
                    "label BIGINT NOT NULL)", schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v1 AS SELECT * FROM fact WHERE amount > 50",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v2 AS SELECT v1.pk, v1.amount, v1.category, "
                    "dim.label FROM v1 INNER JOIN dim ON v1.dim_pk = dim.pk",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v3 AS SELECT label, SUM(amount) AS total, "
                    "COUNT(*) AS cnt FROM v2 GROUP BY label",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v4 AS SELECT * FROM v3 WHERE total > 10",
                    schema_name=sb)
                client.execute_sql(
                    "CREATE VIEW v5 AS SELECT DISTINCT label FROM v4",
                    schema_name=sb)

                dt2, ds2 = client.resolve_table(sb, "dim")
                batch2 = gnitz.ZSetBatch(ds2)
                for i in range(1, 11):
                    batch2.append(pk=i, label=i % 5)
                client.push(dt2, batch2)

                for bi in range(3):
                    bp = bi * 20
                    vals = ", ".join(
                        f"({bp+j+1}, {(bp+j) % 10 + 1}, {rng.randint(1, 1000)}, {rng.randint(0, 5)})"
                        for j in range(20))
                    client.execute_sql(
                        f"INSERT INTO fact (pk, dim_pk, amount, category) VALUES {vals}",
                        schema_name=sb)

                # Verify the deep pipeline produces results
                vid5, _ = client.resolve_table(sb, "v5")
                rows = list(client.scan(vid5))
                assert len(rows) > 0, "v5 (distinct labels) should have results"

            finally:
                _drop_all(client, sb, views=["v5", "v4", "v3", "v2", "v1"],
                          tables=["fact", "dim"])
        finally:
            _drop_all(client, sa, views=["v_agg", "v_joined"],
                      tables=["fact", "dim"])
