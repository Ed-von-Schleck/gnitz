"""Bulk / large-scale DBSP operator and FLSM storage tests.

Run:
    cd rust_client/gnitz-py
    GNITZ_WORKERS=4 uv run pytest tests/test_bulk.py -v --tb=short
"""
import os
import random
import pytest
import gnitz

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


def _uid():
    return str(random.randint(100_000, 999_999))


def _make_table(client, sn, cols, unique_pk=True):
    name = "t_" + _uid()
    tid = client.create_table(sn, name, cols, unique_pk=unique_pk)
    return tid, gnitz.Schema(cols), name


def _push_batch(client, tid, schema, rows):
    """rows: iterable of dicts matching schema column names"""
    batch = gnitz.ZSetBatch(schema)
    for r in rows:
        batch.append(**r)
    client.push(tid, batch)


def _live_count(client, vid):
    return sum(1 for r in client.scan(vid) if r.weight > 0)


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


# ---------------------------------------------------------------------------
# Class 1: TestBulkFilter
# 100 000 rows through filter(val > 50 000); exercises op_filter bulk path
# and L0 flushes in ~50% of partitions.
# ---------------------------------------------------------------------------

class TestBulkFilter:

    def test_filter_100k(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tid, schema, tname = _make_table(client, sn, cols)
        vname = "v_" + _uid()
        try:
            eb = gnitz.ExprBuilder()
            r0 = eb.load_col_int(1)      # col 1 = val
            r1 = eb.load_const(50_000)
            cond = eb.cmp_gt(r0, r1)
            prog = eb.build(result_reg=cond)

            cb = client.circuit_builder(source_table_id=tid)
            inp = cb.input_delta()
            filt = cb.filter(inp, prog)
            cb.sink(filt)
            circuit = cb.build()

            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)

            N = 100_000
            _push_batch(client, tid, schema,
                        ({"pk": i, "val": i} for i in range(1, N + 1)))

            assert _live_count(client, vid) == 50_000
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Class 2: TestBulkReduce
# 100 000 rows, 100 groups — exercises UniversalAccumulator.combine and
# group hash distribution.
# ---------------------------------------------------------------------------

class TestBulkReduce:

    def test_reduce_100_groups(self, client):
        sn = "s" + _uid()
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
            tid, t_schema = client.resolve_table(sn, "t")
            vid, _ = client.resolve_table(sn, "v")

            N = 100_000
            _push_batch(client, tid, t_schema,
                        ({"pk": i, "grp": i % 100, "val": 1}
                         for i in range(1, N + 1)))

            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 100, f"expected 100 groups, got {len(rows)}"
            totals = {r[1]: r[2] for r in rows}
            for g in range(100):
                assert totals[g] == 1_000, \
                    f"group {g}: expected 1000, got {totals.get(g)}"
        finally:
            _drop_all(client, sn, views=["v"], tables=["t"])


# ---------------------------------------------------------------------------
# Class 3: TestBulkJoin
# 10 000 rows each side — exercises op_join_delta_trace with 10K-entry trace.
# ---------------------------------------------------------------------------

class TestBulkJoin:

    def test_join_10k(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, a_val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, b_val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.pk, a.a_val, b.b_val FROM a JOIN b ON a.pk = b.pk",
                schema_name=sn,
            )
            tid_a, schema_a = client.resolve_table(sn, "a")
            tid_b, schema_b = client.resolve_table(sn, "b")
            vid, _ = client.resolve_table(sn, "v")

            N = 10_000
            # push b first (populates trace), then a (delta → join_delta_trace)
            _push_batch(client, tid_b, schema_b,
                        ({"pk": i, "b_val": i * 3} for i in range(1, N + 1)))
            _push_batch(client, tid_a, schema_a,
                        ({"pk": i, "a_val": i * 2} for i in range(1, N + 1)))

            live_rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(live_rows) == N, \
                f"expected {N} join rows, got {len(live_rows)}"

            # spot-check: r[0]=join_pk(U128), r[1]=pk, r[2]=a_val, r[3]=b_val
            by_pk = {r[1]: r for r in live_rows}
            for pk in (1, 500, N):
                r = by_pk[pk]
                assert r[2] == pk * 2, f"pk={pk} a_val: expected {pk*2}, got {r[2]}"
                assert r[3] == pk * 3, f"pk={pk} b_val: expected {pk*3}, got {r[3]}"
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])


# ---------------------------------------------------------------------------
# Class 4: TestBulkAntiJoin
# 50 000 rows in a, 25 000 rows in b — exercises op_anti_join_delta_trace
# probe loop at scale.
# ---------------------------------------------------------------------------

class TestBulkAntiJoin:

    def test_anti_join_50k(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
                schema_name=sn,
            )
            tid_a, schema_a = client.resolve_table(sn, "a")
            tid_b, schema_b = client.resolve_table(sn, "b")
            vid, _ = client.resolve_table(sn, "v")

            # push b first so I(B) is populated when ΔA is processed
            _push_batch(client, tid_b, schema_b,
                        ({"pk": i, "val": i} for i in range(1, 25_001)))
            _push_batch(client, tid_a, schema_a,
                        ({"pk": i, "val": i} for i in range(1, 50_001)))

            # PKs 25_001..50_000 survive (not in b)
            assert _live_count(client, vid) == 25_000
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])


# ---------------------------------------------------------------------------
# Class 5: TestBulkSemiJoin
# Same two-table setup — exercises op_semi_join_delta_delta at scale.
# ---------------------------------------------------------------------------

class TestBulkSemiJoin:

    def test_semi_join_50k(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a INTERSECT SELECT * FROM b",
                schema_name=sn,
            )
            tid_a, schema_a = client.resolve_table(sn, "a")
            tid_b, schema_b = client.resolve_table(sn, "b")
            vid, _ = client.resolve_table(sn, "v")

            _push_batch(client, tid_a, schema_a,
                        ({"pk": i, "val": i} for i in range(1, 50_001)))
            _push_batch(client, tid_b, schema_b,
                        ({"pk": i, "val": i} for i in range(1, 25_001)))

            # PKs 1..25_000 are in both tables (exact row match)
            assert _live_count(client, vid) == 25_000
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])


# ---------------------------------------------------------------------------
# Class 6: TestBulkUnionAll
# 50 000 rows each, disjoint PK ranges — exercises op_union merge path and
# L0 flushes across many partitions.
# ---------------------------------------------------------------------------

class TestBulkUnionAll:

    def test_union_all_100k(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a UNION ALL SELECT * FROM b",
                schema_name=sn,
            )
            tid_a, schema_a = client.resolve_table(sn, "a")
            tid_b, schema_b = client.resolve_table(sn, "b")
            vid, _ = client.resolve_table(sn, "v")

            _push_batch(client, tid_a, schema_a,
                        ({"pk": i, "val": i} for i in range(1, 50_001)))
            _push_batch(client, tid_b, schema_b,
                        ({"pk": 50_000 + i, "val": i} for i in range(1, 50_001)))

            assert _live_count(client, vid) == 100_000
        finally:
            _drop_all(client, sn, views=["v"], tables=["a", "b"])


# ---------------------------------------------------------------------------
# Class 7: TestBulkJoinReduce
# join → SUM(val) GROUP BY grp — exercises join→reduce pipeline with
# 10 000-row intermediate Z-Set.
# ---------------------------------------------------------------------------

class TestBulkJoinReduce:

    def test_join_reduce_10k(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        a_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                  gnitz.ColumnDef("grp", gnitz.TypeCode.I64),
                  gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        b_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                  gnitz.ColumnDef("extra", gnitz.TypeCode.I64)]
        a_tid, a_schema, a_name = _make_table(client, sn, a_cols)
        b_tid, b_schema, b_name = _make_table(client, sn, b_cols)
        vname = "v_" + _uid()
        try:
            # push b first (trace), then a (delta)
            N = 10_000
            _push_batch(client, b_tid, b_schema,
                        ({"pk": i, "extra": 0} for i in range(1, N + 1)))

            # join output payload cols: [pk(0), grp(1), val(2), extra(3)]
            # group_by_cols=[1] → grp, agg_col_idx=2 → val
            cb = client.circuit_builder(source_table_id=a_tid)
            inp = cb.input_delta()
            j = cb.join(inp, b_tid)
            red = cb.reduce(j, group_by_cols=[1], agg_func_id=2, agg_col_idx=2)
            cb.sink(red)
            circuit = cb.build()

            out_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U128, primary_key=True),
                        gnitz.ColumnDef("grp", gnitz.TypeCode.I64),
                        gnitz.ColumnDef("agg", gnitz.TypeCode.I64)]
            vid = client.create_view_with_circuit(sn, vname, circuit, out_cols)

            _push_batch(client, a_tid, a_schema,
                        ({"pk": i, "grp": i % 50, "val": 1} for i in range(1, N + 1)))

            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 50, f"expected 50 groups, got {len(rows)}"
            # r[0]=U128 pk, r[1]=grp, r[2]=agg
            totals = {r[1]: r[2] for r in rows}
            for g in range(50):
                assert totals[g] == 200, \
                    f"group {g}: expected 200, got {totals.get(g)}"
        finally:
            try:
                client.drop_view(sn, vname)
            except Exception:
                pass
            for tname in [a_name, b_name]:
                try:
                    client.drop_table(sn, tname)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Class 8: TestBulkExchange
# 100 000 rows pushed in a single call — exercises exchange fan-out and
# per-worker storage under real load; L0 flushes.
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
class TestBulkExchange:

    def test_exchange_100k(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tid, schema, tname = _make_table(client, sn, cols)
        try:
            N = 100_000
            _push_batch(client, tid, schema,
                        ({"pk": i, "val": i} for i in range(1, N + 1)))

            assert _live_count(client, tid) == N
        finally:
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Class 9: TestBulkStorage
# 1 200 000 rows in 6 batches — triggers L0→L1 FLSM compaction in all 256
# partitions and exercises multi-shard cursor merge during final scan.
# ---------------------------------------------------------------------------

class TestBulkStorage:

    def test_storage_1_2m(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tid, schema, tname = _make_table(client, sn, cols)
        try:
            BATCH_SIZE = 200_000
            for b in range(6):
                _push_batch(client, tid, schema,
                            ({"pk": b * BATCH_SIZE + i, "val": i % 1000}
                             for i in range(1, BATCH_SIZE + 1)))

            assert _live_count(client, tid) == 1_200_000
        finally:
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)
