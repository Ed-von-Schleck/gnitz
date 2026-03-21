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

    def test_join_a_first_10k(self, client):
        """Push a first (builds I(A)), then push b — exercises
        join_delta_trace(ΔB, I(A)), a different code path than test_join_10k."""
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
            # push a first (builds trace I(A)), then b (delta → join_delta_trace(ΔB, I(A)))
            _push_batch(client, tid_a, schema_a,
                        ({"pk": i, "a_val": i * 2} for i in range(1, N + 1)))
            _push_batch(client, tid_b, schema_b,
                        ({"pk": i, "b_val": i * 3} for i in range(1, N + 1)))

            live_rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(live_rows) == N, \
                f"expected {N} join rows, got {len(live_rows)}"

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

    def test_join_reduce_flsm(self, client):
        """250K rows per table — L0 flushes in all partitions (single-worker) or
        partial flushes (multi-worker). Verifies join→reduce correctness when
        source tables span L0 shards."""
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
            N = 250_000
            _push_batch(client, b_tid, b_schema,
                        ({"pk": i, "extra": 0} for i in range(1, N + 1)))

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
            totals = {r[1]: r[2] for r in rows}
            expected_per_group = N // 50  # 5000
            for g in range(50):
                assert totals[g] == expected_per_group, \
                    f"group {g}: expected {expected_per_group}, got {totals.get(g)}"
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
# partitions (single-worker) and exercises multi-shard cursor merge.
# Requires multi-worker so exchange fan-out + compaction interact.
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
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

            # Verify count and spot-check values in one pass.
            # expected val for a given pk: (pk - (pk-1)//BATCH_SIZE*BATCH_SIZE) % 1000
            total = 0
            errors = []
            for r in client.scan(tid):
                if r.weight <= 0:
                    continue
                total += 1
                if total % 10_000 == 1:
                    expected = (r.pk - (r.pk - 1) // BATCH_SIZE * BATCH_SIZE) % 1000
                    if r.val != expected:
                        errors.append(
                            f"pk={r.pk}: expected val={expected}, got {r.val}"
                        )
            assert total == 1_200_000, f"expected 1200000 rows, got {total}"
            assert not errors, f"value corruption after compaction: {errors[:5]}"
        finally:
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Class 10: TestBulkL0Reads
# 250 000 rows — all 256 partitions receive at least one L0 flush (single-
# worker). Verifies value correctness across multi-shard cursor reads, not
# just row counts.
# ---------------------------------------------------------------------------

class TestBulkL0Reads:

    def test_l0_values_250k(self, client):
        """Push 250K rows (pk=i, val=i%1000); verify count and value sum after
        multi-shard cursor merge — catches offset bugs that preserve count but
        return values from the wrong shard."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tid, schema, tname = _make_table(client, sn, cols)
        try:
            N = 250_000
            _push_batch(client, tid, schema,
                        ({"pk": i, "val": i % 1000} for i in range(1, N + 1)))

            count = 0
            total_val = 0
            for r in client.scan(tid):
                if r.weight <= 0:
                    continue
                count += 1
                total_val += r.val
                # spot-check periodically: val == pk % 1000
                if r.pk % 5_000 == 0:
                    assert r.val == r.pk % 1000, \
                        f"pk={r.pk}: expected val={r.pk % 1000}, got {r.val}"

            assert count == N, f"expected {N} rows, got {count}"
            # sum(i%1000 for i=1..250000) = 250 complete cycles of 0..999
            # each cycle sums to 0+1+...+999 = 499500
            assert total_val == 250 * 499_500, \
                f"expected val sum={250 * 499_500}, got {total_val}"
        finally:
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Class 11: TestBulkRetract
# Insert at L0-flush scale, then retract half — verifies that retractions
# applied against already-flushed L0 shard data are handled correctly.
# ---------------------------------------------------------------------------

class TestBulkRetract:

    def test_retract_post_l0_flush(self, client):
        """Push 100K rows (L0 flushes in ~50% of partitions), then retract the
        first 50K. Verifies count and that surviving rows have correct values."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        # unique_pk=False: explicit weight=-1 retractions, no upsert semantics
        tid, schema, tname = _make_table(client, sn, cols, unique_pk=False)
        try:
            N = 100_000
            _push_batch(client, tid, schema,
                        ({"pk": i, "val": i} for i in range(1, N + 1)))
            _push_batch(client, tid, schema,
                        ({"pk": i, "val": i, "weight": -1}
                         for i in range(1, N // 2 + 1)))

            live_rows = [r for r in client.scan(tid) if r.weight > 0]
            assert len(live_rows) == N // 2, \
                f"expected {N // 2} rows after retraction, got {len(live_rows)}"
            # surviving rows are pk=50001..100000 with val=pk; spot-check every 500th
            for r in live_rows[::500]:
                assert r.val == r.pk, \
                    f"pk={r.pk}: expected val={r.pk}, got {r.val}"
        finally:
            try:
                client.drop_table(sn, tname)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Class 12: TestHeavyAggregation
# 500 K events × 7 concurrent reduce views (10 / 100 / 1000 groups).
# Data pushed in 10 ticks of 50K rows each → JIT warm-up after tick 2-3.
# One view adds a filter before the reduce to vary the circuit topology.
#
# Schema: events(pk U64, region_id I64, product_id I64, store_id I64,
#                quantity I64, revenue I64)
#   col indices: 0=pk  1=region_id  2=product_id  3=store_id
#                4=quantity  5=revenue
#
# Views:
#   v_region_qty   SUM(quantity) GROUP BY region_id    [10 groups]
#   v_region_cnt   COUNT(*)      GROUP BY region_id    [10 groups]
#   v_product_rev  SUM(revenue)  GROUP BY product_id  [100 groups]
#   v_product_cnt  COUNT(*)      GROUP BY product_id  [100 groups]
#   v_store_rev    SUM(revenue)  GROUP BY store_id   [1000 groups]
#   v_store_cnt    COUNT(*)      GROUP BY store_id   [1000 groups]
#   v_highrev      filter(rev>500) → SUM(rev) GROUP BY store_id [≤1000 groups]
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
class TestHeavyAggregation:

    TICKS         = 10
    ROWS_PER_TICK = 50_000
    REGIONS       = 10
    PRODUCTS      = 100
    STORES        = 1_000
    AGG_COUNT     = 1
    AGG_SUM       = 2

    def _events_cols(self):
        return [
            gnitz.ColumnDef("pk",         gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("region_id",  gnitz.TypeCode.I64),
            gnitz.ColumnDef("product_id", gnitz.TypeCode.I64),
            gnitz.ColumnDef("store_id",   gnitz.TypeCode.I64),
            gnitz.ColumnDef("quantity",   gnitz.TypeCode.I64),
            gnitz.ColumnDef("revenue",    gnitz.TypeCode.I64),
        ]

    def _reduce_out_cols(self, grp_name, agg_name):
        return [
            gnitz.ColumnDef("pk",      gnitz.TypeCode.U128, primary_key=True),
            gnitz.ColumnDef(grp_name,  gnitz.TypeCode.I64),
            gnitz.ColumnDef(agg_name,  gnitz.TypeCode.I64),
        ]

    def _make_view(self, client, sn, tid, vname,
                   group_col, agg_func_id, agg_col,
                   grp_name, agg_name, filter_expr=None):
        cb = client.circuit_builder(source_table_id=tid)
        node = cb.input_delta()
        if filter_expr is not None:
            node = cb.filter(node, filter_expr)
        node = cb.reduce(node,
                         group_by_cols=[group_col],
                         agg_func_id=agg_func_id,
                         agg_col_idx=agg_col)
        cb.sink(node)
        circuit = cb.build()
        return client.create_view_with_circuit(
            sn, vname, circuit,
            self._reduce_out_cols(grp_name, agg_name),
        )

    def test_heavy_agg_500k(self, client):
        sn = "hagg_" + _uid()
        client.create_schema(sn)
        events_cols = self._events_cols()
        events_schema = gnitz.Schema(events_cols)
        e_tid, _, e_name = _make_table(client, sn, events_cols)

        # filter predicate: revenue (col 5) > 500
        eb = gnitz.ExprBuilder()
        r_rev  = eb.load_col_int(5)
        r_thr  = eb.load_const(500)
        r_cond = eb.cmp_gt(r_rev, r_thr)
        highrev_pred = eb.build(result_reg=r_cond)

        vnames = ("v_region_qty", "v_region_cnt",
                  "v_product_rev", "v_product_cnt",
                  "v_store_rev", "v_store_cnt", "v_highrev")
        try:
            v_region_qty  = self._make_view(client, sn, e_tid, "v_region_qty",
                                            1, self.AGG_SUM,   4, "region_id",  "total_qty")
            v_region_cnt  = self._make_view(client, sn, e_tid, "v_region_cnt",
                                            1, self.AGG_COUNT, 4, "region_id",  "cnt")
            v_product_rev = self._make_view(client, sn, e_tid, "v_product_rev",
                                            2, self.AGG_SUM,   5, "product_id", "total_rev")
            v_product_cnt = self._make_view(client, sn, e_tid, "v_product_cnt",
                                            2, self.AGG_COUNT, 4, "product_id", "cnt")
            v_store_rev   = self._make_view(client, sn, e_tid, "v_store_rev",
                                            3, self.AGG_SUM,   5, "store_id",   "total_rev")
            v_store_cnt   = self._make_view(client, sn, e_tid, "v_store_cnt",
                                            3, self.AGG_COUNT, 4, "store_id",   "cnt")
            v_highrev     = self._make_view(client, sn, e_tid, "v_highrev",
                                            3, self.AGG_SUM,   5, "store_id",   "total_rev",
                                            filter_expr=highrev_pred)

            exp_region_qty  = {}
            exp_region_cnt  = {}
            exp_product_rev = {}
            exp_product_cnt = {}
            exp_store_rev   = {}
            exp_store_cnt   = {}
            exp_highrev     = {}

            for tick in range(self.TICKS):
                batch = gnitz.ZSetBatch(events_schema)
                base = tick * self.ROWS_PER_TICK
                for i in range(self.ROWS_PER_TICK):
                    pk         = base + i
                    region_id  = pk % self.REGIONS
                    product_id = pk % self.PRODUCTS
                    store_id   = pk % self.STORES
                    quantity   = 1 + (pk % 10)
                    revenue    = 100 + (pk % 1000)
                    batch.append(pk=pk, region_id=region_id, product_id=product_id,
                                 store_id=store_id, quantity=quantity, revenue=revenue)
                    exp_region_qty[region_id]   = exp_region_qty.get(region_id, 0)   + quantity
                    exp_region_cnt[region_id]   = exp_region_cnt.get(region_id, 0)   + 1
                    exp_product_rev[product_id] = exp_product_rev.get(product_id, 0) + revenue
                    exp_product_cnt[product_id] = exp_product_cnt.get(product_id, 0) + 1
                    exp_store_rev[store_id]     = exp_store_rev.get(store_id, 0)     + revenue
                    exp_store_cnt[store_id]     = exp_store_cnt.get(store_id, 0)     + 1
                    if revenue > 500:
                        exp_highrev[store_id] = exp_highrev.get(store_id, 0) + revenue
                client.push(e_tid, batch)

            def _check(vid, expected, label):
                rows = {r[1]: r[2] for r in client.scan(vid) if r.weight > 0}
                assert len(rows) == len(expected), \
                    f"{label}: expected {len(expected)} groups, got {len(rows)}"
                for g, exp in expected.items():
                    assert rows[g] == exp, \
                        f"{label}[{g}]: expected {exp}, got {rows.get(g)}"

            _check(v_region_qty,  exp_region_qty,  "v_region_qty")
            _check(v_region_cnt,  exp_region_cnt,  "v_region_cnt")
            _check(v_product_rev, exp_product_rev, "v_product_rev")
            _check(v_product_cnt, exp_product_cnt, "v_product_cnt")
            _check(v_store_rev,   exp_store_rev,   "v_store_rev")
            _check(v_store_cnt,   exp_store_cnt,   "v_store_cnt")
            _check(v_highrev,     exp_highrev,      "v_highrev")

        finally:
            for vname in vnames:
                try:
                    client.drop_view(sn, vname)
                except Exception:
                    pass
            try:
                client.drop_table(sn, e_name)
            except Exception:
                pass
            client.drop_schema(sn)
