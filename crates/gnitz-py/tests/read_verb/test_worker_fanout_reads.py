"""Reads that fan out across workers: a PK seek and an index seek that must find
the one worker owning the key, and a range scan that broadcasts and merges.

Boundary inclusivity and a signed range have to survive the wire round-trip in
both directions, and a composite index at the maximum column arity has to
carry a two-sided range in its descriptor.
"""

import os
import pytest
from _uid import uid as _uid

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


def _drop_all(client, sn, tables=(), views=(), indices=()):
    for idx in indices:
        try:
            client.execute_sql(f"DROP INDEX {idx}", schema_name=sn)
        except Exception:
            pass
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








def _range_pks(client, sn, sql):
    """Sorted PKs from a direct-SELECT range query (positive weight only)."""
    result = client.execute_sql(sql, schema_name=sn)
    assert result[0]["type"] == "Rows"
    return sorted(row.pk for row in result[0]["rows"])











@_NEEDS_MULTI
def test_seek_routes_to_correct_worker(client):
    """PK seek fan-out returns exactly the one matching row from whichever worker owns it."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (42, 999)", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        result = client.seek(tid, pk=42)
        assert result.schema is not None and len(result.pks) == 1
        assert result.pks[0] == 42
    finally:
        _drop_all(client, sn, tables=["t"])

@_NEEDS_MULTI
def test_seek_by_index_broadcast(client):
    """Index seek broadcast returns the correct single row from whichever worker owns it."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, cust_id BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE INDEX ON t(cust_id)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (7, 1234)", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        result = client.seek_by_index(tid, [1], [1234])
        assert result.schema is not None and len(result.pks) == 1
        assert result.pks[0] == 7
    finally:
        _drop_all(client, sn, indices=[f"{sn}__t__idx_cust_id"], tables=["t"])

@_NEEDS_MULTI
def test_range_scan_broadcast_merge(client):
    """A range SELECT over a secondary index must BROADCAST to every worker and
    MERGE the per-worker partials: the index is partitioned by source PK, so a
    range's matches scatter across workers. The merged result must equal a
    full-scan-and-filter reference over the same data."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
            schema_name=sn)
        n = 200
        # pk == x, so PKs (hence index entries) scatter across all partitions and
        # a range on x matches rows owned by many different workers.
        vals = ",".join(f"({i}, {i})" for i in range(1, n + 1))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)

        tid, _ = client.resolve_table(sn, "t")
        ref = {row.pk: row.x for row in client.scan(tid)}

        def reference(lo, hi, lo_incl, hi_incl):
            return sorted(pk for pk, x in ref.items()
                          if (x >= lo if lo_incl else x > lo)
                          and (x <= hi if hi_incl else x < hi))

        assert _range_pks(client, sn, "SELECT * FROM t WHERE x BETWEEN 40 AND 160") \
            == reference(40, 160, True, True)
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x > 40 AND x < 160") \
            == reference(40, 160, False, False)
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x >= 150") \
            == reference(150, n, True, True)
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x <= 50") \
            == reference(1, 50, True, True)
    finally:
        _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])

@_NEEDS_MULTI
def test_range_scan_signed_and_inclusivity_over_wire(client):
    """Boundary inclusivity and a signed range survive the wire round-trip
    (descriptor encode/decode, master verbatim forward, worker OPK encode) across
    workers."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x INT NOT NULL)",
            schema_name=sn)
        # Signed values scattered across PKs/partitions.
        rows = [(i, i - 100) for i in range(1, 201)]   # x from -99 .. 100
        vals = ",".join(f"({pk}, {x})" for pk, x in rows)
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        client.execute_sql("CREATE INDEX ON t(x)", schema_name=sn)

        ref = {pk: x for pk, x in rows}

        def reference(lo, hi, lo_incl, hi_incl):
            return sorted(pk for pk, x in ref.items()
                          if (x >= lo if lo_incl else x > lo)
                          and (x <= hi if hi_incl else x < hi))

        # Signed contiguous interval (the cff7c58 payoff over the wire).
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x BETWEEN -10 AND 10") \
            == reference(-10, 10, True, True)
        # Inclusive vs exclusive boundaries land on exactly the right rows.
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x > -10 AND x < 10") \
            == reference(-10, 10, False, False)
        assert _range_pks(client, sn, "SELECT * FROM t WHERE x >= -50 AND x <= -50") \
            == reference(-50, -50, True, True)   # single-value inclusive band
    finally:
        _drop_all(client, sn, indices=[f"{sn}__t__idx_x"], tables=["t"])

@_NEEDS_MULTI
def test_range_scan_max_arity_descriptor(client):
    """A composite index at PK_LIST_MAX_COLS = 4 with a two-sided range on the
    last column (`a = .. AND b = .. AND c = .. AND d > .. AND d < ..`, n_eq = 3)
    produces an 82-byte descriptor — over the 64-byte seek_pk_extra / 80-byte
    PkBuf cap — exercising the explicit-blob send path end-to-end across
    workers."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "a BIGINT NOT NULL, b BIGINT NOT NULL, c BIGINT NOT NULL, d BIGINT NOT NULL)",
            schema_name=sn)
        # Rows sharing (a,b,c)=(1,2,3) with varying d, plus decoys on (a,b,c).
        rows = []
        pk = 1
        for d in range(0, 60):
            rows.append((pk, 1, 2, 3, d)); pk += 1
        rows.append((pk, 1, 2, 4, 30)); pk += 1   # c differs → excluded
        rows.append((pk, 2, 2, 3, 30)); pk += 1   # a differs → excluded
        vals = ",".join(f"({p},{a},{b},{c},{d})" for p, a, b, c, d in rows)
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        client.execute_sql("CREATE INDEX ON t(a, b, c, d)", schema_name=sn)

        # a=1 AND b=2 AND c=3 AND 10 < d < 50  → the (1,2,3,d) rows with d in (10,50).
        got = _range_pks(client, sn,
            "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3 AND d > 10 AND d < 50")
        expect = sorted(p for p, a, b, c, d in rows
                        if a == 1 and b == 2 and c == 3 and 10 < d < 50)
        assert got == expect, f"got {got}, expect {expect}"
    finally:
        _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b_c_d"], tables=["t"])
