"""EXCEPT / INTERSECT under UPDATEs and across ticks.

A set operation is weight arithmetic, so an update — a retraction plus an
insert — is where a clamp that is applied on the wrong side of the
subtraction shows up: the row set stays plausible while the weight inflates
or the exclusion silently lapses. Every assertion here is over the maintained
value after the update, not over the first tick.
"""

import os

from _serverproc import NEEDS_MULTI
from _uid import uid as _uid

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
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


@NEEDS_MULTI
def test_except_stable_after_update(client):
    """UPDATE on a UNIQUE PK table generates a retraction + insertion with the
    same PK in one delta.  The anti-join DT cursor must re-seek for the second
    row, not advance past the PK after the first."""
    sn = "exu_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        # Full-row identity: b's (1,999) does NOT exclude a's (1,100) — they
        # differ in payload — so both a rows survive.
        client.execute_sql("INSERT INTO b VALUES (1, 999)", schema_name=sn)
        client.execute_sql("INSERT INTO a VALUES (1, 100), (2, 200)", schema_name=sn)

        rows1 = list(client.scan(vid))
        pks1 = sorted(r["pk"] for r in rows1)
        assert pks1 == [1, 2], f"before update: {pks1}"

        # UPDATE pk=2 → delta has (pk=2,val=200,w=-1) and (pk=2,val=300,w=+1).
        # pk=2 is NOT in b, so both delta rows pass anti-join.
        # Without cursor re-seek: second row misses → pk=2 disappears.
        client.execute_sql("UPDATE a SET val = 300 WHERE pk = 2", schema_name=sn)

        rows2 = list(client.scan(vid))
        pks2 = sorted(r["pk"] for r in rows2)
        assert pks2 == [1, 2], f"after update: expected [1, 2], got {pks2}"
        val2 = next(r["val"] for r in rows2 if r["pk"] == 2)
        assert val2 == 300, f"expected pk=2 val=300, got {val2}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


@NEEDS_MULTI
def test_intersect_stable_after_update(client):
    """UPDATE through INTERSECT: the join-free min(da,db) arithmetic must track
    membership per full-row identity so the new row (same PK, different payload)
    is treated as a distinct element."""
    sn = "inu_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM a INTERSECT SELECT * FROM b",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        client.execute_sql("INSERT INTO b VALUES (1, 100), (2, 200)", schema_name=sn)
        client.execute_sql("INSERT INTO a VALUES (1, 100), (3, 300)", schema_name=sn)

        rows1 = list(client.scan(vid))
        pks1 = sorted(r["pk"] for r in rows1)
        assert pks1 == [1], f"before update: {pks1}"

        # UPDATE pk=1 val=100→500. Delta: (pk=1,val=100,w=-1), (pk=1,val=500,w=+1).
        # The retraction of (1,100) must drop the intersect row. Under full-row
        # identity the new (1,500) does NOT match b's (1,100), so pk=1 leaves the
        # INTERSECT entirely.
        client.execute_sql("UPDATE a SET val = 500 WHERE pk = 1", schema_name=sn)

        rows2 = list(client.scan(vid))
        pks2 = sorted(r["pk"] for r in rows2)
        assert pks2 == [], f"after update: expected [], got {pks2}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


# -----------------------------------------------------------------------
# INTERSECT no weight inflation
# -----------------------------------------------------------------------


@NEEDS_MULTI
def test_intersect_no_weight_inflation(client):
    """INTERSECT must not duplicate rows when one side has multiple entries for
    the same projected value. The leaf distinct caps each side's multiplicity at
    1, so min(da, db) stays in {0,1}."""
    sn = "iwi_" + _uid()
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
        vid, _ = client.resolve_table(sn, "v")

        b_vals = ", ".join(f"({i}, {i * 10})" for i in [1, 2, 3])
        client.execute_sql(f"INSERT INTO b VALUES {b_vals}", schema_name=sn)

        # 30 rows >> 3 in b → triggers swap path
        a_vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 31))
        client.execute_sql(f"INSERT INTO a VALUES {a_vals}", schema_name=sn)

        rows = list(client.scan(vid))
        pks = sorted(r["pk"] for r in rows)
        assert pks == [1, 2, 3], f"expected [1,2,3], got {pks}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


# -----------------------------------------------------------------------
# Exchange merge consolidated flag
# -----------------------------------------------------------------------

@NEEDS_MULTI
def test_except_update_non_excluded_row(client):
    """UPDATE a row that is NOT excluded by b.  The delta has same-PK
    retraction + insertion; the anti-join DT must process both correctly."""
    sn = "eur_" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY UNIQUE, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM a EXCEPT SELECT * FROM b",
            schema_name=sn,
        )
        vid, _ = client.resolve_table(sn, "v")

        # b excludes pk=1; pk=2 is free
        client.execute_sql("INSERT INTO b VALUES (1, 10)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn,
        )
        rows1 = list(client.scan(vid))
        assert sorted(r["pk"] for r in rows1) == [2, 3]

        # Update pk=3 (not excluded) → should stay in output with new value
        client.execute_sql("UPDATE a SET val = 999 WHERE pk = 3", schema_name=sn)
        rows2 = list(client.scan(vid))
        pks2 = sorted(r["pk"] for r in rows2)
        assert pks2 == [2, 3], f"after update: expected [2, 3], got {pks2}"
        val3 = next(r["val"] for r in rows2 if r["pk"] == 3)
        assert val3 == 999, f"expected val=999, got {val3}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


@NEEDS_MULTI
def test_except_multi_tick_exclusion(client):
    """EXCEPT across multiple ticks: insert b, then a in separate epochs.
    Verifies correct PK-based exclusion with 20 rows across workers."""
    sn = "emt_" + _uid()
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
        vid, _ = client.resolve_table(sn, "v")

        # b excludes even PKs
        b_vals = ", ".join(f"({i}, {i * 10})" for i in range(2, 21, 2))
        client.execute_sql(f"INSERT INTO b VALUES {b_vals}", schema_name=sn)

        # a has all 20 PKs
        a_vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 21))
        client.execute_sql(f"INSERT INTO a VALUES {a_vals}", schema_name=sn)

        rows = list(client.scan(vid))
        pks = sorted(r["pk"] for r in rows)
        expected = sorted(range(1, 21, 2))  # odd PKs only
        assert pks == expected, f"expected {expected}, got {pks}"
    finally:
        _drop_all(client, sn, views=["v"], tables=["a", "b"])


# ---------------------------------------------------------------------------
# Regression: multi-schema exchange interleaving
# ---------------------------------------------------------------------------
