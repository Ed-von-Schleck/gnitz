"""SQL-standard INSERT semantics + PostgreSQL-style ON CONFLICT tests.

Covers:
 - Plain INSERT rejects PK conflicts with a PG-style error.
 - Intra-batch PK duplicates are rejected atomically.
 - ON CONFLICT (pk) DO NOTHING skips conflicting rows.
 - ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col upserts rows.
 - Unsupported ON CONFLICT variants return clear error messages.
"""

import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _make_table(client):
    """CREATE TABLE t (pk BIGINT PK, val BIGINT) with fresh schema. Returns (sn, tid)."""
    sn = "c" + _uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn,
    )
    tid, _ = client.resolve_table(sn, "t")
    return sn, tid


def _cleanup(client, sn, table="t"):
    try:
        client.drop_table(sn, table)
    except Exception:
        pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _scan_rows(client, tid):
    """Return sorted list of (pk, val) for all positive-weight rows."""
    return sorted(
        (row.pk, row.val)
        for row in client.scan(tid)
        if row.weight > 0
    )


# ---------------------------------------------------------------------------
# Plain INSERT: SQL-standard rejection
# ---------------------------------------------------------------------------


def test_insert_duplicate_pk_fails(client):
    """Two separate INSERTs with the same PK: the second must fail."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql("INSERT INTO t VALUES (1, 20)", schema_name=sn)
        assert "duplicate key" in str(exc.value).lower()
        # The original row is unchanged
        assert _scan_rows(client, tid) == [(1, 10)]
    finally:
        _cleanup(client, sn)


def test_insert_duplicate_pk_in_values_fails(client):
    """Intra-batch PK duplicate: `INSERT VALUES (1,10),(1,20)` fails; table empty."""
    sn, tid = _make_table(client)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (1, 20)", schema_name=sn,
            )
        assert "duplicate key" in str(exc.value).lower()
        # Entire batch failed atomically — no rows applied
        assert _scan_rows(client, tid) == []
    finally:
        _cleanup(client, sn)


def test_insert_partial_batch_atomicity(client):
    """Mid-batch conflict rolls back the entire batch."""
    sn, tid = _make_table(client)
    try:
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (1, 30)",
                schema_name=sn,
            )
        assert _scan_rows(client, tid) == []
    finally:
        _cleanup(client, sn)


def test_insert_conflict_with_prior_data_atomicity(client):
    """Against-store conflict rolls back the entire batch."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (2, 200)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
                schema_name=sn,
            )
        # Only the pre-existing row survives
        assert _scan_rows(client, tid) == [(2, 200)]
    finally:
        _cleanup(client, sn)


# ---------------------------------------------------------------------------
# ON CONFLICT (pk) DO NOTHING
# ---------------------------------------------------------------------------


def test_insert_on_conflict_do_nothing_skip_existing(client):
    """Second INSERT skips conflicting row, inserts the new one."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 20), (2, 30) ON CONFLICT (pk) DO NOTHING",
            schema_name=sn,
        )
        assert _scan_rows(client, tid) == [(1, 10), (2, 30)]
    finally:
        _cleanup(client, sn)


def test_insert_on_conflict_do_nothing_all_conflict(client):
    """All rows conflict → zero survivors, pre-existing rows unchanged."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 20) ON CONFLICT (pk) DO NOTHING",
            schema_name=sn,
        )
        assert _scan_rows(client, tid) == [(1, 10)]
    finally:
        _cleanup(client, sn)


def test_insert_on_conflict_do_nothing_no_target(client):
    """`ON CONFLICT DO NOTHING` with no target: same as PK target in v1."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 20), (2, 30) ON CONFLICT DO NOTHING",
            schema_name=sn,
        )
        assert _scan_rows(client, tid) == [(1, 10), (2, 30)]
    finally:
        _cleanup(client, sn)


# ---------------------------------------------------------------------------
# ON CONFLICT (pk) DO UPDATE
# ---------------------------------------------------------------------------


def test_insert_on_conflict_do_update_set_excluded(client):
    """`DO UPDATE SET val = EXCLUDED.val` upserts the conflicting row."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 20) ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
            schema_name=sn,
        )
        assert _scan_rows(client, tid) == [(1, 20)]
    finally:
        _cleanup(client, sn)


def test_insert_on_conflict_do_update_new_row_inserted(client):
    """Non-conflicting rows still insert under DO UPDATE."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 100), (2, 200) "
            "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
            schema_name=sn,
        )
        assert _scan_rows(client, tid) == [(1, 100), (2, 200)]
    finally:
        _cleanup(client, sn)


def test_insert_on_conflict_do_update_literal_assignment(client):
    """DO UPDATE SET val = 42: literal assignment."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 20) ON CONFLICT (pk) DO UPDATE SET val = 42",
            schema_name=sn,
        )
        assert _scan_rows(client, tid) == [(1, 42)]
    finally:
        _cleanup(client, sn)


# ---------------------------------------------------------------------------
# Unsupported ON CONFLICT variants
# ---------------------------------------------------------------------------


def test_insert_on_conflict_non_pk_target_unsupported(client):
    """ON CONFLICT on a non-PK, non-unique-index column is v1 unsupported."""
    sn, tid = _make_table(client)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10) ON CONFLICT (val) DO NOTHING",
                schema_name=sn,
            )
        err = str(exc.value).lower()
        # Accept either "primary key" or "unsupported"
        assert "primary key" in err or "unsupported" in err
    finally:
        _cleanup(client, sn)


def test_insert_composite_on_conflict_target_not_supported(client):
    """Composite ON CONFLICT targets are v1 unsupported."""
    sn, tid = _make_table(client)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10) ON CONFLICT (pk, val) DO NOTHING",
                schema_name=sn,
            )
        assert "composite" in str(exc.value).lower() or "unsupported" in str(exc.value).lower()
    finally:
        _cleanup(client, sn)


def test_insert_do_update_where_unsupported(client):
    """ON CONFLICT ... DO UPDATE WHERE is v1 unsupported."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "INSERT INTO t VALUES (1, 20) "
                "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val WHERE val > 5",
                schema_name=sn,
            )
        assert "where" in str(exc.value).lower() or "unsupported" in str(exc.value).lower()
    finally:
        _cleanup(client, sn)


# ---------------------------------------------------------------------------
# Regression checks that existing semantics still work
# ---------------------------------------------------------------------------


def test_update_still_works_after_fix(client):
    """Plain SQL UPDATE path is unaffected by the new INSERT semantics."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql("UPDATE t SET val = 20 WHERE pk = 1", schema_name=sn)
        assert _scan_rows(client, tid) == [(1, 20)]
    finally:
        _cleanup(client, sn)


def test_on_conflict_batch_with_multiple_conflicts(client):
    """Mix of conflicting and non-conflicting rows under DO NOTHING."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 999), (2, 999), (3, 30) "
            "ON CONFLICT (pk) DO NOTHING",
            schema_name=sn,
        )
        assert _scan_rows(client, tid) == [(1, 10), (2, 20), (3, 30)]
    finally:
        _cleanup(client, sn)


def test_insert_fresh_batch_still_works(client):
    """Plain INSERT of fresh PKs still succeeds as before."""
    sn, tid = _make_table(client)
    try:
        client.execute_sql(
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn,
        )
        assert _scan_rows(client, tid) == [(1, 10), (2, 20), (3, 30)]
    finally:
        _cleanup(client, sn)


def test_insert_on_conflict_intra_batch_duplicate_do_update_rejected(client):
    """Intra-batch duplicate PK under DO UPDATE: reject in v1 (matches PG)."""
    sn, tid = _make_table(client)
    try:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "INSERT INTO t VALUES (1, 10), (1, 20) "
                "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
                schema_name=sn,
            )
        err = str(exc.value).lower()
        assert "second time" in err or "duplicate" in err
    finally:
        _cleanup(client, sn)
