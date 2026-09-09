"""A UNIQUE index refusing a write, on a table that already carries the index.

The value a row holds is a claim on the index, and a batch is admitted only if
every claim it leaves standing is held once. That is what makes an upsert that
vacates a value and a fresh PK that takes it admissible in one batch, and two
survivors claiming one value a rejection — including when the claim is forged
by a retraction the pusher never held.

Create-time validation over a table that already holds rows is the other half,
in `test_unique_index_create.py`.
"""

import pytest
import gnitz
from uuid import uuid4



def _sn():
    """Unique schema name for test isolation."""
    return "idx" + uuid4().hex[:8]


def _drop_all(client, sn, tables=(), views=(), indices=()):
    """Drop tables, views, and indices before dropping schema."""
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




def _insert_rows(client, sn, rows, chunk=500):
    """Multi-row INSERT into table `t` of same-width value tuples, split into
    at-most-`chunk`-row statements; a literal 'NULL' passes through."""
    for i in range(0, len(rows), chunk):
        values = ", ".join(
            f"({', '.join(str(v) for v in r)})" for r in rows[i:i + chunk])
        client.execute_sql(f"INSERT INTO t VALUES {values}", schema_name=sn)









class TestIndexIntegrity:
    def test_unique_index_violation(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (2, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_column_unique_constraint_enforced(self, client):
        """A column-level UNIQUE in CREATE TABLE creates a unique index and
        rejects duplicate values end-to-end (the silent-discard defect)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, val BIGINT UNIQUE)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO u VALUES (1, 1)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO u VALUES (2, 1)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__u__idx_val"],
                      tables=["u"])

    def test_unique_index_anticorrelated_multi_source(self, client):
        """Unique-constraint check must hold across an anti-correlated,
        multi-source index.

        The duplicate check seeks the indexed value in the secondary index,
        whose PK is compound ``(value, src_pk)``. With several distinct values
        inserted one-per-push (anti-correlated: value falls as pk rises), the
        check merges N runs. If that merge orders by the raw u128 PK
        (``(src_pk, value)``) instead of column order (``(value, src_pk)``),
        the seek for a smaller value lands on a larger value's run, reports
        "absent", and a duplicate slips through. Every duplicate here must be
        rejected.
        """
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY,"
                " val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            n = 6
            vals = [(n - i) * 100 for i in range(n)]  # 600,500,...,100
            for i, val in enumerate(vals):
                client.execute_sql(
                    f"INSERT INTO t VALUES ({i + 1}, {val})", schema_name=sn)
            # Re-inserting any existing value (with a fresh pk) must be
            # rejected — including the smallest values, which under a u128
            # merge would be masked by a larger value's run.
            for j, val in enumerate(vals):
                try:
                    client.execute_sql(
                        f"INSERT INTO t VALUES ({1000 + j}, {val})",
                        schema_name=sn,
                    )
                    raise AssertionError(
                        f"duplicate val={val} was accepted — unique check "
                        f"missed it (index merge order bug)"
                    )
                except gnitz.GnitzError:
                    pass  # expected: duplicate rejected
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_cross_partition(self, client):
        """Two separate INSERTs with different PKs but same indexed value must fail."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                # pk=1000000 likely lands on a different partition than pk=1
                client.execute_sql("INSERT INTO t VALUES (1000000, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_batch_internal_duplicate(self, client):
        """Single INSERT with two rows sharing the same indexed value must fail."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "INSERT INTO t VALUES (1, 42), (2, 42)", schema_name=sn
                )
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_upsert_same_value_allowed(self, client):
        """UPSERT with same indexed value must succeed (not a violation)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            # Re-insert same PK with same value via explicit UPSERT
            client.execute_sql(
                "INSERT INTO t VALUES (1, 42) "
                "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
                schema_name=sn,
            )
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_upsert_change_value(self, client):
        """UPSERT that changes the indexed value must succeed."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 99) "
                "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
                schema_name=sn,
            )
            # Verify final state
            tid, _ = client.resolve_table(sn, "t")
            result = client.scan(tid)
            assert result.schema is not None
            assert len(result.pks) == 1
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_upsert_to_existing_value(self, client):
        """UPSERT pk=1 to val=99 when pk=2 already holds val=99 must fail."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, 99)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                # UPSERT pk=1 to val=99 — conflicts with pk=2
                client.execute_sql("INSERT INTO t VALUES (1, 99)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_update_to_existing_value(self, client):
        """UPDATE pk=1 SET val=99 when pk=2 already holds val=99 must fail."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, 99)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "UPDATE t SET val = 99 WHERE pk = 1", schema_name=sn
                )
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_concurrent_buffered(self, server):
        """Two back-to-back inserts with the same unique value must fail.

        The second insert arrives before any scan forces a flush, so both
        writes would be buffered without this fix — the TOCTOU window.
        With the fix, flush_pending_for_tid drains the first insert before
        validating the second.
        """
        sn = _sn()
        with gnitz.connect(server) as c1, gnitz.connect(server) as c2:
            c1.create_schema(sn)
            try:
                c1.execute_sql(
                    "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                    schema_name=sn,
                )
                c1.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
                # First insert via c1 — may be buffered, not yet flushed to workers.
                c1.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
                # Second insert via c2 — must detect the conflict.
                with pytest.raises(gnitz.GnitzError):
                    c2.execute_sql("INSERT INTO t VALUES (2, 42)", schema_name=sn)
            finally:
                _drop_all(c1, sn,
                          indices=[f"{sn}__t__idx_val"],
                          tables=["t"])

    def test_unique_null_not_violation(self, client):
        """Multiple NULLs in a unique-indexed nullable column must not conflict."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, NULL)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_after_delete(self, client):
        """Inserting a value freed by a prior DELETE must succeed."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
            # val=42 is now free
            client.execute_sql("INSERT INTO t VALUES (2, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_multi_index(self, client):
        """Table with two unique indices; violation on first index is caught."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(b)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 10, 20)", schema_name=sn)
            # Unique on both a and b
            client.execute_sql("INSERT INTO t VALUES (2, 11, 21)", schema_name=sn)
            # Violates unique on a
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (3, 10, 22)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_a", f"{sn}__t__idx_b"],
                      tables=["t"])

    def test_unique_upsert_intra_batch_duplicate_new_value(self, client):
        """Two UPSERT rows (both existing PKs) set the same NEW unique value in
        one batch. The new value is absent from committed storage, so the
        occupancy probe answers "free"; only the in-batch duplicate check can
        catch it. Must raise — otherwise the unique index is silently corrupted
        with two rows holding the same value."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                # Both pk=1 and pk=2 already exist; both UPSERT to val=99,
                # which is not present in storage.
                client.execute_sql(
                    "INSERT INTO t VALUES (1, 99), (2, 99) "
                    "ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val",
                    schema_name=sn,
                )
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

class TestAtomicUniqueTransfers:
    def _raw_table(self, client, sn, cols):
        """Create a raw table named `t` + a SQL unique index on `val`.
        Returns (tid, schema)."""
        schema = gnitz.Schema(cols)
        tid = client.create_table(sn, "t", cols)
        client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
        return tid, schema

    def test_sql_bulk_shift_accepted(self, client):
        """`UPDATE t SET val = val + 1` on a dense sequence ships one batch of
        same-PK +1 upserts with no retraction; the holder of each new value is
        the previous row, itself upserted off it. Must succeed (form 2)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT UNIQUE)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)", schema_name=sn)
            client.execute_sql("UPDATE t SET val = val + 1", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            rows = sorted((r.pk, r.val) for r in client.scan(tid))
            assert rows == [(1, 2), (2, 3), (3, 4)], rows
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_sql_bulk_swap_accepted(self, client):
        """`UPDATE t SET val = 3 - val` swaps two rows' unique values in one batch
        of same-PK +1 upserts. Must succeed (form 2)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT UNIQUE)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 1), (2, 2)", schema_name=sn)
            client.execute_sql("UPDATE t SET val = 3 - val", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            rows = sorted((r.pk, r.val) for r in client.scan(tid))
            assert rows == [(1, 2), (2, 1)], rows
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_raw_transfer_accepted(self, client):
        """One batch {retract P1/5, insert P2/5} moves a unique value to a fresh
        PK; the committed holder P1 releases it via explicit retraction. Must
        succeed (form 1)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            tid, schema = self._raw_table(client, sn, cols)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=5)
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=5, _weight=-1)
            b.append(pk=2, val=5, _weight=1)
            client.push(tid, b)  # must succeed
            rows = sorted((r.pk, r.val) for r in client.scan(tid))
            assert rows == [(2, 5)], rows
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_raw_genuine_duplicate_rejected(self, client):
        """A fresh-PK insertion of a still-held value with no retraction freeing
        it is a genuine duplicate — rejected."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            tid, schema = self._raw_table(client, sn, cols)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=5)
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=3, val=5, _weight=1)  # no retraction frees val=5
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_raw_forged_retraction_rejected(self, client):
        """The exemption keys on (holder PK, value): a retraction naming a
        non-holder must not let a real duplicate through."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            tid, schema = self._raw_table(client, sn, cols)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=5)  # only pk=1 holds 5
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=3, val=5, _weight=-1)  # forged: pk=3 does not hold 5
            b.append(pk=2, val=5, _weight=1)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_raw_forged_retraction_freed_by_upserted_holder_accepted(self, client):
        """`{P2/7@+1, P4/6@+1, P3/6@-1}` over committed `P2/6`. The batch folds
        to `{P2→7, P4→6}`, and 6's committed holder P2 survives holding 7 — so
        the post-write index `{6→P4, 7→P2}` is unique and the write is valid,
        whatever the (forged) P3 retraction names."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            tid, schema = self._raw_table(client, sn, cols)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=2, val=6)  # committed holder of 6
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=2, val=7, _weight=1)   # P2 upserted off 6
            b.append(pk=4, val=6, _weight=1)   # fresh insert of 6
            b.append(pk=3, val=6, _weight=-1)  # retraction of an absent PK
            client.push(tid, b)
            rows = sorted((r.pk, r.val) for r in client.scan(tid))
            assert rows == [(2, 7), (4, 6)], rows
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

class TestPlainPushFoldedValidation:
    def _raw_table(self, client, sn, cols=None):
        """Raw table `t (pk U64 PK, val I64)` + a SQL unique index on `val`."""
        cols = cols or [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                        gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        schema = gnitz.Schema(cols)
        tid = client.create_table(sn, "t", cols)
        client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
        return tid, schema

    def test_repeated_identical_row_in_one_batch_accepted(self, client):
        """Case A: `{P1/10@+1, P1/10@+1}` — one live row after the fold."""
        sn = _sn()
        client.create_schema(sn)
        try:
            tid, schema = self._raw_table(client, sn)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10)
            b.append(pk=1, val=10)
            client.push(tid, b)
            assert sorted((r.pk, r.val) for r in client.scan(tid)) == [(1, 10)]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_superseded_value_never_written_accepted(self, client):
        """Case B: `{P1/10@+1, P1/20@+1}` over committed `P9/10`. Only 20 is
        written, so the superseded 10 must not be probed against P9."""
        sn = _sn()
        client.create_schema(sn)
        try:
            tid, schema = self._raw_table(client, sn)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=9, val=10)
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10)
            b.append(pk=1, val=20)
            client.push(tid, b)
            assert sorted((r.pk, r.val) for r in client.scan(tid)) == [(1, 20), (9, 10)]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_in_batch_claimant_removed_again_accepted(self, client):
        """Case C: `{P1/10@+1, P2/10@+1, P2@-1}` — P2 is gone after the fold, so
        only P1 claims 10."""
        sn = _sn()
        client.create_schema(sn)
        try:
            tid, schema = self._raw_table(client, sn)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10)
            b.append(pk=2, val=10)
            b.append(pk=2, val=10, _weight=-1)
            client.push(tid, b)
            assert sorted((r.pk, r.val) for r in client.scan(tid)) == [(1, 10)]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_fresh_pk_takes_value_vacated_by_upsert_accepted(self, client):
        """Case D: `{P1/99@+1, P2/10@+1}` over committed `P1/10`. P2 is a fresh
        PK, and the row that frees 10 is an upsert of a different PK."""
        sn = _sn()
        client.create_schema(sn)
        try:
            tid, schema = self._raw_table(client, sn)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10)
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=99)
            b.append(pk=2, val=10)
            client.push(tid, b)
            assert sorted((r.pk, r.val) for r in client.scan(tid)) == [(1, 99), (2, 10)]
            # The index moved with the rows: 10 is P2's now, and nothing holds 5.
            b = gnitz.ZSetBatch(schema)
            b.append(pk=3, val=10)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_fresh_pk_takes_value_vacated_by_filler_retraction_accepted(self, client):
        """Case E: a retraction row carries filler payload (what `delete` ships),
        so the freed value is not on the wire at all — the committed row's own
        payload is what the apply retracts."""
        sn = _sn()
        client.create_schema(sn)
        try:
            tid, schema = self._raw_table(client, sn)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=5)
            client.push(tid, b)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=0, _weight=-1)  # filler payload, as `delete` sends
            b.append(pk=2, val=5)
            client.push(tid, b)
            assert sorted((r.pk, r.val) for r in client.scan(tid)) == [(2, 5)]
            # P1's index entry went with its row: 5 is P2's alone.
            b = gnitz.ZSetBatch(schema)
            b.append(pk=3, val=5)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_two_survivors_claiming_one_value_rejected(self, client):
        """The in-bundle duplicate that survives the fold is still a violation."""
        sn = _sn()
        client.create_schema(sn)
        try:
            tid, schema = self._raw_table(client, sn)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10)
            b.append(pk=2, val=10)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
            assert not list(client.scan(tid))
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_bulk_colliding_fresh_pks_rejected(self, client):
        """A re-run bulk load: 2000 fresh PKs re-claiming 2000 committed values,
        none of which the batch frees. Every span comes back occupied by a holder
        the bundle does not retire, so the whole push must be rejected — and one
        occupancy probe decides all 2000."""
        sn = _sn()
        client.create_schema(sn)
        try:
            tid, schema = self._raw_table(client, sn)
            n = 2000
            b = gnitz.ZSetBatch(schema)
            for i in range(n):
                b.append(pk=i, val=i)
            client.push(tid, b)

            b = gnitz.ZSetBatch(schema)
            for i in range(n):
                b.append(pk=n + i, val=i)  # fresh PKs, committed values
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, b)
            assert len(list(client.scan(tid))) == n
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

class TestWidePkUniqueIndex:
    def _wide_table(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
            "c BIGINT UNSIGNED NOT NULL, val BIGINT UNSIGNED NOT NULL, "
            "PRIMARY KEY (a, b, c))",
            schema_name=sn,
        )
        client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)

    def test_duplicate_value_on_distinct_wide_pk_rejected(self, client):
        sn = _sn()
        client.create_schema(sn)
        try:
            self._wide_table(client, sn)
            client.execute_sql("INSERT INTO t VALUES (1, 1, 1, 42)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (2, 2, 2, 42)", schema_name=sn)
            res = client.execute_sql("SELECT a, val FROM t", schema_name=sn)
            assert sorted(tuple(r) for r in res[0]["rows"]) == [(1, 42)]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_prefix_colliding_wide_pks_are_distinct_rows(self, client):
        """`(7,7,100)` and `(7,7,200)` share their first 16 bytes. Moving the
        second row's value onto the first must be rejected: a 16-byte-truncated
        holder compare would misread the collision as the row's own entry."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._wide_table(client, sn)
            client.execute_sql(
                "INSERT INTO t VALUES (7, 7, 100, 10), (7, 7, 200, 42)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "UPDATE t SET val = 42 WHERE a = 7 AND b = 7 AND c = 100", schema_name=sn)
            res = client.execute_sql("SELECT c, val FROM t", schema_name=sn)
            assert sorted(tuple(r) for r in res[0]["rows"]) == [(100, 10), (200, 42)]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_wide_pk_keeping_its_own_value_admitted(self, client):
        """A row rewriting its own unchanged value collides only with its own
        committed index entry, which the write retracts."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._wide_table(client, sn)
            client.execute_sql("INSERT INTO t VALUES (5, 6, 7, 42)", schema_name=sn)
            client.execute_sql(
                "UPDATE t SET val = 42 WHERE a = 5 AND b = 6 AND c = 7", schema_name=sn)
            res = client.execute_sql("SELECT c, val FROM t", schema_name=sn)
            assert sorted(tuple(r) for r in res[0]["rows"]) == [(7, 42)]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

_HOLDER_PKS = (7, 999983)

# The PK a value moves to during the drop-index window. Outside _HOLDER_PKS, so
# "moves to a different PK" is true for every sweep value.
_MOVED_PK = 31337

# NULL-valued rows spread over a PK range wide enough to reach every worker.
_NULL_PK_LO, _NULL_PK_HI = 10_000, 10_048

# `(column type, the value whose order-preserving image is all-zero for it, that
# value's unsigned native image)`. The last is what the seek API takes: its key
# values are `u128`, the zero-extended cell the engine reads out of a row, so a
# signed column's negative value arrives as its two's-complement image.
_ZERO_IMAGE_COLUMNS = [
    ("BIGINT UNSIGNED", 0, 0),
    ("INT", -2147483648, 1 << 31),
]

class TestUniqueHolderFromProbe:
    def _setup(self, client, sn, col_type):
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a {col_type})",
            schema_name=sn,
        )

    def _fill_nulls(self, client, sn):
        """One multi-row INSERT of NULL-valued rows."""
        _insert_rows(client, sn, [(pk, "NULL") for pk in range(_NULL_PK_LO, _NULL_PK_HI)])

    def _holders_of(self, client, sn, value):
        """Committed PKs whose `a` equals `value`, read by a full scan (a SELECT
        by the indexed column would itself be the thing under test)."""
        res = client.execute_sql("SELECT pk, a FROM t", schema_name=sn)
        assert res[0]["type"] == "Rows"
        return sorted(r.pk for r in res[0]["rows"] if r.a == value)

    def _seek_pk(self, client, tid, value):
        """PKs a direct single-column index seek returns for `value`."""
        res = client.seek_by_index(tid, [1], [value])
        if res.schema is None:
            return []
        return sorted(pk for pk, w in zip(res.pks, res.weights) if w > 0)

    # -- A NULL indexed cell must not claim the all-zero key image -------------

    @pytest.mark.parametrize("col_type,value,seek_key", _ZERO_IMAGE_COLUMNS)
    @pytest.mark.parametrize("holder_pk", _HOLDER_PKS)
    def test_index_seek_finds_the_zero_image_holder_among_nulls(
            self, client, col_type, value, seek_key, holder_pk):
        """`value` is the one whose order-preserving image is all-zero for this
        column type — the image a NULL indexed cell would fold onto if anything
        keyed by it treated NULL as a value. The index is NULL-distinct (a NULL
        row has no entry at all), so a direct seek for `value` must return the
        holder however many NULL rows were written after it."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._setup(client, sn, col_type)
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(
                f"INSERT INTO t VALUES ({holder_pk}, {value})", schema_name=sn)
            self._fill_nulls(client, sn)

            assert self._seek_pk(client, tid, seek_key) == [holder_pk]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a"], tables=["t"])

    @pytest.mark.parametrize("col_type,value",
                             [(c, v) for c, v, _ in _ZERO_IMAGE_COLUMNS])
    @pytest.mark.parametrize("holder_pk", _HOLDER_PKS)
    def test_update_delete_by_unique_value_with_null_rows(
            self, client, col_type, value, holder_pk):
        """The same all-zero-image value, reached through SQL instead of a direct
        seek: with NULL rows committed alongside the holder, UPDATE and then
        DELETE by that value must each affect exactly the holder's one row."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._setup(client, sn, col_type)
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            client.execute_sql(
                f"INSERT INTO t VALUES ({holder_pk}, {value})", schema_name=sn)
            self._fill_nulls(client, sn)

            res = client.execute_sql(
                f"UPDATE t SET a = {value} WHERE a = {value}", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            res = client.execute_sql(f"DELETE FROM t WHERE a = {value}", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1
            assert self._holders_of(client, sn, value) == []
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a"], tables=["t"])

    @pytest.mark.parametrize("holder_pk", _HOLDER_PKS)
    def test_unique_constraint_holds_with_null_rows(self, client, holder_pk):
        """Under the same NULL poisoning, a bundle that moves a second committed
        row's value AND claims the holder's value for a fresh PK must be
        rejected — the second row is what makes the bundle touch a committed PK,
        so the check reaches the holder rather than stopping at the claim."""
        sn = _sn()
        client.create_schema(sn)
        other, fresh = _NULL_PK_HI + 1, _NULL_PK_HI + 2
        try:
            self._setup(client, sn, "BIGINT UNSIGNED")
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            client.execute_sql(f"INSERT INTO t VALUES ({holder_pk}, 0)", schema_name=sn)
            client.execute_sql(f"INSERT INTO t VALUES ({other}, 77)", schema_name=sn)
            self._fill_nulls(client, sn)

            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    f"INSERT INTO t VALUES ({other}, 88), ({fresh}, 0)"
                    " ON CONFLICT (pk) DO UPDATE SET a = EXCLUDED.a",
                    schema_name=sn)
            assert self._holders_of(client, sn, 0) == [holder_pk]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a"], tables=["t"])

    # -- A value that moved while no index existed ----------------------------

    @pytest.mark.parametrize("holder_pk", _HOLDER_PKS)
    def test_value_moved_across_the_drop_index_window(self, client, holder_pk):
        """The value is deleted and re-inserted under a different PK while no
        index exists at all. Nothing rewrites the moved row through the index
        before the re-create, so both a direct seek and a bundle claiming the
        value must name the NEW holder, not the pre-DROP one."""
        sn = _sn()
        client.create_schema(sn)
        other, fresh = 555, 556
        try:
            self._setup(client, sn, "BIGINT NOT NULL")
            # Both rows are written WHILE the index exists, then it is dropped:
            # the window below writes with no index in the catalog at all.
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            client.execute_sql(f"INSERT INTO t VALUES ({holder_pk}, 7)", schema_name=sn)
            client.execute_sql(f"INSERT INTO t VALUES ({other}, 77)", schema_name=sn)
            client.execute_sql(f"DROP INDEX {sn}__t__idx_a", schema_name=sn)

            client.execute_sql(f"DELETE FROM t WHERE pk = {holder_pk}", schema_name=sn)
            client.execute_sql(f"INSERT INTO t VALUES ({_MOVED_PK}, 7)", schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)

            assert self._seek_pk(client, tid, 7) == [_MOVED_PK]
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    f"INSERT INTO t VALUES ({other}, 88), ({fresh}, 7)"
                    " ON CONFLICT (pk) DO UPDATE SET a = EXCLUDED.a",
                    schema_name=sn)
            assert self._holders_of(client, sn, 7) == [_MOVED_PK]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a"], tables=["t"])

    # -- The two shapes the reply split newly carries -------------------------

    def test_composite_unique_holder_split(self, client):
        """A composite span is wider than one column, so the `[span ‖ holder PK]`
        split must land at the whole span's width. The bundle touches a second
        committed PK, so the check resolves a holder rather than stopping at the
        claim."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY,"
                " a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 1, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, 2, 2)", schema_name=sn)

            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "INSERT INTO t VALUES (2, 3, 3), (999983, 1, 1)"
                    " ON CONFLICT (pk) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b",
                    schema_name=sn)
            res = client.execute_sql("SELECT pk, a, b FROM t", schema_name=sn)
            assert sorted(r.pk for r in res[0]["rows"] if (r.a, r.b) == (1, 1)) == [1]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_replicated_owner_holder_is_deduped_and_decisive(self, client):
        """Every worker holds a replicated table's whole index, so every worker
        answers for the same span. The `W` identical answers must collapse to one
        holder, and that holder must decide the verdict: the same bundle shape is
        rejected when the holder keeps the value and accepted when it releases it
        in the same bundle."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)"
                " WITH (replicated = true)", schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, 77)", schema_name=sn)

            # pk=1 keeps 42 -> the fresh claim collides.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "INSERT INTO t VALUES (2, 88), (999983, 42)"
                    " ON CONFLICT (pk) DO UPDATE SET a = EXCLUDED.a",
                    schema_name=sn)
            res = client.execute_sql("SELECT pk, a FROM t", schema_name=sn)
            assert sorted(r.pk for r in res[0]["rows"] if r.a == 42) == [1]

            # Same bundle, but the holder releases 42 in it -> accepted.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 99), (999983, 42)"
                " ON CONFLICT (pk) DO UPDATE SET a = EXCLUDED.a",
                schema_name=sn)
            res = client.execute_sql("SELECT pk, a FROM t", schema_name=sn)
            rows = {r.pk: r.a for r in res[0]["rows"]}
            assert rows[1] == 99 and rows[999983] == 42
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a"], tables=["t"])
