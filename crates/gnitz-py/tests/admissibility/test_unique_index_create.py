"""`CREATE UNIQUE INDEX` over a table that already holds rows.

The master validates uniqueness globally, across every worker, before it
broadcasts the index. It is the only uniqueness check there is on this path —
the per-worker backfill runs none — so without it every duplicate, within one
partition or across two, would be silently accepted and the index would
thereafter enforce a constraint the table never satisfied.
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


def _table_has_index(client, sn, table):
    """True if any live IdxTab row names `table` as its owner."""
    from gnitz._native import IDX_TAB
    batch_obj = client.scan(IDX_TAB)
    if batch_obj.schema is None:
        return False
    tid, _ = client.resolve_table(sn, table)
    # Hoisted: every `.scalars`/`.weights` read rebuilds the whole list, so
    # reading one inside the row loop is quadratic in the catalog size.
    owners = batch_obj.scalars("owner_id")
    return any(w > 0 and o == tid for w, o in zip(batch_obj.weights, owners))


def _insert_rows(client, sn, rows, chunk=500):
    """Multi-row INSERT into table `t` of same-width value tuples, split into
    at-most-`chunk`-row statements; a literal 'NULL' passes through."""
    for i in range(0, len(rows), chunk):
        values = ", ".join(
            f"({', '.join(str(v) for v in r)})" for r in rows[i:i + chunk])
        client.execute_sql(f"INSERT INTO t VALUES {values}", schema_name=sn)

def _pks(results):
    """Sorted PKs of a Rows result."""
    assert results[0]["type"] == "Rows", results[0]
    return sorted(row.pk for row in results[0]["rows"])








class TestCreateUniqueIndexValidation:
    def test_within_partition_duplicate_rejected_cluster_survives(self, client):
        """Many rows share one non-PK value across consecutive PKs, so several
        land on the SAME worker (pigeonhole over the worker count). CREATE
        UNIQUE INDEX must return a clean error — and crucially every worker must
        stay alive and answer a follow-up scan."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            # 50 rows, all val=42, PKs 1..50. With >1 worker, pigeonhole forces
            # at least two duplicates onto one worker; with 1 worker all are.
            rows = ", ".join(f"({pk}, 42)" for pk in range(1, 51))
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)

            with pytest.raises(gnitz.GnitzError) as exc:
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            # The rejection names the qualified table and the column — the
            # pre-flight is the only producer of this message.
            assert f"{sn}.t" in str(exc.value), exc.value
            assert "val" in str(exc.value), exc.value

            # Cluster survived: a full-table scan fans out to every worker and
            # must return all 50 rows.
            tid, _ = client.resolve_table(sn, "t")
            result = client.scan(tid)
            assert result.schema is not None
            assert len(result.pks) == 50, "all workers must answer the scan"
            # No phantom constraint: the index was never created.
            assert not _table_has_index(client, sn, "t")
            # And the write path is still healthy across workers.
            client.execute_sql("INSERT INTO t VALUES (51, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_cross_partition_duplicate_rejected(self, client):
        """Rows sharing one non-PK value spread across a wide PK range land on
        different workers; the duplicate is invisible to any single worker's
        backfill but must be caught by the master pre-flight. The index must NOT
        exist afterward and a fresh duplicate INSERT must still be permitted (no
        phantom constraint). (Pre-fix: silently succeeds.)"""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            spread = [1, 7, 13, 1000, 99999, 123456, 777777, 8888888, 73501234, 901234567]
            rows = ", ".join(f"({pk}, 42)" for pk in spread)
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)

            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)

            assert not _table_has_index(client, sn, "t"), \
                "no index may exist after a rejected CREATE UNIQUE INDEX"
            # No phantom constraint: another duplicate value is still accepted.
            client.execute_sql("INSERT INTO t VALUES (2, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_column_succeeds_and_enforces(self, client):
        """CREATE UNIQUE INDEX on an all-distinct column succeeds; a subsequent
        duplicate INSERT is rejected by the steady-state path, and an indexed
        lookup returns the right row."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            rows = ", ".join(f"({pk}, {pk * 10})" for pk in range(1, 31))
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)

            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")

            # Steady-state enforcement: re-inserting an existing value fails.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (999, 100)", schema_name=sn)
            # A distinct value still inserts.
            client.execute_sql("INSERT INTO t VALUES (999, 99999)", schema_name=sn)

            # Indexed lookup returns the correct row.
            results = client.execute_sql(
                "SELECT * FROM t WHERE val = 200", schema_name=sn)
            assert results[0]["type"] == "Rows"
            rows = results[0]["rows"]
            assert len(rows.pks) == 1
            assert rows.pks[0] == 20
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_multiple_nulls_allowed(self, client):
        """A nullable non-PK column with several NULLs is valid under SQL UNIQUE;
        CREATE UNIQUE INDEX must succeed, and a later duplicate non-NULL value is
        still rejected."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                schema_name=sn,
            )
            # Several NULLs plus distinct non-NULL values, spread across workers.
            client.execute_sql(
                "INSERT INTO t VALUES (1, NULL), (5, NULL), (9, NULL), "
                "(100000, 10), (200000, 20), (300000, 30)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")

            # More NULLs are still fine after creation.
            client.execute_sql("INSERT INTO t VALUES (13, NULL)", schema_name=sn)
            # A duplicate non-NULL value is rejected.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (400000, 10)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_empty_table_succeeds_and_enforces(self, client):
        """CREATE UNIQUE INDEX on an empty table succeeds and enforces
        uniqueness on later inserts."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")

            client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (1000000, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_non_unique_index_on_duplicate_column_succeeds(self, client):
        """The pre-flight is gated on is_unique: a NON-unique index over a
        column full of duplicates must still be created without error."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            rows = ", ".join(f"({pk}, 42)" for pk in range(1, 41))
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)
            # Non-unique index over an all-duplicate column: no validation.
            results = client.execute_sql("CREATE INDEX ON t(val)", schema_name=sn)
            assert results[0]["type"] == "IndexCreated"
            assert _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_larger_table_passing_and_failing_keep_cluster_alive(self, client):
        """A larger table (rows fanned across all workers) for both a passing
        and a failing CREATE UNIQUE INDEX; the cluster must stay alive either
        way. (The drain-all-frames-per-worker wedge safety on a multi-frame scan
        train is exercised directly by the drain_index_scan Rust unit test; the
        server's frame cap makes a true multi-frame scan impractical to provoke
        from E2E, so this guards the realistic large-table path.)"""
        sn = _sn()
        client.create_schema(sn)
        n = 3000
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            # Distinct values across a wide PK spread → all workers populated.
            batch = ", ".join(f"({pk}, {pk})" for pk in range(1, n + 1))
            client.execute_sql(f"INSERT INTO t VALUES {batch}", schema_name=sn)

            # Passing: all-distinct → index created.
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
            client.execute_sql(f"DROP INDEX {sn}__t__idx_val", schema_name=sn)

            # Failing: introduce one duplicate, then CREATE must reject and the
            # cluster must remain alive.
            client.execute_sql(f"INSERT INTO t VALUES ({n + 1}, 1)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)

            tid, _ = client.resolve_table(sn, "t")
            result = client.scan(tid)
            assert result.schema is not None
            assert len(result.pks) == n + 1, "all workers must answer post-failure"
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_concurrent_inserts_during_create(self, server):
        """A steady INSERT stream into the owner table concurrent with CREATE
        UNIQUE INDEX. All streamed values are distinct, so — whatever the
        interleaving — the catalog write lock orders every INSERT strictly
        before or after the pre-flight+backfill snapshot, the index must be
        created and enforce, no row may be lost from it, and no worker may
        wedge. Guards the snapshot-vs-backfill ordering."""
        import threading
        sn = _sn()
        with gnitz.connect(server) as c_writer, gnitz.connect(server) as c_ddl:
            c_ddl.create_schema(sn)
            try:
                c_ddl.execute_sql(
                    "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                    schema_name=sn,
                )
                # Seed so the pre-flight scan has data on every worker.
                seed = ", ".join(f"({i}, {i})" for i in range(1, 201))
                c_ddl.execute_sql(f"INSERT INTO t VALUES {seed}", schema_name=sn)

                stop = threading.Event()
                errors = []

                def insert_stream():
                    v = 1000
                    try:
                        while not stop.is_set() and v < 5000:
                            c_writer.execute_sql(
                                f"INSERT INTO t VALUES ({v}, {v})", schema_name=sn)
                            v += 1
                    except Exception as e:  # noqa: BLE001 — surfaced via `errors`
                        errors.append(e)

                th = threading.Thread(target=insert_stream)
                th.start()
                try:
                    c_ddl.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
                finally:
                    stop.set()
                    th.join(timeout=60)
                assert not th.is_alive(), "insert stream did not stop"
                assert not errors, f"streaming inserts errored: {errors}"
                assert _table_has_index(c_ddl, sn, "t")

                # Index enforces a seeded value, and the cluster is alive.
                with pytest.raises(gnitz.GnitzError):
                    c_ddl.execute_sql("INSERT INTO t VALUES (888888, 1)", schema_name=sn)
                # A brand-new distinct value still inserts (no spurious reject).
                c_ddl.execute_sql("INSERT INTO t VALUES (888888, 888888)", schema_name=sn)
            finally:
                _drop_all(c_ddl, sn,
                          indices=[f"{sn}__t__idx_val"],
                          tables=["t"])

class TestUniqueIndexCreatePreflight:
    def _bigint_table(self, client, sn, val_type="BIGINT NOT NULL"):
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val {val_type})",
            schema_name=sn,
        )

    def _compound_pk_table(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL,"
            " payload BIGINT, PRIMARY KEY (a, b))",
            schema_name=sn,
        )

    def test_preexisting_cross_partition_duplicate_rejected(self, client):
        """One value held by many PKs spans partitions (and, by pigeonhole
        over <=8 workers, repeats within at least one); the merge must reject
        either way, and no index may be created."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            _insert_rows(client, sn, [(pk, 42) for pk in range(1, 9)])
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t"), \
                "failed pre-flight must not create the index"
            # The failed DDL must leave the table fully usable.
            client.execute_sql("INSERT INTO t VALUES (100, 42)", schema_name=sn)
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_preexisting_duplicate_among_many_rows_rejected(self, client):
        """A single planted duplicate pair among hundreds of distinct values
        is found by the merge."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            rows = [(pk, pk * 10) for pk in range(1, 301)]
            rows.append((1000, 1500))  # duplicates val of pk=150
            _insert_rows(client, sn, rows)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_create_succeeds_at_scale_then_enforces(self, client):
        """A few thousand distinct values across all workers pass the
        pre-flight; the very first post-create INSERT of a duplicate is
        rejected (the merge's seed must be complete, never truncated) while a
        fresh value is accepted."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            _insert_rows(client, sn, [(pk, pk * 3 + 1) for pk in range(1, 2001)])
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                # duplicates val of pk=500 (500*3+1)
                client.execute_sql("INSERT INTO t VALUES (9001, 1501)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (9002, 0)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_preexisting_nulls_allowed(self, client):
        """SQL UNIQUE permits many NULLs: NULL keys never enter any worker's
        key stream, so a NULL-heavy column passes and stays NULL-insertable."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn, val_type="BIGINT")
            rows = [(pk, "NULL") for pk in range(1, 9)]
            rows += [(pk, pk) for pk in range(100, 108)]
            _insert_rows(client, sn, rows)
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (200, NULL)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_pk_column_unique_index_short_circuits(self, client):
        """A unique index on the sole PK column is trivially satisfiable (PK
        uniqueness is enforced on every ingest path): the create succeeds without scanning and the table keeps
        working."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            _insert_rows(client, sn, [(pk, pk) for pk in range(1, 50)])
            client.execute_sql("CREATE UNIQUE INDEX ON t(pk)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
            client.execute_sql("INSERT INTO t VALUES (1000, 1000)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_pk"], tables=["t"])

    def test_compound_pk_member_duplicates_rejected(self, client):
        """A member of a compound PK is not trivially unique: duplicate `a`
        values across distinct (a, b) rows must fail the pre-flight."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._compound_pk_table(client, sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 10), (1, 2, 20), (3, 1, 30)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_compound_pk_member_distinct_accepted(self, client):
        """Distinct values in a compound-PK member pass the full-path scan."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._compound_pk_table(client, sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 1, 10), (2, 2, 20), (3, 1, 30)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (2, 9, 90)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a"], tables=["t"])

    def test_signed_negative_duplicate_rejected(self, client):
        """Equal negative values must collide despite the merge's u128 order
        not being monotonic in signed value (equal value => equal key is what
        the verdict needs)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            _insert_rows(client, sn, [(1, -5), (2, 300), (3, -5)])
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_signed_distinct_negatives_accepted_then_enforced(self, client):
        """Distinct negative values pass; the post-create filter then rejects a
        re-insert of one of them (native-key round-trip, no sign confusion)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            _insert_rows(client, sn, [(1, -1), (2, -2), (3, -3), (4, 0)])
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (10, -2)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (11, -9)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_multi_frame_key_train(self, unique_preflight_frame_server):
        """With frames shrunk to 7 keys, every worker streams a multi-frame
        continuation train; the merge must stay exact across frame boundaries —
        both verdicts."""
        client = unique_preflight_frame_server
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            _insert_rows(client, sn, [(pk, pk * 7) for pk in range(1, 201)])
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
            client.execute_sql(f"DROP INDEX {sn}__t__idx_val", schema_name=sn)

            # Plant one duplicate pair on far-apart PKs and re-create: the
            # equal keys are adjacent in the merged stream wherever the frame
            # boundaries fall.
            client.execute_sql("INSERT INTO t VALUES (1000, 700)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, tables=["t"])

    def test_worker_fault_mid_preflight(self, unique_preflight_fault_server):
        """An injected worker fault during the pre-flight scan must surface as
        a client error with no index created, no filter seeded, and every
        worker drained (not wedged): the table stays fully usable, and the
        PK short-circuit — which never fans out — still succeeds."""
        client = unique_preflight_fault_server
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            _insert_rows(client, sn, [(pk, pk) for pk in range(1, 33)])
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t")
            # No filter was seeded and no index exists: a duplicate value must
            # be accepted, proving no partial constraint leaked out of the
            # failed DDL.
            client.execute_sql("INSERT INTO t VALUES (100, 1)", schema_name=sn)
            # All workers answer a full scan: nobody is wedged on a
            # half-drained pre-flight train.
            tid, _ = client.resolve_table(sn, "t")
            rows = list(client.scan(tid))
            assert len(rows) == 33
            # The PK short-circuit returns before any fan-out, so it succeeds
            # even while every worker's scan path is faulted.
            client.execute_sql("CREATE UNIQUE INDEX ON t(pk)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn,
                      indices=[f"{sn}__t__idx_pk", f"{sn}__t__idx_val"],
                      tables=["t"])

    def test_unique_pk_weight2_push_collapses_to_one_instance(self, client):
        """A pushed weight ≥ 2 is the row repeated: it
        must collapse to ONE live instance (repeated upsert of itself), a
        single delete must remove it entirely, and the sole-PK-column unique
        index stays trivially creatable (the PK short-circuit's premise)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                    gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
            schema = gnitz.Schema(cols)
            tid = client.create_table(sn, "t", cols)
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10, _weight=2)
            client.push(tid, b)
            rows = [(r.pk, r.weight) for r in client.scan(tid)]
            assert rows == [(1, 1)], rows
            b = gnitz.ZSetBatch(schema)
            b.append(pk=1, val=10, _weight=-1)
            client.push(tid, b)
            assert list(client.scan(tid)) == []
            client.execute_sql("CREATE UNIQUE INDEX ON t(pk)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_pk"], tables=["t"])

    def test_preflight_spill_large_partition_bounded_and_correct(
            self, unique_preflight_spill_server):
        """With a tiny spill budget, a CREATE UNIQUE INDEX over a table whose
        per-worker partition far exceeds the budget drives the external merge
        sort (many spill runs + a k-way merge over the runs). On all-distinct
        data the index is created and then enforces uniqueness on a fresh
        INSERT; a genuinely new value is still admitted."""
        client = unique_preflight_spill_server
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            # 2000 distinct vals over a wide PK spread → hundreds of spans per
            # worker, far past the 256-byte (32-span) budget → many spilled runs.
            n = 2000
            _insert_rows(client, sn, [(i * 7 + 1, i) for i in range(n)])
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert _table_has_index(client, sn, "t")
            # The created index enforces uniqueness: val=5 is already present.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    f"INSERT INTO t VALUES ({n * 10}, 5)", schema_name=sn)
            # A genuinely new value is admitted.
            client.execute_sql(
                f"INSERT INTO t VALUES ({n * 10 + 1}, {n + 1})", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

    def test_preflight_spill_rejects_duplicate_across_runs(
            self, unique_preflight_spill_server):
        """A pre-existing duplicate value buried in a large partition (spilled
        across many external-sort runs, on the same worker or across partitions)
        is still caught by the pre-flight: the CREATE is rejected, no index
        remains, and a fresh duplicate INSERT is still permitted (no phantom
        constraint from the aborted DDL)."""
        client = unique_preflight_spill_server
        sn = _sn()
        client.create_schema(sn)
        try:
            self._bigint_table(client, sn)
            n = 2000
            rows = [(i * 7 + 1, i) for i in range(n)]
            # Repeat the first row's val on a far-away PK appended last, so the
            # two equal spans are buried among many runs / partitions and only
            # the sorted merge brings them adjacent.
            rows.append((n * 10 + 3, 0))
            _insert_rows(client, sn, rows)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _table_has_index(client, sn, "t"), \
                "no index may exist after a rejected spill-scale CREATE UNIQUE INDEX"
            # No phantom constraint: a fresh duplicate is still allowed.
            client.execute_sql(
                f"INSERT INTO t VALUES ({n * 10 + 4}, 0)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_val"], tables=["t"])

class TestCompositeUniqueIndex:
    """Composite UNIQUE (a, b): both DDL entry points, distributed create-time
    pre-flight, distributed insert-time enforcement, NULL-distinctness, the
    trivial-PK short-circuit, and atomic composite-value transfer. Multi-worker
    coverage comes from GNITZ_WORKERS=4 in the E2E run."""

    def test_table_level_unique_constraint_entry_point(self, client):
        """CREATE TABLE ... UNIQUE (a, b) (table constraint) creates and enforces
        the composite index — the removed table-level planner gate."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL, UNIQUE (a, b))",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO t VALUES (1, 5, 1)", schema_name=sn)
            # Distinct composite admitted; duplicate rejected.
            client.execute_sql("INSERT INTO t VALUES (2, 5, 2)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (3, 5, 1)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_create_preflight_rejects_cross_partition_duplicate(self, client):
        """A pre-existing duplicate (a, b) spread across a wide PK range is
        invisible to any single worker's backfill but caught by the master
        composite pre-flight. The index must NOT exist afterward, and a fresh
        duplicate INSERT must still be permitted (no phantom constraint)."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            spread = [1, 7, 13, 1000, 99999, 123456, 777777, 8888888, 73501234, 901234567]
            # All share (a, b) = (5, 9): a cross-partition composite duplicate.
            rows = ", ".join(f"({pk}, 5, 9)" for pk in spread)
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)
            assert not _table_has_index(client, sn, "t"), \
                "no index may exist after a rejected composite CREATE UNIQUE INDEX"
            client.execute_sql("INSERT INTO t VALUES (2, 5, 9)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_create_preflight_accepts_leading_column_collision(self, client):
        """Many rows sharing the leading column `a` but with distinct trailing
        `b` are all distinct composites — CREATE UNIQUE INDEX (a, b) succeeds
        (the regression a u128 leading-column truncation would have falsely
        rejected), then enforces correctly."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            spread = [1, 1000, 99999, 8888888, 901234567]
            # All share a=5 but every b is distinct → distinct composites.
            rows = ", ".join(f"({pk}, 5, {i})" for i, pk in enumerate(spread))
            client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)
            # A genuine duplicate (a=5, b=0) is still rejected.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (10, 5, 0)", schema_name=sn)
            # A distinct trailing column is admitted.
            client.execute_sql("INSERT INTO t VALUES (11, 5, 999)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_insert_duplicate_on_different_worker_rejected(self, client):
        """An insert-time duplicate (a, b) committed on a DIFFERENT worker than
        the holder must be rejected (the distributed cross-worker check). Wide
        PK spread scatters holders across workers."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)
            spread = [1, 1000, 99999, 8888888, 901234567]
            for i, pk in enumerate(spread):
                client.execute_sql(f"INSERT INTO t VALUES ({pk}, 5, {i})", schema_name=sn)
            # Re-insert each (a, b) on a fresh PK (likely a different worker) —
            # all must be rejected.
            for i in range(len(spread)):
                with pytest.raises(gnitz.GnitzError):
                    client.execute_sql(
                        f"INSERT INTO t VALUES ({2000 + i}, 5, {i})", schema_name=sn)
            # A fully fresh composite is still admitted.
            client.execute_sql("INSERT INTO t VALUES (3000, 6, 0)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_null_distinctness(self, client):
        """A row NULL in ANY indexed column is not indexed and never collides:
        multiple (NULL, b), (a, NULL), and (NULL, NULL) rows are all admitted,
        while a fully non-null (a, b) still enforces."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)",
                schema_name=sn,
            )
            client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, NULL, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, NULL, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (3, 5, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (4, 5, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (5, NULL, NULL)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (6, NULL, NULL)", schema_name=sn)
            # A fully non-null (a, b) still enforces.
            client.execute_sql("INSERT INTO t VALUES (7, 5, 1)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (8, 5, 1)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

    def test_trivial_pk_shortcircuit(self, client):
        """UNIQUE over a table whose PK is exactly those columns (any order) is
        created without a duplicate error and admits rows the PK already
        permits — the trivial-uniqueness short-circuit."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, "
                "v BIGINT NOT NULL, PRIMARY KEY (a, b))",
                schema_name=sn,
            )
            # Duplicate v across distinct (a, b) PKs — a per-column unique would
            # choke, but the composite over the PK columns can never collide.
            client.execute_sql("INSERT INTO t VALUES (1, 2, 100)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 3, 100)", schema_name=sn)
            # UNIQUE on the PK columns in the reverse order still short-circuits.
            client.execute_sql("CREATE UNIQUE INDEX ON t(b, a)", schema_name=sn)
            # A new (a, b) the PK permits is admitted.
            client.execute_sql("INSERT INTO t VALUES (2, 3, 100)", schema_name=sn)
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_b_a"], tables=["t"])

    def test_composite_bulk_shift_accepted(self, client):
        """UPDATE t SET b = b + 1 over a dense (a, b) sequence under a composite
        UNIQUE (a, b) ships same-PK upserts whose new composite value is held by
        the previous (also-upserted) row — every occupied span resolves to a
        holder the same bundle retires. Must succeed."""
        sn = _sn()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
                "a BIGINT NOT NULL, b BIGINT NOT NULL, UNIQUE (a, b))",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO t VALUES (1, 7, 1), (2, 7, 2), (3, 7, 3)", schema_name=sn)
            client.execute_sql("UPDATE t SET b = b + 1", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            rows = sorted((r.pk, r.a, r.b) for r in client.scan(tid))
            assert rows == [(1, 7, 2), (2, 7, 3), (3, 7, 4)], rows
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])

class TestCompositeUniqueDdl:

    def _setup(self, client, sn, b_nullable=False):
        b_decl = "b BIGINT" if b_nullable else "b BIGINT NOT NULL"
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            f"a BIGINT NOT NULL, {b_decl}, c BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES "
            "(10, 1, 100, 7), (20, 1, 200, 8), (30, 2, 100, 9)",
            schema_name=sn,
        )

    def test_composite_unique_created_and_enforced(self, client):
        """Composite UNIQUE (a, b) is created (all (a, b) pairs distinct) and
        enforced end-to-end: a duplicate (a, b) is rejected, rows sharing only
        `a` or only `b` are admitted (the trailing/leading column distinguishes
        them — the regression a u128-leading-column truncation would have
        falsely merged), and the composite seek returns exactly one row."""
        sn = _sn()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            # (a, b) pairs are all distinct: (1,100), (1,200), (2,100).
            client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)

            # A duplicate (a, b) = (1, 100) is rejected by the steady-state check.
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("INSERT INTO t VALUES (40, 1, 100, 1)", schema_name=sn)

            # A row sharing only `a` (a=1, b=999) is a distinct composite → admitted;
            # likewise one sharing only `b` (a=9, b=100).
            client.execute_sql("INSERT INTO t VALUES (41, 1, 999, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (42, 9, 100, 1)", schema_name=sn)

            # The composite seek (broadcast-and-merge) returns exactly one row.
            assert _pks(client.execute_sql(
                "SELECT * FROM t WHERE a = 1 AND b = 200", schema_name=sn)) == [20]
        finally:
            _drop_all(client, sn, indices=[f"{sn}__t__idx_a_b"], tables=["t"])
