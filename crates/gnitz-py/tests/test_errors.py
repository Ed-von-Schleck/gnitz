import threading

import pytest
import gnitz
from _uid import uid as _uid




def _cleanup(client, sn, tables=()):
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def test_a_written_qualifier_must_name_the_relation(client):
    """A single-relation statement resolves a qualified reference against the FROM
    item's effective alias — the written one when there is one — so a qualifier
    naming something else is a rejection, not a silently ignored decoration."""
    sn = "q" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)", schema_name=sn
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=sn)

        # The relation's own name and a written alias both answer.
        assert len(client.execute_sql("SELECT t.a FROM t", schema_name=sn)[0]["rows"]) == 2
        assert len(client.execute_sql("SELECT x.a FROM t AS x", schema_name=sn)[0]["rows"]) == 2
        client.execute_sql("UPDATE t AS x SET a = 99 WHERE x.a = 10", schema_name=sn)
        assert sorted(r["a"] for r in client.execute_sql("SELECT a FROM t", schema_name=sn)[0]["rows"]) == [20, 99]

        # A qualifier naming no relation in scope, on each surface.
        for stmt in [
            "SELECT b.a FROM t",
            "SELECT a FROM t WHERE b.a = 1",
            # The written alias displaces the table name.
            "SELECT t.a FROM t AS x",
            "UPDATE t AS x SET a = 1 WHERE t.a = 1",
            "DELETE FROM t WHERE nope.a = 1",
            "CREATE VIEW v AS SELECT b.a FROM t",
        ]:
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(stmt, schema_name=sn)
    finally:
        _cleanup(client, sn, ["t"])


def test_push_to_nonexistent_target(client):
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10)
    with pytest.raises(gnitz.GnitzError):
        client.push(99999, batch)


def test_scan_nonexistent_target(client):
    with pytest.raises(gnitz.GnitzError):
        client.scan(99999)


# ---------------------------------------------------------------------------
# Schema mismatch — validate_schema_match in executor.rs is called on every
# buffered push that includes a schema descriptor.  No test previously sent
# a deliberately wrong schema.
# ---------------------------------------------------------------------------

class TestSchemaMismatch:
    """Push batches whose schema disagrees with the stored table schema."""

    def _make_table(self, client, sn):
        """Two-column table: pk BIGINT PK, val BIGINT."""
        client.create_schema(sn)
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        tid, _ = client.resolve_table(sn, "t")
        return tid

    def test_wrong_column_count(self, client):
        """Batch with one column fewer than the table schema must be rejected."""
        sn = "err" + _uid()
        try:
            tid = self._make_table(client, sn)
            wrong_cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
            wrong_schema = gnitz.Schema(wrong_cols)
            batch = gnitz.ZSetBatch(wrong_schema)
            batch.append(pk=1)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, batch)
        finally:
            _cleanup(client, sn, tables=["t"])

    def test_wrong_pk_index(self, client):
        """Batch keyed on column 1 when the table is keyed on column 0 must be rejected."""
        sn = "err" + _uid()
        try:
            tid = self._make_table(client, sn)
            # Same types, but primary_key flag swapped: pk_index becomes 1.
            wrong_cols = [
                gnitz.ColumnDef("pk",  gnitz.TypeCode.U64),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64, primary_key=True),
            ]
            wrong_schema = gnitz.Schema(wrong_cols)
            batch = gnitz.ZSetBatch(wrong_schema)
            batch.append(pk=1, val=42)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, batch)
        finally:
            _cleanup(client, sn, tables=["t"])

    def test_wrong_column_type(self, client):
        """Batch where val is F64 instead of I64 must be rejected."""
        sn = "err" + _uid()
        try:
            tid = self._make_table(client, sn)
            wrong_cols = [
                gnitz.ColumnDef("pk",  gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.F64),   # wrong: table has I64
            ]
            wrong_schema = gnitz.Schema(wrong_cols)
            batch = gnitz.ZSetBatch(wrong_schema)
            batch.append(pk=1, val=3.14)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, batch)
        finally:
            _cleanup(client, sn, tables=["t"])

    def test_wrong_nullable(self, client):
        """Batch that marks a NOT NULL column as nullable must be rejected."""
        sn = "err" + _uid()
        try:
            tid = self._make_table(client, sn)  # val BIGINT NOT NULL (nullable=0)
            wrong_cols = [
                gnitz.ColumnDef("pk",  gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64, is_nullable=True),  # wrong
            ]
            wrong_schema = gnitz.Schema(wrong_cols)
            batch = gnitz.ZSetBatch(wrong_schema)
            batch.append(pk=1, val=42)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, batch)
        finally:
            _cleanup(client, sn, tables=["t"])

    def test_wrong_pk_type_rejected_when_cache_warm(self, client):
        """A PK-type mismatch must be rejected whether or not the table schema
        is cached. Warming the cache (via a scan of the table) used to route
        the push through the schema-less warm path, where the server skipped
        validate_schema_match and silently reinterpreted the U64-encoded PK as
        I64 — corrupting at rest. The push must fail identically to the cold
        path (cf. test_wrong_column_type)."""
        sn = "err" + _uid()
        try:
            tid = self._make_table(client, sn)  # pk BIGINT (stored I64)
            # Warm the client's schema cache for this table.
            client.scan(tid)
            wrong_cols = [
                gnitz.ColumnDef("pk",  gnitz.TypeCode.U64, primary_key=True),  # wrong: table is I64
                gnitz.ColumnDef("val", gnitz.TypeCode.I64),
            ]
            wrong_schema = gnitz.Schema(wrong_cols)
            batch = gnitz.ZSetBatch(wrong_schema)
            batch.append(pk=1, val=42)
            with pytest.raises(gnitz.GnitzError):
                client.push(tid, batch)
        finally:
            _cleanup(client, sn, tables=["t"])


# ---------------------------------------------------------------------------
# Push target writability — a view's tid lives in the same id space as base
# tables, so a raw-client push/delete addressed to a view would otherwise
# commit rows into the view's output store that its circuit never produced.
# SQL DML already refuses view targets in the binder; the raw push API is the
# only exposure, and the server is the only place the guard can live (the
# client's schema cache carries no relation kind).
# ---------------------------------------------------------------------------

_NOT_WRITABLE = "is not writable: pushes must target a base table"


class TestPushViewTargetRejected:
    def _make_table_and_view(self, client, sn):
        """t(pk, val) with a filter view v; rows (1,100) in v, (2,10) not."""
        client.create_schema(sn)
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM t WHERE val > 50",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 100), (2, 10)", schema_name=sn)
        tid, t_schema = client.resolve_table(sn, "t")
        vid, v_schema = client.resolve_table(sn, "v")
        return tid, t_schema, vid, v_schema

    def _teardown(self, client, sn):
        for sql in ["DROP VIEW v", "DROP TABLE t"]:
            try:
                client.execute_sql(sql, schema_name=sn)
            except Exception:
                pass
        try:
            client.drop_schema(sn)
        except Exception:
            pass

    def _view_rows(self, client, vid):
        return sorted((r.pk, r.val, r.weight) for r in client.scan(vid))

    def test_push_to_view_rejected_content_unchanged(self, client):
        """A non-empty push to a view tid errors on both the cold path (schema
        block on the wire) and the warm path (schema-omitted after a scan),
        and the view's content is untouched either way."""
        sn = "err" + _uid()
        try:
            _, _, vid, v_schema = self._make_table_and_view(client, sn)
            batch = gnitz.ZSetBatch(v_schema)
            batch.append(pk=999, val=999)
            # Cold path: no prior scan, the push frame carries a schema block.
            with pytest.raises(gnitz.GnitzError, match=_NOT_WRITABLE):
                client.push(vid, batch)
            before = self._view_rows(client, vid)
            assert before == [(1, 100, 1)]
            # Warm path: the scan above cached the view schema client-side.
            with pytest.raises(gnitz.GnitzError, match=_NOT_WRITABLE):
                client.push(vid, batch)
            assert self._view_rows(client, vid) == before
        finally:
            self._teardown(client, sn)

    def test_delete_to_view_rejected_content_unchanged(self, client):
        """delete is a plain negative-weight push — same rejection."""
        sn = "err" + _uid()
        try:
            _, _, vid, v_schema = self._make_table_and_view(client, sn)
            before = self._view_rows(client, vid)
            with pytest.raises(gnitz.GnitzError, match=_NOT_WRITABLE):
                client.delete(vid, v_schema, [1])
            assert self._view_rows(client, vid) == before
        finally:
            self._teardown(client, sn)

    def test_empty_push_to_view_rejected(self, client):
        """The empty-push arm applies the same gate: a client bug that happens
        to produce an empty batch fails identically instead of being masked by
        the no-op ACK."""
        sn = "err" + _uid()
        try:
            _, _, vid, v_schema = self._make_table_and_view(client, sn)
            empty = gnitz.ZSetBatch(v_schema)
            with pytest.raises(gnitz.GnitzError, match=_NOT_WRITABLE):
                client.push(vid, empty)
        finally:
            self._teardown(client, sn)

    def test_empty_push_to_base_table_still_noop_acks(self, client):
        sn = "err" + _uid()
        try:
            tid, t_schema, _, _ = self._make_table_and_view(client, sn)
            empty = gnitz.ZSetBatch(t_schema)
            assert client.push(tid, empty) == 0
        finally:
            self._teardown(client, sn)

    def test_push_to_absent_tid_still_not_found(self, client):
        """The consolidated gate preserves the existence-check semantics: a
        genuinely absent tid reports 'not found', not 'not writable'."""
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
        batch = gnitz.ZSetBatch(gnitz.Schema(cols))
        batch.append(pk=1)
        with pytest.raises(gnitz.GnitzError, match="not found"):
            client.push(99999999, batch)

    def test_absent_relation_raises_the_not_found_class(self, client):
        """A relation the client itself could not resolve raises a catchable
        class, so a caller branches on absence without matching prose."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            with pytest.raises(gnitz.GnitzNotFoundError) as ei:
                client.drop_table(sn, "nope")
            assert f"{sn}.nope" in str(ei.value)
            # Still a GnitzError, so an existing broad handler keeps working.
            assert isinstance(ei.value, gnitz.GnitzError)
        finally:
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Schema construction limits
# ---------------------------------------------------------------------------

class TestSchemaColumnLimit:
    """Schema must reject invalid column counts — guards the u64 null bitmask."""

    def test_zero_columns_raises(self):
        with pytest.raises(ValueError):
            gnitz.Schema([])

    def test_exactly_65_columns_ok(self):
        """1 PK + 64 payload = 65 total fills the u64 null bitmask exactly."""
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
        cols += [gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64, is_nullable=True)
                 for i in range(64)]
        s = gnitz.Schema(cols)
        assert len(s.columns) == 65

    def test_66_columns_raises(self):
        """1 PK + 65 payload shifts by 64 bits — must be rejected."""
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
        cols += [gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64, is_nullable=True)
                 for i in range(65)]
        with pytest.raises(ValueError, match="MAX_COLUMNS"):
            gnitz.Schema(cols)

    def test_100_columns_raises(self):
        cols = [gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64) for i in range(100)]
        with pytest.raises(ValueError):
            gnitz.Schema(cols)


class TestCreateTableColumnNames:
    def test_duplicate_column_name_rejected(self, client):
        """The SQL layer rejects a duplicate at parse time, but `create_table` is
        reachable without it."""
        sn = "err" + _uid()
        try:
            client.create_schema(sn)
            cols = [
                gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("a", gnitz.TypeCode.I64),
                gnitz.ColumnDef("A", gnitz.TypeCode.I64),
            ]
            with pytest.raises(gnitz.GnitzError, match="duplicate column name"):
                client.create_table(sn, "t", cols)
        finally:
            _cleanup(client, sn)


# ---------------------------------------------------------------------------
# Name reservation — user identifiers cannot start with `_` (reserved for the
# system prefix and for the engine's own internal relation names). The SQL
# planner enforces this for CREATE/DROP TABLE and VIEW; the client enforces it
# for create_schema (no SQL surface). This is the single production gate.
# ---------------------------------------------------------------------------

class TestNameReservation:
    def test_create_table_leading_underscore_rejected(self, client):
        sn = "err" + _uid()
        try:
            client.create_schema(sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE TABLE _secret (pk BIGINT NOT NULL PRIMARY KEY)",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn)

    def test_create_view_leading_underscore_rejected(self, client):
        sn = "err" + _uid()
        try:
            client.create_schema(sn)
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE VIEW _v AS SELECT * FROM t", schema_name=sn)
        finally:
            _cleanup(client, sn, tables=["t"])

    def test_drop_table_leading_underscore_rejected(self, client):
        sn = "err" + _uid()
        try:
            client.create_schema(sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("DROP TABLE _nope", schema_name=sn)
        finally:
            _cleanup(client, sn)

    def test_drop_view_leading_underscore_rejected(self, client):
        sn = "err" + _uid()
        try:
            client.create_schema(sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("DROP VIEW _nope", schema_name=sn)
        finally:
            _cleanup(client, sn)

    def test_create_schema_leading_underscore_rejected(self, client):
        with pytest.raises(gnitz.GnitzError):
            client.create_schema("_reserved")


# ---------------------------------------------------------------------------
# Duplicate-key rejection under concurrent binary upserts
# ---------------------------------------------------------------------------


def test_insert_duplicate_key_raises_while_upserts_are_in_flight(server):
    """`INSERT` ships conflict mode `Error`, whose duplicate-key rejection is a
    master-side pre-flight against committed state — there is no apply-time
    backstop. It therefore keeps the exclusive table guard even though binary
    upserts to the same table hold the shared one, and its verdict must survive
    that concurrency: a duplicate `INSERT` still raises, and never degrades
    into a silent upsert."""
    sn = "err" + _uid()
    with gnitz.connect(server) as setup:
        setup.create_schema(sn)
        setup.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        tid, schema = setup.resolve_table(sn, "t")
        setup.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)

    stop = threading.Event()
    failures = []

    def upserter(seed):
        # Disjoint PKs, so nothing here can be the source of a duplicate.
        try:
            with gnitz.connect(server) as c:
                i = 0
                while not stop.is_set():
                    batch = gnitz.ZSetBatch(schema)
                    batch.append(pk=1000 * seed + (i % 64) + 1, val=i)
                    c.push(tid, batch)
                    i += 1
        except Exception as e:  # noqa: BLE001 — surfaced after the join
            failures.append(e)

    pushers = [threading.Thread(target=upserter, args=(s,)) for s in range(1, 5)]
    for p in pushers:
        p.start()
    try:
        with gnitz.connect(server) as c:
            for _ in range(20):
                with pytest.raises(gnitz.GnitzError) as exc:
                    c.execute_sql("INSERT INTO t VALUES (1, 20)", schema_name=sn)
                assert "duplicate key" in str(exc.value).lower()
    finally:
        stop.set()
        for p in pushers:
            p.join()
    assert not failures, f"concurrent upserts failed: {failures}"

    with gnitz.connect(server) as c:
        rows = {row.pk: row.val for row in c.scan(tid)}
        # The rejected INSERTs left the original row untouched.
        assert rows[1] == 10
        _cleanup(c, sn, tables=["t"])
