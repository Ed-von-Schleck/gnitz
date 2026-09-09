"""The catalog's own object lifecycle: creating, dropping and recreating a
schema, a table, a view and an index, the CASCADE a schema drop runs, and the
name isolation two schemas give one table name.

Ids are asserted **never reused**, not monotone: non-reuse is what keeps a stale
descriptor from resolving to a new relation, while the allocator's ordering is
its own business. The verdicts the catalog refuses live in
`admissibility/test_catalog_guards.py`.
"""

import gnitz
import pytest
from _read import bag, rows, scanned
from _uid import uid as _uid

_KV = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
       gnitz.ColumnDef("val", gnitz.TypeCode.I64)]


def _push(client, tid, *pairs):
    batch = gnitz.ZSetBatch(gnitz.Schema(_KV))
    for pk, val in pairs:
        batch.append(pk=pk, val=val)
    client.push(tid, batch)


# ── Schemas ─────────────────────────────────────────────────────────────────


def test_schema_ids_are_distinct_and_never_reused(client):
    """Three live schemas take three ids, and a recreated name takes an id none
    of them held — a stale descriptor must never resolve to a new relation."""
    names = ["s" + _uid() for _ in range(3)]
    ids = [client.create_schema(n) for n in names]
    assert len(set(ids)) == 3
    for n in names:
        client.drop_schema(n)

    again = client.create_schema(names[0])
    assert again not in ids
    client.drop_schema(names[0])


def test_drop_nonempty_schema_cascades(client):
    """`drop_schema` is one atomic CASCADE bundle: it retracts every view, then
    every table (each cascading its own indexes), then the schema row — views
    before tables, and across a view-on-view chain.

    The members are *removed*, not orphaned, which only recreating the same
    qualified names can show: an orphaned row keeps the name taken.
    """
    sn = "s" + _uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE mt (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE INDEX ON mt (val)", schema_name=sn)
    client.execute_sql("CREATE VIEW v1 AS SELECT * FROM mt WHERE val > 0", schema_name=sn)
    client.execute_sql("CREATE VIEW v2 AS SELECT * FROM v1 WHERE val > 10", schema_name=sn)

    client.drop_schema(sn)

    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE mt (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE VIEW v1 AS SELECT * FROM mt WHERE val > 0", schema_name=sn)
    client.drop_schema(sn)


def test_drop_schema_restrict_external_dependent(client):
    """A member referenced from OUTSIDE the schema blocks the cascade: the drop
    errors, and it retracted nothing on the way to the row it failed on — the
    still-referenced table, its schema row and the foreign dependent all stand.
    """
    s1, s2 = "s" + _uid(), "s" + _uid()
    client.create_schema(s1)
    client.create_schema(s2)
    tid = client.create_table(s1, "t", _KV)
    # A view in s2 whose circuit scans s1.t, so dropping s1.t is blocked.
    client.create_view(s2, "v", tid, gnitz.Schema(_KV))

    with pytest.raises(gnitz.GnitzError):
        client.drop_schema(s1)

    assert client.resolve_table(s1, "t")[0] == tid
    client.resolve_table(s2, "v")

    client.drop_view(s2, "v")
    client.drop_schema(s1)
    client.drop_schema(s2)


def test_two_schemas_isolate_one_table_name(client, schema_name):
    """The same table name in two schemas addresses two relations."""
    other = "s" + _uid()
    client.create_schema(other)
    try:
        a = client.create_table(schema_name, "items", _KV)
        b = client.create_table(other, "items", _KV)
        _push(client, a, (1, 100))
        _push(client, b, (1, 200))
        assert bag(client.scan(a), "pk", "val") == {(1, 100): 1}
        assert bag(client.scan(b), "pk", "val") == {(1, 200): 1}
    finally:
        client.drop_schema(other)


# ── Tables ──────────────────────────────────────────────────────────────────


def test_create_table_carries_every_type_and_the_declared_pk(client, schema_name):
    """`create_table` accepts every `TypeCode` and hands each one back unchanged,
    with the PK at the index the caller declared rather than at column 0.

    The per-type value round trip is `value_domain/test_types.py`'s; what this
    asserts is that the catalog does not silently retype or re-key a column.
    """
    cols = [gnitz.ColumnDef("val", gnitz.TypeCode.I64),
            gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
    cols += [gnitz.ColumnDef(f"c_{tc.name.lower()}", tc, is_nullable=True)
             for tc in (gnitz.TypeCode.U8, gnitz.TypeCode.I8, gnitz.TypeCode.U16,
                        gnitz.TypeCode.I16, gnitz.TypeCode.U32, gnitz.TypeCode.I32,
                        gnitz.TypeCode.F32, gnitz.TypeCode.U64, gnitz.TypeCode.F64,
                        gnitz.TypeCode.STRING)]
    tid = client.create_table(schema_name, "orders_archive_2024", cols)

    # Resolved by a name past the 12-byte German-string inline prefix.
    resolved, schema = client.resolve_table(schema_name, "orders_archive_2024")
    assert resolved == tid
    assert [c.type_code for c in schema.columns] == [c.type_code for c in cols]
    assert schema.pk_indices == [1]


def test_a_schema_fills_the_column_cap_but_not_one_past_it(client, schema_name):
    """`MAX_COLUMNS` counts the PK and the payload together. The cap is located
    rather than named, so moving it moves this test with it."""
    def build(n):
        return [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)] + [
            gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64, is_nullable=True)
            for i in range(n - 1)]

    client.create_table(schema_name, "wide", build(gnitz.MAX_COLUMNS))
    assert len(client.resolve_table(schema_name, "wide")[1].columns) == gnitz.MAX_COLUMNS

    with pytest.raises(Exception, match="MAX_COLUMNS"):
        client.create_table(schema_name, "wider", build(gnitz.MAX_COLUMNS + 1))


def test_table_ids_are_never_reused_and_a_dropped_table_is_unscannable(client, schema_name):
    """Dropping a populated table succeeds, its id stops answering scans, and
    recreating the name takes an id the old one never held."""
    tid = client.create_table(schema_name, "t", _KV)
    _push(client, tid, (1, 10), (2, 20))
    client.drop_table(schema_name, "t")

    with pytest.raises(gnitz.GnitzError):
        client.scan(tid)

    again = client.create_table(schema_name, "t", _KV)
    assert again != tid


# ── Views ───────────────────────────────────────────────────────────────────


def test_a_passthrough_view_carries_its_sources_zset(client, schema_name):
    """A binary passthrough view holds exactly its source's Z-set — weights
    included, so a delta applied twice is a divergence and not a matching row
    set — and a retraction reaches it as a retraction.

    Also the negative: a second table's pushes do not reach it.
    """
    tid = client.create_table(schema_name, "src", _KV)
    other = client.create_table(schema_name, "other", _KV)
    vid = client.create_view(schema_name, "v", tid, gnitz.Schema(_KV))
    assert bag(client.scan(vid)) == {}

    _push(client, tid, (1, 10), (2, 20), (3, 30))
    _push(client, other, (4, 40))
    assert bag(client.scan(vid), "pk", "val") == bag(client.scan(tid), "pk", "val")
    assert bag(client.scan(vid), "pk", "val") == {(1, 10): 1, (2, 20): 1, (3, 30): 1}

    batch = gnitz.ZSetBatch(gnitz.Schema(_KV))
    batch.append(pk=2, val=20, _weight=-1)
    client.push(tid, batch)
    assert bag(client.scan(vid), "pk", "val") == {(1, 10): 1, (3, 30): 1}


def test_dropping_one_of_two_views_leaves_the_other_serving(client, schema_name):
    """Two views over one source both receive every push; dropping one retires
    exactly that id and leaves the other maintained."""
    tid = client.create_table(schema_name, "src", _KV)
    schema = gnitz.Schema(_KV)
    v1 = client.create_view(schema_name, "va", tid, schema)
    v2 = client.create_view(schema_name, "vb", tid, schema)

    _push(client, tid, (1, 10))
    assert bag(client.scan(v1), "pk", "val") == {(1, 10): 1}
    assert bag(client.scan(v2), "pk", "val") == {(1, 10): 1}

    client.drop_view(schema_name, "va")
    with pytest.raises(gnitz.GnitzError):
        client.scan(v1)
    _push(client, tid, (2, 20))
    assert bag(client.scan(v2), "pk", "val") == {(1, 10): 1, (2, 20): 1}


def test_a_dropped_views_catalog_row_is_retracted_not_shadowed(client, schema_name):
    """`VIEW_TAB` returns to its exact prior height after ten create/drop cycles.

    A scan drops ghosts and keeps only positive weights, so a weight-0 leftover
    is invisible here by construction — what this catches is a row surviving the
    DROP at *positive* weight, which accumulates one entry per cycle.
    """
    tid = client.create_table(schema_name, "src", _KV)
    schema = gnitz.Schema(_KV)
    baseline = len(list(client.scan(gnitz.VIEW_TAB)))
    for _ in range(10):
        vn = "rv" + _uid()
        client.create_view(schema_name, vn, tid, schema)
        client.drop_view(schema_name, vn)
    assert len(list(client.scan(gnitz.VIEW_TAB))) == baseline


def test_a_view_id_is_never_reused(client, schema_name):
    """A recreated view name takes a fresh id, and the fresh id is maintained."""
    tid = client.create_table(schema_name, "src", _KV)
    schema = gnitz.Schema(_KV)
    first = client.create_view(schema_name, "v", tid, schema)
    client.drop_view(schema_name, "v")
    second = client.create_view(schema_name, "v", tid, schema)
    assert second != first

    _push(client, tid, (1, 10))
    assert bag(client.scan(second), "pk", "val") == {(1, 10): 1}


# ── One DDL_TXN frame per catalog write ─────────────────────────────────────


def test_every_single_family_catalog_write_rides_one_ddl_txn_frame(client, schema_name):
    """CREATE and DROP of each of schema, table, index and view in sequence, so
    every single-family path through the frame is exercised once. The view is
    read between them: incremental maintenance has to survive the bundle."""
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql("CREATE INDEX ib ON t(b)", schema_name=schema_name)
    client.execute_sql("CREATE VIEW v AS SELECT a FROM t", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=schema_name)
    assert bag(scanned(client, schema_name, "v"), "a") == {(1,): 1}

    client.execute_sql("DROP VIEW v", schema_name=schema_name)
    client.execute_sql("DROP INDEX ib", schema_name=schema_name)
    client.execute_sql("DROP TABLE t", schema_name=schema_name)


# ── A DDL is visible to the next statement on another connection ────────────


def test_drop_index_is_seen_by_the_next_statement(client, schema_name, server):
    """A drops the index B's last plan used; B's very next statement must be
    planned against the *absence* of that index.

    A UUID equality makes the two outcomes distinguishable, because it has no
    index-free plan at all: the predicate VM has no register for a 128-bit
    column, so the WHERE only becomes servable when an index bound can consume
    it byte-exactly (the `exact` arm).

      * fresh (now empty) index list -> the planner's own
        "128-bit columns cannot be used in expressions";
      * stale index list -> an `exact` bound against a dropped index, which the
        engine answers with "No index on cols ... for table ...".

    Both are errors, so asserting on *which* error is what actually pins
    freshness — a bare `raises` would pass either way.
    """
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, wide UUID NOT NULL)",
        schema_name=schema_name)
    u1 = "11111111-1111-1111-1111-111111111111"
    u2 = "22222222-2222-2222-2222-222222222222"
    client.execute_sql(f"INSERT INTO t VALUES (1, '{u1}'), (2, '{u2}')", schema_name=schema_name)
    client.execute_sql("CREATE INDEX ix_wide ON t(wide)", schema_name=schema_name)

    with gnitz.connect(server) as b:
        # B plans against the index.
        assert bag(rows(b, schema_name, f"SELECT id FROM t WHERE wide = '{u1}'"), "id") == {(1,): 1}

        client.execute_sql("DROP INDEX ix_wide", schema_name=schema_name)

        with pytest.raises(Exception) as ei:
            rows(b, schema_name, f"SELECT id FROM t WHERE wide = '{u1}'")
        msg = str(ei.value)
        assert "128-bit columns cannot be used in expressions" in msg, (
            f"B planned against a stale index list: {msg}")
        assert "No index on cols" not in msg

        # The relation itself is unaffected, and recreating the index makes the
        # bounded read servable again at once.
        assert bag(rows(b, schema_name, "SELECT id FROM t"), "id") == {(1,): 1, (2,): 1}
        client.execute_sql("CREATE INDEX ix_wide ON t(wide)", schema_name=schema_name)
        assert bag(rows(b, schema_name, f"SELECT id FROM t WHERE wide = '{u2}'"), "id") == {(2,): 1}
