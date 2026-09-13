"""Wire- and client-level refusals: a schema that disagrees with the target, a
push aimed at something that is not writable, a name in the reserved range, and
a relation that is not there.
"""

import threading

import pytest
import gnitz
from _read import bag
from _serverproc import join_or_fail

_DDL = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"

# `t`'s own PK type, so a mismatch case varies exactly the column it names.
# A U64 key against a BIGINT table would mismatch on the PK type as well, and
# every case would then pass for a reason other than its own.
_U64_DDL = "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"

_PK = gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)


@pytest.fixture
def t(client, schema_name):
    """`t(pk BIGINT PK, val BIGINT NOT NULL)`; yields its tid."""
    client.execute_sql(_DDL, schema_name=schema_name)
    return client.resolve_table(schema_name, "t")[0]


@pytest.fixture
def t_u64(client, schema_name):
    """`t` keyed on BIGINT UNSIGNED, matching `_PK`; yields its tid."""
    client.execute_sql(_U64_DDL, schema_name=schema_name)
    return client.resolve_table(schema_name, "t")[0]


def test_a_written_qualifier_must_name_the_relation(client, schema_name):
    """A single-relation statement resolves a qualified reference against the FROM
    item's effective alias — the written one when there is one — so a qualifier
    naming something else is a rejection, not a silently ignored decoration."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=schema_name)

    # The relation's own name and a written alias both answer.
    assert len(client.execute_sql("SELECT t.a FROM t", schema_name=schema_name)[0]["rows"]) == 2
    assert len(client.execute_sql("SELECT x.a FROM t AS x", schema_name=schema_name)[0]["rows"]) == 2
    client.execute_sql("UPDATE t AS x SET a = 99 WHERE x.a = 10", schema_name=schema_name)
    assert sorted(r["a"] for r in
                  client.execute_sql("SELECT a FROM t", schema_name=schema_name)[0]["rows"]) == [20, 99]

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
            client.execute_sql(stmt, schema_name=schema_name)


# ---------------------------------------------------------------------------
# Schema mismatch — `validate_schema_match` runs on every push carrying a schema
# descriptor. Which mismatches it distinguishes, and that it names each one
# differently, is pinned in Rust; what only an end-to-end push can show is that
# the wire actually carries the descriptor to it — on the cold path AND on the
# warm path, where a cached schema once let the push skip validation entirely
# and reinterpret a U64-encoded PK as I64, corrupting at rest.
# ---------------------------------------------------------------------------

_MISMATCHED = [
    ("column count", [_PK], {"pk": 1}),
    ("pk index",     [gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
                      gnitz.ColumnDef("val", gnitz.TypeCode.I64, primary_key=True)], {"pk": 1, "val": 42}),
    ("column type",  [_PK, gnitz.ColumnDef("val", gnitz.TypeCode.F64)], {"pk": 1, "val": 3.14}),
    ("nullable",     [_PK, gnitz.ColumnDef("val", gnitz.TypeCode.I64, is_nullable=True)], {"pk": 1, "val": 42}),
    # The table's PK is BIGINT UNSIGNED; this batch claims a signed one.
    ("pk type",      [gnitz.ColumnDef("pk", gnitz.TypeCode.I64, primary_key=True),
                      gnitz.ColumnDef("val", gnitz.TypeCode.I64)], {"pk": 1, "val": 42}),
]


@pytest.mark.parametrize("warm", [False, True], ids=["cold", "warm"])
@pytest.mark.parametrize("cols,row", [c[1:] for c in _MISMATCHED],
                         ids=[c[0] for c in _MISMATCHED])
def test_push_with_a_mismatched_schema_is_rejected(client, t_u64, warm, cols, row):
    if warm:
        client.scan(t_u64)   # caches the table schema client-side
    batch = gnitz.ZSetBatch(gnitz.Schema(cols))
    batch.append(**row)
    with pytest.raises(gnitz.GnitzError):
        client.push(t_u64, batch)


# ---------------------------------------------------------------------------
# Push target writability — a view's tid lives in the same id space as base
# tables, so a raw-client push/delete addressed to a view would otherwise
# commit rows into the view's output store that its circuit never produced.
# SQL DML already refuses view targets in the binder; the raw push API is the
# only exposure, and the server is the only place the guard can live (the
# client's schema cache carries no relation kind).
# ---------------------------------------------------------------------------

_NOT_WRITABLE = "is not writable"


@pytest.fixture
def view_target(client, schema_name):
    """`t(pk, val)` with a filter view `v`; rows (1,100) in v, (2,10) not.
    Yields `(tid, t_schema, vid, v_schema)`."""
    client.execute_sql(_DDL, schema_name=schema_name)
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t WHERE val > 50", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 100), (2, 10)", schema_name=schema_name)
    tid, t_schema = client.resolve_table(schema_name, "t")
    vid, v_schema = client.resolve_table(schema_name, "v")
    return tid, t_schema, vid, v_schema


def test_writes_to_a_view_are_rejected_and_change_nothing(client, view_target):
    """Push and delete are one guard, applied on the cold path (schema block on
    the wire), the warm path (schema omitted after a scan), and the empty-batch
    arm alike — so a client bug producing an empty batch fails identically
    instead of being masked by the no-op ACK. A base table still no-op ACKs."""
    tid, t_schema, vid, v_schema = view_target

    def rows():
        return sorted((r.pk, r.val, r._weight) for r in client.scan(vid))

    batch = gnitz.ZSetBatch(v_schema)
    batch.append(pk=999, val=999)
    with pytest.raises(gnitz.GnitzError, match=_NOT_WRITABLE):
        client.push(vid, batch)
    before = rows()
    assert before == [(1, 100, 1)]

    # Warm path: the scan above cached the view schema client-side.
    with pytest.raises(gnitz.GnitzError, match=_NOT_WRITABLE):
        client.push(vid, batch)
    with pytest.raises(gnitz.GnitzError, match=_NOT_WRITABLE):
        client.delete(vid, v_schema, [1])
    with pytest.raises(gnitz.GnitzError, match=_NOT_WRITABLE):
        client.push(vid, gnitz.ZSetBatch(v_schema))
    assert rows() == before

    # The same empty batch against the base table is the ordinary no-op ACK.
    assert client.push(tid, gnitz.ZSetBatch(t_schema)) == 0


def test_an_absent_relation_is_a_miss_not_a_writability_failure(client, schema_name):
    """A genuinely absent tid reports 'not found' rather than 'not writable',
    and a name the client itself cannot resolve raises a catchable class, so a
    caller branches on absence without matching prose."""
    batch = gnitz.ZSetBatch(gnitz.Schema([_PK]))
    batch.append(pk=1)
    with pytest.raises(gnitz.GnitzError, match="not found"):
        client.push(99999999, batch)
    with pytest.raises(gnitz.GnitzError):
        client.scan(99999999)

    with pytest.raises(gnitz.GnitzNotFoundError) as ei:
        client.drop_table(schema_name, "nope")
    assert f"{schema_name}.nope" in str(ei.value)
    # Still a GnitzError, so an existing broad handler keeps working.
    assert isinstance(ei.value, gnitz.GnitzError)

    with pytest.raises(gnitz.GnitzError):
        client.drop_schema("nonexistent_schema_xyz")


class TestSchemaColumnLimit:
    """`Schema` must reject invalid column counts — this guards the u64 null
    bitmask, which a 66th column would shift out of."""

    def test_zero_columns_raises(self):
        with pytest.raises(ValueError):
            gnitz.Schema([])

    def test_exactly_65_columns_ok(self):
        """1 PK + 64 payload = 65 total fills the u64 null bitmask exactly."""
        cols = [_PK] + [gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64, is_nullable=True)
                        for i in range(64)]
        assert len(gnitz.Schema(cols).columns) == 65

    def test_66_columns_raises(self):
        cols = [_PK] + [gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64, is_nullable=True)
                        for i in range(65)]
        with pytest.raises(ValueError, match="MAX_COLUMNS"):
            gnitz.Schema(cols)


def test_duplicate_column_name_rejected(client, schema_name):
    """The SQL layer rejects a duplicate at parse time, but `create_table` is
    reachable without it — and the match is case-insensitive."""
    cols = [_PK,
            gnitz.ColumnDef("a", gnitz.TypeCode.I64),
            gnitz.ColumnDef("A", gnitz.TypeCode.I64)]
    with pytest.raises(gnitz.GnitzError, match="duplicate column name"):
        client.create_table(schema_name, "t", cols)


# ---------------------------------------------------------------------------
# Name reservation — a user identifier cannot start with `_` (reserved for the
# system prefix and the engine's own internal relation names). One rule,
# `validate_user_name`, reached from each surface that mints or names a
# relation; `create_schema` is the one with no SQL surface at all.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stmt", [
    "CREATE TABLE _secret (pk BIGINT NOT NULL PRIMARY KEY)",
    "CREATE VIEW _v AS SELECT * FROM t",
    "DROP TABLE _nope",
])
def test_sql_rejects_a_leading_underscore(client, schema_name, t, stmt):
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(stmt, schema_name=schema_name)


def test_create_schema_rejects_a_leading_underscore(client):
    with pytest.raises(gnitz.GnitzError):
        client.create_schema("_reserved")


def test_insert_duplicate_key_raises_while_upserts_are_in_flight(server, client, schema_name, t):
    """`INSERT` ships conflict mode `Error`, whose duplicate-key rejection is a
    master-side pre-flight against committed state — there is no apply-time
    backstop. It therefore keeps the exclusive table guard even though binary
    upserts to the same table hold the shared one, and its verdict must survive
    that concurrency: a duplicate `INSERT` still raises, and never degrades
    into a silent upsert."""
    sn, tid = schema_name, t
    schema = client.resolve_table(sn, "t")[1]
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)

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

    pushers = [threading.Thread(target=upserter, args=(s,), daemon=True) for s in range(1, 5)]
    for p in pushers:
        p.start()
    try:
        for _ in range(20):
            with pytest.raises(gnitz.GnitzError) as exc:
                client.execute_sql("INSERT INTO t VALUES (1, 20)", schema_name=sn)
            assert "duplicate key" in str(exc.value).lower()
    finally:
        stop.set()
        join_or_fail("a concurrent upsert hung", *pushers)
    assert not failures, f"concurrent upserts failed: {failures}"

    # The rejected INSERTs left the original row untouched, at weight 1.
    assert {k: w for k, w in bag(client.scan(tid)).items() if k[0] == 1} == {(1, 10): 1}
