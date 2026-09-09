"""What a write is, and how writes are grouped.

Two halves. The binary push path — batch folding by PK, the empty and the
thousand-row batch, and concurrent pushes coalescing into one committed zone —
and the SQL UPDATE / DELETE dispatch, which resolves its target rows through one
access-path ladder that both statements share.
"""

import threading

import pytest
import gnitz

_KV_COLS = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]


@pytest.fixture
def kv(client, schema_name):
    """`t(pk U64 PK, val I64)` created through the binary API; `(tid, schema)`."""
    tid = client.create_table(schema_name, "t", _KV_COLS)
    return tid, gnitz.Schema(_KV_COLS)


def _rows(client, tid):
    """Every live row as `(pk, val, weight)`, sorted."""
    return sorted((r.pk, r.val, r.weight) for r in client.scan(tid))


# ── the binary push path ──────────────────────────────────────────────────────


def test_push_scan_round_trip(client, kv):
    """A fresh table is empty, and a pushed batch reads back with its values and
    at the weight `enforce_unique_pk` clamps a base row to."""
    tid, schema = kv
    assert _rows(client, tid) == []

    batch = gnitz.ZSetBatch(schema)
    for i in range(1, 11):
        batch.append(pk=i, val=i * 100)
    client.push(tid, batch)

    assert _rows(client, tid) == [(i, i * 100, 1) for i in range(1, 11)]


def test_one_push_carries_a_thousand_rows(client, kv):
    """A batch far past any single wire frame still lands whole, values intact."""
    tid, schema = kv
    batch = gnitz.ZSetBatch(schema)
    for i in range(1, 1001):
        batch.append(pk=i, val=i * 10)
    client.push(tid, batch)

    assert _rows(client, tid) == [(i, i * 10, 1) for i in range(1, 1001)]


def test_empty_push_no_desync(client, kv):
    """A push of an empty batch is a no-op push on the wire (FLAG_PUSH with no
    data rows) that the server ACKs with the "nothing written" LSN 0 — never a
    scan, whose streamed table dump would desync the one-frame push reply
    reader. A following non-empty push must return a genuine ingest LSN and
    scan must reflect exactly the real writes."""
    tid, schema = kv

    # Seed a row so a mis-routed scan would actually stream table data.
    seed = gnitz.ZSetBatch(schema)
    seed.append(pk=1, val=10)
    client.push(tid, seed)

    empty = gnitz.ZSetBatch(schema)
    assert len(empty) == 0
    assert client.push(tid, empty) == 0

    # Connection is still aligned: a real push returns a real LSN and the scan
    # sees exactly the two written rows (seed + this one), nothing leaked.
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=2, val=20)
    assert client.push(tid, batch) > 0
    assert _rows(client, tid) == [(1, 10, 1), (2, 20, 1)]


# A base table upserts by PK: within one push the last weight-positive row for a
# PK wins, and a retraction matches on the PK alone — the payload it carries is
# filler, which is what `delete` ships.
_FOLD_CASES = [
    ("last write wins",       [], [(1, 10, 1), (1, 20, 1)],                [(1, 20)]),
    ("insert then retract",   [], [(1, 10, 1), (1, 0, -1)],                []),
    ("retract then insert",   [(1, 10)], [(1, 0, -1), (1, 99, 1)],         [(1, 99)]),
    ("weight 2 is one row",   [], [(1, 10, 2)],                            [(1, 10)]),
    ("independent keys",      [(1, 10), (2, 20), (3, 30)], [(2, 99, 1)],   [(1, 10), (2, 99), (3, 30)]),
]


@pytest.mark.parametrize("seed,rows,expect",
                         [c[1:] for c in _FOLD_CASES], ids=[c[0] for c in _FOLD_CASES])
def test_push_folds_by_pk(client, kv, seed, rows, expect):
    tid, schema = kv
    if seed:
        b = gnitz.ZSetBatch(schema)
        for pk, val in seed:
            b.append(pk=pk, val=val)
        client.push(tid, b)
    b = gnitz.ZSetBatch(schema)
    for pk, val, w in rows:
        b.append(pk=pk, val=val, _weight=w)
    client.push(tid, b)
    # Weight 1 on every survivor: a base table never accumulates past it.
    assert _rows(client, tid) == [(pk, val, 1) for pk, val in expect]


def test_delete_by_pk_and_absent_pk_is_a_noop(client, kv):
    """`delete` retracts by PK alone; a key that is not there retracts nothing."""
    tid, schema = kv
    b = gnitz.ZSetBatch(schema)
    for pk, val in [(1, 10), (2, 20), (3, 30)]:
        b.append(pk=pk, val=val)
    client.push(tid, b)

    client.delete(tid, schema, [2])
    assert _rows(client, tid) == [(1, 10, 1), (3, 30, 1)]

    client.delete(tid, schema, [999])
    assert _rows(client, tid) == [(1, 10, 1), (3, 30, 1)]


# The binary push path's own string handling. `value_domain` covers these widths
# through SQL; what is varied here is the ZSetBatch encoding of each, including
# the exact 12-byte inline/heap boundary.
@pytest.mark.parametrize("label", ["", "abcdefghijkl", "this_is_a_longer_string_value", None],
                         ids=["empty", "inline-boundary-12", "heap", "null"])
def test_pushed_string_round_trips(client, schema_name, label):
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("s", gnitz.TypeCode.STRING, is_nullable=True)]
    tid = client.create_table(schema_name, "strs", cols)
    batch = gnitz.ZSetBatch(gnitz.Schema(cols))
    batch.append(pk=1, s=label)
    client.push(tid, batch)

    rows = list(client.scan(tid))
    assert [(r.pk, r.s, r.weight) for r in rows] == [(1, label, 1)]


# ── concurrent pushes coalescing into one commit ──────────────────────────────
#
# A push whose validation reads no committed state (an unconstrained base table
# under the binary API's upsert mode) holds its table lock shared, so several
# such pushes reach the committer together and fold into ONE merged batch under
# one SAL zone and one fsync. Both tests below need enough connections for that
# fold to happen at all: the committer drains only what is already queued when
# it wakes, so two connections almost never have a second request enqueued and
# nothing merges. Eight is comfortably past the threshold, and each asserts the
# fold actually occurred — a push returns its zone LSN, so fewer distinct LSNs
# than pushes means pushes shared a zone.

_CONCURRENT_CONNS = 8
# Distinct zones as a fraction of pushes. Deliberately loose: the assertion is
# that pushes coalesce at all, not how far they coalesce.
_MERGE_BAR = 0.75


def _run_threads(fn, n):
    """Run `fn(i)` on `n` threads, join them, and re-raise the first failure."""
    errors = []

    def body(i):
        try:
            fn(i)
        except Exception as e:  # noqa: BLE001 — re-raised below
            errors.append(e)

    threads = [threading.Thread(target=body, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]


def _assert_pushes_coalesced(lsns):
    seen = [lsn for per_conn in lsns for lsn in per_conn]
    assert len(set(seen)) <= _MERGE_BAR * len(seen), (
        f"{len(set(seen))} zones for {len(seen)} pushes: pushes did not coalesce"
    )


def test_concurrent_same_pk_upserts(server, client, schema_name):
    """Eight connections upserting the same PK concurrently. Every push
    succeeds and the table is left holding exactly one row, at weight 1,
    carrying one of the pushed values — which of them is the committer's
    dequeue order and is not observable from here."""
    n = 40
    tid = client.create_table(schema_name, "t", _KV_COLS)
    schema = gnitz.Schema(_KV_COLS)
    lsns = [[] for _ in range(_CONCURRENT_CONNS)]

    def worker(w):
        with gnitz.connect(server) as c:
            for i in range(n):
                batch = gnitz.ZSetBatch(schema)
                batch.append(pk=1, val=w * 1000 + i)
                lsns[w].append(c.push(tid, batch))

    _run_threads(worker, _CONCURRENT_CONNS)

    rows = _rows(client, tid)
    assert len(rows) == 1
    pk, val, weight = rows[0]
    # Positivity under contention: the accumulated weight is 1, never W.
    assert (pk, weight) == (1, 1)
    assert val in {w * 1000 + i for w in range(_CONCURRENT_CONNS) for i in range(n)}
    _assert_pushes_coalesced(lsns)


def test_concurrent_disjoint_pk_pushes_with_strings(server, client, schema_name):
    """Eight connections pushing disjoint PKs to one table, long enough that the
    committer's pooled merge batch is reused across many merges. The STRING
    values mix inline (<= 12 B) and heap payloads, and every one is asserted:
    the merge relocates each German string into the destination's own blob
    heap, and a mis-relocated pointer or a mis-reused pool entry leaves the row
    count intact while the value is wrong."""
    rounds, per_push = 30, 4
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("s", gnitz.TypeCode.STRING, is_nullable=False)]
    tid = client.create_table(schema_name, "strs", cols)
    schema = gnitz.Schema(cols)

    def value_of(pk):
        # Alternates short (inline) and long (blob heap) within one batch.
        return f"v{pk}" if pk % 2 == 0 else f"long-payload-for-row-{pk}"

    def pk_of(w, r, j):
        return (w * rounds + r) * per_push + j

    lsns = [[] for _ in range(_CONCURRENT_CONNS)]

    def worker(w):
        with gnitz.connect(server) as c:
            for r in range(rounds):
                batch = gnitz.ZSetBatch(schema)
                for j in range(per_push):
                    pk = pk_of(w, r, j)
                    batch.append(pk=pk, s=value_of(pk))
                lsns[w].append(c.push(tid, batch))

    _run_threads(worker, _CONCURRENT_CONNS)

    expected = {pk_of(w, r, j): value_of(pk_of(w, r, j))
                for w in range(_CONCURRENT_CONNS)
                for r in range(rounds)
                for j in range(per_push)}
    rows = list(client.scan(tid))
    assert {r.pk: r.s for r in rows} == expected
    assert all(r.weight == 1 for r in rows)
    _assert_pushes_coalesced(lsns)


# ── the shared UPDATE / DELETE access-path ladder ─────────────────────────────

_CREATE_T3 = (
    "CREATE TABLE t "
    "(pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL, cat_id BIGINT NOT NULL)"
)
_INSERT_3ROWS = "INSERT INTO t VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30)"
# cat_id 10 twice, so a non-unique index seek returns a key group, not one row.
_INSERT_DUP_CAT = "INSERT INTO t VALUES (1, 100, 10), (2, 200, 10), (3, 300, 99)"


@pytest.fixture
def t3(client, schema_name):
    """`t(pk, val, cat_id)` holding `_INSERT_3ROWS`; yields its tid."""
    client.execute_sql(_CREATE_T3, schema_name=schema_name)
    client.execute_sql(_INSERT_3ROWS, schema_name=schema_name)
    return client.resolve_table(schema_name, "t")[0]


# UPDATE and DELETE resolve their targets through one ladder — PK equality, a PK
# set, a unique-index seek, a non-unique index falling through to a scan, and a
# bare predicate scan. Both statements are driven over the same table and the
# same cases, because the rung each takes is a property of the WHERE, not of the
# verb. Each case is (setup DDL, seed INSERT, statement, rows affected, surviving
# {pk: val}).
_LADDER_CASES = [
    ("update-pk-eq", None, _INSERT_3ROWS,
     "UPDATE t SET val = 99 WHERE pk = 1", 1, {1: 99, 2: 200, 3: 300}),
    ("update-pk-eq-expr", None, _INSERT_3ROWS,
     "UPDATE t SET val = val * 2 WHERE pk = 2", 1, {1: 100, 2: 400, 3: 300}),
    ("update-pk-set", None, _INSERT_3ROWS,
     "UPDATE t SET val = 0 WHERE pk IN (1, 3)", 2, {1: 0, 2: 200, 3: 0}),
    # A repeat counts once and an absent key contributes nothing.
    ("update-pk-set-repeated-absent", None, _INSERT_3ROWS,
     "UPDATE t SET val = val + 1 WHERE pk IN (2, 2, 999)", 1, {1: 100, 2: 201, 3: 300}),
    # The set still gathers; the rest of the WHERE post-filters what it gathered.
    ("update-pk-set-with-residual", None, _INSERT_3ROWS,
     "UPDATE t SET val = 0 WHERE pk IN (1, 3) AND val > 150", 1, {1: 100, 2: 200, 3: 0}),
    # `Not(...)` over the membership test never matches the gather; it is residual.
    ("update-pk-not-in-scan", None, _INSERT_3ROWS,
     "UPDATE t SET val = 0 WHERE pk NOT IN (2)", 2, {1: 0, 2: 200, 3: 0}),
    ("update-unique-index", "CREATE UNIQUE INDEX ON t(cat_id)", _INSERT_3ROWS,
     "UPDATE t SET val = 0 WHERE cat_id = 20", 1, {1: 100, 2: 0, 3: 300}),
    ("update-nonunique-index-scan", "CREATE INDEX ON t(cat_id)", _INSERT_DUP_CAT,
     "UPDATE t SET val = 0 WHERE cat_id = 10", 2, {1: 0, 2: 0, 3: 300}),
    ("update-predicate-scan", None, _INSERT_3ROWS,
     "UPDATE t SET val = 0 WHERE val > 150", 2, {1: 100, 2: 0, 3: 0}),
    ("update-no-where", None, _INSERT_3ROWS,
     "UPDATE t SET val = 42", 3, {1: 42, 2: 42, 3: 42}),
    ("update-matches-nothing", None, _INSERT_3ROWS,
     "UPDATE t SET val = 99 WHERE pk = 9999", 0, {1: 100, 2: 200, 3: 300}),

    ("delete-pk-eq", None, _INSERT_3ROWS,
     "DELETE FROM t WHERE pk = 2", 1, {1: 100, 3: 300}),
    ("delete-pk-set", None, _INSERT_3ROWS,
     "DELETE FROM t WHERE pk IN (1, 3)", 2, {2: 200}),
    # A one-element list is the equality it spells.
    ("delete-pk-set-of-one", None, _INSERT_3ROWS,
     "DELETE FROM t WHERE pk IN (2)", 1, {1: 100, 3: 300}),
    ("delete-unique-index", "CREATE UNIQUE INDEX ON t(cat_id)", _INSERT_3ROWS,
     "DELETE FROM t WHERE cat_id = 20", 1, {1: 100, 3: 300}),
    ("delete-nonunique-index-scan", "CREATE INDEX ON t(cat_id)", _INSERT_DUP_CAT,
     "DELETE FROM t WHERE cat_id = 10", 2, {3: 300}),
    ("delete-predicate-scan", None, _INSERT_3ROWS,
     "DELETE FROM t WHERE val > 150", 2, {1: 100}),
    # A non-PK IN list is a residual predicate over the scan, not a gather.
    ("delete-non-pk-set-scan", None, _INSERT_3ROWS,
     "DELETE FROM t WHERE val IN (100, 300, 555)", 2, {2: 200}),
    ("delete-no-where", None, _INSERT_3ROWS,
     "DELETE FROM t", 3, {}),
]


@pytest.mark.parametrize("ddl,seed,stmt,affected,survivors",
                         [c[1:] for c in _LADDER_CASES],
                         ids=[c[0] for c in _LADDER_CASES])
def test_update_delete_access_path_ladder(client, schema_name, ddl, seed, stmt,
                                          affected, survivors):
    client.execute_sql(_CREATE_T3, schema_name=schema_name)
    client.execute_sql(seed, schema_name=schema_name)
    if ddl:
        client.execute_sql(ddl, schema_name=schema_name)
    tid = client.resolve_table(schema_name, "t")[0]

    res = client.execute_sql(stmt, schema_name=schema_name)
    assert res[0]["type"] == "RowsAffected"
    assert res[0]["count"] == affected

    rows = list(client.scan(tid))
    assert {r.pk: r.val for r in rows} == survivors
    # An UPDATE is a retract plus a re-insert; a survivor left at weight 2 would
    # mean the retraction never landed.
    assert all(r.weight == 1 for r in rows)


def test_pk_set_gather_is_shared_with_select(client, t3, schema_name):
    """The `pk IN (…)` gather DML uses is the one SELECT uses: the same dedup of
    a repeated key, the same silence on an absent one, feeding the shared
    projection/LIMIT tail."""
    res = client.execute_sql("SELECT * FROM t WHERE pk IN (3, 3, 1, 999)",
                             schema_name=schema_name)
    assert res[0]["type"] == "Rows"
    assert sorted((r.pk, r.val) for r in res[0]["rows"]) == [(1, 100), (3, 300)]

    res = client.execute_sql("SELECT pk FROM t WHERE pk IN (1, 2, 3) AND val > 150",
                             schema_name=schema_name)
    assert sorted(r.pk for r in res[0]["rows"]) == [2, 3]

    res = client.execute_sql("SELECT pk, val FROM t WHERE pk IN (1, 2, 3) LIMIT 2",
                             schema_name=schema_name)
    assert len(list(res[0]["rows"])) == 2


def test_uuid_pk_in_update_select_delete_parity(client, schema_name):
    """UUID string keys route through the same IN gather as integer ones, for
    all three verbs."""
    ua = '550e8400-e29b-41d4-a716-446655440000'
    ub = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
    uc = '01935000-0000-7000-8000-000000000001'
    client.execute_sql("CREATE TABLE t (pk UUID NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                       schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"('{u}', {v})" for u, v in [(ua, 1), (ub, 2), (uc, 3)]),
        schema_name=schema_name)

    res = client.execute_sql(f"UPDATE t SET v = 99 WHERE pk IN ('{ua}', '{uc}')",
                             schema_name=schema_name)
    assert res[0]["count"] == 2

    res = client.execute_sql(f"SELECT * FROM t WHERE pk IN ('{ua}', '{ub}')",
                             schema_name=schema_name)
    assert sorted(r.v for r in res[0]["rows"]) == [2, 99]

    # A repeated key deletes the one row it names, once.
    res = client.execute_sql(f"DELETE FROM t WHERE pk IN ('{ub}', '{ub}')",
                             schema_name=schema_name)
    assert res[0]["count"] == 1


def test_delete_retracts_the_secondary_index_entry(client, schema_name):
    """DELETE retracts the source row, and the index seek that found it must
    then find nothing — with the index itself still resolving, which is what
    separates "no rows match" from "the index is gone"."""
    client.execute_sql(_CREATE_T3, schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 100, 10)", schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON t(cat_id)", schema_name=schema_name)
    tid = client.resolve_table(schema_name, "t")[0]

    before = client.seek_by_index(tid, [2], [10])
    assert before.schema is not None and len(before.pks) == 1

    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=schema_name)

    after = client.seek_by_index(tid, [2], [10])
    assert after.schema is not None, "the index itself must still resolve"
    assert len(after.pks) == 0


def test_update_pk_column_rejects(client, t3, schema_name):
    """The PK is the row's identity; an UPDATE may not move it."""
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("UPDATE t SET pk = 999 WHERE pk = 1", schema_name=schema_name)


def test_update_join_rejects(client, t3, schema_name):
    """`Update.table` is a `TableWithJoins`, so a join parses. Honoring only
    its relation would drop the join and update every row of the target."""
    client.execute_sql("CREATE TABLE o (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT)",
                       schema_name=schema_name)
    client.execute_sql("INSERT INTO o VALUES (1, 7)", schema_name=schema_name)

    with pytest.raises(gnitz.GnitzError) as e:
        client.execute_sql("UPDATE t JOIN o ON t.pk = o.pk SET val = 1",
                           schema_name=schema_name)
    assert "exactly one simple FROM table" in str(e.value)

    assert {r.val for r in client.scan(t3)} == {100, 200, 300}
