"""SERIAL / BIGSERIAL / SMALLSERIAL auto-increment primary keys.

The client draws monotone ids from a per-table durable sequence on the master
(range-cached per connection), stamps them into the PK before pushing, and
answers INSERT ... RETURNING locally. Users may not supply a value for a SERIAL
column.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/mutation_pattern/test_serial.py -v
"""

import threading

import pytest
import gnitz
from _serverproc import HANG_TIMEOUT, START_TIMEOUT

_CREATE = "CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)"


@pytest.fixture
def serial_t(client, schema_name):
    """`t(id SERIAL PK, name TEXT)`; yields its tid."""
    client.execute_sql(_CREATE, schema_name=schema_name)
    return client.resolve_table(schema_name, "t")[0]


def _returned_rows(result):
    """The row list from an execute_sql Rows result (e.g. RETURNING / SELECT)."""
    r = next(x for x in result if x["type"] == "Rows")
    return list(r["rows"])


def _names(n, prefix="n"):
    """A `VALUES ('n0'), ('n1'), …` list of `n` rows."""
    return ",".join(f"('{prefix}{i}')" for i in range(n))


# ── assignment across every SERIAL width ─────────────────────────────────────


@pytest.mark.parametrize("ty", ["SERIAL", "BIGSERIAL", "SMALLSERIAL",
                                "SERIAL2", "SERIAL4", "SERIAL8"])
def test_serial_widths_assign_from_one(client, schema_name, ty):
    """Every spelling and width draws from the same sequence, starting at 1."""
    client.execute_sql(f"CREATE TABLE t (id {ty} PRIMARY KEY, name TEXT)",
                       schema_name=schema_name)
    client.execute_sql("INSERT INTO t (name) VALUES ('a'), ('b'), ('c')",
                       schema_name=schema_name)
    tid = client.resolve_table(schema_name, "t")[0]
    assert sorted((r.id, r.name) for r in client.scan(tid)) == [(1, "a"), (2, "b"), (3, "c")]


def test_serial_midcolumn_payload_indexing(client, schema_name):
    """SERIAL need not be column 0. With payload columns on both sides of the PK,
    each VALUES element must land in the right column — the single-PK payload_idx
    closed form (`ci if ci < pk_index else ci - 1`). A `row[ci]`-style bug would
    misplace or OOB here, but is invisible in the (id SERIAL PK, one payload) shape."""
    client.execute_sql("CREATE TABLE t (a BIGINT, id SERIAL PRIMARY KEY, b TEXT)",
                       schema_name=schema_name)
    # Explicit non-SERIAL column list (a, b) — omits the middle SERIAL column.
    client.execute_sql("INSERT INTO t (a, b) VALUES (10, 'x')", schema_name=schema_name)
    # Bare positional INSERT supplies the non-SERIAL columns in schema order.
    client.execute_sql("INSERT INTO t VALUES (20, 'y')", schema_name=schema_name)
    tid = client.resolve_table(schema_name, "t")[0]
    assert sorted((r.id, r.a, r.b) for r in client.scan(tid)) == [(1, 10, "x"), (2, 20, "y")]


def test_serial_null_payload(client, schema_name, serial_t):
    """A NULL payload value round-trips through the null bitmap while the SERIAL
    PK is still auto-assigned."""
    client.execute_sql("INSERT INTO t (name) VALUES ('a'), (NULL), ('c')",
                       schema_name=schema_name)
    assert sorted((r.id, r.name) for r in client.scan(serial_t)) == [
        (1, "a"), (2, None), (3, "c")]


def test_serial_no_reuse_after_delete(client, schema_name, serial_t):
    """The sequence never rewinds: a deleted id is not reissued on the next INSERT."""
    client.execute_sql("INSERT INTO t (name) VALUES ('a'), ('b')", schema_name=schema_name)
    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=schema_name)
    client.execute_sql("INSERT INTO t (name) VALUES ('c')", schema_name=schema_name)
    assert sorted(r.id for r in client.scan(serial_t)) == [1, 3]


def test_multiworker_scatter_all_ids_once(client, schema_name, serial_t):
    """Insert many rows (crossing hash partitions via `mix`) and confirm a full
    scan returns every id exactly once — the correctness property behind the
    no-hot-partition scatter."""
    client.execute_sql(f"INSERT INTO t (name) VALUES {_names(200)}", schema_name=schema_name)
    assert sorted(r.id for r in client.scan(serial_t)) == list(range(1, 201))


# ── the per-connection range cache ───────────────────────────────────────────


def test_ids_stay_monotone_across_a_range_cache_refill(client, schema_name, serial_t):
    """`reserve_serial_ids` serves one statement from the cached range and
    refills when it cannot, abandoning whatever tail the old range still held.
    Three statements of 30 exhaust the 64-id range on the third, so the ids are
    monotone with a gap — never contiguous, and never reissued.

    One 90-row statement would not reach this: a reservation takes
    `count.max(SERIAL_RANGE_SIZE)` in a single durable advance, so it is
    contiguous by construction and crosses no boundary.
    """
    ids = []
    for chunk in range(3):
        res = client.execute_sql(
            f"INSERT INTO t (name) VALUES {_names(30, prefix=f'c{chunk}_')} RETURNING id",
            schema_name=schema_name)
        got = [r.id for r in _returned_rows(res)]
        assert got == sorted(got), "ids within one statement must ascend"
        ids += got

    assert len(ids) == 90
    assert len(set(ids)) == 90, "an id was issued twice across the refill"
    assert ids == sorted(ids), "the sequence rewound across the refill"
    assert sorted(r.id for r in client.scan(serial_t)) == sorted(ids)


# ── RETURNING ────────────────────────────────────────────────────────────────


def test_returning_projections(client, schema_name, serial_t):
    """`RETURNING id`, `RETURNING *` and `RETURNING id, name` each answer from
    the ids the client just stamped, before the write is acknowledged."""
    res = client.execute_sql("INSERT INTO t (name) VALUES ('a'), ('b') RETURNING id",
                             schema_name=schema_name)
    assert sorted(r.id for r in _returned_rows(res)) == [1, 2]

    res = client.execute_sql("INSERT INTO t (name) VALUES ('c') RETURNING *",
                             schema_name=schema_name)
    assert [(r.id, r.name) for r in _returned_rows(res)] == [(3, "c")]

    res = client.execute_sql("INSERT INTO t (name) VALUES ('d'), ('e') RETURNING id, name",
                             schema_name=schema_name)
    assert sorted((r.id, r.name) for r in _returned_rows(res)) == [(4, "d"), (5, "e")]

    # A wildcard modifier reduces the returned column set exactly as it reduces
    # a SELECT's: RETURNING expands through the same helper.
    res = client.execute_sql("INSERT INTO t (name) VALUES ('f') RETURNING * EXCEPT (name)",
                             schema_name=schema_name)
    row = _returned_rows(res)[0]
    assert row.id == 6 and not hasattr(row, "name")


def test_returning_rejections(client, schema_name, serial_t):
    """An expression, a non-PK projection, an UPDATE, and a combination with ON
    CONFLICT are each out of scope for the locally-answered RETURNING."""
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("INSERT INTO t (name) VALUES ('a') RETURNING id + 1",
                           schema_name=schema_name)
    # A non-PK-only projection is rejected (a Z-set batch needs an identity).
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("INSERT INTO t (name) VALUES ('a') RETURNING name",
                           schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError) as exc:
        client.execute_sql(
            "INSERT INTO t (name) VALUES ('a') ON CONFLICT (id) DO NOTHING RETURNING id",
            schema_name=schema_name)
    assert "conflict" in str(exc.value).lower()

    client.execute_sql("INSERT INTO t (name) VALUES ('a')", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("UPDATE t SET name = 'z' RETURNING id", schema_name=schema_name)


# ── refusals ─────────────────────────────────────────────────────────────────


def test_cannot_supply_serial_value(client, schema_name, serial_t):
    """The sequence owns the column: neither an INSERT nor an UPDATE may write it."""
    # Naming the SERIAL column in the column list is a targeted error.
    with pytest.raises(gnitz.GnitzError) as exc:
        client.execute_sql("INSERT INTO t (id, name) VALUES (5, 'x')", schema_name=schema_name)
    assert "serial" in str(exc.value).lower()
    # A bare positional INSERT supplies a value for the auto-assigned PK too.
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("INSERT INTO t VALUES (5, 'x')", schema_name=schema_name)
    # UPDATE of the (SERIAL) primary key is rejected by the PK-write guard.
    client.execute_sql("INSERT INTO t (name) VALUES ('a')", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("UPDATE t SET id = 9", schema_name=schema_name)


@pytest.mark.parametrize("ddl", [
    "CREATE TABLE a (id SERIAL, name TEXT PRIMARY KEY)",          # SERIAL is not the PK
    "CREATE TABLE b (id SERIAL, x BIGINT, PRIMARY KEY (id, x))",  # inside a compound PK
    "CREATE TABLE c (id SERIAL PRIMARY KEY, id2 SERIAL)",         # two SERIAL columns
], ids=["not-the-pk", "compound-pk", "two-serials"])
def test_serial_must_be_lone_pk(client, schema_name, ddl):
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(ddl, schema_name=schema_name)


def test_smallserial_overflow(client, schema_name):
    """SMALLSERIAL exhausts at i16::MAX (32767); the next id raises. The rows
    have to be inserted — no surface advances the sequence without them."""
    client.execute_sql("CREATE TABLE t (id SMALLSERIAL PRIMARY KEY, name TEXT)",
                       schema_name=schema_name)
    # Fill exactly up to i16::MAX in one statement (ids 1..32767, all fit).
    client.execute_sql(f"INSERT INTO t (name) VALUES {','.join(['(\'n\')'] * 32767)}",
                       schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError) as exc:
        client.execute_sql("INSERT INTO t (name) VALUES ('overflow')", schema_name=schema_name)
    assert "exhausted" in str(exc.value).lower()


# ── across connections and across a restart ──────────────────────────────────


def test_serial_recognized_cross_connection(client, schema_name, serial_t, server):
    """A connection that only fetched the schema — never saw the CREATE — must
    still recognize the SERIAL PK (via the is_serial marker round-tripped through
    COL_TAB) and auto-assign it, rather than treating it as a user-supplied PK."""
    with gnitz.connect(server) as c2:
        # c2's schema for `t` comes purely from COL_TAB, not from the CREATE.
        c2.execute_sql("INSERT INTO t (name) VALUES ('a'), ('b')", schema_name=schema_name)
        tid, _ = c2.resolve_table(schema_name, "t")
        assert sorted((r.id, r.name) for r in c2.scan(tid)) == [(1, "a"), (2, "b")]
        # And a bare positional value is still rejected on the fresh connection.
        with pytest.raises(gnitz.GnitzError):
            c2.execute_sql("INSERT INTO t VALUES (5, 'x')", schema_name=schema_name)


def test_serial_two_connections_disjoint_ids(client, schema_name, serial_t, server):
    """Two connections drawing from the same table's sequence get disjoint id
    ranges — no collision, no reuse. If the durable per-range advance were not
    serialized, both would draw the same range and the second INSERT would hit a
    PK violation (or, worse, silently duplicate)."""
    with gnitz.connect(server) as c1, gnitz.connect(server) as c2:
        # Interleave so c1 draws from its range both before and after c2 does.
        c1.execute_sql("INSERT INTO t (name) VALUES ('a1'), ('a2')", schema_name=schema_name)
        c2.execute_sql("INSERT INTO t (name) VALUES ('b1'), ('b2')", schema_name=schema_name)
        c1.execute_sql("INSERT INTO t (name) VALUES ('a3')", schema_name=schema_name)
        ids = [r.id for r in c1.scan(serial_t)]
    assert len(ids) == 5
    assert len(set(ids)) == 5  # all distinct across both connections


def test_concurrent_serial_alloc_with_seeks(client, schema_name, serial_t, server):
    """Many connections drawing SERIAL ids concurrently — forcing repeated durable
    range refills (every SERIAL_RANGE_SIZE = 64 inserts) — while a reader streams
    SEEKs the whole time. Exercises the SERIAL path releasing the catalog write
    lock BEFORE its fdatasync: concurrent range fsyncs now overlap and a SEEK may
    run during one. Asserts (1) no id is ever issued twice (a zone double-open
    would collide), (2) each connection's own ids are strictly increasing, and
    (3) the concurrent SEEK loop never hangs or errors.
    """
    n_writers = 4
    per_writer = 100  # > 64 forces at least one durable range refill per writer
    writer_ids = [None] * n_writers
    errors = []
    writers_running = threading.Event()

    def writer(idx):
        try:
            with gnitz.connect(server) as c:
                got = []
                for i in range(per_writer):
                    res = c.execute_sql(
                        f"INSERT INTO t (name) VALUES ('w{idx}_{i}') RETURNING id",
                        schema_name=schema_name)
                    got.extend(r.id for r in _returned_rows(res))
                    if i == 0:
                        writers_running.set()
                writer_ids[idx] = got
        except Exception as exc:  # noqa: BLE001
            errors.append(("writer", exc))

    def seeker():
        writers_running.wait(timeout=START_TIMEOUT)
        try:
            with gnitz.connect(server) as c:
                for _ in range(200):
                    # A miss returns empty (not an error); the point is that
                    # the round-trip completes while SERIAL fsyncs are in flight.
                    list(c.seek(serial_t, pk=1))
        except Exception as exc:  # noqa: BLE001
            errors.append(("seek", exc))

    wthreads = [threading.Thread(target=writer, args=(i,), daemon=True)
                for i in range(n_writers)]
    sthread = threading.Thread(target=seeker, daemon=True)
    for t in wthreads:
        t.start()
    sthread.start()
    # Hang ceilings, not perf budgets — see _serverproc.py.
    for t in wthreads:
        t.join(timeout=HANG_TIMEOUT)
    sthread.join(timeout=HANG_TIMEOUT)

    assert all(not t.is_alive() for t in wthreads), "a SERIAL writer hung"
    assert not sthread.is_alive(), "the concurrent SEEK loop hung"
    for src, exc in errors:
        raise AssertionError(f"{src} thread raised: {exc}")

    # (2) per-connection strict monotonicity.
    for idx, ids in enumerate(writer_ids):
        assert ids is not None and len(ids) == per_writer, f"writer {idx} lost rows"
        assert ids == sorted(ids) and len(set(ids)) == len(ids), (
            f"writer {idx} ids not strictly increasing: {ids}"
        )

    # (1) global uniqueness — no id issued to two connections.
    all_ids = [i for ids in writer_ids for i in ids]
    assert len(all_ids) == n_writers * per_writer
    assert len(set(all_ids)) == len(all_ids), "a SERIAL id was issued twice (zone double-open)"

    # A full scan agrees with the set of issued ids (gaps from partially
    # consumed per-connection range tails are permitted).
    assert sorted(r.id for r in client.scan(serial_t)) == sorted(set(all_ids))


def test_restart_id_monotonicity(own_server):
    """A committed id is never re-issued: after a crash-restart on the same data
    dir, every new id exceeds every id committed before the restart (gaps from a
    discarded range tail are permitted)."""
    sock = own_server.sock_path
    own_server.start()
    with gnitz.connect(sock) as c:
        c.create_schema("s")
        c.execute_sql(_CREATE, schema_name="s")
        c.execute_sql("INSERT INTO t (name) VALUES ('a'), ('b'), ('c')", schema_name="s")
        tid, _ = c.resolve_table("s", "t")
        before = sorted(r.id for r in c.scan(tid))
    assert before == [1, 2, 3]

    # Crash-restart on the SAME data dir (durable catalog + sequence survive).
    own_server.restart()

    with gnitz.connect(sock) as c:
        # A fresh connection re-fetches the schema (is_serial round-trips
        # through COL_TAB) and continues drawing ids above the durable
        # high-water.
        c.execute_sql("INSERT INTO t (name) VALUES ('d'), ('e')", schema_name="s")
        tid, _ = c.resolve_table("s", "t")
        after = sorted(r.id for r in c.scan(tid) if r.id not in before)
    assert after, "expected new rows after restart"
    assert min(after) > max(before), f"new ids {after} must exceed pre-restart max {max(before)}"
