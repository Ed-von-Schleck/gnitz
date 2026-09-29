"""SERIAL / BIGSERIAL / SMALLSERIAL auto-increment primary keys.

The client draws ids from a per-table durable sequence on the master, caching a
range per connection, stamps them into the PK before pushing, and answers
INSERT … RETURNING from the ids it stamped. An id is never issued twice and one
connection's ids only rise; a cached range abandoned on refill leaves a gap.
"""

import threading

import gnitz
import pytest
from _read import bag, rows, scanned
from _serverproc import join_or_fail


@pytest.fixture
def serial_t(client, schema_name):
    """`t(id SERIAL PK, name TEXT)`; yields its tid."""
    client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)", schema_name=schema_name)
    return client.resolve_table(schema_name, "t")[0]


def test_every_spelling_numbers_rows_around_its_payload(client, schema_name):
    """Every spelling draws its own sequence from 1. The SERIAL column sits
    between two payload columns, so a column list and a positional INSERT must
    each land their values around it, and a NULL payload keeps its null bit. A
    deleted id is never reissued, and a 200-row statement numbers its rows in
    order and scatters them over every partition exactly once."""
    sn = schema_name
    for ty in ["SERIAL", "BIGSERIAL", "SMALLSERIAL", "SERIAL2", "SERIAL4", "SERIAL8"]:
        t = f"t_{ty.lower()}"
        client.execute_sql(
            f"CREATE TABLE {t} (a BIGINT, id {ty} PRIMARY KEY, b TEXT); "
            f"INSERT INTO {t} (a, b) VALUES (10, 'x'), (NULL, NULL); "
            f"INSERT INTO {t} VALUES (30, 'z'); "
            f"DELETE FROM {t} WHERE id = 3", schema_name=sn)
        values = ", ".join(f"('n{i}')" for i in range(200))
        first = min(r.id for r in rows(client, sn, f"INSERT INTO {t} (b) VALUES {values} RETURNING id"))
        assert first > 3, ty
        assert bag(scanned(client, sn, t), "id", "a", "b") == {
            (1, 10, "x"): 1, (2, None, None): 1,
            **{(first + i, None, f"n{i}"): 1 for i in range(200)}}, ty


def test_returning_answers_from_the_stamped_ids(client, schema_name, serial_t):
    """Every projection RETURNING accepts — a wildcard modifier included —
    answers from the ids the client stamped, and names the rows written."""
    sn = schema_name
    assert bag(rows(client, sn, "INSERT INTO t (name) VALUES ('a'), ('b') RETURNING id")) == {(1,): 1, (2,): 1}
    assert bag(rows(client, sn, "INSERT INTO t (name) VALUES ('c') RETURNING *")) == {(3, "c"): 1}
    assert bag(rows(client, sn, "INSERT INTO t (name) VALUES ('d') RETURNING id, name")) == {(4, "d"): 1}
    [row] = rows(client, sn, "INSERT INTO t (name) VALUES ('e') RETURNING * EXCEPT (name)")
    assert (row._fields, row.id) == (("id",), 5)
    # A RETURNING list binds as a SELECT list: expressions, and no key required.
    [row] = rows(client, sn, "INSERT INTO t (name) VALUES ('f') RETURNING name, id + 1 AS nxt")
    assert (row._fields, row.name, row.nxt) == (("name", "nxt"), "f", 7)
    assert bag(rows(client, sn, "INSERT INTO t (name) VALUES ('g') RETURNING name")) == {("g",): 1}
    assert bag(scanned(client, sn, "t")) == {(i + 1, n): 1 for i, n in enumerate("abcdefg")}


def test_a_serial_key_reaches_derived_schemas_as_a_plain_column(client, schema_name, serial_t):
    """SERIAL is the base table's property, so a schema derived from it — a join
    view's payload, an aliased projection, a GROUP BY key — carries the id as an
    ordinary integer column: each plans and answers with the stamped ids."""
    sn = schema_name
    client.execute_sql(
        "INSERT INTO t (name) VALUES ('a'), ('b'), ('c'); "
        "CREATE TABLE u (k BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL); "
        "INSERT INTO u VALUES (10, 1), (20, 3); "
        "CREATE VIEW j AS SELECT u.k, t.id, t.name FROM u JOIN t ON u.tid = t.id; "
        "CREATE VIEW g AS SELECT id, COUNT(*) AS n FROM t GROUP BY id", schema_name=sn)
    assert bag(scanned(client, sn, "j"), "k", "id", "name") == {(10, 1, "a"): 1, (20, 3, "c"): 1}
    assert bag(scanned(client, sn, "g"), "id", "n") == {(1, 1): 1, (2, 1): 1, (3, 1): 1}
    assert bag(rows(client, sn, "SELECT id AS i FROM t")) == {(1,): 1, (2,): 1, (3,): 1}
    # The views follow a later stamped insert.
    [row] = rows(client, sn, "INSERT INTO t (name) VALUES ('d') RETURNING id")
    client.execute_sql(f"INSERT INTO u VALUES (30, {row.id})", schema_name=sn)
    assert bag(scanned(client, sn, "j"), "k", "id", "name") == {
        (10, 1, "a"): 1, (20, 3, "c"): 1, (30, 4, "d"): 1}
    assert bag(scanned(client, sn, "g"), "id", "n") == {(i, 1): 1 for i in range(1, 5)}


def test_concurrent_connections_never_share_an_id(client, schema_name, serial_t, server):
    """Four fresh connections — none saw the CREATE, so each recognizes the
    SERIAL column from the catalog alone — draw ids at once in 30-row
    statements, so each refills its cached range several times and abandons a
    tail each time, while a fifth connection seeks the table throughout. No id
    is issued twice, each connection's ids rise, a seek never waits out a range
    allocation's sync, and the table holds exactly the returned ids."""
    sn = schema_name
    writers, statements, per = 4, 5, 30
    ids = [[] for _ in range(writers)]
    errors = []
    done = threading.Event()

    def writer(w):
        try:
            with gnitz.connect(server) as c:
                for s in range(statements):
                    values = ", ".join(f"('w{w}s{s}r{r}')" for r in range(per))
                    ids[w] += [r.id for r in rows(c, sn, f"INSERT INTO t (name) VALUES {values} RETURNING id")]
        except Exception as e:  # noqa: BLE001 — re-raised below
            errors.append(e)

    def seeker():
        try:
            with gnitz.connect(server) as c:
                schema = c.resolve_table(sn, "t")[1]
                while not done.is_set():
                    list(c.seek(serial_t, schema, pk=1))
        except Exception as e:  # noqa: BLE001 — re-raised below
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(w,), daemon=True) for w in range(writers)]
    seek = threading.Thread(target=seeker, daemon=True)
    for t in [*threads, seek]:
        t.start()
    join_or_fail("a SERIAL writer hung", *threads)
    done.set()
    join_or_fail("a seek hung behind SERIAL allocation", seek)
    assert not errors, errors

    for got in ids:
        assert len(got) == statements * per and got == sorted(set(got)), got
    issued = [i for got in ids for i in got]
    assert len(set(issued)) == len(issued), "an id was issued to two connections"
    assert bag(scanned(client, sn, "t"), "id") == {(i,): 1 for i in issued}
