"""SERIAL / BIGSERIAL / SMALLSERIAL auto-increment primary keys.

The client draws ids from a per-table durable sequence on the master, caching a
range per connection, stamps them into the PK before pushing, and answers
INSERT … RETURNING from the ids it stamped. An id is never issued twice and one
connection's ids only rise; a cached range abandoned on refill leaves a gap.
"""

import threading

import pytest
from _read import bag, ordered, rows, scanned
from _serverproc import join_or_fail, spawn


@pytest.fixture
def serial_t(client):
    """`t(id SERIAL PK, name TEXT)`; yields its tid."""
    client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT)")
    return client.resolve_table("t")[0]


@pytest.mark.parametrize("ty", ["SMALLSERIAL", "SERIAL", "BIGSERIAL"])
def test_every_width_numbers_rows_around_its_payload(client, ty):
    """Each width draws its sequence from 1; the other spellings are aliases the
    binder resolves. The SERIAL column sits between two payload columns, so a
    column list and a positional INSERT must each land their values around it,
    and a NULL payload keeps its null bit. A deleted id is never reissued, and a
    200-row statement numbers its rows in order and scatters them over every
    partition exactly once."""
    client.execute_sql(
        f"CREATE TABLE t (a BIGINT, id {ty} PRIMARY KEY, b TEXT); "
        "INSERT INTO t (a, b) VALUES (10, 'x'), (NULL, NULL); "
        "INSERT INTO t VALUES (30, 'z'); "
        "DELETE FROM t WHERE id = 3")
    values = ", ".join(f"('n{i}')" for i in range(200))
    first = min(r.id for r in rows(client, f"INSERT INTO t (b) VALUES {values} RETURNING id"))
    assert first > 3
    assert bag(scanned(client, "t"), "id", "a", "b") == {
        (1, 10, "x"): 1, (2, None, None): 1,
        **{(first + i, None, f"n{i}"): 1 for i in range(200)}}


def test_returning_answers_from_the_stamped_ids(client, serial_t):
    """Every projection RETURNING accepts — a wildcard modifier included —
    answers from the ids the client stamped, under the names it binds, and names
    the rows written. It binds as a SELECT list: expressions, and no key
    required."""
    for names, returning, want in [
            ("('a'), ('b')", "id", [{"id": 1}, {"id": 2}]),
            ("('c')", "*", [{"id": 3, "name": "c"}]),
            ("('d')", "id, name", [{"id": 4, "name": "d"}]),
            ("('e')", "* EXCEPT (name)", [{"id": 5}]),
            ("('f')", "name, id + 1 AS nxt", [{"name": "f", "nxt": 7}]),
            ("('g')", "name", [{"name": "g"}])]:
        got = ordered(rows(client, f"INSERT INTO t (name) VALUES {names} RETURNING {returning}"))
        assert got == [(tuple(w.items()), 1) for w in want], returning
    assert bag(scanned(client, "t")) == {(i + 1, n): 1 for i, n in enumerate("abcdefg")}


def test_concurrent_connections_never_share_an_id(client, serial_t, peer):
    """Four fresh connections — none saw the CREATE, so each recognizes the
    SERIAL column from the catalog alone — draw ids at once in 30-row
    statements, so each refills its cached range several times and abandons a
    tail each time, while a fifth connection seeks the table throughout. No id
    is issued twice, each connection's ids rise, a seek does not hang behind a
    range allocation and finds id 1 at most once, and the table holds exactly the
    returned ids."""
    writers, statements, per = 4, 5, 30
    ids = [[] for _ in range(writers)]
    done = threading.Event()

    def writer(c, w):
        for s in range(statements):
            values = ", ".join(f"('w{w}s{s}r{r}')" for r in range(per))
            ids[w] += [r.id for r in rows(c, f"INSERT INTO t (name) VALUES {values} RETURNING id")]

    def seeker(c):
        schema = c.resolve_table("t")[1]
        while not done.is_set():
            got = bag(c.seek(serial_t, schema, pk=1), "id")
            assert got in ({}, {(1,): 1}), got

    threads = [spawn(writer, peer(), w) for w in range(writers)]
    seek = spawn(seeker, peer())
    join_or_fail("a SERIAL writer hung", *threads)
    done.set()
    join_or_fail("a seek hung behind SERIAL allocation", seek)

    for got in ids:
        assert len(got) == statements * per and got == sorted(set(got)), got
    issued = [i for got in ids for i in got]
    assert len(set(issued)) == len(issued), "an id was issued to two connections"
    assert bag(scanned(client, "t"), "id") == {(i,): 1 for i in issued}
