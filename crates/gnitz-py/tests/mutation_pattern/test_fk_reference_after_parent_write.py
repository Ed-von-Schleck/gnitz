"""A reference the engine has already found valid, written again after its
parent was written: the parent's leaving refuses it, by whatever write it left."""

import contextlib
import random

import pytest
import gnitz
import _fk

_DDL = (
    "CREATE TABLE root (id BIGINT PRIMARY KEY); "
    "CREATE TABLE parent (id BIGINT PRIMARY KEY, root_id BIGINT REFERENCES root(id) ON DELETE CASCADE); "
    "CREATE TABLE child (cid BIGINT PRIMARY KEY, pid BIGINT NOT NULL REFERENCES parent(id))"
)
_ROWS = {"root": [(1,), (2,)], "parent": [(1, 1), (2, 2), (3, None)], "child": []}
# More rows than one write's inserts are recorded for.
_MANY = range(1000, 1100)
_IN_MANY = "(" + ", ".join(map(str, _MANY)) + ")"

# `(the write to the parent, whether a reference to parent 1 is still valid after it)`.
_PARENT_WRITES = {
    "deleted": ("DELETE FROM parent WHERE id = 1", False),
    "deleted in a transaction": ("BEGIN; INSERT INTO root VALUES (9); DELETE FROM parent WHERE id = 1; COMMIT", False),
    "deleted by a cascade": ("DELETE FROM root WHERE id = 1", False),
    "deleted among many": (f"DELETE FROM parent WHERE id IN (1, {_IN_MANY[1:]}", False),
    "deleted and re-inserted": ("BEGIN; DELETE FROM parent WHERE id = 1; INSERT INTO parent VALUES (1, 2); COMMIT", True),
    "deleted, then re-inserted": ("DELETE FROM parent WHERE id = 1; INSERT INTO parent VALUES (1, NULL)", True),
    "updated": ("UPDATE parent SET root_id = 2 WHERE id = 1", True),
    "another row deleted": ("DELETE FROM parent WHERE id = 2", True),
}


@pytest.mark.parametrize("write,valid", _PARENT_WRITES.values(), ids=_PARENT_WRITES.keys())
def test_a_proven_reference_follows_its_parent(client, write, valid):
    _fk.seed(client, _DDL, _ROWS)
    client.execute_sql("INSERT INTO parent VALUES " + ", ".join(f"({i}, NULL)" for i in _MANY))
    # Proven by a probe, then written again and removed, so nothing references the parent.
    client.execute_sql("INSERT INTO child VALUES (10, 1), (11, 1000)")
    client.execute_sql("INSERT INTO child VALUES (12, 1)")
    client.execute_sql("DELETE FROM child WHERE cid IN (10, 11, 12)")

    client.execute_sql(write)
    with contextlib.nullcontext() if valid else _fk.refused():
        client.execute_sql("INSERT INTO child VALUES (20, 1)")
    kept = _fk.held(client, ["child"])["child"]
    assert (20, 1) in kept if valid else (20, 1) not in kept


def test_a_proven_reference_is_proven_again_after_a_restart(own_server):
    own_server.start()
    with gnitz.connect(own_server.target) as conn:
        _fk.seed(conn, _DDL, _ROWS)
        conn.execute_sql("INSERT INTO child VALUES (10, 1)")
        conn.execute_sql("DELETE FROM child WHERE cid = 10")
        conn.execute_sql("DELETE FROM parent WHERE id = 1")
    own_server.restart()
    with gnitz.connect(own_server.target) as conn:
        with _fk.refused():
            conn.execute_sql("INSERT INTO child VALUES (20, 1)")
        conn.execute_sql("INSERT INTO child VALUES (21, 2)")
        assert _fk.held(conn, ["child"]) == _fk.want({"child": [(21, 2)]})


@pytest.mark.parametrize("keys", [None, "3"], ids=["default bound", "three keys"])
def test_random_writes_agree_with_the_set_of_parents(own_server, keys):
    """Every verdict of a random stream of parent and child writes is the one the
    parents held at that moment give, with the engine's memory of proven keys
    at its default bound and at one the stream overruns."""
    own_server.start(extra_env={"GNITZ_FK_PRESENCE_KEYS": keys} if keys else None)
    rng = random.Random(20261007)
    parents, children = set(), {}
    block = list(range(100, 170))

    def step():
        """`(statement, whether it is refused, its effect on the model)`."""
        k = rng.random()
        pid, cid = rng.randrange(1, 9), rng.randrange(1, 13)
        if k < 0.04:
            fresh = not parents & set(block)
            return ("INSERT INTO parent VALUES " + ", ".join(f"({i}, NULL)" for i in block), not fresh,
                    lambda: parents.update(block))
        if k < 0.08:
            free = not set(children.values()) & set(block)
            return (f"DELETE FROM parent WHERE id IN ({', '.join(map(str, block))})", not free,
                    lambda: parents.difference_update(block))
        if k < 0.30:
            return (f"INSERT INTO parent VALUES ({pid}, NULL)", pid in parents, lambda: parents.add(pid))
        if k < 0.45:
            return (f"DELETE FROM parent WHERE id = {pid}", pid in children.values(), lambda: parents.discard(pid))
        if k < 0.85:
            ref = rng.choice(sorted(parents)) if parents and rng.random() < 0.8 else rng.choice([pid, 100, 169])
            return (f"INSERT INTO child VALUES ({cid}, {ref})", cid in children or ref not in parents,
                    lambda: children.__setitem__(cid, ref))
        return (f"DELETE FROM child WHERE cid = {cid}", False, lambda: children.pop(cid, None))

    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(_DDL)
        for i in range(700):
            sql, refused, apply = step()
            with pytest.raises(gnitz.GnitzIntegrityError) if refused else contextlib.nullcontext():
                conn.execute_sql(sql)
            if not refused:
                apply()
        want = {"parent": [(p, None) for p in parents], "child": list(children.items())}
        assert _fk.held(conn, want) == _fk.want(want)
