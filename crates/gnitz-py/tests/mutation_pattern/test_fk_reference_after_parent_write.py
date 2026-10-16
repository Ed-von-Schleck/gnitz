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
_ROWS = {"root": [(1,), (2,)], "parent": [(1, 1), (2, 2), (3, None)]}

# `(the write to the parent, whether a reference to parent 1 is still valid after it)`.
_PARENT_WRITES = {
    "deleted": ("DELETE FROM parent WHERE id = 1", False),
    "deleted in a transaction": ("BEGIN; INSERT INTO root VALUES (9); DELETE FROM parent WHERE id = 1; COMMIT", False),
    "deleted by a cascade": ("DELETE FROM root WHERE id = 1", False),
    "deleted and re-inserted": ("BEGIN; DELETE FROM parent WHERE id = 1; INSERT INTO parent VALUES (1, 2); COMMIT", True),
    "deleted, then re-inserted": ("DELETE FROM parent WHERE id = 1; INSERT INTO parent VALUES (1, NULL)", True),
    "updated": ("UPDATE parent SET root_id = 2 WHERE id = 1", True),
    "another row deleted": ("DELETE FROM parent WHERE id = 2", True),
}


@pytest.mark.parametrize("write,valid", _PARENT_WRITES.values(), ids=_PARENT_WRITES.keys())
def test_a_proven_reference_follows_its_parent(client, write, valid):
    _fk.seed(client, _DDL, _ROWS)
    # Proven by a probe, then written again and removed, so nothing references the parent.
    client.execute_sql("INSERT INTO child VALUES (10, 1); INSERT INTO child VALUES (12, 1); "
                       "DELETE FROM child WHERE cid IN (10, 12)")

    client.execute_sql(write)
    with contextlib.nullcontext() if valid else _fk.refused():
        client.execute_sql("INSERT INTO child VALUES (20, 1)")
    assert _fk.held(client, ["child"]) == _fk.want({"child": [(20, 1)] if valid else []})


@pytest.mark.parametrize("keys", [None, "3"], ids=["default bound", "three keys"])
def test_random_writes_agree_with_the_set_of_parents(own_server, keys):
    """Every verdict of a random stream of parent and child writes is the one the
    parents held at that moment give, with the engine's memory of proven keys
    at its default bound and at one the stream overruns."""
    own_server.start(extra_env={"GNITZ_FK_PRESENCE_KEYS": keys} if keys else None)
    rng = random.Random(20261007)
    parents, children = set(), {}
    block = list(range(100, 170))

    fk, dup_key = "foreign key", "duplicate key"

    def step():
        """`(statement, the pattern of the refusal it meets or None, its effect
        on the model)`."""
        k = rng.random()
        pid, cid = rng.randrange(1, 9), rng.randrange(1, 13)
        if k < 0.04:
            return ("INSERT INTO parent VALUES " + ", ".join(f"({i}, NULL)" for i in block),
                    dup_key if parents.intersection(block) else None, lambda: parents.update(block))
        if k < 0.08:
            return (f"DELETE FROM parent WHERE id IN ({', '.join(map(str, block))})",
                    fk if set(children.values()).intersection(block) else None,
                    lambda: parents.difference_update(block))
        if k < 0.30:
            return (f"INSERT INTO parent VALUES ({pid}, NULL)", dup_key if pid in parents else None,
                    lambda: parents.add(pid))
        if k < 0.45:
            return (f"DELETE FROM parent WHERE id = {pid}", fk if pid in children.values() else None,
                    lambda: parents.discard(pid))
        if k < 0.85:
            ref = rng.choice(sorted(parents)) if parents and rng.random() < 0.8 else rng.choice([pid, 100, 169])
            dup, dangling = cid in children, ref not in parents
            refusal = f"{dup_key}|{fk}" if dup and dangling else dup_key if dup else fk if dangling else None
            return (f"INSERT INTO child VALUES ({cid}, {ref})", refusal, lambda: children.__setitem__(cid, ref))
        return (f"DELETE FROM child WHERE cid = {cid}", None, lambda: children.pop(cid, None))

    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(_DDL)
        for _ in range(700):
            sql, refusal, apply = step()
            with (contextlib.nullcontext() if refusal is None
                  else pytest.raises(gnitz.GnitzIntegrityError, match=f"(?i){refusal}")):
                conn.execute_sql(sql)
            if refusal is None:
                apply()
        want = {"parent": [(p, None) for p in parents], "child": list(children.items())}
        assert _fk.held(conn, want) == _fk.want(want)
