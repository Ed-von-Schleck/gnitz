"""A restart at a different worker count re-homes every relation's children.

The SAL is written pre-sliced per worker at the then-current count and read back
by slot, and a replicated table's per-worker copy is addressed by rank. So a
restart at a changed count must replay every slot the un-checkpointed tail
carries, re-cut each partitioned group for the launched topology, take exactly
one slot of each broadcast group, and re-derive every rank's index slice — or
client-ACKed, fdatasync-durable rows are silently lost or silently doubled.

One test holds the count fixed and varies what exists at the boundary instead: a
recovered replicated copy must be live on every rank for a circuit compiled after
the reboot, which is the same re-homing seen from the other side.

Every worker count below is hardcoded, so these run multi-worker even under
`make e2e WORKERS=1`. Assertions are on weights throughout: `scan` concatenates
per-worker frames with no cross-worker consolidation, so a row that survived on
two workers arrives twice at weight 1 — exactly what a botched re-slice or a
duplicated replicated copy produces, and exactly what keying by PK would hide.
"""

import pytest
import gnitz
from _read import bag, scanned


def _replicated_dim(conn):
    conn.execute_sql(
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) "
        "WITH (replicated = true)")


def _join_over_dim(conn):
    """A partitioned `fact` and the join view over an existing `dim`.

    The join is the observable both replicated-copy tests read through, because a
    scan of the replicated table itself is single-sourced to worker 0, whose copy
    is current at every worker count — a damaged copy on workers 1..W-1 is
    invisible to a plain SELECT. `fact JOIN dim` skips the exchange on both sides
    and cogroups against each worker's own `dim`, union-gathering the result, so a
    worker with an empty, stale or duplicated copy shows up in the weights."""
    conn.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, dim_pk BIGINT NOT NULL)")
    conn.execute_sql(
        "CREATE VIEW j AS SELECT f.pk AS pk, d.v AS v "
        "FROM fact f JOIN dim d ON f.dim_pk = d.pk")


def _insert_pairs(conn, lo, hi):
    """One `dim` row and one matching `fact` row per key in [lo, hi)."""
    conn.execute_sql(
        "INSERT INTO dim VALUES " + ", ".join(f"({i}, {i * 10})" for i in range(lo, hi)))
    conn.execute_sql(
        "INSERT INTO fact VALUES " + ", ".join(f"({i}, {i})" for i in range(lo, hi)))


def _joined(srv):
    with gnitz.connect(srv.target) as conn:
        return bag(scanned(conn, "j"), "pk", "v")


@pytest.mark.parametrize("wrote,launched", [(4, 2), (1, 4)])
def test_an_unflushed_tail_survives_a_worker_count_change(wrote, launched, own_server):
    """SIGKILL with an un-checkpointed tail at `wrote` workers, restart at
    `launched`. Every launched rank must read all `wrote` slots and keep exactly
    the rows its own slice owns — on the growth leg three of the four ranks have
    no slot of their own at all and depend entirely on slot 0 being re-cut.

    The table is `CLUSTER BY` + compound PK, so a re-slice that hashed the full PK
    rather than the distribution prefix misroutes; it carries a UNIQUE column, so
    an over-populated secondary index shows up; and the tail ends in a DELETE +
    re-INSERT, so the replay carries a retraction."""
    own_server.start(workers=wrote)
    keys = [(a, b) for a in range(20) for b in range(10)]
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, "
            "u BIGINT NOT NULL UNIQUE, PRIMARY KEY (a, b)) CLUSTER BY a")
        # A pinned key set — 20 distinct `a`, 10 `b` each — so a changed hash
        # fails loudly instead of silently degrading the partition spread.
        conn.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({a}, {b}, {a * 10 + b})"
                                               for a, b in keys))
        # Still in the same tail: retract one key and re-insert it.
        conn.execute_sql("DELETE FROM t WHERE a = 7 AND b = 3")
        conn.execute_sql("INSERT INTO t VALUES (7, 3, 73)")

    own_server.restart(workers=launched)
    assert own_server.resliced(), \
        "a restart at a changed worker count must replay every written slot"

    with gnitz.connect(own_server.target) as conn:
        assert bag(scanned(conn, "t"), "a", "b") == {k: 1 for k in keys}
        # The only index assertion that discriminates: re-inserting a `u` value
        # whose sole holder was just deleted must SUCCEED. Rejecting a duplicate
        # and admitting a fresh value both pass with an over-populated index.
        conn.execute_sql("DELETE FROM t WHERE a = 4 AND b = 5")
        conn.execute_sql("INSERT INTO t VALUES (4, 5, 45)")
        assert bag(scanned(conn, "t")) == {(a, b, a * 10 + b): 1 for a, b in keys}


def test_a_replicated_tail_survives_a_worker_count_shrink(own_server):
    """A replicated table's rows are broadcast into EVERY SAL slot, not sliced, so
    the changed-count replay must take exactly one slot and never re-cut it:
    re-slicing would cut each worker's copy down to its partition share, and
    replaying all four slots would leave every row at weight 4."""
    own_server.start(workers=4)
    with gnitz.connect(own_server.target) as conn:
        _replicated_dim(conn)
        _join_over_dim(conn)
        _insert_pairs(conn, 0, 64)

    own_server.restart(workers=2)
    assert own_server.resliced()
    assert _joined(own_server) == {(i, i * 10): 1 for i in range(64)}


def test_every_rank_reads_a_current_replicated_copy_after_a_relayout(own_server):
    """A replicated copy is addressed by rank, so every launched rank must read
    one that is current after the count changes. 4 -> 3 covers reclamation plus
    the surviving ranks keeping their own copies; 3 -> 4 covers the rebuild (rank
    3's copy was retired and must be re-derived) and a boot reading a tail a
    narrower count wrote: the clean shutdown leaves a live group at SAL offset 0
    written at W=3, and rank 3 must see its absent slot as empty rather than as
    the earlier W=4 run's leftover at the same offset. Every transition is a CLEAN
    shutdown, so the data under test lives in shards rather than the tail."""
    own_server.start(workers=4)
    with gnitz.connect(own_server.target) as conn:
        _replicated_dim(conn)
        _join_over_dim(conn)
        _insert_pairs(conn, 0, 64)
        # Mutate `dim`, so the W=3 leg has something a stale copy would miss.
        conn.execute_sql("DELETE FROM dim WHERE pk < 16")
        _insert_pairs(conn, 64, 80)

    want = {(i, i * 10): 1 for i in range(16, 80)}
    assert _joined(own_server) == want, "W=4: every fact must join its dim"

    own_server.restart(graceful=True, workers=3)
    assert _joined(own_server) == want, \
        "W=3: every surviving rank must read its own copy, mutation included"

    own_server.restart(graceful=True, workers=4)
    assert _joined(own_server) == want, \
        "W=4 again: re-growing past a narrower run must boot and read every copy"


def test_a_join_compiled_after_a_reboot_reaches_every_replicated_copy(own_server):
    """The copy has to be live on every rank for a circuit that did not exist when
    the rows arrived. Only `dim` is present before the SIGKILL; the fact table and
    the join are created afterwards, so the join compiles fresh against a recovered
    replicated table and each worker cogroups against its own restored copy. A rank
    whose copy did not come back contributes no rows for the facts it owns."""
    own_server.start(workers=4)
    with gnitz.connect(own_server.target) as conn:
        _replicated_dim(conn)
        conn.execute_sql(
            "INSERT INTO dim VALUES " + ", ".join(f"({i}, {i * 10})" for i in range(8)))

    own_server.restart(workers=4)
    with gnitz.connect(own_server.target) as conn:
        _join_over_dim(conn)
        conn.execute_sql(
            "INSERT INTO fact VALUES " + ", ".join(f"({i}, {i % 8})" for i in range(40)))
    assert _joined(own_server) == {(i, (i % 8) * 10): 1 for i in range(40)}
