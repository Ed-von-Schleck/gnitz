"""An index across a process boundary: resumed from its checkpoint when it can
be, re-derived slice-locally when it cannot.

An index is derived state the checkpoint persists, so a clean restart must
reload it rather than re-scan the owner's slice on every worker. Correctness
alone cannot show that — a silent rebuild produces the same holders — so the
resume tests read the boot's per-worker rebuild marker as well.

A boot-recovered index is **slice-local**: every worker's copy holds exactly its
own base slice, so the master's HAS_PK / seek union over workers is exactly
global existence. A full replicated copy instead goes stale in its foreign
(W-1)/W fraction as those rows churn on their owning workers, and the
distributed unique and FK probes then see phantom entries.
"""

import pytest
import gnitz
from _read import bag, scanned


def test_an_index_resumes_across_a_clean_restart(own_server):
    """A graceful stop's shutdown barrier runs a full checkpoint sequence, whose
    ephemeral round publishes every worker's index at the committed generation.
    The restart must reload it: the seeks alone would pass on a silent rebuild,
    so the marker is the discriminating assertion."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("idxres")
        conn.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
            schema_name="idxres")
        conn.execute_sql(
            "INSERT INTO t VALUES " + ", ".join(f"({i}, {i * 10})" for i in range(64)),
            schema_name="idxres")
        conn.execute_sql("CREATE INDEX ON t(g)", schema_name="idxres")

    own_server.restart(graceful=True)

    with gnitz.connect(own_server.sock_path) as conn:
        tid, _ = conn.resolve_table("idxres", "t")
        for i in (0, 63):
            assert sorted(conn.seek_by_index(tid, [1], [i * 10]).pks) == [i], \
                f"g={i * 10} must still resolve to its source PK after the restart"

    counts = own_server.rebuilt_index_counts()
    assert counts == [0] * own_server.workers, (
        f"a clean restart must resume every index from its checkpoint, "
        f"but workers rebuilt {counts}")


def test_a_rebuilt_index_holds_exactly_its_own_slice(own_server):
    """Four probes of the same fact, over one crash restart. Each is a *negative*
    consequence of a stale foreign entry, which is the only kind that
    discriminates: rejecting a genuine duplicate and admitting a genuinely fresh
    value both pass with an over-populated index."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("slice")
        for stmt in (
            "CREATE TABLE uq (id BIGINT NOT NULL PRIMARY KEY, u BIGINT NOT NULL UNIQUE)",
            "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, u BIGINT NOT NULL UNIQUE)",
            "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, "
            "f BIGINT NOT NULL REFERENCES p(u))",
            "CREATE TABLE p2 (id BIGINT NOT NULL PRIMARY KEY, u BIGINT NOT NULL UNIQUE)",
            "CREATE TABLE c2 (id BIGINT NOT NULL PRIMARY KEY, "
            "f BIGINT NOT NULL REFERENCES p2(u))",
            "CREATE TABLE sx (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
            "INSERT INTO uq VALUES (1, 5)",
            "INSERT INTO p VALUES (1, 5)",
            "INSERT INTO p2 VALUES (1, 6)",
            "INSERT INTO c2 VALUES (10, 6)",
            # Remove the only child before the boundary, so the parent value is
            # free to retarget afterwards.
            "DELETE FROM c2 WHERE id = 10",
            "INSERT INTO sx VALUES " + ", ".join(
                f"({i}, {7 if i % 3 == 0 else 1000 + i})" for i in range(30)),
            "CREATE INDEX ON sx(g)",
        ):
            conn.execute_sql(stmt, schema_name="slice")

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        # 1. A unique value whose sole holder is deleted must be re-insertable:
        #    a non-owning worker keeping the value's stale +1 rejects it.
        conn.execute_sql("DELETE FROM uq WHERE id = 1", schema_name="slice")
        conn.execute_sql("INSERT INTO uq VALUES (2, 5)", schema_name="slice")
        assert bag(scanned(conn, "slice", "uq"), "id", "u") == {(2, 5): 1}

        # 2. A child referencing a parent value deleted after the boundary must
        #    be refused: the parent-existence probe broadcast-seeks the parent's
        #    UNIQUE index, and a stale worker answers "present".
        conn.execute_sql("DELETE FROM p WHERE id = 1", schema_name="slice")
        with pytest.raises(gnitz.GnitzError):
            conn.execute_sql("INSERT INTO c VALUES (10, 5)", schema_name="slice")
        assert bag(scanned(conn, "slice", "c"), "id", "f") == {}

        # 3. Retargeting a parent value whose only child is gone must succeed:
        #    the RESTRICT check probes the child's FK index for the old value,
        #    and a stale worker still holding the deleted child blocks it.
        conn.execute_sql("UPDATE p2 SET u = 7", schema_name="slice")
        assert bag(scanned(conn, "slice", "p2"), "id", "u") == {(1, 7): 1}

        # 4. A non-unique index spanning partitions returns the right holder set
        #    and tracks post-restart writes. Read through the distributed seek,
        #    which merges every worker's slice-local index and resolves the hits
        #    against that worker's own base slice.
        sx, _ = conn.resolve_table("slice", "sx")

        def holders():
            return sorted(conn.seek_by_index(sx, [1], [7]).pks)

        assert holders() == [i for i in range(30) if i % 3 == 0]
        conn.execute_sql("DELETE FROM sx WHERE id = 0", schema_name="slice")
        conn.execute_sql("INSERT INTO sx VALUES (100, 7)", schema_name="slice")
        assert holders() == [i for i in range(3, 30) if i % 3 == 0] + [100]
        # The whole bag, not its length: a row the replay duplicated onto a
        # second worker arrives twice at weight 1 and keeps the count at 30.
        assert bag(scanned(conn, "slice", "sx"), "id", "g") == (
            {(i, 7 if i % 3 == 0 else 1000 + i): 1 for i in range(1, 30)}
            | {(100, 7): 1})


def test_the_ordered_index_of_a_top_n_view_survives_a_restart(own_server):
    """The index below a top-N view's cut is checkpointed with the view, not
    rebuilt from the base on demand: after a restart the window keeps moving
    under new writes, promotion included. A rebuilt-on-demand index would answer
    the first read correctly and then fail to promote."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("tn")
        conn.execute_sql(
            "CREATE TABLE scores (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
            "score BIGINT, name VARCHAR(30) NOT NULL)", schema_name="tn")
        conn.execute_sql(
            "CREATE VIEW top2 AS SELECT id, score FROM scores ORDER BY score DESC LIMIT 2",
            schema_name="tn")
        conn.execute_sql(
            "INSERT INTO scores VALUES (1, 0, 10, 'a'), (2, 0, 30, 'b'), (3, 0, 20, 'c')",
            schema_name="tn")
        assert bag(scanned(conn, "tn", "top2"), "id") == {(2,): 1, (3,): 1}

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        assert bag(scanned(conn, "tn", "top2"), "id") == {(2,): 1, (3,): 1}
        conn.execute_sql("DELETE FROM scores WHERE id = 2", schema_name="tn")
        assert bag(scanned(conn, "tn", "top2"), "id") == {(3,): 1, (1,): 1}
