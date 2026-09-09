"""A secondary index is derived state the checkpoint persists: it resumes on its
own generation check across a clean restart, and is rehomed across a restart
at a different worker count.
"""

import os
import gnitz


















def test_index_resumes_across_clean_restart(own_server):
    """A secondary index is derived state that the checkpoint persists, so a
    restart after a graceful stop must reload it — not re-derive it from a full
    scan of the owner's slice on every worker. Correctness alone cannot show
    that: a silent rebuild produces the same holders. So this asserts the seeks
    AND that no worker rebuilt anything."""
    sock_path = own_server.sock_path

    own_server.start()
    conn = gnitz.connect(sock_path)
    conn.create_schema("idxres")
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
        schema_name="idxres",
    )
    values = ", ".join(f"({i}, {i * 10})" for i in range(64))
    conn.execute_sql(f"INSERT INTO t VALUES {values}", schema_name="idxres")
    conn.execute_sql("CREATE INDEX ON t(g)", schema_name="idxres")
    conn.close()

    # Graceful: the shutdown barrier runs a full checkpoint sequence, whose
    # ephemeral round publishes every worker's index at the committed generation.
    own_server.restart(graceful=True)

    conn = gnitz.connect(sock_path)
    tid, _ = conn.resolve_table("idxres", "t")
    for i in range(0, 64, 8):
        res = conn.seek_by_index(tid, [1], [i * 10])
        assert res.schema is not None and sorted(res.pks) == [i], (
            f"g={i * 10} must still resolve to its source PK after the restart"
        )
    conn.close()

    counts = own_server.rebuilt_index_counts()
    assert counts == [0] * own_server.workers, (
        f"a clean restart must resume every index from its checkpoint, "
        f"but workers rebuilt {counts}"
    )


def test_index_rebuilds_across_worker_count_change(own_server):
    """A restart at a different worker count moves every rank's index home at
    once: the `w{k}of4` children are retired and each launched rank re-derives
    its slice into `w{k}of2`. Asserts the seeks, that every worker rebuilt
    exactly its one index, and that no retired child survives on disk."""
    sock_path = own_server.sock_path

    own_server.start(workers=4)
    conn = gnitz.connect(sock_path)
    conn.create_schema("idxres")
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
        schema_name="idxres",
    )
    values = ", ".join(f"({i}, {i * 10})" for i in range(64))
    conn.execute_sql(f"INSERT INTO t VALUES {values}", schema_name="idxres")
    conn.execute_sql("CREATE INDEX ON t(g)", schema_name="idxres")
    conn.close()

    own_server.restart(graceful=True, workers=2)

    conn = gnitz.connect(sock_path)
    tid, _ = conn.resolve_table("idxres", "t")
    for i in range(0, 64, 8):
        res = conn.seek_by_index(tid, [1], [i * 10])
        assert res.schema is not None and sorted(res.pks) == [i], (
            f"g={i * 10} must still resolve to its source PK after the relayout"
        )
    conn.close()

    counts = own_server.rebuilt_index_counts()
    assert counts == [1, 1], (
        f"every launched rank must re-derive its slice of the one index, "
        f"but workers rebuilt {counts}"
    )
    table_dir = os.path.join(own_server.data_dir, "idxres", f"t_{tid}")
    idx_dirs = [d for d in os.listdir(table_dir) if d.startswith("idx_")]
    assert idx_dirs, f"the index directory must survive under {table_dir}"
    for idx in idx_dirs:
        children = sorted(os.listdir(os.path.join(table_dir, idx)))
        assert children == ["w0of2", "w1of2"], (
            f"only the launched ranks' children may remain under {idx}, got {children}"
        )
