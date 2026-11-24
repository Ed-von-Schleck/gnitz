"""A keyed read of a view returns every row its key names.

A base table's PK names at most one row — every ingest runs the unique-PK
enforcement. A view output store enforces nothing, and a join view's synthetic
`_join_pk` **is** the join key, so it names one row per row the join produced
for it. A seek must return that whole group from the worker its key hashes to,
at the weights a scan of the same key reports. A hidden view key is only
reachable through the binary client.
"""

import gnitz
from _read import bag
from _serverproc import TINY_REPLY_FRAMES


def test_a_join_view_seek_returns_every_row_of_its_key(own_server):
    """Every fact joins one of four dims, so each `_join_pk` names a quarter of
    the facts — ~100 KiB of distinct payload, many 16 KiB frames. A seek of each
    key returns its whole group, a retracted fact leaves exactly its own row,
    and an absent key is an empty answer."""
    nfacts, ndims, width = 400, 4, 1024
    with gnitz.connect(own_server.start(extra_env=TINY_REPLY_FRAMES).target) as client:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL); "
            "CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, s TEXT NOT NULL); "
            "CREATE VIEW jv AS SELECT f.id AS fid, d.id AS did, f.s AS s "
            "FROM fact f JOIN dim d ON f.k = d.k")
        client.execute_sql("INSERT INTO dim VALUES " + ", ".join(f"({i}, {i})" for i in range(ndims)))
        # Distinct strings: a Z-set row is (PK, payload), so equal payloads would
        # consolidate a group down to one row.
        facts = {f: f"{f:08d}".ljust(width, "x") for f in range(nfacts)}
        fact_id, fact_schema = client.resolve_table("fact")
        client.push(fact_id, gnitz.ZSetBatch(fact_schema).extend(
            {"id": f, "k": f % ndims, "s": s} for f, s in facts.items()))
        client.execute_sql("DELETE FROM fact WHERE id = 5")
        del facts[5]

        vid, schema = client.resolve_table("jv")
        for k in range(ndims):
            got = client.seek(vid, schema, pk=k).including_hidden()
            assert bag(got, "_join_pk", "fid", "did", "s") == \
                {(k, f, k, s): 1 for f, s in facts.items() if f % ndims == k}, f"seek(jv, {k})"
        assert list(client.seek(vid, schema, pk=ndims + 1000)) == []
