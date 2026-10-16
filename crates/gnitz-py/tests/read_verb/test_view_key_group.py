"""A keyed read of a view returns every row its key names.

A base table's PK names at most one row — every ingest runs the unique-PK
enforcement. A view output store enforces nothing, and a join view's synthetic
`_join_pk` **is** the join key, so it names one row per row the join produced
for it. A seek must return that whole group, at the weights a scan of the same
key reports.

The view here is deliberately **not** replicated, so its store is hashed and the
seek's routing is not part of what is under test. A hidden view key is only
reachable through the binary client, so these drive `push`/`seek`, not SQL.
"""

import gnitz
import pytest
from _read import bag, scanned
from _sql import insert

NFACTS = 40
NDIMS = 4


@pytest.fixture
def fact_dim(client):
    """`fact JOIN dim` on `k`, with `NFACTS` facts spread over `NDIMS` dims — so
    every `_join_pk`, the join key `k`, names `NFACTS // NDIMS` view rows. The
    view's id and schema."""
    for name in ("dim", "fact"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)")
    client.execute_sql(
        "CREATE VIEW jv AS SELECT f.id AS fid, d.id AS did "
        "FROM fact f JOIN dim d ON f.k = d.k")
    insert(client, "dim", [(i, i) for i in range(NDIMS)])
    insert(client, "fact", [(i, i % NDIMS) for i in range(NFACTS)])
    return client.resolve_table("jv")


def test_a_join_view_seek_returns_every_row_of_its_key(client, fact_dim):
    """A seek of each key returns its whole group in one request, weights
    included."""
    vid, schema = fact_dim
    for k in range(NDIMS):
        before = client.requests_sent
        got = client.seek(vid, schema, pk=k).including_hidden()
        assert client.requests_sent == before + 1, "a seek is one request"
        assert bag(got, "_join_pk", "fid", "did") == \
            {(k, f, k): 1 for f in range(NFACTS) if f % NDIMS == k}, f"seek(jv, {k})"

    # An absent key is an empty answer, not an error.
    assert list(client.seek(vid, schema, pk=NDIMS + 1000)) == []


def test_a_group_past_one_frame_is_returned_whole(client):
    """A view key whose group exceeds one 64 MiB frame is returned whole, and the
    connection answers the next statement."""
    rows_per_push, npush, width = 272, 16, 16384
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)")
    client.execute_sql(
        "CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "s TEXT NOT NULL)")
    client.execute_sql(
        "CREATE VIEW jv AS SELECT f.s AS s FROM fact f JOIN dim d ON f.k = d.k")
    client.execute_sql("INSERT INTO dim VALUES (1, 1)")

    # Every fact carries the same join key, so the view is one PK group of
    # `rows_per_push * npush` rows at `width` bytes each — past 64 MiB.
    # Each string is distinct: the view projects `s` alone, and a Z-set row is
    # identified by (PK, payload), so equal payloads would consolidate the
    # group down to one row at weight N.
    fact_id, fact_schema = client.resolve_table("fact")
    for p in range(npush):
        fids = range(p * rows_per_push, (p + 1) * rows_per_push)
        client.push(fact_id, gnitz.ZSetBatch(fact_schema).extend(
            {"id": fid, "k": 1, "s": f"{fid:08d}" + "x" * (width - 8)} for fid in fids))

    got = list(client.seek(*client.resolve_table("jv"), pk=1))
    assert all(r._weight == 1 and len(r.s) == width for r in got)
    assert sorted(r.s[:8] for r in got) == [f"{fid:08d}" for fid in range(rows_per_push * npush)]

    # The connection is intact: the next statement answers normally.
    assert bag(scanned(client, "dim")) == {(1, 1): 1}
