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

NFACTS = 40
NDIMS = 4


@pytest.fixture
def fact_dim(client):
    """`fact JOIN dim` on `k`, with `NFACTS` facts spread over `NDIMS` dims — so
    every `_join_pk` names `NFACTS // NDIMS` view rows. The view's id and schema."""
    for name in ("dim", "fact"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)")
    client.execute_sql(
        "CREATE VIEW jv AS SELECT f.id AS fid, d.id AS did "
        "FROM fact f JOIN dim d ON f.k = d.k")
    client.execute_sql(
        "INSERT INTO dim VALUES " + ",".join(f"({i}, {i})" for i in range(NDIMS)))
    client.execute_sql(
        "INSERT INTO fact VALUES "
        + ",".join(f"({i}, {i % NDIMS})" for i in range(NFACTS)))
    return client.resolve_table("jv")


def test_a_join_view_seek_returns_every_row_of_its_key(client, fact_dim):
    """The ground truth is a full scan of the view grouped by key; a seek of each
    key must reproduce its whole group, weights included."""
    vid, schema = fact_dim
    groups = {}
    before = client.requests_sent
    for r in client.scan(vid, schema).including_hidden():
        groups.setdefault(r["_join_pk"], []).append(r)
    assert client.requests_sent == before + 1, "a scan is one request"
    assert len(groups) == NDIMS, groups

    for key, want in groups.items():
        assert len(want) == NFACTS // NDIMS, f"fixture: key {key} -> {want}"
        before = client.requests_sent
        got = list(client.seek(vid, schema, pk=key).including_hidden())
        assert client.requests_sent == before + 1, "a seek is one request"
        assert bag(got, "fid", "did") == bag(want, "fid", "did"), f"seek(jv, {key})"
        assert all(r["_join_pk"] == key for r in got)

    # An absent key is an empty answer, not an error.
    assert list(client.seek(vid, schema, pk=max(groups) + 1000)) == []


@pytest.mark.parametrize("replicated", [False, True])
def test_a_base_table_seek_stays_one_row(client, replicated):
    """The group walk must not widen a unique PK: a base table's ingest enforces
    it, so each key's group is one row at weight 1 — under replication too, where
    every worker holds the whole table and a mis-scoped walk would answer once
    per worker."""
    opts = " WITH (replicated = true)" if replicated else ""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)" + opts)
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 10})" for i in range(NFACTS)))
    tid, schema = client.resolve_table("t")
    for i in range(NFACTS):
        assert bag(client.seek(tid, schema, pk=i)) == {(i, i * 10): 1}, f"seek(t, {i})"
    assert list(client.seek(tid, schema, pk=NFACTS + 7)) == []


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
