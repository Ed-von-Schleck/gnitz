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
def fact_dim(client, schema_name):
    """`fact JOIN dim` on `k`, with `NFACTS` facts spread over `NDIMS` dims — so
    every `_join_pk` names `NFACTS // NDIMS` view rows. Yields `(sn, view id)`."""
    sn = schema_name
    for name in ("dim", "fact"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
            schema_name=sn)
    client.execute_sql(
        "CREATE VIEW jv AS SELECT f.id AS fid, d.id AS did "
        "FROM fact f JOIN dim d ON f.k = d.k", schema_name=sn)
    client.execute_sql(
        "INSERT INTO dim VALUES " + ",".join(f"({i}, {i})" for i in range(NDIMS)),
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO fact VALUES "
        + ",".join(f"({i}, {i % NDIMS})" for i in range(NFACTS)), schema_name=sn)
    return sn, client.resolve_table(sn, "jv")[0]


def test_a_join_view_seek_returns_every_row_of_its_key(client, fact_dim):
    """The ground truth is a full scan of the view grouped by key; a seek of each
    key must reproduce its whole group, weights included."""
    _, vid = fact_dim
    groups = {}
    for r in client.scan(vid, include_hidden=True):
        groups.setdefault(r["_join_pk"], []).append(r)
    assert len(groups) == NDIMS, groups

    for key, want in groups.items():
        assert len(want) == NFACTS // NDIMS, f"fixture: key {key} -> {want}"
        got = list(client.seek(vid, pk=key, include_hidden=True))
        assert bag(got, "fid", "did") == bag(want, "fid", "did"), f"seek(jv, {key})"
        assert all(r["_join_pk"] == key for r in got)

    # An absent key is an empty answer, not an error.
    assert list(client.seek(vid, pk=max(groups) + 1000)) == []


@pytest.mark.parametrize("replicated", [False, True])
def test_a_base_table_seek_stays_one_row(client, schema_name, replicated):
    """The group walk must not widen a unique PK: a base table's ingest enforces
    it, so each key's group is one row at weight 1 — under replication too, where
    every worker holds the whole table and a mis-scoped walk would answer once
    per worker."""
    sn = schema_name
    opts = " WITH (replicated = true)" if replicated else ""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)" + opts,
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 10})" for i in range(NFACTS)),
        schema_name=sn)
    tid = client.resolve_table(sn, "t")[0]
    for i in range(NFACTS):
        assert bag(client.seek(tid, pk=i)) == {(i, i * 10): 1}, f"seek(t, {i})"
    assert list(client.seek(tid, pk=NFACTS + 7)) == []


def test_an_oversized_group_errors_and_the_connection_survives(client, schema_name):
    """A seek reply leaves the worker as one frame, and a view key's group has no
    size bound — so a group past the 64 MiB frame the client accepts is rejected
    rather than emitted. The reply is a plain error: the connection stays usable.

    A seek is the one reply verb that never chunks — its consumer forwards the
    single frame verbatim — so the cap binds it whatever the schema; the TEXT
    payload also puts a string heap in that frame. The limit is a compiled-in
    constant, not a debug-only seam, so this holds in a release build as well —
    which also means the test must genuinely move ~68 MiB. Wide rows keep that
    cheap: the push path is per-row bound well below 16 KiB/row, so few-and-wide
    costs about half of many-and-narrow.
    """
    sn = schema_name
    rows_per_push, npush, width = 272, 16, 16384
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "s TEXT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW jv AS SELECT f.s AS s FROM fact f JOIN dim d ON f.k = d.k",
        schema_name=sn)
    client.execute_sql("INSERT INTO dim VALUES (1, 1)", schema_name=sn)

    # Every fact carries the same join key, so the view is one PK group of
    # `rows_per_push * npush` rows at `width` bytes each — past 64 MiB.
    # Each string is distinct: the view projects `s` alone, and a Z-set row is
    # identified by (PK, payload), so equal payloads would consolidate the
    # group down to one row at weight N.
    fact_id, fact_schema = client.resolve_table(sn, "fact")
    for p in range(npush):
        fids = range(p * rows_per_push, (p + 1) * rows_per_push)
        client.push(fact_id, gnitz.ZSetBatch(fact_schema).extend(
            {"id": fid, "k": 1, "s": f"{fid:08d}" + "x" * (width - 8)} for fid in fids))

    vid = client.resolve_table(sn, "jv")[0]
    with pytest.raises(Exception) as excinfo:
        list(client.seek(vid, pk=1))
    msg = str(excinfo.value)
    assert "67108864" in msg, f"the error must name the limit: {msg}"
    assert "seek" in msg, msg

    # The connection is intact: the next statement answers normally.
    assert bag(scanned(client, sn, "dim")) == {(1, 1): 1}
