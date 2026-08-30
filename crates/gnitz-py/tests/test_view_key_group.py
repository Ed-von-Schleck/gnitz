"""E2E: a keyed read of a view returns every row its key names.

A base table's PK names at most one row — every ingest runs the unique-PK
enforcement. A view output store enforces nothing, and a join view's synthetic
`_join_pk` **is** the join key, so it names one row per row the join produced
for it. A seek must return that whole group: the same rows a `pk = k` range read
of the key returns.

The view here is deliberately **not** replicated, so its store is hashed and the
seek's routing is not part of what is under test.

Run with GNITZ_WORKERS=4 and =1 (a hidden view key is only reachable through the
binary client, so these drive `push`/`seek`, not SQL):
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_view_key_group.py -v --tb=short
"""

import pytest
import gnitz
from _uid import uid as _uid

NFACTS = 40
NDIMS = 4




def _cleanup(client, sn, *names):
    for name in names:
        for kind in ("VIEW", "TABLE"):
            try:
                client.execute_sql(f"DROP {kind} {name}", schema_name=sn)
            except Exception:
                pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _fact_dim_view(client, sn):
    """`fact JOIN dim` on `k`, with `NFACTS` facts spread over `NDIMS` dims — so
    every `_join_pk` names `NFACTS // NDIMS` view rows. Returns the view id."""
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW jv AS SELECT f.id AS fid, d.id AS did "
        "FROM fact f JOIN dim d ON f.k = d.k",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO dim VALUES " + ",".join(f"({i}, {i})" for i in range(NDIMS)),
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO fact VALUES "
        + ",".join(f"({i}, {i % NDIMS})" for i in range(NFACTS)),
        schema_name=sn)
    return client.resolve_table(sn, "jv")[0]


def _groups(client, vid):
    """The view's true contents per key, from a full scan — the ground truth a
    seek of each key must reproduce."""
    out = {}
    for r in client.scan(vid, include_hidden=True):
        out.setdefault(r["_join_pk"], []).append((r["fid"], r["did"]))
    return out


def test_join_view_seek_returns_every_row_of_its_key(client):
    sn = "vkg" + _uid()
    try:
        vid = _fact_dim_view(client, sn)
        groups = _groups(client, vid)
        assert len(groups) == NDIMS, groups
        for key, want in groups.items():
            assert len(want) == NFACTS // NDIMS, f"fixture: key {key} -> {want}"
            got = [(r["fid"], r["did"]) for r in client.seek(vid, pk=key, include_hidden=True)]
            assert sorted(got) == sorted(want), f"seek(jv, {key}) answered {len(got)} of {len(want)}"
            assert all(r["_join_pk"] == key for r in client.seek(vid, pk=key, include_hidden=True))
    finally:
        _cleanup(client, sn, "jv", "fact", "dim")


def test_join_view_seek_of_an_absent_key_is_empty(client):
    sn = "vkg" + _uid()
    try:
        vid = _fact_dim_view(client, sn)
        absent = max(_groups(client, vid)) + 1000
        assert list(client.seek(vid, pk=absent)) == []
    finally:
        _cleanup(client, sn, "jv", "fact", "dim")


@pytest.mark.parametrize("replicated", [False, True])
def test_base_table_seek_stays_one_row(client, replicated):
    """The group walk must not widen a unique PK: a base table's ingest enforces
    it, so each key's group is one row — under replication too, where every
    worker holds the whole table."""
    sn = "vkg" + _uid()
    try:
        client.create_schema(sn)
        opts = " WITH (replicated = true)" if replicated else ""
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)" + opts,
            schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 10})" for i in range(NFACTS)),
            schema_name=sn)
        tid = client.resolve_table(sn, "t")[0]
        for i in range(NFACTS):
            rows = list(client.seek(tid, pk=i))
            assert len(rows) == 1, f"seek(t, {i}) returned {len(rows)} rows"
            assert (rows[0].id, rows[0].v) == (i, i * 10)
        assert list(client.seek(tid, pk=NFACTS + 7)) == []
    finally:
        _cleanup(client, sn, "t")


def test_oversized_group_errors_and_the_connection_survives(client):
    """A seek reply leaves the worker as one frame, and a view key's group has no
    size bound — so a group past the 64 MiB frame the client accepts is rejected
    rather than emitted. The reply is a plain error: the connection stays usable.

    The payload is TEXT, the ordinary string type here, so the case also covers
    the blob-bearing reply shape (which never passes through the chunking frame
    budget). The limit is a compiled-in constant, not a debug-only seam, so this
    holds in a release build as well — which also means the test must genuinely
    move ~68 MiB. Wide rows keep that cheap: the push path is per-row bound well
    below 16 KiB/row, so few-and-wide costs about half of many-and-narrow.
    """
    sn = "vkg" + _uid()
    rows_per_push, npush, width = 272, 16, 16384
    try:
        client.create_schema(sn)
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, s TEXT NOT NULL)",
            schema_name=sn)
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
            batch = gnitz.ZSetBatch(fact_schema)
            for i in range(rows_per_push):
                fid = p * rows_per_push + i
                batch.append(id=fid, k=1, s=f"{fid:08d}" + "x" * (width - 8))
            client.push(fact_id, batch)

        vid = client.resolve_table(sn, "jv")[0]
        with pytest.raises(Exception) as excinfo:
            list(client.seek(vid, pk=1))
        msg = str(excinfo.value)
        assert "67108864" in msg, f"the error must name the limit: {msg}"
        assert "seek" in msg and "wire_size" in msg, msg

        # The connection is intact: the next statement answers normally.
        res = client.execute_sql("SELECT id FROM dim", schema_name=sn)[0]
        assert [r.id for r in res["rows"]] == [1]
    finally:
        _cleanup(client, sn, "jv", "fact", "dim")
