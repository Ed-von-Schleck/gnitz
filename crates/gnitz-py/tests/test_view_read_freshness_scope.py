"""A view read waits for its own sources' ticks, not for every pending tick.

Freshness is a comparison on the LSN axis, scoped by the dependency graph: a
read of view V is served immediately when every relation in V's transitive
source closure committed at or below the last completed tick's watermark. A
pending tick on a relation V does not depend on — another table's push, a DDL
bundle, a serial id-range allocation — no longer forces V's reader to drain.

Two properties are at stake and this file gates both:

  * **Correctness.** The watermark is written *after* the tick's worker ACKs, so
    an in-flight tick still reads as un-absorbed. A freshness test that consulted
    the pending-tick *queue* instead would pass while a large tick is mid-flight
    and serve a view that is missing every one of its rows.
  * **Scope.** A read of a clean view must not pay for an unrelated dirty one.

The transitive walk (`t -> v1 -> v2`) is gated by
`test_view_seek_freshness.py::test_view_over_view_seek_reads_your_writes`, which
enters through the same `read_lock`.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_view_read_freshness_scope.py -v --tb=short
"""
import random

import gnitz


# 40k rows: 4x the 10 000-row coalesce threshold, so the push fires an auto-tick,
# and heavy enough that the tick is still in flight when the read arrives — the
# window a queue-based freshness test would read straight through.
BIG_ROWS = 40_000
# 5 000 rows: under the threshold, so no auto-tick fires and the tid simply sits
# pending until someone reads its view.
SMALL_ROWS = 5_000


def _uid():
    return str(random.randint(100000, 999999))


def _schema():
    return gnitz.Schema([
        gnitz.ColumnDef("pk", gnitz.TypeCode.I64, primary_key=True),
        gnitz.ColumnDef("g", gnitz.TypeCode.I64),
    ])


def _push_rows(client, tid, schema, start, count):
    """One push of `count` rows with distinct `pk` AND distinct `g`, so a
    GROUP BY over `g` produces one group per row. Returns the commit's LSN."""
    batch = gnitz.ZSetBatch(schema)
    for i in range(start, start + count):
        batch.append(pk=i, g=i)
    return client.push(tid, batch)


def _create_t(client, sn, name):
    client.execute_sql(
        f"CREATE TABLE {name} (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
        schema_name=sn,
    )
    tid, _ = client.resolve_table(sn, name)
    return tid


# ── correctness: the in-flight tick window ───────────────────────────────────

def test_read_waits_for_its_own_in_flight_tick(client):
    """Push enough rows to fire an auto-tick, then read the dependent GROUP BY
    view on the same connection with nothing in between. The view must report
    every group.

    The push ACKs as soon as its commit is durable; the tick it triggered is
    still running. The watermark that decides freshness is written only after
    that tick's worker ACKs land, so the read correctly sees its source as
    un-absorbed and drains. A freshness test keyed on the pending-tick queue
    would see an empty queue — the tick loop empties it at dequeue, before the
    tick body runs — and serve the view with 0 of its groups.
    """
    sn = "vrf" + _uid()
    client.create_schema(sn)
    schema = _schema()
    try:
        # A fresh table per iteration: a re-read of an already-ticked view would
        # pass regardless of the freshness verdict. Repeated because losing the
        # race is what the test detects.
        for it in range(6):
            t, v = f"t{it}", f"v{it}"
            tid = _create_t(client, sn, t)
            client.execute_sql(
                f"CREATE VIEW {v} AS SELECT g, COUNT(*) AS n FROM {t} GROUP BY g",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, v)

            _push_rows(client, tid, schema, 0, BIG_ROWS)
            rows = client.scan(vid)
            assert len(rows) == BIG_ROWS, \
                f"iteration {it}: view reported {len(rows)} of {BIG_ROWS} groups — read overtook its own in-flight tick"
    finally:
        client.drop_schema(sn)


# ── scope: an unrelated pending tick does not gate the read ──────────────────

def test_unrelated_pending_tick_does_not_gate_the_read(client):
    """A read of a clean `v1` must not tick `t2`, which `v1` does not depend on.

    Asserted on the watermark the scan reports, not on wall-clock: a scan's
    terminal frame carries `last_tick_lsn`, which only a completed tick moves. If
    the read drained, the watermark would jump past `t2`'s commit.
    """
    sn = "vrf" + _uid()
    client.create_schema(sn)
    schema = _schema()
    try:
        t1 = _create_t(client, sn, "t1")
        t2 = _create_t(client, sn, "t2")
        client.execute_sql("CREATE VIEW v1 AS SELECT pk, g FROM t1 WHERE g >= 0", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v2 AS SELECT g, COUNT(*) AS n FROM t2 GROUP BY g", schema_name=sn)
        v1id, _ = client.resolve_table(sn, "v1")
        v2id, _ = client.resolve_table(sn, "v2")

        _push_rows(client, t1, schema, 0, 10)
        client.scan(v1id)                 # drains t1; v1 is now clean
        before = client.scan(v1id).lsn    # fresh: no drain, so the watermark stands

        # Under the coalesce threshold, so no auto-tick fires: t2 sits pending.
        push_lsn = _push_rows(client, t2, schema, 0, SMALL_ROWS)
        after = client.scan(v1id).lsn
        assert after == before and after < push_lsn, (
            f"read of a clean view ticked an unrelated relation "
            f"(watermark {before} -> {after}, unrelated push at {push_lsn})"
        )

        # And the drain is not simply broken: v2's own read does absorb it.
        assert client.scan(v2id).lsn >= push_lsn, "a read of the dirty view must drain its source"
    finally:
        client.drop_schema(sn)
