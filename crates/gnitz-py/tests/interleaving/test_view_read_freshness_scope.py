"""A view read waits for its own sources' ticks, not for every pending tick.

Freshness is a comparison on the LSN axis, scoped by the dependency graph: a
read of view V is served immediately when every relation in V's transitive
source closure committed at or below the last completed tick's watermark. A
pending tick on a relation V does not depend on — another table's push, a DDL
bundle, a serial id-range allocation — does not force V's reader to drain.

Two properties are at stake and this file gates both:

  * **Correctness.** The watermark is written *after* the tick's worker ACKs, so
    an in-flight tick still reads as un-absorbed. A freshness test that consulted
    the pending-tick *queue* instead would pass while a large tick is mid-flight
    and serve a view that is missing every one of its rows.
  * **Scope.** A read of a clean view must not pay for an unrelated dirty one.

The transitive walk (`t -> v1 -> v2`) is gated by
`test_view_seek_freshness.py::test_view_over_view_seek_reads_your_writes`, which
enters through the same `read_lock`.
"""

import gnitz

# 40k rows: 4x the 10 000-row coalesce threshold. Crossing the threshold is what
# fires the auto-tick; the excess is the width of the window in which the tick is
# still running when the read arrives, so it is this test's sensitivity, not a
# scale knob.
BIG_ROWS = 40_000
# 5 000 rows: under the threshold, so no auto-tick fires and the tid simply sits
# pending until someone reads its view.
SMALL_ROWS = 5_000


def _push_rows(client, tid, schema, start, count):
    """One push of `count` rows with distinct `pk` AND distinct `g`, so a
    GROUP BY over `g` produces one group per row. Returns the commit's LSN."""
    batch = gnitz.ZSetBatch(schema)
    for i in range(start, start + count):
        batch.append(pk=i, g=i)
    return client.push(tid, batch)


def _create_t(client, sn, name):
    """`(tid, schema)` for a fresh two-column table. The schema comes from
    `resolve_table` rather than hand-built: a BIGINT PK is stored signed, and a
    mis-typed batch would encode a different order-preserving key."""
    client.execute_sql(
        f"CREATE TABLE {name} (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
        schema_name=sn,
    )
    return client.resolve_table(sn, name)


# ── correctness: the in-flight tick window ───────────────────────────────────

def test_read_waits_for_its_own_in_flight_tick(client, schema_name):
    """Push enough rows to fire an auto-tick, then read the dependent GROUP BY
    view on the same connection with nothing in between. The view must report
    every group, each counting exactly one row.

    The push ACKs as soon as its commit is durable; the tick it triggered is
    still running. The watermark that decides freshness is written only after
    that tick's worker ACKs land, so the read correctly sees its source as
    un-absorbed and drains. A freshness test keyed on the pending-tick queue
    would see an empty queue — the tick loop empties it at dequeue, before the
    tick body runs — and serve the view with 0 of its groups.
    """
    # A fresh table per iteration: a re-read of an already-ticked view would
    # pass regardless of the freshness verdict. Repeated because losing the
    # race is what the test detects.
    for it in range(6):
        t, v = f"t{it}", f"v{it}"
        tid, schema = _create_t(client, schema_name, t)
        client.execute_sql(
            f"CREATE VIEW {v} AS SELECT g, COUNT(*) AS n FROM {t} GROUP BY g",
            schema_name=schema_name,
        )
        vid, _ = client.resolve_table(schema_name, v)

        _push_rows(client, tid, schema, 0, BIG_ROWS)
        rows = client.scan(vid)
        assert len(rows) == BIG_ROWS, (
            f"iteration {it}: view reported {len(rows)} of {BIG_ROWS} groups — "
            "read overtook its own in-flight tick")
        # One row per group, so a tick absorbed twice keeps the group count and
        # doubles every count — invisible to the length check above.
        assert sum(r.n for r in rows) == BIG_ROWS, f"iteration {it}: counts doubled"


# ── scope: an unrelated pending tick does not gate the read ──────────────────

def test_unrelated_pending_tick_does_not_gate_the_read(client, schema_name):
    """A read of a clean `v1` must not tick `t2`, which `v1` does not depend on.

    Asserted on the watermark the scan reports, not on wall-clock: a scan's
    terminal frame carries `last_tick_lsn`, which only a completed tick moves. If
    the read drained, the watermark would jump past `t2`'s commit.
    """
    t1, schema = _create_t(client, schema_name, "t1")
    t2, _ = _create_t(client, schema_name, "t2")
    client.execute_sql(
        "CREATE VIEW v1 AS SELECT pk, g FROM t1 WHERE g >= 0", schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW v2 AS SELECT g, COUNT(*) AS n FROM t2 GROUP BY g",
        schema_name=schema_name)
    v1id, _ = client.resolve_table(schema_name, "v1")
    v2id, _ = client.resolve_table(schema_name, "v2")

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
