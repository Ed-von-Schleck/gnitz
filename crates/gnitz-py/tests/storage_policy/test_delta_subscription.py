"""A delta feed read under a subscription: a `SELECT` over the view whose WHERE
and column list the server applies to every bootstrap and every poll.

A filter and a projection are linear, so the subscription's deltas are the
view's deltas under it, and a copy built from one bootstrap and its polls must
equal the same `SELECT` over the view. As everywhere in the feed, the
comparison is of weights: a retraction the filter dropped on one side of an
UPDATE and kept on the other is the failure a row-set test cannot see.
"""
import pytest
import gnitz
from _feedviews import GROUPBY, JOIN, LINEAR, Subscriber, base_tables, churn, mk_feed
from _read import rows


CASES = [
    # (view body, subscription over `f`, the same filter in Python, columns kept)
    (LINEAR, "SELECT id, v, body FROM f WHERE v % 2 = 0", lambda r: r.v % 2 == 0, ("id", "v", "body")),
    (LINEAR, "SELECT id, v FROM f", lambda r: True, ("id", "v")),
    (LINEAR, "SELECT id, body FROM f WHERE id >= 100 AND id < 300", lambda r: 100 <= r.id < 300, ("id", "body")),
    (LINEAR, "SELECT id, v FROM f WHERE id IN (5, 77, 150, 420, 9999)", lambda r: r.id in (5, 77, 150, 420, 9999), ("id", "v")),
    (LINEAR, "SELECT id FROM f WHERE id = 150", lambda r: r.id == 150, ("id",)),
    (LINEAR, "SELECT v FROM f WHERE v > 300", lambda r: r.v > 300, ("v",)),
    (LINEAR, "SELECT id, v FROM f WHERE body LIKE '%7' AND v > 300", lambda r: r.body.endswith("7") and r.v > 300, ("id", "v")),
    (JOIN, "SELECT id, w FROM f WHERE w > 700", lambda r: r.w > 700, ("id", "w")),
    (GROUPBY, "SELECT tid, total FROM f WHERE total > 1000", lambda r: r.total > 1000, ("tid", "total")),
    (GROUPBY, "SELECT tid, n FROM f WHERE tid IN (3, 50, 250)", lambda r: r.tid in (3, 50, 250), ("tid", "n")),
]


@pytest.mark.parametrize("body, sql, keep, cols", CASES, ids=[c[1] for c in CASES])
def test_a_subscribed_copy_is_the_select_over_the_view(client, body, sql, keep, cols):
    """Through inserts, UPDATEs that move rows across the filter, single-key
    rounds most workers emit nothing for, and deletes. A key list and a key
    range reach only the workers that own them, which is where a wrong route
    would lose a row."""
    base_tables(client)
    mk_feed(client, "f", body)
    churn(client, 1, 200)
    sub = Subscriber(client, "f", sql, keep, cols)
    sub.bootstrap()
    held = sub.assert_converged("after the bootstrap")
    for lo, hi in ((201, 400), (401, 600)):
        churn(client, lo, hi)
        held = max(held, sub.assert_converged(f"through key {hi}"))
    for i in (150, 77, 5):
        client.execute_sql(f"UPDATE t SET v = v + 2 WHERE id = {i}")
        client.execute_sql(f"UPDATE u SET w = w + 500 WHERE id = {i}")
        sub.assert_converged(f"single-key round {i}")
    client.execute_sql("DELETE FROM t WHERE id IN (150, 420)")
    held = max(held, sub.assert_converged("after the deletes"))
    assert held > 0, "the subscription kept nothing at any point, so the comparison agreed about nothing"


def test_a_subscription_bounded_by_the_views_index(client):
    """The planner bounds the read by the view's index: an index walk at the
    bootstrap, the same range as a filter over the feed past it."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    client.execute_sql("CREATE INDEX by_v ON f(v)")
    churn(client, 1, 2000)
    sub = Subscriber(client, "f", "SELECT id, v FROM f WHERE v >= 900 AND v < 1500", lambda r: 900 <= r.v < 1500, ("id", "v"))
    sub.bootstrap()
    for lo, hi in ((2001, 2400), (2401, 2600)):
        churn(client, lo, hi)
        client.execute_sql(f"UPDATE t SET v = 1000 WHERE id >= {lo} AND id < {lo + 20}")
        client.execute_sql("UPDATE t SET v = 5 WHERE id >= 320 AND id < 330")
        assert sub.assert_converged(f"through key {hi}") > 0


def test_an_update_of_a_column_the_subscription_drops_ships_nothing(client):
    """An UPDATE is a retraction and an insert. Under a projection that drops
    the column it changed they are one row, and cancel before the reply."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    churn(client, 1, 200)
    slim = Subscriber(client, "f", "SELECT id, v FROM f", lambda r: True, ("id", "v"))
    wide = Subscriber(client, "f", "SELECT id, v, body FROM f", lambda r: True, ("id", "v", "body"))
    for s in (slim, wide):
        s.bootstrap()
        s.assert_converged("bootstrap")
    client.execute_sql("UPDATE t SET body = 'rewritten' WHERE id < 100")
    assert len(wide.poll()) > 100
    assert len(slim.poll()) == 0
    slim.assert_converged("after")


def test_a_cursor_of_another_subscription_is_refused(client):
    """The copy a cursor belongs to is one subscription's. Polled under another
    it would apply that one's deltas to this one's rows, so the tag differs and
    the poll is refused as any foreign cursor is."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    churn(client, 1, 200)
    a = Subscriber(client, "f", "SELECT id, v FROM f WHERE v > 100", lambda r: r.v > 100, ("id", "v"))
    b = Subscriber(client, "f", "SELECT id, v FROM f WHERE v > 200", lambda r: r.v > 200, ("id", "v"))
    a.bootstrap()
    b.bootstrap()
    _, whole = client.delta_bootstrap(a.vid, a.view_schema)
    assert len({a.cursor[0], b.cursor[0], whole[0]}) == 3
    churn(client, 201, 300)
    b.cursor = a.cursor
    with pytest.raises(gnitz.GnitzDeltaExpiredError):
        b.poll()
    with pytest.raises(gnitz.GnitzDeltaExpiredError):
        client.delta_poll(a.vid, a.view_schema, a.cursor)


def test_a_synced_cursor_passes_the_rounds_it_kept_nothing_of(own_server):
    """A subscription that keeps almost nothing, over a budget its view's
    rounds overrun: each sync moves its cursor past the rounds that left it no
    row, so a reader that resumes from that cursor is not refused as expired —
    as one that stayed at its bootstrap is."""
    own_server.start()
    target = own_server.target
    one = ("f", "SELECT id, v FROM f WHERE id = 5", lambda r: r.id == 5, ("id", "v"))
    wide = lambda i: f"wide-{i:0>300}"
    with gnitz.connect(target) as writer, gnitz.connect(target) as reader:
        base_tables(writer)
        mk_feed(writer, "f", LINEAR, feed="1 KB")
        writer.execute_sql(f"INSERT INTO t VALUES (5, 50, '{wide(5)}')")
        sub = Subscriber(reader, *one)
        sub.bootstrap()
        sub.subscribe()
        stayed = Subscriber(writer, *one)
        stayed.bootstrap()
        for r in range(12):
            lo = 100 + r * 40
            writer.execute_sql(
                "INSERT INTO t VALUES " + ",".join(f"({i}, {i}, '{wide(i)}')" for i in range(lo, lo + 40)),
            )
            # The read ticks the round.
            assert len(rows(writer, "SELECT id FROM f WHERE id = 5")) == 1
            assert len(sub.sync()) == 0
            assert sub.copy == sub.scan()

        with pytest.raises(gnitz.GnitzDeltaExpiredError):
            stayed.poll()
        with gnitz.connect(target) as other:
            resumed = Subscriber(other, *one)
            resumed.copy, resumed.cursor = dict(sub.copy), sub.cursor
            assert len(resumed.poll()) == 0
            writer.execute_sql("UPDATE t SET v = v + 1 WHERE id = 5")
            resumed.poll()
            assert resumed.copy == resumed.scan() == {(5, 51): 1}


@pytest.mark.parametrize(
    "sql",
    ["SELECT COUNT(*) FROM f", "SELECT DISTINCT v FROM f", "SELECT id FROM f ORDER BY v", "SELECT id FROM f LIMIT 3"],
)
def test_a_subscription_is_a_plain_select(client, sql):
    """A delta of an aggregate, an order or a cut is not that read of a delta."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    with pytest.raises(gnitz.GnitzError, match="subscription"):
        client.subscription(sql)
