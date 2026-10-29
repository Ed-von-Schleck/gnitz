"""A delta feed read under a subscription: a `SELECT` over the view whose WHERE
and column list the server applies to every bootstrap and every poll.

A filter and a projection are linear, so the subscription's deltas are the
view's deltas under it, and a copy built from one bootstrap and its polls must
equal the same `SELECT` over the view. As everywhere in the feed, the
comparison is of weights: a retraction the filter dropped on one side of an
UPDATE and kept on the other is the failure a row-set test cannot see.

That a subscription is a plain rows read, and that an UPDATE of a column it
drops ships nothing, are the planner's and the store's own tests.
"""
import pytest
import gnitz
from _feedviews import GROUPBY, JOIN, LINEAR, Subscriber, base_tables, churn, fed_view, mk_feed
from _read import access, rows
from _sql import insert


# `(fed view, subscription over it)`: `lin` over LINEAR, `jn` over JOIN,
# `grp` over GROUPBY.
_SUBSCRIPTIONS = [
    ("lin", "SELECT id, v, body FROM lin WHERE v % 2 = 0"),
    ("lin", "SELECT id, v FROM lin"),
    ("lin", "SELECT id, body FROM lin WHERE id >= 100 AND id < 300"),
    ("lin", "SELECT id, v FROM lin WHERE id IN (5, 77, 150, 420, 9999)"),
    ("lin", "SELECT id FROM lin WHERE id = 150"),
    ("lin", "SELECT v FROM lin WHERE v > 300"),
    ("lin", "SELECT id, v FROM lin WHERE body LIKE '%7' AND v > 300"),
    ("jn", "SELECT id, w FROM jn WHERE w > 700"),
    ("grp", "SELECT tid, total FROM grp WHERE total > 1000"),
    ("grp", "SELECT tid, n FROM grp WHERE tid IN (3, 50, 250)"),
]


def test_a_subscribed_copy_is_the_select_over_the_view(client):
    """Every subscription, each its own copy over one churn: through inserts,
    UPDATEs that move rows across the filter, single-key rounds most workers
    emit nothing for, and deletes. A key list and a key range reach only the
    workers that own them, which is where a wrong route would lose a row."""
    base_tables(client)
    for name, body in (("lin", LINEAR), ("jn", JOIN), ("grp", GROUPBY)):
        mk_feed(client, name, body)
    churn(client, 1, 200)
    subs = [Subscriber(client, name, sql) for name, sql in _SUBSCRIPTIONS]

    def converged(what):
        return [sub.assert_converged(f"{sub.sql}: {what}") for sub in subs]

    for sub in subs:
        sub.bootstrap()
    assert all(converged("after the bootstrap")), "every subscription keeps something"
    for lo, hi in ((201, 400), (401, 600)):
        churn(client, lo, hi)
        converged(f"through key {hi}")
    # An odd step flips `v % 2` and carries each of these keys over `v > 300`.
    for i in (150, 77, 5):
        client.execute_sql(f"UPDATE t SET v = v + 301 WHERE id = {i}")
        client.execute_sql(f"UPDATE u SET w = w + 500 WHERE id = {i}")
        converged(f"single-key round {i}")
    client.execute_sql("DELETE FROM t WHERE id IN (150, 420)")
    converged("after the deletes")


def test_a_subscription_bounded_by_the_views_index(client):
    """The planner bounds the read by the view's index: an index walk at the
    bootstrap, the same range as a filter over the feed past it."""
    fed_view(client)
    client.execute_sql("CREATE INDEX by_v ON f(v)")
    churn(client, 1, 2000)
    sql = "SELECT id, v FROM f WHERE v >= 900 AND v < 1500"
    assert access(client, sql) == "index range on (v)"
    sub = Subscriber(client, "f", sql)
    sub.bootstrap()
    for lo, hi in ((2001, 2400), (2401, 2600)):
        churn(client, lo, hi)
        client.execute_sql(f"UPDATE t SET v = 1000 WHERE id >= {lo} AND id < {lo + 20}")
        client.execute_sql("UPDATE t SET v = 5 WHERE id >= 320 AND id < 330")
        assert sub.assert_converged(f"through key {hi}") > 0


def test_a_cursor_of_another_subscription_is_refused(client):
    """The copy a cursor belongs to is one subscription's. Polled under another
    it would apply that one's deltas to this one's rows, so the tag differs and
    the poll is refused as any foreign cursor is."""
    fed_view(client)
    churn(client, 1, 200)
    a = Subscriber(client, "f", "SELECT id, v FROM f WHERE v > 100")
    b = Subscriber(client, "f", "SELECT id, v FROM f WHERE v > 200")
    a.bootstrap()
    b.bootstrap()
    _, whole = client.delta_read(a.vid, a.view_schema)
    assert len({a.cursor[0], b.cursor[0], whole[0]}) == 3
    churn(client, 201, 300)
    b.cursor = a.cursor
    with pytest.raises(gnitz.GnitzDeltaExpiredError):
        b.poll()
    with pytest.raises(gnitz.GnitzDeltaExpiredError):
        client.delta_read(a.vid, a.view_schema, a.cursor)


def test_a_synced_cursor_passes_the_rounds_it_kept_nothing_of(client, peer):
    """A subscription that keeps almost nothing, over a budget its view's
    rounds overrun: each sync moves its cursor past the rounds that left it no
    row, so a reader that resumes from that cursor is not refused as expired —
    as one that stayed at its bootstrap is."""
    one = "SELECT id, v FROM f WHERE id = 5"
    wide = lambda i: f"wide-{i:0>300}"
    base_tables(client)
    mk_feed(client, "f", LINEAR, feed="1 KB")
    client.execute_sql(f"INSERT INTO t VALUES (5, 50, '{wide(5)}')")
    sub = Subscriber(peer(), "f", one)
    sub.bootstrap()
    sub.subscribe()
    stayed = Subscriber(client, "f", one)
    stayed.bootstrap()
    for r in range(12):
        lo = 100 + r * 40
        insert(client, "t", [(i, i, wide(i)) for i in range(lo, lo + 40)])
        # The read ticks the round.
        assert len(rows(client, one)) == 1
        assert len(sub.sync()) == 0
        assert sub.copy == sub.scan()

    with pytest.raises(gnitz.GnitzDeltaExpiredError):
        stayed.poll()
    resumed = Subscriber(peer(), "f", one)
    resumed.copy, resumed.cursor = dict(sub.copy), sub.cursor
    assert len(resumed.poll()) == 0
    client.execute_sql("UPDATE t SET v = v + 1 WHERE id = 5")
    resumed.poll()
    assert resumed.copy == resumed.scan() == {(5, 51): 1}
