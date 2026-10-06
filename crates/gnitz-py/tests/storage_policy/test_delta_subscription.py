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
from _feedviews import GROUPBY, JOIN, LINEAR, base_tables, churn, mk_feed
from _read import bag


class Subscribed:
    """A hand-driven copy of `sql`'s rows, and the same filter in Python over a
    plain scan of the view to hold it against."""

    def __init__(self, client, view, sql, keep, cols):
        self.client, self.keep, self.cols = client, keep, cols
        self.view_id, self.view_schema = client.resolve_table(view)
        vid, self.schema, self.spec = client.subscription(sql)
        assert vid == self.view_id
        self.copy, self.cursor, self.last = {}, None, None

    def bootstrap(self):
        rows, self.cursor = self.client.delta_bootstrap(self.view_id, self.schema, self.spec)
        self.copy = bag(rows, *self.cols)

    def poll(self):
        rows, self.cursor = self.client.delta_poll(self.view_id, self.schema, self.cursor, spec=self.spec)
        self.last = list(rows)
        for k, w in bag(self.last, *self.cols).items():
            self.copy[k] = self.copy.get(k, 0) + w
            if self.copy[k] == 0:
                del self.copy[k]

    def want(self):
        full = self.client.scan(self.view_id, self.view_schema)
        return bag([r for r in full if self.keep(r)], *self.cols)

    def assert_converged(self, what):
        self.poll()
        self.poll()
        assert self.last == [], "one poll left a round behind"
        want = self.want()
        assert self.copy == want, what
        return len(want)


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
    sub = Subscribed(client, "f", sql, keep, cols)
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
    sub = Subscribed(client, "f", "SELECT id, v FROM f WHERE v >= 900 AND v < 1500", lambda r: 900 <= r.v < 1500, ("id", "v"))
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
    slim = Subscribed(client, "f", "SELECT id, v FROM f", lambda r: True, ("id", "v"))
    wide = Subscribed(client, "f", "SELECT id, v, body FROM f", lambda r: True, ("id", "v", "body"))
    for s in (slim, wide):
        s.bootstrap()
        s.assert_converged("bootstrap")
    client.execute_sql("UPDATE t SET body = 'rewritten' WHERE id < 100")
    wide.poll()
    slim.poll()
    assert len(wide.last) > 100
    assert slim.last == []
    slim.assert_converged("after")


def test_a_cursor_of_another_subscription_is_refused(client):
    """The copy a cursor belongs to is one subscription's. Polled under another
    it would apply that one's deltas to this one's rows, so the tag differs and
    the poll is refused as any foreign cursor is."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    churn(client, 1, 200)
    a = Subscribed(client, "f", "SELECT id, v FROM f WHERE v > 100", lambda r: r.v > 100, ("id", "v"))
    b = Subscribed(client, "f", "SELECT id, v FROM f WHERE v > 200", lambda r: r.v > 200, ("id", "v"))
    a.bootstrap()
    b.bootstrap()
    _, whole = client.delta_bootstrap(a.view_id, a.view_schema)
    assert len({a.cursor[0], b.cursor[0], whole[0]}) == 3
    churn(client, 201, 300)
    b.cursor = a.cursor
    with pytest.raises(gnitz.GnitzDeltaExpiredError):
        b.poll()
    with pytest.raises(gnitz.GnitzDeltaExpiredError):
        client.delta_poll(a.view_id, a.view_schema, a.cursor)


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
