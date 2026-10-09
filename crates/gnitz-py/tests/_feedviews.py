"""The base tables, view bodies and churn the storage-policy suites share, and
the weight-exact comparison over the fed ones.

`storage_policy/test_delta_feed.py` drives the feed by hand — bootstrap, poll,
apply — and `client_surface/test_mirror.py` drives the same views through a
mirroring client, which does that for it. Both need the same base tables, the
same churn (inserts, an UPDATE and a DELETE, so a round carries retractions and
not just insertions), and the same view bodies; `Subscriber` is the by-hand feed client. Keeping one copy here is what makes "the mirror agrees with the
feed" a comparison of two mechanisms rather than of two setups.
`storage_policy/test_capacity_bounded_views.py` runs the same bodies bounded, so
"capacity is invisible" is a claim about the shapes the other two already cover.
"""

import threading
import time

import gnitz
from _read import bag

# Big enough that nothing built on these helpers falls off the window; a test
# about retention sets its own.
FEED = "32 MB"

# `own_server.extra_env` for a server whose stores sweep on modest data — the
# capacity cases. A store spills once its RAM tier crosses this ceiling, which
# only happens at a memtable fold or a checkpoint's ephemeral round — so the
# checkpoint threshold is squeezed too.
SWEEP_ENV = {"GNITZ_RAM_TIER_BYTES": "1024", "GNITZ_CHECKPOINT_BYTES": str(32 * 1024)}

LINEAR = "SELECT id, v, body FROM t WHERE v > 10"
JOIN = "SELECT t.id, t.body, u.w FROM t JOIN u ON t.id = u.tid"
GROUPBY = "SELECT tid, COUNT(*) AS n, SUM(w) AS total FROM u GROUP BY tid"
SETOP = "SELECT id FROM t EXCEPT SELECT tid FROM u"


class Subscriber:
    """A client-side copy of a view, maintained the way the feed intends: one
    bootstrap, then polls, applying weights. It is deliberately not a set — the
    whole content of a delta row is its weight.

    With `sql`, the copy is that `SELECT` over the view instead of the view
    whole, and is held against the same filter in Python — `keep`, over the
    columns `cols` — applied to a plain scan of the view."""

    def __init__(self, client, name, sql=None, keep=lambda r: True, cols=()):
        self.client, self.keep, self.cols = client, keep, cols
        self.vid, self.view_schema = client.resolve_table(name)
        self.schema, self.spec = self.view_schema, None
        if sql is not None:
            vid, self.schema, self.spec = client.subscription(sql)
            assert vid == self.vid
        self.copy = {}
        self.cursor = (0, 0)

    def _bag(self, rows):
        return bag(rows, *self.cols) if self.spec is not None else bag(rows.including_hidden())

    def _apply(self, rows):
        for k, w in self._bag(rows).items():
            self.copy[k] = self.copy.get(k, 0) + w
            if self.copy[k] == 0:
                del self.copy[k]
        return rows

    def bootstrap(self):
        rows, self.cursor = self.client.delta_bootstrap(self.vid, self.schema, self.spec)
        # A bootstrap replaces state; it does not add to it.
        self.copy = self._bag(rows)

    def poll(self):
        # No hand-written tag check: `delta_poll` refuses a foreign cursor
        # itself, so a reply that arrives here is one this copy may apply.
        rows, self.cursor = self.client.delta_poll(self.vid, self.schema, self.cursor, self.spec)
        return self._apply(rows)

    def subscribe(self):
        """Poll, and have the server push what the next poll would fetch. From
        here on the copy moves by `sync` alone: a poll beside it would apply a
        round twice."""
        self.sub, rows, self.cursor = self.client.subscribe(self.vid, self.schema, self.cursor, self.spec)
        return self._apply(rows)

    def sync(self, wait=0.0):
        (pushed,) = self.client.sync(wait).pushed
        assert pushed.sub == self.sub and pushed.error is None, pushed
        self.cursor = pushed.cursor
        return self._apply(pushed.rows)

    def drain(self):
        """Collect every push acknowledged so far: one poll, since a poll ticks
        what is pending and covers everything past its cursor — so the next
        one must come back empty."""
        self.poll()
        assert len(self.poll()) == 0, "one poll left a round behind"

    def scan(self):
        full = self.client.scan(self.vid, self.view_schema)
        if self.spec is None:
            return bag(full.including_hidden())
        return bag([r for r in full if self.keep(r)], *self.cols)

    def assert_converged(self, what=""):
        """Poll, then require the copy to equal what it copies as a multiset;
        how many rows that is. A subscription may keep nothing of a view, so
        its caller checks that it kept something at some point."""
        self.drain()
        live = self.scan()
        assert live or self.copy or self.spec is not None, f"{what}: both sides are empty, so they agree about nothing"
        assert self.copy == live, what
        return len(live)


def later(target, schema, after, statement):
    """Run `statement` from a connection of its own, `after` seconds from now."""
    def run():
        with gnitz.connect(target, schema=schema) as other:
            time.sleep(after)
            other.execute_sql(statement)

    t = threading.Thread(target=run)
    t.start()
    return t


def mk_feed(client, name, body, feed=FEED):
    client.execute_sql(f"CREATE VIEW {name} WITH (delta = '{feed}') AS {body}")


def base_tables(client):
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, body TEXT NOT NULL)",
    )
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)",
    )


def churn(client, lo, hi, chunk=None):
    """Inserts, then an UPDATE and a DELETE over part of the range — so the feed
    sees fresh keys, `enforce_unique_pk`'s retract/insert pair, and pure
    retractions. Without the last two the retraction half of the design goes
    untested.

    `chunk` splits the inserts into that many rows per statement. Each statement
    is its own settle, so a caller that needs many spills — the capacity sweep
    budgets itself to one push-down per spill — asks for small ones.
    """
    span = hi - lo + 1
    step = chunk or span
    for a in range(lo, hi + 1, step):
        b = min(a + step - 1, hi)
        client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'body-{i:0>20}')" for i in range(a, b + 1)),
        )
        client.execute_sql(
            "INSERT INTO u VALUES " + ",".join(f"({i}, {i}, {i * 7})" for i in range(a, b + 1)),
        )
    client.execute_sql(f"UPDATE t SET v = v + 1 WHERE id >= {lo} AND id < {lo + span // 3}")
    # `u` loses the longer tail, so `t.id` is never a subset of `u.tid` and the
    # SETOP body (`t EXCEPT u`) holds rows. The other way round it is empty, and
    # a copy-versus-view comparison over it then agrees about nothing.
    client.execute_sql(f"DELETE FROM t WHERE id > {hi - span // 5}")
    client.execute_sql(f"DELETE FROM u WHERE id > {hi - span // 4}")
