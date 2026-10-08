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
# capacity, retention and cursor-expiry cases. A store spills once its RAM tier
# crosses this ceiling, which only happens at a memtable fold or a checkpoint's
# ephemeral round — so the checkpoint threshold is squeezed too. Together they
# give a few thousand rows the many spills a capacity sweep needs: it budgets
# itself to one push-down per spill.
SWEEP_ENV = {"GNITZ_RAM_TIER_BYTES": "1024", "GNITZ_CHECKPOINT_BYTES": str(32 * 1024)}

LINEAR = "SELECT id, v, body FROM t WHERE v > 10"
JOIN = "SELECT t.id, t.body, u.w FROM t JOIN u ON t.id = u.tid"
GROUPBY = "SELECT tid, COUNT(*) AS n, SUM(w) AS total FROM u GROUP BY tid"
SETOP = "SELECT id FROM t EXCEPT SELECT tid FROM u"


class Subscriber:
    """A client-side copy of a view, maintained the way the feed intends: one
    bootstrap, then polls, applying weights. It is deliberately not a set — the
    whole content of a delta row is its weight."""

    def __init__(self, client, name):
        self.client = client
        self.vid, self.schema = client.resolve_table(name)
        self.copy = {}
        self.cursor = (0, 0)

    def bootstrap(self):
        rows, self.cursor = self.client.delta_bootstrap(self.vid, self.schema)
        # A bootstrap replaces state; it does not add to it.
        self.copy = bag(rows.including_hidden())

    def poll(self):
        # No hand-written tag check: `delta_poll` refuses a foreign cursor
        # itself, so a reply that arrives here is one this copy may apply.
        rows, self.cursor = self.client.delta_poll(self.vid, self.schema, self.cursor)
        for k, w in bag(rows.including_hidden()).items():
            self.copy[k] = self.copy.get(k, 0) + w
            if self.copy[k] == 0:
                del self.copy[k]
        return rows

    def subscribe(self):
        """Have the server push what `poll` would fetch. From here on the copy
        moves by `sync` alone: a poll beside it would apply a round twice."""
        self.sub = self.client.subscribe(self.vid, self.schema, self.cursor)

    def sync(self, wait=0.0):
        (pushed,) = self.client.sync(wait).pushed
        assert pushed.sub == self.sub and pushed.error is None, pushed
        self.cursor = pushed.cursor
        for k, w in bag(pushed.rows.including_hidden()).items():
            self.copy[k] = self.copy.get(k, 0) + w
            if self.copy[k] == 0:
                del self.copy[k]
        return pushed.rows

    def drain(self):
        """Collect every push acknowledged so far: one poll, since a poll ticks
        what is pending and covers everything past its cursor — so the next
        one must come back empty."""
        self.poll()
        assert len(self.poll()) == 0, "one poll left a round behind"

    def scan(self):
        return bag(self.client.scan(self.vid, self.schema).including_hidden())

    def assert_converged(self, what=""):
        """Poll, then require the copy to equal the view as a multiset."""
        self.drain()
        live = self.scan()
        assert live or self.copy, f"{what}: both sides are empty, so they agree about nothing"
        assert self.copy == live, what


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


# The delta store is in neither checkpoint round and nothing flushes it, so it
# reaches disk only through its own memtable fold (~192 KiB) and then the RAM
# tier above. Reaching the sweep therefore takes real volume, not just a small
# ceiling — this is the byte width of one delta row, wide enough that a few
# thousand rows cross the fold several times.
_WIDE = "x" * 200


def flood(client, lo_key, rows, chunk=500):
    """Push `rows` wide rows into `t` starting at `lo_key`, in `chunk`-sized
    statements. Each statement is its own settle, so each is its own tick round."""
    for lo in range(lo_key, lo_key + rows, chunk):
        hi = min(lo + chunk - 1, lo_key + rows - 1)
        client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, '{_WIDE}-{i}')" for i in range(lo, hi + 1)),
        )
