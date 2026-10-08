"""A subscription mirrored under an alias: `mirror_subscription(alias, sql)`
keeps a local copy of what `sql` keeps of one view, read as `_local.<alias>`.

The alias is a relation of the client's own. Its columns are the SELECT's, its
rows the ones the WHERE keeps, and a read of it never leaves the process; the
view it reads stays an upstream relation under its own name.
"""
import threading
import time

import pytest
import gnitz
from _feedviews import GROUPBY, LINEAR, base_tables, churn, later, mk_feed
from _read import access, bag, rows


def _same(client, m, local_sql, upstream_sql, what):
    """`local_sql`, over an alias, costs no request and equals `upstream_sql`
    over the view, weight for weight."""
    before = m.requests_sent
    got = bag(rows(m, local_sql))
    assert m.requests_sent == before, f"{what}: the alias read went upstream"
    want = bag(rows(client, upstream_sql))
    assert want, f"{what}: nothing kept, so nothing compared"
    assert got == want, what


def _agree(client, m, alias, sql, what):
    m.sync().mirrored
    _same(client, m, f"SELECT * FROM _local.{alias}", sql, what)


# ── the copy ─────────────────────────────────────────────────────────────────


def test_two_aliases_of_one_view(client, mirror):
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    churn(client, 1, 200)
    even = "SELECT id, v FROM f WHERE v % 2 = 0"
    mine = "SELECT id, body FROM f WHERE id >= 50 AND id < 120"
    r1 = mirror.mirror_subscription("even", even)
    r2 = mirror.mirror_subscription("mine", mine)
    assert r1.reseeded and r2.reseeded
    assert r1.view_id != r2.view_id, "two copies of one upstream view"
    assert sorted(mirror.mirrored_ids()) == sorted([r1.view_id, r2.view_id])
    _agree(client, mirror, "even", even, "even after the bootstrap")
    _agree(client, mirror, "mine", mine, "mine after the bootstrap")
    for lo, hi in ((201, 400), (401, 500)):
        churn(client, lo, hi)
        client.execute_sql("UPDATE t SET v = v + 1 WHERE id >= 50 AND id < 90")
        client.execute_sql(f"DELETE FROM t WHERE id = {lo - 100}")
        _agree(client, mirror, "even", even, f"even through {hi}")
        _agree(client, mirror, "mine", mine, f"mine through {hi}")
    assert all(not o.reseeded for o in mirror.sync().mirrored), "a steady state advances"

    # A read plans against the alias's own columns.
    _same(client, mirror, "SELECT id FROM _local.even WHERE v > 300", "SELECT id FROM f WHERE v % 2 = 0 AND v > 300", "a local predicate")
    with pytest.raises(gnitz.GnitzError):
        rows(mirror, "SELECT body FROM _local.even")
    # The view's own name is not the alias's: it is read upstream, whole.
    before = mirror.requests_sent
    assert bag(rows(mirror, "SELECT id, v, body FROM f")) == bag(rows(client, "SELECT id, v, body FROM f"))
    assert mirror.requests_sent > before


def test_an_alias_whose_select_does_not_name_the_key(client, mirror):
    """The view's key rides along hidden, so rows that agree on every selected
    column stay distinct rows of the copy."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    churn(client, 1, 200)
    sql = "SELECT v FROM f WHERE v > 100"
    mirror.mirror_subscription("vs", sql)
    _agree(client, mirror, "vs", sql, "bootstrap")
    churn(client, 201, 300)
    client.execute_sql("UPDATE t SET v = 500 WHERE id < 150 AND id > 100")
    _agree(client, mirror, "vs", sql, "many rows share one v")
    _same(client, mirror, "SELECT COUNT(*) AS n FROM _local.vs", "SELECT COUNT(*) AS n FROM f WHERE v > 100", "their count")


def test_an_alias_over_an_aggregate(client, mirror):
    """Groups leave the filter and come back as their totals move."""
    base_tables(client)
    mk_feed(client, "g", GROUPBY)
    churn(client, 1, 300)
    big = "SELECT tid, total FROM g WHERE total > 1000"
    mirror.mirror_subscription("big", big)
    _agree(client, mirror, "big", big, "bootstrap")
    client.execute_sql("UPDATE u SET w = w - 5000 WHERE id < 200")
    _agree(client, mirror, "big", big, "groups left the filter")
    client.execute_sql("UPDATE u SET w = w + 9000 WHERE id < 100")
    _agree(client, mirror, "big", big, "groups came back")


def test_an_alias_is_a_local_relation_and_nothing_else(client, mirror):
    """`_local` is no schema of the server's, and an alias takes only reads."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    churn(client, 1, 200)
    mirror.mirror_subscription("even", "SELECT id, v FROM f WHERE v % 2 = 0")
    for ddl in ("CREATE SCHEMA _local", "CREATE TABLE _local.even (id BIGINT NOT NULL PRIMARY KEY)"):
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(ddl)
    for stmt in ("INSERT INTO _local.even VALUES (1, 2)", "DELETE FROM _local.even WHERE id = 1"):
        with pytest.raises(gnitz.GnitzError):
            mirror.execute_sql(stmt)
    with pytest.raises(gnitz.GnitzError):
        rows(client, "SELECT * FROM _local.even")


# ── indexes ──────────────────────────────────────────────────────────────────


def test_an_alias_keeps_the_indexes_whose_columns_it_keeps(client, mirror):
    """Each is a local store over the alias's own rows: under the alias's
    column numbers, and holding nothing the WHERE dropped."""
    base_tables(client)
    mk_feed(client, "g", "SELECT id, tid, w FROM u WHERE w > 0")
    client.execute_sql("CREATE INDEX by_w ON g(w)")
    client.execute_sql("CREATE INDEX by_tid ON g(tid)")
    churn(client, 1, 2000)
    # `w` leads the SELECT list, so it is not at its upstream column number.
    mirror.mirror_subscription("even", "SELECT w, id FROM g WHERE w % 2 = 0")
    mirror.mirror_subscription("low", "SELECT id, tid FROM g WHERE id < 500")
    mirror.sync().mirrored
    assert access(mirror, "SELECT id FROM _local.even WHERE w = 700") == "index range on (w)"
    assert access(mirror, "SELECT id FROM _local.low WHERE tid = 42") == "index range on (tid)"
    assert access(mirror, "SELECT tid FROM _local.low WHERE id = 42") == "pk point lookup"
    with pytest.raises(gnitz.GnitzError):
        rows(mirror, "SELECT id FROM _local.even WHERE tid = 42")
    for local, up in [
        ("SELECT id, w FROM _local.even WHERE w = 700", "SELECT id, w FROM g WHERE w = 700"),
        ("SELECT id, w FROM _local.even WHERE w >= 900 AND w < 960", "SELECT id, w FROM g WHERE w % 2 = 0 AND w >= 900 AND w < 960"),
        ("SELECT id FROM _local.low WHERE tid = 42", "SELECT id FROM g WHERE tid = 42"),
    ]:
        _same(client, mirror, local, up, local)
    churn(client, 2001, 2600)
    client.execute_sql("UPDATE u SET w = 700 WHERE id IN (7, 8, 9)")
    client.execute_sql("UPDATE u SET w = 701 WHERE id = 200")
    mirror.sync().mirrored
    _same(client, mirror, "SELECT id, w FROM _local.even WHERE w = 700", "SELECT id, w FROM g WHERE w = 700", "after churn")
    assert bag(rows(client, "SELECT id FROM g WHERE w = 701")) != {}
    assert bag(rows(mirror, "SELECT id FROM _local.even WHERE w = 701")) == {}, "an odd w is outside the alias, index and all"


def test_an_alias_gains_an_index_at_its_next_plan(client, mirror):
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    churn(client, 1, 500)
    sql = "SELECT id, v FROM f WHERE v % 2 = 0"
    first = mirror.mirror_subscription("even", sql)
    q = "SELECT id FROM _local.even WHERE v = 304"
    assert access(mirror, q) == "full scan"
    client.execute_sql("CREATE INDEX by_v ON f(v)")
    (again,) = mirror.sync().mirrored
    assert again.error is None, again.error
    assert again.view_id == first.view_id and not again.reseeded, "the rows stay; the index is filled from them"
    assert access(mirror, q) == "index range on (v)"
    _same(client, mirror, q, "SELECT id FROM f WHERE v = 304", "through the new index")


# ── the same alias again ─────────────────────────────────────────────────────


def test_an_alias_resumes_and_another_select_under_it_reseeds(client, server, mirror_dir):
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    churn(client, 1, 200)
    a, b = "SELECT id, v FROM f WHERE v > 100", "SELECT id, v FROM f WHERE v > 400"
    with gnitz.connect(server, schema=client.schema) as first:
        first.mirror_at(mirror_dir)
        assert first.mirror_subscription("s", a).reseeded
    with gnitz.connect(server, schema=client.schema) as second:
        second.mirror_at(mirror_dir)
        assert not second.mirror_subscription("s", a).reseeded, "the same SELECT resumes its persisted cursor"
    churn(client, 201, 300)
    with gnitz.connect(server, schema=client.schema) as third:
        third.mirror_at(mirror_dir)
        assert third.mirror_subscription("s", b).reseeded, "another SELECT's cursor is foreign"
        _agree(client, third, "s", b, "after the reseed")
        wide = "SELECT id, v, body FROM f WHERE v > 400"
        assert third.mirror_subscription("s", wide).reseeded
        _agree(client, third, "s", wide, "under other columns")
        assert len(third.mirrored_ids()) == 1


# ── the view goes away ───────────────────────────────────────────────────────


def test_an_alias_follows_its_view_through_a_drop_and_a_recreate(client, mirror):
    """The alias holds an id, which a recreated view does not keep, and a spec
    compiled against a schema, which it may not keep either. `sync` plans the
    SELECT again from its text; until that succeeds the alias is failed, under
    the reason, and its copy answers at the round it reached."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    churn(client, 1, 200)
    sql = "SELECT id, v FROM f WHERE v % 2 = 0"
    first = mirror.mirror_subscription("even", sql)
    held = bag(rows(mirror, "SELECT * FROM _local.even"))
    client.execute_sql("DROP VIEW f")
    for _ in range(2):
        (r,) = mirror.sync().mirrored
        assert r.error is not None and r.view_id == first.view_id
    assert bag(rows(mirror, "SELECT * FROM _local.even")) == held

    mk_feed(client, "f", "SELECT id, v, body FROM t WHERE v > 50")
    churn(client, 201, 300)
    (r,) = mirror.sync().mirrored
    assert r.error is None and r.reseeded and r.view_id == first.view_id, r.error
    _same(client, mirror, "SELECT * FROM _local.even", sql, "after the view came back")
    churn(client, 301, 400)
    (r,) = mirror.sync().mirrored
    assert r.error is None and not r.reseeded
    _same(client, mirror, "SELECT * FROM _local.even", sql, "and advances from there")

    # Recreated without a column the SELECT names: there is no plan to make.
    client.execute_sql("DROP VIEW f")
    mk_feed(client, "f", "SELECT id, body FROM t WHERE v > 50")
    mirror.sync().mirrored
    (r,) = mirror.sync().mirrored
    assert r.error is not None and "v" in str(r.error), r.error


def test_an_alias_reseeds_after_a_restart(own_server, mirror_on, mirror_dir):
    """A restarted server continues no cursor an earlier boot handed out: the
    first poll on the new connection reads the alias whole."""
    own_server.start()
    sql = "SELECT id, v FROM f WHERE v % 2 = 0"
    with gnitz.connect(own_server.target) as client:
        base_tables(client)
        mk_feed(client, "f", LINEAR)
        churn(client, 1, 60)
        m = mirror_on(mirror_dir, own_server.target)
        m.mirror_subscription("even", sql)
    own_server.restart()
    m.reconnect(own_server.target)
    with gnitz.connect(own_server.target) as client:
        churn(client, 61, 120)
        (r,) = m.sync().mirrored
        assert r.error is None and r.reseeded, r.error
        _same(client, m, "SELECT * FROM _local.even", sql, "after the restart")


# ── held polls ───────────────────────────────────────────────────────────────


def test_a_held_poll_sleeps_through_commits_its_alias_keeps_nothing_of(client, mirror, server):
    """Every commit to `t` reaches the view; only one of them leaves the alias
    a row. The poll is held across the rest."""
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    churn(client, 1, 60)
    sql = "SELECT id, v FROM f WHERE id = 7"
    mirror.mirror_subscription("one", sql)
    mirror.sync().mirrored
    before = bag(rows(mirror, "SELECT * FROM _local.one"))

    def write():
        with gnitz.connect(server, schema=client.schema) as w:
            for i in range(40):
                time.sleep(0.01)
                w.execute_sql(f"INSERT INTO t VALUES ({5000 + i}, 777, 'elsewhere')")
            w.execute_sql("UPDATE t SET v = v + 2 WHERE id = 7")

    t = threading.Thread(target=write)
    t0 = time.monotonic()
    t.start()
    mirror.sync(wait=30).mirrored
    took = time.monotonic() - t0
    t.join()
    assert took >= 0.4, "the first commit to the view ended the wait"
    after = bag(rows(mirror, "SELECT * FROM _local.one"))
    assert after != before and after == bag(rows(client, sql)), "the answer carries the one change"

    t0 = time.monotonic()
    mirror.sync(wait=0.3).mirrored
    assert time.monotonic() - t0 >= 0.3, "nothing changed, so the reply was held"
    writer = later(server, client.schema, 0.1, "DELETE FROM t WHERE id = 7")
    mirror.sync(wait=30).mirrored
    writer.join()
    assert bag(rows(mirror, "SELECT * FROM _local.one")) == {}


def test_a_held_poll_of_several_copies_answers_for_whichever_moved(client, mirror, server):
    """Three aliases and a mirrored view in one poll: held while none has
    anything, and one answer carries every one that has."""
    subs = {
        "a7": "SELECT id, v FROM f WHERE id = 7",
        "a9": "SELECT id, v FROM f WHERE id = 9",
        "big": "SELECT id, v FROM f WHERE v > 100000",
    }
    base_tables(client)
    mk_feed(client, "f", LINEAR)
    mk_feed(client, "g", "SELECT id, w FROM u WHERE w > 100")
    churn(client, 1, 60)
    for alias, sql in subs.items():
        mirror.mirror_subscription(alias, sql)
    mirror.mirror_view("g")
    mirror.sync().mirrored

    def agree(what):
        for alias, sql in subs.items():
            assert bag(rows(mirror, f"SELECT * FROM _local.{alias}")) == bag(rows(client, sql)), f"{what}: {alias}"
        assert bag(rows(mirror, "SELECT * FROM g")) == bag(rows(client, "SELECT * FROM g")), f"{what}: g"

    def write(first_key, last):
        with gnitz.connect(server, schema=client.schema) as w:
            for i in range(30):
                time.sleep(0.01)
                w.execute_sql(f"INSERT INTO t VALUES ({first_key + i}, 777, 'elsewhere')")
            w.execute_sql(last)

    for first_key, last, what in [
        (5000, "UPDATE t SET v = v + 2 WHERE id = 9", "the second alias"),
        (5100, "INSERT INTO t VALUES (9000, 200000, 'big')", "the last alias"),
        (5200, "UPDATE u SET w = w + 1 WHERE id = 30", "the mirrored view, of another table"),
        (5300, "UPDATE t SET v = 300000 WHERE id IN (7, 9)", "every alias at once"),
    ]:
        t = threading.Thread(target=write, args=(first_key, last))
        t0 = time.monotonic()
        t.start()
        mirror.sync(wait=30).mirrored
        took = time.monotonic() - t0
        t.join()
        mirror.sync().mirrored
        assert took >= 0.3, f"{what}: a commit no copy keeps a row of ended the wait"
        agree(what)
    t0 = time.monotonic()
    mirror.sync(wait=0.3).mirrored
    assert time.monotonic() - t0 >= 0.3
    agree("quiet")
