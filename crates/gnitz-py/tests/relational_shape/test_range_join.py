"""The non-equi join: pure range (`n_eq == 0`), band (`n_eq >= 1`), and the outer
orientations each admits.

Both sides reindex onto `[eq keys…, range key]`. A band join scatters its delta
by the eq PREFIX, so equal eq-values co-partition and the range walk runs
worker-local; a pure range join has no prefix, so its delta is broadcast to every
worker and probed against that worker's owned trace by an ordered range walk.
Either way the output is re-keyed onto the source-PK pair `(a.pk, b.pk)` and
exchanged by it, so the view is PK-partitioned like every other view.

The two shapes build their outer null-fill `ν` differently. Band subtracts the
matched preserved rows from the unfiltered preserved input,
`ν = positive_part(A − π_A(inner))`, and supports all three orientations. Pure
range decides existence from a single scalar threshold — `∃b. a.x < b.y ⟺
a.x < MAX(b.y)` — so `ν = A − (A ⋈ {m})` against a one-row `m` computed by an
inline shard-free reduce, and supports LEFT only: the mirror null-fill has no
witness on the preserved side. That threshold is reindexed back onto the range
slot, a chain carried only at ≤8 bytes.

Every assertion is a weighted bag of the pair identity. A wrong ν shows up as a
spurious weight-`w−1` null-fill and a broadcast that double-counts shows up as
weight W, neither of which a row set can see.

The equi outer join is in test_outer_join.py, the keyless one in
test_cross_join.py.

Run with GNITZ_WORKERS=4: the broadcast, the eq-prefix scatter and the
partition-local ν all collapse to nothing at one worker.
"""

import pytest
from _read import bag

_OPS = {"<": lambda a, b: a < b, "<=": lambda a, b: a <= b,
        ">": lambda a, b: a > b, ">=": lambda a, b: a >= b}


@pytest.fixture
def rng(client, schema_name):
    """Empty `a(id, x)` / `b(id, y)`, for a pure-range join `ON a.x OP b.y`."""
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)",
        schema_name=schema_name)
    return schema_name


@pytest.fixture
def band(client, schema_name):
    """Empty `a(id, k, lo)` / `b(id, k, t)`, for a band join
    `ON a.k = b.k AND a.lo <= b.t`."""
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "lo BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "t BIGINT NOT NULL)", schema_name=schema_name)
    return schema_name


def _fill(client, sn, table, rows):
    """INSERT `rows` — tuples of column values in schema order, `None` for NULL."""
    vals = ",".join("(" + ",".join("NULL" if v is None else str(v) for v in r) + ")"
                    for r in rows)
    client.execute_sql(f"INSERT INTO {table} VALUES {vals}", schema_name=sn)


def _pair_view(client, sn, on, kind="", name="v"):
    """`a <kind> JOIN b ON <on>` projecting the pair identity `(a.id, b.id)`;
    `b.id` is NULL on a null-fill row. Returns the view id."""
    client.execute_sql(
        f"CREATE VIEW {name} AS SELECT a.id AS aid, b.id AS bid "
        f"FROM a {kind} JOIN b ON {on}", schema_name=sn)
    return client.resolve_table(sn, name)[0]


def _live(client, vid):
    """The view's `{(aid, bid): net weight}`."""
    return bag(client.scan(vid), "aid", "bid")


def _ref(a_rows, b_rows, pred):
    """Inner reference: `{(a.id, b.id): 1}` per pair satisfying `pred(a, b)`.
    Rows are `(id, *cols)` tuples."""
    return {(a[0], b[0]): 1 for a in a_rows for b in b_rows if pred(a, b)}


def _left_ref(a_rows, b_rows, pred):
    """LEFT reference: `_ref` plus one `(a.id, None)` per left row with no match."""
    return _ref(a_rows, b_rows, pred) | {
        (a[0], None): 1 for a in a_rows if not any(pred(a, b) for b in b_rows)}


# ── the inner match set ───────────────────────────────────────────────────────

@pytest.mark.parametrize("op", list(_OPS))
@pytest.mark.parametrize("first", ["a", "b"])
def test_a_pure_range_join_is_the_product_the_operator_admits(client, rng, op, first):
    """`ON a.x OP b.y` with no equality prefix is the cross product filtered by the
    operator, each pair once. The side inserted second is the delta that emits, so
    the two orders drive the two bilinear terms in turn. Both value sets are
    permutations of the same residues, so `x == y` pairs sit inside the reference
    and separate the inclusive operators from the strict ones."""
    vid = _pair_view(client, rng, f"a.x {op} b.y")
    a_rows = [(i, (i * 7) % 23) for i in range(1, 21)]
    b_rows = [(i, (i * 5) % 23) for i in range(1, 21)]
    for table, rows in ([("a", a_rows), ("b", b_rows)] if first == "a"
                        else [("b", b_rows), ("a", a_rows)]):
        _fill(client, rng, table, rows)

    cmp = _OPS[op]
    assert _live(client, vid) == _ref(a_rows, b_rows, lambda a, b: cmp(a[1], b[1]))


def test_a_band_join_matches_only_inside_an_equality_group(client, band):
    """The eq prefix scatters both sides by `hash(k)`, so equal keys co-partition
    and the range walk is worker-local; a row satisfying the range in a *different*
    group must not match. The 40 groups spread across every worker, and each
    group's rows sit on different base-table PK partitions before the scatter
    re-homes them.

    The thin `b` side is seeded first and the wider `a` side arrives in one epoch,
    so on each worker the scattered |ΔA| outnumbers the integrated trace_b and the
    AB term takes the trace-driven merge walk rather than the per-row probe."""
    vid = _pair_view(client, band, "a.k = b.k AND a.lo <= b.t")
    ng = 40
    b_rows = [(i, i % ng, (i * 7) % 20) for i in range(1, ng + 1)]
    a_rows = [(i, i % ng, (i * 3) % 20) for i in range(1, 3 * ng + 1)]
    _fill(client, band, "b", b_rows)
    _fill(client, band, "a", a_rows)

    assert _live(client, vid) == _ref(
        a_rows, b_rows, lambda a, b: a[1] == b[1] and a[2] <= b[2])


def test_a_band_join_keys_on_every_equality_column(client, schema_name):
    """A two-column equality prefix (`n_eq = 2`) makes the trace key `[k1, k2,
    range]`: a match needs both equality columns and the range."""
    sn = schema_name
    for name, last in (("a", "lo"), ("b", "t")):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, k1 BIGINT NOT NULL, "
            f"k2 BIGINT NOT NULL, {last} BIGINT NOT NULL)", schema_name=sn)
    vid = _pair_view(client, sn, "a.k1 = b.k1 AND a.k2 = b.k2 AND a.lo <= b.t")
    a_rows = [(i, i % 3, i % 2, (i * 3) % 15) for i in range(1, 25)]
    b_rows = [(i, i % 3, i % 2, (i * 7) % 15) for i in range(1, 25)]
    _fill(client, sn, "a", a_rows)
    _fill(client, sn, "b", b_rows)

    assert _live(client, vid) == _ref(
        a_rows, b_rows, lambda a, b: a[1] == b[1] and a[2] == b[2] and a[3] <= b[3])


def test_a_pure_range_delta_wider_than_the_trace_takes_the_merge_walk(client, rng):
    """One whole-trace eq group, and a single epoch's broadcast delta outnumbering
    the per-worker trace: pure range broadcasts ΔA in full but worker-filters the
    trace to ~1/W, so the AB term crosses into the trace-driven merge walk. It must
    emit the same pairs the per-row probe does."""
    vid = _pair_view(client, rng, "a.x < b.y")
    b_rows = [(i, (i * 5) % 23) for i in range(1, 9)]
    a_rows = [(i, (i * 7) % 23) for i in range(1, 49)]
    _fill(client, rng, "b", b_rows)
    _fill(client, rng, "a", a_rows)

    assert _live(client, vid) == _ref(a_rows, b_rows, lambda a, b: a[1] < b[1])


def test_a_view_created_over_populated_tables_backfills_both_shapes(client, band):
    """Backfill replays both sources through the same input relay the steady state
    uses, so a view created after the data lands holds the full match set — for the
    broadcast pure-range relay and for the partitioned eq-prefix scatter alike."""
    ng = 24
    a_rows = [(i, i % ng, (i * 11) % 17) for i in range(1, 2 * ng + 1)]
    b_rows = [(i, i % ng, (i * 13) % 17) for i in range(1, 2 * ng + 1)]
    _fill(client, band, "a", a_rows)
    _fill(client, band, "b", b_rows)

    pr = _pair_view(client, band, "a.lo >= b.t", name="pr")
    bd = _pair_view(client, band, "a.k = b.k AND a.lo <= b.t", name="bd")
    assert _live(client, pr) == _ref(a_rows, b_rows, lambda a, b: a[2] >= b[2])
    assert _live(client, bd) == _ref(
        a_rows, b_rows, lambda a, b: a[1] == b[1] and a[2] <= b[2])


def test_a_pair_retracts_across_the_output_exchange(client, band):
    """A pair's `+1` and its later `−1` are emitted by opposite terms on different
    workers and must cancel through the output exchange. One pure-range and one
    band view over the same tables see each delta: deleting either side, moving the
    range column in and out, and — for the band alone — moving the equality key to
    another group."""
    pr = _pair_view(client, band, "a.lo < b.t", name="pr")
    bd = _pair_view(client, band, "a.k = b.k AND a.lo < b.t", name="bd")

    def live():
        return _live(client, pr), _live(client, bd)

    _fill(client, band, "a", [(1, 100, 5)])
    _fill(client, band, "b", [(1, 100, 9)])
    assert live() == ({(1, 1): 1}, {(1, 1): 1})

    client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=band)
    assert live() == ({}, {}), "the term-BA retraction cancels the term-AB emission"
    _fill(client, band, "b", [(1, 100, 9)])
    assert live() == ({(1, 1): 1}, {(1, 1): 1})

    client.execute_sql("UPDATE a SET lo = 99 WHERE id = 1", schema_name=band)
    assert live() == ({}, {}), "the range column moved out of range"
    client.execute_sql("UPDATE a SET lo = 5 WHERE id = 1", schema_name=band)
    assert live() == ({(1, 1): 1}, {(1, 1): 1})

    client.execute_sql("UPDATE a SET k = 200 WHERE id = 1", schema_name=band)
    assert live() == ({(1, 1): 1}, {}), "only the band reads the equality key"
    client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=band)
    assert live() == ({}, {})


def test_epochs_that_match_nothing_keep_the_circuit_live(client, rng):
    """A push that matches nothing still drives both exchanges every epoch, empty
    batches included, so the double exchange cannot deadlock on an empty round — a
    later real match still flows."""
    vid = _pair_view(client, rng, "a.x < b.y")
    for i in range(1, 6):
        _fill(client, rng, "a", [(i, 1000000)])
    for i in range(1, 6):
        _fill(client, rng, "b", [(i, i)])
    assert _live(client, vid) == {}

    _fill(client, rng, "a", [(99, 0)])
    assert _live(client, vid) == {(99, j): 1 for j in range(1, 6)}


def test_a_null_in_any_on_column_matches_nothing(client, schema_name):
    """SQL 3VL through both NULL filters: a NULL in the equality key or in the
    range column, on either side, matches nothing."""
    sn = schema_name
    for name, last in (("a", "x"), ("b", "y")):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, "
            f"{last} BIGINT)", schema_name=sn)
    vid = _pair_view(client, sn, "a.k = b.k AND a.x < b.y")
    # b1 fully defined; b2 NULL eq key; b3 NULL range key. a1 NULL eq key, a2 NULL
    # range key, a3 fully defined and matching b1 alone.
    _fill(client, sn, "b", [(1, 7, 100), (2, None, 100), (3, 7, None)])
    _fill(client, sn, "a", [(1, None, 5), (2, 7, None), (3, 7, 5)])

    assert _live(client, vid) == {(3, 1): 1}


def test_a_view_over_a_range_join_view_sees_well_formed_deltas(client, rng):
    """A GROUP BY view stacked on a range-join view stays consistent under
    retraction: the range join's output deltas are ordinary deltas downstream."""
    _pair_view(client, rng, "a.x < b.y")
    client.execute_sql(
        "CREATE VIEW g AS SELECT aid, COUNT(*) AS n FROM v GROUP BY aid",
        schema_name=rng)
    gid = client.resolve_table(rng, "g")[0]
    _fill(client, rng, "a", [(1, 5), (2, 50)])
    _fill(client, rng, "b", [(10, 10), (11, 20), (12, 100)])

    # a1 (x=5) is under all three b's; a2 (x=50) only under b12.
    assert bag(client.scan(gid), "aid", "n") == {(1, 3): 1, (2, 1): 1}
    client.execute_sql("DELETE FROM b WHERE id = 12", schema_name=rng)
    assert bag(client.scan(gid), "aid", "n") == {(1, 2): 1}


# ── the output pair key ───────────────────────────────────────────────────────

def test_the_pair_pk_routes_a_point_seek_to_the_row_it_holds(client, schema_name):
    """The view is PK-partitioned by `(a.pk, b.pk)`: seeking that pair reaches
    exactly the worker the output exchange routed it to. U64 source PKs, so the
    seek key's native packing is exact and the server's OPK encoding of it is the
    one the exchange hashed."""
    sn = schema_name
    for name, col in (("a", "x"), ("b", "y")):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
            f"{col} BIGINT NOT NULL)", schema_name=sn)
    vid = _pair_view(client, sn, "a.x < b.y")
    a_rows = [(i, i) for i in range(1, 13)]
    b_rows = [(i, i + 5) for i in range(1, 13)]
    _fill(client, sn, "a", a_rows)
    _fill(client, sn, "b", b_rows)

    pairs = _ref(a_rows, b_rows, lambda a, b: a[1] < b[1])
    assert _live(client, vid) == pairs
    for aid, bid in pairs:
        res = client.seek(vid, int(aid) | (int(bid) << 64))
        assert res.schema is not None and len(res.pks) == 1, f"seek ({aid},{bid})"
    # a12.x = 12 is not below b1.y = 6, so that pair was never emitted.
    miss = client.seek(vid, 12 | (1 << 64))
    assert miss.schema is None or len(miss.pks) == 0


def test_a_pair_pk_wider_than_sixteen_bytes_routes_by_hash(client, schema_name):
    """Compound source PKs make a four-column, 32-byte pair-PK, past the width at
    which `worker_for_pk_bytes` switches to hashing the OPK bytes. The full pair
    identity is `(a.k1, a.k2, b.k1, b.k2)`, and it is a hidden key slot — the
    payload twins beside it carry the same values."""
    sn = schema_name
    for name, col in (("a", "x"), ("b", "y")):
        client.execute_sql(
            f"CREATE TABLE {name} (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, "
            f"{col} BIGINT NOT NULL, PRIMARY KEY (k1, k2))", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.k1 AS ak1, a.k2 AS ak2, b.k1 AS bk1, b.k2 AS bk2 "
        "FROM a JOIN b ON a.x < b.y", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    a_rows = [(i, i * 2, (i * 7) % 19) for i in range(1, 13)]
    b_rows = [(i, i * 3, (i * 5) % 19) for i in range(1, 13)]
    _fill(client, sn, "a", a_rows)
    _fill(client, sn, "b", b_rows)

    want = {(a[0], a[1], b[0], b[1]): 1
            for a in a_rows for b in b_rows if a[2] < b[2]}
    assert bag(client.scan(vid), "ak1", "ak2", "bk1", "bk2") == want
    # The hidden pair-PK slots hold exactly those four columns.
    assert bag(client.scan(vid, include_hidden=True),
               "_pair_pk_0", "_pair_pk_1", "_pair_pk_2", "_pair_pk_3") == want


def test_a_range_join_over_the_source_pk_still_broadcasts(client, schema_name):
    """The range column *is* the source PK on both sides. No co-partition shortcut
    applies to a range join, so the broadcast and probe must still run — and the
    pair-PK is then the pair of range values themselves."""
    sn = schema_name
    client.execute_sql("CREATE TABLE a (x BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
    client.execute_sql("CREATE TABLE b (y BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.x AS aid, b.y AS bid FROM a JOIN b ON a.x < b.y",
        schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    a_rows = [(x,) for x in range(1, 16)]
    b_rows = [(y,) for y in range(5, 20)]
    _fill(client, sn, "a", a_rows)
    _fill(client, sn, "b", b_rows)

    assert _live(client, vid) == _ref(a_rows, b_rows, lambda a, b: a[0] < b[0])


def test_the_key_columns_promote_across_width_and_sign(client, schema_name):
    """One band join whose equality key and whose range column both cross width and
    sign: `INT UNSIGNED` against `BIGINT`, common type I64. Equal eq-values must
    hash to the same partition on both legs — the U32 side packs through the
    reindex packer at T, the I64 side through the single-column route key at its
    own width — and the range interval must survive promotion without inverting
    around the negative `b.y` values."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k INT UNSIGNED NOT NULL, "
        "x INT UNSIGNED NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "y BIGINT NOT NULL)", schema_name=sn)
    vid = _pair_view(client, sn, "a.k = b.k AND a.x > b.y")
    ng = 16
    a_rows = [(i, i % ng, i % 10) for i in range(1, 2 * ng + 1)]
    b_rows = [(i, i % ng, (i % 10) - 5) for i in range(1, 2 * ng + 1)]
    _fill(client, sn, "a", a_rows)
    _fill(client, sn, "b", b_rows)

    assert _live(client, vid) == _ref(
        a_rows, b_rows, lambda a, b: a[1] == b[1] and a[2] > b[2])


def test_the_unsupported_range_surfaces_are_refused(client, schema_name):
    """The range shapes the compiler has no construction for. A string or
    float range pair has no order-preserving key at all; pure-range RIGHT and FULL
    have no inner-join witness on the preserved side; and pure-range LEFT needs a
    ≤8-byte integer range column, because its threshold is reindexed back onto the
    range slot. The same 128-bit column is fine under INNER, which builds no
    threshold."""
    sn = schema_name
    for name, col in (("a", "x"), ("b", "y")):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, {col} BIGINT NOT NULL, "
            f"s VARCHAR(20) NOT NULL, f DOUBLE NOT NULL, w DECIMAL(38, 0) NOT NULL)",
            schema_name=sn)
    for on, kind in [("a.s < b.s", ""), ("a.f < b.f", ""),
                     ("a.x < b.y", "RIGHT"), ("a.x < b.y", "FULL"),
                     ("a.w < b.w", "LEFT")]:
        with pytest.raises(Exception):
            _pair_view(client, sn, on, kind, name="bad")
        with pytest.raises(Exception):
            client.resolve_table(sn, "bad")

    _pair_view(client, sn, "a.w < b.w", name="ok")


# ── band outer: ν = positive_part(A − π_A(inner)) ─────────────────────────────

def test_band_left_null_fills_a_row_until_it_has_a_match(client, band):
    """The null-fill life cycle, and its multiplicity. A left row with no b in its
    equality group satisfying the band null-fills; a ΔB giving it a first match
    retracts the null-fill in the same epoch the pair appears, and deleting that b
    restores it. A row matched twice stays matched until the LAST match retracts —
    ν sums the matched weight, it does not count matches. Deleting a *matched* left
    row leaves no `(a, NULL)` tombstone: `ΔA = −a` and `Δinner = −a` cancel. b is
    seeded first throughout, so every a's match is decided in the epoch it
    arrives, alongside a non-matching a in the same batch that still fills."""
    vid = _pair_view(client, band, "a.k = b.k AND a.lo <= b.t", "LEFT")
    _fill(client, band, "b", [(1, 1, 50)])
    # a1 matches b1; a2's group has no b; a3's lo is above every t in its group.
    _fill(client, band, "a", [(1, 1, 10), (2, 2, 10), (3, 3, 999)])
    assert _live(client, vid) == {(1, 1): 1, (2, None): 1, (3, None): 1}

    _fill(client, band, "b", [(2, 2, 20)])
    assert _live(client, vid) == {(1, 1): 1, (2, 2): 1, (3, None): 1}
    client.execute_sql("DELETE FROM b WHERE id = 2", schema_name=band)
    assert _live(client, vid) == {(1, 1): 1, (2, None): 1, (3, None): 1}

    # a1 now matches b1 and b3; dropping one leaves it matched, dropping both fills.
    _fill(client, band, "b", [(3, 1, 30)])
    assert _live(client, vid) == {(1, 1): 1, (1, 3): 1, (2, None): 1, (3, None): 1}
    client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=band)
    assert _live(client, vid) == {(1, 3): 1, (2, None): 1, (3, None): 1}
    client.execute_sql("DELETE FROM b WHERE id = 3", schema_name=band)
    assert _live(client, vid) == {(1, None): 1, (2, None): 1, (3, None): 1}

    # Give a1 a match again, then delete it while matched: no tombstone survives.
    _fill(client, band, "b", [(4, 1, 30)])
    client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=band)
    assert _live(client, vid) == {(2, None): 1, (3, None): 1}


def test_band_left_null_fills_correctly_across_eq_groups_on_every_worker(client, band):
    """ν is computed partition-locally, so it must be right without a global view:
    never a spurious null-fill for a matched row, never a dropped one for an
    unmatched row whose group lives on another worker. b is seeded in the even
    groups only, a in every group, over enough groups to reach all four workers."""
    vid = _pair_view(client, band, "a.k = b.k AND a.lo <= b.t", "LEFT")
    ng = 40
    b_rows = [(g, g, 100) for g in range(2, ng + 1, 2)]
    a_rows = [(g, g, 10) for g in range(1, ng + 1)]
    _fill(client, band, "b", b_rows)
    _fill(client, band, "a", a_rows)

    assert _live(client, vid) == _left_ref(
        a_rows, b_rows, lambda a, b: a[1] == b[1] and a[2] <= b[2])


def test_band_left_null_fills_a_row_no_predicate_can_match(client, schema_name):
    """A left row with a NULL equality or range column is filtered out of the inner
    match, yet is still a left row — ν reads the UNFILTERED input, so it null-fills
    exactly once. There is no bypass branch; the subtraction subsumes it."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, lo BIGINT)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "t BIGINT NOT NULL)", schema_name=sn)
    vid = _pair_view(client, sn, "a.k = b.k AND a.lo <= b.t", "LEFT")
    _fill(client, sn, "b", [(1, 1, 100)])
    _fill(client, sn, "a", [(1, None, 10), (2, 1, None), (3, 1, 10)])

    assert _live(client, vid) == {(1, None): 1, (2, None): 1, (3, 1): 1}


def test_band_left_is_weight_exact_over_a_bag_valued_preserved_side(client, schema_name):
    """A preserved side whose identities carry weight > 1: a `UNION ALL` view that
    drops the source PK, so two duplicate `(k, lo)` rows collapse onto one
    `_set_pk` identity of weight 2.

    ν subtracts the RAW matched multiplicity `w_A · S` and clamps the result, so a
    matched weight-2 identity reaches `w_A · (1 − S) ≤ 0` and null-fills not at
    all, while an unmatched one null-fills at the full weight 2. Clamping the
    witness to 1 instead leaks a weight-1 fill beside the matched rows."""
    sn = schema_name
    for tbl in ("t1", "t2"):
        client.execute_sql(
            f"CREATE TABLE {tbl} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
            f"lo BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE inr (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "t BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW uv AS SELECT k, lo FROM t1 UNION ALL SELECT k, lo FROM t2",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT uv.k AS uk, uv.lo AS ulo, inr.id AS bid "
        "FROM uv LEFT JOIN inr ON uv.k = inr.k AND uv.lo <= inr.t", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]

    _fill(client, sn, "inr", [(100, 5, 50)])
    # (5,10) twice → a matched weight-2 identity; (9,10) twice → unmatched.
    _fill(client, sn, "t1", [(1, 5, 10), (2, 5, 10), (3, 9, 10), (4, 9, 10)])
    assert bag(client.scan(vid), "uk", "ulo", "bid") == {
        (5, 10, 100): 2, (9, 10, None): 2}

    client.execute_sql("DELETE FROM inr WHERE id = 100", schema_name=sn)
    assert bag(client.scan(vid), "uk", "ulo", "bid") == {
        (5, 10, None): 2, (9, 10, None): 2}


@pytest.mark.parametrize("kind", ["RIGHT", "FULL"])
def test_band_preserves_the_other_side_in_the_mirror_orientations(client, band, kind):
    """RIGHT is the band ν with the sides swapped and FULL runs it on both, each
    unmatched row filling exactly once whichever side it is on — whether it is
    unmatched because its equality group is empty or because nothing in that group
    satisfies the range. The b rows share a payload and differ only in PK, and two
    a rows match each of them, so the re-key onto the pair-PK must keep all four
    matched pairs apart."""
    vid = _pair_view(client, band, "a.k = b.k AND a.lo <= b.t", kind)
    # a1, a2 qualify for b1 and b2 alike. a3's group holds no b and a4's lo is
    # above every t in its group; b3's group holds no a and b4's t is below every
    # lo in its group.
    a_rows = [(1, 7, 10), (2, 7, 10), (3, 8, 10), (4, 7, 999)]
    b_rows = [(1, 7, 50), (2, 7, 50), (3, 9, 50), (4, 7, 5)]
    _fill(client, band, "a", a_rows)
    _fill(client, band, "b", b_rows)

    def pred(a, b):
        return a[1] == b[1] and a[2] <= b[2]

    want = _ref(a_rows, b_rows, pred)
    want |= {(None, b[0]): 1 for b in b_rows if not any(pred(a, b) for a in a_rows)}
    if kind == "FULL":
        want |= {(a[0], None): 1 for a in a_rows if not any(pred(a, b) for b in b_rows)}
    assert _live(client, vid) == want


def test_band_full_null_fills_that_share_a_pair_pk_stay_distinct(client, band):
    """A left-only row with `a.id = 0` and a right-only row with `b.id = 0` both
    carry pair-PK `(0, 0)` — the absent side's slot packs to the synthetic 0 — but
    they are two (PK, payload) elements, their null bitmaps differing. Both must
    survive consolidation: a join view has no per-PK uniqueness."""
    vid = _pair_view(client, band, "a.k = b.k AND a.lo <= b.t", "FULL")
    _fill(client, band, "a", [(0, 1, 10)])
    _fill(client, band, "b", [(0, 2, 50)])

    assert _live(client, vid) == {(0, None): 1, (None, 0): 1}


# ── pure-range LEFT: ν = A − (A ⋈ {m}) against the MIN/MAX threshold ──────────

def test_pure_range_left_null_fill_follows_the_extremum(client, rng):
    """Existence depends on the extremum alone: `∃b. a.x < b.y ⟺ a.x < MAX(b.y)`.
    Deleting the EXTREME b raises the threshold, so an untouched a between the old
    and new extremum flips to null-filled; deleting a non-extreme b moves no
    threshold and must flip nothing. Deleting a matched a retracts its pairs and
    leaves no tombstone. b is seeded first, so a matched a's passthrough `+a` and
    the `A ⋈ {m}` term's `−a` are byte-identical and cancel in the epoch a
    arrives — never a transient `(a, NULL)`."""
    vid = _pair_view(client, rng, "a.x < b.y", "LEFT")
    b_rows = [(1, 20), (2, 40), (3, 60), (4, 80), (5, 100)]
    a_rows = [(1, 10), (2, 50), (3, 90), (4, 100), (5, 150)]
    _fill(client, rng, "b", b_rows)
    _fill(client, rng, "a", a_rows)

    def check(a_rows, b_rows, ctx):
        want = _left_ref(a_rows, b_rows, lambda a, b: a[1] < b[1])
        assert _live(client, vid) == want, ctx

    check(a_rows, b_rows, "a4 and a5 are at or above MAX = 100")

    client.execute_sql("DELETE FROM b WHERE id = 5", schema_name=rng)
    b_rows = [r for r in b_rows if r[0] != 5]
    check(a_rows, b_rows, "MAX drops to 80, so the untouched a3 (x=90) null-fills")

    nulls = {a for a, b in _live(client, vid) if b is None}
    client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=rng)
    b_rows = [r for r in b_rows if r[0] != 1]
    check(a_rows, b_rows, "a non-extreme b leaves the threshold where it was")
    assert {a for a, b in _live(client, vid) if b is None} == nulls

    client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=rng)
    a_rows = [r for r in a_rows if r[0] != 1]
    check(a_rows, b_rows, "a matched left row leaves no null-fill tombstone")


def test_pure_range_left_null_fills_each_row_once_under_the_broadcast(client, schema_name):
    """The pure-range relay broadcasts ΔA to every worker, so a row matching no b
    anywhere must still null-fill exactly ONCE rather than W times: the a.x owner
    produces the single fill and the pair-PK shard routes it once. A row whose
    range key is NULL takes the dedicated branch routed by a.pk and must be
    weight 1 too."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)",
        schema_name=sn)
    vid = _pair_view(client, sn, "a.x < b.y", "LEFT")
    _fill(client, sn, "b", [(i, 10 * i) for i in range(1, 9)])       # MAX = 80
    _fill(client, sn, "a", [(i, 80 + i) for i in range(1, 21)] + [(99, None)])

    assert _live(client, vid) == {(i, None): 1 for i in list(range(1, 21)) + [99]}


def test_pure_range_left_with_no_threshold_value_null_fills_everything(
        client, schema_name):
    """`m` is empty when b holds no rows AND when every b.y is NULL (MIN/MAX skip
    NULLs), and `A − ∅ = A` either way — no sentinel value is involved. Refilling
    b retracts the fills of the a's the new threshold covers."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT)", schema_name=sn)
    vid = _pair_view(client, sn, "a.x < b.y", "LEFT")
    _fill(client, sn, "a", [(1, 10), (2, 20), (3, 1000)])
    assert _live(client, vid) == {(1, None): 1, (2, None): 1, (3, None): 1}

    _fill(client, sn, "b", [(1, None), (2, None)])
    assert _live(client, vid) == {(1, None): 1, (2, None): 1, (3, None): 1}, \
        "rows with no live range value leave the threshold empty"

    _fill(client, sn, "b", [(3, 100)])
    assert _live(client, vid) == {(1, 3): 1, (2, 3): 1, (3, None): 1}
    client.execute_sql("DELETE FROM b WHERE id = 3", schema_name=sn)
    assert _live(client, vid) == {(1, None): 1, (2, None): 1, (3, None): 1}


@pytest.mark.parametrize("op", list(_OPS))
def test_pure_range_left_picks_the_threshold_each_operator_needs(client, rng, op):
    """`< <=` null-fill against MAX and `> >=` against MIN, with the exact-boundary
    rows `x == MIN` and `x == MAX` in the data so the strict operators exclude them
    and the inclusive ones do not."""
    vid = _pair_view(client, rng, f"a.x {op} b.y", "LEFT")
    b_rows = [(1, 30), (2, 50), (3, 70)]
    a_rows = [(1, 10), (2, 30), (3, 50), (4, 70), (5, 90)]
    _fill(client, rng, "b", b_rows)
    _fill(client, rng, "a", a_rows)

    cmp = _OPS[op]
    assert _live(client, vid) == _left_ref(
        a_rows, b_rows, lambda a, b: cmp(a[1], b[1]))


def test_pure_range_left_encodes_the_threshold_in_the_promoted_compare_type(
        client, schema_name):
    """MIN/MAX label their output I64 by the aggregate contract, but the threshold
    is probed as OPK bytes in the PROMOTED compare type. With a U64 extremum above
    `I64::MAX`, a wrong signed flip would order it at the bottom and invert every
    match."""
    sn = schema_name
    for name, col in (("a", "x"), ("b", "y")):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, "
            f"{col} BIGINT UNSIGNED NOT NULL)", schema_name=sn)
    vid = _pair_view(client, sn, "a.x < b.y", "LEFT")
    big = 18_000_000_000_000_000_000            # above I64::MAX, below U64::MAX
    _fill(client, sn, "b", [(1, big)])
    _fill(client, sn, "a", [(1, 10), (2, big + 1)])

    assert _live(client, vid) == {(1, 1): 1, (2, None): 1}


@pytest.mark.parametrize("ty", ["INT NOT NULL", "INT UNSIGNED NOT NULL",
                                "SMALLINT NOT NULL"])
def test_pure_range_left_carries_a_narrow_integer_range_column(client, schema_name, ty):
    """MIN/MAX preserve the source integer type, so the inline threshold reduce
    emits the narrow type the reindex consumes directly — I32, U32 and I16 all
    complete the chain, and deleting the extreme b re-null-fills an untouched a
    between the old and new threshold."""
    sn = schema_name
    for name, col in (("a", "x"), ("b", "y")):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, {col} {ty})",
            schema_name=sn)
    vid = _pair_view(client, sn, "a.x < b.y", "LEFT")
    b_rows = [(1, 20), (2, 60), (3, 100)]
    a_rows = [(1, 10), (2, 70), (3, 100), (4, 200)]
    _fill(client, sn, "b", b_rows)
    _fill(client, sn, "a", a_rows)
    assert _live(client, vid) == _left_ref(a_rows, b_rows, lambda a, b: a[1] < b[1])

    client.execute_sql("DELETE FROM b WHERE id = 3", schema_name=sn)
    b_rows = [r for r in b_rows if r[0] != 3]
    assert _live(client, vid) == _left_ref(a_rows, b_rows, lambda a, b: a[1] < b[1])


def test_pure_range_left_is_weight_exact_over_a_bag_valued_preserved_side(
        client, schema_name):
    """A `UNION ALL` view as the preserved side, each logical row present twice.
    The one-row threshold cannot multiply, so a matched row's null-fills sum to
    zero and an unmatched row's sum to its input multiplicity."""
    sn = schema_name
    for tbl in ("a1", "a2"):
        client.execute_sql(
            f"CREATE TABLE {tbl} (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
            schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW u AS SELECT id, x FROM a1 UNION ALL SELECT id, x FROM a2",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT u.id AS aid, b.id AS bid FROM u LEFT JOIN b ON u.x < b.y",
        schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]

    _fill(client, sn, "b", [(1, 50)])
    # id 1 (x=10) matches b1; id 2 (x=99) does not. Each arrives from both branches.
    _fill(client, sn, "a1", [(1, 10), (2, 99)])
    _fill(client, sn, "a2", [(1, 10), (2, 99)])

    assert _live(client, vid) == {(1, 1): 2, (2, None): 2}


def test_a_null_fill_carries_the_preserved_row_payload_byte_for_byte(client, schema_name):
    """A matched row's passthrough `+x` and the matched term's `−x` must be
    byte-identical or a stride or encoding mismatch leaves a spurious null-fill
    behind — so this runs both shapes over the same wide-PK left side, with a
    string column and a nullable column carrying NULL. The matched rows null-fill
    not at all, and the unmatched ones' fills carry the exact preserved payload."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id1 BIGINT NOT NULL, id2 BIGINT NOT NULL, k BIGINT NOT NULL, "
        "x BIGINT NOT NULL, s VARCHAR(16) NOT NULL, note BIGINT, PRIMARY KEY (id1, id2))",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "t BIGINT NOT NULL)", schema_name=sn)
    for name, on in (("bd", "a.k = b.k AND a.x <= b.t"), ("pr", "a.x < b.t")):
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT a.id1, a.id2, a.s, a.note, b.id AS bid "
            f"FROM a LEFT JOIN b ON {on}", schema_name=sn)
    ids = [client.resolve_table(sn, n)[0] for n in ("bd", "pr")]

    def live(vid):
        return bag(client.scan(vid), "id1", "id2", "s", "note", "bid")

    client.execute_sql("INSERT INTO b VALUES (1, 1, 100), (2, 2, 100)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO a VALUES (1, 1, 1, 10, 'alpha', 7), (1, 2, 2, 20, 'beta', NULL)",
        schema_name=sn)
    assert live(ids[0]) == {(1, 1, "alpha", 7, 1): 1, (1, 2, "beta", None, 2): 1}
    assert live(ids[1]) == {(1, 1, "alpha", 7, 1): 1, (1, 1, "alpha", 7, 2): 1,
                            (1, 2, "beta", None, 1): 1, (1, 2, "beta", None, 2): 1}

    # Unmatched under both shapes: no b in group 9, and an x above every b.t.
    client.execute_sql(
        "INSERT INTO a VALUES (2, 1, 9, 10, 'gamma', 42), (2, 2, 1, 999, 'delta', NULL)",
        schema_name=sn)
    assert live(ids[0]) == {
        (1, 1, "alpha", 7, 1): 1, (1, 2, "beta", None, 2): 1,
        (2, 1, "gamma", 42, None): 1, (2, 2, "delta", None, None): 1}
    assert live(ids[1]) == {
        (1, 1, "alpha", 7, 1): 1, (1, 1, "alpha", 7, 2): 1,
        (1, 2, "beta", None, 1): 1, (1, 2, "beta", None, 2): 1,
        (2, 1, "gamma", 42, 1): 1, (2, 1, "gamma", 42, 2): 1,
        (2, 2, "delta", None, None): 1}
