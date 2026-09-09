"""GROUP BY and the ungrouped (global) aggregate as dataflow shapes: the reduce
node, the value index its non-linear aggregates read, the finalize map above it,
and the HAVING filter above that.

Correctness is a weight-multiset. Every churn test recomputes its expectation in
pure Python (`_oracle`) from base state it maintains itself, so a reduce bug
cannot hide behind a hand-written expectation; the value-pinned tests compare
through `bag`, which sums weights, so a ghost or a doubled delta is a mismatch
rather than a plausible row set.

Run with GNITZ_WORKERS=4: a GROUP BY shards each group whole onto one worker and
a global aggregate funnels every row onto V0's owner, so both routing decisions
are only real at W > 1.
"""
import pytest
import gnitz
from gnitz import Opcode
import _oracle as oracle
from _read import bag, scanned


def _oracle_check(client, vid, state, cols, groups, aggs):
    """Build `check(ctx)`: assert the view's weight-multiset equals the aggregate
    recomputed in Python from `state`, which the caller maintains in lockstep
    with the SQL it sends.

    `cols` are the base columns the aggregate reads, `groups` the GROUP BY subset
    (empty for a global aggregate), `aggs` the `(out_name, kind, arg)` specs.
    """
    def check(ctx):
        base = oracle.oracle_filter_project(state, None, cols)
        exp, out_cols = oracle.oracle_groupby_aggregate(base, cols, groups, aggs)
        oracle.assert_view_matches(client, vid, out_cols, exp, ctx=ctx)
    return check


def _col_type(schema, name):
    for c in schema.columns:
        if c.name == name:
            return c.type_code
    raise KeyError(f"column {name!r} not in view schema {[c.name for c in schema.columns]}")


# ── The grouped reduce under churn ────────────────────────────────────────────


def test_every_grouped_aggregate_tracks_update_retraction_and_group_death(client, schema_name):
    """COUNT/SUM/AVG/MIN/MAX over one integer column through insert → UPDATE →
    delete-the-extremum (MIN/MAX must recompute the next-best out of history) →
    empty-a-group → re-create it. AVG lands on 7/3 and 3/2, so the f64 division
    is exercised rather than only whole-number results."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, a BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT g, COUNT(*) AS cnt, SUM(a) AS total, AVG(a) AS av, "
        "MIN(a) AS lo, MAX(a) AS hi FROM t GROUP BY g", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    state = {}
    check = _oracle_check(client, vid, state, ["g", "a"], ["g"], [
        ("cnt", "COUNT", None), ("total", "SUM", "a"), ("av", "AVG", "a"),
        ("lo", "MIN", "a"), ("hi", "MAX", "a")])

    # g=10: {1,2,4} → AVG 7/3; g=20: {5}.
    client.execute_sql(
        "INSERT INTO t VALUES (1, 10, 1), (2, 10, 2), (3, 10, 4), (4, 20, 5)", schema_name=sn)
    oracle.apply_insert(state, "pk", [
        {"pk": 1, "g": 10, "a": 1}, {"pk": 2, "g": 10, "a": 2},
        {"pk": 3, "g": 10, "a": 4}, {"pk": 4, "g": 20, "a": 5}])
    check("after-insert")

    client.execute_sql("UPDATE t SET a = 9 WHERE pk = 3", schema_name=sn)
    oracle.apply_update(state, "pk", 3, {"a": 9})
    check("after-update-max")

    client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
    oracle.apply_delete(state, "pk", [3])
    check("after-delete-extremum")

    client.execute_sql("DELETE FROM t WHERE pk = 4", schema_name=sn)
    oracle.apply_delete(state, "pk", [4])
    check("after-group-empty")

    client.execute_sql("INSERT INTO t VALUES (5, 20, 8)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 5, "g": 20, "a": 8}])
    check("after-reinsert")


def test_a_nullable_aggregate_reads_back_null_once_a_group_loses_its_last_value(
        client, schema_name):
    """SUM/AVG/MIN/MAX skip NULL inputs and read back NULL once a group's last
    non-NULL value is retracted — the group surviving on COUNT(*) — then recover
    when one returns. The oracle compares weights, so a retraction that re-emits
    the old value at the wrong null bit shows up as an uncancelled ghost."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT k, COUNT(*) AS c, SUM(v) AS sm, AVG(v) AS av, "
        "MIN(v) AS mn, MAX(v) AS hi FROM t GROUP BY k", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    state = {}
    check = _oracle_check(client, vid, state, ["k", "v"], ["k"], [
        ("c", "COUNT", None), ("sm", "SUM", "v"), ("av", "AVG", "v"),
        ("mn", "MIN", "v"), ("hi", "MAX", "v")])

    client.execute_sql("INSERT INTO t VALUES (1, 10, 5), (2, 10, NULL)", schema_name=sn)
    oracle.apply_insert(state, "pk", [
        {"pk": 1, "k": 10, "v": 5}, {"pk": 2, "k": 10, "v": None}])
    check("after-insert-mixed")

    client.execute_sql("INSERT INTO t VALUES (3, 10, 15)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 3, "k": 10, "v": 15}])
    check("after-second-nonnull")

    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    oracle.apply_delete(state, "pk", [1])
    check("after-delete-one-nonnull")

    # The group is now all-NULL and stays alive on COUNT(*): every other
    # aggregate must render NULL rather than a saturated 0.
    client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
    oracle.apply_delete(state, "pk", [3])
    check("after-all-null")

    client.execute_sql("INSERT INTO t VALUES (4, 10, 7)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 4, "k": 10, "v": 7}])
    check("after-recover")


@pytest.mark.parametrize("agg,kind", [
    ("SUM(a)", "SUM"), ("AVG(a)", "AVG"), ("COUNT(a)", "COUNT"),
], ids=["sum", "avg", "count-col"])
def test_a_linear_reduce_without_count_star_decides_existence_on_cardinality(
        client, schema_name, agg, kind):
    """An all-linear GROUP BY carrying no user COUNT(*) still gates group
    existence on the hidden cardinality: an emptied group vanishes instead of
    surviving as a zero, and a new group whose only row is NULL appears with the
    aggregate NULL — 0 for COUNT, which SQL never renders NULL. A COUNT(*) in the
    view would mask the gate by touching its accumulator on every row."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, a BIGINT NULL)",
        schema_name=sn)
    client.execute_sql(f"CREATE VIEW v AS SELECT g, {agg} AS m FROM t GROUP BY g", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    state = {}
    check = _oracle_check(client, vid, state, ["g", "a"], ["g"], [("m", kind, "a")])

    client.execute_sql("INSERT INTO t VALUES (1, 10, 4), (2, 20, 6)", schema_name=sn)
    oracle.apply_insert(state, "pk", [
        {"pk": 1, "g": 10, "a": 4}, {"pk": 2, "g": 20, "a": 6}])
    check("after-insert")

    client.execute_sql("INSERT INTO t VALUES (3, 7, NULL)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 3, "g": 7, "a": None}])
    check("after-new-all-null-group")

    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    oracle.apply_delete(state, "pk", [1])
    check("after-empty-g10")

    client.execute_sql("INSERT INTO t VALUES (4, 10, 7)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 4, "g": 10, "a": 7}])
    check("after-recreate-g10")


def test_a_where_filters_rows_before_the_fold(client, schema_name):
    """The WHERE of an aggregate body runs under the reduce, so a group whose
    every row fails it never reaches the fold and never appears."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, a BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW gv AS SELECT g, COUNT(*) AS c, SUM(a) AS s FROM t WHERE a > 100 GROUP BY g",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW av AS SELECT COUNT(*) AS c, SUM(a) AS s FROM t WHERE a > 100",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 10, 50), (2, 10, 200), (3, 10, 300), (4, 20, 5)",
        schema_name=sn)
    assert bag(scanned(client, sn, "gv"), "g", "c", "s") == {(10, 2, 500): 1}
    assert bag(scanned(client, sn, "av"), "c", "s") == {(2, 500): 1}


def test_a_group_by_over_a_distinct_view_counts_distinct_per_group(client, schema_name):
    """Outer `GROUP BY … COUNT(*)` over an inner `SELECT DISTINCT` is
    count-distinct-per-group: a DISTINCT boundary crossing becomes a cross-view
    delta into a downstream aggregate. Both groups stay non-empty throughout, so
    what the churn drives is distinct-pair boundary crossings, duplicate carriers
    and a cross-group key UPDATE — never a group's elimination."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, k BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW d AS SELECT DISTINCT g, k FROM t", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT g, COUNT(*) AS c FROM d GROUP BY g", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    state = {}

    def check(ctx):
        d = oracle.oracle_distinct(oracle.oracle_filter_project(state, None, ["g", "k"]))
        exp, cols = oracle.oracle_groupby_aggregate(d, ["g", "k"], ["g"], [("c", "COUNT", None)])
        oracle.assert_view_matches(client, vid, cols, exp, ctx=ctx)

    # g=1: k in {7,7,8} → distinct {7,8}; g=2: k in {9,10}.
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 7), (2, 1, 7), (3, 1, 8), (4, 2, 9), (5, 2, 10)",
        schema_name=sn)
    oracle.apply_insert(state, "pk", [
        {"pk": 1, "g": 1, "k": 7}, {"pk": 2, "g": 1, "k": 7}, {"pk": 3, "g": 1, "k": 8},
        {"pk": 4, "g": 2, "k": 9}, {"pk": 5, "g": 2, "k": 10}])
    check("after-insert")

    # One of two carriers of (1,7) — the pair survives.
    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    oracle.apply_delete(state, "pk", [1])
    check("after-delete-one-carrier")

    # The last carrier — the pair exits, the group lives on via k=8.
    client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
    oracle.apply_delete(state, "pk", [2])
    check("after-delete-last-carrier-of-pair")

    client.execute_sql("INSERT INTO t VALUES (6, 2, 9)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 6, "g": 2, "k": 9}])
    check("after-duplicate-carrier")

    client.execute_sql("UPDATE t SET k = 9 WHERE pk = 5", schema_name=sn)
    oracle.apply_update(state, "pk", 5, {"k": 9})
    check("after-update-onto-existing-pair")

    # One epoch retracting a DISTINCT pair from one group and inserting another
    # into a second.
    client.execute_sql("UPDATE t SET g = 1 WHERE pk = 4", schema_name=sn)
    oracle.apply_update(state, "pk", 4, {"g": 1})
    check("after-cross-group-move")


def test_an_emptied_inner_reduce_group_leaves_no_phantom_in_the_outer(client, schema_name):
    """Reduce over reduce: when an inner group empties, its row must vanish from
    the inner view — a sum=0 zombie there feeds the outer as a phantom (s=0, c=1)
    and leaves a stale (s=old, c=0) behind."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, a BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW iv AS SELECT g, SUM(a) AS s FROM t GROUP BY g", schema_name=sn)
    client.execute_sql("CREATE VIEW ov AS SELECT s, COUNT(*) AS c FROM iv GROUP BY s", schema_name=sn)
    ov_id = client.resolve_table(sn, "ov")[0]
    state = {}

    def check(ctx):
        base = oracle.oracle_filter_project(state, None, ["g", "a"])
        inner, inner_cols = oracle.oracle_groupby_aggregate(
            base, ["g", "a"], ["g"], [("s", "SUM", "a")])
        outer, outer_cols = oracle.oracle_groupby_aggregate(
            inner, inner_cols, ["s"], [("c", "COUNT", None)])
        oracle.assert_view_matches(client, ov_id, outer_cols, outer, ctx=ctx)

    client.execute_sql("INSERT INTO t VALUES (1, 1, 5), (2, 2, 8)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 1, "g": 1, "a": 5}, {"pk": 2, "g": 2, "a": 8}])
    check("after-insert")

    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    oracle.apply_delete(state, "pk", [1])
    check("after-inner-empty")


# ── The group key ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("pk_type,keys", [
    ("TINYINT", [-128, -1, 0, 7]),
    ("SMALLINT", [-32768, -1, 0, 7]),
    ("INT", [-2_000_000_000, -1, 0, 7]),
    ("BIGINT", [-5, -1, 0, 7]),
    ("UUID", ["550e8400-e29b-41d4-a716-446655440000",
              "6ba7b810-9dad-11d1-80b4-00c04fd430c8"]),
], ids=["tinyint", "smallint", "int", "bigint", "uuid"])
def test_a_group_by_over_the_pk_emits_one_group_per_key_at_its_native_value(
        client, schema_name, pk_type, keys):
    """`GROUP BY pk, other` is one group per PK, since a base table's PK is
    unique. The projected key must be the native value: the PK region stores the
    order-preserving image, whose signed columns are sign-flipped, so projecting
    the stored bytes would turn every negative key into a different number."""
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk {pk_type} NOT NULL PRIMARY KEY, category BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT pk, category, COUNT(*) AS cnt FROM t GROUP BY pk, category",
        schema_name=sn)
    lit = (lambda k: f"'{k}'") if pk_type == "UUID" else str
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(
            f"({lit(k)}, {(i + 1) * 10})" for i, k in enumerate(keys)),
        schema_name=sn)
    assert bag(scanned(client, sn, "v"), "pk", "category", "cnt") == {
        (k, (i + 1) * 10, 1): 1 for i, k in enumerate(keys)}


def test_a_nullable_group_key_keeps_null_distinct_from_zero(client, schema_name):
    """A NULL group key stores as zero bytes under a set null bit, so it must not
    merge with the integer-zero group — neither in the grouping itself, nor in the
    per-group extremes (a nullable group column is not byte-form-eligible for the
    combined value index, so the reduce takes the trace fallback), nor in a HAVING
    null test, which reads the group column's payload null bit."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NULL, v BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT g, COUNT(*) AS c, MIN(v) AS lo, MAX(v) AS hi "
        "FROM t GROUP BY g", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_null AS SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING g IS NULL",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_notnull AS SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING g IS NOT NULL",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, NULL, 100), (2, 0, 7), (3, NULL, 50), (4, 0, 9), (5, 7, 1)",
        schema_name=sn)
    assert bag(scanned(client, sn, "v"), "g", "c", "lo", "hi") == {
        (None, 2, 50, 100): 1, (0, 2, 7, 9): 1, (7, 1, 1, 1): 1}
    assert bag(scanned(client, sn, "v_null"), "g", "c") == {(None, 2): 1}
    assert bag(scanned(client, sn, "v_notnull"), "g", "c") == {(0, 2): 1, (7, 1): 1}

    # Lower the NULL group's MIN and raise the zero group's MAX: neither may move
    # the other.
    client.execute_sql("INSERT INTO t VALUES (6, NULL, 10), (7, 0, 999)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "g", "c", "lo", "hi") == {
        (None, 3, 10, 100): 1, (0, 3, 7, 999): 1, (7, 1, 1, 1): 1}


def test_many_distinct_group_keys_each_get_their_own_group(client, schema_name):
    """A single-STRING key and a multi-column key both fold to a synthetic
    `_group_pk` whose 128-bit identity is what the exchange routes on and what the
    view stores. Two distinct keys must never share a group, and a DELETE must
    move only the group that carried the row."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, "
        "a BIGINT NOT NULL, b BIGINT NOT NULL, val BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vs AS SELECT s, COUNT(*) AS n, SUM(val) AS total FROM t GROUP BY s",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vab AS SELECT a, b, COUNT(*) AS n, SUM(val) AS total FROM t GROUP BY a, b",
        schema_name=sn)

    rows, pk = [], 0
    for a in range(8):
        for b in range(8):
            for k in range(2):
                pk += 1
                rows.append((pk, f"key-{a:02d}-{b:02d}", a, b, a * 1000 + b * 10 + k))
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(
            f"({p}, '{s}', {a}, {b}, {v})" for p, s, a, b, v in rows), schema_name=sn)

    def expected(live):
        by_s, by_ab = {}, {}
        for _p, s, a, b, v in live:
            n, total = by_s.get(s, (0, 0))
            by_s[s] = (n + 1, total + v)
            n, total = by_ab.get((a, b), (0, 0))
            by_ab[(a, b)] = (n + 1, total + v)
        return ({(s, n, total): 1 for s, (n, total) in by_s.items()},
                {(a, b, n, total): 1 for (a, b), (n, total) in by_ab.items()})

    want_s, want_ab = expected(rows)
    assert bag(scanned(client, sn, "vs"), "s", "n", "total") == want_s
    assert bag(scanned(client, sn, "vab"), "a", "b", "n", "total") == want_ab

    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    want_s, want_ab = expected([r for r in rows if r[0] != 1])
    assert bag(scanned(client, sn, "vs"), "s", "n", "total") == want_s
    assert bag(scanned(client, sn, "vab"), "a", "b", "n", "total") == want_ab


# ── MIN / MAX and the combined value index ────────────────────────────────────


@pytest.mark.parametrize("coltype,groups", [
    ("INT", [(a, b) for a in range(3) for b in range(3)]),
    ("BIGINT", [(1_000_000_001, 2_000_000_001), (1_000_000_001, 2_000_000_002),
                (1_000_000_002, 2_000_000_001), (5, 7), (5, 8),
                (9_999_999_999, 9_999_999_998)]),
    ("INT", [(-5, -7), (5, 7), (-5, 7), (5, -7), (0, 0)]),
], ids=["narrow", "wide", "signed-twins"])
def test_a_multi_column_group_key_isolates_each_groups_extremes(
        client, schema_name, coltype, groups):
    """Each group's MIN/MAX are found by seeking the value index on the whole
    group key, so two groups can never share a bucket. The narrow case keeps the
    composite key (a ++ b ++ encoded value) inside the 16-byte cap, the wide one
    is 24 bytes and past it, and the signed one sets the high bit of a group
    column (-5 → 0xFFFFFFFB) against its positive twin. Values arrive in three
    batches, so the index is read across several sources rather than one memtable.
    """
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a {coltype} NOT NULL, "
        f"b {coltype} NOT NULL, val BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a, b, MIN(val) AS lo, MAX(val) AS hi FROM t GROUP BY a, b",
        schema_name=sn)

    pk = 0
    want = {}
    for batch in range(3):
        vals = []
        for gi, (a, b) in enumerate(groups):
            pk += 1
            v = (gi + 1) * 1000 + batch * 10
            vals.append(f"({pk}, {a}, {b}, {v})")
            lo, hi = want.get((a, b), (v, v))
            want[(a, b)] = (min(lo, v), max(hi, v))
        client.execute_sql("INSERT INTO t VALUES " + ", ".join(vals), schema_name=sn)

    def expect(w):
        assert bag(scanned(client, sn, "v"), "a", "b", "lo", "hi") == {
            (a, b, lo, hi): 1 for (a, b), (lo, hi) in w.items()}

    expect(want)

    # A new minimum in one group, then its retraction: only that group moves, and
    # it must recover the next-best from the index rather than keep the retracted
    # value.
    target = groups[0]
    client.execute_sql(
        f"INSERT INTO t VALUES ({pk + 1}, {target[0]}, {target[1]}, -999)", schema_name=sn)
    expect({**want, target: (-999, want[target][1])})
    client.execute_sql(f"DELETE FROM t WHERE pk = {pk + 1}", schema_name=sn)
    expect(want)


def test_min_max_over_a_primary_key_column_keeps_the_decoded_order(client, schema_name):
    """The aggregate argument is a PRIMARY KEY column, which is stored only as
    order-preserving bytes. The value index must OPK-decode it before
    order-encoding, or a round-tripped MIN(b) comes back byte-swapped (256 → 1)
    and the extreme walk — including its retract-at-the-extremum recovery —
    selects the wrong row. `b` straddles the high byte and the sign."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b))",
        schema_name=sn)
    # GROUP BY the whole PK: one row per group, so MIN(b) is just b.
    client.execute_sql(
        "CREATE VIEW vfull AS SELECT a, b, MIN(b) AS mb FROM t GROUP BY a, b", schema_name=sn)
    # GROUP BY a prefix: several b per group, so the extremes are a real walk.
    client.execute_sql(
        "CREATE VIEW vpart AS SELECT a, MIN(b) AS lo, MAX(b) AS hi FROM t GROUP BY a",
        schema_name=sn)
    rows = [(100, 1), (100, 256), (100, 100), (100, 65536), (300, -5), (300, 4), (300, 10)]
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({a}, {b})" for a, b in rows), schema_name=sn)

    assert bag(scanned(client, sn, "vfull"), "a", "b", "mb") == {(a, b, b): 1 for a, b in rows}
    assert bag(scanned(client, sn, "vpart"), "a", "lo", "hi") == {
        (100, 1, 65536): 1, (300, -5, 10): 1}

    # 256 is the byte-swap twin of 1: dropping the MAX holder must fall to 256,
    # not to 1.
    client.execute_sql("DELETE FROM t WHERE a = 100 AND b = 65536", schema_name=sn)
    client.execute_sql("DELETE FROM t WHERE a = 100 AND b = 1", schema_name=sn)
    client.execute_sql("DELETE FROM t WHERE a = 300 AND b = -5", schema_name=sn)
    assert bag(scanned(client, sn, "vpart"), "a", "lo", "hi") == {
        (100, 100, 256): 1, (300, 4, 10): 1}
    assert bag(scanned(client, sn, "vfull"), "a", "b", "mb") == {
        (100, 256, 256): 1, (100, 100, 100): 1, (300, 4, 4): 1, (300, 10, 10): 1}


def test_min_max_over_a_text_column_recedes_to_the_next_value(client, schema_name):
    """MIN/MAX over TEXT order by content over strings that share a long prefix
    and spill past the inline cell, and deleting the row holding an extreme
    recedes it — off the value index, whose MAX ordinal stores a complemented,
    prefix-free image. The global view alongside pins the same column's ground
    row: NULL before any row exists and again after the last is gone."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
        "s TEXT NOT NULL, sn TEXT)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT g, MIN(s) AS lo, MAX(s) AS hi, MIN(sn) AS lon, MAX(sn) AS hin "
        "FROM t GROUP BY g", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW gv AS SELECT MIN(s) AS lo, MAX(s) AS hi FROM t", schema_name=sn)

    def expect(grouped, glo, ghi):
        assert bag(scanned(client, sn, "v"), "g", "lo", "hi", "lon", "hin") == {
            (g, *vals): 1 for g, vals in grouped.items()}
        assert bag(scanned(client, sn, "gv"), "lo", "hi") == {(glo, ghi): 1}

    expect({}, None, None)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 10, 'shared/prefix/ab', 'x'), (2, 10, 'shared/prefix/abc', NULL), "
        "(3, 10, 'shared/prefix/a', NULL), (4, 10, 'shared/prefix/abd', 'y'), "
        "(5, 20, 'z', NULL), (6, 20, '', NULL)", schema_name=sn)
    expect({10: ("shared/prefix/a", "shared/prefix/abd", "x", "y"),
            20: ("", "z", None, None)}, "", "z")

    client.execute_sql("DELETE FROM t WHERE pk IN (3, 4)", schema_name=sn)
    expect({10: ("shared/prefix/ab", "shared/prefix/abc", "x", "x"),
            20: ("", "z", None, None)}, "", "z")

    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    expect({10: ("shared/prefix/abc", "shared/prefix/abc", None, None),
            20: ("", "z", None, None)}, "", "z")

    client.execute_sql("DELETE FROM t WHERE pk = 6", schema_name=sn)
    expect({10: ("shared/prefix/abc", "shared/prefix/abc", None, None),
            20: ("z", "z", None, None)}, "shared/prefix/abc", "z")

    client.execute_sql("UPDATE t SET s = 'shared/prefix/aa' WHERE pk = 2", schema_name=sn)
    expect({10: ("shared/prefix/aa", "shared/prefix/aa", None, None),
            20: ("z", "z", None, None)}, "shared/prefix/aa", "z")

    client.execute_sql("DELETE FROM t", schema_name=sn)
    expect({}, None, None)


def test_min_max_over_16_byte_columns_recede_through_the_value_index(client, schema_name):
    """UUID and DECIMAL(38,0) extremes order on all 16 bytes and recede like any
    other. The source PK is itself a 16-byte DECIMAL holding ids past u64::MAX:
    the combined index is keyed `group ‖ ordinal ‖ value` and never re-reads the
    source trace by PK, so the source key's width must not reach this answer."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id DECIMAL(38,0) NOT NULL PRIMARY KEY, g BIGINT NOT NULL, "
        "u UUID NOT NULL, big DECIMAL(38,0) NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT g, MIN(u) AS ulo, MAX(u) AS uhi, MIN(big) AS blo, "
        "MAX(big) AS bhi FROM t GROUP BY g", schema_name=sn)
    # The UUIDs differ only past their eighth byte; the decimals straddle u64::MAX.
    ua = "550e8400-e29b-41d4-a716-446655440000"
    ub = "550e8400-e29b-41d4-a716-446655440001"
    uc = "550e8400-e29b-41d4-a716-446655430000"
    small, mid, huge = 5, 2**64 + 1, 99999999999999999999999999999999999999
    client.execute_sql(
        f"INSERT INTO t VALUES (1, 1, '{ua}', {mid}), (2, 1, '{ub}', {small}), "
        f"({huge}, 1, '{uc}', {huge}), (3, 2, '{ua}', {small})", schema_name=sn)
    control = (2, ua, ua, small, small)

    def expect(row):
        assert bag(scanned(client, sn, "v"), "g", "ulo", "uhi", "blo", "bhi") == {
            row: 1, control: 1}

    expect((1, uc, ub, small, huge))
    # Dropping the row at the huge U128 id must retract both extremes it held.
    client.execute_sql(f"DELETE FROM t WHERE id = {huge}", schema_name=sn)
    expect((1, ua, ub, small, mid))
    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
    expect((1, ua, ua, mid, mid))


def test_a_lone_min_over_a_nullable_column_keeps_an_all_null_group(client, schema_name):
    """`SELECT g, MIN(a) GROUP BY g` over a nullable `a` with no user COUNT(*):
    the planner appends a hidden cardinality COUNT and the combined index carries
    the nullable MIN, so an all-NULL group is present with MIN = NULL rather than
    silently dropped, and disappears only when its last row goes."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, a BIGINT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT g, MIN(a) AS lo FROM t GROUP BY g", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    state = {}
    check = _oracle_check(client, vid, state, ["g", "a"], ["g"], [("lo", "MIN", "a")])

    client.execute_sql(
        "INSERT INTO t VALUES (1, 10, 5), (2, 10, NULL), (3, 20, NULL)", schema_name=sn)
    oracle.apply_insert(state, "pk", [
        {"pk": 1, "g": 10, "a": 5}, {"pk": 2, "g": 10, "a": None},
        {"pk": 3, "g": 20, "a": None}])
    check("all-NULL group 20 appears as (20, NULL)")

    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    oracle.apply_delete(state, "pk", [1])
    check("group 10 survives its last non-NULL as (10, NULL)")

    client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
    oracle.apply_delete(state, "pk", [2])
    check("group 10 gone with its last row")


@pytest.mark.parametrize("prepopulate", [False, True], ids=["incremental", "backfill"])
def test_a_fanout_join_input_isolates_each_groups_extremes(client, schema_name, prepopulate):
    """The reduce input is a fan-out join, so one `fact` row joins several `dim`
    rows carrying different `g` and the same join-output PK spans several groups —
    a reduce input with no unique PK. The combined index isolates each group by
    its key prefix, so a group's extremes can never pull a neighbour's rows,
    whether the view derives from scratch over a populated join or per tick."""
    sn = schema_name
    client.execute_sql("CREATE TABLE fact (fid BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE dim (did BIGINT NOT NULL PRIMARY KEY, fkey BIGINT NOT NULL, "
        "g INT NOT NULL, x BIGINT NOT NULL, y BIGINT NOT NULL)", schema_name=sn)

    def fill():
        client.execute_sql("INSERT INTO fact VALUES (1), (2)", schema_name=sn)
        # fact 1 → g=10(x=5,y=100), g=20(x=7,y=200); fact 2 → g=10(x=3,y=50), g=20(x=9,y=300).
        client.execute_sql(
            "INSERT INTO dim VALUES (1, 1, 10, 5, 100), (2, 1, 20, 7, 200), "
            "(3, 2, 10, 3, 50), (4, 2, 20, 9, 300)", schema_name=sn)

    if prepopulate:
        fill()
    client.execute_sql(
        "CREATE VIEW j AS SELECT fact.fid AS fid, dim.g AS g, dim.x AS x, dim.y AS y "
        "FROM fact JOIN dim ON fact.fid = dim.fkey", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW agg AS SELECT g, MIN(x) AS lo, MAX(y) AS hi, COUNT(*) AS c "
        "FROM j GROUP BY g", schema_name=sn)
    if not prepopulate:
        fill()

    assert bag(scanned(client, sn, "agg"), "g", "lo", "hi", "c") == {
        (10, 3, 100, 2): 1, (20, 7, 300, 2): 1}

    # Group 10's MIN holder goes: it must recompute to 5 from the surviving
    # group-10 row, never from group 20's smaller-keyed entries.
    client.execute_sql("DELETE FROM dim WHERE did = 3", schema_name=sn)
    assert bag(scanned(client, sn, "agg"), "g", "lo", "hi", "c") == {
        (10, 5, 100, 1): 1, (20, 7, 300, 2): 1}


def test_a_null_to_zero_transition_of_a_min_leaves_no_stale_trace_row(client, schema_name):
    """A NULL → 0 transition of a nullable MIN changes only the null bit; the
    payload bytes are zero either way. Were the reduce trace to declare the
    aggregate column NOT NULL, its rows would compare under the null-blind
    fixed-int comparator, the `old @ -1` / `new @ +1` pair would net to zero and
    the stale NULL row would survive — surfacing on the *next* transition, whose
    retraction is byte-copied from it. Grouped and global reduce both."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW gv AS SELECT k, MIN(v) AS m FROM t GROUP BY k", schema_name=sn)
    client.execute_sql("CREATE VIEW av AS SELECT MIN(v) AS m FROM t", schema_name=sn)

    def expect(m):
        assert bag(scanned(client, sn, "gv"), "k", "m") == {(10, m): 1}
        assert bag(scanned(client, sn, "av"), "m") == {(m,): 1}

    client.execute_sql("INSERT INTO t VALUES (1, 10, NULL)", schema_name=sn)
    expect(None)
    client.execute_sql("UPDATE t SET v = 0 WHERE pk = 1", schema_name=sn)
    expect(0)
    client.execute_sql("UPDATE t SET v = 7 WHERE pk = 1", schema_name=sn)
    expect(7)


# ── The width of a MIN / MAX output column ────────────────────────────────────

# (SQL type, view TypeCode, {group key: [source values]}). Each value set
# straddles its type's interesting boundary: negatives down to the type minimum
# for the signed widths, and values above i32::MAX for the unsigned
# zero-extension path, where a sign-extended read would turn them negative.
_NARROW_CASES = [
    ("TINYINT", gnitz.TypeCode.I8,
     {10: [-128, -5, 60, 127], 20: [-1, 0, 1, 9], 30: [100, -120, 33, 7]}),
    ("SMALLINT", gnitz.TypeCode.I16,
     {10: [-32768, -5, 30000, 32767], 20: [-1, 0, 1000, 9], 30: [12345, -12345, 33, 7]}),
    ("INT", gnitz.TypeCode.I32,
     {10: [-2_000_000_000, -5, 2_000_000_000, 7], 20: [-1, 0, 123456, 9],
      30: [42, -42, 100000, -99999]}),
    ("INT UNSIGNED", gnitz.TypeCode.U32,
     {10: [0, 4_000_000_000, 2_147_483_648, 100],
      20: [2_147_483_647, 2_147_483_649, 1, 4_294_967_295], 30: [10, 20, 30, 40]}),
    # F32 is the one source that does NOT keep its width: MIN/MAX widen it to F64,
    # so the read-back must take the 8-byte `f64::to_bits` verbatim instead of
    # decoding it as an F32 pattern. Values are dyadic and F32-exact, so the
    # widening is exact and `bag` compares bit-for-bit.
    ("FLOAT", gnitz.TypeCode.F64,
     {10: [-2.25, 1.5, 4.0, 0.5], 20: [-1.5, 0.0, 2.25, 8.5], 30: [3.0, -6.75, 0.25, 7.5]}),
]


@pytest.mark.parametrize("sql_type,py_tc,groups", _NARROW_CASES,
                         ids=[c[0].lower().replace(" ", "_") for c in _NARROW_CASES])
def test_a_narrow_min_max_reads_back_at_the_width_it_declares(
        client, schema_name, sql_type, py_tc, groups):
    """An integer MIN/MAX emits at the source column's width rather than widening
    to BIGINT — the extremum is one of the input rows, so it always fits — while a
    float one widens to F64. Either way the trace read-backs reconstruct the
    8-byte accumulator from the *declared* output width, so a wrong-width or
    sign-extended read corrupts the gather combine and, worse, the retraction that
    drops the row holding an extremum. Each group's values are replicated across
    distinct PKs so a group spreads over the workers."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        f"v {sql_type} NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT k, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY k",
        schema_name=sn)
    _vid, vschema = client.resolve_table(sn, "v")
    assert _col_type(vschema, "lo") == py_tc
    assert _col_type(vschema, "hi") == py_tc

    pk, vals_sql = 0, []
    for g, vals in groups.items():
        for _rep in range(4):
            for val in vals:
                pk += 1
                vals_sql.append(f"({pk}, {g}, {val})")
    client.execute_sql("INSERT INTO t VALUES " + ", ".join(vals_sql), schema_name=sn)

    def expect(live):
        assert bag(scanned(client, sn, "v"), "k", "lo", "hi") == {
            (g, min(vs), max(vs)): 1 for g, vs in live.items()}

    expect(groups)

    # Retract every carrier of group 10's extremes; both must fall back to the
    # next value at the source width.
    trimmed = dict(groups)
    for extreme in (max(groups[10]), min(groups[10])):
        client.execute_sql(f"DELETE FROM t WHERE k = 10 AND v = {extreme}", schema_name=sn)
        trimmed[10] = [v for v in trimmed[10] if v != extreme]
        expect(trimmed)


def test_a_narrow_min_max_is_read_at_its_own_width_by_having_and_the_projection(
        client, schema_name):
    """HAVING and the projected column both read the aggregate through the
    planner's `reduce_schema` mirror. A SMALLINT MAX must filter and project as
    SMALLINT — were the mirror still saying BIGINT, the finalize copy and the
    HAVING bind would each use the wrong width."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v SMALLINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT k, MAX(v) AS m FROM t GROUP BY k HAVING MAX(v) > 1000",
        schema_name=sn)
    _vid, vschema = client.resolve_table(sn, "v")
    assert _col_type(vschema, "m") == gnitz.TypeCode.I16

    # Groups straddling the threshold, all within I16: k=10 MAX 900 excluded,
    # k=20 MAX 1500 kept, k=30 MAX 30000 kept, k=40 MAX exactly 1000 excluded.
    client.execute_sql(
        "INSERT INTO t VALUES (1,10,900),(2,10,500),(3,10,-100),(4,20,1500),(5,20,200),"
        "(6,20,-5),(7,30,30000),(8,30,123),(9,30,-32768),(10,40,1000),(11,40,0),(12,40,7)",
        schema_name=sn)
    assert bag(scanned(client, sn, "v"), "k", "m") == {(20, 1500): 1, (30, 30000): 1}


# ── HAVING ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("pred,expected", [
    ("COUNT(*) > 1", {10, 30}),
    ("COUNT(*) = 1", {20}),
    ("NOT (COUNT(*) = 1)", {10, 30}),
    ("SUM(amount) * 2 > 10", {10, 30}),
    ("SUM(amount) BETWEEN 5 AND 20", {10}),
    ("CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1", {10, 30}),
    ("COUNT(*) IN (1, 3)", {20, 30}),
])
def test_a_having_predicate_binds_the_whole_expression_grammar(
        client, schema_name, pred, expected):
    """HAVING is a full expression over the grouped relation, not a comparison
    against one aggregate: `Mul`, `NOT` and the `BETWEEN` desugar all bind here,
    and an aggregate buried inside one of them must still be materialised in the
    reduce — none of these appear in the SELECT list."""
    sn = schema_name
    counts = {10: 2, 20: 1, 30: 3}
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, category BIGINT NOT NULL, "
        "amount BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        f"CREATE VIEW v AS SELECT category, COUNT(*) AS cnt FROM t GROUP BY category "
        f"HAVING {pred}", schema_name=sn)
    # SUM per category: 10 → 8, 20 → 2, 30 → 102.
    client.execute_sql(
        "INSERT INTO t VALUES (1, 10, 4), (2, 10, 4), (3, 20, 2), (4, 30, 100), "
        "(5, 30, 1), (6, 30, 1)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "category", "cnt") == {
        (c, counts[c]): 1 for c in expected}


@pytest.mark.parametrize("having,before,after", [
    ("SUM(v) IS NULL", {20}, {10, 20}),
    ("SUM(v) = 0", {30, 40}, {30, 40}),
    ("MIN(v) IS NULL", {20}, {10, 20}),
    ("MIN(v) IS NOT NULL", {10, 30, 40}, {30, 40}),
    ("MIN(v) = 0", {30}, {30}),
    ("MAX(v) <= 10", {10, 30, 40}, {30, 40}),
    ("MIN(t.v) > 3", {10}, set()),
    ("(k + SUM(v)) IS NULL", {20}, {10, 20}),
    ("COUNT(*) IS NOT NULL", {10, 20, 30, 40}, {10, 20, 30, 40}),
    ("COUNT(*) IS NULL", set(), set()),
    # A scalar wrapper over the aggregate: the value and its null test come from
    # one bind, so a NULL sum is distinct from every value and coalesces.
    ("SUM(v) IS DISTINCT FROM 5", {20, 30, 40}, {10, 20, 30, 40}),
    ("COALESCE(SUM(v), -1) = -1", {20}, {10, 20}),
    ("ABS(SUM(v)) > 3", {10}, set()),
    ("SUM(v) IN (0, 5)", {10, 30, 40}, {30, 40}),
])
def test_a_null_aggregate_in_having_is_unknown_not_zero(
        client, schema_name, having, before, after):
    """A raw aggregate column renders NULL as zero bytes under a set null bit, so
    every one of these predicates must read the null bit rather than the payload:
    a NULL aggregate compared to 0 is UNKNOWN and admits nothing, an IS NULL test
    admits exactly the groups with no non-NULL contributor, and COUNT(*) — which
    is never NULL — const-folds. None of these aggregates is in the SELECT list,
    so the reduce column (and, for SUM, its hidden COUNT_NON_NULL companion) is
    materialised from the predicate alone. Retracting k=10's only value nets its
    raw SUM back to zero, which is what separates the gate from the payload.

    k=10 {5, NULL}, k=20 {NULL}, k=30 {0}, k=40 {5, -5} — so a genuine zero sum,
    a genuine zero extremum and an all-NULL group are all present at once.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NULL)",
        schema_name=sn)
    client.execute_sql(
        f"CREATE VIEW v AS SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING {having}",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 10, 5), (2, 10, NULL), (3, 20, NULL), (4, 30, 0), "
        "(5, 40, 5), (6, 40, -5)", schema_name=sn)
    counts = {10: 2, 20: 1, 30: 1, 40: 2}
    assert bag(scanned(client, sn, "v"), "k", "c") == {(k, counts[k]): 1 for k in before}

    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    counts[10] = 1
    assert bag(scanned(client, sn, "v"), "k", "c") == {(k, counts[k]): 1 for k in after}


def test_having_names_the_grouped_relation_not_the_projection(client, schema_name):
    """HAVING binds before projection: it may name a GROUP BY column the SELECT
    list omits, must use a group column's source name even where SELECT aliases
    it, and may test an aggregate the SELECT list never asks for."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW unprojected AS SELECT a, COUNT(*) AS c FROM t GROUP BY a, b HAVING b > 5",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW aliased AS SELECT a AS x, COUNT(*) AS c FROM t GROUP BY a HAVING a > 5",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW unprojected_agg AS SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 3), (2, 1, 9), (3, 2, 7), (4, 10, 1), (5, 10, 1), "
        "(6, 20, 1)", schema_name=sn)
    # (a, b) groups: (1,3) (1,9) (2,7) (10,1)×2 (20,1) — b > 5 keeps (1,9) and (2,7).
    assert bag(scanned(client, sn, "unprojected"), "a", "c") == {(1, 1): 1, (2, 1): 1}
    assert bag(scanned(client, sn, "aliased"), "x", "c") == {(10, 2): 1, (20, 1): 1}
    assert bag(scanned(client, sn, "unprojected_agg"), "a") == {(1,): 1, (10,): 1}


def test_a_null_test_on_a_pk_group_column_const_folds(client, schema_name):
    """A PK column is non-nullable, so a HAVING null test over one folds at plan
    time — IS NOT NULL passes every group, IS NULL none. This covers a single
    column PK and a compound one, whose group sets are recognised by separate
    arms; emitting the null test against the PK region instead would trip
    `eval_is_null`'s debug assertion on the debug server this suite runs."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE t2 (a BIGINT NOT NULL, b BIGINT NOT NULL, PRIMARY KEY (a, b))",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW s_pass AS SELECT pk, COUNT(*) AS c FROM t1 GROUP BY pk HAVING pk IS NOT NULL",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW s_none AS SELECT pk, COUNT(*) AS c FROM t1 GROUP BY pk HAVING pk IS NULL",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW c_pass AS SELECT a, b, COUNT(*) AS c FROM t2 GROUP BY a, b "
        "HAVING a IS NOT NULL", schema_name=sn)
    client.execute_sql("INSERT INTO t1 VALUES (1, 100), (2, 200)", schema_name=sn)
    client.execute_sql("INSERT INTO t2 VALUES (5, 6), (5, 7)", schema_name=sn)

    assert bag(scanned(client, sn, "s_pass"), "pk", "c") == {(1, 1): 1, (2, 1): 1}
    assert bag(scanned(client, sn, "s_none"), "pk", "c") == {}
    assert bag(scanned(client, sn, "c_pass"), "a", "b", "c") == {(5, 6, 1): 1, (5, 7, 1): 1}


# ── The ungrouped (global) aggregate ──────────────────────────────────────────


def test_a_global_aggregate_tracks_the_ground_row_through_churn(client, schema_name):
    """One logical group at the synthetic constant PK V0. SQL scalar-aggregate
    semantics require exactly one output row even over an empty or fully-retracted
    source, so the churn runs empty → insert → update → delete-all → refill and
    the oracle carries the empty-source result the ground machinery exists for."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total, AVG(a) AS av, "
        "MIN(a) AS lo, MAX(a) AS hi FROM t", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    state = {}
    check = _oracle_check(client, vid, state, ["a"], [], [
        ("cnt", "COUNT", None), ("total", "SUM", "a"), ("av", "AVG", "a"),
        ("lo", "MIN", "a"), ("hi", "MAX", "a")])

    check("empty-at-creation")
    client.execute_sql("INSERT INTO t VALUES (1, 2), (2, 4), (3, 9)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 1, "a": 2}, {"pk": 2, "a": 4}, {"pk": 3, "a": 9}])
    check("after-insert")
    client.execute_sql("UPDATE t SET a = 1 WHERE pk = 3", schema_name=sn)
    oracle.apply_update(state, "pk", 3, {"a": 1})
    check("after-update-min")
    client.execute_sql("DELETE FROM t", schema_name=sn)
    oracle.apply_delete(state, "pk", [1, 2, 3])
    check("after-delete-all")
    client.execute_sql("INSERT INTO t VALUES (4, 8)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 4, "a": 8}])
    check("after-reinsert")


def test_a_lone_global_aggregate_grounds_over_empty_and_all_null_sources(client, schema_name):
    """A global reduce carrying no COUNT(*) still emits exactly one row. Over an
    empty source, and over a non-empty source whose every value is NULL, every
    accumulator is untouched, the cardinality gate sheds the computed row and the
    ground supplies COUNT = 0 with every other aggregate NULL — never a concrete
    0 for SUM, and never a negative COUNT."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NULL, b BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW va AS SELECT COUNT(a) AS c, SUM(a) AS s, AVG(a) AS av, MIN(a) AS mn, "
        "MAX(a) AS mx FROM t", schema_name=sn)
    # A non-nullable lone SUM carries no companion column at all.
    client.execute_sql("CREATE VIEW vb AS SELECT SUM(b) AS total FROM t", schema_name=sn)

    def expect(a_row, b_total):
        assert bag(scanned(client, sn, "va"), "c", "s", "av", "mn", "mx") == {a_row: 1}
        assert bag(scanned(client, sn, "vb"), "total") == {(b_total,): 1}

    expect((0, None, None, None, None), None)
    client.execute_sql("INSERT INTO t VALUES (1, NULL, 3), (2, NULL, 4)", schema_name=sn)
    expect((0, None, None, None, None), 7)
    client.execute_sql("INSERT INTO t VALUES (3, 7, 1)", schema_name=sn)
    expect((1, 7, 7.0, 7, 7), 8)
    client.execute_sql("DELETE FROM t", schema_name=sn)
    expect((0, None, None, None, None), None)


def test_having_over_a_global_aggregate_filters_the_ground_row(client, schema_name):
    """HAVING is a post-reduce filter, so the ground row is in the trace at V0 and
    the predicate decides whether the view shows it. `SUM(x) = 0` is the sharp
    case: the ground renders SUM as NULL even over a NOT NULL source column, its
    finalize is a bare column reference, and NULL = 0 is UNKNOWN — reading the raw
    column's zero bytes instead would admit the ground row as a genuine zero."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vc AS SELECT COUNT(*) AS cnt FROM t HAVING COUNT(*) > 2", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vs AS SELECT SUM(x) AS s FROM t HAVING SUM(x) = 0", schema_name=sn)

    def expect(cnt, total):
        assert bag(scanned(client, sn, "vc"), "cnt") == ({} if cnt is None else {(cnt,): 1})
        assert bag(scanned(client, sn, "vs"), "s") == ({} if total is None else {(total,): 1})

    expect(None, None)
    client.execute_sql("INSERT INTO t VALUES (1, 5), (2, -5)", schema_name=sn)
    expect(None, 0)  # 2 is not > 2; the sum is a genuine zero
    client.execute_sql("INSERT INTO t VALUES (3, 0)", schema_name=sn)
    expect(3, 0)
    client.execute_sql("DELETE FROM t", schema_name=sn)
    expect(None, None)


def test_a_computed_projection_keeps_the_ground_row(client, schema_name):
    """A global aggregate may be computed over on the way in (`SUM(a * 2)`), on
    the way out (`COUNT(*) + 1`) and beside a literal. The ground machinery is
    what these could break, so each is read over the empty source, filled, and
    emptied again: exactly one row throughout, never a ghost."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT COUNT(*) + 1 AS c, SUM(a * 2) AS s, 'x' AS lit FROM t",
        schema_name=sn)

    def expect(row):
        assert bag(scanned(client, sn, "v"), "c", "s", "lit") == {row: 1}

    expect((1, None, "x"))
    client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 7)", schema_name=sn)
    expect((3, 24, "x"))
    client.execute_sql("DELETE FROM t", schema_name=sn)
    expect((1, None, "x"))


def _count_reduce_nodes(client, vid):
    """REDUCE circuit nodes of view `vid`: 2 for the two-phase shape
    (reduce_local + reduce_combine), 1 for the single funnel reduce."""
    return sum(1 for r in client.scan(gnitz.CIRCUIT_NODES_TAB)
               if r["view_id"] == vid and r["opcode"] == Opcode.Reduce)


def test_an_all_linear_global_aggregate_compiles_to_a_two_phase_reduce(client, schema_name):
    """An all-linear global aggregate over a partitioned table takes the two-phase
    path — a per-worker local partial, then at most N partials combined on V0's
    owner — and its answer must equal the funnel's. Integer arithmetic is
    bit-exact, so two-phase == funnel == oracle with no tolerance, and passing at
    every worker count is what pins the weight-exact partial split."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total, AVG(a) AS av, "
        "COUNT(b) AS nb FROM t", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    assert _count_reduce_nodes(client, vid) == 2, "all-linear integer global agg must be two-phase"
    state = {}
    check = _oracle_check(client, vid, state, ["a", "b"], [], [
        ("cnt", "COUNT", None), ("total", "SUM", "a"), ("av", "AVG", "a"), ("nb", "COUNT", "b")])

    check("empty")
    rows = [{"pk": i, "a": (i * 7) % 50, "b": (None if i % 3 == 0 else i)} for i in range(1, 41)]
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(
            f"({r['pk']}, {r['a']}, {'NULL' if r['b'] is None else r['b']})" for r in rows),
        schema_name=sn)
    oracle.apply_insert(state, "pk", rows)
    check("after-insert")

    client.execute_sql("UPDATE t SET a = 100 WHERE pk = 5", schema_name=sn)
    oracle.apply_update(state, "pk", 5, {"a": 100})
    client.execute_sql("UPDATE t SET b = 9 WHERE pk = 9", schema_name=sn)   # was NULL
    oracle.apply_update(state, "pk", 9, {"b": 9})
    check("after-update")

    del_pks = list(range(1, 41, 2))   # odd PKs, across partitions
    client.execute_sql(
        f"DELETE FROM t WHERE pk IN ({', '.join(map(str, del_pks))})", schema_name=sn)
    oracle.apply_delete(state, "pk", del_pks)
    check("after-delete")

    remaining = [r["pk"] for r in rows if r["pk"] not in del_pks]
    client.execute_sql("DELETE FROM t", schema_name=sn)
    oracle.apply_delete(state, "pk", remaining)
    check("after-delete-all")

    client.execute_sql("INSERT INTO t VALUES (99, 3, 4)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 99, "a": 3, "b": 4}])
    check("after-refill")


def test_a_two_phase_count_over_a_fresh_all_null_column_is_zero(client, schema_name):
    """Every worker's partial COUNT_NON_NULL is untouched and the combine's
    SumZero of them grounds to 0, agreeing with the funnel and grouped reduce. The
    column must be *fresh* all-NULL: inserting non-null rows and then nulling them
    leaves a concrete 0 on every path and pins nothing."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NULL)", schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT COUNT(a) AS c FROM t", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    assert _count_reduce_nodes(client, vid) == 2, "COUNT(col) global agg must be two-phase"
    client.execute_sql(
        "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, NULL), (4, NULL)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "c") == {(0,): 1}


def test_a_float_sum_is_excluded_from_the_two_phase_reduce(client, schema_name):
    """Two-phase eligibility is decided per aggregate type: an integer SUM or AVG
    distributes, while a float SUM — and an AVG whose SUM component is float — keep
    the single funnel reduce, because non-associative IEEE addition would make the
    answer depend on the worker count."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, ai BIGINT NOT NULL, "
        "af DOUBLE NOT NULL)", schema_name=sn)
    for name, expr in (("vsi", "SUM(ai)"), ("vai", "AVG(ai)"),
                       ("vsf", "SUM(af)"), ("vaf", "AVG(af)")):
        client.execute_sql(f"CREATE VIEW {name} AS SELECT {expr} AS s FROM t", schema_name=sn)
    nodes = {name: _count_reduce_nodes(client, client.resolve_table(sn, name)[0])
             for name in ("vsi", "vai", "vsf", "vaf")}
    assert nodes == {"vsi": 2, "vai": 2, "vsf": 1, "vaf": 1}


# ── The finalize map over the raw reduce output ───────────────────────────────


def test_a_string_group_column_survives_the_finalize_map(client, schema_name):
    """AVG and a nullable SUM make the reduce output pass through a columnar MAP,
    which must copy the TEXT group column — relocating its blob heap — in the same
    pass. Short values stay in the inline German-string cell; the two long ones do
    not, so both cell shapes cross the finalize."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, "
        "x BIGINT NOT NULL, y BIGINT)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT s, AVG(x) AS avg_x, SUM(y) AS sum_y FROM t GROUP BY s",
        schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    state = {}
    check = _oracle_check(client, vid, state, ["s", "x", "y"], ["s"],
                          [("avg_x", "AVG", "x"), ("sum_y", "SUM", "y")])

    short, long1, long2 = "ab", "long-heap-backed-key-1", "long-heap-backed-key-2"
    rows = [
        {"pk": 1, "s": short, "x": 2, "y": 10},
        {"pk": 2, "s": short, "x": 5, "y": None},
        {"pk": 3, "s": long1, "x": 7, "y": 3},
        {"pk": 4, "s": long1, "x": 8, "y": 4},
        {"pk": 5, "s": long2, "x": 9, "y": None},
    ]
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(
            f"({r['pk']}, '{r['s']}', {r['x']}, {'NULL' if r['y'] is None else r['y']})"
            for r in rows), schema_name=sn)
    oracle.apply_insert(state, "pk", rows)
    check("after-insert")

    client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
    oracle.apply_delete(state, "pk", [2])
    check("after-retract-inline-group")

    # long1 loses its last non-NULL y: SUM(y) must become NULL while AVG(x)
    # recomputes over the survivor.
    client.execute_sql("DELETE FROM t WHERE pk = 3", schema_name=sn)
    oracle.apply_delete(state, "pk", [3])
    check("after-retract-last-nonnull-y")

    client.execute_sql("DELETE FROM t WHERE pk = 5", schema_name=sn)
    oracle.apply_delete(state, "pk", [5])
    check("after-empty-heap-backed-group")


def test_a_compound_group_key_survives_the_finalize_map(client, schema_name):
    """A multi-column GROUP BY folds to a synthetic `_group_pk` whose PK region
    the finalize MAP inherits verbatim, copying both group-exemplar columns into
    payload slots beside the computed AVG."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
        "b BIGINT NOT NULL, val BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a, b, COUNT(*) AS n, AVG(val) AS av FROM t GROUP BY a, b",
        schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    state = {}
    check = _oracle_check(client, vid, state, ["a", "b", "val"], ["a", "b"],
                          [("n", "COUNT", None), ("av", "AVG", "val")])

    # 12 distinct (a, b) keys, 2 rows each → every AVG is an exact .0 or .5.
    rows, pk = [], 0
    for a in range(3):
        for b in range(4):
            for k in range(2):
                pk += 1
                rows.append({"pk": pk, "a": a, "b": b, "val": a * 10 + b + k})
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(
            f"({r['pk']}, {r['a']}, {r['b']}, {r['val']})" for r in rows), schema_name=sn)
    oracle.apply_insert(state, "pk", rows)
    check("after-insert")

    target = next(r for r in rows if r["a"] == 1 and r["b"] == 2)
    client.execute_sql(f"DELETE FROM t WHERE pk = {target['pk']}", schema_name=sn)
    oracle.apply_delete(state, "pk", [target["pk"]])
    check("after-retract-one")

    gone = [r["pk"] for r in rows if r["a"] == 0 and r["b"] == 0]
    client.execute_sql(
        f"DELETE FROM t WHERE pk IN ({', '.join(str(p) for p in gone)})", schema_name=sn)
    oracle.apply_delete(state, "pk", gone)
    check("after-empty-group")


# ── Aggregate-call qualifiers ─────────────────────────────────────────────────


@pytest.mark.parametrize("agg", [
    "SUM(x) FILTER (WHERE x > 0)",
    "SUM(x) OVER (PARTITION BY g)",
], ids=["filter", "over"])
def test_an_unimplemented_aggregate_qualifier_is_refused_not_dropped(
        client, schema_name, agg):
    """A qualifier the binder does not implement must be rejected loudly. Silently
    dropping it computes the plain aggregate instead — a durably wrong answer with
    no error to trace it to."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NOT NULL)",
        schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(
            f"CREATE VIEW v AS SELECT g, {agg} AS s FROM t GROUP BY g", schema_name=sn)


def test_count_all_is_count(client, schema_name):
    """`ALL` is the one aggregate qualifier the binder accepts, and it computes
    the unqualified aggregate — so `COUNT(ALL x)` skips NULL exactly as
    `COUNT(x)` does, and neither is `COUNT(*)`."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, x BIGINT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT g, COUNT(*) AS c_star, COUNT(x) AS c_x, "
        "COUNT(ALL x) AS c_all FROM t GROUP BY g", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 10, 2), (2, 10, NULL), (3, 20, 7)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "g", "c_star", "c_x", "c_all") == {
        (10, 2, 1, 1): 1, (20, 1, 1, 1): 1}


def test_a_16_byte_integer_cannot_be_summed_or_averaged(client, schema_name):
    """SUM and AVG accumulate into a 64-bit slot, which a U128 source overflows —
    the engine marks that decode unreachable, so the planner has to reject the
    view rather than compile a fold that cannot run."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, category BIGINT NOT NULL, "
        "big DECIMAL(38,0) NOT NULL)", schema_name=sn)
    for fn in ("SUM", "AVG"):
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(
                f"CREATE VIEW v AS SELECT category, {fn}(big) AS x FROM t GROUP BY category",
                schema_name=sn)


def test_a_global_aggregate_refuses_a_column_the_grouping_does_not_determine(
        client, schema_name):
    """With no GROUP BY the whole relation is one group, so no source column is
    determined — not even wrapped in an expression."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(
            "CREATE VIEW bad AS SELECT a + 1 AS x, COUNT(*) AS c FROM t", schema_name=sn)


# ── DISTINCT aggregates ───────────────────────────────────────────────────────

_EVENTS = [
    {"pk": 1, "k": 10, "u": 7, "s": "a"}, {"pk": 2, "k": 10, "u": 7, "s": "b"},
    {"pk": 3, "k": 10, "u": 8, "s": "a"}, {"pk": 4, "k": 20, "u": 7, "s": "a"},
    {"pk": 5, "k": 20, "u": None, "s": "a"}, {"pk": 6, "k": 30, "u": None, "s": "c"},
]


def _events_table(client, sn, fill=True):
    """`ev` plus `_EVENTS`, mirrored into the oracle state it returns."""
    client.execute_sql(
        "CREATE TABLE ev (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, u BIGINT NULL, "
        "s TEXT NOT NULL)", schema_name=sn)
    if not fill:
        return {}
    client.execute_sql(
        "INSERT INTO ev VALUES " + ", ".join(
            f"({r['pk']}, {r['k']}, {'NULL' if r['u'] is None else r['u']}, '{r['s']}')"
            for r in _EVENTS), schema_name=sn)
    state = {}
    oracle.apply_insert(state, "pk", _EVENTS)
    return state


def test_a_grouped_count_distinct_is_distinct_composed_into_the_aggregate(client, schema_name):
    """`COUNT(DISTINCT u)` lowers to a plain aggregate over a hidden
    `DISTINCT (group cols, u)` segment, so the churn is checked weight-exactly
    against `distinct` composed into `groupby_aggregate` — the same composition."""
    sn = schema_name
    state = _events_table(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT k, COUNT(DISTINCT u) AS n FROM ev GROUP BY k", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]

    def check(ctx):
        d = oracle.oracle_distinct(oracle.oracle_filter_project(state, None, ["k", "u"]))
        exp, cols = oracle.oracle_groupby_aggregate(d, ["k", "u"], ["k"], [("n", "COUNT", "u")])
        oracle.assert_view_matches(client, vid, cols, exp, ctx=ctx)

    # NULL is not a distinct value: group 30 counts 0 but still exists.
    check("after-insert")

    client.execute_sql("DELETE FROM ev WHERE pk = 1", schema_name=sn)
    oracle.apply_delete(state, "pk", [1])
    check("after-delete-one-carrier")

    client.execute_sql("DELETE FROM ev WHERE pk = 2", schema_name=sn)
    oracle.apply_delete(state, "pk", [2])
    check("after-delete-last-carrier")

    client.execute_sql("UPDATE ev SET k = 10 WHERE pk = 4", schema_name=sn)
    oracle.apply_update(state, "pk", 4, {"k": 10})
    check("after-cross-group-move")

    client.execute_sql("DELETE FROM ev WHERE pk = 6", schema_name=sn)
    oracle.apply_delete(state, "pk", [6])
    check("after-group-emptied")


def test_a_global_count_distinct_and_one_in_having(client, schema_name):
    """The same composition with no GROUP BY, and again in HAVING — which resolves
    through the grouped leaf rather than the SELECT list."""
    sn = schema_name
    state = _events_table(client, sn)
    client.execute_sql("CREATE VIEW g AS SELECT COUNT(DISTINCT s) AS n FROM ev", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW h AS SELECT k, COUNT(DISTINCT s) AS n FROM ev GROUP BY k "
        "HAVING COUNT(DISTINCT s) > 1", schema_name=sn)
    gid = client.resolve_table(sn, "g")[0]

    def check_global(ctx):
        d = oracle.oracle_distinct(oracle.oracle_filter_project(state, None, ["s"]))
        exp, cols = oracle.oracle_groupby_aggregate(d, ["s"], [], [("n", "COUNT", "s")])
        oracle.assert_view_matches(client, gid, cols, exp, ctx=ctx)

    # The oracle models no HAVING, so `h` is compared directly.
    check_global("after-insert")
    assert bag(scanned(client, sn, "h"), "k", "n") == {(10, 2): 1}

    client.execute_sql("DELETE FROM ev WHERE pk = 6", schema_name=sn)
    oracle.apply_delete(state, "pk", [6])
    check_global("after-delete")

    client.execute_sql("INSERT INTO ev VALUES (7, 20, 1, 'z')", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 7, "k": 20, "u": 1, "s": "z"}])
    check_global("after-insert-crossing-having")
    assert bag(scanned(client, sn, "h"), "k", "n") == {(10, 2): 1, (20, 2): 1}


def test_distinct_aggregates_of_one_argument_share_one_distinct_set(client, schema_name):
    """Every DISTINCT aggregate over one argument rides the same DISTINCT segment,
    and a computed argument or group key is materialized below it."""
    sn = schema_name
    _events_table(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT k, SUM(DISTINCT u) AS s, MAX(DISTINCT u) AS m, "
        "COUNT(DISTINCT u) AS n FROM ev WHERE k < 30 GROUP BY k", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW w AS SELECT COUNT(DISTINCT u % 2) AS n FROM ev GROUP BY k * 2",
        schema_name=sn)
    # Group 10 dedups u=7 (two carriers) → SUM 7+8, MAX 8, COUNT 2.
    assert bag(scanned(client, sn, "v"), "k", "s", "m", "n") == {
        (10, 15, 8, 2): 1, (20, 7, 7, 1): 1}
    assert bag(scanned(client, sn, "w"), "n") == {(0,): 1, (1,): 1, (2,): 1}


def test_min_max_distinct_is_the_plain_aggregate(client, schema_name):
    """`MIN`/`MAX(DISTINCT x)` is `MIN`/`MAX(x)`, so the binder drops the
    qualifier — which lets such a call keep company the all-or-nothing DISTINCT
    rule would otherwise refuse: here a plain `COUNT(*)` and a second DISTINCT
    argument."""
    sn = schema_name
    _events_table(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT k, MAX(DISTINCT u) AS m, MIN(DISTINCT pk) AS lo, "
        "COUNT(*) AS c FROM ev GROUP BY k", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "k", "m", "lo", "c") == {
        (10, 8, 1, 3): 1, (20, 7, 4, 2): 1, (30, None, 6, 1): 1}


def test_a_distinct_aggregate_on_an_adhoc_read_is_rejected(client, schema_name):
    """The ad-hoc read path lowers a grouped body to one stateless fold over one
    scan, with nowhere to put the DISTINCT segment."""
    sn = schema_name
    _events_table(client, sn, fill=False)
    with pytest.raises(Exception, match="CREATE VIEW body only"):
        client.execute_sql(
            "SELECT k, COUNT(DISTINCT u) AS n FROM ev GROUP BY k", schema_name=sn)


def test_a_group_by_over_a_compound_key_emits_it_in_source_order_at_any_arity(
        client, schema_name):
    """The grouping *list* is not the output key order: a reduce over the whole
    source key emits it in the key's own declared order, whatever order the
    GROUP BY named its columns. Values are chosen so a transposed mapping is
    observable — `ka` must carry `a`'s values, not `b`'s.

    HAVING binds by source name against the same output, so a filter on the
    leading column and one on the trailing column select different groups; a
    mis-binding would silently answer with the other column's verdict. The
    four-column case is the widest key a reduce can carry, and drives the
    non-linear aggregates' value index at that width.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t2 (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
        "v BIGINT NOT NULL, PRIMARY KEY (a, b))", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW perm AS SELECT a AS ka, b AS kb, SUM(v) AS s FROM t2 GROUP BY b, a",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW hav AS SELECT a AS ka, b AS kb, SUM(v) AS s FROM t2 "
        "GROUP BY a, b HAVING a > 1", schema_name=sn)
    _, ps = client.resolve_table(sn, "perm")
    assert ps.pk_indices == [0, 1], "a permuted grouping list keeps source-order key"

    client.execute_sql("INSERT INTO t2 VALUES (1, 7, 100), (2, 8, 200), (3, 1, 300)",
                       schema_name=sn)
    assert bag(scanned(client, sn, "perm"), "ka", "kb", "s") == {
        (1, 7, 100): 1, (2, 8, 200): 1, (3, 1, 300): 1}
    # `a > 1` keeps (2,8) and (3,1); a `b > 1` mis-binding would keep (1,7) and
    # (2,8) instead — a different pair, not a different count.
    assert bag(scanned(client, sn, "hav"), "ka", "kb", "s") == {
        (2, 8, 200): 1, (3, 1, 300): 1}

    client.execute_sql(
        "CREATE TABLE t4 (a INT UNSIGNED NOT NULL, b INT UNSIGNED NOT NULL, "
        "c INT UNSIGNED NOT NULL, d INT UNSIGNED NOT NULL, v BIGINT NOT NULL, "
        "PRIMARY KEY (a, b, c, d))", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW g4 AS SELECT a, b, c, d, COUNT(*) AS n, MIN(v) AS lo, MAX(v) AS hi "
        "FROM t4 GROUP BY a, b, c, d", schema_name=sn)
    quads = [(1, 1, 1, 1, 10), (1, 1, 1, 2, 20), (1, 2, 3, 4, 30), (2, 1, 1, 1, 40)]
    client.execute_sql(
        "INSERT INTO t4 VALUES " + ", ".join(str(q) for q in quads), schema_name=sn)

    cols = ("a", "b", "c", "d", "n", "lo", "hi")
    assert bag(scanned(client, sn, "g4"), *cols) == \
        {(a, b, c, d, 1, v, v): 1 for a, b, c, d, v in quads}
    # Retracting a singleton group's only row retracts its extremes with it,
    # rather than leaving the value index holding a group with no rows.
    client.execute_sql("DELETE FROM t4 WHERE a = 1 AND b = 1 AND c = 1 AND d = 2",
                       schema_name=sn)
    assert bag(scanned(client, sn, "g4"), *cols) == \
        {(a, b, c, d, 1, v, v): 1 for a, b, c, d, v in quads if (a, b, c, d) != (1, 1, 1, 2)}
