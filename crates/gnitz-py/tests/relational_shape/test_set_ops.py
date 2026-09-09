"""The set-operation graph: UNION / INTERSECT / EXCEPT in both quantifiers,
`MINUS`, and SELECT DISTINCT.

A set op is join-free — a linear combination of `{union, negate}` and the
weight-clamp primitive over content-hashed leaves — so what it computes is a
weight, never a row set. An `ALL` branch overlap legitimately carries weight 2,
and a tuple that should net to 0 surviving at weight 1 is invisible to a row
count, so every assertion here is a weighted bag against the from-scratch
oracle.

Run with GNITZ_WORKERS=4: each side is repartitioned by its content hash, so
both branches of a tuple land on one worker only if the hash is a pure function
of the logical value.
"""
from collections import Counter

import pytest
import gnitz
import _oracle as oracle
from _read import bag, scanned

_SIX_OPS = ["UNION ALL", "UNION", "INTERSECT ALL", "INTERSECT", "EXCEPT ALL", "EXCEPT"]


def _ab(client, sn):
    """The two single-PK `a`/`b` tables the projected set-op views read."""
    for name in ("a", "b"):
        client.execute_sql(
            f"CREATE TABLE {name} (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name=sn)


def _setop_view(client, sn, name, op):
    """`SELECT val FROM a <op> SELECT val FROM b` — projected onto `val` so a
    value's multiplicity, not its PK, is what the operator sees."""
    client.execute_sql(
        f"CREATE VIEW {name} AS SELECT val FROM a {op} SELECT val FROM b", schema_name=sn)
    return client.resolve_table(sn, name)[0]


def _expect(client, vid, op, a_state, b_state, ctx):
    """Assert the view equals the oracle's `op` over the two base states."""
    oracle.assert_view_matches(
        client, vid, ["val"],
        oracle.oracle_setop(op,
                            oracle.oracle_filter_project(a_state, None, ["val"]),
                            oracle.oracle_filter_project(b_state, None, ["val"])),
        ctx=ctx)


@pytest.mark.parametrize("op", _SIX_OPS)
def test_an_operator_tracks_its_weight_algebra_through_every_mutation(client, schema_name, op):
    """Each of the six operators equals its Z-set definition after every epoch of
    an insert / delete / update churn, over branches that carry a value more than
    once. Multiplicity is the point: `UNION ALL` sums it, `INTERSECT ALL` takes
    the min and `EXCEPT ALL` the clamped difference, while the deduplicating
    three collapse the same input to weight 1. The right-only value 30 (cR=2,
    cL=0) drives the clamp's pre-image integral net-negative, which no
    base-positive input reaches."""
    sn = schema_name
    _ab(client, sn)
    vid = _setop_view(client, sn, "v", op)
    a_state, b_state = {}, {}

    # Left {10:3, 20:1} — three rows carry one value, so a DISTINCT/ALL split shows.
    client.execute_sql("INSERT INTO a VALUES (1,10),(2,10),(3,10),(4,20)", schema_name=sn)
    oracle.apply_insert(a_state, "pk", [
        {"pk": 1, "val": 10}, {"pk": 2, "val": 10},
        {"pk": 3, "val": 10}, {"pk": 4, "val": 20}])
    _expect(client, vid, op, a_state, b_state, "left only")

    # Right {10:1, 30:2}: 10 overlaps at unequal multiplicity, 30 is right-only.
    client.execute_sql("INSERT INTO b VALUES (5,10),(6,30),(7,30)", schema_name=sn)
    oracle.apply_insert(b_state, "pk", [
        {"pk": 5, "val": 10}, {"pk": 6, "val": 30}, {"pk": 7, "val": 30}])
    _expect(client, vid, op, a_state, b_state, "both")

    # Retract the lone right 10: the overlap lapses entirely.
    client.execute_sql("DELETE FROM b WHERE pk = 5", schema_name=sn)
    oracle.apply_delete(b_state, "pk", [5])
    _expect(client, vid, op, a_state, b_state, "right 10 retracted")

    # An UPDATE is a retraction and an insertion in one epoch, moving 20 onto the
    # right-only value 30 — an exit and an entry crossing together.
    client.execute_sql("UPDATE a SET val = 30 WHERE pk = 4", schema_name=sn)
    oracle.apply_update(a_state, "pk", 4, {"val": 30})
    _expect(client, vid, op, a_state, b_state, "left 20 moved onto 30")

    client.execute_sql("DELETE FROM a WHERE pk IN (1, 2, 3)", schema_name=sn)
    oracle.apply_delete(a_state, "pk", [1, 2, 3])
    _expect(client, vid, op, a_state, b_state, "left reduced to one value")


def test_a_branch_may_arrive_before_the_other_exists(client, schema_name):
    """The right branch populated first, so each left delta meets a full right
    trace instead of an empty one. All six operators reach the same value as the
    opposite arrival order — an incremental circuit's fixpoint cannot depend on
    which source ticked first."""
    sn = schema_name
    _ab(client, sn)
    vids = {op: _setop_view(client, sn, f"v{i}", op) for i, op in enumerate(_SIX_OPS)}
    a_state, b_state = {}, {}

    client.execute_sql("INSERT INTO b VALUES (5,20),(6,20),(7,30)", schema_name=sn)
    oracle.apply_insert(b_state, "pk", [
        {"pk": 5, "val": 20}, {"pk": 6, "val": 20}, {"pk": 7, "val": 30}])
    for op, vid in vids.items():
        _expect(client, vid, op, a_state, b_state, f"{op} right only")

    client.execute_sql("INSERT INTO a VALUES (1,10),(2,20),(3,30)", schema_name=sn)
    oracle.apply_insert(a_state, "pk", [
        {"pk": 1, "val": 10}, {"pk": 2, "val": 20}, {"pk": 3, "val": 30}])
    for op, vid in vids.items():
        _expect(client, vid, op, a_state, b_state, f"{op} left after right")


@pytest.mark.parametrize("op", ["EXCEPT", "INTERSECT"])
def test_a_value_crosses_the_clamp_boundary_in_both_directions(client, schema_name, op):
    """The deduplicating EXCEPT and INTERSECT both route through
    `positive_part`, so one value driven back and forth across set membership —
    including the tick where its pre-image integral is −1 while the output stays
    clamped at 0 — pins the boundary emit in each direction."""
    sn = schema_name
    _ab(client, sn)
    vid = _setop_view(client, sn, "v", op)
    a_state, b_state = {}, {}

    client.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name=sn)
    oracle.apply_insert(a_state, "pk", [{"pk": 1, "val": 10}])
    _expect(client, vid, op, a_state, b_state, "left only: da − db = 1")

    client.execute_sql("INSERT INTO b VALUES (2, 10)", schema_name=sn)
    oracle.apply_insert(b_state, "pk", [{"pk": 2, "val": 10}])
    _expect(client, vid, op, a_state, b_state, "both: da − db = 0")

    client.execute_sql("DELETE FROM a WHERE pk = 1", schema_name=sn)
    oracle.apply_delete(a_state, "pk", [1])
    _expect(client, vid, op, a_state, b_state, "right only: da − db = −1")

    client.execute_sql("DELETE FROM b WHERE pk = 2", schema_name=sn)
    oracle.apply_delete(b_state, "pk", [2])
    _expect(client, vid, op, a_state, b_state, "neither: da − db = 0")

    client.execute_sql("INSERT INTO a VALUES (3, 10)", schema_name=sn)
    oracle.apply_insert(a_state, "pk", [{"pk": 3, "val": 10}])
    _expect(client, vid, op, a_state, b_state, "left only again")


def test_identity_is_the_whole_row_under_select_star(client, schema_name):
    """An unprojected set op compares (PK, payload), not the PK: b's (1, 999)
    does not exclude a's (1, 100) from the EXCEPT, and an UPDATE — one PK
    carrying a retraction and an insertion in the same delta — moves two distinct
    elements rather than one key. `UNION ALL` of the identical row on both sides
    keeps both copies — only that operator gives the right branch its own hash
    id, so the view sink never sees one synthetic key twice."""
    sn = schema_name
    _ab(client, sn)
    for name, op in (("exc", "EXCEPT"), ("inter", "INTERSECT"), ("ua", "UNION ALL")):
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT * FROM a {op} SELECT * FROM b", schema_name=sn)

    client.execute_sql("INSERT INTO b VALUES (1, 999), (2, 200)", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 100), (2, 200)", schema_name=sn)
    # (1, 999) and (1, 100) share a PK and differ in payload: two elements.
    assert bag(scanned(client, sn, "exc"), "pk", "val") == {(1, 100): 1}
    assert bag(scanned(client, sn, "inter"), "pk", "val") == {(2, 200): 1}
    assert bag(scanned(client, sn, "ua"), "pk", "val") == \
        {(1, 100): 1, (1, 999): 1, (2, 200): 2}

    # The UPDATE retracts (2, 200) and inserts (2, 300); only the first was
    # excluded by b, so the row re-enters EXCEPT and leaves INTERSECT.
    client.execute_sql("UPDATE a SET val = 300 WHERE pk = 2", schema_name=sn)
    assert bag(scanned(client, sn, "exc"), "pk", "val") == {(1, 100): 1, (2, 300): 1}
    assert bag(scanned(client, sn, "inter"), "pk", "val") == {}
    assert bag(scanned(client, sn, "ua"), "pk", "val") == \
        {(1, 100): 1, (1, 999): 1, (2, 200): 1, (2, 300): 1}


def test_a_distinct_tuple_lives_exactly_while_a_row_carries_it(client, schema_name):
    """DISTINCT is the non-linear boundary operator (DBSP Prop 4.7): a projected
    tuple sits at weight 1 while its accumulated weight is positive, whatever
    number of rows carry it, and leaves only when the last one is retracted. Its
    identity is the whole tuple, so two rows sharing one component are two
    tuples, and an UPDATE that moves the last carrier crosses an exit and an
    entry boundary in one epoch."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE VIEW v AS SELECT DISTINCT a, b FROM t", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]
    state = {}

    def check(ctx):
        oracle.assert_view_matches(
            client, vid, ["a", "b"],
            oracle.oracle_distinct(oracle.oracle_filter_project(state, None, ["a", "b"])),
            ctx=ctx)

    # (1,1) carried twice; (1,2) shares `a` with it and (2,1) shares `b`.
    client.execute_sql(
        "INSERT INTO t VALUES (1,1,1), (2,1,1), (3,1,2), (4,2,1), (5,3,3)", schema_name=sn)
    oracle.apply_insert(state, "pk", [
        {"pk": 1, "a": 1, "b": 1}, {"pk": 2, "a": 1, "b": 1}, {"pk": 3, "a": 1, "b": 2},
        {"pk": 4, "a": 2, "b": 1}, {"pk": 5, "a": 3, "b": 3}])
    check("after insert")

    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
    oracle.apply_delete(state, "pk", [1])
    check("one of two carriers gone: boundary not crossed")

    client.execute_sql("DELETE FROM t WHERE pk = 5", schema_name=sn)
    oracle.apply_delete(state, "pk", [5])
    check("last carrier gone: exit boundary crossed")

    client.execute_sql("UPDATE t SET a = 4 WHERE pk = 2", schema_name=sn)
    oracle.apply_update(state, "pk", 2, {"a": 4})
    check("last carrier moved: exit and entry in one epoch")

    # Onto a tuple that already exists: a second carrier, not a second row.
    client.execute_sql("UPDATE t SET a = 2, b = 1 WHERE pk = 3", schema_name=sn)
    oracle.apply_update(state, "pk", 3, {"a": 2, "b": 1})
    check("moved onto an existing tuple")

    client.execute_sql("DELETE FROM t WHERE pk = 4", schema_name=sn)
    oracle.apply_delete(state, "pk", [4])
    check("other carrier of that tuple gone")

    client.execute_sql("INSERT INTO t VALUES (6, 3, 3)", schema_name=sn)
    oracle.apply_insert(state, "pk", [{"pk": 6, "a": 3, "b": 3}])
    check("re-entry")


def test_a_nullable_branch_makes_the_output_nullable(client, schema_name):
    """Output nullability is the union of both inputs, so a NOT NULL left paired
    with a nullable right yields a nullable column and the reader consults the
    null bitmap instead of reading 0. NULL is its own value throughout: it never
    coincides with a genuine 0, several NULLs coalesce to one under a
    deduplicating op and keep their multiplicity under `ALL`."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)", schema_name=sn)
    views = {}
    for op in ("UNION ALL", "UNION", "INTERSECT", "EXCEPT"):
        name = "v" + op.replace(" ", "_").lower()
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT val FROM t1 {op} SELECT val FROM t2", schema_name=sn)
        views[op] = client.resolve_table(sn, name)[0]

    client.execute_sql("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO t2 VALUES (10, NULL), (11, NULL), (12, 0)", schema_name=sn)

    for op, expected in (
        ("UNION ALL", {(10,): 1, (20,): 1, (0,): 2, (None,): 2}),
        ("UNION", {(10,): 1, (20,): 1, (0,): 1, (None,): 1}),
        ("INTERSECT", {(0,): 1}),
        ("EXCEPT", {(10,): 1, (20,): 1}),
    ):
        oracle.assert_view_matches(client, views[op], ["val"], expected, ctx=op)


# ---------------------------------------------------------------------------
# Cross-width / cross-sign branches
#
# The narrower side is widened to the pair's common type before hashing, so
# equal logical values reach one physical representation and coalesce; a value
# outside the narrow type's range must survive the widening unchanged.
# ---------------------------------------------------------------------------

# (label, left type, right type, shared, left-only, right-only). One `*_only`
# value always exceeds the other side's range.
_WIDTH_CASES = [
    ("i32_i64", "INT", "BIGINT", 5, 7, 8_000_000_000),                 # → I64
    ("u16_u32", "SMALLINT UNSIGNED", "INT UNSIGNED", 5, 7, 200_000),   # → U32
    ("u32_i32", "INT UNSIGNED", "INT", 5, 3_000_000_000, 9),           # → I64
]


def _promotion_views(client, sn, lt, rt, lnull="NOT NULL", rnull="NOT NULL"):
    """`t1(val lt)` / `t2(val rt)` plus the three deduplicating set-op views over
    their projected `val`. Returns `(union, intersect, except)` tids."""
    client.execute_sql(
        f"CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, val {lt} {lnull})", schema_name=sn)
    client.execute_sql(
        f"CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, val {rt} {rnull})", schema_name=sn)
    out = []
    for name, op in (("vu", "UNION"), ("vi", "INTERSECT"), ("ve", "EXCEPT")):
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT val FROM t1 {op} SELECT val FROM t2", schema_name=sn)
        out.append(client.resolve_table(sn, name)[0])
    return out


@pytest.mark.parametrize("label,lt,rt,shared,left_only,right_only", _WIDTH_CASES)
def test_a_narrower_branch_widens_to_the_common_type(
        client, schema_name, label, lt, rt, shared, left_only, right_only):
    """Equal values of different declared width hash to one content key, so the
    shared value coalesces under UNION, is the whole INTERSECT and cancels under
    EXCEPT — while a value that only the wider type can hold stays distinct."""
    sn = schema_name
    vu, vi, ve = _promotion_views(client, sn, lt, rt)
    client.execute_sql(f"INSERT INTO t2 VALUES (1, {shared}), (2, {right_only})", schema_name=sn)
    client.execute_sql(f"INSERT INTO t1 VALUES (1, {shared}), (2, {left_only})", schema_name=sn)

    oracle.assert_view_matches(
        client, vu, ["val"],
        {(shared,): 1, (left_only,): 1, (right_only,): 1}, ctx=f"{label} UNION")
    oracle.assert_view_matches(client, vi, ["val"], {(shared,): 1}, ctx=f"{label} INTERSECT")
    oracle.assert_view_matches(client, ve, ["val"], {(left_only,): 1}, ctx=f"{label} EXCEPT")


def test_a_widened_null_coalesces_and_never_collides_with_zero(client, schema_name):
    """A NULL on the widened (INT) side matches a NULL on the native (BIGINT)
    side and cancels under EXCEPT, while a non-null 0 stays its own value — the
    value bytes are skipped when the row's null bit is set, so the two can never
    hash together."""
    sn = schema_name
    vu, vi, ve = _promotion_views(client, sn, "INT", "BIGINT", lnull="NULL", rnull="NULL")
    client.execute_sql(
        "INSERT INTO t2 VALUES (1, NULL), (2, 5), (3, 8000000000)", schema_name=sn)
    client.execute_sql("INSERT INTO t1 VALUES (1, NULL), (2, 5), (3, 0)", schema_name=sn)

    oracle.assert_view_matches(
        client, vu, ["val"],
        {(None,): 1, (5,): 1, (0,): 1, (8000000000,): 1}, ctx="UNION")
    oracle.assert_view_matches(client, vi, ["val"], {(None,): 1, (5,): 1}, ctx="INTERSECT")
    oracle.assert_view_matches(client, ve, ["val"], {(0,): 1}, ctx="EXCEPT")


def test_a_widened_pk_column_sign_extends_from_its_source_width(client, schema_name):
    """A branch that projects a narrow PK column — INT, second in a compound PK,
    so its OPK offset is non-zero — decodes at the source width and
    sign-extends into the wider slot. A negative id is what discriminates:
    zero-extension would read -3 as 4294967293 and break the coalesce."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t1 (a BIGINT NOT NULL, id INT NOT NULL, filler BIGINT NOT NULL, "
        "PRIMARY KEY (a, id))", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
    out = []
    for name, op in (("vu", "UNION"), ("vi", "INTERSECT"), ("ve", "EXCEPT")):
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT id FROM t1 {op} SELECT val FROM t2", schema_name=sn)
        out.append(client.resolve_table(sn, name)[0])
    vu, vi, ve = out

    client.execute_sql("INSERT INTO t2 VALUES (1, 5), (2, -3), (3, 100)", schema_name=sn)
    client.execute_sql("INSERT INTO t1 VALUES (1, 5, 0), (2, -3, 0), (3, 7, 0)", schema_name=sn)

    oracle.assert_view_matches(
        client, vu, ["id"], {(5,): 1, (-3,): 1, (7,): 1, (100,): 1}, ctx="UNION")
    oracle.assert_view_matches(client, vi, ["id"], {(5,): 1, (-3,): 1}, ctx="INTERSECT")
    oracle.assert_view_matches(client, ve, ["id"], {(7,): 1}, ctx="EXCEPT")


def test_every_projected_column_is_part_of_the_promoted_identity(client, schema_name):
    """A two-column projection where one column is promoted (INT vs BIGINT) and
    the other is not: membership flips only on the full pair, so a row agreeing
    in one column alone is not a match, and retracting the sole match empties the
    view."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, k INT NOT NULL, tag BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, tag BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT k, tag FROM t1 INTERSECT SELECT k, tag FROM t2", schema_name=sn)
    vid = client.resolve_table(sn, "v")[0]

    client.execute_sql("INSERT INTO t1 VALUES (1, 5, 100)", schema_name=sn)
    oracle.assert_view_matches(client, vid, ["k", "tag"], {}, ctx="left only")

    client.execute_sql("INSERT INTO t2 VALUES (1, 5, 100)", schema_name=sn)
    oracle.assert_view_matches(client, vid, ["k", "tag"], {(5, 100): 1}, ctx="INT 5 meets BIGINT 5")

    # Same k with another tag, and another k with the same tag: neither matches.
    client.execute_sql("INSERT INTO t2 VALUES (2, 5, 200), (3, 6, 100)", schema_name=sn)
    oracle.assert_view_matches(client, vid, ["k", "tag"], {(5, 100): 1}, ctx="both columns count")

    client.execute_sql("DELETE FROM t2 WHERE pk = 1", schema_name=sn)
    oracle.assert_view_matches(client, vid, ["k", "tag"], {}, ctx="sole match retracted")


# (label, left type, right type, error substring) — a pair with no common
# ≤8-byte integer layout must error rather than read the bytes at one width.
_REJECT_CASES = [
    ("string_vs_int", "TEXT", "BIGINT", "type mismatch"),
    ("u64_vs_i64", "BIGINT UNSIGNED", "BIGINT", "type mismatch"),   # → I128
    ("float", "DOUBLE", "DOUBLE", "float column"),                  # no byte-equal key
]


@pytest.mark.parametrize("label,lt,rt,err", _REJECT_CASES)
def test_branches_with_no_common_key_layout_are_refused(client, schema_name, label, lt, rt, err):
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, val {lt} NOT NULL)", schema_name=sn)
    client.execute_sql(
        f"CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, val {rt} NOT NULL)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match=err):
        client.execute_sql(
            "CREATE VIEW v AS SELECT val FROM t1 UNION ALL SELECT val FROM t2", schema_name=sn)


def test_minus_is_except_down_to_the_quantifier_and_precedence(client, schema_name):
    """`MINUS` is Oracle's spelling of `EXCEPT`: the same operator, taking the
    same `ALL` / `DISTINCT` quantifier and binding at the same precedence, so a
    mixed chain associates identically. `a` carries 20 twice, so the ALL /
    DISTINCT split is a weight rather than a matching row set."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE TABLE c (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)", schema_name=sn)
    pairs = {q: (_setop_view(client, sn, f"m{i}", f"MINUS{q}"),
                 _setop_view(client, sn, f"e{i}", f"EXCEPT{q}"))
             for i, q in enumerate(["", " ALL", " DISTINCT"])}
    chain = "SELECT val FROM a UNION SELECT val FROM b {} SELECT val FROM c"
    client.execute_sql(f"CREATE VIEW mc AS {chain.format('MINUS')}", schema_name=sn)
    client.execute_sql(f"CREATE VIEW ec AS {chain.format('EXCEPT')}", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1,10), (2,20), (3,20), (4,30)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1,30), (2,40)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (1,30)", schema_name=sn)

    for q, (mid, eid) in pairs.items():
        got = oracle.scan_multiset(client, mid, ["val"])
        assert got == oracle.scan_multiset(client, eid, ["val"]), f"MINUS{q} != EXCEPT{q}: {got}"
        # 30 is in b, so it leaves; 20 keeps weight 2 only under ALL.
        assert got == Counter({(10,): 1, (20,): 2 if q == " ALL" else 1}), f"MINUS{q}: {got}"

    # `(a ∪ b) − c` associates left to right: {10,20,30,40} − {30}.
    got = oracle.scan_multiset(client, client.resolve_table(sn, "mc")[0], ["val"])
    assert got == oracle.scan_multiset(client, client.resolve_table(sn, "ec")[0], ["val"])
    assert got == Counter({(10,): 1, (20,): 1, (40,): 1}), got


def test_by_name_set_operations_are_refused_rather_than_aligned_positionally(client, schema_name):
    """`BY NAME` pairs branch columns by name; nothing implements that, so
    accepting it would silently run the positional pairing instead. A refused
    CREATE registers no view."""
    sn = schema_name
    _ab(client, sn)
    for i, op in enumerate(["UNION ALL", "INTERSECT", "EXCEPT"]):
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(
                f"CREATE VIEW rej{i} AS SELECT val FROM a {op} BY NAME SELECT val FROM b",
                schema_name=sn)
        with pytest.raises(gnitz.GnitzError):
            client.resolve_table(sn, f"rej{i}")


def test_a_view_body_refuses_the_clauses_its_shape_would_drop(client, schema_name):
    """Every CREATE VIEW shape (simple, grouped, join, set-op, DISTINCT) reads
    only a hand-picked subset of the SELECT. A clause a shape does not consume is
    refused by name, not dropped — a dropped clause runs a different query than
    the caller wrote."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)", schema_name=sn)

    def rejects(sql, msg):
        with pytest.raises(gnitz.GnitzError, match=msg):
            client.execute_sql(sql, schema_name=sn)

    # DISTINCT ON: a single SELECT, a set-op branch, and the degenerate
    # `DISTINCT ON (a) a` — refused on purpose rather than folded to `DISTINCT a`.
    for sql in (
        "CREATE VIEW v AS SELECT DISTINCT ON (a) a, b FROM t",
        "CREATE VIEW v AS SELECT DISTINCT ON (a) a FROM t UNION SELECT b FROM t",
        "CREATE VIEW v AS SELECT DISTINCT ON (a) a FROM t",
    ):
        rejects(sql, "DISTINCT ON is not supported")

    rejects("CREATE VIEW v AS SELECT DISTINCT a FROM t GROUP BY a", "GROUP BY is not supported")
    rejects("CREATE VIEW v AS SELECT DISTINCT a FROM t HAVING a > 0", "HAVING is not supported")
    # A HAVING without DISTINCT drops nothing: it groups the whole relation, so
    # the body binds as a global aggregate and `a` — neither a group key nor an
    # aggregate — is what fails.
    rejects("CREATE VIEW v AS SELECT a FROM t HAVING a > 0",
            "column 'a' must appear in GROUP BY or an aggregate function")
    rejects("CREATE VIEW v AS SELECT a FROM t GROUP BY ALL", "GROUP BY")

    rejects("CREATE VIEW v AS SELECT DISTINCT a FROM t PREWHERE a > 5", "PREWHERE is not supported")
    rejects("CREATE VIEW v AS SELECT a FROM t PREWHERE a > 5", "PREWHERE is not supported")
    rejects("CREATE VIEW v AS SELECT a, COUNT(*) FROM t PREWHERE a > 5 GROUP BY a",
            "PREWHERE is not supported")
    rejects("CREATE VIEW v AS SELECT DISTINCT TOP 5 a FROM t", "TOP is not supported")
    rejects("CREATE VIEW v AS SELECT a FROM t FETCH FIRST 5 ROWS ONLY", "FETCH is not supported")
    rejects("CREATE VIEW v AS SELECT a FROM t SORT BY a", "SORT BY is not supported")
    # QUALIFY filters on window values, ahead of a DISTINCT: one with no window
    # function to filter on is refused, not dropped.
    rejects("CREATE VIEW v AS SELECT DISTINCT a FROM t QUALIFY a > 1",
            "QUALIFY needs a window function")
    rejects("CREATE VIEW v AS SELECT pk FROM t FOR UPDATE", "FOR UPDATE/SHARE is not supported")
    rejects("CREATE VIEW v AS SELECT pk FROM t SETTINGS max_threads = 1",
            "SETTINGS is not supported")
    rejects("CREATE VIEW v AS SELECT pk FROM t FORMAT JSON", "FORMAT is not supported")

    # Positive controls: every honoured shape still compiles, including the ones
    # whose clauses the list above refuses elsewhere — a grouped or DISTINCT
    # set-op side becomes a hidden segment, and an OUTER range join consumes its
    # own WHERE as a post-null-fill 3VL filter.
    for name, body in {
        "vsimple": "SELECT a, b FROM t WHERE a > 0",
        "vdistinct": "SELECT DISTINCT a, b FROM t",
        "vgroup": "SELECT a, COUNT(*) FROM t GROUP BY a",
        "vjoin": "SELECT t.a, u.c FROM t JOIN u ON t.pk = u.pk",
        "vsetop": "SELECT a FROM t UNION ALL SELECT b FROM t",
        "vsetop_side": "SELECT DISTINCT a FROM t UNION ALL SELECT b FROM t GROUP BY b",
        "vrangeleft": "SELECT t.a FROM t LEFT JOIN u ON t.a < u.c WHERE t.a > 5",
    }.items():
        client.execute_sql(f"CREATE VIEW {name} AS {body}", schema_name=sn)
        client.resolve_table(sn, name)
